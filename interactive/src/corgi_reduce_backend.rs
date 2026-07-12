//! The corgi `ProxyReduceBackend`: the value semantics for the DD `ProxyReduceTactic`.
//!
//! The tactic (differential's `operators::int_proxy::reduce`) owns ALL time/lattice logic over
//! integer proxies `(key_hash, value_id, time, diff)`; this backend supplies only:
//!
//!   * ids — `key_hash`/`value_id` are value-as-id for primitive columns (the value IS the id) and
//!     the canonical native `corgi::hash` for compound columns (columnar, content-addressed, so ids
//!     coincide across the output→input boundary); DD never hashes.
//!   * the value callback — `reduce_many` runs ONE crossing per retire over every `(key, time)`
//!     bracket, building the output value COLUMNS directly (Count → a `u64` prim, Distinct → a
//!     `Unit`, Min → the chosen input rows, Collect → a `List`), never through DDIR rows.
//!   * materialize — resolve proxy ids back to real columns by `gather` from per-retire pools and
//!     seal a `CorgiChunk` batch column-natively.
//!
//! Transcode-free: the real keys/values never leave corgi columns. Ids are resolved to rows by
//! integer index (`key_index`/`val_index` → offsets into the concatenated `key_blocks`/`val_blocks`
//! pools), not by carrying `DValue`s. Min/Collect's ordering is corgi's own — one `sort_blocks` per
//! retire orders every bracket's candidates (Min = each block's first, Collect = each block's sorted
//! run, expanded by diff). This uses corgi's STRUCTURAL order, which equals DDIR `Ord` for the
//! non-negative scalar/tuple values these reductions see (all 6 canonical programs); it diverges only
//! for negative ints (corgi's leaf compare is unsigned) and list-valued compares (corgi lists order
//! length-first) — neither arises here. A signed/​list-general order would need a corgi order fix
//! (offset-binary leaf or lex-first lists), not a change here.
//!
//! The changed-key restriction is honored by presenting only the changed keys: novel batches are
//! read whole (delta-sized), the accumulated history is scanned and filtered to the changed hashes
//! (a columnar semijoin — matching the row-wise tactic's read).

use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;


use differential_dataflow::trace::Description;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::operators::int_proxy::ProxyBridge;
use differential_dataflow::operators::int_proxy::reduce::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

use corgi::arrange::{gather, gather_lanes, sort_blocks};
use corgi::{Bounds, Shape, Value as CValue};

use crate::col_times::ColTime;
use crate::corgi_chunk::{columns_to_batch, CorgiChunk, Presentation};
use crate::ir::Diff;
use crate::parse::Reducer;

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// An identity `Hasher` for the id-index maps: their keys are already well-distributed 64-bit
/// content hashes (`hash_rows`), so passing the id straight through avoids re-hashing it (siphash
/// on `register_keys`/lookups was ~7% of the reduce in profiling). Only `write_u64` is used.
#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    #[inline]
    fn write_u64(&mut self, i: u64) { self.0 = i; }
    #[inline]
    fn write(&mut self, _: &[u8]) { unreachable!("IdMap keys are u64") }
    #[inline]
    fn finish(&self) -> u64 { self.0 }
}
/// `key_hash`/`value_id` → row index, hashed by identity.
type IdMap = HashMap<u64, usize, BuildHasherDefault<IdHasher>>;

/// A corgi reduce backend for a single `Reducer`. All per-retire scratch is corgi columns + integer
/// id→row-index maps; nothing carries a `DValue`.
pub struct CorgiReduceBackend<T> {
    reducer: Reducer,
    /// Input value column for the current window, indexed by `in_index` (for Min/Collect resolution).
    in_vals: CValue,
    /// Input `value_id → row` in `in_vals` for the current window (reduce-time resolution; first row
    /// wins, so equal values — which share a content-hash `value_id` — resolve to one representative).
    in_index: IdMap,
    /// Output tiling for `begin`/`emit`/`finish`: the tile descriptions, and per-tile accumulated
    /// output rows `(key row, value row, time, diff)` (pool indices, gathered into columns at `finish`).
    tiles: Vec<Description<T>>,
    tile_rows: Vec<(Vec<usize>, Vec<usize>, Vec<T>, Vec<Diff>)>,
    /// Key-resolution pool for the current retire: `key_hash → row index` into the concatenation of
    /// `key_blocks` (representative keys from the input + output presentations).
    key_index: IdMap,
    key_blocks: Vec<CValue>,
    key_len: usize,
    /// Value-resolution pool for the current retire: `value_id → row index` into the concatenation of
    /// `val_blocks` (output-history values + values minted by `reduce_many`).
    val_index: IdMap,
    val_blocks: Vec<CValue>,
    val_len: usize,
    /// Two-phase rank-window stash: hit ranges and per-chunk store-rank maps computed by the
    /// rank `next_window`, consumed by `present_historical` (which re-derives the same chunk
    /// lists deterministically from the instance). `rank_novel_*` carry the novel side's
    /// gathered value column and vids so the input pool can be finished after the historical
    /// gather.
    rank_in_hits: Vec<(u32, u32, u32, u32)>,
    rank_in_maps: Vec<Vec<u32>>,
    rank_out_hits: Vec<(u32, u32, u32, u32)>,
    rank_out_maps: Vec<Vec<u32>>,
    rank_novel_vals: CValue,
    rank_novel_vids: Vec<u64>,
    _t: std::marker::PhantomData<T>,
}

impl<T> CorgiReduceBackend<T> {
    pub fn new(reducer: Reducer) -> Self {
        CorgiReduceBackend {
            reducer,
            in_vals: CValue::Unit(0),
            in_index: IdMap::default(),
            tiles: Vec::new(),
            tile_rows: Vec::new(),
            rank_in_hits: Vec::new(),
            rank_in_maps: Vec::new(),
            rank_out_hits: Vec::new(),
            rank_out_maps: Vec::new(),
            rank_novel_vals: CValue::Unit(0),
            rank_novel_vids: Vec::new(),
            key_index: IdMap::default(),
            key_blocks: Vec::new(),
            key_len: 0,
            val_index: IdMap::default(),
            val_blocks: Vec::new(),
            val_len: 0,
            _t: std::marker::PhantomData,
        }
    }

    /// Clear the resolution pools at the start of a retire (called from `next_window`'s first call).
    fn reset_pools(&mut self) {
        self.key_index.clear();
        self.key_blocks.clear();
        self.key_len = 0;
        self.val_index.clear();
        self.val_blocks.clear();
        self.val_len = 0;
    }

    /// Add representative key rows (aligned with `ids`) to the key pool; first id wins.
    fn register_keys(&mut self, col: CValue, ids: &[u64]) {
        for (i, &id) in ids.iter().enumerate() {
            self.key_index.entry(id).or_insert(self.key_len + i);
        }
        self.key_len += col.len();
        self.key_blocks.push(col);
    }

    /// Add value rows (aligned with `ids`) to the val pool; first id wins.
    fn register_vals(&mut self, col: CValue, ids: &[u64]) {
        for (i, &id) in ids.iter().enumerate() {
            self.val_index.entry(id).or_insert(self.val_len + i);
        }
        self.val_len += col.len();
        self.val_blocks.push(col);
    }
}

/// Concatenate corgi columns (skipping empties, which contribute no rows and so don't shift the
/// pool offsets accounted at registration). One `gather_lanes` over the non-empty blocks.
fn concat_columns(blocks: &[CValue]) -> CValue {
    let non_empty: Vec<&CValue> = blocks.iter().filter(|b| b.len() > 0).collect();
    match non_empty.len() {
        0 => CValue::Unit(0),
        1 => non_empty[0].clone(),
        _ => {
            let srcs: Vec<Option<&CValue>> = non_empty.iter().map(|b| Some(*b)).collect();
            let (mut tags, mut offs) = (Vec::new(), Vec::new());
            for (ti, b) in non_empty.iter().enumerate() {
                for o in 0..b.len() {
                    tags.push(ti);
                    offs.push(o);
                }
            }
            gather_lanes(&srcs, &tags, &offs)
        }
    }
}

/// Id column for a key/value column. For a PRIMITIVE column — a bare 64-bit `Prim`, or a 1-field
/// `Prod([Prim(64)])` — the value itself is already a collision-free id (`i64 as u64` is a bijection),
/// so pass it straight through and skip the content hash. Compound shapes (Unit / List / Sum /
/// multi-field `Prod`) hash via the CANONICAL native `corgi::hash` (the designed boundary-id fold,
/// width-blind and consistent-with-equality) — not the branch-local `arrange::hash_rows`; DDIR
/// transcodes every leaf to `u64`, so width-blindness is a no-op for us and there is no cross-path
/// hash comparison (value-as-id and native hash are never used for the same value: shape is uniform
/// per column). The id is used ONLY as an identity for netting/dedup — its numeric order is never
/// relied upon — so the raw two's-complement `u64` is correct even for negative ints (no swizzle).
/// Applied CONSISTENTLY at every id site (both value presentations AND the freshly-produced
/// `reduce_brackets` outputs), else `desired − current` nets across mismatched ids for the same value.
fn ids(col: &CValue) -> Vec<u64> {
    match corgi::shape_of_value(col) {
        Shape::Prim(64) => col.clone().into_u64("ids"),
        Shape::Prod(ref fs) if fs.len() == 1 && matches!(fs[0], Shape::Prim(64)) => match col {
            CValue::Prod(fields) => fields[0].clone().into_u64("ids"),
            _ => unreachable!("shape Prod but value not Prod"),
        },
        _ => corgi::hash(col).into_u64("ids"),
    }
}

/// Build a chunk's memoized hash-order [`Presentation`]: per-row key hashes and value ids, times
/// interned into a sorted distinct table, and a permutation ascending by `(key_hash, value_id,
/// time)`. Paid once per (immutable) chunk; every retire that presents the chunk reuses it.
fn build_presentation<T>(ch: &CorgiChunk<T, Diff>) -> Presentation<T>
where
    T: ColTime + Ord,
{
    let n = ch.diffs().len();
    let kh = ids(ch.keys());
    let vid = ids(ch.vals());
    let ct = ch.times();
    let mut index: BTreeMap<T, u32> = BTreeMap::new();
    let mut tfirst: Vec<u32> = Vec::with_capacity(n);
    for i in 0..n {
        let next = index.len() as u32;
        tfirst.push(*index.entry(ct.get(i)).or_insert(next));
    }
    let mut rank = vec![0u32; index.len()];
    let mut times: Vec<T> = Vec::with_capacity(index.len());
    for (pos, (t, first)) in index.into_iter().enumerate() {
        rank[first as usize] = pos as u32;
        times.push(t);
    }
    let mut perm: Vec<u32> = (0..n as u32).collect();
    perm.sort_unstable_by_key(|&i| (kh[i as usize], vid[i as usize], rank[tfirst[i as usize] as usize]));
    let khs: Vec<u64> = perm.iter().map(|&i| kh[i as usize]).collect();
    let vids: Vec<u64> = perm.iter().map(|&i| vid[i as usize]).collect();
    let tranks: Vec<u32> = perm.iter().map(|&i| rank[tfirst[i as usize] as usize]).collect();
    Presentation { perm, khs, vids, tranks, times }
}

/// One side's merged presentation: gather coordinates + per-record ids (aligned, pre-netting,
/// mirroring the previous per-record gather semantics) and the netted integer bridge records
/// (`time` as a rank into the window's global sorted-time table).
struct MergedSide {
    tags: Vec<usize>,
    offs: Vec<usize>,
    khs: Vec<u64>,
    vids: Vec<u64>,
    records: Vec<((u64, u64), u32, Diff)>,
}

/// Merge chunks' cached hash-order runs, restricted to the ascending `changed` hashes, netting
/// equal `(key_hash, value_id, time)` on the fly. `maps[c]` maps chunk `c`'s local time ranks to
/// the window's global ranks. Replaces the previous per-retire full scan + hash + comparison sort
/// (each was O(history) per retire; the scan variant with `find_ranges` seek had REGRESSED SCC —
/// broad changed sets — but merging pre-sorted cached runs is cheaper than either).
fn merge_presentations<T>(pres: &[&Presentation<T>], maps: &[Vec<u32>], diffs: &[&[Diff]], changed: &[u64]) -> MergedSide {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // Restrict each chunk's run to the changed hashes: ascending position ranges.
    let mut cursors: Vec<(Vec<(u32, u32)>, usize, u32)> = Vec::with_capacity(pres.len());
    for p in pres {
        let khs = &p.khs;
        let mut ranges = Vec::new();
        let (mut i, mut j) = (0usize, 0usize);
        while i < khs.len() && j < changed.len() {
            if khs[i] < changed[j] {
                i += 1;
            } else if khs[i] > changed[j] {
                j += 1;
            } else {
                let s = i;
                while i < khs.len() && khs[i] == changed[j] {
                    i += 1;
                }
                ranges.push((s as u32, i as u32));
                j += 1;
            }
        }
        let start = ranges.first().map(|r| r.0).unwrap_or(0);
        cursors.push((ranges, 0, start));
    }

    let mut out = MergedSide { tags: Vec::new(), offs: Vec::new(), khs: Vec::new(), vids: Vec::new(), records: Vec::new() };
    let mut heap: BinaryHeap<Reverse<(u64, u64, u32, usize)>> = BinaryHeap::new();
    let key_at = |c: usize, pos: u32| {
        let p = pres[c];
        let i = pos as usize;
        (p.khs[i], p.vids[i], maps[c][p.tranks[i] as usize], c)
    };
    for c in 0..pres.len() {
        if !cursors[c].0.is_empty() {
            heap.push(Reverse(key_at(c, cursors[c].2)));
        }
    }
    while let Some(Reverse((kh, vid, grank, c))) = heap.pop() {
        let pos = cursors[c].2;
        let row = pres[c].perm[pos as usize] as usize;
        let d = diffs[c][row];

        out.tags.push(c);
        out.offs.push(row);
        out.khs.push(kh);
        out.vids.push(vid);
        match out.records.last_mut() {
            Some(((lk, lv), lr, ld)) if *lk == kh && *lv == vid && *lr == grank => {
                *ld += d;
                if *ld == 0 {
                    out.records.pop();
                }
            }
            _ => out.records.push(((kh, vid), grank, d)),
        }

        // Advance chunk c's cursor to its next restricted position.
        let (ranges, ri, p) = &mut cursors[c];
        let mut next = *p + 1;
        if next >= ranges[*ri].1 {
            *ri += 1;
            if *ri >= ranges.len() {
                continue;
            }
            next = ranges[*ri].0;
        }
        *p = next;
        heap.push(Reverse(key_at(c, next)));
    }
    out
}

/// All chunks of a batch list, flattened (empty chunks included — `hash_rows` yields nothing for them).
fn chunks_of<T>(batches: &[CBatch<T>]) -> Vec<&CorgiChunk<T, Diff>>
where
    T: ColTime,
{
    batches.iter().flat_map(|b| b.chunks.iter()).collect()
}

impl<T> CorgiReduceBackend<T>
where
    T: ColTime + Ord,
{
    /// The one value crossing for a retire: every `(key, time)` bracket at once. Builds the output
    /// value COLUMN directly per reducer, registers it (id → row) into the val pool, and returns the
    /// proxy `(value_id, diff)` deltas with per-bracket ends. `input[k] = (rep index into the input
    /// presentation, accumulated diff)`; the bracket `i` is `input[ends[i-1]..ends[i]]`, non-empty.
    fn reduce_brackets(&mut self, ends: &[usize], input: &[(usize, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        let mut out_diffs: Vec<Diff> = Vec::new();
        let mut out_ends: Vec<usize> = Vec::with_capacity(ends.len());
        let out_ids: Vec<u64>;

        match self.reducer {
            Reducer::Count => {
                // Per-bracket sum of diffs; survivors become a `Tuple([Int(sum)])` = corgi `Prod([u64])`.
                let mut sums: Vec<u64> = Vec::new();
                let mut start = 0;
                for &end in ends {
                    let c: Diff = input[start..end].iter().map(|&(_, d)| d).sum();
                    if c > 0 {
                        sums.push(c as u64);
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                if sums.is_empty() {
                    return (Vec::new(), out_ends);
                }
                let col = CValue::Prod(vec![CValue::u64(sums)]);
                out_ids = ids(&col);
                self.register_vals(col, &out_ids);
            }
            Reducer::Distinct => {
                // Present iff any value has positive net; output value is unit (a `Unit` column).
                let mut present = 0usize;
                let mut start = 0;
                for &end in ends {
                    if input[start..end].iter().any(|&(_, d)| d > 0) {
                        present += 1;
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                if present == 0 {
                    return (Vec::new(), out_ends);
                }
                let col = CValue::Unit(present);
                out_ids = ids(&col); // all equal (unit content hash)
                self.register_vals(col, &out_ids);
            }
            Reducer::Min => {
                // The DDIR `min` over the positive-diff values, in corgi's structural order (== DDIR
                // `Ord` for the non-negative scalar/tuple values these reductions see; see module doc).
                // Gather all positive-diff candidates across brackets into one column, segment by
                // bracket, and one corgi `sort_blocks` gives every bracket's argmin at once
                // (`perm[block_start]`). The winning ROW is taken columnar and reuses its input value id.
                let mut cand_reps: Vec<usize> = Vec::new(); // input presentation rep index per candidate
                let mut labels: Vec<u64> = Vec::new(); // dense segment id per candidate
                let mut block_starts: Vec<usize> = Vec::new(); // per emitted bracket: start offset in cand_reps
                let mut start = 0;
                for &end in ends {
                    let lo = cand_reps.len();
                    let seg = block_starts.len() as u64;
                    for k in start..end {
                        if input[k].1 > 0 {
                            cand_reps.push(input[k].0);
                            labels.push(seg);
                        }
                    }
                    if cand_reps.len() > lo {
                        block_starts.push(lo);
                        out_diffs.push(1);
                    }
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                if cand_reps.is_empty() {
                    return (Vec::new(), out_ends);
                }
                let cand_col = gather(&self.in_vals, &cand_reps);
                let (perm, _) = sort_blocks(&labels, &cand_col);
                let min_reps: Vec<usize> = block_starts.iter().map(|&lo| cand_reps[perm[lo]]).collect();
                let col = gather(&self.in_vals, &min_reps);
                out_ids = ids(&col);
                self.register_vals(col, &out_ids);
            }
            Reducer::Collect => {
                // One row per bracket: the values sorted in corgi structural order (== DDIR `Ord` here),
                // each repeated by its diff, as a `List`. One `sort_blocks` orders every bracket's
                // entries at once; element rows are then taken columnar. Every bracket emits (empty
                // list if all diffs ≤ 0), matching the row reducer.
                let mut entry_reps: Vec<usize> = Vec::new();
                let mut entry_diffs: Vec<Diff> = Vec::new();
                let mut labels: Vec<u64> = Vec::new();
                let mut blocks: Vec<(usize, usize)> = Vec::with_capacity(ends.len());
                let mut start = 0;
                for (bi, &end) in ends.iter().enumerate() {
                    let lo = entry_reps.len();
                    for k in start..end {
                        entry_reps.push(input[k].0);
                        entry_diffs.push(input[k].1);
                        labels.push(bi as u64);
                    }
                    blocks.push((lo, entry_reps.len()));
                    out_diffs.push(1);
                    out_ends.push(out_diffs.len());
                    start = end;
                }
                let perm = if entry_reps.is_empty() {
                    Vec::new()
                } else {
                    sort_blocks(&labels, &gather(&self.in_vals, &entry_reps)).0
                };
                // Expand each bracket's sorted entries by their diff (max(0, ·) copies).
                let mut elem_reps: Vec<usize> = Vec::new();
                let mut bracket_ends: Vec<usize> = Vec::with_capacity(ends.len());
                for (lo, hi) in blocks {
                    for &e in &perm[lo..hi] {
                        for _ in 0..entry_diffs[e].max(0) {
                            elem_reps.push(entry_reps[e]);
                        }
                    }
                    bracket_ends.push(elem_reps.len());
                }
                let elems = if elem_reps.is_empty() { CValue::Unit(0) } else { gather(&self.in_vals, &elem_reps) };
                let col = CValue::List(Bounds::Offsets(bracket_ends), Box::new(elems));
                out_ids = ids(&col);
                self.register_vals(col, &out_ids);
            }
        }

        let outs = out_ids.into_iter().zip(out_diffs).collect();
        (outs, out_ends)
    }
}

impl<T> ProxyReduceBackend<CBatch<T>, CBatch<T>> for CorgiReduceBackend<T>
where
    T: ColTime + Ord,
{
    type RIn = Diff;
    type ROut = Diff;

    fn seed_times(&self, instance: &ReduceInstance<'_, CBatch<T>, CBatch<T>>) -> Vec<(u64, T)> {
        // The batch's raw (key_hash, time) support — one entry per novel record, sorted by key_hash.
        // Seeds may over-derive (a non-changing seed yields a zero delta), so this superset of
        // b.support suffices; `instance.lower` is not applied (see ReduceInstance). Reads (and warms)
        // the chunks' memoized presentations: khs are cached and already hash-sorted per chunk.
        let mut out: Vec<(u64, T)> = Vec::new();
        for ch in chunks_of(instance.input_batches) {
            let p = ch.presentation_or_init(|| build_presentation(ch));
            for i in 0..p.khs.len() {
                out.push((p.khs[i], p.times[p.tranks[i] as usize].clone()));
            }
        }
        out.sort_by_key(|(k, _)| *k);
        out
    }

    fn begin(&mut self, tiles: &[Description<T>]) {
        // Open a tiled output session for this retire; reset the per-retire resolution pools.
        self.reset_pools();
        self.tiles = tiles.to_vec();
        self.tile_rows = (0..tiles.len()).map(|_| (Vec::new(), Vec::new(), Vec::new(), Vec::new())).collect();
    }

    fn next_window(&mut self, instance: &ReduceInstance<'_, CBatch<T>, CBatch<T>>, changed: &[u64], cursor: &mut usize) -> Option<ReduceWindow<T, Diff, Diff>> {
        // Single window: present ALL remaining changed keys at once (bounded-memory windowing is a
        // later refinement). `changed` is ascending, so `binary_search` is the changed-key filter.
        if *cursor >= changed.len() {
            return None;
        }
        let keys: Vec<u64> = changed[*cursor..].to_vec();
        *cursor = changed.len();

        // Presentations are memoized per (immutable) chunk: hash + hash-order sort paid once per
        // chunk, so a retire is a MERGE of cached sorted runs restricted to the changed keys —
        // replacing the previous per-retire full scan + re-hash + owned-time comparison sort.
        let mut in_chunks = chunks_of(instance.input_batches);
        in_chunks.extend(chunks_of(instance.source_batches));
        let out_chunks = chunks_of(instance.output_batches);
        let in_pres: Vec<&Presentation<T>> = in_chunks.iter().map(|c| c.presentation_or_init(|| build_presentation(c))).collect();
        let out_pres: Vec<&Presentation<T>> = out_chunks.iter().map(|c| c.presentation_or_init(|| build_presentation(c))).collect();

        // The window's global time table: union of the chunks' (small, sorted, distinct) tables;
        // `maps` carries each chunk's local rank into the global rank space.
        let mut global: std::collections::BTreeSet<&T> = Default::default();
        for p in in_pres.iter().chain(out_pres.iter()) {
            global.extend(p.times.iter());
        }
        let sorted_times: Vec<T> = global.iter().map(|t| (*t).clone()).collect();
        let map_of = |p: &Presentation<T>| -> Vec<u32> {
            p.times.iter().map(|t| sorted_times.binary_search(t).expect("global table contains every chunk time") as u32).collect()
        };
        let in_maps: Vec<Vec<u32>> = in_pres.iter().map(|p| map_of(p)).collect();
        let out_maps: Vec<Vec<u32>> = out_pres.iter().map(|p| map_of(p)).collect();

        // Input side: merge, then gather the presented rows (pre-netting, mirroring the previous
        // per-record gather) for the id → representative-row pools.
        let in_diffs: Vec<&[Diff]> = in_chunks.iter().map(|c| c.diffs()).collect();
        let m_in = merge_presentations(&in_pres, &in_maps, &in_diffs, &keys);
        self.in_index = IdMap::default();
        let input: ProxyBridge<T, Diff> = if m_in.khs.is_empty() {
            self.in_vals = CValue::Unit(0);
            Vec::new()
        } else {
            let key_srcs: Vec<Option<&CValue>> = in_chunks.iter().map(|c| Some(c.keys())).collect();
            let val_srcs: Vec<Option<&CValue>> = in_chunks.iter().map(|c| Some(c.vals())).collect();
            let in_keys = gather_lanes(&key_srcs, &m_in.tags, &m_in.offs);
            let in_vals = gather_lanes(&val_srcs, &m_in.tags, &m_in.offs);
            for (r, &vid) in m_in.vids.iter().enumerate() { self.in_index.entry(vid).or_insert(r); }
            self.in_vals = in_vals;
            self.register_keys(in_keys, &m_in.khs);
            m_in.records.into_iter().map(|((k, v), r, d)| ((k, v), sorted_times[r as usize].clone(), d)).collect()
        };

        // Output side, same shape (also registers values for correction resolution).
        let o_diffs: Vec<&[Diff]> = out_chunks.iter().map(|c| c.diffs()).collect();
        let m_out = merge_presentations(&out_pres, &out_maps, &o_diffs, &keys);
        let output: ProxyBridge<T, Diff> = if m_out.khs.is_empty() {
            Vec::new()
        } else {
            let key_srcs: Vec<Option<&CValue>> = out_chunks.iter().map(|c| Some(c.keys())).collect();
            let val_srcs: Vec<Option<&CValue>> = out_chunks.iter().map(|c| Some(c.vals())).collect();
            let o_keys = gather_lanes(&key_srcs, &m_out.tags, &m_out.offs);
            let o_vals = gather_lanes(&val_srcs, &m_out.tags, &m_out.offs);
            self.register_keys(o_keys, &m_out.khs);
            self.register_vals(o_vals, &m_out.vids);
            m_out.records.into_iter().map(|((k, v), r, d)| ((k, v), sorted_times[r as usize].clone(), d)).collect()
        };

        Some(ReduceWindow { keys, input, output })
    }

    fn reduce_corrections(&mut self, keys: &[u64], in_ends: &[usize], input: &[(u64, Diff)], out_ends: &[usize], output: &[(u64, Diff)]) -> (Vec<(u64, Diff)>, Vec<usize>) {
        // Resolve input value_ids to `in_vals` rows, reduce (desired output), then difference the
        // desired against the presented current output per key: correction = desired − current.
        let in_rows: Vec<(usize, Diff)> = input.iter()
            .map(|&(vid, d)| (*self.in_index.get(&vid).expect("input value_id presented this window"), d))
            .collect();
        let (desired, desired_ends) = self.reduce_brackets(in_ends, &in_rows);

        let mut corr: Vec<(u64, Diff)> = Vec::new();
        let mut corr_ends: Vec<usize> = Vec::with_capacity(keys.len());
        let (mut ds, mut os) = (0usize, 0usize);
        for i in 0..keys.len() {
            let (de, oe) = (desired_ends[i], out_ends[i]);
            // Net by value_id: desired (+) minus current output (−); keep non-zero, in first-seen order.
            let mut net: HashMap<u64, Diff, BuildHasherDefault<IdHasher>> = Default::default();
            let mut order: Vec<u64> = Vec::new();
            for &(vid, d) in &desired[ds..de] {
                if let Some(x) = net.get_mut(&vid) { *x += d; } else { net.insert(vid, d); order.push(vid); }
            }
            for &(vid, d) in &output[os..oe] {
                if let Some(x) = net.get_mut(&vid) { *x -= d; } else { net.insert(vid, -d); order.push(vid); }
            }
            for vid in order {
                let d = net[&vid];
                if d != 0 { corr.push((vid, d)); }
            }
            corr_ends.push(corr.len());
            ds = de;
            os = oe;
        }
        (corr, corr_ends)
    }

    fn emit(&mut self, tile: usize, records: &[((u64, u64), T, Diff)]) {
        // Resolve each correction's key/value proxies to pool rows and accumulate into the tile.
        for rec in records {
            let ((kh, vid), t, d) = (rec.0, &rec.1, rec.2);
            let kr = *self.key_index.get(&kh).expect("key resolvable this retire");
            let vr = *self.val_index.get(&vid).expect("value resolvable this retire");
            let (krows, vrows, times, diffs) = &mut self.tile_rows[tile];
            krows.push(kr);
            vrows.push(vr);
            times.push(t.clone());
            diffs.push(d);
        }
    }

    fn finish(&mut self) -> Vec<CBatch<T>> {
        // Seal each tile: gather its accumulated (key, val) pool rows into columns, one CorgiChunk batch.
        let key_pool = concat_columns(&self.key_blocks);
        let val_pool = concat_columns(&self.val_blocks);
        let tiles = std::mem::take(&mut self.tiles);
        let tile_rows = std::mem::take(&mut self.tile_rows);
        tiles.into_iter().zip(tile_rows).map(|(desc, (krows, vrows, times, diffs))| {
            let keys = gather(&key_pool, &krows);
            let vals = gather(&val_pool, &vrows);
            Rc::new(columns_to_batch(keys, vals, times, diffs, desc))
        }).collect()
    }
}




/// Key-major concatenation of the chunks' restricted presentation ranges — the rank bridge's
/// replacement for the k-way merge. The rank tactic's replays re-sort per key and net equal
/// `(value_id, rank)` records in their buffers, so no cross-chunk merge order (and no
/// pre-netting) is needed: per changed key, each chunk's matching range is appended whole.
/// Bucketing is a counting sort over (key index → hits), so there are no per-key allocations
/// and no per-record compares — the per-record work is four array reads and the pushes.
fn concat_restricted<T>(
    pres: &[&Presentation<T>],
    maps: &[Vec<u32>],
    diffs: &[&[Diff]],
    changed: &[u64],
) -> MergedSide {
    // Pass 1: per chunk, two-pointer restriction emitting (key index, chunk, lo, hi) hits.
    let mut hits: Vec<(u32, u32, u32, u32)> = Vec::new();
    for (c, p) in pres.iter().enumerate() {
        let khs = &p.khs;
        let (mut i, mut j) = (0usize, 0usize);
        while i < khs.len() && j < changed.len() {
            if khs[i] < changed[j] {
                i += 1;
            } else if khs[i] > changed[j] {
                j += 1;
            } else {
                let s = i;
                while i < khs.len() && khs[i] == changed[j] {
                    i += 1;
                }
                hits.push((j as u32, c as u32, s as u32, i as u32));
                j += 1;
            }
        }
    }
    // Pass 2: counting sort of hits by key index (chunk order preserved within a key).
    let mut counts = vec![0u32; changed.len() + 1];
    for &(j, ..) in &hits {
        counts[j as usize + 1] += 1;
    }
    for k in 1..counts.len() {
        counts[k] += counts[k - 1];
    }
    let mut ordered: Vec<(u32, u32, u32, u32)> = vec![(0, 0, 0, 0); hits.len()];
    for &h in &hits {
        let slot = counts[h.0 as usize];
        ordered[slot as usize] = h;
        counts[h.0 as usize] += 1;
    }
    // Pass 3: key-major flat rebuild.
    let total: usize = ordered.iter().map(|&(_, _, lo, hi)| (hi - lo) as usize).sum();
    let mut out = MergedSide {
        tags: Vec::with_capacity(total),
        offs: Vec::with_capacity(total),
        khs: Vec::with_capacity(total),
        vids: Vec::with_capacity(total),
        records: Vec::with_capacity(total),
    };
    for &(_, c, lo, hi) in &ordered {
        let (lo, hi, c) = (lo as usize, hi as usize, c as usize);
        let p = pres[c];
        let map = &maps[c];
        let d = diffs[c];
        out.khs.extend_from_slice(&p.khs[lo..hi]);
        out.vids.extend_from_slice(&p.vids[lo..hi]);
        out.tags.extend(std::iter::repeat(c).take(hi - lo));
        out.offs.extend(p.perm[lo..hi].iter().map(|&x| x as usize));
        out.records.extend((lo..hi).map(|i| {
            ((p.khs[i], p.vids[i]), map[p.tranks[i] as usize], d[p.perm[i] as usize])
        }));
    }
    out
}

// ---------------------------------------------------------------------------------------------
// The RANK binding: the same backend machinery behind the rank-proxy seam. Times never
// materialize on the input path — the memoized presentations' interned tables map into the
// store (table-sized work), and merge_presentations' u32 "granks" simply ARE store ranks
// (intern order suffices: the tactic re-sorts through the store wherever order matters, and
// netting needs only equality adjacency). Output times materialize once per emitted record.
// ---------------------------------------------------------------------------------------------

use differential_dataflow::operators::rank_proxy::{RankReduceBackend, RankWindow};
use differential_dataflow::operators::recipes::{Rank, TimeStore};
use crate::lane_store::LaneStore;

type LTime = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// The source batches that are NOT this retire's novel input batches (Rc identity) — the
/// historical accumulation the two-phase window presents lazily.
fn hist_batches_of(instance: &ReduceInstance<'_, CBatch<LTime>, CBatch<LTime>>) -> Vec<CBatch<LTime>> {
    instance
        .source_batches
        .iter()
        .filter(|b| !instance.input_batches.iter().any(|ib| std::rc::Rc::ptr_eq(ib, b)))
        .cloned()
        .collect()
}

/// Per-key DISTINCT time sets over `chunks`, restricted to `keys`: the hit ranges (counting-
/// sorted by key index, chunk order preserved), the per-chunk trank→store-rank maps, and the
/// flat time sets with per-key exclusive ends. The hits/maps feed [`present_side`] later.
fn hist_time_sets(
    chunks: &[&CorgiChunk<LTime, Diff>],
    keys: &[u64],
    store: &mut LaneStore,
) -> (Vec<(u32, u32, u32, u32)>, Vec<Vec<u32>>, Vec<Rank>, Vec<usize>) {
    let pres: Vec<&Presentation<LTime>> =
        chunks.iter().map(|c| c.presentation_or_init(|| build_presentation(c))).collect();
    let maps: Vec<Vec<u32>> =
        pres.iter().map(|p| p.times.iter().map(|t| store.intern(t.clone())).collect()).collect();

    // Hits: per chunk, two-pointer restriction (the concat_restricted pass-1 shape).
    let mut hits: Vec<(u32, u32, u32, u32)> = Vec::new();
    for (c, p) in pres.iter().enumerate() {
        let khs = &p.khs;
        let (mut i, mut j) = (0usize, 0usize);
        while i < khs.len() && j < keys.len() {
            if khs[i] < keys[j] {
                i += 1;
            } else if khs[i] > keys[j] {
                j += 1;
            } else {
                let s = i;
                while i < khs.len() && khs[i] == keys[j] {
                    i += 1;
                }
                hits.push((j as u32, c as u32, s as u32, i as u32));
                j += 1;
            }
        }
    }
    // Counting sort by key index (chunk order preserved within a key).
    let mut counts = vec![0u32; keys.len() + 1];
    for &(j, ..) in &hits {
        counts[j as usize + 1] += 1;
    }
    for k in 1..counts.len() {
        counts[k] += counts[k - 1];
    }
    let mut ordered: Vec<(u32, u32, u32, u32)> = vec![(0, 0, 0, 0); hits.len()];
    for &h in &hits {
        let slot = counts[h.0 as usize];
        ordered[slot as usize] = h;
        counts[h.0 as usize] += 1;
    }

    // Distinct store ranks per key: a seen-stamp per chunk time-table entry (tables are tiny).
    let mut seen: Vec<Vec<u32>> = pres.iter().map(|p| vec![u32::MAX; p.times.len()]).collect();
    let mut times: Vec<Rank> = Vec::new();
    let mut ends: Vec<usize> = Vec::with_capacity(keys.len());
    let mut h = 0usize;
    for ki in 0..keys.len() {
        while h < ordered.len() && ordered[h].0 as usize == ki {
            let (_, c, lo, hi) = ordered[h];
            let (c, lo, hi) = (c as usize, lo as usize, hi as usize);
            let p = pres[c];
            let map = &maps[c];
            let stamp = &mut seen[c];
            for i in lo..hi {
                let t = p.tranks[i] as usize;
                if stamp[t] != ki as u32 {
                    stamp[t] = ki as u32;
                    times.push(map[t]);
                }
            }
            h += 1;
        }
        ends.push(times.len());
    }
    (ordered, maps, times, ends)
}

/// Present one historical side: rows netted under each key's meet (skip `None` keys), plus
/// the (tag, off) gather plan and (khs, vids) for pool registration — one entry per emitted
/// row, the netted group's representative. Within a hit range rows are `(vid asc, trank asc)`
/// and rank advance is monotone in time, so equal `(vid, advanced-rank)` groups are adjacent.
fn present_side(
    chunks: &[&CorgiChunk<LTime, Diff>],
    hits: &[(u32, u32, u32, u32)],
    maps: &[Vec<u32>],
    nkeys: usize,
    meets: &[Option<Rank>],
    store: &mut LaneStore,
) -> (Vec<((u64, u64), Rank, i64)>, Vec<usize>, Vec<usize>, Vec<usize>, Vec<u64>, Vec<u64>) {
    let pres: Vec<&Presentation<LTime>> =
        chunks.iter().map(|c| c.presentation_or_init(|| build_presentation(c))).collect();
    let diffs: Vec<&[Diff]> = chunks.iter().map(|c| c.diffs()).collect();
    let mut rows: Vec<((u64, u64), Rank, i64)> = Vec::new();
    let mut ends: Vec<usize> = Vec::with_capacity(nkeys);
    let (mut tags, mut offs) = (Vec::new(), Vec::new());
    let (mut khs_out, mut vids_out) = (Vec::new(), Vec::new());
    let mut h = 0usize;
    for ki in 0..nkeys {
        match meets[ki] {
            None => {
                while h < hits.len() && hits[h].0 as usize == ki {
                    h += 1;
                }
            }
            Some(meet) => {
                while h < hits.len() && hits[h].0 as usize == ki {
                    let (_, c, lo, hi) = hits[h];
                    let (c, lo, hi) = (c as usize, lo as usize, hi as usize);
                    let p = pres[c];
                    let map = &maps[c];
                    let d = diffs[c];
                    let mut memo: (u32, Rank) = (u32::MAX, 0);
                    // (vid, advanced rank, running sum, representative row)
                    let mut cur: Option<(u64, Rank, i64, usize)> = None;
                    for i in lo..hi {
                        let tr = p.tranks[i];
                        if memo.0 != tr {
                            memo = (tr, store.join(map[tr as usize], meet));
                        }
                        let adv = memo.1;
                        let vid = p.vids[i];
                        let di = d[p.perm[i] as usize];
                        match cur.as_mut() {
                            Some((cv, ca, sum, _)) if *cv == vid && *ca == adv => {
                                *sum += di;
                            }
                            _ => {
                                if let Some((cv, ca, sum, rep)) = cur.take() {
                                    if sum != 0 {
                                        rows.push(((p.khs[rep], cv), ca, sum));
                                        tags.push(c);
                                        offs.push(p.perm[rep] as usize);
                                        khs_out.push(p.khs[rep]);
                                        vids_out.push(cv);
                                    }
                                }
                                cur = Some((vid, adv, di, i));
                            }
                        }
                    }
                    if let Some((cv, ca, sum, rep)) = cur.take() {
                        if sum != 0 {
                            rows.push(((p.khs[rep], cv), ca, sum));
                            tags.push(c);
                            offs.push(p.perm[rep] as usize);
                            khs_out.push(p.khs[rep]);
                            vids_out.push(cv);
                        }
                    }
                    h += 1;
                }
            }
        }
        ends.push(rows.len());
    }
    (rows, ends, tags, offs, khs_out, vids_out)
}

impl RankReduceBackend<CBatch<LTime>, CBatch<LTime>> for CorgiReduceBackend<LTime> {
    type Store = LaneStore;

    fn seed_times(&mut self, instance: &ReduceInstance<'_, CBatch<LTime>, CBatch<LTime>>, store: &mut LaneStore) -> Vec<(u64, Rank)> {
        let mut out: Vec<(u64, Rank)> = Vec::new();
        for ch in chunks_of(instance.input_batches) {
            let p = ch.presentation_or_init(|| build_presentation(ch));
            let map: Vec<Rank> = p.times.iter().map(|t| store.intern(t.clone())).collect();
            for i in 0..p.khs.len() {
                out.push((p.khs[i], map[p.tranks[i] as usize]));
            }
        }
        out.sort_by_key(|(k, _)| *k);
        out
    }

    fn begin(&mut self, tiles: &[Description<LTime>]) {
        <Self as ProxyReduceBackend<CBatch<LTime>, CBatch<LTime>>>::begin(self, tiles)
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, CBatch<LTime>, CBatch<LTime>>,
        changed: &[u64],
        cursor: &mut usize,
        store: &mut LaneStore,
    ) -> Option<RankWindow> {
        if *cursor >= changed.len() {
            return None;
        }
        let keys: Vec<u64> = changed[*cursor..].to_vec();
        *cursor = changed.len();

        // NOVEL side, in the clear: this retire's input batches only. Rows, gathered
        // key/value columns, and vids stashed for the pool finish in `present_historical`.
        let novel_chunks = chunks_of(instance.input_batches);
        let novel_pres: Vec<&Presentation<LTime>> =
            novel_chunks.iter().map(|c| c.presentation_or_init(|| build_presentation(c))).collect();
        let novel_maps: Vec<Vec<u32>> =
            novel_pres.iter().map(|p| p.times.iter().map(|t| store.intern(t.clone())).collect()).collect();
        let novel_diffs: Vec<&[Diff]> = novel_chunks.iter().map(|c| c.diffs()).collect();
        let m_novel = concat_restricted(&novel_pres, &novel_maps, &novel_diffs, &keys);
        self.in_index = IdMap::default();
        self.rank_novel_vids.clear();
        if m_novel.khs.is_empty() {
            self.rank_novel_vals = CValue::Unit(0);
        } else {
            let key_srcs: Vec<Option<&CValue>> = novel_chunks.iter().map(|c| Some(c.keys())).collect();
            let val_srcs: Vec<Option<&CValue>> = novel_chunks.iter().map(|c| Some(c.vals())).collect();
            let nk = gather_lanes(&key_srcs, &m_novel.tags, &m_novel.offs);
            self.rank_novel_vals = gather_lanes(&val_srcs, &m_novel.tags, &m_novel.offs);
            for (r, &vid) in m_novel.vids.iter().enumerate() {
                self.in_index.entry(vid).or_insert(r);
            }
            self.register_keys(nk, &m_novel.khs);
            self.rank_novel_vids.extend_from_slice(&m_novel.vids);
        }

        // HISTORICAL side: source minus the novel batches (Rc identity), TIME SETS only —
        // the rows follow in `present_historical`, netted under the discovered meets.
        let hist_batches = hist_batches_of(instance);
        let hist_chunks = chunks_of(&hist_batches);
        let (in_hits, in_maps, hist_in_times, hist_in_ends) = hist_time_sets(&hist_chunks, &keys, store);
        let out_chunks = chunks_of(instance.output_batches);
        let (out_hits, out_maps, hist_out_times, hist_out_ends) = hist_time_sets(&out_chunks, &keys, store);
        self.rank_in_hits = in_hits;
        self.rank_in_maps = in_maps;
        self.rank_out_hits = out_hits;
        self.rank_out_maps = out_maps;

        Some(RankWindow {
            keys,
            novel: m_novel.records,
            hist_in_times,
            hist_in_ends,
            hist_out_times,
            hist_out_ends,
        })
    }

    fn present_historical(
        &mut self,
        instance: &ReduceInstance<'_, CBatch<LTime>, CBatch<LTime>>,
        window: &RankWindow,
        meets: &[Option<Rank>],
        store: &mut LaneStore,
    ) -> (Vec<((u64, u64), Rank, i64)>, Vec<usize>, Vec<((u64, u64), Rank, i64)>, Vec<usize>) {
        // Same deterministic chunk lists as `next_window`.
        let hist_batches = hist_batches_of(instance);
        let hist_chunks = chunks_of(&hist_batches);
        let out_chunks = chunks_of(instance.output_batches);

        let (in_rows, in_ends, in_tags, in_offs, in_khs, in_vids) =
            present_side(&hist_chunks, &self.rank_in_hits, &self.rank_in_maps, window.keys.len(), meets, store);
        let (out_rows, out_ends, out_tags, out_offs, out_khs, out_vids) =
            present_side(&out_chunks, &self.rank_out_hits, &self.rank_out_maps, window.keys.len(), meets, store);

        // Finish the input value pool: novel block first (its `in_index` rows were assigned
        // in `next_window`), then the netted historical block at the novel offset.
        let novel_len = self.rank_novel_vals.len();
        let hist_in_vals = if in_rows.is_empty() {
            CValue::Unit(0)
        } else {
            let val_srcs: Vec<Option<&CValue>> = hist_chunks.iter().map(|c| Some(c.vals())).collect();
            gather_lanes(&val_srcs, &in_tags, &in_offs)
        };
        self.in_vals = concat_columns(&[std::mem::replace(&mut self.rank_novel_vals, CValue::Unit(0)), hist_in_vals]);
        for (r, &vid) in in_vids.iter().enumerate() {
            self.in_index.entry(vid).or_insert(novel_len + r);
        }
        if !in_rows.is_empty() {
            let key_srcs: Vec<Option<&CValue>> = hist_chunks.iter().map(|c| Some(c.keys())).collect();
            let hist_in_keys = gather_lanes(&key_srcs, &in_tags, &in_offs);
            self.register_keys(hist_in_keys, &in_khs);
        }
        if !out_rows.is_empty() {
            let key_srcs: Vec<Option<&CValue>> = out_chunks.iter().map(|c| Some(c.keys())).collect();
            let val_srcs: Vec<Option<&CValue>> = out_chunks.iter().map(|c| Some(c.vals())).collect();
            let o_keys = gather_lanes(&key_srcs, &out_tags, &out_offs);
            let o_vals = gather_lanes(&val_srcs, &out_tags, &out_offs);
            self.register_keys(o_keys, &out_khs);
            self.register_vals(o_vals, &out_vids);
        }

        (in_rows, in_ends, out_rows, out_ends)
    }

    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, i64)],
        out_ends: &[usize],
        output: &[(u64, i64)],
    ) -> (Vec<(u64, i64)>, Vec<usize>) {
        <Self as ProxyReduceBackend<CBatch<LTime>, CBatch<LTime>>>::reduce_corrections(self, keys, in_ends, input, out_ends, output)
    }

    fn emit(&mut self, tile: usize, records: &[((u64, u64), Rank, i64)], store: &LaneStore) {
        // v1: materialize owned times once per emitted record (output-sized; a lane-native
        // ColTimes egress is the follow-on).
        let owned: Vec<((u64, u64), LTime, Diff)> =
            records.iter().map(|&((k, v), r, d)| ((k, v), store.time(r), d)).collect();
        <Self as ProxyReduceBackend<CBatch<LTime>, CBatch<LTime>>>::emit(self, tile, &owned)
    }

    fn finish(&mut self) -> Vec<CBatch<LTime>> {
        <Self as ProxyReduceBackend<CBatch<LTime>, CBatch<LTime>>>::finish(self)
    }
}
