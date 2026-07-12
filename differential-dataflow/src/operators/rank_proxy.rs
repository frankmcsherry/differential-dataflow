//! The RANK-proxy reduce: the integer-proxy reduce template with times as [`Rank`]s into a
//! [`TimeStore`] — the second binding of the [`recipes`](crate::operators::recipes) logic
//! (the first is [`int_proxy`](crate::operators::int_proxy), which remains the reference this
//! binding is differentially tested against).
//!
//! Everything the tactic stores per record is an integer: `(key_hash: u64, value_id: u64,
//! time: Rank, diff: i64)`. Owned times survive only at the DD boundary (frontiers, tile
//! descriptions, cross-retire pending) and inside the store. Diffs are concrete `i64` in this
//! v1 (per the seam review); genericity can follow once parity is proven.


use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description};
use crate::operators::reduce::ReduceTactic;
use crate::operators::recipes::{
    frontier_le, rank_discover_times, tile_descriptions, Rank, RankDiscoverScratch,
    RankValueHistory, TimeStore,
};
use crate::operators::int_proxy::reduce::ReduceInstance;

/// One window of changed keys, in rank space, presented in TWO PHASES: the novel rows in
/// the clear (they seed discovery and will not compress), and the historical side as
/// per-key DISTINCT time sets only — full historical rows are presented afterwards by
/// [`RankReduceBackend::present_historical`], netted under each key's discovered meet
/// (rank ORDER need not be time order — the tactic re-sorts wherever replay order matters).
pub struct RankWindow {
    /// The window's key hashes, ascending.
    pub keys: Vec<u64>,
    /// NOVEL input rows (this retire's new batches only), in the clear, sorted by
    /// `(key_hash, value_id)`.
    pub novel: Vec<((u64, u64), Rank, i64)>,
    /// Distinct HISTORICAL input times, concatenated per key.
    pub hist_in_times: Vec<Rank>,
    /// Per-key exclusive ends into `hist_in_times`, aligned with `keys`.
    pub hist_in_ends: Vec<usize>,
    /// Distinct historical output times, concatenated per key.
    pub hist_out_times: Vec<Rank>,
    /// Per-key exclusive ends into `hist_out_times`, aligned with `keys`.
    pub hist_out_ends: Vec<usize>,
}

/// The rank-space reduce backend: value semantics + a [`TimeStore`] the tactic drives.
pub trait RankReduceBackend<B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// The time store (per-retire scope: the tactic calls `clear` at each retire's start).
    type Store: TimeStore<Time = B1::Time> + Default;

    /// The novel batches' `(key_hash, rank)` support, sorted by `key_hash`.
    fn seed_times(&mut self, instance: &ReduceInstance<'_, B1, B2>, store: &mut Self::Store) -> Vec<(u64, Rank)>;
    /// Open a tiled output session (as in the int_proxy backend).
    fn begin(&mut self, tiles: &[Description<B1::Time>]);
    /// The next window of `changed[cursor..]`, in rank space.
    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, B1, B2>,
        changed: &[u64],
        cursor: &mut usize,
        store: &mut Self::Store,
    ) -> Option<RankWindow>;
    /// Present the HISTORICAL rows for `window`'s keys: each key's times advanced by its
    /// meet (`meets[k]`; `None` skips the key — it discovered no moments and needs no rows)
    /// and equal `(value_id, advanced-rank)` rows netted, zeros dropped. Returns
    /// `(input rows, per-key ends, output rows, per-key ends)`, rows grouped per key with
    /// ends aligned to `window.keys`. Row order within a key is unconstrained (the tactic
    /// groups by value and store-sorts at load).
    fn present_historical(
        &mut self,
        instance: &ReduceInstance<'_, B1, B2>,
        window: &RankWindow,
        meets: &[Option<Rank>],
        store: &mut Self::Store,
    ) -> (Vec<((u64, u64), Rank, i64)>, Vec<usize>, Vec<((u64, u64), Rank, i64)>, Vec<usize>);
    /// Value reconciliation, identical to the int_proxy contract (no times cross here).
    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, i64)],
        out_ends: &[usize],
        output: &[(u64, i64)],
    ) -> (Vec<(u64, i64)>, Vec<usize>);
    /// Commit updates to a tile; `records` are sorted by `((key_hash, value_id), time)` (time
    /// order via the store) and consolidated. The backend materializes times via the store.
    fn emit(&mut self, tile: usize, records: &[((u64, u64), Rank, i64)], store: &Self::Store);
    /// Close the session begun by `begin`.
    fn finish(&mut self) -> Vec<B2>;
}

/// The rank-space [`ReduceTactic`].
pub struct RankReduceTactic<T, Bk, S> {
    backend: Bk,
    store: S,
    /// Pending interesting times beyond the upper frontier — OWNED (they cross retires; the
    /// store is per-retire). Interned on entry each retire, materialized on exit. Sorted by
    /// key hash, keys unique — every producer and consumer walks keys in ascending order, so
    /// a sorted vec replaces the former BTreeMap (whose inserts were ~4% of SCC profiles).
    pending: Vec<(u64, Vec<T>)>,
    /// `RANK_PRESENT_STATS=1` diagnostics: rows loaded into the replay histories (novel +
    /// meet-netted historical). Compare with the pre-two-phase `presented`/`net_meet`
    /// numbers recorded in the read-out. Reported on drop.
    stats: Option<u64>,
}

impl<T, Bk, S> Drop for RankReduceTactic<T, Bk, S> {
    fn drop(&mut self) {
        if let Some(loaded) = self.stats {
            eprintln!("RANK_PRESENT_STATS loaded={loaded}");
        }
    }
}

impl<T, Bk, S: Default> RankReduceTactic<T, Bk, S> {
    /// A tactic deferring value semantics to `backend`, times to a fresh store.
    pub fn new(backend: Bk) -> Self {
        let stats = std::env::var("RANK_PRESENT_STATS").is_ok().then_some(0);
        RankReduceTactic { backend, store: S::default(), pending: Vec::new(), stats }
    }
}

/// Per-key application state, in rank space (mirror of the int_proxy `KeyState`).
struct RankKeyState {
    key: u64,
    moments: Vec<Rank>,
    meets: Vec<Rank>,
    in_replay: RankValueHistory<u64, i64>,
    out_replay: RankValueHistory<u64, i64>,
    produced: Vec<((u64, Rank), i64)>,
    cursor: usize,
}

impl RankKeyState {
    fn empty() -> Self {
        RankKeyState {
            key: 0,
            moments: Vec::new(),
            meets: Vec::new(),
            in_replay: RankValueHistory::new(),
            out_replay: RankValueHistory::new(),
            produced: Vec::new(),
            cursor: 0,
        }
    }
}

impl<B1, B2, Bk> ReduceTactic<B1, B2> for RankReduceTactic<B1::Time, Bk, Bk::Store>
where
    B1: BatchReader<Time: Lattice + std::hash::Hash>,
    B2: BatchReader<Time = B1::Time>,
    Bk: RankReduceBackend<B1, B2>,
{
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<B1::Time>,
        upper: &Antichain<B1::Time>,
        held: &Antichain<B1::Time>,
    ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>) {
        if held.elements().iter().all(|t| upper.less_equal(t)) {
            return (Vec::new(), held.clone());
        }

        let instance = ReduceInstance {
            source_batches: &source_batches,
            input_batches: &input_batches,
            output_batches: &output_batches,
            lower: lower.borrow(),
        };

        let store = &mut self.store;
        store.clear();
        let upper_ranks: Vec<Rank> = upper.elements().iter().map(|t| store.intern(t.clone())).collect();

        let seeds = self.backend.seed_times(&instance, store);
        debug_assert!(seeds.windows(2).all(|w| w[0].0 <= w[1].0), "seed_times must be sorted by key_hash");
        // `changed` = union of seed keys and pending keys — both sorted, so a plain merge.
        let mut changed: Vec<u64> = Vec::with_capacity(seeds.len().min(64) + self.pending.len());
        {
            let (mut a, mut b) = (0usize, 0usize);
            while a < seeds.len() || b < self.pending.len() {
                let k = match (seeds.get(a), self.pending.get(b)) {
                    (Some(x), Some(y)) => x.0.min(y.0),
                    (Some(x), None) => x.0,
                    (None, Some(y)) => y.0,
                    (None, None) => unreachable!(),
                };
                while a < seeds.len() && seeds[a].0 == k {
                    a += 1;
                }
                if b < self.pending.len() && self.pending[b].0 == k {
                    b += 1;
                }
                changed.push(k);
            }
        }
        if changed.is_empty() {
            self.pending.clear();
            return (Vec::new(), Antichain::new());
        }

        // Cross-retire pending: intern this retire's copy (store-sorted per key on exit).
        // Sorted by key (inherits `pending`'s order); read by the monotone `ps` cursor below.
        let pending_ranks: Vec<(u64, Vec<Rank>)> = self
            .pending
            .iter()
            .map(|(k, ts)| (*k, ts.iter().map(|t| store.intern(t.clone())).collect()))
            .collect();

        let held_elems: Vec<B1::Time> = held.elements().to_vec();
        let (tile_descs, tile_held, tile_of) = tile_descriptions(lower, upper, &held_elems);
        // Held-time ranks, for routing corrections to tiles.
        let held_ranks: Vec<Rank> = held_elems.iter().map(|t| store.intern(t.clone())).collect();
        self.backend.begin(&tile_descs);

        // Keys are visited in ascending order across all windows, so pushes stay sorted.
        let mut new_pending: Vec<(u64, Vec<Rank>)> = Vec::new();

        let mut cursor = 0usize;
        let mut ns = 0usize;
        let mut ps = 0usize;

        let mut discover_scratch: RankDiscoverScratch = RankDiscoverScratch::new();
        let mut states: Vec<RankKeyState> = Vec::new();
        let mut tile_deltas: Vec<Vec<((u64, u64), Rank, i64)>> = (0..held_elems.len()).map(|_| Vec::new()).collect();
        let mut batch_keys: Vec<u64> = Vec::new();
        let mut in_ends: Vec<usize> = Vec::new();
        let mut in_all: Vec<(u64, i64)> = Vec::new();
        let mut out_ends: Vec<usize> = Vec::new();
        let mut out_all: Vec<(u64, i64)> = Vec::new();
        let mut active: Vec<(usize, Rank)> = Vec::new();
        let mut in_accum: Vec<(u64, i64)> = Vec::new();
        let mut cur_out: Vec<(u64, i64)> = Vec::new();
        let mut moments_scratch: Vec<Rank> = Vec::new();
        let mut pended_scratch: Vec<Rank> = Vec::new();

        let mut key_moments: Vec<Vec<Rank>> = Vec::new();
        let mut key_meets: Vec<Option<Rank>> = Vec::new();
        let mut load_scratch: Vec<(u64, Rank, i64)> = Vec::new();

        while let Some(window) = self.backend.next_window(&instance, &changed, &mut cursor, store) {
            for deltas in tile_deltas.iter_mut() {
                deltas.clear();
            }

            // PASS 1 — discovery, times only: novel rows' times + the historical distinct
            // time sets. Produces per-key moments and the meet the presentation may apply.
            key_moments.clear();
            key_meets.clear();
            let mut nv = 0usize;
            for (ki, &key) in window.keys.iter().enumerate() {
                while nv < window.novel.len() && window.novel[nv].0.0 < key {
                    nv += 1;
                }
                let v0 = nv;
                while nv < window.novel.len() && window.novel[nv].0.0 == key {
                    nv += 1;
                }
                let v1 = nv;
                let hi0 = if ki == 0 { 0 } else { window.hist_in_ends[ki - 1] };
                let hi1 = window.hist_in_ends[ki];
                let ho0 = if ki == 0 { 0 } else { window.hist_out_ends[ki - 1] };
                let ho1 = window.hist_out_ends[ki];
                while ns < seeds.len() && seeds[ns].0 < key {
                    ns += 1;
                }
                let n0 = ns;
                while ns < seeds.len() && seeds[ns].0 == key {
                    ns += 1;
                }
                let n1 = ns;
                while ps < pending_ranks.len() && pending_ranks[ps].0 < key {
                    ps += 1;
                }
                let pending: &[Rank] = match pending_ranks.get(ps) {
                    Some((k, rs)) if *k == key => &rs[..],
                    _ => &[],
                };

                moments_scratch.clear();
                pended_scratch.clear();
                rank_discover_times(
                    window.hist_in_times[hi0..hi1]
                        .iter()
                        .copied()
                        .chain(window.novel[v0..v1].iter().map(|r| r.1)),
                    pending,
                    seeds[n0..n1].iter().map(|(_, t)| *t),
                    window.hist_out_times[ho0..ho1].iter().copied(),
                    &upper_ranks,
                    &mut discover_scratch,
                    &mut moments_scratch,
                    &mut pended_scratch,
                    store,
                );
                if !pended_scratch.is_empty() {
                    debug_assert!(new_pending.last().is_none_or(|(k, _)| *k < key));
                    new_pending.push((key, std::mem::take(&mut pended_scratch)));
                }
                if moments_scratch.is_empty() {
                    key_moments.push(Vec::new());
                    key_meets.push(None);
                } else {
                    let mut m = moments_scratch[0];
                    for &t in &moments_scratch[1..] {
                        m = store.meet(m, t);
                    }
                    key_meets.push(Some(m));
                    key_moments.push(std::mem::take(&mut moments_scratch));
                }
            }

            // PASS 2 — historical rows, netted under each key's meet (backend time logic).
            let (hist_in, hist_in_ends, hist_out, hist_out_ends) =
                self.backend.present_historical(&instance, &window, &key_meets, store);

            // PASS 3 — per-key replay state: novel rows in the clear chained with the
            // netted historical rows, grouped by value for the history load.
            let mut n_states = 0usize;
            let mut nv = 0usize;
            for (ki, &key) in window.keys.iter().enumerate() {
                while nv < window.novel.len() && window.novel[nv].0.0 < key {
                    nv += 1;
                }
                let v0 = nv;
                while nv < window.novel.len() && window.novel[nv].0.0 == key {
                    nv += 1;
                }
                let v1 = nv;
                if key_moments[ki].is_empty() {
                    continue;
                }

                if n_states == states.len() {
                    states.push(RankKeyState::empty());
                }
                let st = &mut states[n_states];
                st.key = key;
                st.cursor = 0;
                st.produced.clear();
                st.moments.clear();
                st.moments.append(&mut key_moments[ki]);
                st.meets.clear();
                st.meets.extend(st.moments.iter().copied());
                for i in (1..st.meets.len()).rev() {
                    let m = st.meets[i];
                    st.meets[i - 1] = store.meet(st.meets[i - 1], m);
                }

                let hi0 = if ki == 0 { 0 } else { hist_in_ends[ki - 1] };
                let hi1 = hist_in_ends[ki];
                let ho0 = if ki == 0 { 0 } else { hist_out_ends[ki - 1] };
                let ho1 = hist_out_ends[ki];

                load_scratch.clear();
                load_scratch.extend(window.novel[v0..v1].iter().map(|r| (r.0.1, r.1, r.2)));
                load_scratch.extend(hist_in[hi0..hi1].iter().map(|r| (r.0.1, r.1, r.2)));
                // Group by value (the load's only ordering requirement; time order is the
                // history's own store-sort).
                load_scratch.sort_by_key(|r| r.0);
                if let Some(loaded) = self.stats.as_mut() {
                    *loaded += (load_scratch.len() + (ho1 - ho0)) as u64;
                }
                st.in_replay.load_iter(load_scratch.iter().copied(), st.meets.first().copied(), store);
                load_scratch.clear();
                load_scratch.extend(hist_out[ho0..ho1].iter().map(|r| (r.0.1, r.1, r.2)));
                load_scratch.sort_by_key(|r| r.0);
                st.out_replay.load_iter(load_scratch.iter().copied(), st.meets.first().copied(), store);
                n_states += 1;
            }

            loop {
                batch_keys.clear();
                in_ends.clear();
                in_all.clear();
                out_ends.clear();
                out_all.clear();
                active.clear();
                let mut advanced = false;
                for (si, st) in states[..n_states].iter_mut().enumerate() {
                    if st.cursor >= st.moments.len() {
                        continue;
                    }
                    advanced = true;
                    let j = st.cursor;
                    st.cursor += 1;
                    let t = st.moments[j];
                    st.in_replay.step_through(t, store);
                    st.out_replay.step_through(t, store);
                    st.in_replay.advance_buffer_by(st.meets[j], store);
                    st.out_replay.advance_buffer_by(st.meets[j], store);
                    for ((_, et), _) in st.produced.iter_mut() {
                        *et = store.join(*et, st.meets[j]);
                    }
                    crate::consolidation::consolidate(&mut st.produced);

                    in_accum.clear();
                    for ((vid, et), d) in st.in_replay.buffer().iter() {
                        if store.le(*et, t) {
                            in_accum.push((*vid, *d));
                        }
                    }
                    crate::consolidation::consolidate(&mut in_accum);
                    cur_out.clear();
                    for ((vid, et), d) in st.out_replay.buffer().iter().chain(st.produced.iter()) {
                        if store.le(*et, t) {
                            cur_out.push((*vid, *d));
                        }
                    }
                    crate::consolidation::consolidate(&mut cur_out);

                    if in_accum.is_empty() && cur_out.is_empty() {
                        continue;
                    }
                    batch_keys.push(st.key);
                    in_all.extend(in_accum.drain(..));
                    in_ends.push(in_all.len());
                    out_all.extend(cur_out.drain(..));
                    out_ends.push(out_all.len());
                    active.push((si, t));
                }
                if !advanced {
                    break;
                }
                if batch_keys.is_empty() {
                    continue;
                }

                let (corr, corr_ends) = self.backend.reduce_corrections(&batch_keys, &in_ends, &in_all, &out_ends, &out_all);
                let mut cstart = 0usize;
                for (bi, (si, t)) in active.iter().enumerate() {
                    let cend = corr_ends[bi];
                    if cstart != cend {
                        let idx = (0..held_ranks.len())
                            .rev()
                            .find(|&h| store.le(held_ranks[h], *t))
                            .expect("no held capability <= active time");
                        for (vid, d) in &corr[cstart..cend] {
                            states[*si].produced.push(((*vid, *t), *d));
                            tile_deltas[idx].push(((states[*si].key, *vid), *t, *d));
                        }
                    }
                    cstart = cend;
                }
            }

            for (held_index, deltas) in tile_deltas.iter_mut().enumerate() {
                if deltas.is_empty() {
                    continue;
                }
                if let Some(tile) = tile_of[held_index] {
                    // Sort by ((key_hash, value_id), time-through-the-store), net equal
                    // records, drop zeros — the emit contract, with integer/lane compares.
                    deltas.sort_by(|x, y| x.0.cmp(&y.0).then_with(|| store.cmp_ranks(x.1, y.1)));
                    let mut write = 0usize;
                    let mut read = 0usize;
                    while read < deltas.len() {
                        let mut end = read + 1;
                        while end < deltas.len() && deltas[end].0 == deltas[read].0 && deltas[end].1 == deltas[read].1 {
                            end += 1;
                        }
                        for i in read + 1..end {
                            deltas[read].2 += deltas[i].2;
                        }
                        if deltas[read].2 != 0 {
                            deltas.swap(write, read);
                            write += 1;
                        }
                        read = end;
                    }
                    deltas.truncate(write);
                    self.backend.emit(tile, &deltas[..], store);
                }
            }
        }

        // Materialize cross-retire pending and the returned frontier (stays key-sorted).
        self.pending = new_pending
            .into_iter()
            .map(|(k, rs)| (k, rs.into_iter().map(|r| store.time(r)).collect()))
            .collect();
        let produced: Vec<(B1::Time, B2)> = tile_held.into_iter().zip(self.backend.finish()).collect();
        let mut frontier = Antichain::new();
        for (_, times) in self.pending.iter() {
            for t in times {
                frontier.insert_ref(t);
            }
        }
        (produced, frontier)
    }
}
