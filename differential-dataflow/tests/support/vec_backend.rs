//! The in-memory Vec backend (the reference implementation): real data in [`VecChunk`]
//! arrangements, ids by hashing the real keys and values with the crate's [`Hashable`]
//! (a deterministic, process-wide-stable content hash — the "system-wide hash function"
//! every backend must fix for itself).
//!
//! The backend honors the tactics' delta-proportionality restrictions: a filtered
//! present *seeks* the requested keys in the batch lists rather than scanning them, so
//! per-retire (reduce) and per-unit (join) work tracks the delta, not the accumulated
//! trace — the same access pattern as the cursor tactics. The `work_is_delta_proportional`
//! tests pin this.
//!
//! It does **not** implement the collision-recovery recipe (verify-at-`cross`, partition-by-
//! real-key + key-qualified value ids) — it hashes keys and values (`value_id = hash(value)`,
//! not `hash(key, value)`) and *accepts the birthday-bound collision risk* for simplicity, as
//! the reference backend is entitled to. A production backend that needs exactness adds the
//! recipe; see the collision-risk docs in [`operators::int_proxy`](differential_dataflow::operators::int_proxy).
//!
//! [`VecChunk`]: differential_dataflow::trace::chunk::vec::VecChunk
//! [`Hashable`]: differential_dataflow::hashable::Hashable

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

use timely::progress::Timestamp;

use differential_dataflow::difference::{Abelian, Multiply, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::{Builder, Description, Navigable};
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::chunk::{ChunkBatch, ChunkBuilder};
use differential_dataflow::operators::int_proxy::{ProxyBridge, ProxyBridgeBuilder, SeedTimes, TimesView};
use differential_dataflow::trace::chunk::vec::VecChunk;

use differential_dataflow::operators::int_proxy::{JoinInstance, ProxyJoinBackend};
use differential_dataflow::operators::int_proxy::{ProxyReduceBackend, ReduceInstance, ReduceWindow};

/// The backend's stable content hash: same input → same `u64`, everywhere in the
/// process, with no registry.
pub fn stable_hash<D: Hash>(data: &D) -> u64 {
    data.hashed()
}

/// A batch of the reference backend's arrangements.
pub type RefBatch<K, V, T, R> = Rc<ChunkBatch<VecChunk<K, V, T, R>>>;

/// Proxy rows under construction, with the real record retained per `(key_hash, value_id)`.
struct Rows<K, V, T: columnar::Columnar, R> {
    rows: ProxyBridgeBuilder<T, R>,
    reals: HashMap<(u64, u64), (K, V)>,
}

impl<K, V, T: columnar::Columnar, R> Rows<K, V, T, R> {
    fn new() -> Self {
        Rows { rows: ProxyBridgeBuilder::default(), reals: HashMap::new() }
    }
}

impl<K, V, T, R> Rows<K, V, T, R>
where
    K: Ord + Clone + Hash + 'static,
    V: Ord + Clone + Hash + 'static,
    T: Lattice + Timestamp + columnar::Columnar<Container: differential_dataflow::columnar::layout::OrdContainer>,
    R: Ord + Semigroup + 'static,
{
    /// Append one key's `(val, time, diff)` records read at the cursor's current key,
    /// advancing times by `lower` (sound: every output is produced at or beyond it).
    fn read_key(
        &mut self,
        cursor: &mut <RefBatch<K, V, T, R> as Navigable>::Cursor,
        batch: &RefBatch<K, V, T, R>,
        key: &K,
        kh: u64,
        lower: timely::progress::frontier::AntichainRef<'_, T>,
    ) {
        while let Some(val) = cursor.get_val(batch) {
            let vh = stable_hash(val);
            self.reals.entry((kh, vh)).or_insert_with(|| (key.clone(), val.clone()));
            cursor.map_times(batch, |t, d| {
                let mut t = t.clone();
                t.advance_by(lower);
                self.rows.push((kh, vh), &t, d.clone());
            });
            cursor.step_val(batch);
        }
    }

    /// Read every record of `batches` (the unfiltered, delta-sized read).
    fn scan(&mut self, batches: &[RefBatch<K, V, T, R>], lower: timely::progress::frontier::AntichainRef<'_, T>) {
        for batch in batches {
            let mut cursor = batch.cursor();
            while let Some(key) = cursor.get_key(batch) {
                let key = key.clone();
                self.read_key(&mut cursor, batch, &key, stable_hash(&key), lower);
                cursor.step_key(batch);
            }
        }
    }

    /// Read exactly the records of `keys` (sorted) from `batches`, by seeking — the
    /// filtered read costs `O(|keys| · log)` per batch plus the matched records, never
    /// the batch size.
    fn seek(&mut self, batches: &[RefBatch<K, V, T, R>], keys: &[K], lower: timely::progress::frontier::AntichainRef<'_, T>) {
        debug_assert!(keys.windows(2).all(|w| w[0] < w[1]));
        for batch in batches {
            let mut cursor = batch.cursor();
            for key in keys {
                cursor.seek_key(batch, key);
                if cursor.get_key(batch) == Some(key) {
                    self.read_key(&mut cursor, batch, key, stable_hash(key), lower);
                }
            }
        }
    }

    /// Sort and consolidate into a presentation run, plus the real-record alignment.
    fn present(self) -> (ProxyBridge<T, R>, HashMap<(u64, u64), (K, V)>) {
        let (bridge, _reps) = self.rows.build();
        (bridge, self.reals)
    }
}

/// The distinct keys among `reals` whose hash appears in the sorted `filter`, sorted by
/// the *key* order (ready for seeking).
fn keys_matching<K: Ord + Clone + Hash, V>(reals: &HashMap<(u64, u64), (K, V)>, filter: &[u64]) -> Vec<K> {
    let mut keys: Vec<K> = reals
        .values()
        .filter(|(k, _)| filter.binary_search(&stable_hash(k)).is_ok())
        .map(|(k, _)| k.clone())
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// The join backend: presents [`VecChunk`] batches by hashing, applies a
/// `(key, val0, val1) → data` projection to matched `(key_hash, value_id)` pairs, and
/// emits `Vec<(data, time, diff)>` containers.
///
/// A filtered present resolves the filter's hashes to real keys through the *other*
/// side's presentation (the fresh side, presented first, unfiltered) and seeks them.
pub struct VecJoinBackend<K, V0, V1, D, L> {
    logic: L,
    /// Real records aligned with the current first-input presentation, by `(key_hash, value_id)`.
    left: HashMap<(u64, u64), (K, V0)>,
    /// Real records aligned with the current second-input presentation.
    right: HashMap<(u64, u64), (K, V1)>,
    marker: PhantomData<fn() -> D>,
}

impl<K, V0, V1, D, L> VecJoinBackend<K, V0, V1, D, L> {
    /// A backend applying `logic` to each matched `(key, val0, val1)`.
    pub fn new(logic: L) -> Self {
        VecJoinBackend { logic, left: HashMap::new(), right: HashMap::new(), marker: PhantomData }
    }
}

impl<K, V0, V1, T, R0, R1, RO, D, L> ProxyJoinBackend<RefBatch<K, V0, T, R0>, RefBatch<K, V1, T, R1>> for VecJoinBackend<K, V0, V1, D, L>
where
    K: Ord + Clone + Hash + 'static,
    V0: Ord + Clone + Hash + 'static,
    V1: Ord + Clone + Hash + 'static,
    T: Lattice + Timestamp + columnar::Columnar<Container: differential_dataflow::columnar::layout::OrdContainer>,
    R0: Ord + Semigroup + Multiply<R1, Output = RO> + 'static,
    R1: Ord + Semigroup + 'static,
    RO: Semigroup + 'static,
    D: 'static,
    L: FnMut(&K, &V0, &V1) -> D,
{
    type R0 = R0;
    type R1 = R1;
    type ROut = RO;
    type Output = Vec<(D, T, RO)>;

    fn present0(&mut self, instance: &JoinInstance<'_, RefBatch<K, V0, T, R0>, RefBatch<K, V1, T, R1>>, filter: Option<&[u64]>) -> ProxyBridge<T, R0> {
        let mut rows = Rows::new();
        match filter {
            None => rows.scan(instance.batches0, instance.lower),
            Some(f) => rows.seek(instance.batches0, &keys_matching(&self.right, f), instance.lower),
        }
        let (bridge, reals) = rows.present();
        self.left = reals;
        bridge
    }

    fn present1(&mut self, instance: &JoinInstance<'_, RefBatch<K, V0, T, R0>, RefBatch<K, V1, T, R1>>, filter: Option<&[u64]>) -> ProxyBridge<T, R1> {
        let mut rows = Rows::new();
        match filter {
            None => rows.scan(instance.batches1, instance.lower),
            Some(f) => rows.seek(instance.batches1, &keys_matching(&self.left, f), instance.lower),
        }
        let (bridge, reals) = rows.present();
        self.right = reals;
        bridge
    }

    fn cross(
        &mut self,
        _instance: &JoinInstance<'_, RefBatch<K, V0, T, R0>, RefBatch<K, V1, T, R1>>,
        left: &[(u64, u64)],
        right: &[(u64, u64)],
        times: columnar::ContainerOf<T>,
        diffs: Vec<RO>,
    ) -> Vec<(D, T, RO)> {
        use columnar::{Borrow, Index};
        let view = times.borrow();
        let mut out = Vec::with_capacity(left.len());
        for (((l, r), i), d) in left.iter().zip(right).zip(0..).zip(diffs) {
            let (k, v0) = self.left.get(l).expect("left id presented this unit");
            let (_, v1) = self.right.get(r).expect("right id presented this unit");
            out.push(((self.logic)(k, v0, v1), T::into_owned(view.get(i)), d));
        }
        out
    }
}

/// The reduce backend: presents [`VecChunk`] batches by hashing, applies the user's
/// reduction logic (row-reduce-shaped: `(key, &[(val, diff)], &mut output)`) per
/// correction bracket, mints output ids by hashing produced values, and materializes
/// emitted proxy records back into [`VecChunk`] output batches at `finish`.
///
/// The changed-key restriction is honored by *seeking*: each window's input and output
/// histories are read only at the window's keys. Every changed hash is resolvable to its
/// key — this retire's touched keys from the (delta-sized) novel batches, pending keys
/// from the retire that pended them (the `keys` map persists exactly those entries).
pub struct VecReduceBackend<K, V, V2, T, ROut, L> {
    logic: L,
    /// Keys per window; small by default so multi-window paths are exercised.
    window_size: usize,
    /// `key_hash → key` for the changed keys: primed from each retire's novel batches,
    /// pruned to the changed set on entry (so pending keys survive between retires and
    /// nothing else accumulates).
    keys: HashMap<u64, K>,
    /// `value_id → input value` for the current window's input presentation.
    in_vals: HashMap<u64, V>,
    /// `value_id → output value` for the current retire, primed by the output
    /// presentations and by minting; cleared each retire.
    out_vals: HashMap<u64, V2>,
    /// The session's output tiles: rows resolved to real data at `emit`, built at `finish`.
    tiles: Vec<(Description<T>, Vec<((K, V2), T, ROut)>)>,
}

impl<K, V, V2, T, ROut, L> VecReduceBackend<K, V, V2, T, ROut, L> {
    /// A backend applying `logic` — shaped like the row reduce's closure — per key.
    pub fn new(logic: L) -> Self {
        VecReduceBackend {
            logic,
            window_size: 16,
            keys: HashMap::new(),
            in_vals: HashMap::new(),
            out_vals: HashMap::new(),
            tiles: Vec::new(),
        }
    }
}

impl<K, V, V2, T, RIn, ROut, L> ProxyReduceBackend<RefBatch<K, V, T, RIn>, RefBatch<K, V2, T, ROut>> for VecReduceBackend<K, V, V2, T, ROut, L>
where
    K: Ord + Clone + Hash + 'static,
    V: Ord + Clone + Hash + 'static,
    V2: Ord + Clone + Hash + 'static,
    T: Lattice + Timestamp + columnar::Columnar<Container: differential_dataflow::columnar::layout::OrdContainer>,
    RIn: Ord + Semigroup + 'static,
    ROut: Ord + Abelian + 'static,
    L: FnMut(&K, &[(&V, RIn)], &mut Vec<(V2, ROut)>),
{
    type RIn = RIn;
    type ROut = ROut;

    fn seed_times(&self, instance: &ReduceInstance<'_, RefBatch<K, V, T, RIn>, RefBatch<K, V2, T, ROut>>) -> SeedTimes<T> {
        // The batch's raw (key_hash, time) support: hash keys only (no value work), one
        // entry per record, sorted by key_hash. Never merged with stored history, so no
        // compacted record can cancel a seed.
        let mut out = SeedTimes::default();
        for batch in instance.input_batches {
            let mut cursor = batch.cursor();
            while let Some(k) = cursor.get_key(batch) {
                let kh = stable_hash(k);
                while cursor.get_val(batch).is_some() {
                    cursor.map_times(batch, |t, _| out.push(kh, t));
                    cursor.step_val(batch);
                }
                cursor.step_key(batch);
            }
        }
        out.sort_by_key();
        out
    }

    fn begin(&mut self, tiles: &[Description<T>]) {
        self.tiles = tiles.iter().map(|d| (d.clone(), Vec::new())).collect();
        // The id → value map is per-retire state; start it afresh. A fresh map, not
        // `clear()`: clearing keeps the backing table, whose capacity a past big retire
        // set, and later walks would pay for it.
        self.out_vals = HashMap::new();
        self.keys.shrink_to_fit();
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, RefBatch<K, V, T, RIn>, RefBatch<K, V2, T, ROut>>,
        changed: &[u64],
        cursor: &mut usize,
    ) -> Option<ReduceWindow<T, RIn, ROut>> {
        if *cursor == 0 {
            // Prune the hash→key map to the changed set: what survives from earlier
            // retires is exactly the pending keys, so the map is bounded by the delta.
            self.keys.retain(|h, _| changed.binary_search(h).is_ok());
            // The novel batches are delta-sized and their keys are all changed: prime
            // the map from them (pending keys are already present from their retire).
            for batch in instance.input_batches {
                let mut c = batch.cursor();
                while let Some(k) = c.get_key(batch) {
                    self.keys.entry(stable_hash(k)).or_insert_with(|| k.clone());
                    c.step_key(batch);
                }
            }
        }
        if *cursor >= changed.len() {
            return None;
        }
        let end = (*cursor + self.window_size).min(changed.len());
        let keys: Vec<u64> = changed[*cursor..end].to_vec();
        *cursor = end;

        // The window's real keys, in key order, ready for seeking.
        let mut seek_keys: Vec<K> = keys.iter().filter_map(|h| self.keys.get(h).cloned()).collect();
        seek_keys.sort();
        seek_keys.dedup();

        // Input: the merged (history ∪ novel) run at the window's keys; the accumulated
        // history is never scanned, only sought.
        let mut rows = Rows::new();
        rows.seek(instance.source_batches, &seek_keys, instance.lower);
        rows.seek(instance.input_batches, &seek_keys, instance.lower);
        let (input, reals) = rows.present();
        self.in_vals = reals.into_iter().map(|((_, vh), (_, v))| (vh, v)).collect();

        // Output: the operator's own output history at the window's keys.
        let mut rows = Rows::new();
        rows.seek(instance.output_batches, &seek_keys, instance.lower);
        let (output, reals) = rows.present();
        for ((_, vh), (_, v)) in reals {
            self.out_vals.entry(vh).or_insert(v);
        }

        Some(ReduceWindow { keys, input, output })
    }

    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, RIn)],
        out_ends: &[usize],
        output: &[(u64, ROut)],
    ) -> (Vec<(u64, ROut)>, Vec<usize>) {
        let mut corr = Vec::new();
        let mut corr_ends = Vec::with_capacity(keys.len());
        let (mut in_start, mut out_start) = (0, 0);
        let mut produced = Vec::new();
        for (i, kh) in keys.iter().enumerate() {
            let in_bracket = &input[in_start..in_ends[i]];
            let out_bracket = &output[out_start..out_ends[i]];
            in_start = in_ends[i];
            out_start = out_ends[i];

            // Desired: the user logic over the accumulated input, presented in value order.
            produced.clear();
            if !in_bracket.is_empty() {
                let key = self.keys.get(kh).expect("key presented this retire");
                let mut pairs: Vec<(&V, RIn)> = in_bracket
                    .iter()
                    .map(|(vh, d)| (self.in_vals.get(vh).expect("value presented this window"), d.clone()))
                    .collect();
                pairs.sort_by(|a, b| a.0.cmp(b.0));
                (self.logic)(key, &pairs, &mut produced);
            }

            // Correction: desired − current, keyed by value id (mint ids for produced values).
            let mut delta: Vec<(u64, ROut)> = Vec::with_capacity(produced.len() + out_bracket.len());
            for (v, d) in produced.drain(..) {
                let id = stable_hash(&v);
                self.out_vals.entry(id).or_insert(v);
                delta.push((id, d));
            }
            for (vh, d) in out_bracket {
                let mut nd = d.clone();
                nd.negate();
                delta.push((*vh, nd));
            }
            differential_dataflow::consolidation::consolidate(&mut delta);
            corr.extend(delta);
            corr_ends.push(corr.len());
        }
        (corr, corr_ends)
    }

    fn emit(&mut self, tile: usize, ids: &[(u64, u64)], times: TimesView<'_, T>, diffs: &[ROut]) {
        use columnar::Index;
        // Resolve ids to real data now, while the retire's maps are live.
        let rows = &mut self.tiles[tile].1;
        for (i, (kh, vh)) in ids.iter().enumerate() {
            let k = self.keys.get(kh).expect("key presented this retire").clone();
            let v = self.out_vals.get(vh).expect("value presented or minted this retire").clone();
            rows.push(((k, v), T::into_owned(times.get(i)), diffs[i].clone()));
        }
    }

    fn finish(&mut self) -> Vec<RefBatch<K, V2, T, ROut>> {
        let mut batches = Vec::with_capacity(self.tiles.len());
        for (description, mut rows) in self.tiles.drain(..) {
            // Re-order by the *real* record order — the proxy ordering (by hash) is not
            // the arrangement's ordering.
            differential_dataflow::consolidation::consolidate_updates(&mut rows);
            // Feed the builder TARGET-sized chunks: handing it one giant chunk would make
            // its settle peel TARGET-sized pieces off the front, copying the remaining
            // tail each time — quadratic in the batch size.
            let mut builder = ChunkBuilder::<VecChunk<K, V2, T, ROut>>::with_capacity(0, 0, 0);
            let mut rows = rows.into_iter().peekable();
            while rows.peek().is_some() {
                let mut chunk = VecChunk::default();
                for row in rows.by_ref().take(<VecChunk<K, V2, T, ROut> as differential_dataflow::trace::chunk::Chunk>::TARGET) {
                    use timely::container::PushInto;
                    chunk.push_into(row);
                }
                builder.push(&mut chunk);
            }
            batches.push(builder.done(description));
        }
        batches
    }
}
