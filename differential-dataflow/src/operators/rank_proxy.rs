//! The RANK-proxy reduce: the integer-proxy reduce template with times as [`Rank`]s into a
//! [`TimeStore`] — the second binding of the [`recipes`](crate::operators::recipes) logic
//! (the first is [`int_proxy`](crate::operators::int_proxy), which remains the reference this
//! binding is differentially tested against).
//!
//! Everything the tactic stores per record is an integer: `(key_hash: u64, value_id: u64,
//! time: Rank, diff: i64)`. Owned times survive only at the DD boundary (frontiers, tile
//! descriptions, cross-retire pending) and inside the store. Diffs are concrete `i64` in this
//! v1 (per the seam review); genericity can follow once parity is proven.

use std::collections::BTreeMap;

use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description};
use crate::operators::reduce::ReduceTactic;
use crate::operators::recipes::{
    frontier_le, rank_discover_times, tile_descriptions, Rank, RankDiscoverScratch, RankKeyView,
    RankValueHistory, TimeStore,
};
use crate::operators::int_proxy::reduce::ReduceInstance;

/// One window of changed keys, in rank space: sorted by `key_hash` and grouped by
/// `(key_hash, value_id)` with equal ranks netted (rank ORDER need not be time order — the
/// tactic re-sorts wherever replay order matters).
pub struct RankWindow {
    /// The window's key hashes, ascending.
    pub keys: Vec<u64>,
    /// Input presentation (history ∪ novel), restricted to `keys`.
    pub input: Vec<((u64, u64), Rank, i64)>,
    /// Output-history presentation, same keys.
    pub output: Vec<((u64, u64), Rank, i64)>,
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
    /// store is per-retire). Interned on entry each retire, materialized on exit.
    pending: BTreeMap<u64, Vec<T>>,
}

impl<T, Bk, S: Default> RankReduceTactic<T, Bk, S> {
    /// A tactic deferring value semantics to `backend`, times to a fresh store.
    pub fn new(backend: Bk) -> Self {
        RankReduceTactic { backend, store: S::default(), pending: BTreeMap::new() }
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
        let mut changed: std::collections::BTreeSet<u64> = seeds.iter().map(|(k, _)| *k).collect();
        changed.extend(self.pending.keys().copied());
        if changed.is_empty() {
            self.pending.clear();
            return (Vec::new(), Antichain::new());
        }
        let changed: Vec<u64> = changed.into_iter().collect();

        // Cross-retire pending: intern this retire's copy (store-sorted per key on exit).
        let pending_ranks: BTreeMap<u64, Vec<Rank>> = self
            .pending
            .iter()
            .map(|(k, ts)| (*k, ts.iter().map(|t| store.intern(t.clone())).collect()))
            .collect();

        let held_elems: Vec<B1::Time> = held.elements().to_vec();
        let (tile_descs, tile_held, tile_of) = tile_descriptions(lower, upper, &held_elems);
        // Held-time ranks, for routing corrections to tiles.
        let held_ranks: Vec<Rank> = held_elems.iter().map(|t| store.intern(t.clone())).collect();
        self.backend.begin(&tile_descs);

        let mut new_pending: BTreeMap<u64, Vec<Rank>> = BTreeMap::new();

        let mut cursor = 0usize;
        let mut ns = 0usize;

        let mut discover_scratch: RankDiscoverScratch<i64> = RankDiscoverScratch::new();
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

        while let Some(window) = self.backend.next_window(&instance, &changed, &mut cursor, store) {
            let p_in = &window.input;
            let p_out = &window.output;

            for deltas in tile_deltas.iter_mut() {
                deltas.clear();
            }

            let mut n_states = 0usize;
            let (mut is, mut os) = (0usize, 0usize);
            for &key in &window.keys {
                while is < p_in.len() && p_in[is].0.0 < key {
                    is += 1;
                }
                let i0 = is;
                while is < p_in.len() && p_in[is].0.0 == key {
                    is += 1;
                }
                let i1 = is;
                while os < p_out.len() && p_out[os].0.0 < key {
                    os += 1;
                }
                let o0 = os;
                while os < p_out.len() && p_out[os].0.0 == key {
                    os += 1;
                }
                let o1 = os;
                while ns < seeds.len() && seeds[ns].0 < key {
                    ns += 1;
                }
                let n0 = ns;
                while ns < seeds.len() && seeds[ns].0 == key {
                    ns += 1;
                }
                let n1 = ns;

                moments_scratch.clear();
                pended_scratch.clear();
                {
                    let pending = pending_ranks.get(&key).map(|p| &p[..]).unwrap_or(&[]);
                    let seed_times = seeds[n0..n1].iter().map(|(_, t)| *t);
                    let out_times = (o0..o1).map(|o| p_out[o].1);
                    rank_discover_times(
                        RankKeyView { p_in, i0, i1, pending },
                        seed_times,
                        out_times,
                        &upper_ranks,
                        &mut discover_scratch,
                        &mut moments_scratch,
                        &mut pended_scratch,
                        store,
                    );
                }
                if !pended_scratch.is_empty() {
                    new_pending.insert(key, std::mem::take(&mut pended_scratch));
                }
                if moments_scratch.is_empty() {
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
                st.moments.extend(moments_scratch.drain(..));
                st.meets.clear();
                st.meets.extend(st.moments.iter().copied());
                for i in (1..st.meets.len()).rev() {
                    let m = st.meets[i];
                    st.meets[i - 1] = store.meet(st.meets[i - 1], m);
                }
                st.in_replay.load_iter((i0..i1).map(|i| (p_in[i].0.1, p_in[i].1, p_in[i].2)), st.meets.first().copied(), store);
                st.out_replay.load_iter((o0..o1).map(|o| (p_out[o].0.1, p_out[o].1, p_out[o].2)), st.meets.first().copied(), store);
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

        // Materialize cross-retire pending and the returned frontier.
        self.pending = new_pending
            .into_iter()
            .map(|(k, rs)| (k, rs.into_iter().map(|r| store.time(r)).collect()))
            .collect();
        let produced: Vec<(B1::Time, B2)> = tile_held.into_iter().zip(self.backend.finish()).collect();
        let mut frontier = Antichain::new();
        for times in self.pending.values() {
            for t in times {
                frontier.insert_ref(t);
            }
        }
        (produced, frontier)
    }
}
