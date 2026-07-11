//! The proxy reduce framework.
//!
//! A conventional differential reduce against `(u64, u64)`, where the backend supplies the
//! implementation of the interpretation of the integers.

use columnar::{Borrow, Clear, Columnar, ContainerOf, Index, Len, Push};

use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::Semigroup;
use crate::trace::{BatchReader, Description};
use super::ProxyTime;
use super::bridge::{ProxyBridge, SeedTimes};
use super::column::{suffix_meets_rev, TimesView, TimeVec, UpdateCol};

use crate::operators::reduce::ReduceTactic;

use super::history::{IdHistory, TimeHistory};

/// A unit of proxied reduce work, presented to the backend.
pub struct ReduceInstance<'a, B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// The accumulated input history.
    pub source_batches: &'a [B1],
    /// The freshly arrived input delta.
    pub input_batches: &'a [B1],
    /// The accumulated output history.
    pub output_batches: &'a [B2],
    /// The compaction frontier for loading (the retire's lower bound).
    pub lower: AntichainRef<'a, B1::Time>,
}

/// One window of a retire's changed keys: a bounded, hash-contiguous snip the backend sizes.
///
/// The window has the input (old and new) and output histories, restricted to the window's keys.
pub struct ReduceWindow<T: Columnar, RIn, ROut> {
    /// The window's key hashes: a contiguous, ascending slice of the retire's `changed` keys.
    pub keys: Vec<u64>,
    /// Input presentation for `keys`, sorted & consolidated by `((key_hash, value_id), time)`.
    pub input: ProxyBridge<T, RIn>,
    /// Output-history presentation for `keys`, same ordering.
    pub output: ProxyBridge<T, ROut>,
}

/// The reduce backend: value semantics for a proxy-space reduction, driven by [`ProxyReduceTactic`].
///
/// The protocol is currently (temporarily) for each round of invocation:
/// `seed_times begin [ next_window reduce_correction* emit ]* finish`
/// This should be improved to put the `seed_times` in the per-window loop, or remove it entirely.
pub trait ProxyReduceBackend<B1: BatchReader<Time: ProxyTime>, B2: BatchReader<Time = B1::Time>> {
    /// Diff type presented for the input.
    type RIn: Semigroup;
    /// Diff type of the output.
    type ROut: Semigroup + 'static;

    /// Hash keys and associated times in the instance's novel input batches.
    ///
    /// This is used (with held times) to seed the interesting times for each key.
    fn seed_times(&self, instance: &ReduceInstance<'_, B1, B2>) -> SeedTimes<B1::Time>;

    /// Initiate a session to create batches for these descriptions, which span `[lower, upper)`.
    ///
    /// It is the backend's job to prepare output batches for each of these descriptions.
    /// The computation proceeds in windows of keys, where only the backend maintains this
    /// work in progress, until `finish()` is called.
    fn begin(&mut self, tiles: &[Description<B1::Time>]);

    /// Produce the next window, resticted to `changed[cursor..]`, and update `cursor` to track.
    ///
    /// The size of the window is up to the backend, where the window should be large enough to
    /// amortize the crossings between the harness and the backend. The proxy bridges for the
    /// whole window will be active at the same time, so tighter windows reduce the required state.
    fn next_window(&mut self, instance: &ReduceInstance<'_, B1, B2>, changed: &[u64], cursor: &mut usize) -> Option<ReduceWindow<B1::Time, Self::RIn, Self::ROut>>;

    /// A wave of input-output reconciliation, in which the backend supplies necessary edits.
    ///
    /// Multiple keys are provided concurrently, for each an accumulated input and tentative output.
    /// The backend should provide for each key the necessary output updates to bring the output in
    /// with its desires. The `usize` integers upper bound the range for the corresponding key.
    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, Self::RIn)],
        out_ends: &[usize],
        output: &[(u64, Self::ROut)],
    ) -> (Vec<(u64, Self::ROut)>, Vec<usize>);

    /// Commit to a collection of updates at a specific batch in progress.
    ///
    /// The `tile: usize` indexes the list of descriptions provided to `begin()`, and these updates
    /// are aimed at that batch in progress. The columns are aligned, sorted, and consolidated by
    /// `((key_hash, value_id), time)`.
    fn emit(&mut self, tile: usize, ids: &[(u64, u64)], times: TimesView<'_, B1::Time>, diffs: &[Self::ROut]);

    /// Complete the session matching `begin`. The outputs correspond to the descriptions it was provided.
    fn finish(&mut self) -> Vec<B2>;
}

/// Outstanding interesting times at or beyond a retire's upper frontier, keyed by the
/// stable key hash: ascending keys, each with a run of ascending times in a shared
/// columnar container. Rebuilt each retire (keys are visited in ascending order, so runs
/// append), read back by a cursor advancing in lockstep with the changed keys.
struct PendingTimes<T: Columnar> {
    keys: Vec<u64>,
    ends: Vec<usize>,
    times: ContainerOf<T>,
}

impl<T: ProxyTime> PendingTimes<T> {
    fn new() -> Self {
        PendingTimes { keys: Vec::new(), ends: Vec::new(), times: Default::default() }
    }
    fn clear(&mut self) {
        self.keys.clear();
        self.ends.clear();
        self.times.clear();
    }
    fn keys(&self) -> &[u64] { &self.keys }
    fn times(&self) -> TimesView<'_, T> { self.times.borrow() }
    /// The time range of the `index`th key's run.
    fn run(&self, index: usize) -> std::ops::Range<usize> {
        let lower = if index == 0 { 0 } else { self.ends[index - 1] };
        lower..self.ends[index]
    }
    /// Append `times` as the run of `key`; keys must arrive ascending.
    fn push_run(&mut self, key: u64, times: &TimeVec<T>) {
        debug_assert!(self.keys.iter().last().is_none_or(|k| *k < key));
        let view = times.view();
        for i in 0..view.len() {
            self.times.push(view.get(i));
        }
        self.keys.push(key);
        self.ends.push(self.times.len());
    }
}

/// A proxy-space [`ReduceTactic`]: matches input and output records by `key_hash`.
pub struct ProxyReduceTactic<T: Columnar, Bk> {
    backend: Bk,
    /// Pending interesting times beyond the upper frontier, keyed by key hash.
    pending: PendingTimes<T>,
}

impl<T: ProxyTime, Bk> ProxyReduceTactic<T, Bk> {
    /// A tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyReduceTactic { backend, pending: PendingTimes::new() }
    }
}

impl<B1, B2, Bk> ReduceTactic<B1, B2> for ProxyReduceTactic<B1::Time, Bk>
where
    B1: BatchReader<Time: ProxyTime>,
    B2: BatchReader<Time = B1::Time>,
    Bk: ProxyReduceBackend<B1, B2>,
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

        let seeds = self.backend.seed_times(&instance);
        debug_assert!(seeds.keys().windows(2).all(|w| w[0] <= w[1]), "seed_times must be sorted by key_hash");
        // The changed keys: those seeded by the novel input, merged with those carrying
        // pending times (both sorted runs; a linear merge replaces the old `BTreeSet`).
        let mut changed: Vec<u64> = Vec::new();
        {
            let (skeys, pkeys) = (seeds.keys(), self.pending.keys());
            let (mut s, mut p) = (0, 0);
            while s < skeys.len() || p < pkeys.len() {
                let next = match (skeys.get(s), pkeys.get(p)) {
                    (Some(&sk), Some(&pk)) => sk.min(pk),
                    (Some(&sk), None) => sk,
                    (None, Some(&pk)) => pk,
                    (None, None) => unreachable!(),
                };
                while s < skeys.len() && skeys[s] == next { s += 1; }
                while p < pkeys.len() && pkeys[p] == next { p += 1; }
                changed.push(next);
            }
        }
        if changed.is_empty() {
            self.pending.clear();
            return (Vec::new(), Antichain::new());
        }

        // The output tiling (identical to the Abelian tactic): one tile per held time, keeping
        // non-degenerate intervals; `tile_of[i]` maps held time `i` to its tile.
        let held_elems: Vec<B1::Time> = held.elements().to_vec();
        let mut tile_descs: Vec<Description<B1::Time>> = Vec::new();
        let mut tile_held: Vec<B1::Time> = Vec::new();
        let mut tile_of: Vec<Option<usize>> = vec![None; held_elems.len()];
        {
            let mut out_lower = lower.clone();
            for index in 0..held_elems.len() {
                let mut out_upper = upper.clone();
                for t in &held_elems[index + 1..] {
                    out_upper.insert(t.clone());
                }
                if out_upper != out_lower {
                    tile_of[index] = Some(tile_descs.len());
                    tile_descs.push(Description::new(out_lower.clone(), out_upper.clone(), Antichain::from_elem(<B1::Time as Timestamp>::minimum())));
                    tile_held.push(held_elems[index].clone());
                    out_lower = out_upper;
                }
            }
        }
        self.backend.begin(&tile_descs);

        let mut new_pending: PendingTimes<B1::Time> = PendingTimes::new();
        // Cursor into the carried pending runs; `changed` contains every pending key, and
        // both ascend, so a single pass serves all windows.
        let mut pend_idx = 0usize;

        let mut cursor = 0usize;
        let mut ns = 0usize;

        // Retire-wide reusable scratch (cleared per window/round/moment, not reallocated). See the
        // profiling note on `DiscoverScratch`: fresh per-key/per-round `Vec`s were the dominant cost.
        let mut discover_scratch: DiscoverScratch<B1::Time, Bk::RIn> = DiscoverScratch::new();
        let mut states: Vec<KeyState<B1::Time, Bk::RIn, Bk::ROut>> = Vec::new();
        let mut tile_deltas: Vec<UpdateCol<(u64, u64), B1::Time, Bk::ROut>> = (0..held_elems.len()).map(|_| UpdateCol::new()).collect();
        let mut batch_keys: Vec<u64> = Vec::new();
        let mut in_ends: Vec<usize> = Vec::new();
        let mut in_all: Vec<(u64, Bk::RIn)> = Vec::new();
        let mut out_ends: Vec<usize> = Vec::new();
        let mut out_all: Vec<(u64, Bk::ROut)> = Vec::new();
        // A round's active moments, as (state index, moment index); the time itself is
        // read back from the state's moment column, never cloned out.
        let mut active: Vec<(usize, usize)> = Vec::new();
        let mut in_accum: Vec<(u64, Bk::RIn)> = Vec::new();
        let mut cur_out: Vec<(u64, Bk::ROut)> = Vec::new();
        let mut moments_scratch: TimeVec<B1::Time> = TimeVec::new();
        let mut pended_scratch: TimeVec<B1::Time> = TimeVec::new();
        // Owned working times for lattice tests over container refs.
        let mut t_cur: B1::Time = Timestamp::minimum();
        let mut m_cur: B1::Time = Timestamp::minimum();
        let mut s0: B1::Time = Timestamp::minimum();
        let mut s1: B1::Time = Timestamp::minimum();

        while let Some(window) = self.backend.next_window(&instance, &changed, &mut cursor) {
            let p_in = &window.input;
            let p_out = &window.output;
            p_in.debug_assert_sorted("next_window.input");
            p_out.debug_assert_sorted("next_window.output");

            for deltas in tile_deltas.iter_mut() { deltas.clear(); }

            // Phase 1 (determination): for every key in the window, discover its interesting times
            // (times only — no accumulation) and stand up its per-moment replays. Peak state is
            // O(window presentation), bounded by the window `next_window` already materialized.
            // `states` is a long-lived buffer reloaded slot-by-slot (not cleared/rebuilt): a slot's
            // columns and replays are allocated once and reused, so keys cost no per-key alloc/free.
            // `n_states` is the live prefix this window; higher slots persist (retaining capacity).
            let mut n_states = 0usize;
            let (mut is, mut os) = (0usize, 0usize);
            for &key in &window.keys {
                while is < p_in.len() && p_in.ids()[is].0 < key { is += 1; }
                let i0 = is;
                while is < p_in.len() && p_in.ids()[is].0 == key { is += 1; }
                let i1 = is;
                while os < p_out.len() && p_out.ids()[os].0 < key { os += 1; }
                let o0 = os;
                while os < p_out.len() && p_out.ids()[os].0 == key { os += 1; }
                let o1 = os;
                while ns < seeds.len() && seeds.keys()[ns] < key { ns += 1; }
                let n0 = ns;
                while ns < seeds.len() && seeds.keys()[ns] == key { ns += 1; }
                let n1 = ns;
                while pend_idx < self.pending.keys().len() && self.pending.keys()[pend_idx] < key { pend_idx += 1; }
                let pending_range = if pend_idx < self.pending.keys().len() && self.pending.keys()[pend_idx] == key {
                    self.pending.run(pend_idx)
                } else {
                    0..0
                };

                moments_scratch.clear();
                pended_scratch.clear();
                discover_times(
                    KeyView { p_in, i0, i1, pending: self.pending.times(), pending_range },
                    &seeds, n0..n1, p_out, o0..o1, upper,
                    &mut discover_scratch,
                    &mut moments_scratch, &mut pended_scratch,
                );
                if !pended_scratch.is_empty() {
                    new_pending.push_run(key, &pended_scratch);
                }
                if moments_scratch.is_empty() {
                    continue;
                }

                // Reload slot `n_states` in place (grow the buffer by one only when a window is wider
                // than any before): the moments copy into the slot's column, and the replays reload.
                if n_states == states.len() {
                    states.push(KeyState::empty());
                }
                let st = &mut states[n_states];
                st.key = key;
                st.cursor = 0;
                st.produced.clear();
                st.moments.clear();
                {
                    let view = moments_scratch.view();
                    for i in 0..view.len() {
                        st.moments.push(view.get(i));
                    }
                }
                suffix_meets_rev::<B1::Time>(st.moments.borrow(), 0..st.moments.len(), &mut st.meets_rev, &mut s0, &mut s1);
                // The meet of all the key's moments, to load the replays compacted.
                let first_meet = if st.moments.len() > 0 {
                    s0.copy_from(st.meets_rev.borrow().get(st.moments.len() - 1));
                    Some(&s0)
                } else {
                    None
                };
                st.in_replay.load(p_in, i0..i1, first_meet);
                st.out_replay.load(p_out, o0..o1, first_meet);
                n_states += 1;
            }

            // Phase 2 (application): walk all keys' moments in ROUNDS. Each round assembles every
            // active key's one-moment-deep input and current-output accumulations and crosses them in
            // a SINGLE `reduce_corrections` — batching across keys (a key's own moments stay
            // sequential, each seeing its earlier corrections via `produced`). This caps the backend
            // call count at O(max moments over keys), not O(sum of moments), with peak materialization
            // one moment deep per key. `produced` is meet-collapsed each round, exactly like the
            // reference — bounded, not the O(times × values) delta history.
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
                    t_cur.copy_from(st.moments.borrow().get(j));
                    m_cur.copy_from(st.meets_rev.borrow().get(st.moments.len() - 1 - j));
                    st.in_replay.step_through(&t_cur);
                    st.out_replay.step_through(&t_cur);
                    st.in_replay.advance_buffer_by(&m_cur);
                    st.out_replay.advance_buffer_by(&m_cur);
                    st.produced.advance_by(&m_cur, &mut s0);
                    st.produced.consolidate();

                    in_accum.clear();
                    accumulate_le(st.in_replay.buffer(), &t_cur, &mut in_accum, &mut s0);
                    crate::consolidation::consolidate(&mut in_accum);
                    cur_out.clear();
                    accumulate_le(st.out_replay.buffer(), &t_cur, &mut cur_out, &mut s0);
                    accumulate_le(&st.produced, &t_cur, &mut cur_out, &mut s0);
                    crate::consolidation::consolidate(&mut cur_out);

                    if in_accum.is_empty() && cur_out.is_empty() {
                        continue;
                    }
                    batch_keys.push(st.key);
                    Extend::extend(&mut in_all, in_accum.drain(..));
                    in_ends.push(in_all.len());
                    Extend::extend(&mut out_all, cur_out.drain(..));
                    out_ends.push(out_all.len());
                    active.push((si, j));
                }
                // Terminate only when every key is EXHAUSTED — not merely when this round produced no
                // crossing. A round can be empty because every key's current moment is empty-gated
                // while keys still have later (non-empty) moments; breaking here would drop them.
                if !advanced {
                    break;
                }
                if batch_keys.is_empty() {
                    continue;
                }

                let (corr, corr_ends) = self.backend.reduce_corrections(&batch_keys, &in_ends, &in_all, &out_ends, &out_all);
                let mut cstart = 0usize;
                for (bi, (si, j)) in active.iter().enumerate() {
                    let cend = corr_ends[bi];
                    if cstart != cend {
                        let KeyState { key, moments, produced, .. } = &mut states[*si];
                        let t = moments.borrow().get(*j);
                        s0.copy_from(t);
                        let idx = held_elems.iter().rposition(|h| h.less_equal(&s0)).expect("no held capability <= active time");
                        for (vid, d) in &corr[cstart..cend] {
                            produced.push_ref(*vid, t, d.clone());
                            tile_deltas[idx].push_ref((*key, *vid), t, d.clone());
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
                    deltas.consolidate();
                    self.backend.emit(tile, deltas.ids(), deltas.times(), deltas.diffs());
                }
            }
        }

        self.pending = new_pending;
        let produced: Vec<(B1::Time, B2)> = tile_held.into_iter().zip(self.backend.finish()).collect();
        let mut frontier = Antichain::new();
        {
            let view = self.pending.times();
            for i in 0..view.len() {
                s0.copy_from(view.get(i));
                if !frontier.less_equal(&s0) {
                    frontier.insert(s0.clone());
                }
            }
        }
        (produced, frontier)
    }
}

/// Per-key application state for [`ProxyReduceTactic`]'s round-batched walk: the key's ordered
/// interesting `moments` and their (reversed) suffix meets, its input and output replays
/// (meet-collapsed), the corrections `produced` this round so far, and a `cursor` into `moments`.
/// Held for all of a window's keys at once so each round's crossing batches across keys — a key's
/// own moments stay sequential (each sees its earlier corrections via `produced`), but distinct
/// keys are independent.
struct KeyState<T: Columnar, RIn, ROut> {
    key: u64,
    moments: ContainerOf<T>,
    /// Reversed suffix meets of `moments`: the meet of `moments[j..]` is
    /// `meets_rev[len - 1 - j]`.
    meets_rev: ContainerOf<T>,
    in_replay: IdHistory<T, RIn>,
    out_replay: IdHistory<T, ROut>,
    produced: UpdateCol<u64, T, ROut>,
    cursor: usize,
}

impl<T: ProxyTime, RIn: Semigroup, ROut: Semigroup> KeyState<T, RIn, ROut> {
    /// An empty slot, to be filled by [`ProxyReduceTactic`]'s phase 1 (`reload`-style). The `states`
    /// vector holds these across windows and reloads them in place, so a key's buffers are allocated
    /// once (per slot) and reused — never dropped per key (which was ~18% of load in `free`).
    fn empty() -> Self {
        KeyState {
            key: 0,
            moments: Default::default(),
            meets_rev: Default::default(),
            in_replay: IdHistory::new(),
            out_replay: IdHistory::new(),
            produced: UpdateCol::new(),
            cursor: 0,
        }
    }
}

/// Reusable per-key scratch for [`discover_times`], held once per retire and threaded through every
/// key so the replays and time columns are cleared-and-refilled (retaining capacity) rather than
/// reallocated. Mirrors the reference `HistoryReplayer`'s field-held scratch; without it each of ~n
/// keys paid ~7 fresh allocations per call — profiled at ~54% of the hash-reduce load in `malloc`,
/// against the cursor reduce's ~10%.
struct DiscoverScratch<T: Columnar, RIn> {
    batch_replay: TimeHistory<T>,
    input_replay: IdHistory<T, RIn>,
    output_replay: TimeHistory<T>,
    /// Synthesized interesting times, ascending, consumed from `synth_cursor`.
    synth: TimeVec<T>,
    synth_cursor: usize,
    times_current: TimeVec<T>,
    temporary: TimeVec<T>,
    /// Reversed suffix meets of the key's pending run.
    pending_meets_rev: ContainerOf<T>,
    // Owned working times.
    next_time: T,
    /// The running meet of every remaining source of times; `meet_valid` is its presence
    /// (an inline `Option<T>` that keeps the time's allocations across iterations).
    meet: T,
    meet_valid: bool,
    s0: T,
    s1: T,
}

impl<T: ProxyTime, RIn: Semigroup + Clone> DiscoverScratch<T, RIn> {
    fn new() -> Self {
        DiscoverScratch {
            batch_replay: TimeHistory::new(),
            input_replay: IdHistory::new(),
            output_replay: TimeHistory::new(),
            synth: TimeVec::new(),
            synth_cursor: 0,
            times_current: TimeVec::new(),
            temporary: TimeVec::new(),
            pending_meets_rev: Default::default(),
            next_time: Timestamp::minimum(),
            meet: Timestamp::minimum(),
            meet_valid: false,
            s0: Timestamp::minimum(),
            s1: Timestamp::minimum(),
        }
    }
}

/// A one-key view into the input presentation: the read-only arguments [`discover_times`] needs
/// about a single key — its slice `[i0, i1)` of the merged input run `p_in` and the carried
/// `pending` times (a view of the shared pending column, with the key's run range).
struct KeyView<'a, T: Columnar, RIn> {
    p_in: &'a ProxyBridge<T, RIn>,
    i0: usize,
    i1: usize,
    pending: TimesView<'a, T>,
    pending_range: std::ops::Range<usize>,
}

/// Phase A: discover a key's interesting times in `[lower, upper)` (pending those at/after `upper`)
/// **without** accumulating input brackets. It mirrors the reference's DETERMINATION step (times
/// only, ref `reduce.rs` :671–713): it replays the key's novel (`seed_times`) and `pending` times
/// in ascending order, marking those carrying novel/pending updates interesting and synthesizing the
/// joins thereof — joins against the input/output histories and the reached times. Nothing
/// materializes an input collection, so peak memory is O(times), never O(times × values); the
/// application walk re-assembles each moment's accumulation on the fly, one moment deep — the whole
/// point of the tactic. Buffers are advanced by the meet of the times still to come and
/// consolidated, keeping a key with many distinct times linear rather than quadratic.
///
/// All time sets live in columnar containers; the loop's owned working times (`next_time`, the
/// running `meet`, and the `s0`/`s1` scratches) are the only owned timestamps, reused across every
/// iteration and every key.
#[allow(clippy::too_many_arguments)]
fn discover_times<T, RIn>(
    key: KeyView<'_, T, RIn>,
    seeds: &SeedTimes<T>,
    seed_range: std::ops::Range<usize>,
    p_out: &ProxyBridge<T, impl Semigroup>,
    out_range: std::ops::Range<usize>,
    upper: &Antichain<T>,
    scratch: &mut DiscoverScratch<T, RIn>,
    moments: &mut TimeVec<T>,
    pended: &mut TimeVec<T>,
) where
    T: ProxyTime,
    RIn: Semigroup + Clone,
{
    // Reuse the retire's scratch: loads reset the replays (keeping capacity); the plain
    // columns are cleared here.
    let DiscoverScratch {
        batch_replay, input_replay, output_replay,
        synth, synth_cursor, times_current, temporary, pending_meets_rev,
        next_time, meet, meet_valid, s0, s1,
    } = scratch;
    synth.clear();
    *synth_cursor = 0;
    times_current.clear();
    temporary.clear();

    let seeds_view = seeds.times();
    batch_replay.load(seed_range.map(|i| seeds_view.get(i)), None);

    // Suffix meets of the carried pending times (reversed, as everywhere).
    suffix_meets_rev::<T>(key.pending, key.pending_range.clone(), pending_meets_rev, s0, s1);
    let pending_len = key.pending_range.len();
    // Cursor into the key's pending run: `pending_range.start + pcur` is the next time.
    let mut pcur = 0usize;

    *meet_valid = false;
    if pending_len > 0 {
        meet.copy_from(pending_meets_rev.borrow().get(pending_len - 1));
        *meet_valid = true;
    }
    if let Some(m) = batch_replay.meet() {
        s0.copy_from(m);
        if *meet_valid { meet.meet_assign(&*s0); } else { meet.clone_from(&*s0); *meet_valid = true; }
    }

    // The merged (history ⊎ novel) run — replayed for its TIMES only (join base), never
    // accumulated. Output times likewise: base joins, never seeds.
    let advance_by = if *meet_valid { Some(&*meet) } else { None };
    input_replay.load(key.p_in, key.i0..key.i1, advance_by);
    let out_view = p_out.times();
    output_replay.load(out_range.map(|o| out_view.get(o)), advance_by);

    loop {
        // The next time: the least of every source's next. All candidates are container
        // refs; reborrow to a common lifetime, take the min, and copy it out before any
        // source is stepped.
        let has_next = {
            let candidates = [
                batch_replay.time().map(T::reborrow),
                (pcur < pending_len).then(|| T::reborrow(key.pending.get(key.pending_range.start + pcur))),
                input_replay.time().map(T::reborrow),
                output_replay.time().map(T::reborrow),
                (*synth_cursor < synth.len()).then(|| T::reborrow(synth.get(*synth_cursor))),
            ];
            if let Some(min) = candidates.into_iter().flatten().min() {
                next_time.copy_from(min);
                true
            } else {
                false
            }
        };
        if !has_next {
            break;
        }

        input_replay.step_while_time_is(next_time);
        output_replay.step_while_time_is(next_time);
        let mut interesting = batch_replay.step_while_time_is(next_time);
        if interesting && *meet_valid {
            batch_replay.advance_buffer_by(meet);
        }
        while *synth_cursor < synth.len() && {
            s0.copy_from(synth.get(*synth_cursor));
            *s0 == *next_time
        } {
            times_current.push_ref(synth.get(*synth_cursor));
            *synth_cursor += 1;
            interesting = true;
        }
        while pcur < pending_len && {
            s0.copy_from(key.pending.get(key.pending_range.start + pcur));
            *s0 == *next_time
        } {
            times_current.push_ref(key.pending.get(key.pending_range.start + pcur));
            pcur += 1;
            interesting = true;
        }
        interesting = interesting || any_le(batch_replay.buffer().view(), next_time, s0);
        interesting = interesting || any_le(times_current.view(), next_time, s0);

        if !upper.less_equal(next_time) {
            if interesting {
                // Synthesize joins against the input/output histories (times only — no
                // accumulation), then record `next_time` as an interesting moment.
                if *meet_valid {
                    input_replay.advance_buffer_by(meet);
                }
                join_beyond_into(input_replay.buffer().times(), next_time, temporary, s0);
                if *meet_valid {
                    output_replay.advance_buffer_by(meet);
                }
                join_beyond_into(output_replay.buffer().view(), next_time, temporary, s0);
                moments.push_own(next_time);
            }
            join_beyond_into(batch_replay.buffer().view(), next_time, temporary, s0);
            join_beyond_into(times_current.view(), next_time, temporary, s0);
            temporary.sort_dedup();
            let mut synthesized = false;
            {
                let view = temporary.view();
                for i in 0..view.len() {
                    s0.copy_from(view.get(i));
                    if upper.less_equal(&*s0) {
                        pended.push_own(s0);
                    } else {
                        synth.push_own(s0);
                        synthesized = true;
                    }
                }
            }
            temporary.clear();
            if synthesized {
                synth.sort_dedup_from(synth_cursor);
            }
        } else if interesting {
            pended.push_own(next_time);
        }

        // Track the meet of every remaining source of times, and keep `times_current`
        // advanced by it (the same collapse as the buffers).
        *meet_valid = false;
        for candidate in [batch_replay.meet(), input_replay.meet(), output_replay.meet()] {
            if let Some(m) = candidate {
                s0.copy_from(m);
                if *meet_valid { meet.meet_assign(&*s0); } else { meet.clone_from(&*s0); *meet_valid = true; }
            }
        }
        for i in *synth_cursor..synth.len() {
            s0.copy_from(synth.get(i));
            if *meet_valid { meet.meet_assign(&*s0); } else { meet.clone_from(&*s0); *meet_valid = true; }
        }
        if pcur < pending_len {
            s0.copy_from(pending_meets_rev.borrow().get(pending_len - 1 - pcur));
            if *meet_valid { meet.meet_assign(&*s0); } else { meet.clone_from(&*s0); *meet_valid = true; }
        }
        if *meet_valid {
            times_current.advance_by(meet, s0);
        } else {
            times_current.sort_dedup();
        }
    }
    pended.sort_dedup();
}

/// Append `(id, diff)` for every update in `updates` whose time is `less_equal` the
/// probe — the one-moment-deep accumulation read off a replay buffer.
fn accumulate_le<T: ProxyTime, R: Semigroup + Clone>(
    updates: &UpdateCol<u64, T, R>,
    probe: &T,
    out: &mut Vec<(u64, R)>,
    scratch: &mut T,
) {
    let (ids, times, diffs) = (updates.ids(), updates.times(), updates.diffs());
    for idx in 0..ids.len() {
        scratch.copy_from(times.get(idx));
        if scratch.less_equal(probe) {
            out.push((ids[idx], diffs[idx].clone()));
        }
    }
}

/// True iff any time in `times` is `less_equal` the probe (copying each into `scratch`
/// for the partially-ordered test).
fn any_le<T: ProxyTime>(times: TimesView<'_, T>, probe: &T, scratch: &mut T) -> bool {
    (0..times.len()).any(|i| {
        scratch.copy_from(times.get(i));
        scratch.less_equal(probe)
    })
}

/// For every time in `times` NOT `less_equal` the probe, push its join with the probe
/// into `out` — the synthetic-join step against a replayed history.
fn join_beyond_into<T: ProxyTime>(times: TimesView<'_, T>, probe: &T, out: &mut TimeVec<T>, scratch: &mut T) {
    for i in 0..times.len() {
        scratch.copy_from(times.get(i));
        if !scratch.less_equal(probe) {
            scratch.join_assign(probe);
            out.push_own(scratch);
        }
    }
}
