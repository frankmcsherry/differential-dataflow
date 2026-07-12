//! Operator RECIPES: the hard-won time-logic of differential's operators, packaged for reuse.
//!
//! Two tiers, split by CONTRACT LOCALITY:
//!
//! *   **Templates** (e.g. the [`int_proxy`](crate::operators::int_proxy) tactics) own the
//!     protocol-shaped contracts — sequencing whose correctness is a relation *between* calls.
//!     Most notably: interesting-time seeds must come from a novel batch's own support, never
//!     from a consolidated view (compaction can advance a history record onto a novel time and
//!     cancel it there — the seeding contract pinned by the model and its fuzz tests). Backends
//!     meet a coarse, data-blind boundary and cannot miswire the protocol.
//! *   **Helpers** (this module) are pieces whose correctness is a property of their own inputs
//!     and outputs, callable in isolation: meet-advanced replay ([`ValueHistory`]), time-only
//!     replay ([`TimeHistory`]), one key's interesting-time determination ([`discover_times`]),
//!     the bilinear time-forward wave ([`bilinear_wave`]), and output tiling
//!     ([`tile_descriptions`]). New operator shapes compose these; new templates should be
//!     written AS such compositions and differentially tested against the reference tactics.
//!
//! Everything here consumes `(id, time, diff)` streams plus lattice operations. Key and value
//! representation were made opaque by the integer-proxy design and remain so here; the time
//! type is the one representation these helpers still own, and is the intended next
//! abstraction seam (ranks into a caller-supplied store).

use std::cmp::Ordering;

use timely::progress::{Antichain, Timestamp};

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use crate::trace::Description;
use crate::operators::reduce::sort_dedup;

pub use crate::operators::ValueHistory;

/// A value history minus the values (and diffs): traverses only times, without watching for
/// cancellation of data. The reduce template determines all interesting times before any
/// collection manipulation (and potential cancellation) occurs, so a times-only replay suffices
/// there — and is materially cheaper than a value-carrying one.
pub struct TimeHistory<T> {
    /// Un-replayed `(time, meet)`, sorted descending by time so popping replays ascending;
    /// `meet` is the meet of this time with all times later in the replay.
    history: Vec<(T, T)>,
    /// Stepped-in times, advanced and deduplicated, sorted ascending.
    buffer: Vec<T>,
}

impl<T: Lattice + Clone + Ord> TimeHistory<T> {
    /// An empty history, to be `load`ed.
    pub fn new() -> Self { TimeHistory { history: Vec::new(), buffer: Vec::new() } }

    /// Load `times`, advancing each by `advance_by` if supplied, and organize the replay
    /// (sort + suffix meets).
    pub fn load(&mut self, times: impl Iterator<Item = T>, advance_by: Option<&T>) {
        self.history.clear();
        self.buffer.clear();
        for mut time in times {
            if let Some(m) = advance_by {
                time = time.join(m);
            }
            self.history.push((time.clone(), time));
        }
        self.history.sort_by(|x, y| y.0.cmp(&x.0));
        self.history.iter_mut().reduce(|prev, cur| {
            cur.1.meet_assign(&prev.1);
            cur
        });
    }

    /// The next (least) un-replayed time.
    pub fn time(&self) -> Option<&T> {
        self.history.last().map(|x| &x.0)
    }
    /// The meet of all un-replayed times.
    pub fn meet(&self) -> Option<&T> {
        self.history.last().map(|x| &x.1)
    }

    /// Step times while the next equals `time`; true iff any did.
    pub fn step_while_time_is(&mut self, time: &T) -> bool {
        let mut found = false;
        while self.time() == Some(time) {
            found = true;
            let (t, _) = self.history.pop().unwrap();
            self.buffer.push(t);
        }
        found
    }

    /// Advance buffered times by `meet` and deduplicate — the collapse that keeps replay
    /// linear.
    pub fn advance_buffer_by(&mut self, meet: &T) {
        for time in self.buffer.iter_mut() {
            *time = time.join(meet);
        }
        self.buffer.sort();
        self.buffer.dedup();
    }

    /// The buffered (stepped-in, advanced) times.
    pub fn buffer(&self) -> &[T] {
        &self.buffer
    }
}

/// The bilinear time-forward WAVE for one key: alternately step the side with the earlier
/// un-replayed edit, collapse the other side's buffer by the meet of its remaining times
/// (`advance_buffer_by`), and multiply the edit against the collapsed buffer. Work tracks the
/// NETTED accumulation sizes rather than raw history lengths — the robustification that keeps a
/// key with a long flip-flopping history (recursive iteration) from going quadratic in emitted
/// pairs against another such history.
///
/// `emit` receives every produced `(id0, id1, joined time, multiplied diff)`; callers chunk,
/// flush, or route inside it. Both histories must be pre-loaded (`load`/`load_iter`, with any
/// `advance_by` consolidation applied there) and are fully drained. For small histories a plain
/// cross product is cheaper — callers should gate (the reference tactic uses 16 rows).
pub fn bilinear_wave<V, T, R0, R1, RO>(
    h0: &mut ValueHistory<V, T, R0>,
    h1: &mut ValueHistory<V, T, R1>,
    mut emit: impl FnMut(V, V, T, RO),
) where
    V: Copy + Ord,
    T: Ord + Clone + Lattice,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    while h0.time().is_some() && h1.time().is_some() {
        if h0.time().unwrap() < h1.time().unwrap() {
            h1.advance_buffer_by(h0.meet().unwrap());
            let (v0, t0, d0) = h0.edit().unwrap();
            for ((v1, t1), d1) in h1.buffer() {
                emit(v0, *v1, t0.join(t1), d0.clone().multiply(d1));
            }
            h0.step();
        } else {
            h0.advance_buffer_by(h1.meet().unwrap());
            let (v1, t1, d1) = h1.edit().unwrap();
            for ((v0, t0), d0) in h0.buffer() {
                emit(*v0, v1, t0.join(t1), d0.clone().multiply(d1));
            }
            h1.step();
        }
    }
    while h0.time().is_some() {
        h1.advance_buffer_by(h0.meet().unwrap());
        let (v0, t0, d0) = h0.edit().unwrap();
        for ((v1, t1), d1) in h1.buffer() {
            emit(v0, *v1, t0.join(t1), d0.clone().multiply(d1));
        }
        h0.step();
    }
    while h1.time().is_some() {
        h0.advance_buffer_by(h1.meet().unwrap());
        let (v1, t1, d1) = h1.edit().unwrap();
        for ((v0, t0), d0) in h0.buffer() {
            emit(*v0, v1, t0.join(t1), d0.clone().multiply(d1));
        }
        h1.step();
    }
}

/// The output TILING for a retire: one batch description per held time whose output interval is
/// non-degenerate, walking `held` in order (each tile's upper re-inserts the later held times).
/// Returns the descriptions, the held time owning each tile, and each held index's tile
/// (`None` when degenerate).
pub fn tile_descriptions<T: Timestamp + Lattice>(
    lower: &Antichain<T>,
    upper: &Antichain<T>,
    held: &[T],
) -> (Vec<Description<T>>, Vec<T>, Vec<Option<usize>>) {
    let mut tile_descs: Vec<Description<T>> = Vec::new();
    let mut tile_held: Vec<T> = Vec::new();
    let mut tile_of: Vec<Option<usize>> = vec![None; held.len()];
    let mut out_lower = lower.clone();
    for index in 0..held.len() {
        let mut out_upper = upper.clone();
        for t in &held[index + 1..] {
            out_upper.insert(t.clone());
        }
        if out_upper != out_lower {
            tile_of[index] = Some(tile_descs.len());
            tile_descs.push(Description::new(out_lower.clone(), out_upper.clone(), Antichain::from_elem(T::minimum())));
            tile_held.push(held[index].clone());
            out_lower = out_upper;
        }
    }
    (tile_descs, tile_held, tile_of)
}

/// A one-key view into an input presentation: the read-only arguments [`discover_times`] needs
/// about a single key — its slice `[i0, i1)` of the merged `(id, time, diff)` run `p_in` and
/// the carried `pending` times.
pub struct KeyView<'a, T, RIn> {
    /// The presented `((key_hash, value_id), time, diff)` run the key's records live in.
    pub p_in: &'a [((u64, u64), T, RIn)],
    /// The key's first record.
    pub i0: usize,
    /// One past the key's last record.
    pub i1: usize,
    /// Interesting times pended for this key by earlier retires.
    pub pending: &'a [T],
}

/// Updates an optional meet by an optional time.
fn update_meet<T: Lattice + Clone>(meet: &mut Option<T>, other: Option<&T>) {
    if let Some(time) = other {
        match meet.as_mut() {
            Some(m) => m.meet_assign(time),
            None => *meet = Some(time.clone()),
        }
    }
}

/// Reusable per-key scratch for [`discover_times`], held once per retire and threaded through
/// every key so the replays and time buffers are cleared-and-refilled (`load`/`load_iter` reset
/// while keeping capacity) rather than reallocated — fresh per-key `Vec`s profiled at ~54% of a
/// hash-reduce load in `malloc`.
pub struct DiscoverScratch<T, RIn> {
    batch_replay: TimeHistory<T>,
    input_replay: ValueHistory<u64, T, RIn>,
    output_replay: TimeHistory<T>,
    synth: Vec<T>,
    times_current: Vec<T>,
    temporary: Vec<T>,
    meets: Vec<T>,
}

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone> DiscoverScratch<T, RIn> {
    /// Fresh scratch; hold one per retire and thread it through every key.
    pub fn new() -> Self {
        DiscoverScratch {
            batch_replay: TimeHistory::new(),
            input_replay: ValueHistory::new(),
            output_replay: TimeHistory::new(),
            synth: Vec::new(),
            times_current: Vec::new(),
            temporary: Vec::new(),
            meets: Vec::new(),
        }
    }
}

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone> Default for DiscoverScratch<T, RIn> {
    fn default() -> Self { Self::new() }
}

/// Phase A of a reduce retire: discover a key's interesting times in `[lower, upper)` (pending
/// those at/after `upper`) **without** accumulating input brackets. It replays the key's novel
/// (`seed_times`) and `pending` times in ascending order, marking those carrying novel/pending
/// updates interesting and synthesizing the joins thereof — joins against the input/output
/// histories and the reached times. Nothing materializes an input collection, so peak memory is
/// O(times), never O(times × values). Buffers are advanced by the meet of the times still to
/// come and consolidated, keeping a key with many distinct times linear rather than quadratic.
///
/// CONTRACT (protocol-shaped — the template's responsibility, restated here because it is the
/// known trap): `seed_times` must be the NOVEL batch's own `(time)` support for this key. Seeding
/// from a consolidated presentation is unsound: compaction may advance a history record onto a
/// novel time, where consolidation cancels the novel retraction and erases the interesting time.
#[allow(clippy::too_many_arguments)]
pub fn discover_times<T, RIn>(
    key: KeyView<'_, T, RIn>,
    seed_times: impl Iterator<Item = T>,
    out_times: impl Iterator<Item = T>,
    upper: &Antichain<T>,
    scratch: &mut DiscoverScratch<T, RIn>,
    moments: &mut Vec<T>,
    pended: &mut Vec<T>,
) where
    T: Timestamp + Lattice,
    RIn: Semigroup + Clone,
{
    // Reuse the retire's scratch: `load`/`load_iter` reset the replays (keeping capacity); the plain
    // buffers are cleared here. `meets_slice` reborrows `meets` immutably; the rest stay disjoint.
    let DiscoverScratch { batch_replay, input_replay, output_replay, synth, times_current, temporary, meets } = scratch;
    synth.clear();
    times_current.clear();
    temporary.clear();

    batch_replay.load(seed_times, None);

    meets.clear();
    meets.extend(key.pending.iter().cloned());
    for i in (1..meets.len()).rev() {
        let m = meets[i].clone();
        meets[i - 1].meet_assign(&m);
    }

    let mut meet: Option<T> = None;
    update_meet(&mut meet, meets.first());
    update_meet(&mut meet, batch_replay.meet());

    // The merged (history ⊎ novel) run — replayed for its TIMES only (join base), never
    // accumulated. Output times likewise: base joins, never seeds.
    input_replay.load_iter(
        (key.i0..key.i1).map(|i| (key.p_in[i].0.1, key.p_in[i].1.clone(), key.p_in[i].2.clone())),
        meet.as_ref(),
    );
    output_replay.load(out_times, meet.as_ref());

    let mut times_slice = key.pending;
    let mut meets_slice = &meets[..];

    while let Some(next_time) = [batch_replay.time(), times_slice.first(), input_replay.time(), output_replay.time(), synth.last()]
        .into_iter()
        .flatten()
        .min()
        .cloned()
    {
        input_replay.step_while_time_is(&next_time);
        output_replay.step_while_time_is(&next_time);
        let mut interesting = batch_replay.step_while_time_is(&next_time);
        if interesting {
            if let Some(m) = meet.as_ref() {
                batch_replay.advance_buffer_by(m);
            }
        }
        while synth.last() == Some(&next_time) {
            times_current.push(synth.pop().expect("nonempty"));
            interesting = true;
        }
        while times_slice.first() == Some(&next_time) {
            times_current.push(times_slice[0].clone());
            times_slice = &times_slice[1..];
            meets_slice = &meets_slice[1..];
            interesting = true;
        }
        interesting = interesting || batch_replay.buffer().iter().any(|t| t.less_equal(&next_time));
        interesting = interesting || times_current.iter().any(|t| t.less_equal(&next_time));

        if !upper.less_equal(&next_time) {
            if interesting {
                // Synthesize joins against the input/output histories (times only — no
                // accumulation), then record `next_time` as an interesting moment.
                if let Some(m) = meet.as_ref() {
                    input_replay.advance_buffer_by(m);
                }
                for ((_, t), _) in input_replay.buffer().iter() {
                    if !t.less_equal(&next_time) {
                        temporary.push(next_time.join(t));
                    }
                }
                if let Some(m) = meet.as_ref() {
                    output_replay.advance_buffer_by(m);
                }
                for t in output_replay.buffer().iter() {
                    if !t.less_equal(&next_time) {
                        temporary.push(next_time.join(t));
                    }
                }
                moments.push(next_time.clone());
            }
            temporary.extend(batch_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
            temporary.extend(times_current.iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
            sort_dedup(temporary);
            let synth_len = synth.len();
            for time in temporary.drain(..) {
                if upper.less_equal(&time) {
                    pended.push(time);
                } else {
                    synth.push(time);
                }
            }
            if synth.len() > synth_len {
                synth.sort_by(|x, y| y.cmp(x));
                synth.dedup();
            }
        } else if interesting {
            pended.push(next_time.clone());
        }

        meet = None;
        update_meet(&mut meet, batch_replay.meet());
        update_meet(&mut meet, input_replay.meet());
        update_meet(&mut meet, output_replay.meet());
        for t in synth.iter() {
            update_meet(&mut meet, Some(t));
        }
        update_meet(&mut meet, meets_slice.first());
        if let Some(m) = meet.as_ref() {
            for t in times_current.iter_mut() {
                *t = t.join(m);
            }
        }
        sort_dedup(times_current);
    }
    sort_dedup(pended);
}


// ---------------------------------------------------------------------------------------------
// The TIME STORE: times leave the operator seam as ranks.
// ---------------------------------------------------------------------------------------------

/// A rank names an interned time within one store (per-retire scope: build, use, `clear`).
pub type Rank = u32;

/// Interned time algebra: every tactic-side structure holds [`Rank`]s; the store owns the one
/// canonical value per distinct time and answers order/lattice questions about ranks. This is
/// the seam that lets a representation specialize (e.g. flat fixed-width lanes for
/// `Product<u64, PointStamp<u64>>`) without the operator logic knowing what a time is.
///
/// Invariants an implementation must uphold:
/// * rank equality ⟺ time equality (`intern` canonicalizes; the same time never gets two ranks
///   within one epoch of the store);
/// * `cmp` agrees with `Self::Time`'s total `Ord`, `le` with its `PartialOrder`;
/// * `join`/`meet` agree with its `Lattice`, interning their results.
pub trait TimeStore {
    /// The represented time type.
    type Time: Timestamp + Lattice;
    /// Intern an owned time, returning its rank (stable until `clear`).
    fn intern(&mut self, t: Self::Time) -> Rank;
    /// Materialize the time named by a rank (the egress edge — owned, canonical).
    fn time(&self, r: Rank) -> Self::Time;
    /// The number of distinct interned times.
    fn len(&self) -> usize;
    /// Whether no times are interned.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Total order on ranks, agreeing with `Time`'s `Ord`.
    fn cmp_ranks(&self, a: Rank, b: Rank) -> Ordering;
    /// Partial order on ranks, agreeing with `Time`'s `PartialOrder`.
    fn le(&self, a: Rank, b: Rank) -> bool;
    /// Lattice join; the result is interned.
    fn join(&mut self, a: Rank, b: Rank) -> Rank;
    /// Lattice meet; the result is interned.
    fn meet(&mut self, a: Rank, b: Rank) -> Rank;
    /// Forget everything (capacity retained) — the per-retire reset.
    fn clear(&mut self);
}

/// The reference [`TimeStore`]: an owned-`T` table with direct operations — the
/// differential-testing ORACLE every specialized store must agree with, and a perfectly
/// serviceable store for time types without a flat specialization.
pub struct OwnedStore<T> {
    table: Vec<T>,
    index: std::collections::HashMap<T, Rank>,
    join_memo: std::collections::HashMap<(Rank, Rank), Rank>,
    meet_memo: std::collections::HashMap<(Rank, Rank), Rank>,
}

impl<T> Default for OwnedStore<T> {
    fn default() -> Self {
        OwnedStore {
            table: Vec::new(),
            index: std::collections::HashMap::new(),
            join_memo: std::collections::HashMap::new(),
            meet_memo: std::collections::HashMap::new(),
        }
    }
}

impl<T> TimeStore for OwnedStore<T>
where
    T: Timestamp + Lattice + std::hash::Hash,
{
    type Time = T;

    fn intern(&mut self, t: T) -> Rank {
        if let Some(&r) = self.index.get(&t) {
            return r;
        }
        let r = self.table.len() as Rank;
        self.index.insert(t.clone(), r);
        self.table.push(t);
        r
    }
    fn time(&self, r: Rank) -> T {
        self.table[r as usize].clone()
    }
    fn len(&self) -> usize {
        self.table.len()
    }
    fn cmp_ranks(&self, a: Rank, b: Rank) -> Ordering {
        self.table[a as usize].cmp(&self.table[b as usize])
    }
    fn le(&self, a: Rank, b: Rank) -> bool {
        use timely::PartialOrder;
        self.table[a as usize].less_equal(&self.table[b as usize])
    }
    fn join(&mut self, a: Rank, b: Rank) -> Rank {
        let key = (a.min(b), a.max(b));
        if let Some(&r) = self.join_memo.get(&key) {
            return r;
        }
        let j = self.table[a as usize].join(&self.table[b as usize]);
        let r = self.intern(j);
        self.join_memo.insert(key, r);
        r
    }
    fn meet(&mut self, a: Rank, b: Rank) -> Rank {
        let key = (a.min(b), a.max(b));
        if let Some(&r) = self.meet_memo.get(&key) {
            return r;
        }
        let m = self.table[a as usize].meet(&self.table[b as usize]);
        let r = self.intern(m);
        self.meet_memo.insert(key, r);
        r
    }
    fn clear(&mut self) {
        self.table.clear();
        self.index.clear();
        self.join_memo.clear();
        self.meet_memo.clear();
    }
}

#[cfg(test)]
mod store_tests {
    use super::{OwnedStore, TimeStore};
    use crate::dynamic::pointstamp::PointStamp;
    use crate::lattice::Lattice;
    use timely::order::Product;
    use timely::PartialOrder;

    fn xs(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

    fn random_time(s: &mut u64) -> Product<u64, PointStamp<u64>> {
        let depth = (xs(s) % 4) as usize; // 0..=3 coords, incl. values that trim
        let coords: smallvec::SmallVec<[u64; 1]> = (0..depth).map(|_| xs(s) % 3).collect();
        Product::new(xs(s) % 3, PointStamp::new(coords))
    }

    /// The oracle obeys its own contract on randomized deep times: intern canonicalizes
    /// (equal times share a rank), `time` round-trips, and the rank algebra agrees with the
    /// owned algebra.
    #[test]
    fn owned_store_contract() {
        for seed in 1u64..20 {
            let mut s = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let mut store: OwnedStore<Product<u64, PointStamp<u64>>> = Default::default();
            let times: Vec<_> = (0..50).map(|_| random_time(&mut s)).collect();
            let ranks: Vec<_> = times.iter().map(|t| store.intern(t.clone())).collect();
            for (t, &r) in times.iter().zip(&ranks) {
                assert_eq!(store.time(r), *t, "round-trip");
            }
            for (i, ti) in times.iter().enumerate() {
                for (j, tj) in times.iter().enumerate() {
                    assert_eq!(ranks[i] == ranks[j], ti == tj, "rank eq iff time eq");
                    assert_eq!(store.cmp_ranks(ranks[i], ranks[j]), ti.cmp(tj), "cmp");
                    assert_eq!(store.le(ranks[i], ranks[j]), ti.less_equal(tj), "le");
                    let (ri, rj) = (ranks[i], ranks[j]);
                    let jr = store.join(ri, rj);
                    assert_eq!(store.time(jr), ti.join(tj), "join");
                    let mr = store.meet(ri, rj);
                    assert_eq!(store.time(mr), ti.meet(tj), "meet");
                }
            }
            store.clear();
            assert_eq!(store.len(), 0);
        }
    }
}
