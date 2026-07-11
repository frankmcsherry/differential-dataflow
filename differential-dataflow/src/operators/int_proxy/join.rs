//! The proxy join framework.
//!
//! A conventional differential join against `(u64, u64)` values, which are provided by
//! and then interpreted by a backend, who is relieved of lattice-time reasoning.

use columnar::{ContainerOf, Index};

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::{Multiply, Semigroup};
use crate::trace::BatchReader;
use super::ProxyTime;
use super::bridge::ProxyBridge;
use crate::operators::join::{Fresh, JoinTactic};

use super::column::push_owned;
use super::history::IdHistory;

/// A unit of proxied join work, presented to the backend.
pub struct JoinInstance<'a, B0: BatchReader, B1: BatchReader<Time = B0::Time>> {
    /// The first input's batches.
    pub batches0: &'a [B0],
    /// The second input's batches.
    pub batches1: &'a [B1],
    /// The compaction frontier for loading (the unit's capability time).
    pub lower: AntichainRef<'a, B0::Time>,
}

/// A type that can interpret and retire pairs of batches, joined by key hashes.
///
/// The protocol invokes the `present*` methods with the instance to produce the proxy collection,
/// and then returns with any number (including zero) calls to `cross` to produce outputs.
pub trait ProxyJoinBackend<B0: BatchReader<Time: ProxyTime>, B1: BatchReader<Time = B0::Time>> {
    /// Diff type presented for the first input.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Diff type presented for the second input.
    type R1: Semigroup;
    /// Diff type of matched records (`R0 * R1`), computed by the tactic.
    type ROut: Semigroup;
    /// The output container built from matched value ids.
    type Output;

    /// Prepare a proxy bridge from the instance's first input, optionally key-hash restricted.
    ///
    /// The returned bridge **must be sorted and consolidated** by `((key_hash, value_id), time)`.
    fn present0(&mut self, instance: &JoinInstance<'_, B0, B1>, filter: Option<&[u64]>) -> ProxyBridge<B0::Time, Self::R0>;
    /// Prepare a proxy bridge from the instance's second input, optionally key-hash restricted.
    ///
    /// The returned bridge **must be sorted and consolidated** by `((key_hash, value_id), time)`.
    fn present1(&mut self, instance: &JoinInstance<'_, B0, B1>, filter: Option<&[u64]>) -> ProxyBridge<B0::Time, Self::R1>;
    /// From a list of left and right identifiers, and corresponding times and diffs, the output.
    ///
    /// The times arrive as a columnar container aligned with the id lists.
    fn cross(&mut self, instance: &JoinInstance<'_, B0, B1>, left: &[(u64, u64)], right: &[(u64, u64)], times: ContainerOf<B0::Time>, diffs: Vec<Self::ROut>) -> Self::Output;
}

/// A proxy-space [`JoinTactic`]: matches records of the two presented runs by `key_hash`,
pub struct ProxyJoinTactic<B0, B1, Bk> {
    backend: Bk,
    _marker: std::marker::PhantomData<(B0, B1)>,
}

impl<B0, B1, Bk> ProxyJoinTactic<B0, B1, Bk> {
    /// A join tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyJoinTactic { backend, _marker: std::marker::PhantomData }
    }
}

impl<B0, B1, Bk> JoinTactic<B0, B1, Bk::Output> for ProxyJoinTactic<B0, B1, Bk>
where
    B0: BatchReader<Time: ProxyTime>,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
    Bk::Output: 'static,
{
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: B0::Time) -> Box<dyn Iterator<Item = Bk::Output>> {
        Box::new(join_prep(&mut self.backend, input0, input1, fresh, meet).into_iter())
    }
}

/// Update count threshold used to return to the backend with join output to instantiate.
const JOIN_CHUNK: usize = 1 << 20;

/// Prepare one join unit: present both sides, merge-match key runs over the sorted hashes, and
/// cross-product matched records into a sequence of output containers — one per ~[`JOIN_CHUNK`]
/// matches, flushed wherever the batch fills (including mid-key; parity: their concatenation is the
/// single-container output). `meet` is a lower bound on the fresh side's times, so the accumulated
/// side loads compacted (see [`JoinInstance`]); every output is produced under the capability at
/// `meet`, so advancing loaded times by it leaves the output unchanged.
fn join_prep<B0, B1, Bk>(backend: &mut Bk, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: B0::Time) -> Vec<Bk::Output>
where
    B0: BatchReader<Time: ProxyTime>,
    B1: BatchReader<Time = B0::Time>,
    Bk: ProxyJoinBackend<B0, B1>,
{
    let lower = Antichain::from_elem(meet);
    let instance = JoinInstance { batches0: &input0, batches1: &input1, lower: lower.borrow() };

    // Prepare work using the keys of the just-received side.
    // FIXME: Use the smaller of the two instead.
    let (p0, p1) = match fresh {
        Fresh::Input0 => {
            let p0 = backend.present0(&instance, None);
            if p0.is_empty() { return Vec::new(); }
            let mut keys: Vec<u64> = p0.ids().iter().map(|id| id.0).collect();
            keys.dedup();
            let p1 = backend.present1(&instance, Some(&keys));
            (p0, p1)
        }
        Fresh::Input1 => {
            let p1 = backend.present1(&instance, None);
            if p1.is_empty() { return Vec::new(); }
            let mut keys: Vec<u64> = p1.ids().iter().map(|id| id.0).collect();
            keys.dedup();
            let p0 = backend.present0(&instance, Some(&keys));
            (p0, p1)
        }
    };
    if p0.is_empty() || p1.is_empty() { return Vec::new(); }
    p0.debug_assert_sorted("present0");
    p1.debug_assert_sorted("present1");

    // Merge-join the two presented runs on `key_hash` (both sorted) and cross matched pairs. `flush`
    // ships the accumulated batch as a container and resets the buffers; `join_key` calls it at each
    // `JOIN_CHUNK` boundary — including *within* a high-fanout key's wave — so no single key
    // materializes more than a chunk of its cross product. The closure scope releases its
    // `backend`/`out` borrow before `out` is returned.
    let mut out: Vec<Bk::Output> = Vec::new();
    {
        let mut flush = |li: &mut Vec<(u64, u64)>, ri: &mut Vec<(u64, u64)>, ot: &mut ContainerOf<B0::Time>, od: &mut Vec<Bk::ROut>| {
            out.push(backend.cross(&instance, li.as_slice(), ri.as_slice(), std::mem::take(ot), std::mem::take(od)));
            li.clear();
            ri.clear();
        };

        let (mut li, mut ri) = (Vec::new(), Vec::new());
        let mut ot: ContainerOf<B0::Time> = Default::default();
        let mut od = Vec::new();
        // Per-key replay histories, held across the unit and reloaded per key (only the >=16/>=16
        // replay-wave path touches them), so a high-fanout join pays no per-key `IdHistory` alloc.
        let mut h0 = IdHistory::new();
        let mut h1 = IdHistory::new();
        // Owned working times for lattice joins over container refs, likewise held across the unit.
        let mut s0 = <B0::Time as Timestamp>::minimum();
        let mut s1 = <B0::Time as Timestamp>::minimum();
        let (mut i, mut j) = (0usize, 0usize);
        while i < p0.len() && j < p1.len() {
            let (ki, kj) = (p0.ids()[i].0, p1.ids()[j].0);
            if ki < kj {
                i += 1;
            } else if kj < ki {
                j += 1;
            } else {
                let mut e0 = i;
                while e0 < p0.len() && p0.ids()[e0].0 == ki { e0 += 1; }
                let mut e1 = j;
                while e1 < p1.len() && p1.ids()[e1].0 == ki { e1 += 1; }
                join_key(ki, &p0, i..e0, &p1, j..e1, &mut h0, &mut h1, &mut s0, &mut s1, &mut li, &mut ri, &mut ot, &mut od, &mut flush);
                i = e0;
                j = e1;
            }
        }
        if !li.is_empty() {
            flush(&mut li, &mut ri, &mut ot, &mut od);
        }
    }
    out
}

/// Match one key's records across the two presented runs.
///
/// If either history is small, this performs a simple cross product.
/// If both histories are large, this replays the histories compacting as it goes in
/// order to (potentially) avoid quadratic blow-up.
///
/// `s0`/`s1` are the caller's owned working times: an edit's time is copied into one
/// once per wave, and each produced pair joins into the other before being pushed —
/// container refs in, contiguous times out, no per-pair allocation.
#[allow(clippy::too_many_arguments)]
fn join_key<T, R0, R1, RO, F>(
    kh: u64,
    p0: &ProxyBridge<T, R0>,
    r0: std::ops::Range<usize>,
    p1: &ProxyBridge<T, R1>,
    r1: std::ops::Range<usize>,
    h0: &mut IdHistory<T, R0>,
    h1: &mut IdHistory<T, R1>,
    s0: &mut T,
    s1: &mut T,
    li: &mut Vec<(u64, u64)>,
    ri: &mut Vec<(u64, u64)>,
    ot: &mut ContainerOf<T>,
    od: &mut Vec<RO>,
    flush: &mut F,
) where
    T: ProxyTime,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
    F: FnMut(&mut Vec<(u64, u64)>, &mut Vec<(u64, u64)>, &mut ContainerOf<T>, &mut Vec<RO>),
{
    // `flush` ships the accumulated batch once it reaches `JOIN_CHUNK` and resets the buffers. It's
    // checked after every produced pair — *including inside the replay wave below* — so even a key
    // whose fanout dwarfs `JOIN_CHUNK` never materializes more than a chunk of its cross product.
    if r0.len() < 16 || r1.len() < 16 {
        let (t0s, t1s) = (p0.times(), p1.times());
        for a in r0 {
            s0.copy_from(t0s.get(a));
            for b in r1.clone() {
                li.push((kh, p0.ids()[a].1));
                ri.push((kh, p1.ids()[b].1));
                s1.copy_from(t1s.get(b));
                s1.join_assign(&*s0);
                push_owned::<T>(ot, s1);
                od.push(p0.diffs()[a].clone().multiply(&p1.diffs()[b]));
                if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
            }
        }
        return;
    }

    // Reusable replay scratch, reloaded per key (`load` clears + rebuilds, keeping capacity);
    // the caller holds `h0`/`h1` across the unit so a high-fanout join allocates no per-key history.
    h0.load(p0, r0, None);
    h1.load(p1, r1, None);

    while h0.time().is_some() && h1.time().is_some() {
        // The two next times live in different histories; copy both to compare owned.
        s0.copy_from(h0.time().unwrap());
        s1.copy_from(h1.time().unwrap());
        if *s0 < *s1 {
            s0.copy_from(h0.meet().unwrap());
            h1.advance_buffer_by(&*s0);
            let (v0, t0, d0) = h0.edit().unwrap();
            s0.copy_from(t0);
            let buffer = h1.buffer();
            for idx in 0..buffer.len() {
                li.push((kh, v0));
                ri.push((kh, buffer.ids()[idx]));
                s1.copy_from(buffer.time(idx));
                s1.join_assign(&*s0);
                push_owned::<T>(ot, s1);
                od.push(d0.clone().multiply(&buffer.diffs()[idx]));
                if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
            }
            h0.step();
        } else {
            s0.copy_from(h1.meet().unwrap());
            h0.advance_buffer_by(&*s0);
            let (v1, t1, d1) = h1.edit().unwrap();
            s0.copy_from(t1);
            let buffer = h0.buffer();
            for idx in 0..buffer.len() {
                li.push((kh, buffer.ids()[idx]));
                ri.push((kh, v1));
                s1.copy_from(buffer.time(idx));
                s1.join_assign(&*s0);
                push_owned::<T>(ot, s1);
                od.push(buffer.diffs()[idx].clone().multiply(d1));
                if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
            }
            h1.step();
        }
    }
    while h0.time().is_some() {
        s0.copy_from(h0.meet().unwrap());
        h1.advance_buffer_by(&*s0);
        let (v0, t0, d0) = h0.edit().unwrap();
        s0.copy_from(t0);
        let buffer = h1.buffer();
        for idx in 0..buffer.len() {
            li.push((kh, v0));
            ri.push((kh, buffer.ids()[idx]));
            s1.copy_from(buffer.time(idx));
            s1.join_assign(&*s0);
            push_owned::<T>(ot, s1);
            od.push(d0.clone().multiply(&buffer.diffs()[idx]));
            if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
        }
        h0.step();
    }
    while h1.time().is_some() {
        s0.copy_from(h1.meet().unwrap());
        h0.advance_buffer_by(&*s0);
        let (v1, t1, d1) = h1.edit().unwrap();
        s0.copy_from(t1);
        let buffer = h0.buffer();
        for idx in 0..buffer.len() {
            li.push((kh, buffer.ids()[idx]));
            ri.push((kh, v1));
            s1.copy_from(buffer.time(idx));
            s1.join_assign(&*s0);
            push_owned::<T>(ot, s1);
            od.push(buffer.diffs()[idx].clone().multiply(d1));
            if li.len() >= JOIN_CHUNK { flush(li, ri, ot, od); }
        }
        h1.step();
    }
}
