//! The CHUNK1 / CHUNK2 / LOGIC membrane, in its MULTITEMPORAL form — the partially-ordered-time
//! case, which is the whole point (total order is 1980s tech).
//!
//! `Logic` is a *type that understands both chunks*: it reads the input chunk's value vocabulary
//! (`c1.val`) and mints into the output chunk's, through an ephemeral per-session `OutSpace<C2>`
//! overlay. The reduce/join *operator* stays value-blind — it works only in `u32` proxies and `T`s.
//!
//! What makes this the real test (not a total-order one):
//!   * time `T2(a,b)` is a LATTICE with the componentwise *partial* order — input arrives at
//!     INCOMPARABLE times, so the operator must also evaluate at SYNTHETIC interesting times (joins
//!     of input/output times, where the output changes although no input lands there);
//!   * the LOGIC is JOINT — it reads prior output mid-sweep — so the `OutSpace` namespace must be
//!     live across the whole interesting-times sweep, not merely at a final diff.
//!
//! `Item`/`Out` derive no `Ord` — so the operator provably never compares a value; only `Logic` and
//! the chunks do.
//!
//! ```text
//! cargo run --release --example chunk_protocol_mt
//! ```

use std::collections::HashMap;
use std::hash::Hash;

type Key = u64;
type Proxy = u32;
type Diff = i64;

/// Lattice timestamp. Derived `Ord` is lexicographic — a valid linear extension of the partial
/// order `leq` below (a ≤_cw b ⇒ a ≤_lex b), so processing in `Ord` order respects causality.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
struct T2(u32, u32);
impl T2 {
    fn leq(self, o: T2) -> bool { self.0 <= o.0 && self.1 <= o.1 } // the PARTIAL order
    fn join(self, o: T2) -> T2 { T2(self.0.max(o.0), self.1.max(o.1)) }
}

// ---- CHUNK: read-only store; owns its value type ---------------------------------------------

trait Chunk {
    type Val: Clone;
    fn unload(&self, keys: &[Key]) -> Vec<(Key, Proxy, T2, Diff)>;
    fn val(&self, key: Key, p: Proxy) -> Self::Val;
}

struct VecChunk<V> {
    vals: HashMap<Key, Vec<V>>,
    updates: Vec<(Key, Proxy, T2, Diff)>,
}
impl<V: Clone + Eq> VecChunk<V> {
    fn build(raw: Vec<(Key, V, T2, Diff)>) -> Self {
        let mut vals: HashMap<Key, Vec<V>> = HashMap::new();
        let mut updates = Vec::new();
        for (k, v, t, d) in raw {
            let slot = vals.entry(k).or_default();
            let p = slot.iter().position(|x| *x == v).unwrap_or_else(|| {
                slot.push(v);
                slot.len() - 1
            }) as Proxy;
            updates.push((k, p, t, d));
        }
        VecChunk { vals, updates }
    }
}
impl<V: Clone + Eq> Chunk for VecChunk<V> {
    type Val = V;
    fn unload(&self, keys: &[Key]) -> Vec<(Key, Proxy, T2, Diff)> {
        self.updates.iter().filter(|(k, ..)| keys.contains(k)).cloned().collect()
    }
    fn val(&self, key: Key, p: Proxy) -> V {
        self.vals[&key][p as usize].clone()
    }
}

// ---- OutSpace: ephemeral session namespace over a read-only output chunk ----------------------

struct OutSpace<'a, C: Chunk>
where
    C::Val: Eq + Hash,
{
    intern: HashMap<C::Val, Proxy>,
    vals: Vec<C::Val>,
    touches: usize,
    _base: &'a C,
}
impl<'a, C: Chunk> OutSpace<'a, C>
where
    C::Val: Eq + Hash + Clone,
{
    /// Seed the session namespace from existing output; return prior output as session proxies.
    fn seed(base: &'a C, keys: &[Key]) -> (Self, Vec<(Key, Proxy, T2, Diff)>) {
        let mut me = OutSpace { intern: HashMap::new(), vals: Vec::new(), touches: 0, _base: base };
        let prior = base
            .unload(keys)
            .into_iter()
            .map(|(k, bp, t, d)| {
                let v = base.val(k, bp);
                (k, me.intern_val(v), t, d)
            })
            .collect();
        (me, prior)
    }
    fn intern_val(&mut self, v: C::Val) -> Proxy {
        self.touches += 1;
        if let Some(p) = self.intern.get(&v) {
            return *p;
        }
        let p = self.vals.len() as Proxy;
        self.vals.push(v.clone());
        self.intern.insert(v, p);
        p
    }
    fn val(&mut self, p: Proxy) -> C::Val {
        self.touches += 1;
        self.vals[p as usize].clone()
    }
}

// ---- LOGIC: the membrane — a type understanding both chunks -----------------------------------

trait Logic {
    type C1: Chunk;
    type C2: Chunk;
    fn apply(
        &mut self,
        key: Key,
        c1: &Self::C1,
        out: &mut OutSpace<Self::C2>,
        input: &[(Proxy, Diff)],
        prior: &[(Proxy, Diff)],
        sink: &mut Vec<(Proxy, Diff)>,
    ) where
        <Self::C2 as Chunk>::Val: Eq + Hash + Clone;
}

/// JOINT sum: emits the change directly. Reads prior output values (out.val) mid-sweep to retract
/// them, then adds the new sum — exactly the non-abelian `reduce_core` contract.
struct SumLogic;
impl Logic for SumLogic {
    type C1 = VecChunk<Item>;
    type C2 = VecChunk<Out>;
    fn apply(
        &mut self,
        key: Key,
        c1: &VecChunk<Item>,
        out: &mut OutSpace<VecChunk<Out>>,
        input: &[(Proxy, Diff)],
        prior: &[(Proxy, Diff)],
        sink: &mut Vec<(Proxy, Diff)>,
    ) {
        // retract whatever output was reported so far at this time (reading the LIVE namespace)
        for (p, d) in prior {
            let _old: Out = out.val(*p); // resolve a prior-output proxy DURING the sweep
            sink.push((*p, -*d));
        }
        // add the new desired output, if any input is present
        if !input.is_empty() {
            let s: i64 = input.iter().map(|(p, d)| c1.val(key, *p).0 as i64 * *d).sum();
            sink.push((out.intern_val(Out(s)), 1));
        }
    }
}

// ---- operator: pure proxy choreography over the lattice ---------------------------------------

fn consolidate_pairs(v: Vec<(Proxy, Diff)>) -> Vec<(Proxy, Diff)> {
    let mut acc: HashMap<Proxy, Diff> = HashMap::new();
    for (p, d) in v {
        *acc.entry(p).or_default() += d;
    }
    let mut r: Vec<_> = acc.into_iter().filter(|(_, d)| *d != 0).collect();
    r.sort();
    r
}
fn collection_as_of(updates: &[(Key, Proxy, T2, Diff)], key: Key, t: T2) -> Vec<(Proxy, Diff)> {
    consolidate_pairs(
        updates
            .iter()
            .filter(|(k, _, tt, _)| *k == key && tt.leq(t))
            .map(|(_, p, _, d)| (*p, *d))
            .collect(),
    )
}
/// Interesting times for a key: the join-closure of input+prior times within [lower,upper).
/// A correct superset of the times where output can change (the real algorithm prunes harder).
fn interesting(all: &[(Key, Proxy, T2, Diff)], key: Key, lower: T2, upper: T2) -> Vec<T2> {
    let mut set: Vec<T2> = all.iter().filter(|(k, ..)| *k == key).map(|(_, _, t, _)| *t).collect();
    set.sort();
    set.dedup();
    loop {
        let cur = set.clone();
        let mut added = false;
        for i in 0..cur.len() {
            for j in i..cur.len() {
                let j2 = cur[i].join(cur[j]);
                if !set.contains(&j2) {
                    set.push(j2);
                    added = true;
                }
            }
        }
        if !added {
            break;
        }
    }
    set.retain(|t| lower.leq(*t) && !upper.leq(*t));
    set.sort(); // lex == linear extension of the partial order
    set
}

fn run_session<L: Logic>(
    c1: &L::C1,
    c2: &L::C2,
    keys: &[Key],
    lower: T2,
    upper: T2,
    logic: &mut L,
) -> (Vec<(Key, <L::C2 as Chunk>::Val, T2, Diff)>, usize)
where
    <L::C2 as Chunk>::Val: Eq + Hash + Clone,
{
    let input = c1.unload(keys);
    let (mut space, prior_all) = OutSpace::seed(c2, keys);
    let mut produced: Vec<(Key, Proxy, T2, Diff)> = Vec::new();

    for &key in keys {
        let mut candidates = input.clone();
        candidates.extend(prior_all.iter().cloned());
        for t in interesting(&candidates, key, lower, upper) {
            let in_cell = collection_as_of(&input, key, t);
            let mut combined = prior_all.clone();
            combined.extend(produced.iter().cloned());
            let prior_cell = collection_as_of(&combined, key, t);

            let mut sink = Vec::new();
            logic.apply(key, c1, &mut space, &in_cell, &prior_cell, &mut sink);
            for (p, d) in consolidate_pairs(sink) {
                produced.push((key, p, t, d));
            }
        }
    }

    let out = produced.into_iter().map(|(k, p, t, d)| (k, space.val(p), t, d)).collect();
    (out, space.touches)
}

// ---- value types: no Ord/PartialOrd -> operator provably can't compare them -------------------

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Item(u32);
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Out(i64);

fn normalize(v: Vec<(Key, Out, T2, Diff)>) -> Vec<(u64, i64, u32, u32, i64)> {
    let mut r: Vec<_> = v.into_iter().map(|(k, o, t, d)| (k, o.0, t.0, t.1, d)).collect();
    r.sort();
    r
}

fn main() {
    let keys = [100u64, 200];

    // key 100: input at INCOMPARABLE times (1,0) and (0,1) -> a synthetic interesting time (1,1).
    // key 200: a single time -> no synthetic times (a control).
    let input = VecChunk::build(vec![
        (100, Item(3), T2(1, 0), 1),
        (100, Item(5), T2(0, 1), 1),
        (200, Item(7), T2(0, 0), 1),
    ]);
    let empty: VecChunk<Out> = VecChunk::build(vec![]);

    let (out, touches) =
        run_session::<SumLogic>(&input, &empty, &keys, T2(0, 0), T2(100, 100), &mut SumLogic);
    let stream = normalize(out);
    for (k, v, a, b, d) in &stream {
        println!("  ({}, Out({}), ({},{}), {:+})", k, v, a, b, d);
    }

    // Expected change stream:
    //   key 100: +Out(3)@(1,0), +Out(5)@(0,1), and at the SYNTHETIC time (1,1): -Out(3) -Out(5) +Out(8)
    //   key 200: +Out(7)@(0,0)
    let expected = vec![
        (100, 3, 1, 0, 1),
        (100, 3, 1, 1, -1),
        (100, 5, 0, 1, 1),
        (100, 5, 1, 1, -1),
        (100, 8, 1, 1, 1),
        (200, 7, 0, 0, 1),
    ];
    let mut expected_sorted = expected.clone();
    expected_sorted.sort();
    assert_eq!(stream, expected_sorted, "multitemporal change stream mismatch");

    // Strong check: the *accumulated* output collection at the top time is the true reduce.
    let acc = |key: u64, t: T2| -> Vec<(i64, i64)> {
        let mut m: HashMap<i64, i64> = HashMap::new();
        for (k, v, a, b, d) in &stream {
            if *k == key && T2(*a, *b).leq(t) {
                *m.entry(*v).or_default() += d;
            }
        }
        let mut r: Vec<_> = m.into_iter().filter(|(_, d)| *d != 0).collect();
        r.sort();
        r
    };
    assert_eq!(acc(100, T2(1, 1)), vec![(8, 1)], "key 100 accumulates to Out(8)");
    assert_eq!(acc(200, T2(0, 0)), vec![(7, 1)], "key 200 accumulates to Out(7)");

    println!(
        "\nMultitemporal OK. The synthetic time (1,1) was discovered (join of incomparable inputs),\n\
         the joint LOGIC retracted prior output by resolving proxies in the LIVE session namespace\n\
         mid-sweep, and the accumulated collection is the true reduce. The operator never compared a\n\
         value (Item/Out have no Ord). Value-touches: {} — all in seed/LOGIC, none in the operator.",
        touches
    );
}
