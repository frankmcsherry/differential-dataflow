//! Multitemporal `reduce` over columnar (`ColChunk`) storage: the real `reduce_trace`, unchanged,
//! run over a `ColChunk` input arrangement and producing a maintained `ColChunk` output arrangement.
//!
//! This is the parity-aiming direction: keep the existing multitemporal operator (full lattice time,
//! work-efficient, output-trace, compaction — all of it) and swap only the *storage* to columnar,
//! for its memory benefits. It uses `Arranged::reduce_abelian` with columnar input + output spines
//! and a columnar `push` closure that builds the output trie per key; the output is a live
//! `TraceAgent<ColChunk Spine>` downstream operators can share. Validated against the built-in (Vec)
//! `reduce` over real incremental rounds.
//!
//! Perf note: the compute is identical to the incumbent, so the only delta vs the Vec-spine `reduce`
//! is storage — measured at roughly bulk-parity, ~1.5x slower incremental (the `ColChunk` trie's
//! per-round build/merge vs the flat Vec spine). The (separately explored, total-order) attempt to
//! make the *compute* itself column-native did not beat parity, so it's not pursued here.
//!
//! ```text
//! cargo run --release --example reduce_columnar_arr
//! ```

use std::cell::RefCell;
use std::rc::Rc;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::{Inspect, Probe};

use differential_dataflow::columnar::trace::{Builder, ColChunk, Spine};
use differential_dataflow::columnar::updates::UpdatesTyped;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::Hashable;

type K = u64;
type V = u64;
type T = u64;
type R = isize;
type V2 = i64;

type Out = ((u64, i64), u64, isize);

fn normalize(mut v: Vec<Out>) -> Vec<Out> {
    v.sort();
    let mut out: Vec<Out> = Vec::new();
    for (d, t, r) in v {
        if let Some(last) = out.last_mut() {
            if last.0 == d && last.1 == t {
                last.2 += r;
                continue;
            }
        }
        out.push((d, t, r));
    }
    out.retain(|(_, _, r)| *r != 0);
    out
}

fn main() {
    let rounds: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(20);
    let keys: u64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(200);

    let (mine, builtin) = timely::execute_directly(move |worker| {
        let out_mine: Rc<RefCell<Vec<Out>>> = Rc::new(RefCell::new(Vec::new()));
        let out_built: Rc<RefCell<Vec<Out>>> = Rc::new(RefCell::new(Vec::new()));
        let mut input: InputSession<T, (K, V), R> = InputSession::new();
        let mut probe = ProbeHandle::new();

        {
            let (om, ob) = (out_mine.clone(), out_built.clone());
            worker.dataflow(|scope| {
                let coll = input.to_collection(scope);

                // Arrange the input into a columnar (ColChunk) spine.
                type Chu = ContainerChunker<ColChunk<(K, V, T, R)>>;
                type Ba = differential_dataflow::columnar::trace::Batcher<K, V, T, R>;
                type BuIn = Builder<K, V, T, R>;
                type Sp = Spine<K, V, T, R>;
                let exchange = Exchange::new(|u: &((K, V), T, R)| (u.0).0.hashed());
                let arranged =
                    arrange_core::<_, _, Chu, Ba, BuIn, Sp>(coll.clone().inner, exchange, "ArrangeIn");

                // reduce_abelian -> a maintained ColChunk OUTPUT arrangement.
                let result = arranged.reduce_abelian::<_, Builder<K, V2, T, R>, Spine<K, V2, T, R>, _>(
                    "ReduceColumnar",
                    |_key, in_, out: &mut Vec<(V2, R)>| {
                        let s: i64 = in_.iter().map(|(v, r)| (**v as i64) * (*r as i64)).sum();
                        out.push((s, 1));
                    },
                    |col: &mut ColChunk<(K, V2, T, R)>, key, upds: &mut Vec<(V2, T, R)>| {
                        use columnar::Push;
                        let mut trie = UpdatesTyped::default();
                        for (val, time, diff) in upds.drain(..) {
                            trie.push((key, &val, &time, &diff));
                        }
                        *col.updates_mut() = trie.consolidate();
                    },
                );

                result
                    .as_collection(|k, v| (*k, *v))
                    .inner
                    .probe_with(&mut probe)
                    .inspect(move |x: &Out| om.borrow_mut().push(x.clone()));

                coll.reduce(|_k: &u64, input: &[(&u64, isize)], out: &mut Vec<(i64, isize)>| {
                    let s: i64 = input.iter().map(|(v, r)| (**v as i64) * (*r as i64)).sum();
                    out.push((s, 1));
                })
                .inner
                .inspect(move |x: &Out| ob.borrow_mut().push(x.clone()));
            });
        }

        let seed: &[_] = &[7usize];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        for round in 0..rounds {
            input.advance_to(round);
            for _ in 0..keys {
                let k = rng.gen_range(0, keys);
                let v = rng.gen_range(0, 1000u64);
                if round > 0 && rng.gen_range(0, 10) < 4 {
                    input.remove((k, v));
                } else {
                    input.insert((k, v));
                }
            }
            input.advance_to(round + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        drop(input);
        while worker.step() {}

        let m = out_mine.borrow().clone();
        let b = out_built.borrow().clone();
        (m, b)
    });

    let mine = normalize(mine);
    let builtin = normalize(builtin);
    println!("columnar-arrangement reduce emitted {} updates; built-in {}", mine.len(), builtin.len());
    if mine == builtin {
        println!("MATCH: reduce -> maintained ColChunk arrangement agrees with built-in reduce.");
    } else {
        println!("MISMATCH!");
        let only_mine: Vec<_> = mine.iter().filter(|x| !builtin.contains(x)).take(8).collect();
        let only_built: Vec<_> = builtin.iter().filter(|x| !mine.contains(x)).take(8).collect();
        println!("  only mine    (<=8): {:?}", only_mine);
        println!("  only builtin (<=8): {:?}", only_built);
        std::process::exit(1);
    }
}
