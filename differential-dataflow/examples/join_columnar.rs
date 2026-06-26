//! Join over ColChunk via the REAL `join_traces` (through `Arranged::join_core`).
//!
//! Unlike `reduce`, join has no fold to make column-native: its compute is a cross-product
//! enumeration, and the cursor over a `ColChunk` already reads values from columns (`index_as`). So
//! the faithful "mirror `join_traces`, swap compute" for join is simply to run the existing
//! delta-query operator over `ColChunk` arrangements — which is work-efficient (Δa⋈b + a⋈Δb), unlike
//! a recompute-then-diff approach. This is the operator you'd actually use.
//!
//! Validated against the built-in `join_map` over real incremental rounds.
//!
//! ```text
//! cargo run --release --example join_columnar
//! ```

use std::cell::RefCell;
use std::rc::Rc;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::{Inspect, Probe};

use differential_dataflow::columnar::trace::{Builder as ColBuilder, ColChunk, Spine as ColSpine};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::Hashable;

type U = (u64, u64, u64, isize);
type OutRec = ((u64, u64), u64, isize);

fn normalize(mut v: Vec<OutRec>) -> Vec<OutRec> {
    v.sort();
    let mut out: Vec<OutRec> = Vec::new();
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
    let keys: u64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(100);

    let (mine, builtin) = timely::execute_directly(move |worker| {
        let out_mine: Rc<RefCell<Vec<OutRec>>> = Rc::new(RefCell::new(Vec::new()));
        let out_built: Rc<RefCell<Vec<OutRec>>> = Rc::new(RefCell::new(Vec::new()));
        let mut in_a: InputSession<u64, (u64, u64), isize> = InputSession::new();
        let mut in_b: InputSession<u64, (u64, u64), isize> = InputSession::new();
        let mut probe = ProbeHandle::new();

        {
            let (om, ob) = (out_mine.clone(), out_built.clone());
            worker.dataflow(|scope| {
                let a = in_a.to_collection(scope);
                let b = in_b.to_collection(scope);

                type Chu = ContainerChunker<ColChunk<U>>;
                type Ba = differential_dataflow::columnar::trace::Batcher<u64, u64, u64, isize>;
                type Bu = ColBuilder<u64, u64, u64, isize>;
                type Sp = ColSpine<u64, u64, u64, isize>;
                let exa = Exchange::new(|u: &((u64, u64), u64, isize)| (u.0).0.hashed());
                let exb = Exchange::new(|u: &((u64, u64), u64, isize)| (u.0).0.hashed());
                let a_arr = arrange_core::<_, _, Chu, Ba, Bu, Sp>(a.clone().inner, exa, "ArrangeA");
                let b_arr = arrange_core::<_, _, Chu, Ba, Bu, Sp>(b.clone().inner, exb, "ArrangeB");

                // The real delta-query join, over columnar arrangements.
                a_arr
                    .join_core(b_arr, |_k, v1, v2| Some((*v1, *v2)))
                    .inner
                    .probe_with(&mut probe)
                    .inspect(move |x: &OutRec| om.borrow_mut().push(x.clone()));

                a.join_map(b, |_k, &v1, &v2| (v1, v2))
                    .inner
                    .inspect(move |x: &OutRec| ob.borrow_mut().push(x.clone()));
            });
        }

        let seed: &[_] = &[17usize];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        for round in 0..rounds {
            in_a.advance_to(round);
            in_b.advance_to(round);
            for _ in 0..keys {
                let ka = rng.gen_range(0, keys);
                let va = rng.gen_range(0, 50u64);
                let kb = rng.gen_range(0, keys);
                let vb = rng.gen_range(0, 50u64);
                if round > 0 && rng.gen_range(0, 10) < 4 {
                    in_a.remove((ka, va));
                } else {
                    in_a.insert((ka, va));
                }
                if round > 0 && rng.gen_range(0, 10) < 4 {
                    in_b.remove((kb, vb));
                } else {
                    in_b.insert((kb, vb));
                }
            }
            in_a.advance_to(round + 1);
            in_b.advance_to(round + 1);
            in_a.flush();
            in_b.flush();
            while probe.less_than(in_a.time()) || probe.less_than(in_b.time()) {
                worker.step();
            }
        }
        drop(in_a);
        drop(in_b);
        while worker.step() {}

        let m = out_mine.borrow().clone();
        let bb = out_built.borrow().clone();
        (m, bb)
    });

    let mine = normalize(mine);
    let builtin = normalize(builtin);
    println!("columnar join_core emitted {} updates; built-in join_map {}", mine.len(), builtin.len());
    if mine == builtin {
        println!("MATCH: join_core over ColChunk (real delta-query) agrees with built-in join_map.");
    } else {
        println!("MISMATCH!");
        let only_mine: Vec<_> = mine.iter().filter(|x| !builtin.contains(x)).take(8).collect();
        let only_built: Vec<_> = builtin.iter().filter(|x| !mine.contains(x)).take(8).collect();
        println!("  only mine    (<=8): {:?}", only_mine);
        println!("  only builtin (<=8): {:?}", only_built);
        std::process::exit(1);
    }
}
