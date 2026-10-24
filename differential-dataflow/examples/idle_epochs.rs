//! The cost of epochs that bring no data, for many arrange, join and reduce operators.
//!
//! Fifty copies of a self-join followed by a count, over one input, then many epochs in which
//! the input only advances. Reports the time per idle epoch.
//!
//! ```text
//! idle_epochs [default|local] [copies] [epochs]
//! ```
//!
//! `default` builds the copies with `join` and `count`, whose arrangements are `TraceAgent`s.
//! `local` builds the same operators over `TraceLocal` arrangements, so that the arrange and
//! reduce operators, too, run only while they hold capabilities.

use std::time::Instant;

use timely::dataflow::ProbeHandle;
use timely::dataflow::channels::pact::Exchange;

use differential_dataflow::{Hashable, VecCollection};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::TraceLocal;
use differential_dataflow::operators::arrange::arrangement::arrange_core_with;
use differential_dataflow::operators::reduce::reduce_trace_with;
use differential_dataflow::trace::implementations::{ValBatcher, ValBuilder, ValSpine};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let local = match args.get(1).map(String::as_str) {
        Some("local") => true,
        Some("default") | None => false,
        Some(other) => panic!("unknown mode {other:?}; expected `default` or `local`"),
    };
    let copies: u64 = args.get(2).map_or(50, |a| a.parse().unwrap());
    let epochs: u64 = args.get(3).map_or(10_000, |a| a.parse().unwrap());

    timely::execute(timely::Config::thread(), move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        let probe = ProbeHandle::new();
        worker.dataflow(|scope| {
            let edges = input.to_collection(scope);
            for copy in 0 .. copies {
                let edges = edges.clone().map(move |(a, b)| (a, b + copy));
                let counted = if local { join_count_local(edges) } else { join_count(edges) };
                counted.probe_with(&probe);
            }
        });
        for i in 0 .. 1_000 {
            input.insert((i % 100, i));
        }
        input.advance_to(1);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        let start = Instant::now();
        for epoch in 1 ..= epochs {
            input.advance_to(epoch + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        }
        let elapsed = start.elapsed();
        println!("{copies} copies, {epochs} idle epochs, {}: {:.3} s ({:.1} us/epoch)",
            if local { "local" } else { "default" }, elapsed.as_secs_f64(), elapsed.as_secs_f64() * 1e6 / epochs as f64);
    })
    .unwrap();
}

type Counted<'scope> = VecCollection<'scope, u64, ((u64, u64, u64), isize), isize>;

/// A self-join and a count, as `join` and `count` build them.
fn join_count<'scope>(edges: VecCollection<'scope, u64, (u64, u64), isize>) -> Counted<'scope> {
    edges.clone().join(edges).map(|(k, (a, b))| (k, a, b)).count()
}

/// The same operators, over `TraceLocal` arrangements.
fn join_count_local<'scope>(edges: VecCollection<'scope, u64, (u64, u64), isize>) -> Counted<'scope> {
    type Edges = ValSpine<u64, u64, u64, isize>;
    type Triples = ValSpine<(u64, u64, u64), (), u64, isize>;
    type Counts = ValSpine<(u64, u64, u64), isize, u64, isize>;

    let by_key = Exchange::new(move |update: &((u64, u64), u64, isize)| (update.0).0.hashed().into());
    let arranged = arrange_core_with::<_, _, ValBatcher<_, _, _, _>, TraceLocal<Edges>, _>(edges.inner, by_key, "Arrange", ValBatcher::new, TraceLocal::factory());
    let joined = arranged.clone().join_core(arranged, |k, a, b| Some(((*k, *a, *b), ())));
    let by_key = Exchange::new(move |update: &(((u64, u64, u64), ()), u64, isize)| (update.0).0.hashed().into());
    let triples = arrange_core_with::<_, _, ValBatcher<_, _, _, _>, TraceLocal<Triples>, _>(joined.inner, by_key, "Arrange: Count", ValBatcher::new, TraceLocal::factory());
    reduce_trace_with::<_, ValBuilder<_, _, _, _>, TraceLocal<Counts>, Vec<(u64, u64, u64)>, _, _, _>(
        triples,
        "Count",
        |_key, input, output, change| {
            if !input.is_empty() { change.push((input[0].1, 1)); }
            change.extend(output.drain(..).map(|(x, d)| (x, -d)));
            differential_dataflow::consolidation::consolidate(change);
        },
        |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r))); },
        TraceLocal::factory(),
    )
    .as_collection(|k, c| (*k, *c))
}
