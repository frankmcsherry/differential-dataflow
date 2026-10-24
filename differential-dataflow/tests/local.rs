//! Arrangements built with `TraceLocal` compute what `TraceAgent` ones do, and their operators
//! do not run on frontier advances that bring no data.

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::Capture;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::channels::pact::Exchange;
use timely::progress::Antichain;

use differential_dataflow::{Hashable, VecCollection};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::{Agent, ArrangeContext, Arranged, TraceAgent, TraceLocal};
use differential_dataflow::operators::arrange::arrangement::arrange_core_with;
use differential_dataflow::operators::reduce::reduce_trace_with;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::{ValBatcher, ValBuilder, ValSpine};

type Edges = ValSpine<u64, u64, u64, isize>;
type Triples = ValSpine<(u64, u64, u64), (), u64, isize>;
type Counts = ValSpine<(u64, u64, u64), isize, u64, isize>;

/// Arranges `edges` by key, joins the arrangement with itself, arranges the triples, and counts them.
///
/// The three arrangements are built through the three factories.
fn join_count<'scope, A1, A2, A3>(
    edges: VecCollection<'scope, u64, (u64, u64), isize>,
    f1: impl FnOnce(ArrangeContext) -> (A1, A1::Writer),
    f2: impl FnOnce(ArrangeContext) -> (A2, A2::Writer),
    f3: impl FnOnce(ArrangeContext) -> (A3, A3::Writer),
) -> (Arranged<'scope, A1>, VecCollection<'scope, u64, ((u64, u64, u64), isize), isize>)
where
    A1: Agent<Time = u64, Batch = <Edges as TraceReader>::Batch> + 'static,
    A2: Agent<Time = u64, Batch = <Triples as TraceReader>::Batch> + 'static,
    A3: Agent<Time = u64, Batch = <Counts as TraceReader>::Batch> + 'static,
{
    let by_key = Exchange::new(move |update: &((u64, u64), u64, isize)| (update.0).0.hashed().into());
    let arranged = arrange_core_with::<_, _, ValBatcher<_, _, _, _>, A1, _>(edges.inner, by_key, "Arrange", ValBatcher::new, f1);
    let joined = arranged.clone().join_core(arranged.clone(), |k, a, b| Some(((*k, *a, *b), ())));
    let by_key = Exchange::new(move |update: &(((u64, u64, u64), ()), u64, isize)| (update.0).0.hashed().into());
    let triples = arrange_core_with::<_, _, ValBatcher<_, _, _, _>, A2, _>(joined.inner, by_key, "ArrangeCount", ValBatcher::new, f2);
    let counted = reduce_trace_with::<_, ValBuilder<_, _, _, _>, A3, Vec<(u64, u64, u64)>, _, _, _>(
        triples,
        "Count",
        |_key, input, output, change| {
            if !input.is_empty() { change.push((input[0].1, 1)); }
            change.extend(output.drain(..).map(|(x, d)| (x, -d)));
            differential_dataflow::consolidation::consolidate(change);
        },
        |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r))); },
        f3,
    )
    .as_collection(|k, c| (*k, *c));
    (arranged, counted)
}

/// Runs `join_count` over the three factories: data in epochs 0 and 1, then `idle` epochs with none.
///
/// Returns the upper frontier of the first arrangement's trace at the end, and the captured output.
fn run<A1, A2, A3, F1, F2, F3>(
    factories: impl FnOnce() -> (F1, F2, F3) + Send + Sync + 'static,
    idle: u64,
) -> (Antichain<u64>, Vec<(((u64, u64, u64), isize), u64, isize)>)
where
    A1: Agent<Time = u64, Batch = <Edges as TraceReader>::Batch> + 'static,
    A2: Agent<Time = u64, Batch = <Triples as TraceReader>::Batch> + 'static,
    A3: Agent<Time = u64, Batch = <Counts as TraceReader>::Batch> + 'static,
    F1: FnOnce(ArrangeContext) -> (A1, A1::Writer),
    F2: FnOnce(ArrangeContext) -> (A2, A2::Writer),
    F3: FnOnce(ArrangeContext) -> (A3, A3::Writer),
{
    let result = timely::execute_directly(move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        let mut probe = ProbeHandle::new();
        let (f1, f2, f3) = factories();
        let (mut trace, captured) = worker.dataflow(|scope| {
            let (arranged, counted) = join_count(input.to_collection(scope), f1, f2, f3);
            let counted = counted.probe_with(&mut probe);
            (arranged.trace, counted.inner.capture())
        });

        for i in 0..100u64 { input.insert((i % 10, i)); }
        input.advance_to(1); input.flush();
        worker.step_while(|| probe.less_than(input.time()));
        for i in 0..50u64 { input.remove((i % 10, i)); }
        input.advance_to(2); input.flush();
        worker.step_while(|| probe.less_than(input.time()));
        for epoch in 2..2 + idle {
            input.advance_to(epoch + 1); input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        }

        let mut upper = Antichain::new();
        trace.read_upper(&mut upper);
        (upper, captured)
    });
    // The capture's sender lives in the dataflow, so the output is extracted once the worker is done.
    let (upper, captured) = result;
    let mut results = captured.extract().into_iter().flat_map(|(_, data)| data).collect::<Vec<_>>();
    results.sort();
    (upper, results)
}

#[test]
fn local_matches_agent() {
    let (_, agent) = run(|| (TraceAgent::<Edges>::factory(), TraceAgent::<Triples>::factory(), TraceAgent::<Counts>::factory()), 20);
    let (_, local) = run(|| (TraceLocal::<Edges>::factory(), TraceLocal::<Triples>::factory(), TraceLocal::<Counts>::factory()), 20);
    assert!(!agent.is_empty());
    assert_eq!(agent, local);
}

/// An idle `TraceAgent` arrange seals each epoch; an idle `TraceLocal` arrange does not run, and
/// its trace's upper stays where the data stopped.
#[test]
fn local_idles() {
    let (agent, _) = run(|| (TraceAgent::<Edges>::factory(), TraceAgent::<Triples>::factory(), TraceAgent::<Counts>::factory()), 20);
    let (local, _) = run(|| (TraceLocal::<Edges>::factory(), TraceLocal::<Triples>::factory(), TraceLocal::<Counts>::factory()), 20);
    assert_eq!(agent, Antichain::from_elem(22));
    assert_eq!(local, Antichain::from_elem(2));
}
