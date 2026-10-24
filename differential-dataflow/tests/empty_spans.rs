//! A reader that runs only on data holds the physical compaction of a trace that keeps sealing
//! update-free spans; the spine must fuse those spans rather than keep one per epoch.

use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;

use differential_dataflow::input::InputSession;
use differential_dataflow::trace::TraceReader;

/// A `TraceAgent` arrange seals an update-free span each idle epoch. The join reading it runs only
/// on data, so it holds the trace's physical compaction where the data stopped; the spine must fuse
/// the sealed spans rather than keep one per epoch.
#[test]
fn idle_reader_bounds_empty_spans() {
    let spans = timely::execute_directly(move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        let mut probe = ProbeHandle::new();
        let mut trace = worker.dataflow(|scope| {
            let arranged = input.to_collection(scope).arrange_by_key();
            let joined = arranged.clone().join_core(arranged.clone(), |k, a, b| Some((*k, *a, *b)));
            joined.probe_with(&mut probe);
            arranged.trace
        });
        // This handle should hold nothing back; only the join's holds remain.
        trace.set_logical_compaction(Antichain::new().borrow());
        trace.set_physical_compaction(Antichain::new().borrow());

        for i in 0..100u64 { input.insert((i % 10, i)); }
        input.advance_to(1); input.flush();
        worker.step_while(|| probe.less_than(input.time()));
        for epoch in 1..2001u64 {
            input.advance_to(epoch + 1); input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        }

        let mut spans = 0;
        trace.map_spans(|_| spans += 1);
        spans
    });
    assert!(spans <= 4, "{spans} spans after 2000 idle epochs");
}
