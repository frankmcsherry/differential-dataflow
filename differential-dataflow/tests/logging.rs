use std::sync::{Arc, Mutex};

use timely::dataflow::operators::Probe;
use timely::progress::frontier::AntichainRef;

use differential_dataflow::input::InputSession;
use differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
use differential_dataflow::trace::TraceReader;

/// The spine's batch events balance: adding one per `BatchEvent` and removing one per completed
/// `MergeEvent` and per `DropEvent` counts the batches the trace holds, and returns to zero once the
/// trace is dropped. Rounds without updates insert batches with no updates, which merge without work.
///
/// A consumer that counts only batches with updates also balances.
#[test]
fn spine_batch_events_balance() {

    let batches = Arc::new(Mutex::new((0i64, 0i64)));
    let batches2 = Arc::clone(&batches);

    timely::execute_directly(move |worker| {

        worker.log_register().unwrap().insert::<DifferentialEventBuilder, _>("differential/arrange", move |_time, data| {
            if let Some(data) = data {
                let (all, nonempty) = &mut *batches2.lock().unwrap();
                let count = |length: usize| (length > 0) as i64;
                for (_, event) in data.iter() {
                    match event {
                        DifferentialEvent::Batch(e) => {
                            *all += 1;
                            *nonempty += count(e.length);
                        }
                        DifferentialEvent::Merge(e) => if let Some(complete) = e.complete {
                            *all -= 1;
                            *nonempty += count(complete) - count(e.length1) - count(e.length2);
                        }
                        DifferentialEvent::Drop(e) => {
                            *all -= 1;
                            *nonempty -= count(e.length);
                        }
                        _ => { }
                    }
                }
            }
        });

        let mut input = InputSession::new();
        // Hold the trace, as the arrangement maintains its spine only while a handle exists.
        let (probe, trace) = worker.dataflow(|scope| {
            let arranged = input.to_collection(scope).arrange_by_self();
            let (probe, _) = arranged.stream.probe();
            (probe, arranged.trace)
        });

        for round in 0u64 .. 100 {
            // Updates in some rounds, retractions in others, and nothing in most.
            if round % 10 == 0 { input.insert(round); }
            if round % 10 == 5 { input.remove(round - 5); }
            input.advance_to(round + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        }

        drop(trace);
        let dataflow = worker.installed_dataflows()[0];
        worker.drop_dataflow(dataflow);
        worker.step();
    });

    assert_eq!(*batches.lock().unwrap(), (0, 0));
}

/// With the trace's compaction advancing every round, the logged batch count matches the spans the
/// trace holds after every round, through bursts of updates and long runs of rounds without them.
#[test]
fn spine_batch_events_track_spans() {

    let logged = Arc::new(Mutex::new(0i64));
    let logged2 = Arc::clone(&logged);

    timely::execute_directly(move |worker| {

        worker.log_register().unwrap().insert::<DifferentialEventBuilder, _>("differential/arrange", move |_time, data| {
            if let Some(data) = data {
                let count = &mut *logged2.lock().unwrap();
                for (_, event) in data.iter() {
                    match event {
                        DifferentialEvent::Batch(_) => *count += 1,
                        DifferentialEvent::Merge(e) => if e.complete.is_some() { *count -= 1 },
                        DifferentialEvent::Drop(_) => *count -= 1,
                        _ => { }
                    }
                }
            }
        });

        let mut input = InputSession::new();
        let (probe, mut trace) = worker.dataflow(|scope| {
            let arranged = input.to_collection(scope).arrange_by_self();
            let (probe, _) = arranged.stream.probe();
            (probe, arranged.trace)
        });

        for round in 0u64 .. 200 {
            // A burst of updates every fifty rounds, and nothing in between.
            if round % 50 == 0 { for x in 0 .. 20 { input.insert(round + x); } }
            input.advance_to(round + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));
            trace.set_logical_compaction(AntichainRef::new(&[round + 1]));
            trace.set_physical_compaction(AntichainRef::new(&[round + 1]));
            worker.log_register().unwrap().flush();

            let mut spans = 0;
            trace.map_spans(|_| spans += 1);
            assert_eq!(*logged.lock().unwrap(), spans, "round {round}");
        }
    });
}
