use std::sync::{Arc, Mutex};

use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{Antichain, frontier::AntichainRef};

use differential_dataflow::trace::implementations::{ValBatcher, ValSpine};
use differential_dataflow::batcher::{Batcher, Extract};
use differential_dataflow::trace::{Description, Span, Trace, TraceReader};
use differential_dataflow::trace::cursor::{Cursor, cursor_list};

type IntegerTrace = ValSpine<u64, u64, usize, i64>;

fn get_trace() -> ValSpine<u64, u64, usize, i64> {
    let op_info = OperatorInfo::new(0, 0, [].into());
    let mut trace = IntegerTrace::new(op_info, None, None);
    {
        let mut batcher = ValBatcher::<u64,u64,usize,i64>::new(None, 0);

        let mut input: Vec<((u64, u64), usize, i64)> = vec![
            ((1, 2), 0, 1),
            ((2, 3), 1, 1),
            ((2, 3), 2, -1),
        ];
        batcher.insert(&mut input);

        let batch_ts = &[1, 2, 3];
        let mut lower = Antichain::from_elem(0);
        for i in batch_ts {
            let upper = Antichain::from_elem(*i);
            let (batch, _retained) = batcher.extract(upper.borrow());
            let description = Description::new(lower, upper.clone(), Antichain::from_elem(0));
            trace.insert(Span::new(description, batch.map(Into::into)));
            lower = upper;
        }
    }
    trace
}

#[test]
fn test_trace() {
    let mut trace = get_trace();

    let (mut cursor1, storage1) = cursor_list(trace.spans_through(AntichainRef::new(&[1])).unwrap().into_iter().filter_map(|b| b.inner).collect());
    let vec_1 = cursor1.to_vec(&storage1, |k| k.clone(), |v| v.clone());
    assert_eq!(vec_1, vec![((1, 2), vec![(0, 1)])]);

    let (mut cursor2, storage2) = cursor_list(trace.spans_through(AntichainRef::new(&[2])).unwrap().into_iter().filter_map(|b| b.inner).collect());
    let vec_2 = cursor2.to_vec(&storage2, |k| k.clone(), |v| v.clone());
    println!("--> {:?}", vec_2);
    assert_eq!(vec_2, vec![
               ((1, 2), vec![(0, 1)]),
               ((2, 3), vec![(1, 1)]),
    ]);

    let (mut cursor3, storage3) = cursor_list(trace.spans_through(AntichainRef::new(&[3])).unwrap().into_iter().filter_map(|b| b.inner).collect());
    let vec_3 = cursor3.to_vec(&storage3, |k| k.clone(), |v| v.clone());
    assert_eq!(vec_3, vec![
               ((1, 2), vec![(0, 1)]),
               ((2, 3), vec![(1, 1), (2, -1)]),
    ]);

    let batches = trace.spans_through(Antichain::new().borrow()).unwrap();
    let (mut cursor4, storage4) = cursor_list(batches.into_iter().filter_map(|b| b.inner).collect());
    let vec_4 = cursor4.to_vec(&storage4, |k| k.clone(), |v| v.clone());
    assert_eq!(vec_4, vec_3);
}

/// An idle trace that receives one span without updates per tick settles: each such span extends
/// the span before it, so no merge begins, the trace keeps one span, and its update is untouched.
#[test]
fn test_idle_trace_settles() {
    let op_info = OperatorInfo::new(0, 0, [].into());
    let mut trace = IntegerTrace::new(op_info, None, None);

    // Record the layers the exertion logic observes, without ever granting effort.
    let observed: Arc<Mutex<Vec<(usize, usize, usize)>>> = Arc::default();
    let observed_in_logic = Arc::clone(&observed);
    trace.set_exert_logic(Arc::new(move |layers| {
        observed_in_logic.lock().unwrap().extend_from_slice(layers);
        None
    }));

    let mut batcher = ValBatcher::<u64,u64,usize,i64>::new(None, 0);
    batcher.insert(&mut vec![((1, 2), 0, 1)]);
    let upper = Antichain::from_elem(1);
    let (batch, _retained) = batcher.extract(upper.borrow());
    let description = Description::new(Antichain::from_elem(0), upper, Antichain::from_elem(0));
    trace.insert(Span::new(description, batch.map(Into::into)));

    for time in 1 .. 100 {
        trace.set_logical_compaction(AntichainRef::new(&[time]));
        trace.set_physical_compaction(AntichainRef::new(&[time]));
        trace.insert(Span::empty(Antichain::from_elem(time), Antichain::from_elem(time + 1)));
        trace.set_physical_compaction(AntichainRef::new(&[time + 1]));
        observed.lock().unwrap().clear();
        trace.exert();
        let layers = std::mem::take(&mut *observed.lock().unwrap());
        if time > 10 {
            assert!(
                layers.iter().all(|(_, count, _)| *count < 2),
                "merge in progress at time {time}: {layers:?}",
            );
        }
    }

    let mut spans = Vec::new();
    trace.map_spans(|span| spans.push((span.lower().clone(), span.upper().clone(), span.has_updates())));
    assert_eq!(spans, vec![(Antichain::from_elem(0), Antichain::from_elem(100), true)]);
    let (mut cursor, storage) = cursor_list(trace.spans_through(Antichain::new().borrow()).unwrap().into_iter().filter_map(|b| b.inner).collect());
    let contents = cursor.to_vec(&storage, |k| k.clone(), |v| v.clone());
    assert_eq!(contents, vec![((1, 2), vec![(0, 1)])]);
}
