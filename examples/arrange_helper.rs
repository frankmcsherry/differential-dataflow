extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::rc::Rc;
use std::cell::RefCell;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;

fn main() {

    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args().skip(1), move |worker| {

        // The plan is to accept a stream of the form:
        //      (key, Option<val>, time)
        // The intent is that subsequent tuples overwrite the previous values.
        // Our job is to translate this stream into a differential collection
        // whose records have the form
        //      ((key, val), time, diff)
        //
        // Plan is to build a sequence operators:
        //
        //  input => arrange => NEWOP => arrange => probe
        //
        // The first `arrange` isn't actually going to maintain an arrangement.
        // It will really just produce batches of updates.

        type Key = String;
        type Val = String;

        let mut input = timely::dataflow::InputHandle::<usize, (Key, Option<Val>, usize)>::new();
        let mut probe = timely::dataflow::ProbeHandle::new();

        // create a dataflow managing an ever-changing edge collection.
    	worker.dataflow::<usize,_,_>(|scope| {

            use timely::dataflow::operators::{Input, Inspect, Map, Probe};

            use differential_dataflow::operators::arrange::TraceAgent;
            use differential_dataflow::trace::implementations::ord::OrdValSpine;

            // Initially empty shared reference to the resulting trace handle.
            let shared1: Rc<RefCell<Option<TraceAgent<OrdValSpine<Key, Val, usize, isize>>>>> = Rc::new(RefCell::new(None));
            let shared2 = shared1.clone();

            // Produce an arrangement of the correctly maintained
            // (key, val) pairs, reflecting the input sequence of
            // insert, update, delete statements.
            let arrangement =
            scope
                .input_from(&mut input)
                .map(|(key, val, time)| ((key, val), time, 1))
                .as_collection()
                .arrange_by_key()
                .stream
                // Our exciting new operator!!!
                .flat_map({
                    let mut queue = Vec::new();
                    let mut shared_upper = timely::progress::frontier::Antichain::new();
                    move |batch| {
                        // Each received batch, assumed in order, needs
                        // to await shared1's upper bound matches the
                        // lower bound of the batch.

                        // Enqueue the batch, because `shared1` might
                        // not yet be ready to process it.
                        queue.push(batch);

                        use differential_dataflow::trace::TraceReader;
                        use differential_dataflow::trace::BatchReader;
                        use differential_dataflow::trace::Cursor;

                        // Advance `shared_upper` to track the *current*
                        // state of shared1.
                        shared1.borrow_mut().as_mut().unwrap().read_upper(&mut shared_upper);
                        shared1.borrow_mut().as_mut().unwrap().advance_by(shared_upper.elements());
                        shared1.borrow_mut().as_mut().unwrap().distinguish_since(shared_upper.elements());

                        // Determine if we are able to drain the queue.
                        let drain_queue = queue.first().map(|b| b.lower() == shared_upper.elements()).unwrap_or(false);

                        let mut output = Vec::new();

                        if drain_queue {

                            // collect the contents of the queue.
                            let mut input_cursors = Vec::new();
                            let mut input_storage = Vec::new();
                            for batch in queue.drain(..) {
                                // shared_upper = batch.upper().clone();
                                let cursor = batch.cursor();
                                input_cursors.push(cursor);
                                input_storage.push(batch);
                            }
                            use differential_dataflow::trace::cursor::CursorList;
                            let mut input_cursor = CursorList::new(input_cursors, &input_storage[..]);

                            let mut trace_borrow = shared1.borrow_mut();
                            let (mut trace_cursor, trace_storage) = trace_borrow.as_mut().unwrap().cursor();

                            while let Some(key) = input_cursor.get_key(&input_storage) {

                                // Order sequence of updates by time, then by value.
                                let mut val_times = Vec::new();
                                while let Some(val) = input_cursor.get_val(&input_storage) {
                                    input_cursor.map_times(&input_storage, |time, _diff| val_times.push((time.clone(), val.clone())));
                                    input_cursor.step_val(&input_storage);
                                }
                                val_times.sort();

                                // Determine the initial value associated with the key.
                                let mut current_val: Option<Val> = None;
                                trace_cursor.seek_key(&trace_storage, key);
                                if trace_cursor.get_key(&trace_storage) == Some(key) {
                                    while let Some(val) = trace_cursor.get_val(&trace_storage) {
                                        let mut count = 0;
                                        trace_cursor.map_times(&trace_storage, |_time, diff| {
                                            count += diff;
                                        });
                                        assert!(count == 0 || count == 1);
                                        if count == 1 {
                                            assert!(current_val.is_none());
                                            current_val = Some(val.clone());
                                        }

                                        trace_cursor.step_val(&trace_storage);
                                    }
                                }

                                // For each update, in time order, apply the correct changes.
                                for (time, val) in val_times {
                                    if let Some(prev) = current_val {
                                        output.push(((key.clone(), prev), time.clone(), -1));
                                    }
                                    if let Some(next) = val.clone() {
                                        output.push(((key.clone(), next), time.clone(), 1));
                                    }
                                    current_val = val;
                                }

                                input_cursor.step_key(&input_storage);
                            }
                        }

                        // Yield the produced updates as our output.
                        output
                    }
                })
                .inspect(|x| println!("{:?}", x))
                .as_collection()
                .arrange_by_key();


            // Share acces to the trace back to the map operator.
            *shared2.borrow_mut() = Some(arrangement.trace);
            arrangement.stream.probe_with(&mut probe);
        });

        input.send(("frank".to_string(), Some("mcsherry".to_string()), 3));
        input.advance_to(4);
        while probe.less_than(input.time()) { worker.step(); }

        input.send(("frank".to_string(), Some("zappa".to_string()), 4));
        input.advance_to(5);
        while probe.less_than(input.time()) { worker.step(); }

        input.send(("frank".to_string(), None, 5));
        input.advance_to(9);
        while probe.less_than(input.time()) { worker.step(); }

        input.send(("frank".to_string(), Some("oz".to_string()), 9));
        input.advance_to(10);
        while probe.less_than(input.time()) { worker.step(); }

        input.send(("frank".to_string(), None, 15));
        input.close();

    }).unwrap();
}
