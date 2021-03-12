extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;

fn main() {

    let keys: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: u32 = std::env::args().nth(2).unwrap().parse().unwrap();

    let mode = std::env::args().any(|a| a == "new");

    if mode {
        println!("Running NEW arrangement");
    }
    else {
        println!("Running OLD arrangement");
    }

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut data_input, mut keys_input) = worker.dataflow(|scope| {

            use differential_dataflow::operators::arrange::Arrange;
            use differential_dataflow::trace::implementations::ord::{OrdKeySpine, ColKeySpine};

            let (data_input, data) = scope.new_collection::<String, isize>();
            let (keys_input, keys) = scope.new_collection::<String, isize>();

            if mode {
                let data = data.arrange::<ColKeySpine<_,_,_>>();
                let keys = keys.arrange::<ColKeySpine<_,_,_>>();

                use differential_dataflow::operators::JoinCore;
                keys.join_core(&data, |_k, &(), &()| Some(()))
                    .probe_with(&mut probe);
            }
            else {
                let data = data.arrange::<OrdKeySpine<_,_,_>>();
                let keys = keys.arrange::<OrdKeySpine<_,_,_>>();

                use differential_dataflow::operators::JoinCore;
                keys.join_core(&data, |_k, &(), &()| Some(()))
                    .probe_with(&mut probe);
            }

            (data_input, keys_input)
        });

        // Load up data in batches.
        let mut counter = 0;
        while counter < 10 * keys {
            for i in 0 .. size {
                let val = (counter + i) % keys;
                data_input.insert(format!("{:?}", val));
            }
            counter += size;
            data_input.advance_to(data_input.time() + 1);
            data_input.flush();
            keys_input.advance_to(keys_input.time() + 1);
            keys_input.flush();
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }
        println!("{:?}\tloading complete", timer.elapsed());

        let mut queries = 0;

        while queries < 10 * keys {
            for i in 0 .. size {
                let val = (queries + i) % keys;
                keys_input.insert(format!("{:?}", val));
            }
            queries += size;
            data_input.advance_to(data_input.time() + 1);
            data_input.flush();
            keys_input.advance_to(keys_input.time() + 1);
            keys_input.flush();
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }

        println!("{:?}\tqueries complete", timer.elapsed());

        loop {

        }

    }).unwrap();
}