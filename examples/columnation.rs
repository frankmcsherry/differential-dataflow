extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;

fn main() {

    let keys: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: u32 = std::env::args().nth(2).unwrap().parse().unwrap();

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let mut input = worker.dataflow(|scope| {

            use timely::dataflow::operators::Probe;
            use differential_dataflow::operators::arrange::Arrange;
            use differential_dataflow::trace::implementations::ord::{OrdKeySpine, ColKeySpine};

            let (keys_input, keys) = scope.new_collection();
            // keys.arrange::<ColKeySpine<_,_,_>>()
            keys.arrange::<OrdKeySpine<_,_,_>>()
                .stream
                .probe_with(&mut probe);

            keys_input
        });

        let mut counter = 0;
        while counter < 10 * keys {
            for i in 0 .. size {
                let val = (counter + i) % keys;
                let array: Vec<_> = (0 .. 1024).map(|_| val).collect();
                input.insert(array);
            }
            counter += size;
            input.advance_to(input.time() + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        println!("{:?}\tcomplete", timer.elapsed());

    }).unwrap();
}