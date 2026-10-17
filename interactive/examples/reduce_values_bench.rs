//! Isolate a wave of Corgi reduce value reconciliation, after column presentation.
//!
//! cargo run --release -p interactive --example reduce_values_bench -- [keys=50000] [width=8] [rounds=11]
//!
//! Native columns and proxy presentations are prepared outside the timer. One separately
//! counted pass reports allocation/reallocation calls. Uses System and scalar u64 time.
use std::alloc::{GlobalAlloc, Layout, System};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use corgi::{Bounds, Value};
use differential_dataflow::operators::int_proxy::reduce::{
    ProxyReduceBackend, ReduceInstance, ReduceWindow,
};
use differential_dataflow::operators::int_proxy::KeyPosition;
use differential_dataflow::trace::{chunk::ChunkBatch, Description};
use interactive::corgi::{chunk::CorgiChunk, reduce::CorgiReduceBackend};
use interactive::ir::Reducer;
use timely::progress::Antichain;

struct CountingAllocator;
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
fn count() {
    if COUNTING.load(Ordering::Relaxed) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    }
}
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        System.realloc(ptr, layout, size)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

type Batch = Rc<ChunkBatch<CorgiChunk<u64, i64>>>;
type Prepared = (
    CorgiReduceBackend<u64>,
    Vec<i64>,
    Vec<usize>,
    Vec<(u64, i64)>,
    Vec<usize>,
    Vec<(u64, i64)>,
);

fn prepare(reducer: &Reducer, input: &[Batch], output: &[Batch], keys: usize) -> Prepared {
    let mut backend = CorgiReduceBackend::with_window(reducer.clone(), usize::MAX);
    let lower = Antichain::from_elem(0);
    backend.begin(Description::new(
        lower.clone(),
        Antichain::new(),
        lower.clone(),
    ));
    let instance = ReduceInstance {
        source_batches: &[],
        input_batches: input,
        output_batches: output,
        lower: lower.borrow(),
    };
    let mut window = ReduceWindow::default();
    let mut from = KeyPosition::Start;
    backend.next_window(&instance, &[], &mut from, &mut window);
    assert_eq!(from, KeyPosition::End);
    let mut ends = Vec::with_capacity(keys);
    for (i, row) in window.input.iter().enumerate() {
        if i + 1 == window.input.len() || row.0 .0 != window.input[i + 1].0 .0 {
            ends.push(i + 1);
        }
    }
    assert_eq!(ends.len(), keys);
    let in_values = window.input.iter().map(|r| (r.0 .1, r.2)).collect();
    let out_values = window.output.iter().map(|r| (r.0 .1, r.2)).collect();
    let out_ends = if output.is_empty() {
        vec![0; keys]
    } else {
        (1..=keys).collect()
    };
    (
        backend,
        (0..keys as i64).collect(),
        ends,
        in_values,
        out_ends,
        out_values,
    )
}

fn reconcile(prepared: &mut Prepared) -> (Vec<(u64, i64)>, Vec<usize>) {
    let (backend, keys, in_ends, input, out_ends, output) = prepared;
    backend.reduce_corrections(keys, in_ends, input, out_ends, output)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let keys: usize = args.next().map(|x| x.parse().unwrap()).unwrap_or(50_000);
    let width: usize = args.next().map(|x| x.parse().unwrap()).unwrap_or(8);
    let rounds: usize = args.next().map(|x| x.parse().unwrap()).unwrap_or(11);
    assert!(keys > 0 && width > 0 && rounds > 0);
    println!("reducer,values,history,keys,width,median_ns_per_key,allocation_calls,samples_ns");
    for compound in [false, true] {
        let leaf = Value::i64((0..keys).flat_map(|_| -(width as i64)..0).collect());
        let values = if compound {
            Value::Prod(vec![leaf.clone(), leaf])
        } else {
            leaf
        };
        let key_column = Value::i64(
            (0..keys)
                .flat_map(|k| std::iter::repeat_n(k as i64, width))
                .collect(),
        );
        let input = [Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
            key_column,
            values.clone(),
            std::iter::repeat_n(0, keys * width).collect(),
            vec![1; keys * width],
        )]))];
        for reducer in [Reducer::Count, Reducer::Min, Reducer::Collect] {
            let expected = match reducer {
                Reducer::Count => Value::i64(vec![width as i64; keys]),
                Reducer::Min => corgi::arrange::gather_lanes(
                    &[Some(&values)],
                    &vec![0; keys],
                    &(0..keys).map(|k| k * width).collect::<Vec<_>>(),
                ),
                Reducer::Collect => Value::List(
                    Bounds::offsets((1..=keys).map(|k| k * width).collect()),
                    Box::new(values.clone()),
                ),
                _ => unreachable!(),
            };
            let output = [Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
                Value::i64((0..keys as i64).collect()),
                expected.clone(),
                std::iter::repeat_n(0, keys).collect(),
                vec![1; keys],
            )]))];
            for history in [false, true] {
                let prior = if history { &output[..] } else { &[] };
                let mut samples = Vec::new();
                let mut allocations = 0;
                for round in 0..rounds + 2 {
                    let mut prepared = prepare(&reducer, &input, prior, keys);
                    if round == 1 {
                        ALLOCATIONS.store(0, Ordering::Relaxed);
                        COUNTING.store(true, Ordering::Relaxed);
                    }
                    let start = Instant::now();
                    let (corrections, ends) = reconcile(&mut prepared);
                    let elapsed = start.elapsed().as_nanos();
                    COUNTING.store(false, Ordering::Relaxed);
                    if round == 1 {
                        allocations = ALLOCATIONS.load(Ordering::Relaxed);
                    }
                    if round >= 2 {
                        samples.push(elapsed);
                    }
                    assert_eq!(corrections.len(), if history { 0 } else { keys });
                    assert_eq!(ends.len(), keys);
                    // Check actual emitted columns, outside the measured reconciliation.
                    if !history && round == 0 {
                        let records = corrections
                            .iter()
                            .enumerate()
                            .map(|(k, &(id, diff))| ((k as i64, id), 0, diff))
                            .collect::<Vec<_>>();
                        prepared.0.emit(&records);
                        let result = prepared.0.finish().unwrap();
                        assert_eq!(result.chunks.len(), 1);
                        for k in 0..keys {
                            assert!(corgi::arrange::compare_at(
                                result.chunks[0].vals(),
                                k,
                                &expected,
                                k
                            )
                            .is_eq());
                        }
                    }
                }
                let mut sorted = samples.clone();
                sorted.sort_unstable();
                println!(
                    "{reducer:?},{},{history},{keys},{width},{:.1},{allocations},{samples:?}",
                    if compound { "pair" } else { "int" },
                    sorted[rounds / 2] as f64 / keys as f64
                );
            }
        }
    }
}
