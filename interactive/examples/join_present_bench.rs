//! Isolate Corgi join presentation over already-built native columns.
//!
//! cargo run --release -p interactive --example join_present_bench -- [keys=50000] [rounds=9]
//!
//! This measures an `advance` pass including bridge checks, not sorting/ingestion,
//! proxy matching, `cross`, or a graph query.
//! One allocation-counting pass is separate from warm timing passes. Uses the system allocator.
use std::alloc::{GlobalAlloc, Layout, System};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use corgi::Value;
use differential_dataflow::operators::int_proxy::{JoinInstance, KeyPosition, ProxyJoinBackend};
use differential_dataflow::trace::chunk::ChunkBatch;
use interactive::corgi::chunk::CorgiChunk;
use interactive::corgi::join::CorgiJoinBackend;
use interactive::ir::Term;

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

fn batch(keys: usize, compound: bool) -> Batch {
    let width = 2;
    let k = Value::i64(
        (0..keys)
            .flat_map(|k| std::iter::repeat_n(k as i64, width))
            .collect(),
    );
    let v = Value::i64((0..keys).flat_map(|_| 0..width as i64).collect());
    let v = if compound {
        Value::Prod(vec![v.clone(), v])
    } else {
        v
    };
    Rc::new(ChunkBatch::new(vec![CorgiChunk::from_columns(
        k,
        v,
        std::iter::repeat_n(0, keys * width).collect(),
        vec![1; keys * width],
    )]))
}

fn present(instance: &JoinInstance<u64, Batch, Batch>, keys: usize, chunks: usize) {
    let mut backend = CorgiJoinBackend::new(Term::Var(0), Term::Var(1), None);
    let (mut left, mut right) = (Vec::new(), Vec::new());
    let mut from = KeyPosition::Start;
    let (mut rows, mut diffs) = (0, 0);
    while from != KeyPosition::End {
        let before = from;
        left.clear();
        right.clear();
        backend.advance(instance, &mut from, &mut left, &mut right);
        assert!(from > before);
        assert_eq!(left, right);
        rows += left.len();
        diffs += left.iter().map(|r| r.2).sum::<i64>();
    }
    assert_eq!(rows, keys * 2);
    assert_eq!(diffs, (keys * 2 * chunks) as i64);
}

fn main() {
    let mut args = std::env::args().skip(1);
    let keys = args
        .next()
        .map(|x| x.parse().expect("integer key count"))
        .unwrap_or(50_000);
    let rounds = args
        .next()
        .map(|x| x.parse().expect("integer round count"))
        .unwrap_or(9);
    assert!(keys > 0 && rounds > 0);
    println!("values,chunks,keys,median_ns_per_key,allocation_calls,allocations_per_key");
    for compound in [false, true] {
        for chunks in [1, 4] {
            // Distinct chunks with equal contents exercise cross-chunk cancellation/identity.
            let batches: Vec<_> = (0..chunks).map(|_| batch(keys, compound)).collect();
            let instance = JoinInstance {
                batches0: batches.clone(),
                batches1: batches,
                lower: 0,
            };
            present(&instance, keys, chunks);
            ALLOCATIONS.store(0, Ordering::Relaxed);
            COUNTING.store(true, Ordering::Relaxed);
            present(&instance, keys, chunks);
            COUNTING.store(false, Ordering::Relaxed);
            let allocations = ALLOCATIONS.load(Ordering::Relaxed);
            let mut times = Vec::with_capacity(rounds);
            for _ in 0..rounds {
                let start = Instant::now();
                present(&instance, keys, chunks);
                times.push(start.elapsed().as_nanos());
            }
            times.sort_unstable();
            println!(
                "{},{chunks},{keys},{:.1},{allocations},{:.4}",
                if compound { "pair" } else { "int" },
                times[rounds / 2] as f64 / keys as f64,
                allocations as f64 / keys as f64
            );
        }
    }
}
