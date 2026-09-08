//! File CHUNK + sparse join + standard DD count, with bounded input/output chunks.
#[path = "file_chunk_support/join.rs"]
mod join;
#[path = "file_chunk_support/storage.rs"]
mod storage;

use differential_dataflow::columnar::updates::UpdatesTyped;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::join::join_with_tactic;
use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};
use differential_dataflow::trace::Description;
use differential_dataflow::AsCollection;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;
use storage::{DiskChunk, Inner};
use timely::container::PushInto;
use timely::dataflow::operators::ToStream;
use timely::progress::Antichain;

fn rss_kib() -> usize {
    let result = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    std::str::from_utf8(&result.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn main() {
    let raw: Vec<_> = std::env::args().collect();
    let args: Vec<_> = raw.iter().filter(|s| !s.starts_with("--")).collect();
    let keys: u64 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(65_536);
    let fanout: u64 = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(16);
    let rounds: u64 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(8);
    let hot_count: u64 = args
        .get(4)
        .map(|s| s.parse().unwrap())
        .unwrap_or((keys / 100).max(1));
    let capacity: usize = args
        .get(5)
        .map(|s| s.parse().unwrap())
        .unwrap_or((hot_count * (fanout + 1) + 1) as usize);
    let nocache = raw.iter().any(|s| s == "--nocache");
    let clustered = raw.iter().any(|s| s == "--clustered");
    let moving = raw.iter().any(|s| s == "--moving");
    let profile = raw.iter().any(|s| s == "--profile");
    assert!(keys > 0 && fanout > 0 && hot_count > 0 && hot_count <= keys && rounds > 0);
    if raw.iter().any(|s| s == "--cursor-control") {
        cursor_control(keys, fanout, rounds, hot_count, nocache);
        return;
    }
    timely::execute_directly(move |worker| {
        let store = storage::install(nocache);
        let stats = Rc::clone(&store.borrow().stats);
        let start = Instant::now();
        let mut chunks = Vec::new();
        generate(keys, fanout, |c| chunks.push(DiskChunk::new(c).persist()));
        assert!(chunks.iter().all(DiskChunk::is_file));
        let batch = Rc::new(ChunkBatch::new(
            chunks,
            Description::new(
                Antichain::from_elem(0),
                Antichain::new(),
                Antichain::from_elem(0),
            ),
        ));
        store.borrow_mut().rotate();
        println!("build keys={keys} fanout={fanout} records={} hot={hot_count} capacity={capacity} nocache={nocache} elapsed_ms={} rss_kib={} file_bytes={} writes={}", keys * fanout, start.elapsed().as_millis(), rss_kib(), stats.live_bytes.get(), stats.writes.get());
        let trace = join::StaticTrace::new(batch);
        let calls = Rc::new(Cell::new(0));
        let mut input = InputSession::<u64, (u64, u64), i64>::new();
        let mut probe = timely::dataflow::operators::probe::Handle::new();
        let observed = Rc::new(RefCell::new(BTreeMap::<(u64, i64), isize>::new()));
        let output = Rc::clone(&observed);
        let mut tactic = join::Tactic::new(trace.clone(), capacity, Rc::clone(&calls));
        tactic.profile = profile;
        worker.dataflow::<u64, _, _>(|scope| {
            let cold = Arranged {
                stream: vec![Rc::clone(&trace.batch)].into_iter().to_stream(scope),
                trace,
            };
            let queries = input.to_collection(scope).arrange_by_key();
            let joined = join_with_tactic(cold, queries, "FileSparseJoin", tactic).as_collection();
            joined
                .count()
                .inspect(move |((key, count), _, diff)| {
                    let mut output = output.borrow_mut();
                    let weight = output.entry((*key, *count)).or_default();
                    *weight += *diff;
                    if *weight == 0 {
                        output.remove(&(*key, *count));
                    }
                })
                .probe_with(&mut probe);
        });
        for round in 0..rounds {
            // Change the set only after its previous membership has been retracted.
            let offset = if moving { round / 2 } else { 0 };
            let mut hot: Vec<_> = (0..hot_count)
                .map(|i| (if clustered { i } else { i * keys / hot_count } + offset) % keys)
                .collect();
            hot.sort_unstable();
            let before_reads = stats.reads.get();
            let before_bytes = stats.read_bytes.get();
            let before_writes = stats.writes.get();
            let start = Instant::now();
            let diff = if round % 2 == 0 { 1 } else { -1 };
            for &key in &hot {
                input.update((key, 0), diff);
            }
            input.advance_to(round + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            let expected: BTreeMap<_, _> = if diff == 1 {
                hot.iter().map(|&k| ((k, fanout as i64), 1)).collect()
            } else {
                BTreeMap::new()
            };
            assert_eq!(
                *observed.borrow(),
                expected,
                "wrong DD count at round {round}"
            );
            println!("round={round} elapsed_us={} rss_kib={} reads={} read_bytes={} writes={} active_keys={} calls={}", start.elapsed().as_micros(), rss_kib(), stats.reads.get() - before_reads, stats.read_bytes.get() - before_bytes, stats.writes.get() - before_writes, observed.borrow().len(), calls.get());
        }
        input.close();
        while !probe.done() {
            worker.step();
        }
        println!(
            "verified rounds={rounds} rss_kib={} file_bytes={} total_read_bytes={}",
            rss_kib(),
            stats.live_bytes.get(),
            stats.read_bytes.get()
        );
        storage::uninstall();
    });
}

fn generate(keys: u64, fanout: u64, mut consume: impl FnMut(Inner)) {
    let mut updates = UpdatesTyped::default();
    for key in 0..keys {
        for val in 0..fanout {
            updates.push_into(((key, val), 0u64, 1i64));
            if updates.len() == DiskChunk::TARGET {
                consume(Inner::from_trie(std::mem::take(&mut updates).consolidate()));
            }
        }
    }
    if updates.len() > 0 {
        consume(Inner::from_trie(updates.consolidate()));
    }
}

// Read-only control: existing ColChunk spill + existing cursor pins decoded chunks.
// It does less computation than the join, but uses the same file backend and keys.
fn cursor_control(keys: u64, fanout: u64, rounds: u64, hot_count: u64, nocache: bool) {
    use differential_dataflow::columnar::trace::spill;
    use differential_dataflow::trace::{Cursor, Navigable};
    use std::collections::VecDeque;
    let store = storage::install(nocache);
    let stats = Rc::clone(&store.borrow().stats);
    spill::install(
        0,
        Box::new(storage::BytesAdapter(Rc::clone(&store))),
        Default::default(),
    );
    let start = Instant::now();
    let mut chunks = VecDeque::new();
    generate(keys, fanout, |c| {
        Inner::settle(&mut VecDeque::from([c]), true, &mut chunks)
    });
    store.borrow_mut().rotate();
    let batch = ChunkBatch::new(
        chunks.into(),
        Description::new(
            Antichain::from_elem(0),
            Antichain::new(),
            Antichain::from_elem(0),
        ),
    );
    println!("build mode=cursor keys={keys} fanout={fanout} records={} hot={hot_count} nocache={nocache} elapsed_ms={} rss_kib={} file_bytes={} writes={}", keys * fanout, start.elapsed().as_millis(), rss_kib(), stats.live_bytes.get(), stats.writes.get());
    for round in 0..rounds {
        let start = Instant::now();
        let before_reads = stats.reads.get();
        let before_bytes = stats.read_bytes.get();
        let mut cursor = batch.cursor();
        let mut count = 0i64;
        for key in (0..hot_count).map(|i| i * keys / hot_count) {
            cursor.seek_key(&batch, &key);
            assert_eq!(*cursor.key(&batch), key);
            while cursor.val_valid(&batch) {
                cursor.map_times(&batch, |_, d| count += d);
                cursor.step_val(&batch);
            }
        }
        assert_eq!(count, (hot_count * fanout) as i64);
        println!(
            "round={round} elapsed_us={} rss_kib={} reads={} read_bytes={}",
            start.elapsed().as_micros(),
            rss_kib(),
            stats.reads.get() - before_reads,
            stats.read_bytes.get() - before_bytes
        );
    }
    println!("verified mode=cursor rounds={rounds} rss_kib={}", rss_kib());
    spill::uninstall();
    storage::uninstall();
}
