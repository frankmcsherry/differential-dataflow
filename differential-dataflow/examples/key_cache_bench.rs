//! Read-side cache benchmark. Run in release mode; args: keys queries capacity [workload].
//! Files fit in the OS cache: this measures forced paging, not cold disk latency.
//! All modes consume every requested update and check the same checksum.

use std::cell::Cell;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use timely::container::PushInto;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{frontier::AntichainRef, Antichain};

use differential_dataflow::columnar::trace::{spill, ColChunk};
use differential_dataflow::columnar::updates::UpdatesTyped;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::chunk::{settle_all, ChunkBatch, ChunkSpine};
use differential_dataflow::trace::wrappers::cached::CachedTrace;
use differential_dataflow::trace::{Cursor, Description, Navigable, Trace, TraceReader};

type Data = (u64, u64, u64, i64);
type Batch = Rc<ChunkBatch<ColChunk<Data>>>;

#[derive(Default)]
struct Io {
    reads: Cell<usize>,
    bytes: Cell<usize>,
}
struct Store {
    path: PathBuf,
    next: usize,
    io: Rc<Io>,
}
struct Source {
    path: PathBuf,
    io: Rc<Io>,
}
impl spill::BytesStore for Store {
    fn store(&mut self, bytes: &[u8]) -> Box<dyn spill::BytesSource> {
        let path = self.path.join(self.next.to_string());
        self.next += 1;
        std::fs::write(&path, bytes).unwrap();
        Box::new(Source {
            path,
            io: Rc::clone(&self.io),
        })
    }
}
impl spill::BytesSource for Source {
    fn load(&self) -> Vec<u8> {
        let bytes = std::fs::read(&self.path).unwrap();
        self.io.reads.set(self.io.reads.get() + 1);
        self.io.bytes.set(self.io.bytes.get() + bytes.len());
        bytes
    }
}
struct SpillGuard;
impl Drop for SpillGuard {
    fn drop(&mut self) {
        spill::uninstall();
    }
}

fn batch(keys: impl IntoIterator<Item = u64>, time: u64) -> Batch {
    let mut chunks = Vec::new();
    let mut rows = UpdatesTyped::<Data>::default();
    for key in keys {
        for val in 0..4 {
            rows.push_into(((key, val), time, 1));
        }
        if rows.len() >= 8192 {
            chunks.push(ColChunk::from_trie(std::mem::take(&mut rows).consolidate()));
        }
    }
    if rows.len() > 0 {
        chunks.push(ColChunk::from_trie(rows.consolidate()));
    }
    Rc::new(ChunkBatch::new(
        settle_all(chunks),
        Description::new(
            Antichain::from_elem(time),
            Antichain::from_elem(time + 1),
            Antichain::from_elem(0),
        ),
    ))
}

fn consume(batches: &[Batch], keys: &[u64]) -> u64 {
    let mut checksum = 0u64;
    for batch in batches {
        let mut cursor = batch.cursor();
        for key in keys {
            cursor.seek_key(batch, key);
            if cursor.get_key(batch) != Some(key) {
                continue;
            }
            while cursor.val_valid(batch) {
                let val = *cursor.val(batch);
                cursor.map_times(batch, |time, diff| {
                    checksum = checksum.wrapping_add(
                        (key.wrapping_mul(31) + val * 7 + time).wrapping_mul(*diff as u64),
                    );
                });
                cursor.step_val(batch);
            }
        }
    }
    black_box(checksum)
}

fn requests(kind: &str, count: usize, key_count: usize) -> Vec<Vec<u64>> {
    let domain = match kind {
        "hot64" => 64,
        "hot4096" => 4096,
        _ => key_count,
    };
    let mut rng = 0x123456789abcdefu64;
    let mut result: Vec<Vec<u64>> = Vec::new();
    for i in 0..count {
        if i > 0 && (kind == "repeat" || kind.starts_with("append")) {
            result.push(result[0].clone());
        } else {
            let mut keys = std::collections::BTreeSet::new();
            while keys.len() < 16 {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                keys.insert(((rng as usize % domain) * key_count / domain) as u64);
            }
            result.push(keys.into_iter().collect());
        }
    }
    result
}

fn run(
    base: &[Batch],
    requests: &[Vec<u64>],
    mode: &str,
    capacity: usize,
    workload: &str,
    io: &Io,
) -> u64 {
    let info = OperatorInfo::new(0, 0, [].into());
    let spine = ChunkSpine::new(info.clone(), None, None);
    let (reader, mut writer) = TraceAgent::new(spine, info, None);
    for b in base {
        writer.insert(Rc::clone(b), None);
    }
    let mut cache = CachedTrace::new(reader, if mode == "cached" { capacity } else { 0 });
    let mut times = Vec::new();
    let mut reads = 0;
    let mut bytes = 0;
    let mut checksum = 0u64;
    let mut peak_charge = 0;
    let mut peak_entries = 0;
    for (i, keys) in requests.iter().enumerate() {
        if workload.starts_with("append") {
            writer.insert(batch(keys.iter().copied(), (base.len() + i) as u64), None);
        }
        if workload == "append_compact" {
            cache.set_physical_compaction(AntichainRef::new(&[(base.len() + i) as u64]));
        }
        let before_reads = io.reads.get();
        let before_bytes = io.bytes.get();
        let start = Instant::now();
        let value = if mode == "cursor" {
            consume(
                &cache.batches_through(AntichainRef::new(&[])).unwrap(),
                keys,
            )
        } else {
            let selected = cache
                .batch_through_keys(AntichainRef::new(&[]), keys)
                .unwrap();
            consume(std::slice::from_ref(&selected), keys)
        };
        times.push(start.elapsed().as_nanos() as u64);
        checksum = checksum.wrapping_add(value);
        reads += io.reads.get() - before_reads;
        bytes += io.bytes.get() - before_bytes;
        peak_charge = peak_charge.max(cache.charge());
        peak_entries = peak_entries.max(cache.len());
    }
    let mean = times.iter().sum::<u64>() as f64 / times.len() as f64 / 1000.;
    times.sort_unstable();
    print!(
        "{mean:.3},{:.3},{:.3},{:.3},{:.1},{peak_charge},{peak_entries}",
        times[times.len() / 2] as f64 / 1000.,
        times[times.len() * 95 / 100] as f64 / 1000.,
        reads as f64 / times.len() as f64,
        bytes as f64 / times.len() as f64
    );
    checksum
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let key_count: usize = args.get(1).map(|x| x.parse().unwrap()).unwrap_or(65_536);
    let queries: usize = args.get(2).map(|x| x.parse().unwrap()).unwrap_or(256);
    let capacity: usize = args.get(3).map(|x| x.parse().unwrap()).unwrap_or(65_536);
    assert!(key_count >= 4096 && queries > 0);
    println!("storage,keys,queries,capacity,workload,round,mode,mean_us,p50_us,p95_us,reads_per_query,bytes_per_query,peak_charge,peak_entries");
    for storage in ["resident", "paged"] {
        let directory = tempfile::tempdir().unwrap();
        let io = Rc::new(Io::default());
        if storage == "paged" {
            spill::install(
                0,
                Box::new(Store {
                    path: directory.path().to_owned(),
                    next: 0,
                    io: Rc::clone(&io),
                }),
                Arc::new(spill::SpillStats::default()),
            );
        }
        let _guard = SpillGuard;
        let base: Vec<_> = (0..8)
            .map(|time| batch(0..key_count as u64, time))
            .collect();
        for kind in [
            "repeat",
            "hot64",
            "hot4096",
            "uniform",
            "append",
            "append_compact",
        ] {
            if args.get(4).is_some_and(|filter| filter != kind) {
                continue;
            }
            let queries = requests(kind, queries, key_count);
            let mut expected = None;
            for round in 0..2 {
                let modes: &[&str] = match (storage, round) {
                    ("resident", 0) => &["cursor", "uncached", "cached"],
                    ("resident", _) => &["cached", "uncached", "cursor"],
                    (_, 0) => &["uncached", "cached"],
                    _ => &["cached", "uncached"],
                };
                for &mode in modes {
                    print!(
                        "{storage},{key_count},{},{capacity},{kind},{round},{mode},",
                        queries.len()
                    );
                    let checksum = run(&base, &queries, mode, capacity, kind, &io);
                    println!();
                    assert_eq!(*expected.get_or_insert(checksum), checksum);
                }
            }
        }
    }
}
