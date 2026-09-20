//! Selected reads from a file-backed columnar trace, with fetch counts.
//!
//! Run with `cargo run -p differential-dataflow --example key_cache`.

use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use timely::container::PushInto;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{frontier::AntichainRef, Antichain};

use differential_dataflow::columnar::trace::{spill, ColChunk};
use differential_dataflow::columnar::updates::UpdatesTyped;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::chunk::{settle_all, ChunkBatch, ChunkSpine};
use differential_dataflow::trace::wrappers::cached::CachedTrace;
use differential_dataflow::trace::{BatchReader, Description, Trace};

type Data = (u64, u64, u64, i64);

struct FileStore {
    directory: PathBuf,
    next: usize,
    loads: Rc<Cell<usize>>,
}
struct FileSource {
    path: PathBuf,
    loads: Rc<Cell<usize>>,
}

impl spill::BytesStore for FileStore {
    fn store(&mut self, bytes: &[u8]) -> Box<dyn spill::BytesSource> {
        let path = self.directory.join(format!("{}.chunk", self.next));
        self.next += 1;
        std::fs::write(&path, bytes).unwrap();
        Box::new(FileSource {
            path,
            loads: Rc::clone(&self.loads),
        })
    }
}
impl spill::BytesSource for FileSource {
    fn load(&self) -> Vec<u8> {
        self.loads.set(self.loads.get() + 1);
        std::fs::read(&self.path).unwrap()
    }
}
struct SpillGuard;
impl Drop for SpillGuard {
    fn drop(&mut self) {
        spill::uninstall();
    }
}

fn batch(time: u64) -> Rc<ChunkBatch<ColChunk<Data>>> {
    let mut updates = UpdatesTyped::default();
    for key in 0..16_384u64 {
        updates.push_into(((key, 0), time, 1));
    }
    let chunks = settle_all([ColChunk::from_trie(updates.consolidate())]);
    Rc::new(ChunkBatch::new(
        chunks,
        Description::new(
            Antichain::from_elem(time),
            Antichain::from_elem(time + 1),
            Antichain::from_elem(0),
        ),
    ))
}

fn main() {
    let directory = tempfile::tempdir().unwrap();
    let loads = Rc::new(Cell::new(0));
    spill::install(
        0,
        Box::new(FileStore {
            directory: directory.path().to_owned(),
            next: 0,
            loads: Rc::clone(&loads),
        }),
        Arc::new(spill::SpillStats::default()),
    );
    let _guard = SpillGuard;

    let info = OperatorInfo::new(0, 0, [].into());
    let spine = ChunkSpine::new(info.clone(), None, None);
    let (reader, mut writer) = TraceAgent::new(spine, info, None);
    writer.insert(batch(0), None);
    writer.insert(batch(1), None);
    let mut cache = CachedTrace::new(reader, 1024);
    let keys = [1, 9000];

    for phase in ["cold", "repeat", "new batch", "evicted"] {
        if phase == "new batch" {
            writer.insert(batch(2), None);
        }
        if phase == "evicted" {
            cache.clear();
        }
        let before = loads.get();
        let result = cache
            .batch_through_keys(AntichainRef::new(&[]), &keys)
            .unwrap();
        println!(
            "{phase:>9}: {} file reads, {} updates in {} cached chunks, upper {:?}",
            loads.get() - before,
            result.len(),
            result.chunks.len(),
            result.upper().elements()
        );
        assert!(result
            .chunks
            .iter()
            .all(|c| matches!(c, ColChunk::Resident(_))));
    }
}
