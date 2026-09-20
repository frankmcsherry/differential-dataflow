use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;

use timely::container::PushInto;
use timely::progress::{frontier::AntichainRef, Antichain, Timestamp};
use timely::PartialOrder;

use differential_dataflow::columnar::trace::spill::{self, BytesSource, BytesStore, SpillStats};
use differential_dataflow::columnar::trace::ColChunk;
use differential_dataflow::columnar::updates::UpdatesTyped;
use differential_dataflow::trace::chunk::vec::VecChunk;
use differential_dataflow::trace::chunk::{is_graded, settle_all, Chunk, ChunkBatch, KeyedChunk};
use differential_dataflow::trace::wrappers::cached::CachedTrace;
use differential_dataflow::trace::{BatchReader, Description, TraceReader};

// A shared append-only source whose batch representation tests can compact.
// Like Spine, batches_through omits empty batches.
struct Reader<C: Chunk> {
    batches: Rc<RefCell<Vec<Rc<ChunkBatch<C>>>>>,
    logical: Antichain<C::Time>,
    physical: Antichain<C::Time>,
}

impl<C: Chunk> Reader<C> {
    fn new(batches: Vec<Rc<ChunkBatch<C>>>) -> Self {
        Self {
            batches: Rc::new(RefCell::new(batches)),
            logical: Antichain::from_elem(C::Time::minimum()),
            physical: Antichain::from_elem(C::Time::minimum()),
        }
    }
}

impl<C: Chunk + 'static> TraceReader for Reader<C> {
    type Time = C::Time;
    type Batch = Rc<ChunkBatch<C>>;
    fn batches_through(&mut self, upper: AntichainRef<C::Time>) -> Option<Vec<Self::Batch>> {
        let mut result = Vec::new();
        for batch in self.batches.borrow().iter() {
            if batch.is_empty() {
                continue;
            }
            if PartialOrder::less_equal(&batch.upper().borrow(), &upper) {
                result.push(Rc::clone(batch));
            } else if !PartialOrder::less_equal(&upper, &batch.lower().borrow()) {
                return None;
            }
        }
        Some(result)
    }
    fn set_logical_compaction(&mut self, f: AntichainRef<C::Time>) {
        self.logical = f.to_owned();
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, C::Time> {
        self.logical.borrow()
    }
    fn set_physical_compaction(&mut self, f: AntichainRef<C::Time>) {
        self.physical = f.to_owned();
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, C::Time> {
        self.physical.borrow()
    }
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
        self.batches.borrow().iter().for_each(f);
    }
}

type Rows<T = u64> = Vec<((u64, u64), T, i64)>;
type VChunk<T = u64> = VecChunk<u64, u64, T, i64>;

fn vec_chunk<T: Timestamp + differential_dataflow::lattice::Lattice>(rows: Rows<T>) -> VChunk<T> {
    let mut chunk = VChunk::default();
    for row in rows {
        chunk.push_into(row);
    }
    chunk
}

fn batch<C: Chunk>(chunks: Vec<C>, lower: C::Time, upper: C::Time) -> Rc<ChunkBatch<C>> {
    Rc::new(ChunkBatch::new(
        chunks,
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(C::Time::minimum()),
        ),
    ))
}

fn rows(batch: &ChunkBatch<VChunk>) -> Rows {
    batch
        .chunks
        .iter()
        .flat_map(|c| c.as_slice().iter().cloned())
        .collect()
}

#[test]
fn preserves_history_and_reads_only_requested_keys() {
    let source = Reader::new(vec![
        batch(vec![vec_chunk(vec![((1, 0), 0, 1), ((2, 0), 0, 1)])], 0, 1),
        batch(vec![vec_chunk(vec![((1, 0), 1, -1), ((3, 0), 1, 1)])], 1, 2),
    ]);
    let mut cache = CachedTrace::new(source, 100);
    let first = cache
        .batch_through_keys(AntichainRef::new(&[1]), &[1])
        .unwrap();
    assert_eq!(rows(&first), vec![((1, 0), 0, 1)]);
    let second = cache
        .batch_through_keys(AntichainRef::new(&[2]), &[3, 1, 1])
        .unwrap();
    assert_eq!(
        rows(&second),
        vec![((1, 0), 0, 1), ((1, 0), 1, -1), ((3, 0), 1, 1)]
    );
    assert_eq!(second.description().since(), &Antichain::from_elem(0));
    assert!(is_graded(&second.chunks));
    assert_eq!(cache.len(), 1, "new selection subsumes the first");
    let hit = cache
        .batch_through_keys(AntichainRef::new(&[2]), &[1, 3])
        .unwrap();
    assert!(Rc::ptr_eq(&second, &hit));
    // Going back in coverage must not expose the cached suffix.
    let earlier = cache
        .batch_through_keys(AntichainRef::new(&[1]), &[1])
        .unwrap();
    assert_eq!(rows(&earlier), rows(&first));
    assert_eq!(
        cache
            .batches_through(AntichainRef::new(&[2]))
            .unwrap()
            .iter()
            .map(|b| b.len())
            .sum::<usize>(),
        4
    );
}

struct MemoryStore(Rc<Cell<usize>>);
struct MemorySource(Vec<u8>, Rc<Cell<usize>>);
impl BytesStore for MemoryStore {
    fn store(&mut self, bytes: &[u8]) -> Box<dyn BytesSource> {
        Box::new(MemorySource(bytes.to_vec(), Rc::clone(&self.0)))
    }
}
impl BytesSource for MemorySource {
    fn load(&self) -> Vec<u8> {
        self.1.set(self.1.get() + 1);
        self.0.clone()
    }
}
struct SpillGuard;
impl Drop for SpillGuard {
    fn drop(&mut self) {
        spill::uninstall();
    }
}

fn paged(keys: std::ops::Range<u64>, time: u64) -> Rc<ChunkBatch<ColChunk<(u64, u64, u64, i64)>>> {
    let mut trie = UpdatesTyped::default();
    for key in keys {
        trie.push_into(((key, 0u64), time, 1i64));
    }
    batch(
        settle_all([ColChunk::from_trie(trie.consolidate())]),
        time,
        time + 1,
    )
}

#[test]
fn paged_reads_reuse_overlapping_selections_and_fetch_only_new_batches() {
    let loads = Rc::new(Cell::new(0));
    spill::install(
        0,
        Box::new(MemoryStore(Rc::clone(&loads))),
        Arc::new(SpillStats::default()),
    );
    let _guard = SpillGuard;
    use differential_dataflow::operators::arrange::TraceAgent;
    use differential_dataflow::trace::{chunk::ChunkSpine, Trace};
    use timely::dataflow::operators::generic::OperatorInfo;
    let info = OperatorInfo::new(0, 0, [].into());
    let spine = ChunkSpine::new(info.clone(), None, None);
    let (source, mut writer) = TraceAgent::new(spine, info, None);
    writer.insert(paged(0..32, 0), None);
    writer.insert(paged(0..32, 1), None);
    let mut cache = CachedTrace::new(source, 1000);
    let first = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1, 3])
        .unwrap();
    assert_eq!(loads.get(), 2);
    assert_eq!(first.len(), 4);
    assert_eq!(first.upper(), &Antichain::from_elem(2));
    assert!(first
        .chunks
        .iter()
        .all(|c| matches!(c, ColChunk::Resident(_))));
    let hit = cache
        .batch_through_keys(AntichainRef::new(&[]), &[3, 1])
        .unwrap();
    assert!(Rc::ptr_eq(&first, &hit));
    assert_eq!(loads.get(), 2);

    let overlap = cache
        .batch_through_keys(AntichainRef::new(&[]), &[3, 5])
        .unwrap();
    assert_eq!(overlap.len(), 4);
    assert_eq!(
        loads.get(),
        4,
        "missing key 5 reads both source bodies again; neither was pinned"
    );
    writer.insert(paged(0..32, 2), None);
    let extended = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1, 3, 5])
        .unwrap();
    assert_eq!(extended.len(), 9);
    let got: Vec<_> = extended
        .chunks
        .iter()
        .flat_map(|chunk| {
            let ColChunk::Resident(trie) = chunk else {
                panic!("cache spilled")
            };
            trie.iter()
                .map(|(k, v, t, d)| ((*k, *v), *t, *d))
                .collect::<Vec<_>>()
        })
        .collect();
    let expected: Rows = [1, 3, 5]
        .into_iter()
        .flat_map(|k| (0..3).map(move |t| ((k, 0), t, 1)))
        .collect();
    assert_eq!(got, expected);
    assert_eq!(
        loads.get(),
        5,
        "overlapping entries share one fetch of the suffix"
    );
    assert_eq!(extended.upper(), &Antichain::from_elem(3));
    assert_eq!(cache.len(), 1);

    // Resident bounds reject out-of-range keys without loading any body.
    assert!(cache
        .batch_through_keys(AntichainRef::new(&[]), &[99])
        .unwrap()
        .is_empty());
    assert_eq!(loads.get(), 5);
    writer.insert(paged(99..100, 3), None);
    assert_eq!(
        cache
            .batch_through_keys(AntichainRef::new(&[]), &[99])
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        loads.get(),
        6,
        "an absent key is complete only through its cached upper"
    );
    cache.clear();
    cache
        .batch_through_keys(AntichainRef::new(&[2]), &[1])
        .unwrap();
    assert_eq!(
        loads.get(),
        8,
        "clearing releases the only retained copies of selected source data"
    );
}

#[test]
fn compaction_crossing_a_cached_upper_does_not_double_count() {
    let source = Reader::new(vec![batch(vec![vec_chunk(vec![((1, 0), 0, 1)])], 0, 1)]);
    let writer = Rc::clone(&source.batches);
    let mut cache = CachedTrace::new(source, 100);
    cache
        .batch_through_keys(AntichainRef::new(&[]), &[1])
        .unwrap();
    // Simulate a merged batch in which source times have also advanced.
    *writer.borrow_mut() = vec![Rc::new(ChunkBatch::new(
        vec![vec_chunk(vec![((1, 0), 2, 2)])],
        Description::new(
            Antichain::from_elem(0),
            Antichain::from_elem(2),
            Antichain::from_elem(2),
        ),
    ))];
    cache.set_logical_compaction(AntichainRef::new(&[2]));
    cache.set_physical_compaction(AntichainRef::new(&[2]));
    let result = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1])
        .unwrap();
    assert_eq!(rows(&result), vec![((1, 0), 2, 2)]);
    assert_eq!(result.description().since(), &Antichain::from_elem(2));
    assert!(cache
        .batch_through_keys(AntichainRef::new(&[1]), &[1])
        .is_none());
}

#[test]
fn a_vanished_batch_can_cancel_a_cached_prefix() {
    let source = Reader::new(vec![batch(vec![vec_chunk(vec![((1, 0), 0, 1)])], 0, 1)]);
    let writer = Rc::clone(&source.batches);
    let mut cache = CachedTrace::new(source, 100);
    cache
        .batch_through_keys(AntichainRef::new(&[]), &[1])
        .unwrap();
    // A later -1 and logical compaction cancel the entire source batch. The
    // reader omits this empty batch, so its descriptions alone cannot reveal
    // that the cached cut was crossed.
    *writer.borrow_mut() = vec![batch(Vec::new(), 0, 2)];
    cache.set_logical_compaction(AntichainRef::new(&[2]));
    cache.set_physical_compaction(AntichainRef::new(&[2]));
    let result = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1])
        .unwrap();
    assert!(result.is_empty());
    assert_eq!(result.description().since(), &Antichain::from_elem(2));
}

#[test]
fn capacity_accounts_for_absent_keys_and_bypasses_oversized_results() {
    let source = Reader::new(vec![batch(vec![vec_chunk(vec![((1, 0), 0, 1)])], 0, 1)]);
    let mut cache = CachedTrace::new(source, 4);
    let absent = cache
        .batch_through_keys(AntichainRef::new(&[]), &[2])
        .unwrap();
    assert_eq!(cache.charge(), 2);
    cache
        .batch_through_keys(AntichainRef::new(&[]), &[3])
        .unwrap();
    assert_eq!(cache.charge(), 4);
    cache
        .batch_through_keys(AntichainRef::new(&[]), &[4])
        .unwrap();
    assert_eq!(cache.len(), 1, "overflow flushes the cache");
    assert_eq!(cache.charge(), 2);
    let large = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1, 2, 3, 4, 5])
        .unwrap();
    assert_eq!(rows(&large), vec![((1, 0), 0, 1)]);
    assert_eq!(
        cache.charge(),
        2,
        "oversized requests do not evict useful entries"
    );
    cache.set_capacity(0);
    assert!(cache.is_empty());
    assert!(absent.is_empty(), "returned batches outlive eviction");
    cache
        .batch_through_keys(AntichainRef::new(&[]), &[1])
        .unwrap();
    assert_eq!(cache.charge(), 0);
    assert!(cache
        .batch_through_keys(AntichainRef::new(&[]), &[])
        .unwrap()
        .is_empty());
}

// Deliberately has no Navigable or Cursor implementation.
#[derive(Clone)]
struct Opaque<C>(C);
impl<C: Chunk> Chunk for Opaque<C> {
    type Time = C::Time;
    const TARGET: usize = C::TARGET;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn merge(a: &mut VecDeque<Self>, b: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let mut aa = a.drain(..).map(|c| c.0).collect();
        let mut bb = b.drain(..).map(|c| c.0).collect();
        let mut oo = VecDeque::new();
        C::merge(&mut aa, &mut bb, &mut oo);
        a.extend(aa.into_iter().map(Self));
        b.extend(bb.into_iter().map(Self));
        out.extend(oo.into_iter().map(Self));
    }
    fn extract(
        _: &mut VecDeque<Self>,
        _: AntichainRef<C::Time>,
        _: &mut Antichain<C::Time>,
        _: &mut VecDeque<Self>,
        _: &mut VecDeque<Self>,
    ) {
        panic!("cache must not extract by timestamp");
    }
    fn advance(_: &mut VecDeque<Self>, _: AntichainRef<C::Time>, _: bool, _: &mut VecDeque<Self>) {
        panic!("cache must not advance timestamps");
    }
    fn settle(_: &mut VecDeque<Self>, _: bool, _: &mut VecDeque<Self>) {
        panic!("cache must not invoke storage's spill policy");
    }
}
impl<C: KeyedChunk> KeyedChunk for Opaque<C> {
    type Key = C::Key;
    fn select_keys(chunks: &[Self], keys: &[Self::Key]) -> Vec<Self> {
        C::select_keys(
            &chunks.iter().map(|c| c.0.clone()).collect::<Vec<_>>(),
            keys,
        )
        .into_iter()
        .map(Self)
        .collect()
    }
}

#[test]
fn cursor_free_chunks_support_antichain_coverage() {
    use timely::order::Product;
    let t = Product::new;
    let minimum = Antichain::from_elem(t(0u64, 0u64));
    let middle = Antichain::from(vec![t(1, 0), t(0, 1)]);
    let upper = Antichain::from(vec![t(2, 0), t(0, 2)]);
    let first = Rc::new(ChunkBatch::new(
        vec![Opaque(vec_chunk(vec![((1, 0), t(0, 0), 1)]))],
        Description::new(minimum.clone(), middle.clone(), minimum.clone()),
    ));
    let second = Rc::new(ChunkBatch::new(
        vec![Opaque(vec_chunk(vec![
            ((1, 0), t(0, 1), -1),
            ((1, 0), t(1, 0), 1),
        ]))],
        Description::new(middle.clone(), upper.clone(), minimum),
    ));
    let mut cache = CachedTrace::new(Reader::new(vec![first, second]), 100);
    cache.batch_through_keys(middle.borrow(), &[1]).unwrap();
    let result = cache.batch_through_keys(upper.borrow(), &[1]).unwrap();
    let got: Vec<_> = result
        .chunks
        .iter()
        .flat_map(|c| c.0.as_slice().iter().cloned())
        .collect();
    assert_eq!(
        got,
        vec![
            ((1, 0), t(0, 0), 1),
            ((1, 0), t(0, 1), -1),
            ((1, 0), t(1, 0), 1)
        ]
    );
    assert_eq!(result.upper(), &upper);
}

#[test]
fn large_keys_straddle_chunks_and_pack_without_losing_history() {
    let expected: Rows = (0..3 * VChunk::<u64>::TARGET as u64)
        .map(|t| ((7, 0), t, 1))
        .collect();
    let chunks = expected
        .chunks(37)
        .map(|rows| vec_chunk(rows.to_vec()))
        .collect();
    let mut cache = CachedTrace::new(
        Reader::new(vec![batch(chunks, 0, expected.len() as u64)]),
        usize::MAX,
    );
    let result = cache
        .batch_through_keys(AntichainRef::new(&[]), &[7])
        .unwrap();
    assert_eq!(rows(&result), expected);
    assert_eq!(result.chunks.len(), 3);
    assert!(is_graded(&result.chunks));
}

#[test]
fn columnar_selection_copies_string_keys_and_straddling_value_histories() {
    type Data = (String, String, u64, i64);
    let mut all = Vec::new();
    for key in ["alpha", "middle", "omega"] {
        for val in ["first", "last"] {
            for time in 0..3000u64 {
                all.push(((key.to_owned(), val.to_owned()), time, 1i64));
            }
        }
    }
    let chunks = all
        .chunks(83)
        .map(|rows| {
            ColChunk::from_trie(UpdatesTyped::<Data>::form(
                rows.iter()
                    .map(|((k, v), t, d)| (k.as_bytes(), v.as_bytes(), t, d)),
            ))
        })
        .collect();
    let source = batch(chunks, 0, 3000);
    let result = source.select_keys(&["alpha".to_owned(), "omega".to_owned()]);
    assert_eq!(result.description(), source.description());
    assert!(is_graded(&result.chunks));
    assert_eq!(result.chunks.len(), 2);
    let got: Vec<_> = result
        .chunks
        .iter()
        .flat_map(|chunk| {
            let ColChunk::Resident(trie) = chunk else {
                panic!("selection spilled")
            };
            trie.iter()
                .map(|(k, v, t, d)| {
                    (
                        (
                            <String as columnar::Columnar>::into_owned(k),
                            <String as columnar::Columnar>::into_owned(v),
                        ),
                        *t,
                        *d,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    all.retain(|((k, _), _, _)| k != "middle");
    assert_eq!(got, all);
}

#[test]
fn wraps_a_real_chunk_spine() {
    use differential_dataflow::trace::chunk::ChunkSpine;
    use differential_dataflow::trace::Trace;
    use timely::dataflow::operators::generic::OperatorInfo;
    let mut spine = ChunkSpine::<VChunk>::new(OperatorInfo::new(0, 0, [].into()), None, None);
    spine.insert(batch(
        vec![vec_chunk(vec![((1, 0), 0, 1), ((2, 0), 0, 1)])],
        0,
        1,
    ));
    spine.insert(batch(vec![vec_chunk(vec![((1, 0), 1, -1)])], 1, 2));
    let mut cache = CachedTrace::new(spine, 100);
    cache
        .batch_through_keys(AntichainRef::new(&[1]), &[1])
        .unwrap();
    assert_eq!(
        rows(
            &cache
                .batch_through_keys(AntichainRef::new(&[]), &[1])
                .unwrap()
        ),
        vec![((1, 0), 0, 1), ((1, 0), 1, -1)]
    );
    cache.set_logical_compaction(AntichainRef::new(&[2]));
    cache.set_physical_compaction(AntichainRef::new(&[2]));
    // Different selection avoids the exact-hit shortcut and reads compacted data.
    let result = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1, 2])
        .unwrap();
    let mut weight = 0;
    for ((key, _), _, diff) in rows(&result) {
        if key == 1 {
            weight += diff;
        }
    }
    assert_eq!(weight, 0);
}
