use super::*;
use differential_dataflow::columnar::updates::UpdatesTyped;
use differential_dataflow::trace::chunk::{ChunkBatch, ChunkBatchMerger, ChunkMerger};
use differential_dataflow::trace::implementations::merge_batcher::Merger as BatcherMerger;
use differential_dataflow::trace::{Description, Merger};
use timely::container::PushInto;

type Row = ((u64, u64), u64, i64);
fn chunks(rows: &[Row], size: usize) -> Vec<DiskChunk> {
    rows.chunks(size)
        .map(|rows| {
            let mut updates = UpdatesTyped::default();
            for row in rows {
                updates.push_into(*row);
            }
            DiskChunk::new(Inner::from_trie(updates.consolidate())).persist()
        })
        .collect()
}
fn flatten(chunks: &[DiskChunk]) -> Vec<Row> {
    chunks.iter().flat_map(rows).collect()
}
fn consolidated(rows: impl IntoIterator<Item = Row>) -> Vec<Row> {
    let mut map = std::collections::BTreeMap::new();
    for (data, time, diff) in rows {
        *map.entry((data, time)).or_insert(0) += diff;
    }
    map.into_iter()
        .filter(|(_, d)| *d != 0)
        .map(|((data, time), diff)| (data, time, diff))
        .collect()
}
fn graded(chunks: &[DiskChunk]) {
    assert!(chunks
        .iter()
        .all(|c| c.is_file() && c.len() > 0 && c.len() <= DiskChunk::TARGET));
    assert!(chunks
        .windows(2)
        .all(|c| c[0].len() + c[1].len() > DiskChunk::TARGET));
}

#[test]
fn file_roundtrip_selection_and_reclamation() {
    let store = install(false);
    let stats = Rc::clone(&store.borrow().stats);
    let input: Vec<_> = (0..20_000).map(|k| ((k, 0), k % 7, 1)).collect();
    let source = chunks(&input, DiskChunk::TARGET);
    store.borrow_mut().rotate();
    assert_eq!(flatten(&source), input);
    let before = stats.reads.get();
    assert!(DiskChunk::select_keys(&source, &[20_000]).is_empty());
    assert_eq!(stats.reads.get(), before, "bounds should skip missing keys");
    let keys = [1, 19_999];
    let selected = DiskChunk::select_keys(&source, &keys);
    assert_eq!(stats.reads.get(), before + 2);
    assert_eq!(flatten(&selected), vec![input[1], input[19_999]]);
    let _again = DiskChunk::select_keys(&source, &keys);
    assert_eq!(
        stats.reads.get(),
        before + 4,
        "source must not pin decoded data"
    );
    drop(source);
    assert_eq!(
        stats.live_bytes.get(),
        0,
        "resident selection must not retain source files"
    );
    assert_eq!(flatten(&selected), vec![input[1], input[19_999]]);
    uninstall();
}

#[test]
fn settle_existing_full_chunks_without_io() {
    let store = install(false);
    let stats = Rc::clone(&store.borrow().stats);
    let input: Vec<_> = (0..3 * DiskChunk::TARGET as u64)
        .map(|k| ((k, 0), 0, 1))
        .collect();
    let mut source = chunks(&input, DiskChunk::TARGET).into();
    let mut output = VecDeque::new();
    let before = (stats.reads.get(), stats.writes.get());
    DiskChunk::settle(&mut source, true, &mut output);
    assert!(source.is_empty());
    assert_eq!((stats.reads.get(), stats.writes.get()), before);
    let output: Vec<_> = output.into();
    graded(&output);
    assert_eq!(flatten(&output), input);
    uninstall();
}

#[test]
fn batcher_merge_and_extract_match_independent_oracle() {
    let _store = install(false);
    let left: Vec<_> = (0..25_000).map(|k| ((k, 0), k % 5, 1)).collect();
    let right: Vec<_> = (0..25_000)
        .filter(|k| k % 3 != 0)
        .map(|k| ((k, 0), k % 5, -1))
        .collect();
    let expected = consolidated(left.iter().chain(&right).copied());
    let mut merger = ChunkMerger::<DiskChunk>::default();
    let mut merged = Vec::new();
    merger.merge(
        chunks(&left, 811),
        chunks(&right, 439),
        &mut merged,
        &mut Vec::new(),
    );
    graded(&merged);
    assert_eq!(flatten(&merged), expected);
    let mut frontier = Antichain::new();
    let (mut ship, mut keep) = (Vec::new(), Vec::new());
    merger.extract(
        merged,
        AntichainRef::new(&[3]),
        &mut frontier,
        &mut ship,
        &mut keep,
        &mut Vec::new(),
    );
    graded(&ship);
    graded(&keep);
    assert_eq!(
        flatten(&ship),
        expected
            .iter()
            .filter(|r| r.1 < 3)
            .copied()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        flatten(&keep),
        expected
            .iter()
            .filter(|r| r.1 >= 3)
            .copied()
            .collect::<Vec<_>>()
    );
    assert_eq!(frontier, Antichain::from_elem(3));
    uninstall();
}

#[test]
fn fueled_batch_merge_advances_and_cancels_across_file_boundaries() {
    let _store = install(false);
    let left: Vec<_> = (0..15_000)
        .flat_map(|k| [((k, 0), 0, 1), ((k, 0), 1, 2)])
        .collect();
    let right: Vec<_> = (0..15_000)
        .flat_map(|k| [((k, 0), 2, -3), ((k, 1), 3, 1)])
        .collect();
    let expected = consolidated(left.iter().chain(&right).map(|&(d, t, r)| (d, t.max(4), r)));
    let a = ChunkBatch::new(
        chunks(&left, 601),
        Description::new(
            Antichain::from_elem(0),
            Antichain::from_elem(2),
            Antichain::from_elem(0),
        ),
    );
    let b = ChunkBatch::new(
        chunks(&right, 399),
        Description::new(
            Antichain::from_elem(2),
            Antichain::from_elem(4),
            Antichain::from_elem(0),
        ),
    );
    let mut merger = ChunkBatchMerger::new(&a, &b, AntichainRef::new(&[4]));
    loop {
        let mut fuel = 100;
        merger.work(&a, &b, &mut fuel);
        if fuel > 0 {
            break;
        }
    }
    let merged = merger.done();
    graded(&merged.chunks);
    assert_eq!(flatten(&merged.chunks), expected);
    uninstall();
}

#[test]
fn advance_single_group_split_across_chunks() {
    let _store = install(false);
    let source: Vec<_> = (0..20_000)
        .map(|t| ((7, 2), t, if t % 2 == 0 { 1 } else { -1 }))
        .collect();
    let mut input = chunks(&source, 997).into();
    let mut output = VecDeque::new();
    DiskChunk::advance(&mut input, AntichainRef::new(&[19_999]), true, &mut output);
    assert!(input.is_empty());
    assert!(output.is_empty(), "all advanced differences should cancel");
    uninstall();
}

#[test]
fn file_spine_cache_reads_only_new_suffix_and_survives_compaction() {
    use differential_dataflow::operators::arrange::TraceAgent;
    use differential_dataflow::trace::chunk::ChunkSpine;
    use differential_dataflow::trace::wrappers::cached::CachedTrace;
    use differential_dataflow::trace::{Trace, TraceReader};
    use timely::dataflow::operators::generic::OperatorInfo;
    let store = install(false);
    let stats = Rc::clone(&store.borrow().stats);
    let info = OperatorInfo::new(0, 0, [].into());
    let spine = ChunkSpine::<DiskChunk>::new(info.clone(), None, None);
    let (trace, mut writer) = TraceAgent::new(spine, info, None);
    let batch = |time, diff| {
        let data: Vec<_> = (0..20_000).map(|k| ((k, 0), time, diff)).collect();
        Rc::new(ChunkBatch::new(
            chunks(&data, DiskChunk::TARGET),
            Description::new(
                Antichain::from_elem(time),
                Antichain::from_elem(time + 1),
                Antichain::from_elem(0),
            ),
        ))
    };
    writer.insert(batch(0, 1), None);
    let mut cache = CachedTrace::new(trace, 100);
    let first = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1, 19_999])
        .unwrap();
    assert_eq!(stats.reads.get(), 2);
    let hit = cache
        .batch_through_keys(AntichainRef::new(&[]), &[19_999, 1])
        .unwrap();
    assert!(Rc::ptr_eq(&first, &hit));
    assert_eq!(stats.reads.get(), 2);
    writer.insert(batch(1, -1), None);
    let before = stats.reads.get();
    let extended = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1, 19_999])
        .unwrap();
    assert_eq!(
        stats.reads.get() - before,
        2,
        "read only the two intersecting chunks in the new batch"
    );
    assert_eq!(
        flatten(&extended.chunks),
        vec![
            ((1, 0), 0, 1),
            ((1, 0), 1, -1),
            ((19_999, 0), 0, 1),
            ((19_999, 0), 1, -1)
        ]
    );
    // Real spine compaction may erase all cold and hot updates after advancement.
    cache.set_logical_compaction(AntichainRef::new(&[2]));
    cache.set_physical_compaction(AntichainRef::new(&[2]));
    let compacted = cache
        .batch_through_keys(AntichainRef::new(&[]), &[1, 2, 19_999])
        .unwrap();
    assert_eq!(
        flatten(&compacted.chunks).iter().map(|r| r.2).sum::<i64>(),
        0
    );
    uninstall();
}
