use timely::dataflow::operators::{ToStream, Capture};
use timely::dataflow::operators::capture::Extract;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::CountTotal;

/// `count_total` is the total-order-specialized counting operator (distinct from
/// `Collection::count`, which routes through `reduce`). This exercises the
/// unload-based trace lookup in `operators/count.rs`.
#[test]
fn count_total() {

    let data = timely::example(|scope| {
        // Keys 0 (×2) and 1 (×1), all at time 0.
        vec![(0u32, 0u64, 1isize), (0u32, 0u64, 1), (1u32, 0u64, 1)]
            .into_iter()
            .to_stream(scope)
            .as_collection()
            .count_total()
            .inner
            .capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
    assert_eq!(extracted[0].1, vec![
        ((0u32, 2isize), 0u64, 1),
        ((1u32, 1isize), 0u64, 1),
    ]);
}

/// Counts must update across batches: a later retraction of one occurrence of a
/// key flips its count, which forces the operator to read the prior count back
/// out of the trace (the unload path) and emit the difference.
#[test]
fn count_total_incremental() {

    let data = timely::example(|scope| {
        vec![
            // time 0: key 0 appears twice, key 1 once.
            (0u32, 0u64, 1isize),
            (0u32, 0u64, 1),
            (1u32, 0u64, 1),
            // time 1: retract one occurrence of key 0 (count 2 -> 1).
            (0u32, 1u64, -1),
        ]
            .into_iter()
            .to_stream(scope)
            .as_collection()
            .count_total()
            .inner
            .capture()
    });

    let mut extracted = data.extract();
    // Flatten across captured rounds into a single (data, time, diff) list.
    let mut updates: Vec<((u32, isize), u64, isize)> = Vec::new();
    for (_t, batch) in extracted.drain(..) {
        updates.extend(batch);
    }
    updates.sort();

    // At time 0: key 0 -> count 2, key 1 -> count 1.
    // At time 1: key 0 count 2 retracted, count 1 asserted.
    assert_eq!(updates, vec![
        ((0, 1), 1, 1),
        ((0, 2), 0, 1),
        ((0, 2), 1, -1),
        ((1, 1), 0, 1),
    ]);
}
