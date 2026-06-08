use timely::dataflow::operators::{ToStream, Capture};
use timely::dataflow::operators::capture::Extract;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::ThresholdTotal;

/// `distinct_total` is the total-order-specialized threshold operator; it shares
/// the unload-based trace lookup in `operators/threshold.rs` with `count_total`.
#[test]
fn distinct_total() {

    let data = timely::example(|scope| {
        // Key 0 appears twice, key 1 once; distinct collapses each to one.
        vec![(0u32, 0u64, 1isize), (0u32, 0u64, 1), (1u32, 0u64, 1)]
            .into_iter()
            .to_stream(scope)
            .as_collection()
            .distinct_total()
            .inner
            .capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
    assert_eq!(extracted[0].1, vec![
        (0u32, 0u64, 1),
        (1u32, 0u64, 1),
    ]);
}

/// A count-sensitive threshold (present iff accumulated count >= 2) crossing its
/// boundary across batches: the second batch must read the prior count back out
/// of the trace (the unload path) to compute the 2 -> 1 transition and retract.
#[test]
fn threshold_total_incremental() {

    let data = timely::example(|scope| {
        vec![
            (0u32, 0u64, 1isize),   // count 0 -> 2 at time 0; present (>= 2)
            (0u32, 0u64, 1),
            (0u32, 1u64, -1),       // count 2 -> 1 at time 1; no longer present
        ]
            .into_iter()
            .to_stream(scope)
            .as_collection()
            .threshold_total(|_, c| if *c >= 2 { 1isize } else { 0 })
            .inner
            .capture()
    });

    let mut extracted = data.extract();
    let mut updates: Vec<(u32, u64, isize)> = Vec::new();
    for (_t, batch) in extracted.drain(..) {
        updates.extend(batch);
    }
    updates.sort();

    assert_eq!(updates, vec![
        (0, 0, 1),
        (0, 1, -1),
    ]);
}
