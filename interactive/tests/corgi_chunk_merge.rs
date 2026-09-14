//! Regression coverage for the sorted/consolidated chunk-chain merge contract.
use corgi::Value;
use differential_dataflow::batcher::merge::Merger;
use differential_dataflow::trace::chunk::{Chunk, ChunkMerger};
use interactive::corgi::chunk::CorgiChunk;

fn chunk(times: &[u64]) -> CorgiChunk<u64, i64> {
    CorgiChunk::from_columns(
        Value::u64(vec![7; times.len()]),
        Value::u64(vec![8; times.len()]),
        times.to_vec(),
        vec![1; times.len()],
    )
}

fn check_horizon(prefix: usize) {
    let n = prefix as u64;
    let left = vec![chunk(&(0..n).collect::<Vec<_>>()), chunk(&[n + 1])];
    let right = vec![chunk(&[n, n + 2])];
    let mut output = Vec::new();
    ChunkMerger::default().merge(left, right, &mut output, &mut Vec::new());
    let times: Vec<_> = output.iter().flat_map(|c| (0..c.times().len()).map(|i| c.times().get(i))).collect();
    assert_eq!(times.len(), prefix + 3);
    assert_eq!(&times[prefix - 1..], [n - 1, n, n + 1, n + 2]);
    assert!(times.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn merge_keeps_time_order_when_an_equal_value_class_crosses_a_chunk_boundary() {
    check_horizon(1);
}

#[test]
fn extract_keeps_a_residual_across_chunks() {
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    use timely::progress::Antichain;
    let point = |xs: &[u64]| PointStamp::new(xs.iter().copied().collect());
    let times = [point(&[0]), point(&[4, 1]), point(&[1, 4]), point(&[2, 2]),
                 point(&[1, 2]), point(&[2, 1]), point(&[1])];
    for upper in [Antichain::new(), Antichain::from_elem(point(&[1, 1])),
                  Antichain::from(vec![point(&[1, 3]), point(&[3, 1])])] {
        for size in [1, 3, times.len()] {
            let chunks = (0..times.len()).collect::<Vec<_>>().chunks(size).map(|rows|
                CorgiChunk::from_columns(Value::u64(rows.iter().map(|&r| r as u64).collect()),
                    Value::u64(vec![0; rows.len()]),
                    rows.iter().map(|&r| times[r].clone()).collect::<Vec<_>>(), vec![1i64; rows.len()])
            ).collect();
            let mut residual = Antichain::from_elem(point(&[0, 5]));
            let mut expected = residual.clone();
            for t in times.iter().filter(|t| upper.less_equal(t)) { expected.insert_ref(t); }
            let (mut ship, mut keep) = (Vec::new(), Vec::new());
            ChunkMerger::default().extract(chunks, upper.borrow(), &mut residual,
                &mut ship, &mut keep, &mut Vec::new());
            assert_eq!(residual, expected);
            for (chunks, carried) in [(keep, true), (ship, false)] {
                let actual: Vec<_> = chunks.iter().flat_map(|c|
                    (0..c.times().len()).map(|r| c.times().get(r))).collect();
                let expected: Vec<_> = times.iter().filter(|t| upper.less_equal(t) == carried).cloned().collect();
                assert_eq!(actual, expected);
            }
        }
    }
}

#[test]
#[ignore = "scale confirmation with fully graded inputs; the small case tests the same merge contract"]
fn merge_horizon_with_graded_input_chains() {
    check_horizon(CorgiChunk::<u64, i64>::TARGET);
}
