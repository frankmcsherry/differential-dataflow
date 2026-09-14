//! Primitive-lane implementation of DD's bulk time algebra. No operator scheduling.
use super::col_times::{radix_sort_with, ColTime, ColTimes, RadixScratch};
use differential_dataflow::operators::int_proxy::time_container::{
    Binary, Operand, Operation, Rows, TimeContainer,
};
use std::{cmp::Ordering, ops::Range, sync::Arc};

// Resolve selections once per lane. Inner loops see only primitive slices or a scalar.
enum Lane<'a> {
    Slice(&'a [u64]),
    Gather(&'a [u64], &'a [usize]),
    Repeat(u64),
}
impl Lane<'_> {
    fn at(&self, i: usize) -> u64 {
        match self {
            Self::Slice(values) => values[i],
            Self::Gather(values, rows) => values[rows[i]],
            Self::Repeat(value) => *value,
        }
    }
}
fn read_lane<'a, T: ColTime>(operand: &'a Operand<'_, ColTimes<T>>, j: usize) -> Lane<'a> {
    let Operand(c, rows) = operand;
    let Some(lane) = c.lane(j) else { return Lane::Repeat(0); };
    match rows {
        Rows::Range(r) => Lane::Slice(&lane[r.clone()]),
        Rows::Indices(rows) => Lane::Gather(lane, rows),
        Rows::Repeat { row, .. } => Lane::Repeat(lane[*row]),
    }
}

/// Reused lane-ordering workspace; contains no operator state.
#[derive(Default)]
pub struct ColumnOrderScratch {
    rows: Vec<usize>,
    radix: RadixScratch,
}

impl<T: ColTime> TimeContainer for ColTimes<T> {
    type Time = T;
    type Suffix = Vec<Vec<(usize, u64)>>;
    type OrderScratch = ColumnOrderScratch;
    fn len(&self) -> usize {
        self.len()
    }
    fn clear(&mut self) {
        self.clear();
    }
    fn copy_many(&mut self, sources: &[Operand<'_, Self>]) {
        let rows: usize = sources.iter().map(Operand::len).sum();
        if rows == 0 { return; }
        self.ensure_width(sources.iter().map(|s| s.0.width()).max().unwrap_or(0));
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            let lane = Arc::make_mut(lane);
            lane.reserve(rows);
            for source in sources.iter().filter(|s| !s.is_empty()) {
                match (source.0.lane(j), &source.1) {
                    (None, _) => lane.resize(lane.len() + source.len(), 0),
                    (Some(values), Rows::Range(r)) => lane.extend_from_slice(&values[r.clone()]),
                    (Some(values), Rows::Indices(rows)) => lane.extend(rows.iter().map(|&r| values[r])),
                    (Some(values), Rows::Repeat { row, count }) => lane.resize(lane.len() + count, values[*row]),
                }
            }
        }
        self.rows += rows;
    }
    fn map(&mut self, op: Operation, requests: &[Binary<'_, Self>]) {
        let rows: usize = requests.iter().map(Binary::len).sum::<usize>();
        if rows == 0 { return; }
        self.ensure_width(
            requests
                .iter()
                .map(|r| r.left.0.width().max(r.right.0.width()))
                .max()
                .unwrap_or(0),
        );
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            let lane = Arc::make_mut(lane);
            for r in requests {
                let (a, b) = (read_lane(&r.left, j), read_lane(&r.right, j));
                lane.extend((0..r.len()).map(|i| {
                    let (a, b) = (a.at(i), b.at(i));
                    match op {
                        Operation::Join => a.max(b),
                        Operation::Meet => a.min(b),
                    }
                }));
            }
        }
        self.rows += rows;
    }
    fn apply(&mut self, op: Operation, requests: &[(Range<usize>, Operand<'_, Self>)]) {
        assert!(requests.iter().all(|(r, source)| r.len() == source.len()));
        self.ensure_width(requests.iter().map(|(_, source)| source.0.width()).max().unwrap_or(0));
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            let lane = Arc::make_mut(lane);
            for (range, source) in requests {
                let apply = |value: &mut u64, bound| *value = match op {
                    Operation::Join => (*value).max(bound),
                    Operation::Meet => (*value).min(bound),
                };
                let source = read_lane(source, j);
                if let Lane::Repeat(bound) = source {
                    for value in &mut lane[range.clone()] { apply(value, bound); }
                } else {
                    for (i, value) in lane[range.clone()].iter_mut().enumerate() {
                        apply(value, source.at(i));
                    }
                }
            }
        }
    }
    fn less_equal(requests: &[Binary<'_, Self>], output: &mut [bool]) {
        assert_eq!(
            output.len(),
            requests.iter().map(Binary::len).sum::<usize>()
        );
        output.fill(true);
        let lanes = requests
            .iter()
            .map(|r| r.left.0.width().max(r.right.0.width()))
            .max()
            .unwrap_or(0);
        for j in 0..lanes {
            let mut offset = 0;
            for r in requests {
                let (a, b) = (read_lane(&r.left, j), read_lane(&r.right, j));
                for i in 0..r.len() {
                    output[offset + i] &= a.at(i) <= b.at(i);
                }
                offset += r.len();
            }
        }
    }
    fn compare(requests: &[Binary<'_, Self>], output: &mut [Ordering]) {
        assert_eq!(
            output.len(),
            requests.iter().map(Binary::len).sum::<usize>()
        );
        output.fill(Ordering::Equal);
        let lanes = requests
            .iter()
            .map(|r| r.left.0.width().max(r.right.0.width()))
            .max()
            .unwrap_or(0);
        for j in 0..lanes {
            let mut offset = 0;
            for r in requests {
                let (a, b) = (read_lane(&r.left, j), read_lane(&r.right, j));
                for i in 0..r.len() {
                    if output[offset + i] == Ordering::Equal {
                        output[offset + i] = a.at(i).cmp(&b.at(i));
                    }
                }
                offset += r.len();
            }
        }
    }
    fn order(
        &self,
        indices: &mut [usize],
        segments: &[Range<usize>],
        scratch: &mut Self::OrderScratch,
    ) {
        let ColumnOrderScratch { rows, radix } = scratch;
        for segment in segments {
            if segment.len() < 2 {
                continue;
            }
            rows.clear();
            rows.extend_from_slice(&indices[segment.clone()]);
            radix_sort_with(self.lanes.iter().map(|l| l.as_slice()), rows, radix);
            indices[segment.clone()].copy_from_slice(&rows);
        }
    }
    fn meet_reduce(&self, ranges: &[Range<usize>], output: &mut Self) {
        assert!(ranges.iter().all(|r| !r.is_empty()));
        output.ensure_width(self.width());
        for (j, lane) in output.lanes.iter_mut().enumerate() {
            let lane = Arc::make_mut(lane);
            for r in ranges {
                lane.push((r.clone()).map(|i| self.coordinate(j, i)).min().unwrap());
            }
        }
        output.rows += ranges.len();
    }
    fn suffix_meets(&self, summaries: &mut Self::Suffix) {
        summaries.resize_with(self.width(), Vec::new);
        for (lane, changes) in self.lanes.iter().zip(summaries) {
            changes.clear();
            let mut minimum = u64::MAX;
            for (i, &x) in lane.iter().enumerate().rev() {
                if changes.is_empty() || x < minimum {
                    minimum = x;
                    changes.push((i, x));
                }
            }
        }
    }
    fn suffix_meet(&self, summaries: &Self::Suffix, position: usize, output: &mut Self) {
        if position >= self.len() {
            return;
        }
        output.ensure_width(self.width());
        for (j, lane) in output.lanes.iter_mut().enumerate() {
            let value = summaries.get(j).map_or(0, |changes| {
                changes[changes.partition_point(|&(end, _)| end >= position) - 1].1
            });
            Arc::make_mut(lane).push(value);
        }
        output.rows += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::operators::int_proxy::{
        join::{JoinMatches as Matches, Walk},
        updates::{Keyed, Updates},
        time_container::Rows,
    };
    use differential_dataflow::{dynamic::pointstamp::PointStamp, lattice::Lattice};
    use std::collections::BTreeMap;
    use timely::progress::Antichain;
    type T = PointStamp<u64>;
    fn point(xs: impl IntoIterator<Item = u64>) -> T {
        T::new(xs.into_iter().collect())
    }
    fn random(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }
    fn times<C: TimeContainer<Time = T> + FromIterator<T> + IntoIterator<Item = T> + Clone>(rows: &[T]) -> C {
        rows.iter().cloned().collect()
    }
    fn values<C: TimeContainer<Time = T> + FromIterator<T> + IntoIterator<Item = T> + Clone>(c: &C) -> Vec<T> {
        c.clone().into_iter().collect()
    }

    #[test]
    fn bulk_algebra_matches_rows_across_widths_and_selections() {
        let mut state = 0x619ba83;
        for width in 0..=4 {
            let a: Vec<_> = (0..31)
                .map(|_| point((0..width).map(|_| random(&mut state) % 7)))
                .collect();
            let b: Vec<_> = (0..31)
                .map(|_| point((0..width + 1).map(|_| random(&mut state) % 7)))
                .collect();
            let (ca, cb) = (times::<ColTimes<T>>(&a), times::<ColTimes<T>>(&b));
            let gather = [7, 1, 7, 3, 0];
            let broadcast = vec![point([2, 5, 1])];
            let cbroadcast = times::<ColTimes<T>>(&broadcast);
            let row_requests = [
                Binary {
                    left: Operand(&a, Rows::Range(3..12)),
                    right: Operand(&b, Rows::Range(20..29)),
                },
                Binary {
                    left: Operand::repeat_row(&broadcast, 0, 5),
                    right: Operand(&a, Rows::Indices(&gather)),
                },
            ];
            let column_requests = [
                Binary {
                    left: Operand(&ca, Rows::Range(3..12)),
                    right: Operand(&cb, Rows::Range(20..29)),
                },
                Binary {
                    left: Operand::repeat_row(&cbroadcast, 0, 5),
                    right: Operand(&ca, Rows::Indices(&gather)),
                },
            ];
            for op in [Operation::Join, Operation::Meet] {
                let mut expected = a.clone();
                let mut actual = times::<ColTimes<T>>(&a);
                expected.map(op, &row_requests);
                actual.map(op, &column_requests);
                assert_eq!(values(&actual), expected);
                let other = vec![point([4])];
                let cother = times::<ColTimes<T>>(&other);
                expected.apply(op, &[
                    (1..5, Operand::repeat_row(&broadcast, 0, 4)),
                    (5..10, Operand::repeat_row(&b, 7, 5)),
                    (10..15, Operand(&b, Rows::Indices(&gather))),
                    (15..20, Operand::repeat_row(&other, 0, 5)),
                    (25..28, Operand(&b, Rows::Range(3..6))),
                ]);
                actual.apply(op, &[
                    (1..5, Operand::repeat_row(&cbroadcast, 0, 4)),
                    (5..10, Operand::repeat_row(&cb, 7, 5)),
                    (10..15, Operand(&cb, Rows::Indices(&gather))),
                    (15..20, Operand::repeat_row(&cother, 0, 5)),
                    (25..28, Operand(&cb, Rows::Range(3..6))),
                ]);
                assert_eq!(values(&actual), expected);
            }
            let (mut expected, mut actual) = (vec![false; 14], vec![false; 14]);
            Vec::<T>::less_equal(&row_requests, &mut expected);
            ColTimes::<T>::less_equal(&column_requests, &mut actual);
            assert_eq!(actual, expected);
            let (mut expected, mut actual) = (vec![Ordering::Equal; 14], vec![Ordering::Equal; 14]);
            Vec::<T>::compare(&row_requests, &mut expected);
            ColTimes::<T>::compare(&column_requests, &mut actual);
            assert_eq!(actual, expected);
            let mut minima = ColTimes::<T>::default();
            for source in [&ca, &cb] {
                differential_dataflow::operators::int_proxy::time_container::extend_antichain(
                    &mut minima, source, &gather,
                );
            }
            let expected: Antichain<_> = [&a, &b].into_iter()
                .flat_map(|src| gather.iter().map(|&r| src[r].clone())).collect();
            assert_eq!(minima.len(), expected.len());
            assert!(values(&minima).iter().all(|t| expected.elements().contains(t)));
            let (mut rows, mut columns) = (Vec::new(), ColTimes::<T>::default());
            a.meet_reduce(&[0..5, 7..30], &mut rows);
            ca.meet_reduce(&[0..5, 7..30], &mut columns);
            assert_eq!(values(&columns), rows);
            let mut summary = Default::default();
            ca.suffix_meets(&mut summary);
            // Append into retained storage, including lanes absent from the source.
            let mut expected = vec![point([9, 8, 7, 6, 5])];
            let mut suffix = times::<ColTimes<T>>(&expected);
            for pos in 0..=a.len() {
                ca.suffix_meet(&summary, pos, &mut suffix);
                expected.extend(a[pos..].iter().cloned().reduce(|x, y| x.meet(&y)));
                assert_eq!(values(&suffix), expected);
            }
            let mut expected: Vec<_> = (0..a.len()).rev().collect();
            let mut actual = expected.clone();
            a.order(&mut expected, &[0..13, 13..31], &mut ());
            ca.order(&mut actual, &[0..13, 13..31], &mut Default::default());
            assert_eq!(actual, expected, "stable segmented order");
            let mut empty = cb.clone(); empty.clear();
            let mut copied = ColTimes::<T>::default();
            copied.copy_many(&[Operand(&ca, Rows::Range(2..5)), Operand::repeat_row(&cb, 7, 3),
                Operand(&cb, Rows::Range(0..0)), Operand::repeat_row(&empty, 0, 0),
                Operand(&ca, Rows::Indices(&gather))]);
            let expected: Vec<_> = a[2..5].iter().cloned().chain(std::iter::repeat_n(b[7].clone(), 3))
                .chain(gather.iter().map(|&r| a[r].clone())).collect();
            assert_eq!(values(&copied), expected);
            // Shared source lanes remain intact after maps and in-place destination changes.
            assert_eq!(values(&ca), a);
        }
    }

    fn check_join<C: TimeContainer<Time = T> + FromIterator<T> + IntoIterator<Item = T> + Clone>(
        a: &[(u64, T, i64)],
        b: &[(u64, T, i64)],
        limit: usize,
    ) {
        let make = |rows: &[(u64, T, i64)]| {
            let mut out: Updates<C, i64> = Updates::default();
            out.times = rows.iter().map(|r| r.1.clone()).collect();
            for (id, _, d) in rows {
                out.keys.push(1);
                out.ids.push(*id);
                out.diffs.push(*d);
            }
            out.consolidate();
            out
        };
        let (left, right) = (make(a), make(b));
        let mut oracle = BTreeMap::new();
        for (i, t, d) in a {
            for (j, u, e) in b {
                *oracle.entry((*i, *j, t.join(u))).or_insert(0) += d * e;
            }
        }
        oracle.retain(|_, d| *d != 0);
        let mut walk = Walk::default();
        walk.load(&left, 0..left.len(), &right, 0..right.len());
        let mut actual = BTreeMap::new();
        loop {
            let mut out = Matches::default();
            let more = walk.fill(1, &left, &right, limit, &mut out);
            assert!(out.ids.len() <= limit);
            for (r, t) in out.times.into_iter().enumerate() {
                let (_, (i, j)) = out.ids[r];
                *actual.entry((i, j, t)).or_insert(0) += out.diffs[r];
            }
            if !more {
                break;
            }
        }
        actual.retain(|_, d| *d != 0);
        assert_eq!(actual, oracle);
    }

    #[test]
    fn shared_join_matches_cartesian_oracle_for_rows_and_lanes() {
        let mut state = 0x713abce;
        for width in 0..=4 {
            for n in [0, 3, 80] {
                for _ in 0..12 {
                    let mut draw = || {
                        (0..n)
                            .map(|_| {
                                (
                                    random(&mut state) % 12,
                                    point((0..width).map(|_| random(&mut state) % 5)),
                                    (random(&mut state) % 5) as i64 - 2,
                                )
                            })
                            .collect::<Vec<_>>()
                    };
                    let (a, b) = (draw(), draw());
                    for limit in [1, 17, 256] {
                        check_join::<Vec<T>>(&a, &b, limit);
                        check_join::<ColTimes<T>>(&a, &b, limit);
                    }
                }
            }
        }
    }

    #[test]
    fn shared_reduce_cancels_clamped_runs_before_scheduling() {
        use differential_dataflow::operators::int_proxy::reduce::{Sweep, ReduceWindow};
        fn check<C: TimeContainer<Time = T> + FromIterator<T> + IntoIterator<Item = T> + Clone>() {
            let input = Updates {
                keys: vec![0, 0],
                ids: vec![1, 1],
                diffs: vec![1i64, -1],
                times: times::<C>(&[point([0, 1]), point([1, 1])]),
            };
            let output = Updates::<C, i64>::default();
            let seeds = times::<C>(&[point([2, 0])]);
            let mut sweep = Sweep::<C, i64, i64>::default();
            let window = ReduceWindow { input, output, seeds: Keyed {
                keys: vec![0], times: seeds,
            } };
            sweep.load(&window, &[0], &C::default());
            assert!(sweep.next(&C::default()));
            assert_eq!(values(&sweep.times), vec![point([2, 0])]);
            assert!(sweep.input.is_empty() && sweep.output.is_empty());
            sweep.commit(&[], &[0], &mut Updates::default());
            assert!(!sweep.next(&C::default()));
        }
        check::<Vec<T>>();
        check::<ColTimes<T>>();
    }

    #[test]
    fn reduce_matches_snapshots_on_a_product_grid() {
        use differential_dataflow::operators::int_proxy::reduce::{Sweep, ReduceWindow};
        use timely::PartialOrder;
        fn append<C: TimeContainer>(into: &mut Updates<C, i64>, from: &Updates<C, i64>) {
            into.keys.extend_from_slice(&from.keys); into.ids.extend_from_slice(&from.ids);
            into.diffs.extend_from_slice(&from.diffs);
            into.times.copy(Operand(&from.times, Rows::Range(0..from.len())));
        }
        fn check<C: TimeContainer<Time = T> + FromIterator<T> + IntoIterator<Item = T> + Clone>() {
            let mut state = 1234567;
            let mut sweep = Sweep::<C, i64, i64>::default();
            for width in 0..=3 {
                let grid: Vec<_> = (0..4usize.pow(width))
                    .map(|i| point((0..width).map(|j| (i / 4usize.pow(j) % 4) as u64))).collect();
                for _ in 0..10 {
                    // Mix single-seed keys with incomparable histories; slot reuse must
                    // tolerate different subsets finishing or deferring in each wave.
                    let raw: Vec<_> = (0..40).map(|_| {
                        let key = random(&mut state) % 4;
                        let t = match key { 0 => grid[0].clone(), 2 => grid.last().unwrap().clone(),
                            _ => grid[random(&mut state) as usize % grid.len()].clone() };
                        (key, random(&mut state) % 4, t, (random(&mut state) % 3) as i64 - 1)
                    }).collect();
                    for limit in [1, 3, 32] {
                        let mut window = ReduceWindow {
                            input: Updates { keys: raw.iter().map(|r| r.0).collect(), ids: raw.iter().map(|r| r.1).collect(),
                                times: times::<C>(&raw.iter().map(|r| r.2.clone()).collect::<Vec<_>>()), diffs: raw.iter().map(|r| r.3).collect() },
                            output: Updates::default(),
                            seeds: Keyed { keys: raw.iter().map(|r| r.0).collect(),
                                times: times::<C>(&raw.iter().map(|r| r.2.clone()).collect::<Vec<_>>()) },
                        };
                        window.input.consolidate(); window.seeds.consolidate();
                        for frontier in [Antichain::from_elem(point(vec![2; width as usize])), Antichain::new()] {
                            let upper = times::<C>(frontier.elements());
                            let mut keys = window.seeds.keys.clone(); keys.dedup();
                            let mut deferred = Keyed::default();
                            let mut deltas = Updates::default();
                            let mut steps = 0;
                            for group in keys.chunks(limit) {
                                sweep.load(&window, group, &upper);
                                while sweep.next(&upper) {
                                    steps += sweep.keys.len();
                                    assert!(steps <= grid.len() * 4);
                                    let (mut corrections, mut ends) = (Vec::new(), Vec::new());
                                    let (mut a, mut b) = (0, 0);
                                    for (&ae, &be) in sweep.input_ends.iter().zip(&sweep.output_ends) {
                                        let start = corrections.len();
                                        corrections.extend(sweep.input[a..ae].iter().filter(|r| r.1 > 0).map(|r| (r.0, 1)));
                                        corrections.extend(sweep.output[b..be].iter().map(|r| (r.0, -r.1)));
                                        differential_dataflow::consolidation::consolidate_from(&mut corrections, start);
                                        ends.push(corrections.len()); (a, b) = (ae, be);
                                    }
                                    sweep.commit(&corrections, &ends, &mut deltas);
                                }
                                deferred.append_range(sweep.pending(), 0..sweep.pending().len());
                            }
                            append(&mut window.output, &deltas);
                            window.output.consolidate();
                            // Independently evaluate every completed snapshot, including keys
                            // not revisited in this retirement. No time-walk machinery in the oracle.
                            let output_times = values(&window.output.times);
                            for at in grid.iter().filter(|t| !frontier.less_equal(t)) {
                                let (mut expected, mut actual) = ([[0i64; 4]; 4], [[0i64; 4]; 4]);
                                for (k, id, t, d) in &raw {
                                    if t.less_equal(at) { expected[*k as usize][*id as usize] += d; }
                                }
                                for counts in &mut expected { for count in counts { *count = i64::from(*count > 0); } }
                                for (r, t) in output_times.iter().enumerate() {
                                    if t.less_equal(at) { actual[window.output.keys[r] as usize][window.output.ids[r] as usize] += window.output.diffs[r]; }
                                }
                                assert_eq!(actual, expected, "limit={limit}, snapshot={at:?}, frontier={frontier:?}");
                            }
                            if frontier.is_empty() { assert!(deferred.is_empty()); }
                            window.seeds = deferred; window.seeds.consolidate();
                        }
                    }
                }
            }
        }
        check::<Vec<T>>();
        check::<ColTimes<T>>();
    }

}
