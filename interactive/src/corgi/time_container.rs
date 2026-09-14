//! Primitive-lane implementation of DD's bulk time algebra. No operator scheduling.
use super::col_times::{radix_sort_with, ColTime, ColTimes, RadixScratch};
use differential_dataflow::operators::int_proxy::time_container::{
    Binary, Operand, Operation, TimeContainer,
};
use std::{cmp::Ordering, ops::Range, sync::Arc};
use timely::progress::frontier::AntichainRef;

fn width<T: ColTime>(operand: &Operand<'_, ColTimes<T>>) -> usize {
    match operand {
        Operand::Rows(c, _) => c.width(),
        Operand::Repeat(t, _) => t.width(),
    }
}
fn coordinate<T: ColTime>(operand: &Operand<'_, ColTimes<T>>, lane: usize, row: usize) -> u64 {
    match operand {
        Operand::Rows(c, r) => c.coordinate(lane, r.at(row)),
        Operand::Repeat(t, n) => {
            assert!(row < *n);
            t.coordinate(lane)
        }
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
    fn time_at(&self, row: usize) -> T {
        self.get(row)
    }
    fn copy(&mut self, source: Operand<'_, Self>) {
        self.ensure_width(width(&source));
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            Arc::make_mut(lane).extend((0..source.len()).map(|i| coordinate(&source, j, i)));
        }
        self.rows += source.len();
    }
    fn map(&mut self, op: Operation, requests: &[Binary<'_, Self>]) {
        let rows: usize = requests.iter().map(Binary::len).sum();
        self.ensure_width(
            requests
                .iter()
                .map(|r| width(&r.left).max(width(&r.right)))
                .max()
                .unwrap_or(0),
        );
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            let lane = Arc::make_mut(lane);
            for r in requests {
                lane.extend((0..r.len()).map(|i| {
                    let (a, b) = (coordinate(&r.left, j, i), coordinate(&r.right, j, i));
                    match op {
                        Operation::Join => a.max(b),
                        Operation::Meet => a.min(b),
                    }
                }));
            }
        }
        self.rows += rows;
    }
    fn apply(&mut self, op: Operation, requests: &[(Range<usize>, T)]) {
        self.ensure_width(requests.iter().map(|(_, t)| t.width()).max().unwrap_or(0));
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            let lane = Arc::make_mut(lane);
            for (range, t) in requests {
                let bound = t.coordinate(j);
                for value in &mut lane[range.clone()] {
                    *value = match op {
                        Operation::Join => (*value).max(bound),
                        Operation::Meet => (*value).min(bound),
                    };
                }
            }
        }
    }
    fn advance_by(&mut self, ranges: &[Range<usize>], frontier: AntichainRef<'_, T>) {
        if frontier.is_empty() {
            return;
        }
        self.ensure_width(frontier.iter().map(ColTime::width).max().unwrap_or(0));
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            let floor = frontier.iter().map(|f| f.coordinate(j)).min().unwrap();
            if floor == 0 {
                continue;
            }
            let lane = Arc::make_mut(lane);
            for range in ranges {
                for value in &mut lane[range.clone()] {
                    *value = (*value).max(floor);
                }
            }
        }
    }
    fn less_equal(requests: &[Binary<'_, Self>], output: &mut [bool]) {
        assert_eq!(output.len(), requests.iter().map(Binary::len).sum());
        output.fill(true);
        let lanes = requests
            .iter()
            .map(|r| width(&r.left).max(width(&r.right)))
            .max()
            .unwrap_or(0);
        for j in 0..lanes {
            let mut offset = 0;
            for r in requests {
                for i in 0..r.len() {
                    output[offset + i] &= coordinate(&r.left, j, i) <= coordinate(&r.right, j, i);
                }
                offset += r.len();
            }
        }
    }
    fn compare(requests: &[Binary<'_, Self>], output: &mut [Ordering]) {
        assert_eq!(output.len(), requests.iter().map(Binary::len).sum());
        output.fill(Ordering::Equal);
        let lanes = requests
            .iter()
            .map(|r| width(&r.left).max(width(&r.right)))
            .max()
            .unwrap_or(0);
        for j in 0..lanes {
            let mut offset = 0;
            for r in requests {
                for i in 0..r.len() {
                    if output[offset + i] == Ordering::Equal {
                        output[offset + i] =
                            coordinate(&r.left, j, i).cmp(&coordinate(&r.right, j, i));
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
    fn suffix_meet(&self, summaries: &Self::Suffix, position: usize) -> Option<T> {
        (position < self.len()).then(|| {
            T::from_coordinates(self.width(), |j| {
                let changes = &summaries[j];
                changes[changes.partition_point(|&(end, _)| end >= position) - 1].1
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::operators::int_proxy::{
        join::{JoinMatches as Matches, Walk},
        updates::Updates,
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
    fn times<C: TimeContainer<Time = T>>(rows: &[T]) -> C {
        let mut out = C::default();
        for t in rows {
            out.copy(Operand::Repeat(t, 1));
        }
        out
    }
    fn values<C: TimeContainer<Time = T>>(c: &C) -> Vec<T> {
        (0..c.len()).map(|r| c.time_at(r)).collect()
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
            let broadcast = point([2, 5, 1]);
            let row_requests = [
                Binary {
                    left: Operand::Rows(&a, Rows::Range(3..12)),
                    right: Operand::Rows(&b, Rows::Range(20..29)),
                },
                Binary {
                    left: Operand::Repeat(&broadcast, 5),
                    right: Operand::Rows(&a, Rows::Indices(&gather)),
                },
            ];
            let column_requests = [
                Binary {
                    left: Operand::Rows(&ca, Rows::Range(3..12)),
                    right: Operand::Rows(&cb, Rows::Range(20..29)),
                },
                Binary {
                    left: Operand::Repeat(&broadcast, 5),
                    right: Operand::Rows(&ca, Rows::Indices(&gather)),
                },
            ];
            for op in [Operation::Join, Operation::Meet] {
                let mut expected = a.clone();
                let mut actual = times::<ColTimes<T>>(&a);
                expected.map(op, &row_requests);
                actual.map(op, &column_requests);
                assert_eq!(values(&actual), expected);
                let changes = [(1..5, broadcast.clone()), (15..20, point([4]))];
                expected.apply(op, &changes);
                actual.apply(op, &changes);
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
            for upper in [
                Antichain::new(),
                Antichain::from(vec![point([1, 4]), point([3, 1])]),
            ] {
                let mut expected = a.clone();
                let mut actual = times::<ColTimes<T>>(&a);
                TimeContainer::advance_by(&mut expected, &[0..7, 11..24], upper.borrow());
                TimeContainer::advance_by(&mut actual, &[0..7, 11..24], upper.borrow());
                assert_eq!(values(&actual), expected);
            }
            let (mut rows, mut columns) = (Vec::new(), ColTimes::<T>::default());
            a.meet_reduce(&[0..5, 7..30], &mut rows);
            ca.meet_reduce(&[0..5, 7..30], &mut columns);
            assert_eq!(values(&columns), rows);
            let mut summary = Default::default();
            ca.suffix_meets(&mut summary);
            for pos in 0..=a.len() {
                assert_eq!(
                    ca.suffix_meet(&summary, pos),
                    a[pos..].iter().cloned().reduce(|x, y| x.meet(&y))
                );
            }
            let mut expected: Vec<_> = (0..a.len()).rev().collect();
            let mut actual = expected.clone();
            a.order(&mut expected, &[0..13, 13..31], &mut ());
            ca.order(&mut actual, &[0..13, 13..31], &mut Default::default());
            assert_eq!(actual, expected, "stable segmented order");
            // Shared source lanes remain intact after maps and in-place destination changes.
            assert_eq!(values(&ca), a);
        }
    }

    fn check_join<C: TimeContainer<Time = T>>(
        a: &[(u64, T, i64)],
        b: &[(u64, T, i64)],
        limit: usize,
    ) {
        let make = |rows: &[(u64, T, i64)]| {
            let mut out: Updates<C, i64> = Updates::default();
            for (id, t, d) in rows {
                out.keys.push(1);
                out.ids.push(*id);
                out.times.copy(Operand::Repeat(t, 1));
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
            for r in 0..out.ids.len() {
                let (_, (i, j)) = out.ids[r];
                *actual.entry((i, j, out.times.time_at(r))).or_insert(0) += out.diffs[r];
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
        use differential_dataflow::operators::int_proxy::reduce::Sweep;
        fn check<C: TimeContainer<Time = T>>() {
            let input = Updates {
                keys: vec![0, 0],
                ids: vec![1, 1],
                diffs: vec![1i64, -1],
                times: times::<C>(&[point([0, 1]), point([1, 1])]),
            };
            let output = Updates::<C, i64>::default();
            let seeds = times::<C>(&[point([2, 0])]);
            let mut sweep = Sweep::<C, i64, i64>::default();
            sweep.load(&input, 0..2, &output, 0..0, &seeds, 0..1);
            let (mut inputs, mut outputs) = (Vec::new(), Vec::new());
            assert_eq!(
                sweep.next(&Antichain::new(), &mut inputs, &mut outputs),
                Some(point([2, 0]))
            );
            assert!(inputs.is_empty() && outputs.is_empty());
            sweep.commit(&[]);
            assert!(sweep
                .next(&Antichain::new(), &mut inputs, &mut outputs)
                .is_none());
        }
        check::<Vec<T>>();
        check::<ColTimes<T>>();
    }

    #[test]
    fn reduce_matches_snapshots_on_a_product_grid() {
        use differential_dataflow::operators::int_proxy::reduce::Sweep;
        use timely::PartialOrder;
        fn check<C: TimeContainer<Time = T>>() {
            let mut state = 1234567;
            let mut sweep = Sweep::<C, i64, i64>::default();
            for width in 0..=3 {
                let grid: Vec<_> = (0..4usize.pow(width))
                    .map(|i| point((0..width).map(|j| (i / 4usize.pow(j) % 4) as u64)))
                    .collect();
                for _ in 0..20 {
                    let raw: Vec<_> = (0..30).map(|_| (
                        random(&mut state) % 4,
                        grid[random(&mut state) as usize % grid.len()].clone(),
                        (random(&mut state) % 3) as i64 - 1,
                    )).collect();
                    let seeds = times::<C>(&raw.iter().map(|r| r.1.clone()).collect::<Vec<_>>());
                    let mut input = Updates {
                        keys: vec![0; raw.len()],
                        ids: raw.iter().map(|r| r.0).collect(),
                        times: times::<C>(&raw.iter().map(|r| r.1.clone()).collect::<Vec<_>>()),
                        diffs: raw.iter().map(|r| r.2).collect(),
                    };
                    input.consolidate();
                    sweep.load(&input, 0..input.len(), &Updates::default(), 0..0, &seeds, 0..seeds.len());
                    let (mut ins, mut outs, mut emitted) = (Vec::new(), Vec::new(), Vec::new());
                    let mut steps = 0;
                    while let Some(at) = sweep.next(&Antichain::new(), &mut ins, &mut outs) {
                        let at = at.clone();
                        steps += 1;
                        assert!(steps <= grid.len());
                        let mut corrections: Vec<_> = ins.iter().filter(|r| r.1 > 0).map(|r| (r.0, 1)).collect();
                        corrections.extend(outs.iter().map(|r| (r.0, -r.1)));
                        differential_dataflow::consolidation::consolidate(&mut corrections);
                        emitted.extend(corrections.iter().map(|&(id, d)| (id, at.clone(), d)));
                        sweep.commit(&corrections);
                        ins.clear(); outs.clear();
                    }
                    // Evaluate snapshots directly, independently of the tactic's time walk.
                    for at in &grid {
                        let mut expected = [0i64; 4];
                        let mut actual = [0i64; 4];
                        for (id, t, d) in &raw {
                            if t.less_equal(at) { expected[*id as usize] += d; }
                        }
                        for count in &mut expected { *count = i64::from(*count > 0); }
                        for (id, t, d) in &emitted {
                            if t.less_equal(at) { actual[*id as usize] += d; }
                        }
                        assert_eq!(actual, expected, "snapshot at {at:?}");
                    }
                    assert!(sweep.pending().is_empty());
                }
            }
        }
        check::<Vec<T>>();
        check::<ColTimes<T>>();
    }

}
