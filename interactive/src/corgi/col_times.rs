//! Product timestamps in shared primitive lanes. Owned timestamps belong at control and I/O
//! boundaries; data-sized operations below work on one coordinate column at a time.
use columnar::Columnar;
use differential_dataflow::{dynamic::pointstamp::PointStamp, lattice::Lattice};
use std::{cmp::Ordering, marker::PhantomData, sync::Arc};
use timely::{
    order::Product,
    progress::{frontier::AntichainRef, Timestamp},
};

/// The product-of-unsigned-primitives time domain used by Corgi. Absent dynamic coordinates
/// are zero. This deliberately does not claim to implement arbitrary lattice timestamps.
/// Implementations must preserve lexicographic order, product partial order, coordinatewise
/// min/max, and the checked-add/truncate summary semantics under this representation.
pub trait ColTime: Timestamp + Lattice + Columnar {
    fn width(&self) -> usize;
    fn coordinate(&self, lane: usize) -> u64;
    fn from_coordinates(width: usize, coordinate: impl Fn(usize) -> u64) -> Self;
    fn valid_width(width: usize) -> bool;
    /// Truncate first, then add these coordinate summaries (checked for overflow).
    fn summary(summary: &Self::Summary) -> (Option<usize>, Vec<u64>);
    fn maximum(lane: usize) -> u64;
}

macro_rules! primitive {
    ($($t:ty),*) => {$(impl ColTime for $t {
        fn width(&self) -> usize { 1 }
        fn coordinate(&self, lane: usize) -> u64 { if lane == 0 { *self as u64 } else { 0 } }
        fn from_coordinates(_: usize, coordinate: impl Fn(usize) -> u64) -> Self { coordinate(0).try_into().expect("timestamp coordinate overflow") }
        fn valid_width(width: usize) -> bool { width == 1 }
        fn summary(summary: &Self::Summary) -> (Option<usize>, Vec<u64>) { (None, vec![*summary as u64]) }
        fn maximum(_: usize) -> u64 { <$t>::MAX as u64 }
    })*};
}
primitive!(u64, u32, usize);

impl ColTime for () {
    fn width(&self) -> usize {
        0
    }
    fn coordinate(&self, _: usize) -> u64 {
        0
    }
    fn from_coordinates(_: usize, _: impl Fn(usize) -> u64) -> Self {}
    fn valid_width(width: usize) -> bool {
        width == 0
    }
    fn summary(_: &Self::Summary) -> (Option<usize>, Vec<u64>) {
        (None, vec![])
    }
    fn maximum(_: usize) -> u64 {
        0
    }
}

impl ColTime for PointStamp<u64> {
    fn width(&self) -> usize {
        self.len()
    }
    fn coordinate(&self, lane: usize) -> u64 {
        self.get(lane).copied().unwrap_or(0)
    }
    fn from_coordinates(width: usize, coordinate: impl Fn(usize) -> u64) -> Self {
        Self::new((0..width).map(coordinate).collect())
    }
    fn valid_width(_: usize) -> bool {
        true
    }
    fn summary(summary: &Self::Summary) -> (Option<usize>, Vec<u64>) {
        (summary.retain, summary.actions.clone())
    }
    fn maximum(_: usize) -> u64 {
        u64::MAX
    }
}

impl<I: ColTime> ColTime for Product<u64, I> {
    fn width(&self) -> usize {
        1 + self.inner.width()
    }
    fn coordinate(&self, lane: usize) -> u64 {
        if lane == 0 {
            self.outer
        } else {
            self.inner.coordinate(lane - 1)
        }
    }
    fn from_coordinates(width: usize, coordinate: impl Fn(usize) -> u64) -> Self {
        Product::new(
            coordinate(0),
            I::from_coordinates(width - 1, |i| coordinate(i + 1)),
        )
    }
    fn valid_width(width: usize) -> bool {
        width > 0 && I::valid_width(width - 1)
    }
    fn summary(summary: &Self::Summary) -> (Option<usize>, Vec<u64>) {
        let (retain, inner) = I::summary(&summary.inner);
        (
            retain.map(|r| r + 1),
            std::iter::once(summary.outer).chain(inner).collect(),
        )
    }
    fn maximum(lane: usize) -> u64 {
        if lane == 0 {
            u64::MAX
        } else {
            I::maximum(lane - 1)
        }
    }
}

/// K primitive lanes with a common row count, including the zero-dimensional product.
/// Clone shares each lane; mutation copies only lanes whose contents change.
#[derive(Clone, Debug)]
pub struct ColTimes<T: ColTime> {
    pub(crate) lanes: Vec<Arc<Vec<u64>>>,
    pub(crate) rows: usize,
    marker: PhantomData<T>,
}

impl<T: ColTime> Default for ColTimes<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: ColTime> ColTimes<T> {
    pub fn new() -> Self {
        Self::from_lanes(vec![Vec::new(); T::minimum().width()], 0)
    }
    pub(crate) fn from_lanes(lanes: Vec<Vec<u64>>, rows: usize) -> Self {
        assert!(
            T::valid_width(lanes.len()),
            "invalid product timestamp width"
        );
        assert!(lanes.iter().all(|l| l.len() == rows));
        Self {
            lanes: lanes.into_iter().map(Arc::new).collect(),
            rows,
            marker: PhantomData,
        }
    }
    pub fn len(&self) -> usize {
        self.rows
    }
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
    pub fn width(&self) -> usize {
        self.lanes.len()
    }
    /// Full antichain membership, rather than membership above the frontier's meet.
    pub(crate) fn beyond(&self, frontier: AntichainRef<T>) -> Vec<bool> {
        let mut beyond = vec![false; self.len()];
        let mut dominated = vec![true; self.len()];
        for time in frontier.iter() {
            dominated.fill(true);
            for j in 0..self.width().max(time.width()) {
                let bound = time.coordinate(j);
                if let Some(lane) = self.lane(j) {
                    for (flag, &x) in dominated.iter_mut().zip(lane) {
                        *flag &= bound <= x;
                    }
                } else if bound > 0 {
                    dominated.fill(false);
                }
            }
            for (keep, &flag) in beyond.iter_mut().zip(&dominated) {
                *keep |= flag;
            }
        }
        beyond
    }
    pub fn lane(&self, lane: usize) -> Option<&[u64]> {
        self.lanes.get(lane).map(|l| l.as_slice())
    }
    pub fn coordinate(&self, lane: usize, row: usize) -> u64 {
        assert!(row < self.rows);
        self.lanes.get(lane).map_or(0, |l| l[row])
    }
    pub(crate) fn ensure_width(&mut self, width: usize) {
        while self.width() < width {
            self.lanes.push(Arc::new(vec![0; self.rows]));
        }
    }
    pub fn clear(&mut self) {
        if self.rows == 0 { return; }
        for l in &mut self.lanes {
            // Clearing shared storage should not first copy its old contents.
            if let Some(lane) = Arc::get_mut(l) {
                lane.clear();
            } else {
                *l = Arc::new(Vec::new());
            }
        }
        self.rows = 0;
    }
    pub fn push(&mut self, time: &T) {
        self.ensure_width(time.width());
        for (i, l) in self.lanes.iter_mut().enumerate() {
            Arc::make_mut(l).push(time.coordinate(i));
        }
        self.rows += 1;
    }
    pub fn push_ref(&mut self, other: &Self, row: usize) {
        self.push_range(other, row, row + 1);
    }
    pub fn push_range(&mut self, other: &Self, start: usize, end: usize) {

        assert!(start <= end && end <= other.len());
        if start == end {
            return;
        }
        assert!(T::valid_width(self.width().max(other.width())));
        self.ensure_width(other.width());
        for (i, lane) in self.lanes.iter_mut().enumerate() {
            let lane = Arc::make_mut(lane);
            if let Some(src) = other.lane(i) {
                lane.extend_from_slice(&src[start..end]);
            } else {
                lane.resize(lane.len() + end - start, 0);
            }
        }
        self.rows += end - start;
    }
    pub fn get(&self, row: usize) -> T {
        T::from_coordinates(self.width(), |i| self.coordinate(i, row))
    }
    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        (0..self.len()).map(|r| self.get(r))
    }
    pub fn to_vec(&self) -> Vec<T> {
        self.iter().collect()
    }
    pub fn gather(&self, index: &[usize]) -> Self {
        Self::from_lanes(
            self.lanes
                .iter()
                .map(|l| index.iter().map(|&i| l[i]).collect())
                .collect(),
            index.len(),
        )
    }
    pub fn cmp(&self, a: usize, b: usize) -> Ordering {
        self.cmp_cross(a, self, b)
    }
    pub fn cmp_cross(&self, a: usize, other: &Self, b: usize) -> Ordering {
        (0..self.width().max(other.width()))
            .map(|j| self.coordinate(j, a).cmp(&other.coordinate(j, b)))
            .find(|c| *c != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    }
    /// On a product of chains, advance_by(F) = join(meet(F)) for nonempty F.
    /// Empty frontiers leave timestamps unchanged, as in DD's Lattice implementation.
    pub fn advance_by(&mut self, frontier: AntichainRef<T>) {
        if frontier.is_empty() {
            return;
        }
        self.ensure_width(frontier.iter().map(ColTime::width).max().unwrap());
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            let floor = frontier.iter().map(|t| t.coordinate(j)).min().unwrap();
            if floor != 0 {
                for x in Arc::make_mut(lane) {
                    *x = (*x).max(floor);
                }
            }
        }
    }
    pub fn join_assign(&mut self, lower: &T) {
        self.ensure_width(lower.width());
        for (j, lane) in self.lanes.iter_mut().enumerate() {
            let floor = lower.coordinate(j);
            if floor != 0 {
                for x in Arc::make_mut(lane) {
                    *x = (*x).max(floor);
                }
            }
        }
    }
    /// Apply feedback summaries lane by lane, returning the surviving source rows.
    pub fn results_in(&mut self, summary: &T::Summary) -> Vec<usize> {
        let (retain, actions) = T::summary(summary);
        if let Some(retain) = retain {
            self.lanes.truncate(retain);
        }
        self.ensure_width(actions.len());
        let mut alive = vec![true; self.rows];
        for (j, (&action, lane)) in actions.iter().zip(&mut self.lanes).enumerate() {
            if action == 0 {
                continue;
            }
            for (x, live) in Arc::make_mut(lane).iter_mut().zip(&mut alive) {
                if let Some(sum) = x.checked_add(action).filter(|s| *s <= T::maximum(j)) {
                    *x = sum;
                } else {
                    *live = false;
                }
            }
        }
        let keep: Vec<_> = (0..self.rows).filter(|&r| alive[r]).collect();
        if keep.len() != self.rows {
            *self = self.gather(&keep);
        }
        keep
    }
    /// Stable primitive sort of selected rows; payloads move only after the permutation is known.
    pub fn sort_indices(&self, index: &mut Vec<usize>) {
        radix_sort(self.lanes.iter().map(|l| l.as_slice()), index);
    }
}

/// LSD radix order, reading each lane through the selection once, then making contiguous
/// byte passes through key/index scratch. Constant bytes incur no scatter pass.
pub(crate) fn radix_sort<'a>(
    lanes: impl DoubleEndedIterator<Item = &'a [u64]>,
    index: &mut Vec<usize>,
) {
    radix_sort_with(lanes, index, &mut RadixScratch::default());
}

#[derive(Default)]
pub(crate) struct RadixScratch {
    alternate: Vec<usize>,
    keys: Vec<u64>,
    other_keys: Vec<u64>,
}
pub(crate) fn radix_sort_with<'a>(
    lanes: impl DoubleEndedIterator<Item = &'a [u64]>,
    index: &mut Vec<usize>,
    scratch: &mut RadixScratch,
) {
    if index.len() < 2 {
        return;
    }
    let RadixScratch {
        alternate,
        keys,
        other_keys,
    } = scratch;
    for lane in lanes.rev() {
        keys.clear();
        keys.extend(index.iter().map(|&i| lane[i]));
        // This primitive lane is already ordered under the current selection.
        // A stable sort would preserve it exactly, including ties.
        if keys.windows(2).all(|w| w[0] <= w[1]) {
            continue;
        }
        // Tiny selections cannot amortize a 256-bucket scatter. Stable insertion
        // orders this one primitive lane and its row selection; timestamp tuples
        // are never compared or materialized. The quadratic work is bounded.
        if index.len() <= 16 {
            for i in 1..keys.len() {
                let (key, row) = (keys[i], index[i]);
                let mut dest = i;
                while dest > 0 && keys[dest - 1] > key {
                    keys[dest] = keys[dest - 1];
                    index[dest] = index[dest - 1];
                    dest -= 1;
                }
                keys[dest] = key;
                index[dest] = row;
            }
            continue;
        }
        alternate.resize(index.len(), 0);
        other_keys.resize(index.len(), 0);
        let changed = keys.iter().fold(0, |a, x| a | (x ^ keys[0]));
        for byte in 0..8 {
            let shift = byte * 8;
            if changed >> shift & 255 == 0 {
                continue;
            }
            let mut starts = [0usize; 256];
            for &key in keys.iter() {
                starts[(key >> shift & 255) as usize] += 1;
            }
            let mut sum = 0;
            for slot in &mut starts {
                let n = *slot;
                *slot = sum;
                sum += n;
            }
            for (&key, &row) in keys.iter().zip(index.iter()) {
                let slot = &mut starts[(key >> shift & 255) as usize];
                alternate[*slot] = row;
                other_keys[*slot] = key;
                *slot += 1;
            }
            std::mem::swap(index, alternate);
            std::mem::swap(keys, other_keys);
        }
    }
}

impl<T: ColTime> FromIterator<T> for ColTimes<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut result = Self::new();
        for t in iter {
            result.push(&t);
        }
        result
    }
}
impl<T: ColTime> From<Vec<T>> for ColTimes<T> {
    fn from(value: Vec<T>) -> Self {
        value.into_iter().collect()
    }
}
impl<T: ColTime> IntoIterator for ColTimes<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.to_vec().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::dynamic::pointstamp::PointStampSummary;
    use timely::progress::{Antichain, PathSummary};
    type T = Product<u64, PointStamp<u64>>;
    fn t(outer: u64, inner: &[u64]) -> T {
        Product::new(outer, PointStamp::new(inner.iter().copied().collect()))
    }
    #[test]
    fn primitive_sort_matches_row_order() {
        let mut state = 12345u64;
        for width in 0..=4 {
            for len in 0..64 {
                let mut rows: Vec<Vec<u64>> = (0..len)
                    .map(|_| {
                        (0..width)
                            .map(|_| {
                                state ^= state << 13;
                                state ^= state >> 7;
                                state ^= state << 17;
                                state % 4
                            })
                            .collect()
                    })
                    .collect();
                for sorted in [false, true] {
                    if sorted {
                        rows.sort();
                    }
                    let lanes: Vec<Vec<u64>> = (0..width)
                        .map(|j| rows.iter().map(|r| r[j]).collect())
                        .collect();
                    let mut indices: Vec<_> = (0..len).rev().collect();
                    let mut expected = indices.clone();
                    expected.sort_by_key(|&i| &rows[i]);
                    radix_sort(lanes.iter().map(Vec::as_slice), &mut indices);
                    assert_eq!(indices, expected, "stable ordering including ties");
                }
            }
        }
    }

    #[test]
    fn product_algebra_matches_owned_timestamps() {
        let rows = vec![
            t(0, &[]),
            t(1, &[0, 2]),
            t(0, &[1]),
            t(2, &[3, 1, 4]),
            t(u64::MAX, &[u64::MAX]),
        ];
        let source: ColTimes<_> = rows.clone().into();
        for a in 0..rows.len() {
            for b in 0..rows.len() {
                assert_eq!(source.cmp(a, b), rows[a].cmp(&rows[b]));
            }
        }
        for subset in 0..1 << rows.len() {
            let frontier: Antichain<_> = rows
                .iter()
                .enumerate()
                .filter(|(i, _)| subset & (1 << i) != 0)
                .map(|(_, t)| t.clone())
                .collect();
            let mut times = source.clone();
            times.advance_by(frontier.borrow());
            let expected: Vec<_> = rows
                .iter()
                .cloned()
                .map(|mut t| {
                    t.advance_by(frontier.borrow());
                    t
                })
                .collect();
            assert_eq!(times.to_vec(), expected);
            let mut order: Vec<_> = (0..rows.len()).collect();
            times.sort_indices(&mut order);
            let mut sorted = expected.clone();
            sorted.sort();
            assert_eq!(times.gather(&order).to_vec(), sorted);
        }
        for retain in [None, Some(0), Some(1), Some(4)] {
            for actions in [vec![], vec![1], vec![0, u64::MAX, 2]] {
                let summary = Product::new(1, PointStampSummary { retain, actions });
                let mut times = source.clone();
                let keep = times.results_in(&summary);
                let expected: Vec<_> = rows.iter().filter_map(|t| summary.results_in(t)).collect();
                assert_eq!(times.to_vec(), expected);
                assert_eq!(
                    keep,
                    (0..rows.len())
                        .filter(|&r| summary.results_in(&rows[r]).is_some())
                        .collect::<Vec<_>>()
                );
            }
        }
        assert_eq!(
            source.to_vec(),
            rows,
            "copy-on-write must preserve the shared input"
        );
    }
    #[test]
    fn sharing_ranges_and_zero_dimensions() {
        let source: ColTimes<_> = vec![t(0, &[]), t(1, &[2]), t(2, &[3, 4])].into();
        let copy = source.clone();
        assert!(source
            .lanes
            .iter()
            .zip(&copy.lanes)
            .all(|(a, b)| Arc::ptr_eq(a, b)));
        let mut dest: ColTimes<_> = vec![t(8, &[])].into();
        dest.push_range(&source, 1, 3);
        assert_eq!(dest.to_vec(), vec![t(8, &[]), t(1, &[2]), t(2, &[3, 4])]);
        let mut unit: ColTimes<()> = vec![(); 5].into();
        assert_eq!(unit.width(), 0);
        unit.push_range(&vec![(); 3].into(), 0, 3);
        assert_eq!(unit.gather(&[7, 1, 0]).to_vec(), vec![(); 3]);
    }
}
