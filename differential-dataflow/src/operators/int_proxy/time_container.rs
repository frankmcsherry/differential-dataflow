//! Bulk timestamp algebra, independent of keys, differences, and operator scheduling.
//!
//! Requests select collection rows. Implementations may loop over primitive lanes
//! before requests/rows. Scalar import and export belong to the driver boundary;
//! suffix-summary queries append to reusable container storage.

use crate::lattice::Lattice;
use std::{cmp::Ordering, ops::Range};
use timely::progress::Timestamp;

/// Rows to read, in output order. Indices may repeat (for example in a join).
#[derive(Clone, Debug)]
pub enum Rows<'a> {
    /// A contiguous range.
    Range(Range<usize>),
    /// A gather, possibly containing repeated indices.
    Indices(&'a [usize]),
    /// Repeat one collection row without materializing a control timestamp.
    Repeat {
        /// Source row.
        row: usize,
        /// Number of repetitions.
        count: usize,
    },
}
impl Rows<'_> {
    /// Number of selected rows.
    pub fn len(&self) -> usize {
        match self {
            Self::Range(r) => r.len(),
            Self::Indices(i) => i.len(),
            Self::Repeat { count, .. } => *count,
        }
    }
    /// Whether no rows are selected.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Source index at an output position.
    pub fn at(&self, i: usize) -> usize {
        assert!(i < self.len());
        match self {
            Self::Range(r) => r.start + i,
            Self::Indices(rows) => rows[i],
            Self::Repeat { row, .. } => *row,
        }
    }
}

/// Selected collection rows, including repeated rows.
pub struct Operand<'a, C: TimeContainer>(
    /// Source collection.
    pub &'a C,
    /// Rows to select.
    pub Rows<'a>,
);
impl<'a, C: TimeContainer> Operand<'a, C> {
    /// Broadcast a collection row without constructing an owned timestamp.
    pub fn repeat_row(times: &'a C, row: usize, count: usize) -> Self {
        Self(times, Rows::Repeat { row, count })
    }
    /// Number of operand rows.
    pub fn len(&self) -> usize {
        self.1.len()
    }
    /// Whether no rows are selected.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One pointwise request. Operands must have equal lengths.
pub struct Binary<'a, C: TimeContainer> {
    /// First operand.
    pub left: Operand<'a, C>,
    /// Second operand.
    pub right: Operand<'a, C>,
}
impl<C: TimeContainer> Binary<'_, C> {
    /// Validate and return the request's output length.
    pub fn len(&self) -> usize {
        assert_eq!(self.left.len(), self.right.len());
        self.left.len()
    }
    /// Whether the request has no output.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Pointwise lattice operation.
#[derive(Clone, Copy)]
pub enum Operation {
    /// Least upper bound.
    Join,
    /// Greatest lower bound.
    Meet,
}

/// A timestamp collection. Mutating operations retain capacity where practical.
///
/// `map`, `copy`, `meet_reduce`, and `suffix_meet` append; comparisons overwrite their output
/// slices, whose lengths equal the sum of request lengths. `apply` modifies
/// only disjoint destination ranges. `order` stably sorts
/// index segments, without moving timestamp or payload columns. Its equality
/// must agree with timestamp equality and its total order extend the partial order.
pub trait TimeContainer: Default + 'static {
    /// Logical timestamp, used for small control values and capabilities.
    type Time: Timestamp + Lattice;
    /// Reusable suffix-meet summaries; need not store one timestamp per row.
    type Suffix: Default;
    /// Reusable storage for ordering; the harness controls its lifetime.
    type OrderScratch: Default;

    /// Number of logical rows, including for zero-dimensional timestamps.
    fn len(&self) -> usize;
    /// Whether there are no logical rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Remove all rows, retaining storage.
    fn clear(&mut self);
    /// Append selected collection rows.
    fn copy(&mut self, source: Operand<'_, Self>) where Self: Sized {
        self.copy_many(&[source]);
    }
    /// Append selections in request order, allowing implementations to scan lane first.
    fn copy_many(&mut self, sources: &[Operand<'_, Self>]) where Self: Sized;
    /// Append pointwise results, in request order.
    fn map(&mut self, op: Operation, requests: &[Binary<'_, Self>])
    where
        Self: Sized;
    /// Apply a pointwise operation to disjoint ranges; each operand has its range's length.
    /// Broadcast bounds are repeated rows of another container.
    fn apply(&mut self, op: Operation, requests: &[(Range<usize>, Operand<'_, Self>)])
    where Self: Sized;
    /// Bulk partial-order comparison.
    fn less_equal(requests: &[Binary<'_, Self>], output: &mut [bool])
    where
        Self: Sized;
    /// Bulk total-order comparison.
    fn compare(requests: &[Binary<'_, Self>], output: &mut [Ordering])
    where
        Self: Sized;
    /// Stable total-order sort of the supplied index segments.
    fn order(
        &self,
        indices: &mut [usize],
        segments: &[Range<usize>],
        scratch: &mut Self::OrderScratch,
    );
    /// Append one meet for each nonempty range. Empty ranges are invalid.
    fn meet_reduce(&self, ranges: &[Range<usize>], output: &mut Self);
    /// Build summaries for this collection in its current row order.
    fn suffix_meets(&self, summaries: &mut Self::Suffix);
    /// Append the meet of the unconsumed suffix; append nothing if it is empty.
    /// The caller owns the destination and can reuse it across summary queries.
    fn suffix_meet(&self, summaries: &Self::Suffix, position: usize, output: &mut Self);
}

/// Extend an antichain entirely in collection storage. Materialize only the final
/// minimal times when handing a capability frontier back to the driver.
pub fn extend_antichain<C: TimeContainer>(minimum: &mut C, times: &C, rows: &[usize]) {
    if rows.is_empty() { return; }
    let rows = if minimum.is_empty() {
        minimum.copy(Operand(times, Rows::Range(rows[0]..rows[0] + 1)));
        &rows[1..]
    } else { rows };
    // Discard already-covered candidates lane by lane before the incremental walk.
    // Most trace rows are above the retained frontier, so this avoids a call per row.
    let mut covered = vec![false; rows.len()];
    let mut mask = vec![false; rows.len()];
    for row in 0..minimum.len() {
        C::less_equal(&[Binary {
            left: Operand::repeat_row(minimum, row, rows.len()),
            right: Operand(times, Rows::Indices(rows)),
        }], &mut mask);
        for (covered, &hit) in covered.iter_mut().zip(&mask) { *covered |= hit; }
    }
    if covered.iter().all(|&c| c) { return; }
    let mut keep = Vec::new();
    let mut scratch = C::default();
    for (&row, covered) in rows.iter().zip(covered) {
        if covered { continue; }
        mask.resize(minimum.len(), false);
        C::less_equal(
            &[Binary {
                left: Operand(minimum, Rows::Range(0..minimum.len())),
                right: Operand::repeat_row(times, row, minimum.len()),
            }],
            &mut mask,
        );
        if mask.iter().any(|&v| v) {
            continue;
        }
        C::less_equal(
            &[Binary {
                left: Operand::repeat_row(times, row, minimum.len()),
                right: Operand(minimum, Rows::Range(0..minimum.len())),
            }],
            &mut mask,
        );
        keep.clear();
        keep.extend((0..minimum.len()).filter(|&r| !mask[r]));
        if keep.len() != minimum.len() {
            scratch.clear();
            scratch.copy(Operand(minimum, Rows::Indices(&keep)));
            std::mem::swap(minimum, &mut scratch);
        }
        minimum.copy(Operand(times, Rows::Range(row..row + 1)));
    }
}

fn value<'a, T: Timestamp + Lattice>(operand: &'a Operand<'_, Vec<T>>, i: usize) -> &'a T {
    &operand.0[operand.1.at(i)]
}

impl<T: Timestamp + Lattice> TimeContainer for Vec<T> {
    type Time = T;
    type Suffix = Vec<T>;
    type OrderScratch = ();
    fn len(&self) -> usize {
        self.len()
    }
    fn clear(&mut self) {
        self.clear();
    }
    fn copy_many(&mut self, sources: &[Operand<'_, Self>]) {
        self.reserve(sources.iter().map(Operand::len).sum());
        for source in sources.iter().filter(|s| !s.is_empty()) { match source {
            Operand(c, Rows::Range(r)) => self.extend_from_slice(&c[r.clone()]),
            Operand(c, Rows::Indices(rows)) => {
                self.extend(rows.iter().map(|&r| c[r].clone()))
            }
            Operand(c, Rows::Repeat { row, count }) => {
                self.resize_with(self.len() + count, || c[*row].clone())
            }
        } }
    }
    fn map(&mut self, op: Operation, requests: &[Binary<'_, Self>]) {
        for r in requests {
            for i in 0..r.len() {
                let (a, b) = (value(&r.left, i), value(&r.right, i));
                self.push(match op {
                    Operation::Join => a.join(b),
                    Operation::Meet => a.meet(b),
                });
            }
        }
    }
    fn apply(&mut self, op: Operation, requests: &[(Range<usize>, Operand<'_, Self>)]) {
        for (range, source) in requests {
            assert_eq!(range.len(), source.len());
            for (i, row) in self[range.clone()].iter_mut().enumerate() {
                let t = value(source, i);
                match op {
                    Operation::Join => row.join_assign(t),
                    Operation::Meet => row.meet_assign(t),
                }
            }
        }
    }
    fn less_equal(requests: &[Binary<'_, Self>], output: &mut [bool]) {
        assert_eq!(
            output.len(),
            requests.iter().map(Binary::len).sum::<usize>()
        );
        let mut offset = 0;
        for r in requests {
            for i in 0..r.len() {
                output[offset + i] = value(&r.left, i).less_equal(value(&r.right, i));
            }
            offset += r.len();
        }
    }
    fn compare(requests: &[Binary<'_, Self>], output: &mut [Ordering]) {
        assert_eq!(
            output.len(),
            requests.iter().map(Binary::len).sum::<usize>()
        );
        let mut offset = 0;
        for r in requests {
            for i in 0..r.len() {
                output[offset + i] = value(&r.left, i).cmp(value(&r.right, i));
            }
            offset += r.len();
        }
    }
    fn order(&self, indices: &mut [usize], segments: &[Range<usize>], _: &mut ()) {
        for segment in segments {
            indices[segment.clone()].sort_by(|&a, &b| self[a].cmp(&self[b]));
        }
    }
    fn meet_reduce(&self, ranges: &[Range<usize>], output: &mut Self) {
        for r in ranges {
            assert!(!r.is_empty());
            let mut meet = self[r.start].clone();
            for t in &self[r.start + 1..r.end] {
                meet.meet_assign(t);
            }
            output.push(meet);
        }
    }
    fn suffix_meets(&self, summaries: &mut Self::Suffix) {
        summaries.clone_from(self);
        for i in (1..summaries.len()).rev() {
            let (a, b) = summaries.split_at_mut(i);
            a[i - 1].meet_assign(&b[0]);
        }
    }
    fn suffix_meet(&self, summaries: &Self::Suffix, position: usize, output: &mut Self) {
        if let Some(t) = summaries.get(position) {
            output.push(t.clone());
        }
    }
}
