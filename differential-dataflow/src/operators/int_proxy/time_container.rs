//! Bulk timestamp algebra, independent of keys, differences, and operator scheduling.
//!
//! Requests describe selected rows or broadcast control times. Implementations may loop
//! over primitive lanes before requests/rows. Only `time_at` and suffix-summary queries
//! materialize individual control times; data-sized work uses the bulk methods.

use crate::lattice::Lattice;
use std::{cmp::Ordering, ops::Range};
use timely::progress::{frontier::AntichainRef, Timestamp};

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

/// A collection selection or a repeated control time.
pub enum Operand<'a, C: TimeContainer> {
    /// Selected collection rows.
    Rows(&'a C, Rows<'a>),
    /// Repeat a single time a specified number of times.
    Repeat(&'a C::Time, usize),
}
impl<C: TimeContainer> Operand<'_, C> {
    /// Number of operand rows.
    pub fn len(&self) -> usize {
        match self {
            Self::Rows(_, r) => r.len(),
            Self::Repeat(_, n) => *n,
        }
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
/// `map`, `copy`, and `meet_reduce` append; comparisons overwrite their output
/// slices, whose lengths equal the sum of request lengths. `apply` and
/// `advance_by` modify only disjoint destination ranges. `order` stably sorts
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
    /// Read a control time, not an ingestion/iteration interface.
    fn time_at(&self, row: usize) -> Self::Time;
    /// Append selected rows or repeated control times.
    fn copy(&mut self, source: Operand<'_, Self>)
    where
        Self: Sized;
    /// Append pointwise results, in request order.
    fn map(&mut self, op: Operation, requests: &[Binary<'_, Self>])
    where
        Self: Sized;
    /// Apply an operation with a broadcast time to each disjoint range.
    fn apply(&mut self, op: Operation, requests: &[(Range<usize>, Self::Time)]);
    /// Advance selected disjoint ranges by a frontier (empty frontier is identity).
    fn advance_by(&mut self, ranges: &[Range<usize>], frontier: AntichainRef<'_, Self::Time>);
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
    /// Meet of the unconsumed suffix, or None if empty.
    fn suffix_meet(&self, summaries: &Self::Suffix, position: usize) -> Option<Self::Time>;
}

fn value<'a, T: Timestamp + Lattice>(operand: &'a Operand<'_, Vec<T>>, i: usize) -> &'a T {
    match operand {
        Operand::Rows(c, r) => &c[r.at(i)],
        Operand::Repeat(t, n) => {
            assert!(i < *n);
            t
        }
    }
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
    fn time_at(&self, row: usize) -> T {
        self[row].clone()
    }
    fn copy(&mut self, source: Operand<'_, Self>) {
        for i in 0..source.len() {
            self.push(value(&source, i).clone());
        }
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
    fn apply(&mut self, op: Operation, requests: &[(Range<usize>, T)]) {
        for (range, t) in requests {
            for row in &mut self[range.clone()] {
                match op {
                    Operation::Join => row.join_assign(t),
                    Operation::Meet => row.meet_assign(t),
                }
            }
        }
    }
    fn advance_by(&mut self, ranges: &[Range<usize>], frontier: AntichainRef<'_, T>) {
        for range in ranges {
            for t in &mut self[range.clone()] {
                t.advance_by(frontier);
            }
        }
    }
    fn less_equal(requests: &[Binary<'_, Self>], output: &mut [bool]) {
        assert_eq!(output.len(), requests.iter().map(Binary::len).sum());
        let mut offset = 0;
        for r in requests {
            for i in 0..r.len() {
                output[offset + i] = value(&r.left, i).less_equal(value(&r.right, i));
            }
            offset += r.len();
        }
    }
    fn compare(requests: &[Binary<'_, Self>], output: &mut [Ordering]) {
        assert_eq!(output.len(), requests.iter().map(Binary::len).sum());
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
    fn suffix_meet(&self, summaries: &Self::Suffix, position: usize) -> Option<T> {
        summaries.as_slice().get(position).cloned()
    }
}
