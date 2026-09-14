//! Aligned proxy columns and shared ordering helpers.
use super::time_container::{Binary, Operand, Rows, TimeContainer};
use crate::difference::Semigroup;
use std::{cmp::Ordering, ops::Range};

/// Raw key/time support, without value identities or differences.
/// Deduplication preserves support even when the corresponding updates cancel.
pub struct Keyed<C> {
    /// Independent key hashes.
    pub keys: Vec<u64>,
    /// One timestamp per key association.
    pub times: C,
}
impl<C: Default> Default for Keyed<C> {
    fn default() -> Self { Self { keys: vec![], times: C::default() } }
}
impl<C: TimeContainer> Keyed<C> {
    /// Number of key/time associations.
    pub fn len(&self) -> usize { self.keys.len() }
    /// Whether there are no associations.
    pub fn is_empty(&self) -> bool { self.keys.is_empty() }
    /// Clear while retaining allocation.
    pub fn clear(&mut self) { self.keys.clear(); self.times.clear(); }
    /// Append selected associations without deduplicating them.
    pub fn append_range(&mut self, source: &Self, range: Range<usize>) {
        self.times.copy(Operand(&source.times, Rows::Range(range.clone())));
        self.keys.extend_from_slice(&source.keys[range]);
    }
    /// Sort and deduplicate by (key, time).
    pub fn consolidate(&mut self) { self.normalize(&mut Scratch::default()); }
    pub(super) fn normalize(&mut self, scratch: &mut Scratch<C, ()>) {
        assert_eq!(self.times.len(), self.len());
        scratch.index.clear(); scratch.index.extend(0..self.len());
        adjacent(&self.times, &scratch.index, &mut scratch.comparisons);
        if (1..self.len()).all(|i| self.keys[i - 1].cmp(&self.keys[i])
            .then(scratch.comparisons[i - 1]) == Ordering::Less) { return; }
        order_by(&self.times, scratch, |r| self.keys[r]);
        adjacent(&self.times, &scratch.index, &mut scratch.comparisons);
        scratch.keep.clear();
        for (i, &r) in scratch.index.iter().enumerate() {
            if i == 0 || self.keys[r] != self.keys[scratch.index[i - 1]]
                || scratch.comparisons[i - 1] != Ordering::Equal { scratch.keep.push(r); }
        }
        scratch.keys.clear(); scratch.times.clear();
        scratch.keys.extend(scratch.keep.iter().map(|&r| self.keys[r]));
        scratch.times.copy(Operand(&self.times, Rows::Indices(&scratch.keep)));
        std::mem::swap(&mut self.keys, &mut scratch.keys);
        std::mem::swap(&mut self.times, &mut scratch.times);
    }
}

/// Parallel proxy key, identity, timestamp, and difference columns.
pub struct Updates<C, R> {
    /// Independent key hashes.
    pub keys: Vec<u64>,
    /// Backend-owned value identities.
    pub ids: Vec<u64>,
    /// Timestamp storage with the same row count as the other columns.
    pub times: C,
    /// Update differences.
    pub diffs: Vec<R>,
}
impl<C: Default, R> Default for Updates<C, R> {
    fn default() -> Self {
        Self {
            keys: vec![],
            ids: vec![],
            times: C::default(),
            diffs: vec![],
        }
    }
}
impl<C: TimeContainer, R: Semigroup> Updates<C, R> {
    /// Number of updates.
    pub fn len(&self) -> usize {
        self.diffs.len()
    }
    /// Whether there are no updates.
    pub fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }
    /// Clear while retaining allocation.
    pub fn clear(&mut self) {
        self.keys.clear();
        self.ids.clear();
        self.times.clear();
        self.diffs.clear();
    }
    /// Append a presentation, without consolidating it.
    pub fn extend(&mut self, other: Self) {
        self.times
            .copy(Operand(&other.times, Rows::Range(0..other.len())));
        self.keys.extend(other.keys);
        self.ids.extend(other.ids);
        self.diffs.extend(other.diffs);
    }
    /// Sort and consolidate by (key, identity, time).
    pub fn consolidate(&mut self) {
        self.normalize(&mut Scratch::default());
    }
    pub(super) fn normalize(&mut self, scratch: &mut Scratch<C, R>) {
        assert_eq!(self.times.len(), self.len());
        assert_eq!(self.ids.len(), self.len());
        assert_eq!(self.keys.len(), self.len());
        scratch.index.clear();
        scratch.index.extend(0..self.len());
        adjacent(&self.times, &scratch.index, &mut scratch.comparisons);
        let already_unique = (1..self.len()).all(|i| {
            (self.keys[i - 1], self.ids[i - 1])
                .cmp(&(self.keys[i], self.ids[i]))
                .then(scratch.comparisons[i - 1])
                == Ordering::Less
        });
        if already_unique && self.diffs.iter().all(|d| !d.is_zero()) {
            return;
        }
        order_by(&self.times, scratch, |r| (self.keys[r], self.ids[r]));
        adjacent(&self.times, &scratch.index, &mut scratch.comparisons);
        scratch.keep.clear();
        scratch.data.clear();
        let mut pos = 0;
        while pos < self.len() {
            let row = scratch.index[pos];
            let mut diff = self.diffs[row].clone();
            pos += 1;
            while pos < self.len()
                && scratch.comparisons[pos - 1] == Ordering::Equal
                && (self.keys[row], self.ids[row])
                    == (self.keys[scratch.index[pos]], self.ids[scratch.index[pos]])
            {
                diff.plus_equals(&self.diffs[scratch.index[pos]]);
                pos += 1;
            }
            if !diff.is_zero() {
                scratch.keep.push(row);
                scratch.data.push(diff);
            }
        }
        scratch.keys.clear();
        scratch.ids.clear();
        scratch.times.clear();
        scratch
            .keys
            .extend(scratch.keep.iter().map(|&r| self.keys[r]));
        scratch
            .ids
            .extend(scratch.keep.iter().map(|&r| self.ids[r]));
        scratch
            .times
            .copy(Operand(&self.times, Rows::Indices(&scratch.keep)));
        std::mem::swap(&mut self.keys, &mut scratch.keys);
        std::mem::swap(&mut self.ids, &mut scratch.ids);
        std::mem::swap(&mut self.times, &mut scratch.times);
        std::mem::swap(&mut self.diffs, &mut scratch.data);
    }
    pub(super) fn append_range(&mut self, source: &Self, range: Range<usize>) {
        self.times
            .copy(Operand(&source.times, Rows::Range(range.clone())));
        self.keys.extend_from_slice(&source.keys[range.clone()]);
        self.ids.extend_from_slice(&source.ids[range.clone()]);
        self.diffs.extend_from_slice(&source.diffs[range]);
    }
}

pub(super) struct Scratch<C: TimeContainer, R> {
    pub(super) index: Vec<usize>,
    pub(super) keep: Vec<usize>,
    pub(super) segments: Vec<Range<usize>>,
    pub(super) comparisons: Vec<Ordering>,
    pub(super) times: C,
    pub(super) keys: Vec<u64>,
    pub(super) ids: Vec<u64>,
    pub(super) data: Vec<R>,
    pub(super) order: C::OrderScratch,
}
impl<C: TimeContainer, R> Default for Scratch<C, R> {
    fn default() -> Self {
        Self {
            index: vec![],
            keep: vec![],
            segments: vec![],
            comparisons: vec![],
            times: C::default(),
            keys: vec![],
            ids: vec![],
            data: vec![],
            order: C::OrderScratch::default(),
        }
    }
}

// Order identities first, then timestamp indices within each identity's segment.
fn order_by<C: TimeContainer, R, K: Ord>(times: &C, scratch: &mut Scratch<C, R>, key: impl Fn(usize) -> K) {
    scratch.index.sort_by_key(|&r| key(r));
    scratch.segments.clear();
    let mut start = 0;
    while start < scratch.index.len() {
        let mut end = start + 1;
        while end < scratch.index.len() && key(scratch.index[start]) == key(scratch.index[end]) { end += 1; }
        scratch.segments.push(start..end); start = end;
    }
    times.order(&mut scratch.index, &scratch.segments, &mut scratch.order);
}

pub(super) fn adjacent<C: TimeContainer>(times: &C, index: &[usize], output: &mut Vec<Ordering>) {
    output.resize(index.len().saturating_sub(1), Ordering::Equal);
    if index.len() > 1 {
        C::compare(
            &[Binary {
                left: Operand(times, Rows::Indices(&index[..index.len() - 1])),
                right: Operand(times, Rows::Indices(&index[1..])),
            }],
            output,
        );
    }
}

pub(super) fn unique<C: TimeContainer>(times: &mut C, scratch: &mut Scratch<C, ()>) {
    if times.len() <= 1 {
        return;
    }
    scratch.index.clear();
    scratch.index.extend(0..times.len());
    times.order(&mut scratch.index, &[0..times.len()], &mut scratch.order);
    adjacent(times, &scratch.index, &mut scratch.comparisons);
    scratch.keep.clear();
    for (i, &row) in scratch.index.iter().enumerate() {
        if i == 0 || scratch.comparisons[i - 1] != Ordering::Equal {
            scratch.keep.push(row);
        }
    }
    scratch.times.clear();
    scratch
        .times
        .copy(Operand(times, Rows::Indices(&scratch.keep)));
    std::mem::swap(times, &mut scratch.times);
}

pub(super) fn visible<C: TimeContainer>(times: &C, at: (&C, usize), output: &mut Vec<bool>) {
    output.resize(times.len(), false);
    C::less_equal(
        &[Binary {
            left: Operand(times, Rows::Range(0..times.len())),
            right: Operand::repeat_row(at.0, at.1, times.len()),
        }],
        output,
    );
}

pub(super) fn beyond<C: TimeContainer>(
    times: &C,
    upper: &C,
    result: &mut Vec<bool>,
    scratch: &mut Vec<bool>,
) {
    result.clear();
    result.resize(times.len(), false);
    scratch.resize(times.len(), false);
    for f in 0..upper.len() {
        C::less_equal(
            &[Binary {
                left: Operand::repeat_row(upper, f, times.len()),
                right: Operand(times, Rows::Range(0..times.len())),
            }],
            scratch,
        );
        for (r, &s) in result.iter_mut().zip(scratch.iter()) {
            *r |= s;
        }
    }
}
