//! A timestamp type as in Naiad, where a vector of timestamps of different lengths are comparable.
//!
//! This type compares using "standard" tuple logic as if each timestamp were extended indefinitely with minimal elements.
//!
//! The path summary for this type allows *run-time* rather than *type-driven* iterative scopes.
//! Each summary represents some journey within and out of some number of scopes, followed by entry
//! into and iteration within some other number of scopes.
//!
//! As a result, summaries describe some number of trailing coordinates to truncate, and some increments
//! to the resulting vector. Structurally, the increments can only be to one non-truncated coordinate
//! (as iteration within a scope requires leaving contained scopes), and then to any number of appended
//! default coordinates (which is effectively just *setting* the coordinate).

use columnar::Columnar;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// A sequence of timestamps, partially ordered by the product order.
///
/// Sequences of different lengths are compared as if extended indefinitely by `T::minimum()`.
/// Sequences are guaranteed to be "minimal", and may not end with `T::minimum()` entries.
#[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize, Columnar)]
#[columnar(derive(Eq, PartialEq, Ord, PartialOrd))]
pub struct PointStamp<T> {
    /// A sequence of timestamps corresponding to timestamps in a sequence of nested scopes.
    vector: SmallVec<[T; 2]>,
}

impl<T: Timestamp> PartialEq<[T]> for PointStamp<T> {
    fn eq(&self, other: &[T]) -> bool {
        self.vector.iter()
            .zip(other.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.eq(t2))
    }
}

impl<T: Timestamp> PartialEq<PointStamp<T>> for [T] {
    fn eq(&self, other: &PointStamp<T>) -> bool {
        self.iter()
            .zip(other.vector.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.eq(t2))
    }
}

impl<T: Timestamp> PartialOrder<[T]> for PointStamp<T> {
    fn less_equal(&self, other: &[T]) -> bool {
        self.vector.iter()
            .zip(other.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.less_equal(t2))
    }
}

impl<T: Timestamp> PartialOrder<PointStamp<T>> for [T] {
    fn less_equal(&self, other: &PointStamp<T>) -> bool {
        self.iter()
            .zip(other.vector.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.less_equal(t2))
    }
}

impl<T: Timestamp> PointStamp<T> {
    /// Create a new sequence.
    ///
    /// This method will modify `vector` to ensure it does not end with `T::minimum()`.
    pub fn new(mut vector: SmallVec<[T; 1]>) -> Self {
        while vector.last() == Some(&T::minimum()) {
            vector.pop();
        }
        // Preserve the public constructor's input type. Longer vectors transfer
        // their allocation; the two common iteration coordinates stay inline.
        let vector = if vector.len() > 2 {
            SmallVec::from_vec(vector.into_vec())
        } else {
            vector.into_iter().collect()
        };
        PointStamp { vector }
    }

    fn from_inline(mut vector: SmallVec<[T; 2]>) -> Self {
        while vector.last() == Some(&T::minimum()) { vector.pop(); }
        PointStamp { vector }
    }
    /// Retain at most `len` coordinates and remove trailing minimums in place.
    /// This preserves the timestamp's canonical representation without converting
    /// through the public one-coordinate small-vector representation.
    pub fn truncate(&mut self, len: usize) {
        self.vector.truncate(len);
        while self.vector.last() == Some(&T::minimum()) { self.vector.pop(); }
    }

    /// Returns the wrapped small vector.
    ///
    /// Contents can be changed by extracting
    /// the vector and then re-introducing it with `PointStamp::new` to re-establish
    /// the invariant that the vector not end with `T::minimum`.
    pub fn into_inner(self) -> SmallVec<[T; 1]> {
        if self.vector.len() > 2 {
            SmallVec::from_vec(self.vector.into_vec())
        } else {
            self.vector.into_iter().collect()
        }
    }
}

impl<T> std::ops::Deref for PointStamp<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.vector
    }
}

impl<T: Timestamp> FromIterator<T> for PointStamp<T> {
    /// Collect coordinates directly into the inline representation and remove
    /// trailing minimums, without passing through a smaller public small vector.
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_inline(iter.into_iter().collect())
    }
}

// Implement timely dataflow's `PartialOrder` trait.
use timely::order::PartialOrder;
impl<T: Timestamp> PartialOrder for PointStamp<T> {
    fn less_equal(&self, other: &Self) -> bool {
        // Every present coordinate must be less-equal the corresponding coordinate,
        // where absent corresponding coordinates are `T::minimum()`. Coordinates
        // absent from `self.vector` are themselves `T::minimum()` and are less-equal
        // any corresponding coordinate in `other.vector`.
        self.vector
            .iter()
            .zip(other.vector.iter().chain(std::iter::repeat(&T::minimum())))
            .all(|(t1, t2)| t1.less_equal(t2))
    }
}

use timely::progress::timestamp::Refines;
impl<T: Timestamp> Refines<()> for PointStamp<T> {
    fn to_inner(_outer: ()) -> Self {
        Self { vector: Default::default() }
    }
    fn to_outer(self) -> () {
        ()
    }
    fn summarize(_summary: <Self>::Summary) -> () {
        ()
    }
}

// Implement timely dataflow's `PathSummary` trait.
// This is preparation for the `Timestamp` implementation below.
use timely::progress::PathSummary;

/// Describes an action on a `PointStamp`: truncation to `length` followed by `actions`.
#[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct PointStampSummary<TS> {
    /// Number of leading coordinates to retain.
    ///
    /// A `None` value indicates that all coordinates should be retained.
    pub retain: Option<usize>,
    /// Summary actions to apply to all coordinates.
    ///
    /// If `actions.len()` is greater than `retain`, a timestamp should be extended by
    /// `T::minimum()` in order to be subjected to `actions`.
    pub actions: Vec<TS>,
}

impl<T: Timestamp> PathSummary<PointStamp<T>> for PointStampSummary<T::Summary> {
    fn results_in(&self, timestamp: &PointStamp<T>) -> Option<PointStamp<T>> {
        // Get a slice of timestamp coordinates appropriate for consideration.
        let timestamps = if let Some(retain) = self.retain {
            if retain < timestamp.vector.len() {
                &timestamp.vector[..retain]
            } else {
                &timestamp.vector[..]
            }
        } else {
            &timestamp.vector[..]
        };

        let mut vector = SmallVec::<[T; 2]>::with_capacity(std::cmp::max(timestamps.len(), self.actions.len()));
        // Introduce elements where both timestamp and action exist.
        let min_len = std::cmp::min(timestamps.len(), self.actions.len());
        for (action, timestamp) in self.actions.iter().zip(timestamps.iter()) {
            vector.push(action.results_in(timestamp)?);
        }
        // Any remaining timestamps should be copied in.
        for timestamp in timestamps.iter().skip(min_len) {
            vector.push(timestamp.clone());
        }
        // Any remaining actions should be applied to the empty timestamp.
        for action in self.actions.iter().skip(min_len) {
            vector.push(action.results_in(&T::minimum())?);
        }

        Some(PointStamp::from_inline(vector))
    }
    fn followed_by(&self, other: &Self) -> Option<Self> {
        // The output `retain` will be the minimum of the two inputs.
        let retain = match (self.retain, other.retain) {
            (Some(x), Some(y)) => Some(std::cmp::min(x, y)),
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };

        // The output `actions` will depend on the relative sizes of the input `retain`s.
        let self_actions = if let Some(retain) = other.retain {
            if retain < self.actions.len() {
                &self.actions[..retain]
            } else {
                &self.actions[..]
            }
        } else {
            &self.actions[..]
        };

        let mut actions = Vec::with_capacity(std::cmp::max(self_actions.len(), other.actions.len()));
        // Introduce actions where both input actions apply.
        let min_len = std::cmp::min(self_actions.len(), other.actions.len());
        for (action1, action2) in self_actions.iter().zip(other.actions.iter()) {
            actions.push(action1.followed_by(action2)?);
        }
        // Append any remaining self actions.
        actions.extend(self_actions.iter().skip(min_len).cloned());
        // Append any remaining other actions.
        actions.extend(other.actions.iter().skip(min_len).cloned());

        Some(Self { retain, actions })
    }
}

impl<TS: PartialOrder> PartialOrder for PointStampSummary<TS> {
    fn less_equal(&self, other: &Self) -> bool {
        // If the `retain`s are not the same, there is some coordinate which
        // could either be bigger or smaller as the timestamp or the replacement.
        // In principle, a `T::minimum()` extension could break this rule, and
        // we could tighten this logic if needed; I think it is fine not to though.
        self.retain == other.retain
            && self.actions.len() <= other.actions.len()
            && self
                .actions
                .iter()
                .zip(other.actions.iter())
                .all(|(t1, t2)| t1.less_equal(t2))
    }
}

// Implement timely dataflow's `Timestamp` trait.
use timely::progress::Timestamp;
impl<T: Timestamp> Timestamp for PointStamp<T> {
    fn minimum() -> Self {
        Self::new(Default::default())
    }
    type Summary = PointStampSummary<T::Summary>;
}

// Implement differential dataflow's `Lattice` trait.
// This extends the `PartialOrder` implementation with additional structure.
use crate::lattice::Lattice;
impl<T: Lattice + Timestamp> Lattice for PointStamp<T> {
    #[inline(always)]
    fn join(&self, other: &Self) -> Self {
        let min_len = ::std::cmp::min(self.vector.len(), other.vector.len());
        let max_len = ::std::cmp::max(self.vector.len(), other.vector.len());
        let mut vector = SmallVec::with_capacity(max_len);
        // For coordinates in both inputs, apply `join` to the pair.
        for index in 0..min_len {
            vector.push(self.vector[index].join(&other.vector[index]));
        }
        // Only one of the two vectors will have remaining elements; copy them.
        for time in &self.vector[min_len..] {
            vector.push(time.clone());
        }
        for time in &other.vector[min_len..] {
            vector.push(time.clone());
        }
        Self::from_inline(vector)
    }
    #[inline]
    fn join_assign(&mut self, other: &Self) {
        let my_len = self.vector.len();
        let other_len = other.vector.len();
        let min_len = my_len.min(other_len);
        for i in 0..min_len {
            self.vector[i].join_assign(&other.vector[i]);
        }
        if other_len > my_len {
            self.vector.extend(other.vector[my_len..].iter().cloned());
        }
    }
    #[inline(always)]
    fn meet(&self, other: &Self) -> Self {
        let min_len = ::std::cmp::min(self.vector.len(), other.vector.len());
        let mut vector = SmallVec::with_capacity(min_len);
        // For coordinates in both inputs, apply `meet` to the pair.
        for index in 0..min_len {
            vector.push(self.vector[index].meet(&other.vector[index]));
        }
        // Remaining coordinates are `T::minimum()` in one input, and so in the output.
        Self::from_inline(vector)
    }
    #[inline]
    fn meet_assign(&mut self, other: &Self) {
        let min_len = ::std::cmp::min(self.vector.len(), other.vector.len());
        self.vector.truncate(min_len);
        for (this, that) in self.vector.iter_mut().zip(other.vector.iter()) {
            this.meet_assign(that);
        }
        while self.vector.last() == Some(&T::minimum()) { self.vector.pop(); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use columnar::{Borrow, Index};

    #[test]
    fn truncation_and_summaries_match_coordinate_reference() {
        for length in 0..8 {
            let stamp: PointStamp<u64> = (0..length).map(|i| if i % 2 == 0 { i + 1 } else { 0 }).collect();
            for keep in 0..9 {
                let mut truncated = stamp.clone();
                truncated.truncate(keep);
                let expected: PointStamp<u64> = stamp.iter().take(keep).copied().collect();
                assert_eq!(truncated, expected);
            }
            for retain in std::iter::once(None).chain((0..9).map(Some)) {
                for actions_len in 0..8 {
                    let actions: Vec<u64> = (0..actions_len).collect();
                    let summary = PointStampSummary { retain, actions: actions.clone() };
                    let retained: Vec<_> = stamp.iter().take(retain.unwrap_or(usize::MAX)).copied().collect();
                    let expected: PointStamp<u64> = (0..retained.len().max(actions.len()))
                        .map(|i| retained.as_slice().get(i).copied().unwrap_or(0) + actions.as_slice().get(i).copied().unwrap_or(0))
                        .collect();
                    let actual = summary.results_in(&stamp).unwrap();
                    assert_eq!(actual, expected);
                    if retained.len().max(actions.len()) <= 2 { assert!(!actual.vector.spilled()); }
                }
            }
        }
        let overflow = PointStampSummary { retain: None, actions: vec![1u64] };
        assert_eq!(overflow.results_in(&[u64::MAX].into_iter().collect()), None);
    }

    #[test]
    fn public_and_columnar_roundtrips_across_inline_boundary() {
        let mut reusable = PointStamp::<u64>::minimum();
        for length in (0..8).chain((0..8).rev()) {
            let input: SmallVec<[u64; 1]> = (1..=length).chain([0, 0]).collect();
            let stamp = PointStamp::new(input);
            let collected: PointStamp<u64> = (1..=length).chain([0, 0]).collect();
            assert_eq!(collected, stamp);
            assert_eq!(&*stamp, &(1..=length).collect::<Vec<_>>());
            assert_eq!(&*stamp.clone().into_inner(), &*stamp);
            let columns = PointStamp::as_columns([&stamp]);
            assert_eq!(PointStamp::into_owned(columns.borrow().get(0)), stamp);
            Columnar::copy_from(&mut reusable, columns.borrow().get(0));
            assert_eq!(reusable, stamp);
            assert_eq!(stamp.vector.spilled(), length > 2);
        }
        eprintln!("u64 layout: PointStamp={}, Product<u64,PointStamp>={}, SmallVec<[u64;1]>={}, SmallVec<[u64;2]>={}",
            std::mem::size_of::<PointStamp<u64>>(),
            std::mem::size_of::<timely::order::Product<u64, PointStamp<u64>>>(),
            std::mem::size_of::<SmallVec<[u64; 1]>>(), std::mem::size_of::<SmallVec<[u64; 2]>>());
    }
}

mod columnation {
    use columnation::{Columnation, Region};
    use smallvec::SmallVec;
    use crate::dynamic::pointstamp::PointStamp;

    impl<T: Columnation+Clone> Columnation for PointStamp<T> {
        type InnerRegion = PointStampStack<T::InnerRegion>;
    }

    /// Stack for PointStamp. Part of Columnation implementation.
    pub struct PointStampStack<R: Region<Item: Columnation+Clone>>(<SmallVec<[R::Item; 2]> as Columnation>::InnerRegion);

    impl<R: Region<Item: Columnation+Clone>> Default for PointStampStack<R> {
        #[inline]
        fn default() -> Self {
            Self(Default::default())
        }
    }

    impl<R: Region<Item: Columnation+Clone>> Region for PointStampStack<R> {
        type Item = PointStamp<R::Item>;

        #[inline]
        unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item {
            Self::Item { vector: self.0.copy(&item.vector) }
        }

        fn clear(&mut self) {
            self.0.clear();
        }

        fn reserve_items<'a, I>(&mut self, items: I) where Self: 'a, I: Iterator<Item=&'a Self::Item> + Clone {
            self.0.reserve_items(items.map(|x| &x.vector));
        }

        fn reserve_regions<'a, I>(&mut self, regions: I) where Self: 'a, I: Iterator<Item=&'a Self> + Clone {
            self.0.reserve_regions(regions.map(|r| &r.0));
        }

        fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            self.0.heap_size(callback);
        }
    }
}
