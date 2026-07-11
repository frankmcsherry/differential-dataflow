//! Columnar working storage for the proxy tactics' time-heavy state.
//!
//! The tactics manipulate many timestamps at once — presentations, replay histories,
//! interesting-time sets, staged output deltas — and for nested times (`T: Columnar`
//! with allocations, e.g. `Product<u64, PointStamp<u64>>`) a `Vec<T>` of them costs one
//! allocation per element. The types here keep the times in a single columnar container
//! (`ContainerOf<T>`), laid down contiguously, and read them as [`TimeRef`]s.
//!
//! A columnar container is append-only: it cannot be mutated in place or sorted by
//! `swap`. Mutation therefore proceeds by *rebuilding*: a permutation of indices is
//! sorted (comparing `TimeRef`s, whose order matches the owned order), and the successor
//! column is gathered into a spare container which is then swapped in. Reorganizations —
//! sort, dedup, consolidate, join-with-meet — cost bulk copies, never per-time
//! allocations; the spare containers retain their capacity across uses.
//!
//! Lattice operations (`join`, `meet`, `less_equal`) are defined on owned times, not on
//! refs, so call sites keep a few owned scratch timestamps and `copy_from` refs into
//! them — an `O(|T|)` copy that reuses the scratch's allocations, the same cost class as
//! the comparison or join it feeds.

use columnar::{Borrow, Clear, Columnar, ContainerOf, Index, Len, Push};

use crate::difference::Semigroup;
use crate::lattice::Lattice;

/// A reference to a time stored in a columnar container.
pub type TimeRef<'a, T> = columnar::Ref<'a, T>;
/// A borrowed view of a columnar time container.
pub type TimesView<'a, T> = columnar::BorrowedOf<'a, T>;

/// A `Vec<T>`-alike over a columnar time container: contiguous storage, permutation
/// sort, and rebuild-style mutation (see the module docs).
pub(crate) struct TimeVec<T: Columnar> {
    data: ContainerOf<T>,
    spare: ContainerOf<T>,
    perm: Vec<usize>,
}

impl<T: super::ProxyTime> TimeVec<T> {
    pub fn new() -> Self {
        TimeVec { data: Default::default(), spare: Default::default(), perm: Vec::new() }
    }

    pub fn len(&self) -> usize { self.data.len() }
    pub fn is_empty(&self) -> bool { self.data.is_empty() }
    pub fn clear(&mut self) { self.data.clear(); }

    pub fn push_ref(&mut self, time: TimeRef<'_, T>) { self.data.push(time); }
    pub fn push_own(&mut self, time: &T) { push_owned::<T>(&mut self.data, time); }

    pub fn get(&self, index: usize) -> TimeRef<'_, T> { self.data.borrow().get(index) }
    pub fn view(&self) -> TimesView<'_, T> { self.data.borrow() }

    /// Sort ascending and deduplicate, in place (by rebuild).
    pub fn sort_dedup(&mut self) {
        let mut zero = 0;
        self.sort_dedup_from(&mut zero);
    }

    /// Sort and deduplicate the live suffix `[*cursor..]`, discarding the consumed
    /// prefix and resetting `*cursor` to zero. This is the "pop from the front, push at
    /// the back, reorganize" cycle of a consumable, growable time set (e.g. the
    /// synthetic-times queue): consumed entries are dropped at the next reorganization.
    pub fn sort_dedup_from(&mut self, cursor: &mut usize) {
        let Self { data, spare, perm } = self;
        let view = data.borrow();
        perm.clear();
        Extend::extend(perm, *cursor..view.len());
        perm.sort_unstable_by(|&a, &b| view.get(a).cmp(&view.get(b)));
        spare.clear();
        for i in 0..perm.len() {
            if i == 0 || view.get(perm[i - 1]) != view.get(perm[i]) {
                spare.push(view.get(perm[i]));
            }
        }
        std::mem::swap(data, spare);
        *cursor = 0;
    }

    /// Advance every element by `meet` (join), then sort and deduplicate — the collapse
    /// that keeps a replayed time set an antichain-ish small buffer. `scratch` is the
    /// caller's owned working time.
    pub fn advance_by(&mut self, meet: &T, scratch: &mut T) {
        {
            let Self { data, spare, .. } = self;
            let view = data.borrow();
            spare.clear();
            for i in 0..view.len() {
                scratch.copy_from(view.get(i));
                scratch.join_assign(meet);
                push_owned::<T>(spare, scratch);
            }
            std::mem::swap(data, spare);
        }
        self.sort_dedup();
    }
}

/// A consolidated run of `(id, time, diff)` updates over columnar time storage: the
/// working form of replay buffers and staged output deltas. `I` is the id — `u64` for a
/// value id, `(u64, u64)` for a full `(key_hash, value_id)`.
pub(crate) struct UpdateCol<I, T: Columnar, R> {
    ids: Vec<I>,
    times: ContainerOf<T>,
    diffs: Vec<R>,
    spare_ids: Vec<I>,
    spare_times: ContainerOf<T>,
    spare_diffs: Vec<R>,
    perm: Vec<usize>,
}

impl<I, T, R> UpdateCol<I, T, R>
where
    I: Copy + Ord,
    T: super::ProxyTime,
    R: Semigroup + Clone,
{
    pub fn new() -> Self {
        UpdateCol {
            ids: Vec::new(),
            times: Default::default(),
            diffs: Vec::new(),
            spare_ids: Vec::new(),
            spare_times: Default::default(),
            spare_diffs: Vec::new(),
            perm: Vec::new(),
        }
    }

    pub fn len(&self) -> usize { self.ids.len() }
    pub fn is_empty(&self) -> bool { self.ids.is_empty() }
    pub fn clear(&mut self) {
        self.ids.clear();
        self.times.clear();
        self.diffs.clear();
    }

    pub fn push_ref(&mut self, id: I, time: TimeRef<'_, T>, diff: R) {
        self.ids.push(id);
        self.times.push(time);
        self.diffs.push(diff);
    }
    pub fn ids(&self) -> &[I] { &self.ids }
    pub fn diffs(&self) -> &[R] { &self.diffs }
    pub fn times(&self) -> TimesView<'_, T> { self.times.borrow() }
    pub fn time(&self, index: usize) -> TimeRef<'_, T> { self.times.borrow().get(index) }

    /// Advance every time by `meet` (join), leaving ids and diffs in place. `scratch` is
    /// the caller's owned working time.
    pub fn advance_by(&mut self, meet: &T, scratch: &mut T) {
        let Self { times, spare_times, .. } = self;
        let view = times.borrow();
        spare_times.clear();
        for i in 0..view.len() {
            scratch.copy_from(view.get(i));
            scratch.join_assign(meet);
            push_owned::<T>(spare_times, scratch);
        }
        std::mem::swap(times, spare_times);
    }

    /// Sort by `(id, time)` and merge equal pairs, summing diffs and dropping zeros —
    /// the columnar analogue of `consolidation::consolidate`, by rebuild.
    pub fn consolidate(&mut self) {
        let Self { ids, times, diffs, spare_ids, spare_times, spare_diffs, perm } = self;
        let view = times.borrow();
        perm.clear();
        Extend::extend(perm, 0..ids.len());
        perm.sort_unstable_by(|&a, &b| ids[a].cmp(&ids[b]).then_with(|| view.get(a).cmp(&view.get(b))));
        spare_ids.clear();
        spare_times.clear();
        spare_diffs.clear();
        let mut index = 0;
        while index < perm.len() {
            let this = perm[index];
            let mut diff = diffs[this].clone();
            index += 1;
            while index < perm.len() && ids[perm[index]] == ids[this] && view.get(perm[index]) == view.get(this) {
                diff.plus_equals(&diffs[perm[index]]);
                index += 1;
            }
            if !diff.is_zero() {
                spare_ids.push(ids[this]);
                spare_times.push(view.get(this));
                spare_diffs.push(diff);
            }
        }
        std::mem::swap(ids, spare_ids);
        std::mem::swap(times, spare_times);
        std::mem::swap(diffs, spare_diffs);
    }
}

/// Push the reversed suffix meets of `times[range]` into `out`: `out[j]` is the meet of
/// the range's last `j + 1` times, so the meet of `times[range.start + i ..]` reads as
/// `out[range.len() - 1 - i]`. Built in one backward pass; reading through the reversed
/// index avoids a second (reversing) rebuild.
pub(crate) fn suffix_meets_rev<T>(
    times: TimesView<'_, T>,
    range: std::ops::Range<usize>,
    out: &mut ContainerOf<T>,
    acc: &mut T,
    scratch: &mut T,
) where
    T: Columnar + Lattice,
{
    out.clear();
    for (count, index) in range.rev().enumerate() {
        if count == 0 {
            acc.copy_from(times.get(index));
        } else {
            scratch.copy_from(times.get(index));
            acc.meet_assign(&*scratch);
        }
        push_owned::<T>(out, acc);
    }
}

/// Push an owned time into a columnar container, disambiguating between the container's
/// `Push<Ref>` and `Push<&T>` implementations.
#[inline(always)]
pub(crate) fn push_owned<T: Columnar>(container: &mut ContainerOf<T>, time: &T) {
    Push::<&T>::push(container, time);
}
