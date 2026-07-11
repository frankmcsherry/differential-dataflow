//! [`ProxyBridge`]: the integers-only medium of exchange across the DD↔backend boundary.
//!
//! A backend `present`s one per work unit or window: a sorted, consolidated run of
//! `((key_hash, value_id), time, diff)` records projected from its real records. DD
//! reads it columnarly — the ids as a plain slice, the times through a columnar
//! container view — and refers back to backend records only by the ids themselves.
//!
//! The time column is a columnar container (`T: Columnar`), not a `Vec<T>`: for nested
//! times a vector of owned timestamps costs one allocation per record at every
//! presentation, the measured dominant cost for columnar backends. Backends build the
//! column by pushing `&T` (or refs) into a `ContainerOf<T>` and hand it to
//! [`from_unsorted`](ProxyBridge::from_unsorted), which sorts and consolidates by
//! permutation without ever materializing an owned time.
//!
//! It is *not* storage: `value_id`s are ephemeral (meaningful only within the single
//! presentation that mints them), so a bridge must never be arranged, merged, or
//! persisted — doing so would give the ephemeral ids a lifetime they do not have.

use columnar::{Borrow, Columnar, Container, ContainerOf, Index, Len, Push};

use crate::difference::Semigroup;
use super::column::{TimeRef, TimesView};

/// A sorted, consolidated presentation of `((key_hash, value_id), time, diff)` records,
/// exchanged across the DD↔backend seam for one work unit or window. Columns are
/// aligned and sorted by `((key_hash, value_id), time)`.
pub struct ProxyBridge<T: Columnar, R> {
    ids: Vec<(u64, u64)>,
    times: ContainerOf<T>,
    diffs: Vec<R>,
}

impl<T: Columnar, R> Default for ProxyBridge<T, R> {
    fn default() -> Self {
        ProxyBridge { ids: Vec::new(), times: Default::default(), diffs: Vec::new() }
    }
}

impl<T: Columnar, R> ProxyBridge<T, R> {
    /// The number of records.
    pub fn len(&self) -> usize { self.ids.len() }
    /// True iff there are no records.
    pub fn is_empty(&self) -> bool { self.ids.is_empty() }
    /// The `(key_hash, value_id)` column.
    pub fn ids(&self) -> &[(u64, u64)] { &self.ids }
    /// The time column, as a borrowed columnar view.
    pub fn times(&self) -> TimesView<'_, T> { self.times.borrow() }
    /// The time at `index`.
    pub fn time(&self, index: usize) -> TimeRef<'_, T> { self.times.borrow().get(index) }
    /// The diff column.
    pub fn diffs(&self) -> &[R] { &self.diffs }
    /// Move the columns out — the write-side seam a backend's `materialize` reads.
    pub fn into_parts(self) -> (Vec<(u64, u64)>, ContainerOf<T>, Vec<R>) {
        (self.ids, self.times, self.diffs)
    }
}

impl<T, R> ProxyBridge<T, R>
where
    T: Columnar<Container: for<'a> Container<Ref<'a>: Ord>>,
    R: Semigroup + Clone,
{
    /// Sort columns by `((key_hash, value_id), time)` and consolidate equal triples
    /// (summing diffs, dropping zeros). Returns the run and, per retained record, the
    /// original index of a *representative* input record — the link a backend keeps to
    /// align its real columns with the id run it presents (equal ids denote equal
    /// values, so any member of a consolidated group serves).
    pub fn from_unsorted(ids: Vec<(u64, u64)>, times: ContainerOf<T>, diffs: Vec<R>) -> (Self, Vec<usize>) {
        let n = ids.len();
        debug_assert!(times.len() == n && diffs.len() == n);
        let view = times.borrow();
        let perm = sort_perm::<T>(&ids, view);

        let mut out_ids = Vec::new();
        let mut out_times: ContainerOf<T> = Default::default();
        let mut out_diffs = Vec::new();
        let mut reps = Vec::new();
        let mut i = 0;
        while i < n {
            let r = perm[i];
            let mut d = diffs[r].clone();
            let mut j = i + 1;
            while j < n && {
                let s = perm[j];
                ids[s] == ids[r] && view.get(s) == view.get(r)
            } {
                d.plus_equals(&diffs[perm[j]]);
                j += 1;
            }
            if !d.is_zero() {
                out_ids.push(ids[r]);
                out_times.push(view.get(r));
                out_diffs.push(d);
                reps.push(r);
            }
            i = j;
        }
        (ProxyBridge { ids: out_ids, times: out_times, diffs: out_diffs }, reps)
    }

    /// Debug check that this bridge is sorted and consolidated by `((key_hash, value_id), time)`.
    ///
    /// Operator harnesses use the test to flag backend implementations that do not uphold it.
    pub(crate) fn debug_assert_sorted(&self, who: &str) {
        debug_assert!(
            {
                let view = self.times.borrow();
                (1..self.ids.len()).all(|i| {
                    (self.ids[i - 1], view.get(i - 1)) < (self.ids[i], view.get(i))
                })
            },
            "{}: a presented bridge must be sorted & consolidated by ((key_hash, value_id), time)",
            who,
        );
    }
}

/// A sorting permutation for `((key_hash, value_id), time)`. For small runs a plain
/// unstable sort; for large ones an MSD counting sort on the top byte of `key_hash` (a
/// content hash, so uniformly distributed) into 256 ascending buckets, each finished by
/// the full comparison. The order is identical to a full sort; `unstable` is fine
/// because `from_unsorted` only needs *a* representative per consolidated group, not a
/// particular one. Degenerate `key_hash` (all one top byte) falls back to a single
/// bucket — the small-`n` path plus `O(n)`, never worse.
fn sort_perm<T>(ids: &[(u64, u64)], times: TimesView<'_, T>) -> Vec<usize>
where
    T: Columnar<Container: for<'a> Container<Ref<'a>: Ord>>,
{
    let n = ids.len();
    let cmp = |&a: &usize, &b: &usize| ids[a].cmp(&ids[b]).then_with(|| times.get(a).cmp(&times.get(b)));
    if n < 512 {
        let mut perm: Vec<usize> = (0..n).collect();
        perm.sort_unstable_by(cmp);
        return perm;
    }
    let bucket = |i: usize| (ids[i].0 >> 56) as usize;
    let mut counts = [0usize; 256];
    for i in 0..n { counts[bucket(i)] += 1; }
    let mut starts = [0usize; 257];
    for b in 0..256 { starts[b + 1] = starts[b] + counts[b]; }
    let mut perm = vec![0usize; n];
    let mut cursor = starts;
    for i in 0..n {
        let b = bucket(i);
        perm[cursor[b]] = i;
        cursor[b] += 1;
    }
    for b in 0..256 {
        perm[starts[b]..starts[b + 1]].sort_unstable_by(cmp);
    }
    perm
}

/// The novel batches' raw `(key_hash, time)` support, sorted by `key_hash` — the seed
/// seam of the reduce protocol. Times ride in a columnar container; within a key their
/// order is immaterial (the tactic organizes its own replay).
pub struct SeedTimes<T: Columnar> {
    keys: Vec<u64>,
    times: ContainerOf<T>,
}

impl<T: Columnar> Default for SeedTimes<T> {
    fn default() -> Self {
        SeedTimes { keys: Vec::new(), times: Default::default() }
    }
}

impl<T: Columnar> SeedTimes<T> {
    /// The number of seeds.
    pub fn len(&self) -> usize { self.keys.len() }
    /// True iff there are no seeds.
    pub fn is_empty(&self) -> bool { self.keys.is_empty() }
    /// The key-hash column (ascending).
    pub fn keys(&self) -> &[u64] { &self.keys }
    /// The time column, aligned with the keys.
    pub fn times(&self) -> TimesView<'_, T> { self.times.borrow() }

    /// Append one `(key_hash, time)` seed; the time is copied into the column.
    pub fn push(&mut self, key_hash: u64, time: &T) {
        self.keys.push(key_hash);
        Push::<&T>::push(&mut self.times, time);
    }

    /// Order the seeds by key hash (by permutation; times within a key stay in arrival
    /// order, which is all the seam requires). A no-op on already-ordered seeds.
    pub fn sort_by_key(&mut self) {
        if self.keys.windows(2).all(|w| w[0] <= w[1]) {
            return;
        }
        let mut perm: Vec<usize> = (0..self.keys.len()).collect();
        perm.sort_by_key(|&i| self.keys[i]);
        let mut out_keys = Vec::with_capacity(self.keys.len());
        let mut out_times: ContainerOf<T> = Default::default();
        {
            let view = self.times.borrow();
            for &i in &perm {
                out_keys.push(self.keys[i]);
                out_times.push(view.get(i));
            }
        }
        self.keys = out_keys;
        self.times = out_times;
    }
}

/// Accumulates unsorted `((key_hash, value_id), time, diff)` records and builds a
/// sorted, consolidated [`ProxyBridge`] — the backend-side construction path. Times are
/// pushed by reference (owned `&T` or a columnar ref) straight into the column; no owned
/// timestamp is ever materialized.
pub struct ProxyBridgeBuilder<T: Columnar, R> {
    ids: Vec<(u64, u64)>,
    times: ContainerOf<T>,
    diffs: Vec<R>,
}

impl<T: Columnar, R> Default for ProxyBridgeBuilder<T, R> {
    fn default() -> Self {
        ProxyBridgeBuilder { ids: Vec::new(), times: Default::default(), diffs: Vec::new() }
    }
}

impl<T: Columnar, R> ProxyBridgeBuilder<T, R> {
    /// The number of records so far.
    pub fn len(&self) -> usize { self.ids.len() }
    /// True iff no records have been pushed.
    pub fn is_empty(&self) -> bool { self.ids.is_empty() }

    /// Append one record, copying the time into the column.
    pub fn push(&mut self, id: (u64, u64), time: &T, diff: R) {
        self.ids.push(id);
        Push::<&T>::push(&mut self.times, time);
        self.diffs.push(diff);
    }

    /// Append one record whose time is already a columnar ref.
    pub fn push_ref(&mut self, id: (u64, u64), time: TimeRef<'_, T>, diff: R) {
        self.ids.push(id);
        self.times.push(time);
        self.diffs.push(diff);
    }
}

impl<T, R> ProxyBridgeBuilder<T, R>
where
    T: Columnar<Container: for<'a> Container<Ref<'a>: Ord>>,
    R: Semigroup + Clone,
{
    /// Sort and consolidate into a [`ProxyBridge`], with the representative original
    /// index per retained record (see [`ProxyBridge::from_unsorted`]).
    pub fn build(self) -> (ProxyBridge<T, R>, Vec<usize>) {
        ProxyBridge::from_unsorted(self.ids, self.times, self.diffs)
    }
}
