//! A worked [`Chunk`]: a layered trie of plain `Vec`s, replacing `ord_neu`.
//!
//! Where [`vec`](super::vec) backs a chunk with a flat `Vec<((K, V), T, R)>`, this backs it
//! with the layered trie `ord_neu` uses for whole batches — deduplicated keys, per-key val
//! runs, per-val `(time, diff)` runs — but cut into bounded chunks. It is the `Chunk`-framework
//! replacement for [`ord_neu`](crate::trace::implementations::ord_neu):
//!
//! * `ord_neu` forms each batch as one massive allocation, and its merges pre-allocate
//!   `merge_capacity` for both inputs at once. Here a batch is a graded sequence of
//!   `TARGET`-bounded chunks, and every operation holds at most a chunk's worth of new
//!   allocation at a time.
//! * `ord_neu` routes every element access through `BatchContainer`, whose monomorphization
//!   does not always pan out (a columnar container pays a borrow tax on each access).
//!   The [`Chunk`] operations are whole-chunk: the implementations below work directly on
//!   the trie's slices, and a cursor is the only per-access surface.
//!
//! The exported aliases mirror `ord_neu`'s: [`OrdValSpine`] / [`OrdValBatcher`] /
//! [`OrdValBuilder`] (and their `OrdKey` counterparts) plug into `arrange_core` and
//! `reduce_abelian` exactly where the `ord_neu` types did, with identical cursor item
//! types (`&K`, `&V`, `&T`, `&R`).

use std::collections::VecDeque;
use std::rc::Rc;

use timely::Accountable;
use timely::container::{PushInto, SizableContainer};
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::consolidation::{consolidate, consolidate_updates, Consolidate};
use crate::difference::Semigroup;
use crate::lattice::Lattice;

use super::Chunk;

/// The chunk size: the [`Chunk::TARGET`] grading value.
const TARGET: usize = 8192;

/// Cumulative group bounds: a sequence starting at zero, compressed while strided.
///
/// The dominant shapes — one val per key (key-only arrangements), one update per
/// val (snapshot data) — make the bounds `0, s, 2s, ..`, which this stores in O(1)
/// instead of a word per group. Bounds beyond a break in the stride spill into an
/// explicit vector, and the final bound is held separately (`tail`) because it is
/// the one the writers mutate: a fresh group opens with its end equal to the
/// previous bound and grows leaf by leaf, and only on the next group's arrival is
/// it folded into the compressed form.
///
/// The represented sequence is `[0]`, then `i * stride` for `i in 1 ..= strided`,
/// then `spill`, then `tail` when present.
#[derive(Clone, Debug, Default)]
pub(crate) struct Offsets {
    /// The stride of the leading compressed bounds; `0` until the first fold.
    stride: usize,
    /// The number of leading bounds (beyond the implicit zero) equal to `i * stride`.
    strided: usize,
    /// Explicit bounds after the stride broke.
    spill: Vec<usize>,
    /// The final bound, when it has not been folded.
    tail: Option<usize>,
}

impl Offsets {
    /// The number of bounds, including the implicit leading zero.
    #[cfg_attr(not(test), allow(dead_code))]
    fn count(&self) -> usize {
        1 + self.strided + self.spill.len() + (self.tail.is_some() as usize)
    }

    /// The `i`th bound; `bound(0) == 0`.
    #[inline]
    fn bound(&self, i: usize) -> usize {
        if i <= self.strided { i * self.stride }
        else if i <= self.strided + self.spill.len() { self.spill[i - self.strided - 1] }
        else { self.tail.unwrap() }
    }

    /// The final bound.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    fn last(&self) -> usize { self.bound(self.count() - 1) }

    /// Fold the tail into the compressed form (called as the next group opens).
    fn close(&mut self) {
        if let Some(t) = self.tail.take() {
            if self.spill.is_empty() && self.strided == 0 && self.stride == 0 {
                // First closed bound decides the stride.
                self.stride = t;
                self.strided = 1;
            } else if self.spill.is_empty() && t == (self.strided + 1) * self.stride {
                self.strided += 1;
            } else {
                self.spill.push(t);
            }
        }
    }

    /// Append a bound (opening a new group whose end it is).
    fn push(&mut self, x: usize) {
        self.close();
        self.tail = Some(x);
    }

    /// Overwrite the final bound (the open group's end grew).
    fn set_last(&mut self, x: usize) {
        if self.tail.is_none() {
            // Reopen the last folded bound.
            if !self.spill.is_empty() { self.spill.pop(); }
            else if self.strided > 0 { self.strided -= 1; }
        }
        self.tail = Some(x);
    }

    /// Remove the final bound.
    fn pop(&mut self) {
        if self.tail.take().is_none() {
            if !self.spill.is_empty() { self.spill.pop(); }
            else { self.strided -= 1; if self.strided == 0 { self.stride = 0; } }
        }
    }

    /// Append `other`'s bounds at indices `range`, each shifted by `delta`.
    fn extend_shifted(&mut self, other: &Offsets, range: std::ops::Range<usize>, delta: isize) {
        for i in range {
            self.push(((other.bound(i) as isize) + delta) as usize);
        }
    }

    /// The largest group index `g` with `bound(g) <= x`, for strictly increasing
    /// bounds (a formed trie's shape).
    fn group_containing(&self, x: usize) -> usize {
        let mut g = if self.stride > 0 { (x / self.stride).min(self.strided) } else { 0 };
        if g == self.strided {
            g += self.spill.partition_point(|&b| b <= x);
            if g == self.strided + self.spill.len() {
                if let Some(t) = self.tail { if t <= x { g += 1; } }
            }
        }
        g
    }

    /// Release excess capacity.
    fn shrink_to_fit(&mut self) { self.spill.shrink_to_fit(); }

    /// Clear to the empty sequence, keeping allocations.
    fn clear(&mut self) {
        self.stride = 0;
        self.strided = 0;
        self.spill.clear();
        self.tail = None;
    }
}

/// A column of values, compressed while every value is the same.
///
/// Times and diffs are heavily repetitive in practice — a chunk of snapshot data
/// carries one time and one diff — so the column starts as a repetition and
/// materializes only when a differing value arrives.
#[derive(Clone, Debug)]
pub(crate) enum Column<T> {
    /// `count` copies of `item` (zero copies of nothing when empty).
    Repeat { item: Option<T>, count: usize },
    /// One value per entry.
    Explicit(Vec<T>),
}

impl<T> Default for Column<T> {
    fn default() -> Self { Column::Repeat { item: None, count: 0 } }
}

impl<T> Column<T> {
    fn len(&self) -> usize {
        match self {
            Column::Repeat { count, .. } => *count,
            Column::Explicit(v) => v.len(),
        }
    }

    fn is_empty(&self) -> bool { self.len() == 0 }

    #[inline]
    fn index(&self, i: usize) -> &T {
        match self {
            Column::Repeat { item, count } => { debug_assert!(i < *count); item.as_ref().unwrap() }
            Column::Explicit(v) => &v[i],
        }
    }

    fn last(&self) -> Option<&T> {
        if self.is_empty() { None } else { Some(self.index(self.len() - 1)) }
    }

    /// Iterate the column's values.
    fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        (0 .. self.len()).map(move |i| self.index(i))
    }

    fn pop(&mut self) {
        match self {
            Column::Repeat { item, count } => {
                *count -= 1;
                if *count == 0 { *item = None; }
            }
            Column::Explicit(v) => { v.pop(); }
        }
    }

    fn shrink_to_fit(&mut self) {
        if let Column::Explicit(v) = self { v.shrink_to_fit(); }
    }

    fn clear(&mut self) {
        match self {
            Column::Repeat { item, count } => { *item = None; *count = 0; }
            Column::Explicit(v) => v.clear(),
        }
    }
}

impl<T: Clone + PartialEq> Column<T> {
    /// Convert to the explicit form, cloning out the repetitions.
    fn materialize(&mut self) -> &mut Vec<T> {
        if let Column::Repeat { item, count } = self {
            let mut v = Vec::with_capacity(*count + 1);
            if let Some(item) = item.take() {
                v.resize(*count, item);
            }
            *self = Column::Explicit(v);
        }
        match self { Column::Explicit(v) => v, _ => unreachable!() }
    }

    fn push(&mut self, x: T) {
        match self {
            Column::Repeat { item: None, count } => { debug_assert_eq!(*count, 0); *self = Column::Repeat { item: Some(x), count: 1 }; }
            Column::Repeat { item: Some(item), count } => {
                if *item == x { *count += 1; }
                else { self.materialize().push(x); }
            }
            Column::Explicit(v) => v.push(x),
        }
    }

    /// Append `other[range]`, preserving compression when both sides repeat the
    /// same value.
    fn extend_from(&mut self, other: &Self, range: std::ops::Range<usize>) {
        if range.is_empty() { return; }
        match (&mut *self, other) {
            (Column::Repeat { item, count }, Column::Repeat { item: Some(o), .. }) => {
                match item {
                    None => { *item = Some(o.clone()); *count = range.len(); }
                    Some(item) if item == o => { *count += range.len(); }
                    Some(_) => { self.materialize().extend(std::iter::repeat_with(|| o.clone()).take(range.len())); }
                }
            }
            (_, Column::Repeat { item: Some(o), .. }) => {
                self.materialize().extend(std::iter::repeat_with(|| o.clone()).take(range.len()));
            }
            (_, Column::Explicit(ov)) => {
                self.materialize().extend_from_slice(&ov[range]);
            }
            (_, Column::Repeat { item: None, .. }) => unreachable!("non-empty range of an empty column"),
        }
    }
}

/// Trie-layered storage for sorted, consolidated `((key, val), time, diff)` updates.
///
/// Three levels: `keys`, each with a run of `vals`, each with a parallel run of
/// `times` / `diffs` (one diff per time; nothing accumulates to zero). The offset
/// vectors carry one entry more than their value vectors, starting at zero, so
/// key `i`'s vals are `vals[key_offs[i] .. key_offs[i + 1]]` and val `j`'s updates
/// are `times[val_offs[j] .. val_offs[j + 1]]` with no bounds logic.
///
/// The `stage` vector holds unordered rows pushed by the chunker before
/// consolidation; a formed trie (everything the [`Chunk`] operations touch) has an
/// empty stage.
pub struct TrieStorage<K, V, T, R> {
    /// Ordered, deduplicated keys.
    pub(crate) keys: Vec<K>,
    /// `keys.len() + 1` cumulative bounds into `vals` (compressed while strided).
    pub(crate) key_offs: Offsets,
    /// Ordered vals, deduplicated within each key's run.
    pub(crate) vals: Vec<V>,
    /// `vals.len() + 1` cumulative bounds into `times` / `diffs`.
    pub(crate) val_offs: Offsets,
    /// Ordered times, deduplicated within each val's run (compressed while constant).
    pub(crate) times: Column<T>,
    /// Diffs, parallel to `times`; none are zero (compressed while constant).
    pub(crate) diffs: Column<R>,
    /// Unordered staged rows (batcher side); empty once the trie is formed.
    stage: Vec<((K, V), T, R)>,
}

impl<K, V, T, R> Default for TrieStorage<K, V, T, R> {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            key_offs: Offsets::default(),
            vals: Vec::new(),
            val_offs: Offsets::default(),
            times: Column::default(),
            diffs: Column::default(),
            stage: Vec::new(),
        }
    }
}

impl<K: Clone, V: Clone, T: Clone, R: Clone> Clone for TrieStorage<K, V, T, R> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            key_offs: self.key_offs.clone(),
            vals: self.vals.clone(),
            val_offs: self.val_offs.clone(),
            times: self.times.clone(),
            diffs: self.diffs.clone(),
            stage: self.stage.clone(),
        }
    }
}

/// A flat cursor into a [`TrieStorage`]: key, val, and update indices, the latter
/// two within (or at the start of) their parents' ranges. The all-consumed position
/// has every index at its column's length.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Pos {
    k: usize,
    v: usize,
    t: usize,
}

impl Pos {
    /// The starting position.
    const START: Pos = Pos { k: 0, v: 0, t: 0 };

    /// The position at the start of key `k` (or the end position, past the last key).
    fn at_key<K, V, T, R>(s: &TrieStorage<K, V, T, R>, k: usize) -> Pos {
        if k < s.keys.len() {
            let v = s.key_offs.bound(k);
            Pos { k, v, t: s.val_offs.bound(v) }
        } else {
            Pos { k: s.keys.len(), v: s.vals.len(), t: s.times.len() }
        }
    }

    /// Step past the current val, normalizing the key and update indices.
    fn step_val<K, V, T, R>(&mut self, s: &TrieStorage<K, V, T, R>) {
        self.v += 1;
        if self.v >= s.key_offs.bound(self.k + 1) {
            self.k += 1;
        }
        self.t = if self.v < s.vals.len() { s.val_offs.bound(self.v) } else { s.times.len() };
    }
}

impl<K, V, T, R> TrieStorage<K, V, T, R> {
    /// The number of formed updates (excluding staged rows).
    fn formed_len(&self) -> usize { self.times.len() }

    /// The number of updates, staged rows included.
    fn total_len(&self) -> usize { self.times.len() + self.stage.len() }

    /// Clear all content, keeping allocations.
    fn clear(&mut self) {
        self.keys.clear();
        self.key_offs.clear();
        self.vals.clear();
        self.val_offs.clear();
        self.times.clear();
        self.diffs.clear();
        self.stage.clear();
    }

    /// Release excess capacity on every column.
    ///
    /// Chunks are built by extending fresh vectors, whose geometric growth can
    /// leave up to another length's worth of capacity. Committed chunks are
    /// immutable, so [`Chunk::settle`] passes them through here (a no-op once
    /// capacities are exact).
    fn shrink_to_fit(&mut self) {
        self.keys.shrink_to_fit();
        self.key_offs.shrink_to_fit();
        self.vals.shrink_to_fit();
        self.val_offs.shrink_to_fit();
        self.times.shrink_to_fit();
        self.diffs.shrink_to_fit();
        self.stage.shrink_to_fit();
    }
}

impl<K, V, T, R> TrieStorage<K, V, T, R>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Ord + Clone,
    R: Semigroup + PartialEq,
{
    /// Whether the open (last) key's val run is non-empty and ends with `v`.
    fn open_val_is(&self, v: &V) -> bool {
        let nk = self.keys.len();
        nk > 0 && self.key_offs.bound(nk - 1) < self.vals.len() && self.vals.last() == Some(v)
    }

    /// Open a new val under the open key.
    fn push_val(&mut self, v: &V) {
        self.vals.push(v.clone());
        self.val_offs.push(self.times.len());
        self.key_offs.set_last(self.vals.len());
    }

    /// Append one `(time, diff)` to the open val's run.
    fn push_leaf(&mut self, t: &T, r: &R) {
        self.times.push(t.clone());
        self.diffs.push(r.clone());
        self.val_offs.set_last(self.times.len());
    }

    /// After removing a trailing leaf, drop the open val / key if now empty.
    fn pop_empty_groups(&mut self) {
        self.val_offs.set_last(self.times.len());
        let nv = self.vals.len();
        if self.val_offs.bound(nv) == self.val_offs.bound(nv - 1) {
            self.vals.pop();
            self.val_offs.pop();
            self.key_offs.set_last(self.vals.len());
            let nk = self.keys.len();
            if self.key_offs.bound(nk) == self.key_offs.bound(nk - 1) {
                self.keys.pop();
                self.key_offs.pop();
            }
        }
    }

    /// Ensure the open `(key, val)` is `(k, v)`, opening either as needed.
    ///
    /// An equal trailing key or val can only be the same logical group: the trie is
    /// built in strictly increasing `(key, val, time)` order, so a value recurs only
    /// as a continuation.
    fn ensure_kv(&mut self, k: &K, v: &V) {
        if self.keys.last() != Some(k) {
            self.keys.push(k.clone());
            self.key_offs.push(self.vals.len());
        }
        if !self.open_val_is(v) {
            self.push_val(v);
        }
    }

    /// Append one update in trie order, consolidating an equal trailing triple.
    ///
    /// The caller feeds updates in non-decreasing `(key, val, time)` order; equal
    /// triples accumulate (and vanish if they cancel), everything else appends.
    /// The by-ref counterpart of [`Self::push_update_owned`], for callers holding
    /// borrowed rows (currently the tests).
    #[cfg_attr(not(test), allow(dead_code))]
    fn push_update(&mut self, k: &K, v: &V, t: &T, r: &R) {
        if self.keys.last() == Some(k) && self.open_val_is(v) {
            if self.times.last() == Some(t) {
                self.accumulate_last(r);
                return;
            }
        } else {
            self.ensure_kv(k, v);
        }
        self.push_leaf(t, r);
    }

    /// Fold `r` into the trailing diff, removing the leaf if it cancels.
    fn accumulate_last(&mut self, r: &R) {
        let mut d = self.diffs.last().unwrap().clone();
        d.plus_equals(r);
        self.diffs.pop();
        if d.is_zero() {
            self.times.pop();
            self.pop_empty_groups();
        } else {
            self.diffs.push(d);
        }
    }

    /// As [`Self::push_update`], but moving the components rather than cloning them.
    /// The hot path for chunk formation, where the rows are owned anyway.
    fn push_update_owned(&mut self, k: K, v: V, t: T, r: R) {
        if self.keys.last() == Some(&k) && self.open_val_is(&v) {
            if self.times.last() == Some(&t) {
                self.accumulate_last(&r);
                return;
            }
        } else {
            if self.keys.last() != Some(&k) {
                self.keys.push(k);
                self.key_offs.push(self.vals.len());
            }
            self.vals.push(v);
            self.val_offs.push(self.times.len());
            self.key_offs.set_last(self.vals.len());
        }
        self.times.push(t);
        self.diffs.push(r);
        self.val_offs.set_last(self.times.len());
    }

    /// Extend the open val's run with `other.times[range]` (all strictly greater).
    fn extend_leaves(&mut self, other: &Self, range: std::ops::Range<usize>) {
        self.times.extend_from(&other.times, range.clone());
        self.diffs.extend_from(&other.diffs, range);
        self.val_offs.set_last(self.times.len());
    }

    /// Append whole vals `range` of `other`, with their update runs, into the open key.
    fn extend_vals(&mut self, other: &Self, range: std::ops::Range<usize>) {
        if range.is_empty() { return; }
        let t_lo = other.val_offs.bound(range.start);
        let t_hi = other.val_offs.bound(range.end);
        let t_base = self.times.len();
        self.vals.extend_from_slice(&other.vals[range.clone()]);
        self.val_offs.extend_shifted(&other.val_offs, range.start + 1 .. range.end + 1, t_base as isize - t_lo as isize);
        self.times.extend_from(&other.times, t_lo .. t_hi);
        self.diffs.extend_from(&other.diffs, t_lo .. t_hi);
        self.key_offs.set_last(self.vals.len());
    }

    /// Append whole keys `range` of `other`, with their val and update runs.
    fn extend_keys(&mut self, other: &Self, range: std::ops::Range<usize>) {
        if range.is_empty() { return; }
        let v_lo = other.key_offs.bound(range.start);
        let v_hi = other.key_offs.bound(range.end);
        let v_base = self.vals.len();
        let t_lo = other.val_offs.bound(v_lo);
        let t_hi = other.val_offs.bound(v_hi);
        let t_base = self.times.len();
        self.keys.extend_from_slice(&other.keys[range.clone()]);
        self.key_offs.extend_shifted(&other.key_offs, range.start + 1 .. range.end + 1, v_base as isize - v_lo as isize);
        self.vals.extend_from_slice(&other.vals[v_lo .. v_hi]);
        self.val_offs.extend_shifted(&other.val_offs, v_lo + 1 .. v_hi + 1, t_base as isize - t_lo as isize);
        self.times.extend_from(&other.times, t_lo .. t_hi);
        self.diffs.extend_from(&other.diffs, t_lo .. t_hi);
    }

    /// Append `other`'s vals `[v0, val_end)` under `key`, the first val possibly
    /// entered mid-run at update index `t0`, continuing `self`'s open key and val
    /// where they match. All appended triples are strictly greater than `self`'s last.
    fn append_vals_from(&mut self, other: &Self, key: &K, v0: usize, t0: usize, val_end: usize) {
        if v0 >= val_end { return; }
        let mut v = v0;
        if self.keys.last() != Some(key) {
            self.keys.push(key.clone());
            self.key_offs.push(self.vals.len());
        }
        if self.open_val_is(&other.vals[v]) {
            // The open val continues: extend its run with the remaining updates.
            self.extend_leaves(other, t0 .. other.val_offs.bound(v + 1));
            v += 1;
        } else if t0 > other.val_offs.bound(v) {
            // A fresh val entered mid-run: open it and copy the remaining updates.
            self.push_val(&other.vals[v]);
            self.extend_leaves(other, t0 .. other.val_offs.bound(v + 1));
            v += 1;
        }
        self.extend_vals(other, v .. val_end);
    }

    /// Append `other[pos ..)` limited to keys `.. key_end`, continuing `self`'s open
    /// key / val groups on a shared boundary. All appended triples are strictly
    /// greater than `self`'s last.
    fn append_range(&mut self, other: &Self, pos: Pos, key_end: usize) {
        if pos.k >= key_end { return; }
        self.append_vals_from(other, &other.keys[pos.k], pos.v, pos.t, other.key_offs.bound(pos.k + 1));
        self.extend_keys(other, pos.k + 1 .. key_end);
    }

    /// Reconstruct `other[pos ..]` as a standalone trie: the merge survivor's suffix.
    fn suffix(other: &Self, pos: Pos) -> Self {
        let mut out = Self::default();
        out.append_range(other, pos, other.keys.len());
        out
    }

    /// Split into the first `n` updates and the remaining `len - n`, cutting the
    /// containing key / val groups. Both halves are standalone tries.
    fn split_at(self, n: usize) -> (Self, Self) {
        let total = self.formed_len();
        if n == 0 { return (Self::default(), self); }
        if n >= total { return (self, Self::default()); }

        // The val and key containing update `n` (offsets are strictly increasing).
        let v = self.val_offs.group_containing(n);
        let k = self.key_offs.group_containing(v);

        let mut first = Self::default();
        first.extend_keys(&self, 0 .. k);
        if v > self.key_offs.bound(k) || n > self.val_offs.bound(v) {
            first.keys.push(self.keys[k].clone());
            first.key_offs.push(first.vals.len());
            first.extend_vals(&self, self.key_offs.bound(k) .. v);
            if n > self.val_offs.bound(v) {
                first.push_val(&self.vals[v]);
                first.extend_leaves(&self, self.val_offs.bound(v) .. n);
            }
        }
        let second = Self::suffix(&self, Pos { k, v, t: n });
        (first, second)
    }

    /// Merge two formed tries from the given positions through their shared horizon
    /// into `out`, returning the positions each stopped at. At least one position is
    /// at its trie's end; the other's remainder is entirely greater than everything
    /// written to `out`.
    fn merge_into(a: &Self, b: &Self, mut pa: Pos, mut pb: Pos, out: &mut Self) -> (Pos, Pos) {
        use std::cmp::Ordering;
        while pa.k < a.keys.len() && pb.k < b.keys.len() {
            match a.keys[pa.k].cmp(&b.keys[pb.k]) {
                Ordering::Less => {
                    // Bulk-copy a's keys strictly below b's head key.
                    let hi = gallop(&a.keys, pa.k + 1, a.keys.len(), |x| x < &b.keys[pb.k]);
                    out.append_range(a, pa, hi);
                    pa = Pos::at_key(a, hi);
                }
                Ordering::Greater => {
                    let hi = gallop(&b.keys, pb.k + 1, b.keys.len(), |x| x < &a.keys[pa.k]);
                    out.append_range(b, pb, hi);
                    pb = Pos::at_key(b, hi);
                }
                Ordering::Equal => {
                    Self::merge_key(a, b, &mut pa, &mut pb, out);
                }
            }
        }
        (pa, pb)
    }

    /// Merge the shared key both positions sit on, advancing whichever side(s)
    /// exhaust their key to its successor. Keys and vals that wholly cancel are
    /// never opened in `out` (leaves are pushed lazily via [`Self::ensure_kv`]).
    fn merge_key(a: &Self, b: &Self, pa: &mut Pos, pb: &mut Pos, out: &mut Self) {
        use std::cmp::Ordering;
        let (ka, kb) = (pa.k, pb.k);
        let (va_end, vb_end) = (a.key_offs.bound(ka + 1), b.key_offs.bound(kb + 1));
        while pa.v < va_end && pb.v < vb_end {
            match a.vals[pa.v].cmp(&b.vals[pb.v]) {
                Ordering::Less => {
                    // Bulk-copy a's vals strictly below b's head val (first possibly mid-run).
                    let hi = gallop(&a.vals, pa.v + 1, va_end, |x| x < &b.vals[pb.v]);
                    out.append_vals_from(a, &a.keys[ka], pa.v, pa.t, hi);
                    pa.v = hi;
                    pa.t = if hi < a.vals.len() { a.val_offs.bound(hi) } else { a.times.len() };
                }
                Ordering::Greater => {
                    let hi = gallop(&b.vals, pb.v + 1, vb_end, |x| x < &a.vals[pa.v]);
                    out.append_vals_from(b, &b.keys[kb], pb.v, pb.t, hi);
                    pb.v = hi;
                    pb.t = if hi < b.vals.len() { b.val_offs.bound(hi) } else { b.times.len() };
                }
                Ordering::Equal => {
                    Self::merge_times(a, b, pa, pb, out);
                }
            }
        }
        if pa.v >= va_end { *pa = Pos::at_key(a, ka + 1); }
        if pb.v >= vb_end { *pb = Pos::at_key(b, kb + 1); }
    }

    /// Merge the shared `(key, val)` both positions sit on, consolidating equal
    /// times (dropping cancellations), and step whichever side(s) exhaust the val.
    fn merge_times(a: &Self, b: &Self, pa: &mut Pos, pb: &mut Pos, out: &mut Self) {
        use std::cmp::Ordering;
        let key = &a.keys[pa.k];
        let val = &a.vals[pa.v];
        let (ta_end, tb_end) = (a.val_offs.bound(pa.v + 1), b.val_offs.bound(pb.v + 1));
        while pa.t < ta_end && pb.t < tb_end {
            match a.times.index(pa.t).cmp(b.times.index(pb.t)) {
                Ordering::Less => {
                    let hi = gallop_idx(pa.t + 1, ta_end, |i| a.times.index(i) < b.times.index(pb.t));
                    out.ensure_kv(key, val);
                    out.extend_leaves(a, pa.t .. hi);
                    pa.t = hi;
                }
                Ordering::Greater => {
                    let hi = gallop_idx(pb.t + 1, tb_end, |i| b.times.index(i) < a.times.index(pa.t));
                    out.ensure_kv(key, val);
                    out.extend_leaves(b, pb.t .. hi);
                    pb.t = hi;
                }
                Ordering::Equal => {
                    let mut d = a.diffs.index(pa.t).clone();
                    d.plus_equals(b.diffs.index(pb.t));
                    if !d.is_zero() {
                        out.ensure_kv(key, val);
                        out.push_leaf(a.times.index(pa.t), &d);
                    }
                    pa.t += 1;
                    pb.t += 1;
                }
            }
        }
        if pa.t >= ta_end { pa.step_val(a); }
        if pb.t >= tb_end { pb.step_val(b); }
    }

    /// Advance times by `frontier`, consolidating each `(key, val)`'s run in
    /// isolation. Advancing preserves `(key, val)` order, so only the per-group
    /// time runs reorder — they are re-sorted and merged locally (a no-op sort
    /// when advancing is monotone); a group that wholly cancels is dropped.
    fn advance_trie(self, frontier: AntichainRef<T>) -> Self
    where
        T: Lattice,
    {
        let mut out = Self::default();
        let mut run: Vec<(T, R)> = Vec::new();
        for k in 0 .. self.keys.len() {
            for v in self.key_offs.bound(k) .. self.key_offs.bound(k + 1) {
                run.clear();
                for t in self.val_offs.bound(v) .. self.val_offs.bound(v + 1) {
                    let mut time = self.times.index(t).clone();
                    time.advance_by(frontier);
                    run.push((time, self.diffs.index(t).clone()));
                }
                consolidate(&mut run);
                for (t, d) in run.iter() {
                    out.ensure_kv(&self.keys[k], &self.vals[v]);
                    out.push_leaf(t, d);
                }
            }
        }
        out
    }
}

/// First index in `[start, end)` at which `pred` turns false, by galloping
/// (exponential) search. `pred` must hold for a prefix then not — i.e. `|x| x < target`.
fn gallop<U>(s: &[U], start: usize, end: usize, pred: impl Fn(&U) -> bool) -> usize {
    gallop_idx(start, end, |i| pred(&s[i]))
}

/// As [`gallop`], but over indices, for columns that are not slices.
fn gallop_idx(start: usize, end: usize, pred: impl Fn(usize) -> bool) -> usize {
    let mut pos = start;
    if pos < end && pred(pos) {
        let mut step = 1;
        while pos + step < end && pred(pos + step) { pos += step; step <<= 1; }
        step >>= 1;
        while step > 0 {
            if pos + step < end && pred(pos + step) { pos += step; }
            step >>= 1;
        }
        pos += 1;
    }
    pos
}

/// A sorted, consolidated trie chunk of `((key, val), time, diff)`, shared via `Rc`.
pub struct TrieChunk<K, V, T, R>(Rc<TrieStorage<K, V, T, R>>);

impl<K, V, T, R> TrieChunk<K, V, T, R> {
    /// The chunk's trie storage.
    pub fn storage(&self) -> &TrieStorage<K, V, T, R> { &self.0 }
}

impl<K, V, T, R> Clone for TrieChunk<K, V, T, R> {
    fn clone(&self) -> Self { TrieChunk(Rc::clone(&self.0)) }
}
impl<K, V, T, R> Default for TrieChunk<K, V, T, R> {
    fn default() -> Self { TrieChunk(Rc::new(TrieStorage::default())) }
}

/// Take the storage out of a chunk, copying only if the `Rc` is shared.
fn take<K: Clone, V: Clone, T: Clone, R: Clone>(chunk: TrieChunk<K, V, T, R>) -> TrieStorage<K, V, T, R> {
    Rc::try_unwrap(chunk.0).unwrap_or_else(|rc| (*rc).clone())
}

/// Wrap a non-empty storage as a chunk and append it to `out`.
fn emit<K, V, T, R>(storage: TrieStorage<K, V, T, R>, out: &mut VecDeque<TrieChunk<K, V, T, R>>) {
    if !storage.times.is_empty() { out.push_back(TrieChunk(Rc::new(storage))); }
}

// --- Container traits (batcher side, via `ContainerChunker<TrieChunk>`) ---

impl<K: 'static, V: 'static, T: 'static, R: 'static> Accountable for TrieChunk<K, V, T, R> {
    fn record_count(&self) -> i64 { (self.0.times.len() + self.0.stage.len()) as i64 }
}

impl<K, V, T, R> SizableContainer for TrieChunk<K, V, T, R>
where K: Ord + Clone + 'static, V: Ord + Clone + 'static, T: Ord + Clone + 'static, R: Semigroup + 'static {
    // Absorb at `TARGET`, the grading size, so the chunker emits pre-graded chunks.
    fn at_capacity(&self) -> bool { self.0.total_len() >= TARGET }
    fn ensure_capacity(&mut self, _stash: &mut Option<Self>) {
        let inner = Rc::make_mut(&mut self.0);
        inner.stage.reserve(TARGET.saturating_sub(inner.stage.len()));
    }
}

impl<K, V, T, R> Consolidate for TrieChunk<K, V, T, R>
where K: Ord + Clone + 'static, V: Ord + Clone + 'static, T: Ord + Clone + 'static, R: Semigroup + Ord + 'static {
    fn len(&self) -> usize { self.0.total_len() }
    fn clear(&mut self) { Rc::make_mut(&mut self.0).clear() }
    fn consolidate_into(&mut self, target: &mut Self) {
        let this = Rc::make_mut(&mut self.0);
        let mut rows = std::mem::take(&mut this.stage);
        // Fold in any formed content (cold: the chunker consolidates staged rows only).
        for k in 0 .. this.keys.len() {
            for v in this.key_offs.bound(k) .. this.key_offs.bound(k + 1) {
                for t in this.val_offs.bound(v) .. this.val_offs.bound(v + 1) {
                    rows.push(((this.keys[k].clone(), this.vals[v].clone()), this.times.index(t).clone(), this.diffs.index(t).clone()));
                }
            }
        }
        this.clear();
        consolidate_updates(&mut rows);
        let out = Rc::make_mut(&mut target.0);
        out.clear();
        for ((k, v), t, r) in rows.drain(..) {
            out.push_update_owned(k, v, t, r);
        }
    }
}

impl<K, V, T, R> PushInto<((K, V), T, R)> for TrieChunk<K, V, T, R>
where K: Clone + 'static, V: Clone + 'static, T: Clone + 'static, R: Clone + 'static {
    fn push_into(&mut self, item: ((K, V), T, R)) {
        Rc::make_mut(&mut self.0).stage.push(item);
    }
}

// --- The Chunk transducers (trace side) ---

impl<K, V, T, R> Chunk for TrieChunk<K, V, T, R>
where K: Ord + Clone + 'static, V: Ord + Clone + 'static, T: Lattice + Timestamp, R: Semigroup + PartialEq + 'static {
    type Time = T;

    const TARGET: usize = TARGET;

    fn len(&self) -> usize { self.0.total_len() }

    /// Trie-native binary merge of the two deques' loaded content, through their
    /// shared horizon. Reads chunks by reference (no materialization), walking chunk
    /// boundaries with positions and refilling each side from its deque as chunks
    /// are consumed; output flushes in near-`TARGET` chunks as it accumulates. When
    /// one deque runs dry, the other's unconsumed suffix is rebuilt once as a
    /// standalone chunk and pushed back to the front of its deque.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let mut c1 = in1.pop_front().unwrap();
        let mut c2 = in2.pop_front().unwrap();
        let (mut p1, mut p2) = (Pos::START, Pos::START);
        let mut result = TrieStorage::default();
        loop {
            let (pa, pb) = TrieStorage::merge_into(c1.storage(), c2.storage(), p1, p2, &mut result);
            (p1, p2) = (pa, pb);
            if result.formed_len() >= TARGET {
                emit(std::mem::take(&mut result), out);
            }
            // Refill a consumed side; if its deque is empty, stop.
            if p1.k >= c1.storage().keys.len() {
                match in1.pop_front() { Some(c) => { c1 = c; p1 = Pos::START; } None => break }
            }
            if p2.k >= c2.storage().keys.len() {
                match in2.pop_front() { Some(c) => { c2 = c; p2 = Pos::START; } None => break }
            }
        }
        emit(result, out);
        // Push back the survivor's unconsumed suffix (one copy), ahead of its
        // remaining loaded chunks.
        if p1.k < c1.storage().keys.len() {
            in1.push_front(TrieChunk(Rc::new(TrieStorage::suffix(c1.storage(), p1))));
        }
        if p2.k < c2.storage().keys.len() {
            in2.push_front(TrieChunk(Rc::new(TrieStorage::suffix(c2.storage(), p2))));
        }
    }

    /// Partition the front chunk by `frontier` (keep `>=`, ship `<`), folding kept
    /// times into `residual`. One chunk per call. A chunk that lands wholly on one
    /// side passes through by `Rc` clone, with no rebuild.
    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        residual: &mut Antichain<T>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let Some(chunk) = input.pop_front() else { return };
        let s = chunk.storage();
        let kept_count = s.times.iter().filter(|t| frontier.less_equal(t)).count();
        if kept_count == s.times.len() {
            for t in s.times.iter() { residual.insert_ref(t); }
            keep.push_back(chunk);
        } else if kept_count == 0 {
            ship.push_back(chunk);
        } else {
            let mut kept = TrieStorage::default();
            let mut shipped = TrieStorage::default();
            for k in 0 .. s.keys.len() {
                for v in s.key_offs.bound(k) .. s.key_offs.bound(k + 1) {
                    for t in s.val_offs.bound(v) .. s.val_offs.bound(v + 1) {
                        let target = if frontier.less_equal(s.times.index(t)) {
                            residual.insert_ref(s.times.index(t));
                            &mut kept
                        } else {
                            &mut shipped
                        };
                        target.ensure_kv(&s.keys[k], &s.vals[v]);
                        target.push_leaf(s.times.index(t), s.diffs.index(t));
                    }
                }
            }
            emit(kept, keep);
            emit(shipped, ship);
        }
    }

    /// Advance times by `frontier`, consolidating each complete `(key, val)` group
    /// and withholding the last unless `done`.
    ///
    /// Streams the input one chunk at a time, holding at most a chunk plus the
    /// withheld trailing `(key, val)` group (the carry, kept un-advanced so it
    /// appends cleanly onto the next chunk's head when the group straddles the
    /// boundary). Grading the emitted chunks is left to [`Chunk::settle`].
    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        let mut carry: Option<TrieStorage<K, V, T, R>> = None;
        while let Some(chunk) = input.pop_front() {
            let combined = match carry.take() {
                None => take(chunk),
                Some(mut c) => {
                    c.append_range(chunk.storage(), Pos::START, chunk.storage().keys.len());
                    c
                }
            };
            if combined.times.is_empty() { continue; }

            // The trailing `(key, val)` group is the last val's run; it may continue
            // in the next chunk, so it is withheld as the carry.
            let tail = combined.times.len() - combined.val_offs.bound(combined.vals.len() - 1);
            if tail == combined.times.len() {
                // A single `(key, val)` spans the chunk; hold it all as the carry.
                carry = Some(combined);
                continue;
            }
            let split = combined.times.len() - tail;
            let (keep, rest) = combined.split_at(split);
            carry = Some(rest);
            emit(keep.advance_trie(frontier), out);
        }
        if let Some(c) = carry {
            if done { emit(c.advance_trie(frontier), out); }
            else { input.push_front(TrieChunk(Rc::new(c))); }
        }
    }

    /// Maximal packing via the harness [`pack`](super::pack): coalesce by appending
    /// the next trie onto the carry (adjacent chunks of one sorted, consolidated
    /// chain), split with [`TrieStorage::split_at`], and seal by releasing excess
    /// capacity (committed chunks are immutable; a shared or exact chunk passes
    /// through untouched).
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        super::pack(
            input, done, out,
            |acc, next| {
                let acc = Rc::make_mut(&mut acc.0);
                acc.append_range(next.storage(), Pos::START, next.storage().keys.len());
            },
            |chunk, n| {
                let (first, rest) = take(chunk).split_at(n);
                (TrieChunk(Rc::new(first)), TrieChunk(Rc::new(rest)))
            },
            |mut chunk| {
                if let Some(storage) = Rc::get_mut(&mut chunk.0) { storage.shrink_to_fit(); }
                chunk
            },
        );
    }
}

// --- Cursor (trace side), navigating the trie directly (cf. `OrdValCursor`) ---

/// Implementations specific to the `Cursor` trait.
pub mod cursor {

    use std::marker::PhantomData;

    use timely::progress::Timestamp;

    use crate::difference::Semigroup;
    use crate::lattice::Lattice;
    use crate::trace::Navigable;
    use crate::trace::cursor::Cursor;

    use super::{gallop, TrieChunk};

    /// A cursor over a [`TrieChunk`], tracking the current key and value as
    /// absolute indices into the trie's `keys` / `vals` columns.
    pub struct TrieCursor<K, V, T, R> {
        key_cursor: usize,
        val_cursor: usize,
        phantom: PhantomData<(K, V, T, R)>,
    }

    impl<K, V, T, R> Cursor for TrieCursor<K, V, T, R>
    where K: Ord + Clone + 'static, V: Ord + Clone + 'static, T: Lattice + Timestamp, R: Ord + Semigroup + 'static {
        type Storage = TrieChunk<K, V, T, R>;

        type KeyContainer = Vec<K>;
        type Key<'a> = &'a K;
        type ValContainer = Vec<V>;
        type Val<'a> = &'a V;
        type ValOwn = V;
        type TimeContainer = Vec<T>;
        type TimeGat<'a> = &'a T;
        type Time = T;
        type DiffContainer = Vec<R>;
        type DiffGat<'a> = &'a R;
        type Diff = R;

        fn key_valid(&self, s: &Self::Storage) -> bool { self.key_cursor < s.0.keys.len() }
        fn val_valid(&self, s: &Self::Storage) -> bool {
            self.key_cursor < s.0.keys.len() && self.val_cursor < s.0.key_offs.bound(self.key_cursor + 1)
        }
        fn key<'a>(&self, s: &'a Self::Storage) -> &'a K { &s.0.keys[self.key_cursor] }
        fn val<'a>(&self, s: &'a Self::Storage) -> &'a V { &s.0.vals[self.val_cursor] }
        fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<&'a K> {
            if self.key_valid(s) { Some(self.key(s)) } else { None }
        }
        fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<&'a V> {
            if self.val_valid(s) { Some(self.val(s)) } else { None }
        }
        fn map_times<L: FnMut(&T, &R)>(&mut self, s: &Self::Storage, mut logic: L) {
            if !self.val_valid(s) { return; }
            for i in s.0.val_offs.bound(self.val_cursor) .. s.0.val_offs.bound(self.val_cursor + 1) {
                logic(s.0.times.index(i), s.0.diffs.index(i));
            }
        }
        fn step_key(&mut self, s: &Self::Storage) {
            self.key_cursor += 1;
            if self.key_valid(s) { self.rewind_vals(s); }
            else { self.key_cursor = s.0.keys.len(); }
        }
        fn seek_key(&mut self, s: &Self::Storage, key: &K) {
            self.key_cursor = gallop(&s.0.keys, self.key_cursor, s.0.keys.len(), |x| x < key);
            if self.key_valid(s) { self.rewind_vals(s); }
        }
        fn step_val(&mut self, s: &Self::Storage) {
            self.val_cursor += 1;
            if !self.val_valid(s) {
                self.val_cursor = s.0.key_offs.bound(self.key_cursor + 1);
            }
        }
        fn seek_val(&mut self, s: &Self::Storage, val: &V) {
            if !self.key_valid(s) { return; }
            let upper = s.0.key_offs.bound(self.key_cursor + 1);
            self.val_cursor = gallop(&s.0.vals, self.val_cursor, upper, |x| x < val);
        }
        fn rewind_keys(&mut self, s: &Self::Storage) {
            self.key_cursor = 0;
            if self.key_valid(s) { self.rewind_vals(s); }
        }
        fn rewind_vals(&mut self, s: &Self::Storage) {
            self.val_cursor = s.0.key_offs.bound(self.key_cursor);
        }
    }

    impl<K, V, T, R> Navigable for TrieChunk<K, V, T, R>
    where K: Ord + Clone + 'static, V: Ord + Clone + 'static, T: Lattice + Timestamp, R: Ord + Semigroup + 'static {
        type Cursor = TrieCursor<K, V, T, R>;

        fn cursor(&self) -> Self::Cursor {
            TrieCursor { key_cursor: 0, val_cursor: 0, phantom: PhantomData }
        }
    }

    impl<K, V, T, R> crate::trace::chunk::NavigableChunk for TrieChunk<K, V, T, R>
    where K: Ord + Clone + 'static, V: Ord + Clone + 'static, T: Lattice + Timestamp, R: Ord + Semigroup + 'static {
        fn bounds(&self) -> ((&K, &V, &T), (&K, &V, &T)) {
            let s = &self.0;
            ((&s.keys[0], &s.vals[0], s.times.index(0)),
             (s.keys.last().unwrap(), s.vals.last().unwrap(), s.times.last().unwrap()))
        }
    }
}

// --- Builder over ordered `Vec` input (the `VecOrdValBuilder` replacement) ---

/// A [`Builder`](crate::trace::Builder) accepting ordered `Vec<((K, V), T, R)>`
/// input and producing a graded [`ChunkBatch`](super::ChunkBatch) of [`TrieChunk`]s.
///
/// This is the shape `reduce_abelian` and `arrange_from_upsert` feed: sorted,
/// consolidated update vectors, pushed in order. Chunks are cut at `TARGET`
/// updates wherever that falls; the straddle cursor joins groups across cuts.
pub struct TrieBuilder<K, V, T, R> {
    /// The chunk under construction.
    current: TrieStorage<K, V, T, R>,
    /// Completed chunks, each of exactly `TARGET` updates.
    chunks: Vec<TrieChunk<K, V, T, R>>,
}

impl<K, V, T, R> Default for TrieBuilder<K, V, T, R> {
    fn default() -> Self {
        Self { current: TrieStorage::default(), chunks: Vec::new() }
    }
}

impl<K, V, T, R> crate::trace::Builder for TrieBuilder<K, V, T, R>
where K: Ord + Clone + 'static, V: Ord + Clone + 'static, T: Lattice + Timestamp, R: Semigroup + PartialEq + 'static {
    type Input = Vec<((K, V), T, R)>;
    type Time = T;
    type Output = super::ChunkBatch<TrieChunk<K, V, T, R>>;

    fn push(&mut self, chunk: &mut Self::Input) {
        for ((k, v), t, r) in chunk.drain(..) {
            self.current.push_update_owned(k, v, t, r);
            if self.current.formed_len() >= TARGET {
                let mut sealed = std::mem::take(&mut self.current);
                sealed.shrink_to_fit();
                self.chunks.push(TrieChunk(Rc::new(sealed)));
            }
        }
    }

    fn done(mut self) -> Option<Self::Output> {
        if self.current.formed_len() > 0 {
            self.current.shrink_to_fit();
            self.chunks.push(TrieChunk(Rc::new(self.current)));
        }
        (!self.chunks.is_empty()).then(|| super::ChunkBatch::new(self.chunks))
    }
}

// --- The `ord_neu` replacement surface ---

use crate::batcher::merge::chunker::ContainerChunker;

/// A trace implementation backed by trie chunks: the [`OrdValSpine`](crate::trace::implementations::ord_neu::OrdValSpine) replacement.
pub type OrdValSpine<K, V, T, R> = super::ChunkSpine<TrieChunk<K, V, T, R>>;
/// A batcher over trie chunks.
pub type OrdValBatcher<K, V, T, R> = super::ChunkBatcher<ContainerChunker<TrieChunk<K, V, T, R>>, TrieChunk<K, V, T, R>>;
/// A batch builder over ordered `Vec` input.
pub type OrdValBuilder<K, V, T, R> = TrieBuilder<K, V, T, R>;

/// A key-only trace implementation backed by trie chunks (`V = ()`).
pub type OrdKeySpine<K, T, R> = OrdValSpine<K, (), T, R>;
/// A key-only batcher over trie chunks.
pub type OrdKeyBatcher<K, T, R> = OrdValBatcher<K, (), T, R>;
/// A key-only batch builder over ordered `Vec` input.
pub type OrdKeyBuilder<K, T, R> = TrieBuilder<K, (), T, R>;

#[cfg(test)]
mod test {
    use timely::progress::Antichain;
    use std::collections::VecDeque;
    use std::rc::Rc;
    use super::{Chunk, Pos, TrieChunk, TrieStorage, TARGET};
    use crate::trace::Navigable;
    use crate::trace::chunk::merge_chains;
    use crate::consolidation::consolidate_updates;

    type Upd = ((u64, u64), u64, i64);

    // A sorted, consolidated trie chunk from raw updates.
    fn chunk(mut updates: Vec<Upd>) -> TrieChunk<u64, u64, u64, i64> {
        consolidate_updates(&mut updates);
        let mut s = TrieStorage::default();
        for ((k, v), t, r) in updates { s.push_update(&k, &v, &t, &r); }
        TrieChunk(Rc::new(s))
    }

    // Flatten a chunk sequence back to its update stream.
    fn flat<I: IntoIterator<Item = TrieChunk<u64, u64, u64, i64>>>(chunks: I) -> Vec<Upd> {
        let mut out = Vec::new();
        for c in chunks {
            let s = c.storage();
            for k in 0 .. s.keys.len() {
                for v in s.key_offs.bound(k) .. s.key_offs.bound(k + 1) {
                    for t in s.val_offs.bound(v) .. s.val_offs.bound(v + 1) {
                        out.push(((s.keys[k], s.vals[v]), *s.times.index(t), *s.diffs.index(t)));
                    }
                }
            }
        }
        out
    }

    // Structural invariants of a formed trie.
    fn check_invariants(s: &TrieStorage<u64, u64, u64, i64>) {
        assert_eq!(s.key_offs.count(), s.keys.len() + 1);
        assert_eq!(s.val_offs.count(), s.vals.len() + 1);
        assert_eq!(s.key_offs.bound(0), 0);
        assert_eq!(s.val_offs.bound(0), 0);
        assert_eq!(s.key_offs.last(), s.vals.len());
        assert_eq!(s.val_offs.last(), s.times.len());
        assert_eq!(s.times.len(), s.diffs.len());
        for k in 0 .. s.keys.len() {
            assert!(s.key_offs.bound(k) < s.key_offs.bound(k + 1), "empty key group");
        }
        for v in 0 .. s.vals.len() {
            assert!(s.val_offs.bound(v) < s.val_offs.bound(v + 1), "empty val group");
        }
        assert!(s.keys.windows(2).all(|w| w[0] < w[1]), "keys not strictly sorted");
        for k in 0 .. s.keys.len() {
            let vs = &s.vals[s.key_offs.bound(k) .. s.key_offs.bound(k + 1)];
            assert!(vs.windows(2).all(|w| w[0] < w[1]), "vals not strictly sorted within key");
        }
        for v in 0 .. s.vals.len() {
            for t in s.val_offs.bound(v) + 1 .. s.val_offs.bound(v + 1) {
                assert!(s.times.index(t - 1) < s.times.index(t), "times not strictly sorted within val");
            }
        }
        assert!(s.diffs.iter().all(|d| *d != 0), "zero diff retained");
    }

    // Cut a consolidated set into a chain of small chunks, so groups straddle boundaries.
    fn chain(updates: &[Upd], sz: usize) -> Vec<TrieChunk<u64, u64, u64, i64>> {
        updates.chunks(sz).map(|c| chunk(c.to_vec())).collect()
    }

    // A sorted, consolidated update set over a small space, so runs collide.
    fn gen(rng: &mut impl FnMut() -> u64, n: usize) -> Vec<Upd> {
        let mut v: Vec<Upd> = (0..n).map(|_| {
            let k = rng() % 20; let val = rng() % 3; let t = rng() % 8;
            let d = if rng() % 4 == 0 { -1 } else { 1 };
            ((k, val), t, d)
        }).collect();
        consolidate_updates(&mut v);
        v
    }

    fn rng_from(mut seed: u64) -> impl FnMut() -> u64 {
        move || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed }
    }

    // `push_update` consolidates an equal trailing triple, popping emptied groups.
    #[test]
    fn push_update_cancellation_pops_groups() {
        let mut s = TrieStorage::<u64, u64, u64, i64>::default();
        s.push_update(&1, &10, &100, &1);
        s.push_update(&2, &20, &200, &3);
        s.push_update(&2, &20, &200, &-3);
        check_invariants(&s);
        assert_eq!(s.keys, vec![1]);
        assert_eq!(s.times.len(), 1);
        // The trie remains extendable after the pop.
        s.push_update(&3, &30, &300, &1);
        check_invariants(&s);
        assert_eq!(s.keys, vec![1, 3]);
    }

    // `split_at` then `append_range` round-trips at every cut point.
    #[test]
    fn split_append_roundtrip() {
        let mut rng = rng_from(0xA076_1D64_78BD_642F);
        for _ in 0..40 {
            let updates = gen(&mut rng, 30);
            if updates.is_empty() { continue; }
            let full = chunk(updates.clone());
            let reference = flat([full.clone()]);
            for n in 0 ..= reference.len() {
                let (first, second) = full.storage().clone().split_at(n);
                if !first.times.is_empty() { check_invariants(&first); }
                if !second.times.is_empty() { check_invariants(&second); }
                assert_eq!(first.times.len(), n);
                let mut rejoined = first.clone();
                rejoined.append_range(&second, Pos::START, second.keys.len());
                if !rejoined.times.is_empty() { check_invariants(&rejoined); }
                assert_eq!(flat([TrieChunk(Rc::new(rejoined))]), reference, "cut at {n}");
            }
        }
    }

    // Property test: merging two multi-chunk chains (driven through `merge` by
    // `merge_chains`) reproduces the union of all updates, consolidated. Tiny
    // chunks force `(key, val)` groups to straddle chunk boundaries on both
    // sides, exercising the horizon stop and suffix push-back.
    #[test]
    fn merge_matches_reference() {
        let mut rng = rng_from(0x2545_F491_4F6C_DD1D);
        for _ in 0..300 {
            let (n1, n2) = ((rng() as usize % 60) + 1, (rng() as usize % 60) + 1);
            let u1 = gen(&mut rng, n1);
            let u2 = gen(&mut rng, n2);
            if u1.is_empty() || u2.is_empty() { continue; }
            let sz = (rng() as usize % 5) + 1;

            let mut out = VecDeque::new();
            merge_chains(chain(&u1, sz), chain(&u2, sz), &mut out);
            for c in out.iter() { check_invariants(c.storage()); }
            let merged = flat(out);

            let mut reference: Vec<Upd> = u1.iter().chain(u2.iter()).cloned().collect();
            consolidate_updates(&mut reference);

            assert_eq!(merged, reference, "chunk size {sz}\n  u1={u1:?}\n  u2={u2:?}");
        }
    }

    // `extract` partitions by frontier and passes uniform chunks through untouched.
    #[test]
    fn extract_partitions_and_folds_residual() {
        use crate::trace::chunk::{is_graded, settle_all};

        let n = 4 * TARGET as u64;
        let mut input: VecDeque<_> = (0..n).map(|i| chunk(vec![((i, 0), i % 2, 1)])).collect();
        let frontier = Antichain::from_elem(1u64);
        let mut residual = Antichain::new();
        let (mut keep, mut ship) = (VecDeque::new(), VecDeque::new());
        while !input.is_empty() {
            TrieChunk::extract(&mut input, frontier.borrow(), &mut residual, &mut keep, &mut ship);
        }
        let (keep, ship) = (settle_all(keep), settle_all(ship));

        assert_eq!(residual, Antichain::from_elem(1u64));
        assert!(is_graded(&keep));
        assert!(is_graded(&ship));
        assert_eq!(keep.iter().map(Chunk::len).sum::<usize>(), n as usize / 2);
        assert_eq!(ship.iter().map(Chunk::len).sum::<usize>(), n as usize / 2);
        for c in keep.iter().chain(ship.iter()) { check_invariants(c.storage()); }
    }

    // `advance` advances and consolidates complete `(key, val)` groups eagerly,
    // withholding the (possibly-growing) last group as the carry when not `done`.
    #[test]
    fn advance_emits_complete_groups_eagerly() {
        let frontier = Antichain::from_elem(5u64);
        // Group (0,0) is complete within this chunk; group (1,0) might still grow.
        let mut q = VecDeque::from([chunk(vec![((0, 0), 0, 1), ((0, 0), 1, 1), ((1, 0), 0, 1)])]);
        let mut out = VecDeque::new();
        TrieChunk::advance(&mut q, frontier.borrow(), false, &mut out);
        // The trailing group (1,0) is withheld as the carry at the front of `input`.
        assert_eq!(q.len(), 1);
        assert_eq!(Chunk::len(&q[0]), 1);
        // Group (0,0)'s times {0,1} advanced to 5 and consolidated, emitted now.
        assert_eq!(flat(out), vec![((0, 0), 5, 2)]);
    }

    // A single `(key, val)` spanning every pushed chunk: `advance` makes no
    // progress until `done`, accumulating in the carry.
    #[test]
    fn advance_single_key_spanning_pushes() {
        let frontier = Antichain::from_elem(100u64);
        let n = 50u64;
        let mut q = VecDeque::new();
        let mut out = VecDeque::new();
        for t in 0..n {
            q.push_back(chunk(vec![((7, 0), t, 1)]));
            TrieChunk::advance(&mut q, frontier.borrow(), false, &mut out);
        }
        TrieChunk::advance(&mut q, frontier.borrow(), true, &mut out);
        assert_eq!(flat(out), vec![((7, 0), 100, n as i64)]);
    }

    // Property test: driving `advance` resumably over many tiny chunks must match a
    // row oracle (advance each time to `max(t, frontier)`, then consolidate).
    #[test]
    fn advance_matches_row_reference() {
        let mut rng = rng_from(0x2545_F491_4F6C_DD1D);
        for _ in 0..200 {
            let n = rng() as usize % 60 + 1;
            let mut rows: Vec<Upd> = (0..n).map(|_| {
                let k = rng() % 8; let v = rng() % 3; let t = rng() % 6;
                let d = if rng() % 4 == 0 { -1 } else { 1 };
                ((k, v), t, d)
            }).collect();
            consolidate_updates(&mut rows);
            if rows.is_empty() { continue; }
            let f = rng() % 6;
            let frontier = Antichain::from_elem(f);
            let sz = rng() as usize % 5 + 1;

            let mut q = VecDeque::new();
            let mut out = VecDeque::new();
            for c in rows.chunks(sz) {
                q.push_back(chunk(c.to_vec()));
                TrieChunk::advance(&mut q, frontier.borrow(), false, &mut out);
            }
            TrieChunk::advance(&mut q, frontier.borrow(), true, &mut out);
            for c in out.iter() { check_invariants(c.storage()); }
            let got = flat(out);

            let mut want: Vec<Upd> = rows.iter().map(|&((k, v), t, d)| ((k, v), t.max(f), d)).collect();
            consolidate_updates(&mut want);

            assert_eq!(got, want, "frontier {f}, chunk size {sz}, rows {rows:?}");
        }
    }

    // `settle` produces a maximal packing: chunks `<= TARGET`, adjacent pairs
    // summing past `TARGET`, contents preserved exactly.
    #[test]
    fn settle_maximal_packing() {
        use crate::trace::chunk::is_graded;

        let t = TARGET;
        let sizes = [t / 3, t / 3, t / 3, t, t / 2, t / 2, t, 1, t - 1];
        let total: usize = sizes.iter().sum();
        let mut key = 0u64;
        let mut input = VecDeque::new();
        let mut output = VecDeque::new();
        for &s in &sizes {
            let updates: Vec<Upd> = (0..s).map(|_| { let k = key; key += 1; ((k, 0), 0, 1) }).collect();
            input.push_back(chunk(updates));
            TrieChunk::settle(&mut input, false, &mut output);
        }
        TrieChunk::settle(&mut input, true, &mut output);
        let chunks: Vec<_> = output.into();

        assert!(is_graded(&chunks), "not graded: {:?}", chunks.iter().map(Chunk::len).collect::<Vec<_>>());
        for c in chunks.iter() { check_invariants(c.storage()); }
        let got = flat(chunks);
        assert_eq!(got.len(), total);
        assert!(got.windows(2).all(|w| w[0].0.0 < w[1].0.0));
    }

    // The straddle-aware `ChunkBatch` cursor reconstructs the same grouped
    // updates as a flat reference, even when a key — and a `(key, val)`'s times —
    // span a chunk boundary.
    #[test]
    fn cursor_handles_straddle() {
        use crate::trace::cursor::Cursor;
        use crate::trace::chunk::ChunkBatch;

        let chunks = vec![
            chunk(vec![((0, 0), 0, 1), ((1, 0), 0, 1), ((1, 1), 0, 1)]),
            chunk(vec![((1, 1), 1, 1), ((1, 2), 0, 1)]),
            chunk(vec![((2, 0), 0, 1)]),
        ];
        let batch = ChunkBatch::new(chunks);

        let mut cursor = batch.cursor();
        let got = cursor.to_vec(&batch, |k| *k, |v| *v);
        let want = vec![
            ((0u64, 0u64), vec![(0u64, 1i64)]),
            ((1, 0), vec![(0, 1)]),
            ((1, 1), vec![(0, 1), (1, 1)]),
            ((1, 2), vec![(0, 1)]),
            ((2, 0), vec![(0, 1)]),
        ];
        assert_eq!(got, want);
    }

    // Driving `ChunkBatchMerger` to completion with tiny `fuel` (so it suspends
    // and settles on nearly every tick) yields the same advanced-and-consolidated
    // batch as a one-shot reference, and that batch is graded. Exercises the
    // resumable merge -> advance -> settle pipeline end to end.
    #[test]
    fn batch_merger_resumable_matches_reference() {
        use crate::trace::implementations::spine_fueled::Merger;
        use crate::trace::chunk::{ChunkBatch, ChunkBatchMerger, is_graded};
        use crate::trace::cursor::Cursor;

        let mut rng = rng_from(0x9E37_79B9_7F4A_7C15);

        fn batch(updates: &[Upd], sz: usize) -> ChunkBatch<TrieChunk<u64, u64, u64, i64>> {
            ChunkBatch::new(updates.chunks(sz).map(|c| chunk(c.to_vec())).collect())
        }
        fn read(b: &ChunkBatch<TrieChunk<u64, u64, u64, i64>>) -> Vec<Upd> {
            let mut out = Vec::new();
            let mut c = b.cursor();
            while c.key_valid(b) {
                let k = *c.key(b);
                while c.val_valid(b) {
                    let v = *c.val(b);
                    c.map_times(b, |t, d| out.push(((k, v), *t, *d)));
                    c.step_val(b);
                }
                c.step_key(b);
            }
            consolidate_updates(&mut out);
            out
        }

        for _ in 0..200 {
            let n1 = rng() as usize % 40 + 1;
            let u1 = gen(&mut rng, n1);
            let n2 = rng() as usize % 40 + 1;
            let u2 = gen(&mut rng, n2);
            if u1.is_empty() || u2.is_empty() { continue; }
            let sz = (rng() as usize % 4) + 1;
            let f = rng() % 6;
            let (s1, s2) = (batch(&u1, sz), batch(&u2, sz));
            let frontier = Antichain::from_elem(f);

            let mut merger = ChunkBatchMerger::new(&s1, &s2, frontier.borrow());
            loop {
                let mut fuel = 1isize;
                merger.work(&s1, &s2, &mut fuel);
                if fuel > 0 { break; }
            }
            let result = merger.done();

            let chunks: &[TrieChunk<u64, u64, u64, i64>] = result.as_ref().map_or(&[], |b| &b.chunks[..]);
            assert!(is_graded(chunks), "ungraded result: {:?}",
                chunks.iter().map(Chunk::len).collect::<Vec<_>>());
            for c in chunks.iter() { check_invariants(c.storage()); }
            let got = result.as_ref().map_or_else(Vec::new, read);
            let mut want: Vec<Upd> =
                u1.iter().chain(u2.iter()).map(|&((k, v), t, d)| ((k, v), t.max(f), d)).collect();
            consolidate_updates(&mut want);
            assert_eq!(result.is_none(), want.is_empty(), "absence must track emptiness\n  u1={u1:?}\n  u2={u2:?}\n  f={f}");
            assert_eq!(got, want, "fuel-driven merge mismatch\n  u1={u1:?}\n  u2={u2:?}\n  f={f}");
        }
    }

    // Snapshot-shaped data — one update per val, one shared time, diff +1 — must
    // stay compressed: strided offsets and constant time / diff columns, so the
    // chunk's storage beyond the keys and vals is O(1). Merging two snapshot
    // chunks and settling must preserve the compression.
    #[test]
    fn snapshot_chunks_stay_compressed() {
        use super::{Column, Offsets};

        fn assert_compressed(s: &TrieStorage<u64, u64, u64, i64>) {
            assert!(matches!(s.key_offs, Offsets { ref spill, .. } if spill.is_empty()), "key_offs spilled");
            assert!(matches!(s.val_offs, Offsets { ref spill, .. } if spill.is_empty()), "val_offs spilled");
            assert!(matches!(s.times, Column::Repeat { .. }), "times materialized");
            assert!(matches!(s.diffs, Column::Repeat { .. }), "diffs materialized");
        }

        // Formed directly (the builder path).
        let evens = chunk((0..1000u64).map(|k| ((2 * k, 0), 7, 1)).collect());
        let odds = chunk((0..1000u64).map(|k| ((2 * k + 1, 0), 7, 1)).collect());
        assert_compressed(evens.storage());

        // Merged (the trace maintenance path), then settled.
        let mut out = VecDeque::new();
        merge_chains(vec![evens.clone()], vec![odds], &mut out);
        let mut settled = VecDeque::new();
        TrieChunk::settle(&mut out, true, &mut settled);
        assert!(!settled.is_empty());
        for c in settled.iter() {
            check_invariants(c.storage());
            assert_compressed(c.storage());
        }

        // Advanced (compaction), which rewrites times to the frontier.
        let frontier = Antichain::from_elem(100u64);
        let mut q = VecDeque::from([evens]);
        let mut adv = VecDeque::new();
        TrieChunk::advance(&mut q, frontier.borrow(), true, &mut adv);
        for c in adv.iter() {
            check_invariants(c.storage());
            assert_compressed(c.storage());
        }
    }

    // The `TrieBuilder` (the `VecOrdValBuilder` replacement) forms a graded batch
    // from ordered pushed vectors, cutting at `TARGET` without losing anything.
    #[test]
    fn builder_forms_graded_batches() {
        use crate::trace::Builder;
        use crate::trace::chunk::is_graded;

        let n = 2 * TARGET as u64 + 17;
        let mut builder = super::TrieBuilder::<u64, u64, u64, i64>::default();
        // Push in several ordered vectors, splitting mid-key.
        let mut rows: Vec<Upd> = (0..n).map(|i| ((i / 3, i % 3), 0, 1)).collect();
        let tail = rows.split_off(rows.len() / 2);
        builder.push(&mut rows.clone());
        builder.push(&mut tail.clone());
        let batch = builder.done().unwrap();
        assert!(is_graded(&batch.chunks));
        for c in batch.chunks.iter() { check_invariants(c.storage()); }
        let got = flat(batch.chunks.iter().cloned());
        let want: Vec<Upd> = rows.into_iter().chain(tail).collect();
        assert_eq!(got, want);
    }
}
