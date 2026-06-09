//! A common staging area for unloaded updates, and a cursor over it.
//!
//! The read side of abstract storage replaces random `Cursor` navigation with
//! bulk *unload-by-keys* into a shared area: storage answers "give me the updates
//! for these keys" by filling a [`Staging`], and operators read the result. The
//! staging type is deliberately layout-shaped but storage-agnostic — it is a
//! columnar K→V→T→R area related to storage only through [`WithLayout`], so any
//! storage (row, columnar, paged) can produce one and any operator can consume it.
//!
//! [`Staging`] generalizes the per-key [`EditList`](crate::operators::EditList)
//! two ways: its values, times, and diffs live in the layout's
//! [`BatchContainer`]s (rather than `Vec`s of borrowed references), so a staging
//! area outlives the source cursor and admits non-row layouts; and it carries a
//! per-key bounds array so a single area holds *many* keys, the shape an
//! `extract(keys)` needs. Unlike the storage layers it imitates it keeps plain
//! cumulative `Vec<usize>` offsets and applies no singleton-update compression:
//! the updates it stages come already consolidated from their source.

use std::marker::PhantomData;

use crate::trace::cursor::Cursor;
use crate::trace::implementations::{BatchContainer, Layout, WithLayout, layout};

/// A consolidated K→V→T→R area filled by unloading updates for a list of keys.
///
/// The keys are stored in order in `keys`; key `i`'s values are the slice
/// `vals[key_offs[i] .. key_offs[i+1]]`, and value `j`'s `(time, diff)` updates
/// are `(times, diffs)[val_offs[j] .. val_offs[j+1]]`. Both offset arrays are
/// cumulative, start with a leading `0`, and end one longer than the layer they
/// index (`key_offs.len() == keys.len() + 1`, `val_offs.len() == vals.len() + 1`).
pub struct Staging<L: Layout> {
    /// The staged keys, in order.
    pub keys: L::KeyContainer,
    /// For each key, the upper bound (exclusive) of its values in `vals`.
    ///
    /// Length is `keys.len() + 1`, with a leading `0`; key `i`'s values are
    /// `vals[key_offs[i] .. key_offs[i+1]]`.
    pub key_offs: Vec<usize>,
    /// The staged values, concatenated across keys.
    pub vals: L::ValContainer,
    /// For each value, the upper bound (exclusive) of its updates in `times`/`diffs`.
    ///
    /// Length is `vals.len() + 1`, with a leading `0`; value `j`'s updates are
    /// `(times, diffs)[val_offs[j] .. val_offs[j+1]]`.
    pub val_offs: Vec<usize>,
    /// The staged update times, concatenated across values.
    pub times: L::TimeContainer,
    /// The staged update diffs, concatenated across values.
    pub diffs: L::DiffContainer,
    /// Owned scratch for advancing and consolidating one value's updates before
    /// sealing (see [`stage_update`](Staging::stage_update) /
    /// [`seal_scratch_val`](Staging::seal_scratch_val)).
    scratch: Vec<(layout::Time<L>, layout::Diff<L>)>,
}

impl<L: Layout> WithLayout for Staging<L> {
    type Layout = L;
}

impl<L: Layout> Default for Staging<L> {
    fn default() -> Self { Self::with_capacity(0, 0, 0) }
}

impl<L: Layout> Staging<L> {
    /// Allocates an empty staging area with the given capacities.
    pub fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
        let mut key_offs = Vec::with_capacity(keys + 1);
        key_offs.push(0);
        let mut val_offs = Vec::with_capacity(vals + 1);
        val_offs.push(0);
        Self {
            keys: L::KeyContainer::with_capacity(keys),
            key_offs,
            vals: L::ValContainer::with_capacity(vals),
            val_offs,
            times: L::TimeContainer::with_capacity(upds),
            diffs: L::DiffContainer::with_capacity(upds),
            scratch: Vec::new(),
        }
    }

    /// Clears the area without releasing its allocations.
    pub fn clear(&mut self) {
        self.keys.clear();
        self.key_offs.clear();
        self.key_offs.push(0);
        self.vals.clear();
        self.val_offs.clear();
        self.val_offs.push(0);
        self.times.clear();
        self.diffs.clear();
        self.scratch.clear();
    }

    /// The number of staged keys.
    pub fn len(&self) -> usize { self.keys.len() }
    /// Whether any keys are staged.
    pub fn is_empty(&self) -> bool { self.keys.is_empty() }

    /// Stages one `(time, diff)` update for the value currently being assembled.
    ///
    /// Updates are expected to arrive already consolidated (their source — a
    /// trace cursor — guarantees it); the staging area does not re-consolidate.
    pub fn push_update(&mut self, time: <L::TimeContainer as BatchContainer>::ReadItem<'_>, diff: <L::DiffContainer as BatchContainer>::ReadItem<'_>) {
        self.times.push_ref(time);
        self.diffs.push_ref(diff);
    }

    /// Completes the value `val`, associating it with the updates pushed since the
    /// previous `seal_val`.
    pub fn seal_val(&mut self, val: <L::ValContainer as BatchContainer>::ReadItem<'_>) {
        self.vals.push_ref(val);
        self.val_offs.push(self.times.len());
    }

    /// Stages one owned `(time, diff)` into the per-value scratch.
    ///
    /// Unlike [`push_update`](Staging::push_update) (which copies an already
    /// consolidated update straight in), the scratch path lets the caller advance
    /// times by a frontier before [`seal_scratch_val`](Staging::seal_scratch_val)
    /// consolidates them — the fill counterpart to `EditList::push` + `seal`.
    pub fn stage_update(&mut self, time: layout::Time<L>, diff: layout::Diff<L>) {
        self.scratch.push((time, diff));
    }

    /// Consolidates the scratch and, if any updates survive, seals them as `val`'s.
    ///
    /// Returns whether `val` was sealed (false if everything consolidated away, so
    /// callers can skip empty values exactly as `EditList::seal` does).
    pub fn seal_scratch_val(&mut self, val: <L::ValContainer as BatchContainer>::ReadItem<'_>) -> bool {
        crate::consolidation::consolidate(&mut self.scratch);
        if self.scratch.is_empty() { return false; }
        for (time, diff) in self.scratch.drain(..) {
            self.times.push_own(&time);
            self.diffs.push_own(&diff);
        }
        self.seal_val(val);
        true
    }

    /// Completes the key `key`, associating it with the values sealed since the
    /// previous `seal_key`.
    pub fn seal_key(&mut self, key: <L::KeyContainer as BatchContainer>::ReadItem<'_>) {
        self.keys.push_ref(key);
        self.key_offs.push(self.vals.len());
    }

    /// Acquires a cursor over the staged updates.
    pub fn cursor(&self) -> StagingCursor<L> {
        StagingCursor { key_cursor: 0, val_cursor: 0, phantom: PhantomData }
    }
}

/// Bulk, range-copy analogue of [`Unload::extract`](crate::trace::unload::Unload::extract),
/// specialized to a [`Staging`] source.
///
/// Where the blanket `extract` walks a cursor element by element (it must — a
/// generic cursor exposes only element access), this copies each matched key's
/// values, times, and diffs as contiguous *ranges* via [`BatchContainer::copy_range`]
/// (a `memcpy` for `Copy` elements), touching individual elements only for the
/// cheap offset bookkeeping. It prototypes what a native `extract` on real
/// storage (e.g. `ord_neu`) would do. `keys` must be sorted and distinct; absent
/// keys are dropped.
pub fn extract_bulk<L: Layout, KC>(
    src: &Staging<L>,
    keys: &KC,
    dest: &mut Staging<L>,
)
where
    KC: for<'a> BatchContainer<ReadItem<'a> = <L::KeyContainer as BatchContainer>::ReadItem<'a>>,
{
    dest.clear();
    let mut pos = 0;
    for index in 0 .. keys.len() {
        let key = keys.index(index);
        // Gallop to the first source key >= `key` (forward, like a cursor seek).
        pos += src.keys.advance(pos, src.keys.len(), |k| <L::KeyContainer as BatchContainer>::reborrow(k).lt(&<L::KeyContainer as BatchContainer>::reborrow(key)));
        if pos < src.keys.len() && src.keys.index(pos) == <L::KeyContainer as BatchContainer>::reborrow(key) {
            let vlo = src.key_offs[pos];
            let vhi = src.key_offs[pos + 1];
            let tlo = src.val_offs[vlo];
            let thi = src.val_offs[vhi];
            let tbase = dest.times.len();
            // Bulk range copies of the actual data.
            dest.vals.copy_range(&src.vals, vlo, vhi);
            dest.times.copy_range(&src.times, tlo, thi);
            dest.diffs.copy_range(&src.diffs, tlo, thi);
            // Per-value / per-key offset bookkeeping (cheap; just `usize`s).
            for j in vlo .. vhi {
                dest.val_offs.push(tbase + (src.val_offs[j + 1] - tlo));
            }
            dest.keys.push_ref(src.keys.index(pos));
            dest.key_offs.push(dest.vals.len());
        }
    }
}

/// A cursor navigating the contents of a [`Staging`] area.
pub struct StagingCursor<L: Layout> {
    /// Absolute position of the current key.
    key_cursor: usize,
    /// Absolute position of the current value.
    val_cursor: usize,
    /// Phantom marker for Rust happiness.
    phantom: PhantomData<L>,
}

impl<L: Layout> WithLayout for StagingCursor<L> {
    type Layout = L;
}

impl<L: Layout> Cursor for StagingCursor<L> {

    type Storage = Staging<L>;

    fn get_key<'a>(&self, storage: &'a Staging<L>) -> Option<Self::Key<'a>> { storage.keys.get(self.key_cursor) }
    fn get_val<'a>(&self, storage: &'a Staging<L>) -> Option<Self::Val<'a>> { if self.val_valid(storage) { Some(self.val(storage)) } else { None } }

    fn key<'a>(&self, storage: &'a Staging<L>) -> Self::Key<'a> { storage.keys.index(self.key_cursor) }
    fn val<'a>(&self, storage: &'a Staging<L>) -> Self::Val<'a> { storage.vals.index(self.val_cursor) }
    fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Staging<L>, mut logic: L2) {
        let lower = storage.val_offs[self.val_cursor];
        let upper = storage.val_offs[self.val_cursor + 1];
        for index in lower .. upper {
            logic(storage.times.index(index), storage.diffs.index(index));
        }
    }
    fn key_valid(&self, storage: &Staging<L>) -> bool { self.key_cursor < storage.keys.len() }
    fn val_valid(&self, storage: &Staging<L>) -> bool { self.val_cursor < storage.key_offs[self.key_cursor + 1] }
    fn step_key(&mut self, storage: &Staging<L>) {
        self.key_cursor += 1;
        if self.key_valid(storage) {
            self.rewind_vals(storage);
        }
        else {
            self.key_cursor = storage.keys.len();
        }
    }
    fn seek_key(&mut self, storage: &Staging<L>, key: Self::Key<'_>) {
        self.key_cursor += storage.keys.advance(self.key_cursor, storage.keys.len(), |x| <L::KeyContainer as BatchContainer>::reborrow(x).lt(&<L::KeyContainer as BatchContainer>::reborrow(key)));
        if self.key_valid(storage) {
            self.rewind_vals(storage);
        }
    }
    fn step_val(&mut self, storage: &Staging<L>) {
        self.val_cursor += 1;
        if !self.val_valid(storage) {
            self.val_cursor = storage.key_offs[self.key_cursor + 1];
        }
    }
    fn seek_val(&mut self, storage: &Staging<L>, val: Self::Val<'_>) {
        // No operator calls `seek_val`; provided for completeness and parity with other cursors.
        self.val_cursor += storage.vals.advance(self.val_cursor, storage.key_offs[self.key_cursor + 1], |x| <L::ValContainer as BatchContainer>::reborrow(x).lt(&<L::ValContainer as BatchContainer>::reborrow(val)));
    }
    fn rewind_keys(&mut self, storage: &Staging<L>) {
        self.key_cursor = 0;
        if self.key_valid(storage) {
            self.rewind_vals(storage);
        }
    }
    fn rewind_vals(&mut self, storage: &Staging<L>) {
        self.val_cursor = storage.key_offs[self.key_cursor];
    }
}
