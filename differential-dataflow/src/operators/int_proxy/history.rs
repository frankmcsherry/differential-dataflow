//! Time-ordered replay of proxy update histories, with meet-advancement, over columnar
//! time storage.
//!
//! Replay steps a run of edits in ascending time order into a working buffer that is
//! repeatedly *advanced* — times joined with the meet of the times still to be
//! considered — and consolidated. Advancement keeps the buffer collapsed (for totally
//! ordered times, a whole prefix folds to a single entry), which keeps a key with many
//! distinct times linear rather than quadratic: accumulations read the small buffer,
//! never the raw history.
//!
//! # Why this is a second copy of the replay algorithm
//!
//! The cursor tactics' `ValueHistory` owns this algorithm, and `IdHistory` was
//! previously an alias for `ValueHistory<u64, T, R>` — one implementation. The
//! divergence here is *representation*, not algorithm: `ValueHistory` stores owned
//! times (`Vec<(T, D)>` edits, `Vec<(T, T, usize, usize)>` history, `Vec<((V, T), D)>`
//! buffer), which for allocating time types costs one allocation per edit per load —
//! the measured dominant cost for columnar backends with nested times (e.g.
//! `Product<u64, PointStamp<u64>>`). The types here keep every time column in a
//! `ContainerOf<T>` (see [`column`](super::column)) and organize the replay by
//! permutation, so a load performs a handful of bulk copies and no per-time
//! allocations. Making `ValueHistory` itself generic over its time storage would fix
//! both paths at one site, but drags the reference-lifetimed cursor machinery through
//! the same genericity, and is deliberately not attempted here. Until then: same replay
//! discipline (one sort, suffix meets, step, advance-collapse), different storage, and
//! any *algorithmic* change must land in both.

use columnar::{Borrow, Clear, Columnar, ContainerOf, Index, Len, Push};

use crate::difference::Semigroup;

use super::bridge::ProxyBridge;
use super::column::{push_owned, suffix_meets_rev, TimeRef, TimeVec, UpdateCol};

/// A replayable history of `(value_id, time, diff)` edits over columnar time storage.
///
/// Loading consolidates the edits by `(value_id, time)` and orders the replay by
/// `(time, value_id)` in a single permutation sort (equal `(value_id, time)` pairs are
/// adjacent in either order, so one sort serves both consolidation and organization),
/// then precomputes suffix meets. Replay walks a cursor forward; stepped-in edits land
/// in a consolidated [`UpdateCol`] buffer.
pub(crate) struct IdHistory<T: Columnar, R> {
    /// Consolidated edits in replay order (ascending `(time, value_id)`).
    vids: Vec<u64>,
    times: ContainerOf<T>,
    diffs: Vec<R>,
    /// Reversed suffix meets: the meet of `times[i..]` is `meets_rev[len - 1 - i]`.
    meets_rev: ContainerOf<T>,
    /// Replay position: edits `[cursor..]` are un-replayed.
    cursor: usize,
    /// Stepped-in edits, advanced and consolidated by `advance_buffer_by`.
    buffer: UpdateCol<u64, T, R>,
    // Load staging and rebuild scratch (retain capacity across loads).
    stage_vids: Vec<u64>,
    stage_times: ContainerOf<T>,
    stage_diffs: Vec<R>,
    perm: Vec<usize>,
    scratch: T,
    scratch2: T,
}

impl<T, R> IdHistory<T, R>
where
    T: super::ProxyTime,
    R: Semigroup + Clone,
{
    pub fn new() -> Self {
        IdHistory {
            vids: Vec::new(),
            times: Default::default(),
            diffs: Vec::new(),
            meets_rev: Default::default(),
            cursor: 0,
            buffer: UpdateCol::new(),
            stage_vids: Vec::new(),
            stage_times: Default::default(),
            stage_diffs: Vec::new(),
            perm: Vec::new(),
            scratch: T::minimum(),
            scratch2: T::minimum(),
        }
    }

    /// Load the records `bridge[range]`, advancing each time by `advance_by` if
    /// supplied, and organize the replay (consolidate + sort + suffix meets). Clears any
    /// prior state; capacity is retained.
    pub fn load(&mut self, bridge: &ProxyBridge<T, R>, range: std::ops::Range<usize>, advance_by: Option<&T>) {
        self.stage_vids.clear();
        self.stage_times.clear();
        self.stage_diffs.clear();
        let view = bridge.times();
        for i in range {
            self.stage_vids.push(bridge.ids()[i].1);
            self.stage_diffs.push(bridge.diffs()[i].clone());
            if let Some(m) = advance_by {
                self.scratch.copy_from(view.get(i));
                self.scratch.join_assign(m);
                push_owned::<T>(&mut self.stage_times, &self.scratch);
            } else {
                self.stage_times.push(view.get(i));
            }
        }
        self.organize();
    }

    /// Consolidate the staged edits and organize the replay.
    fn organize(&mut self) {
        let Self { vids, times, diffs, meets_rev, cursor, buffer, stage_vids, stage_times, stage_diffs, perm, scratch, scratch2 } = self;
        *cursor = 0;
        buffer.clear();
        vids.clear();
        times.clear();
        diffs.clear();

        let stage = stage_times.borrow();
        perm.clear();
        Extend::extend(perm, 0..stage_vids.len());
        perm.sort_unstable_by(|&a, &b| stage.get(a).cmp(&stage.get(b)).then_with(|| stage_vids[a].cmp(&stage_vids[b])));

        // Merge equal (value_id, time) pairs — adjacent in (time, value_id) order too —
        // summing diffs and dropping zeros; the survivors are already in replay order.
        let mut index = 0;
        while index < perm.len() {
            let this = perm[index];
            let mut diff = stage_diffs[this].clone();
            index += 1;
            while index < perm.len() && stage_vids[perm[index]] == stage_vids[this] && stage.get(perm[index]) == stage.get(this) {
                diff.plus_equals(&stage_diffs[perm[index]]);
                index += 1;
            }
            if !diff.is_zero() {
                vids.push(stage_vids[this]);
                times.push(stage.get(this));
                diffs.push(diff);
            }
        }

        suffix_meets_rev::<T>(times.borrow(), 0..vids.len(), meets_rev, scratch, scratch2);
    }

    /// The next (least) un-replayed time.
    pub fn time(&self) -> Option<TimeRef<'_, T>> {
        (self.cursor < self.vids.len()).then(|| self.times.borrow().get(self.cursor))
    }
    /// The meet of all un-replayed times.
    pub fn meet(&self) -> Option<TimeRef<'_, T>> {
        (self.cursor < self.vids.len()).then(|| self.meets_rev.borrow().get(self.vids.len() - 1 - self.cursor))
    }
    /// The next un-replayed edit, as `(value_id, time, diff)`.
    pub fn edit(&self) -> Option<(u64, TimeRef<'_, T>, &R)> {
        (self.cursor < self.vids.len()).then(|| {
            (self.vids[self.cursor], self.times.borrow().get(self.cursor), &self.diffs[self.cursor])
        })
    }

    /// Move the next edit into the buffer.
    pub fn step(&mut self) {
        self.buffer.push_ref(
            self.vids[self.cursor],
            self.times.borrow().get(self.cursor),
            self.diffs[self.cursor].clone(),
        );
        self.cursor += 1;
    }
    /// Step edits while the next time equals `time`; true iff any did.
    pub fn step_while_time_is(&mut self, time: &T) -> bool {
        let mut found = false;
        while self.cursor < self.vids.len() && {
            self.scratch.copy_from(self.times.borrow().get(self.cursor));
            self.scratch == *time
        } {
            found = true;
            self.step();
        }
        found
    }
    /// Step edits while the next time is at most `time` in the *total* order (a superset
    /// of the partially-ordered downset; readers filter the buffer by `less_equal`
    /// themselves).
    pub fn step_through(&mut self, time: &T) {
        while self.cursor < self.vids.len() && {
            self.scratch.copy_from(self.times.borrow().get(self.cursor));
            self.scratch <= *time
        } {
            self.step();
        }
    }
    /// Advance buffered times by `meet` and consolidate — the collapse that keeps replay
    /// linear.
    pub fn advance_buffer_by(&mut self, meet: &T) {
        self.buffer.advance_by(meet, &mut self.scratch);
        self.buffer.consolidate();
    }
    /// The buffered (stepped-in, advanced, consolidated) edits.
    pub fn buffer(&self) -> &UpdateCol<u64, T, R> {
        &self.buffer
    }
}

/// A replayable history of interesting *times* — the seed source, the same meet-advance
/// replay pattern as [`IdHistory`] but carrying no values or diffs.
///
/// This type mirrors [`IdHistory`], but traverses only the times and does not watch for
/// cancelation of data (and their times): the int-proxy tactic determines all
/// interesting times before any collection manipulation (and potential cancelation)
/// occurs, and seeds may over-approximate — a spurious interesting time yields a zero
/// delta — so the times are taken raw, unconsolidated (a multiset).
pub(crate) struct TimeHistory<T: Columnar> {
    /// Un-replayed times, ascending from `cursor`; a raw multiset (duplicates retained).
    times: ContainerOf<T>,
    /// Reversed suffix meets, as in [`IdHistory`].
    meets_rev: ContainerOf<T>,
    cursor: usize,
    /// Stepped-in times, advanced and deduplicated.
    buffer: TimeVec<T>,
    perm: Vec<usize>,
    stage: ContainerOf<T>,
    scratch: T,
    scratch2: T,
}

impl<T> TimeHistory<T>
where
    T: super::ProxyTime,
{
    pub fn new() -> Self {
        TimeHistory {
            times: Default::default(),
            meets_rev: Default::default(),
            cursor: 0,
            buffer: TimeVec::new(),
            perm: Vec::new(),
            stage: Default::default(),
            scratch: T::minimum(),
            scratch2: T::minimum(),
        }
    }

    /// Load `times`, advancing each by `advance_by` if supplied, and organize the replay
    /// (sort + suffix meets). Clears any prior state; capacity is retained.
    pub fn load<'a>(&mut self, times: impl Iterator<Item = TimeRef<'a, T>>, advance_by: Option<&T>)
    where
        T: 'a,
    {
        self.cursor = 0;
        self.buffer.clear();
        self.stage.clear();
        for time in times {
            if let Some(m) = advance_by {
                self.scratch.copy_from(time);
                self.scratch.join_assign(m);
                push_owned::<T>(&mut self.stage, &self.scratch);
            } else {
                self.stage.push(time);
            }
        }
        let Self { times, meets_rev, perm, stage, scratch, scratch2, .. } = self;
        let view = stage.borrow();
        perm.clear();
        Extend::extend(perm, 0..view.len());
        perm.sort_unstable_by(|&a, &b| view.get(a).cmp(&view.get(b)));
        times.clear();
        for &i in perm.iter() {
            times.push(view.get(i));
        }
        suffix_meets_rev::<T>(times.borrow(), 0..times.len(), meets_rev, scratch, scratch2);
    }

    /// The next (least) un-replayed time.
    pub fn time(&self) -> Option<TimeRef<'_, T>> {
        (self.cursor < self.times.len()).then(|| self.times.borrow().get(self.cursor))
    }
    /// The meet of all un-replayed times.
    pub fn meet(&self) -> Option<TimeRef<'_, T>> {
        (self.cursor < self.times.len()).then(|| self.meets_rev.borrow().get(self.times.len() - 1 - self.cursor))
    }

    /// Step times while the next equals `time`; true iff any did.
    pub fn step_while_time_is(&mut self, time: &T) -> bool {
        let mut found = false;
        while self.cursor < self.times.len() && {
            self.scratch.copy_from(self.times.borrow().get(self.cursor));
            self.scratch == *time
        } {
            found = true;
            self.buffer.push_ref(self.times.borrow().get(self.cursor));
            self.cursor += 1;
        }
        found
    }

    /// Advance buffered times by `meet` and deduplicate — the collapse that keeps replay
    /// linear.
    pub fn advance_buffer_by(&mut self, meet: &T) {
        self.buffer.advance_by(meet, &mut self.scratch);
    }

    /// The buffered (stepped-in, advanced) times.
    pub fn buffer(&self) -> &TimeVec<T> {
        &self.buffer
    }
}
