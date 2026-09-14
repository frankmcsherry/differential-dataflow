//! One reduce sweep and callback harness for any bulk timestamp container.
use super::time_container::{Binary, Operand, Operation, Rows, TimeContainer};
use super::pending::Pending;
use super::{
    history::{Buffer, Replay},
    updates::{beyond, unique, visible, Keyed, Scratch, Updates},
};
use crate::{
    difference::Semigroup,
    operators::reduce::ReduceTactic,
    trace::{Description, Span},
};
use std::{cmp::Ordering, ops::Range};
use timely::progress::{Antichain, frontier::AntichainRef};

/// A unit of proxied reduce work, presented to the backend.
pub struct ReduceInstance<'a, T, B1, B2> {
    /// The accumulated input history.
    pub source_batches: &'a [B1],
    /// The freshly arrived input delta.
    pub input_batches: &'a [B1],
    /// The accumulated output history.
    pub output_batches: &'a [B2],
    /// The compaction frontier for loading (the retire's lower bound).
    pub lower: AntichainRef<'a, T>,
}

/// A presentation window preserving raw time support even when records cancel.
pub struct ReduceWindow<C, RIn, ROut> {
    /// Novel and prior input, netted together.
    pub input: Updates<C, RIn>,
    /// Raw novel (key, time) support, before advancement or netting.
    pub seeds: Keyed<C>,
    /// Prior output.
    pub output: Updates<C, ROut>,
}
impl<C: Default, RIn, ROut> Default for ReduceWindow<C, RIn, ROut> {
    fn default() -> Self {
        Self {
            input: Updates::default(),
            seeds: Keyed::default(),
            output: Updates::default(),
        }
    }
}

/// Value callbacks with bulk presentation and emission. Implementors need not
/// provide a row-oriented timestamp interface.
pub trait ProxyReduceBackend<C: TimeContainer, B1, B2> {
    /// Input differences.
    type RIn: Semigroup;
    /// Output differences.
    type ROut: Semigroup;
    /// Open one retirement's output session.
    fn begin(&mut self, description: Description<C::Time>);
    /// Present the next window of the key space, and advance `from` past it.
    ///
    /// On entry `from` is the inclusive lower bound on key hashes still to be covered. The backend
    /// chooses the window's exclusive upper bound and writes it back, or writes `None` to report the
    /// key space exhausted. An implementor must advance `from`, as it is guaranteed to be non-`None`.
    ///
    /// The window must present, for every key hash in `[from_before, from_after)` that either
    /// carries an update in the instance's novel batches or appears in `changed`: that key's merged
    /// input (novel and prior together, netted), its raw novel time support in `seeds`, and its
    /// accumulated output. A key must be reported entirely within the window that first mentions
    /// it: splitting one across windows drops the interaction between the halves. `changed` is
    /// ascending; the harness reads no key outside the window's range, so a backend that keeps its
    /// own key order need not consult the whole space.
    ///
    /// `seeds` must be recorded from the novel batches before any consolidation or advancement:
    /// a novel record that nets to zero against compacted history vanishes from `input`, but its
    /// time must still seed — that cancellation is exactly the case that loses updates otherwise.
    ///
    /// The size of the window is up to the backend: large enough to amortize the crossings, small
    /// enough that the presentations are affordable, as all are live at once.
    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, C::Time, B1, B2>,
        changed: &[u64],
        from: &mut Option<u64>,
        window: &mut ReduceWindow<C, Self::RIn, Self::ROut>,
    );
    /// Redeem corrections while keeping their timestamp representation.
    fn emit(&mut self, records: &Updates<C, Self::ROut>);
    /// Reconcile accumulated input and tentative output for a wave of keys.
    /// End offsets delimit each key's bracket, including empty brackets.
    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, Self::RIn)],
        out_ends: &[usize],
        output: &[(u64, Self::ROut)],
    ) -> (Vec<(u64, Self::ROut)>, Vec<usize>);
    /// Finish the session, including when it produced no updates.
    fn finish(&mut self) -> Option<B2>;
}

struct Run<C: TimeContainer> {
    times: C,
    pos: usize,
    suffix: C::Suffix,
}
impl<C: TimeContainer> Default for Run<C> {
    fn default() -> Self {
        Self {
            times: C::default(),
            pos: 0,
            suffix: C::Suffix::default(),
        }
    }
}
impl<C: TimeContainer> Run<C> {
    fn reset(&mut self) {
        self.pos = 0;
        self.times.suffix_meets(&mut self.suffix);
    }
    fn head(&self) -> Option<(&C, usize)> {
        (self.pos < self.times.len()).then_some((&self.times, self.pos))
    }
    fn meet_into(&self, output: &mut C) {
        self.times.suffix_meet(&self.suffix, self.pos, output);
    }

}
struct Schedule<C: TimeContainer> {
    bins: Vec<Option<Run<C>>>,
}
impl<C: TimeContainer> Default for Schedule<C> {
    fn default() -> Self {
        Self { bins: vec![] }
    }
}
impl<C: TimeContainer> Schedule<C> {
    fn insert(&mut self, mut times: C, scratch: &mut Scratch<C, ()>) {
        if times.is_empty() {
            return;
        }
        loop {
            let level = times.len().ilog2() as usize;
            self.bins
                .resize_with(self.bins.len().max(level + 1), || None);
            if let Some(old) = self.bins[level].take() {
                times.copy(Operand(
                    &old.times,
                    Rows::Range(old.pos..old.times.len()),
                ));
                unique(&mut times, scratch);
            } else {
                let mut run = Run {
                    times,
                    pos: 0,
                    suffix: C::Suffix::default(),
                };
                run.reset();
                self.bins[level] = Some(run);
                return;
            }
        }
    }
}

// Per-key histories; evaluation times and floors live in the batch, not these slots.
struct KeySweep<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> {
    key: u64,
    floor_row: usize,
    input: Replay<C, RIn>,
    output: Replay<C, ROut>,
    seeds: Run<C>,
    schedule: Schedule<C>,
    reached: C,
    produced: Buffer<C, ROut>,
    pending: C,
    candidates: C,
    time_scratch: Scratch<C, ()>,
    mask: Vec<bool>,
    carried: Vec<bool>,
    frontier_mask: Vec<bool>,
    selected: Vec<usize>,
}
impl<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> Default for KeySweep<C, RIn, ROut> {
    fn default() -> Self {
        Self { key: 0, floor_row: 0, input: Replay::default(), output: Replay::default(),
            seeds: Run::default(), schedule: Schedule::default(), reached: C::default(),
            produced: Buffer::default(), pending: C::default(), candidates: C::default(),
            time_scratch: Scratch::default(), mask: vec![], carried: vec![],
            frontier_mask: vec![], selected: vec![] }
    }
}
impl<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> KeySweep<C, RIn, ROut> {
    fn load(&mut self, window: &ReduceWindow<C, RIn, ROut>,
        (ir, or, sr): &(Range<usize>, Range<usize>, Range<usize>), floor: (&C, usize)) {
        self.seeds.times.clear();
        self.seeds.times.copy(Operand(&window.seeds.times, Rows::Range(sr.clone())));
        self.seeds.reset(); // Window seeds are already distinct and sorted by key/time.
        self.schedule.bins.clear();
        self.reached.clear();
        self.produced.clear();
        self.pending.clear();
        self.input.load(&window.input, ir.clone(), Some(floor));
        self.output.load(&window.output, or.clone(), Some(floor));
    }
    fn heads(&self) -> impl Iterator<Item = (&C, usize)> {
        self.input.head().into_iter().chain(self.output.head()).chain(self.seeds.head())
            .chain(self.schedule.bins.iter().flatten().filter_map(Run::head))
    }
    fn has_remaining(&self) -> bool { self.heads().next().is_some() }
    fn consume(&mut self, comparisons: &[Ordering]) -> bool {
        let mut equal = comparisons.iter().map(|c| c.is_eq());
        if self.input.head().is_some() && equal.next().unwrap() { self.input.step(); }
        if self.output.head().is_some() && equal.next().unwrap() { self.output.step(); }
        let mut fresh = false;
        if self.seeds.head().is_some() && equal.next().unwrap() {
            self.seeds.pos += 1;
            fresh = true;
        }
        for bin in &mut self.schedule.bins {
            if let Some(run) = bin {
                if equal.next().unwrap() { run.pos += 1; fresh = true; }
                if run.pos == run.times.len() { *bin = None; }
            }
        }
        assert!(equal.next().is_none());
        fresh
    }
    fn evaluate(&mut self, at: (&C, usize), floor: (&C, usize), carried: bool,
        fresh: bool, upper: &C, input: &mut Vec<(u64, RIn)>, output: &mut Vec<(u64, ROut)>) -> bool {
        if fresh {
            self.reached.copy(Operand::repeat_row(at.0, at.1, 1));
        }
        visible(&self.reached, at, &mut self.mask);
        let interested = fresh || self.mask.iter().any(|&v| v);
        if carried {
            if interested {
                self.pending.copy(Operand::repeat_row(at.0, at.1, 1));
            }
            return false;
        }
        self.candidates.clear();
        append_forward(
            &self.reached,
            at,
            &self.mask,
            &mut self.selected,
            &mut self.candidates,
        );
        if interested {
            read_and_forward(
                &mut self.input.buffer,
                at,
                Some(floor),
                input,
                &mut self.candidates,
                &mut self.mask,
                &mut self.selected,
            );
            let start = output.len();
            read_and_forward(
                &mut self.output.buffer,
                at,
                Some(floor),
                output,
                &mut self.candidates,
                &mut self.mask,
                &mut self.selected,
            );
            read_and_forward(
                &mut self.produced,
                at,
                Some(floor),
                output,
                &mut self.candidates,
                &mut self.mask,
                &mut self.selected,
            );
            crate::consolidation::consolidate_from(output, start);
        }
        unique(&mut self.candidates, &mut self.time_scratch);
        beyond(
            &self.candidates,
            upper,
            &mut self.carried,
            &mut self.frontier_mask,
        );
        self.selected.clear();
        self.selected
            .extend((0..self.candidates.len()).filter(|&r| self.carried[r]));
        self.pending.copy(Operand(
            &self.candidates,
            Rows::Indices(&self.selected),
        ));
        self.selected.clear();
        self.selected
            .extend((0..self.candidates.len()).filter(|&r| !self.carried[r]));
        if !self.selected.is_empty() {
            let mut future = C::default();
            future.copy(Operand(
                &self.candidates,
                Rows::Indices(&self.selected),
            ));
            self.schedule.insert(future, &mut self.time_scratch);
        }
        interested
    }
}

/// A bounded group of reduce walks. Time selection, readiness, floors and emission
/// operate on shared columns. Commit each returned wave before calling `next` again.
pub struct Sweep<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> {
    /// Keys of the current evaluation wave.
    pub keys: Vec<u64>,
    /// One timestamp per evaluation key.
    pub times: C,
    /// Input brackets, delimited by `input_ends`.
    pub input: Vec<(u64, RIn)>,
    /// Exclusive input bracket ends, including empty brackets.
    pub input_ends: Vec<usize>,
    /// Output brackets, delimited by `output_ends`.
    pub output: Vec<(u64, ROut)>,
    /// Exclusive output bracket ends, including empty brackets.
    pub output_ends: Vec<usize>,
    slots: Vec<KeySweep<C, RIn, ROut>>,
    used: usize, // Current group; spare slots retain allocations.
    live: Vec<usize>,
    active: Vec<Option<usize>>, // Simple keys need no retained slot.
    heads: C,
    candidates: C,
    ranges: Vec<Range<usize>>,
    window_ranges: Vec<(Range<usize>, Range<usize>, Range<usize>)>,
    simple: Vec<bool>,
    winners: Vec<usize>,
    challengers: Vec<(usize, usize)>,
    left: Vec<usize>,
    right: Vec<usize>,
    ready: Vec<usize>,
    comparisons: Vec<Ordering>,
    floors: C,
    minima: C,
    deferred: Keyed<C>,
    carried: Vec<bool>,
    mask: Vec<bool>,
    initial: bool,
    evaluating: bool,
    finished: bool,
}
impl<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> Default for Sweep<C, RIn, ROut> {
    fn default() -> Self {
        Self { keys: vec![], times: C::default(), input: vec![], input_ends: vec![],
            output: vec![], output_ends: vec![], slots: vec![], used: 0, live: vec![],
            active: vec![], heads: C::default(), floors: C::default(), minima: C::default(),
            candidates: C::default(), ranges: vec![], window_ranges: vec![], simple: vec![],
            winners: vec![], challengers: vec![], left: vec![], right: vec![], ready: vec![], comparisons: vec![],
            deferred: Keyed::default(), carried: vec![], mask: vec![],
            initial: false, evaluating: false, finished: true }
    }
}
impl<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> Sweep<C, RIn, ROut> {
    fn clear_wave(&mut self) {
        self.keys.clear(); self.times.clear(); self.input.clear(); self.output.clear();
        self.input_ends.clear(); self.output_ends.clear(); self.active.clear();
    }
    /// Load affected keys from a window with consolidated seeds. Single dominating
    /// seeds are classified in bulk and need no per-key replay state.
    pub fn load(&mut self, window: &ReduceWindow<C, RIn, ROut>, keys: &[u64], upper: &C) {
        assert!(!self.evaluating, "commit before replacing a wave");
        self.clear_wave(); self.deferred.clear(); self.live.clear();
        self.initial = true; self.finished = false;
        self.window_ranges.clear();
        self.window_ranges.extend(keys.iter().map(|&k| (key_range(&window.input.keys, k),
            key_range(&window.output.keys, k), key_range(&window.seeds.keys, k))));
        let ranges = &self.window_ranges;
        assert!(ranges.iter().all(|r| !r.2.is_empty()), "each key needs a seed");
        let requests: Vec<_> = ranges.iter().filter(|r| r.2.len() == 1).flat_map(|(ir, or, sr)| [
            Binary { left: Operand(&window.input.times, Rows::Range(ir.clone())),
                right: Operand::repeat_row(&window.seeds.times, sr.start, ir.len()) },
            Binary { left: Operand(&window.output.times, Rows::Range(or.clone())),
                right: Operand::repeat_row(&window.seeds.times, sr.start, or.len()) },
        ]).collect();
        self.mask.resize(requests.iter().map(Binary::len).sum(), false);
        C::less_equal(&requests, &mut self.mask);
        drop(requests);
        let mut offset = 0;
        self.simple.clear();
        self.simple.extend(ranges.iter().map(|(ir, or, sr)| {
            if sr.len() != 1 { return false; }
            let start = offset; offset += ir.len() + or.len();
            self.mask[start..offset].iter().all(|&x| x)
        }));
        self.left.clear();
        self.left.extend(ranges.iter().zip(&self.simple).filter(|(_, s)| **s).map(|(r, _)| r.2.start));
        self.heads.clear();
        self.heads.copy(Operand(&window.seeds.times, Rows::Indices(&self.left)));
        beyond(&self.heads, upper, &mut self.carried, &mut self.mask);
        self.ready.clear(); self.right.clear(); self.winners.clear();
        let mut seed = 0;
        for (i, ((ir, or, _), simple)) in ranges.iter().zip(&self.simple).enumerate() {
            if *simple {
                if self.carried[seed] {
                    self.right.push(seed);
                    self.deferred.keys.push(keys[i]);
                } else {
                    self.ready.push(seed);
                    self.keys.push(keys[i]); self.active.push(None);
                    accumulate(&window.input, ir.clone(), &mut self.input);
                    accumulate(&window.output, or.clone(), &mut self.output);
                    self.input_ends.push(self.input.len()); self.output_ends.push(self.output.len());
                }
                seed += 1;
            } else { self.winners.push(i); }
        }
        self.times.copy(Operand(&self.heads, Rows::Indices(&self.ready)));
        self.deferred.times.copy(Operand(&self.heads, Rows::Indices(&self.right)));
        self.floors.clear();
        self.ranges.clear();
        self.ranges.extend(self.winners.iter().map(|&i| ranges[i].2.clone()));
        window.seeds.times.meet_reduce(&self.ranges, &mut self.floors);
        self.used = self.winners.len();
        self.slots.resize_with(self.slots.len().max(self.used), KeySweep::default);
        for (slot, &i) in self.winners.iter().enumerate() {
            self.slots[slot].key = keys[i]; self.slots[slot].floor_row = slot;
            self.slots[slot].load(window, &ranges[i], (&self.floors, slot));
            self.live.push(slot);
        }
    }
    fn settle(&mut self) {
        self.live.retain(|&i| self.slots[i].has_remaining());
        self.minima.clear();
        self.ranges.clear();
        for (row, &i) in self.live.iter().enumerate() {
            let s = &mut self.slots[i];
            let start = self.minima.len();
            s.input.meet_into(&mut self.minima); s.output.meet_into(&mut self.minima);
            s.seeds.meet_into(&mut self.minima);
            for run in s.schedule.bins.iter().flatten() { run.meet_into(&mut self.minima); }
            self.ranges.push(start..self.minima.len()); s.floor_row = row;
        }
        self.floors.clear();
        self.minima.meet_reduce(&self.ranges, &mut self.floors);
        for &i in &self.live {
            let s = &mut self.slots[i];
            s.reached.apply(Operation::Join, &[(0..s.reached.len(), Operand::repeat_row(&self.floors, s.floor_row, s.reached.len()))]);
            unique(&mut s.reached, &mut s.time_scratch);
        }
    }
    /// Produce a wave of ready evaluations. False means this group is drained.
    pub fn next(&mut self, upper: &C) -> bool {
        assert!(!self.evaluating, "commit corrections before resuming a wave");
        if std::mem::take(&mut self.initial) && !self.keys.is_empty() {
            self.evaluating = true; return true;
        }
        self.clear_wave();
        while !self.live.is_empty() {
            // Collect once in key/range order. Each comparison then resolves one
            // pair of lanes, with indices selecting all keys' candidates together.
            self.candidates.clear(); self.ranges.clear();
            let mut sources = Vec::new();
            for &i in &self.live {
                let start = sources.len();
                sources.extend(self.slots[i].heads().map(|(t, r)| Operand::repeat_row(t, r, 1)));
                self.ranges.push(start..sources.len());
            }
            self.candidates.copy_many(&sources);
            drop(sources);
            self.winners.clear(); self.challengers.clear();
            self.winners.extend(self.ranges.iter().map(|r| r.start));
            self.challengers.extend(self.ranges.iter().enumerate().filter(|(_, r)| r.len() > 1)
                .map(|(key, r)| (key, r.start + 1)));
            while !self.challengers.is_empty() {
                self.left.clear(); self.right.clear();
                self.left.extend(self.challengers.iter().map(|&(key, _)| self.winners[key]));
                self.right.extend(self.challengers.iter().map(|&(_, candidate)| candidate));
                self.comparisons.resize(self.challengers.len(), Ordering::Equal);
                C::compare(&[Binary {
                    left: Operand(&self.candidates, Rows::Indices(&self.left)),
                    right: Operand(&self.candidates, Rows::Indices(&self.right)),
                }], &mut self.comparisons);
                for (&(key, candidate), c) in self.challengers.iter().zip(&self.comparisons) {
                    if c.is_gt() { self.winners[key] = candidate; }
                }
                self.challengers.retain_mut(|(key, candidate)| { *candidate += 1; *candidate < self.ranges[*key].end });
            }
            self.heads.clear();
            self.heads.copy(Operand(&self.candidates, Rows::Indices(&self.winners)));
            self.right.clear();
            for (r, &winner) in self.ranges.iter().zip(&self.winners) {
                self.right.resize(self.right.len() + r.len(), winner);
            }
            self.comparisons.resize(self.candidates.len(), Ordering::Equal);
            C::compare(&[Binary {
                left: Operand(&self.candidates, Rows::Range(0..self.candidates.len())),
                right: Operand(&self.candidates, Rows::Indices(&self.right)),
            }], &mut self.comparisons);
            beyond(&self.heads, upper, &mut self.carried, &mut self.mask);
            self.ready.clear();
            for (row, (&i, r)) in self.live.iter().zip(&self.ranges).enumerate() {
                let s = &mut self.slots[i];
                let fresh = s.consume(&self.comparisons[r.clone()]);
                if s.evaluate((&self.heads, row), (&self.floors, s.floor_row), self.carried[row], fresh, upper, &mut self.input, &mut self.output) {
                    self.ready.push(row); self.keys.push(s.key); self.active.push(Some(i));
                    self.input_ends.push(self.input.len()); self.output_ends.push(self.output.len());
                }
            }
            if !self.ready.is_empty() {
                self.times.copy(Operand(&self.heads, Rows::Indices(&self.ready)));
                self.evaluating = true; return true;
            }
            self.settle();
        }
        if !self.finished {
            self.deferred.times.copy_many(&self.slots[..self.used].iter().map(|s| Operand(&s.pending, Rows::Range(0..s.pending.len()))).collect::<Vec<_>>());
            for s in &self.slots[..self.used] { self.deferred.keys.resize(self.deferred.keys.len() + s.pending.len(), s.key); }
            self.finished = true;
        }
        false
    }
    /// Emit the callback's corrections in bulk, then advance the surviving keys' floors.
    pub fn commit(&mut self, corrections: &[(u64, ROut)], ends: &[usize], into: &mut Updates<C, ROut>) {
        assert!(self.evaluating && ends.len() == self.keys.len(), "commit needs one bracket per active key");
        let mut start = 0;
        let requests: Vec<_> = ends.iter().enumerate().map(|(row, &end)| {
            let n = end - start; start = end; Operand::repeat_row(&self.times, row, n)
        }).collect();
        assert_eq!(start, corrections.len());
        into.times.copy_many(&requests);
        start = 0;
        for (row, &end) in ends.iter().enumerate() {
            let rows = &corrections[start..end]; start = end;
            into.keys.resize(into.keys.len() + rows.len(), self.keys[row]);
            if let Some(i) = self.active[row] {
                if self.slots[i].has_remaining() { self.slots[i].produced.corrections((&self.times, row), rows); }
            }
        }
        into.ids.extend(corrections.iter().map(|r| r.0)); into.diffs.extend(corrections.iter().map(|r| r.1.clone()));
        if self.active.iter().any(Option::is_some) { self.settle(); }
        self.evaluating = false;
        self.clear_wave();
    }
    /// Deferred key/time associations, available after `next` reports exhaustion.
    pub fn pending(&self) -> &Keyed<C> { &self.deferred }
}

fn accumulate<C: TimeContainer, R: Semigroup>(
    data: &Updates<C, R>,
    range: Range<usize>,
    into: &mut Vec<(u64, R)>,
) {
    let mut pos = range.start;
    while pos < range.end {
        let id = data.ids[pos];
        let mut sum = data.diffs[pos].clone();
        pos += 1;
        while pos < range.end && data.ids[pos] == id {
            sum.plus_equals(&data.diffs[pos]);
            pos += 1;
        }
        if !sum.is_zero() {
            into.push((id, sum));
        }
    }
}

fn append_forward<C: TimeContainer>(
    times: &C,
    at: (&C, usize),
    mask: &[bool],
    rows: &mut Vec<usize>,
    into: &mut C,
) {
    rows.clear();
    rows.extend((0..times.len()).filter(|&r| !mask[r]));
    into.map(
        Operation::Join,
        &[Binary {
            left: Operand(times, Rows::Indices(rows)),
            right: Operand::repeat_row(at.0, at.1, rows.len()),
        }],
    );
}
fn read_and_forward<C: TimeContainer, R: Semigroup>(
    buffer: &mut Buffer<C, R>,
    at: (&C, usize),
    floor: Option<(&C, usize)>,
    into: &mut Vec<(u64, R)>,
    candidates: &mut C,
    mask: &mut Vec<bool>,
    rows: &mut Vec<usize>,
) {
    buffer.prepare(floor);
    let data = &buffer.data;
    visible(&data.times, at, mask);
    let mut pos = 0;
    while pos < data.len() {
        let id = data.ids[pos];
        let mut sum: Option<R> = None;
        while pos < data.len() && data.ids[pos] == id {
            if mask[pos] {
                if let Some(s) = &mut sum {
                    s.plus_equals(&data.diffs[pos]);
                } else {
                    sum = Some(data.diffs[pos].clone());
                }
            }
            pos += 1;
        }
        if let Some(sum) = sum.filter(|s| !s.is_zero()) {
            into.push((id, sum));
        }
    }
    append_forward(&data.times, at, mask, rows, candidates);
}

/// A complete reduce tactic with retained sweep slots and bulk presentation/emission.
pub struct ProxyReduceTactic<C: TimeContainer, RIn: Semigroup, ROut: Semigroup, Bk> {
    backend: Bk,
    pending: Pending<C>,
    sweep: Sweep<C, RIn, ROut>,
    key_batch_size: usize,
}
impl<C: TimeContainer, RIn: Semigroup, ROut: Semigroup, Bk>
    ProxyReduceTactic<C, RIn, ROut, Bk>
{
    /// Construct a tactic; use `with_key_batch_size` to bound simultaneous sweeps.
    pub fn new(backend: Bk) -> Self {
        Self {
            backend,
            pending: Pending::default(),
            sweep: Sweep::default(),
            key_batch_size: usize::MAX,
        }
    }
    /// Bound the number of simultaneously suspended keys, independently of windows.
    pub fn with_key_batch_size(mut self, count: usize) -> Self {
        assert!(count > 0);
        self.key_batch_size = count;
        self
    }
}
impl<
        C: TimeContainer + FromIterator<C::Time> + IntoIterator<Item = C::Time>,
        RIn: Semigroup,
        ROut: Semigroup,
        B1,
        B2,
        Bk: ProxyReduceBackend<C, B1, B2, RIn = RIn, ROut = ROut>,
    > ReduceTactic<C::Time, B1, B2> for ProxyReduceTactic<C, RIn, ROut, Bk>
{
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<C::Time>,
        upper: &Antichain<C::Time>,
        held: &Antichain<C::Time>,
    ) -> (Option<Span<C::Time, B2>>, Antichain<C::Time>) {
        use timely::progress::Timestamp;
        if held.elements().iter().all(|t| upper.less_equal(t)) {
            return (None, held.clone());
        }
        let upper_times: C = upper.elements().iter().cloned().collect();
        let due = self.pending.activate(&upper_times);
        let mut changed = due.keys.clone();
        changed.dedup();
        if changed.is_empty() && input_batches.is_empty() {
            return (None, self.pending.frontier().into_iter().collect());
        }
        let instance = ReduceInstance {
            source_batches: &source_batches,
            input_batches: &input_batches,
            output_batches: &output_batches,
            lower: lower.borrow(),
        };
        let description = Description::new(
            lower.clone(),
            upper.clone(),
            Antichain::from_elem(C::Time::minimum()),
        );
        self.backend.begin(description.clone());
        let mut from = Some(0);
        let mut window = ReduceWindow::default();
        let mut deltas: Updates<C, ROut> = Updates::default();
        let mut deferred = Keyed::default();
        while from.is_some() {
            window.input.clear();
            window.output.clear();
            window.seeds.clear();
            let before = from;
            self.backend
                .next_window(&instance, &changed, &mut from, &mut window);
            assert!(from.is_none() || from > before, "presentation must advance");
            let ds = due.keys.partition_point(|&k| Some(k) < before);
            let de = due.keys.partition_point(|&k| from.is_none_or(|f| k < f));
            window.seeds.append_range(&due, ds..de);
            window.seeds.consolidate();
            let mut keys = window.seeds.keys.clone();
            keys.dedup();
            deltas.clear();
            deferred.clear();
            for group in keys.chunks(self.key_batch_size) {
                self.sweep.load(&window, group, &upper_times);
                while self.sweep.next(&upper_times) {
                    let (corrections, ends) = self.backend.reduce_corrections(
                        &self.sweep.keys, &self.sweep.input_ends, &self.sweep.input,
                        &self.sweep.output_ends, &self.sweep.output,
                    );
                    self.sweep.commit(&corrections, &ends, &mut deltas);
                }
                deferred.append_range(self.sweep.pending(), 0..self.sweep.pending().len());
            }
            self.backend.emit(&deltas);
            self.pending.insert(std::mem::take(&mut deferred));
        }
        let result = Some(Span::new(description, self.backend.finish()));
        (result, self.pending.frontier().into_iter().collect())
    }
}
fn key_range(keys: &[u64], key: u64) -> Range<usize> {
    keys.partition_point(|&k| k < key)..keys.partition_point(|&k| k <= key)
}
