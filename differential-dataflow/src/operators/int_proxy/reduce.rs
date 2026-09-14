//! One reduce sweep and callback harness for any bulk timestamp container.
use super::{
    time_container::{Binary, Operand, Operation, Rows, TimeContainer},
};
use super::pending::Pending;
use super::{
    history::{include, Buffer, Replay},
    updates::{beyond, unique, visible, Scratch, Updates},
};
use crate::{
    difference::Semigroup,
    operators::reduce::ReduceTactic,
    trace::{Description, Span},
};
use std::ops::Range;
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

/// A presentation window; seed differences/identities are ignored.
pub struct ReduceWindow<C, RIn, ROut> {
    /// Novel and prior input, netted together.
    pub input: Updates<C, RIn>,
    /// Raw novel (key, time) support, before advancement or netting.
    pub seeds: Updates<C, i64>,
    /// Prior output.
    pub output: Updates<C, ROut>,
}
impl<C: Default, RIn, ROut> Default for ReduceWindow<C, RIn, ROut> {
    fn default() -> Self {
        Self {
            input: Updates::default(),
            seeds: Updates::default(),
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
    fn head(&self) -> Option<C::Time> {
        (self.pos < self.times.len()).then(|| self.times.time_at(self.pos))
    }
    fn meet(&self) -> Option<C::Time> {
        self.times.suffix_meet(&self.suffix, self.pos)
    }
    fn consume(&mut self, at: &C::Time) -> bool {
        if self.head().as_ref() == Some(at) {
            self.pos += 1;
            true
        } else {
            false
        }
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
    fn head(&self) -> Option<C::Time> {
        self.bins.iter().flatten().filter_map(Run::head).min()
    }
    fn consume(&mut self, at: &C::Time) -> bool {
        let mut found = false;
        for bin in &mut self.bins {
            if let Some(run) = bin {
                found |= run.consume(at);
                if run.pos == run.times.len() {
                    *bin = None;
                }
            }
        }
        found
    }
    fn insert(&mut self, mut times: C, scratch: &mut Scratch<C, ()>) {
        if times.is_empty() {
            return;
        }
        loop {
            let level = times.len().ilog2() as usize;
            self.bins
                .resize_with(self.bins.len().max(level + 1), || None);
            if let Some(old) = self.bins[level].take() {
                times.copy(Operand::Rows(
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

/// Resumable reduce time walk. Keys/values are opaque identities; time work is bulk.
/// Source rows enter replay buffers in total time order. Seeds and generated joins
/// mark evaluations; reached witnesses preserve their influence at later times.
/// A callback must commit its corrections before the next evaluation can read them.
pub struct Sweep<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> {
    input: Replay<C, RIn>,
    output: Replay<C, ROut>,
    seeds: Run<C>,
    schedule: Schedule<C>,
    reached: C,
    produced: Buffer<C, ROut>,
    floor: Option<C::Time>,
    at: Option<C::Time>,
    pending: C,
    candidates: C,
    time_scratch: Scratch<C, ()>,
    mask: Vec<bool>,
    carried: Vec<bool>,
    frontier_mask: Vec<bool>,
    selected: Vec<usize>,
    simple: bool,
    single: Option<C::Time>,
    single_input: Vec<(u64, RIn)>,
    single_output: Vec<(u64, ROut)>,
}
impl<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> Default for Sweep<C, RIn, ROut> {
    fn default() -> Self {
        Self {
            input: Replay::default(),
            output: Replay::default(),
            seeds: Run::default(),
            schedule: Schedule::default(),
            reached: C::default(),
            produced: Buffer::default(),
            floor: None,
            at: None,
            pending: C::default(),
            candidates: C::default(),
            time_scratch: Scratch::default(),
            mask: vec![],
            carried: vec![],
            frontier_mask: vec![],
            selected: vec![],
            simple: false,
            single: None,
            single_input: vec![],
            single_output: vec![],
        }
    }
}
impl<C: TimeContainer, RIn: Semigroup, ROut: Semigroup> Sweep<C, RIn, ROut> {
    /// Initialize a selected key. Seeds are separate from netted record histories.
    pub fn load(
        &mut self,
        input: &Updates<C, RIn>,
        ir: Range<usize>,
        output: &Updates<C, ROut>,
        or: Range<usize>,
        seeds: &C,
        sr: Range<usize>,
    ) {
        self.seeds.times.clear();
        self.seeds.times.copy(Operand::Rows(seeds, Rows::Range(sr)));
        unique(&mut self.seeds.times, &mut self.time_scratch);
        self.seeds.reset();
        assert!(
            !self.seeds.times.is_empty(),
            "a sweep needs raw or owed seeds"
        );
        self.schedule.bins.clear();
        self.reached.clear();
        self.produced.clear();
        self.pending.clear();
        self.at = None;
        self.simple = false;
        self.single = None;
        // One dominating seed collapses the entire history to one evaluation.
        // This is a lattice property, not a product-specific shortcut.
        if self.seeds.times.len() == 1 {
            let at = self.seeds.times.time_at(0);
            self.mask.resize(ir.len() + or.len(), false);
            C::less_equal(
                &[
                    Binary {
                        left: Operand::Rows(&input.times, Rows::Range(ir.clone())),
                        right: Operand::Repeat(&at, ir.len()),
                    },
                    Binary {
                        left: Operand::Rows(&output.times, Rows::Range(or.clone())),
                        right: Operand::Repeat(&at, or.len()),
                    },
                ],
                &mut self.mask,
            );
            if self.mask.iter().all(|&m| m) {
                self.simple = true;
                self.single = Some(at);
                accumulate(input, ir, &mut self.single_input);
                accumulate(output, or, &mut self.single_output);
                return;
            }
        }
        self.floor = self.seeds.meet();
        self.input.load(input, ir, self.floor.as_ref());
        self.output.load(output, or, self.floor.as_ref());
    }
    fn head(&self) -> Option<C::Time> {
        [
            self.input.head(),
            self.output.head(),
            self.seeds.head(),
            self.schedule.head(),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    fn settle(&mut self) {
        let mut floor = None;
        include(&mut floor, self.input.meet());
        include(&mut floor, self.output.meet());
        include(&mut floor, self.seeds.meet());
        for run in self.schedule.bins.iter().flatten() {
            include(&mut floor, run.meet());
        }
        if let Some(floor) = floor {
            self.reached
                .apply(Operation::Join, &[(0..self.reached.len(), floor.clone())]);
            unique(&mut self.reached, &mut self.time_scratch);
            self.floor = Some(floor);
        }
    }
    /// Walk to one evaluation, appending its input/output brackets to caller buffers.
    /// Call `commit` before resuming this key. Other keys may suspend independently.
    pub fn next(
        &mut self,
        upper: &Antichain<C::Time>,
        input: &mut Vec<(u64, RIn)>,
        output: &mut Vec<(u64, ROut)>,
    ) -> Option<C::Time> {
        assert!(
            self.at.is_none(),
            "commit corrections before resuming a sweep"
        );
        if self.simple {
            let at = self.single.take()?;
            if upper.less_equal(&at) {
                self.pending.copy(Operand::Repeat(&at, 1));
                return None;
            }
            input.extend_from_slice(&self.single_input);
            output.extend_from_slice(&self.single_output);
            self.at = Some(at.clone());
            return Some(at);
        }
        while let Some(at) = self.head() {
            self.input.step_at(&at);
            self.output.step_at(&at);
            let fresh = self.seeds.consume(&at) | self.schedule.consume(&at);
            if fresh {
                self.reached.copy(Operand::Repeat(&at, 1));
            }
            visible(&self.reached, &at, &mut self.mask);
            let interested = fresh || self.mask.iter().any(|&v| v);
            if upper.less_equal(&at) {
                if interested {
                    self.pending.copy(Operand::Repeat(&at, 1));
                }
                self.settle();
                continue;
            }
            self.candidates.clear();
            append_forward(
                &self.reached,
                &at,
                &self.mask,
                &mut self.selected,
                &mut self.candidates,
            );
            if interested {
                read_and_forward(
                    &mut self.input.buffer,
                    &at,
                    self.floor.as_ref(),
                    input,
                    &mut self.candidates,
                    &mut self.mask,
                    &mut self.selected,
                );
                let start = output.len();
                read_and_forward(
                    &mut self.output.buffer,
                    &at,
                    self.floor.as_ref(),
                    output,
                    &mut self.candidates,
                    &mut self.mask,
                    &mut self.selected,
                );
                read_and_forward(
                    &mut self.produced,
                    &at,
                    self.floor.as_ref(),
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
                upper.elements(),
                &mut self.carried,
                &mut self.frontier_mask,
            );
            self.selected.clear();
            self.selected
                .extend((0..self.candidates.len()).filter(|&r| self.carried[r]));
            self.pending.copy(Operand::Rows(
                &self.candidates,
                Rows::Indices(&self.selected),
            ));
            self.selected.clear();
            self.selected
                .extend((0..self.candidates.len()).filter(|&r| !self.carried[r]));
            if !self.selected.is_empty() {
                let mut future = C::default();
                future.copy(Operand::Rows(
                    &self.candidates,
                    Rows::Indices(&self.selected),
                ));
                self.schedule.insert(future, &mut self.time_scratch);
            }
            if interested {
                self.at = Some(at.clone());
                return Some(at);
            }
            self.settle();
        }
        None
    }
    /// Incorporate a callback's corrections before selecting the next time.
    pub fn commit(&mut self, corrections: &[(u64, ROut)]) {
        let at = self.at.take().expect("commit needs a suspended sweep");
        if self.simple {
            return;
        }
        if self.head().is_some() {
            self.produced.corrections(&at, corrections);
        }
        self.settle();
    }
    /// Deferred timestamps, kept in their original representation.
    pub fn pending(&self) -> &C {
        &self.pending
    }
}

fn accumulate<C: TimeContainer, R: Semigroup>(
    data: &Updates<C, R>,
    range: Range<usize>,
    into: &mut Vec<(u64, R)>,
) {
    into.clear();
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
    at: &C::Time,
    mask: &[bool],
    rows: &mut Vec<usize>,
    into: &mut C,
) {
    rows.clear();
    rows.extend((0..times.len()).filter(|&r| !mask[r]));
    into.map(
        Operation::Join,
        &[Binary {
            left: Operand::Rows(times, Rows::Indices(rows)),
            right: Operand::Repeat(at, rows.len()),
        }],
    );
}
fn read_and_forward<C: TimeContainer, R: Semigroup>(
    buffer: &mut Buffer<C, R>,
    at: &C::Time,
    floor: Option<&C::Time>,
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
    slots: Vec<Sweep<C, RIn, ROut>>,
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
            slots: vec![],
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
        C: TimeContainer,
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
        let due = self.pending.activate(upper);
        let mut changed = due.keys.clone();
        changed.dedup();
        if changed.is_empty() && input_batches.is_empty() {
            return (None, self.pending.frontier());
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
        let mut in_all = Vec::new();
        let mut out_all = Vec::new();
        let mut in_ends = Vec::new();
        let mut out_ends = Vec::new();
        let mut batch_keys = Vec::new();
        let mut active = Vec::new();
        let mut deltas: Updates<C, ROut> = Updates::default();
        let mut deferred: Updates<C, i64> = Updates::default();
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
                while self.slots.len() < group.len() {
                    self.slots.push(Sweep::default());
                }
                for (i, &key) in group.iter().enumerate() {
                    self.slots[i].load(
                        &window.input,
                        key_range(&window.input.keys, key),
                        &window.output,
                        key_range(&window.output.keys, key),
                        &window.seeds.times,
                        key_range(&window.seeds.keys, key),
                    );
                }
                let mut live: Vec<_> = (0..group.len()).collect();
                while !live.is_empty() {
                    in_all.clear();
                    out_all.clear();
                    in_ends.clear();
                    out_ends.clear();
                    batch_keys.clear();
                    active.clear();
                    for &i in &live {
                        if let Some(at) = self.slots[i].next(upper, &mut in_all, &mut out_all) {
                            batch_keys.push(group[i]);
                            in_ends.push(in_all.len());
                            out_ends.push(out_all.len());
                            active.push((i, at));
                        }
                    }
                    live.clear();
                    if active.is_empty() {
                        break;
                    }
                    let (corrections, ends) = self.backend.reduce_corrections(
                        &batch_keys,
                        &in_ends,
                        &in_all,
                        &out_ends,
                        &out_all,
                    );
                    assert_eq!(ends.len(), active.len());
                    let mut start = 0;
                    for ((i, at), end) in active.drain(..).zip(ends) {
                        let rows = &corrections[start..end];
                        start = end;
                        deltas.times.copy(Operand::Repeat(&at, rows.len()));
                        deltas.keys.resize(deltas.keys.len() + rows.len(), group[i]);
                        deltas.ids.extend(rows.iter().map(|r| r.0));
                        deltas.diffs.extend(rows.iter().map(|r| r.1.clone()));
                        self.slots[i].commit(rows);
                        live.push(i);
                    }
                    assert_eq!(start, corrections.len());
                }
                for (i, &key) in group.iter().enumerate() {
                    let times = self.slots[i].pending();
                    let n = times.len();
                    deferred.times.copy(Operand::Rows(times, Rows::Range(0..n)));
                    deferred.keys.resize(deferred.keys.len() + n, key);
                    deferred.ids.resize(deferred.ids.len() + n, 0);
                    deferred.diffs.resize(deferred.diffs.len() + n, 1);
                }
            }
            self.backend.emit(&deltas);
            self.pending.insert(std::mem::take(&mut deferred));
        }
        let result = Some(Span::new(description, self.backend.finish()));
        (result, self.pending.frontier())
    }
}
fn key_range(keys: &[u64], key: u64) -> Range<usize> {
    keys.partition_point(|&k| k < key)..keys.partition_point(|&k| k <= key)
}
