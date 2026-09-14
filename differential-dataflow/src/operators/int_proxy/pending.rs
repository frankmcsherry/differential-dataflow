//! Flat pending time/key runs. Activation touches key associations only when due.
use super::time_container::{extend_antichain, Binary, Operand, Rows, TimeContainer};
use super::updates::{adjacent, beyond, Keyed, Scratch};

struct Run<C: TimeContainer> {
    times: C,
    keys: Vec<u64>,
    ends: Vec<usize>,
    live: Vec<usize>,
    live_keys: usize,
    minimum: C,
}
impl<C: TimeContainer> Run<C> {
    fn new(updates: Keyed<C>, scratch: &mut Scratch<C, ()>) -> Self {
        scratch.index.clear();
        scratch.index.extend(0..updates.len());
        scratch.index.sort_by_key(|&r| updates.keys[r]);
        updates
            .times
            .order(&mut scratch.index, &[0..updates.len()], &mut scratch.order);
        adjacent(&updates.times, &scratch.index, &mut scratch.comparisons);
        scratch.keep.clear();
        let mut keys = Vec::new();
        let mut ends = Vec::new();
        let mut pos = 0;
        while pos < updates.len() {
            scratch.keep.push(scratch.index[pos]);
            let mut last = None;
            loop {
                let key = updates.keys[scratch.index[pos]];
                if last != Some(key) {
                    keys.push(key);
                    last = Some(key);
                }
                pos += 1;
                if pos == updates.len() || scratch.comparisons[pos - 1] != std::cmp::Ordering::Equal
                {
                    break;
                }
            }
            ends.push(keys.len());
        }
        let mut times = C::default();
        times.copy(Operand(&updates.times, Rows::Indices(&scratch.keep)));
        let live = (0..times.len()).collect::<Vec<_>>();
        let mut minimum = C::default();
        extend_antichain(&mut minimum, &times, &live);
        Self {
            times,
            live_keys: keys.len(),
            keys,
            ends,
            live,
            minimum,
        }
    }
    fn range(&self, row: usize) -> std::ops::Range<usize> {
        if row == 0 {
            0..self.ends[0]
        } else {
            self.ends[row - 1]..self.ends[row]
        }
    }
    fn append(&self, groups: &[usize], into: &mut Keyed<C>) {
        // A selection only for the associations being moved, not retained keys.
        let mut rows = Vec::new();
        for &r in groups {
            let range = self.range(r);
            into.keys.extend_from_slice(&self.keys[range.clone()]);
            rows.resize(rows.len() + range.len(), r);
        }
        into.times
            .copy(Operand(&self.times, Rows::Indices(&rows)));
    }
}

pub(super) struct Pending<C: TimeContainer> {
    runs: Vec<Option<Run<C>>>,
    mask: Vec<bool>,
    scratch: Vec<bool>,
    order: Scratch<C, ()>,
}
impl<C: TimeContainer> Default for Pending<C> {
    fn default() -> Self {
        Self {
            runs: vec![],
            mask: vec![],
            scratch: vec![],
            order: Scratch::default(),
        }
    }
}
impl<C: TimeContainer> Pending<C> {
    pub fn insert(&mut self, mut updates: Keyed<C>) {
        if updates.is_empty() {
            return;
        }
        loop {
            let run = Run::new(updates, &mut self.order);
            let level = (run.live_keys + run.live.len()).ilog2() as usize;
            self.runs
                .resize_with(self.runs.len().max(level + 1), || None);
            if let Some(old) = self.runs[level].take() {
                updates = Keyed::default();
                run.append(&run.live, &mut updates);
                old.append(&old.live, &mut updates);
            } else {
                self.runs[level] = Some(run);
                return;
            }
        }
    }
    pub fn activate(&mut self, upper: &C) -> Keyed<C> {
        let mut due: Keyed<C> = Keyed::default();
        for bin in &mut self.runs {
            let Some(run) = bin else {
                continue;
            };
            beyond(
                &run.minimum,
                upper,
                &mut self.mask,
                &mut self.scratch,
            );
            if self.mask.iter().all(|&b| b) {
                continue;
            }
            self.mask.clear();
            self.mask.resize(run.live.len(), false);
            self.scratch.resize(run.live.len(), false);
            for f in 0..upper.len() {
                C::less_equal(
                    &[Binary {
                        left: Operand::repeat_row(upper, f, run.live.len()),
                        right: Operand(&run.times, Rows::Indices(&run.live)),
                    }],
                    &mut self.scratch,
                );
                for (m, &s) in self.mask.iter_mut().zip(&self.scratch) {
                    *m |= s;
                }
            }
            let activated: Vec<_> = run
                .live
                .iter()
                .zip(&self.mask)
                .filter_map(|(&r, &carry)| (!carry).then_some(r))
                .collect();
            run.append(&activated, &mut due);
            for &r in &activated {
                run.live_keys -= run.range(r).len();
            }
            let mut pos = 0;
            run.live.retain(|_| {
                let carry = self.mask[pos];
                pos += 1;
                carry
            });
            if run.live.is_empty() {
                *bin = None;
                continue;
            }
            if run.live.len() * 2 <= run.times.len() && run.live_keys * 2 <= run.keys.len() {
                let mut compacted = Keyed::default();
                run.append(&run.live, &mut compacted);
                *run = Run::new(compacted, &mut self.order);
            } else {
                run.minimum.clear();
                extend_antichain(&mut run.minimum, &run.times, &run.live);
            }
        }
        due.normalize(&mut self.order);
        due
    }
    pub fn frontier(&self) -> C {
        let mut minimum = C::default();
        for run in self.runs.iter().flatten() {
            let rows: Vec<_> = (0..run.minimum.len()).collect();
            extend_antichain(&mut minimum, &run.minimum, &rows);
        }
        minimum
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::order::Product;
    #[test]
    fn activation_is_not_a_lexicographic_prefix() {
        let mut pending = Pending::<Vec<Product<u64, u64>>>::default();
        let mut updates: Keyed<Vec<Product<u64, u64>>> = Keyed::default();
        for k in 0..100 {
            for t in [Product::new(0, 4), Product::new(1, 0), Product::new(2, 2)] {
                updates.keys.push(k);
                updates.times.push(t);
            }
        }
        pending.insert(updates);
        assert_eq!(
            pending
                .runs
                .iter()
                .flatten()
                .map(|r| r.times.len())
                .sum::<usize>(),
            3
        );
        let due = pending.activate(&vec![
            Product::new(0, 3),
            Product::new(2, 0),
        ]);
        assert_eq!(due.len(), 100);
        assert!(due.times.iter().all(|t| *t == Product::new(1, 0)));
        assert_eq!(pending.activate(&vec![]).len(), 200);
        assert!(pending.frontier().is_empty());
    }
}
