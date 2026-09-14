//! Flat pending time/key runs. Activation touches key associations only when due.
use super::time_container::{Binary, Operand, Rows, TimeContainer};
use super::updates::{adjacent, beyond, Scratch, Updates};
use timely::progress::Antichain;

struct Run<C: TimeContainer> {
    times: C,
    keys: Vec<u64>,
    ends: Vec<usize>,
    live: Vec<usize>,
    live_keys: usize,
    minimum: C,
}
impl<C: TimeContainer> Run<C> {
    fn new(updates: Updates<C, i64>, scratch: &mut Scratch<C, ()>) -> Self {
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
        times.copy(Operand::Rows(&updates.times, Rows::Indices(&scratch.keep)));
        let live = (0..times.len()).collect::<Vec<_>>();
        let mut minimum = C::default();
        extend_minimum(&mut minimum, &times, &live);
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
    fn append(&self, groups: &[usize], into: &mut Updates<C, i64>) {
        // A selection only for the associations being moved, not retained keys.
        let mut rows = Vec::new();
        for &r in groups {
            let range = self.range(r);
            into.keys.extend_from_slice(&self.keys[range.clone()]);
            rows.resize(rows.len() + range.len(), r);
        }
        into.times
            .copy(Operand::Rows(&self.times, Rows::Indices(&rows)));
        into.ids.resize(into.keys.len(), 0);
        into.diffs.resize(into.keys.len(), 1);
    }
}

/// Extend an antichain entirely in collection storage. Materialize only the final
/// minimal times when handing a capability frontier back to the driver.
fn extend_minimum<C: TimeContainer>(minimum: &mut C, times: &C, rows: &[usize]) {
    let mut mask = Vec::new();
    let mut keep = Vec::new();
    let mut scratch = C::default();
    for &row in rows {
        mask.resize(minimum.len(), false);
        C::less_equal(
            &[Binary {
                left: Operand::Rows(minimum, Rows::Range(0..minimum.len())),
                right: Operand::Rows(
                    times,
                    Rows::Repeat {
                        row,
                        count: minimum.len(),
                    },
                ),
            }],
            &mut mask,
        );
        if mask.iter().any(|&v| v) {
            continue;
        }
        C::less_equal(
            &[Binary {
                left: Operand::Rows(
                    times,
                    Rows::Repeat {
                        row,
                        count: minimum.len(),
                    },
                ),
                right: Operand::Rows(minimum, Rows::Range(0..minimum.len())),
            }],
            &mut mask,
        );
        keep.clear();
        keep.extend((0..minimum.len()).filter(|&r| !mask[r]));
        if keep.len() != minimum.len() {
            scratch.clear();
            scratch.copy(Operand::Rows(minimum, Rows::Indices(&keep)));
            std::mem::swap(minimum, &mut scratch);
        }
        minimum.copy(Operand::Rows(times, Rows::Range(row..row + 1)));
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
    pub fn insert(&mut self, mut updates: Updates<C, i64>) {
        if updates.is_empty() {
            return;
        }
        loop {
            let run = Run::new(updates, &mut self.order);
            let level = (run.live_keys + run.live.len()).ilog2() as usize;
            self.runs
                .resize_with(self.runs.len().max(level + 1), || None);
            if let Some(old) = self.runs[level].take() {
                updates = Updates::default();
                run.append(&run.live, &mut updates);
                old.append(&old.live, &mut updates);
            } else {
                self.runs[level] = Some(run);
                return;
            }
        }
    }
    pub fn activate(&mut self, upper: &Antichain<C::Time>) -> Updates<C, i64> {
        let mut due: Updates<C, i64> = Updates::default();
        for bin in &mut self.runs {
            let Some(run) = bin else {
                continue;
            };
            beyond(
                &run.minimum,
                upper.elements(),
                &mut self.mask,
                &mut self.scratch,
            );
            if self.mask.iter().all(|&b| b) {
                continue;
            }
            self.mask.clear();
            self.mask.resize(run.live.len(), false);
            self.scratch.resize(run.live.len(), false);
            for f in upper.elements() {
                C::less_equal(
                    &[Binary {
                        left: Operand::Repeat(f, run.live.len()),
                        right: Operand::Rows(&run.times, Rows::Indices(&run.live)),
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
                let mut compacted = Updates::default();
                run.append(&run.live, &mut compacted);
                *run = Run::new(compacted, &mut self.order);
            } else {
                run.minimum.clear();
                extend_minimum(&mut run.minimum, &run.times, &run.live);
            }
        }
        due.consolidate();
        due
    }
    pub fn frontier(&self) -> Antichain<C::Time> {
        let mut minimum = C::default();
        for run in self.runs.iter().flatten() {
            let rows: Vec<_> = (0..run.minimum.len()).collect();
            extend_minimum(&mut minimum, &run.minimum, &rows);
        }
        (0..minimum.len()).map(|r| minimum.time_at(r)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::order::Product;
    #[test]
    fn activation_is_not_a_lexicographic_prefix() {
        let mut pending = Pending::<Vec<Product<u64, u64>>>::default();
        let mut updates: Updates<Vec<Product<u64, u64>>, i64> = Updates::default();
        for k in 0..100 {
            for t in [Product::new(0, 4), Product::new(1, 0), Product::new(2, 2)] {
                updates.keys.push(k);
                updates.ids.push(0);
                updates.times.push(t);
                updates.diffs.push(1);
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
        let due = pending.activate(&Antichain::from(vec![
            Product::new(0, 3),
            Product::new(2, 0),
        ]));
        assert_eq!(due.len(), 100);
        assert!(due.times.iter().all(|t| *t == Product::new(1, 0)));
        assert_eq!(pending.activate(&Antichain::new()).len(), 200);
        assert!(pending.frontier().is_empty());
    }
}
