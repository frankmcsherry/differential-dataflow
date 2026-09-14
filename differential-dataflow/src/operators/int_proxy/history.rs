//! Shared replay and buffer organization. All record-sized time work is bulk.
use super::time_container::{Operand, Operation, Rows, TimeContainer};
use super::updates::{adjacent, Scratch, Updates};
use crate::{difference::Semigroup, lattice::Lattice};
use std::ops::Range;

pub(super) struct Buffer<C: TimeContainer, R> {
    pub data: Updates<C, R>,
    scratch: Scratch<C, R>,
    floor: Option<C::Time>,
    dirty: bool,
}
impl<C: TimeContainer, R: Semigroup> Default for Buffer<C, R> {
    fn default() -> Self {
        Self {
            data: Updates::default(),
            scratch: Scratch::default(),
            floor: None,
            dirty: false,
        }
    }
}
impl<C: TimeContainer, R: Semigroup> Buffer<C, R> {
    pub fn clear(&mut self) {
        self.data.clear();
        self.floor = None;
        self.dirty = false;
    }
    pub fn append(&mut self, source: &Updates<C, R>, range: Range<usize>) {
        self.dirty |= !range.is_empty();
        self.data.append_range(source, range);
    }
    pub fn corrections(&mut self, at: &C::Time, rows: &[(u64, R)]) {
        self.data.times.copy(Operand::Repeat(at, rows.len()));
        self.data.keys.resize(self.data.keys.len() + rows.len(), 0);
        self.data.ids.extend(rows.iter().map(|r| r.0));
        self.data.diffs.extend(rows.iter().map(|r| r.1.clone()));
        self.dirty |= !rows.is_empty();
    }
    pub fn prepare(&mut self, floor: Option<&C::Time>) {
        if !self.dirty && self.floor.as_ref() == floor {
            return;
        }
        if let Some(floor) = floor {
            self.data
                .times
                .apply(Operation::Join, &[(0..self.data.len(), floor.clone())]);
        }
        self.data.normalize(&mut self.scratch);
        self.floor = floor.cloned();
        self.dirty = false;
    }
}

pub(super) struct Replay<C: TimeContainer, R> {
    pub data: Updates<C, R>,
    pub buffer: Buffer<C, R>,
    pub pos: usize,
    ends: Vec<usize>,
    suffix: C::Suffix,
    scratch: Scratch<C, R>,
}
impl<C: TimeContainer, R: Semigroup> Default for Replay<C, R> {
    fn default() -> Self {
        Self {
            data: Updates::default(),
            buffer: Buffer::default(),
            pos: 0,
            ends: vec![],
            suffix: C::Suffix::default(),
            scratch: Scratch::default(),
        }
    }
}
impl<C: TimeContainer, R: Semigroup> Replay<C, R> {
    pub fn load(&mut self, source: &Updates<C, R>, range: Range<usize>, floor: Option<&C::Time>) {
        self.data.clear();
        self.data.append_range(source, range);
        if let Some(f) = floor {
            self.data
                .times
                .apply(Operation::Join, &[(0..self.data.len(), f.clone())]);
        }
        self.data.normalize(&mut self.scratch);
        let n = self.data.len();
        self.scratch.index.clear();
        self.scratch.index.extend(0..n);
        self.data
            .times
            .order(&mut self.scratch.index, &[0..n], &mut self.scratch.order);
        adjacent(
            &self.data.times,
            &self.scratch.index,
            &mut self.scratch.comparisons,
        );
        self.ends.clear();
        for i in 1..n {
            if self.scratch.comparisons[i - 1] != std::cmp::Ordering::Equal {
                self.ends.push(i);
            }
        }
        if n > 0 {
            self.ends.push(n);
        }
        self.scratch.times.clear();
        self.scratch.keys.clear();
        self.scratch.ids.clear();
        self.scratch.data.clear();
        self.scratch.times.copy(Operand::Rows(
            &self.data.times,
            Rows::Indices(&self.scratch.index),
        ));
        for &r in &self.scratch.index {
            self.scratch.keys.push(self.data.keys[r]);
            self.scratch.ids.push(self.data.ids[r]);
            self.scratch.data.push(self.data.diffs[r].clone());
        }
        std::mem::swap(&mut self.data.times, &mut self.scratch.times);
        std::mem::swap(&mut self.data.keys, &mut self.scratch.keys);
        std::mem::swap(&mut self.data.ids, &mut self.scratch.ids);
        std::mem::swap(&mut self.data.diffs, &mut self.scratch.data);
        self.data.times.suffix_meets(&mut self.suffix);
        self.pos = 0;
        self.buffer.clear();
    }
    pub fn head(&self) -> Option<C::Time> {
        (self.pos < self.data.len()).then(|| self.data.times.time_at(self.pos))
    }
    pub fn meet(&self) -> Option<C::Time> {
        self.data.times.suffix_meet(&self.suffix, self.pos)
    }
    pub fn end(&self) -> usize {
        self.ends[self.ends.partition_point(|&e| e <= self.pos)]
    }
    pub fn step(&mut self) {
        let end = self.end();
        self.buffer.append(&self.data, self.pos..end);
        self.pos = end;
    }
    pub fn step_at(&mut self, at: &C::Time) {
        if self.head().as_ref() == Some(at) {
            self.step();
        }
    }
}

pub(super) fn include<T: Lattice + Clone>(meet: &mut Option<T>, other: Option<T>) {
    if let Some(t) = other {
        if let Some(m) = meet {
            m.meet_assign(&t);
        } else {
            *meet = Some(t);
        }
    }
}
