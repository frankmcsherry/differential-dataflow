//! Shared replay and buffer organization. All record-sized time work is bulk.
use super::time_container::Binary;
use super::time_container::{Operand, Operation, Rows, TimeContainer};
use super::updates::{adjacent, Scratch, Updates};
use crate::difference::Semigroup;
use std::ops::Range;

pub(super) struct Buffer<C: TimeContainer, R> {
    pub data: Updates<C, R>,
    scratch: Scratch<C, R>,
    floor: C,
    dirty: bool,
}
impl<C: TimeContainer, R: Semigroup> Default for Buffer<C, R> {
    fn default() -> Self {
        Self {
            data: Updates::default(),
            scratch: Scratch::default(),
            floor: C::default(),
            dirty: false,
        }
    }
}
impl<C: TimeContainer, R: Semigroup> Buffer<C, R> {
    pub fn clear(&mut self) {
        self.data.clear();
        self.floor.clear();
        self.dirty = false;
    }
    pub fn append(&mut self, source: &Updates<C, R>, range: Range<usize>) {
        self.dirty |= !range.is_empty();
        self.data.append_range(source, range);
    }
    pub fn corrections(&mut self, at: (&C, usize), rows: &[(u64, R)]) {
        self.data.times.copy(Operand::repeat_row(at.0, at.1, rows.len()));
        self.data.keys.resize(self.data.keys.len() + rows.len(), 0);
        self.data.ids.extend(rows.iter().map(|r| r.0));
        self.data.diffs.extend(rows.iter().map(|r| r.1.clone()));
        self.dirty |= !rows.is_empty();
    }
    pub fn prepare(&mut self, floor: Option<(&C, usize)>) {
        let unchanged = match floor {
            Some(f) => head_is((!self.floor.is_empty()).then_some((&self.floor, 0)), f),
            None => self.floor.is_empty(),
        };
        if !self.dirty && unchanged { return; }
        if let Some(floor) = floor {
            self.data
                .times
                .apply(Operation::Join, &[(0..self.data.len(), Operand::repeat_row(floor.0, floor.1, self.data.len()))]);
        }
        self.data.normalize(&mut self.scratch);
        if !unchanged {
            self.floor.clear();
            if let Some(floor) = floor {
                self.floor.copy(Operand::repeat_row(floor.0, floor.1, 1));
            }
        }
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
    pub fn load(&mut self, source: &Updates<C, R>, range: Range<usize>, floor: Option<(&C, usize)>) {
        self.data.clear();
        let n = range.len();
        let source_times = Operand(&source.times, Rows::Range(range.clone()));
        if let Some(f) = floor {
            self.data.times.map(
                Operation::Join,
                &[Binary {
                    left: source_times,
                    right: Operand::repeat_row(f.0, f.1, n),
                }],
            );
        } else {
            self.data.times.copy(source_times);
        }
        self.scratch.index.clear();
        self.scratch.index.extend(0..n);
        // Presentation normally supplies identity order. Preserve that order within
        // time ties; if a caller supplied unordered rows, establish it here first.
        let identity = |r: usize| (source.keys[range.start + r], source.ids[range.start + r]);
        if (1..n).any(|i| identity(i - 1) > identity(i)) {
            self.scratch.index.sort_by_key(|&r| identity(r));
        }
        self.data
            .times
            .order(&mut self.scratch.index, &[0..n], &mut self.scratch.order);
        adjacent(
            &self.data.times,
            &self.scratch.index,
            &mut self.scratch.comparisons,
        );
        self.scratch.keep.clear();
        self.ends.clear();
        let mut pos = 0;
        while pos < n {
            let mut end = pos + 1;
            while end < n && self.scratch.comparisons[end - 1] == std::cmp::Ordering::Equal {
                end += 1;
            }
            let before = self.data.len();
            while pos < end {
                let row = self.scratch.index[pos];
                let (key, id) = identity(row);
                let mut diff = source.diffs[range.start + row].clone();
                pos += 1;
                while pos < end && identity(self.scratch.index[pos]) == (key, id) {
                    diff.plus_equals(&source.diffs[range.start + self.scratch.index[pos]]);
                    pos += 1;
                }
                if !diff.is_zero() {
                    self.scratch.keep.push(row);
                    self.data.keys.push(key);
                    self.data.ids.push(id);
                    self.data.diffs.push(diff);
                }
            }
            // Entirely canceled runs must not become time heads.
            if self.data.len() > before {
                self.ends.push(self.data.len());
            }
        }
        if self.scratch.keep.len() != n
            || self.scratch.keep.iter().enumerate().any(|(i, &r)| i != r)
        {
            self.scratch.times.clear();
            self.scratch.times.copy(Operand(
                &self.data.times,
                Rows::Indices(&self.scratch.keep),
            ));
            std::mem::swap(&mut self.data.times, &mut self.scratch.times);
        }
        self.data.times.suffix_meets(&mut self.suffix);
        self.pos = 0;
        self.buffer.clear();
    }
    pub fn head(&self) -> Option<(&C, usize)> {
        (self.pos < self.data.len()).then_some((&self.data.times, self.pos))
    }
    pub fn meet_into(&self, output: &mut C) {
        self.data.times.suffix_meet(&self.suffix, self.pos, output);
    }
    pub fn end(&self) -> usize {
        self.ends[self.ends.partition_point(|&e| e <= self.pos)]
    }
    pub fn step(&mut self) {
        let end = self.end();
        self.buffer.append(&self.data, self.pos..end);
        self.pos = end;
    }
}

pub(super) fn compare_heads<C: TimeContainer>(
    (a, i): (&C, usize),
    (b, j): (&C, usize),
) -> std::cmp::Ordering {
    let mut result = [std::cmp::Ordering::Equal];
    C::compare(
        &[Binary {
            left: Operand(a, Rows::Range(i..i + 1)),
            right: Operand(b, Rows::Range(j..j + 1)),
        }],
        &mut result,
    );
    result[0]
}

pub(super) fn head_is<C: TimeContainer>(head: Option<(&C, usize)>, at: (&C, usize)) -> bool {
    head.is_some_and(|head| compare_heads(head, at).is_eq())
}
