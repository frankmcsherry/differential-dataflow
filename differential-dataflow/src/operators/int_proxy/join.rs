//! A bounded bilinear join over an opaque time container.
use super::{
    time_container::{Binary, Operand, Operation, Rows, TimeContainer},
};
use super::{history::Replay, updates::Updates};
use crate::difference::{Multiply, Semigroup};
use std::{marker::PhantomData, ops::Range};

/// Value presentation/redemption for a container join. Each iterator owns its backend.
pub trait ProxyJoinBackend<C: TimeContainer, B0, B1> {
    /// Left differences.
    type R0: Semigroup + Multiply<Self::R1, Output = Self::ROut>;
    /// Right differences.
    type R1: Semigroup;
    /// Output differences.
    type ROut: Semigroup;
    /// Output container.
    type Output;
    /// Present complete common keys, sorted and consolidated by (key, id, time).
    /// Advance `from` strictly, or set it to None. Interpretation state lives until
    /// the next call; the iterator flushes matches before requesting another window.
    fn advance(
        &mut self,
        instance: &JoinInstance<C::Time, B0, B1>,
        from: &mut Option<u64>,
        left: &mut Updates<C, Self::R0>,
        right: &mut Updates<C, Self::R1>,
    );
    /// Redeem bounded matches into output containers, preserving collision checks.
    fn cross(
        &mut self,
        instance: &JoinInstance<C::Time, B0, B1>,
        matches: &mut JoinMatches<C, Self::ROut>,
        output: &mut Vec<Self::Output>,
    );
}

/// A unit of proxied join work, for presentation to the backend.
pub struct JoinInstance<T, B0, B1> {
    /// The first input's batches.
    pub batches0: Vec<B0>,
    /// The second input's batches.
    pub batches1: Vec<B1>,
    /// A lower bound on the meet of pairs of update times.
    ///
    /// This can be applied when loading updates to consolidate on load.
    pub lower: T,
}

/// Aligned match columns. Time storage is preserved through redemption.
pub struct JoinMatches<C, R> {
    /// Key and paired value identities.
    pub ids: Vec<(u64, (u64, u64))>,
    /// Joined timestamps.
    pub times: C,
    /// Multiplied differences.
    pub diffs: Vec<R>,
}
impl<C: Default, R> Default for JoinMatches<C, R> {
    fn default() -> Self {
        Self {
            ids: vec![],
            times: C::default(),
            diffs: vec![],
        }
    }
}
impl<C: TimeContainer, R> JoinMatches<C, R> {
    fn clear(&mut self) {
        self.ids.clear();
        self.times.clear();
        self.diffs.clear();
    }
}

/// Resumable work for one complete common key, including within a Cartesian block.
pub struct Walk<C: TimeContainer, R0: Semigroup, R1: Semigroup> {
    left: Replay<C, R0>,
    right: Replay<C, R1>,
    // Cartesian cursor: left row/end, right row/start/end.
    direct: Option<(usize, usize, usize, usize, usize)>,
    // Replay cursor: active side, its run end/current row, opposite buffer row.
    crossing: Option<(bool, usize, usize, usize)>,
    a_rows: Vec<usize>,
    b_rows: Vec<usize>,
}
impl<C: TimeContainer, R0: Semigroup, R1: Semigroup> Default for Walk<C, R0, R1> {
    fn default() -> Self {
        Self {
            left: Replay::default(),
            right: Replay::default(),
            direct: None,
            crossing: None,
            a_rows: vec![],
            b_rows: vec![],
        }
    }
}
impl<C: TimeContainer, R0: Semigroup, R1: Semigroup> Walk<C, R0, R1> {
    /// Reuse this walk for selected ranges; the sources must remain valid until drained.
    pub fn load(
        &mut self,
        left: &Updates<C, R0>,
        a: Range<usize>,
        right: &Updates<C, R1>,
        b: Range<usize>,
    ) {
        self.crossing = None;
        self.direct = if a.is_empty() || b.is_empty() {
            Some((0, 0, 0, 0, 0))
        } else if a.len() < 16 || b.len() < 16 {
            Some((a.start, a.end, b.start, b.start, b.end))
        } else {
            self.left.load(left, a, None);
            self.right.load(right, b, None);
            None
        };
    }
    /// Append up to `limit` matches (total destination length). False means drained.
    pub fn fill<ROut: Semigroup>(
        &mut self,
        key: u64,
        left: &Updates<C, R0>,
        right: &Updates<C, R1>,
        limit: usize,
        out: &mut JoinMatches<C, ROut>,
    ) -> bool
    where
        R0: Multiply<R1, Output = ROut>,
    {
        assert!(limit > out.ids.len());
        if let Some((mut row, end, mut column, start1, end1)) = self.direct {
            self.a_rows.clear();
            self.b_rows.clear();
            while row < end && out.ids.len() + self.a_rows.len() < limit {
                self.a_rows.push(row);
                self.b_rows.push(column);
                column += 1;
                if column == end1 {
                    column = start1;
                    row += 1;
                }
            }
            append_pairs(key, left, &self.a_rows, right, &self.b_rows, out);
            self.direct = Some((row, end, column, start1, end1));
            return row < end;
        }
        loop {
            if self.crossing.is_none() {
                let (a, b) = (self.left.head(), self.right.head());
                if a.is_none() && b.is_none() {
                    return false;
                }
                let take_left = a.is_some() && (b.is_none() || a < b);
                if take_left {
                    let meet = self.left.meet();
                    self.right.buffer.prepare(meet.as_ref());
                    if self.right.buffer.data.is_empty() {
                        self.left.step();
                        continue;
                    }
                    self.crossing = Some((true, self.left.end(), self.left.pos, 0));
                } else {
                    let meet = self.right.meet();
                    self.left.buffer.prepare(meet.as_ref());
                    if self.left.buffer.data.is_empty() {
                        self.right.step();
                        continue;
                    }
                    self.crossing = Some((false, self.right.end(), self.right.pos, 0));
                }
            }
            let (take_left, end, mut row, mut column) = self.crossing.unwrap();
            let n = if take_left {
                self.right.buffer.data.len()
            } else {
                self.left.buffer.data.len()
            };
            self.a_rows.clear();
            self.b_rows.clear();
            while row < end && out.ids.len() + self.a_rows.len() < limit {
                self.a_rows.push(row);
                self.b_rows.push(column);
                column += 1;
                if column == n {
                    column = 0;
                    row += 1;
                }
            }
            if take_left {
                append_pairs(
                    key,
                    &self.left.data,
                    &self.a_rows,
                    &self.right.buffer.data,
                    &self.b_rows,
                    out,
                );
            } else {
                append_pairs(
                    key,
                    &self.left.buffer.data,
                    &self.b_rows,
                    &self.right.data,
                    &self.a_rows,
                    out,
                );
            }
            if row == end {
                if take_left {
                    self.left.step();
                } else {
                    self.right.step();
                }
                self.crossing = None;
            } else {
                self.crossing = Some((take_left, end, row, column));
            }
            return true;
        }
    }
}

fn append_pairs<
    C: TimeContainer,
    R0: Semigroup + Multiply<R1, Output = ROut>,
    R1: Semigroup,
    ROut,
>(
    key: u64,
    a: &Updates<C, R0>,
    ar: &[usize],
    b: &Updates<C, R1>,
    br: &[usize],
    out: &mut JoinMatches<C, ROut>,
) {
    out.times.map(
        Operation::Join,
        &[Binary {
            left: Operand::Rows(&a.times, Rows::Indices(ar)),
            right: Operand::Rows(&b.times, Rows::Indices(br)),
        }],
    );
    for (&i, &j) in ar.iter().zip(br) {
        out.ids.push((key, (a.ids[i], b.ids[j])));
        out.diffs.push(a.diffs[i].clone().multiply(&b.diffs[j]));
    }
}

/// A join tactic's deferred iterator; keeps output and pair indices bounded.
pub struct JoinIter<C: TimeContainer, B0, B1, Bk: ProxyJoinBackend<C, B0, B1>> {
    backend: Bk,
    instance: JoinInstance<C::Time, B0, B1>,
    from: Option<u64>,
    left: Updates<C, Bk::R0>,
    right: Updates<C, Bk::R1>,
    positions: (usize, usize),
    walk: Walk<C, Bk::R0, Bk::R1>,
    active: Option<u64>,
    limit: usize,
    matches: JoinMatches<C, Bk::ROut>,
    ready: Vec<Bk::Output>,
    marker: PhantomData<C>,
}
impl<C: TimeContainer, B0, B1, Bk: ProxyJoinBackend<C, B0, B1>> JoinIter<C, B0, B1, Bk> {
    /// Each iterator receives its own backend interpretation state.
    pub fn new(backend: Bk, instance: JoinInstance<C::Time, B0, B1>, limit: usize) -> Self {
        assert!(limit > 0);
        Self {
            backend,
            instance,
            from: Some(0),
            left: Updates::default(),
            right: Updates::default(),
            positions: (0, 0),
            walk: Walk::default(),
            active: None,
            limit,
            matches: JoinMatches::default(),
            ready: vec![],
            marker: PhantomData,
        }
    }
}
impl<C: TimeContainer, B0, B1, Bk: ProxyJoinBackend<C, B0, B1>> Iterator for JoinIter<C, B0, B1, Bk> {
    type Item = Bk::Output;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(out) = self.ready.pop() {
                return Some(out);
            }
            if self.matches.ids.len() == self.limit
                || (self.active.is_none()
                    && self.positions.0 == self.left.len()
                    && !self.matches.ids.is_empty())
            {
                self.backend
                    .cross(&self.instance, &mut self.matches, &mut self.ready);
                self.matches.clear();
                self.ready.reverse();
                continue;
            }
            if let Some(key) = self.active {
                if !self
                    .walk
                    .fill(key, &self.left, &self.right, self.limit, &mut self.matches)
                {
                    self.active = None;
                }
                continue;
            }
            let (a, b) = self.positions;
            if a < self.left.len() {
                let key = self.left.keys[a];
                assert_eq!(Some(&key), self.right.keys.get(b));
                let ae = a + self.left.keys[a..].partition_point(|&k| k == key);
                let be = b + self.right.keys[b..].partition_point(|&k| k == key);
                self.walk.load(&self.left, a..ae, &self.right, b..be);
                self.positions = (ae, be);
                self.active = Some(key);
            } else {
                assert_eq!(b, self.right.len());
                self.from?;
                self.left.clear();
                self.right.clear();
                let before = self.from;
                self.backend.advance(
                    &self.instance,
                    &mut self.from,
                    &mut self.left,
                    &mut self.right,
                );
                assert!(self.from.is_none() || self.from > before);
                self.positions = (0, 0);
            }
        }
    }
}

/// Join tactic with a separate backend clone for each outstanding iterator.
/// Cloning must preserve configuration and isolate mutable interpretation state.
pub struct ProxyJoinTactic<C, B0, B1, Bk> {
    backend: Bk,
    marker: PhantomData<(C, B0, B1)>,
}
impl<C, B0, B1, Bk> ProxyJoinTactic<C, B0, B1, Bk> {
    /// Construct a tactic from its value backend.
    pub fn new(backend: Bk) -> Self { Self { backend, marker: PhantomData } }
}
impl<C, B0, B1, Bk> crate::operators::join::JoinTactic<C::Time, B0, B1, Bk::Output>
    for ProxyJoinTactic<C, B0, B1, Bk>
where
    C: TimeContainer,
    B0: 'static,
    B1: 'static,
    Bk: ProxyJoinBackend<C, B0, B1> + Clone + 'static,
    Bk::Output: 'static,
{
    fn prep(&mut self, batches0: Vec<B0>, batches1: Vec<B1>, _: crate::operators::join::Fresh, lower: C::Time)
        -> Box<dyn Iterator<Item = Bk::Output>> {
        Box::new(JoinIter::new(self.backend.clone(), JoinInstance { batches0, batches1, lower }, 1 << 18))
    }
}
