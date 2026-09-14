//! A bounded bilinear join over an opaque time container.
use super::{
    time_container::{Binary, Operand, Operation, Rows, TimeContainer},
};
use super::{history::{compare_heads, Replay}, updates::Updates};
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
    meet: C,
}
impl<C: TimeContainer, R0: Semigroup, R1: Semigroup> Default for Walk<C, R0, R1> {
    fn default() -> Self {
        Self {
            left: Replay::default(),
            right: Replay::default(),
            direct: None,
            crossing: None,
            meet: C::default(),
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
            while row < end && out.ids.len() < limit {
                let count = (limit - out.ids.len()).min(end1 - column);
                append_pairs(
                    key,
                    left,
                    Rows::Repeat { row, count },
                    right,
                    Rows::Range(column..column + count),
                    out,
                );
                column += count;
                if column == end1 {
                    column = start1;
                    row += 1;
                }
            }
            self.direct = Some((row, end, column, start1, end1));
            return row < end;
        }
        loop {
            if self.crossing.is_none() {
                let (a, b) = (self.left.head(), self.right.head());
                if a.is_none() && b.is_none() {
                    return false;
                }
                let take_left =
                    a.is_some() && (b.is_none() || compare_heads(a.unwrap(), b.unwrap()).is_lt());
                if take_left {
                    self.meet.clear();
                    self.left.meet_into(&mut self.meet);
                    self.right.buffer.prepare(Some((&self.meet, 0)));
                    if self.right.buffer.data.is_empty() {
                        self.left.step();
                        continue;
                    }
                    self.crossing = Some((true, self.left.end(), self.left.pos, 0));
                } else {
                    self.meet.clear();
                    self.right.meet_into(&mut self.meet);
                    self.left.buffer.prepare(Some((&self.meet, 0)));
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
            while row < end && out.ids.len() < limit {
                let count = (limit - out.ids.len()).min(n - column);
                if take_left {
                    append_pairs(
                        key,
                        &self.left.data,
                        Rows::Repeat { row, count },
                        &self.right.buffer.data,
                        Rows::Range(column..column + count),
                        out,
                    );
                } else {
                    append_pairs(
                        key,
                        &self.left.buffer.data,
                        Rows::Range(column..column + count),
                        &self.right.data,
                        Rows::Repeat { row, count },
                        out,
                    );
                }
                column += count;
                if column == n {
                    column = 0;
                    row += 1;
                }
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
    ar: Rows<'_>,
    b: &Updates<C, R1>,
    br: Rows<'_>,
    out: &mut JoinMatches<C, ROut>,
) {
    out.times.map(
        Operation::Join,
        &[Binary {
            left: Operand(&a.times, ar.clone()),
            right: Operand(&b.times, br.clone()),
        }],
    );
    for r in 0..ar.len() {
        let (i, j) = (ar.at(r), br.at(r));
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
