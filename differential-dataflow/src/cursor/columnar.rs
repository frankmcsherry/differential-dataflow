//! Cursors for [`columnar`](crate::columnar): over the trie in a [`ColChunk`], and reading a columnar arrangement back into a collection.

use std::marker::PhantomData;

use columnar::{Borrow, Index, Len};

use crate::cursor::{Cursor, Navigable};
use crate::cursor::chunk::NavigableChunk;
use crate::trace::implementations::{BatchContainer, Layout};
use crate::columnar::layout::{self, ColumnarLayout, ColumnarUpdate, Coltainer};
use crate::columnar::updates::child_range;
use crate::columnar::trie_merger;
use crate::columnar::trace::{Spine, chunk::ColChunk};
use crate::columnar::collection::{Builder, RecordedUpdates};

/// A cursor over a [`ColChunk`], tracking the current key and value as absolute
/// indices into the trie's flat `keys.values` / `vals.values` columns.
pub struct ColChunkCursor<U: ColumnarUpdate> {
    key_cursor: usize,
    val_cursor: usize,
    phantom: PhantomData<U>,
}

impl<U: ColumnarUpdate> Cursor for ColChunkCursor<U> {
    type Storage = ColChunk<U>;

    type KeyContainer = <ColumnarLayout<U> as Layout>::KeyContainer;
    type Key<'a> = <<ColumnarLayout<U> as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
    type ValContainer = <ColumnarLayout<U> as Layout>::ValContainer;
    type Val<'a> = <<ColumnarLayout<U> as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
    type ValOwn = <<ColumnarLayout<U> as Layout>::ValContainer as BatchContainer>::Owned;
    type TimeContainer = <ColumnarLayout<U> as Layout>::TimeContainer;
    type TimeGat<'a> = <<ColumnarLayout<U> as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
    type Time = <<ColumnarLayout<U> as Layout>::TimeContainer as BatchContainer>::Owned;
    type DiffContainer = <ColumnarLayout<U> as Layout>::DiffContainer;
    type DiffGat<'a> = <<ColumnarLayout<U> as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;
    type Diff = <<ColumnarLayout<U> as Layout>::DiffContainer as BatchContainer>::Owned;

    fn key_valid(&self, s: &Self::Storage) -> bool { self.key_cursor < s.trie().view().keys.values.len() }
    fn val_valid(&self, s: &Self::Storage) -> bool {
        let view = s.trie().view();
        self.key_cursor < view.keys.values.len()
            && self.val_cursor < child_range(view.vals.bounds, self.key_cursor).end
    }
    fn key<'a>(&self, s: &'a Self::Storage) -> Self::Key<'a> { s.trie().view().keys.values.get(self.key_cursor) }
    fn val<'a>(&self, s: &'a Self::Storage) -> Self::Val<'a> { s.trie().view().vals.values.get(self.val_cursor) }
    fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<Self::Key<'a>> {
        if self.key_valid(s) { Some(self.key(s)) } else { None }
    }
    fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<Self::Val<'a>> {
        if self.val_valid(s) { Some(self.val(s)) } else { None }
    }
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, s: &Self::Storage, mut logic: L) {
        if !self.val_valid(s) { return; }
        let view = s.trie().view();
        for t in child_range(view.times.bounds, self.val_cursor) {
            let time = view.times.values.get(t);
            for d in child_range(view.diffs.bounds, t) {
                logic(time, view.diffs.values.get(d));
            }
        }
    }
    fn step_key(&mut self, s: &Self::Storage) {
        self.key_cursor += 1;
        if self.key_valid(s) { self.rewind_vals(s); }
        else { self.key_cursor = s.trie().view().keys.values.len(); }
    }
    fn seek_key(&mut self, s: &Self::Storage, key: Self::Key<'_>) {
        let view = s.trie().view();
        let n = view.keys.values.len();
        let mut lo = self.key_cursor;
        trie_merger::gallop(view.keys.values, &mut lo, n, |x|
            <Coltainer<U::Key> as BatchContainer>::reborrow(x).lt(&<Coltainer<U::Key> as BatchContainer>::reborrow(key)));
        self.key_cursor = lo;
        if self.key_valid(s) { self.rewind_vals(s); }
    }
    fn step_val(&mut self, s: &Self::Storage) {
        self.val_cursor += 1;
        if !self.val_valid(s) {
            self.val_cursor = child_range(s.trie().view().vals.bounds, self.key_cursor).end;
        }
    }
    fn seek_val(&mut self, s: &Self::Storage, val: Self::Val<'_>) {
        if !self.key_valid(s) { return; }
        let view = s.trie().view();
        let upper = child_range(view.vals.bounds, self.key_cursor).end;
        let mut lo = self.val_cursor;
        trie_merger::gallop(view.vals.values, &mut lo, upper, |x|
            <Coltainer<U::Val> as BatchContainer>::reborrow(x).lt(&<Coltainer<U::Val> as BatchContainer>::reborrow(val)));
        self.val_cursor = lo;
    }
    fn rewind_keys(&mut self, s: &Self::Storage) { self.key_cursor = 0; self.rewind_vals(s); }
    fn rewind_vals(&mut self, s: &Self::Storage) {
        if self.key_valid(s) {
            self.val_cursor = child_range(s.trie().view().vals.bounds, self.key_cursor).start;
        }
    }
}

impl<U: ColumnarUpdate> Navigable for ColChunk<U>
where U::Time: 'static {
    type Cursor = ColChunkCursor<U>;

    fn cursor(&self) -> Self::Cursor {
        ColChunkCursor { key_cursor: 0, val_cursor: 0, phantom: PhantomData }
    }
}

impl<U: ColumnarUpdate> NavigableChunk for ColChunk<U>
where U::Time: 'static {
    fn bounds(&self) -> (
        (<Self::Cursor as Cursor>::Key<'_>, <Self::Cursor as Cursor>::Val<'_>, <Self::Cursor as Cursor>::TimeGat<'_>),
        (<Self::Cursor as Cursor>::Key<'_>, <Self::Cursor as Cursor>::Val<'_>, <Self::Cursor as Cursor>::TimeGat<'_>),
    ) {
        match self {
            ColChunk::Resident(rc) => {
                let view = rc.view();
                let (nk, nv, nt) = (view.keys.values.len(), view.vals.values.len(), view.times.values.len());
                // Sorted trie: the first/last of each flat column are the bounding triples.
                ((view.keys.values.get(0), view.vals.values.get(0), view.times.values.get(0)),
                 (view.keys.values.get(nk - 1), view.vals.values.get(nv - 1), view.times.values.get(nt - 1)))
            }
            ColChunk::Paged(p) => {
                // Read the resident metadata — no fetch.
                let m = &p.meta;
                ((m.fk.borrow().get(0), m.fv.borrow().get(0), m.ft.borrow().get(0)),
                 (m.lk.borrow().get(0), m.lv.borrow().get(0), m.lt.borrow().get(0)))
            }
        }
    }
}

/// Extract a `Collection<_, RecordedUpdates<U>>` from a columnar `Arranged`.
///
/// Cursors through each batch and pushes `(key, val, time, diff)` refs into
/// a `Builder`, which sorts and consolidates on flush.
pub fn as_recorded_updates<U>(
    arranged: crate::operators::arrange::Arranged<
        crate::operators::arrange::TraceAgent<Spine<U::Key, U::Val, U::Time, U::Diff>>,
    >,
) -> crate::Collection<U::Time, RecordedUpdates<U>>
where
    U: layout::ColumnarUpdate,
{
    use timely::dataflow::operators::generic::Operator;
    use timely::dataflow::channels::pact::Pipeline;
    use crate::AsCollection;

    arranged.stream
        .unary::<Builder<U>, _, _, _>(Pipeline, "AsRecordedUpdates", |_, _| {
            move |input, output| {
                input.for_each(|time, batches| {
                    let mut session = output.session_with_builder(&time);
                    for batch in batches.drain(..) {
                        let Some(batch) = batch.inner else { continue };
                        let mut cursor = batch.cursor();
                        while cursor.key_valid(&batch) {
                            while cursor.val_valid(&batch) {
                                let key = cursor.key(&batch);
                                let val = cursor.val(&batch);
                                cursor.map_times(&batch, |time, diff| {
                                    session.give((key, val, time, diff));
                                });
                                cursor.step_val(&batch);
                            }
                            cursor.step_key(&batch);
                        }
                    }
                });
            }
        })
        .as_collection()
}

#[cfg(test)]
mod test {
    use columnar::Push;
    use crate::cursor::Navigable;
    use crate::columnar::updates::UpdatesTyped;
    use crate::columnar::trace::chunk::ColChunk;

    type Upd = (u64, u64, u64, i64);

    // A sorted, consolidated columnar chunk from raw updates.
    fn chunk(updates: Vec<Upd>) -> ColChunk<Upd> {
        let mut u = UpdatesTyped::<Upd>::default();
        for (k, v, t, d) in updates { u.push((&k, &v, &t, &d)); }
        ColChunk::from_trie(u.consolidate())
    }

    // The straddle-aware `ChunkBatch` cursor reconstructs the same grouped
    // updates as a flat reference, even when a key — and a `(key, val)`'s times —
    // span a chunk boundary.
    #[test]
    fn cursor_handles_straddle() {
        use crate::cursor::Cursor;
        use crate::trace::chunk::ChunkBatch;

        let chunks = vec![
            chunk(vec![(0, 0, 0, 1), (1, 0, 0, 1), (1, 1, 0, 1)]),
            chunk(vec![(1, 1, 1, 1), (1, 2, 0, 1)]),
            chunk(vec![(2, 0, 0, 1)]),
        ];
        let batch = ChunkBatch::new(chunks);

        let mut cursor = batch.cursor();
        let got = cursor.to_vec(&batch, |k| *k, |v| *v);
        let want = vec![
            ((0u64, 0u64), vec![(0u64, 1i64)]),
            ((1, 0), vec![(0, 1)]),
            ((1, 1), vec![(0, 1), (1, 1)]),
            ((1, 2), vec![(0, 1)]),
            ((2, 0), vec![(0, 1)]),
        ];
        assert_eq!(got, want);
    }
}
