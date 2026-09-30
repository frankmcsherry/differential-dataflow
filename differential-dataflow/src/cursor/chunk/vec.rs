//! Cursor for [`VecChunk`](crate::trace::chunk::vec::VecChunk).

use std::marker::PhantomData;

use timely::progress::Timestamp;

use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::cursor::{Cursor, Navigable};
use crate::trace::implementations::{BatchContainer, Layout, Vector};

use crate::trace::chunk::vec::{VecChunk, gallop};

/// A cursor over a [`VecChunk`], tracking the current key and `(key, val)`
/// group starts as indices into the flat vector.
pub struct VecChunkCursor<K, V, T, R> {
    key_pos: usize,
    val_pos: usize,
    phantom: PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R> Cursor for VecChunkCursor<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
    type Storage = VecChunk<K, V, T, R>;

    type KeyContainer = <Vector<((K, V), T, R)> as Layout>::KeyContainer;
    type Key<'a> = <<Vector<((K, V), T, R)> as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
    type ValContainer = <Vector<((K, V), T, R)> as Layout>::ValContainer;
    type Val<'a> = <<Vector<((K, V), T, R)> as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
    type ValOwn = <<Vector<((K, V), T, R)> as Layout>::ValContainer as BatchContainer>::Owned;
    type TimeContainer = <Vector<((K, V), T, R)> as Layout>::TimeContainer;
    type TimeGat<'a> = <<Vector<((K, V), T, R)> as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
    type Time = <<Vector<((K, V), T, R)> as Layout>::TimeContainer as BatchContainer>::Owned;
    type DiffContainer = <Vector<((K, V), T, R)> as Layout>::DiffContainer;
    type DiffGat<'a> = <<Vector<((K, V), T, R)> as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;
    type Diff = <<Vector<((K, V), T, R)> as Layout>::DiffContainer as BatchContainer>::Owned;

    fn key_valid(&self, s: &Self::Storage) -> bool { self.key_pos < s.0.len() }
    fn val_valid(&self, s: &Self::Storage) -> bool {
        self.key_pos < s.0.len() && self.val_pos < s.0.len() && s.0[self.val_pos].0.0 == s.0[self.key_pos].0.0
    }
    fn key<'a>(&self, s: &'a Self::Storage) -> &'a K { &s.0[self.key_pos].0.0 }
    fn val<'a>(&self, s: &'a Self::Storage) -> &'a V { &s.0[self.val_pos].0.1 }
    fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<&'a K> {
        if self.key_valid(s) { Some(self.key(s)) } else { None }
    }
    fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<&'a V> {
        if self.val_valid(s) { Some(self.val(s)) } else { None }
    }
    fn map_times<L: FnMut(&T, &R)>(&mut self, s: &Self::Storage, mut logic: L) {
        if !self.val_valid(s) { return; }
        let kv = &s.0[self.val_pos].0;
        let mut i = self.val_pos;
        while i < s.0.len() && &s.0[i].0 == kv {
            logic(&s.0[i].1, &s.0[i].2);
            i += 1;
        }
    }
    fn step_key(&mut self, s: &Self::Storage) {
        // Linear: stepping is a short hop to the next group; an inlined scan
        // beats a gallop call for the common small-group case.
        if self.key_pos >= s.0.len() { return; }
        let key = s.0[self.key_pos].0.0.clone();
        let mut i = self.key_pos;
        while i < s.0.len() && s.0[i].0.0 == key { i += 1; }
        self.key_pos = i;
        self.val_pos = i;
    }
    fn seek_key(&mut self, s: &Self::Storage, key: &K) {
        // Logarithmic: O(log distance), independent of chunk size.
        self.key_pos = gallop(&s.0, self.key_pos, |u| &u.0.0 < key);
        self.val_pos = self.key_pos;
    }
    fn step_val(&mut self, s: &Self::Storage) {
        if !self.val_valid(s) { return; }
        let kv = s.0[self.val_pos].0.clone();
        let mut i = self.val_pos;
        while i < s.0.len() && s.0[i].0 == kv { i += 1; }
        self.val_pos = i;
    }
    fn seek_val(&mut self, s: &Self::Storage, val: &V) {
        if !self.key_valid(s) { return; }
        let key = s.0[self.key_pos].0.0.clone();
        self.val_pos = gallop(&s.0, self.val_pos, |u| (&u.0.0, &u.0.1) < (&key, val));
    }
    fn rewind_keys(&mut self, _s: &Self::Storage) { self.key_pos = 0; self.val_pos = 0; }
    fn rewind_vals(&mut self, _s: &Self::Storage) { self.val_pos = self.key_pos; }
}

impl<K, V, T, R> Navigable for VecChunk<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
    type Cursor = VecChunkCursor<K, V, T, R>;

    fn cursor(&self) -> Self::Cursor {
        VecChunkCursor { key_pos: 0, val_pos: 0, phantom: PhantomData }
    }
}

impl<K, V, T, R> crate::cursor::chunk::NavigableChunk for VecChunk<K, V, T, R>
where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
    fn bounds(&self) -> ((&K, &V, &T), (&K, &V, &T)) {
        let s = &self.0[..];
        let (f, l) = (&s[0], &s[s.len() - 1]);
        ((&f.0.0, &f.0.1, &f.1), (&l.0.0, &l.0.1, &l.1))
    }
}

#[cfg(test)]
mod test {
    use std::rc::Rc;
    use crate::cursor::Navigable;
    use crate::trace::chunk::vec::VecChunk;

    fn chunk(updates: Vec<((u64, u64), u64, i64)>) -> VecChunk<u64, u64, u64, i64> {
        VecChunk(Rc::new(updates))
    }

    // The indexed cursor must reconstruct the same grouped updates as a flat
    // reference, even when a key — and a `(key, val)`'s times — straddle a
    // chunk boundary.
    #[test]
    fn cursor_handles_straddle() {
        use crate::cursor::Cursor;
        use crate::trace::chunk::ChunkBatch;

        let chunks = vec![
            chunk(vec![((0, 0), 0, 1), ((1, 0), 0, 1), ((1, 1), 0, 1)]),
            chunk(vec![((1, 1), 1, 1), ((1, 2), 0, 1)]),
            chunk(vec![((2, 0), 0, 1)]),
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

    // `seek_key` must land at the first key `>= target` regardless of where the cursor
    // starts, so the galloping hint (and its backward-seek fallback) never changes the
    // answer. Probe every (start, target) pair — forward and backward — against an
    // analytic oracle.
    #[test]
    fn seek_key_hint_is_direction_independent() {
        use crate::cursor::Cursor;
        use crate::trace::chunk::ChunkBatch;

        // One key per chunk (even keys 0, 2, .., 38) so seeks cross boundaries both ways.
        let chunks: Vec<_> = (0..20u64).map(|k| chunk(vec![((2 * k, 0u64), 0u64, 1i64)])).collect();
        let batch = ChunkBatch::new(chunks);

        // First key `>= tgt`: the next even at or above `tgt`, or absent past the last (38).
        let oracle = |tgt: u64| { let e = tgt + (tgt & 1); (e <= 38).then_some(e) };
        for start in 0..=40u64 {
            for tgt in 0..=40u64 {
                let mut c = batch.cursor();
                c.seek_key(&batch, &start);
                c.seek_key(&batch, &tgt);
                assert_eq!(c.get_key(&batch).copied(), oracle(tgt), "start={start} tgt={tgt}");
            }
        }
    }
}
