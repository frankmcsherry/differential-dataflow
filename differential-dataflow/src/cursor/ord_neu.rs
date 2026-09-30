//! Cursors for the batch types of [`ord_neu`](crate::trace::implementations::ord_neu).

pub use self::val_batch::OrdValCursor;
pub use self::key_batch::OrdKeyCursor;

/// Cursor for [`OrdValBatch`](crate::trace::implementations::ord_neu::OrdValBatch).
pub mod val_batch {

    use std::marker::PhantomData;

    use crate::cursor::{Cursor, Navigable};
    use crate::trace::implementations::{BatchContainer, Layout};
    use crate::trace::implementations::ord_neu::OrdValBatch;

    impl<L: Layout> Navigable for OrdValBatch<L> {
        type Cursor = OrdValCursor<L>;
        fn cursor(&self) -> Self::Cursor {
            OrdValCursor {
                key_cursor: 0,
                val_cursor: 0,
                phantom: PhantomData,
            }
        }
    }

    /// A cursor for navigating a single layer.
    pub struct OrdValCursor<L: Layout> {
        /// Absolute position of the current key.
        key_cursor: usize,
        /// Absolute position of the current value.
        val_cursor: usize,
        /// Phantom marker for Rust happiness.
        phantom: PhantomData<L>,
    }

    impl<L: Layout> Cursor for OrdValCursor<L> {

        type Storage = OrdValBatch<L>;

        type KeyContainer = <L as Layout>::KeyContainer;
        type Key<'a> = <<L as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
        type ValContainer = <L as Layout>::ValContainer;
        type Val<'a> = <<L as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
        type ValOwn = <<L as Layout>::ValContainer as BatchContainer>::Owned;
        type TimeContainer = <L as Layout>::TimeContainer;
        type TimeGat<'a> = <<L as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Time = <<L as Layout>::TimeContainer as BatchContainer>::Owned;
        type DiffContainer = <L as Layout>::DiffContainer;
        type DiffGat<'a> = <<L as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <<L as Layout>::DiffContainer as BatchContainer>::Owned;

        fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { storage.storage.keys.get(self.key_cursor) }
        fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { if self.val_valid(storage) { Some(self.val(storage)) } else { None } }

        fn key<'a>(&self, storage: &'a OrdValBatch<L>) -> Self::Key<'a> { storage.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, storage: &'a OrdValBatch<L>) -> Self::Val<'a> { storage.storage.vals.get_abs(self.val_cursor) }
        fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &OrdValBatch<L>, mut logic: L2) {
            let (lower, upper) = storage.storage.upds.bounds(self.val_cursor);
            for index in lower .. upper {
                let (time, diff) = storage.storage.upds.get_abs(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &OrdValBatch<L>) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, storage: &OrdValBatch<L>) -> bool { self.val_cursor < storage.storage.vals.bounds(self.key_cursor).1 }
        fn step_key(&mut self, storage: &OrdValBatch<L>){
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &OrdValBatch<L>, key: Self::Key<'_>) {
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| <L::KeyContainer as BatchContainer>::reborrow(x).lt(&<L::KeyContainer as BatchContainer>::reborrow(key)));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, storage: &OrdValBatch<L>) {
            self.val_cursor += 1;
            if !self.val_valid(storage) {
                self.val_cursor = storage.storage.vals.bounds(self.key_cursor).1;
            }
        }
        fn seek_val(&mut self, storage: &OrdValBatch<L>, val: Self::Val<'_>) {
            self.val_cursor += storage.storage.vals.vals.advance(self.val_cursor, storage.storage.vals.bounds(self.key_cursor).1, |x| <L::ValContainer as BatchContainer>::reborrow(x).lt(&<L::ValContainer as BatchContainer>::reborrow(val)));
        }
        fn rewind_keys(&mut self, storage: &OrdValBatch<L>) {
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, storage: &OrdValBatch<L>) {
            self.val_cursor = storage.storage.vals.bounds(self.key_cursor).0;
        }
    }
}

/// Cursor for [`OrdKeyBatch`](crate::trace::implementations::ord_neu::OrdKeyBatch).
pub mod key_batch {

    use std::marker::PhantomData;

    use crate::cursor::{Cursor, Navigable};
    use crate::trace::implementations::{BatchContainer, Layout};
    use crate::trace::implementations::ord_neu::OrdKeyBatch;

    impl<L: Layout<ValContainer: BatchContainer<Owned: Default>>> Navigable for OrdKeyBatch<L> {
        type Cursor = OrdKeyCursor<L>;
        fn cursor(&self) -> Self::Cursor {
            OrdKeyCursor {
                key_cursor: 0,
                val_stepped: false,
                phantom: std::marker::PhantomData,
            }
        }
    }

    /// A cursor for navigating a single layer.
    pub struct OrdKeyCursor<L: Layout> {
        /// Absolute position of the current key.
        key_cursor: usize,
        /// If the value has been stepped for the key, there are no more values.
        val_stepped: bool,
        /// Phantom marker for Rust happiness.
        phantom: PhantomData<L>,
    }

    impl<L: for<'a> Layout<ValContainer: BatchContainer<Owned: Default>>> Cursor for OrdKeyCursor<L> {

        type Storage = OrdKeyBatch<L>;

        type KeyContainer = <L as Layout>::KeyContainer;
        type Key<'a> = <<L as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
        type ValContainer = <L as Layout>::ValContainer;
        type Val<'a> = <<L as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
        type ValOwn = <<L as Layout>::ValContainer as BatchContainer>::Owned;
        type TimeContainer = <L as Layout>::TimeContainer;
        type TimeGat<'a> = <<L as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Time = <<L as Layout>::TimeContainer as BatchContainer>::Owned;
        type DiffContainer = <L as Layout>::DiffContainer;
        type DiffGat<'a> = <<L as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <<L as Layout>::DiffContainer as BatchContainer>::Owned;

        fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>> { storage.storage.keys.get(self.key_cursor) }
        fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>> { if self.val_valid(storage) { Some(self.val(storage)) } else { None } }

        fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { storage.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a> { storage.value.index(0) }
        fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, mut logic: L2) {
            let (lower, upper) = storage.storage.upds.bounds(self.key_cursor);
            for index in lower .. upper {
                let (time, diff) = storage.storage.upds.get_abs(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &Self::Storage) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, _storage: &Self::Storage) -> bool { !self.val_stepped }
        fn step_key(&mut self, storage: &Self::Storage){
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) {
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| <L::KeyContainer as BatchContainer>::reborrow(x).lt(&<L::KeyContainer as BatchContainer>::reborrow(key)));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, _storage: &Self::Storage) {
            self.val_stepped = true;
        }
        fn seek_val(&mut self, _storage: &Self::Storage, _val: Self::Val<'_>) { }
        fn rewind_keys(&mut self, storage: &Self::Storage) {
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, _storage: &Self::Storage) {
            self.val_stepped = false;
        }
    }
}
