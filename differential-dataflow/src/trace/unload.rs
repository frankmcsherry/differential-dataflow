//! Bulk extraction of updates from storage into a [`Staging`] area.
//!
//! [`Unload`] is the read-side counterpart to a write-side chunking abstraction: it
//! exposes storage to operators only through key-level bulk operations — how many
//! keys there are, an enumeration of them, and an `extract(keys)` that fills a
//! [`Staging`] area — never through random `Cursor::seek_*` navigation. An
//! operator that reads through `Unload` therefore does not constrain its storage
//! to be resident, decompressed, and randomly addressable.
//!
//! The trait is blanket-implemented over today's [`Cursor`], so every current
//! trace and batch gets an unload path for free: the blanket impl simply walks
//! the existing cursor into staging. That is what decouples the two read-side
//! migrations — operators can move onto `Unload` (phase C) over *all* existing
//! storage immediately, while a storage type's *native* `extract` (phase D, which
//! overrides this impl and drops the per-element cursor) lands independently.
//!
//! Both access patterns route through this one surface: random access is always a
//! key-list `extract`, a full pass is always a sequential `enumerate_keys` —
//! neither exposes random seeking. There is intentionally no val-level skip:
//! no operator calls `seek_val`.

use crate::trace::cursor::Cursor;
use crate::trace::staging::Staging;

/// Bulk, key-level extraction of updates into a [`Staging`] area.
///
/// Blanket-implemented for every [`Cursor`] by walking the cursor; a storage type
/// may override with a native implementation that bypasses the cursor entirely.
pub trait Unload: Cursor {

    /// The number of keys reachable from this cursor.
    fn count_keys(&mut self, storage: &Self::Storage) -> usize;

    /// Appends every key, in order, to `out`.
    ///
    /// A full pass over storage is always expressed this way — a sequential
    /// enumeration — rather than as random seeking.
    fn enumerate_keys(&mut self, storage: &Self::Storage, out: &mut Self::KeyContainer);

    /// Extracts the updates for `keys` into `staging`, replacing its contents.
    ///
    /// The keys are named by a key container — a column, not a slice of borrows —
    /// since they always originate somewhere columnar (a batch's keys, an
    /// [`enumerate_keys`](Unload::enumerate_keys) result, a pending-key buffer).
    /// Any container with the right read type serves; it need not be this
    /// storage's own `KeyContainer` (an output trace can be probed with keys
    /// assembled in its input's container, for example).
    /// They must be sorted and distinct. Keys absent from storage are dropped,
    /// so the staged keys are the intersection of `keys` with storage; callers
    /// learn which matched from the staged keys (e.g. `staging.len()`). This is
    /// the single random-access primitive: a one-element `keys` reproduces a
    /// point lookup, a longer list amortizes the probe across a key batch.
    ///
    /// Seeks *forward* from the cursor's current position, like [`Cursor::seek_key`]:
    /// successive `extract` calls with ascending key lists march the cursor once
    /// through storage (the operator probing pattern), rather than restarting each
    /// time. Rewind the cursor first ([`Cursor::rewind_keys`]) for a full pass.
    fn extract<KC>(&mut self, storage: &Self::Storage, keys: &KC, staging: &mut Staging<Self::Layout>)
    where
        KC: for<'a> crate::trace::implementations::BatchContainer<ReadItem<'a> = Self::Key<'a>>;
}

impl<C: Cursor> Unload for C {

    fn count_keys(&mut self, storage: &Self::Storage) -> usize {
        let mut count = 0;
        self.rewind_keys(storage);
        while self.key_valid(storage) {
            count += 1;
            self.step_key(storage);
        }
        count
    }

    fn enumerate_keys(&mut self, storage: &Self::Storage, out: &mut Self::KeyContainer) {
        use crate::trace::implementations::BatchContainer;
        self.rewind_keys(storage);
        while let Some(key) = self.get_key(storage) {
            out.push_ref(key);
            self.step_key(storage);
        }
    }

    fn extract<KC>(&mut self, storage: &Self::Storage, keys: &KC, staging: &mut Staging<Self::Layout>)
    where
        KC: for<'a> crate::trace::implementations::BatchContainer<ReadItem<'a> = Self::Key<'a>>,
    {
        use crate::trace::implementations::BatchContainer;
        staging.clear();
        for index in 0 .. keys.len() {
            // Reborrow to a local lifetime: `Self::Key` is an opaque GAT, so the
            // compiler will not shorten it to match `storage` on its own.
            let key = <Self::KeyContainer as BatchContainer>::reborrow(keys.index(index));
            self.seek_key(storage, key);
            if self.get_key(storage) == Some(key) {
                self.rewind_vals(storage);
                while let Some(val) = self.get_val(storage) {
                    self.map_times(storage, |time, diff| staging.push_update(time, diff));
                    staging.seal_val(val);
                    self.step_val(storage);
                }
                staging.seal_key(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use timely::container::PushInto;
    use timely::dataflow::operators::generic::OperatorInfo;
    use timely::progress::Antichain;

    use crate::trace::implementations::{BatchContainer, Vector, ValBatcher, ValBuilder, ValSpine};
    use crate::trace::{Batcher, Builder, Trace, TraceReader};
    use crate::trace::cursor::Cursor;
    use crate::trace::staging::Staging;
    use super::Unload;

    /// The staging type for this trace's layout.
    type Stage = Staging<Vector<((u64, u64), usize, i64)>>;

    type IntegerTrace = ValSpine<u64, u64, usize, i64>;
    type IntegerBuilder = ValBuilder<u64, u64, usize, i64>;

    fn get_trace() -> IntegerTrace {
        let op_info = OperatorInfo::new(0, 0, [].into());
        let mut trace = IntegerTrace::new(op_info, None, None);
        let mut batcher = ValBatcher::<u64, u64, usize, i64>::new(None, 0);
        batcher.push_into(vec![
            ((1, 2), 0, 1),
            ((1, 7), 0, 1),
            ((2, 3), 1, 1),
            ((2, 3), 2, -1),
            ((4, 5), 0, 1),
        ]);
        let (mut chain, description) = batcher.seal(Antichain::from_elem(3));
        trace.insert(IntegerBuilder::seal(&mut chain, description));
        trace
    }

    /// `enumerate_keys` then `extract(all keys)` and read the staging cursor must
    /// reproduce a direct cursor walk, key-for-key, update-for-update.
    #[test]
    fn unload_matches_cursor_walk() {
        let mut trace = get_trace();

        let (mut cursor, storage) = trace.cursor();
        let baseline = cursor.to_vec(&storage, |k| *k, |v| *v);

        // Enumerate keys, then extract them all into staging: the `enumerate_keys`
        // output is a key container, exactly what `extract` consumes.
        let mut keys = Vec::<u64>::with_capacity(0);
        cursor.enumerate_keys(&storage, &mut keys);

        assert_eq!(cursor.count_keys(&storage), keys.len());

        // `extract` seeks forward from the current position; rewind for a full pass.
        let mut staging = Stage::default();
        cursor.rewind_keys(&storage);
        cursor.extract(&storage, &keys, &mut staging);

        let mut staging_cursor = staging.cursor();
        let unloaded = staging_cursor.to_vec(&staging, |k| *k, |v| *v);
        assert_eq!(unloaded, baseline);
    }

    /// A key subset extracts exactly those keys; an absent key is dropped.
    #[test]
    fn unload_subset_and_absent() {
        let mut trace = get_trace();
        let (mut cursor, storage) = trace.cursor();

        // Keys 2 and 4 are present; 3 is absent and must be dropped.
        let subset: Vec<u64> = vec![2, 3, 4];
        let mut staging = Stage::default();
        cursor.extract(&storage, &subset, &mut staging);

        let mut staging_cursor = staging.cursor();
        let unloaded = staging_cursor.to_vec(&staging, |k| *k, |v| *v);
        assert_eq!(unloaded, vec![
            ((2, 3), vec![(1, 1), (2, -1)]),
            ((4, 5), vec![(0, 1)]),
        ]);
    }

    /// `Cursor::populate_staging` fills the staging containers for one key,
    /// advancing times by `meet` and consolidating per value. With no `meet` it
    /// reproduces the (already consolidated) trace contents; with a `meet` it
    /// joins times forward, which can merge or cancel updates.
    #[test]
    fn populate_staging_advances_and_consolidates() {
        let op_info = OperatorInfo::new(0, 0, [].into());
        let mut trace = IntegerTrace::new(op_info, None, None);
        let mut batcher = ValBatcher::<u64, u64, usize, i64>::new(None, 0);
        batcher.push_into(vec![
            ((8, 1), 0, 1),   // key 8, val 1: two same-sign updates at times 0, 3
            ((8, 1), 3, 1),
            ((9, 2), 1, 1),   // key 9, val 2: opposing updates at times 1, 2
            ((9, 2), 2, -1),
        ]);
        let (mut chain, description) = batcher.seal(Antichain::from_elem(4));
        trace.insert(IntegerBuilder::seal(&mut chain, description));

        let (mut cursor, storage) = trace.cursor();
        let mut staging = Stage::default();
        let (eight, nine, seven) = (8u64, 9u64, 7u64);

        // No meet: reproduce the trace's consolidated contents for key 8.
        cursor.populate_staging(&storage, &eight, None, &mut staging);
        assert_eq!(staging.cursor().to_vec(&staging, |k| *k, |v| *v), vec![
            ((8, 1), vec![(0, 1), (3, 1)]),
        ]);

        // meet = 5: both of key 8's times advance to 5 and merge to a single +2.
        cursor.populate_staging(&storage, &eight, Some(&5), &mut staging);
        assert_eq!(staging.cursor().to_vec(&staging, |k| *k, |v| *v), vec![
            ((8, 1), vec![(5, 2)]),
        ]);

        // meet = 5: key 9's opposing updates both advance to 5 and cancel; the
        // value is dropped, leaving no updates to read.
        cursor.populate_staging(&storage, &nine, Some(&5), &mut staging);
        assert!(staging.cursor().to_vec(&staging, |k| *k, |v| *v).is_empty());

        // Absent key: nothing staged.
        cursor.rewind_keys(&storage);
        cursor.populate_staging(&storage, &seven, None, &mut staging);
        assert!(staging.is_empty());
    }

    /// The range-copy `extract_bulk` must agree with the per-element blanket
    /// `extract`, including on a key subset with an absent key.
    #[test]
    fn extract_bulk_matches_extract() {
        use crate::trace::staging::extract_bulk;

        let mut trace = get_trace();
        let (mut cursor, storage) = trace.cursor();

        // Build a staging source holding the whole trace.
        let mut keys = Vec::<u64>::with_capacity(0);
        cursor.enumerate_keys(&storage, &mut keys);
        let mut source = Stage::default();
        cursor.rewind_keys(&storage);
        cursor.extract(&storage, &keys, &mut source);

        // Full set, and a subset with an absent key (3 is not present).
        for query in [keys.clone(), vec![2u64, 3, 4]] {
            let mut a = Stage::default();
            let mut b = Stage::default();
            source.cursor().extract(&source, &query, &mut a);
            extract_bulk(&source, &query, &mut b);
            assert_eq!(
                a.cursor().to_vec(&a, |k| *k, |v| *v),
                b.cursor().to_vec(&b, |k| *k, |v| *v),
            );
        }
    }
}
