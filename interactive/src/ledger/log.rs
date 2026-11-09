//! Immutable conclusion records linked from a cohort root, with legacy snapshots as checkpoints.

use std::collections::{BTreeMap, BTreeSet};

use bincode::Options;
use serde::{Deserialize, Serialize};
use timely::order::PartialOrder;

use super::resolve::{wide, Row};
use super::{canonical, BlobId, Error, Frontier, Id, Registration, Root, Shape, Span, State, Store, StoreError};
use crate::runtime::container::Updates;

const PREFIX: &[u8] = b"ddir-log ";
const MAGIC: &[u8] = b"ddir-log 1\n";

/// The resolver's accepted registry additions and ordered span additions.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Delta {
    pub registry: Updates,
    pub manifest: Updates,
}

#[derive(Serialize, Deserialize)]
struct Log {
    previous: Option<BlobId>,
    deltas: Vec<Delta>,
}

/// Publish all supplied batches with one blob write and one root CAS.
/// Payload blobs referenced by these conclusions must already be durable.
/// A lost CAS leaves an unreferenced proposal; an unknown outcome is never retried here.
pub(super) fn publish(store: &dyn Store, name: &str, root: Root, deltas: &[Delta]) -> Result<Option<Root>, Error> {
    if deltas.is_empty() {
        return Ok(Some(root));
    }
    let version =
        root.version.checked_add(1).ok_or_else(|| StoreError::Unavailable("root version exhausted".into()))?;
    let deltas = deltas
        .iter()
        .map(|delta| Ok(Delta { registry: compact(&delta.registry)?, manifest: compact(&delta.manifest)? }))
        .collect::<Result<_, Error>>()?;
    let record = Log { previous: root.state, deltas };
    let mut bytes = MAGIC.to_vec();
    bytes.extend(bincode::serialize(&record).map_err(|error| corrupt(&error.to_string()))?);
    let state = store.put(&bytes)?;
    if store.cas_root(name, root.version, state)? {
        Ok(Some(Root { version, state: Some(state) }))
    } else {
        Ok(None)
    }
}

/// A list reference can retain unrelated arena rows; persist only the rows these columns name.
fn compact(updates: &Updates) -> Result<Updates, Error> {
    let copy = |value: &corgi::Value| {
        corgi::OpLike::eval(&corgi::NumOp::Core(corgi::Op::Clone), value.clone()).map_err(|error| corrupt(&error))
    };
    Ok(Updates { keys: copy(&updates.keys)?, vals: copy(&updates.vals)?, diffs: updates.diffs.clone() })
}

/// Recover the committed head, then replay conclusions without rerunning their transactions.
pub(super) fn load(store: &dyn Store, name: &str) -> Result<(Root, State), Error> {
    let root = store.read_root(name)?;
    if (root.version == 0) != root.state.is_none() {
        return Err(corrupt("invalid root version"));
    }
    let mut cursor = root.state;
    let mut seen = BTreeSet::new();
    let mut records = Vec::new();
    let mut state = State::default();
    while let Some(hash) = cursor {
        if !seen.insert(hash) {
            return Err(corrupt("repeated predecessor"));
        }
        let bytes = store.get(hash)?;
        if bytes.starts_with(PREFIX) {
            if !bytes.starts_with(MAGIC) {
                return Err(corrupt("unsupported record version"));
            }
            let record: Log = decode(&bytes[MAGIC.len()..])?;
            cursor = record.previous;
            records.push(record);
            if records.len() as u64 > root.version {
                return Err(corrupt("more records than root versions"));
            }
        } else {
            state = decode(&bytes)?;
            break;
        }
    }
    let mut writes = current(&mut state)?;
    for record in records.into_iter().rev() {
        for delta in record.deltas {
            apply_with_writes(&mut state, &mut writes, &delta)?;
        }
    }
    Ok((root, state))
}

/// Apply one conclusion batch; invalid batches leave the supplied state unchanged.
#[cfg(test)]
pub(super) fn apply(state: &mut State, delta: &Delta) -> Result<(), Error> {
    let mut writes = current(state)?;
    apply_with_writes(state, &mut writes, delta)
}

fn apply_with_writes(state: &mut State, writes: &mut BTreeMap<Id, Frontier>, delta: &Delta) -> Result<(), Error> {
    let registry = rows(&delta.registry)?;
    let mut registrations = BTreeMap::new();
    for index in 0..registry.len() {
        let [id] = Row { value: &registry.keys, index }.product()?;
        let [shape, start] = Row { value: &registry.vals, index }.product()?;
        let id = id.id()?;
        let shape: Shape = decode(&shape.bytes()?)?;
        let start = canonical(&start.frontier()?, shape.time_width).map_err(corrupt)?;
        if state.registry.contains_key(&id) || registrations.contains_key(&id) {
            return Err(corrupt("duplicate registration"));
        }
        writes.insert(id, start.clone());
        registrations.insert(id, Registration { shape, start });
    }
    let manifest = rows(&delta.manifest)?;
    let mut ordered = BTreeMap::new();
    for index in 0..manifest.len() {
        let [transaction, group] = Row { value: &manifest.keys, index }.product()?;
        let group = group.integer()?;
        if group < 0 {
            return Err(corrupt("negative group index"));
        }
        if ordered.insert((transaction.integer()?, group), index).is_some() {
            return Err(corrupt("duplicate span order key"));
        }
    }
    let mut spans = Vec::with_capacity(ordered.len());
    for index in ordered.into_values() {
        let [id, lower, upper, since, batch] = Row { value: &manifest.vals, index }.product()?;
        let id = id.id()?;
        let entry = registrations
            .get(&id)
            .or_else(|| state.registry.get(&id))
            .ok_or_else(|| corrupt("span has no registration"))?;
        let span = Span {
            id,
            lower: canonical(&lower.frontier()?, entry.shape.time_width).map_err(corrupt)?,
            upper: canonical(&upper.frontier()?, entry.shape.time_width).map_err(corrupt)?,
            since: canonical(&since.frontier()?, entry.shape.time_width).map_err(corrupt)?,
            batch: batch.payload()?,
        };
        advance(writes, entry, &span)?;
        spans.push(span);
    }
    state.registry.extend(registrations);
    state.manifest.extend(spans);
    Ok(())
}

fn rows(updates: &Updates) -> Result<Updates, Error> {
    if updates.keys.len() != updates.len()
        || updates.vals.len() != updates.len()
        || updates.diffs.iter().any(|diff| *diff != 1)
    {
        return Err(corrupt("conclusions must be sets of rows"));
    }
    Ok(Updates { keys: wide(updates.keys.clone())?, vals: wide(updates.vals.clone())?, diffs: updates.diffs.clone() })
}

/// Validate a legacy checkpoint once and derive its current write frontiers.
fn current(state: &mut State) -> Result<BTreeMap<Id, Frontier>, Error> {
    let mut writes = BTreeMap::new();
    for (id, entry) in &mut state.registry {
        entry.start = canonical(&entry.start, entry.shape.time_width).map_err(corrupt)?;
        writes.insert(*id, entry.start.clone());
    }
    for span in &mut state.manifest {
        let entry = state.registry.get(&span.id).ok_or_else(|| corrupt("checkpoint span has no registration"))?;
        span.lower = canonical(&span.lower, entry.shape.time_width).map_err(corrupt)?;
        span.upper = canonical(&span.upper, entry.shape.time_width).map_err(corrupt)?;
        span.since = canonical(&span.since, entry.shape.time_width).map_err(corrupt)?;
        advance(&mut writes, entry, span)?;
    }
    Ok(writes)
}

fn advance(writes: &mut BTreeMap<Id, Frontier>, entry: &Registration, span: &Span) -> Result<(), Error> {
    if !PartialOrder::less_than(&span.lower, &span.upper) || span.since != entry.start {
        return Err(corrupt("invalid raw span bounds or since"));
    }
    if writes.get(&span.id) != Some(&span.lower) {
        return Err(corrupt("span does not extend the committed chain"));
    }
    writes.insert(span.id, span.upper.clone());
    Ok(())
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(bytes)
        .map_err(|error| corrupt(&error.to_string()))
}

fn corrupt(message: &str) -> Error {
    Error::Corrupt(format!("ledger conclusion log: {message}"))
}

#[cfg(test)]
mod tests {
    use super::super::{Change, Cohort, MemoryStore, Payload, Time};
    use super::*;
    use corgi::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn id() -> Id {
        Id::from_u128(1)
    }
    fn at(time: i64) -> Frontier {
        [Time::new(vec![time].into())].into_iter().collect()
    }
    fn shape() -> Shape {
        Shape { columns: corgi::Shape::Int, time_width: 1 }
    }

    fn bytes(values: &[Vec<u8>]) -> Value {
        let mut ends = Vec::new();
        let mut flat = Vec::new();
        for value in values {
            flat.extend_from_slice(value);
            ends.push(flat.len());
        }
        Value::List(ends.into(), Box::new(Value::u8(flat)))
    }

    fn fronts(values: &[Frontier]) -> Value {
        let (mut frontiers, mut points, mut coordinates) = (Vec::new(), Vec::new(), Vec::new());
        for value in values {
            for point in value.elements() {
                coordinates.extend_from_slice(point);
                points.push(coordinates.len());
            }
            frontiers.push(points.len());
        }
        Value::List(frontiers.into(), Box::new(Value::List(points.into(), Box::new(Value::int(coordinates)))))
    }

    // (transaction, group, lower, upper); this fixture's ledger starts at zero.
    fn delta(register: bool, spans: &[(i64, i64, i64, i64)]) -> Delta {
        let count = usize::from(register);
        Delta {
            registry: Updates {
                keys: Value::Prod(vec![bytes(&vec![id().as_bytes().to_vec(); count])]),
                vals: Value::Prod(vec![
                    bytes(&vec![bincode::serialize(&shape()).unwrap(); count]),
                    fronts(&vec![at(0); count]),
                ]),
                diffs: vec![1; count],
            },
            manifest: Updates {
                keys: Value::Prod(vec![
                    Value::int(spans.iter().map(|span| span.0)),
                    Value::int(spans.iter().map(|span| span.1)),
                ]),
                vals: Value::Prod(vec![
                    bytes(&vec![id().as_bytes().to_vec(); spans.len()]),
                    fronts(&spans.iter().map(|span| at(span.2)).collect::<Vec<_>>()),
                    fronts(&spans.iter().map(|span| at(span.3)).collect::<Vec<_>>()),
                    fronts(&vec![at(0); spans.len()]),
                    Value::sum(vec![0; spans.len()], vec![Value::Unit(spans.len()), bytes(&[]), bytes(&[])]),
                ]),
                diffs: vec![1; spans.len()],
            },
        }
    }

    #[derive(Default)]
    struct CountingStore {
        inner: MemoryStore,
        gets: AtomicUsize,
    }

    impl Store for CountingStore {
        fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
            self.inner.put(bytes)
        }
        fn get(&self, hash: BlobId) -> Result<Vec<u8>, StoreError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get(hash)
        }
        fn read_root(&self, name: &str) -> Result<Root, StoreError> {
            self.inner.read_root(name)
        }
        fn cas_root(&self, name: &str, expected: u64, state: BlobId) -> Result<bool, StoreError> {
            self.inner.cas_root(name, expected, state)
        }
    }

    #[test]
    fn linked_conclusions_replay_in_batch_and_span_order_with_legacy_checkpoints() {
        let store = CountingStore::default();
        let cohort = Cohort::new(&store, "root");
        assert_eq!(
            cohort.transact(&[(
                id(),
                Change::CreateAppend { shape: shape(), start: at(0), upper: at(1), batch: Payload::Empty }
            )]),
            Ok(Ok(()))
        );
        let root = store.read_root("root").unwrap();
        let root = publish(&store, "root", root, &[delta(false, &[(30, 0, 2, 3), (20, 0, 1, 2)])]).unwrap().unwrap();
        let root = publish(&store, "root", root, &[delta(false, &[(99, 0, 3, 4)]), delta(false, &[(-1, 0, 4, 5)])])
            .unwrap()
            .unwrap();
        store.gets.store(0, Ordering::SeqCst);
        let (loaded, state) = load(&store, "root").unwrap();
        assert_eq!(loaded, root);
        assert_eq!(store.gets.load(Ordering::SeqCst), 3);
        assert_eq!(
            state.manifest.iter().map(|span| span.upper.clone()).collect::<Vec<_>>(),
            vec![at(1), at(2), at(3), at(4), at(5)]
        );
        // A native snapshot replaces the linked head and checkpoints its whole committed history.
        assert_eq!(cohort.append(id(), at(5), at(6), Payload::Empty), Ok(Ok(())));
        store.gets.store(0, Ordering::SeqCst);
        assert_eq!(load(&store, "root").unwrap().1.write(id()), Some(&at(6)));
        assert_eq!(store.gets.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn invalid_conclusions_leave_state_unchanged() {
        let mut state = State::default();
        apply(&mut state, &delta(true, &[(1, 0, 0, 1)])).unwrap();
        let before = state.clone();
        let mut negative = delta(false, &[(2, 0, 1, 2)]);
        negative.manifest.diffs[0] = -1;
        let mut too_wide = delta(false, &[(2, 0, 1, 2)]);
        let Value::Prod(fields) = &mut too_wide.manifest.vals else { unreachable!() };
        fields[2] = fronts(&[[Time::new(vec![2, 3].into())].into_iter().collect()]);
        for bad in [
            delta(true, &[]),
            delta(false, &[(2, 0, 1, 2), (2, 0, 2, 3)]),
            delta(false, &[(2, 0, 0, 2)]),
            delta(false, &[(2, 0, 1, 1)]),
            negative,
            too_wide,
        ] {
            assert!(matches!(apply(&mut state, &bad), Err(Error::Corrupt(_))));
            assert_eq!(state, before);
        }
    }

    fn record(store: &dyn Store, log: Log) -> BlobId {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(bincode::serialize(&log).unwrap());
        store.put(&bytes).unwrap()
    }

    #[test]
    fn missing_predecessors_and_unknown_or_malformed_record_versions_are_errors() {
        let store = MemoryStore::default();
        let hash = record(&store, Log { previous: Some([0; 32]), deltas: vec![] });
        assert!(store.cas_root("missing", 0, hash).unwrap());
        assert_eq!(load(&store, "missing"), Err(Error::Store(StoreError::Missing([0; 32]))));
        for (name, bytes) in [("version", b"ddir-log 2\n".as_slice()), ("malformed", MAGIC)] {
            let hash = store.put(bytes).unwrap();
            assert!(store.cas_root(name, 0, hash).unwrap());
            assert!(matches!(load(&store, name), Err(Error::Corrupt(_))));
        }
    }

    #[test]
    fn a_lost_cas_does_not_make_a_proposal_part_of_history() {
        let store = MemoryStore::default();
        let initial = Root::default();
        let committed = publish(&store, "root", initial, &[delta(true, &[(1, 0, 0, 1)])]).unwrap().unwrap();
        assert_eq!(publish(&store, "root", initial, &[delta(false, &[(2, 0, 1, 99)])]), Ok(None));
        assert_eq!(load(&store, "root").unwrap().0, committed);
        assert_eq!(load(&store, "root").unwrap().1.write(id()), Some(&at(1)));
        assert_eq!(publish(&store, "root", committed, &[]), Ok(Some(committed)));
        let next = publish(&store, "root", committed, &[delta(false, &[])]).unwrap().unwrap();
        assert_eq!(next.version, committed.version + 1);
        assert_eq!(load(&store, "root").unwrap().1.write(id()), Some(&at(1)));
    }

    #[test]
    fn publication_materializes_only_the_selected_arena_rows() {
        use std::sync::Arc;

        let selected = 7;
        let mut ids = (100..132).map(|number| Id::from_u128(number).as_bytes().to_vec()).collect::<Vec<_>>();
        ids[selected] = id().as_bytes().to_vec();
        let ids = Arc::new(bytes(&ids));
        let mut shapes = vec![vec![0xa5; 128]; 32];
        shapes[selected] = bincode::serialize(&shape()).unwrap();
        let shapes = Arc::new(bytes(&shapes));
        let mut referenced = delta(true, &[(1, 0, 0, 1)]);
        let mut expected = referenced.clone();
        let Value::Prod(keys) = &mut referenced.registry.keys else { unreachable!() };
        keys[0] = Value::Ref(Arc::clone(&ids), Arc::new(vec![selected]));
        let Value::Prod(vals) = &mut referenced.registry.vals else { unreachable!() };
        vals[0] = Value::Ref(Arc::clone(&shapes), Arc::new(vec![selected]));
        let Value::Prod(vals) = &mut referenced.manifest.vals else { unreachable!() };
        vals[0] = Value::Ref(Arc::clone(&ids), Arc::new(vec![selected]));
        let Value::Prod(keys) = &mut expected.registry.keys else { unreachable!() };
        keys[0] = corgi::arrange::gather(&ids, &[selected]);
        let Value::Prod(vals) = &mut expected.registry.vals else { unreachable!() };
        vals[0] = corgi::arrange::gather(&shapes, &[selected]);
        let Value::Prod(vals) = &mut expected.manifest.vals else { unreachable!() };
        vals[0] = corgi::arrange::gather(&ids, &[selected]);
        let compact_record = Log { previous: None, deltas: vec![expected] };
        let expected = bincode::serialize(&compact_record).unwrap();
        let retained_size =
            bincode::serialize(&Log { previous: None, deltas: vec![referenced.clone()] }).unwrap().len();
        assert!(retained_size > expected.len());
        let store = MemoryStore::default();
        let root = publish(&store, "root", Root::default(), &[referenced]).unwrap().unwrap();
        let stored = store.get(root.state.unwrap()).unwrap();
        assert_eq!(&stored[MAGIC.len()..], expected);
        let recovered = load(&store, "root").unwrap().1;
        assert_eq!(recovered.registry.len(), 1);
        assert_eq!(recovered.manifest.len(), 1);
        assert_eq!(recovered.write(id()), Some(&at(1)));
    }
}
