//! Publish conclusions from the DDP resolver through the cohort root.

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::time::Duration;

use interactive::ledger::{
    BlobId, Change, Cohort, Error, Failure, Frontier, Id, MemoryStore, Payload, Rejection, Resolver, Root, Shape,
    State, Store, StoreError, Time,
};

fn id(number: u128) -> Id {
    Id::from_u128(number)
}
fn at(time: i64) -> Frontier {
    [Time::new(vec![time].into())].into_iter().collect()
}
fn shape() -> Shape {
    Shape { columns: corgi::Shape::Int, time_width: 1 }
}

fn create(number: u128) -> (Id, Change) {
    (id(number), Change::Create { shape: shape(), start: at(0) })
}

fn append(number: u128, lower: i64, upper: i64, batch: Payload) -> (Id, Change) {
    (id(number), Change::Append { lower: at(lower), upper: at(upper), batch })
}

fn populated(number: u128, upper: i64, batch: Payload) -> (Id, Change) {
    (id(number), Change::CreateAppend { shape: shape(), start: at(0), upper: at(upper), batch })
}

/// Intercept the real publication boundary, while keeping ordinary blob and root operations.
struct CasHook<F> {
    inner: Arc<MemoryStore>,
    reads: AtomicUsize,
    calls: AtomicUsize,
    cas: F,
}

impl<F: Fn(&MemoryStore, &str, u64, BlobId) -> Result<bool, StoreError> + Send + Sync> CasHook<F> {
    fn new(inner: Arc<MemoryStore>, cas: F) -> Self {
        Self { inner, reads: AtomicUsize::new(0), calls: AtomicUsize::new(0), cas }
    }
}

impl<F: Fn(&MemoryStore, &str, u64, BlobId) -> Result<bool, StoreError> + Send + Sync> Store for CasHook<F> {
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        self.inner.put(bytes)
    }
    fn get(&self, id: BlobId) -> Result<Vec<u8>, StoreError> {
        self.inner.get(id)
    }
    fn read_root(&self, name: &str) -> Result<Root, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_root(name)
    }
    fn cas_root(&self, name: &str, expected: u64, state: BlobId) -> Result<bool, StoreError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        (self.cas)(&self.inner, name, expected, state)
    }
}

#[test]
fn one_publication_commits_the_accepted_transactions() {
    let store = Arc::new(MemoryStore::default());
    let hooked = CasHook::new(Arc::clone(&store), |inner, name, expected, state| inner.cas_root(name, expected, state));
    let cohort = Cohort::new(&hooked, "cohort");
    let resolver = Resolver::new(2).unwrap();
    let outcomes = cohort
        .transact_batch(
            &resolver,
            &[
                (30, vec![append(1, 1, 2, Payload::Inline(vec![2]))]),
                (10, vec![populated(1, 1, Payload::Inline(vec![1]))]),
                (40, vec![populated(2, 3, Payload::Empty)]),
                (20, vec![create(0), append(1, 0, 9, Payload::Empty)]),
            ],
        )
        .unwrap();
    assert_eq!(
        outcomes,
        BTreeMap::from([
            (10, Ok(())),
            (20, Err(Rejection { group: 1, reason: Failure::Write(at(1)) })),
            (30, Ok(())),
            (40, Ok(())),
        ])
    );
    assert_eq!(hooked.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.read_root("cohort").unwrap().version, 1);
    let state = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
    assert_eq!(state.registry.len(), 2);
    assert!(!state.registry.contains_key(&id(0)));
    assert_eq!(state.manifest.len(), 3);
    assert_eq!(state.write(id(1)), Some(&at(2)));
    assert_eq!(state.write(id(2)), Some(&at(3)));
}

#[test]
fn cas_loss_reresolves_against_the_competing_commit() {
    let store = Arc::new(MemoryStore::default());
    assert_eq!(Cohort::new(store.as_ref(), "cohort").transact(&[create(1)]), Ok(Ok(())));
    let attempts = Mutex::new(Vec::new());
    let hooked = CasHook::new(Arc::clone(&store), |inner, name, expected, state| {
        let proposal: State = bincode::deserialize(&inner.get(state)?).unwrap();
        let mut attempts = attempts.lock().unwrap();
        attempts.push(proposal);
        if attempts.len() == 1 {
            assert_eq!(Cohort::new(inner, name).append(id(1), at(0), at(2), Payload::Empty), Ok(Ok(())));
        }
        inner.cas_root(name, expected, state)
    });
    let outcomes = Cohort::new(&hooked, "cohort")
        .transact_batch(
            &Resolver::new(2).unwrap(),
            &[(10, vec![append(1, 0, 1, Payload::Empty)]), (20, vec![create(2)])],
        )
        .unwrap();
    assert_eq!(
        outcomes,
        BTreeMap::from([(10, Err(Rejection { group: 0, reason: Failure::Write(at(2)) })), (20, Ok(())),])
    );
    assert_eq!(hooked.reads.load(Ordering::SeqCst), 2);
    assert_eq!(hooked.calls.load(Ordering::SeqCst), 2);
    let attempts = attempts.lock().unwrap();
    assert_eq!(attempts[0].write(id(1)), Some(&at(1)));
    assert_eq!(attempts[1].write(id(1)), Some(&at(2)));
    assert!(attempts.iter().all(|state| state.registry.contains_key(&id(2))));
    let state = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
    assert_eq!(state, attempts[1]);
    assert_eq!(state.manifest.len(), 1);
    assert_eq!(store.read_root("cohort").unwrap().version, 3);
}

#[test]
fn uncertain_publication_is_returned_without_rerunning_the_batch() {
    let resolver = Resolver::new(2).unwrap();
    for published in [false, true] {
        let store = Arc::new(MemoryStore::default());
        let hooked = CasHook::new(Arc::clone(&store), |inner, name, expected, state| {
            if published {
                assert!(inner.cas_root(name, expected, state)?);
            }
            Err(StoreError::CommitUnknown)
        });
        let result = Cohort::new(&hooked, "cohort").transact_batch(
            &resolver,
            &[(10, vec![populated(1, 1, Payload::Empty)]), (20, vec![populated(2, 2, Payload::Empty)])],
        );
        assert_eq!(result, Err(Error::Store(StoreError::CommitUnknown)));
        assert_eq!(hooked.reads.load(Ordering::SeqCst), 1);
        assert_eq!(hooked.calls.load(Ordering::SeqCst), 1);
        let recovered = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
        assert_eq!(recovered.registry.len(), if published { 2 } else { 0 });
        assert_eq!(recovered.manifest.len(), if published { 2 } else { 0 });
    }
}

#[test]
fn only_accepted_blob_references_must_exist_before_publication() {
    let store = Arc::new(MemoryStore::default());
    let hooked = CasHook::new(Arc::clone(&store), |inner, name, expected, state| inner.cas_root(name, expected, state));
    let cohort = Cohort::new(&hooked, "cohort");
    let resolver = Resolver::new(2).unwrap();
    let missing = Payload::Blob([0; 32]);
    assert_eq!(
        cohort.transact_batch(&resolver, &[(10, vec![populated(1, 1, missing.clone())]), (20, vec![create(2)]),]),
        Err(Error::Store(StoreError::Missing([0; 32])))
    );
    assert_eq!(hooked.calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.read_root("cohort").unwrap(), Root::default());
    assert_eq!(Cohort::new(store.as_ref(), "cohort").snapshot().unwrap(), State::default());
    assert_eq!(
        cohort
            .transact_batch(
                &resolver,
                &[(10, vec![populated(1, 1, Payload::Empty)]), (20, vec![append(1, 0, 2, missing)]),]
            )
            .unwrap(),
        BTreeMap::from([(10, Ok(())), (20, Err(Rejection { group: 0, reason: Failure::Write(at(1)) })),])
    );
    assert_eq!(hooked.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn outcomes_are_unavailable_until_publication_completes() {
    let store = Arc::new(MemoryStore::default());
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let hooked = CasHook::new(Arc::clone(&store), move |inner, name, expected, state| {
        let result = inner.cas_root(name, expected, state)?;
        ready_tx.send(()).unwrap();
        release_rx.lock().unwrap().recv().map_err(|error| StoreError::Unavailable(error.to_string()))?;
        Ok(result)
    });
    let worker = std::thread::spawn(move || {
        let result = Cohort::new(&hooked, "cohort")
            .transact_batch(&Resolver::new(2).unwrap(), &[(10, vec![populated(1, 1, Payload::Empty)])]);
        let _ = result_tx.send(result);
    });
    ready_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert_eq!(store.read_root("cohort").unwrap().version, 1);
    assert!(matches!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    release_tx.send(()).unwrap();
    assert_eq!(result_rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap(), BTreeMap::from([(10, Ok(()))]));
    worker.join().unwrap();
}

struct TrapStore;

impl Store for TrapStore {
    fn put(&self, _: &[u8]) -> Result<BlobId, StoreError> {
        panic!("unexpected put")
    }
    fn get(&self, _: BlobId) -> Result<Vec<u8>, StoreError> {
        panic!("unexpected get")
    }
    fn read_root(&self, _: &str) -> Result<Root, StoreError> {
        panic!("unexpected root read")
    }
    fn cas_root(&self, _: &str, _: u64, _: BlobId) -> Result<bool, StoreError> {
        panic!("unexpected CAS")
    }
}

#[test]
fn empty_batch_is_a_local_noop() {
    assert_eq!(Cohort::new(&TrapStore, "cohort").transact_batch(&Resolver::new(2).unwrap(), &[]), Ok(BTreeMap::new()));
}

#[test]
fn duplicate_transaction_keys_are_rejected_before_storage() {
    let result = Cohort::new(&TrapStore, "cohort").transact_batch(&Resolver::new(1).unwrap(), &[(1, vec![]), (1, vec![])]);
    assert!(matches!(result, Err(Error::Invalid(_))));
}

#[test]
fn declared_widths_survive_padding_and_aborted_creations() {
    let store = MemoryStore::default();
    let reference = Cohort::new(&store, "reference");
    let publisher = Cohort::new(&store, "resolved");
    let point = |coordinates: &[i64]| -> Frontier {
        [Time::new(coordinates.into())].into_iter().collect()
    };
    let high_start = point(&[0, i64::MIN, i64::MIN]);
    let high_upper = [Time::new(vec![0, 1, 0].into()), Time::new(vec![1, 0, 0].into())].into_iter().collect();
    let transactions = vec![
        (0, vec![create(1), (id(2), Change::Create { shape: Shape { time_width: 3, ..shape() }, start: high_start.clone() })]),
        (10, vec![(id(0), Change::Create { shape: Shape { time_width: 5, ..shape() }, start: point(&[0, 0, 0, 0, 0]) }), append(1, 1, 2, Payload::Empty)]),
        (20, vec![(id(0), Change::Create { shape: Shape { time_width: 2, ..shape() }, start: point(&[0, 0]) })]),
        (30, vec![append(1, 0, 1, Payload::Empty), (id(2), Change::Append { lower: high_start, upper: high_upper, batch: Payload::Empty })]),
    ];
    let expected = transactions.iter().map(|(key, groups)| (*key, reference.transact(groups).unwrap())).collect();
    assert_eq!(publisher.transact_batch(&Resolver::new(2).unwrap(), &transactions).unwrap(), expected);
    let actual = publisher.snapshot().unwrap();
    assert_eq!(actual, reference.snapshot().unwrap());
    assert_eq!(actual.registry[&id(0)].shape.time_width, 2);
    assert_eq!(actual.registry[&id(2)].shape.time_width, 3);
}
