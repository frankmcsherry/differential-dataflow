use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Barrier,
};

fn frontier(times: &[&[i64]]) -> Frontier {
    times.iter().map(|time| Time::new((*time).into())).collect()
}

fn at(time: &[i64]) -> Frontier {
    frontier(&[time])
}

fn shape(time_width: usize) -> Shape {
    Shape { columns: corgi::Shape::Int, time_width }
}

/// Intercept publication to schedule competing commits and storage failures deterministically.
struct CasHook<'a> {
    inner: &'a MemoryStore,
    cas: Box<dyn Fn(&MemoryStore, &str, u64, BlobId) -> Result<bool, StoreError> + Send + Sync + 'a>,
}

impl Store for CasHook<'_> {
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        self.inner.put(bytes)
    }
    fn get(&self, id: BlobId) -> Result<Vec<u8>, StoreError> {
        self.inner.get(id)
    }
    fn read_root(&self, name: &str) -> Result<Root, StoreError> {
        self.inner.read_root(name)
    }
    fn cas_root(&self, name: &str, expected: u64, state: BlobId) -> Result<bool, StoreError> {
        (self.cas)(self.inner, name, expected, state)
    }
}

#[test]
fn blobs_are_immutable_and_named_by_content() {
    let store = MemoryStore::default();
    let first = store.put(b"payload").unwrap();
    assert_eq!(first, store.put(b"payload").unwrap());
    assert_ne!(first, store.put(b"another payload").unwrap());
    assert_eq!(store.get(first).unwrap(), b"payload");
    assert_eq!(store.get([0; 32]), Err(StoreError::Missing([0; 32])));
}

#[test]
fn creation_empty_appends_payloads_and_recovery() {
    let store = MemoryStore::default();
    let start = at(&[4, 5]);
    let (id, state) = {
        let cohort = Cohort::new(&store, "cohort");
        let id = cohort.create(shape(2), start.clone()).unwrap();
        let initial = cohort.snapshot().unwrap();
        assert_eq!(initial.registry[&id], Registration { shape: shape(2), start: start.clone() });
        assert_eq!(initial.write(id), Some(&start));
        assert!(initial.manifest.is_empty());
        assert_eq!(cohort.append(id, start.clone(), at(&[5, 5]), Payload::Empty), Ok(Ok(())));
        let blob = store.put(b"updates with timestamps").unwrap();
        assert_eq!(cohort.append(id, at(&[5, 5]), at(&[5, 6]), Payload::Blob(blob)), Ok(Ok(())));
        assert_eq!(cohort.append(id, at(&[5, 6]), Frontier::new(), Payload::Inline(vec![8, 9])), Ok(Ok(())));
        let state = cohort.snapshot().unwrap();
        assert_eq!(state.write(id), Some(&Frontier::new()));
        assert!(state.manifest.iter().all(|span| span.since == start));
        assert_eq!(state.manifest[0].batch, Payload::Empty);
        assert_eq!(state.manifest[1].batch, Payload::Blob(blob));
        assert_eq!(state.manifest[2].batch, Payload::Inline(vec![8, 9]));
        assert_eq!(store.get(blob).unwrap(), b"updates with timestamps");
        (id, state)
    };
    let recovered = Cohort::new(&store, "cohort");
    assert_eq!(recovered.snapshot().unwrap(), state);
    assert_eq!(recovered.append(id, at(&[5, 6]), at(&[5, 7]), Payload::Empty), Ok(Err(Frontier::new())));
}

#[test]
fn product_frontiers_advance_and_stale_lowers_report_current_write() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let start = at(&[0, 0]);
    let id = cohort.create(shape(2), start.clone()).unwrap();
    let middle = frontier(&[&[0, 1], &[1, 0]]);
    assert_eq!(cohort.append(id, start.clone(), middle.clone(), Payload::Empty), Ok(Ok(())));
    assert_eq!(cohort.append(id, start, at(&[1, 1]), Payload::Empty), Ok(Err(middle.clone())));
    assert_eq!(cohort.append(id, frontier(&[&[1, 0], &[0, 1]]), at(&[1, 1]), Payload::Empty), Ok(Ok(())));
    let spans = cohort.snapshot().unwrap().manifest;
    assert_eq!(spans[0].upper, middle);
    assert_eq!(spans[1].lower, middle);
}

#[test]
fn equal_regressing_incomparable_and_wrong_width_bounds_do_not_commit() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let start = at(&[0, 2]);
    let id = cohort.create(shape(2), start.clone()).unwrap();
    let before = store.read_root("cohort").unwrap();
    for upper in [start.clone(), at(&[0, 1]), at(&[1, 0])] {
        assert_eq!(
            cohort.append(id, start.clone(), upper, Payload::Empty),
            Err(Error::Invalid("append requires lower < upper"))
        );
    }
    assert_eq!(
        cohort.append(id, start, at(&[1, 2, 3]), Payload::Empty),
        Err(Error::Invalid("time exceeds the ledger's time width"))
    );
    assert_eq!(store.read_root("cohort").unwrap(), before);
    assert!(cohort.snapshot().unwrap().manifest.is_empty());
}

#[test]
fn missing_payload_is_a_storage_error_and_does_not_publish() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let id = cohort.create(shape(1), at(&[0])).unwrap();
    let before = store.read_root("cohort").unwrap();
    assert_eq!(
        cohort.append(id, at(&[0]), at(&[1]), Payload::Blob([0; 32])),
        Err(Error::Store(StoreError::Missing([0; 32])))
    );
    assert_eq!(store.read_root("cohort").unwrap(), before);
}

#[test]
fn competing_appends_have_one_winner() {
    let store = MemoryStore::default();
    let id = Cohort::new(&store, "cohort").create(shape(1), at(&[0])).unwrap();
    let barrier = Barrier::new(2);
    let calls = AtomicUsize::new(0);
    let racing = CasHook {
        inner: &store,
        cas: Box::new(|inner, name, expected, state| {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                barrier.wait();
            }
            inner.cas_root(name, expected, state)
        }),
    };
    let results = std::thread::scope(|scope| {
        let one =
            scope.spawn(|| Cohort::new(&racing, "cohort").append(id, at(&[0]), at(&[1]), Payload::Inline(vec![1])));
        let two =
            scope.spawn(|| Cohort::new(&racing, "cohort").append(id, at(&[0]), at(&[1]), Payload::Inline(vec![2])));
        [one.join().unwrap(), two.join().unwrap()]
    });
    assert_eq!(results.iter().filter(|result| **result == Ok(Ok(()))).count(), 1);
    assert_eq!(results.iter().filter(|result| **result == Ok(Err(at(&[1])))).count(), 1);
    assert_eq!(Cohort::new(&store, "cohort").snapshot().unwrap().manifest.len(), 1);
}

#[test]
fn competing_creations_preserve_both_registry_entries() {
    let store = MemoryStore::default();
    let barrier = Barrier::new(2);
    let calls = AtomicUsize::new(0);
    let racing = CasHook {
        inner: &store,
        cas: Box::new(|inner, name, expected, state| {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                barrier.wait();
            }
            inner.cas_root(name, expected, state)
        }),
    };
    let ids = std::thread::scope(|scope| {
        let one = scope.spawn(|| Cohort::new(&racing, "cohort").create(shape(1), at(&[0])));
        let two = scope.spawn(|| Cohort::new(&racing, "cohort").create(shape(1), at(&[1])));
        [one.join().unwrap().unwrap(), two.join().unwrap().unwrap()]
    });
    assert_ne!(ids[0], ids[1]);
    let state = Cohort::new(&store, "cohort").snapshot().unwrap();
    assert_eq!(state.registry.len(), 2);
    assert_eq!(state.registry[&ids[0]].start, at(&[0]));
    assert_eq!(state.registry[&ids[1]].start, at(&[1]));
    assert_eq!(store.read_root("cohort").unwrap().version, 2);
}

#[test]
fn unrelated_ledger_commit_retries_without_rejecting_append() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let one = cohort.create(shape(1), at(&[0])).unwrap();
    let two = cohort.create(shape(1), at(&[0])).unwrap();
    let calls = AtomicUsize::new(0);
    let interrupted = CasHook {
        inner: &store,
        cas: Box::new(|inner, name, expected, state| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                assert_eq!(Cohort::new(inner, name).append(two, at(&[0]), at(&[2]), Payload::Empty), Ok(Ok(())));
            }
            inner.cas_root(name, expected, state)
        }),
    };
    assert_eq!(Cohort::new(&interrupted, "cohort").append(one, at(&[0]), at(&[1]), Payload::Empty), Ok(Ok(())));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let state = cohort.snapshot().unwrap();
    assert_eq!(state.write(one), Some(&at(&[1])));
    assert_eq!(state.write(two), Some(&at(&[2])));
    assert_eq!(state.manifest.len(), 2);
}

#[test]
fn failed_publication_leaves_only_an_uncommitted_proposal() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let id = cohort.create(shape(1), at(&[0])).unwrap();
    let before = cohort.snapshot().unwrap();
    let failing = CasHook {
        inner: &store,
        cas: Box::new(|_, _, _, _| Err(StoreError::Unavailable("interrupted before publication".into()))),
    };
    assert_eq!(
        Cohort::new(&failing, "cohort").append(id, at(&[0]), at(&[1]), Payload::Inline(vec![1])),
        Err(Error::Store(StoreError::Unavailable("interrupted before publication".into())))
    );
    assert_eq!(Cohort::new(&store, "cohort").snapshot().unwrap(), before);
    assert_eq!(cohort.append(id, at(&[0]), at(&[1]), Payload::Empty), Ok(Ok(())));
}

#[test]
fn unknown_append_outcome_may_or_may_not_have_published() {
    for published in [false, true] {
        let store = MemoryStore::default();
        let cohort = Cohort::new(&store, "cohort");
        let id = cohort.create(shape(1), at(&[0])).unwrap();
        let calls = AtomicUsize::new(0);
        let lost_reply = CasHook {
            inner: &store,
            cas: Box::new(|inner, name, expected, state| {
                calls.fetch_add(1, Ordering::SeqCst);
                if published {
                    assert!(inner.cas_root(name, expected, state)?);
                }
                Err(StoreError::CommitUnknown)
            }),
        };
        assert_eq!(
            Cohort::new(&lost_reply, "cohort").append(id, at(&[0]), at(&[1]), Payload::Empty),
            Err(Error::CommitUnknown { id })
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "an unknown publication must not be retried");
        let recovered = Cohort::new(&store, "cohort").snapshot().unwrap();
        assert_eq!(recovered.write(id), Some(&at(&[i64::from(published)])));
        assert_eq!(recovered.manifest.len(), usize::from(published));
    }
}

#[test]
fn uncertain_creation_identifies_the_possibly_committed_ledger() {
    let store = MemoryStore::default();
    let lost_reply = CasHook {
        inner: &store,
        cas: Box::new(|inner, name, expected, state| {
            assert!(inner.cas_root(name, expected, state)?);
            Err(StoreError::CommitUnknown)
        }),
    };
    let Err(Error::CommitUnknown { id }) = Cohort::new(&lost_reply, "cohort").create(shape(1), at(&[0])) else {
        panic!("creation must report its uncertain outcome");
    };
    let recovered = Cohort::new(&store, "cohort").snapshot().unwrap();
    assert_eq!(recovered.registry.len(), 1);
    assert_eq!(recovered.write(id), Some(&at(&[0])));
}

#[test]
fn independent_cohorts_have_independent_roots() {
    let store = MemoryStore::default();
    let one = Cohort::new(&store, "one");
    let two = Cohort::new(&store, "two");
    let id = one.create(shape(1), at(&[0])).unwrap();
    let other_root = store.read_root("two").unwrap();
    assert_eq!(one.append(id, at(&[0]), at(&[1]), Payload::Empty), Ok(Ok(())));
    assert_eq!(store.read_root("two").unwrap(), other_root);
    assert!(two.snapshot().unwrap().registry.is_empty());
}

fn create_change(id: Id, start: Frontier) -> (Id, Change) {
    (id, Change::Create { shape: shape(2), start })
}

fn append_change(id: Id, lower: Frontier, upper: Frontier, byte: u8) -> (Id, Change) {
    (id, Change::Append { lower, upper, batch: Payload::Inline(vec![byte]) })
}

fn create_append_change(id: Id, start: Frontier, upper: Frontier, byte: u8) -> (Id, Change) {
    (id, Change::CreateAppend { shape: shape(2), start, upper, batch: Payload::Inline(vec![byte]) })
}

#[test]
fn grouped_transaction_creates_and_appends_with_one_publication() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let one = Id::from_u128(1);
    let two = Id::from_u128(2);
    let three = Id::from_u128(3);
    let middle = frontier(&[&[0, 1], &[1, 0]]);
    let changes = [
        create_append_change(one, at(&[0, 0]), middle.clone(), 1),
        create_append_change(two, at(&[2, 3]), Frontier::new(), 2),
        create_change(three, at(&[7, 8])),
    ];
    assert_eq!(cohort.transact(&changes), Ok(Ok(())));
    assert_eq!(store.read_root("cohort").unwrap().version, 1);
    let state = cohort.snapshot().unwrap();
    assert_eq!(state.registry.len(), 3);
    assert_eq!(state.manifest.len(), 2);
    assert_eq!(state.write(one), Some(&middle));
    assert_eq!(state.write(two), Some(&Frontier::new()));
    assert_eq!(state.write(three), Some(&at(&[7, 8])));
    assert_eq!(state.manifest[0].lower, at(&[0, 0]));
    assert_eq!(state.manifest[0].since, at(&[0, 0]));
    assert_eq!(state.manifest[1].lower, at(&[2, 3]));
    assert_eq!(state.manifest[1].since, at(&[2, 3]));
    // A subsequent transaction can extend the same ledger's chain.
    assert_eq!(cohort.transact(&[append_change(one, middle, at(&[1, 1]), 3)]), Ok(Ok(())));
    assert_eq!(cohort.snapshot().unwrap().write(one), Some(&at(&[1, 1])));
    assert_eq!(store.read_root("cohort").unwrap().version, 2);
}

#[test]
fn rejected_group_discards_every_tentative_effect() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let existing = Id::from_u128(10);
    assert_eq!(cohort.transact(&[create_change(existing, at(&[0, 0]))]), Ok(Ok(())));
    let before_root = store.read_root("cohort").unwrap();
    let before = cohort.snapshot().unwrap();
    let changes = [
        create_change(Id::from_u128(1), at(&[0, 0])),
        create_append_change(Id::from_u128(2), at(&[0, 0]), at(&[1, 1]), 1),
        append_change(existing, at(&[1, 1]), at(&[2, 2]), 2),
    ];
    assert_eq!(cohort.transact(&changes), Ok(Err(Rejection { group: 2, reason: Failure::Write(at(&[0, 0])) })));
    assert_eq!(store.read_root("cohort").unwrap(), before_root);
    assert_eq!(cohort.snapshot().unwrap(), before);
}

#[test]
fn transaction_diagnostics_reject_without_partial_effects() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let fresh = Id::from_u128(1);
    let existing = Id::from_u128(10);
    let unknown = Id::from_u128(20);
    assert_eq!(cohort.transact(&[create_change(existing, at(&[0, 0]))]), Ok(Ok(())));
    let before_root = store.read_root("cohort").unwrap();
    let before = cohort.snapshot().unwrap();
    let cases = [
        (create_change(existing, at(&[0, 0])), Failure::Exists(existing)),
        (create_append_change(existing, at(&[0, 0]), at(&[1, 1]), 1), Failure::Exists(existing)),
        (append_change(unknown, at(&[0, 0]), at(&[1, 1]), 1), Failure::UnknownLedger(unknown)),
        (append_change(existing, at(&[1, 1]), at(&[2, 2]), 1), Failure::Write(at(&[0, 0]))),
        (append_change(existing, at(&[0, 0]), at(&[0, 0]), 1), Failure::Invalid("append requires lower < upper")),
    ];
    for (last, reason) in cases {
        assert_eq!(
            cohort.transact(&[create_change(fresh, at(&[0, 0])), last]),
            Ok(Err(Rejection { group: 1, reason }))
        );
        assert_eq!(store.read_root("cohort").unwrap(), before_root);
        assert_eq!(cohort.snapshot().unwrap(), before);
    }
}

#[test]
fn payload_storage_failure_discards_tentative_creation() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let id = Id::from_u128(1);
    let changes = [(
        id,
        Change::CreateAppend { shape: shape(2), start: at(&[0, 0]), upper: at(&[1, 1]), batch: Payload::Blob([0; 32]) },
    )];
    assert_eq!(cohort.transact(&changes), Err(Error::Store(StoreError::Missing([0; 32]))));
    assert_eq!(store.read_root("cohort").unwrap(), Root::default());
    assert_eq!(cohort.snapshot().unwrap(), State::default());
}

#[test]
fn transaction_retry_preserves_candidate_ids_and_unrelated_commits() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let unrelated = cohort.create(shape(2), at(&[0, 0])).unwrap();
    let fresh = Id::from_u128(1);
    let calls = AtomicUsize::new(0);
    let interrupted = CasHook {
        inner: &store,
        cas: Box::new(|inner, name, expected, state| {
            let candidate: State = bincode::deserialize(&inner.get(state)?).unwrap();
            assert!(candidate.registry.contains_key(&fresh));
            assert_eq!(candidate.write(fresh), Some(&at(&[1, 1])));
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                assert_eq!(
                    Cohort::new(inner, name).append(unrelated, at(&[0, 0]), at(&[2, 2]), Payload::Empty),
                    Ok(Ok(()))
                );
            }
            inner.cas_root(name, expected, state)
        }),
    };
    let changes = [create_append_change(fresh, at(&[0, 0]), at(&[1, 1]), 1)];
    assert_eq!(Cohort::new(&interrupted, "cohort").transact(&changes), Ok(Ok(())));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let state = cohort.snapshot().unwrap();
    assert_eq!(state.registry.len(), 2);
    assert_eq!(state.write(fresh), Some(&at(&[1, 1])));
    assert_eq!(state.write(unrelated), Some(&at(&[2, 2])));
}

#[test]
fn concurrent_transactions_commit_all_or_none() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    let one = Id::from_u128(10);
    let two = Id::from_u128(20);
    assert_eq!(cohort.transact(&[create_change(one, at(&[0, 0])), create_change(two, at(&[0, 0]))]), Ok(Ok(())));
    let new_one = Id::from_u128(1);
    let new_two = Id::from_u128(2);
    let changes = |fresh, byte| {
        [
            create_change(fresh, at(&[0, 0])),
            append_change(one, at(&[0, 0]), at(&[1, 1]), byte),
            append_change(two, at(&[0, 0]), at(&[1, 1]), byte),
        ]
    };
    let barrier = Barrier::new(2);
    let calls = AtomicUsize::new(0);
    let racing = CasHook {
        inner: &store,
        cas: Box::new(|inner, name, expected, state| {
            if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                barrier.wait();
            }
            inner.cas_root(name, expected, state)
        }),
    };
    let results = std::thread::scope(|scope| {
        let first = scope.spawn(|| Cohort::new(&racing, "cohort").transact(&changes(new_one, 1)));
        let second = scope.spawn(|| Cohort::new(&racing, "cohort").transact(&changes(new_two, 2)));
        [first.join().unwrap(), second.join().unwrap()]
    });
    assert_eq!(results.iter().filter(|result| **result == Ok(Ok(()))).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Ok(Err(Rejection { group: 1, reason: Failure::Write(at(&[1, 1])) })))
            .count(),
        1
    );
    let winner = if results[0] == Ok(Ok(())) { (new_one, 1) } else { (new_two, 2) };
    let state = cohort.snapshot().unwrap();
    assert_eq!(state.registry.len(), 3);
    assert!(state.registry.contains_key(&winner.0));
    assert_eq!(state.manifest.len(), 2);
    assert!(state.manifest.iter().all(|span| span.batch == Payload::Inline(vec![winner.1])));
    assert_eq!(store.read_root("cohort").unwrap().version, 2);
}

#[test]
fn uncertain_transaction_publishes_all_or_none_without_retry() {
    for published in [false, true] {
        let store = MemoryStore::default();
        let one = Id::from_u128(1);
        let two = Id::from_u128(2);
        let calls = AtomicUsize::new(0);
        let lost_reply = CasHook {
            inner: &store,
            cas: Box::new(|inner, name, expected, state| {
                calls.fetch_add(1, Ordering::SeqCst);
                if published {
                    assert!(inner.cas_root(name, expected, state)?);
                }
                Err(StoreError::CommitUnknown)
            }),
        };
        let changes = [
            create_append_change(one, at(&[0, 0]), at(&[1, 1]), 1),
            create_append_change(two, at(&[2, 3]), Frontier::new(), 2),
        ];
        assert_eq!(Cohort::new(&lost_reply, "cohort").transact(&changes), Err(Error::Store(StoreError::CommitUnknown)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let recovered = Cohort::new(&store, "cohort").snapshot().unwrap();
        if published {
            assert_eq!(recovered.registry.len(), 2);
            assert_eq!(recovered.manifest.len(), 2);
            assert_eq!(recovered.write(one), Some(&at(&[1, 1])));
            assert_eq!(recovered.write(two), Some(&Frontier::new()));
        } else {
            assert_eq!(recovered, State::default());
        }
    }
}

#[test]
fn empty_transaction_has_no_publication() {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "cohort");
    assert_eq!(cohort.transact(&[]), Ok(Ok(())));
    assert_eq!(store.read_root("cohort").unwrap(), Root::default());
}

/// Structurally invalid requests must never consult storage.
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
fn structural_validation_rejects_without_storage() {
    let one = Id::from_u128(1);
    let two = Id::from_u128(2);
    let cohort = Cohort::new(&TrapStore, "cohort");
    let cases = [
        (
            vec![create_change(one, at(&[0, 0])), create_change(one, at(&[0, 0]))],
            1,
            "transaction groups must have strictly increasing ids",
        ),
        (
            vec![create_change(two, at(&[0, 0])), create_change(one, at(&[0, 0]))],
            1,
            "transaction groups must have strictly increasing ids",
        ),
        (
            vec![append_change(one, at(&[0, 0]), at(&[1, 1]), 1), append_change(one, at(&[1, 1]), at(&[2, 2]), 2)],
            1,
            "transaction groups must have strictly increasing ids",
        ),
        (vec![append_change(one, at(&[0, 0]), at(&[0, 0]), 1)], 0, "append requires lower < upper"),
        (vec![create_append_change(one, at(&[1, 1]), at(&[0, 0]), 1)], 0, "append requires lower < upper"),
        (
            vec![append_change(one, at(&[0, 0]), at(&[0, 0]), 1), create_change(one, at(&[0, 0]))],
            0,
            "append requires lower < upper",
        ),
    ];
    for (changes, group, message) in cases {
        let rejection = Rejection { group, reason: Failure::Invalid(message) };
        assert_eq!(validate(&changes), Err(rejection.clone()));
        assert_eq!(cohort.transact(&changes), Ok(Err(rejection)));
    }
}
