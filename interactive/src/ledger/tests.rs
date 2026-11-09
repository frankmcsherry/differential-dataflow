//! The serial rules in `State::transact`, and the memory backing.
//! Publication, retries and recovery are tested through `Writer` in `tests/ledger_writer.rs`.

use super::*;

fn frontier(times: &[&[i64]]) -> Frontier {
    times.iter().map(|time| Time::new((*time).into())).collect()
}

fn at(time: &[i64]) -> Frontier {
    frontier(&[time])
}

fn shape(time_width: usize) -> Shape {
    Shape { columns: corgi::Shape::Int, time_width }
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

/// Apply one single-group transaction, reporting its failure without the group index.
fn single(state: &mut State, id: Id, change: Change) -> Result<(), Failure> {
    state.transact(&[(id, change)]).map_err(|rejection| rejection.reason)
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
fn creation_and_empty_blob_and_inline_appends() {
    let mut state = State::default();
    let id = Id::from_u128(1);
    let start = at(&[4, 5]);
    assert_eq!(single(&mut state, id, Change::Create { shape: shape(2), start: start.clone() }), Ok(()));
    assert_eq!(state.registry[&id], Registration { shape: shape(2), start: start.clone() });
    assert_eq!(state.write(id), Some(&start));
    assert!(state.manifest.is_empty());
    let blob = [7; 32];
    for (lower, upper, batch) in [
        (start.clone(), at(&[5, 5]), Payload::Empty),
        (at(&[5, 5]), at(&[5, 6]), Payload::Blob(blob)),
        (at(&[5, 6]), Frontier::new(), Payload::Inline(vec![8, 9])),
    ] {
        assert_eq!(single(&mut state, id, Change::Append { lower, upper, batch }), Ok(()));
    }
    assert_eq!(state.write(id), Some(&Frontier::new()));
    assert!(state.manifest.iter().all(|span| span.since == start));
    assert_eq!(state.manifest[0].batch, Payload::Empty);
    assert_eq!(state.manifest[1].batch, Payload::Blob(blob));
    assert_eq!(state.manifest[2].batch, Payload::Inline(vec![8, 9]));
    // A sealed ledger accepts nothing more; the failure reports the empty write frontier.
    let more = Change::Append { lower: at(&[5, 6]), upper: at(&[5, 7]), batch: Payload::Empty };
    assert_eq!(single(&mut state, id, more), Err(Failure::Write(Frontier::new())));
}

#[test]
fn product_frontiers_advance_and_stale_lowers_report_current_write() {
    let mut state = State::default();
    let id = Id::from_u128(1);
    let start = at(&[0, 0]);
    assert_eq!(single(&mut state, id, Change::Create { shape: shape(2), start: start.clone() }), Ok(()));
    let middle = frontier(&[&[0, 1], &[1, 0]]);
    let append = |lower, upper| Change::Append { lower, upper, batch: Payload::Empty };
    assert_eq!(single(&mut state, id, append(start.clone(), middle.clone())), Ok(()));
    assert_eq!(single(&mut state, id, append(start, at(&[1, 1]))), Err(Failure::Write(middle.clone())));
    assert_eq!(single(&mut state, id, append(frontier(&[&[1, 0], &[0, 1]]), at(&[1, 1]))), Ok(()));
    assert_eq!(state.manifest[0].upper, middle);
    assert_eq!(state.manifest[1].lower, middle);
}

#[test]
fn equal_regressing_incomparable_and_wrong_width_bounds_do_not_commit() {
    let mut state = State::default();
    let id = Id::from_u128(1);
    let start = at(&[0, 2]);
    assert_eq!(single(&mut state, id, Change::Create { shape: shape(2), start: start.clone() }), Ok(()));
    let before = state.clone();
    let append = |upper| Change::Append { lower: start.clone(), upper, batch: Payload::Empty };
    for upper in [start.clone(), at(&[0, 1]), at(&[1, 0])] {
        assert_eq!(single(&mut state, id, append(upper)), Err(Failure::Invalid("append requires lower < upper")));
    }
    assert_eq!(single(&mut state, id, append(at(&[1, 2, 3]))), Err(Failure::Invalid("time exceeds the ledger's time width")));
    assert_eq!(state, before);
}

#[test]
fn grouped_transaction_creates_and_appends() {
    let mut state = State::default();
    let one = Id::from_u128(1);
    let two = Id::from_u128(2);
    let three = Id::from_u128(3);
    let middle = frontier(&[&[0, 1], &[1, 0]]);
    let changes = [
        create_append_change(one, at(&[0, 0]), middle.clone(), 1),
        create_append_change(two, at(&[2, 3]), Frontier::new(), 2),
        create_change(three, at(&[7, 8])),
    ];
    assert_eq!(state.transact(&changes), Ok(()));
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
    assert_eq!(state.transact(&[append_change(one, middle, at(&[1, 1]), 3)]), Ok(()));
    assert_eq!(state.write(one), Some(&at(&[1, 1])));
}

#[test]
fn rejected_group_discards_every_tentative_effect() {
    let mut state = State::default();
    let existing = Id::from_u128(10);
    assert_eq!(state.transact(&[create_change(existing, at(&[0, 0]))]), Ok(()));
    let before = state.clone();
    let changes = [
        create_change(Id::from_u128(1), at(&[0, 0])),
        create_append_change(Id::from_u128(2), at(&[0, 0]), at(&[1, 1]), 1),
        append_change(existing, at(&[1, 1]), at(&[2, 2]), 2),
    ];
    assert_eq!(state.transact(&changes), Err(Rejection { group: 2, reason: Failure::Write(at(&[0, 0])) }));
    assert_eq!(state, before);
}

#[test]
fn transaction_diagnostics_reject_without_partial_effects() {
    let mut state = State::default();
    let fresh = Id::from_u128(1);
    let existing = Id::from_u128(10);
    let unknown = Id::from_u128(20);
    assert_eq!(state.transact(&[create_change(existing, at(&[0, 0]))]), Ok(()));
    let before = state.clone();
    let cases = [
        (create_change(existing, at(&[0, 0])), Failure::Exists(existing)),
        (create_append_change(existing, at(&[0, 0]), at(&[1, 1]), 1), Failure::Exists(existing)),
        (append_change(unknown, at(&[0, 0]), at(&[1, 1]), 1), Failure::UnknownLedger(unknown)),
        (append_change(existing, at(&[1, 1]), at(&[2, 2]), 1), Failure::Write(at(&[0, 0]))),
        (append_change(existing, at(&[0, 0]), at(&[0, 0]), 1), Failure::Invalid("append requires lower < upper")),
    ];
    for (last, reason) in cases {
        assert_eq!(state.transact(&[create_change(fresh, at(&[0, 0])), last]), Err(Rejection { group: 1, reason }));
        assert_eq!(state, before);
    }
}

#[test]
fn empty_transaction_commits_and_changes_nothing() {
    let mut state = State::default();
    assert_eq!(state.transact(&[]), Ok(()));
    assert_eq!(state, State::default());
}

#[test]
fn structural_validation_rejects_before_any_effect() {
    let one = Id::from_u128(1);
    let two = Id::from_u128(2);
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
        let mut state = State::default();
        assert_eq!(state.transact(&changes), Err(rejection));
        assert_eq!(state, State::default());
    }
}
