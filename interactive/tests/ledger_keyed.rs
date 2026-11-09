//! Compare the production resolver with ordered native transactions on the same snapshot.

use std::collections::BTreeMap;

use interactive::ledger::{Change, Cohort, Failure, Frontier, Id, MemoryStore, Payload, Rejection, Resolver, Shape, Time, Transaction};

fn id(value: u128) -> Id { Id::from_u128(value) }
fn at(value: i64) -> Frontier { frontier(&[&[value, value]]) }
fn frontier(points: &[&[i64]]) -> Frontier {
    points.iter().map(|point| Time::new((*point).into())).collect()
}
fn shape(width: usize) -> Shape { Shape { columns: corgi::Shape::Int, time_width: width } }
fn create(id: Id) -> (Id, Change) { (id, Change::Create { shape: shape(2), start: at(0) }) }
fn append(id: Id, lower: Frontier, upper: Frontier) -> (Id, Change) {
    (id, Change::Append { lower, upper, batch: Payload::Empty })
}

fn compare(resolver: &Resolver, seed: &[(Id, Change)], transactions: &[Transaction]) -> BTreeMap<i64, Result<(), Rejection>> {
    let store = MemoryStore::default();
    let cohort = Cohort::new(&store, "oracle");
    assert_eq!(cohort.transact(seed).unwrap(), Ok(()));
    let before = cohort.snapshot().unwrap();
    let mut ordered = transactions.to_vec();
    ordered.sort_by_key(|(key, _)| *key);
    let expected = ordered.iter().map(|(key, groups)| (*key, cohort.transact(groups).unwrap()))
        .collect::<BTreeMap<_, _>>();
    let actual = resolver.resolve(&before, transactions).unwrap();
    assert_eq!(actual.outcomes.keys().collect::<Vec<_>>(), expected.keys().collect::<Vec<_>>());
    for (key, expected) in &expected {
        // Invalid's text describes the implementation's diagnostic, not its logical result.
        match (&actual.outcomes[key], expected) {
            (Err(Rejection { group: a, reason: Failure::Invalid(_) }),
             Err(Rejection { group: b, reason: Failure::Invalid(_) })) => assert_eq!(a, b),
            (actual, expected) => assert_eq!(actual, expected, "transaction {key}, batch {transactions:?}"),
        }
    }
    assert_eq!(actual.state, cohort.snapshot().unwrap(), "batch {transactions:?}");
    actual.outcomes
}

#[test]
fn cascading_rollbacks_reinstate_later_transactions() {
    let [a, b, c, d] = [id(1), id(2), id(3), id(4)];
    let seed = [create(a), create(b), create(c), create(d)];
    // If all proposed writes are counted initially, transaction 20 rejects behind transaction 10; 10 then aborts.
    // Committing 20 aborts 30, which reinstates 40.
    let mut transactions = vec![
        (10, vec![append(a, at(0), at(1)), append(b, at(1), at(2))]),
        (20, vec![append(a, at(0), at(1)), append(c, at(0), at(1))]),
        (30, vec![append(b, at(0), at(1)), append(c, at(0), at(1))]),
        (40, vec![append(b, at(0), at(1)), append(d, at(0), at(1))]),
        (50, vec![append(c, at(1), at(2)), append(d, at(1), at(2))]),
        (60, vec![append(a, at(1), at(2)), append(b, at(1), at(2))]),
    ];
    transactions.reverse();
    for workers in [1, 2] {
        let outcomes = compare(&Resolver::new(workers).unwrap(), &seed, &transactions);
        assert_eq!(outcomes.iter().map(|(key, result)| (*key, result.is_ok())).collect::<Vec<_>>(),
                   vec![(10, false), (20, true), (30, false), (40, true), (50, true), (60, true)]);
    }
}

#[test]
fn concurrent_creations_and_extreme_transaction_keys() {
    let a = id((1_u128 << 64) | (1_u128 << 63) | 1);
    let b = id((1_u128 << 127) | 1);
    let c = id(u128::MAX);
    let unknown = id((1_u128 << 127) | 2);
    let middle = frontier(&[&[0, 1], &[1, 0]]);
    let populated = |id| (id, Change::CreateAppend {
        shape: shape(2), start: at(0), upper: middle.clone(), batch: Payload::Empty,
    });
    let transactions = vec![
        (i64::MAX, vec![append(a, at(1), Frontier::new()), append(b, at(1), Frontier::new())]),
        (-1, vec![populated(b), create(c)]),
        (i64::MIN + 1, vec![populated(a)]),
        (0, vec![append(a, middle.clone(), at(1)), append(b, middle.clone(), at(1))]),
        (i64::MIN, vec![create(a)]),
        (-3, vec![append(unknown, at(0), at(1))]),
        (-2, vec![append(a, at(0), middle.clone())]),
        (1, vec![populated(b)]),
        (2, vec![]),
    ];
    for workers in [1, 2] {
        let outcomes = compare(&Resolver::new(workers).unwrap(), &[], &transactions);
        assert!(matches!(&outcomes[&(i64::MIN + 1)], Err(Rejection { group: 0, reason: Failure::Exists(found) }) if *found == a));
        assert!(matches!(&outcomes[&1], Err(Rejection { group: 0, reason: Failure::Exists(found) }) if *found == b));
        assert!(matches!(&outcomes[&-3], Err(Rejection { group: 0, reason: Failure::UnknownLedger(found) }) if *found == unknown));
        assert!(outcomes[&i64::MIN].is_ok());
        assert!(outcomes[&i64::MAX].is_ok());
        assert!(outcomes[&2].is_ok());
    }
}

#[test]
fn rolled_back_creation_does_not_supply_since() {
    let [a, b] = [id(1), id(2)];
    let transactions = vec![
        (10, vec![
            (a, Change::CreateAppend { shape: shape(2), start: at(5), upper: at(6), batch: Payload::Empty }),
            append(b, at(0), at(1)),
        ]),
        (20, vec![(a, Change::Create { shape: shape(2), start: at(10) })]),
        (30, vec![append(a, at(10), at(11))]),
    ];
    for workers in [1, 2] {
        // Comparing complete states checks that the committed span's since is 10, not 5.
        let outcomes = compare(&Resolver::new(workers).unwrap(), &[], &transactions);
        assert!(matches!(&outcomes[&10], Err(Rejection { group: 1, reason: Failure::UnknownLedger(found) }) if *found == b));
        assert!(outcomes[&20].is_ok());
        assert!(outcomes[&30].is_ok());
    }
}

#[test]
fn structural_validation_precedes_first_state_failure() {
    let [a, b, c] = [id(1), id(2), id(3)];
    let transactions = vec![
        (0, vec![create(a), append(b, at(0), at(0))]),
        (1, vec![create(c), create(c)]),
        (2, vec![append(c, at(0), at(1)), create(b)]),
        (3, vec![create(a), append(b, at(0), at(1))]),
        (4, vec![append(b, at(0), at(1))]),
        (5, vec![append(a, at(1), at(2)), append(b, at(0), at(1))]),
        (6, vec![append(a, at(0), at(1)), append(b, at(0), at(1))]),
        (7, vec![append(a, at(0), at(1))]),
    ];
    for workers in [1, 2] {
        let resolver = Resolver::new(workers).unwrap();
        let outcomes = compare(&resolver, &[create(a)], &transactions);
        for key in [0, 1, 2] {
            assert!(matches!(&outcomes[&key], Err(Rejection { group: 1, reason: Failure::Invalid(_) })));
        }
        assert!(matches!(&outcomes[&3], Err(Rejection { group: 0, reason: Failure::Exists(found) }) if *found == a));
        assert!(matches!(&outcomes[&4], Err(Rejection { group: 0, reason: Failure::UnknownLedger(found) }) if *found == b));
        assert!(matches!(&outcomes[&5], Err(Rejection { group: 0, reason: Failure::Write(write) }) if *write == at(0)));
        assert!(matches!(&outcomes[&6], Err(Rejection { group: 1, reason: Failure::UnknownLedger(found) }) if *found == b));
        assert!(outcomes[&7].is_ok());
        compare(&resolver, &[], &[
            (i64::MIN, vec![]),
            (-1, vec![create(c), create(c)]),
            (i64::MAX, vec![]),
        ]);
    }
}

#[test]
fn product_antichains_and_sealing_agree() {
    let a = id(1);
    let start = frontier(&[&[-2, -2, -2]]);
    let middle = frontier(&[&[1, 0, 0], &[0, 1, 0], &[0, 0, 1]]);
    let end = frontier(&[&[1, 1, 1]]);
    let seed = [(a, Change::Create { shape: shape(3), start: start.clone() })];
    let transactions = vec![
        (0, vec![append(a, start, middle.clone())]),
        (1, vec![append(a, frontier(&[&[0, 2, 0]]), frontier(&[&[1, 0, 0]]))]),
        (2, vec![append(a, middle, end.clone())]),
        (3, vec![append(a, end.clone(), Frontier::new())]),
        (4, vec![append(a, end, frontier(&[&[2, 2, 2]]))]),
    ];
    for workers in [1, 2] {
        let outcomes = compare(&Resolver::new(workers).unwrap(), &seed, &transactions);
        assert!(matches!(&outcomes[&1], Err(Rejection { group: 0, reason: Failure::Invalid(_) })));
        assert!(matches!(&outcomes[&4], Err(Rejection { group: 0, reason: Failure::Write(write) }) if write.elements().is_empty()));
    }
}

#[test]
fn small_conflicting_histories_agree() {
    let [a, b] = [id(1), id(2)];
    let resolver = Resolver::new(1).unwrap();
    for mask in 0..16 {
        let choose = |bit, zero, one| if mask & (1 << bit) == 0 { zero } else { one };
        let transactions = vec![
            (0, vec![append(a, at(0), at(1)), append(b, at(choose(0, 1, 0)), at(2))]),
            (1, vec![append(a, at(choose(1, 0, 1)), at(2))]),
            (2, vec![append(b, at(choose(2, 0, 2)), at(3))]),
            (3, vec![append(a, at(choose(3, 0, 2)), at(3)), append(b, at(choose(2, 0, 3)), at(4))]),
        ];
        compare(&resolver, &[create(a), create(b)], &transactions);
    }
}
