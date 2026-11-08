//! Compare the Rust backing and the .ddp transaction rules on the same histories.
//! Fixture IDs map to UUIDs, and shapes are serialized as opaque bytes for .ddp.

use std::collections::BTreeMap;

use interactive::ledger::{validate, Change, Cohort, Failure, Frontier, Id, MemoryStore, Payload, Rejection, Shape, State, Store, Time};
use interactive::runtime::container::Updates;

type Row = (String, String, i64);
type Transaction = (i64, Vec<(Id, Change)>);

fn tuple(fields: &[String]) -> String { format!("tuple({})", fields.join(",")) }
fn list(fields: &[String]) -> String { format!("list({})", fields.join(",")) }
fn inject(tag: usize, payload: &str) -> String { format!("inject({tag},{payload})") }
fn number(id: Id) -> i64 { id.as_u128().try_into().unwrap() }
fn id(number: i64) -> Id { Id::from_u128(number.try_into().unwrap()) }
fn bytes(bytes: &[u8]) -> String { list(&bytes.iter().map(u8::to_string).collect::<Vec<_>>()) }

fn frontier(points: &[&[i64]]) -> Frontier {
    points.iter().map(|point| Time::new((*point).into())).collect()
}

fn at(x: i64, y: i64) -> Frontier { frontier(&[&[x, y]]) }

fn shape() -> Shape {
    Shape { columns: corgi::Shape::Int, time_width: 2 }
}

fn print_frontier(frontier: &Frontier) -> String {
    let mut points = frontier.elements().to_vec();
    points.sort();
    list(&points.iter().map(|point| {
        list(&(0..2).map(|i| point.get(i).copied().unwrap_or(i64::MIN).to_string()).collect::<Vec<_>>())
    }).collect::<Vec<_>>())
}

fn print_shape(shape: &Shape) -> String { bytes(&bincode::serialize(shape).unwrap()) }

fn print_payload(payload: &Payload) -> String {
    match payload {
        Payload::Empty => inject(0, "tuple()"),
        Payload::Inline(data) => inject(1, &bytes(data)),
        Payload::Blob(hash) => inject(2, &bytes(hash)),
    }
}

fn print_group((id, change): &(Id, Change)) -> String {
    let change = match change {
        Change::Create { shape, start } => inject(0, &tuple(&[
            print_shape(shape), print_frontier(start),
        ])),
        Change::Append { lower, upper, batch } => inject(1, &tuple(&[
            print_frontier(lower), print_frontier(upper), print_payload(batch),
        ])),
        Change::CreateAppend { shape, start, upper, batch } => inject(2, &tuple(&[
            print_shape(shape), print_frontier(start), print_frontier(upper), print_payload(batch),
        ])),
    };
    tuple(&[number(*id).to_string(), change])
}

fn row(key: i64, value: String) -> Row {
    (tuple(&[key.to_string()]), tuple(&[value]), 1)
}

fn print_result(txn: i64, result: Result<(), Rejection>) -> Row {
    let result = match result {
        Ok(()) => inject(0, "tuple()"),
        Err(Rejection { group, reason }) => {
            let reason = match reason {
                Failure::Write(write) => inject(0, &print_frontier(&write)),
                Failure::Exists(id) => inject(1, &number(id).to_string()),
                Failure::UnknownLedger(id) => inject(2, &number(id).to_string()),
                Failure::Invalid(_) => inject(3, "tuple()"),
            };
            inject(1, &tuple(&[group.to_string(), reason]))
        }
    };
    row(txn, result)
}

fn registry(state: &State) -> Vec<Row> {
    state.registry.iter().map(|(id, entry)| (
        tuple(&[number(*id).to_string()]),
        tuple(&[print_shape(&entry.shape), print_frontier(&entry.start)]),
        1,
    )).collect()
}

fn manifest(state: &State) -> Vec<Row> {
    state.manifest.iter().map(|span| (
        tuple(&[number(span.id).to_string()]),
        tuple(&[print_frontier(&span.lower), print_frontier(&span.upper), print_frontier(&span.since), print_payload(&span.batch)]),
        1,
    )).collect()
}

fn feed(rows: &[Row]) -> Vec<String> {
    rows.iter().map(|(key, value, diff)| format!("{key} val={value} diff={diff}")).collect()
}

fn evaluate(before: &State, txns: &[Transaction], workers: usize) -> BTreeMap<String, Updates> {
    let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
        include_str!("../model/transactions.ddp"),
    ));
    interactive::passes::optimize(&mut program);
    let commands = txns.iter().map(|(txn, groups)| {
        row(*txn, list(&groups.iter().map(print_group).collect::<Vec<_>>()))
    }).collect::<Vec<_>>();
    interactive::server::evaluate(timely::Config::process(workers), &program,
        &[feed(&registry(before)), feed(&manifest(before)), feed(&commands)])
}

fn create(number: i64, start: Frontier) -> (Id, Change) {
    (id(number), Change::Create { shape: shape(), start })
}

fn append(number: i64, lower: Frontier, upper: Frontier) -> (Id, Change) {
    (id(number), Change::Append { lower, upper, batch: Payload::Empty })
}

fn populated(number: i64, start: Frontier, upper: Frontier, batch: Payload) -> (Id, Change) {
    (id(number), Change::CreateAppend { shape: shape(), start, upper, batch })
}

/// Each round resolves concurrent transactions in key order; changes are grouped by strictly increasing ledger ID.
fn check(rounds: &[Vec<Transaction>], workers: usize, payloads: &[&[u8]]) {
    let store = MemoryStore::default();
    for payload in payloads { store.put(payload).unwrap(); }
    let cohort = Cohort::new(&store, "comparison");
    for txns in rounds {
        let before = cohort.snapshot().unwrap();
        let mut ordered = txns.to_vec();
        ordered.sort_by_key(|(txn, _)| *txn);
        let results = ordered.iter().map(|(txn, commands)| {
            print_result(*txn, cohort.transact(commands).expect("fixture storage succeeds"))
        }).collect::<Vec<_>>();
        let invalid_transactions = ordered.iter().filter_map(|(txn, changes)| {
            validate(changes).err().map(|rejection| row(*txn, rejection.group.to_string()))
        }).collect::<Vec<_>>();
        let after = cohort.snapshot().unwrap();
        let actual = evaluate(&before, txns, workers);
        let registry_add = State {
            registry: after.registry.iter().filter(|(id, _)| !before.registry.contains_key(*id))
                .map(|(id, entry)| (*id, entry.clone())).collect(),
            manifest: after.manifest[before.manifest.len()..].to_vec(),
        };
        let expected = [
            ("results", results),
            ("invalid_transactions", invalid_transactions),
            ("registry", registry(&after)),
            ("manifest", manifest(&after)),
            ("registry_add", registry(&registry_add)),
            ("manifest_add", manifest(&registry_add)),
        ];
        for (name, mut expected) in expected {
            let mut got = actual.get(name).unwrap_or_else(|| panic!("missing {name}")).rows();
            got.sort();
            expected.sort();
            assert_eq!(got, expected, "{workers} workers, export {name}, transactions {ordered:?}");
        }
    }
}

#[test]
fn creation_chained_appends_and_competition_agree() {
    let start = at(0, 0);
    let middle = frontier(&[&[0, 1], &[1, 0]]);
    let rounds = vec![
        vec![
            (30, vec![append(1, middle.clone(), at(1, 1)), create(2, at(4, 5))]),
            (10, vec![populated(1, start.clone(), middle.clone(), Payload::Empty)]),
            (20, vec![append(1, start.clone(), at(2, 2))]),
            (40, vec![]),
        ],
        vec![
            (10, vec![append(1, at(1, 1), at(2, 2)), append(2, at(4, 5), at(5, 5))]),
            (20, vec![append(1, at(1, 1), at(3, 3))]),
            (30, vec![append(1, at(2, 2), Frontier::new())]),
            (40, vec![append(1, at(2, 2), at(3, 3))]),
        ],
    ];
    for workers in [1, 2] { check(&rounds, workers, &[]); }
}

#[test]
fn rollback_and_rejection_diagnostics_agree() {
    let start = at(0, 0);
    let rounds = vec![
        vec![(0, vec![create(1, start.clone()), create(2, start.clone())])],
        vec![
            (10, vec![append(1, start.clone(), at(1, 1)), append(2, at(1, 1), at(2, 2))]),
            (20, vec![append(1, start.clone(), at(2, 2))]),
            (30, vec![populated(3, start.clone(), start.clone(), Payload::Empty)]),
            (40, vec![populated(3, start.clone(), at(1, 1), Payload::Empty)]),
            (50, vec![append(99, start.clone(), at(1, 1))]),
            (60, vec![create(1, start.clone())]),
            (70, vec![append(99, start.clone(), start.clone())]),
            (80, vec![append(2, at(0, 2), at(1, 0))]),
            (85, vec![populated(4, start.clone(), at(1, 1), Payload::Empty), append(99, start.clone(), at(1, 1))]),
            (86, vec![create(4, start.clone())]),
            (90, vec![append(2, start.clone(), at(1, 1)), append(2, start, at(2, 2))]),
        ],
        vec![(0, vec![append(2, at(0, 0), at(3, 3))])],
    ];
    for workers in [1, 2] { check(&rounds, workers, &[]); }
}

#[test]
fn opaque_payloads_and_signed_product_times_agree() {
    let store = MemoryStore::default();
    let blob = store.put(b"opaque blob contents").unwrap();
    let mut external = append(1, at(0, 0), at(1, 1));
    if let (_, Change::Append { batch, .. }) = &mut external { *batch = Payload::Blob(blob); }
    let rounds = vec![vec![
        (0, vec![populated(1, at(-2, 0), at(0, 0), Payload::Inline(vec![9, 8, 7]))]),
        (1, vec![external]),
        (2, vec![append(1, at(1, 1), Frontier::new())]),
    ]];
    for workers in [1, 2] { check(&rounds, workers, &[b"opaque blob contents"]); }
}

#[test]
fn grouped_changes_and_local_conflicts_agree() {
    let start = at(0, 0);
    let middle = frontier(&[&[0, 1], &[1, 0]]);
    let rounds = vec![vec![
        (0, vec![populated(1, start.clone(), middle.clone(), Payload::Empty)]),
        (10, vec![create(2, start.clone()), create(2, start.clone())]),
        (20, vec![create(2, start.clone())]),
        (30, vec![append(1, middle.clone(), at(1, 1)), append(1, at(1, 1), at(2, 2))]),
        (40, vec![append(1, middle.clone(), at(1, 1))]),
        (50, vec![create(4, start.clone()), create(3, start.clone())]),
        (60, vec![create(3, start.clone()), create(4, start.clone())]),
        (70, vec![populated(5, start.clone(), start.clone(), Payload::Empty)]),
        (80, vec![populated(5, start, middle, Payload::Empty)]),
    ]];
    for workers in [1, 2] { check(&rounds, workers, &[]); }
}

#[test]
fn local_rejection_without_any_cohort_rows_agrees() {
    let rounds = vec![vec![(0, vec![create(1, at(0, 0)), create(1, at(0, 0))])]];
    for workers in [1, 2] { check(&rounds, workers, &[]); }
}
