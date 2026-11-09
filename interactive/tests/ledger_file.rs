//! Check filesystem recovery in fresh processes and root CAS between processes.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use interactive::ledger::{Change, Cohort, FileStore, Frontier, Id, Payload, Shape, Store, Time, Writer};
use interactive::runtime::feed::FeedDecoder;
use interactive::server::{encode_ledger_batch, HostTime, LedgerSnapshot, Server};

const DIRECTORY: &str = "DDIR_FILE_TEST_DIRECTORY";
const ROLE: &str = "DDIR_FILE_TEST_ROLE";

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("ddir-ledger-process-{}", Id::new_v4())))
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn id(value: u128) -> Id {
    Id::from_u128(value)
}
fn frontier(points: &[&[i64]]) -> Frontier {
    points
        .iter()
        .map(|point| Time::new((*point).into()))
        .collect()
}
fn at(x: i64, y: i64) -> Frontier {
    frontier(&[&[x, y]])
}

fn shape() -> Shape {
    Shape {
        columns: corgi::Shape::Prod(vec![
            corgi::Shape::Prod(vec![corgi::Shape::Int]),
            corgi::Shape::Unit,
        ]),
        time_width: 2,
    }
}

fn payload(rows: &str, lanes: &[Vec<i64>]) -> Vec<u8> {
    let mut decoder = FeedDecoder::new(
        &corgi::Shape::Prod(vec![corgi::Shape::Int]),
        &corgi::Shape::Unit,
    );
    for row in rows.lines() {
        decoder.push_line(row).unwrap();
    }
    encode_ledger_batch(&decoder.finish(), lanes).unwrap()
}

fn child(test: &str, path: &Path, role: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test, "--nocapture"])
        .env(DIRECTORY, path)
        .env(ROLE, role);
    command
}

fn run(test: &str, path: &Path, role: &str) {
    let result = child(test, path, role).output().unwrap();
    assert!(
        result.status.success(),
        "{role}: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

fn ready(child: &mut Child) {
    let mut output = BufReader::new(child.stdout.as_mut().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            output.read_line(&mut line).unwrap(),
            0,
            "CAS worker exited before readiness"
        );
        if line.contains("DDIR_CAS_READY") {
            return;
        }
    }
}

#[test]
fn root_cas_excludes_other_processes() {
    if let Some(path) = std::env::var_os(DIRECTORY) {
        let store = FileStore::open(path).unwrap();
        let value: u8 = std::env::var(ROLE).unwrap().parse().unwrap();
        let blob = store.put(&[value]).unwrap();
        let root = store.read_root("race").unwrap();
        assert_eq!(root.version, 0);
        println!("DDIR_CAS_READY");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_exact(&mut [0]).unwrap();
        let won = store.cas_root("race", root.version, blob).unwrap();
        std::process::exit(if won { 0 } else { 3 });
    }
    let directory = Directory::new();
    let mut first = child("root_cas_excludes_other_processes", &directory.0, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut second = child("root_cas_excludes_other_processes", &directory.0, "2")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    ready(&mut first);
    ready(&mut second);
    first.stdin.as_mut().unwrap().write_all(&[1]).unwrap();
    second.stdin.as_mut().unwrap().write_all(&[1]).unwrap();
    let outcomes = [first.wait().unwrap().code(), second.wait().unwrap().code()];
    assert_eq!(outcomes.iter().filter(|code| **code == Some(0)).count(), 1);
    assert_eq!(outcomes.iter().filter(|code| **code == Some(3)).count(), 1);
    let store = FileStore::open(&directory.0).unwrap();
    let root = store.read_root("race").unwrap();
    let winner = if outcomes[0] == Some(0) { 1 } else { 2 };
    assert_eq!(root.version, 1);
    assert_eq!(root.state, Some(store.put(&[winner]).unwrap()));
}

#[test]
fn root_names_differing_by_case_are_distinct() {
    let directory = Directory::new();
    let store = FileStore::open(&directory.0).unwrap();
    let blob = store.put(b"state").unwrap();
    assert_eq!(store.cas_root("root", 0, blob), Ok(true));
    assert_eq!(store.read_root("Root").unwrap().version, 0);
    assert_eq!(store.cas_root("Root", 0, blob), Ok(true));
    assert_eq!(store.read_root("root").unwrap().version, 1);
}

#[test]
fn resolved_cohort_recovers_without_reresolving_intents() {
    use interactive::ledger::{Failure, Rejection};

    if let Some(path) = std::env::var_os(DIRECTORY) {
        let store = Arc::new(FileStore::open(path).unwrap());
        let cohort = Cohort::new(store.as_ref(), "resolved");
        let middle = frontier(&[&[0, 1], &[1, 0]]);
        match std::env::var(ROLE).unwrap().as_str() {
            "seed" => {
                let blob = store.put(&payload("1", &[vec![0], vec![0]])).unwrap();
                let outcomes = Writer::open(store.clone(), "resolved", 2)
                    .unwrap()
                    .transact_batch(
                        &[
                            (
                                10,
                                vec![(
                                    id(1),
                                    Change::CreateAppend {
                                        shape: shape(),
                                        start: at(0, 0),
                                        upper: middle.clone(),
                                        batch: Payload::Blob(blob),
                                    },
                                )],
                            ),
                            (
                                20,
                                vec![
                                    (id(0), Change::Create { shape: shape(), start: at(0, 0) }),
                                    (id(1), Change::Append { lower: at(0, 0), upper: at(1, 1), batch: Payload::Empty }),
                                ],
                            ),
                            (
                                30,
                                vec![(
                                    id(1),
                                    Change::Append {
                                        lower: middle.clone(),
                                        upper: at(1, 1),
                                        batch: Payload::Inline(payload("2", &[vec![0], vec![1]])),
                                    },
                                )],
                            ),
                            (
                                40,
                                vec![(
                                    id(1),
                                    Change::Append { lower: at(1, 1), upper: Frontier::new(), batch: Payload::Empty },
                                )],
                            ),
                        ],
                    )
                    .unwrap();
                assert_eq!(outcomes.len(), 4);
                assert_eq!(outcomes[&10], Ok(()));
                assert_eq!(outcomes[&20], Err(Rejection { group: 1, reason: Failure::Write(middle) }));
                assert_eq!(outcomes[&30], Ok(()));
                assert_eq!(outcomes[&40], Ok(()));
                assert_eq!(store.read_root("resolved").unwrap().version, 1);
            }
            "read" => {
                // Recovery consumes committed state and payloads, without constructing a writer.
                let state = cohort.snapshot().unwrap();
                assert_eq!(state.registry.len(), 1);
                assert!(!state.registry.contains_key(&id(0)));
                assert_eq!(state.manifest.len(), 3);
                assert_eq!(state.write(id(1)), Some(&Frontier::new()));
                assert_eq!(store.read_root("resolved").unwrap().version, 1);
                let cut = LedgerSnapshot::capture(store.as_ref(), "resolved", id(1)).unwrap();
                let (before, after) = timely::execute_directly(move |worker| {
                    let mut server = Server::new();
                    server.publish_ledger(worker, "saved", &cut).unwrap();
                    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
                        "output \"rows\" = import \"saved\" : ((int), ());",
                    ));
                    server.install_with_root(worker, "reader", &program, 2, HostTime::default()).unwrap();
                    let at = |x, y| HostTime::new([x, y].into());
                    (
                        server.snapshot_as_of(worker, "reader::rows", &at(0, 0)).unwrap().rows(),
                        server.snapshot_as_of(worker, "reader::rows", &at(0, 1)).unwrap().rows(),
                    )
                });
                assert_eq!(before, vec![("tuple(1)".into(), "tuple()".into(), 1)]);
                assert_eq!(
                    after,
                    vec![("tuple(1)".into(), "tuple()".into(), 1), ("tuple(2)".into(), "tuple()".into(), 1),]
                );
            }
            role => panic!("unknown role {role}"),
        }
        // Skip destructors: the published resolver conclusions must already be durable.
        std::process::exit(0);
    }
    let directory = Directory::new();
    run("resolved_cohort_recovers_without_reresolving_intents", &directory.0, "seed");
    run("resolved_cohort_recovers_without_reresolving_intents", &directory.0, "read");
}
