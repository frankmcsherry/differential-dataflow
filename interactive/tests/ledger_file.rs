//! Check filesystem recovery in fresh processes and root CAS between processes.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use interactive::ledger::{Change, Cohort, FileStore, Frontier, Id, Payload, Shape, Store, Time};
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

#[test]
fn committed_cohort_recovers_after_process_exit() {
    if let Some(path) = std::env::var_os(DIRECTORY) {
        let store = FileStore::open(path).unwrap();
        let cohort = Cohort::new(&store, "cohort");
        let middle = frontier(&[&[0, 1], &[1, 0]]);
        match std::env::var(ROLE).unwrap().as_str() {
            "seed" => {
                let blob = store.put(&payload("1", &[vec![0], vec![0]])).unwrap();
                assert_eq!(
                    cohort.transact(&[
                        (
                            id(1),
                            Change::CreateAppend {
                                shape: shape(),
                                start: at(0, 0),
                                upper: middle,
                                batch: Payload::Blob(blob)
                            }
                        ),
                        (
                            id(2),
                            Change::Create {
                                shape: shape(),
                                start: at(4, 5)
                            }
                        ),
                    ]),
                    Ok(Ok(()))
                );
                // A valid proposal blob is not committed merely because it is durable.
                let mut proposal = cohort.snapshot().unwrap();
                proposal.registry.remove(&id(2));
                store.put(&bincode::serialize(&proposal).unwrap()).unwrap();
            }
            "resume" => {
                let before = cohort.snapshot().unwrap();
                assert_eq!(before.registry.len(), 2);
                assert_eq!(before.manifest.len(), 1);
                assert_eq!(before.write(id(1)), Some(&middle));
                assert_eq!(
                    cohort.append(id(1), at(0, 0), at(1, 1), Payload::Empty),
                    Ok(Err(middle.clone()))
                );
                assert_eq!(
                    cohort.append(id(1), middle, at(1, 1), Payload::Empty),
                    Ok(Ok(()))
                );
                let data = payload("2\n1 diff=-1", &[vec![1, 1], vec![1, 2]]);
                assert_eq!(
                    cohort.append(id(1), at(1, 1), at(2, 2), Payload::Inline(data)),
                    Ok(Ok(()))
                );
                assert_eq!(
                    cohort.append(id(1), at(2, 2), Frontier::new(), Payload::Empty),
                    Ok(Ok(()))
                );
                let root = store.read_root("cohort").unwrap();
                assert!(cohort
                    .transact(&[
                        (
                            id(0),
                            Change::Create {
                                shape: shape(),
                                start: at(0, 0)
                            }
                        ),
                        (
                            id(1),
                            Change::Append {
                                lower: at(0, 0),
                                upper: at(3, 3),
                                batch: Payload::Empty
                            }
                        ),
                    ])
                    .unwrap()
                    .is_err());
                assert_eq!(store.read_root("cohort").unwrap(), root);
                assert!(!cohort.snapshot().unwrap().registry.contains_key(&id(0)));
            }
            "read" => {
                let state = cohort.snapshot().unwrap();
                assert_eq!(state.registry.len(), 2);
                assert_eq!(state.manifest.len(), 4);
                assert_eq!(state.write(id(1)), Some(&Frontier::new()));
                assert_eq!(state.write(id(2)), Some(&at(4, 5)));
                assert_eq!(store.read_root("cohort").unwrap().version, 4);
                let cut = Arc::new(LedgerSnapshot::capture(&store, "cohort", id(1)).unwrap());
                let guards = timely::execute(timely::Config::process(2), move |worker| {
                    let mut server = Server::new();
                    server.publish_ledger(worker, "saved", &cut).unwrap();
                    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
                        "output \"rows\" = import \"saved\" : ((int), ());",
                    ));
                    server
                        .install_with_root(worker, "reader", &program, 2, HostTime::default())
                        .unwrap();
                    let at = |x, y| HostTime::new([x, y].into());
                    let before = server
                        .snapshot_as_of(worker, "reader::rows", &at(1, 1))
                        .unwrap()
                        .rows();
                    let after = server
                        .snapshot_as_of(worker, "reader::rows", &at(1, 2))
                        .unwrap()
                        .rows();
                    (before, after)
                })
                .unwrap();
                for (worker, result) in guards.join().into_iter().enumerate() {
                    let (before, after) = result.unwrap();
                    if worker == 0 {
                        assert_eq!(
                            before,
                            vec![
                                ("tuple(1)".into(), "tuple()".into(), 1),
                                ("tuple(2)".into(), "tuple()".into(), 1)
                            ]
                        );
                        assert_eq!(after, vec![("tuple(2)".into(), "tuple()".into(), 1)]);
                    } else {
                        assert!(before.is_empty() && after.is_empty());
                    }
                }
            }
            role => panic!("unknown role {role}"),
        }
        // Skip destructors: acknowledged state must already be on disk.
        std::process::exit(0);
    }
    let directory = Directory::new();
    run(
        "committed_cohort_recovers_after_process_exit",
        &directory.0,
        "seed",
    );
    run(
        "committed_cohort_recovers_after_process_exit",
        &directory.0,
        "resume",
    );
    run(
        "committed_cohort_recovers_after_process_exit",
        &directory.0,
        "read",
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
