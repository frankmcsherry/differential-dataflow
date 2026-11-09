//! Resident resolution, queued publication, and recovery through ordinary cohort reads.

use std::collections::BTreeMap;
use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, mpsc, Arc, Mutex};
use std::time::Duration;

use interactive::ledger::{BlobId, Change, Cohort, Error, Failure, Frontier, Id, MemoryStore, Payload, Rejection, Root, Shape, State, Store, StoreError, Time, Transaction, Writer};

type Outcomes = BTreeMap<i64, Result<(), Rejection>>;
type Cas = dyn Fn(&dyn Store, &str, u64, BlobId) -> Result<bool, StoreError> + Send + Sync;

fn id(number: u128) -> Id { Id::from_u128(number) }
fn frontier(points: &[&[i64]]) -> Frontier {
    points.iter().map(|point| Time::new((*point).into())).collect()
}
fn at(time: i64) -> Frontier { frontier(&[&[time]]) }
fn shape(width: usize) -> Shape { Shape { columns: corgi::Shape::Int, time_width: width } }
fn create(number: u128) -> (Id, Change) { (id(number), Change::Create { shape: shape(1), start: at(0) }) }
fn populated(number: u128, upper: i64) -> (Id, Change) {
    (id(number), Change::CreateAppend { shape: shape(1), start: at(0), upper: at(upper), batch: Payload::Empty })
}
fn append(number: u128, lower: i64, upper: i64) -> (Id, Change) {
    (id(number), Change::Append { lower: at(lower), upper: at(upper), batch: Payload::Empty })
}
fn success(key: i64) -> Outcomes { BTreeMap::from([(key, Ok(()))]) }

struct Hook {
    inner: Arc<dyn Store>,
    calls: AtomicUsize,
    cas: Box<Cas>,
}

impl Store for Hook {
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> { self.inner.put(bytes) }
    fn get(&self, id: BlobId) -> Result<Vec<u8>, StoreError> { self.inner.get(id) }
    fn read_root(&self, name: &str) -> Result<Root, StoreError> { self.inner.read_root(name) }
    fn cas_root(&self, name: &str, expected: u64, state: BlobId) -> Result<bool, StoreError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        (self.cas)(self.inner.as_ref(), name, expected, state)
    }
}

/// Hold the first Store CAS until the test releases it; later calls pass through.
fn gated(inner: Arc<dyn Store>, first: impl Fn(&dyn Store, &str, u64, BlobId) -> Result<bool, StoreError> + Send + Sync + 'static)
    -> (Arc<Hook>, mpsc::Receiver<()>, mpsc::Sender<()>)
{
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let waiting = AtomicBool::new(true);
    let hook = Hook { inner, calls: AtomicUsize::new(0), cas: Box::new(move |store, name, expected, state| {
        if waiting.swap(false, Ordering::SeqCst) {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv_timeout(Duration::from_secs(30))
                .map_err(|error| StoreError::Unavailable(error.to_string()))?;
            first(store, name, expected, state)
        } else {
            store.cas_root(name, expected, state)
        }
    }) };
    (Arc::new(hook), entered_rx, release_tx)
}

/// The serial rules' outcomes for one batch, applied in key order.
fn native(oracle: &mut State, transactions: &[Transaction]) -> Outcomes {
    let mut ordered = transactions.to_vec();
    ordered.sort_by_key(|(key, _)| *key);
    ordered.iter().map(|(key, groups)| (*key, oracle.transact(groups))).collect()
}

/// Commit one batch through a short-lived writer, as a competing process would.
fn commit(store: &Arc<MemoryStore>, transactions: &[Transaction]) -> Outcomes {
    Writer::open(store.clone(), "cohort", 1).unwrap().transact_batch(transactions).unwrap()
}

#[test]
fn repeated_keys_and_restart_replay_match_native_transactions() {
    let store = Arc::new(MemoryStore::default());
    let mut oracle = State::default();
    let cohort = Cohort::new(store.as_ref(), "cohort");
    let batches = [
        vec![(0, vec![create(1)])],
        vec![(1, vec![append(1, 0, 9)]), (0, vec![append(1, 0, 1), create(2)])],
        vec![(0, vec![append(1, 1, 2), append(2, 1, 2)]), (1, vec![append(1, 1, 3)])],
        vec![(0, vec![append(1, 3, 4), append(2, 0, 1)]), (1, vec![])],
    ];
    let mut writer = Writer::open(store.clone(), "cohort", 2).unwrap();
    for batch in batches {
        assert_eq!(writer.transact_batch(&batch).unwrap(), native(&mut oracle, &batch));
        assert_eq!(cohort.snapshot().unwrap(), oracle);
    }
    writer.flush().unwrap();
    drop(writer);
    assert_eq!(cohort.snapshot().unwrap(), oracle);
    // A restarted writer recovers the log and extends it.
    let mut restarted = Writer::open(store.clone(), "cohort", 1).unwrap();
    for batch in [vec![(0, vec![append(2, 1, 2)])], vec![(0, vec![append(2, 2, 3)])]] {
        assert_eq!(restarted.transact_batch(&batch).unwrap(), native(&mut oracle, &batch));
        assert_eq!(cohort.snapshot().unwrap(), oracle);
    }
}

#[test]
fn blocked_publication_gates_outcomes_and_groups_queued_batches() {
    let store = Arc::new(MemoryStore::default());
    let (hook, entered, release) = gated(store.clone(), |inner, name, expected, state| inner.cas_root(name, expected, state));
    let (queued_tx, queued_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let worker_hook = hook.clone();
    let worker = std::thread::spawn(move || {
        let mut writer = Writer::open(worker_hook, "cohort", 2).unwrap();
        let first = writer.submit(&[(0, vec![populated(1, 1)])]).unwrap();
        let second = writer.submit(&[(0, vec![append(1, 1, 2)])]).unwrap();
        let third = writer.submit(&[(0, vec![append(1, 2, 3)])]).unwrap();
        queued_tx.send(()).unwrap();
        result_tx.send(writer.wait(first)).unwrap();
        result_tx.send(writer.wait(second)).unwrap();
        result_tx.send(writer.wait(third)).unwrap();
        writer.flush().unwrap();
    });
    entered.recv_timeout(Duration::from_secs(30)).unwrap();
    queued_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(Cohort::new(store.as_ref(), "cohort").snapshot().unwrap().registry.is_empty());
    assert!(matches!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    release.send(()).unwrap();
    for _ in 0..3 {
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap(), success(0));
    }
    worker.join().unwrap();
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.read_root("cohort").unwrap().version, 2);
    let state = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
    assert_eq!(state.write(id(1)), Some(&at(3)));
    assert_eq!(state.manifest.len(), 3);
}

#[test]
fn lost_cas_reresolves_the_entire_dependent_suffix() {
    let store = Arc::new(MemoryStore::default());
    assert_eq!(commit(&store, &[(0, vec![create(1)])]), success(0));
    let competitor = store.clone();
    let (hook, entered, release) = gated(store.clone(), move |inner, name, expected, state| {
        assert_eq!(commit(&competitor, &[(0, vec![append(1, 0, 3)])]), success(0));
        inner.cas_root(name, expected, state)
    });
    let mut writer = Writer::open(hook.clone(), "cohort", 2).unwrap();
    let first = writer.submit(&[(0, vec![append(1, 0, 1)])]).unwrap();
    entered.recv_timeout(Duration::from_secs(30)).unwrap();
    // These reject against the speculative upper 1, but must succeed after the competing upper 3.
    let second = writer.submit(&[(0, vec![append(1, 3, 4)])]).unwrap();
    let third = writer.submit(&[(0, vec![append(1, 4, 5)])]).unwrap();
    release.send(()).unwrap();
    assert_eq!(writer.wait(third).unwrap(), success(0));
    assert_eq!(writer.wait(first).unwrap(), BTreeMap::from([(0, Err(Rejection { group: 0, reason: Failure::Write(at(3)) }))]));
    assert_eq!(writer.wait(second).unwrap(), success(0));
    writer.flush().unwrap();
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    let state = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
    assert_eq!(state.write(id(1)), Some(&at(5)));
    assert_eq!(state.manifest.len(), 3);
}

#[test]
fn uncertain_publication_is_not_retried_and_cancels_unsent_batches() {
    for published in [false, true] {
        let store = Arc::new(MemoryStore::default());
        let (hook, entered, release) = gated(store.clone(), move |inner, name, expected, state| {
            if published { assert!(inner.cas_root(name, expected, state)?); }
            Err(StoreError::CommitUnknown)
        });
        let mut writer = Writer::open(hook.clone(), "cohort", 1).unwrap();
        let first = writer.submit(&[(0, vec![populated(1, 1)])]).unwrap();
        entered.recv_timeout(Duration::from_secs(30)).unwrap();
        let second = writer.submit(&[(0, vec![append(1, 1, 2)])]).unwrap();
        let third = writer.submit(&[(0, vec![create(2)])]).unwrap();
        release.send(()).unwrap();
        assert_eq!(writer.wait(first), Err(Error::Store(StoreError::CommitUnknown)));
        assert!(matches!(writer.wait(second), Err(Error::Store(StoreError::Unavailable(_)))));
        assert!(matches!(writer.wait(third), Err(Error::Store(StoreError::Unavailable(_)))));
        assert!(writer.submit(&[(0, vec![create(3)])]).is_err());
        assert!(writer.flush().is_err());
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
        let state = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
        assert_eq!(state.registry.len(), usize::from(published));
        assert_eq!(state.manifest.len(), usize::from(published));
        assert!(!state.registry.contains_key(&id(2)));
    }
}

#[test]
fn width_growth_commits_before_rebuilding_the_resident_index() {
    let store = Arc::new(MemoryStore::default());
    let (hook, entered, release) = gated(store.clone(), |inner, name, expected, state| inner.cas_root(name, expected, state));
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut writer = Writer::open(hook, "cohort", 2).unwrap();
        let narrow = writer.submit(&[(0, vec![populated(1, 1)])]).unwrap();
        let start = frontier(&[&[0, 0, 0]]);
        let middle = frontier(&[&[0, 0, 1], &[0, 1, 0], &[1, 0, 0]]);
        attempt_tx.send(()).unwrap();
        let wide = writer.submit(&[(0, vec![(id(2), Change::CreateAppend {
            shape: shape(3), start, upper: middle.clone(), batch: Payload::Empty,
        })])]).unwrap();
        result_tx.send(writer.wait(wide)).unwrap();
        result_tx.send(writer.wait(narrow)).unwrap();
        result_tx.send(writer.transact_batch(&[(0, vec![
            append(1, 1, 2),
            (id(2), Change::Append { lower: middle, upper: frontier(&[&[1, 1, 1]]), batch: Payload::Empty }),
        ])])).unwrap();
    });
    entered.recv_timeout(Duration::from_secs(30)).unwrap();
    attempt_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(matches!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    release.send(()).unwrap();
    for _ in 0..3 {
        assert_eq!(result_rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap(), success(0));
    }
    worker.join().unwrap();
    let state = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
    assert_eq!(state.registry[&id(1)].shape.time_width, 1);
    assert_eq!(state.registry[&id(2)].shape.time_width, 3);
    assert_eq!(state.write(id(1)), Some(&at(2)));
    assert_eq!(state.write(id(2)), Some(&frontier(&[&[1, 1, 1]])));
    assert_eq!(state.manifest.len(), 4);
}

#[cfg(unix)]
#[test]
fn writer_log_recovers_after_process_exit() {
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use interactive::ledger::FileStore;
    use interactive::runtime::feed::FeedDecoder;
    use interactive::server::{encode_ledger_batch, HostTime, LedgerSnapshot, Server};

    const DIRECTORY: &str = "DDIR_WRITER_TEST_DIRECTORY";
    const ROLE: &str = "DDIR_WRITER_TEST_ROLE";

    fn point(time: i64) -> Frontier { frontier(&[&[time, time]]) }
    fn payload(rows: &str, time: i64) -> Vec<u8> {
        let mut decoder = FeedDecoder::new(&corgi::Shape::Prod(vec![corgi::Shape::Int]), &corgi::Shape::Unit);
        for row in rows.lines() { decoder.push_line(row).unwrap(); }
        let updates = decoder.finish();
        encode_ledger_batch(&updates, &[vec![time; updates.len()], vec![time; updates.len()]]).unwrap()
    }
    fn run(directory: &Path, role: &str) {
        let result = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "writer_log_recovers_after_process_exit", "--nocapture"])
            .env(DIRECTORY, directory).env(ROLE, role).output().unwrap();
        assert!(result.status.success(), "{role}: {}\n{}", String::from_utf8_lossy(&result.stdout), String::from_utf8_lossy(&result.stderr));
    }
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
    }

    if let Some(directory) = std::env::var_os(DIRECTORY) {
        let store = Arc::new(FileStore::open(directory).unwrap());
        let middle = frontier(&[&[0, 1], &[1, 0]]);
        match std::env::var(ROLE).unwrap().as_str() {
            "seed" => {
                let batch = Payload::Blob(store.put(&payload("7", 0)).unwrap());
                let columns = corgi::Shape::Prod(vec![corgi::Shape::Prod(vec![corgi::Shape::Int]), corgi::Shape::Unit]);
                let (hook, entered, release) = gated(store.clone(), |inner, name, expected, state| inner.cas_root(name, expected, state));
                let mut writer = Writer::open(hook.clone(), "cohort", 2).unwrap();
                let first = writer.submit(&[(0, vec![(id(1), Change::CreateAppend {
                    shape: Shape { columns, time_width: 2 }, start: point(0), upper: middle.clone(), batch,
                })])]).unwrap();
                entered.recv_timeout(Duration::from_secs(30)).unwrap();
                let progress = writer.submit(&[(0, vec![(id(1), Change::Append {
                    lower: middle, upper: point(1), batch: Payload::Empty,
                })])]).unwrap();
                let last = writer.submit(&[(0, vec![(id(1), Change::Append {
                    lower: point(1), upper: point(2), batch: Payload::Inline(payload("9\n7 diff=-1", 1)),
                })])]).unwrap();
                release.send(()).unwrap();
                assert_eq!(writer.wait(last).unwrap(), success(0));
                assert_eq!(writer.wait(first).unwrap(), success(0));
                assert_eq!(writer.wait(progress).unwrap(), success(0));
                assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
                assert_eq!(store.read_root("cohort").unwrap().version, 2);
            }
            "read" | "verify" => {
                // Recovery and DDIR replay in this fresh process construct no Writer or Resolver.
                let resumed = std::env::var(ROLE).unwrap() == "verify";
                let state = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
                let mut bounds = vec![(point(0), middle.clone()), (middle, point(1)), (point(1), point(2))];
                if resumed { bounds.push((point(2), point(3))); }
                assert_eq!(state.registry.len(), 1);
                assert_eq!(state.write(id(1)), Some(&point(if resumed { 3 } else { 2 })));
                assert_eq!(state.manifest.iter().map(|span| (span.lower.clone(), span.upper.clone())).collect::<Vec<_>>(), bounds);
                assert!(state.manifest.iter().all(|span| span.id == id(1) && span.since == point(0)));
                assert_eq!(store.read_root("cohort").unwrap().version, if resumed { 3 } else { 2 });
                let cut = Arc::new(LedgerSnapshot::capture(store.as_ref(), "cohort", id(1)).unwrap());
                let guards = timely::execute(timely::Config::process(2), move |worker| {
                    let mut server = Server::new();
                    server.publish_ledger(worker, "saved", &cut).unwrap();
                    let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
                        "output \"rows\" = import \"saved\" : ((int), ());",
                    ));
                    server.install_with_root(worker, "reader", &program, 2, HostTime::default()).unwrap();
                    let before = server.snapshot_as_of(worker, "reader::rows", &HostTime::new([0, 0].into())).unwrap().rows();
                    let after = server.snapshot_as_of(worker, "reader::rows", &HostTime::new([1, 1].into())).unwrap().rows();
                    (before, after)
                }).unwrap();
                for (worker, result) in guards.join().into_iter().enumerate() {
                    let (before, after) = result.unwrap();
                    if worker == 0 {
                        assert_eq!(before, vec![("tuple(7)".into(), "tuple()".into(), 1)]);
                        assert_eq!(after, vec![("tuple(9)".into(), "tuple()".into(), 1)]);
                    } else {
                        assert!(before.is_empty() && after.is_empty());
                    }
                }
            }
            "resume" => {
                let mut writer = Writer::open(store.clone(), "cohort", 2).unwrap();
                assert_eq!(writer.transact_batch(&[(0, vec![(id(1), Change::Append {
                    lower: point(2), upper: point(3), batch: Payload::Empty,
                })])]).unwrap(), success(0));
            }
            role => panic!("unknown role {role}"),
        }
        // Successful acknowledgments must suffice even when no destructors run.
        std::process::exit(0);
    }
    let directory = Directory(std::env::temp_dir().join(format!("ddir-writer-process-{}", Id::new_v4())));
    for role in ["seed", "read", "resume", "verify"] { run(&directory.0, role); }
}

/// Count CAS calls without changing them.
fn counted(inner: Arc<dyn Store>) -> Arc<Hook> {
    Arc::new(Hook { inner, calls: AtomicUsize::new(0), cas: Box::new(|store, name, expected, state| store.cas_root(name, expected, state)) })
}

#[test]
fn one_publication_commits_the_accepted_transactions() {
    let store = Arc::new(MemoryStore::default());
    let hook = counted(store.clone());
    let mut writer = Writer::open(hook.clone(), "cohort", 2).unwrap();
    let outcomes = writer.transact_batch(&[
        (30, vec![(id(1), Change::Append { lower: at(1), upper: at(2), batch: Payload::Inline(vec![2]) })]),
        (10, vec![(id(1), Change::CreateAppend { shape: shape(1), start: at(0), upper: at(1), batch: Payload::Inline(vec![1]) })]),
        (40, vec![populated(2, 3)]),
        (20, vec![create(0), append(1, 0, 9)]),
    ]).unwrap();
    assert_eq!(outcomes, BTreeMap::from([
        (10, Ok(())),
        (20, Err(Rejection { group: 1, reason: Failure::Write(at(1)) })),
        (30, Ok(())),
        (40, Ok(())),
    ]));
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    let state = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
    assert_eq!(state.registry.len(), 2);
    assert!(!state.registry.contains_key(&id(0)));
    assert_eq!(state.manifest.len(), 3);
    assert_eq!(state.write(id(1)), Some(&at(2)));
    assert_eq!(state.write(id(2)), Some(&at(3)));
}

#[test]
fn only_accepted_blob_references_must_exist_before_publication() {
    let store = Arc::new(MemoryStore::default());
    let hook = counted(store.clone());
    let missing = Payload::Blob([0; 32]);
    let missing_upper = |lower, upper| (id(1), Change::Append { lower: at(lower), upper: at(upper), batch: missing.clone() });
    let mut writer = Writer::open(hook.clone(), "cohort", 2).unwrap();
    assert_eq!(
        writer.submit(&[(10, vec![(id(1), Change::CreateAppend { shape: shape(1), start: at(0), upper: at(1), batch: missing.clone() })]), (20, vec![create(2)])]).err(),
        Some(Error::Store(StoreError::Missing([0; 32])))
    );
    // A failed submission stops the writer; nothing was published.
    assert!(writer.submit(&[(0, vec![create(3)])]).is_err());
    drop(writer);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.read_root("cohort").unwrap(), Root::default());
    // A rejected append may name a missing blob.
    let mut writer = Writer::open(hook.clone(), "cohort", 2).unwrap();
    assert_eq!(
        writer.transact_batch(&[(10, vec![populated(1, 1)]), (20, vec![missing_upper(0, 2)])]).unwrap(),
        BTreeMap::from([(10, Ok(())), (20, Err(Rejection { group: 0, reason: Failure::Write(at(1)) }))])
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_batches_publish_nothing_and_duplicate_keys_are_refused() {
    let store = Arc::new(MemoryStore::default());
    let hook = counted(store.clone());
    let mut writer = Writer::open(hook.clone(), "cohort", 1).unwrap();
    assert_eq!(writer.transact_batch(&[]), Ok(BTreeMap::new()));
    assert!(matches!(writer.transact_batch(&[(1, vec![]), (1, vec![])]), Err(Error::Invalid(_))));
    assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.read_root("cohort").unwrap(), Root::default());
}

#[test]
fn declared_widths_survive_padding_and_aborted_creations() {
    let store = Arc::new(MemoryStore::default());
    let mut oracle = State::default();
    let point = |coordinates: &[i64]| -> Frontier { [Time::new(coordinates.into())].into_iter().collect() };
    let high_start = point(&[0, i64::MIN, i64::MIN]);
    let high_upper = [Time::new(vec![0, 1, 0].into()), Time::new(vec![1, 0, 0].into())].into_iter().collect();
    let transactions = vec![
        (0, vec![create(1), (id(2), Change::Create { shape: shape(3), start: high_start.clone() })]),
        (10, vec![(id(0), Change::Create { shape: shape(5), start: point(&[0, 0, 0, 0, 0]) }), append(1, 1, 2)]),
        (20, vec![(id(0), Change::Create { shape: shape(2), start: point(&[0, 0]) })]),
        (30, vec![append(1, 0, 1), (id(2), Change::Append { lower: high_start, upper: high_upper, batch: Payload::Empty })]),
    ];
    let mut writer = Writer::open(store.clone(), "cohort", 2).unwrap();
    assert_eq!(writer.transact_batch(&transactions).unwrap(), native(&mut oracle, &transactions));
    let actual = Cohort::new(store.as_ref(), "cohort").snapshot().unwrap();
    assert_eq!(actual, oracle);
    assert_eq!(actual.registry[&id(0)].shape.time_width, 2);
    assert_eq!(actual.registry[&id(2)].shape.time_width, 3);
}
