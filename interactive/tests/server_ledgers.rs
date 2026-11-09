//! Ledger state outlives its consuming arrangements and computations.
//! Every worker receives the same captured cuts; only worker zero injects payload rows.
//! Reads use explicit product-time points and committed frontiers, independently of `tick`.

use std::sync::Arc;

use corgi::Shape;
use interactive::ledger::{Change, Cohort, Frontier, Id, MemoryStore, Payload, Shape as LedgerShape, Store, Time, Writer};
use interactive::runtime::container::Updates;
use interactive::runtime::feed::FeedDecoder;
use interactive::server::{encode_ledger_batch, HostTime, LedgerSnapshot, Server};
use interactive::{lower, parse};

fn frontier(points: &[&[i64]]) -> Frontier {
    points.iter().map(|point| Time::new((*point).into())).collect()
}

fn point(coords: &[u64]) -> HostTime { HostTime::new(coords.into()) }

fn shape(width: usize) -> LedgerShape {
    LedgerShape {
        columns: Shape::Prod(vec![Shape::Prod(vec![Shape::Int]), Shape::Unit]),
        time_width: width,
    }
}

fn rows(text: &str) -> Updates {
    let mut decoder = FeedDecoder::new(&Shape::Prod(vec![Shape::Int]), &Shape::Unit);
    for line in text.lines() { decoder.push_line(line).unwrap(); }
    decoder.finish()
}

/// Creates and appends through a writer, one committed transaction at a time.
struct Ledgers { writer: Writer, next: u128 }

impl Ledgers {
    fn open(store: &Arc<MemoryStore>) -> Self {
        Self { writer: Writer::open(store.clone(), "test", 1).unwrap(), next: 1 }
    }
    fn commit(&mut self, id: Id, change: Change) {
        let outcomes = self.writer.transact_batch(&[(0, vec![(id, change)])]).unwrap();
        assert_eq!(outcomes[&0], Ok(()));
    }
    fn create(&mut self, shape: LedgerShape, start: Frontier) -> Id {
        let id = Id::from_u128(self.next);
        self.next += 1;
        self.commit(id, Change::Create { shape, start });
        id
    }
    fn append(&mut self, id: Id, lower: Frontier, upper: Frontier, batch: Payload) {
        self.commit(id, Change::Append { lower, upper, batch });
    }
}

fn cuts() -> (Arc<MemoryStore>, interactive::ledger::Id, Vec<LedgerSnapshot>) {
    let store = Arc::new(MemoryStore::default());
    let mut ledgers = Ledgers::open(&store);
    let start = frontier(&[&[0, 0]]);
    let middle = frontier(&[&[0, 1], &[1, 0]]);
    let later = frontier(&[&[1, 1]]);
    let last = frontier(&[&[2, 2]]);
    let id = ledgers.create(shape(2), start.clone());
    let mut cuts = vec![LedgerSnapshot::capture(store.as_ref(), "test", id).unwrap()];
    let bytes = encode_ledger_batch(&rows("1"), &[vec![0], vec![0]]).unwrap();
    let hash = store.put(&bytes).unwrap();
    ledgers.append(id, start, middle.clone(), Payload::Blob(hash));
    cuts.push(LedgerSnapshot::capture(store.as_ref(), "test", id).unwrap());
    ledgers.append(id, middle, later.clone(), Payload::Empty);
    cuts.push(LedgerSnapshot::capture(store.as_ref(), "test", id).unwrap());
    let bytes = encode_ledger_batch(&rows("2\n1 diff=-1"), &[vec![1, 1], vec![1, 2]]).unwrap();
    ledgers.append(id, later, last.clone(), Payload::Inline(bytes));
    cuts.push(LedgerSnapshot::capture(store.as_ref(), "test", id).unwrap());
    ledgers.append(id, last, Frontier::new(), Payload::Empty);
    cuts.push(LedgerSnapshot::capture(store.as_ref(), "test", id).unwrap());
    (store, id, cuts)
}

#[test]
fn committed_product_updates_and_empty_span_progress_reach_computations() {
    for peers in [1, 4] {
        let (store, id, cuts) = cuts();
        let shared = Arc::new(cuts);
        let worker_cuts = shared.clone();
        let results = timely::execute(timely::Config::process(peers), move |worker| {
            let cuts = &worker_cuts;
            let mut server = Server::new();
            server.publish_ledger(worker, "ledger_input", &cuts[0]).unwrap();
            assert!(server.program_info().is_empty(), "publishing a ledger must not install a fake program");
            assert_eq!(server.trace_width("ledger_input"), Some(2));
            let program = lower::lower_tree(parse::pipe::parse(r#"
                let data = import "ledger_input" : ((int), ());
                output "doubled" = data | map(((n), ()) -> (((n, 2) mul), ()));
            "#));
            server.install_with_root(worker, "query", &program, 2, point(&[0, 0])).unwrap();
            assert!(server.unpublish_ledger(worker, "ledger_input").is_err());
            assert!(server.install_with_root(worker, "ledger_input", &program, 2, point(&[0, 0])).is_err());

            server.refresh_ledger(worker, "ledger_input", &cuts[1]).unwrap();
            server.wait_for_frontier(worker, "query::doubled", cuts[1].frontier()).unwrap();
            assert!(server.snapshot_as_of(worker, "query::doubled", &point(&[0, 1])).is_err());
            let mut seen = vec![server.snapshot_as_of(worker, "query::doubled", &point(&[0, 0])).unwrap().rows()];

            // An empty span advances the source's antichain without introducing any row.
            server.refresh_ledger(worker, "ledger_input", &cuts[2]).unwrap();
            seen.push(server.snapshot_as_of(worker, "query::doubled", &point(&[0, 1])).unwrap().rows());
            server.refresh_ledger(worker, "ledger_input", &cuts[3]).unwrap();
            server.refresh_ledger(worker, "ledger_input", &cuts[3]).unwrap();
            seen.push(server.snapshot_as_of(worker, "query::doubled", &point(&[1, 1])).unwrap().rows());
            seen.push(server.snapshot_as_of(worker, "query::doubled", &point(&[1, 2])).unwrap().rows());
            assert!(server.refresh_ledger(worker, "ledger_input", &cuts[1]).is_err());

            server.refresh_ledger(worker, "ledger_input", &cuts[4]).unwrap();
            seen.push(server.snapshot_as_of(worker, "query::doubled", &point(&[100, 100])).unwrap().rows());
            server.drop_program(worker, "query").unwrap();
            assert!(server.program_info().is_empty());
            assert!(server.published("ledger_input").is_some());
            server.unpublish_ledger(worker, "ledger_input").unwrap();
            server.publish_ledger(worker, "ledger_input", &cuts[4]).unwrap();
            seen.push(server.snapshot_as_of(worker, "ledger_input", &point(&[1, 1])).unwrap().rows());
            server.unpublish_ledger(worker, "ledger_input").unwrap();
            seen
        }).unwrap().join();

        let expected = vec![
            vec![("tuple(2)".to_string(), "tuple()".to_string(), 1)],
            vec![("tuple(2)".to_string(), "tuple()".to_string(), 1)],
            vec![("tuple(2)".to_string(), "tuple()".to_string(), 1), ("tuple(4)".to_string(), "tuple()".to_string(), 1)],
            vec![("tuple(4)".to_string(), "tuple()".to_string(), 1)],
            vec![("tuple(4)".to_string(), "tuple()".to_string(), 1)],
            vec![("tuple(1)".to_string(), "tuple()".to_string(), 1), ("tuple(2)".to_string(), "tuple()".to_string(), 1)],
        ];
        for (worker, result) in results.into_iter().enumerate() {
            let rows = result.unwrap();
            if worker == 0 { assert_eq!(rows, expected); }
            else { assert!(rows.iter().all(Vec::is_empty), "only worker zero gathers results"); }
        }
        // Dropping every consuming arrangement did not change the committed ledger.
        assert!(Cohort::new(store.as_ref(), "test").snapshot().unwrap().registry.contains_key(&id));
    }
}

#[test]
fn invalid_payloads_and_signed_times_are_refused_before_source_installation() {
    let store = Arc::new(MemoryStore::default());
    let mut ledgers = Ledgers::open(&store);
    let negative = ledgers.create(shape(2), frontier(&[&[-1, 0]]));
    assert!(LedgerSnapshot::capture(store.as_ref(), "test", negative).is_err());
    let id = ledgers.create(shape(2), frontier(&[&[0, 0]]));
    let invalid = encode_ledger_batch(&rows("1"), &[vec![2], vec![2]]).unwrap();
    ledgers.append(id, frontier(&[&[0, 0]]), frontier(&[&[1, 1]]), Payload::Inline(invalid));
    assert!(LedgerSnapshot::capture(store.as_ref(), "test", id).is_err(), "raw update outside its span");

    let wrong_width = ledgers.create(shape(2), frontier(&[&[0, 0]]));
    let bytes = encode_ledger_batch(&rows("1"), &[vec![0]]).unwrap();
    ledgers.append(wrong_width, frontier(&[&[0, 0]]), frontier(&[&[1, 1]]), Payload::Inline(bytes));
    assert!(LedgerSnapshot::capture(store.as_ref(), "test", wrong_width).is_err());
}

#[test]
fn nonzero_start_bounds_reads_and_imports() {
    let store = Arc::new(MemoryStore::default());
    let mut ledgers = Ledgers::open(&store);
    let id = ledgers.create(shape(2), frontier(&[&[2, 3]]));
    let bytes = encode_ledger_batch(&rows("9"), &[vec![2], vec![3]]).unwrap();
    ledgers.append(id, frontier(&[&[2, 3]]), frontier(&[&[3, 4]]), Payload::Inline(bytes));
    let cut = LedgerSnapshot::capture(store.as_ref(), "test", id).unwrap();
    timely::execute_directly(move |worker| {
        let mut server = Server::new();
        server.publish_ledger(worker, "ledger_input", &cut).unwrap();
        let program = lower::lower_tree(parse::pipe::parse("output \"out\" = import \"ledger_input\";"));
        assert!(server.install_with_root(worker, "query", &program, 2, point(&[0, 0])).is_err());
        assert!(server.program_info().is_empty());
        assert!(server.snapshot_as_of(worker, "ledger_input", &point(&[1, 3])).is_err());
        server.install_with_root(worker, "query", &program, 2, point(&[2, 3])).unwrap();
        assert_eq!(server.snapshot_as_of(worker, "query::out", &point(&[2, 3])).unwrap().rows(), rows("9").rows());
        server.drop_program(worker, "query").unwrap();
        server.unpublish_ledger(worker, "ledger_input").unwrap();
    });
}
