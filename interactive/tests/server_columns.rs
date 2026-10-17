use interactive::ingest::{Column, ColumnBatch};
use interactive::ir::Value;
use interactive::server::{RenderBackend, Server};

fn batch(values: Vec<i64>, diffs: Vec<i64>) -> ColumnBatch {
    let n = values.len();
    ColumnBatch::new(
        Column::Prod(vec![Column::i64(values)]),
        Column::Unit(n),
        diffs,
    )
    .unwrap()
}

#[test]
fn native_input_checks_shapes_and_observes_epoch_completion() {
    for backend in [RenderBackend::Vec, RenderBackend::Corgi] {
        timely::execute_directly(move |worker| {
            let program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
                "let x = input 0 : ((int) ; ()); export \"x\" = x;",
            ));
            let mut server = Server::with_backend(backend);
            server.install(worker, "p", &program).unwrap();
            let bad = ColumnBatch::new(Column::i64(vec![99]), Column::Unit(1), vec![1]).unwrap();
            assert!(server.feed_columns("p", 0, bad).is_err());
            assert!(server
                .feed_columns("p", 1, batch(vec![99], vec![1]))
                .is_err());
            server
                .feed_columns("p", 0, batch(vec![1, 2, 1], vec![2, 1, -1]))
                .unwrap();
            assert!(server.snapshot(worker, "x").unwrap().is_empty());
            server.tick(worker);
            let row = |key| (Value::Tuple(vec![Value::Int(key)]), Value::unit(), 1);
            assert_eq!(server.snapshot(worker, "x").unwrap(), vec![row(1), row(2)]);
            server
                .feed_columns("p", 0, batch(vec![1, 2], vec![-1, -1]))
                .unwrap();
            server.tick(worker);
            assert!(server.snapshot(worker, "x").unwrap().is_empty());
        });
    }
}
