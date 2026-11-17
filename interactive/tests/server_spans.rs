//! Spans: a piloted program's inputs each take updates at their own times, and each moves alone,
//! with no `tick`. The program's outputs are complete before the least of the times its inputs
//! stand at, and are read, compacted and imported by that, not by the epoch.

use interactive::runtime::container::Updates;
use interactive::runtime::feed::FeedDecoder;
use interactive::server::{host_time, HostTime, Server};
use interactive::{lower, parse, scope_ir::Program};

fn program(text: &str) -> Program {
    lower::lower_tree(parse::pipe::parse(text))
}

/// Rows of shape `((int), (int))`, each `(key, val, time, diff)`, as a span: columns and times.
fn span(rows: &[(i64, i64, u64, i64)]) -> (Updates, Vec<u64>) {
    let shape = program("output \"x\" = input 0 : ((int), (int));").root.imports[0].shape.clone().unwrap();
    let mut decoder = FeedDecoder::new(&shape.0, &shape.1);
    for (key, val, _, diff) in rows {
        decoder.push(&key.to_string(), Some(&val.to_string()), *diff).unwrap();
    }
    (decoder.finish(), rows.iter().map(|row| row.2).collect())
}

/// Move one input, and then run until the outputs have caught up: `advance` and then `spin`.
fn advance(server: &mut Server, worker: &mut timely::worker::Worker, prog: &str, input: usize, time: &HostTime) -> Result<(), String> {
    server.advance(prog, input, time)?;
    server.spin(worker);
    Ok(())
}

/// The first coordinate of a time.
fn first(time: &HostTime) -> u64 {
    time.get(0).copied().unwrap_or(0)
}

/// A trace's complete contents, as `(key, val, diff)` of integers, sorted. Worker 0 has them.
fn contents(server: &mut Server, worker: &mut timely::worker::Worker, trace: &str) -> Vec<(i64, i64, i64)> {
    let number = |text: &str| text.trim_matches(|c: char| !c.is_ascii_digit() && c != '-').parse::<i64>().unwrap();
    let mut rows: Vec<_> = server.snapshot(worker, trace).unwrap().rows().into_iter().map(|(k, v, d)| (number(&k), number(&v), d)).collect();
    rows.sort();
    rows
}

fn spans_on(workers: usize) {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let w0 = worker.index() == 0;
        let mut server = Server::new();
        let two = program(r#"let a = input 0 : ((int), (int)); let b = input 1 : ((int), (int)); output "both" = a + b;"#);
        server.install_piloted(worker, "p", &two).unwrap();
        assert!(server.is_piloted("p"));

        // Each input takes a span: updates at their own times. A span enters at one worker.
        if w0 {
            let (columns, times) = span(&[(1, 10, 0, 1), (2, 20, 1, 1), (3, 30, 2, 1), (1, 10, 2, -1)]);
            server.feed_span("p", 0, columns, times).unwrap();
            let (columns, times) = span(&[(1, 100, 0, 1), (2, 200, 3, 1)]);
            server.feed_span("p", 1, columns, times).unwrap();
        }
        assert!(contents(&mut server, worker, "p::both").is_empty(), "nothing is complete at the start");

        // One input moves, and the other stays: the output is complete before the least of them.
        advance(&mut server, worker, "p", 0, &host_time(2)).unwrap();
        assert_eq!(first(server.input_frontier("p", 0).unwrap()), 2);
        assert_eq!(first(server.input_frontier("p", 1).unwrap()), 0);
        assert!(contents(&mut server, worker, "p::both").is_empty(), "input 1 is still at 0");
        advance(&mut server, worker, "p", 1, &host_time(1)).unwrap();
        if w0 { assert_eq!(contents(&mut server, worker, "p::both"), [(1, 10, 1), (1, 100, 1)], "time 0"); } else { contents(&mut server, worker, "p::both"); }

        // `tick` moves neither input, and does not wait for the output at the epoch.
        for _ in 0..5 { server.tick(worker); }
        assert_eq!(server.epoch(), 5);
        assert_eq!(first(server.input_frontier("p", 0).unwrap()), 2);
        assert_eq!(first(server.input_frontier("p", 1).unwrap()), 1);

        // The trace compacts to just before its frontier, not to the epoch.
        advance(&mut server, worker, "p", 1, &host_time(4)).unwrap();
        assert_eq!(server.trace_since("p::both").map(first), Some(1));
        if w0 { assert_eq!(contents(&mut server, worker, "p::both"), [(1, 10, 1), (1, 100, 1), (2, 20, 1)], "times 0 and 1"); } else { contents(&mut server, worker, "p::both"); }
        advance(&mut server, worker, "p", 0, &host_time(5)).unwrap();
        assert_eq!(server.trace_since("p::both").map(first), Some(3));
        if w0 { assert_eq!(contents(&mut server, worker, "p::both"), [(1, 100, 1), (2, 20, 1), (2, 200, 1), (3, 30, 1)], "times 0 to 3"); } else { contents(&mut server, worker, "p::both"); }

        // An input takes nothing before where it stands, and does not move back.
        let (columns, times) = span(&[(7, 70, 5, 1), (8, 80, 4, 1)]);
        let error = server.feed_span("p", 0, columns, times).unwrap_err();
        assert_eq!(error, "cannot feed at time 4: input \"p\"/0 stands at 5");
        let error = advance(&mut server, worker, "p", 0, &host_time(3)).unwrap_err();
        assert_eq!(error, "cannot advance input \"p\"/0 to 3: it stands at 5");

        // A program that `tick` moves is not advanced, and a piloted one is not bound at ticks.
        let one = program(r#"output "rows" = input 0 : ((int), (int));"#);
        server.install(worker, "ticked", &one).unwrap();
        assert!(!server.is_piloted("ticked"));
        assert!(advance(&mut server, worker, "ticked", 0, &host_time(9)).unwrap_err().contains("`tick` moves this program's inputs"));
        assert!(server.bind(worker, "p::both", "ticked", 0).unwrap_err().contains("piloted"));
        assert!(server.bind(worker, "ticked::rows", "p", 0).unwrap_err().contains("piloted"));

        // Everything a piloted input holds is taken back from its own output, at its frontier.
        server.install_piloted(worker, "x", &one).unwrap();
        if w0 {
            let (columns, times) = span(&[(1, 1, 0, 1), (2, 2, 0, 1), (3, 3, 6, 1), (4, 4, 6, 2)]);
            server.feed_span("x", 0, columns, times).unwrap();
        }
        advance(&mut server, worker, "x", 0, &host_time(7)).unwrap();
        if w0 { assert_eq!(contents(&mut server, worker, "x::rows").len(), 4); } else { contents(&mut server, worker, "x::rows"); }
        server.load(worker, "x", 0, "x::rows", -1).unwrap();
        advance(&mut server, worker, "x", 0, &host_time(8)).unwrap();
        assert!(contents(&mut server, worker, "x::rows").is_empty());

        // A program that imports a piloted trace is piloted, reads it from where it is complete,
        // and follows it: its output carries the producer's times.
        let tens = program(r#"output "tens" = import "p::both" | map((k, (v)) -> (k, ((v, 10) mul)));"#);
        let error = server.install_with_root(worker, "bad", &tens, 1, server.import_as_of()).unwrap_err();
        assert!(error.contains("must be installed piloted"), "{error}");
        server.install(worker, "c", &tens).unwrap();
        assert!(server.is_piloted("c"));
        advance(&mut server, worker, "p", 1, &host_time(5)).unwrap();
        if w0 { assert_eq!(contents(&mut server, worker, "c::tens"), [(1, 1000, 1), (2, 200, 1), (2, 2000, 1), (3, 300, 1)]); } else { contents(&mut server, worker, "c::tens"); }
        if w0 {
            let (columns, times) = span(&[(9, 90, 6, 1)]);
            server.feed_span("p", 0, columns, times).unwrap();
        }
        advance(&mut server, worker, "p", 0, &host_time(8)).unwrap();
        advance(&mut server, worker, "p", 1, &host_time(8)).unwrap();
        assert_eq!(server.trace_since("c::tens").map(first), Some(7));
        if w0 { assert_eq!(contents(&mut server, worker, "c::tens"), [(1, 1000, 1), (2, 200, 1), (2, 2000, 1), (3, 300, 1), (9, 900, 1)]); } else { contents(&mut server, worker, "c::tens"); }
    })
    .unwrap();
    for result in guards.join() { result.unwrap(); }
}

#[test]
fn spans_move_each_input_alone_on_one_worker() {
    spans_on(1);
}

#[test]
fn spans_move_each_input_alone_on_three_workers() {
    spans_on(3);
}
