//! Q6 on prebuilt native columns; also writes/loads a local column batch stream.
//!
//! q6_native_bench <four-integer-lineitem.txt> <q6.ddp> [rounds=5] [output.col]
//! Input projection: shipdate (YYYYMMDD), quantity * 100, price * 100, discount * 100.
//! Parsing, building columns, file writing, plan install and answer retrieval are untimed.
//! The timer includes native feed and timestamp construction, or file decoding plus feed,
//! followed by settlement. Native key/value/difference buffers are prepared outside the timer.
//! Text and column files use the same process/allocator. This is a single-worker, one-query control.
use std::io::{BufRead, Write};
use std::time::Instant;

use interactive::ingest::{Column, ColumnBatch};
use interactive::ir::Value;
use interactive::server::{RenderBackend, Server};

fn main() {
    let mut args = std::env::args().skip(1);
    let data = args.next().expect("projected lineitem integer file");
    let plan = args.next().expect("Q6 DDP plan");
    let rounds: usize = args.next().map(|x| x.parse().unwrap()).unwrap_or(5);
    assert!(rounds > 0);
    let output = args.next();
    let mut batches = Vec::new();
    let mut columns = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    let mut revenue = 0;
    let mut rows = 0;
    let mut flush = |columns: &mut [Vec<i64>; 4]| {
        let n = columns[0].len();
        if n != 0 {
            let keys = Column::Prod(
                columns
                    .iter_mut()
                    .map(|c| Column::i64(std::mem::take(c)))
                    .collect(),
            );
            batches.push(ColumnBatch::new(keys, Column::Unit(n), vec![1; n]).unwrap());
        }
    };
    for line in std::io::BufReader::new(std::fs::File::open(&data).unwrap()).lines() {
        let line = line.unwrap();
        let fields: Vec<i64> = line
            .split_whitespace()
            .map(|x| x.parse().unwrap())
            .collect();
        let [ship, qty, price, discount] = fields[..] else {
            panic!("expected four integer columns")
        };
        for (c, v) in columns.iter_mut().zip([ship, qty, price, discount]) {
            c.push(v);
        }
        if (19940101..19950101).contains(&ship) && qty < 2400 && (5..=7).contains(&discount) {
            revenue += price * discount;
        }
        rows += 1;
        if columns[0].len() == 1 << 16 {
            flush(&mut columns);
        }
    }
    flush(&mut columns);
    assert!(rows > 0 && revenue > 0);
    let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(
        &std::fs::read_to_string(plan).unwrap(),
    ));
    program.optimize();
    if let Some(path) = &output {
        let mut file = std::io::BufWriter::new(
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .unwrap(),
        );
        for batch in &batches {
            batch.write_to(&mut file).unwrap();
        }
        file.flush().unwrap();
    }
    timely::execute_directly(move |worker| {
        println!(
            "rows={rows},revenue_1e4={revenue},batches={}",
            batches.len()
        );
        for source in ["native_columns", "column_file", "text_file"] {
            if source == "column_file" && output.is_none() {
                continue;
            }
            let mut samples = Vec::new();
            let mut server = Server::with_backend(RenderBackend::Corgi);
            for round in 0..=rounds {
                server.install(worker, "q", &program).unwrap();
                // Copy the difference vectors outside the timer; key/value leaf buffers are shared.
                let input = if source == "native_columns" {
                    batches.clone()
                } else {
                    Vec::new()
                };
                let start = Instant::now();
                if source == "column_file" {
                    assert_eq!(
                        server
                            .load_columns(worker, "q", 0, output.as_ref().unwrap(), 16 << 20)
                            .unwrap(),
                        rows
                    );
                } else if source == "text_file" {
                    assert_eq!(server.load(worker, "q", 0, &data).unwrap(), rows);
                } else {
                    for batch in input {
                        server.feed_columns("q", 0, batch).unwrap();
                    }
                }
                server.tick(worker);
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                let result = server.snapshot(worker, "answer").unwrap();
                assert_eq!(
                    result,
                    vec![(Value::unit(), Value::Tuple(vec![Value::Int(revenue)]), 1)]
                );
                if round != 0 {
                    samples.push(elapsed);
                }
                server.drop_program(worker, "q").unwrap();
            }
            println!("{source},samples_ms={samples:?}");
            samples.sort_by(f64::total_cmp);
            println!("median_ms={:.3}", samples[rounds / 2]);
        }
    });
}
