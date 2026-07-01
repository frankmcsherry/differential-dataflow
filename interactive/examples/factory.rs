//! Rate-factory driver with a "DD touched this" overlay.
//!
//! A line of stages, each with a capacity. The program computes the steady-state
//! rate at each stage; we poke one capacity per tick and show (a) the new rates
//! and (b) exactly which stages DD recomputed -- the latter read straight off the
//! capture delta, which IS the set of records DD reworked this tick.
//!
//! The "buffer dial" is which program you run:
//!   examples/programs/rate_prefix.ddp  -- infinite buffers, no backpressure
//!                                          (a change propagates downstream only)
//!   examples/programs/rate_global.ddp  -- finite buffers / backpressure
//!                                          (the global bottleneck throttles all)
//!
//!   cargo run --release --example factory -- examples/programs/rate_prefix.ddp
//!   cargo run --release --example factory -- examples/programs/rate_global.ddp

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::mpsc::{channel, Receiver, TryRecvError};
use std::thread::sleep;
use std::time::Duration;

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::core::capture::{Capture, Event};
use differential_dataflow::input::Input;
use differential_dataflow::dynamic::pointstamp::PointStamp;

use interactive::{parse, lower};
use interactive::scope_ir as st;
use interactive::ir::{Diff, Value};
use interactive::backend::vec::{render_tree, Row};

fn first_int(v: &Value) -> i64 {
    match v { Value::Int(n) => *n, Value::Tuple(f) => first_int(&f[0]), other => panic!("no int in {:?}", other) }
}

/// Drain this tick's capture delta into `counts`; return the (stage,rate,diff)
/// records seen now (the footprint of DD's work this tick).
fn drain(rx: &Receiver<Event<u64, Vec<((Row, Row), u64, Diff)>>>,
         counts: &mut BTreeMap<(i64, i64), Diff>) -> Vec<(i64, i64, Diff)> {
    let mut seen = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(Event::Messages(_t, data)) => {
                for ((k, v), _t2, d) in data {
                    let (stage, rate) = (first_int(&k), first_int(&v));
                    *counts.entry((stage, rate)).or_insert(0) += d;
                    seen.push((stage, rate, d));
                }
            }
            Ok(Event::Progress(_)) => {}
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
    seen
}

fn rates_now(counts: &BTreeMap<(i64, i64), Diff>) -> BTreeMap<i64, i64> {
    let mut out = BTreeMap::new();
    for (&(stage, rate), &c) in counts { if c > 0 { out.insert(stage, rate); } }
    out
}

fn row_str(vals: &BTreeMap<i64, i64>, n: i64) -> String {
    (0..n).map(|s| format!("{:>3}", vals.get(&s).copied().unwrap_or(0))).collect::<Vec<_>>().join(" ")
}

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| "examples/programs/rate_prefix.ddp".to_string());
    let n: i64 = std::env::var("FACTORY_N").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let delay_ms: u64 = std::env::var("BELT_DELAY_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(900);

    let source = interactive::load_program(&path);
    let stmts = parse::pipe::parse(&source);
    let (n_inputs, _imports) = interactive::survey_sources(&stmts);
    let mut tree = lower::lower_tree(stmts);
    tree.optimize();
    let export_idx = tree.root.exports.iter().position(|e| e.name == "result").unwrap();

    // Initial capacities; stage 0 is the source rate. Bottleneck at stage 2.
    let mut caps: Vec<i64> = vec![9, 9, 3, 9, 9, 9, 9, 9];
    caps.resize(n as usize, 9);

    // Scripted pokes: (stage, new capacity, what it teaches).
    let script: Vec<(usize, i64, &str)> = vec![
        (5, 20, "raise a NON-bottleneck (stage 5): nothing binds -> expect no work"),
        (2,  7, "raise THE bottleneck (stage 2 -> 7): throughput climbs"),
        (2,  9, "raise it again (stage 2 -> 9): line now balanced at 9"),
        (6,  2, "DROP a DOWNSTREAM stage (stage 6 -> 2): watch who recomputes"),
        (6,  9, "fix the downstream stage (stage 6 -> 9)"),
    ];

    timely::execute_directly(move |worker| {
        let (tx, rx) = channel::<Event<u64, Vec<((Row, Row), u64, Diff)>>>();

        let (mut inputs, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..n_inputs {
                let (h, c) = scope.new_collection::<(Row, Row), Diff>();
                handles.push(h); collections.push(c);
            }
            let probe = ProbeHandle::new();
            let exports = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let root_imports: Vec<_> = tree.root.imports.iter().map(|imp| match &imp.from {
                    st::Source::Input(idx) => entered[*idx].clone(),
                    other => panic!("factory: unsupported source {:?}", other),
                }).collect();
                let rendered = render_tree(&tree.root, inner.clone(), 0, root_imports);
                rendered.into_iter().map(|c| c.leave(scope)).collect::<Vec<_>>()
            });
            let out = exports.into_iter().nth(export_idx).unwrap();
            let out = out.probe_with(&probe);
            out.inner.capture_into(tx);
            (handles, probe)
        });

        let cap_row = |s: i64, c: i64| (Value::Tuple(vec![Value::Int(s), Value::Int(c)]), Value::unit());
        let edge = |a: i64, b: i64| (Value::Tuple(vec![Value::Int(a), Value::Int(b)]), Value::unit());

        for s in 0..n { inputs[0].update(cap_row(s, caps[s as usize]), 1); }
        if n_inputs > 1 { for i in 0..n - 1 { inputs[1].update(edge(i, i + 1), 1); } }

        for h in inputs.iter_mut() { h.advance_to(1); h.flush(); }
        let mut steps = 0u64;
        while probe.less_than(&1u64) { worker.step(); steps += 1; }

        let mut counts: BTreeMap<(i64, i64), Diff> = BTreeMap::new();
        drain(&rx, &mut counts);
        let rates = rates_now(&counts);
        println!("== {} ==  (cold build: {} steps)", path, steps);
        println!("cap : {}", row_str(&caps.iter().enumerate().map(|(i, &c)| (i as i64, c)).collect(), n));
        println!("rate: {}    throughput = {}\n", row_str(&rates, n), rates.get(&(n - 1)).copied().unwrap_or(0));
        let _ = std::io::stdout().flush();
        sleep(Duration::from_millis(delay_ms));

        for (ti, (stage, newcap, desc)) in script.iter().enumerate() {
            let old = caps[*stage];
            caps[*stage] = *newcap;
            inputs[0].update(cap_row(*stage as i64, old), -1);
            inputs[0].update(cap_row(*stage as i64, *newcap), 1);

            let time = (ti as u64) + 2;
            for h in inputs.iter_mut() { h.advance_to(time); h.flush(); }
            steps = 0;
            while probe.less_than(&time) { worker.step(); steps += 1; }

            let seen = drain(&rx, &mut counts);
            let touched: BTreeSet<i64> = seen.iter().map(|&(s, _, _)| s).collect();
            let rates = rates_now(&counts);

            println!("t{}: {}", ti + 1, desc);
            println!("  poke: stage {} : {} -> {}", stage, old, newcap);
            println!("  cap : {}", row_str(&caps.iter().enumerate().map(|(i, &c)| (i as i64, c)).collect(), n));
            println!("  rate: {}    throughput = {}", row_str(&rates, n), rates.get(&(n - 1)).copied().unwrap_or(0));
            let mark: String = (0..n).map(|s| if touched.contains(&s) { " ^^" } else { "  ." }).collect::<Vec<_>>().join("");
            println!("  DD  :{}   {} stages recomputed, {} record-changes ({} steps)",
                     mark, touched.len(), seen.len(), steps);
            println!();
            let _ = std::io::stdout().flush();
            sleep(Duration::from_millis(delay_ms));
        }
    });
}
