//! Belt driver: host-ticked belt CA over a `.ddp` program (output fed back as
//! the next tick's occupancy input). Front half mirrors `ddir_vec` (parse ->
//! lower -> optimize -> render); we capture the `result` export back into the
//! host and feed it into the occupancy input, driving the simulation.
//!
//! Topology via BELT_TOPO=line|merge. `merge` runs two feeders A and B into a
//! shared junction J, then a trunk to the drain; the program's per-target `min`
//! arbitrates the two feeders competing for J. Use the local-TASEP `belt.ddp`
//! for merge (the rigid-scan programs assume single-successor lockstep).
//!
//! Columns: `work` = moves applied (= input updates pushed = DD work).
//! `steps` = worker.step() calls to quiesce this tick (proxy for iteration depth).
//!
//! Env: BELT_TOPO, BELT_N (cells per segment), BELT_FILL=full|source,
//! BELT_CLOSE, BELT_OPEN, BELT_TICKS, BELT_DELAY_MS. Program is argv[1].
//!
//!   BELT_TOPO=merge BELT_N=14 BELT_FILL=full BELT_CLOSE=999 BELT_TICKS=40 \
//!     BELT_DELAY_MS=150 cargo run --release --example belt -- examples/programs/belt.ddp

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

fn env_i64(k: &str, d: i64) -> i64 { std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d) }

fn as_int(v: &Value) -> i64 {
    match v { Value::Int(n) => *n, other => panic!("expected Int, got {:?}", other) }
}
fn key_pair(k: &Value) -> (i64, i64) {
    match k { Value::Tuple(f) => (as_int(&f[0]), as_int(&f[1])), other => panic!("bad key {:?}", other) }
}

/// A belt layout: directed succ edges, cells kept fed each tick, the drain cell,
/// the gate edge that the demo opens/closes, the real cells, and labelled rows
/// for rendering.
struct Topo {
    edges: Vec<(i64, i64)>,
    sources: Vec<i64>,
    drain: i64,
    gate: (i64, i64),
    cells: Vec<i64>,
    rows: Vec<(String, Vec<i64>)>,
}

fn build_line(n: i64) -> Topo {
    let edges = (0..n).map(|i| (i, i + 1)).collect();
    Topo { edges, sources: vec![0], drain: n, gate: (n - 1, n),
           cells: (0..n).collect(), rows: vec![("belt".into(), (0..n).collect())] }
}

/// Two feeders A=[0,n), B=[n,2n) into junction J=2n, then trunk [2n,3n) to drain=3n.
fn build_merge(n: i64) -> Topo {
    let j = 2 * n;
    let drain = 3 * n;
    let mut edges = Vec::new();
    for i in 0..n - 1 { edges.push((i, i + 1)); }           // feeder A internal
    edges.push((n - 1, j));                                  // A -> J
    for i in n..2 * n - 1 { edges.push((i, i + 1)); }        // feeder B internal
    edges.push((2 * n - 1, j));                              // B -> J
    for i in j..drain { edges.push((i, i + 1)); }            // J -> trunk -> drain
    Topo {
        edges, sources: vec![0, n], drain, gate: (drain - 1, drain),
        cells: (0..drain).collect(),
        rows: vec![
            ("A    ".into(), (0..n).collect()),
            ("B    ".into(), (n..2 * n).collect()),
            ("trunk".into(), (j..drain).collect()),
        ],
    }
}

fn current_moves(rx: &Receiver<Event<u64, Vec<((Row, Row), u64, Diff)>>>,
                 counts: &mut BTreeMap<(i64, i64), Diff>) -> Vec<(i64, i64)> {
    loop {
        match rx.try_recv() {
            Ok(Event::Messages(_t, data)) => {
                for ((k, _v), _t2, d) in data { *counts.entry(key_pair(&k)).or_insert(0) += d; }
            }
            Ok(Event::Progress(_)) => {}
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
    counts.iter().filter(|(_, &c)| c > 0).map(|(&k, _)| k).collect()
}

fn render_row(cells: &[i64], occ: &BTreeSet<i64>) -> String {
    let w = (cells.len() as i64).min(100);
    (0..w).map(|col| {
        let cell = cells[(col * cells.len() as i64 / w) as usize];
        if occ.contains(&cell) { '#' } else { '.' }
    }).collect()
}

fn print_belt(tick: usize, occ: &BTreeSet<i64>, topo: &Topo, work: usize, steps: u64, gate_open: bool) {
    let gate = if gate_open { "open" } else { "SHUT" };
    let flag = if work == 0 { "  (idle)" } else { "" };
    let body: Vec<String> = topo.rows.iter()
        .map(|(label, cells)| format!("{}:|{}|", label, render_row(cells, occ)))
        .collect();
    println!("t{:>3} g:{} w:{:>3} s:{:>3}  {}{}", tick, gate, work, steps, body.join("  "), flag);
    let _ = std::io::stdout().flush();
}

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| "examples/programs/belt.ddp".to_string());
    let n = env_i64("BELT_N", 28);
    let topo = match std::env::var("BELT_TOPO").as_deref() {
        Ok("merge") => build_merge(n),
        _ => build_line(n),
    };
    let fill_full = std::env::var("BELT_FILL").map(|s| s == "full").unwrap_or(false);
    let close_at = env_i64("BELT_CLOSE", 24) as usize;
    let open_at = env_i64("BELT_OPEN", 70) as usize;
    let ticks = env_i64("BELT_TICKS", 110) as usize;
    let delay_ms = env_i64("BELT_DELAY_MS", 180) as u64;

    let source = interactive::load_program(&path);
    let stmts = parse::pipe::parse(&source);
    let mut tree = lower::lower_tree(stmts);
    tree.optimize();
    let export_idx = tree.root.exports.iter().position(|e| e.name == "result")
        .expect("program must export \"result\"");

    timely::execute_directly(move |worker| {
        let (tx, rx) = channel::<Event<u64, Vec<((Row, Row), u64, Diff)>>>();

        let (mut succ_in, mut occ_in, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (succ_h, succ_c) = scope.new_collection::<(Row, Row), Diff>();
            let (occ_h, occ_c)   = scope.new_collection::<(Row, Row), Diff>();
            let collections = vec![succ_c, occ_c]; // 0 = topology, 1 = occupancy
            let probe = ProbeHandle::new();
            let exports = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let root_imports: Vec<_> = tree.root.imports.iter().map(|imp| match &imp.from {
                    st::Source::Input(idx) => entered[*idx].clone(),
                    other => panic!("belt: unsupported source {:?}", other),
                }).collect();
                let rendered = render_tree(&tree.root, inner.clone(), 0, root_imports);
                rendered.into_iter().map(|c| c.leave(scope)).collect::<Vec<_>>()
            });
            let out = exports.into_iter().nth(export_idx).unwrap();
            let out = out.probe_with(&probe);
            out.inner.capture_into(tx);
            (succ_h, occ_h, probe)
        });

        let cell = |c: i64| (Value::Tuple(vec![Value::Int(c)]), Value::unit());
        let edge = |a: i64, b: i64| (Value::Tuple(vec![Value::Int(a), Value::Int(b)]), Value::unit());

        for &(a, b) in &topo.edges { succ_in.update(edge(a, b), 1); }

        let mut occ: BTreeSet<i64> = BTreeSet::new();
        if fill_full { occ.extend(topo.cells.iter().copied()); }
        else { occ.extend(topo.sources.iter().copied()); }
        for &c in &occ { occ_in.update(cell(c), 1); }

        succ_in.advance_to(1); succ_in.flush();
        occ_in.advance_to(1); occ_in.flush();
        let mut steps = 0u64;
        while probe.less_than(&1u64) { worker.step(); steps += 1; }

        let mut counts: BTreeMap<(i64, i64), Diff> = BTreeMap::new();
        let mut moves = current_moves(&rx, &mut counts);
        let mut gate_open = true;

        for tick in 0..ticks {
            if tick == close_at && gate_open { succ_in.update(edge(topo.gate.0, topo.gate.1), -1); gate_open = false; }
            if tick == open_at && !gate_open { succ_in.update(edge(topo.gate.0, topo.gate.1), 1); gate_open = true; }

            print_belt(tick, &occ, &topo, moves.len(), steps, gate_open);
            sleep(Duration::from_millis(delay_ms));

            // Set-based update: sources and targets may overlap (lockstep / runs).
            let sources: BTreeSet<i64> = moves.iter().map(|&(s, _)| s).collect();
            let targets: BTreeSet<i64> = moves.iter().filter(|&&(_, t)| t != topo.drain).map(|&(_, t)| t).collect();
            let mut next: BTreeSet<i64> = occ.difference(&sources).copied().collect();
            next.extend(targets);
            next.extend(topo.sources.iter().copied()); // keep feeders fed
            for &c in next.difference(&occ) { occ_in.update(cell(c), 1); }
            for &c in occ.difference(&next) { occ_in.update(cell(c), -1); }
            occ = next;

            let time = (tick as u64) + 2;
            succ_in.advance_to(time); succ_in.flush();
            occ_in.advance_to(time); occ_in.flush();
            steps = 0;
            while probe.less_than(&time) { worker.step(); steps += 1; }

            moves = current_moves(&rx, &mut counts);
        }
    });
}
