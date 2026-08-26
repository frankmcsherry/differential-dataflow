//! Perpetuum: a puzzle game about bringing a differential dataflow to fixed point.
//!
//! The machine is a belt cellular automaton (`belt.ddp`, rule-184/TASEP) whose
//! expressions are sealed: the player can only edit its input data. Levels start
//! in perpetual motion — a bubble circulating a ring forever, DD dutifully
//! processing its diffs every tick. The goal is to reach a configuration where
//! the machine produces *zero* updates: a genuine DD fixed point, at which the
//! dataflow does no work and may exit. The win condition is not judged, it is
//! measured — you win when the system goes silent.
//!
//! Non-triviality: items are conserved (no create/destroy, only `mv`), levels
//! have no drains, so the empty fixed point is unreachable. Stability for
//! rule-184 means the occupied set is successor-closed: cycles fill completely
//! or drain completely, which turns each level into arithmetic on cycle sizes.
//!
//! Score: `energy` counts every input diff the machine processes — CA moves and
//! your edits alike. Par is the energy of a known clean solution; extra churn
//! (running the machine while mis-configured, oversized edits) costs energy.
//!
//! Run:  cargo run --release --example perpetuum -- examples/perpetuum/levels/01-two-rings.lvl
//! Cmds: mv A B | cut A B | link A B | t [n] | r | help | q

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Write};
use std::sync::mpsc::{channel, Receiver, TryRecvError};

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::input::Input;
use timely::dataflow::operators::core::capture::{Capture, Event};
use timely::dataflow::ProbeHandle;

use interactive::backend::vec::{render_tree, Row};
use interactive::ir::{Diff, Value};
use interactive::scope_ir as st;
use interactive::{lower, parse};

fn as_int(v: &Value) -> i64 {
    match v {
        Value::Int(n) => *n,
        other => panic!("expected Int, got {:?}", other),
    }
}
fn key_pair(k: &Value) -> (i64, i64) {
    match k {
        Value::Tuple(f) => (as_int(&f[0]), as_int(&f[1])),
        other => panic!("bad key {:?}", other),
    }
}

/// A parsed level: topology, initial items, welded cells, tool budgets, par,
/// and labelled rows for rendering.
struct Level {
    name: String,
    blurb: Vec<String>,
    edges: BTreeSet<(i64, i64)>,
    items: BTreeSet<i64>,
    welds: BTreeSet<i64>,
    mv: usize,
    cut: usize,
    link: usize,
    par: usize,
    rows: Vec<(String, Vec<i64>)>,
}

fn load_level(path: &str) -> Level {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("cannot read {}: {}", path, e));
    let mut lvl = Level {
        name: path.to_string(),
        blurb: Vec::new(),
        edges: BTreeSet::new(),
        items: BTreeSet::new(),
        welds: BTreeSet::new(),
        mv: 0,
        cut: 0,
        link: 0,
        par: 0,
        rows: Vec::new(),
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut it = line.split_whitespace();
        let word = it.next().unwrap();
        let ints = |it: std::str::SplitWhitespace| -> Vec<i64> {
            it.map(|t| t.parse().unwrap_or_else(|_| panic!("bad number {:?} in {:?}", t, line)))
                .collect()
        };
        match word {
            "name" => lvl.name = line["name".len()..].trim().to_string(),
            "blurb" => lvl.blurb.push(line["blurb".len()..].trim().to_string()),
            "cycle" => {
                let cs = ints(it);
                for w in cs.windows(2) {
                    lvl.edges.insert((w[0], w[1]));
                }
                lvl.edges.insert((cs[cs.len() - 1], cs[0]));
            }
            "chain" => {
                let cs = ints(it);
                for w in cs.windows(2) {
                    lvl.edges.insert((w[0], w[1]));
                }
            }
            "edge" => {
                let cs = ints(it);
                lvl.edges.insert((cs[0], cs[1]));
            }
            "item" | "items" => lvl.items.extend(ints(it)),
            "weld" => lvl.welds.extend(ints(it)),
            "mv" => lvl.mv = it.next().unwrap().parse().unwrap(),
            "cut" => lvl.cut = it.next().unwrap().parse().unwrap(),
            "link" => lvl.link = it.next().unwrap().parse().unwrap(),
            "par" => lvl.par = it.next().unwrap().parse().unwrap(),
            "row" => {
                let label = it.next().unwrap().to_string();
                lvl.rows.push((label, ints(it)));
            }
            other => panic!("unknown level directive {:?}", other),
        }
    }
    // A welded cell holds an immovable item: occupied, and its out-edge removed.
    for &w in &lvl.welds {
        lvl.items.insert(w);
        let out: Vec<(i64, i64)> = lvl.edges.iter().filter(|(a, _)| *a == w).copied().collect();
        for e in out {
            lvl.edges.remove(&e);
        }
    }
    // The physics duplicates an item that has two empty successors; forbid the
    // topology rather than complicate the sealed program.
    let mut outs: BTreeMap<i64, usize> = BTreeMap::new();
    for (a, _) in &lvl.edges {
        *outs.entry(*a).or_insert(0) += 1;
    }
    for (c, n) in outs {
        assert!(n <= 1, "cell {} has out-degree {} (must be <= 1)", c, n);
    }
    if lvl.rows.is_empty() {
        let mut cells: Vec<i64> = lvl.edges.iter().flat_map(|(a, b)| [*a, *b]).collect();
        cells.sort();
        cells.dedup();
        lvl.rows.push(("cells".into(), cells));
    }
    lvl
}

fn current_moves(
    rx: &Receiver<Event<u64, Vec<((Row, Row), u64, Diff)>>>,
    counts: &mut BTreeMap<(i64, i64), Diff>,
) -> Vec<(i64, i64)> {
    loop {
        match rx.try_recv() {
            Ok(Event::Messages(_t, data)) => {
                for ((k, _v), _t2, d) in data {
                    *counts.entry(key_pair(&k)).or_insert(0) += d;
                }
            }
            Ok(Event::Progress(_)) => {}
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
    counts.iter().filter(|(_, &c)| c > 0).map(|(&k, _)| k).collect()
}

fn render(lvl: &Level, occ: &BTreeSet<i64>, tick: u64, energy: usize, mv: usize, cut: usize, link: usize, quiet: bool, complete: bool) {
    let width = lvl.rows.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
    let state = match (quiet, complete) {
        (true, true) => "  == STILL ==",
        (true, false) => "  == jammed: silent, but loose ends ==",
        _ => "  ~~ churning ~~",
    };
    println!();
    println!(
        "t{:>4}  energy:{:>4} (par {})  tools: mv:{} cut:{} link:{}{}",
        tick, energy, lvl.par, mv, cut, link, state
    );
    for (label, cells) in &lvl.rows {
        let body: String = cells
            .iter()
            .map(|c| {
                if lvl.welds.contains(c) {
                    '@'
                } else if occ.contains(c) {
                    '#'
                } else {
                    '.'
                }
            })
            .collect();
        let ids: String = cells
            .iter()
            .map(|c| char::from_digit((*c % 10) as u32, 10).unwrap())
            .collect();
        println!("  {:>width$}:|{}|", label, body, width = width);
        if std::env::var("PERPETUUM_IDS").is_ok() {
            println!("  {:>width$} |{}|", "", ids, width = width);
        }
    }
    let _ = std::io::stdout().flush();
}

fn main() {
    let level_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/perpetuum/levels/01-two-rings.lvl".to_string());
    let program_path = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "examples/programs/belt.ddp".to_string());

    let lvl = load_level(&level_path);
    let source = interactive::load_program(&program_path);
    let stmts = parse::pipe::parse(&source);
    let mut tree = lower::lower_tree(stmts);
    tree.optimize();
    let export_idx = tree
        .root
        .exports
        .iter()
        .position(|e| e.name == "result")
        .expect("program must export \"result\"");

    timely::execute_directly(move |worker| {
        let (tx, rx) = channel::<Event<u64, Vec<((Row, Row), u64, Diff)>>>();

        let (mut succ_in, mut occ_in, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (succ_h, succ_c) = scope.new_collection::<(Row, Row), Diff>();
            let (occ_h, occ_c) = scope.new_collection::<(Row, Row), Diff>();
            let collections = vec![succ_c, occ_c]; // 0 = topology, 1 = occupancy
            let probe = ProbeHandle::new();
            let exports = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let root_imports: Vec<_> = tree
                    .root
                    .imports
                    .iter()
                    .map(|imp| match &imp.from {
                        st::Source::Input(idx) => entered[*idx].clone(),
                        other => panic!("perpetuum: unsupported source {:?}", other),
                    })
                    .collect();
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

        // Host mirror of the machine's inputs.
        let mut edges = lvl.edges.clone();
        let mut occ = lvl.items.clone();
        let cells: BTreeSet<i64> = lvl.edges.iter().flat_map(|(a, b)| [*a, *b]).chain(lvl.items.iter().copied()).collect();
        let (mut mv_left, mut cut_left, mut link_left) = (lvl.mv, lvl.cut, lvl.link);
        let mut energy = 0usize;
        let mut epoch = 0u64;

        for &(a, b) in &edges {
            succ_in.update(edge(a, b), 1);
        }
        for &c in &occ {
            occ_in.update(cell(c), 1);
        }

        // Advance both inputs past `epoch` and run the worker until the probe
        // catches up, then refresh the current move set.
        let mut counts: BTreeMap<(i64, i64), Diff> = BTreeMap::new();
        macro_rules! sync {
            () => {{
                epoch += 1;
                succ_in.advance_to(epoch);
                succ_in.flush();
                occ_in.advance_to(epoch);
                occ_in.flush();
                while probe.less_than(&epoch) {
                    worker.step();
                }
                current_moves(&rx, &mut counts)
            }};
        }

        let mut moves = sync!();

        println!("== {} ==", lvl.name);
        for line in &lvl.blurb {
            println!("   {}", line);
        }
        println!("   The machine runs on {} sealed expressions ({}).", "rule-184", program_path);
        println!("   Bring it to fixed point: zero updates, nothing lost.  (help for commands)");

        // One CA application: move every item whose successor is empty.
        macro_rules! step_machine {
            () => {{
                for &(s, t) in &moves {
                    occ_in.update(cell(s), -1);
                    occ_in.update(cell(t), 1);
                    occ.remove(&s);
                    occ.insert(t);
                    energy += 2;
                }
            }};
        }

        let stdin = std::io::stdin();
        let mut lines = stdin.lock().lines();
        let mut resolved_at: Option<(u64, usize)> = None;

        loop {
            let quiet = moves.is_empty();
            // A resolved machine has no loose ends: every unwelded cell keeps an
            // out-edge, so silence-by-plugging (cut a belt, let items pile up
            // behind the stump) is a jam, not a fixed point of a whole machine.
            let dangling: Vec<i64> = cells
                .iter()
                .filter(|c| !lvl.welds.contains(*c) && !edges.iter().any(|(a, _)| a == *c))
                .copied()
                .collect();
            render(&lvl, &occ, epoch, energy, mv_left, cut_left, link_left, quiet, dangling.is_empty());
            if quiet && dangling.is_empty() {
                let (t, e) = (epoch, energy);
                resolved_at = Some((t, e));
                break;
            }
            if quiet {
                println!("  loose ends at {:?}: reconnect them; a jam is stillness by strangulation.", dangling);
            }
            print!("> ");
            let _ = std::io::stdout().flush();
            let line = match lines.next() {
                Some(Ok(l)) => l,
                _ => break,
            };
            let toks: Vec<&str> = line.split_whitespace().collect();
            match toks.as_slice() {
                ["q"] | ["quit"] => break,
                ["help"] => {
                    println!("  mv A B    teleport the item at A to empty cell B (costs 1 mv, 2 energy)");
                    println!("  cut A B   remove belt edge A->B (costs 1 cut, 1 energy)");
                    println!("  link A B  add belt edge A->B; A must have no out-edge (costs 1 link, 1 energy)");
                    println!("  t [n]     let the machine run n ticks (default 1)");
                    println!("  r         run until still or 200 ticks");
                    println!("  q         abandon the machine to its churning");
                }
                ["mv", a, b] => {
                    let (a, b): (i64, i64) = (a.parse().unwrap_or(-1), b.parse().unwrap_or(-1));
                    if mv_left == 0 {
                        println!("  no mv tools left");
                    } else if !occ.contains(&a) {
                        println!("  {} holds no item", a);
                    } else if lvl.welds.contains(&a) {
                        println!("  the item at {} is welded down", a);
                    } else if !cells.contains(&b) {
                        println!("  {} is not a cell", b);
                    } else if occ.contains(&b) {
                        println!("  {} is already occupied", b);
                    } else {
                        occ_in.update(cell(a), -1);
                        occ_in.update(cell(b), 1);
                        occ.remove(&a);
                        occ.insert(b);
                        energy += 2;
                        mv_left -= 1;
                        moves = sync!();
                    }
                }
                ["cut", a, b] => {
                    let (a, b): (i64, i64) = (a.parse().unwrap_or(-1), b.parse().unwrap_or(-1));
                    if cut_left == 0 {
                        println!("  no cut tools left");
                    } else if !edges.contains(&(a, b)) {
                        println!("  no edge {}->{}", a, b);
                    } else {
                        succ_in.update(edge(a, b), -1);
                        edges.remove(&(a, b));
                        energy += 1;
                        cut_left -= 1;
                        println!("  edge {}->{} removed", a, b);
                        moves = sync!();
                    }
                }
                ["link", a, b] => {
                    let (a, b): (i64, i64) = (a.parse().unwrap_or(-1), b.parse().unwrap_or(-1));
                    if link_left == 0 {
                        println!("  no link tools left");
                    } else if !cells.contains(&a) || !cells.contains(&b) {
                        println!("  both ends must be existing cells");
                    } else if edges.iter().any(|(x, _)| *x == a) {
                        println!("  {} already has an out-edge (cut it first)", a);
                    } else {
                        succ_in.update(edge(a, b), 1);
                        edges.insert((a, b));
                        energy += 1;
                        link_left -= 1;
                        println!("  edge {}->{} added", a, b);
                        moves = sync!();
                    }
                }
                ["t"] | ["t", _] => {
                    let n: usize = toks.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
                    for _ in 0..n {
                        if moves.is_empty() {
                            break;
                        }
                        step_machine!();
                        moves = sync!();
                    }
                }
                ["r"] => {
                    let mut n = 0;
                    while !moves.is_empty() && n < 200 {
                        step_machine!();
                        moves = sync!();
                        n += 1;
                    }
                    if !moves.is_empty() {
                        println!("  200 ticks and still churning; this is what perpetual motion costs.");
                    }
                }
                [] => {}
                other => println!("  unknown command {:?} (try help)", other),
            }
        }

        match resolved_at {
            Some((t, e)) => {
                println!();
                println!("  ================================================");
                println!("   RESOLVED at t{} — the machine is at fixed point.", t);
                println!("   No updates flow. No work is done. It may exit.");
                println!("   energy spent: {}   par: {}", e, lvl.par);
                println!("  ================================================");
            }
            None => println!("  You walk away. Somewhere, a bubble is still circling."),
        }
    });
}
