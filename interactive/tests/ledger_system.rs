//! The state of a system of ledgers (`examples/ledger/system.ddp`), changed by commands (`examples/ledger/commands.ddp`),
//! with each change judged by the rules of a state and the rules of a step (`examples/ledger/step.ddp`).
//!
//! A transaction is made only if it breaks no rule. The test builds a small system by commands, reads its views,
//! and then offers changes that each break rules, and checks that exactly those rules complain.
//!
//! Admission (`examples/ledger/admit.ddp`) is a short check of appends that stands in for the rules.
//! Each transaction it covers is judged both ways, and the two must agree.
//!
//! A round (`examples/ledger/round.ddp`) is admission of many transactions at once, against an index that is
//! kept beside the entries (`examples/ledger/index.ddp` says what it should be). What a round makes, the rules
//! must make too, one transaction after another, and they must leave the same state and the same index.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use interactive::scope_ir::Program;

/// A collection as rows of text, each with its multiplicity.
type Rows = BTreeMap<(String, String), i64>;

fn program(text: &str) -> Program {
    let mut program = interactive::lower::lower_tree(interactive::parse::pipe::parse(text));
    interactive::passes::optimize(&mut program);
    program
}

fn evaluate(program: &Program, inputs: &[Vec<String>], workers: usize) -> BTreeMap<String, Rows> {
    interactive::server::evaluate(timely::Config::process(workers), program, inputs).into_iter()
        .map(|(name, updates)| {
            let mut rows = Rows::new();
            for (key, val, diff) in updates.rows() { *rows.entry((key, val)).or_default() += diff; }
            rows.retain(|_, diff| *diff != 0);
            (name, rows)
        }).collect()
}

fn lines(rows: &Rows) -> Vec<String> {
    rows.iter().map(|((key, val), diff)| format!("{key} val={val} diff={diff}")).collect()
}

fn rows(rows: &[(&str, &str)]) -> Rows {
    rows.iter().map(|(key, val)| ((key.to_string(), val.to_string()), 1)).collect()
}

/// The commands, the rules of a state, the rules of a step, admission, admission of a round, and the index a round reads.
struct System { commands: Program, state: Program, step: Program, admit: Program, round: Program, index: Program }
fn system() -> System {
    System {
        commands: program(include_str!("../examples/ledger/commands.ddp")),
        state: program(include_str!("../examples/ledger/system-full.ddp")),
        step: program(include_str!("../examples/ledger/step-full.ddp")),
        admit: program(include_str!("../examples/ledger/admit.ddp")),
        round: program(include_str!("../examples/ledger/round.ddp")),
        index: program(include_str!("../examples/ledger/index.ddp")),
    }
}

const STATE_RULES: [&str; 28] = [
    "repeated", "reregistered", "forked", "doubled", "ambiguous", "reprogrammed", "redefined", "contested", "reheld",
    "orphaned", "unclocked", "mismatched", "misnamed", "unprogrammed", "unregistered", "misheld", "undefined",
    "malformed", "early", "skipped", "forgotten",
    "spanned", "hollow", "starved", "belated",
    "misbound", "misplaced", "misshapen",
];
const UNFOUNDED: &str = "unfounded";
const STEP_RULES: [&str; 8] = ["reshaped", "rewritten", "reworded", "rebound", "regressed", "revised", "detached", "usurped"];
const UNMET: &str = "unmet";
/// The checks of admission, and the commands it leaves to the rules.
const ADMISSION: [&str; 4] = ["absent", "stale", "empty", "barred"];
const DEFERRED: &str = "deferred";
/// The transactions that admission covered, by what it said of them.
static ADMITTED: AtomicUsize = AtomicUsize::new(0);
static TURNED_AWAY: AtomicUsize = AtomicUsize::new(0);

/// Who makes a change: a user, or a runner of a computation.
const USER: &str = "tuple() val=tuple(inject(0,tuple()))";
fn runner(computation: i64) -> String { format!("tuple() val=tuple(inject(1,tuple(0,{computation})))") }

/// The rules a change breaks: those of the state after it, and those of the step from the state before it.
fn judge(system: &System, before: &Rows, after: &Rows, actor: &str, workers: usize) -> BTreeSet<&'static str> {
    let was = evaluate(&system.state, &[lines(before)], workers);
    let is = evaluate(&system.state, &[lines(after)], workers);
    let (removes, adds) = change(before, after);
    let step = evaluate(&system.step, &[lines(before), lines(&removes), lines(&adds), lines(&was["standing"]), lines(&is["standing"]), vec![actor.to_string()]], workers);
    let mut broken = BTreeSet::new();
    for rule in STATE_RULES.into_iter().chain([UNFOUNDED]) { if !is[rule].is_empty() { broken.insert(rule); } }
    for rule in STEP_RULES { if !step[rule].is_empty() { broken.insert(rule); } }
    broken
}

/// A step as its change: what it takes out of `before`, and what it puts in, to make `after`.
fn change(before: &Rows, after: &Rows) -> (Rows, Rows) {
    let mut removes = before.clone();
    for (row, diff) in after { *removes.entry(row.clone()).or_default() -= diff; }
    let mut adds = Rows::new();
    removes.retain(|row, diff| {
        if *diff < 0 { adds.insert(row.clone(), -*diff); }
        *diff > 0
    });
    (removes, adds)
}

/// `entries` with some rows removed and others added.
fn changed(entries: &Rows, removes: &Rows, adds: &Rows) -> Rows {
    let mut after = entries.clone();
    for (row, diff) in removes { *after.entry(row.clone()).or_default() -= diff; }
    for (row, diff) in adds { *after.entry(row.clone()).or_default() += diff; }
    after.retain(|_, diff| *diff != 0);
    after
}

/// One transaction: its commands as changes, which are made if they break no rule.
/// Returns the rules that stopped it, which is none if it was made.
fn transact(system: &System, entries: &mut Rows, actor: &str, commands: &[String], workers: usize) -> BTreeSet<&'static str> {
    let commands: Vec<String> = commands.iter().enumerate().map(|(index, command)| format!("{index} val=tuple({command})")).collect();
    let change = evaluate(&system.commands, &[commands.clone(), lines(entries)], workers);
    let after = changed(entries, &change["removes"], &change["adds"]);
    let mut broken = judge(system, entries, &after, actor, workers);
    if !change[UNMET].is_empty() { broken.insert(UNMET); }
    // Admission reads the standing and the bindings before, and should say what the rules say.
    let was = evaluate(&system.state, &[lines(entries)], workers);
    let admit = evaluate(&system.admit, &[commands, lines(&was["standing"]), lines(&was["bound"]), vec![actor.to_string()]], workers);
    if admit[DEFERRED].is_empty() {
        let admitted = ADMISSION.iter().all(|check| admit[*check].is_empty());
        assert_eq!(admitted, broken.is_empty(), "admission and the rules disagree: {admit:?} against {broken:?}");
        if admitted { &ADMITTED } else { &TURNED_AWAY }.fetch_add(1, Ordering::Relaxed);
    }
    if broken.is_empty() { *entries = after; }
    broken
}

fn id(id: i64) -> String { format!("tuple(0,{id})") }
/// An input that is the state, where a ledger would be named.
const STATE: i64 = i64::MIN;
/// A computation's inputs, in order: each is read from a ledger, or is the state.
fn sources(inputs: &[i64]) -> String {
    inputs.iter().map(|input| if *input == STATE { "inject(0,tuple())".to_string() } else { format!("inject(1,{})", id(*input)) }).collect::<Vec<_>>().join(",")
}
fn frontier(time: &[i64]) -> String { format!("list(list({}))", time.iter().map(i64::to_string).collect::<Vec<_>>().join(",")) }
const NEVER: &str = "list()";
/// A batch held inline, as a compacted copy of a span might be.
const INLINE: &str = "inject(1,list(1,2,3))";
fn shapes(shapes: &[i64]) -> String { shapes.iter().map(|shape| format!("list({shape})")).collect::<Vec<_>>().join(",") }
/// A computation's outputs, in order: each is written to a ledger, or is not written.
fn places(outputs: &[Option<i64>]) -> String {
    outputs.iter().map(|output| output.map_or("inject(0,tuple())".to_string(), |ledger| format!("inject(1,{})", id(ledger)))).collect::<Vec<_>>().join(",")
}
const DURABLE: &str = "inject(0,tuple())";
const DERIVED: &str = "inject(1,tuple())";

// The commands, as text.
fn create(ledger: i64, shape: i64, width: i64, start: &[i64], clock: Option<i64>) -> String {
    let clock = clock.map_or("inject(0,tuple())".to_string(), |clock| format!("inject(1,{})", id(clock)));
    format!("inject(0,tuple({},list({shape}),{width},{},{clock},{DURABLE}))", id(ledger), frontier(start))
}
/// A ledger whose contents are not stored.
fn create_derived(ledger: i64, shape: i64, start: &[i64]) -> String {
    format!("inject(0,tuple({},list({shape}),1,{},inject(0,tuple()),{DERIVED}))", id(ledger), frontier(start))
}
fn append_to(ledger: i64, number: i64, lower: &str, upper: &str) -> String {
    format!("inject(1,tuple({},{number},{lower},{upper},inject(0,tuple())))", id(ledger))
}
fn append(ledger: i64, number: i64, lower: &[i64], upper: &[i64]) -> String { append_to(ledger, number, &frontier(lower), &frontier(upper)) }
/// Cover the appends from `from` up to `to` again, as one span at `since`.
fn merge(ledger: i64, from: i64, to: i64, since: &[i64]) -> String { format!("inject(2,tuple({},{from},{to},{},{INLINE}))", id(ledger), frontier(since)) }
fn discard(ledger: i64, from: i64, to: i64, since: &[i64]) -> String { format!("inject(3,tuple({},{from},{to},{}))", id(ledger), frontier(since)) }
fn drop(ledger: i64) -> String { format!("inject(4,{})", id(ledger)) }
fn alias(name: i64, ledger: i64) -> String { format!("inject(5,tuple(list({name}),{}))", id(ledger)) }
fn unalias(name: i64) -> String { format!("inject(6,list({name}))") }
fn publish(program: i64, takes: &[i64], gives: &[i64], text: i64) -> String {
    format!("inject(7,tuple(list({program}),list({}),list({}),list({text})))", shapes(takes), shapes(gives))
}
fn define(computation: i64, program: i64, inputs: &[i64], outputs: &[Option<i64>]) -> String {
    format!("inject(8,tuple({},list({program}),list({}),list({})))", id(computation), sources(inputs), places(outputs))
}
fn retire(computation: i64) -> String { format!("inject(9,{})", id(computation)) }
fn start(instance: i64, computation: i64) -> String { format!("inject(10,tuple({},{}))", id(instance), id(computation)) }
fn stop(instance: i64, computation: i64) -> String { format!("inject(11,tuple({},{}))", id(instance), id(computation)) }
/// A holder asks that a ledger be kept readable from a frontier.
fn hold(holder: i64, ledger: i64, from: &[i64]) -> String { format!("inject(12,tuple({},{},{}))", id(holder), id(ledger), frontier(from)) }
fn release(holder: i64, ledger: i64) -> String { format!("inject(13,tuple({},{}))", id(holder), id(ledger)) }

// Entries, as rows, for changes that no command makes.
// Each is its key and then the rest.
fn entry(kind: usize, key: String, val: String) -> Rows { rows(&[(&format!("tuple(inject({kind},tuple(tuple({key}),tuple({val}))))"), "tuple()")]) }
fn ledger(ledger: i64, shape: i64, start: &[i64]) -> Rows { entry(0, id(ledger), format!("list({shape}),1,{},inject(0,tuple()),{DURABLE}", frontier(start))) }
/// One append, as a new span.
fn span(ledger: i64, number: i64, lower: &[i64], upper: &[i64]) -> Rows { span_at(ledger, number, lower, upper, lower, "inject(0,tuple())") }
/// One append as a span at `since`, with `batch`.
fn span_at(ledger: i64, number: i64, lower: &[i64], upper: &[i64], since: &[i64], batch: &str) -> Rows {
    run(ledger, number, number + 1, lower, upper, since, batch)
}
/// The appends from `from` up to `to`, as one span at `since`, with `batch`.
fn run(ledger: i64, from: i64, to: i64, lower: &[i64], upper: &[i64], since: &[i64], batch: &str) -> Rows {
    entry(1, format!("{},{from},{to},{}", id(ledger), frontier(since)), format!("{},{},{batch}", frontier(lower), frontier(upper)))
}
fn name(name: i64, ledger: i64) -> Rows { entry(2, format!("list({name})"), id(ledger)) }
fn coded(program: i64, takes: &[i64], gives: &[i64], text: i64) -> Rows {
    entry(3, format!("list({program})"), format!("list({}),list({}),list({text})", shapes(takes), shapes(gives)))
}
fn computation(computation: i64, program: i64, inputs: &[i64], outputs: &[Option<i64>]) -> Rows {
    entry(4, id(computation), format!("list({program}),list({}),list({})", sources(inputs), places(outputs)))
}

fn held(holder: i64, ledger: i64, from: &[i64]) -> Rows { entry(6, format!("{},{}", id(holder), id(ledger)), frontier(from)) }

fn broken(rules: &[&'static str]) -> BTreeSet<&'static str> { rules.iter().copied().collect() }
fn keyed(ledgers: &[i64]) -> Rows { ledgers.iter().map(|ledger| ((format!("tuple({})", id(*ledger)), "tuple()".to_string()), 1)).collect() }

/// A small system, built by commands: nine ledgers, five programs, three computations, two instances, and some appends.
fn small(system: &System, workers: usize) -> Rows {
    let mut entries = Rows::new();
    let none = broken(&[]);
    // Ledgers 1 to 8 of shape 7, except 6 of shape 8. Ledger 9 is a clock, and 8 follows it.
    let mut ledgers: Vec<String> = (1..=7).map(|ledger| create(ledger, if ledger == 6 { 8 } else { 7 }, 1, &[0], None)).collect();
    ledgers.push(create(9, 7, 1, &[0], None));
    assert_eq!(transact(&system, &mut entries, USER, &ledgers, workers), none);
    assert_eq!(transact(&system, &mut entries, USER, &[create(8, 7, 1, &[0], Some(9))], workers), none);
    // Program 70 takes one input and gives two outputs; 71 takes one and gives one; 80 and 81 take two and give one; 90 takes one and gives none.
    let programs = [publish(70, &[7], &[7, 7], 1), publish(71, &[7], &[7], 2), publish(80, &[7, 7], &[7], 3), publish(81, &[7, 7], &[7], 4), publish(90, &[7], &[], 5)];
    assert_eq!(transact(&system, &mut entries, USER, &programs, workers), none);
    // 100 reads 1 and writes 2 and 3; 101 reads 2 and 3 and writes 4; 102 reads 4 and writes nothing.
    // Instances 500 and 501 both run 100, and 501 also runs 101. Ledgers 1 and 4 are named. All in one transaction.
    let setup = [
        define(100, 70, &[1], &[Some(2), Some(3)]), define(101, 80, &[2, 3], &[Some(4)]), define(102, 90, &[4], &[]),
        start(500, 100), start(501, 100), start(501, 101),
        alias(1, 1), alias(4, 4),
    ];
    assert_eq!(transact(&system, &mut entries, USER, &setup, workers), none);
    // A user adds to the unbound ledger 1, and the runners add to the ledgers their computations write.
    assert_eq!(transact(&system, &mut entries, USER, &[append(1, 0, &[0], &[9])], workers), none);
    assert_eq!(transact(&system, &mut entries, &runner(100), &[append(2, 0, &[0], &[7]), append(3, 0, &[0], &[7])], workers), none);
    assert_eq!(transact(&system, &mut entries, &runner(101), &[append(4, 0, &[0], &[5])], workers), none);
    // The clock advances, and takes its follower with it.
    assert_eq!(transact(&system, &mut entries, USER, &[append(9, 0, &[0], &[3])], workers), none);
    entries
}

#[test]
fn commands_change_a_system_within_its_rules() {
    let system = system();
    for workers in [1, 2] {
        let mut entries = small(&system, workers);
        let none = broken(&[]);

        // THE VIEWS.
        let out = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(out["standing"], rows(&[
            ("tuple(tuple(0,1))", "tuple(1,list(list(9)))"), ("tuple(tuple(0,2))", "tuple(1,list(list(7)))"), ("tuple(tuple(0,3))", "tuple(1,list(list(7)))"),
            ("tuple(tuple(0,4))", "tuple(1,list(list(5)))"), ("tuple(tuple(0,5))", "tuple(0,list(list(0)))"), ("tuple(tuple(0,6))", "tuple(0,list(list(0)))"),
            ("tuple(tuple(0,7))", "tuple(0,list(list(0)))"), ("tuple(tuple(0,8))", "tuple(0,list(list(3)))"), ("tuple(tuple(0,9))", "tuple(1,list(list(3)))"),
        ]));
        assert_eq!(out["bound"], rows(&[
            ("tuple(tuple(0,2))", "tuple(tuple(0,100),0)"), ("tuple(tuple(0,3))", "tuple(tuple(0,100),1)"), ("tuple(tuple(0,4))", "tuple(tuple(0,101),0)"),
        ]));
        assert_eq!(out["unbound"], keyed(&[1, 5, 6, 7, 8, 9]));
        assert_eq!(out["plan"], rows(&[
            ("tuple(tuple(0,500))", "tuple(tuple(0,100),list(70),list(inject(1,tuple(0,1))),list(inject(1,tuple(0,2)),inject(1,tuple(0,3))))"),
            ("tuple(tuple(0,501))", "tuple(tuple(0,100),list(70),list(inject(1,tuple(0,1))),list(inject(1,tuple(0,2)),inject(1,tuple(0,3))))"),
            ("tuple(tuple(0,501))", "tuple(tuple(0,101),list(80),list(inject(1,tuple(0,2)),inject(1,tuple(0,3))),list(inject(1,tuple(0,4))))"),
        ]));
        assert_eq!(out["idle"], keyed(&[102]));
        // Each input is kept back to where the outputs of its reader stand.
        assert_eq!(out["needs"], rows(&[
            ("tuple(tuple(0,1))", "tuple(tuple(0,100),tuple(0,2),list(list(7)))"), ("tuple(tuple(0,1))", "tuple(tuple(0,100),tuple(0,3),list(list(7)))"),
            ("tuple(tuple(0,2))", "tuple(tuple(0,101),tuple(0,4),list(list(5)))"), ("tuple(tuple(0,3))", "tuple(tuple(0,101),tuple(0,4),list(list(5)))"),
        ]));
        assert_eq!(out["names"], rows(&[("tuple(list(1))", "tuple(tuple(0,1))"), ("tuple(list(4))", "tuple(tuple(0,4))")]));

        // TRANSACTIONS THAT ARE NOT MADE, each with the rules that stop it. The state is as it was after each.
        let standing = entries.clone();
        let refused: Vec<(&str, String, Vec<String>, &[&'static str])> = vec![
            ("a user adds to a bound ledger", USER.into(), vec![append(2, 1, &[7], &[8])], &["usurped"]),
            ("a span that begins after the write frontier", runner(100), vec![append(2, 1, &[8], &[9])], &["detached"]),
            ("a span that begins before the write frontier", runner(100), vec![append(2, 1, &[6], &[9])], &["detached", "early"]),
            ("a span that begins before the clock its ledger follows", USER.into(), vec![append(8, 0, &[0], &[5])], &["detached"]),
            ("an append that skips a number", runner(100), vec![append(2, 2, &[7], &[8])], &["skipped", "forgotten"]),
            ("a first append that is not numbered 0", USER.into(), vec![append(6, 1, &[0], &[1])], &["skipped"]),
            ("a second append of one number", runner(100), vec![append(2, 0, &[7], &[8])], &["forked", "revised"]),
            ("the only span of an append removed", USER.into(), vec![discard(1, 0, 1, &[0])], &["regressed"]),
            ("an append kept only from after where a reader needs it", USER.into(), vec![merge(1, 0, 1, &[8]), discard(1, 0, 1, &[0])], &["forgotten"]),
            ("a span removed that is not there", USER.into(), vec![discard(1, 0, 1, &[5])], &["unmet"]),
            ("appends merged that are not there", USER.into(), vec![merge(1, 0, 2, &[0])], &["unmet"]),
            ("a span that covers nothing", runner(100), vec![append(2, 1, &[7], &[7])], &["malformed"]),
            ("an append to no registered ledger", USER.into(), vec![append(77, 0, &[0], &[1])], &["orphaned"]),
            ("a ledger registered again, as it is", USER.into(), vec![create(5, 7, 1, &[0], None)], &["repeated"]),
            ("a ledger registered again, with another shape", USER.into(), vec![create(5, 8, 1, &[0], None)], &["reregistered"]),
            ("a ledger that follows no registered clock", USER.into(), vec![create(10, 7, 1, &[0], Some(77))], &["unclocked"]),
            ("a ledger that follows a clock of another width", USER.into(), vec![create(10, 7, 2, &[0, 0], Some(9))], &["mismatched"]),
            ("a ledger dropped while it is named and read", USER.into(), vec![drop(1)], &["misnamed", "unregistered"]),
            ("a run stopped that was not started", USER.into(), vec![stop(502, 100)], &["unmet"]),
            ("a name removed that was not there", USER.into(), vec![unalias(42)], &["unmet"]),
            ("a hold on no registered ledger", USER.into(), vec![hold(900, 77, &[0])], &["misheld"]),
            ("a hold removed that was not there", USER.into(), vec![release(900, 1)], &["unmet"]),
            ("a computation retired while instances run it", USER.into(), vec![retire(100)], &["undefined"]),
            ("a second computation that writes a ledger", USER.into(), vec![define(103, 81, &[2, 3], &[Some(4)])], &["contested"]),
            ("a ledger written by two outputs", USER.into(), vec![define(103, 70, &[1], &[Some(5), Some(5)])], &["contested"]),
            ("a computation of no known program", USER.into(), vec![define(103, 99, &[1], &[])], &["unprogrammed"]),
            ("a computation with an input too many", USER.into(), vec![define(103, 90, &[1, 2], &[])], &["misbound"]),
            ("a computation with an input too few", USER.into(), vec![define(103, 81, &[2], &[None])], &["misbound"]),
            ("a computation with an output its program does not give", USER.into(), vec![define(103, 90, &[1], &[Some(5)])], &["misplaced"]),
            ("a computation with an output too few", USER.into(), vec![define(103, 70, &[1], &[Some(5)])], &["misplaced"]),
            ("a computation that reads a ledger of another shape", USER.into(), vec![define(103, 90, &[6], &[])], &["misshapen"]),
            ("a computation that writes a ledger of another shape", USER.into(), vec![define(103, 71, &[1], &[Some(6)])], &["misshapen"]),
            ("a computation that reads no registered ledger", USER.into(), vec![define(103, 90, &[77], &[])], &["unregistered"]),
            ("a computation that writes no registered ledger", USER.into(), vec![define(103, 71, &[1], &[Some(77)])], &["unregistered"]),
            ("a run of no defined computation", USER.into(), vec![start(502, 999)], &["undefined"]),
            ("a name for no registered ledger", USER.into(), vec![alias(9, 77)], &["misnamed"]),
            ("two computations that each read what the other writes", USER.into(), vec![define(105, 71, &[5], &[Some(7)]), define(106, 71, &[7], &[Some(5)])], &["unfounded"]),
            ("a second program of one hash", USER.into(), vec![publish(70, &[7], &[7, 7], 9)], &["reprogrammed"]),
            ("a second definition of a computation", USER.into(), vec![define(101, 81, &[2, 3], &[Some(4)])], &["redefined"]),
        ];
        for (what, actor, commands, rules) in refused {
            assert_eq!(transact(&system, &mut entries, &actor, &commands, workers), broken(rules), "{what}");
            assert_eq!(entries, standing, "{what}");
        }

        // CHANGES THAT NO COMMAND MAKES: entries removed and entries added, and the rules that complain.
        let nothing = Rows::new();
        let forced: Vec<(&str, Rows, Rows, &[&'static str])> = vec![
            ("a span rewritten to end earlier", span(1, 0, &[0], &[9]), span(1, 0, &[0], &[8]), &["rewritten", "regressed", "revised"]),
            ("a span removed", span(4, 0, &[0], &[5]), nothing.clone(), &["regressed"]),
            ("a span added again, at a later since", nothing.clone(), span_at(1, 0, &[0], &[9], &[5], INLINE), &[]),
            ("a span added again, with another interval", nothing.clone(), span_at(1, 0, &[0], &[8], &[5], INLINE), &["forked", "revised"]),
            ("a span that covers an old append with a new one", nothing.clone(), run(1, 0, 2, &[0], &[12], &[0], INLINE), &["detached"]),
            // It ends where append 0 ends, and at another frontier, so the rules about append 0 complain too.
            ("a span with no appends", nothing.clone(), run(1, 1, 1, &[9], &[12], &[9], INLINE), &["malformed", "forked", "early", "revised"]),
            ("a second span at one since", nothing.clone(), span_at(1, 0, &[0], &[9], &[0], INLINE), &["doubled"]),
            ("a registration changed", ledger(7, 7, &[0]), ledger(7, 7, &[5]), &["reshaped"]),
            ("a program changed", coded(90, &[7], &[], 5), coded(90, &[7], &[], 6), &["reworded"]),
            ("a computation changed", computation(102, 90, &[4], &[]), computation(102, 90, &[3], &[]), &["rebound"]),
            ("a name with two ledgers", nothing.clone(), name(4, 3), &["ambiguous"]),
            ("a span of no registered ledger", nothing.clone(), span(77, 0, &[0], &[1]), &["orphaned"]),
        ];
        for (what, removes, adds, rules) in forced {
            for row in removes.keys() { assert!(entries.contains_key(row), "{what}: {row:?} is not there to remove"); }
            assert_eq!(judge(&system, &entries, &changed(&entries, &removes, &adds), USER, workers), broken(rules), "{what}");
        }

        // An append with spans at two sinces may lose either, but not both.
        // Ledger 1 is needed from [7], so a since of [5] serves, and one of [8] does not.
        let twice = changed(&entries, &nothing, &span_at(1, 0, &[0], &[9], &[5], INLINE));
        assert_eq!(judge(&system, &twice, &changed(&twice, &span(1, 0, &[0], &[9]), &nothing), USER, workers), none);
        assert_eq!(judge(&system, &twice, &changed(&twice, &span_at(1, 0, &[0], &[9], &[5], INLINE), &nothing), USER, workers), none);
        let late = changed(&entries, &nothing, &span_at(1, 0, &[0], &[9], &[8], INLINE));
        assert_eq!(judge(&system, &late, &changed(&late, &span(1, 0, &[0], &[9]), &nothing), USER, workers), broken(&["forgotten"]));
        // The batch it has at one since may not change.
        let other = changed(&changed(&twice, &span_at(1, 0, &[0], &[9], &[5], INLINE), &nothing), &nothing, &span_at(1, 0, &[0], &[9], &[5], "inject(1,list(4))"));
        assert_eq!(judge(&system, &twice, &other, USER, workers), broken(&["rewritten"]));
        let both = changed(&changed(&twice, &span(1, 0, &[0], &[9]), &nothing), &span_at(1, 0, &[0], &[9], &[5], INLINE), &nothing);
        assert_eq!(judge(&system, &twice, &both, USER, workers), broken(&["regressed"]));

        // A blob is in use while a span names it. No span of this system names one, until one is merged into a blob.
        assert!(out["blobs"].is_empty());
        let blobbed = evaluate(&system.state, &[lines(&changed(&entries, &nothing, &span_at(1, 0, &[0], &[9], &[5], "inject(2,list(42))")))], workers);
        assert_eq!(blobbed["blobs"], rows(&[("tuple(list(42))", "tuple()")]));

        // Only a registered ledger has a standing.
        let orphan = evaluate(&system.state, &[lines(&changed(&entries, &nothing, &span(77, 0, &[0], &[1])))], workers);
        assert_eq!(orphan["standing"], out["standing"]);

        // TRANSACTIONS THAT ARE MADE.
        // A name moves to another ledger, and nothing else changes.
        assert_eq!(transact(&system, &mut entries, USER, &[alias(4, 3)], workers), none);
        assert_eq!(evaluate(&system.state, &[lines(&entries)], workers)["names"], rows(&[("tuple(list(1))", "tuple(tuple(0,1))"), ("tuple(list(4))", "tuple(tuple(0,3))")]));
        // A name is removed, and its ledger is as it was.
        assert_eq!(transact(&system, &mut entries, USER, &[unalias(1)], workers), none);
        assert_eq!(evaluate(&system.state, &[lines(&entries)], workers)["names"], rows(&[("tuple(list(4))", "tuple(tuple(0,3))")]));
        // A ledger that nothing names, reads, writes or follows is dropped.
        assert_eq!(transact(&system, &mut entries, USER, &[drop(5)], workers), none);
        // A computation is replaced by another that writes the same ledger, in one transaction.
        // The ledger stays bound, and only the new computation's runners add to it.
        let replace = [stop(501, 101), retire(101), define(103, 81, &[2, 3], &[Some(4)]), start(501, 103)];
        assert_eq!(transact(&system, &mut entries, USER, &replace, workers), none);
        assert_eq!(transact(&system, &mut entries, &runner(101), &[append(4, 1, &[5], &[6])], workers), broken(&["usurped"]));
        assert_eq!(transact(&system, &mut entries, &runner(103), &[append(4, 1, &[5], &[6])], workers), none);
        // The two appends of ledger 4 are merged into one span, and the two spans are let go. Its standing is as it was.
        // Anyone may do this, though the ledger is bound: it adds nothing to the ledger.
        let was = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(transact(&system, &mut entries, USER, &[merge(4, 0, 2, &[0])], workers), none);
        assert_eq!(transact(&system, &mut entries, USER, &[discard(4, 0, 1, &[0]), discard(4, 1, 2, &[5])], workers), none);
        assert_eq!(evaluate(&system.state, &[lines(&entries)], workers)["standing"], was["standing"]);
        assert!(entries.contains_key(&run(4, 0, 2, &[0], &[6], &[0], INLINE).into_keys().next().unwrap()));
        // The merged span cannot be taken apart again: what its appends were is no longer there.
        assert_eq!(transact(&system, &mut entries, USER, &[merge(4, 0, 1, &[0])], workers), broken(&["unmet"]));
        // 103 writes ledger 4, which stands at [6], and so needs ledgers 2 and 3 from [6].
        // Ledger 2 may be kept from [6] alone, and not from [7] alone.
        assert_eq!(transact(&system, &mut entries, USER, &[merge(2, 0, 1, &[7]), discard(2, 0, 1, &[0])], workers), broken(&["forgotten"]));
        assert_eq!(transact(&system, &mut entries, USER, &[merge(2, 0, 1, &[6]), discard(2, 0, 1, &[0])], workers), none);
        // A HOLD. Nothing that writes a durable ledger reads ledger 4, so only a hold keeps it back.
        // Holder 900 asks for it from [3], and it may then not be kept from [5] alone.
        assert_eq!(transact(&system, &mut entries, USER, &[hold(900, 4, &[3])], workers), none);
        let compact = [merge(4, 0, 2, &[5]), discard(4, 0, 2, &[0])];
        assert_eq!(transact(&system, &mut entries, USER, &compact, workers), broken(&["forgotten"]));
        // A holder has one hold on a ledger. A second hold by command takes the place of the first.
        assert_eq!(judge(&system, &entries, &changed(&entries, &nothing, &held(900, 4, &[5])), USER, workers), broken(&["reheld"]));
        assert_eq!(transact(&system, &mut entries, USER, &[hold(900, 4, &[5])], workers), none);
        assert!(entries.contains_key(&held(900, 4, &[5]).into_keys().next().unwrap()));
        assert!(!entries.contains_key(&held(900, 4, &[3]).into_keys().next().unwrap()));
        assert_eq!(transact(&system, &mut entries, USER, &compact, workers), none);
        // The hold cannot now go back to before [5], and a second holder cannot ask for that either.
        assert_eq!(transact(&system, &mut entries, USER, &[hold(900, 4, &[3])], workers), broken(&["forgotten"]));
        assert_eq!(transact(&system, &mut entries, USER, &[hold(901, 4, &[3])], workers), broken(&["forgotten"]));
        // A computation is retired, and the ledgers it wrote are unbound: a user may add to them, even in the same transaction.
        assert_eq!(transact(&system, &mut entries, USER, &[stop(500, 100), stop(501, 100), retire(100), append(2, 1, &[7], &[8])], workers), none);
        // A ledger is sealed by a span that ends at the empty frontier: no span can follow it.
        assert_eq!(transact(&system, &mut entries, USER, &[append_to(1, 1, &frontier(&[9]), NEVER)], workers), none);
        assert_eq!(transact(&system, &mut entries, USER, &[append_to(1, 2, NEVER, NEVER)], workers), broken(&["malformed"]));
        // A ledger is dropped with its spans, once what referred to it is gone. A hold refers to it.
        let clear = [stop(501, 103), retire(103), retire(102), drop(4)];
        assert_eq!(transact(&system, &mut entries, USER, &clear, workers), broken(&["misheld"]));
        assert_eq!(transact(&system, &mut entries, USER, &[&clear[..], &[release(900, 4)]].concat(), workers), none);

        let out = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(out["standing"], rows(&[
            ("tuple(tuple(0,1))", "tuple(2,list())"), ("tuple(tuple(0,2))", "tuple(2,list(list(8)))"), ("tuple(tuple(0,3))", "tuple(1,list(list(7)))"),
            ("tuple(tuple(0,6))", "tuple(0,list(list(0)))"), ("tuple(tuple(0,7))", "tuple(0,list(list(0)))"),
            ("tuple(tuple(0,8))", "tuple(0,list(list(3)))"), ("tuple(tuple(0,9))", "tuple(1,list(list(3)))"),
        ]));
        assert_eq!(out["unbound"], keyed(&[1, 2, 3, 6, 7, 8, 9]));
        for view in ["bound", "plan", "idle", "needs"] { assert!(out[view].is_empty(), "{view}: {:?}", out[view]); }
        assert!(!entries.keys().any(|(entry, _)| entry.starts_with("tuple(inject(1,tuple(tuple(tuple(0,4),")), "the spans of ledger 4 are gone");
        // Admission was held to the rules both ways: on transactions it admitted, and on ones it turned away.
        assert!(ADMITTED.load(Ordering::Relaxed) > 0 && TURNED_AWAY.load(Ordering::Relaxed) > 0);
    }
}

/// A derived ledger is recorded as any ledger is, and has no spans: its contents are on the instances that write it.
#[test]
fn a_derived_ledger_is_recorded_and_not_stored() {
    let system = system();
    for workers in [1, 2] {
        let mut entries = Rows::new();
        let none = broken(&[]);

        // Ledgers 1 and 3 are durable, and 2 is derived. Programs 71 and 72 take one input and give one output; 73 gives two.
        // 100 reads 1 and writes 2; 101 reads 2 and writes 3. Instance 500 runs both.
        let setup = [
            create(1, 7, 1, &[0], None), create_derived(2, 7, &[0]), create(3, 7, 1, &[0], None),
            publish(71, &[7], &[7], 1), publish(72, &[7], &[7], 2), publish(73, &[7], &[7, 7], 3),
            define(100, 71, &[1], &[Some(2)]), define(101, 71, &[2], &[Some(3)]),
            start(500, 100), start(500, 101),
        ];
        assert_eq!(transact(&system, &mut entries, USER, &setup, workers), none);
        assert_eq!(transact(&system, &mut entries, USER, &[append(1, 0, &[0], &[9])], workers), none);
        assert_eq!(transact(&system, &mut entries, &runner(101), &[append(3, 0, &[0], &[4])], workers), none);

        // Only the durable ledgers stand somewhere. The derived ledger is bound as any other.
        let out = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(out["standing"], rows(&[("tuple(tuple(0,1))", "tuple(1,list(list(9)))"), ("tuple(tuple(0,3))", "tuple(1,list(list(4)))")]));
        assert_eq!(out["bound"], rows(&[("tuple(tuple(0,2))", "tuple(tuple(0,100),0)"), ("tuple(tuple(0,3))", "tuple(tuple(0,101),0)")]));
        assert_eq!(out["unbound"], keyed(&[1]));
        // 101 resumes from where ledger 3 stands, and needs ledger 2 from there.
        // So 100 must make ledger 2 again from there, and needs ledger 1 from there.
        assert_eq!(out["needs"], rows(&[
            ("tuple(tuple(0,1))", "tuple(tuple(0,100),tuple(0,3),list(list(4)))"),
            ("tuple(tuple(0,2))", "tuple(tuple(0,101),tuple(0,3),list(list(4)))"),
        ]));

        let standing = entries.clone();
        let refused: Vec<(&str, String, Vec<String>, &[&'static str])> = vec![
            ("a span added to a derived ledger", runner(100), vec![append(2, 0, &[0], &[1])], &["spanned"]),
            ("a derived ledger that nothing writes", USER.into(), vec![create_derived(4, 7, &[0])], &["hollow"]),
            // 101 resumes from [4], so 100 must make ledger 2 again from [4], and ledger 1 is needed from [4].
            ("the source of a derived ledger kept only from after where its reader resumes", USER.into(),
                vec![merge(1, 0, 1, &[5]), discard(1, 0, 1, &[0])], &["forgotten"]),
            ("a reader started where the writer does not run", USER.into(), vec![start(501, 101)], &["starved"]),
            ("the writer stopped where a reader runs", USER.into(), vec![stop(500, 100)], &["starved"]),
            ("the writer retired while the ledger remains", USER.into(), vec![stop(500, 100), retire(100)], &["hollow"]),
            ("a derived ledger dropped while it is read", USER.into(), vec![stop(500, 100), retire(100), drop(2)], &["unregistered"]),
            ("two computations that read each other through derived ledgers", USER.into(),
                vec![create_derived(5, 7, &[0]), create_derived(6, 7, &[0]), define(105, 71, &[5], &[Some(6)]), define(106, 71, &[6], &[Some(5)])], &["unfounded"]),
        ];
        for (what, actor, commands, rules) in refused {
            assert_eq!(transact(&system, &mut entries, &actor, &commands, workers), broken(rules), "{what}");
            assert_eq!(entries, standing, "{what}");
        }

        // The source may be kept from [4] alone.
        assert_eq!(transact(&system, &mut entries, USER, &[merge(1, 0, 1, &[4]), discard(1, 0, 1, &[0])], workers), none);
        // A second instance runs the reader, once it runs the writer too.
        assert_eq!(transact(&system, &mut entries, USER, &[start(501, 100), start(501, 101)], workers), none);
        // The writer is replaced by a computation of another program, and the reader is not touched: it names the ledger.
        let replace = [stop(500, 100), stop(501, 100), retire(100), define(102, 72, &[1], &[Some(2)]), start(500, 102), start(501, 102)];
        assert_eq!(transact(&system, &mut entries, USER, &replace, workers), none);
        let out = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(out["needs"], rows(&[
            ("tuple(tuple(0,1))", "tuple(tuple(0,102),tuple(0,3),list(list(4)))"),
            ("tuple(tuple(0,2))", "tuple(tuple(0,101),tuple(0,3),list(list(4)))"),
        ]));
        // A HOLD ON A DERIVED LEDGER is passed to the computation that writes it, and so to ledger 1.
        // Ledger 1 is kept from [4] alone, so ledger 2 cannot be asked for from [3].
        assert_eq!(transact(&system, &mut entries, USER, &[hold(900, 2, &[3])], workers), broken(&["forgotten"]));
        assert_eq!(transact(&system, &mut entries, USER, &[hold(900, 2, &[6])], workers), none);
        let out = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(out["needs"], rows(&[
            ("tuple(tuple(0,1))", "tuple(tuple(0,102),tuple(0,2),list(list(6)))"),
            ("tuple(tuple(0,1))", "tuple(tuple(0,102),tuple(0,3),list(list(4)))"),
            ("tuple(tuple(0,2))", "tuple(tuple(0,101),tuple(0,3),list(list(4)))"),
        ]));
        // Ledger 3 is appended to, and its reader then resumes from [8]. The hold is what keeps ledger 1 back, to [6].
        assert_eq!(transact(&system, &mut entries, &runner(101), &[append(3, 1, &[4], &[8])], workers), none);
        assert_eq!(transact(&system, &mut entries, USER, &[merge(1, 0, 1, &[7]), discard(1, 0, 1, &[4])], workers), broken(&["forgotten"]));
        assert_eq!(transact(&system, &mut entries, USER, &[merge(1, 0, 1, &[6]), discard(1, 0, 1, &[4])], workers), none);
        // A computation may write some of its outputs and not others, and an output not written binds no ledger.
        // Ledger 1 is now kept from [6] alone, so a new reader of it cannot write a ledger that starts before [6].
        assert_eq!(transact(&system, &mut entries, USER, &[create(7, 7, 1, &[0], None), define(103, 73, &[1], &[None, Some(7)])], workers), broken(&["forgotten"]));
        assert_eq!(transact(&system, &mut entries, USER, &[create(7, 7, 1, &[6], None), define(103, 73, &[1], &[None, Some(7)])], workers), none);
        let out = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(out["bound"], rows(&[
            ("tuple(tuple(0,2))", "tuple(tuple(0,102),0)"), ("tuple(tuple(0,3))", "tuple(tuple(0,101),0)"), ("tuple(tuple(0,7))", "tuple(tuple(0,103),1)"),
        ]));
    }
}

/// A computation may read the state, and what depends on the state is derived.
#[test]
fn a_computation_may_read_the_state() {
    let system = system();
    for workers in [1, 2] {
        let mut entries = Rows::new();
        let none = broken(&[]);

        // Ledgers 1 and 2 are derived, and 3 is durable. Program 71 takes one input and gives one output.
        // 100 reads the state and writes 1; 101 reads 1 and writes 2. Instance 500 runs both.
        let setup = [
            create_derived(1, 7, &[0]), create_derived(2, 7, &[0]), create(3, 7, 1, &[0], None),
            publish(71, &[7], &[7], 1),
            define(100, 71, &[STATE], &[Some(1)]), define(101, 71, &[1], &[Some(2)]),
            start(500, 100), start(500, 101),
        ];
        assert_eq!(transact(&system, &mut entries, USER, &setup, workers), none);
        let out = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(out["bound"], rows(&[("tuple(tuple(0,1))", "tuple(tuple(0,100),0)"), ("tuple(tuple(0,2))", "tuple(tuple(0,101),0)")]));
        assert_eq!(out["plan"], rows(&[
            ("tuple(tuple(0,500))", "tuple(tuple(0,100),list(71),list(inject(0,tuple())),list(inject(1,tuple(0,1))))"),
            ("tuple(tuple(0,500))", "tuple(tuple(0,101),list(71),list(inject(1,tuple(0,1))),list(inject(1,tuple(0,2))))"),
        ]));
        // Nothing durable is written, and nothing is held, so nothing is needed.
        assert!(out["needs"].is_empty());

        let standing = entries.clone();
        let refused: Vec<(&str, String, Vec<String>, &[&'static str])> = vec![
            ("a durable ledger written from the state", USER.into(), vec![define(102, 71, &[STATE], &[Some(3)])], &["belated"]),
            ("a durable ledger written from the state, through two derived ledgers", USER.into(), vec![define(102, 71, &[2], &[Some(3)])], &["belated"]),
            ("a reader of the state's ledger started where its writer does not run", USER.into(), vec![start(501, 101)], &["starved"]),
        ];
        for (what, actor, commands, rules) in refused {
            assert_eq!(transact(&system, &mut entries, &actor, &commands, workers), broken(rules), "{what}");
            assert_eq!(entries, standing, "{what}");
        }

        // A second computation reads the state, and writes nothing that is recorded.
        assert_eq!(transact(&system, &mut entries, USER, &[define(102, 71, &[STATE], &[None])], workers), none);
        // A hold on ledger 2 is passed back to 101, which needs ledger 1 from there, and then to 100, which reads no ledger.
        assert_eq!(transact(&system, &mut entries, USER, &[hold(900, 2, &[5])], workers), none);
        let out = evaluate(&system.state, &[lines(&entries)], workers);
        assert_eq!(out["needs"], rows(&[("tuple(tuple(0,1))", "tuple(tuple(0,101),tuple(0,2),list(list(5)))")]));
    }
}

/// Admission says what the rules say, of every append tried: each ledger of several kinds, at each of
/// several numbers and intervals, by a user and by a runner.
#[test]
fn admission_agrees_with_the_rules() {
    let system = system();
    let entries = small(&system, 1);
    // Ledger 1 is unbound, 2 and 4 are bound, 5 is empty, 8 follows a clock, and 77 is not registered.
    let (mut made, mut tried) = (0, 0);
    for ledger in [1, 2, 4, 5, 8, 77] {
        for number in [0, 1, 2] {
            for lower in [[0], [3], [7], [9]] {
                for upper in [[7], [12]] {
                    for actor in [USER.to_string(), runner(100)] {
                        // `transact` judges the append both ways, and fails if admission and the rules disagree.
                        let mut trial = entries.clone();
                        if transact(&system, &mut trial, &actor, &[append(ledger, number, &lower, &upper)], 1).is_empty() { made += 1; }
                        tried += 1;
                    }
                }
            }
        }
    }
    // Ledger 1 at its version and frontier, by either; 2 by its runner; 5 and 8 at theirs, to either upper, by either.
    assert_eq!((made, tried), (11, 288));
}

/// One transaction of a round: its key, the computation its maker runs if it is a runner's, and its appends,
/// each as (ledger, number, lower, upper).
type Asked = (i64, Option<i64>, Vec<(i64, i64, i64, i64)>);

/// The transactions an output of round.ddp names.
fn named(rows: &Rows) -> BTreeSet<i64> {
    rows.keys().map(|(key, _)| key.trim_start_matches("tuple(").trim_end_matches(')').parse().unwrap()).collect()
}

#[test]
fn a_round_makes_what_the_rules_make() {
    let system = system();
    let entries = small(&system, 1);
    // Where the ledgers stand: 1 at (1, [9]), unbound; 2 and 3 at (1, [7]), written by computation 100; 4 at (1, [5]), written by 101;
    // 5, 6 and 7 at (0, [0]); 9 is a clock at (1, [3]), and 8 follows it, at (0, [3]). Ledger 77 is not registered.
    let rounds: [(Vec<Asked>, &[i64], &[i64]); 4] = [
        (vec![
            (9, None, vec![(4, 1, 5, 6)]),                      // refused: a user is barred from a bound ledger
            (10, None, vec![(1, 1, 9, 12)]),
            (11, None, vec![(5, 0, 0, 4), (6, 0, 0, 4)]),
            (12, Some(100), vec![(2, 1, 7, 8), (3, 1, 7, 8)]),
            (13, Some(101), vec![(4, 1, 5, 6)]),                // waits: transaction 9 is before it on ledger 4
            (14, None, vec![(7, 0, 0, 2), (77, 0, 0, 2)]),      // refused: ledger 77 is absent, so ledger 7 is not appended to
            (15, None, vec![(7, 0, 0, 2)]),                     // waits, though the transaction before it is refused
            (16, None, vec![(8, 0, 3, 4)]),                     // the follower, before its clock
            (17, None, vec![(9, 1, 3, 5)]),                     // the clock, after its follower
            (18, None, vec![(1, 1, 9, 12)]),                    // waits
        ], &[13, 15, 18], &[9, 14]),
        (vec![
            (20, None, vec![(1, 0, 9, 12)]),                    // refused: stale, by its number
            (21, Some(101), vec![(4, 1, 5, 5)]),                // refused: empty
            (22, None, vec![(5, 0, 0, 1), (5, 1, 1, 2)]),       // refused: two appends to one ledger, which the rules alone would make
            (23, Some(100), vec![(2, 1, 7, 9)]),
            (24, Some(101), vec![(3, 1, 7, 9)]),                // refused: barred, as the runner of another computation
            (25, None, vec![(8, 0, 0, 4)]),                     // refused: stale, by its lower
            (26, None, vec![(6, 0, 0, 3)]),
        ], &[], &[20, 21, 22, 24, 25]),
        (vec![
            (30, None, vec![(9, 1, 3, 5)]),                     // the clock, before its follower
            (31, None, vec![(8, 0, 3, 4)]),                     // waits: transaction 30 appends to the clock that ledger 8 reads
            (32, None, vec![(1, 1, 9, 12)]),
        ], &[31], &[]),
        (vec![
            (40, None, vec![(9, 1, 3, 2)]),                     // refused: empty
            (41, None, vec![(5, 0, 0, 4), (8, 0, 3, 4)]),       // waits, though the append to its clock is refused
            (42, None, vec![(8, 0, 3, 4), (9, 1, 3, 5)]),       // waits: transaction 41 is before it on ledger 8, and 40 on ledger 9
            (43, None, vec![(6, 0, 0, 3)]),
        ], &[41, 42], &[40]),
    ];
    let was = evaluate(&system.state, &[lines(&entries)], 1);
    let index = evaluate(&system.index, &[lines(&entries)], 1);
    for workers in [1, 2] {
        for (asked, waiting, refused) in &rounds {
            let appends: Vec<String> = asked.iter().flat_map(|(tx, _, appends)| appends.iter().map(move |(ledger, number, lower, upper)|
                format!("{tx} val=tuple({},{number},{},{},inject(0,tuple()))", id(*ledger), frontier(&[*lower]), frontier(&[*upper])))).collect();
            let runners: Vec<String> = asked.iter().filter_map(|(tx, actor, _)| actor.map(|computation| format!("{tx} val=tuple({})", id(computation)))).collect();
            let out = evaluate(&system.round, &[appends, runners, lines(&index["own"]), lines(&was["bound"])], workers);
            assert_eq!(named(&out["waiting"]), waiting.iter().copied().collect());
            assert_eq!(named(&out["refused"]), refused.iter().copied().collect());

            // Each transaction the round judged, judged alone by the rules, against the state the round found.
            let actor = |actor: &Option<i64>| actor.map_or(USER.to_string(), runner);
            let commands = |appends: &[(i64, i64, i64, i64)]| -> Vec<String> {
                appends.iter().map(|(ledger, number, lower, upper)| append(*ledger, *number, &[*lower], &[*upper])).collect()
            };
            let mut made = Vec::new();
            for (tx, who, appends) in asked.iter().filter(|(tx, _, _)| !waiting.contains(tx)) {
                // A round takes appends as frames it does not open, so it refuses a transaction that appends to one
                // ledger twice, which would otherwise wait in every round. The rules alone would make it.
                let ledgers: BTreeSet<i64> = appends.iter().map(|(ledger, _, _, _)| *ledger).collect();
                if ledgers.len() < appends.len() {
                    assert!(refused.contains(tx), "a round refuses transaction {tx}, which appends to one ledger twice");
                    continue;
                }
                let mut trial = entries.clone();
                let alone = transact(&system, &mut trial, &actor(who), &commands(appends), workers).is_empty();
                assert_eq!(alone, !refused.contains(tx), "the round and the rules disagree on transaction {tx}");
                if alone { made.push((who, appends)); }
            }
            // The transactions the round made, by the rules, one after another in the order of their keys.
            let mut after = entries.clone();
            for (who, appends) in made {
                assert_eq!(transact(&system, &mut after, &actor(who), &commands(appends), workers), broken(&[]));
            }
            // They leave the state the round leaves, and the index the round leaves is what that state says.
            assert_eq!(after, changed(&entries, &Rows::new(), &out["spans"]));
            assert_eq!(evaluate(&system.index, &[lines(&after)], workers)["own"], changed(&index["own"], &Rows::new(), &out["moved"]));
        }
    }
}
