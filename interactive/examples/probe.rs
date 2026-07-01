//! Tiny batch probe: parse a .ddp, feed a full belt (line 0..n -> drain, all
//! cells occupied), run to quiescence, print each export's rows.
//!   BELT_N=8 cargo run --release --example probe -- examples/programs/closure_test.ddp

use interactive::{parse, lower};
use interactive::backend::vec::evaluate;
use interactive::ir::Value;

fn main() {
    let path = std::env::args().nth(1).expect("usage: probe <program.ddp>");
    let n: i64 = std::env::var("BELT_N").ok().and_then(|s| s.parse().ok()).unwrap_or(8);

    let src = interactive::load_program(&path);
    let stmts = parse::pipe::parse(&src);
    let mut tree = lower::lower_tree(stmts);
    if std::env::var("BELT_NOOPT").is_err() { tree.optimize(); }

    let edges: Vec<_> = (0..n).map(|i| (Value::Tuple(vec![Value::Int(i), Value::Int(i + 1)]), Value::unit())).collect();
    let occ: Vec<_> = (0..n).map(|i| (Value::Tuple(vec![Value::Int(i)]), Value::unit())).collect();

    let out = evaluate(&tree, &[edges, occ]);
    for (name, rows) in out {
        println!("export {:?}: {} rows", name, rows.len());
        for ((k, v), d) in rows.iter().take(60) {
            println!("   {:?}  ;  {:?}   x{}", k, v, d);
        }
    }
}
