//! Measure the keyed resolver and publication path on disjoint multi-ledger transactions.
//! Usage: cargo run --release -p interactive --example ledger_resolve -- MODE [TXNS WIDTH ROUNDS WORKERS [serial|pipeline]]
//! MODE is resolve, memory, or file:PATH; defaults are memory, 16, 2, 5, and 1.
//! Appends have empty payloads, so the measurement covers span metadata and publication.
//! Resolve mode starts a worker group per batch; memory/file modes retain a Writer.
//! Pipeline mode overlaps resolution with ordered durable group commits.

use std::collections::BTreeMap;
use std::time::Instant;
use std::sync::Arc;

use interactive::ledger::{Change, Cohort, Frontier, Id, MemoryStore, Payload, Rejection, Resolver, Shape, State, Store, Time, Transaction, Writer};

fn at(epoch: i64) -> Frontier { std::iter::once(Time::new([epoch].as_slice().into())).collect() }

fn count(args: &mut impl Iterator<Item = String>, default: usize) -> Result<usize, String> {
    let value = args.next().map_or(Ok(default), |arg| arg.parse::<usize>().map_err(|error| error.to_string()))?;
    if value == 0 { return Err("counts must be positive".into()); }
    Ok(value)
}

fn main() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "memory".into());
    let txns = count(&mut args, 16)?;
    let width = count(&mut args, 2)?;
    let rounds = count(&mut args, 5)?;
    let workers = count(&mut args, 1)?;
    let pipeline = match args.next().as_deref() {
        None | Some("serial") => false,
        Some("pipeline") if mode != "resolve" => true,
        _ => return Err("schedule must be serial or pipeline (memory/file modes)".into()),
    };
    if args.next().is_some() { return Err("unexpected trailing argument".into()); }
    let ledgers = txns.checked_mul(width).ok_or("ledger count overflow")?;
    let last_epoch = i64::try_from(rounds).map_err(|error| error.to_string())?.checked_add(1).ok_or("epoch overflow")?;
    let store: Arc<dyn Store> = match mode.as_str() {
        "resolve" | "memory" => Arc::new(MemoryStore::default()),
        mode if mode.starts_with("file:") => {
            if mode.len() == 5 { return Err("filesystem mode requires a directory".into()); }
            #[cfg(unix)]
            { Arc::new(interactive::ledger::FileStore::open(&mode[5..]).map_err(|error| error.to_string())?) }
            #[cfg(not(unix))]
            { return Err("filesystem mode requires Unix".into()); }
        }
        _ => return Err("MODE must be resolve, memory, or file:PATH".into()),
    };
    let cohort = Cohort::new(store.as_ref(), "benchmark");
    if cohort.snapshot().map_err(|error| format!("{error:?}"))? != State::default() {
        return Err("benchmark cohort already contains data; choose a fresh filesystem directory".into());
    }
    let compiled = Instant::now();
    let resolver = if mode == "resolve" { Some(Resolver::new(workers).map_err(|error| format!("{error:?}"))?) } else { None };
    let mut writer = if mode == "resolve" { None } else {
        Some(Writer::open(Arc::clone(&store), "benchmark", workers).map_err(|error| format!("{error:?}"))?)
    };
    let setup = compiled.elapsed();
    let shape = Shape { columns: corgi::Shape::Int, time_width: 1 };
    let ids = (0..ledgers).map(|n| Id::from_u128(n as u128 + 1)).collect::<Vec<_>>();
    let creations = vec![(0, ids.iter().map(|id| (*id, Change::Create { shape: shape.clone(), start: at(0) })).collect())];
    let batch = |epoch: i64| -> Vec<Transaction> {
        ids.chunks(width).enumerate().map(|(txn, ids)| (
            i64::try_from(txn).unwrap(),
            ids.iter().map(|id| (*id, Change::Append { lower: at(epoch), upper: at(epoch + 1), batch: Payload::Empty })).collect(),
        )).collect()
    };
    let mut state = State::default();
    let elapsed = {
        let mut submit = |transactions: &[Transaction]| -> Result<BTreeMap<i64, Result<(), Rejection>>, String> {
            if mode == "resolve" {
                let resolution = resolver.as_ref().unwrap().resolve(&state, transactions).map_err(|error| format!("{error:?}"))?;
                state = resolution.state;
                Ok(resolution.outcomes)
            } else {
                writer.as_mut().unwrap().transact_batch(transactions).map_err(|error| format!("{error:?}"))
            }
        };
        let check = |outcomes: BTreeMap<i64, Result<(), Rejection>>, expected: usize| -> Result<(), String> {
            if outcomes.len() != expected || outcomes.values().any(Result::is_err) {
                return Err(format!("unexpected transaction outcomes: {outcomes:?}"));
            }
            Ok(())
        };
        check(submit(&creations)?, 1)?;
        check(submit(&batch(0))?, txns)?;
        let batches = (1..last_epoch).map(batch).collect::<Vec<_>>();
        let start = Instant::now();
        if pipeline {
            drop(submit);
            let writer = writer.as_mut().unwrap();
            let receipts = batches.iter().map(|transactions| writer.submit(transactions).map_err(|error| format!("{error:?}")))
                .collect::<Result<Vec<_>, _>>()?;
            for receipt in receipts { check(writer.wait(receipt).map_err(|error| format!("{error:?}"))?, txns)?; }
        } else {
            for transactions in batches { check(submit(&transactions)?, txns)?; }
        }
        start.elapsed()
    };
    if mode != "resolve" { state = cohort.snapshot().map_err(|error| format!("{error:?}"))?; }
    let expected_spans = ledgers.checked_mul(rounds.checked_add(1).ok_or("round count overflow")?).ok_or("span count overflow")?;
    if state.registry.len() != ledgers || state.manifest.len() != expected_spans
        || ids.iter().any(|id| state.write(*id) != Some(&at(last_epoch))) {
        return Err("final state differs from the submitted history".into());
    }
    let transactions = txns.checked_mul(rounds).ok_or("transaction count overflow")?;
    let spans = transactions.checked_mul(width).ok_or("append count overflow")?;
    println!("mode={mode} transactions_per_batch={txns} ledgers_per_transaction={width} workers={workers} rounds={rounds} pipeline={pipeline} payload=empty");
    println!("setup_ms={:.3} elapsed_ms={:.3} transactions_s={:.3} appends_s={:.3}",
        setup.as_secs_f64() * 1000.0, elapsed.as_secs_f64() * 1000.0,
        transactions as f64 / elapsed.as_secs_f64(), spans as f64 / elapsed.as_secs_f64());
    Ok(())
}
