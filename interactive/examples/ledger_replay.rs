//! Replay an exact request fixture through the public Writer.
//! Usage: cargo run --release -p interactive --example ledger_replay -- INPUT memory|file:DIR serial|pipeline [WORKERS=4] [WARMUP=5]
//! Requests, reference outcomes, checkpoint seeding, startup, and final verification are outside timing.
//! Timed submissions include production validation, encoding, resolution, publication, and outcomes.
//! DDIRMAT1 fixtures contain six little-endian u64 header fields: ledgers, epochs, requests per epoch, seed, records, and commits.
//! Each record has five little-endian i64 fields: epoch, transaction key, ledger index, lower, and upper.
//! DDIR_SAMPLE_GATE=PATH writes the PID to PATH.with_extension("ready") after warmup, then waits for PATH.with_extension("go").
//! CPU totals cover measured batches; unavailable clocks report zero.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use interactive::ledger::{
    Change, Cohort, Failure, Frontier, Id, MemoryStore, Payload, Registration, Shape, State, Store, Time, Transaction,
    Writer,
};

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone, Copy)]
struct Request {
    txn: i64,
    ledger: usize,
    lower: i64,
    upper: i64,
}

struct Input {
    ids: usize,
    seed: u64,
    appends: usize,
    committed: usize,
    batches: Vec<Vec<Request>>,
}

fn at(value: i64) -> Frontier {
    std::iter::once(Time::new([value].as_slice().into())).collect()
}

fn word(bytes: &[u8], offset: usize) -> Result<u64, String> {
    let bytes = bytes.get(offset..offset + 8).ok_or("truncated input")?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read(path: &str) -> Result<Input, String> {
    let data = std::fs::read(path).map_err(|error| error.to_string())?;
    if data.get(..8) != Some(b"DDIRMAT1".as_slice()) {
        return Err("invalid request magic".into());
    }
    let number = |offset| usize::try_from(word(&data, offset)?).map_err(|error| error.to_string());
    let (ids, ticks, appends, seed, records, committed) =
        (number(8)?, number(16)?, number(24)?, word(&data, 32)?, number(40)?, number(48)?);
    if ids == 0
        || ticks == 0
        || appends == 0
        || records != ticks.checked_mul(appends).ok_or("count overflow")?
        || data.len() != 56usize.checked_add(records.checked_mul(40).ok_or("size overflow")?).ok_or("size overflow")?
    {
        return Err("request counts or file length disagree".into());
    }
    let mut batches = vec![Vec::with_capacity(appends); ticks];
    for index in 0..records {
        let offset = 56 + index * 40;
        let epoch = usize::try_from(word(&data, offset)?).map_err(|error| error.to_string())?;
        let ledger = usize::try_from(word(&data, offset + 16)?).map_err(|error| error.to_string())?;
        let request = Request {
            txn: word(&data, offset + 8)? as i64,
            ledger,
            lower: word(&data, offset + 24)? as i64,
            upper: word(&data, offset + 32)? as i64,
        };
        if epoch != index / appends + 1 || ledger >= ids || request.lower < 0 || request.upper <= request.lower {
            return Err("invalid request bounds, epoch, or ledger".into());
        }
        let batch = &mut batches[epoch - 1];
        if batch.last().is_some_and(|previous: &Request| previous.txn >= request.txn) {
            return Err("transaction keys must increase within each epoch".into());
        }
        batch.push(request);
    }
    Ok(Input { ids, seed, appends, committed, batches })
}

fn scalar(frontier: &Frontier) -> Result<i64, String> {
    let [point] = frontier.elements() else {
        return Err("expected singleton frontier".into());
    };
    let [value] = &point[..] else {
        return Err("expected one time coordinate".into());
    };
    Ok(*value)
}

fn main() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("INPUT required")?;
    let mode = args.next().unwrap_or_else(|| "memory".into());
    let schedule = args.next().unwrap_or_else(|| "serial".into());
    let workers = args.next().unwrap_or_else(|| "4".into()).parse::<usize>().map_err(|error| error.to_string())?;
    let warmup = args.next().unwrap_or_else(|| "5".into()).parse::<usize>().map_err(|error| error.to_string())?;
    if args.next().is_some() || workers == 0 || !matches!(schedule.as_str(), "serial" | "pipeline") {
        return Err("usage: INPUT memory|file:DIR serial|pipeline [WORKERS=4] [WARMUP=5]".into());
    }
    let input = read(&path)?;
    if warmup >= input.batches.len() {
        return Err("warmup must leave measured batches".into());
    }
    let prepared = Instant::now();
    let mut expected_uppers = vec![0; input.ids];
    let mut expected = Vec::new();
    let mut expected_total = 0;
    let batches = input
        .batches
        .iter()
        .map(|requests| {
            let mut outcomes = Vec::with_capacity(requests.len());
            let transactions = requests
                .iter()
                .map(|request| {
                    let upper = &mut expected_uppers[request.ledger];
                    outcomes.push(if request.lower == *upper {
                        *upper = request.upper;
                        expected_total += 1;
                        Ok(())
                    } else {
                        Err(*upper)
                    });
                    (
                        request.txn,
                        vec![(
                            Id::from_u128(request.ledger as u128 + 1),
                            Change::Append {
                                lower: at(request.lower),
                                upper: at(request.upper),
                                batch: Payload::Empty,
                            },
                        )],
                    )
                })
                .collect::<Vec<Transaction>>();
            expected.push(outcomes);
            transactions
        })
        .collect::<Vec<_>>();
    if expected_total != input.committed {
        return Err("request replay differs from fixture commit count".into());
    }
    let store: Arc<dyn Store> = match mode.as_str() {
        "memory" => Arc::new(MemoryStore::default()),
        mode if mode.starts_with("file:") && mode.len() > 5 => {
            #[cfg(unix)]
            { Arc::new(interactive::ledger::FileStore::open(&mode[5..]).map_err(|error| error.to_string())?) }
            #[cfg(not(unix))]
            { return Err("filesystem mode requires Unix".into()); }
        }
        _ => return Err("mode must be memory or file:DIR".into()),
    };
    if store.read_root("replay").map_err(|error| format!("{error:?}"))?.version != 0 {
        return Err("replay cohort already exists; select a fresh directory".into());
    }
    let shape = Shape { columns: corgi::Shape::Int, time_width: 1 };
    let origin = at(0);
    let checkpoint = State {
        registry: (0..input.ids)
            .map(|number| {
                (Id::from_u128(number as u128 + 1), Registration { shape: shape.clone(), start: origin.clone() })
            })
            .collect::<BTreeMap<_, _>>(),
        manifest: Vec::new(),
    };
    let bytes = bincode::serialize(&checkpoint).map_err(|error| error.to_string())?;
    let hash = store.put(&bytes).map_err(|error| format!("{error:?}"))?;
    if !store.cas_root("replay", 0, hash).map_err(|error| format!("{error:?}"))? {
        return Err("seed CAS lost".into());
    }
    drop(bytes);
    drop(checkpoint);
    let mut writer = Writer::open(Arc::clone(&store), "replay", workers).map_err(|error| format!("{error:?}"))?;
    let setup = prepared.elapsed();
    let mut outcomes = Vec::with_capacity(batches.len());
    let pipeline = schedule == "pipeline";
    let run = |writer: &mut Writer, range: std::ops::Range<usize>, outcomes: &mut Vec<_>| -> Result<(), String> {
        if pipeline {
            let receipts = range
                .map(|epoch| writer.submit(&batches[epoch]).map_err(|error| format!("{error:?}")))
                .collect::<Result<Vec<_>, _>>()?;
            for receipt in receipts {
                outcomes.push(writer.wait(receipt).map_err(|error| format!("{error:?}"))?);
            }
        } else {
            for epoch in range {
                outcomes.push(writer.transact_batch(&batches[epoch]).map_err(|error| format!("{error:?}"))?);
            }
        }
        Ok(())
    };
    run(&mut writer, 0..warmup, &mut outcomes)?;
    sample_gate()?;
    let (process_cpu, main_cpu) = cpu_now();
    let start = Instant::now();
    run(&mut writer, warmup..batches.len(), &mut outcomes)?;
    let elapsed = start.elapsed();
    let (process_end, main_end) = cpu_now();
    let process_cpu = if process_cpu == 0 { 0 } else { process_end.saturating_sub(process_cpu) };
    let main_cpu = if main_cpu == 0 { 0 } else { main_end.saturating_sub(main_cpu) };
    drop(writer);
    let verify = Instant::now();
    let mut committed = 0;
    let mut measured_committed = 0;
    for (epoch, ((requests, expected), actual)) in input.batches.iter().zip(&expected).zip(&outcomes).enumerate() {
        if actual.len() != requests.len() {
            return Err(format!("outcome count differs in epoch {}", epoch + 1));
        }
        for (request, expected) in requests.iter().zip(expected) {
            let actual = actual.get(&request.txn).ok_or("missing outcome")?;
            match (expected, actual) {
                (Ok(()), Ok(())) => {
                    committed += 1;
                    if epoch >= warmup {
                        measured_committed += 1;
                    }
                }
                (Err(expected), Err(rejection)) if rejection.group == 0 => match &rejection.reason {
                    Failure::Write(actual) if scalar(actual)? == *expected => (),
                    _ => {
                        return Err(format!(
                            "rejection differs at epoch {}, txn {}: {actual:?}",
                            epoch + 1,
                            request.txn
                        ))
                    }
                },
                _ => return Err(format!("outcome differs at epoch {}, txn {}: {actual:?}", epoch + 1, request.txn)),
            }
        }
    }
    if committed != input.committed {
        return Err("committed count differs".into());
    }
    let state = Cohort::new(store.as_ref(), "replay").snapshot().map_err(|error| format!("{error:?}"))?;
    if state.registry.len() != input.ids || state.manifest.len() != committed {
        return Err("final state counts differ".into());
    }
    for (number, (id, registration)) in state.registry.iter().enumerate() {
        if *id != Id::from_u128(number as u128 + 1) || registration.shape != shape || registration.start != origin {
            return Err("final registry differs".into());
        }
    }
    let mut actual_uppers = vec![0; input.ids];
    for span in &state.manifest {
        let ledger = usize::try_from(span.id.as_u128().checked_sub(1).ok_or("invalid span id")?)
            .map_err(|error| error.to_string())?;
        let upper = actual_uppers.get_mut(ledger).ok_or("span id outside seeded range")?;
        if scalar(&span.lower)? != *upper || span.since != origin || span.batch != Payload::Empty {
            return Err("final span chain differs".into());
        }
        *upper = scalar(&span.upper)?;
    }
    if actual_uppers != expected_uppers {
        return Err("final uppers differ".into());
    }
    let attempts = (batches.len() - warmup) * input.appends;
    println!("mode={mode} schedule={schedule} allocator=mimalloc ids={} seed={} ticks={} appends={} workers={workers} warmup={warmup}",
        input.ids, input.seed, batches.len(), input.appends);
    println!("setup_ms={:.3} measured_ms={:.3} measured_attempts={attempts} measured_committed={measured_committed} attempts_s={:.3} committed_s={:.3} verify_ms={:.3} total_committed={committed}",
        setup.as_secs_f64() * 1000.0, elapsed.as_secs_f64() * 1000.0,
        attempts as f64 / elapsed.as_secs_f64(), measured_committed as f64 / elapsed.as_secs_f64(), verify.elapsed().as_secs_f64() * 1000.0);
    println!("process_cpu_ms={:.3} main_cpu_ms={:.3}", process_cpu as f64 / 1_000_000.0, main_cpu as f64 / 1_000_000.0);
    Ok(())
}

fn sample_gate() -> Result<(), String> {
    if let Some(path) = std::env::var_os("DDIR_SAMPLE_GATE") {
        let path = std::path::Path::new(&path);
        std::fs::write(path.with_extension("ready"), std::process::id().to_string()).map_err(|error| error.to_string())?;
        while !path.with_extension("go").exists() {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn cpu_now() -> (u64, u64) {
    fn clock(id: libc::clockid_t) -> u64 {
        let mut time = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        if unsafe { libc::clock_gettime(id, &mut time) } != 0 { return 0; }
        time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
    }
    (clock(libc::CLOCK_PROCESS_CPUTIME_ID), clock(libc::CLOCK_THREAD_CPUTIME_ID))
}

#[cfg(not(unix))]
fn cpu_now() -> (u64, u64) { (0, 0) }
