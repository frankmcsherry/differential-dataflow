//! Durable ledgers: `save <trace> to <ledger>` writes a trace's changes to a ledger at each tick, and
//! `restore <prog> <in#> from <ledger>` feeds a ledger's contents into an input.
//!
//! A ledger is one named, durable changelog in the store `DDIR_STORE` names
//! ([`interactive::storage`]). A save is a standing subscription, like `tail`: every worker sends
//! its share of the trace's changes to worker 0, which, each time the trace's frontier advances,
//! writes the changes before it as one batch and appends it to the ledger by compare-and-swap.
//! `tick` waits for the saves, so a batch is durable before `tick` answers.
//!
//! A save resumes a ledger rather than repeating it. Its first batch is the trace's contents less
//! what the ledger already holds, so a server restarted from its ledgers, saving the same traces
//! again, appends only what changed. If another writer appends to the ledger, the save stops with
//! an error rather than write over it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::OnceLock;

use differential_dataflow::operators::arrange::ShutdownButton;
use interactive::runtime::container::Updates;
use interactive::runtime::exchange::GatherPact;
use interactive::runtime::frame;
use interactive::server::{host_time, HostContainer, HostTime, RowShape, Server};
use interactive::storage::{self, FileStore, Store};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::{CapabilitySet, Probe};
use timely::worker::Worker;

/// The process's store: the directory `DDIR_STORE` names.
pub fn store() -> Result<&'static dyn Store, String> {
    static STORE: OnceLock<Result<FileStore, String>> = OnceLock::new();
    STORE
        .get_or_init(|| match storage::from_env() {
            Some(Ok(store)) => Ok(store),
            Some(Err(error)) => Err(format!("DDIR_STORE: {error}")),
            None => Err("no durable store: set DDIR_STORE to a directory".into()),
        })
        .as_ref()
        .map(|store| store as &dyn Store)
        .map_err(Clone::clone)
}

/// A shape as a ledger records it: `(key, val)`, as corgi prints shapes.
pub fn shape_text((key, val): &RowShape) -> String {
    format!("({key}, {val})")
}

/// Ledger `name`'s contents as rows of `shape`, consolidated: what a restore feeds.
pub fn contents(name: &str, shape: &RowShape) -> Result<Updates, String> {
    let store = store()?;
    let (_, ledger) = storage::read_ledger(store, name).map_err(|e| format!("ledger {name:?}: {e}"))?;
    let Some(ledger) = ledger else { return Err(format!("no ledger {name:?}")) };
    if ledger.shape != shape_text(shape) {
        return Err(format!("ledger {name:?} holds rows of shape {}, not {}", ledger.shape, shape_text(shape)));
    }
    let mut parts = Vec::with_capacity(ledger.batches.len());
    for (lower, _, blob) in &ledger.batches {
        let bytes = store.get(blob).map_err(|e| format!("ledger {name:?} batch {lower}: {e}"))?;
        parts.push(frame::decode_feed(&bytes, shape, u64::MAX).map_err(|e| format!("ledger {name:?} batch {lower}: {e}"))?);
    }
    Ok(Updates::consolidate(parts, shape))
}

/// What a save has done, kept on worker 0.
#[derive(Default)]
pub struct SaveStatus {
    /// The ledger's root version this save last wrote or read.
    version: u64,
    /// Batches this save has appended.
    batches: u64,
    /// Why the save stopped writing, if it has.
    error: Option<String>,
}

/// A standing save of a trace to a ledger.
pub struct Save {
    dataflow_id: usize,
    _shutdown: ShutdownButton<CapabilitySet<HostTime>>,
    pub trace: String,
    pub probe: ProbeHandle<HostTime>,
    status: Rc<RefCell<SaveStatus>>,
}

/// Start saving `trace` to `ledger`, and return once the trace's contents through the current epoch
/// are durable in it (less what the ledger already held).
pub fn start_save(trace: &str, ledger: &str, server: &Server, saves: &mut HashMap<String, Save>, worker: &mut Worker) -> Result<(), String> {
    if saves.contains_key(ledger) {
        return Err(format!("ledger {ledger:?} is already being saved"));
    }
    let mut published = server.published(trace).ok_or_else(|| format!("no trace {trace:?}"))?;
    let shape = server.trace_shape(trace).expect("a published trace has a shape");
    let shape_name = shape_text(&shape);
    let status = Rc::new(RefCell::new(SaveStatus::default()));
    // Worker 0 writes, so only worker 0 reads the ledger: its version, and what it already holds,
    // negated, to subtract from the first batch. Every worker builds the dataflow.
    let mut held = None;
    if worker.index() == 0 {
        let store = store()?;
        let (version, existing) = storage::read_ledger(store, ledger).map_err(|e| format!("ledger {ledger:?}: {e}"))?;
        if let Some(existing) = &existing {
            if existing.shape != shape_name {
                return Err(format!("ledger {ledger:?} holds rows of shape {}, not {shape_name}", existing.shape));
            }
        }
        let mut contents = if existing.is_some() { contents(ledger, &shape)? } else { Updates::consolidate(Vec::new(), &shape) };
        for diff in contents.diffs.iter_mut() {
            *diff = -*diff;
        }
        *status.borrow_mut() = SaveStatus { version, ..Default::default() };
        held = Some(contents);
    }

    let dataflow_id = worker.next_dataflow_index();
    let probe = ProbeHandle::new();
    let (writer, name) = (Rc::clone(&status), ledger.to_string());
    // The first batch waits for the trace's contents through the epoch closed at the save.
    let start = server.epoch();
    let shutdown = interactive::server::host_dataflow(worker, |scope| {
        let (containers, shutdown) = published.import_containers(scope, "SaveImport", server.import_as_of());
        // Changes not yet before the frontier: their updates and epochs.
        let mut pending: Vec<(Updates, Vec<u64>)> = Vec::new();
        let mut written_to = None::<u64>;
        containers
            .inner
            .unary_frontier::<timely::container::CapacityContainerBuilder<Vec<()>>, _, _, _>(GatherPact, "Save", move |capability, _info| {
                drop(capability);
                move |(input, frontier), _output| {
                    input.for_each(|_time, data| {
                        let HostContainer { keys, vals, times, diffs } = std::mem::take(data);
                        let epochs = (0..times.len()).map(|i| times.get(i).first().copied().unwrap_or(0)).collect();
                        pending.push((Updates { keys, vals, diffs }, epochs));
                    });
                    let Some(held) = held.as_mut() else { return };  // workers other than 0 write nothing
                    let Some(to) = frontier.frontier().iter().map(|t| t.first().copied().unwrap_or(0)).min() else { return };
                    if to < start || written_to == Some(to) || writer.borrow().error.is_some() {
                        return;
                    }
                    written_to = Some(to);
                    // Everything before the frontier is final: it is this batch.
                    let mut parts = Vec::new();
                    let mut later = Vec::new();
                    for (updates, epochs) in pending.drain(..) {
                        let (before, after): (Vec<usize>, Vec<usize>) = (0..epochs.len()).partition(|&i| epochs[i] < to);
                        if !after.is_empty() {
                            later.push((updates.select(&after), after.iter().map(|&i| epochs[i]).collect()));
                        }
                        parts.push(if after.is_empty() { updates } else { updates.select(&before) });
                    }
                    pending = later;
                    // The first batch subtracts what the ledger already held; `held` is then empty.
                    parts.push(std::mem::replace(held, Updates::consolidate(Vec::new(), &shape)));
                    let batch = Updates::consolidate(parts, &shape);
                    if batch.is_empty() {
                        return;
                    }
                    let mut saved = writer.borrow_mut();
                    let result = store().and_then(|store| {
                        storage::append(store, &name, saved.version, &shape_name, &frame::encode(&batch, &[])).map_err(|e| format!("ledger {name:?}: {e}"))
                    });
                    match result {
                        Ok((version, _)) => {
                            saved.version = version;
                            saved.batches += 1;
                        }
                        Err(error) => {
                            eprintln!("ddir_server: save to {error}; the save has stopped");
                            saved.error = Some(error);
                        }
                    }
                }
            })
            .probe_with(&probe);
        shutdown
    });
    // As a tail does: drive the new import through the closed epoch, so the first batch is
    // durable before the save is acknowledged.
    let epoch = server.epoch();
    while probe.less_than(&host_time(epoch)) {
        worker.step();
    }
    if let Some(error) = status.borrow().error.clone() {
        worker.drop_dataflow(dataflow_id);
        return Err(error);
    }
    saves.insert(ledger.to_string(), Save { dataflow_id, _shutdown: shutdown, trace: trace.to_string(), probe, status });
    Ok(())
}

/// Stop saving to `ledger`.
pub fn stop_save(ledger: &str, saves: &mut HashMap<String, Save>, worker: &mut Worker) -> Result<(), String> {
    let save = saves.remove(ledger).ok_or_else(|| format!("ledger {ledger:?} is not being saved"))?;
    worker.drop_dataflow(save.dataflow_id);
    Ok(())
}

/// What the store holds for `ledger`, and what its save, if any, has done: one line.
pub fn describe(ledger: &str, saves: &HashMap<String, Save>) -> Result<String, String> {
    let (version, stored) = storage::read_ledger(store()?, ledger).map_err(|e| format!("ledger {ledger:?}: {e}"))?;
    let mut text = match stored {
        Some(s) => format!("ledger={ledger:?} version={version} upper={} batches={} shape={}", s.upper(), s.batches.len(), s.shape),
        None => format!("ledger={ledger:?} version=0 upper=0 batches=0"),
    };
    if let Some(save) = saves.get(ledger) {
        let status = save.status.borrow();
        text.push_str(&format!(" saving={:?} appended={}", save.trace, status.batches));
        if let Some(error) = &status.error {
            text.push_str(&format!(" error={error:?}"));
        }
    }
    Ok(text)
}
