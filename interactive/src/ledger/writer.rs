//! Resident resolution with one ordered publication in flight.
//! Receipts become available only after the root CAS commits their conclusions.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread::JoinHandle;

use super::engine::Engine;
use super::log::{self, Delta};
use super::resolve;
use super::{Change, Error, Id, Payload, Rejection, Resolver, Root, State, Store, StoreError, Transaction};
use crate::runtime::container::Updates;

type Outcomes = BTreeMap<i64, Result<(), Rejection>>;

/// A local submission token, consumed by its writer's `wait` method.
/// It is not a durable transaction identifier.
pub struct Receipt { owner: Id, number: u64 }

struct Batch {
    number: u64,
    transactions: Vec<Transaction>,
    delta: Delta,
    outcomes: Outcomes,
}

struct Flight {
    batches: Vec<Batch>,
    thread: JoinHandle<Result<Option<Root>, Error>>,
}

/// A cohort writer with a resident frontier index and columnar conclusion log.
/// `submit` may resolve against earlier unacknowledged submissions.
/// `wait` and `flush` drive retries and ordered group commits.
/// A publication error stops further submissions; reopen from the committed root.
/// Dropping a writer joins its in-flight publication and discards queued, unpublished work.
pub struct Writer {
    store: Arc<dyn Store>,
    name: String,
    resolver: Resolver,
    engine: Option<Engine>,
    root: Root,
    width: usize,
    widths: BTreeMap<Id, usize>,
    pending: Vec<Batch>,
    flight: Option<Flight>,
    completed: BTreeMap<u64, Result<Outcomes, Error>>,
    failure: Option<Error>,
    owner: Id,
    next: u64,
}

impl Writer {
    pub fn open(store: Arc<dyn Store>, root: impl Into<String>, workers: usize) -> Result<Self, Error> {
        let name = root.into();
        let resolver = Resolver::new(workers)?;
        let (root, state) = log::load(store.as_ref(), &name)?;
        let width = state.registry.values().map(|entry| entry.shape.time_width).max().unwrap_or(0);
        let widths = state.registry.iter().map(|(id, entry)| (*id, entry.shape.time_width)).collect();
        let engine = Engine::new(&resolver, initial(&state, width))?;
        Ok(Self { store, name, resolver, engine: Some(engine), root, width, widths,
            pending: Vec::new(), flight: None, completed: BTreeMap::new(), failure: None,
            owner: Id::new_v4(), next: 0 })
    }

    /// Resolve a batch and queue its conclusions; this does not acknowledge a commit.
    /// Keys need be unique only within this batch.
    pub fn submit(&mut self, transactions: &[Transaction]) -> Result<Receipt, Error> {
        resolve::unique_keys(transactions)?;
        let number = self.next;
        self.next = self.next.checked_add(1).ok_or(Error::Invalid("submission counter exhausted"))?;
        let receipt = Receipt { owner: self.owner, number };
        if transactions.is_empty() {
            self.completed.insert(number, Ok(BTreeMap::new()));
            return Ok(receipt);
        }
        if let Some(error) = &self.failure { return Err(error.clone()); }
        self.poll()?;
        let required = resolve::maximum_width(&[], transactions);
        if required > self.width {
            self.flush()?;
            self.reload(required)?;
        }
        match self.resolve(number, transactions.to_vec()) {
            Ok(batch) => self.pending.push(batch),
            Err(error) => {
                // Earlier valid submissions still own their publication decisions.
                let _ = self.flush();
                self.failure = Some(error.clone());
                return Err(error);
            }
        }
        if self.flight.is_none() { self.start()?; }
        Ok(receipt)
    }

    /// Wait for all queued work, then consume this receipt's committed result.
    pub fn wait(&mut self, receipt: Receipt) -> Result<Outcomes, Error> {
        if receipt.owner != self.owner { return Err(Error::Invalid("receipt belongs to another writer")); }
        let flushed = self.flush();
        self.completed.remove(&receipt.number)
            .unwrap_or_else(|| flushed.and(Err(Error::Invalid("unknown receipt"))))
    }

    pub fn transact_batch(&mut self, transactions: &[Transaction]) -> Result<Outcomes, Error> {
        let receipt = self.submit(transactions)?;
        self.wait(receipt)
    }

    /// Finish every queued publication, grouping batches that accumulated during a prior commit.
    pub fn flush(&mut self) -> Result<(), Error> {
        while self.flight.is_some() || !self.pending.is_empty() {
            if self.flight.is_none() { self.start()?; }
            self.settle()?;
        }
        self.failure.clone().map_or(Ok(()), Err)
    }

    fn poll(&mut self) -> Result<(), Error> {
        if self.flight.as_ref().is_some_and(|flight| flight.thread.is_finished()) { self.settle()?; }
        Ok(())
    }

    fn start(&mut self) -> Result<(), Error> {
        if self.pending.is_empty() { return Ok(()); }
        let batches = std::mem::take(&mut self.pending);
        let deltas = batches.iter().map(|batch| batch.delta.clone()).collect::<Vec<_>>();
        let (store, name, root) = (Arc::clone(&self.store), self.name.clone(), self.root);
        match std::thread::Builder::new().name("ledger-publish".into())
            .spawn(move || log::publish(store.as_ref(), &name, root, &deltas)) {
            Ok(thread) => { self.flight = Some(Flight { batches, thread }); Ok(()) }
            Err(error) => {
                self.pending = batches;
                let error = Error::Store(StoreError::Unavailable(error.to_string()));
                self.cancel(error.clone());
                Err(error)
            }
        }
    }

    fn settle(&mut self) -> Result<(), Error> {
        let Some(flight) = self.flight.take() else { return Ok(()); };
        let published = flight.thread.join().unwrap_or(Err(Error::Store(StoreError::CommitUnknown)));
        match published {
            Ok(Some(root)) => {
                self.root = root;
                for batch in flight.batches { self.completed.insert(batch.number, Ok(batch.outcomes)); }
                Ok(())
            }
            Ok(None) => {
                let mut retry = flight.batches;
                retry.append(&mut self.pending);
                let width = self.width;
                if let Err(error) = self.reload(width) {
                    self.pending = retry;
                    self.cancel(error.clone());
                    return Err(error);
                }
                let mut retry = retry.into_iter();
                while let Some(batch) = retry.next() {
                    match self.resolve(batch.number, batch.transactions) {
                        Ok(batch) => self.pending.push(batch),
                        Err(error) => {
                            self.completed.insert(batch.number, Err(error.clone()));
                            self.pending.extend(retry);
                            self.cancel(error.clone());
                            return Err(error);
                        }
                    }
                }
                Ok(())
            }
            Err(error) => {
                for batch in flight.batches { self.completed.insert(batch.number, Err(error.clone())); }
                self.cancel(error.clone());
                Err(error)
            }
        }
    }

    fn cancel(&mut self, error: Error) {
        for batch in self.pending.drain(..) {
            self.completed.insert(batch.number, Err(Error::Store(StoreError::Unavailable(
                "an earlier publication failed; this batch was not attempted".into()))));
        }
        self.failure = Some(error);
    }

    fn reload(&mut self, minimum_width: usize) -> Result<(), Error> {
        let (root, state) = log::load(self.store.as_ref(), &self.name)?;
        let width = minimum_width.max(state.registry.values().map(|entry| entry.shape.time_width).max().unwrap_or(0));
        self.engine.take();
        self.engine = Some(Engine::new(&self.resolver, initial(&state, width))?);
        self.root = root;
        self.width = width;
        self.widths = state.registry.iter().map(|(id, entry)| (*id, entry.shape.time_width)).collect();
        Ok(())
    }

    fn resolve(&mut self, number: u64, transactions: Vec<Transaction>) -> Result<Batch, Error> {
        let outputs = self.engine.as_mut().ok_or(Error::Invalid("writer has no evaluator"))?
            .resolve(resolve::commands(&transactions, self.width)?)?;
        let mut results = resolve::output(&outputs, "results")?.clone();
        results.keys = resolve::wide(results.keys)?;
        results.vals = resolve::wide(results.vals)?;
        let outcomes = resolve::outcomes(&results, &transactions)?;
        let delta = Delta { registry: resolve::output(&outputs, "registry_add")?.clone(),
            manifest: resolve::output(&outputs, "manifest_ordered")?.clone() };
        let mut ordered = transactions.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|(key, _)| *key);
        let mut created = BTreeMap::new();
        let mut spans = 0;
        for (key, groups) in ordered {
            if outcomes[key].is_err() { continue; }
            for (id, change) in groups {
                let (width, bounds, payload) = match change {
                    Change::Create { shape, start } => {
                        created.insert(*id, shape.time_width);
                        (shape.time_width, [Some(start), None], None)
                    }
                    Change::CreateAppend { shape, start, upper, batch } => {
                        created.insert(*id, shape.time_width);
                        spans += 1;
                        (shape.time_width, [Some(start), Some(upper)], Some(batch))
                    }
                    Change::Append { lower, upper, batch } => {
                        let width = created.get(id).or_else(|| self.widths.get(id))
                            .copied().ok_or_else(|| Error::Corrupt("accepted append has no registration".into()))?;
                        spans += 1;
                        (width, [Some(lower), Some(upper)], Some(batch))
                    }
                };
                if bounds.into_iter().flatten().any(|f| f.elements().iter().any(|time| time.len() > width)) {
                    return Err(Error::Invalid("time exceeds the ledger's time width"));
                }
                if let Some(Payload::Blob(hash)) = payload { self.store.get(*hash)?; }
            }
        }
        if delta.registry.len() != created.len() || delta.manifest.len() != spans {
            return Err(Error::Corrupt("conclusions do not match accepted requests".into()));
        }
        self.widths.extend(created);
        Ok(Batch { number, transactions, delta, outcomes })
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if let Some(flight) = self.flight.take() { let _ = flight.thread.join(); }
    }
}

fn initial(state: &State, width: usize) -> Updates {
    let mut entries = state.registry.iter().map(|(id, entry)| (*id, (entry, &entry.start))).collect::<BTreeMap<_, _>>();
    for span in &state.manifest {
        if let Some((_, write)) = entries.get_mut(&span.id) { *write = &span.upper; }
    }
    Updates {
        keys: corgi::Value::Prod(vec![resolve::ids(entries.keys().copied())]),
        vals: corgi::Value::Prod(vec![
            resolve::frontiers(entries.values().map(|(entry, _)| &entry.start), width),
            resolve::frontiers(entries.values().map(|(_, write)| *write), width),
        ]),
        diffs: vec![1; entries.len()],
    }
}
