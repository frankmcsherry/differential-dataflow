//! The ledger verbs from `interactive/model.md`, backed by immutable blobs and one root per cohort.
//!
//! `MemoryStore` models storage that survives dropping clients, while the process stays alive.
//! On Unix, `FileStore` makes completed writes durable on a local filesystem.
//! `Writer` is the one way to change a cohort: the keyed `.ddp` rules resolve each batch, and the root links its conclusions.
//! `Cohort` reads the committed state; `State::transact` is the serial rules in Rust, the reference the resolver is tested against.
//! Payload bytes are opaque and writer-validated.
//! Compaction, reclamation, checkpoints, and a stable storage format remain future work.
//! Run the backing tests with `cargo test -p interactive --lib ledger::`.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;

#[cfg(unix)]
mod file;
#[cfg(unix)]
pub use file::FileStore;
mod resolve;
pub use resolve::Transaction;
mod log;
mod engine;
mod writer;
pub use writer::{Receipt, Writer};

/// Signed product time, with trailing minimum coordinates omitted by `PointStamp`.
pub type Time = differential_dataflow::dynamic::pointstamp::PointStamp<i64, 2>;
/// An antichain of product times; the empty frontier is greatest.
pub type Frontier = Antichain<Time>;
/// A fresh ledger identifier, independent of its cohort or backing.
pub type Id = uuid::Uuid;
/// SHA-256 of a blob's bytes.
pub type BlobId = [u8; 32];

/// Declared data columns and the number of time coordinates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shape {
    pub columns: corgi::Shape,
    pub time_width: usize,
}

/// Where a span's raw updates are held.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Payload {
    Empty,
    Inline(Vec<u8>),
    Blob(BlobId),
}

/// One registry entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Registration {
    pub shape: Shape,
    pub start: Frontier,
}

/// A committed statement about the updates in `[lower, upper)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub id: Id,
    pub lower: Frontier,
    pub upper: Frontier,
    pub since: Frontier,
    pub batch: Payload,
}

/// A cohort's registry and manifest, recovered from its root.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub registry: BTreeMap<Id, Registration>,
    pub manifest: Vec<Span>,
}

impl State {
    /// The last appended upper, or the ledger's start if no spans exist.
    /// Only create and append produce these snapshots, so each ledger's spans form a chain.
    pub fn write(&self, id: Id) -> Option<&Frontier> {
        let entry = self.registry.get(&id)?;
        Some(self.manifest.iter().rev().find(|span| span.id == id).map_or(&entry.start, |span| &span.upper))
    }

    /// The serial rules: apply one transaction's groups in order, all or none.
    /// This is the reference for the `.ddp` rules; it checks no payload storage and publishes nothing.
    pub fn transact(&mut self, changes: &[(Id, Change)]) -> Result<(), Rejection> {
        validate(changes)?;
        // Rejection discards every tentative effect: this transaction's spans and the ledgers it created.
        let spans = self.manifest.len();
        let mut created = Vec::new();
        for (group, (id, change)) in changes.iter().enumerate() {
            let fresh = !self.registry.contains_key(id);
            let result = self.apply(*id, change);
            if fresh && self.registry.contains_key(id) { created.push(*id); }
            if let Err(reason) = result {
                self.manifest.truncate(spans);
                for id in created { self.registry.remove(&id); }
                return Err(Rejection { group, reason });
            }
        }
        Ok(())
    }

    fn apply(&mut self, id: Id, change: &Change) -> Result<(), Failure> {
        match change {
            Change::Create { shape, start } => self.create(id, shape, start),
            Change::Append { lower, upper, batch } => self.append(id, lower, upper, batch),
            Change::CreateAppend { shape, start, upper, batch } => {
                self.create(id, shape, start)?;
                self.append(id, start, upper, batch)
            }
        }
    }

    fn create(&mut self, id: Id, shape: &Shape, start: &Frontier) -> Result<(), Failure> {
        if self.registry.contains_key(&id) {
            return Err(Failure::Exists(id));
        }
        let start = canonical(start, shape.time_width).map_err(Failure::Invalid)?;
        self.registry.insert(id, Registration { shape: shape.clone(), start });
        Ok(())
    }

    fn append(&mut self, id: Id, lower: &Frontier, upper: &Frontier, batch: &Payload) -> Result<(), Failure> {
        let registration = self.registry.get(&id).ok_or(Failure::UnknownLedger(id))?;
        let lower = canonical(lower, registration.shape.time_width).map_err(Failure::Invalid)?;
        let upper = canonical(upper, registration.shape.time_width).map_err(Failure::Invalid)?;
        let write = self.write(id).expect("a registered ledger has a write frontier");
        if &lower != write {
            return Err(Failure::Write(write.clone()));
        }
        let since = registration.start.clone();
        self.manifest.push(Span { id, lower, upper, since, batch: batch.clone() });
        Ok(())
    }
}

/// All changes to one ledger within an atomic transaction.
/// Create identifiers are allocated by the caller and remain fixed across retries.
/// A combined creation and append uses `start` as both the span's lower and since.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Create { shape: Shape, start: Frontier },
    Append { lower: Frontier, upper: Frontier, batch: Payload },
    CreateAppend { shape: Shape, start: Frontier, upper: Frontier, batch: Payload },
}

/// The first group that failed, with its zero-based position in the transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejection {
    pub group: usize,
    pub reason: Failure,
}

/// Logical group failures, distinct from storage failures and uncertain publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    Write(Frontier),
    Exists(Id),
    UnknownLedger(Id),
    Invalid(&'static str),
}

/// Check the transaction's structure without consulting a cohort or storage.
/// Groups must be in strictly increasing id order, and spans must have strictly advancing bounds.
/// The first offending group determines the diagnostic.
pub fn validate(changes: &[(Id, Change)]) -> Result<(), Rejection> {
    for (group, (id, change)) in changes.iter().enumerate() {
        let bounds = match change {
            Change::Create { .. } => None,
            Change::Append { lower, upper, .. } => Some((lower, upper)),
            Change::CreateAppend { start, upper, .. } => Some((start, upper)),
        };
        let message = if group > 0 && changes[group - 1].0 >= *id {
            Some("transaction groups must have strictly increasing ids")
        } else if bounds.is_some_and(|(lower, upper)| !PartialOrder::less_than(lower, upper)) {
            Some("append requires lower < upper")
        } else {
            None
        };
        if let Some(message) = message {
            return Err(Rejection { group, reason: Failure::Invalid(message) });
        }
    }
    Ok(())
}

/// A versioned root; an unwritten root has version zero and no state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Root {
    pub version: u64,
    pub state: Option<BlobId>,
}

/// A storage failure is distinct from an append rejected by its write frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    Unavailable(String),
    Missing(BlobId),
    /// A CAS may have taken effect; the caller must not retry it as a known failure.
    CommitUnknown,
}

/// Immutable durable blobs and linearizable durable roots.
/// Implementations must preserve completed writes for their stated lifetime.
pub trait Store: Send + Sync {
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError>;
    fn get(&self, id: BlobId) -> Result<Vec<u8>, StoreError>;
    fn read_root(&self, name: &str) -> Result<Root, StoreError>;
    /// A successful CAS advances the version once, without ever reusing versions.
    /// Errors other than `CommitUnknown` guarantee that this call did not publish.
    fn cas_root(&self, name: &str, expected: u64, state: BlobId) -> Result<bool, StoreError>;
}

/// An in-memory durability stub, retained across client restarts.
#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    blobs: BTreeMap<BlobId, Vec<u8>>,
    roots: BTreeMap<String, Root>,
}

impl Store for MemoryStore {
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        use sha2::Digest;
        let id = sha2::Sha256::digest(bytes).into();
        self.inner.lock().unwrap().blobs.entry(id).or_insert_with(|| bytes.to_vec());
        Ok(id)
    }

    fn get(&self, id: BlobId) -> Result<Vec<u8>, StoreError> {
        self.inner.lock().unwrap().blobs.get(&id).cloned().ok_or(StoreError::Missing(id))
    }

    fn read_root(&self, name: &str) -> Result<Root, StoreError> {
        Ok(self.inner.lock().unwrap().roots.get(name).copied().unwrap_or_default())
    }

    fn cas_root(&self, name: &str, expected: u64, state: BlobId) -> Result<bool, StoreError> {
        let mut inner = self.inner.lock().unwrap();
        let root = inner.roots.entry(name.to_owned()).or_default();
        if root.version != expected {
            return Ok(false);
        }
        let version =
            expected.checked_add(1).ok_or_else(|| StoreError::Unavailable("root version exhausted".into()))?;
        *root = Root { version, state: Some(state) };
        Ok(true)
    }
}

/// Failures outside a transaction's own `Rejection`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Store(StoreError),
    Invalid(&'static str),
    Corrupt(String),
    Resolve(String),
}

impl From<StoreError> for Error {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// A reader of one cohort; all committed state resides in the store.
pub struct Cohort<'a> {
    store: &'a dyn Store,
    root: &'a str,
}

impl<'a> Cohort<'a> {
    pub fn new(store: &'a dyn Store, root: &'a str) -> Self {
        Self { store, root }
    }

    /// Recover the current committed snapshot, without replaying proposals.
    pub fn snapshot(&self) -> Result<State, Error> {
        Ok(log::load(self.store, self.root)?.1)
    }
}

/// Canonicalize antichains for stable encoding, while checking the declared time width.
fn canonical(frontier: &Frontier, width: usize) -> Result<Frontier, &'static str> {
    let mut times = frontier.elements().to_vec();
    if times.iter().any(|time| time.len() > width) {
        return Err("time exceeds the ledger's time width");
    }
    times.sort();
    Ok(times.into_iter().collect())
}

#[cfg(test)]
mod tests;
