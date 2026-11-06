//! Durable storage: immutable blobs named by their content, and named roots replaced only by
//! compare-and-swap.
//!
//! Everything durable is built from these two. A blob is written once and never changes, so it
//! needs no coordination: writing the same bytes twice is harmless. A root is the one mutable
//! thing: a small value with a version, replaced only by a writer that names the version it read.
//! A root names blobs, and blobs may name other blobs.
//!
//! [`Ledger`] is the first thing built on them: one named, durable changelog. Its root holds its
//! manifest, the list of its batches, and each batch is a blob holding one frame of updates.
//! [`append`] adds a batch by compare-and-swap on the manifest, so concurrent writers to one ledger
//! cannot both succeed.
//!
//! [`FileStore`] keeps both in a directory. Every write is synced before it returns; on macOS,
//! `File::sync_all` issues `F_FULLFSYNC`, which empties the drive's cache.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A blob's name: the SHA-256 of its bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobId(pub [u8; 32]);

impl BlobId {
    /// The name of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        use sha2::Digest;
        BlobId(sha2::Sha256::digest(bytes).into())
    }

    /// The name as 64 hex digits.
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The name from 64 hex digits.
    pub fn from_hex(text: &str) -> Result<Self, String> {
        if text.len() != 64 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("not a blob id: {text:?}"));
        }
        let mut id = [0u8; 32];
        for (i, byte) in id.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).expect("checked hex digits");
        }
        Ok(BlobId(id))
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Immutable blobs, named by their content.
pub trait Blobs: Send + Sync {
    /// Store `bytes` durably, and return their name. Storing the same bytes again is harmless.
    fn put(&self, bytes: &[u8]) -> io::Result<BlobId>;
    /// The bytes named `id`; `NotFound` if there are none.
    fn get(&self, id: &BlobId) -> io::Result<Vec<u8>>;
}

/// Named roots, each a version and a value, replaced only by compare-and-swap.
pub trait Roots: Send + Sync {
    /// Root `name`'s version and value. A root never written is version 0 with no value.
    fn read(&self, name: &str) -> io::Result<(u64, Option<Vec<u8>>)>;
    /// If root `name` is at version `expected`, durably replace its value with `value` at version
    /// `expected + 1` and return `true`; otherwise change nothing and return `false`.
    fn cas(&self, name: &str, expected: u64, value: &[u8]) -> io::Result<bool>;
}

/// Blobs and roots together.
pub trait Store: Blobs + Roots {}
impl<T: Blobs + Roots> Store for T {}

/// A store in a local directory: `blobs/<hex>` and `roots/<name>`.
pub struct FileStore {
    dir: PathBuf,
    /// Makes temporary file names unique within this process.
    counter: AtomicU64,
}

impl FileStore {
    /// The store in `dir`, created if it does not exist.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(dir.join("blobs"))?;
        fs::create_dir_all(dir.join("roots"))?;
        Ok(FileStore { dir, counter: AtomicU64::new(0) })
    }

    fn temporary(&self, directory: &Path) -> PathBuf {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        directory.join(format!(".tmp-{}-{n}", std::process::id()))
    }

    /// Write `bytes` to `path` durably: a synced temporary file renamed into place, then the
    /// directory synced so the rename survives too.
    fn write_durably(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let directory = path.parent().expect("store paths have a parent");
        let temporary = self.temporary(directory);
        let mut file = File::create(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        File::open(directory)?.sync_all()
    }

    fn root_path(&self, name: &str) -> io::Result<PathBuf> {
        let valid = !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)) && !name.starts_with('.');
        if !valid {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("root names are letters, digits, '-', '_' and '.': {name:?}")));
        }
        Ok(self.dir.join("roots").join(name))
    }
}

impl Blobs for FileStore {
    fn put(&self, bytes: &[u8]) -> io::Result<BlobId> {
        let id = BlobId::of(bytes);
        let path = self.dir.join("blobs").join(id.to_hex());
        if !path.exists() {
            self.write_durably(&path, bytes)?;
        }
        Ok(id)
    }

    fn get(&self, id: &BlobId) -> io::Result<Vec<u8>> {
        fs::read(self.dir.join("blobs").join(id.to_hex()))
    }
}

impl Roots for FileStore {
    fn read(&self, name: &str) -> io::Result<(u64, Option<Vec<u8>>)> {
        match fs::read(self.root_path(name)?) {
            Ok(bytes) => decode_root(&bytes).map(|(version, value)| (version, Some(value))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok((0, None)),
            Err(error) => Err(error),
        }
    }

    fn cas(&self, name: &str, expected: u64, value: &[u8]) -> io::Result<bool> {
        let path = self.root_path(name)?;
        // A lock file per root serializes compare-and-swaps across threads and processes.
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(path.with_extension("lock"))?;
        lock_exclusive(&lock)?;
        if self.read(name)?.0 != expected {
            return Ok(false);
        }
        let mut bytes = format!("{}\n", expected + 1).into_bytes();
        bytes.extend_from_slice(value);
        self.write_durably(&path, &bytes)?;
        Ok(true)
    }
}

/// Hold an exclusive lock on `file` until it is closed. (`File::lock` would do, from Rust 1.89.)
#[cfg(unix)]
fn lock_exclusive(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    loop {
        // SAFETY: `flock` on a file descriptor this `File` owns and keeps open.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Elsewhere, no lock: compare-and-swaps from two processes on one directory could interleave.
#[cfg(not(unix))]
fn lock_exclusive(_file: &File) -> io::Result<()> {
    Ok(())
}

fn decode_root(bytes: &[u8]) -> io::Result<(u64, Vec<u8>)> {
    let corrupt = || io::Error::new(io::ErrorKind::InvalidData, "root file has no version line");
    let newline = bytes.iter().position(|&b| b == b'\n').ok_or_else(corrupt)?;
    let version = std::str::from_utf8(&bytes[..newline]).ok().and_then(|v| v.parse().ok()).ok_or_else(corrupt)?;
    Ok((version, bytes[newline + 1..].to_vec()))
}

/// One named, durable changelog: its declared row shape, and its batches in order. Batch `i`
/// covers `[lower, upper)` in the ledger's own time, each upper one more than its lower, and holds
/// one frame of updates with no time lanes ([`crate::runtime::frame`]).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Ledger {
    /// The rows' shape, as `(key, val)`.
    pub shape: String,
    /// `(lower, upper, blob)` for each batch, in order.
    pub batches: Vec<(u64, u64, BlobId)>,
}

impl Ledger {
    /// Where the next batch starts.
    pub fn upper(&self) -> u64 {
        self.batches.last().map_or(0, |b| b.1)
    }

    /// The manifest's text: a header, the shape, and one line per batch.
    pub fn encode(&self) -> Vec<u8> {
        let mut text = format!("ddir-ledger 1\nshape {}\n", self.shape);
        for (lower, upper, blob) in &self.batches {
            text.push_str(&format!("batch {lower} {upper} {}\n", blob.to_hex()));
        }
        text.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(bytes).map_err(|_| "ledger manifest is not UTF-8".to_string())?;
        let mut lines = text.lines();
        if lines.next() != Some("ddir-ledger 1") {
            return Err("not a version 1 ledger manifest".into());
        }
        let shape = lines.next().and_then(|l| l.strip_prefix("shape ")).ok_or("ledger manifest has no shape")?.to_string();
        let mut batches = Vec::new();
        for line in lines {
            let fields: Vec<&str> = line.split(' ').collect();
            match fields[..] {
                ["batch", lower, upper, blob] => {
                    let number = |t: &str| t.parse::<u64>().map_err(|_| format!("bad batch line {line:?}"));
                    batches.push((number(lower)?, number(upper)?, BlobId::from_hex(blob)?));
                }
                _ => return Err(format!("bad ledger manifest line {line:?}")),
            }
        }
        Ok(Ledger { shape, batches })
    }
}

/// Ledger `name` as of now, with the version of its root; `None` if it has never been written.
pub fn read_ledger(store: &dyn Store, name: &str) -> io::Result<(u64, Option<Ledger>)> {
    let (version, value) = store.read(name)?;
    let ledger = value.map(|v| Ledger::decode(&v).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))).transpose()?;
    Ok((version, ledger))
}

/// Why an append did not happen.
#[derive(Debug)]
pub enum AppendError {
    Io(io::Error),
    /// The ledger's rows have another shape.
    Shape { ledger: String, rows: String },
    /// Another writer changed the ledger since `expected` was read.
    Moved { expected: u64 },
}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppendError::Io(error) => write!(f, "{error}"),
            AppendError::Shape { ledger, rows } => write!(f, "the ledger holds rows of shape {ledger}, not {rows}"),
            AppendError::Moved { expected } => write!(f, "the ledger moved past version {expected}: another writer appended"),
        }
    }
}

/// Append one frame of updates (rows of `shape`) to ledger `name`, which this writer last saw at
/// root version `expected`: store the frame as a blob, then compare-and-swap the manifest to
/// include it. Returns the new version and the batch's `[lower, upper)`. Fails, changing nothing
/// that is read, if the ledger moved past `expected`: the caller decides whether to read it again
/// and retry, or to stop.
pub fn append(store: &dyn Store, name: &str, expected: u64, shape: &str, frame: &[u8]) -> Result<(u64, (u64, u64)), AppendError> {
    let (version, ledger) = read_ledger(store, name).map_err(AppendError::Io)?;
    if version != expected {
        return Err(AppendError::Moved { expected });
    }
    let mut ledger = ledger.unwrap_or_else(|| Ledger { shape: shape.to_string(), batches: Vec::new() });
    if ledger.shape != shape {
        return Err(AppendError::Shape { ledger: ledger.shape, rows: shape.to_string() });
    }
    let blob = store.put(frame).map_err(AppendError::Io)?;
    let lower = ledger.upper();
    ledger.batches.push((lower, lower + 1, blob));
    if !store.cas(name, version, &ledger.encode()).map_err(AppendError::Io)? {
        return Err(AppendError::Moved { expected });
    }
    Ok((version + 1, (lower, lower + 1)))
}

/// The store `DDIR_STORE` names, a directory, if it is set.
pub fn from_env() -> Option<io::Result<FileStore>> {
    std::env::var_os("DDIR_STORE").map(FileStore::open)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ddir-storage-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// What every store must do; run against each implementation.
    fn conformance(store: Arc<dyn Store>) {
        // Blobs: named by content, idempotent, missing ones are NotFound.
        let a = store.put(b"hello").unwrap();
        assert_eq!(a, BlobId::of(b"hello"));
        assert_eq!(store.put(b"hello").unwrap(), a);
        assert_eq!(store.get(&a).unwrap(), b"hello");
        assert_eq!(store.get(&BlobId::of(b"absent")).unwrap_err().kind(), io::ErrorKind::NotFound);
        let empty = store.put(b"").unwrap();
        assert_eq!(store.get(&empty).unwrap(), b"");

        // Roots: version 0 until written; a swap succeeds only from the version it names.
        assert_eq!(store.read("r").unwrap(), (0, None));
        assert!(!store.cas("r", 1, b"x").unwrap());
        assert!(store.cas("r", 0, b"one").unwrap());
        assert_eq!(store.read("r").unwrap(), (1, Some(b"one".to_vec())));
        assert!(!store.cas("r", 0, b"stale").unwrap());
        assert!(store.cas("r", 1, b"").unwrap());
        assert_eq!(store.read("r").unwrap(), (2, Some(Vec::new())));
        assert!(store.read("bad/name").is_err());

        // Racing swaps from one version: exactly one wins, every round.
        for round in 0..20u64 {
            let winners: usize = (0..8)
                .map(|t| {
                    let store = Arc::clone(&store);
                    std::thread::spawn(move || store.cas("race", round, format!("{t}").as_bytes()).unwrap())
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap() as usize)
                .sum();
            assert_eq!(winners, 1, "round {round}");
        }
        assert_eq!(store.read("race").unwrap().0, 20);
    }

    #[test]
    fn file_store_conforms() {
        let dir = scratch("conform");
        conformance(Arc::new(FileStore::open(&dir).unwrap()));
        // Two stores on one directory are one store: their swaps exclude each other too.
        let (one, two) = (FileStore::open(&dir).unwrap(), FileStore::open(&dir).unwrap());
        let version = one.read("race").unwrap().0;
        assert!(one.cas("race", version, b"a").unwrap());
        assert!(!two.cas("race", version, b"b").unwrap());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn ledgers_append_in_order_and_refuse_stale_writers() {
        let dir = scratch("ledger");
        let store = FileStore::open(&dir).unwrap();
        let shape = "((int), ())";
        let (v1, range1) = append(&store, "s", 0, shape, b"frame one").unwrap();
        let (v2, range2) = append(&store, "s", v1, shape, b"frame two").unwrap();
        assert_eq!((range1, range2), ((0, 1), (1, 2)));
        // A writer still at the first version is refused, and changes nothing.
        assert!(matches!(append(&store, "s", v1, shape, b"late"), Err(AppendError::Moved { .. })));
        assert!(matches!(append(&store, "s", v2, "((int), (int))", b"x"), Err(AppendError::Shape { .. })));
        let (version, ledger) = read_ledger(&store, "s").unwrap();
        let ledger = ledger.unwrap();
        assert_eq!((version, ledger.upper(), ledger.shape.as_str()), (v2, 2, shape));
        let frames: Vec<Vec<u8>> = ledger.batches.iter().map(|(_, _, b)| store.get(b).unwrap()).collect();
        assert_eq!(frames, vec![b"frame one".to_vec(), b"frame two".to_vec()]);
        assert_eq!(Ledger::decode(&ledger.encode()).unwrap(), ledger);
        fs::remove_dir_all(&dir).unwrap();
    }
}
