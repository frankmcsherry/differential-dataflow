//! Durable blobs and roots on a local Unix filesystem.
//!
//! Every published file is synced before its name is installed, then its directory is synced.
//! On macOS, `File::sync_all` uses `F_FULLFSYNC` to flush the drive cache.
//! Root locks occupy a separate directory and are never removed or replaced.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use super::{BlobId, Id, Root, Store, StoreError};

const ROOT_MAGIC: &[u8] = b"ddir-root 1\n";

/// A store in a local Unix directory, using immutable blobs and one lock file per named root.
pub struct FileStore {
    dir: PathBuf,
}

impl FileStore {
    /// Open or create the store, making its directories and their ancestors durable.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        if dir.as_ref().as_os_str().is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "store directory must not be empty"));
        }
        for name in ["blobs", "roots", "locks"] {
            fs::create_dir_all(dir.as_ref().join(name))?;
        }
        let dir = fs::canonicalize(dir)?;
        for name in ["blobs", "roots", "locks"] {
            sync_directory(&dir.join(name))?;
        }
        // Newly created nested parents must survive too, including concurrent directory creation.
        for ancestor in dir.ancestors() {
            sync_directory(ancestor)?;
        }
        Ok(Self { dir })
    }

    fn blob_path(&self, id: BlobId) -> PathBuf {
        self.dir.join("blobs").join(hex(&id))
    }

    fn root_path(&self, name: &str) -> PathBuf {
        // Preserve exact root spellings even on case-insensitive filesystems.
        self.dir.join("roots").join(hex(name.as_bytes()))
    }

    fn lock_root(&self, name: &str) -> io::Result<File> {
        if name.is_empty()
            || name.starts_with('.')
            || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "root names must use ASCII letters, digits, '-', '_' or '.', without a leading '.'",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.dir.join("locks").join(hex(name.as_bytes())))?;
        loop {
            // SAFETY: the descriptor belongs to this live File; closing it releases the lock.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                return Ok(file);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    /// Called with the root lock held, including throughout the directory sync.
    fn read_locked(&self, name: &str) -> io::Result<Root> {
        let root = match fs::read(self.root_path(name)) {
            Ok(bytes) => decode_root(&bytes)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Root::default(),
            Err(error) => return Err(error),
        };
        // A writer may have died after rename but before directory sync.
        // Settle that publication before returning its value or deciding another CAS.
        sync_directory(&self.dir.join("roots"))?;
        Ok(root)
    }

    fn publish_root(&self, name: &str, root: Root) -> Result<(), StoreError> {
        let directory = self.dir.join("roots");
        let temporary = Temporary::write(&directory, &encode_root(root)).map_err(unavailable)?;
        // Once rename is attempted, an error cannot establish that publication did not happen.
        fs::rename(&temporary.path, self.root_path(name)).map_err(|_| StoreError::CommitUnknown)?;
        sync_directory(&directory).map_err(|_| StoreError::CommitUnknown)
    }
}

impl Store for FileStore {
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let id = hash(bytes);
        let directory = self.dir.join("blobs");
        let path = self.blob_path(id);
        match fs::read(&path) {
            Ok(existing) if existing == bytes => {
                // Visibility alone does not imply that another writer synced this name.
                sync_directory(&directory).map_err(unavailable)?;
                return Ok(id);
            }
            Ok(_) => return Err(StoreError::Unavailable("blob contents disagree with their hash".into())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(error) => return Err(unavailable(error)),
        }
        let temporary = Temporary::write(&directory, bytes).map_err(unavailable)?;
        // Linking publishes without replacing an existing immutable name.
        match fs::hard_link(&temporary.path, &path) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if fs::read(&path).map_err(unavailable)? != bytes {
                    return Err(StoreError::Unavailable("blob contents disagree with their hash".into()));
                }
            }
            Err(error) => return Err(unavailable(error)),
        }
        sync_directory(&directory).map_err(unavailable)?;
        Ok(id)
    }

    fn get(&self, id: BlobId) -> Result<Vec<u8>, StoreError> {
        let bytes = fs::read(self.blob_path(id)).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                StoreError::Missing(id)
            } else {
                unavailable(error)
            }
        })?;
        if hash(&bytes) != id {
            return Err(StoreError::Unavailable("blob contents disagree with their hash".into()));
        }
        // Also settle a name exposed by a put whose writer has not completed its directory sync.
        sync_directory(&self.dir.join("blobs")).map_err(unavailable)?;
        Ok(bytes)
    }

    fn read_root(&self, name: &str) -> Result<Root, StoreError> {
        let _lock = self.lock_root(name).map_err(unavailable)?;
        self.read_locked(name).map_err(unavailable)
    }

    fn cas_root(&self, name: &str, expected: u64, state: BlobId) -> Result<bool, StoreError> {
        let _lock = self.lock_root(name).map_err(unavailable)?;
        if self.read_locked(name).map_err(unavailable)?.version != expected {
            return Ok(false);
        }
        let version =
            expected.checked_add(1).ok_or_else(|| StoreError::Unavailable("root version exhausted".into()))?;
        self.publish_root(name, Root { version, state: Some(state) })?;
        Ok(true)
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hash(bytes: &[u8]) -> BlobId {
    use sha2::Digest;
    sha2::Sha256::digest(bytes).into()
}

fn unavailable(error: io::Error) -> StoreError {
    StoreError::Unavailable(error.to_string())
}

fn encode_root(root: Root) -> Vec<u8> {
    let mut bytes = ROOT_MAGIC.to_vec();
    bytes.extend_from_slice(&root.version.to_le_bytes());
    bytes.extend_from_slice(&root.state.expect("published roots name state"));
    bytes
}

fn decode_root(bytes: &[u8]) -> io::Result<Root> {
    if bytes.len() != ROOT_MAGIC.len() + 8 + 32 || !bytes.starts_with(ROOT_MAGIC) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid root format"));
    }
    let version = u64::from_le_bytes(bytes[ROOT_MAGIC.len()..ROOT_MAGIC.len() + 8].try_into().unwrap());
    if version == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "published root has version zero"));
    }
    Ok(Root { version, state: Some(bytes[ROOT_MAGIC.len() + 8..].try_into().unwrap()) })
}

/// A fresh temporary name, cleaned up on success or failure; interrupted processes can leave it.
struct Temporary {
    path: PathBuf,
    file: File,
}

impl Temporary {
    fn write(directory: &Path, bytes: &[u8]) -> io::Result<Self> {
        let mut temporary = loop {
            let path = directory.join(format!(".tmp-{}", Id::new_v4()));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => break Self { path, file },
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
                Err(error) => return Err(error),
            }
        };
        temporary.file.write_all(bytes)?;
        temporary.file.sync_all()?;
        Ok(temporary)
    }
}

impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests;
