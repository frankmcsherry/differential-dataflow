use super::*;
use std::sync::Barrier;

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("ddir-file-{}", Id::new_v4())))
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn no_temporaries(path: &Path) -> bool {
    fs::read_dir(path).unwrap().all(|entry| !entry.unwrap().file_name().as_encoded_bytes().starts_with(b".tmp-"))
}

#[test]
fn blobs_and_roots_survive_reopen() {
    let directory = Directory::new();
    let path = directory.0.join("nested/store");
    let id;
    {
        let store = FileStore::open(&path).unwrap();
        assert_eq!(store.read_root("root"), Ok(Root::default()));
        let first = store.put(b"first").unwrap();
        assert_eq!(store.put(b"first"), Ok(first));
        assert_eq!(store.get(first), Ok(b"first".to_vec()));
        let empty = store.put(b"").unwrap();
        assert_eq!(store.get(empty), Ok(Vec::new()));
        assert_eq!(store.get([0; 32]), Err(StoreError::Missing([0; 32])));
        assert_eq!(store.cas_root("root", 1, first), Ok(false));
        assert_eq!(store.cas_root("root", 0, first), Ok(true));
        id = store.put(b"second").unwrap();
        assert_eq!(store.cas_root("root", 1, id), Ok(true));
        assert_eq!(store.cas_root("root", 0, first), Ok(false));
        assert!(no_temporaries(&path.join("blobs")));
        assert!(no_temporaries(&path.join("roots")));
    }
    let reopened = FileStore::open(path).unwrap();
    assert_eq!(reopened.read_root("root"), Ok(Root { version: 2, state: Some(id) }));
    assert_eq!(reopened.get(id), Ok(b"second".to_vec()));
}

#[test]
fn root_names_are_safe_and_cannot_alias_lock_files() {
    assert_eq!(FileStore::open("").err().unwrap().kind(), io::ErrorKind::InvalidInput);
    let directory = Directory::new();
    let store = FileStore::open(&directory.0).unwrap();
    let state = store.put(b"state").unwrap();
    for name in ["", ".", "..", ".hidden", "a/b", "/root", "a\\b", "a\nb", "a\0b"] {
        assert!(matches!(store.read_root(name), Err(StoreError::Unavailable(_))));
        assert!(matches!(store.cas_root(name, 0, state), Err(StoreError::Unavailable(_))));
    }
    for name in ["foo", "foo.lock", "a-b_c.d", "1"] {
        assert_eq!(store.cas_root(name, 0, state), Ok(true));
        assert_eq!(store.read_root(name), Ok(Root { version: 1, state: Some(state) }));
    }
    assert!(store.dir.join("locks").join(hex(b"foo")).is_file());
    assert!(store.dir.join("locks").join(hex(b"foo.lock")).is_file());
}

#[test]
fn independent_handles_serialize_cas_and_duplicate_blob_puts() {
    let directory = Directory::new();
    let stores: Vec<_> = (0..3).map(|_| FileStore::open(&directory.0).unwrap()).collect();
    let barrier = Barrier::new(stores.len());
    let results = std::thread::scope(|scope| {
        let workers: Vec<_> = stores
            .iter()
            .map(|store| {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let id = store.put(b"same blob").unwrap();
                    (id, store.cas_root("root", 0, id).unwrap())
                })
            })
            .collect();
        workers.into_iter().map(|worker| worker.join().unwrap()).collect::<Vec<_>>()
    });
    assert!(results.iter().all(|(id, _)| *id == results[0].0));
    assert_eq!(results.iter().filter(|(_, success)| *success).count(), 1);
    assert_eq!(stores[0].read_root("root"), Ok(Root { version: 1, state: Some(results[0].0) }));
    assert_eq!(stores[0].get(results[0].0), Ok(b"same blob".to_vec()));
    assert!(no_temporaries(&directory.0.join("blobs")));
    assert!(no_temporaries(&directory.0.join("roots")));
}

#[test]
fn corruption_and_version_exhaustion_do_not_reset_roots() {
    let directory = Directory::new();
    let store = FileStore::open(&directory.0).unwrap();
    let id = store.put(b"state").unwrap();
    let path = store.root_path("root");
    fs::write(&path, b"malformed root").unwrap();
    assert!(matches!(store.read_root("root"), Err(StoreError::Unavailable(_))));
    assert!(matches!(store.cas_root("root", 0, id), Err(StoreError::Unavailable(_))));
    assert_eq!(fs::read(&path).unwrap(), b"malformed root");
    let exhausted = Root { version: u64::MAX, state: Some(id) };
    fs::write(&path, encode_root(exhausted)).unwrap();
    assert_eq!(store.read_root("root"), Ok(exhausted));
    assert!(matches!(store.cas_root("root", u64::MAX, id), Err(StoreError::Unavailable(_))));
    assert_eq!(store.read_root("root"), Ok(exhausted));
    fs::write(&path, encode_root(Root { version: 0, state: Some(id) })).unwrap();
    assert!(matches!(store.read_root("root"), Err(StoreError::Unavailable(_))));
    fs::write(store.blob_path(id), b"corrupt payload").unwrap();
    assert!(matches!(store.get(id), Err(StoreError::Unavailable(_))));
    assert!(matches!(store.put(b"state"), Err(StoreError::Unavailable(_))));
    assert_eq!(fs::read(store.blob_path(id)).unwrap(), b"corrupt payload");
}

#[test]
fn preparation_and_rename_failures_have_distinct_outcomes() {
    let directory = Directory::new();
    let store = FileStore::open(&directory.0).unwrap();
    let roots = store.dir.join("roots");
    let target = store.root_path("directory");
    fs::create_dir(&target).unwrap();
    let root = Root { version: 1, state: Some([1; 32]) };
    assert_eq!(store.publish_root("directory", root), Err(StoreError::CommitUnknown));
    assert!(target.is_dir());
    assert!(no_temporaries(&roots));
    fs::remove_dir(target).unwrap();
    fs::remove_dir(&roots).unwrap();
    assert!(matches!(store.publish_root("root", root), Err(StoreError::Unavailable(_))));
}
