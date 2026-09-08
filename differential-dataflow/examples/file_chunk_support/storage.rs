//! Spike: scoped file reads around a chunk codec; deliberately no Navigable impl.
#[cfg(test)]
mod tests;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom, Write};
use std::rc::Rc;

use differential_dataflow::columnar::trace::ColChunk;
use differential_dataflow::columnar::updates::Updates;
use differential_dataflow::trace::chunk::{Chunk, KeyedChunk};
use timely::progress::{frontier::AntichainRef, Antichain};

pub type Data = (u64, u64, u64, i64);
pub type Inner = ColChunk<Data>;
pub type DiskChunk = FileChunk<Inner>;

#[derive(Default)]
pub struct Stats {
    pub reads: Cell<u64>,
    pub read_bytes: Cell<u64>,
    pub writes: Cell<u64>,
    pub write_bytes: Cell<u64>,
    pub live_bytes: Cell<u64>,
    pub peak_live_bytes: Cell<u64>,
}
pub struct Segment {
    file: RefCell<std::fs::File>,
    len: Cell<u64>,
    stats: Rc<Stats>,
}
impl Drop for Segment {
    fn drop(&mut self) {
        self.stats
            .live_bytes
            .set(self.stats.live_bytes.get() - self.len.get());
    }
}
pub struct FileRef {
    segment: Rc<Segment>,
    offset: u64,
    len: usize,
}
impl FileRef {
    fn load(&self) -> Vec<u8> {
        let mut result = vec![0; self.len];
        let mut file = self.segment.file.borrow_mut();
        file.seek(SeekFrom::Start(self.offset)).unwrap();
        file.read_exact(&mut result).unwrap();
        let s = &self.segment.stats;
        s.reads.set(s.reads.get() + 1);
        s.read_bytes.set(s.read_bytes.get() + self.len as u64);
        result
    }
}
pub struct Store {
    current: Option<Rc<Segment>>,
    pub stats: Rc<Stats>,
    nocache: bool,
}
impl Store {
    fn write(&mut self, bytes: &[u8]) -> Rc<FileRef> {
        if self.current.as_ref().is_none_or(|s| s.len.get() >= 8 << 20) {
            let file = tempfile::tempfile().unwrap();
            if self.nocache {
                #[cfg(target_os = "macos")]
                {
                    use std::os::fd::AsRawFd;
                    unsafe extern "C" {
                        fn fcntl(fd: i32, command: i32, ...) -> i32;
                    }
                    // F_NOCACHE from the macOS SDK sys/fcntl.h.
                    assert_eq!(unsafe { fcntl(file.as_raw_fd(), 48, 1i32) }, 0);
                }
                #[cfg(not(target_os = "macos"))]
                panic!("--nocache currently requires macOS");
            }
            self.current = Some(Rc::new(Segment {
                file: RefCell::new(file),
                len: Cell::new(0),
                stats: Rc::clone(&self.stats),
            }));
        }
        let segment = Rc::clone(self.current.as_ref().unwrap());
        let offset = segment.len.get();
        {
            let mut file = segment.file.borrow_mut();
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(bytes).unwrap();
        }
        segment.len.set(offset + bytes.len() as u64);
        let s = &self.stats;
        s.writes.set(s.writes.get() + 1);
        s.write_bytes.set(s.write_bytes.get() + bytes.len() as u64);
        s.live_bytes.set(s.live_bytes.get() + bytes.len() as u64);
        s.peak_live_bytes
            .set(s.peak_live_bytes.get().max(s.live_bytes.get()));
        Rc::new(FileRef {
            segment,
            offset,
            len: bytes.len(),
        })
    }
    /// Release the writer's reference to the current segment; source handles own storage.
    pub fn rotate(&mut self) {
        self.current = None;
    }
}
thread_local! { static STORE: RefCell<Option<Rc<RefCell<Store>>>> = const { RefCell::new(None) }; }
pub fn install(nocache: bool) -> Rc<RefCell<Store>> {
    let store = Rc::new(RefCell::new(Store {
        current: None,
        stats: Rc::new(Stats::default()),
        nocache,
    }));
    STORE.with(|s| *s.borrow_mut() = Some(Rc::clone(&store)));
    store
}
pub fn uninstall() {
    STORE.with(|s| *s.borrow_mut() = None);
}
fn current() -> Rc<RefCell<Store>> {
    STORE.with(|s| Rc::clone(s.borrow().as_ref().expect("install file store first")))
}

pub trait Codec: KeyedChunk + Default + 'static {
    type Meta: Clone;
    fn metadata(&self) -> Self::Meta;
    fn encode(&self) -> Vec<u8>;
    fn decode(bytes: Vec<u8>) -> Self;
    fn key_range(meta: &Self::Meta, keys: &[Self::Key]) -> std::ops::Range<usize>;
}
impl Codec for Inner {
    type Meta = Option<(u64, u64)>;
    fn metadata(&self) -> Self::Meta {
        let Self::Resident(trie) = self else {
            panic!("do not nest file paging")
        };
        let v = trie.view();
        (trie.len() > 0).then(|| (v.keys.values[0], v.keys.values[v.keys.values.len() - 1]))
    }
    fn encode(&self) -> Vec<u8> {
        let Self::Resident(trie) = self else {
            panic!("do not nest file paging")
        };
        let mut bytes = Vec::new();
        Updates::<Data>::from((**trie).clone()).write_to(&mut bytes);
        bytes
    }
    fn decode(bytes: Vec<u8>) -> Self {
        Self::from_trie(
            Updates::<Data>::read_from(timely::bytes::arc::BytesMut::from(bytes).freeze())
                .into_typed(),
        )
    }
    fn key_range(meta: &Self::Meta, keys: &[u64]) -> std::ops::Range<usize> {
        match meta {
            Some((first, last)) => {
                keys.partition_point(|k| k < first)..keys.partition_point(|k| k <= last)
            }
            None => 0..0,
        }
    }
}
enum Body<C> {
    Resident(C),
    File(Rc<FileRef>),
}
pub struct FileChunk<C: Codec> {
    body: Body<C>,
    len: usize,
    meta: C::Meta,
    store: Rc<RefCell<Store>>,
}
impl<C: Codec> Clone for FileChunk<C> {
    fn clone(&self) -> Self {
        Self {
            body: match &self.body {
                Body::Resident(c) => Body::Resident(c.clone()),
                Body::File(f) => Body::File(Rc::clone(f)),
            },
            len: self.len,
            meta: self.meta.clone(),
            store: Rc::clone(&self.store),
        }
    }
}
impl<C: Codec> Default for FileChunk<C> {
    fn default() -> Self {
        Self::resident(C::default(), current())
    }
}
impl<C: Codec> FileChunk<C> {
    fn resident(chunk: C, store: Rc<RefCell<Store>>) -> Self {
        Self {
            len: chunk.len(),
            meta: chunk.metadata(),
            body: Body::Resident(chunk),
            store,
        }
    }
    pub fn new(chunk: C) -> Self {
        Self::resident(chunk, current())
    }
    pub fn load(&self) -> C {
        match &self.body {
            Body::Resident(c) => c.clone(),
            Body::File(f) => C::decode(f.load()),
        }
    }
    fn into_inner(self) -> C {
        // Maintenance owns these handles: let the codec reuse a unique resident
        // carry instead of making it clone that carry on every chunk boundary.
        match self.body {
            Body::Resident(c) => c,
            Body::File(f) => C::decode(f.load()),
        }
    }
    pub fn is_file(&self) -> bool {
        matches!(self.body, Body::File(_))
    }
    pub fn persist(mut self) -> Self {
        if let Body::Resident(c) = &self.body {
            self.body = Body::File(self.store.borrow_mut().write(&c.encode()));
        }
        self
    }
}
impl<C: Codec> KeyedChunk for FileChunk<C> {
    type Key = C::Key;
    fn select_keys(chunks: &[Self], keys: &[Self::Key]) -> Vec<Self> {
        let Some(first) = chunks.first() else {
            return Vec::new();
        };
        let mut pending = Vec::new();
        let mut pending_len = 0;
        let mut result = Vec::new();
        for chunk in chunks {
            let wanted = &keys[C::key_range(&chunk.meta, keys)];
            if !wanted.is_empty() {
                // Scoped read; no OnceCell or other pin on the source chunk.
                // Restrict the key argument too: don't copy the full request
                // once per disk chunk in the underlying columnar selector.
                for c in C::select_keys(&[chunk.load()], wanted) {
                    pending_len += c.len();
                    pending.push(c);
                    if pending_len >= C::TARGET {
                        // Pack the small selections incrementally, so we never
                        // retain one separately allocated trie per source chunk.
                        let mut packed = C::select_keys(&pending, keys);
                        pending.clear();
                        pending_len = 0;
                        if packed.last().is_some_and(|c| c.len() < C::TARGET) {
                            let carry = packed.pop().unwrap();
                            pending_len = carry.len();
                            pending.push(carry);
                        }
                        result.extend(packed);
                    }
                }
            }
        }
        result.extend(C::select_keys(&pending, keys));
        result
            .into_iter()
            .map(|c| Self::resident(c, Rc::clone(&first.store)))
            .collect()
    }
}
impl<C: Codec> Chunk for FileChunk<C> {
    type Time = C::Time;
    const TARGET: usize = C::TARGET;
    fn len(&self) -> usize {
        self.len
    }
    fn merge(a: &mut VecDeque<Self>, b: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let aa = a.pop_front().unwrap();
        let bb = b.pop_front().unwrap();
        let store = Rc::clone(&aa.store);
        let mut ai = VecDeque::from([aa.into_inner()]);
        let mut bi = VecDeque::from([bb.into_inner()]);
        let mut produced = VecDeque::new();
        C::merge(&mut ai, &mut bi, &mut produced);
        for c in ai.into_iter().rev() {
            a.push_front(Self::resident(c, Rc::clone(&store)));
        }
        for c in bi.into_iter().rev() {
            b.push_front(Self::resident(c, Rc::clone(&store)));
        }
        out.extend(
            produced
                .into_iter()
                .map(|c| Self::resident(c, Rc::clone(&store))),
        );
    }
    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<Self::Time>,
        residual: &mut Antichain<Self::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let Some(chunk) = input.pop_front() else {
            return;
        };
        let store = Rc::clone(&chunk.store);
        let mut inner = VecDeque::from([chunk.into_inner()]);
        let (mut k, mut s) = (VecDeque::new(), VecDeque::new());
        C::extract(&mut inner, frontier, residual, &mut k, &mut s);
        for c in inner.into_iter().rev() {
            input.push_front(Self::resident(c, Rc::clone(&store)));
        }
        keep.extend(k.into_iter().map(|c| Self::resident(c, Rc::clone(&store))));
        ship.extend(s.into_iter().map(|c| Self::resident(c, Rc::clone(&store))));
    }
    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<Self::Time>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        let Some(first) = input.front() else { return };
        let store = Rc::clone(&first.store);
        let (mut pending, mut produced) = (VecDeque::new(), VecDeque::new());
        while let Some(chunk) = input.pop_front() {
            pending.push_back(chunk.into_inner());
            C::advance(&mut pending, frontier, false, &mut produced);
            out.extend(
                produced
                    .drain(..)
                    .map(|c| Self::resident(c, Rc::clone(&store))),
            );
        }
        if done {
            C::advance(&mut pending, frontier, true, &mut produced);
            out.extend(
                produced
                    .into_iter()
                    .map(|c| Self::resident(c, Rc::clone(&store))),
            );
        }
        input.extend(
            pending
                .into_iter()
                .map(|c| Self::resident(c, Rc::clone(&store))),
        );
    }
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        let Some(first) = input.front() else { return };
        let store = Rc::clone(&first.store);
        let (mut pending, mut produced) = (VecDeque::new(), VecDeque::new());
        while let Some(chunk) = input.pop_front() {
            if pending.is_empty() && chunk.len == C::TARGET && chunk.is_file() {
                out.push_back(chunk);
            } else {
                pending.push_back(chunk.into_inner());
                C::settle(&mut pending, false, &mut produced);
                out.extend(
                    produced
                        .drain(..)
                        .map(|c| Self::resident(c, Rc::clone(&store)).persist()),
                );
            }
        }
        if done {
            C::settle(&mut pending, true, &mut produced);
            out.extend(
                produced
                    .into_iter()
                    .map(|c| Self::resident(c, Rc::clone(&store)).persist()),
            );
        }
        input.extend(
            pending
                .into_iter()
                .map(|c| Self::resident(c, Rc::clone(&store))),
        );
    }
}

pub fn rows(chunk: &DiskChunk) -> Vec<((u64, u64), u64, i64)> {
    let c = chunk.load();
    let Inner::Resident(trie) = c else {
        unreachable!()
    };
    trie.iter().map(|(k, v, t, d)| ((*k, *v), *t, *d)).collect()
}

// Reuse the same file backend for a control experiment with ColChunk's existing
// cursor-pinning spill implementation.
pub struct BytesAdapter(pub Rc<RefCell<Store>>);
impl differential_dataflow::columnar::trace::spill::BytesStore for BytesAdapter {
    fn store(
        &mut self,
        bytes: &[u8],
    ) -> Box<dyn differential_dataflow::columnar::trace::spill::BytesSource> {
        struct Source(Rc<FileRef>);
        impl differential_dataflow::columnar::trace::spill::BytesSource for Source {
            fn load(&self) -> Vec<u8> {
                self.0.load()
            }
        }
        Box::new(Source(self.0.borrow_mut().write(bytes)))
    }
}
