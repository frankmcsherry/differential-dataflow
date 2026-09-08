//! A bounded cache of key selections, physically merged without advancing time.
//!
//! [`CachedTrace`] wraps any reader of `Rc<ChunkBatch<C>>`, including a shared
//! trace agent. Ordinary [`TraceReader`] methods retain their usual meaning;
//! [`CachedTrace::span_through_keys`] opts into key selection and caching.
//! Each entry records both its requested keys (including absent keys) and its
//! committed upper. Reads can reuse overlapping selections and fetch only the
//! uncovered suffix for cached keys. Source span descriptions are consulted on
//! every read, but cached prefixes do not require reading source chunk bodies.
//!
//! The prototype does not pin physical compaction. An entry is reusable only if
//! its upper is still protected by the reader's physical compaction frontier,
//! and no source span straddles it. Otherwise the keys are read afresh. No
//! timestamp filtering reconstructs provenance.
//! Cache contents keep their timestamps, with `since` tracking any logical
//! compaction already performed by the sources.
//!
//! Capacity counts updates plus requested keys and entries, not bytes. On
//! overflow the cache is cleared; an oversized result is returned uncached.
//! This bounds retained cache contents, not transient read/merge memory or
//! batches retained by callers. Requests are synchronous and fully materialized.
//!
//! The `key_cache` example exercises a file-backed columnar trace and reports
//! cold, repeated, incremental, and post-eviction fetch counts:
//! `cargo run -p differential-dataflow --example key_cache`.

use std::rc::Rc;

use timely::progress::{frontier::AntichainRef, Antichain, Timestamp};
use timely::PartialOrder;

use crate::lattice::Lattice;
use crate::trace::chunk::{merge_chains, ChunkBatch, KeyedChunk};
use crate::trace::{Description, Span, TraceReader};

struct Entry<C: KeyedChunk> {
    keys: Vec<C::Key>,
    batch: Rc<Span<C::Time, Rc<ChunkBatch<C>>>>,
}

/// A trace reader with an optional, cursor-free cache of selected keys.
pub struct CachedTrace<Tr, C: KeyedChunk> {
    trace: Tr,
    entries: Vec<Entry<C>>,
    capacity: usize,
    charge: usize,
}

impl<Tr, C: KeyedChunk> CachedTrace<Tr, C> {
    /// Wrap `trace` with a budget of updates + requested keys + entries.
    /// A zero budget disables retention but still permits selected reads.
    pub fn new(trace: Tr, capacity: usize) -> Self {
        Self {
            trace,
            entries: Vec::new(),
            capacity,
            charge: 0,
        }
    }

    /// Release all cache entries. Previously returned batches remain valid.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.charge = 0;
    }

    /// Change the budget, clearing retained entries if they no longer fit.
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        if self.charge > capacity {
            self.clear();
        }
    }

    /// Current charge in updates + requested keys + entries.
    pub fn charge(&self) -> usize {
        self.charge
    }

    /// The number of retained key selections.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no key selections are retained.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Borrow the wrapped reader.
    pub fn inner(&self) -> &Tr {
        &self.trace
    }

    /// Mutably borrow the reader, clearing coverage claims before arbitrary edits.
    /// Updates to a shared trace through another handle need no explicit clearing.
    pub fn inner_mut(&mut self) -> &mut Tr {
        self.clear();
        &mut self.trace
    }

    /// Release the cache and return the reader.
    pub fn into_inner(self) -> Tr {
        self.trace
    }
}

impl<Tr, C> CachedTrace<Tr, C>
where
    C: KeyedChunk + 'static,
    Tr: TraceReader<Time = C::Time, Batch = Rc<ChunkBatch<C>>>,
{
    /// Materialize exactly `keys` through `upper` as a span with a compact resident batch.
    ///
    /// Keys may be unordered or repeated. The upper must satisfy the wrapped
    /// reader's `spans_through` contract. In particular, an empty frontier
    /// requests all committed data, not future data: the returned description
    /// and cache coverage end at the trace's current committed upper.
    ///
    /// No span is returned when the requested/committed upper is the minimum
    /// timestamp: that prefix has no time interval to describe.
    ///
    /// Results preserve timestamp distinctions; only identical `(key,val,time)`
    /// updates consolidate. A returned span covers only the requested keys and
    /// must not be inserted into the source trace as additional updates.
    pub fn span_through_keys(
        &mut self,
        upper: AntichainRef<C::Time>,
        keys: &[C::Key],
    ) -> Option<Rc<Span<C::Time, Rc<ChunkBatch<C>>>>> {
        let sources = self.trace.spans_through(upper)?;
        let mut committed = Antichain::new();
        self.trace.read_upper(&mut committed);
        let upper = upper.to_owned().meet(&committed);
        if upper.less_equal(&C::Time::minimum()) {
            return None;
        }
        let mut keys = keys.to_vec();
        keys.sort_unstable();
        keys.dedup();

        // Exact hits can return the same allocation even if the source has
        // compacted internally: no suffix needs to be separated at this cut.
        if let Some(entry) = self
            .entries
            .iter()
            .rev()
            .find(|e| e.keys == keys && e.batch.upper() == &upper)
        {
            return Some(Rc::clone(&entry.batch));
        }

        let mut since = self.trace.get_logical_compaction().to_owned();
        let physical = self.trace.get_physical_compaction().to_owned();
        let mut missing = keys.clone();
        let mut chains = Vec::new();
        let mut source_keys = vec![Vec::new(); sources.len()];
        // Newer entries win. Remove assigned keys so overlapping cache entries
        // never contribute the same updates twice.
        for entry in self.entries.iter().rev() {
            if !PartialOrder::less_equal(entry.batch.upper(), &upper) {
                continue;
            }
            let cut = entry.batch.upper();
            // Reuse prefixes only while physical compaction protects their cut,
            // including when cancellation changes the source payloads.
            if !PartialOrder::less_equal(&physical, cut) {
                continue;
            }
            if sources.iter().any(|b| {
                !PartialOrder::less_equal(b.upper(), cut)
                    && !PartialOrder::less_equal(cut, b.lower())
            }) {
                continue;
            }
            let mut selected = Vec::new();
            missing.retain(|key| {
                if entry.keys.binary_search(key).is_ok() {
                    selected.push(key.clone());
                    false
                } else {
                    true
                }
            });
            if selected.is_empty() {
                continue;
            }
            since = since.join(entry.batch.desc.since());
            chains.push(if selected == entry.keys {
                chunks(&entry.batch).to_vec()
            } else {
                C::select_keys(chunks(&entry.batch), &selected)
            });
            for (batch, wanted) in sources.iter().zip(&mut source_keys) {
                if PartialOrder::less_equal(cut, batch.lower()) {
                    wanted.extend_from_slice(&selected);
                }
            }
            if missing.is_empty() {
                break;
            }
        }
        if !missing.is_empty() {
            for wanted in &mut source_keys {
                wanted.extend_from_slice(&missing);
            }
        }
        // Coalesce all requests for a source batch before touching its bodies.
        for (batch, mut wanted) in sources.iter().zip(source_keys) {
            if !wanted.is_empty() {
                wanted.sort_unstable();
                since = since.join(batch.desc.since());
                chains.push(C::select_keys(chunks(batch), &wanted));
            }
        }

        // Pairwise rounds avoid repeatedly rewriting a growing prefix for each
        // source batch. Chunk::merge consolidates but does not advance time.
        chains.retain(|c| !c.is_empty());
        while chains.len() > 1 {
            let mut next = Vec::new();
            let mut inputs = chains.into_iter();
            while let Some(left) = inputs.next() {
                if let Some(right) = inputs.next() {
                    let mut merged = std::collections::VecDeque::new();
                    merge_chains(left, right, &mut merged);
                    next.push(merged.into());
                } else {
                    next.push(left);
                }
            }
            chains = next;
        }
        // Selection also packs a chain, without invoking settle's spill policy.
        let chunks = C::select_keys(&chains.pop().unwrap_or_default(), &keys);
        let batch = Rc::new(Span::new(
            Description::new(Antichain::from_elem(C::Time::minimum()), upper, since),
            (!chunks.is_empty()).then(|| Rc::new(ChunkBatch::new(chunks))),
        ));
        let charge = updates(&batch).saturating_add(keys.len()).saturating_add(1);
        if !keys.is_empty() && charge <= self.capacity {
            // Retire entries wholly subsumed in both keys and coverage.
            self.entries.retain(|entry| {
                !(PartialOrder::less_equal(entry.batch.upper(), batch.upper())
                    && entry.keys.iter().all(|k| keys.binary_search(k).is_ok()))
            });
            self.charge = self
                .entries
                .iter()
                .map(|e| updates(&e.batch) + e.keys.len() + 1)
                .sum();
            if charge > self.capacity - self.charge {
                self.clear();
            }
            self.charge += charge;
            self.entries.push(Entry {
                keys,
                batch: Rc::clone(&batch),
            });
        }
        Some(batch)
    }
}

impl<Tr, C> TraceReader for CachedTrace<Tr, C>
where
    C: KeyedChunk + 'static,
    Tr: TraceReader<Time = C::Time, Batch = Rc<ChunkBatch<C>>>,
{
    type Time = C::Time;
    type Batch = Rc<ChunkBatch<C>>;

    fn spans_through(
        &mut self,
        upper: AntichainRef<C::Time>,
    ) -> Option<Vec<Span<C::Time, Self::Batch>>> {
        self.trace.spans_through(upper)
    }
    fn set_logical_compaction(&mut self, frontier: AntichainRef<C::Time>) {
        self.trace.set_logical_compaction(frontier);
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, C::Time> {
        self.trace.get_logical_compaction()
    }
    fn set_physical_compaction(&mut self, frontier: AntichainRef<C::Time>) {
        self.trace.set_physical_compaction(frontier);
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, C::Time> {
        self.trace.get_physical_compaction()
    }
    fn map_spans<F: FnMut(&Span<C::Time, Self::Batch>)>(&self, f: F) {
        self.trace.map_spans(f);
    }
    fn read_upper(&mut self, target: &mut Antichain<C::Time>) {
        self.trace.read_upper(target);
    }
    fn advance_upper(&mut self, upper: &mut Antichain<C::Time>) {
        self.trace.advance_upper(upper);
    }
}

fn chunks<C: KeyedChunk>(span: &Span<C::Time, Rc<ChunkBatch<C>>>) -> &[C] {
    span.inner
        .as_ref()
        .map_or(&[], |batch| batch.chunks.as_slice())
}
fn updates<C: KeyedChunk>(span: &Span<C::Time, Rc<ChunkBatch<C>>>) -> usize {
    chunks(span).iter().map(C::len).sum()
}
