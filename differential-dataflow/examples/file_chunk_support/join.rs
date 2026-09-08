//! Static cold-side join adapter. Source CHUNKs never offer cursors.
use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;

use super::storage::{self, DiskChunk};
use differential_dataflow::operators::join::{Fresh, JoinTactic};
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::wrappers::cached::CachedTrace;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use timely::progress::{frontier::AntichainRef, Antichain};

pub type Batch = Rc<ChunkBatch<DiskChunk>>;
type QueryBatch = <ValSpine<u64, u64, u64, i64> as TraceReader>::Batch;

#[derive(Clone)]
pub struct StaticTrace {
    pub batch: Batch,
    logical: Antichain<u64>,
    physical: Antichain<u64>,
}
impl StaticTrace {
    pub fn new(batch: Batch) -> Self {
        Self {
            batch,
            logical: Antichain::from_elem(0),
            physical: Antichain::from_elem(0),
        }
    }
}
impl TraceReader for StaticTrace {
    type Time = u64;
    type Batch = Batch;
    fn batches_through(&mut self, upper: AntichainRef<u64>) -> Option<Vec<Batch>> {
        if upper.less_equal(&0) {
            Some(Vec::new())
        } else if upper.is_empty() {
            Some(vec![Rc::clone(&self.batch)])
        } else {
            None
        }
    }
    fn set_logical_compaction(&mut self, f: AntichainRef<u64>) {
        self.logical = f.to_owned();
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, u64> {
        self.logical.borrow()
    }
    fn set_physical_compaction(&mut self, f: AntichainRef<u64>) {
        self.physical = f.to_owned();
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, u64> {
        self.physical.borrow()
    }
    fn map_batches<F: FnMut(&Batch)>(&self, mut f: F) {
        f(&self.batch);
    }
}

pub struct Tactic {
    cache: CachedTrace<StaticTrace, DiskChunk>,
    pub calls: Rc<Cell<u64>>,
    pub profile: bool,
}
impl Tactic {
    pub fn new(trace: StaticTrace, capacity: usize, calls: Rc<Cell<u64>>) -> Self {
        Self {
            cache: CachedTrace::new(trace, capacity),
            calls,
            profile: false,
        }
    }
}
impl JoinTactic<Batch, QueryBatch, Vec<(u64, u64, i64)>> for Tactic {
    fn prep(
        &mut self,
        source: Vec<Batch>,
        query: Vec<QueryBatch>,
        fresh: Fresh,
        meet: u64,
    ) -> Box<dyn Iterator<Item = Vec<(u64, u64, i64)>>> {
        if source.is_empty() || query.is_empty() {
            return Box::new(std::iter::empty());
        }
        // Explicit spike restriction: changing query memberships, static cold data.
        assert!(
            matches!(fresh, Fresh::Input1),
            "changing the cold side needs a symmetric tactic"
        );
        assert_eq!(source.len(), 1);
        assert!(Rc::ptr_eq(&source[0], &self.cache.inner().batch));
        let mut queries: BTreeMap<u64, Vec<(u64, i64)>> = BTreeMap::new();
        for batch in query {
            let mut c = batch.cursor();
            while c.key_valid(&batch) {
                let key = *c.key(&batch);
                while c.val_valid(&batch) {
                    c.map_times(&batch, |t, r| {
                        queries.entry(key).or_default().push((*t, *r))
                    });
                    c.step_val(&batch);
                }
                c.step_key(&batch);
            }
        }
        let keys: Vec<_> = queries.keys().copied().collect();
        let selected = self
            .cache
            .batch_through_keys(AntichainRef::new(&[]), &keys)
            .unwrap();
        self.calls.set(self.calls.get() + 1);
        if self.profile {
            println!(
                "selected call={} rss_kib={} charge={} chunks={}",
                self.calls.get(),
                super::rss_kib(),
                self.cache.charge(),
                selected.chunks.len()
            );
        }
        Box::new(JoinOutput {
            chunks: selected.chunks.clone().into_iter(),
            rows: Vec::new().into_iter(),
            current: None,
            index: 0,
            queries,
            meet,
        })
    }
}

struct JoinOutput {
    chunks: std::vec::IntoIter<DiskChunk>,
    rows: std::vec::IntoIter<((u64, u64), u64, i64)>,
    current: Option<((u64, u64), u64, i64)>,
    index: usize,
    queries: BTreeMap<u64, Vec<(u64, i64)>>,
    meet: u64,
}
impl Iterator for JoinOutput {
    type Item = Vec<(u64, u64, i64)>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut output = Vec::with_capacity(1024);
        while output.len() < 1024 {
            if self.current.is_none() {
                if let Some(row) = self.rows.next() {
                    self.current = Some(row);
                    self.index = 0;
                } else if let Some(chunk) = self.chunks.next() {
                    self.rows = storage::rows(&chunk).into_iter();
                    continue;
                } else {
                    break;
                }
            }
            let ((key, _value), time, diff) = self.current.unwrap();
            let queries = &self.queries[&key];
            if let Some(&(qt, qr)) = queries.get(self.index) {
                output.push((key, time.max(qt).max(self.meet), diff * qr));
                self.index += 1;
            } else {
                self.current = None;
            }
        }
        (!output.is_empty()).then_some(output)
    }
}
