//! Named sources consuming pinned ledger snapshots.
//!
//! Capture a snapshot once and pass the same value to every worker.
//! Publishing or refreshing is collective; worker zero introduces the updates exactly once.
//! Ledger storage and the source arrangement have independent lifetimes.
//! Use explicit frontier waits and `snapshot_as_of` with these sources.
//! The compatibility `tick` assumes epoch-paced width-one sources and can wait indefinitely for an independently clocked ledger.
//! DDIR times are unsigned, so this bridge rejects negative ledger coordinates.

use serde::{Deserialize, Serialize};

use super::*;
use crate::ledger::{self, Cohort, Payload, Registration, Span, Store};
use crate::runtime::col_times::ColTimes;

type InputHandle = timely::dataflow::operators::core::unordered_input::UnorderedHandle<HostTime, timely::container::CapacityContainerBuilder<HostContainer>>;
type Capability = timely::dataflow::operators::ActivateCapability<HostTime>;

#[derive(Serialize, Deserialize)]
struct Batch {
    keys: Vec<u8>,
    vals: Vec<u8>,
    diffs: Vec<Diff>,
    lanes: Vec<Vec<i64>>,
}

/// Encode raw ledger updates with all of their signed product-time lanes.
/// Each lane must have one coordinate per update; the ledger registration determines the width.
/// This is an internal payload encoding, independent of the server's binary wire frames.
pub fn encode_ledger_batch(updates: &Updates, lanes: &[Vec<i64>]) -> Result<Vec<u8>, String> {
    if updates.keys.len() != updates.len() || updates.vals.len() != updates.len()
        || lanes.iter().any(|lane| lane.len() != updates.len()) {
        return Err("ledger batch columns have different lengths".into());
    }
    let (mut keys, mut vals) = (Vec::new(), Vec::new());
    corgi::bytes::write_to(&updates.keys, &mut keys).map_err(|error| error.to_string())?;
    corgi::bytes::write_to(&updates.vals, &mut vals).map_err(|error| error.to_string())?;
    bincode::serialize(&Batch {
        keys, vals, diffs: updates.diffs.clone(), lanes: lanes.to_vec(),
    }).map_err(|error| error.to_string())
}

/// An immutable, validated cut of one ledger and its payloads.
/// Capturing once outside the worker group prevents workers from reading different commits.
#[derive(Clone)]
pub struct LedgerSnapshot {
    id: ledger::Id,
    registration: Registration,
    shape: RowShape,
    spans: Vec<(Span, HostContainer)>,
    initial: Antichain<HostTime>,
    frontier: Antichain<HostTime>,
}

impl LedgerSnapshot {
    /// Read one committed cohort state, then retrieve its immutable payloads.
    /// Only raw append history is supported; compacted or discontinuous histories are refused.
    pub fn capture(store: &dyn Store, root: &str, id: ledger::Id) -> Result<Self, String> {
        let state = Cohort::new(store, root).snapshot().map_err(|error| format!("ledger snapshot: {error:?}"))?;
        let registration = state.registry.get(&id).ok_or_else(|| format!("unknown ledger {id}"))?.clone();
        let shape = match &registration.shape.columns {
            corgi::Shape::Prod(columns) if columns.len() == 2 => (columns[0].clone(), columns[1].clone()),
            _ => return Err("a DDIR ledger must declare its columns as (key, val)".into()),
        };
        let width = registration.shape.time_width;
        if width == 0 { return Err("a DDIR ledger needs a positive time width".into()); }
        let initial = convert_frontier(&registration.start, width)?;
        let mut frontier = initial.clone();
        let mut spans = Vec::new();
        for span in state.manifest.into_iter().filter(|span| span.id == id) {
            let lower = convert_frontier(&span.lower, width)?;
            let upper = convert_frontier(&span.upper, width)?;
            if lower != frontier || !PartialOrder::less_than(&lower, &upper) || span.since != registration.start {
                return Err("ledger source requires contiguous raw append spans".into());
            }
            let container = match &span.batch {
                Payload::Empty => HostContainer {
                    keys: corgi::Value::empty(&shape.0), vals: corgi::Value::empty(&shape.1),
                    times: ColTimes::from_raw_lanes(vec![Vec::new(); width], 0), diffs: Vec::new(),
                },
                payload => {
                    let bytes = match payload {
                        Payload::Inline(bytes) => bytes.clone(),
                        Payload::Blob(hash) => store.get(*hash).map_err(|error| format!("ledger payload: {error:?}"))?,
                        Payload::Empty => unreachable!(),
                    };
                    let batch: Batch = bincode::deserialize(&bytes).map_err(|error| format!("ledger batch: {error}"))?;
                    let (keys, key_bytes) = corgi::bytes::read_from(&batch.keys)?;
                    let (vals, val_bytes) = corgi::bytes::read_from(&batch.vals)?;
                    let rows = batch.diffs.len();
                    if key_bytes != batch.keys.len() || val_bytes != batch.vals.len()
                        || keys.len() != rows || vals.len() != rows || batch.lanes.len() != width
                        || batch.lanes.iter().any(|lane| lane.len() != rows) {
                        return Err("ledger batch dimensions differ from its registration".into());
                    }
                    if (corgi::shape_of_value(&keys), corgi::shape_of_value(&vals)) != shape {
                        return Err("ledger batch column shapes differ from its registration".into());
                    }
                    let lanes = batch.lanes.into_iter().map(|lane| lane.into_iter().map(|value| {
                        u64::try_from(value).map_err(|_| "DDIR ledger times must be nonnegative".to_string())
                    }).collect()).collect::<Result<Vec<Vec<_>>, _>>()?;
                    let times = ColTimes::from_raw_lanes(lanes, rows);
                    if (0..rows).any(|row| !lower.less_equal(&times.get(row)) || upper.less_equal(&times.get(row))) {
                        return Err("ledger batch contains an update outside its span".into());
                    }
                    HostContainer { keys, vals, times, diffs: batch.diffs }
                },
            };
            spans.push((span, container));
            frontier = upper;
        }
        Ok(Self { id, registration, shape, spans, initial, frontier })
    }

    /// The committed write frontier, in the DDIR timestamp representation.
    pub fn frontier(&self) -> &Antichain<HostTime> { &self.frontier }

    /// The declared product-time width, including trailing zero coordinates.
    pub fn time_width(&self) -> usize { self.registration.shape.time_width }
}

fn convert_frontier(frontier: &ledger::Frontier, width: usize) -> Result<Antichain<HostTime>, String> {
    frontier.elements().iter().map(|time| {
        if time.len() > width { return Err("ledger frontier exceeds its declared width".into()); }
        (0..width).map(|index| u64::try_from(time.get(index).copied().unwrap_or(i64::MIN))
            .map_err(|_| "DDIR ledger times must be nonnegative".to_string()))
            .collect::<Result<Vec<_>, _>>().map(|point| HostTime::new(point.into()))
    }).collect()
}

pub(super) struct LedgerSource {
    input: InputHandle,
    capabilities: Vec<Capability>,
    snapshot: LedgerSnapshot,
    dataflow_id: usize,
}

impl LedgerSource {
    pub(super) fn frontier(&self) -> &Antichain<HostTime> { &self.snapshot.frontier }
    pub(super) fn initial(&self) -> &Antichain<HostTime> { &self.snapshot.initial }

    fn refresh(&mut self, snapshot: &LedgerSnapshot, writer: bool) {
        if writer {
            for (_, container) in &snapshot.spans[self.snapshot.spans.len()..] {
                let mut groups = vec![Vec::new(); self.capabilities.len()];
                for row in 0..container.times.len() {
                    let capability = self.capabilities.iter().position(|cap| cap.time().less_equal(&container.times.get(row)))
                        .expect("a raw update is at or beyond the preceding write frontier");
                    groups[capability].push(row);
                }
                for (capability, rows) in self.capabilities.iter().zip(groups).filter(|(_, rows)| !rows.is_empty()) {
                    let updates = Updates { keys: container.keys.clone(), vals: container.vals.clone(), diffs: container.diffs.clone() }.select(&rows);
                    let times = ColTimes::from_raw_lanes(container.times.raw_lanes().iter()
                        .map(|lane| rows.iter().map(|&row| lane[row]).collect()).collect(), rows.len());
                    let mut data = HostContainer { keys: updates.keys, vals: updates.vals, times, diffs: updates.diffs };
                    self.input.activate().session(capability).give_container(&mut data);
                }
            }
        }
        self.capabilities = snapshot.frontier.elements().iter().map(|point| {
            self.capabilities.iter().find(|cap| cap.time().less_equal(point))
                .expect("committed append frontiers advance monotonically").delayed(point)
        }).collect();
        self.snapshot = snapshot.clone();
    }
}

impl Server {
    /// Publish an arrangement consuming this pinned ledger cut, without installing a program.
    /// Collective: every worker must receive the same name and snapshot.
    /// The ledger and its storage already exist independently of this arrangement.
    pub fn publish_ledger(&mut self, worker: &mut Worker, name: &str, snapshot: &LedgerSnapshot) -> Result<(), String> {
        if name.is_empty() || name.contains("::") || name == "clock" || Recipe::parse(name).is_some()
            || crate::diag::is_source(name) || self.programs.contains_key(name) || self.outputs.contains_key(name) {
            return Err(format!("ledger source name {name:?} is reserved or already in use"));
        }
        let dataflow_id = worker.next_dataflow_index();
        self.note_dataflow(dataflow_id, name);
        let probe = ProbeHandle::new();
        let ((input, capability), trace) = host_dataflow(worker, |scope| {
            use timely::dataflow::operators::core::UnorderedInput as _;
            use timely::dataflow::operators::Probe;
            use differential_dataflow::AsCollection;
            let (input, stream) = scope.new_unordered_input::<timely::container::CapacityContainerBuilder<HostContainer>>();
            let arranged = crate::backend::corgi::arrange_export(stream.as_collection());
            arranged.stream.probe_with(&probe);
            (input, arranged.trace)
        });
        let empty = LedgerSnapshot { spans: Vec::new(), frontier: Antichain::from_elem(HostTime::default()), ..snapshot.clone() };
        let mut source = LedgerSource { input, capabilities: vec![capability], snapshot: empty, dataflow_id };
        source.refresh(snapshot, worker.index() == 0);
        self.traces.insert(name.to_string(), trace);
        self.shapes.insert(name.to_string(), snapshot.shape.clone());
        self.outputs.insert(name.to_string(), Output {
            width: snapshot.time_width(), owner: name.to_string(), probe, since: HostTime::default(),
        });
        self.ledger_sources.insert(name.to_string(), source);
        Ok(())
    }

    /// Consume the newly committed suffix of a later cut; never reread worker-local storage.
    /// Collective: every worker must receive the same snapshot.
    /// Refreshing the same cut is harmless; a replaced or shortened history is refused.
    pub fn refresh_ledger(&mut self, worker: &Worker, name: &str, snapshot: &LedgerSnapshot) -> Result<(), String> {
        let source = self.ledger_sources.get_mut(name).ok_or_else(|| format!("no ledger source {name:?}"))?;
        if source.snapshot.id != snapshot.id || source.snapshot.registration != snapshot.registration
            || source.snapshot.spans.len() > snapshot.spans.len()
            || !source.snapshot.spans.iter().zip(&snapshot.spans).all(|((old, _), (new, _))| old == new) {
            return Err("ledger refresh must extend the same committed append history".into());
        }
        source.refresh(snapshot, worker.index() == 0);
        Ok(())
    }

    /// Drop only the consuming arrangement, once its readers are gone.
    /// This leaves the ledger's registry entry, spans, and payloads intact.
    pub fn unpublish_ledger(&mut self, worker: &mut Worker, name: &str) -> Result<(), String> {
        if !self.ledger_sources.contains_key(name) { return Err(format!("no ledger source {name:?}")); }
        if self.importers.get(name).copied().unwrap_or(0) != 0 {
            return Err(format!("ledger source {name:?} still has live importers"));
        }
        let source = self.ledger_sources.remove(name).unwrap();
        self.traces.remove(name);
        self.outputs.remove(name);
        self.shapes.remove(name);
        self.importers.remove(name);
        let dataflow_id = source.dataflow_id;
        drop(source);
        worker.drop_dataflow(dataflow_id);
        Ok(())
    }
}
