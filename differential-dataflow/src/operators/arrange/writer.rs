//! Write endpoint for a sequence of batches.
//!
//! A `TraceWriter` accepts a sequence of batches and distributes them
//! to both a shared trace and to a sequence of private queues.

use std::rc::{Rc, Weak};
use std::cell::RefCell;

use timely::progress::{Antichain, Stamp, Timestamp};
use timely::progress::operate::FrontierInterest;

use crate::lattice::Lattice;
use crate::trace::{Trace, Span, SpanOf};

use super::TraceAgentQueueWriter;
use super::TraceReplayInstruction;
use super::agent::trace_box::TraceBox;

/// What an arranging operator does with the spans it forms.
///
/// The arranging operators (`arrange_core_with`, `arrange_from_upsert_with`, `reduce_with_tactic_with`)
/// hand each span they form to a writer, seal the intervals they learn are complete, and exert
/// maintenance effort on every activation. The writer decides where the spans go. What happens when
/// a writer is dropped is the implementation's business; operators do not rely on it.
pub trait Writer {
    /// The timestamp type of the spans.
    type Time: Timestamp + Lattice;
    /// The batch type the spans carry.
    type Batch: 'static + Clone;

    /// Introduces the next span.
    ///
    /// `hint` is the stamp of the message carrying the span's updates: a set of times each at or
    /// below some update, and suitable for use as capabilities. It is empty exactly when the span
    /// carries no updates.
    fn insert(&mut self, span: Span<Self::Time, Self::Batch>, hint: Stamp<Self::Time>);

    /// Introduces an update-free span up to `upper`, if `upper` differs from the current upper.
    fn seal(&mut self, upper: Antichain<Self::Time>);

    /// Exerts maintenance effort, even without additional updates.
    fn exert(&mut self);

    /// Whether the writing operator must run on every input frontier advance.
    ///
    /// An operator that holds no capabilities has no updates to form into spans. Its only business
    /// on a frontier advance is to `seal` the newly complete interval, and the only parties that
    /// need those update-free spans are readers in other dataflows, which learn progress from them.
    /// A writer with such readers (the default, as a [`TraceAgent`](super::TraceAgent) has) answers
    /// `Always`; a writer without them can answer `IfCapability`, so that the operator runs on a
    /// frontier advance only while it holds capabilities. Readers in the operator's own dataflow
    /// take their frontiers from timely, and need no spans without updates.
    fn frontier_interest(&self) -> FrontierInterest { FrontierInterest::Always }
}

/// Write endpoint for a sequence of batches.
///
/// A `TraceWriter` accepts a sequence of batches and distributes them
/// to a shared trace and, if it has them, to a sequence of private queues.
pub struct TraceWriter<Tr: Trace> {
    /// Current upper limit.
    upper: Antichain<Tr::Time>,
    /// Shared trace, possibly absent (due to weakness).
    trace: Weak<RefCell<TraceBox<Tr>>>,
    /// A sequence of private queues into which batches are written, when the trace is importable.
    queues: Option<Rc<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>>,
}

impl<Tr: Trace> TraceWriter<Tr> {
    /// Creates a new `TraceWriter`.
    ///
    /// The `queues` are those of a [`TraceAgent`](super::TraceAgent), whose importers listen on them;
    /// a [`TraceLocal`](super::TraceLocal) has none.
    pub fn new(
        upper: Vec<Tr::Time>,
        trace: Weak<RefCell<TraceBox<Tr>>>,
        queues: Option<Rc<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>>,
    ) -> Self
    {
        let mut temp = Antichain::new();
        temp.extend(upper);
        Self { upper: temp, trace, queues }
    }
}

impl<Tr: Trace> Writer for TraceWriter<Tr> {

    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn exert(&mut self) {
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.exert();
        }
    }

    fn insert(&mut self, span: SpanOf<Tr>, hint: Stamp<Tr::Time>) {

        // Something is wrong if not a sequence.
        if !(&self.upper == span.lower()) {
            println!("{:?} vs {:?}", self.upper, span.lower());
        }
        assert!(&self.upper == span.lower());
        assert!(span.lower() != span.upper());

        self.upper.clone_from(span.upper());

        // push information to each listener that still exists.
        if let Some(queues) = &self.queues {
            let mut borrow = queues.borrow_mut();
            for queue in borrow.iter_mut() {
                if let Some(pair) = queue.upgrade() {
                    pair.1.borrow_mut().push_back(TraceReplayInstruction::Span(span.clone(), hint.clone()));
                    pair.1.borrow_mut().push_back(TraceReplayInstruction::Frontier(span.upper().clone()));
                    pair.0.activate();
                }
            }
            borrow.retain(|w| w.upgrade().is_some());
        }

        // push data to the trace, if it still exists.
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.insert(span);
        }

    }

    fn seal(&mut self, upper: Antichain<Tr::Time>) {
        if self.upper != upper {
            self.insert(Span::empty(self.upper.clone(), upper), Stamp::new());
        }
    }

    /// `Always` with listener queues, which learn progress only from sealed spans; `IfCapability` without.
    fn frontier_interest(&self) -> FrontierInterest {
        if self.queues.is_some() { FrontierInterest::Always } else { FrontierInterest::IfCapability }
    }
}

impl<Tr: Trace> Drop for TraceWriter<Tr> {
    fn drop(&mut self) {
        self.seal(Antichain::new())
    }
}
