//! Arranges a collection into a re-usable trace structure.
//!
//! The `arrange` operator applies to a differential dataflow `Collection` and returns an `Arranged`
//! structure, provides access to both an indexed form of accepted updates as well as a stream of
//! batches of newly arranged updates.
//!
//! Several operators (`join`, `reduce`, and `count`, among others) are implemented against `Arranged`,
//! and can be applied directly to arranged data instead of the collection. Internally, the operators
//! will borrow the shared state, and listen on the timely stream for shared batches of data. The
//! resources to index the collection---communication, computation, and memory---are spent only once,
//! and only one copy of the index needs to be maintained as the collection changes.
//!
//! The arranged collection is stored in a trace, whose append-only operation means that it is safe to
//! share between the single `arrange` writer and multiple readers. Each reader is expected to interrogate
//! the trace only at times for which it knows the trace is complete, as indicated by the frontiers on its
//! incoming channels. Failing to do this is "safe" in the Rust sense of memory safety, but the reader may
//! see ill-defined data at times for which the trace is not complete. (All current implementations
//! commit only completed data to the trace).

use timely::dataflow::operators::{Enter, vec::Map};
use timely::order::PartialOrder;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::progress::Timestamp;
use timely::progress::Antichain;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::progress::Stamp;

use crate::AsCollection;
use crate::lattice::Lattice;
use crate::logging::Logger;
use crate::batcher::Batcher;
use crate::trace::{self, Description, SpanOf, Trace, TraceReader};

use trace::wrappers::enter::{TraceEnter, enter_span};

use super::TraceAgent;

/// An arranged collection of `(K,V)` values.
///
/// An `Arranged` allows multiple differential operators to share the resources (communication,
/// computation, memory) required to produce and maintain an indexed representation of a collection.
pub struct Arranged<'scope, Tr: TraceReader> {
    /// A stream containing arranged updates.
    ///
    /// This stream contains the same batches of updates the trace itself accepts, so there should
    /// be no additional overhead to receiving these records. The batches can be navigated just as
    /// the batches in the trace, by key and by value.
    pub stream: Stream<'scope, Tr::Time, Vec<SpanOf<Tr>>>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: Tr,
}

impl<'scope, Tr: TraceReader+Clone> Clone for Arranged<'scope, Tr> {
    fn clone(&self) -> Self {
        Arranged {
            stream: self.stream.clone(),
            trace: self.trace.clone(),
        }
    }
}

use ::timely::progress::timestamp::Refines;
use timely::Container;

impl<'scope, Tr: TraceReader> Arranged<'scope, Tr> {
    /// Brings an arranged collection into a nested scope.
    ///
    /// This method produces a proxy trace handle that uses the same backing data, but acts as if the timestamps
    /// have all been extended with an additional coordinate with the default value. The resulting collection does
    /// not vary with the new timestamp coordinate.
    pub fn enter<'inner, TInner>(self, child: Scope<'inner, TInner>) -> Arranged<'inner, TraceEnter<Tr, TInner>>
    where
        TInner: Refines<Tr::Time>+Lattice,
    {
        Arranged {
            stream: self.stream.enter(child).map(enter_span),
            trace: TraceEnter::make_from(self.trace),
        }
    }

    /// Brings an arranged collection into a nested region.
    ///
    /// This method only applies to *regions*, which are subscopes with the same timestamp
    /// as their containing scope. In this case, the trace type does not need to change.
    pub fn enter_region<'inner>(self, child: Scope<'inner, Tr::Time>) -> Arranged<'inner, Tr> {
        Arranged {
            stream: self.stream.enter(child),
            trace: self.trace,
        }
    }

    /// Extracts a collection of any container from the stream of batches.
    ///
    /// This method is like `self.stream.flat_map`, except that it produces containers
    /// directly, rather than form a container of containers as `flat_map` would.
    pub fn as_container<I, L>(self, mut logic: L) -> crate::Collection<'scope, Tr::Time, I::Item>
    where
        I: IntoIterator<Item: Container>,
        L: FnMut(SpanOf<Tr>) -> I+'static,
    {
        self.stream.unary(Pipeline, "AsContainer", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                let mut session = output.session(&time);
                for wrapper in data.drain(..) {
                    for mut container in logic(wrapper) {
                        session.give_container(&mut container);
                    }
                }
            });
        })
        .as_collection()
    }
}


impl<'scope, Tr: TraceReader> Arranged<'scope, Tr> {
    /// Brings an arranged collection out of a nested region.
    ///
    /// This method only applies to *regions*, which are subscopes with the same timestamp
    /// as their containing scope. In this case, the trace type does not need to change.
    pub fn leave_region<'outer>(self, outer: Scope<'outer, Tr::Time>) -> Arranged<'outer, Tr> {
        use timely::dataflow::operators::Leave;
        Arranged {
            stream: self.stream.leave(outer),
            trace: self.trace,
        }
    }
}

/// Arranges a stream of updates by a key, configured with a name and a parallelization contract.
///
/// This operator arranges a stream of values into a shared trace, whose contents it maintains.
/// It uses the supplied parallelization contract to distribute the data, which does not need to
/// be consistently by key (though this is the most common).
pub fn arrange_core<'scope, P, C, Ba, Tr>(
    stream: Stream<'scope, Tr::Time, C>,
    pact: P,
    name: &str,
    batcher: impl FnOnce(Option<Logger>, usize) -> Ba,
) -> Arranged<'scope, TraceAgent<Tr>>
where
    C: Container + Clone + 'static,
    P: ParallelizationContract<Tr::Time, C>,
    Ba: Batcher<C, Time = Tr::Time, Output: Into<Tr::Batch>> + 'static,
    Tr: Trace+'static,
{
    // The `Arrange` operator is tasked with reacting to an advancing input
    // frontier by producing the sequence of batches whose lower and upper
    // bounds are those frontiers, containing updates at times greater or
    // equal to lower and not greater or equal to upper.
    //
    // The operator uses its batch type's `Batcher`, which accepts update
    // triples and responds to requests to "seal" batches (presented as new
    // upper frontiers).
    //
    // Each sealed batch is presented to the trace, and if at all possible
    // transmitted along the outgoing channel. Empty batches may not have
    // a corresponding capability, as they are only retained for actual data
    // held by the batcher, which may prevents the operator from sending an
    // empty batch.

    let mut reader: Option<TraceAgent<Tr>> = None;

    // fabricate a data-parallel operator that holds capabilities and consults its input frontier.
    let reader_ref = &mut reader;
    let scope = stream.scope();

    let stream = stream.unary_frontier(pact, name, move |_capability, info| {

        // Acquire a logger for arrange events.
        let logger = scope.worker().logger_for::<crate::logging::DifferentialEventBuilder>("differential/arrange").map(Into::into);

        // Where we will deposit received updates, and from which we extract batches.
        let mut batcher = batcher(logger.clone(), info.global_id);

        // Capabilities for the lower envelope of updates in `batcher`.
        let mut capabilities = Antichain::<Capability<Tr::Time>>::new();

        let activator = Some(scope.activator_for(std::rc::Rc::clone(&info.address)));
        let mut empty_trace = Tr::new(info.clone(), logger.clone(), activator);
        // If there is default exertion logic set, install it.
        if let Some(exert_logic) = scope.worker().config().get::<trace::ExertionLogic>("differential/default_exert_logic").cloned() {
            empty_trace.set_exert_logic(exert_logic);
        }

        let (reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);

        *reader_ref = Some(reader_local);

        // Initialize to the minimal input frontier.
        let mut prev_frontier = Antichain::from_elem(Tr::Time::minimum());

        move |(input, frontier), output| {

            // As we receive data, we need to (i) stash the data and (ii) keep *enough* capabilities.
            // We don't have to keep all capabilities, but we need to be able to form output messages
            // when we realize that time intervals are complete.

            input.for_each(|cap, data| {
                for capability in cap.retain_stamp(0).iter() {
                    capabilities.insert(capability.clone());
                }
                batcher.insert(data);
            });

            // The frontier may have advanced by multiple elements, which is an issue because
            // timely dataflow currently only allows one capability per message. This means we
            // must pretend to process the frontier advances one element at a time, batching
            // and sending smaller bites than we might have otherwise done.

            // Assert that the frontier never regresses.
            assert!(PartialOrder::less_equal(&prev_frontier.borrow(), &frontier.frontier()));

            // Test to see if strict progress has occurred, which happens whenever the new
            // frontier isn't equal to the previous. It is only in this case that we have any
            // data processing to do.
            if prev_frontier.borrow() != frontier.frontier() {
                // There are two cases to handle with some care:
                //
                // 1. If any held capabilities are not in advance of the new input frontier,
                //    we must carve out updates now in advance of the new input frontier and
                //    transmit them as a batch, stamped with the capabilities they retire.
                //
                // 2. If there are no held capabilities in advance of the new input frontier,
                //    then there are no updates not in advance of the new input frontier and
                //    we can simply create an empty input batch with the new upper frontier
                //    and feed this to the trace agent (but not along the timely output).

                // If there is at least one capability not in advance of the input frontier ...
                if capabilities.elements().iter().any(|c| !frontier.less_equal(c.time())) {

                    // The capabilities to retire: those not in advance of the input frontier.
                    // Each update extracted below is greater or equal to one of them, as updates
                    // supported only by the remaining capabilities are in advance of the input
                    // frontier and remain in the batcher.
                    let retired = capabilities
                        .elements()
                        .iter()
                        .filter(|c| !frontier.less_equal(c.time()))
                        .cloned()
                        .collect::<CapabilitySet<_>>();

                    // Extract all updates not in advance of the input frontier, as one batch.
                    // The batch spans the interval from the previously reported frontier to the
                    // current one, which is exactly the interval the batcher carves out.
                    let description = Description::new(
                        prev_frontier.clone(),
                        frontier.frontier().to_owned(),
                        Antichain::from_elem(Tr::Time::minimum()),
                    );
                    let (extracted, retained) = batcher.extract(frontier.frontier());

                    let batch = trace::Span::new(description, extracted.map(Into::into));

                    let stamp = retired.iter().map(|c| c.time().clone()).collect::<Stamp<_>>();
                    writer.insert(batch.clone(), stamp);

                    // send the batch to downstream consumers, empty or not.
                    output.session(&retired).give(batch);

                    // Having extracted and sent the batch of updates not in advance of the input
                    // frontier, we downgrade all capabilities to match the batcher's lower update
                    // frontier.
                    // This may involve discarding capabilities, which is fine as any new updates arrive
                    // in messages with new capabilities.
                    let mut new_capabilities = Antichain::new();
                    for time in retained.iter() {
                        if let Some(capability) = capabilities.elements().iter().find(|c| c.time().less_equal(time)) {
                            new_capabilities.insert(capability.delayed(time));
                        }
                        else {
                            panic!("failed to find capability");
                        }
                    }
                    capabilities = new_capabilities;
                }
                else {
                    // Announce progress updates, even without data. No held capability precedes
                    // the input frontier, so no update does either, and the batcher has nothing
                    // to extract.
                    writer.seal(frontier.frontier().to_owned());
                }

                prev_frontier.clear();
                prev_frontier.extend(frontier.frontier().iter().cloned());
            }

            writer.exert();
        }
    });

    Arranged { stream, trace: reader.unwrap() }
}
