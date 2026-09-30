//! Methods on [`Arranged`] that read arrangements by cursor.

use timely::dataflow::Stream;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::Pipeline;
use timely::container::PushInto;

use crate::{Data, VecCollection, AsCollection};
use crate::difference::{Abelian, Multiply, Semigroup};
use crate::operators::arrange::{Arranged, TraceAgent};
use crate::trace::{SpanOf, Trace, TraceReader, Builder};
use crate::trace::implementations::containers::BatchContainer;
use crate::cursor::{Navigable, Cursor, BatchCursor, BatchDiff, BatchKey, BatchVal, BatchValOwn};
use crate::cursor::operators::join::join_traces;
use crate::cursor::operators::reduce::reduce_trace;

// Flattening arrangements into collections.
impl<'scope, Tr: TraceReader> Arranged<'scope, Tr> {
    /// Flattens the stream into a `VecCollection`.
    ///
    /// The underlying `Stream<T, Vec<SpanOf<T>>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    pub fn as_collection<D: Data, L>(self, mut logic: L) -> VecCollection<'scope, Tr::Time, D, BatchDiff<Tr>>
        where
            Tr::Batch: Navigable,
            BatchCursor<Tr>: Cursor<Time = Tr::Time>,
            L: FnMut(BatchKey<'_, Tr>, BatchVal<'_, Tr>) -> D+'static,
    {
        self.flat_map_ref(move |key, val| Some(logic(key,val)))
    }

    /// Flattens the stream into a `VecCollection`.
    ///
    /// The underlying `Stream<T, Vec<SpanOf<T>>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    ///
    /// The method takes `K` and `V` as generic arguments, in order to constrain the reference types to support
    /// cloning into owned types. If this bound does not work, the `as_collection` method allows arbitrary logic
    /// on the reference types.
    pub fn as_vecs<K, V>(self) -> VecCollection<'scope, Tr::Time, (K, V), BatchDiff<Tr>>
    where
        K: crate::ExchangeData,
        V: crate::ExchangeData,
        Tr::Batch: Navigable,
        BatchCursor<Tr>: Cursor<Time = Tr::Time>,
        for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V>,
    {
        self.flat_map_ref(move |key, val| [(key.clone(), val.clone())])
    }

    /// Extracts elements from an arrangement as a `VecCollection`.
    ///
    /// The supplied logic may produce an iterator over output values, allowing either
    /// filtering or flat mapping as part of the extraction.
    pub fn flat_map_ref<I, L>(self, logic: L) -> VecCollection<'scope, Tr::Time, I::Item, BatchDiff<Tr>>
        where
            Tr::Batch: Navigable,
            BatchCursor<Tr>: Cursor<Time = Tr::Time>,
            I: IntoIterator<Item: Data>,
            L: FnMut(BatchKey<'_, Tr>, BatchVal<'_, Tr>) -> I+'static,
    {
        Self::flat_map_batches(self.stream, logic)
    }

    /// Extracts elements from a stream of batches as a `VecCollection`.
    ///
    /// The supplied logic may produce an iterator over output values, allowing either
    /// filtering or flat mapping as part of the extraction.
    ///
    /// This method exists for streams of batches without the corresponding arrangement.
    /// If you have the arrangement, its `flat_map_ref` method is equivalent to this.
    pub fn flat_map_batches<I, L>(stream: Stream<'scope, Tr::Time, Vec<SpanOf<Tr>>>, mut logic: L) -> VecCollection<'scope, Tr::Time, I::Item, BatchDiff<Tr>>
    where
        Tr::Batch: Navigable,
        BatchCursor<Tr>: Cursor<Time = Tr::Time>,
        I: IntoIterator<Item: Data>,
        L: FnMut(BatchKey<'_, Tr>, BatchVal<'_, Tr>) -> I+'static,
    {
        stream.unary(Pipeline, "AsCollection", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                let mut session = output.session(&time);
                for wrapper in data.iter() {
                    let Some(batch) = wrapper.inner.as_ref() else { continue };
                    let mut cursor = batch.cursor();
                    while let Some(key) = cursor.get_key(batch) {
                        while let Some(val) = cursor.get_val(batch) {
                            for datum in logic(key, val) {
                                cursor.map_times(batch, |time, diff| {
                                    session.give((datum.clone(), <BatchCursor<Tr> as Cursor>::owned_time(time), <BatchCursor<Tr> as Cursor>::owned_diff(diff)));
                                });
                            }
                            cursor.step_val(batch);
                        }
                        cursor.step_key(batch);
                    }
                }
            });
        })
        .as_collection()
    }
}
// Direct join implementations.
impl<'scope, Tr1: TraceReader<Batch: Navigable>+'static> Arranged<'scope, Tr1> {
    /// A convenience method to join and produce `VecCollection` output.
    ///
    /// Avoid this method, as it is likely to evolve into one without the `VecCollection` opinion.
    pub fn join_core<Tr2,I,L,R1,R2,KC>(self, other: Arranged<'scope, Tr2>, mut result: L) -> VecCollection<'scope, Tr1::Time,I::Item,<R1 as Multiply<R2>>::Output>
    where
        Tr2: TraceReader<Batch: Navigable, Time=Tr1::Time>+Clone+'static,
        // Pin the cursor diffs to named params `R1`/`R2`: a `Multiply` bound on a projection
        // does not connect to its use-site (the solver normalizes the use but not the bound's
        // subject), so we constrain plain params instead.
        BatchCursor<Tr1>: Cursor<Diff = R1, Time = Tr1::Time, KeyContainer = KC>,
        BatchCursor<Tr2>: Cursor<Diff = R2, Time = Tr1::Time>,
        KC: BatchContainer,
        for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        R1: Multiply<R2, Output: Semigroup+'static> + Clone,
        I: IntoIterator<Item: Data>,
        L: FnMut(KC::ReadItem<'_>,BatchVal<'_, Tr1>,BatchVal<'_, Tr2>)->I+'static
    {
        let mut result = move |k: KC::ReadItem<'_>, v1: BatchVal<'_, Tr1>, v2: BatchVal<'_, Tr2>, t: Tr1::Time, r1: &R1, r2: &R2| {
            let r = (r1.clone()).multiply(r2);
            result(k, v1, v2).into_iter().map(move |d| (d, t.clone(), r.clone()))
        };

        join_traces::<_, _, _, _, crate::consolidation::ConsolidatingContainerBuilder<_>>(
            self,
            other,
            "Join",
            move |k, v1, v2, t, d1, d2, c| {
                for datum in result(k, v1, v2, t, d1, d2) {
                    c.push_into(datum);
                }
            }
        )
            .as_collection()
    }
}

// Direct reduce implementations.
impl<'scope, Tr1: TraceReader<Batch: Navigable>+'static> Arranged<'scope, Tr1> {
    /// A direct implementation of `ReduceCore::reduce_abelian`.
    pub fn reduce_abelian<L, Bu, Tr2, KC, P>(self, name: &str, mut logic: L, push: P) -> Arranged<'scope, TraceAgent<Tr2>>
    where
        Tr2: Trace<Batch: Navigable, Time=Tr1::Time>+'static,
        KC: BatchContainer,
        BatchCursor<Tr1>: Cursor<Time = Tr1::Time, KeyContainer = KC>,
        for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>, ValOwn: Data, Time = Tr2::Time, Diff: Abelian>,
        Bu: Builder<Time=Tr1::Time, Output: Into<Tr2::Batch>, Input: Default> + 'static,
        L: FnMut(KC::ReadItem<'_>, &[(BatchVal<'_, Tr1>, BatchDiff<Tr1>)], &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>)+'static,
        P: FnMut(&mut Bu::Input, KC::ReadItem<'_>, &mut Vec<(BatchValOwn<Tr2>, Tr2::Time, BatchDiff<Tr2>)>) + 'static,
    {
        self.reduce_core::<_,Bu,Tr2,KC,_>(name, move |key, input, output, change| {
            if !input.is_empty() {
                logic(key, input, change);
            }
            change.extend(output.drain(..).map(|(x,mut d)| { d.negate(); (x, d) }));
            crate::consolidation::consolidate(change);
        }, push)
    }

    /// A direct implementation of `ReduceCore::reduce_core`.
    pub fn reduce_core<L, Bu, Tr2, KC, P>(self, name: &str, logic: L, push: P) -> Arranged<'scope, TraceAgent<Tr2>>
    where
        Tr2: Trace<Batch: Navigable, Time=Tr1::Time>+'static,
        KC: BatchContainer,
        BatchCursor<Tr1>: Cursor<Time = Tr1::Time, KeyContainer = KC>,
        for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>, ValOwn: Data, Time = Tr2::Time>,
        Bu: Builder<Time=Tr1::Time, Output: Into<Tr2::Batch>, Input: Default> + 'static,
        L: FnMut(KC::ReadItem<'_>, &[(BatchVal<'_, Tr1>, BatchDiff<Tr1>)], &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>, &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>)+'static,
        P: FnMut(&mut Bu::Input, KC::ReadItem<'_>, &mut Vec<(BatchValOwn<Tr2>, Tr2::Time, BatchDiff<Tr2>)>) + 'static,
    {
        reduce_trace::<_,Bu,_,KC,_,_>(self, name, logic, push)
    }
}
