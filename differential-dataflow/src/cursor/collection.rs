//! Methods on [`VecCollection`] that arrange their input and read it back by cursor.

use timely::progress::Timestamp;

use crate::VecCollection as Collection;
use crate::difference::{Semigroup, Abelian, Multiply};
use crate::lattice::Lattice;
use crate::hashable::Hashable;
use crate::operators::arrange::{Arranged, TraceAgent};
use crate::trace::{Trace, Builder};
use crate::cursor::{BatchCursor, BatchDiff, BatchKey, BatchVal, Cursor, Navigable};

impl <'scope, T, K, V, R> Collection<'scope, T, (K, V), R>
where
    T: Timestamp + Lattice + Ord,
    K: crate::ExchangeData+Hashable,
    V: crate::ExchangeData,
    R: crate::ExchangeData+Semigroup,
{
    /// Applies a reduction function on records grouped by key.
    ///
    /// Input data must be structured as `(key, val)` pairs.
    /// The user-supplied reduction function takes as arguments
    ///
    /// 1. a reference to the key,
    /// 2. a reference to the slice of values and their accumulated updates,
    /// 3. a mutuable reference to a vector to populate with output values and accumulated updates.
    ///
    /// The user logic is only invoked for non-empty input collections, and it is safe to assume that the
    /// slice of input values is non-empty. The values are presented in sorted order, as defined by their
    /// `Ord` implementations.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///     // report the smallest value for each group
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map(|x| (x / 3, x))
    ///          .reduce(|_key, input, output| {
    ///              output.push((*input[0].0, 1))
    ///          });
    /// });
    /// ```
    pub fn reduce<L, V2: crate::Data, R2: Ord+Abelian+'static>(self, logic: L) -> Collection<'scope, T, (K, V2), R2>
    where L: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static {
        self.reduce_named("Reduce", logic)
    }

    /// As `reduce` with the ability to name the operator.
    pub fn reduce_named<L, V2: crate::Data, R2: Ord+Abelian+'static>(self, name: &str, logic: L) -> Collection<'scope, T, (K, V2), R2>
    where L: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static {
        use crate::trace::implementations::{ValBuilder, ValSpine};

        self.arrange_by_key_named(&format!("Arrange: {}", name))
            .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<K,V2,_,_>,_,_>(
                name,
                logic,
                |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
            )
            .as_collection(|k,v| (k.clone(), v.clone()))
    }

    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    ///
    /// This method is used by the more ergonomic `reduce`, `distinct`, and `count` methods, although
    /// it can be very useful if one needs to manually attach and re-use existing arranged collections.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let trace =
    ///     scope.new_collection_from(1 .. 10u32).1
    ///          .map(|x| (x, x))
    ///          .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>(
    ///             "Example",
    ///              move |_key, src, dst| dst.push((*src[0].0, 1))
    ///          )
    ///          .trace;
    /// });
    /// ```
    pub fn reduce_abelian<L, Bu, T2>(self, name: &str, mut logic: L) -> Arranged<'scope, TraceAgent<T2>>
    where
        T2: Trace<Batch: Navigable, Time=T>+'static,
        for<'a> BatchCursor<T2>: Cursor<Key<'a>= &'a K, ValOwn = V, Time = T2::Time, Diff: Abelian>,
        Bu: Builder<Time=T2::Time, Input = Vec<((K, V), T2::Time, BatchDiff<T2>)>, Output: Into<T2::Batch>> + 'static,
        L: FnMut(&K, &[(&V, R)], &mut Vec<(V, BatchDiff<T2>)>)+'static,
    {
        self.reduce_core::<_,Bu,T2>(name, move |key, input, output, change| {
            if !input.is_empty() { logic(key, input, change); }
            change.extend(output.drain(..).map(|(x,mut d)| { d.negate(); (x, d) }));
            crate::consolidation::consolidate(change);
        })
    }

    /// Solves for output updates when presented with inputs and would-be outputs.
    ///
    /// Unlike `reduce_arranged`, this method may be called with an empty `input`,
    /// and it may not be safe to index into the first element.
    /// At least one of the two collections will be non-empty.
    pub fn reduce_core<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<'scope, TraceAgent<T2>>
    where
        V: Clone+'static,
        T2: Trace<Batch: Navigable, Time=T>+'static,
        for<'a> BatchCursor<T2>: Cursor<Key<'a>=&'a K, ValOwn = V, Time = T2::Time>,
        Bu: Builder<Time=T2::Time, Input = Vec<((K, V), T2::Time, BatchDiff<T2>)>, Output: Into<T2::Batch>> + 'static,
        L: FnMut(&K, &[(&V, R)], &mut Vec<(V,BatchDiff<T2>)>, &mut Vec<(V, BatchDiff<T2>)>)+'static,
    {
        self.arrange_by_key_named(&format!("Arrange: {}", name))
            .reduce_core::<_,Bu,_,_,_>(
                name,
                logic,
                |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
            )
    }
}

impl<'scope, T, K, R1> Collection<'scope, T, K, R1>
where
    T: Timestamp + Lattice + Ord,
    K: crate::ExchangeData+Hashable,
    R1: crate::ExchangeData+Semigroup
{

    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///     // report at most one of each key.
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map(|x| x / 3)
    ///          .distinct();
    /// });
    /// ```
    pub fn distinct(self) -> Collection<'scope, T, K, isize> {
        self.distinct_core()
    }

    /// Distinct for general integer differences.
    ///
    /// This method allows `distinct` to produce collections whose difference
    /// type is something other than an `isize` integer, for example perhaps an
    /// `i32`.
    pub fn distinct_core<R2: Ord+Abelian+'static+From<i8>>(self) -> Collection<'scope, T, K, R2> {
        self.threshold_named("Distinct", |_,_| R2::from(1i8))
    }

    /// Transforms the multiplicity of records.
    ///
    /// The `threshold` function is obliged to map `R1::zero` to `R2::zero`, or at
    /// least the computation may behave as if it does. Otherwise, the transformation
    /// can be nearly arbitrary: the code does not assume any properties of `threshold`.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///     // report at most one of each key.
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map(|x| x / 3)
    ///          .threshold(|_,c| c % 2);
    /// });
    /// ```
    pub fn threshold<R2: Ord+Abelian+'static, F: FnMut(&K, &R1)->R2+'static>(self, thresh: F) -> Collection<'scope, T, K, R2> {
        self.threshold_named("Threshold", thresh)
    }

    /// A `threshold` with the ability to name the operator.
    pub fn threshold_named<R2: Ord+Abelian+'static, F: FnMut(&K,&R1)->R2+'static>(self, name: &str, mut thresh: F) -> Collection<'scope, T, K, R2> {
        use crate::trace::implementations::{KeyBuilder, KeySpine};

        self.arrange_by_self_named(&format!("Arrange: {}", name))
            .reduce_abelian::<_,KeyBuilder<K,T,R2>,KeySpine<K,T,R2>,_,_>(
                name,
                move |k,s,t| t.push(((), thresh(k, &s[0].1))),
                |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
            )
            .as_collection(|k,_| k.clone())
    }

}

impl<'scope, T, K, R> Collection<'scope, T, K, R>
where
    T: Timestamp + Lattice + Ord,
    K: crate::ExchangeData+Hashable,
    R: crate::ExchangeData+Semigroup
{

    /// Counts the number of occurrences of each element.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///     // report the number of occurrences of each key
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map(|x| x / 3)
    ///          .count();
    /// });
    /// ```
    pub fn count(self) -> Collection<'scope, T, (K, R), isize> { self.count_core() }

    /// Count for general integer differences.
    ///
    /// This method allows `count` to produce collections whose difference
    /// type is something other than an `isize` integer, for example perhaps an
    /// `i32`.
    pub fn count_core<R2: Ord + Abelian + From<i8> + 'static>(self) -> Collection<'scope, T, (K, R), R2> {
        use crate::trace::implementations::{ValBuilder, ValSpine};
        self.arrange_by_self_named("Arrange: Count")
            .reduce_abelian::<_,ValBuilder<K,R,T,R2>,ValSpine<K,R,T,R2>,_,_>(
                "Count",
                |_k,s,t| t.push((s[0].1.clone(), R2::from(1i8))),
                |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
            )
            .as_collection(|k,c| (k.clone(), c.clone()))
    }
}

/// Methods which assert properties of collections, and consolidate to do so.
impl<'scope, T: Timestamp + Clone + 'static, D: Clone+'static, R: Clone+'static> Collection<'scope, T, D, R> {
    /// Assert if the collection is ever non-empty.
    ///
    /// Because this is a dataflow fragment, the test is only applied as the computation is run. If the computation
    /// is not run, or not run to completion, there may be un-exercised times at which the collection could be
    /// non-empty. Typically, a timely dataflow computation runs to completion on drop, and so clean exit from a
    /// program should indicate that this assertion never found cause to complain.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map(|x| x * 2)
    ///          .filter(|x| x % 2 == 1)
    ///          .assert_empty();
    /// });
    /// ```
    pub fn assert_empty(self)
    where
        D: crate::ExchangeData+Hashable,
        R: crate::ExchangeData+Hashable + Semigroup,
        T: Lattice+Ord,
    {
        self.consolidate()
            .inspect(|x| panic!("Assertion failed: non-empty collection: {:?}", x));
    }
}

/// Methods requiring an Abelian difference, to support negation.
impl<'scope, T: Timestamp + Clone + 'static, D: Clone+'static, R: Abelian+'static> Collection<'scope, T, D, R> {
    /// Assert if the collections are ever different.
    ///
    /// Because this is a dataflow fragment, the test is only applied as the computation is run. If the computation
    /// is not run, or not run to completion, there may be un-exercised times at which the collections could vary.
    /// Typically, a timely dataflow computation runs to completion on drop, and so clean exit from a program should
    /// indicate that this assertion never found cause to complain.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let data = scope.new_collection_from(1 .. 10).1;
    ///
    ///     let odds = data.clone().filter(|x| x % 2 == 1);
    ///     let evens = data.clone().filter(|x| x % 2 == 0);
    ///
    ///     odds.concat(evens)
    ///         .assert_eq(data);
    /// });
    /// ```
    pub fn assert_eq(self, other: Self)
    where
        D: crate::ExchangeData+Hashable,
        R: crate::ExchangeData+Hashable,
        T: Lattice+Ord,
    {
        self.negate()
            .concat(other)
            .assert_empty();
    }
}

/// Methods which require data be arrangeable.
impl<'scope, T, D, R> Collection<'scope, T, D, R>
where
    T: Timestamp + Clone + 'static + Lattice,
    D: crate::ExchangeData+Hashable,
    R: crate::ExchangeData+Semigroup,
{
    /// Aggregates the weights of equal records into at most one record.
    ///
    /// This method uses the type `D`'s `hashed()` method to partition the data. The data are
    /// accumulated in place, each held back until their timestamp has completed.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///     x.clone()
    ///      .negate()
    ///      .concat(x)
    ///      .consolidate() // <-- ensures cancellation occurs
    ///      .assert_empty();
    /// });
    /// ```
    pub fn consolidate(self) -> Self {
        use crate::trace::implementations::{KeyBatcher, KeySpine};
        self.consolidate_named::<_, KeySpine<_,_,_>,_>("Consolidate", KeyBatcher::new, |key,&()| key.clone())
    }

    /// As `consolidate` but with the ability to name the operator, specify the trace type,
    /// and provide the function `reify` to produce owned keys and values..
    pub fn consolidate_named<Ba, Tr, F>(self, name: &str, batcher: impl FnOnce(Option<crate::logging::Logger>, usize) -> Ba, reify: F) -> Self
    where
        Ba: crate::batcher::Batcher<Vec<((D, ()), T, R)>, Time = T, Output: Into<Tr::Batch>> + 'static,
        Tr: crate::trace::Trace<Batch: Navigable, Time=T>+'static,
        for<'a> BatchCursor<Tr>: Cursor<Time=Tr::Time, Diff=R>,
        F: Fn(BatchKey<'_, Tr>, BatchVal<'_, Tr>) -> D + 'static,
    {
        self.map(|k| (k, ()))
            .arrange_named::<Ba, Tr>(name, batcher)
            .as_collection(reify)
    }
}

impl<'scope, T, K, V, R> Collection<'scope, T, (K, V), R>
where
    T: Timestamp + Lattice + Ord,
    K: crate::ExchangeData+Hashable,
    V: crate::ExchangeData,
    R: crate::ExchangeData+Semigroup,
{
    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and yields pairs `(key, (val1, val2))`.
    ///
    /// The [`join_map`](Join::join_map) method may be more convenient for non-trivial processing pipelines.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///     let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
    ///     let z = scope.new_collection_from(vec![(0, (1, 'a')), (1, (3, 'b'))]).1;
    ///
    ///     x.join(y)
    ///      .assert_eq(z);
    /// });
    /// ```
    pub fn join<V2, R2>(self, other: Collection<'scope, T, (K,V2), R2>) -> Collection<'scope, T, (K,(V,V2)), <R as Multiply<R2>>::Output>
    where
        K:  crate::ExchangeData,
        V2: crate::ExchangeData,
        R2: crate::ExchangeData+Semigroup,
        R: Multiply<R2, Output: Semigroup+'static>,
    {
        self.join_map(other, |k,v,v2| (k.clone(),(v.clone(),v2.clone())))
    }

    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and then applies a function.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///     let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
    ///     let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
    ///
    ///     x.join_map(y, |_key, &a, &b| (a,b))
    ///      .assert_eq(z);
    /// });
    /// ```
    pub fn join_map<V2: crate::ExchangeData, R2: crate::ExchangeData+Semigroup, D: crate::Data, L>(self, other: Collection<'scope, T, (K, V2), R2>, mut logic: L) -> Collection<'scope, T, D, <R as Multiply<R2>>::Output>
    where R: Multiply<R2, Output: Semigroup+'static>, L: FnMut(&K, &V, &V2)->D+'static {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_key();
        arranged1.join_core(arranged2, move |k,v1,v2| Some(logic(k,v1,v2)))
    }

    /// Matches pairs `(key, val)` and `key` based on `key`, producing the former with frequencies multiplied.
    ///
    /// When the second collection contains frequencies that are either zero or one this is the more traditional
    /// relational semijoin. When the second collection may contain multiplicities, this operation may scale up
    /// the counts of the records in the first input.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///     let y = scope.new_collection_from(vec![0, 2]).1;
    ///     let z = scope.new_collection_from(vec![(0, 1)]).1;
    ///
    ///     x.semijoin(y)
    ///      .assert_eq(z);
    /// });
    /// ```
    pub fn semijoin<R2: crate::ExchangeData+Semigroup>(self, other: Collection<'scope, T, K, R2>) -> Collection<'scope, T, (K, V), <R as Multiply<R2>>::Output>
    where R: Multiply<R2, Output: Semigroup+'static> {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_self();
        arranged1.join_core(arranged2, |k,v,_| Some((k.clone(), v.clone())))
    }

    /// Subtracts the semijoin with `other` from `self`.
    ///
    /// In the case that `other` has multiplicities zero or one this results
    /// in a relational antijoin, in which we discard input records whose key
    /// is present in `other`. If the multiplicities could be other than zero
    /// or one, the semantic interpretation of this operator is less clear.
    ///
    /// In almost all cases, you should ensure that `other` has multiplicities
    /// that are zero or one, perhaps by using the `distinct` operator.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///     let y = scope.new_collection_from(vec![0, 2]).1;
    ///     let z = scope.new_collection_from(vec![(1, 3)]).1;
    ///
    ///     x.antijoin(y)
    ///      .assert_eq(z);
    /// });
    /// ```
    pub fn antijoin<R2: crate::ExchangeData+Semigroup>(self, other: Collection<'scope, T, K, R2>) -> Collection<'scope, T, (K, V), R>
    where R: Multiply<R2, Output=R>, R: Abelian+'static {
        self.clone().concat(self.semijoin(other).negate())
    }

    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function,
    /// which produces something implementing `IntoIterator`, where the output collection will have an entry for
    /// every value returned by the iterator.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::trace::Trace;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let x = scope.new_collection_from(vec![(0u32, 1), (1, 3)]).1
    ///                  .arrange_by_key();
    ///     let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1
    ///                  .arrange_by_key();
    ///
    ///     let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
    ///
    ///     x.join_core(y, |_key, &a, &b| Some((a, b)))
    ///      .assert_eq(z);
    /// });
    /// ```
    pub fn join_core<Tr2,I,L,R2> (self, stream2: Arranged<'scope, Tr2>, result: L) -> Collection<'scope, T,I::Item,<R as Multiply<R2>>::Output>
    where
        Tr2: crate::trace::TraceReader<Batch: Navigable, Time=T>+Clone+'static,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a>=&'a K>,
        // Pin the cursor diff to a named param `R2`: a `Multiply` bound on a projection does not
        // connect to its use-site (the solver normalizes the use but not the bound's subject).
        BatchCursor<Tr2>: Cursor<Diff = R2, Time = T>,
        R: Multiply<R2, Output: Semigroup+'static>,
        I: IntoIterator<Item: crate::Data>,
        L: FnMut(&K,&V,BatchVal<'_, Tr2>)->I+'static,
    {
        self.arrange_by_key()
            .join_core(stream2, result)
    }
}
