//! Coarse dispatch to compiled reductions over identity-key columns.
//!
//! Experimental extraction: unit-valued distinct, with unit proxy tokens.
//! This draft still requires scalar identity keys and chooses from a nonempty batch.
//! General threshold support and construction-time shape metadata are separate work.
//! Only `retire` is virtual; presentation, accumulation and emission stay typed.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::rc::Rc;

use corgi::arrange::leaf_slice;
use corgi::Value as CValue;
use differential_dataflow::operators::int_proxy::reduce::{
    ProxyReduceBackend, ProxyReduceTactic, ReduceInstance, ReduceWindow,
};
use differential_dataflow::operators::int_proxy::ProxyBridge;
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::{Description, Span};
use timely::progress::Antichain;

use super::{chunks_of, CBatch, CorgiReduceBackend};
use crate::corgi::chunk::{columns_to_batch, CorgiChunk};
use crate::corgi::col_times::ColTime;
use crate::corgi::search::MatchingRanges;
use crate::ir::Diff;
use crate::parse::Reducer;

/// Select one compiled tactic when the first nonempty input reveals its shape.
/// Empty retires do not pin the choice, which matters on sparsely fed workers.
pub struct CorgiDistinctTactic<T: ColTime> {
    reducer: Reducer,
    inner: Option<Box<dyn ReduceTactic<T, CBatch<T>, CBatch<T>>>>,
}

impl<T: ColTime> CorgiDistinctTactic<T> {
    pub fn new(reducer: Reducer) -> Self {
        let inner = if matches!(reducer, Reducer::Distinct) {
            None
        } else {
            Some(Self::general(reducer.clone()))
        };
        Self { reducer, inner }
    }

    fn general(reducer: Reducer) -> Box<dyn ReduceTactic<T, CBatch<T>, CBatch<T>>> {
        Box::new(ProxyReduceTactic::<T, _, u64, u64>::new(
            CorgiReduceBackend::new(reducer),
        ))
    }
}

impl<T: ColTime> ReduceTactic<T, CBatch<T>, CBatch<T>> for CorgiDistinctTactic<T> {
    fn retire(
        &mut self,
        source: Vec<CBatch<T>>,
        output: Vec<CBatch<T>>,
        input: Vec<CBatch<T>>,
        lower: &Antichain<T>,
        upper: &Antichain<T>,
        held: &Antichain<T>,
    ) -> (Option<Span<T, CBatch<T>>>, Antichain<T>) {
        if self.inner.is_none() {
            // No tactic exists yet, hence no pending times. Mirror the ordinary
            // retire's eligibility check before looking for a shape.
            if held.elements().iter().all(|t| upper.less_equal(t)) {
                return (None, held.clone());
            }
            let first = input
                .iter()
                .chain(source.iter())
                .flat_map(|b| b.chunks.iter())
                .find(|c| !c.diffs().is_empty());
            let Some(first) = first else {
                return (
                    Some(Span::new(
                        Description::new(
                            lower.clone(),
                            upper.clone(),
                            Antichain::from_elem(T::minimum()),
                        ),
                        None,
                    )),
                    Antichain::new(),
                );
            };
            self.inner = Some(match (leaf_depth(first.keys()), first.vals()) {
                (Some(depth), CValue::Unit(_)) if matches!(self.reducer, Reducer::Distinct) => {
                    Box::new(ProxyReduceTactic::<T, _, (), ()>::new(UnitDistinct::new(
                        depth,
                    )))
                }
                _ => Self::general(self.reducer.clone()),
            });
        }
        self.inner
            .as_mut()
            .unwrap()
            .retire(source, output, input, lower, upper, held)
    }
}

/// A bare u64 or nested singleton products thereof. Preserve the exact product
/// wrapping when emitting keys; a carried hash is not an identity key.
fn leaf_depth(keys: &CValue) -> Option<usize> {
    leaf_slice(keys)?;
    let (mut column, mut depth) = (keys, 0);
    while let CValue::Prod(fields) = column {
        depth += 1;
        column = &fields[0];
    }
    Some(depth)
}

fn threshold_corrections(input: &[((), Diff)], output: &[((), Diff)], into: &mut Vec<((), Diff)>) {
    let present = input.iter().any(|(_, d)| *d != 0);
    let current: Diff = output.iter().map(|(_, d)| *d).sum();
    let correction = Diff::from(present) - current;
    if correction != 0 {
        into.push(((), correction));
    }
}

struct UnitDistinct<T: ColTime> {
    key_depth: usize,
    scratch: ProxyBridge<T, Diff, ()>,
    run_ends: Vec<usize>,
    keys: Vec<u64>,
    times: Vec<T>,
    diffs: Vec<Diff>,
}

impl<T: ColTime> UnitDistinct<T> {
    fn new(key_depth: usize) -> Self {
        Self {
            key_depth,
            scratch: Vec::new(),
            run_ends: Vec::new(),
            keys: Vec::new(),
            times: Vec::new(),
            diffs: Vec::new(),
        }
    }

    /// Select already ordered `(key, value, time, diff)` runs directly from chunks.
    /// There are no value IDs, value pools, gathers, or per-row shape dispatch.
    fn present(
        &mut self,
        chunks: &[&CorgiChunk<T, Diff>],
        changed: &[u64],
        into: &mut ProxyBridge<T, Diff, ()>,
    ) {
        self.scratch.clear();
        self.run_ends.clear();
        into.clear();
        for chunk in chunks.iter().filter(|c| !c.diffs().is_empty()) {
            assert_eq!(
                leaf_depth(chunk.keys()),
                Some(self.key_depth),
                "identity reduce: key shape changed"
            );
            assert_eq!(
                matches!(chunk.vals(), CValue::Unit(_)).then_some(0),
                Some(0),
                "identity reduce: value shape changed"
            );
            let keys = leaf_slice(chunk.keys()).unwrap();
            let before = self.scratch.len();
            for (j, range) in MatchingRanges::new(changed, keys) {
                for i in range {
                    self.scratch
                        .push(((changed[j], ()), chunk.times().get(i), chunk.diffs()[i]));
                }
            }
            if self.scratch.len() > before {
                self.run_ends.push(self.scratch.len());
            }
        }
        if self.run_ends.len() <= 1 {
            std::mem::swap(into, &mut self.scratch);
            return;
        }
        // Identity tokens preserve physical key/value/time ordering. Merge and
        // cancel equal triples; raw novel time support is kept apart.
        let mut heap = BinaryHeap::new();
        let mut lo = 0;
        for (run, &hi) in self.run_ends.iter().enumerate() {
            let ((key, value), time, _) = &self.scratch[lo];
            heap.push(Reverse((*key, *value, time, run, lo)));
            lo = hi;
        }
        while let Some(mut head) = heap.peek_mut() {
            let Reverse((key, value, time, run, i)) = *head;
            let diff = self.scratch[i].2;
            if let Some(last) = into
                .last_mut()
                .filter(|r| r.0 == (key, value) && &r.1 == time)
            {
                last.2 += diff;
            } else {
                if into.last().is_some_and(|r| r.2 == 0) {
                    into.pop();
                }
                into.push(((key, value), time.clone(), diff));
            }
            if i + 1 < self.run_ends[run] {
                let ((key, value), time, _) = &self.scratch[i + 1];
                // Advance the existing run head and repair the root once.
                *head = Reverse((*key, *value, time, run, i + 1));
            } else {
                std::collections::binary_heap::PeekMut::pop(head);
            }
        }
        if into.last().is_some_and(|r| r.2 == 0) {
            into.pop();
        }
    }
}

impl<T: ColTime> ProxyReduceBackend<T, CBatch<T>, CBatch<T>, (), ()> for UnitDistinct<T> {
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, _: Description<T>) {
        self.keys.clear();
        self.times.clear();
        self.diffs.clear();
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, T, CBatch<T>, CBatch<T>>,
        changed: &[u64],
        from: &mut Option<u64>,
        window: &mut ReduceWindow<T, Diff, Diff, (), ()>,
    ) {
        if from.take().is_none() {
            return;
        }
        let novel = chunks_of(instance.input_batches);
        let mut keys = changed.to_vec();
        // Seeds retain the novel time support even if presentation subsequently
        // cancels a novel update against prior history at exactly the same time.
        for chunk in &novel {
            if chunk.diffs().is_empty() {
                continue;
            }
            let ids = leaf_slice(chunk.keys()).expect("identity reduce: identity keys");
            for (i, &key) in ids.iter().enumerate() {
                keys.push(key);
                window.seeds.push((key, chunk.times().get(i)));
            }
        }
        keys.sort_unstable();
        keys.dedup();
        window.seeds.sort_unstable();
        window.seeds.dedup();
        let mut chunks = chunks_of(instance.source_batches);
        chunks.extend(novel);
        self.present(&chunks, &keys, &mut window.input);
        self.present(
            &chunks_of(instance.output_batches),
            &keys,
            &mut window.output,
        );
    }

    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[((), Diff)],
        out_ends: &[usize],
        output: &[((), Diff)],
    ) -> (Vec<((), Diff)>, Vec<usize>) {
        let mut corrections = Vec::with_capacity(keys.len());
        let mut ends = Vec::with_capacity(keys.len());
        let (mut is, mut os) = (0, 0);
        for (&ie, &oe) in in_ends.iter().zip(out_ends) {
            threshold_corrections(&input[is..ie], &output[os..oe], &mut corrections);
            ends.push(corrections.len());
            is = ie;
            os = oe;
        }
        (corrections, ends)
    }

    fn emit(&mut self, records: &[((u64, ()), T, Diff)]) {
        for ((key, ()), time, diff) in records {
            self.keys.push(*key);
            self.times.push(time.clone());
            self.diffs.push(*diff);
        }
    }

    fn finish(&mut self) -> Option<CBatch<T>> {
        if self.times.is_empty() {
            return None;
        }
        let mut keys = CValue::u64(std::mem::take(&mut self.keys));
        for _ in 0..self.key_depth {
            keys = CValue::Prod(vec![keys]);
        }
        Some(Rc::new(columns_to_batch(
            keys,
            CValue::Unit(self.times.len()),
            std::mem::take(&mut self.times),
            std::mem::take(&mut self.diffs),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::consolidation::consolidate_updates;
    use timely::order::Product;
    use timely::PartialOrder;

    fn batch<T: ColTime>(rows: &[(u64, T, Diff)], depth: usize) -> CBatch<T> {
        let mut keys = CValue::u64(rows.iter().map(|r| r.0).collect());
        for _ in 0..depth {
            keys = CValue::Prod(vec![keys]);
        }
        Rc::new(columns_to_batch(
            keys,
            CValue::Unit(rows.len()),
            rows.iter().map(|r| r.1.clone()).collect(),
            rows.iter().map(|r| r.2).collect(),
        ))
    }

    fn records<T: ColTime>(batches: &[CBatch<T>]) -> ProxyBridge<T, Diff, ()> {
        let mut result = Vec::new();
        for c in chunks_of(batches) {
            assert!(matches!(c.vals(), CValue::Unit(_)));
            for (i, &key) in leaf_slice(c.keys()).unwrap().iter().enumerate() {
                result.push(((key, ()), c.times().get(i), c.diffs()[i]));
            }
        }
        consolidate_updates(&mut result);
        result
    }

    #[test]
    fn presentation_merges_cancelling_runs_with_seek_and_scan() {
        let a: Vec<_> = (0..1000).map(|k| (k, 0u64, 1)).collect();
        let b: Vec<_> = (0..1000)
            .flat_map(|k| [(k, 0u64, -1), (k, 1u64, 2)])
            .collect();
        for depth in 0..=2 {
            let batches = [batch(&b, depth), batch(&[], depth), batch(&a, depth)];
            for changed in [vec![1, 777], (0..1000).collect()] {
                let mut expected = records(&batches);
                expected.retain(|r| changed.binary_search(&r.0 .0).is_ok());
                let mut actual = Vec::new();
                UnitDistinct::new(depth).present(&chunks_of(&batches), &changed, &mut actual);
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn partial_order_pending_and_negative_counts_match_general_and_oracle() {
        type T = Product<u64, u64>;
        let t = T::new;
        // Incomparable insertions induce a correction at their join. Later
        // updates cancel earlier input at the same time, retaining raw seeds.
        let rounds = [
            vec![(7, t(0, 2), 1), (7, t(2, 0), 1), (8, t(0, 0), -3)],
            vec![(7, t(0, 2), -1), (8, t(1, 1), 3)],
            vec![(7, t(3, 1), -1), (7, t(3, 3), 2)],
        ];
        for depth in 0..=2 {
            let mut unit = CorgiDistinctTactic::new(Reducer::Distinct);
            let mut general = CorgiDistinctTactic::general(Reducer::Distinct);
            let mut source = Vec::new();
            let mut uout = Vec::new();
            let mut gout = Vec::new();
            for novel in &rounds {
                let input = vec![batch(novel, depth)];
                let lower = Antichain::from_elem(t(0, 0));
                let upper = Antichain::from_elem(t(2, 2));
                let held = lower.clone();
                let (u, up) = unit.retire(
                    source.clone(),
                    uout.clone(),
                    input.clone(),
                    &lower,
                    &upper,
                    &held,
                );
                let (g, gp) = general.retire(
                    source.clone(),
                    gout.clone(),
                    input.clone(),
                    &lower,
                    &upper,
                    &held,
                );
                assert_eq!(up, gp);
                if let Some(b) = u.and_then(|s| s.inner) {
                    uout.push(b);
                }
                if let Some(b) = g.and_then(|s| s.inner) {
                    gout.push(b);
                }
                source.extend(input);
                // Drain pending interesting times without novel input.
                let (u, up) = unit.retire(
                    source.clone(),
                    uout.clone(),
                    vec![],
                    &upper,
                    &Antichain::new(),
                    &up,
                );
                let (g, gp) = general.retire(
                    source.clone(),
                    gout.clone(),
                    vec![],
                    &upper,
                    &Antichain::new(),
                    &gp,
                );
                assert_eq!(up, gp);
                assert!(up.is_empty());
                if let Some(b) = u.and_then(|s| s.inner) {
                    uout.push(b);
                }
                if let Some(b) = g.and_then(|s| s.inner) {
                    gout.push(b);
                }
                let actual = records(&uout);
                assert_eq!(actual, records(&gout));
                let original = records(&source);
                for x in 0..=4 {
                    for y in 0..=4 {
                        for key in [7, 8] {
                            let time = t(x, y);
                            let count: Diff = original
                                .iter()
                                .filter(|r| r.0 .0 == key && r.1.less_equal(&time))
                                .map(|r| r.2)
                                .sum();
                            let distinct: Diff = actual
                                .iter()
                                .filter(|r| r.0 .0 == key && r.1.less_equal(&time))
                                .map(|r| r.2)
                                .sum();
                            assert_eq!(
                                distinct,
                                Diff::from(count != 0),
                                "key {key}, time {time:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn empty_first_retire_does_not_pin_dispatch_and_nonunit_falls_back() {
        let mut tactic = CorgiDistinctTactic::<u64>::new(Reducer::Distinct);
        let lower = Antichain::from_elem(0);
        let upper = Antichain::from_elem(1);
        tactic.retire(vec![], vec![], vec![batch(&[], 0)], &lower, &upper, &lower);
        assert!(tactic.inner.is_none());
        let input = Rc::new(columns_to_batch(
            CValue::u64(vec![1]),
            CValue::u64(vec![9]),
            vec![0],
            vec![1],
        ));
        let (result, pending) = tactic.retire(vec![], vec![], vec![input], &lower, &upper, &lower);
        assert!(pending.is_empty());
        let result = result.unwrap().inner.unwrap();
        assert!(matches!(result.chunks[0].vals(), CValue::Unit(1)));
        assert_eq!(result.chunks[0].diffs(), &[1]);
    }

    #[test]
    fn compound_key_retractions_use_general_fallback() {
        use crate::corgi::chunk::{present_key, recover_key};
        use crate::corgi::logic::{transcode, untranscode};
        use crate::ir::Value;
        let pair = |a, b| Value::Tuple(vec![Value::Int(a), Value::Int(b)]);
        let shape = corgi::Shape::Prod(vec![corgi::Shape::Prim(64); 2]);
        let mut tactic = CorgiDistinctTactic::new(Reducer::Distinct);
        let mut source = Vec::new();
        let mut output = Vec::new();
        for epoch in 0..3u64 {
            let keys = vec![pair(1, 2), pair(1, 3)];
            let values = CValue::Unit(2);
            let diffs = match epoch {
                0 => vec![2, -1],
                1 => vec![-2, 1],
                _ => vec![-3, 4],
            };
            let input = Rc::new(columns_to_batch(
                present_key(transcode(&keys, &shape)),
                values,
                vec![epoch; 2],
                diffs,
            ));
            // This representation cannot enter the scalar identity-key arm.
            assert!(leaf_depth(input.chunks[0].keys()).is_none());
            let lower = Antichain::from_elem(epoch);
            let upper = Antichain::from_elem(epoch + 1);
            let (result, pending) = tactic.retire(
                source.clone(),
                output.clone(),
                vec![input.clone()],
                &lower,
                &upper,
                &lower,
            );
            assert!(pending.is_empty());
            source.push(input);
            if let Some(batch) = result.and_then(|s| s.inner) {
                output.push(batch);
            }
            let mut actual = Vec::new();
            for chunk in chunks_of(&output) {
                let keys = untranscode(recover_key(chunk.keys()), &shape);
                let values =
                    untranscode(chunk.vals().clone(), &corgi::shape_of_value(chunk.vals()));
                for (i, (key, value)) in keys.into_iter().zip(values).enumerate() {
                    actual.push(((key, value), chunk.diffs()[i]));
                }
            }
            differential_dataflow::consolidation::consolidate(&mut actual);
            let expected = if epoch == 1 {
                vec![]
            } else {
                vec![
                    ((pair(1, 2), Value::unit()), 1),
                    ((pair(1, 3), Value::unit()), 1),
                ]
            };
            assert_eq!(actual, expected, "epoch {epoch}");
        }
    }
}
