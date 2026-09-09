//! Coarse dispatch to compiled reductions over identity-key columns.
//!
//! Experimental extraction: signed scalar minimum, with raw u64 proxy tokens.
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
pub struct CorgiMinTactic<T: ColTime> {
    reducer: Reducer,
    inner: Option<Box<dyn ReduceTactic<T, CBatch<T>, CBatch<T>>>>,
}

impl<T: ColTime> CorgiMinTactic<T> {
    pub fn new(reducer: Reducer) -> Self {
        let inner = if matches!(reducer, Reducer::Min) {
            None
        } else {
            Some(Self::general(reducer.clone()))
        };
        Self { reducer, inner }
    }

    fn general(reducer: Reducer) -> Box<dyn ReduceTactic<T, CBatch<T>, CBatch<T>>> {
        Box::new(ProxyReduceTactic::<T, _>::new(CorgiReduceBackend::new(
            reducer,
        )))
    }
}

impl<T: ColTime> ReduceTactic<T, CBatch<T>, CBatch<T>> for CorgiMinTactic<T> {
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
                (Some(key_depth), values)
                    if matches!(self.reducer, Reducer::Min) && leaf_depth(values).is_some() =>
                {
                    Box::new(ProxyReduceTactic::<T, _>::new(ScalarMin::new(
                        key_depth,
                        leaf_depth(values).unwrap(),
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

fn min_corrections(input: &[(u64, Diff)], output: &[(u64, Diff)], into: &mut Vec<(u64, Diff)>) {
    // Physical token order is unsigned; DDIR integer minimum is signed.
    // Negative multiplicities remain present, just as in the general reducer.
    let desired = input
        .iter()
        .filter(|(_, d)| *d != 0)
        .map(|(v, _)| *v)
        .min_by_key(|v| *v as i64);
    let mut inserted = false;
    for &(value, diff) in output {
        let keep = Some(value) == desired;
        inserted |= keep;
        let correction = Diff::from(keep) - diff;
        if correction != 0 {
            into.push((value, correction));
        }
    }
    if !inserted {
        if let Some(value) = desired {
            into.push((value, 1));
        }
    }
}
fn value_column(values: Vec<u64>, depth: usize) -> CValue {
    let mut column = CValue::u64(values);
    for _ in 0..depth {
        column = CValue::Prod(vec![column]);
    }
    column
}

struct ScalarMin<T: ColTime> {
    key_depth: usize,
    value_depth: usize,
    values: Vec<u64>,
    scratch: ProxyBridge<T, Diff>,
    run_ends: Vec<usize>,
    keys: Vec<u64>,
    times: Vec<T>,
    diffs: Vec<Diff>,
}

impl<T: ColTime> ScalarMin<T> {
    fn new(key_depth: usize, value_depth: usize) -> Self {
        Self {
            key_depth,
            value_depth,
            values: Vec::new(),
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
        into: &mut ProxyBridge<T, Diff>,
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
                leaf_depth(chunk.vals()),
                Some(self.value_depth),
                "identity reduce: value shape changed"
            );
            let values = leaf_slice(chunk.vals());
            let keys = leaf_slice(chunk.keys()).unwrap();
            let before = self.scratch.len();
            for (j, range) in MatchingRanges::new(changed, keys) {
                for i in range {
                    self.scratch.push((
                        (changed[j], values.unwrap()[i]),
                        chunk.times().get(i),
                        chunk.diffs()[i],
                    ));
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

impl<T: ColTime> ProxyReduceBackend<T, CBatch<T>, CBatch<T>> for ScalarMin<T> {
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, _: Description<T>) {
        self.keys.clear();
        self.values.clear();
        self.times.clear();
        self.diffs.clear();
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, T, CBatch<T>, CBatch<T>>,
        changed: &[u64],
        from: &mut Option<u64>,
        window: &mut ReduceWindow<T, Diff, Diff>,
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
        input: &[(u64, Diff)],
        out_ends: &[usize],
        output: &[(u64, Diff)],
    ) -> (Vec<(u64, Diff)>, Vec<usize>) {
        let mut corrections = Vec::with_capacity(keys.len());
        let mut ends = Vec::with_capacity(keys.len());
        let (mut is, mut os) = (0, 0);
        for (&ie, &oe) in in_ends.iter().zip(out_ends) {
            min_corrections(&input[is..ie], &output[os..oe], &mut corrections);
            ends.push(corrections.len());
            is = ie;
            os = oe;
        }
        (corrections, ends)
    }

    fn emit(&mut self, records: &[((u64, u64), T, Diff)]) {
        for ((key, value), time, diff) in records {
            self.keys.push(*key);
            self.values.push(*value);
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
            value_column(std::mem::take(&mut self.values), self.value_depth),
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

    fn scalar_records<T: ColTime>(batches: &[CBatch<T>]) -> ProxyBridge<T, Diff> {
        let mut result = Vec::new();
        for c in chunks_of(batches) {
            let keys = leaf_slice(c.keys()).unwrap();
            let values = leaf_slice(c.vals()).unwrap();
            for i in 0..keys.len() {
                result.push(((keys[i], values[i]), c.times().get(i), c.diffs()[i]));
            }
        }
        consolidate_updates(&mut result);
        result
    }

    #[test]
    fn scalar_min_preserves_signed_order_wrapping_and_pending_corrections() {
        type T = Product<u64, u64>;
        let t = T::new;
        let initial = vec![
            ((7, 0), t(0, 0), 1),
            ((7, i64::MIN as u64), t(1, 0), 1),
            ((7, -1i64 as u64), t(0, 1), 1),
            ((8, i64::MAX as u64), t(0, 0), -3),
            ((8, i64::MIN as u64), t(0, 1), 2),
        ];
        let novel = vec![
            ((7, i64::MIN as u64), t(1, 1), -1),
            ((8, i64::MIN as u64), t(1, 1), -2),
        ];
        for key_depth in 0..=2 {
            for value_depth in 0..=2 {
                let wrap = |values: Vec<u64>, depth| {
                    let mut column = CValue::u64(values);
                    for _ in 0..depth {
                        column = CValue::Prod(vec![column]);
                    }
                    column
                };
                let batch = |rows: &[((u64, u64), T, Diff)]| {
                    Rc::new(columns_to_batch(
                        wrap(rows.iter().map(|r| r.0 .0).collect(), key_depth),
                        wrap(rows.iter().map(|r| r.0 .1).collect(), value_depth),
                        rows.iter().map(|r| r.1).collect(),
                        rows.iter().map(|r| r.2).collect(),
                    ))
                };
                let mut specialized = CorgiMinTactic::new(Reducer::Min);
                let mut general = CorgiMinTactic::general(Reducer::Min);
                let mut source = Vec::new();
                let (mut sout, mut gout) = (Vec::new(), Vec::new());
                for (rows, lower, upper) in
                    [(&initial, t(0, 0), t(1, 1)), (&novel, t(1, 1), t(2, 2))]
                {
                    let input = vec![batch(rows)];
                    let lower = Antichain::from_elem(lower);
                    let upper = Antichain::from_elem(upper);
                    let (s, sp) = specialized.retire(
                        source.clone(),
                        sout.clone(),
                        input.clone(),
                        &lower,
                        &upper,
                        &lower,
                    );
                    let (g, gp) = general.retire(
                        source.clone(),
                        gout.clone(),
                        input.clone(),
                        &lower,
                        &upper,
                        &lower,
                    );
                    assert_eq!(sp, gp);
                    if let Some(batch) = s.and_then(|s| s.inner) {
                        sout.push(batch);
                    }
                    if let Some(batch) = g.and_then(|s| s.inner) {
                        gout.push(batch);
                    }
                    source.extend(input);
                    assert_eq!(scalar_records(&sout), scalar_records(&gout));
                }
                let original = scalar_records(&source);
                let actual = scalar_records(&sout);
                for time in [t(0, 0), t(0, 1), t(1, 0), t(1, 1)] {
                    for key in [7, 8] {
                        let mut input = std::collections::BTreeMap::new();
                        let mut output = std::collections::BTreeMap::new();
                        for ((k, value), at, diff) in &original {
                            if *k == key && at.less_equal(&time) {
                                *input.entry(*value as i64).or_insert(0) += diff;
                            }
                        }
                        for ((k, value), at, diff) in &actual {
                            if *k == key && at.less_equal(&time) {
                                *output.entry(*value as i64).or_insert(0) += diff;
                            }
                        }
                        output.retain(|_, diff| *diff != 0);
                        let expected: std::collections::BTreeMap<_, _> = input
                            .into_iter()
                            .filter(|(_, diff)| *diff != 0)
                            .take(1)
                            .map(|(value, _)| (value, 1))
                            .collect();
                        assert_eq!(output, expected, "key {key} at {time:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn compound_key_retractions_use_general_fallback() {
        use crate::corgi::chunk::{present_key, recover_key};
        use crate::corgi::logic::{transcode, untranscode};
        use crate::ir::Value;
        let pair = |a, b| Value::Tuple(vec![Value::Int(a), Value::Int(b)]);
        let shape = corgi::Shape::Prod(vec![corgi::Shape::Prim(64); 2]);
        let mut tactic = CorgiMinTactic::new(Reducer::Min);
        let mut source = Vec::new();
        let mut output = Vec::new();
        for epoch in 0..3u64 {
            let keys = vec![pair(1, 2), pair(1, 3)];
            let values = transcode(&[pair(-1, 2), pair(3, -4)], &shape);
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
                    ((pair(1, 2), pair(-1, 2)), 1),
                    ((pair(1, 3), pair(3, -4)), 1),
                ]
            };
            assert_eq!(actual, expected, "epoch {epoch}");
        }
    }
}
