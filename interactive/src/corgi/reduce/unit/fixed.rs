//! Fixed-coordinate time arithmetic inside the coarse reducer boundary.
//!
//! Trace columns and operator frontiers retain the runtime's dynamic time type.
//! Only the proxy sweep and its presentations use scalar/product timestamps.

use differential_dataflow::operators::int_proxy::reduce::ProxyReduceTactic;
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::{Description, Span};
use timely::progress::Antichain;

use super::{
    CBatch, CorgiChunk, CorgiReduceTactic, IdentityReduce, MinKernel, ProxyTime, ScalarKernel,
    UnitKernel, leaf_depth,
};
use crate::corgi::time_kernel::{One, Two};
use crate::ir::Time;
use crate::parse::Reducer;

struct FixedTactic<P: ProxyTime<Time>, K: ScalarKernel> {
    inner: ProxyReduceTactic<P, IdentityReduce<Time, K, P>, K::Token, K::Token>,
}

impl<P: ProxyTime<Time>, K: ScalarKernel> FixedTactic<P, K> {
    fn new(key_depth: usize, value_depth: usize) -> Self {
        Self {
            inner: ProxyReduceTactic::new(IdentityReduce::new(key_depth, value_depth))
                .with_key_batch_size(1),
        }
    }
}

impl<P: ProxyTime<Time>, K: ScalarKernel> ReduceTactic<Time, CBatch<Time>, CBatch<Time>>
    for FixedTactic<P, K>
{
    fn retire(
        &mut self,
        source: Vec<CBatch<Time>>,
        output: Vec<CBatch<Time>>,
        input: Vec<CBatch<Time>>,
        lower: &Antichain<Time>,
        upper: &Antichain<Time>,
        held: &Antichain<Time>,
    ) -> (Option<Span<Time, CBatch<Time>>>, Antichain<Time>) {
        let decode = |frontier: &Antichain<Time>| {
            Antichain::from(
                frontier
                    .elements()
                    .iter()
                    .map(P::from_time)
                    .collect::<Vec<_>>(),
            )
        };
        let encode = |frontier: &Antichain<P>| {
            Antichain::from(
                frontier
                    .elements()
                    .iter()
                    .cloned()
                    .map(P::into_time)
                    .collect::<Vec<_>>(),
            )
        };
        let (span, pending) = self.inner.retire(
            source,
            output,
            input,
            &decode(lower),
            &decode(upper),
            &decode(held),
        );
        let span = span.map(|span| {
            Span::new(
                Description::new(
                    encode(span.desc.lower()),
                    encode(span.desc.upper()),
                    encode(span.desc.since()),
                ),
                span.inner,
            )
        });
        (span, encode(&pending))
    }
}

fn kernel<K: ScalarKernel + 'static>(
    key_depth: usize,
    value_depth: usize,
    depth: usize,
) -> Box<dyn ReduceTactic<Time, CBatch<Time>, CBatch<Time>>> {
    match depth {
        0 => Box::new(FixedTactic::<u64, K>::new(key_depth, value_depth)),
        1 => Box::new(FixedTactic::<One, K>::new(key_depth, value_depth)),
        2 => Box::new(FixedTactic::<Two, K>::new(key_depth, value_depth)),
        _ => unreachable!(),
    }
}

pub(super) fn select(
    reducer: &Reducer,
    first: &CorgiChunk<Time, crate::ir::Diff>,
    depth: usize,
) -> Box<dyn ReduceTactic<Time, CBatch<Time>, CBatch<Time>>> {
    if depth <= 2 {
        if let Some(key_depth) = leaf_depth(first.keys()) {
            match reducer {
                Reducer::Distinct if UnitKernel::depth(first.vals()).is_some() => {
                    return kernel::<UnitKernel>(key_depth, 0, depth);
                }
                Reducer::Min => {
                    if let Some(value_depth) = MinKernel::depth(first.vals()) {
                        return kernel::<MinKernel>(key_depth, value_depth, depth);
                    }
                }
                _ => {}
            }
        }
    }
    CorgiReduceTactic::<Time>::select(reducer, first, depth)
}

impl CorgiReduceTactic<Time> {
    /// Select fixed time arithmetic for known scope depths zero through two.
    /// Deeper scopes and unsupported value shapes retain the dynamic tactic.
    pub fn with_depth(reducer: Reducer, depth: usize) -> Self {
        let mut result = Self::new(reducer);
        result.depth = depth;
        result.select = select;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corgi::col_times::ColTimes;
    use differential_dataflow::lattice::Lattice;
    use timely::PartialOrder;

    fn check<P: ProxyTime<Time>>(depth: usize) {
        let values: Vec<_> = (0..3)
            .flat_map(|outer| {
                (0..3).flat_map(move |a| {
                    (0..3).map(move |b| {
                        let coordinates = [a, b];
                        Time::new(outer, coordinates[..depth].iter().copied().collect())
                    })
                })
            })
            .collect();
        let columns: ColTimes<Time> = values.iter().cloned().collect();
        for (index, value) in values.iter().enumerate() {
            assert_eq!(P::read(&columns, index).into_time(), *value);
            let fixed = P::from_time(value);
            assert_eq!(fixed.clone().into_time(), *value);
            for other in &values {
                let right = P::from_time(other);
                assert_eq!(fixed.cmp(&right), value.cmp(other));
                assert_eq!(fixed.less_equal(&right), value.less_equal(other));
                assert_eq!(fixed.join(&right).into_time(), value.join(other));
                assert_eq!(fixed.meet(&right).into_time(), value.meet(other));
            }
        }
    }

    #[test]
    fn fixed_bases_preserve_storage_order_and_lattice_operations() {
        check::<u64>(0);
        check::<One>(1);
        check::<Two>(2);
    }

    #[test]
    fn dispatched_retires_preserve_frontiers_corrections_and_deep_fallback() {
        use crate::corgi::chunk::columns_to_batch;
        use crate::ir::Diff;
        use corgi::{Value as CValue, arrange::leaf_slice};
        use differential_dataflow::consolidation::consolidate_updates;
        use std::rc::Rc;
        fn read(batches: &[CBatch<Time>]) -> Vec<((u64, u64), Time, Diff)> {
            let mut result = Vec::new();
            for batch in batches {
                for chunk in &batch.chunks {
                    let keys = leaf_slice(chunk.keys()).unwrap();
                    let values = leaf_slice(chunk.vals());
                    for i in 0..keys.len() {
                        result.push((
                            (keys[i], values.map_or(0, |v| v[i])),
                            chunk.times().get(i),
                            chunk.diffs()[i],
                        ));
                    }
                }
            }
            consolidate_updates(&mut result);
            result
        }
        for depth in 0..=3 {
            for reducer in [Reducer::Distinct, Reducer::Min] {
                let stamp = |outer, last| {
                    let mut coordinates = vec![0; depth];
                    if let Some(c) = coordinates.last_mut() {
                        *c = last;
                    }
                    Time::new(outer, coordinates.into_iter().collect())
                };
                let zero = stamp(0, 0);
                let left = stamp(0, 2);
                let right = stamp(1, 0);
                let middle = if depth == 0 {
                    stamp(2, 0)
                } else {
                    left.join(&right)
                };
                let end = stamp(3, 3);
                let make = |rows: Vec<(u64, u64, Time, Diff)>| {
                    Rc::new(columns_to_batch(
                        CValue::u64(rows.iter().map(|r| r.0).collect()),
                        if matches!(reducer, Reducer::Distinct) {
                            CValue::Unit(rows.len())
                        } else {
                            CValue::u64(rows.iter().map(|r| r.1).collect())
                        },
                        rows.iter().map(|r| r.2.clone()).collect(),
                        rows.iter().map(|r| r.3).collect(),
                    ))
                };
                let rounds = [
                    (
                        make(vec![
                            (7, 3, zero.clone(), 1),
                            (7, 7, left, 1),
                            (7, -1i64 as u64, right, 1),
                            (8, 9, zero.clone(), -3),
                        ]),
                        zero,
                        middle.clone(),
                    ),
                    (
                        make(vec![
                            (7, -1i64 as u64, middle.clone(), -1),
                            (8, 9, middle.clone(), 3),
                        ]),
                        middle,
                        end,
                    ),
                ];
                let mut typed = CorgiReduceTactic::with_depth(reducer.clone(), depth);
                let mut dynamic = CorgiReduceTactic::new(reducer);
                let (mut source, mut tout, mut dout) = (Vec::new(), Vec::new(), Vec::new());
                for (input, lower, upper) in rounds {
                    let lower = Antichain::from_elem(lower);
                    let upper = Antichain::from_elem(upper);
                    let (t, tp) = typed.retire(
                        source.clone(),
                        tout.clone(),
                        vec![input.clone()],
                        &lower,
                        &upper,
                        &lower,
                    );
                    let (d, dp) = dynamic.retire(
                        source.clone(),
                        dout.clone(),
                        vec![input.clone()],
                        &lower,
                        &upper,
                        &lower,
                    );
                    assert_eq!(tp, dp, "depth {depth}");
                    assert_eq!(t.is_some(), d.is_some());
                    if let (Some(t), Some(d)) = (t, d) {
                        assert_eq!(t.desc.lower(), d.desc.lower());
                        assert_eq!(t.desc.upper(), d.desc.upper());
                        assert_eq!(t.desc.since(), d.desc.since());
                        tout.extend(t.inner);
                        dout.extend(d.inner);
                    }
                    assert_eq!(read(&tout), read(&dout), "depth {depth}");
                    source.push(input);
                }
            }
        }
    }
}
