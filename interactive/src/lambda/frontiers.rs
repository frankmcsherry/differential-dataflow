//! Shared frontier operations over `List(List(int))` antichains of product times.
//! Comparisons ignore order, duplicates, and dominated points.
//! Join and meet return sorted minimal antichains, retaining the input time width.
//! All points in each argument pair must have the same width; an empty frontier has no width witness.

use std::sync::{Arc, OnceLock};

use corgi::{Bounds, HostKernel, Shape, Value};
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::lattice::Lattice;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;

use super::Function;

type Time = PointStamp<i64, 2>;
type Frontier = Antichain<Time>;

pub(super) const NAMES: [&str; 5] = ["frontier_le", "frontier_lt", "frontier_eq", "frontier_join", "frontier_meet"];

#[derive(Clone, Copy)]
enum Operation { Le, Lt, Eq, Join, Meet }

struct Kernel {
    name: &'static str,
    operation: Operation,
    input: Shape,
    output: Shape,
}

pub(super) fn functions() -> &'static [Function] {
    static FUNCTIONS: OnceLock<Vec<Function>> = OnceLock::new();
    FUNCTIONS.get_or_init(|| {
        let frontier = Shape::List(Box::new(Shape::List(Box::new(Shape::Int))));
        let input = Shape::Prod(vec![frontier.clone(), frontier.clone()]);
        NAMES.into_iter().zip([Operation::Le, Operation::Lt, Operation::Eq, Operation::Join, Operation::Meet])
            .map(|(name, operation)| {
                let output = match operation {
                    Operation::Le | Operation::Lt | Operation::Eq => Shape::Int,
                    Operation::Join | Operation::Meet => frontier.clone(),
                };
                Arc::new(Kernel { name, operation, input: input.clone(), output }) as Function
            }).collect()
    })
}

/// The two list partitions over one coordinate column.
struct Frontiers {
    frontiers: Bounds,
    times: Bounds,
    coordinates: Value,
}

fn rows(bounds: &Bounds) -> usize {
    match bounds { Bounds::Offsets(ends) => ends.len(), Bounds::Stride(_, rows) => *rows }
}

fn span(bounds: &Bounds, row: usize) -> (usize, usize) {
    match bounds {
        Bounds::Offsets(ends) => (if row == 0 { 0 } else { ends[row - 1] }, ends[row]),
        Bounds::Stride(width, _) => (row * width, (row + 1) * width),
    }
}

impl Frontiers {
    fn new(value: Value, name: &str) -> Result<Self, String> {
        let (frontiers, times) = value.into_list(name)?;
        let (times, coordinates) = times.into_list(name)?;
        coordinates.as_i64(name)?;
        Ok(Self { frontiers, times, coordinates })
    }

    fn row(&self, row: usize, width: &mut Option<usize>, name: &str) -> Result<Frontier, String> {
        let coordinates = self.coordinates.as_i64(name)?;
        let (lo, hi) = span(&self.frontiers, row);
        let mut result = Frontier::new();
        for index in lo..hi {
            let (lo, hi) = span(&self.times, index);
            if let Some(width) = *width {
                if hi - lo != width {
                    return Err(format!("{name}: time widths differ: {width} and {}", hi - lo));
                }
            } else {
                *width = Some(hi - lo);
            }
            result.insert(Time::new(coordinates[lo..hi].iter().copied().collect()));
        }
        Ok(result)
    }
}

impl HostKernel for Kernel {
    fn name(&self) -> &str { self.name }
    fn input(&self) -> &Shape { &self.input }
    fn output(&self) -> &Shape { &self.output }

    fn eval(&self, input: Value) -> Result<Value, String> {
        let args = input.into_prod(self.name)?;
        let [left, right]: [Value; 2] = args.try_into()
            .map_err(|_| format!("{}: expected two frontiers", self.name))?;
        let left = Frontiers::new(left, self.name)?;
        let right = Frontiers::new(right, self.name)?;
        if rows(&left.frontiers) != rows(&right.frontiers) {
            return Err(format!("{}: argument columns have different lengths", self.name));
        }
        let mut comparisons = Vec::new();
        let mut frontier_ends = Vec::new();
        let mut time_ends = Vec::new();
        let mut coordinates = Vec::new();
        for row in 0..rows(&left.frontiers) {
            let mut width = None;
            let left = left.row(row, &mut width, self.name)?;
            let right = right.row(row, &mut width, self.name)?;
            let frontier = match self.operation {
                Operation::Le => { comparisons.push(i64::from(PartialOrder::less_equal(&left, &right))); continue; }
                Operation::Lt => { comparisons.push(i64::from(PartialOrder::less_than(&left, &right))); continue; }
                Operation::Eq => { comparisons.push(i64::from(left == right)); continue; }
                Operation::Join => left.join(&right),
                Operation::Meet => left.meet(&right),
            };
            let mut times = frontier.elements().to_vec();
            times.sort();
            for time in times {
                // PointStamp omits trailing minimum coordinates; the list representation retains them.
                coordinates.extend((0..width.unwrap_or(0)).map(|i| time.get(i).copied().unwrap_or(i64::MIN)));
                time_ends.push(coordinates.len());
            }
            frontier_ends.push(time_ends.len());
        }
        Ok(match self.operation {
            Operation::Le | Operation::Lt | Operation::Eq => Value::int(comparisons),
            Operation::Join | Operation::Meet => Value::List(frontier_ends.into(), Box::new(
                Value::List(time_ends.into(), Box::new(Value::int(coordinates))))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(rows: &[Vec<Vec<i64>>]) -> Value {
        let mut frontier_ends = Vec::new();
        let mut time_ends = Vec::new();
        let mut coordinates = Vec::new();
        for row in rows {
            for time in row {
                coordinates.extend_from_slice(time);
                time_ends.push(coordinates.len());
            }
            frontier_ends.push(time_ends.len());
        }
        Value::List(frontier_ends.into(), Box::new(Value::List(time_ends.into(), Box::new(Value::int(coordinates)))))
    }

    fn pair(left: Vec<Vec<i64>>, right: Vec<Vec<i64>>) -> Value {
        Value::Prod(vec![column(&[left]), column(&[right])])
    }

    fn evaluate(name: &str, args: Value) -> Result<Value, String> {
        functions().iter().find(|f| f.name() == name).unwrap().eval(args)
    }

    #[test]
    fn comparisons_use_frontier_order() {
        let left = vec![
            vec![vec![0, 1], vec![1, 0]],
            vec![vec![0, 2]],
            vec![],
            vec![vec![0, 0]],
        ];
        let right = vec![
            vec![vec![1, 0], vec![2, 2], vec![0, 1], vec![1, 0]],
            vec![vec![1, 0]],
            vec![],
            vec![],
        ];
        let args = Value::Prod(vec![column(&left), column(&right)]);
        assert_eq!(evaluate("frontier_le", args.clone()).unwrap(), Value::int([1, 0, 1, 1]));
        assert_eq!(evaluate("frontier_lt", args.clone()).unwrap(), Value::int([0, 0, 0, 1]));
        assert_eq!(evaluate("frontier_eq", args).unwrap(), Value::int([1, 0, 1, 0]));
    }

    #[test]
    fn lattice_operations_normalize_and_preserve_width() {
        let left = vec![vec![0, 2], vec![2, 0]];
        let right = vec![vec![1, 1]];
        assert_eq!(evaluate("frontier_join", pair(left.clone(), right.clone())).unwrap(),
                   column(&[vec![vec![1, 2], vec![2, 1]]]));
        assert_eq!(evaluate("frontier_meet", pair(left.clone(), right)).unwrap(),
                   column(&[vec![vec![0, 2], vec![1, 1], vec![2, 0]]]));
        assert_eq!(evaluate("frontier_join", pair(left.clone(), vec![])).unwrap(), column(&[vec![]]));
        assert_eq!(evaluate("frontier_meet", pair(vec![], left.clone())).unwrap(), column(&[left]));
        let padded = vec![vec![0, i64::MIN], vec![0, i64::MIN]];
        assert_eq!(evaluate("frontier_meet", pair(padded, vec![])).unwrap(), column(&[vec![vec![0, i64::MIN]]]));
    }

    #[test]
    fn widths_are_checked_even_for_dominated_points() {
        for name in NAMES {
            for args in [pair(vec![vec![0]], vec![vec![0, 0]]),
                         pair(vec![vec![0, 0], vec![1]], vec![])] {
                assert!(evaluate(name, args).unwrap_err().contains("time widths differ"));
            }
        }
    }

    #[test]
    fn words_are_available_in_the_common_scope() {
        for function in functions() {
            let lambda = super::super::Lambda::new(format!("input call {}", function.name()), &super::super::base_scope());
            let args = pair(vec![vec![0, 1], vec![1, 0]], vec![vec![1, 1]]);
            assert_eq!(corgi::eval_graph(lambda.graph().unwrap(), args.clone()), function.eval(args).unwrap());
            assert!(super::super::is_base_word(function.name()));
        }
    }
}
