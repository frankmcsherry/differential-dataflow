//! Shared frontier operations over `List(List(int))` antichains of product times.
//! Comparisons ignore order, duplicates, and dominated points.
//! Normalize, join, and meet return sorted minimal antichains, retaining the input time width.
//! All points within an operation's row must have the same width; an empty frontier has no width witness.

use std::sync::{Arc, OnceLock};

use corgi::{Bounds, HostKernel, Shape, Value};
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::Antichain;

use super::Function;

type Time = PointStamp<i64, 2>;
type Frontier = Antichain<Time>;

pub(super) const NAMES: [&str; 6] = ["frontier_le", "frontier_lt", "frontier_eq", "frontier_join", "frontier_meet", "frontier_normalize"];

#[derive(Clone, Copy)]
enum Operation { Le, Lt, Eq, Join, Meet, Normalize }

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
        NAMES.into_iter().zip([Operation::Le, Operation::Lt, Operation::Eq, Operation::Join, Operation::Meet, Operation::Normalize])
            .map(|(name, operation)| {
                let input = match operation {
                    Operation::Normalize => frontier.clone(),
                    _ => Shape::Prod(vec![frontier.clone(), frontier.clone()]),
                };
                let output = match operation {
                    Operation::Le | Operation::Lt | Operation::Eq => Shape::Int,
                    Operation::Join | Operation::Meet | Operation::Normalize => frontier.clone(),
                };
                Arc::new(Kernel { name, operation, input, output }) as Function
            }).collect()
    })
}

/// The two list partitions over one coordinate column.
struct Frontiers {
    frontiers: Bounds,
    times: Bounds,
    coordinates: Value,
}

/// A row borrows its points from the input columns, without building an antichain.
struct FrontierRef<'a> {
    times: &'a Bounds,
    coordinates: &'a [i64],
    lower: usize,
    upper: usize,
}

impl FrontierRef<'_> {
    fn point(&self, index: usize) -> &[i64] {
        let (lower, upper) = span(self.times, index);
        &self.coordinates[lower..upper]
    }

    fn less_equal(&self, other: &FrontierRef<'_>) -> bool {
        // Every point in the upper frontier needs a point below it in the lower.
        // This remains valid for unordered, repeated, or dominated input points.
        (other.lower..other.upper).all(|b| {
            (self.lower..self.upper).any(|a| {
                self.point(a).iter().zip(other.point(b)).all(|(a, b)| a <= b)
            })
        })
    }

    fn antichain(&self) -> Frontier {
        (self.lower..self.upper).map(|index| Time::new(self.point(index).iter().copied().collect())).collect()
    }
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
        Ok(Self { frontiers, times, coordinates })
    }

    fn row<'a>(&'a self, coordinates: &'a [i64], row: usize, width: &mut Option<usize>, name: &str) -> Result<FrontierRef<'a>, String> {
        let (lo, hi) = span(&self.frontiers, row);
        // Check every point before comparisons can short-circuit, including dominated points.
        for index in lo..hi {
            let (lo, hi) = span(&self.times, index);
            if let Some(width) = *width {
                if hi - lo != width {
                    return Err(format!("{name}: time widths differ: {width} and {}", hi - lo));
                }
            } else {
                *width = Some(hi - lo);
            }
        }
        Ok(FrontierRef { times: &self.times, coordinates, lower: lo, upper: hi })
    }
}

impl HostKernel for Kernel {
    fn name(&self) -> &str { self.name }
    fn input(&self) -> &Shape { &self.input }
    fn output(&self) -> &Shape { &self.output }

    fn eval(&self, input: Value) -> Result<Value, String> {
        let (left, right) = match self.operation {
            Operation::Normalize => (input, None),
            _ => {
                let args = input.into_prod(self.name)?;
                let [left, right]: [Value; 2] = args.try_into()
                    .map_err(|_| format!("{}: expected two frontiers", self.name))?;
                (left, Some(right))
            }
        };
        let left = Frontiers::new(left, self.name)?;
        let right = right.map(|right| Frontiers::new(right, self.name)).transpose()?;
        // Byte coordinates widen once per column; every row shares that slice.
        let left_coordinates = left.coordinates.as_i64(self.name)?;
        let right_coordinates = right.as_ref().map(|right| right.coordinates.as_i64(self.name)).transpose()?;
        if right.as_ref().is_some_and(|right| rows(&left.frontiers) != rows(&right.frontiers)) {
            return Err(format!("{}: argument columns have different lengths", self.name));
        }
        let mut comparisons = Vec::new();
        let mut frontier_ends = Vec::new();
        let mut time_ends = Vec::new();
        let mut coordinates = Vec::new();
        for row in 0..rows(&left.frontiers) {
            let mut width = None;
            let left = left.row(&left_coordinates, row, &mut width, self.name)?;
            let right = match (&right, &right_coordinates) {
                (Some(right), Some(coordinates)) => Some(right.row(coordinates, row, &mut width, self.name)?),
                _ => None,
            };
            let frontier = match self.operation {
                Operation::Le | Operation::Lt | Operation::Eq => {
                    let right = right.as_ref().unwrap();
                    let le = left.less_equal(right);
                    let comparison = match self.operation {
                        Operation::Le => le,
                        Operation::Lt => le && !right.less_equal(&left),
                        Operation::Eq => le && right.less_equal(&left),
                        _ => unreachable!(),
                    };
                    comparisons.push(i64::from(comparison));
                    continue;
                }
                Operation::Join => left.antichain().join(&right.as_ref().unwrap().antichain()),
                Operation::Meet => left.antichain().meet(&right.as_ref().unwrap().antichain()),
                Operation::Normalize => left.antichain(),
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
            Operation::Join | Operation::Meet | Operation::Normalize => Value::List(frontier_ends.into(), Box::new(
                Value::List(time_ends.into(), Box::new(Value::int(coordinates))))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use timely::order::PartialOrder;

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
        for name in &NAMES[..5] {
            for args in [pair(vec![vec![0]], vec![vec![0, 0]]),
                         pair(vec![vec![0, 0], vec![1]], vec![])] {
                assert!(evaluate(name, args).unwrap_err().contains("time widths differ"));
            }
        }
        let input = column(&[vec![vec![0, 0], vec![1]]]);
        assert!(evaluate("frontier_normalize", input).unwrap_err().contains("time widths differ"));
    }

    #[test]
    fn words_are_available_in_the_common_scope() {
        for function in functions() {
            let lambda = super::super::Lambda::new(format!("input call {}", function.name()), &super::super::base_scope());
            let args = if function.name() == "frontier_normalize" {
                column(&[vec![vec![0, 1], vec![1, 0]]])
            } else {
                pair(vec![vec![0, 1], vec![1, 0]], vec![vec![1, 1]])
            };
            assert_eq!(corgi::eval_graph(lambda.graph().unwrap(), args.clone()), function.eval(args).unwrap());
            assert!(super::super::is_base_word(function.name()));
        }
    }

    #[test]
    fn borrowed_comparisons_and_normalization_match_antichains() {
        let mut left = Vec::new();
        let mut right = Vec::new();
        let mut expected_le = Vec::new();
        let mut expected_lt = Vec::new();
        let mut expected_eq = Vec::new();
        let mut normalized = Vec::new();
        for width in [0, 1, 2, 5] {
            let zero = vec![0; width];
            let one = vec![1; width];
            let minimum = vec![i64::MIN; width];
            let mut a = zero.clone();
            let mut b = zero.clone();
            if width > 0 { a[0] = 1; b[width - 1] = 1; }
            let mut padded = minimum.clone();
            if width > 0 { padded[0] = 1; }
            let cases = vec![
                vec![], vec![zero.clone()], vec![one.clone()], vec![minimum.clone()],
                vec![a.clone(), b.clone()], vec![b, one.clone(), a, one.clone()],
                vec![one, zero.clone(), zero], vec![padded.clone(), padded, minimum],
            ];
            let antichain = |points: &[Vec<i64>]| -> Frontier {
                points.iter().map(|point| Time::new(point.iter().copied().collect())).collect()
            };
            for a in &cases {
                for b in &cases {
                    let lower = antichain(a);
                    let upper = antichain(b);
                    expected_le.push(i64::from(PartialOrder::less_equal(&lower, &upper)));
                    expected_lt.push(i64::from(PartialOrder::less_than(&lower, &upper)));
                    expected_eq.push(i64::from(lower == upper));
                    let mut points = lower.elements().to_vec();
                    points.sort();
                    normalized.push(points.iter().map(|point| {
                        (0..width).map(|i| point.get(i).copied().unwrap_or(i64::MIN)).collect()
                    }).collect());
                    left.push(a.clone());
                    right.push(b.clone());
                }
            }
        }
        let args = Value::Prod(vec![column(&left), column(&right)]);
        assert_eq!(evaluate("frontier_le", args.clone()).unwrap(), Value::int(expected_le));
        assert_eq!(evaluate("frontier_lt", args.clone()).unwrap(), Value::int(expected_lt));
        assert_eq!(evaluate("frontier_eq", args).unwrap(), Value::int(expected_eq));
        assert_eq!(evaluate("frontier_normalize", column(&left)).unwrap(), column(&normalized));
    }
}
