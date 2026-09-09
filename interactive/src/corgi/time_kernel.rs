//! Fixed-coordinate kernels over the existing dynamic timestamp columns.

use crate::corgi::col_times::{ColTime, ColTimes};
use crate::ir::Time;
use columnar::{Index, Len};
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::lattice::Lattice;
use timely::order::Product;
use timely::progress::Timestamp;

/// Convert at a coarse operation boundary while arithmetic stays monomorphized.
pub(crate) trait KernelTime<T: ColTime>: Timestamp + Lattice {
    fn read(times: &ColTimes<T>, index: usize) -> Self;
    fn from_time(time: &T) -> Self;
    fn into_time(self) -> T;
}

impl<T: ColTime> KernelTime<T> for T {
    fn read(times: &ColTimes<T>, index: usize) -> Self {
        times.get(index)
    }
    fn from_time(time: &T) -> Self {
        time.clone()
    }
    fn into_time(self) -> T {
        self
    }
}

pub(crate) type One = Product<u64, u64>;
pub(crate) type Two = Product<u64, Product<u64, u64>>;

impl KernelTime<Time> for u64 {
    fn read(times: &ColTimes<Time>, index: usize) -> Self {
        let time = times.get_ref(index);
        assert_eq!(
            time.inner.vector.len(),
            0,
            "timestamp exceeds reducer scope depth"
        );
        *time.outer
    }
    fn from_time(time: &Time) -> Self {
        assert!(
            time.inner.is_empty(),
            "frontier exceeds reducer scope depth"
        );
        time.outer
    }
    fn into_time(self) -> Time {
        Product::new(self, PointStamp::minimum())
    }
}

impl KernelTime<Time> for One {
    fn read(times: &ColTimes<Time>, index: usize) -> Self {
        let time = times.get_ref(index);
        let coordinates = time.inner.vector;
        assert!(
            coordinates.len() <= 1,
            "timestamp exceeds reducer scope depth"
        );
        Product::new(
            *time.outer,
            if coordinates.is_empty() {
                0
            } else {
                *coordinates.get(0)
            },
        )
    }
    fn from_time(time: &Time) -> Self {
        assert!(
            time.inner.len() <= 1,
            "frontier exceeds reducer scope depth"
        );
        Product::new(time.outer, time.inner.first().copied().unwrap_or(0))
    }
    fn into_time(self) -> Time {
        Product::new(self.outer, [self.inner].into_iter().collect())
    }
}

impl KernelTime<Time> for Two {
    fn read(times: &ColTimes<Time>, index: usize) -> Self {
        let time = times.get_ref(index);
        let coordinates = time.inner.vector;
        assert!(
            coordinates.len() <= 2,
            "timestamp exceeds reducer scope depth"
        );
        Product::new(
            *time.outer,
            Product::new(
                if coordinates.is_empty() {
                    0
                } else {
                    *coordinates.get(0)
                },
                if coordinates.len() < 2 {
                    0
                } else {
                    *coordinates.get(1)
                },
            ),
        )
    }
    fn from_time(time: &Time) -> Self {
        assert!(
            time.inner.len() <= 2,
            "frontier exceeds reducer scope depth"
        );
        Product::new(
            time.outer,
            Product::new(
                time.inner.first().copied().unwrap_or(0),
                time.inner.get(1).copied().unwrap_or(0),
            ),
        )
    }
    fn into_time(self) -> Time {
        Product::new(
            self.outer,
            [self.inner.outer, self.inner.inner].into_iter().collect(),
        )
    }
}

/// Check the coordinate bound using columnar offsets, without owning timestamps.
/// Stop as soon as the dynamic fallback is necessary.
pub(crate) fn coordinate_bound(times: &ColTimes<Time>, end: usize) -> usize {
    let mut bound = 0;
    for index in 0..end {
        bound = bound.max(times.get_ref(index).inner.vector.len());
        if bound > 2 {
            break;
        }
    }
    bound
}
