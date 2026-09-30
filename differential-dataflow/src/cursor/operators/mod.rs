//! Operator implementations based on cursor traversal.
//!
//! [`join`] and [`reduce`] are tactics for the general drivers in
//! [`operators::join`](crate::operators::join) and [`operators::reduce`](crate::operators::reduce).
//! [`count`], [`threshold`] and [`upsert`] are whole operators with no non-cursor variant.

mod history;

pub mod count;
pub mod join;
pub mod reduce;
pub mod threshold;
pub mod upsert;

pub use self::count::CountTotal;
pub use self::threshold::ThresholdTotal;
