//! Shared helpers for the PostgreSQL `interval` UDFs in this module.
//!
//! PostgreSQL has a single `interval` type made of a month, a day and a time
//! field. Arrow has three interval units; these helpers accept all of them,
//! convert each value losslessly to `(months, days, nanoseconds)`, and always
//! produce `Interval(MonthDayNano)`, matching PostgreSQL's one result type.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveBuilder, new_null_array};
use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalMonthDayNano, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType,
};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};

/// Nanoseconds in one 24-hour day (PostgreSQL `USECS_PER_DAY`, in nanoseconds).
pub(crate) const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;

/// Days in one month, as assumed by PostgreSQL (`DAYS_PER_MONTH`).
pub(crate) const DAYS_PER_MONTH: i32 = 30;

/// Nanoseconds in one millisecond, for `Interval(DayTime)` input.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// The error PostgreSQL raises when an interval field overflows.
pub(crate) const OUT_OF_RANGE: &str = "interval out of range";

/// Signature for a function taking one interval of any Arrow unit, or a NULL
/// literal.
pub(crate) fn single_interval_signature() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::MonthDayNano)]),
            TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::DayTime)]),
            TypeSignature::Exact(vec![DataType::Interval(IntervalUnit::YearMonth)]),
            TypeSignature::Exact(vec![DataType::Null]),
        ],
        Volatility::Immutable,
    )
}

/// Apply `f(months, days, nanoseconds)` to every non-null element of an
/// interval array, producing an `Interval(MonthDayNano)` array. NULLs stay
/// NULL; an error from `f` fails the whole call.
pub(crate) fn map_interval_array<F>(name: &str, input: &ArrayRef, f: F) -> Result<ArrayRef>
where
    F: Fn(i32, i32, i64) -> Result<IntervalMonthDayNano>,
{
    let len = input.len();
    let mut out = PrimitiveBuilder::<IntervalMonthDayNanoType>::with_capacity(len);
    match input.data_type() {
        DataType::Null => {
            return Ok(new_null_array(
                &DataType::Interval(IntervalUnit::MonthDayNano),
                len,
            ));
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            for v in input.as_primitive::<IntervalMonthDayNanoType>().iter() {
                match v {
                    Some(v) => out.append_value(f(v.months, v.days, v.nanoseconds)?),
                    None => out.append_null(),
                }
            }
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            for v in input.as_primitive::<IntervalDayTimeType>().iter() {
                match v {
                    Some(v) => {
                        out.append_value(f(0, v.days, v.milliseconds as i64 * NANOS_PER_MILLI)?)
                    }
                    None => out.append_null(),
                }
            }
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            for v in input.as_primitive::<IntervalYearMonthType>().iter() {
                match v {
                    Some(months) => out.append_value(f(months, 0, 0)?),
                    None => out.append_null(),
                }
            }
        }
        other => return exec_err!("{name} expects an interval argument, got {other:?}"),
    }
    Ok(Arc::new(out.finish()))
}
