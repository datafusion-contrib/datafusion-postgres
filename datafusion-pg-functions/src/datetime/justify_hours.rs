//! PostgreSQL `justify_hours(interval)` — promote groups of 24 hours to days.
//!
//! Per the [PostgreSQL manual](https://www.postgresql.org/docs/current/functions-datetime.html):
//!
//! > `justify_hours ( interval ) → interval`
//! >
//! > Adjust interval, converting 24-hour time periods to days.
//!
//! This follows `interval_justify_hours` in PostgreSQL's
//! `src/backend/utils/adt/timestamp.c`:
//!
//! 1. Whole days are split off the time field, truncating toward zero, and
//!    added to the day field. The month field is never changed.
//! 2. If the day and time fields then have opposite signs, one day is
//!    borrowed so both share a sign (e.g. `1 day -01:00:00` becomes
//!    `23:00:00`).
//!
//! # Input and output types
//!
//! Accepts any Arrow interval unit (`YearMonth`, `DayTime`, `MonthDayNano`)
//! or a NULL literal, and always returns `Interval(MonthDayNano)`, matching
//! PostgreSQL's single `interval` result type. `DayTime` milliseconds and
//! `YearMonth` months convert to `MonthDayNano` losslessly.
//!
//! # Deviations from PostgreSQL
//!
//! - **Precision:** PostgreSQL stores the time field in microseconds and
//!   rounds sub-microsecond input when parsing; Arrow `MonthDayNano` stores
//!   nanoseconds, and DataFusion can produce sub-microsecond values. These
//!   are kept and justified exactly, so the result always equals the input
//!   (with 24-hour days). The difference is only visible for such input:
//!   `justify_hours(INTERVAL '1 day -1 nanosecond')` returns
//!   `23:59:59.999999999`, which arrow-pg truncates to `23:59:59.999999` on
//!   the wire, whereas PostgreSQL would have rounded the input to `1 day`
//!   and returned `1 day`.
//! - **Overflow:** like PostgreSQL, a day field that overflows `i32` is an
//!   error (`interval out of range`), which fails the query.

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

use super::interval::{NANOS_PER_DAY, OUT_OF_RANGE, map_interval_array, single_interval_signature};

/// Create the PostgreSQL `justify_hours(interval)` UDF.
pub fn create_justify_hours_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(JustifyHoursUDF::default())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JustifyHoursUDF {
    signature: Signature,
}

impl Default for JustifyHoursUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl JustifyHoursUDF {
    pub fn new() -> Self {
        Self {
            signature: single_interval_signature(),
        }
    }
}

impl ScalarUDFImpl for JustifyHoursUDF {
    fn name(&self) -> &str {
        "justify_hours"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        if args.len() != 1 {
            return exec_err!("justify_hours requires 1 argument, got {}", args.len());
        }
        Ok(ColumnarValue::Array(justify_hours_array(&args[0])?))
    }
}

/// Apply `justify_hours` to every element of an interval array.
fn justify_hours_array(input: &ArrayRef) -> Result<ArrayRef> {
    map_interval_array("justify_hours", input, justify_hours)
}

/// Core of PostgreSQL's `interval_justify_hours`, on Arrow interval fields.
fn justify_hours(months: i32, days: i32, nanos: i64) -> Result<IntervalMonthDayNano> {
    // Split whole days off the time field. Rust's `/` and `%` truncate
    // toward zero, matching PostgreSQL's TMODULO macro.
    let whole_days = nanos / NANOS_PER_DAY;
    let mut time = nanos % NANOS_PER_DAY;

    // |nanos| / NANOS_PER_DAY is at most ~106,752, so this cast is lossless.
    let mut days = match days.checked_add(whole_days as i32) {
        Some(d) => d,
        None => return exec_err!("{OUT_OF_RANGE}"),
    };

    // Borrow one day so that the day and time fields share a sign. This moves
    // `days` toward zero, so it cannot overflow.
    if days > 0 && time < 0 {
        time += NANOS_PER_DAY;
        days -= 1;
    } else if days < 0 && time > 0 {
        time -= NANOS_PER_DAY;
        days += 1;
    }

    Ok(IntervalMonthDayNano::new(months, days, time))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion::arrow::array::{
        Array, AsArray, IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
    };
    use datafusion::arrow::datatypes::{IntervalDayTime, IntervalMonthDayNanoType};
    use datafusion::prelude::SessionContext;

    const NANOS_PER_MICRO: i64 = 1_000;
    const NANOS_PER_SEC: i64 = 1_000_000_000;
    const NANOS_PER_HOUR: i64 = 3_600 * NANOS_PER_SEC;

    fn mdn(months: i32, days: i32, nanos: i64) -> IntervalMonthDayNano {
        IntervalMonthDayNano::new(months, days, nanos)
    }

    fn hours(h: i64) -> i64 {
        h * NANOS_PER_HOUR
    }

    /// Expected values were recorded from PostgreSQL 16.15.
    #[test]
    fn matches_postgres_results() {
        let cases = [
            // (input, expected, PostgreSQL rendering of input -> result)
            (
                mdn(0, 0, hours(50) + 10 * 60 * NANOS_PER_SEC),
                mdn(0, 2, hours(2) + 10 * 60 * NANOS_PER_SEC),
                "50:10:00 -> 2 days 02:10:00",
            ),
            (mdn(0, 0, hours(24)), mdn(0, 1, 0), "24:00:00 -> 1 day"),
            (
                mdn(0, 0, hours(24) - NANOS_PER_MICRO),
                mdn(0, 0, hours(24) - NANOS_PER_MICRO),
                "23:59:59.999999 -> unchanged",
            ),
            (
                mdn(0, 0, hours(25)),
                mdn(0, 1, hours(1)),
                "25:00:00 -> 1 day 01:00:00",
            ),
            (
                mdn(0, 0, hours(-25)),
                mdn(0, -1, hours(-1)),
                "-25:00:00 -> -1 days -01:00:00",
            ),
            (
                mdn(0, 1, hours(-25)),
                mdn(0, 0, hours(-1)),
                "1 day -25:00:00 -> -01:00:00",
            ),
            (
                mdn(0, -1, hours(25)),
                mdn(0, 0, hours(1)),
                "-1 days +25:00:00 -> 01:00:00",
            ),
            (
                mdn(0, 1, hours(-1)),
                mdn(0, 0, hours(23)),
                "1 day -01:00:00 -> 23:00:00",
            ),
            (
                mdn(2, 0, hours(49)),
                mdn(2, 2, hours(1)),
                "2 mons 49:00:00 -> 2 mons 2 days 01:00:00",
            ),
            (
                mdn(1, -1, hours(1)),
                mdn(1, 0, hours(-23)),
                "1 mon -1 days +01:00:00 -> 1 mon -23:00:00",
            ),
            (
                mdn(0, 0, hours(48) + NANOS_PER_MICRO),
                mdn(0, 2, NANOS_PER_MICRO),
                "48:00:00.000001 -> 2 days 00:00:00.000001",
            ),
            (
                mdn(0, 1000, hours(1000)),
                mdn(0, 1041, hours(16)),
                "1000 days 1000:00:00 -> 1041 days 16:00:00",
            ),
            (mdn(1, -1, 0), mdn(1, -1, 0), "1 mon -1 days -> unchanged"),
            (mdn(0, 0, 0), mdn(0, 0, 0), "00:00:00 -> 00:00:00"),
        ];
        for (input, expected, label) in cases {
            let got = justify_hours(input.months, input.days, input.nanoseconds).unwrap();
            assert_eq!(got, expected, "{label}");
        }
    }

    #[test]
    fn overflow_is_an_error() {
        // PostgreSQL: justify_hours(interval '2147483647 days 24 hours')
        // -> ERROR: interval out of range
        let err = justify_hours(0, i32::MAX, hours(24)).unwrap_err();
        assert!(err.to_string().contains("interval out of range"), "{err}");

        let err = justify_hours(0, i32::MIN, hours(-24)).unwrap_err();
        assert!(err.to_string().contains("interval out of range"), "{err}");
    }

    #[test]
    fn largest_day_values_that_fit() {
        assert_eq!(
            justify_hours(0, i32::MAX - 1, hours(24)).unwrap(),
            mdn(0, i32::MAX, 0)
        );
        assert_eq!(
            justify_hours(0, i32::MAX, hours(23)).unwrap(),
            mdn(0, i32::MAX, hours(23))
        );
    }

    #[test]
    fn keeps_sub_microsecond_nanos() {
        assert_eq!(
            justify_hours(0, 0, hours(24) + 500).unwrap(),
            mdn(0, 1, 500)
        );
    }

    #[test]
    fn month_day_nano_batch_with_nulls() {
        let input: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(mdn(0, 0, hours(25))),
            None,
            Some(mdn(0, 1, hours(-1))),
            Some(mdn(3, 0, hours(-49))),
        ]));
        let out = justify_hours_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), mdn(0, 1, hours(1)));
        assert!(out.is_null(1));
        assert_eq!(out.value(2), mdn(0, 0, hours(23)));
        assert_eq!(out.value(3), mdn(3, -2, hours(-1)));
    }

    #[test]
    fn empty_input() {
        let input: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(Vec::<
            Option<IntervalMonthDayNano>,
        >::new()));
        let out = justify_hours_array(&input).unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(
            out.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn day_time_input() {
        let input: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(0, 25 * 3_600_000)),
            Some(IntervalDayTime::new(1, -3_600_000)),
            None,
        ]));
        let out = justify_hours_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.value(0), mdn(0, 1, hours(1)));
        assert_eq!(out.value(1), mdn(0, 0, hours(23)));
        assert!(out.is_null(2));
    }

    #[test]
    fn year_month_input_is_unchanged() {
        let input: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![Some(14), None]));
        let out = justify_hours_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.value(0), mdn(14, 0, 0));
        assert!(out.is_null(1));
    }

    #[tokio::test]
    async fn sql_null_literal() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_justify_hours_udf());
        let batches = ctx
            .sql("SELECT justify_hours(NULL)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        assert_eq!(
            col.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert!(col.is_null(0));
    }

    #[tokio::test]
    async fn sql_interval_literal() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_justify_hours_udf());
        let batches = ctx
            .sql("SELECT justify_hours(INTERVAL '50 hours 10 minutes')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(col.value(0), mdn(0, 2, hours(2) + 10 * 60 * NANOS_PER_SEC));
    }
}
