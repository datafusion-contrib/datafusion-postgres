//! PostgreSQL `justify_days(interval)` — promote groups of 30 days to months.
//!
//! Per the [PostgreSQL manual](https://www.postgresql.org/docs/current/functions-datetime.html):
//!
//! > `justify_days ( interval ) → interval`
//! >
//! > Adjust interval, converting 30-day time periods to months.
//!
//! This follows `interval_justify_days` in PostgreSQL's
//! `src/backend/utils/adt/timestamp.c`:
//!
//! 1. Whole 30-day periods are split off the day field, truncating toward
//!    zero, and added to the month field.
//! 2. If the month and day fields then have opposite signs, one month is
//!    borrowed so both share a sign (e.g. `1 mon -1 days` becomes `29 days`).
//!
//! The time field is never changed, even when it holds more than 24 hours or
//! has a different sign (`justify_days('1 mon -1 hour')` is unchanged).
//!
//! # Input and output types
//!
//! Accepts any Arrow interval unit (`YearMonth`, `DayTime`, `MonthDayNano`)
//! or a NULL literal, and always returns `Interval(MonthDayNano)`.
//!
//! # Deviations from PostgreSQL
//!
//! - **Precision:** the time field is passed through unchanged, including any
//!   sub-microsecond nanoseconds PostgreSQL could not store.
//! - **Overflow:** like PostgreSQL, a month field that overflows `i32` is an
//!   error (`interval out of range`), which fails the query.

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

use super::interval::{
    DAYS_PER_MONTH, OUT_OF_RANGE, map_interval_array, single_interval_signature,
};

/// Create the PostgreSQL `justify_days(interval)` UDF.
pub fn create_justify_days_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(JustifyDaysUDF::default())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JustifyDaysUDF {
    signature: Signature,
}

impl Default for JustifyDaysUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl JustifyDaysUDF {
    pub fn new() -> Self {
        Self {
            signature: single_interval_signature(),
        }
    }
}

impl ScalarUDFImpl for JustifyDaysUDF {
    fn name(&self) -> &str {
        "justify_days"
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
            return exec_err!("justify_days requires 1 argument, got {}", args.len());
        }
        Ok(ColumnarValue::Array(justify_days_array(&args[0])?))
    }
}

/// Apply `justify_days` to every element of an interval array.
fn justify_days_array(input: &ArrayRef) -> Result<ArrayRef> {
    map_interval_array("justify_days", input, justify_days)
}

/// Core of PostgreSQL's `interval_justify_days`, on Arrow interval fields.
fn justify_days(months: i32, days: i32, nanos: i64) -> Result<IntervalMonthDayNano> {
    // Rust's `/` truncates toward zero, matching C integer division.
    let whole_months = days / DAYS_PER_MONTH;
    let mut days = days - whole_months * DAYS_PER_MONTH;
    let mut months = match months.checked_add(whole_months) {
        Some(m) => m,
        None => return exec_err!("{OUT_OF_RANGE}"),
    };

    // Borrow one month so that the month and day fields share a sign. This
    // moves `months` toward zero, so it cannot overflow.
    if months > 0 && days < 0 {
        days += DAYS_PER_MONTH;
        months -= 1;
    } else if months < 0 && days > 0 {
        days -= DAYS_PER_MONTH;
        months += 1;
    }

    Ok(IntervalMonthDayNano::new(months, days, nanos))
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
    const NANOS_PER_HOUR: i64 = 3_600 * 1_000_000_000;

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
            // Documented example.
            (
                mdn(12, 65, 0),
                mdn(14, 5, 0),
                "1 year 65 days -> 1 year 2 mons 5 days",
            ),
            (mdn(0, 30, 0), mdn(1, 0, 0), "30 days -> 1 mon"),
            (mdn(0, 29, 0), mdn(0, 29, 0), "29 days -> unchanged"),
            (mdn(0, 31, 0), mdn(1, 1, 0), "31 days -> 1 mon 1 day"),
            (mdn(0, 60, 0), mdn(2, 0, 0), "60 days -> 2 mons"),
            (mdn(0, -30, 0), mdn(-1, 0, 0), "-30 days -> -1 mons"),
            (
                mdn(0, -31, 0),
                mdn(-1, -1, 0),
                "-31 days -> -1 mons -1 days",
            ),
            // Mixed signs between months and days.
            (mdn(1, -35, 0), mdn(0, -5, 0), "1 mon -35 days -> -5 days"),
            (mdn(-1, 35, 0), mdn(0, 5, 0), "-1 mons +35 days -> 5 days"),
            (mdn(1, -1, 0), mdn(0, 29, 0), "1 mon -1 days -> 29 days"),
            (mdn(-1, 1, 0), mdn(0, -29, 0), "-1 mons +1 day -> -29 days"),
            (
                mdn(12, -400, 0),
                mdn(-1, -10, 0),
                "1 year -400 days -> -1 mons -10 days",
            ),
            (
                mdn(3, -91, hours(1)),
                mdn(0, -1, hours(1)),
                "3 mons -91 days +01:00:00 -> -1 days +01:00:00",
            ),
            (
                mdn(2, -59, hours(-48)),
                mdn(0, 1, hours(-48)),
                "2 mons -59 days -48:00:00 -> 1 day -48:00:00",
            ),
            (
                mdn(14, -3, hours(100)),
                mdn(13, 27, hours(100)),
                "1 year 2 mons -3 days 100:00:00 -> 1 year 1 mon 27 days 100:00:00",
            ),
            // The time field is never touched.
            (
                mdn(0, 0, hours(50) + 600 * 1_000_000_000),
                mdn(0, 0, hours(50) + 600 * 1_000_000_000),
                "50:10:00 -> unchanged",
            ),
            (
                mdn(1, 0, hours(-1)),
                mdn(1, 0, hours(-1)),
                "1 mon -01:00:00 -> unchanged",
            ),
            (
                mdn(0, 35, hours(50)),
                mdn(1, 5, hours(50)),
                "35 days 50:00:00 -> 1 mon 5 days 50:00:00",
            ),
            (
                mdn(0, 1, hours(-1)),
                mdn(0, 1, hours(-1)),
                "1 day -01:00:00 -> unchanged",
            ),
            (
                mdn(1, 0, -NANOS_PER_MICRO),
                mdn(1, 0, -NANOS_PER_MICRO),
                "1 mon -00:00:00.000001 -> unchanged",
            ),
            (mdn(0, 0, 0), mdn(0, 0, 0), "00:00:00 -> 00:00:00"),
        ];
        for (input, expected, label) in cases {
            let got = justify_days(input.months, input.days, input.nanoseconds).unwrap();
            assert_eq!(got, expected, "{label}");
        }
    }

    #[test]
    fn extreme_day_values() {
        // PostgreSQL 16.15 results.
        assert_eq!(justify_days(0, i32::MAX, 0).unwrap(), mdn(71_582_788, 7, 0));
        assert_eq!(
            justify_days(0, i32::MIN, 0).unwrap(),
            mdn(-71_582_788, -8, 0)
        );
    }

    #[test]
    fn largest_month_values_that_fit() {
        // PostgreSQL 16.15 results.
        assert_eq!(
            justify_days(i32::MAX - 1, 30, 0).unwrap(),
            mdn(i32::MAX, 0, 0)
        );
        assert_eq!(
            justify_days(i32::MAX, -1, 0).unwrap(),
            mdn(i32::MAX - 1, 29, 0)
        );
        assert_eq!(
            justify_days(i32::MIN + 1, -30, 0).unwrap(),
            mdn(i32::MIN, 0, 0)
        );
        assert_eq!(
            justify_days(i32::MIN, 1, 0).unwrap(),
            mdn(i32::MIN + 1, -29, 0)
        );
        assert_eq!(
            justify_days(i32::MAX, 29, hours(24)).unwrap(),
            mdn(i32::MAX, 29, hours(24))
        );
    }

    #[test]
    fn overflow_is_an_error() {
        // PostgreSQL: justify_days(interval '2147483647 mons 30 days')
        // -> ERROR: interval out of range
        let err = justify_days(i32::MAX, 30, 0).unwrap_err();
        assert!(err.to_string().contains(OUT_OF_RANGE), "{err}");
        let err = justify_days(i32::MIN, -30, 0).unwrap_err();
        assert!(err.to_string().contains(OUT_OF_RANGE), "{err}");
    }

    #[test]
    fn month_day_nano_batch_with_nulls() {
        let input: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(mdn(0, 31, 0)),
            None,
            Some(mdn(1, -1, hours(5))),
        ]));
        let out = justify_days_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), mdn(1, 1, 0));
        assert!(out.is_null(1));
        assert_eq!(out.value(2), mdn(0, 29, hours(5)));
    }

    #[test]
    fn empty_input() {
        let input: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(Vec::<
            Option<IntervalMonthDayNano>,
        >::new()));
        let out = justify_days_array(&input).unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(
            out.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn day_time_input() {
        let input: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(65, 3_600_000)),
            None,
        ]));
        let out = justify_days_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.value(0), mdn(2, 5, hours(1)));
        assert!(out.is_null(1));
    }

    #[test]
    fn year_month_input_is_unchanged() {
        let input: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![Some(-14), None]));
        let out = justify_days_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.value(0), mdn(-14, 0, 0));
        assert!(out.is_null(1));
    }

    #[tokio::test]
    async fn sql_null_and_literal() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_justify_days_udf());
        let batches = ctx
            .sql("SELECT justify_days(NULL), justify_days(INTERVAL '1 year 65 days')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let null_col = batches[0].column(0);
        assert_eq!(
            null_col.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert!(null_col.is_null(0));
        let col = batches[0]
            .column(1)
            .as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(col.value(0), mdn(14, 5, 0));
    }
}
