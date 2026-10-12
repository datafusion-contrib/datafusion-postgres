//! PostgreSQL `justify_interval(interval)` — normalize days and time, with
//! sign adjustment.
//!
//! Per the [PostgreSQL manual](https://www.postgresql.org/docs/current/functions-datetime.html):
//!
//! > `justify_interval ( interval ) → interval`
//! >
//! > Adjust interval using `justify_days` and `justify_hours`, with additional
//! > sign adjustments.
//!
//! This is not simply `justify_days(justify_hours(x))`. It follows
//! `interval_justify_interval` in PostgreSQL's
//! `src/backend/utils/adt/timestamp.c` step by step:
//!
//! 1. If days and time share a sign, whole 30-day periods are first moved
//!    from days to months (PostgreSQL does this to avoid overflow).
//! 2. Whole days are split off the time field (truncating toward zero) and
//!    added to days.
//! 3. Whole 30-day periods are moved from days to months.
//! 4. If months and the remaining days have opposite signs — or days are
//!    zero and time has the opposite sign — one month is borrowed.
//! 5. If days and time have opposite signs, one day is borrowed.
//!
//! The result therefore has all three fields sharing one sign, e.g.
//! `1 mon -00:00:00.000001` becomes `29 days 23:59:59.999999`.
//!
//! # Input and output types
//!
//! Accepts any Arrow interval unit (`YearMonth`, `DayTime`, `MonthDayNano`)
//! or a NULL literal, and always returns `Interval(MonthDayNano)`.
//!
//! # Deviations from PostgreSQL
//!
//! - **Precision:** PostgreSQL stores the time field in microseconds and
//!   rounds sub-microsecond input when parsing; Arrow `MonthDayNano` stores
//!   nanoseconds. Sub-microsecond values are kept and justified exactly
//!   rather than rounded first, so e.g.
//!   `justify_interval(INTERVAL '1 month -1 nanosecond')` returns
//!   `29 days 23:59:59.999999999`, where PostgreSQL would return `1 mon`
//!   for the rounded input. See `justify_hours` for details.
//! - **Overflow:** like PostgreSQL, a month field that overflows `i32` is an
//!   error (`interval out of range`), which fails the query.

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, IntervalUnit};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

use super::interval::{
    DAYS_PER_MONTH, NANOS_PER_DAY, OUT_OF_RANGE, map_interval_array, single_interval_signature,
};

/// Create the PostgreSQL `justify_interval(interval)` UDF.
pub fn create_justify_interval_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(JustifyIntervalUDF::default())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JustifyIntervalUDF {
    signature: Signature,
}

impl Default for JustifyIntervalUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl JustifyIntervalUDF {
    pub fn new() -> Self {
        Self {
            signature: single_interval_signature(),
        }
    }
}

impl ScalarUDFImpl for JustifyIntervalUDF {
    fn name(&self) -> &str {
        "justify_interval"
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
            return exec_err!("justify_interval requires 1 argument, got {}", args.len());
        }
        Ok(ColumnarValue::Array(justify_interval_array(&args[0])?))
    }
}

/// Apply `justify_interval` to every element of an interval array.
fn justify_interval_array(input: &ArrayRef) -> Result<ArrayRef> {
    map_interval_array("justify_interval", input, justify_interval)
}

/// Move whole 30-day periods from `days` into `months`, as PostgreSQL does.
fn days_to_months(months: i32, days: i32) -> Result<(i32, i32)> {
    // Rust's `/` truncates toward zero, matching C integer division.
    let whole_months = days / DAYS_PER_MONTH;
    match months.checked_add(whole_months) {
        Some(m) => Ok((m, days - whole_months * DAYS_PER_MONTH)),
        None => exec_err!("{OUT_OF_RANGE}"),
    }
}

/// Core of PostgreSQL's `interval_justify_interval`, on Arrow interval fields.
fn justify_interval(months: i32, days: i32, nanos: i64) -> Result<IntervalMonthDayNano> {
    let (mut months, mut days, mut time) = (months, days, nanos);

    // 1. Pre-justify days when days and time share a sign.
    if (days > 0 && time > 0) || (days < 0 && time < 0) {
        (months, days) = days_to_months(months, days)?;
    }

    // 2. Split whole days off the time field (truncating toward zero, like
    // PostgreSQL's TMODULO). |nanos| / NANOS_PER_DAY is at most ~106,752, so
    // the cast is lossless. PostgreSQL shows this addition cannot overflow
    // (after step 1, |days| < 30 or days and time differ in sign); it is
    // still checked here rather than assumed.
    let whole_days = time / NANOS_PER_DAY;
    time %= NANOS_PER_DAY;
    days = match days.checked_add(whole_days as i32) {
        Some(d) => d,
        None => return exec_err!("{OUT_OF_RANGE}"),
    };

    // 3. Move whole 30-day periods into months.
    (months, days) = days_to_months(months, days)?;

    // 4. Borrow a month so months agree in sign with days, or with time when
    // days are zero. This moves `months` toward zero, so it cannot overflow.
    if months > 0 && (days < 0 || (days == 0 && time < 0)) {
        days += DAYS_PER_MONTH;
        months -= 1;
    } else if months < 0 && (days > 0 || (days == 0 && time > 0)) {
        days -= DAYS_PER_MONTH;
        months += 1;
    }

    // 5. Borrow a day so days agree in sign with time.
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
                mdn(1, 0, hours(-1)),
                mdn(0, 29, hours(23)),
                "1 mon -01:00:00 -> 29 days 23:00:00",
            ),
            (
                mdn(-1, 0, hours(1)),
                mdn(0, -29, hours(-23)),
                "-1 mons +01:00:00 -> -29 days -23:00:00",
            ),
            (
                mdn(12, 65, 0),
                mdn(14, 5, 0),
                "1 year 65 days -> 1 year 2 mons 5 days",
            ),
            (
                mdn(0, 0, hours(50) + 600 * 1_000_000_000),
                mdn(0, 2, hours(2) + 600 * 1_000_000_000),
                "50:10:00 -> 2 days 02:10:00",
            ),
            (
                mdn(0, 35, hours(50)),
                mdn(1, 7, hours(2)),
                "35 days 50:00:00 -> 1 mon 7 days 02:00:00",
            ),
            (
                mdn(0, -35, hours(-50)),
                mdn(-1, -7, hours(-2)),
                "-35 days -50:00:00 -> -1 mons -7 days -02:00:00",
            ),
            (
                mdn(0, 29, hours(47)),
                mdn(1, 0, hours(23)),
                "29 days 47:00:00 -> 1 mon 23:00:00",
            ),
            (
                mdn(0, -29, hours(-47)),
                mdn(-1, 0, hours(-23)),
                "-29 days -47:00:00 -> -1 mons -23:00:00",
            ),
            (
                mdn(1, 29, hours(24)),
                mdn(2, 0, 0),
                "1 mon 29 days 24:00:00 -> 2 mons",
            ),
            (
                mdn(-1, -29, hours(-24)),
                mdn(-2, 0, 0),
                "-1 mons -29 days -24:00:00 -> -2 mons",
            ),
            // Mixed signs.
            (mdn(1, -31, 0), mdn(0, -1, 0), "1 mon -31 days -> -1 days"),
            (mdn(-1, 31, 0), mdn(0, 1, 0), "-1 mons +31 days -> 1 day"),
            (
                mdn(1, 1, hours(-25)),
                mdn(0, 29, hours(23)),
                "1 mon 1 day -25:00:00 -> 29 days 23:00:00",
            ),
            (
                mdn(0, 1, hours(-1)),
                mdn(0, 0, hours(23)),
                "1 day -01:00:00 -> 23:00:00",
            ),
            (
                mdn(0, -1, hours(1)),
                mdn(0, 0, hours(-23)),
                "-1 days +01:00:00 -> -23:00:00",
            ),
            (
                mdn(1, -1, hours(1)),
                mdn(0, 29, hours(1)),
                "1 mon -1 days +01:00:00 -> 29 days 01:00:00",
            ),
            (
                mdn(2, -59, hours(-48)),
                mdn(0, -1, 0),
                "2 mons -59 days -48:00:00 -> -1 days",
            ),
            (
                mdn(3, -91, hours(1)),
                mdn(0, 0, hours(-23)),
                "3 mons -91 days +01:00:00 -> -23:00:00",
            ),
            (
                mdn(14, -3, hours(100)),
                mdn(14, 1, hours(4)),
                "1 year 2 mons -3 days 100:00:00 -> 1 year 2 mons 1 day 04:00:00",
            ),
            // Month borrow driven by the time field when days are zero.
            (
                mdn(1, 0, -NANOS_PER_MICRO),
                mdn(0, 29, hours(24) - NANOS_PER_MICRO),
                "1 mon -00:00:00.000001 -> 29 days 23:59:59.999999",
            ),
            (
                mdn(-1, 0, NANOS_PER_MICRO),
                mdn(0, -29, -(hours(24) - NANOS_PER_MICRO)),
                "-1 mons +00:00:00.000001 -> -29 days -23:59:59.999999",
            ),
            (mdn(0, 0, 0), mdn(0, 0, 0), "00:00:00 -> 00:00:00"),
        ];
        for (input, expected, label) in cases {
            let got = justify_interval(input.months, input.days, input.nanoseconds).unwrap();
            assert_eq!(got, expected, "{label}");
        }
    }

    #[test]
    fn extreme_values() {
        // PostgreSQL 16.15 results.
        assert_eq!(
            justify_interval(0, i32::MAX, 0).unwrap(),
            mdn(71_582_788, 7, 0)
        );
        assert_eq!(
            justify_interval(0, i32::MIN, 0).unwrap(),
            mdn(-71_582_788, -8, 0)
        );
        assert_eq!(
            justify_interval(0, i32::MAX, hours(1)).unwrap(),
            mdn(71_582_788, 7, hours(1))
        );
        assert_eq!(
            justify_interval(0, i32::MAX, hours(-1)).unwrap(),
            mdn(71_582_788, 6, hours(23))
        );
        assert_eq!(
            justify_interval(0, i32::MIN, hours(1)).unwrap(),
            mdn(-71_582_788, -7, hours(-23))
        );
        // Largest whole-hour time Arrow can hold: 2562047 hours.
        assert_eq!(
            justify_interval(0, 0, hours(2_562_047)).unwrap(),
            mdn(3_558, 11, hours(23))
        );
        assert_eq!(
            justify_interval(0, 0, hours(-2_562_047)).unwrap(),
            mdn(-3_558, -11, hours(-23))
        );
        assert_eq!(
            justify_interval(i32::MAX - 1, 30, 0).unwrap(),
            mdn(i32::MAX, 0, 0)
        );
        assert_eq!(
            justify_interval(i32::MAX, -1, 0).unwrap(),
            mdn(i32::MAX - 1, 29, 0)
        );
        assert_eq!(
            justify_interval(i32::MIN + 1, -30, 0).unwrap(),
            mdn(i32::MIN, 0, 0)
        );
        assert_eq!(
            justify_interval(i32::MIN, 1, 0).unwrap(),
            mdn(i32::MIN + 1, -29, 0)
        );
        assert_eq!(
            justify_interval(i32::MAX, 0, hours(-1)).unwrap(),
            mdn(i32::MAX - 1, 29, hours(23))
        );
        assert_eq!(
            justify_interval(i32::MIN, 0, hours(1)).unwrap(),
            mdn(i32::MIN + 1, -29, hours(-23))
        );
        assert_eq!(
            justify_interval(i32::MAX, 29, 0).unwrap(),
            mdn(i32::MAX, 29, 0)
        );
    }

    #[test]
    fn overflow_is_an_error() {
        // PostgreSQL: each of these raises ERROR: interval out of range.
        for (m, d, n) in [
            (i32::MAX, 30, 0),
            (i32::MIN, -30, 0),
            (i32::MAX, 29, hours(24)),
            (i32::MIN, -29, hours(-24)),
        ] {
            let err = justify_interval(m, d, n).unwrap_err();
            assert!(err.to_string().contains(OUT_OF_RANGE), "{m} {d} {n}: {err}");
        }
    }

    #[test]
    fn month_day_nano_batch_with_nulls() {
        let input: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(mdn(1, 0, hours(-1))),
            None,
            Some(mdn(0, 29, hours(47))),
        ]));
        let out = justify_interval_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), mdn(0, 29, hours(23)));
        assert!(out.is_null(1));
        assert_eq!(out.value(2), mdn(1, 0, hours(23)));
    }

    #[test]
    fn empty_input() {
        let input: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(Vec::<
            Option<IntervalMonthDayNano>,
        >::new()));
        let out = justify_interval_array(&input).unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(
            out.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn day_time_input() {
        let input: ArrayRef = Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(29, 47 * 3_600_000)),
            None,
        ]));
        let out = justify_interval_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.value(0), mdn(1, 0, hours(23)));
        assert!(out.is_null(1));
    }

    #[test]
    fn year_month_input_is_unchanged() {
        let input: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![Some(14), None]));
        let out = justify_interval_array(&input).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.value(0), mdn(14, 0, 0));
        assert!(out.is_null(1));
    }

    #[tokio::test]
    async fn sql_null_and_literal() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_justify_interval_udf());
        let batches = ctx
            .sql("SELECT justify_interval(NULL), justify_interval(INTERVAL '1 month -1 hour')")
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
        assert_eq!(col.value(0), mdn(0, 29, hours(23)));
    }
}
