//! PostgreSQL `age(timestamp, timestamp)` and `age(timestamp)` — the
//! difference between two timestamps in years, months and days.
//!
//! Per the [PostgreSQL manual](https://www.postgresql.org/docs/current/functions-datetime.html):
//!
//! > `age ( timestamp, timestamp ) → interval`
//! >
//! > Subtract arguments, producing a "symbolic" result that uses years and
//! > months, rather than just days.
//! >
//! > `age ( timestamp ) → interval`
//! >
//! > Subtract argument from `current_date` (at midnight).
//!
//! This follows `timestamp_age` / `timestamptz_age` in PostgreSQL's
//! `src/backend/utils/adt/timestamp.c`:
//!
//! 1. Both timestamps are split into calendar fields (year, month, day, hour,
//!    minute, second, fraction) and subtracted field by field.
//! 2. If the first timestamp is the earlier one, every field is negated, so
//!    the borrowing below works on a non-negative difference.
//! 3. Negative fields borrow from the next larger one: a second is 60
//!    seconds, an hour 60 minutes, a day 24 hours, a year 12 months. A day
//!    borrows a whole month, sized by the month of the *earlier* timestamp
//!    (`age('2024-03-01', '2024-01-31')` is `1 mon 1 day`, borrowing 31 days).
//! 4. The sign is restored, and years and months are combined.
//!
//! `age(x)` is rewritten to `age(current_date, x)` during planning, the way
//! PostgreSQL defines it, so it uses DataFusion's `current_date` and its time
//! zone setting.
//!
//! # Input and output types
//!
//! Accepts `Timestamp` of any unit, with or without a time zone, `Date32`,
//! `Date64`, string literals (cast to `Timestamp(Nanosecond)`), or NULL, and
//! always returns `Interval(MonthDayNano)`. The two arguments may have
//! different types. When one carries a time zone, both are cast to the first
//! such zone before the subtraction (see below).
//!
//! # Deviations from PostgreSQL
//!
//! - **Time zones:** PostgreSQL takes the calendar fields of both arguments
//!   in the session's `TimeZone`. Here they are taken in the zone of the
//!   first argument that has one: the other argument is cast to it, so a
//!   timestamp in another zone keeps its instant, and a plain timestamp,
//!   date or literal is read as local time in that zone. Without any zone,
//!   timestamps are taken as written. This matches PostgreSQL when the
//!   session zone is that zone. As in PostgreSQL, which argument is earlier
//!   is decided by the instants, not the local fields.
//! - **Precision:** Arrow timestamps can carry nanoseconds; the fraction is
//!   kept to the nanosecond, where PostgreSQL stores microseconds.
//! - **Infinity:** Arrow timestamps have no `infinity`, so PostgreSQL's
//!   infinite results do not arise.
//! - **Transaction IDs:** `age(xid)` and `mxid_age` are not provided.

use std::sync::Arc;

use chrono::{Datelike, NaiveDateTime, Timelike};
use datafusion::arrow::array::temporal_conversions::{
    as_datetime, as_datetime_with_timezone, date32_to_datetime, date64_to_datetime,
};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, IntervalMonthDayNano, IntervalMonthDayNanoType, IntervalUnit,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use datafusion::common::{Result, exec_err, internal_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::interval::OUT_OF_RANGE;

/// Nanoseconds in one second.
const NANOS_PER_SEC: i64 = 1_000_000_000;

/// The error PostgreSQL raises for a timestamp it cannot split into fields.
const TIMESTAMP_OUT_OF_RANGE: &str = "timestamp out of range";

/// Create the PostgreSQL `age(timestamp[, timestamp])` UDF.
pub fn create_age_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(AgeUDF::default())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AgeUDF {
    signature: Signature,
}

impl Default for AgeUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl AgeUDF {
    pub fn new() -> Self {
        Self {
            // Immutable for two arguments, as in PostgreSQL. The one-argument
            // form is rewritten to use `current_date` before it is evaluated.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AgeUDF {
    fn name(&self) -> &str {
        "age"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !matches!(arg_types.len(), 1 | 2) {
            return plan_err!("age expects 1 or 2 arguments, got {}", arg_types.len());
        }
        // PostgreSQL takes the fields of both arguments in one time zone, the
        // session's. Do the same: when an argument carries a zone, cast every
        // argument to the first such zone. Plain timestamps, dates and string
        // literals are then read as local time in that zone, as PostgreSQL
        // reads them in the session zone; a timestamp with another zone keeps
        // its instant.
        let zone = arg_types.iter().find_map(|t| match t {
            DataType::Timestamp(_, Some(tz)) => Some(Arc::clone(tz)),
            _ => None,
        });
        arg_types
            .iter()
            .map(|t| match t {
                DataType::Timestamp(unit, tz) => Ok(DataType::Timestamp(
                    *unit,
                    zone.clone().or_else(|| tz.clone()),
                )),
                DataType::Date32 | DataType::Date64 if zone.is_some() => {
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, zone.clone()))
                }
                DataType::Date32 | DataType::Date64 | DataType::Null => Ok(t.clone()),
                // An untyped literal, as in `age('2024-03-01', '2024-01-31')`.
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    Ok(DataType::Timestamp(TimeUnit::Nanosecond, zone.clone()))
                }
                other => plan_err!("age expects timestamp or date arguments, got {other}"),
            })
            .collect()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    fn simplify(&self, mut args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args.len() != 1 {
            return Ok(ExprSimplifyResult::Original(args));
        }
        // PostgreSQL defines age(x) as age(current_date, x).
        let today = datafusion::functions::datetime::current_date().call(vec![]);
        let arg = args.remove(0);
        Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
            ScalarFunction::new_udf(Arc::new(create_age_udf()), vec![today, arg]),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        match args.as_slice() {
            [end, start] => Ok(ColumnarValue::Array(age_arrays(end, start)?)),
            [_] => internal_err!(
                "age(timestamp) should have been rewritten to age(current_date, timestamp)"
            ),
            _ => exec_err!("age requires 1 or 2 arguments, got {}", args.len()),
        }
    }
}

/// A timestamp as `age` needs it: its calendar fields, and the instant used
/// to decide which argument is earlier.
#[derive(Debug, Clone, Copy)]
struct Moment {
    local: NaiveDateTime,
    /// Nanoseconds since the Unix epoch (UTC for a timestamp with a zone).
    instant: i128,
}

/// `age(end, start)` for every row; NULL in either argument gives NULL.
fn age_arrays(end: &ArrayRef, start: &ArrayRef) -> Result<ArrayRef> {
    let (end, start) = (moments(end)?, moments(start)?);
    let mut out = PrimitiveBuilder::<IntervalMonthDayNanoType>::with_capacity(end.len());
    for (end, start) in end.into_iter().zip(start) {
        match (end, start) {
            (Some(end), Some(start)) => out.append_value(age(end, start)?),
            _ => out.append_null(),
        }
    }
    Ok(Arc::new(out.finish()))
}

/// Convert a timestamp or date array to one `Moment` per row.
fn moments(input: &ArrayRef) -> Result<Vec<Option<Moment>>> {
    match input.data_type() {
        DataType::Null => Ok(vec![None; input.len()]),
        DataType::Date32 => input
            .as_primitive::<Date32Type>()
            .iter()
            .map(|v| {
                v.map(|d| {
                    moment(
                        date32_to_datetime(d),
                        d as i128 * 86_400 * NANOS_PER_SEC as i128,
                    )
                })
                .transpose()
            })
            .collect(),
        DataType::Date64 => input
            .as_primitive::<Date64Type>()
            .iter()
            .map(|v| {
                v.map(|ms| moment(date64_to_datetime(ms), ms as i128 * 1_000_000))
                    .transpose()
            })
            .collect(),
        DataType::Timestamp(unit, tz) => {
            let tz = tz.as_deref().map(str::parse::<Tz>).transpose()?;
            match unit {
                TimeUnit::Second => {
                    timestamp_moments::<TimestampSecondType>(input, tz, NANOS_PER_SEC)
                }
                TimeUnit::Millisecond => {
                    timestamp_moments::<TimestampMillisecondType>(input, tz, 1_000_000)
                }
                TimeUnit::Microsecond => {
                    timestamp_moments::<TimestampMicrosecondType>(input, tz, 1_000)
                }
                TimeUnit::Nanosecond => timestamp_moments::<TimestampNanosecondType>(input, tz, 1),
            }
        }
        other => exec_err!("age expects timestamp or date arguments, got {other}"),
    }
}

fn timestamp_moments<T>(
    input: &ArrayRef,
    tz: Option<Tz>,
    nanos_per_unit: i64,
) -> Result<Vec<Option<Moment>>>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    input
        .as_primitive::<T>()
        .iter()
        .map(|v| {
            v.map(|v| {
                let local = match tz {
                    Some(tz) => as_datetime_with_timezone::<T>(v, tz).map(|t| t.naive_local()),
                    None => as_datetime::<T>(v),
                };
                moment(local, v as i128 * nanos_per_unit as i128)
            })
            .transpose()
        })
        .collect()
}

fn moment(local: Option<NaiveDateTime>, instant: i128) -> Result<Moment> {
    match local {
        Some(local) => Ok(Moment { local, instant }),
        None => exec_err!("{TIMESTAMP_OUT_OF_RANGE}"),
    }
}

/// Days in `month` (1-12) of `year`, proleptic Gregorian, like PostgreSQL's
/// `day_tab[isleap(year)][month - 1]`.
fn days_in_month(year: i32, month: u32) -> i64 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 31,
    }
}

/// Core of PostgreSQL's `timestamp_age`: `end - start` as years, months,
/// days and time.
fn age(end: Moment, start: Moment) -> Result<IntervalMonthDayNano> {
    let (t1, t2) = (end.local, start.local);

    // 1. The symbolic, field-by-field difference.
    let mut nanos = t1.nanosecond() as i64 - t2.nanosecond() as i64;
    let mut secs = t1.second() as i64 - t2.second() as i64;
    let mut mins = t1.minute() as i64 - t2.minute() as i64;
    let mut hours = t1.hour() as i64 - t2.hour() as i64;
    let mut days = t1.day() as i64 - t2.day() as i64;
    let mut months = t1.month() as i64 - t2.month() as i64;
    let mut years = t1.year() as i64 - t2.year() as i64;

    // 2. Work on a non-negative difference.
    let negative = end.instant < start.instant;
    let flip = |fields: [&mut i64; 7]| {
        for f in fields {
            *f = -*f;
        }
    };
    if negative {
        flip([
            &mut nanos,
            &mut secs,
            &mut mins,
            &mut hours,
            &mut days,
            &mut months,
            &mut years,
        ]);
    }

    // 3. Borrow into negative fields from the next larger one.
    while nanos < 0 {
        nanos += NANOS_PER_SEC;
        secs -= 1;
    }
    while secs < 0 {
        secs += 60;
        mins -= 1;
    }
    while mins < 0 {
        mins += 60;
        hours -= 1;
    }
    while hours < 0 {
        hours += 24;
        days -= 1;
    }
    let earlier = if negative { t1 } else { t2 };
    let month_days = days_in_month(earlier.year(), earlier.month());
    while days < 0 {
        days += month_days;
        months -= 1;
    }
    while months < 0 {
        months += 12;
        years -= 1;
    }

    // 4. Restore the sign and combine.
    if negative {
        flip([
            &mut nanos,
            &mut secs,
            &mut mins,
            &mut hours,
            &mut days,
            &mut months,
            &mut years,
        ]);
    }
    let months = years
        .checked_mul(12)
        .and_then(|m| m.checked_add(months))
        .and_then(|m| i32::try_from(m).ok());
    let (Some(months), Ok(days)) = (months, i32::try_from(days)) else {
        return exec_err!("{OUT_OF_RANGE}");
    };
    let time = ((hours * 60 + mins) * 60 + secs) * NANOS_PER_SEC + nanos;
    Ok(IntervalMonthDayNano::new(months, days, time))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Date32Array, TimestampNanosecondArray, TimestampSecondArray, new_null_array,
    };
    use datafusion::prelude::SessionContext;

    const NANOS_PER_HOUR: i64 = 3_600 * NANOS_PER_SEC;

    fn mdn(months: i32, days: i32, nanos: i64) -> IntervalMonthDayNano {
        IntervalMonthDayNano::new(months, days, nanos)
    }

    /// A timestamp without a time zone, from `YYYY-MM-DD HH:MM:SS[.f]`.
    fn ts(s: &str) -> Moment {
        let local = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let instant = local.and_utc().timestamp_nanos_opt().unwrap() as i128;
        Moment { local, instant }
    }

    fn hms(h: i64, m: i64, s: i64) -> i64 {
        ((h * 60 + m) * 60 + s) * NANOS_PER_SEC
    }

    /// Expected values were recorded from PostgreSQL 16.15.
    #[test]
    fn matches_postgres_results() {
        let cases = [
            // The example in the PostgreSQL manual: 43 years 9 mons 27 days.
            (
                "2001-04-10 00:00:00",
                "1957-06-13 00:00:00",
                mdn(525, 27, 0),
            ),
            // A day borrow takes the length of the earlier timestamp's month.
            ("2024-03-01 00:00:00", "2024-01-31 00:00:00", mdn(1, 1, 0)),
            ("2024-01-31 00:00:00", "2024-03-01 00:00:00", mdn(-1, -1, 0)),
            ("2023-03-01 00:00:00", "2023-02-28 00:00:00", mdn(0, 1, 0)),
            ("2024-03-01 00:00:00", "2024-02-28 00:00:00", mdn(0, 2, 0)),
            ("2024-02-29 00:00:00", "2023-02-28 00:00:00", mdn(12, 1, 0)),
            ("2025-02-28 00:00:00", "2024-02-29 00:00:00", mdn(11, 28, 0)),
            ("2024-03-31 00:00:00", "2024-02-29 00:00:00", mdn(1, 2, 0)),
            ("2024-05-31 00:00:00", "2024-04-30 00:00:00", mdn(1, 1, 0)),
            ("2024-04-30 00:00:00", "2024-05-31 00:00:00", mdn(-1, -1, 0)),
            ("1900-03-01 00:00:00", "1900-02-28 00:00:00", mdn(0, 1, 0)),
            // Time borrows down to the fraction of a second.
            (
                "2024-01-02 00:00:00",
                "2024-01-01 23:59:59.999999",
                mdn(0, 0, 1_000),
            ),
            (
                "2024-01-01 23:59:59.999999",
                "2024-01-02 00:00:00",
                mdn(0, 0, -1_000),
            ),
            (
                "2024-03-31 10:00:00",
                "2023-01-15 12:30:00",
                mdn(14, 15, hms(21, 30, 0)),
            ),
            (
                "2023-01-15 12:30:00",
                "2024-03-31 10:00:00",
                mdn(-14, -15, -hms(21, 30, 0)),
            ),
            (
                "2024-01-01 00:00:00.5",
                "2024-01-01 00:00:00.75",
                mdn(0, 0, -250_000_000),
            ),
            ("2024-06-15 08:00:00", "2024-06-15 08:00:00", mdn(0, 0, 0)),
            ("2000-01-01 00:00:00", "1970-01-01 00:00:00", mdn(360, 0, 0)),
            // The ends of the nanosecond timestamp range: 584 years 6 mons 20 days 23:34:32.
            (
                "2262-04-11 23:47:16",
                "1677-09-21 00:12:44",
                mdn(7014, 20, hms(23, 34, 32)),
            ),
        ];
        for (end, start, expected) in cases {
            assert_eq!(
                age(ts(end), ts(start)).unwrap(),
                expected,
                "age('{end}', '{start}')"
            );
        }
    }

    #[test]
    fn keeps_nanoseconds() {
        // PostgreSQL cannot represent this; it is kept rather than rounded.
        let got = age(
            ts("2024-01-01 00:00:01.000000001"),
            ts("2024-01-01 00:00:00"),
        )
        .unwrap();
        assert_eq!(got, mdn(0, 0, NANOS_PER_SEC + 1));
    }

    #[test]
    fn an_hour_borrow_cascades_into_a_month_borrow() {
        // The hour borrow takes a day, which takes February's 28 (or 29)
        // days from the month. PostgreSQL 16.15: 23:00:00 for both.
        let got = age(ts("2023-03-01 00:00:00"), ts("2023-02-28 01:00:00")).unwrap();
        assert_eq!(got, mdn(0, 0, hms(23, 0, 0)));
        let got = age(ts("2024-03-01 00:00:00"), ts("2024-02-29 01:00:00")).unwrap();
        assert_eq!(got, mdn(0, 0, hms(23, 0, 0)));
    }

    #[test]
    fn time_zone_fields_are_local() {
        // PostgreSQL 16.15 with TimeZone = 'America/New_York':
        // age('2024-03-10 12:00-04', '2024-03-09 12:00-05') = 1 day,
        // although only 23 hours pass (the clocks change in between).
        let utc_nanos = |s: &str| ts(s).instant as i64;
        let tz = Some(Arc::from("America/New_York"));
        let end: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![utc_nanos("2024-03-10 16:00:00")])
                .with_timezone_opt(tz.clone()),
        );
        let start: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![utc_nanos("2024-03-09 17:00:00")])
                .with_timezone_opt(tz),
        );
        let out = age_arrays(&end, &start).unwrap();
        assert_eq!(
            out.as_primitive::<IntervalMonthDayNanoType>().value(0),
            mdn(0, 1, 0)
        );

        // Without a zone the same instants are 23 hours apart (PostgreSQL in UTC: 23:00:00).
        let end: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![utc_nanos(
            "2024-03-10 16:00:00",
        )]));
        let start: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![utc_nanos(
            "2024-03-09 17:00:00",
        )]));
        let out = age_arrays(&end, &start).unwrap();
        assert_eq!(
            out.as_primitive::<IntervalMonthDayNanoType>().value(0),
            mdn(0, 0, 23 * NANOS_PER_HOUR)
        );
    }

    #[test]
    fn dates_and_mixed_units() {
        // age(date '2024-03-01', timestamp(0) '2024-01-31 00:00:00') = 1 mon 1 day
        let end: ArrayRef = Arc::new(Date32Array::from(vec![Some(19_783), None, Some(19_783)]));
        let start: ArrayRef = Arc::new(TimestampSecondArray::from(vec![
            Some(1_706_659_200),
            Some(0),
            None,
        ]));
        let out = age_arrays(&end, &start).unwrap();
        let out = out.as_primitive::<IntervalMonthDayNanoType>();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), mdn(1, 1, 0));
        assert!(out.is_null(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn null_and_empty_input() {
        let nulls = new_null_array(&DataType::Null, 2);
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![Some(0), Some(1)]));
        let out = age_arrays(&nulls, &dates).unwrap();
        assert_eq!(out.null_count(), 2);
        assert_eq!(
            out.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );

        let empty: ArrayRef = Arc::new(TimestampNanosecondArray::from(Vec::<i64>::new()));
        assert_eq!(age_arrays(&empty, &empty).unwrap().len(), 0);
    }

    #[test]
    fn out_of_range_timestamp_is_an_error() {
        // Arrow's second-precision timestamps reach far beyond any calendar.
        let far: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX]));
        let zero: ArrayRef = Arc::new(TimestampSecondArray::from(vec![0]));
        let err = age_arrays(&far, &zero).unwrap_err();
        assert!(err.to_string().contains(TIMESTAMP_OUT_OF_RANGE), "{err}");
    }

    #[test]
    fn days_in_month_matches_postgres_day_tab() {
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(1900, 2), 28);
        assert_eq!(days_in_month(2000, 2), 29);
        assert_eq!(days_in_month(2024, 4), 30);
        assert_eq!(days_in_month(2024, 12), 31);
    }

    async fn query_one(sql: &str) -> ArrayRef {
        let ctx = SessionContext::new();
        ctx.register_udf(create_age_udf());
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).clone()
    }

    #[tokio::test]
    async fn sql_forms() {
        let col = query_one(
            "SELECT age(TIMESTAMP '2024-03-31 10:00:00', TIMESTAMP '2023-01-15 12:30:00')",
        )
        .await;
        assert_eq!(
            col.as_primitive::<IntervalMonthDayNanoType>().value(0),
            mdn(14, 15, hms(21, 30, 0))
        );

        // String literals, as PostgreSQL resolves age('2024-03-01', '2024-01-31').
        let col = query_one("SELECT age('2024-03-01', '2024-01-31')").await;
        assert_eq!(
            col.as_primitive::<IntervalMonthDayNanoType>().value(0),
            mdn(1, 1, 0)
        );

        let col = query_one("SELECT age(NULL, DATE '2024-01-01')").await;
        assert!(col.is_null(0));
    }

    #[tokio::test]
    async fn one_argument_is_measured_from_today() {
        let col = query_one(
            "SELECT age(TIMESTAMP '2000-02-29 00:00:00') = age(CAST(current_date() AS TIMESTAMP), TIMESTAMP '2000-02-29 00:00:00')",
        )
        .await;
        assert!(col.as_boolean().value(0));
        let col = query_one("SELECT age(current_date())").await;
        assert_eq!(
            col.as_primitive::<IntervalMonthDayNanoType>().value(0),
            mdn(0, 0, 0)
        );
    }

    #[tokio::test]
    async fn mixed_time_zones_are_taken_in_one_zone() {
        // PostgreSQL 16.15 with TimeZone = 'America/New_York'. Without a
        // shared zone these came out as mixed-sign intervals such as
        // `-1 mons 30 days 14:00:00`.
        let ny =
            |s: &str| format!("arrow_cast(TIMESTAMP '{s}', 'Timestamp(ns, \"America/New_York\")')");
        let kolkata =
            |s: &str| format!("arrow_cast(TIMESTAMP '{s}', 'Timestamp(ns, \"Asia/Kolkata\")')");
        let cases = [
            // age('2024-01-01 12:00-05', '2024-01-01 22:00+05:30') = 00:30:00
            (
                format!(
                    "age({}, {})",
                    ny("2024-01-01 12:00:00"),
                    kolkata("2024-01-01 22:00:00")
                ),
                mdn(0, 0, hms(0, 30, 0)),
            ),
            // age(timestamptz '2024-01-01 12:00-05', timestamp '2024-01-01 14:00') = -02:00:00
            (
                format!(
                    "age({}, TIMESTAMP '2024-01-01 14:00:00')",
                    ny("2024-01-01 12:00:00")
                ),
                mdn(0, 0, -hms(2, 0, 0)),
            ),
            // age(timestamp '2024-01-01 14:00', timestamptz '2024-01-01 12:00-05') = 02:00:00
            (
                format!(
                    "age(TIMESTAMP '2024-01-01 14:00:00', {})",
                    ny("2024-01-01 12:00:00")
                ),
                mdn(0, 0, hms(2, 0, 0)),
            ),
            // age(date '2024-03-10', timestamptz '2024-03-09 21:00-05') = 03:00:00
            (
                format!("age(DATE '2024-03-10', {})", ny("2024-03-09 21:00:00")),
                mdn(0, 0, hms(3, 0, 0)),
            ),
            // age('2024-01-01 14:00:00', timestamptz '2024-01-01 12:00-05') = 02:00:00
            (
                format!("age('2024-01-01 14:00:00', {})", ny("2024-01-01 12:00:00")),
                mdn(0, 0, hms(2, 0, 0)),
            ),
        ];
        for (sql, expected) in cases {
            let col = query_one(&format!("SELECT {sql}")).await;
            assert_eq!(
                col.as_primitive::<IntervalMonthDayNanoType>().value(0),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn arguments_are_coerced_to_one_zone() {
        let ny: Option<Arc<str>> = Some(Arc::from("America/New_York"));
        let ts = |unit, tz: &Option<Arc<str>>| DataType::Timestamp(unit, tz.clone());
        let udf = AgeUDF::new();
        assert_eq!(
            udf.coerce_types(&[ts(TimeUnit::Second, &None), DataType::Date32])
                .unwrap(),
            vec![ts(TimeUnit::Second, &None), DataType::Date32],
            "without a zone nothing changes"
        );
        assert_eq!(
            udf.coerce_types(&[DataType::Date32, ts(TimeUnit::Nanosecond, &ny)])
                .unwrap(),
            vec![
                ts(TimeUnit::Microsecond, &ny),
                ts(TimeUnit::Nanosecond, &ny)
            ]
        );
        let kolkata: Option<Arc<str>> = Some(Arc::from("Asia/Kolkata"));
        assert_eq!(
            udf.coerce_types(&[
                ts(TimeUnit::Nanosecond, &kolkata),
                ts(TimeUnit::Millisecond, &ny)
            ])
            .unwrap(),
            vec![
                ts(TimeUnit::Nanosecond, &kolkata),
                ts(TimeUnit::Millisecond, &kolkata)
            ],
            "the first zone wins"
        );
        assert_eq!(
            udf.coerce_types(&[DataType::Utf8, ts(TimeUnit::Microsecond, &ny)])
                .unwrap(),
            vec![
                ts(TimeUnit::Nanosecond, &ny),
                ts(TimeUnit::Microsecond, &ny)
            ]
        );
    }

    #[tokio::test]
    async fn other_argument_types_are_rejected() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_age_udf());
        let err = ctx
            .sql("SELECT age(TIMESTAMP '2024-01-01 00:00:00', 5)")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("age expects timestamp or date arguments"),
            "{err}"
        );
        let err = ctx.sql("SELECT age()").await.unwrap_err();
        assert!(
            err.to_string().contains("age expects 1 or 2 arguments"),
            "{err}"
        );
    }
}
