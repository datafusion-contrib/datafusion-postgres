use std::iter;
use std::sync::Arc;

use arrow_schema::IntervalUnit;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::datatypes::{DataType, Date32Type, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ParamValues;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use futures::{StreamExt, stream};
use pg_interval::Interval;
use pgwire::api::Type;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::QueryResponse;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::types::format::FormatOptions;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use super::{arrow_schema_to_pg_fields, encode_recordbatch, into_pg_type};

pub async fn encode_dataframe(
    df: DataFrame,
    format: &Format,
    data_format_options: Option<Arc<FormatOptions>>,
) -> PgWireResult<QueryResponse> {
    let fields = Arc::new(arrow_schema_to_pg_fields(
        df.schema().as_arrow(),
        format,
        data_format_options,
    )?);

    let recordbatch_stream = df
        .execute_stream()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let fields_ref = fields.clone();
    let pg_row_stream = recordbatch_stream
        .map(move |rb: datafusion::error::Result<RecordBatch>| {
            let row_stream: Box<dyn Iterator<Item = PgWireResult<DataRow>> + Send + Sync> = match rb
            {
                Ok(rb) => encode_recordbatch(fields_ref.clone(), rb),
                Err(e) => Box::new(iter::once(Err(PgWireError::ApiError(e.into())))),
            };
            stream::iter(row_stream)
        })
        .flatten();
    Ok(QueryResponse::new(fields, pg_row_stream))
}

fn invalid_parameter_error(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "22023".to_owned(),
        msg.into(),
    )))
}

fn out_of_range_error(source: &str, value: i64, target: &DataType) -> PgWireError {
    invalid_parameter_error(format!(
        "{} value {} is out of range for {:?}",
        source, value, target
    ))
}

fn checked_int_cast<T>(value: i64, target: &DataType) -> PgWireResult<T>
where
    T: TryFrom<i64>,
{
    T::try_from(value).map_err(|_| out_of_range_error("INT", value, target))
}

fn coerce_int_value(value: Option<i64>, target: &DataType) -> PgWireResult<ScalarValue> {
    match target {
        DataType::Int8 => Ok(ScalarValue::Int8(
            value.map(|n| checked_int_cast(n, target)).transpose()?,
        )),
        DataType::Int16 => Ok(ScalarValue::Int16(
            value.map(|n| checked_int_cast(n, target)).transpose()?,
        )),
        DataType::Int32 => Ok(ScalarValue::Int32(
            value.map(|n| checked_int_cast(n, target)).transpose()?,
        )),
        DataType::Int64 => Ok(ScalarValue::Int64(value)),
        DataType::UInt8 => Ok(ScalarValue::UInt8(
            value.map(|n| checked_int_cast(n, target)).transpose()?,
        )),
        DataType::UInt16 => Ok(ScalarValue::UInt16(
            value.map(|n| checked_int_cast(n, target)).transpose()?,
        )),
        DataType::UInt32 => Ok(ScalarValue::UInt32(
            value.map(|n| checked_int_cast(n, target)).transpose()?,
        )),
        DataType::UInt64 => Ok(ScalarValue::UInt64(
            value.map(|n| checked_int_cast(n, target)).transpose()?,
        )),
        DataType::Float32 => Ok(ScalarValue::Float32(value.map(|n| n as f32))),
        DataType::Float64 => Ok(ScalarValue::Float64(value.map(|n| n as f64))),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(ScalarValue::TimestampSecond(value, None)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(ScalarValue::TimestampMillisecond(
            value.map(|n| n * 1000),
            None,
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(ScalarValue::TimestampMicrosecond(
            value.map(|n| n * 1_000_000),
            None,
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(ScalarValue::TimestampNanosecond(
            value.map(|n| n * 1_000_000_000),
            None,
        )),
        _ => Err(invalid_parameter_error(format!(
            "Cannot coerce integer value to {:?}",
            target
        ))),
    }
}

fn checked_float_cast<T>(value: f64, target: &DataType) -> PgWireResult<T>
where
    T: TryFrom<i64>,
{
    if !value.is_finite() {
        return Err(invalid_parameter_error(format!(
            "FLOAT value {} is out of range for {:?}",
            value, target
        )));
    }
    let n = value as i64;
    if (n as f64 - value).abs() > f64::EPSILON {
        return Err(out_of_range_error("FLOAT", n, target));
    }
    T::try_from(n).map_err(|_| out_of_range_error("FLOAT", n, target))
}

fn coerce_float_value(value: Option<f64>, target: &DataType) -> PgWireResult<ScalarValue> {
    match target {
        DataType::Int8 => Ok(ScalarValue::Int8(
            value.map(|n| checked_float_cast(n, target)).transpose()?,
        )),
        DataType::Int16 => Ok(ScalarValue::Int16(
            value.map(|n| checked_float_cast(n, target)).transpose()?,
        )),
        DataType::Int32 => Ok(ScalarValue::Int32(
            value.map(|n| checked_float_cast(n, target)).transpose()?,
        )),
        DataType::Int64 => Ok(ScalarValue::Int64(
            value.map(|n| checked_float_cast(n, target)).transpose()?,
        )),
        DataType::UInt8 => Ok(ScalarValue::UInt8(
            value.map(|n| checked_float_cast(n, target)).transpose()?,
        )),
        DataType::UInt16 => Ok(ScalarValue::UInt16(
            value.map(|n| checked_float_cast(n, target)).transpose()?,
        )),
        DataType::UInt32 => Ok(ScalarValue::UInt32(
            value.map(|n| checked_float_cast(n, target)).transpose()?,
        )),
        DataType::UInt64 => Ok(ScalarValue::UInt64(
            value.map(|n| checked_float_cast(n, target)).transpose()?,
        )),
        DataType::Float32 => Ok(ScalarValue::Float32(value.map(|n| n as f32))),
        DataType::Float64 => Ok(ScalarValue::Float64(value)),
        _ => Err(invalid_parameter_error(format!(
            "Cannot coerce float value to {:?}",
            target
        ))),
    }
}

fn coerce_timestamp_value(
    value: Option<NaiveDateTime>,
    target: &DataType,
) -> PgWireResult<ScalarValue> {
    match target {
        DataType::Timestamp(TimeUnit::Second, _) => Ok(ScalarValue::TimestampSecond(
            value.map(|t| t.and_utc().timestamp()),
            None,
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(ScalarValue::TimestampMillisecond(
            value.map(|t| t.and_utc().timestamp_millis()),
            None,
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(ScalarValue::TimestampMicrosecond(
            value.map(|t| t.and_utc().timestamp_micros()),
            None,
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(ScalarValue::TimestampNanosecond(
            value.and_then(|t| t.and_utc().timestamp_nanos_opt()),
            None,
        )),
        _ => Err(invalid_parameter_error(format!(
            "Cannot coerce TIMESTAMP to {:?}",
            target
        ))),
    }
}

fn coerce_timestamptz_value(
    value: Option<DateTime<FixedOffset>>,
    target: &DataType,
) -> PgWireResult<ScalarValue> {
    match target {
        DataType::Timestamp(unit, tz) => {
            let tz = tz.clone();
            match unit {
                TimeUnit::Second => Ok(ScalarValue::TimestampSecond(
                    value.map(|t| t.timestamp()),
                    tz,
                )),
                TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(
                    value.map(|t| t.timestamp_millis()),
                    tz,
                )),
                TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(
                    value.map(|t| t.timestamp_micros()),
                    tz,
                )),
                TimeUnit::Nanosecond => Ok(ScalarValue::TimestampNanosecond(
                    value.and_then(|t| t.timestamp_nanos_opt()),
                    tz,
                )),
            }
        }
        _ => Err(invalid_parameter_error(format!(
            "Cannot coerce TIMESTAMPTZ to {:?}",
            target
        ))),
    }
}

fn coerce_interval_value(value: Option<Interval>, target: &DataType) -> PgWireResult<ScalarValue> {
    match target {
        DataType::Interval(IntervalUnit::YearMonth) => Ok(match value {
            Some(i) => {
                if i.days != 0 || i.microseconds != 0 {
                    return Err(invalid_parameter_error(
                        "Cannot coerce INTERVAL with days or time components to YearMonth interval",
                    ));
                }
                ScalarValue::IntervalYearMonth(Some(i.months))
            }
            None => ScalarValue::IntervalYearMonth(None),
        }),
        DataType::Interval(IntervalUnit::DayTime) => Ok(match value {
            Some(i) => {
                if i.months != 0 {
                    return Err(invalid_parameter_error(
                        "Cannot coerce INTERVAL with months component to DayTime interval",
                    ));
                }
                if i.microseconds % 1000 != 0 {
                    return Err(invalid_parameter_error(
                        "Cannot coerce INTERVAL with sub-millisecond precision to DayTime interval",
                    ));
                }
                ScalarValue::new_interval_dt(i.days, (i.microseconds / 1000) as i32)
            }
            None => ScalarValue::IntervalDayTime(None),
        }),
        DataType::Interval(IntervalUnit::MonthDayNano) | DataType::Duration(_) => Ok(match value {
            Some(i) => ScalarValue::new_interval_mdn(i.months, i.days, i.microseconds * 1_000i64),
            None => ScalarValue::IntervalMonthDayNano(None),
        }),
        _ => Err(invalid_parameter_error(format!(
            "Cannot coerce INTERVAL to {:?}",
            target
        ))),
    }
}

/// Deserialize client provided parameter data.
///
/// First we try to use the type information from `pg_type_hint`, which is
/// provided by the client.
/// If the type is empty or unknown, we fallback to datafusion inferenced type
/// from `inferenced_types`.
/// An error will be raised when neither sources can provide type information.
pub fn deserialize_parameters<S>(
    portal: &Portal<S>,
    inferenced_types: &[Option<&DataType>],
) -> PgWireResult<ParamValues>
where
    S: Clone,
{
    fn get_pg_type(
        pg_type_hint: Option<Type>,
        inferenced_type: Option<&DataType>,
    ) -> PgWireResult<Type> {
        if let Some(ty) = pg_type_hint {
            Ok(ty.clone())
        } else if let Some(infer_type) = inferenced_type {
            into_pg_type(infer_type)
        } else {
            Ok(Type::UNKNOWN)
        }
    }

    let param_len = portal.parameter_len();
    let mut deserialized_params = Vec::with_capacity(param_len);
    for i in 0..param_len {
        let inferenced_type = inferenced_types.get(i).and_then(|v| v.to_owned());
        let pg_type = get_pg_type(
            portal
                .statement
                .parameter_types
                .get(i)
                .and_then(|f| f.clone()),
            inferenced_type,
        )?;
        // enumerate all supported parameter types and deserialize the
        // type to ScalarValue, with data coercion when server-inferred
        // types are available
        match pg_type {
            Type::BOOL => {
                let value = portal.parameter::<bool>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Boolean(value));
            }
            Type::CHAR => {
                let value = portal.parameter::<i8>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int8(value));
            }
            Type::INT2 => {
                let value = portal.parameter::<i16>(i, &pg_type)?;
                match inferenced_type {
                    Some(target) => {
                        deserialized_params
                            .push(coerce_int_value(value.map(|n| n as i64), target)?);
                    }
                    None => {
                        deserialized_params.push(ScalarValue::Int16(value));
                    }
                }
            }
            Type::INT4 => {
                let value = portal.parameter::<i32>(i, &pg_type)?;
                match inferenced_type {
                    Some(target) => {
                        deserialized_params
                            .push(coerce_int_value(value.map(|n| n as i64), target)?);
                    }
                    None => {
                        deserialized_params.push(ScalarValue::Int32(value));
                    }
                }
            }
            Type::INT8 => {
                let value = portal.parameter::<i64>(i, &pg_type)?;
                match inferenced_type {
                    Some(target) => {
                        deserialized_params.push(coerce_int_value(value, target)?);
                    }
                    None => {
                        deserialized_params.push(ScalarValue::Int64(value));
                    }
                }
            }
            Type::TEXT | Type::VARCHAR => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                match inferenced_type {
                    Some(DataType::LargeUtf8) => {
                        deserialized_params.push(ScalarValue::LargeUtf8(value));
                    }
                    _ => {
                        deserialized_params.push(ScalarValue::Utf8(value));
                    }
                }
            }
            Type::BYTEA => {
                let value = portal.parameter::<Vec<u8>>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Binary(value));
            }

            Type::FLOAT4 => {
                let value = portal.parameter::<f32>(i, &pg_type)?;
                match inferenced_type {
                    Some(target) => {
                        deserialized_params
                            .push(coerce_float_value(value.map(|n| n as f64), target)?);
                    }
                    None => {
                        deserialized_params.push(ScalarValue::Float32(value));
                    }
                }
            }
            Type::FLOAT8 => {
                let value = portal.parameter::<f64>(i, &pg_type)?;
                match inferenced_type {
                    Some(target) => {
                        deserialized_params.push(coerce_float_value(value, target)?);
                    }
                    None => {
                        deserialized_params.push(ScalarValue::Float64(value));
                    }
                }
            }
            Type::NUMERIC => {
                let value = portal.parameter::<Decimal>(i, &pg_type)?;
                match inferenced_type {
                    Some(DataType::Decimal128(p, s)) => {
                        deserialized_params.push(ScalarValue::Decimal128(
                            value.map(|mut n| {
                                n.rescale(*s as u32);
                                n.mantissa()
                            }),
                            *p,
                            *s,
                        ));
                    }
                    Some(DataType::UInt64) => {
                        deserialized_params
                            .push(ScalarValue::UInt64(value.and_then(|n| n.to_u64())));
                    }
                    Some(DataType::UInt32) => {
                        deserialized_params
                            .push(ScalarValue::UInt32(value.and_then(|n| n.to_u32())));
                    }
                    Some(DataType::Int64) => {
                        deserialized_params
                            .push(ScalarValue::Int64(value.and_then(|n| n.to_i64())));
                    }
                    Some(DataType::Int32) => {
                        deserialized_params
                            .push(ScalarValue::Int32(value.and_then(|n| n.to_i32())));
                    }
                    Some(DataType::Float64) => {
                        deserialized_params
                            .push(ScalarValue::Float64(value.and_then(|n| n.to_f64())));
                    }
                    Some(DataType::Float32) => {
                        deserialized_params
                            .push(ScalarValue::Float32(value.and_then(|n| n.to_f32())));
                    }
                    Some(target) => {
                        return Err(invalid_parameter_error(format!(
                            "Cannot coerce NUMERIC to {:?}",
                            target
                        )));
                    }
                    None => {
                        let scalar = match value {
                            None => ScalarValue::Decimal128(None, 0, 0),
                            Some(v) => {
                                let precision = match v.mantissa() {
                                    0 => 1,
                                    m => (m.abs() as f64).log10().floor() as u8 + 1,
                                };
                                let scale = v.scale() as i8;
                                ScalarValue::Decimal128(Some(v.mantissa()), precision, scale)
                            }
                        };
                        deserialized_params.push(scalar);
                    }
                }
            }
            Type::TIMESTAMP => {
                let value = portal.parameter::<NaiveDateTime>(i, &pg_type)?;
                match inferenced_type {
                    Some(target) => {
                        deserialized_params.push(coerce_timestamp_value(value, target)?);
                    }
                    None => {
                        deserialized_params.push(ScalarValue::TimestampMicrosecond(
                            value.map(|t| t.and_utc().timestamp_micros()),
                            None,
                        ));
                    }
                }
            }
            Type::TIMESTAMPTZ => {
                let value = portal.parameter::<DateTime<FixedOffset>>(i, &pg_type)?;
                match inferenced_type {
                    Some(target) => {
                        deserialized_params.push(coerce_timestamptz_value(value, target)?);
                    }
                    None => {
                        deserialized_params.push(ScalarValue::TimestampMicrosecond(
                            value.map(|t| t.timestamp_micros()),
                            None,
                        ));
                    }
                }
            }
            Type::DATE => {
                let value = portal.parameter::<NaiveDate>(i, &pg_type)?;
                deserialized_params
                    .push(ScalarValue::Date32(value.map(Date32Type::from_naive_date)));
            }
            Type::TIME => {
                let value = portal.parameter::<NaiveTime>(i, &pg_type)?;

                let ns = value.map(|t| {
                    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64
                });

                let scalar_value = match inferenced_type {
                    Some(DataType::Time64(TimeUnit::Nanosecond)) => {
                        ScalarValue::Time64Nanosecond(ns)
                    }
                    Some(DataType::Time64(TimeUnit::Microsecond)) => {
                        ScalarValue::Time64Microsecond(ns.map(|ns| (ns / 1_000) as _))
                    }
                    Some(DataType::Time32(TimeUnit::Millisecond)) => {
                        ScalarValue::Time32Millisecond(ns.map(|ns| (ns / 1_000_000) as _))
                    }
                    Some(DataType::Time32(TimeUnit::Second)) => {
                        ScalarValue::Time32Second(ns.map(|ns| (ns / 1_000_000_000) as _))
                    }
                    _ => ScalarValue::Time64Nanosecond(ns),
                };

                deserialized_params.push(scalar_value);
            }
            Type::UUID => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                // Store UUID as string for now
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::JSON | Type::JSONB => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                // Store JSON as string for now
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::INTERVAL => {
                let value = portal.parameter::<Interval>(i, &pg_type)?;
                match inferenced_type {
                    Some(target) => {
                        deserialized_params.push(coerce_interval_value(value, target)?);
                    }
                    None => {
                        let scalar_value = if let Some(i) = value {
                            ScalarValue::new_interval_mdn(
                                i.months,
                                i.days,
                                i.microseconds * 1_000i64,
                            )
                        } else {
                            ScalarValue::IntervalMonthDayNano(None)
                        };

                        deserialized_params.push(scalar_value);
                    }
                }
            }
            // Array types support
            Type::BOOL_ARRAY => {
                let value = portal.parameter::<Vec<Option<bool>>>(i, &pg_type)?;
                match value {
                    Some(values) => {
                        let scalar_values: Vec<ScalarValue> =
                            values.into_iter().map(ScalarValue::Boolean).collect();
                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Boolean),
                        ));
                    }
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Boolean,
                            true,
                            1,
                        ));
                    }
                }
            }
            Type::INT2_ARRAY => {
                let value = portal.parameter::<Vec<Option<i16>>>(i, &pg_type)?;
                match value {
                    Some(values) => {
                        let scalar_values: Vec<ScalarValue> =
                            values.into_iter().map(ScalarValue::Int16).collect();
                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Int16),
                        ));
                    }
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Int16,
                            true,
                            1,
                        ));
                    }
                }
            }
            Type::INT4_ARRAY => {
                let value = portal.parameter::<Vec<Option<i32>>>(i, &pg_type)?;
                match value {
                    Some(values) => {
                        let scalar_values: Vec<ScalarValue> =
                            values.into_iter().map(ScalarValue::Int32).collect();
                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Int32),
                        ));
                    }
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Int32,
                            true,
                            1,
                        ));
                    }
                }
            }
            Type::INT8_ARRAY => {
                let value = portal.parameter::<Vec<Option<i64>>>(i, &pg_type)?;
                match value {
                    Some(values) => {
                        let scalar_values: Vec<ScalarValue> =
                            values.into_iter().map(ScalarValue::Int64).collect();
                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Int64),
                        ));
                    }
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Int64,
                            true,
                            1,
                        ));
                    }
                }
            }
            Type::FLOAT4_ARRAY => {
                let value = portal.parameter::<Vec<Option<f32>>>(i, &pg_type)?;
                match value {
                    Some(values) => {
                        let scalar_values: Vec<ScalarValue> =
                            values.into_iter().map(ScalarValue::Float32).collect();
                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Float32),
                        ));
                    }
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Float32,
                            true,
                            1,
                        ));
                    }
                }
            }
            Type::FLOAT8_ARRAY => {
                let value = portal.parameter::<Vec<Option<f64>>>(i, &pg_type)?;
                match value {
                    Some(values) => {
                        let scalar_values: Vec<ScalarValue> =
                            values.into_iter().map(ScalarValue::Float64).collect();
                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Float64),
                        ));
                    }
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Float64,
                            true,
                            1,
                        ));
                    }
                }
            }
            Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
                let value = portal.parameter::<Vec<Option<String>>>(i, &pg_type)?;
                match value {
                    Some(values) => {
                        let scalar_values: Vec<ScalarValue> =
                            values.into_iter().map(ScalarValue::Utf8).collect();
                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Utf8),
                        ));
                    }
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Utf8,
                            true,
                            1,
                        ));
                    }
                }
            }
            Type::INTERVAL_ARRAY => {
                let value = portal.parameter::<Vec<Option<Interval>>>(i, &pg_type)?;
                match value {
                    Some(values) => {
                        let scalar_values: Vec<ScalarValue> = values
                            .into_iter()
                            .map(|i| {
                                if let Some(i) = i {
                                    ScalarValue::new_interval_mdn(
                                        i.months,
                                        i.days,
                                        i.microseconds * 1_000i64,
                                    )
                                } else {
                                    ScalarValue::IntervalMonthDayNano(None)
                                }
                            })
                            .collect();
                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(
                                &scalar_values,
                                &DataType::Interval(IntervalUnit::MonthDayNano),
                            ),
                        ));
                    }
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Interval(IntervalUnit::MonthDayNano),
                            true,
                            1,
                        ));
                    }
                }
            }
            Type::NUMERIC_ARRAY => {
                let value = portal.parameter::<Vec<Option<Decimal>>>(i, &pg_type)?;
                match value {
                    Some(values) => match inferenced_type {
                        Some(DataType::List(field)) => match field.data_type() {
                            DataType::Decimal128(p, s) => {
                                let scalar_values: Vec<ScalarValue> = values
                                    .into_iter()
                                    .map(|n| {
                                        ScalarValue::Decimal128(
                                            n.map(|mut n| {
                                                n.rescale(*s as u32);
                                                n.mantissa()
                                            }),
                                            *p,
                                            *s,
                                        )
                                    })
                                    .collect();
                                deserialized_params.push(ScalarValue::List(
                                    ScalarValue::new_list_nullable(
                                        &scalar_values,
                                        &DataType::Decimal128(*p, *s),
                                    ),
                                ));
                            }
                            DataType::UInt64 => {
                                let scalar_values: Vec<ScalarValue> = values
                                    .into_iter()
                                    .map(|n| ScalarValue::UInt64(n.and_then(|n| n.to_u64())))
                                    .collect();
                                deserialized_params.push(ScalarValue::List(
                                    ScalarValue::new_list_nullable(
                                        &scalar_values,
                                        &DataType::UInt64,
                                    ),
                                ));
                            }
                            DataType::Float64 => {
                                let scalar_values: Vec<ScalarValue> = values
                                    .into_iter()
                                    .map(|n| ScalarValue::Float64(n.and_then(|n| n.to_f64())))
                                    .collect();
                                deserialized_params.push(ScalarValue::List(
                                    ScalarValue::new_list_nullable(
                                        &scalar_values,
                                        &DataType::Float64,
                                    ),
                                ));
                            }
                            DataType::Float32 => {
                                let scalar_values: Vec<ScalarValue> = values
                                    .into_iter()
                                    .map(|n| ScalarValue::Float32(n.and_then(|n| n.to_f32())))
                                    .collect();
                                deserialized_params.push(ScalarValue::List(
                                    ScalarValue::new_list_nullable(
                                        &scalar_values,
                                        &DataType::Float32,
                                    ),
                                ));
                            }
                            other => {
                                let scalar_values: Vec<ScalarValue> = values
                                    .into_iter()
                                    .map(|n| {
                                        ScalarValue::Decimal128(
                                            n.map(|mut n| {
                                                n.rescale(0);
                                                n.mantissa()
                                            }),
                                            38,
                                            0,
                                        )
                                    })
                                    .collect();
                                deserialized_params.push(ScalarValue::List(
                                    ScalarValue::new_list_nullable(&scalar_values, other),
                                ));
                            }
                        },
                        _ => {
                            let scalar_values: Vec<ScalarValue> = values
                                .into_iter()
                                .map(|n| match n {
                                    None => ScalarValue::Decimal128(None, 0, 0),
                                    Some(v) => {
                                        let precision = match v.mantissa() {
                                            0 => 1,
                                            m => (m.abs() as f64).log10().floor() as u8 + 1,
                                        };
                                        let scale = v.scale() as i8;
                                        ScalarValue::Decimal128(
                                            Some(v.mantissa()),
                                            precision,
                                            scale,
                                        )
                                    }
                                })
                                .collect();
                            deserialized_params.push(ScalarValue::List(
                                ScalarValue::new_list_nullable(
                                    &scalar_values,
                                    &DataType::Decimal128(38, 0),
                                ),
                            ));
                        }
                    },
                    None => {
                        deserialized_params.push(ScalarValue::new_null_list(
                            DataType::Decimal128(38, 0),
                            true,
                            1,
                        ));
                    }
                }
            }
            // Advanced types
            Type::MONEY => {
                let value = portal.parameter::<i64>(i, &pg_type)?;
                // Store money as int64 (cents)
                deserialized_params.push(ScalarValue::Int64(value));
            }
            Type::INET => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                // Store IP addresses as strings for now
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::MACADDR => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                // Store MAC addresses as strings for now
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            // TODO: add more advanced types (composite types, ranges, etc.)
            _ => {
                // the client didn't provide type information and we are also
                // unable to inference the type, or it's a type that we haven't
                // supported:
                //
                // In this case we retry to resolve it as String or StringArray
                let value = portal.parameter::<String>(i, &pg_type)?;
                if let Some(value) = value {
                    if value.starts_with('{') && value.ends_with('}') {
                        // Looks like an array
                        let items = value.trim_matches(|c| c == '{' || c == '}' || c == ' ');
                        let items = items.split(',').map(|s| s.trim());
                        let scalar_values: Vec<ScalarValue> = items
                            .map(|s| ScalarValue::Utf8(Some(s.to_string())))
                            .collect();

                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Utf8),
                        ));
                    } else {
                        deserialized_params.push(ScalarValue::Utf8(Some(value)));
                    }
                }
            }
        }
    }

    Ok(ParamValues::List(
        deserialized_params.into_iter().map(|p| p.into()).collect(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use bytes::{Bytes, BytesMut};
    use chrono::{FixedOffset, NaiveDate, NaiveTime, TimeZone};
    use datafusion::{
        arrow::datatypes::{IntervalUnit, TimeUnit},
        common::ParamValues,
        scalar::ScalarValue,
    };
    use pg_interval::Interval;
    use pgwire::{
        api::{portal::Portal, stmt::StoredStatement},
        messages::{data::FORMAT_CODE_BINARY, extendedquery::Bind},
    };
    use postgres_types::{ToSql, Type};
    use rust_decimal::Decimal;

    use crate::datatypes::df::deserialize_parameters;

    fn encode_param<T: ToSql + Sync>(value: &T, pg_type: &Type) -> Bytes {
        let mut buf = BytesMut::new();
        value.to_sql(pg_type, &mut buf).unwrap();
        buf.freeze()
    }

    fn make_portal<T: ToSql + Sync>(value: &T, pg_type: Type) -> Portal<&'static str> {
        let data = encode_param(value, &pg_type);
        let bind = Bind::new(
            None,
            None,
            vec![FORMAT_CODE_BINARY],
            vec![Some(data)],
            vec![],
        );
        let stmt = StoredStatement::new("id".into(), "s", vec![Some(pg_type)]);
        Portal::try_new(&bind, Arc::new(stmt)).unwrap()
    }

    fn make_null_portal(pg_type: Type) -> Portal<&'static str> {
        let bind = Bind::new(None, None, vec![FORMAT_CODE_BINARY], vec![None], vec![]);
        let stmt = StoredStatement::new("id".into(), "s", vec![Some(pg_type)]);
        Portal::try_new(&bind, Arc::new(stmt)).unwrap()
    }

    fn get_scalar(result: ParamValues) -> ScalarValue {
        let ParamValues::List(list) = result else {
            panic!("expected list");
        };
        assert_eq!(list.len(), 1);
        list.into_iter().next().unwrap().value().clone()
    }

    fn get_result(portal: &Portal<&'static str>, inferred: Option<&DataType>) -> ScalarValue {
        let result = deserialize_parameters(portal, &[inferred]).unwrap();
        get_scalar(result)
    }

    // -- Basic types --

    #[test]
    fn test_bool() {
        let portal = make_portal(&true, Type::BOOL);
        assert_eq!(get_result(&portal, None), ScalarValue::Boolean(Some(true)));

        let portal = make_null_portal(Type::BOOL);
        assert_eq!(get_result(&portal, None), ScalarValue::Boolean(None));
    }

    #[test]
    fn test_char() {
        let portal = make_portal(&42i8, Type::CHAR);
        assert_eq!(get_result(&portal, None), ScalarValue::Int8(Some(42)));
    }

    #[test]
    fn test_text_varchar() {
        for pg_type in [Type::TEXT, Type::VARCHAR] {
            let portal = make_portal(&"hello".to_string(), pg_type.clone());
            assert_eq!(
                get_result(&portal, None),
                ScalarValue::Utf8(Some("hello".to_string()))
            );
        }
    }

    #[test]
    fn test_text_large_utf8() {
        let portal = make_portal(&"hello".to_string(), Type::TEXT);
        assert_eq!(
            get_result(&portal, Some(&DataType::LargeUtf8)),
            ScalarValue::LargeUtf8(Some("hello".to_string()))
        );
    }

    #[test]
    fn test_bytea() {
        let portal = make_portal(&vec![1u8, 2, 3], Type::BYTEA);
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::Binary(Some(vec![1, 2, 3]))
        );
    }

    // -- INT2 coercion --

    #[test]
    fn test_int2_direct() {
        let portal = make_portal(&42i16, Type::INT2);
        assert_eq!(get_result(&portal, None), ScalarValue::Int16(Some(42)));
    }

    #[test]
    fn test_int2_coerce_to_int32() {
        let portal = make_portal(&100i16, Type::INT2);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int32)),
            ScalarValue::Int32(Some(100))
        );
    }

    #[test]
    fn test_int2_coerce_to_int64() {
        let portal = make_portal(&100i16, Type::INT2);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int64)),
            ScalarValue::Int64(Some(100))
        );
    }

    #[test]
    fn test_int2_coerce_to_float64() {
        let portal = make_portal(&100i16, Type::INT2);
        assert_eq!(
            get_result(&portal, Some(&DataType::Float64)),
            ScalarValue::Float64(Some(100.0))
        );
    }

    #[test]
    fn test_int2_coerce_to_uint32() {
        let portal = make_portal(&100i16, Type::INT2);
        assert_eq!(
            get_result(&portal, Some(&DataType::UInt32)),
            ScalarValue::UInt32(Some(100))
        );
    }

    #[test]
    fn test_int2_null_coercion() {
        let portal = make_null_portal(Type::INT2);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int64)),
            ScalarValue::Int64(None)
        );
    }

    // -- INT4 coercion --

    #[test]
    fn test_int4_coerce_to_int8() {
        let portal = make_portal(&42i32, Type::INT4);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int8)),
            ScalarValue::Int8(Some(42))
        );
    }

    #[test]
    fn test_int4_coerce_to_int64() {
        let portal = make_portal(&100i32, Type::INT4);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int64)),
            ScalarValue::Int64(Some(100))
        );
    }

    #[test]
    fn test_int4_coerce_to_uint64() {
        let portal = make_portal(&100i32, Type::INT4);
        assert_eq!(
            get_result(&portal, Some(&DataType::UInt64)),
            ScalarValue::UInt64(Some(100))
        );
    }

    #[test]
    fn test_int4_coerce_out_of_range() {
        let portal = make_portal(&300i32, Type::INT4);
        assert!(deserialize_parameters(&portal, &[Some(&DataType::Int8)]).is_err());
    }

    #[test]
    fn test_int4_coerce_negative_to_unsigned() {
        let portal = make_portal(&(-1i32), Type::INT4);
        assert!(deserialize_parameters(&portal, &[Some(&DataType::UInt64)]).is_err());
    }

    // -- INT8 coercion --

    #[test]
    fn test_int8_direct() {
        let portal = make_portal(&42i64, Type::INT8);
        assert_eq!(get_result(&portal, None), ScalarValue::Int64(Some(42)));
    }

    #[test]
    fn test_int8_coerce_to_int16() {
        let portal = make_portal(&100i64, Type::INT8);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int16)),
            ScalarValue::Int16(Some(100))
        );
    }

    #[test]
    fn test_int8_coerce_to_uint32() {
        let portal = make_portal(&100i64, Type::INT8);
        assert_eq!(
            get_result(&portal, Some(&DataType::UInt32)),
            ScalarValue::UInt32(Some(100))
        );
    }

    #[test]
    fn test_int8_coerce_out_of_range() {
        let portal = make_portal(&100000i64, Type::INT8);
        assert!(deserialize_parameters(&portal, &[Some(&DataType::Int8)]).is_err());
    }

    // -- FLOAT4 coercion --

    #[test]
    fn test_float4_direct() {
        let portal = make_portal(&3.14f32, Type::FLOAT4);
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::Float32(Some(3.14f32))
        );
    }

    #[test]
    fn test_float4_coerce_to_float64() {
        let portal = make_portal(&3.14f32, Type::FLOAT4);
        assert_eq!(
            get_result(&portal, Some(&DataType::Float64)),
            ScalarValue::Float64(Some(3.14f32 as f64))
        );
    }

    #[test]
    fn test_float4_coerce_to_int32() {
        let portal = make_portal(&42.0f32, Type::FLOAT4);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int32)),
            ScalarValue::Int32(Some(42))
        );
    }

    // -- FLOAT8 coercion --

    #[test]
    fn test_float8_direct() {
        let portal = make_portal(&3.14f64, Type::FLOAT8);
        assert_eq!(get_result(&portal, None), ScalarValue::Float64(Some(3.14)));
    }

    #[test]
    fn test_float8_coerce_to_float32() {
        let portal = make_portal(&3.14f64, Type::FLOAT8);
        assert_eq!(
            get_result(&portal, Some(&DataType::Float32)),
            ScalarValue::Float32(Some(3.14f64 as f32))
        );
    }

    #[test]
    fn test_float8_coerce_to_int64() {
        let portal = make_portal(&42.0f64, Type::FLOAT8);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int64)),
            ScalarValue::Int64(Some(42))
        );
    }

    #[test]
    fn test_float8_coerce_fractional_to_int_error() {
        let portal = make_portal(&3.14f64, Type::FLOAT8);
        assert!(deserialize_parameters(&portal, &[Some(&DataType::Int64)]).is_err());
    }

    // -- NUMERIC coercion --

    #[test]
    fn test_numeric_direct() {
        let portal = make_portal(&Decimal::new(123, 1), Type::NUMERIC);
        let result = get_result(&portal, None);
        match result {
            ScalarValue::Decimal128(Some(v), p, s) => {
                assert_eq!(v, 123);
                assert_eq!(s, 1);
                assert!(p > 0);
            }
            other => panic!("expected Decimal128, got {other:?}"),
        }
    }

    #[test]
    fn test_numeric_coerce_to_decimal128_with_rescale() {
        // Decimal::new(123, 1) = 12.3, rescale to scale 3 => 12.300 => mantissa 12300
        let portal = make_portal(&Decimal::new(123, 1), Type::NUMERIC);
        assert_eq!(
            get_result(&portal, Some(&DataType::Decimal128(10, 3))),
            ScalarValue::Decimal128(Some(12300), 10, 3)
        );
    }

    #[test]
    fn test_numeric_coerce_to_uint64() {
        let portal = make_portal(&Decimal::from(42u64), Type::NUMERIC);
        assert_eq!(
            get_result(&portal, Some(&DataType::UInt64)),
            ScalarValue::UInt64(Some(42))
        );
    }

    #[test]
    fn test_numeric_coerce_to_float64() {
        let portal = make_portal(&Decimal::new(314, 2), Type::NUMERIC);
        assert_eq!(
            get_result(&portal, Some(&DataType::Float64)),
            ScalarValue::Float64(Some(3.14))
        );
    }

    #[test]
    fn test_numeric_coerce_to_int32() {
        let portal = make_portal(&Decimal::from(42i32), Type::NUMERIC);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int32)),
            ScalarValue::Int32(Some(42))
        );
    }

    #[test]
    fn test_numeric_null() {
        let portal = make_null_portal(Type::NUMERIC);
        assert_eq!(
            get_result(&portal, Some(&DataType::Decimal128(10, 2))),
            ScalarValue::Decimal128(None, 10, 2)
        );
    }

    // -- TIMESTAMP coercion --

    #[test]
    fn test_timestamp_direct() {
        let ts = chrono::DateTime::from_timestamp(1700000000, 0)
            .unwrap()
            .naive_utc();
        let portal = make_portal(&ts, Type::TIMESTAMP);
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::TimestampMicrosecond(Some(ts.and_utc().timestamp_micros()), None)
        );
    }

    #[test]
    fn test_timestamp_coerce_to_seconds() {
        let ts = chrono::DateTime::from_timestamp(1700000000, 0)
            .unwrap()
            .naive_utc();
        let portal = make_portal(&ts, Type::TIMESTAMP);
        assert_eq!(
            get_result(&portal, Some(&DataType::Timestamp(TimeUnit::Second, None))),
            ScalarValue::TimestampSecond(Some(1700000000), None)
        );
    }

    #[test]
    fn test_timestamp_coerce_to_milliseconds() {
        let ts = chrono::DateTime::from_timestamp(1700000000, 0)
            .unwrap()
            .naive_utc();
        let portal = make_portal(&ts, Type::TIMESTAMP);
        assert_eq!(
            get_result(
                &portal,
                Some(&DataType::Timestamp(TimeUnit::Millisecond, None))
            ),
            ScalarValue::TimestampMillisecond(Some(1700000000000), None)
        );
    }

    #[test]
    fn test_timestamp_coerce_to_nanoseconds() {
        let ts = chrono::DateTime::from_timestamp(1700000000, 0)
            .unwrap()
            .naive_utc();
        let portal = make_portal(&ts, Type::TIMESTAMP);
        assert_eq!(
            get_result(
                &portal,
                Some(&DataType::Timestamp(TimeUnit::Nanosecond, None))
            ),
            ScalarValue::TimestampNanosecond(
                Some(ts.and_utc().timestamp_nanos_opt().unwrap()),
                None
            )
        );
    }

    // -- TIMESTAMPTZ coercion --

    #[test]
    fn test_timestamptz_direct() {
        let ts = FixedOffset::east_opt(3600).unwrap().from_utc_datetime(
            &chrono::DateTime::from_timestamp(1700000000, 0)
                .unwrap()
                .naive_utc(),
        );
        let portal = make_portal(&ts, Type::TIMESTAMPTZ);
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::TimestampMicrosecond(Some(ts.timestamp_micros()), None)
        );
    }

    #[test]
    fn test_timestamptz_coerce_to_seconds() {
        let ts = FixedOffset::east_opt(3600).unwrap().from_utc_datetime(
            &chrono::DateTime::from_timestamp(1700000000, 0)
                .unwrap()
                .naive_utc(),
        );
        let portal = make_portal(&ts, Type::TIMESTAMPTZ);
        assert_eq!(
            get_result(&portal, Some(&DataType::Timestamp(TimeUnit::Second, None))),
            ScalarValue::TimestampSecond(Some(1700000000), None)
        );
    }

    #[test]
    fn test_timestamptz_coerce_to_nanoseconds() {
        let ts = FixedOffset::east_opt(3600).unwrap().from_utc_datetime(
            &chrono::DateTime::from_timestamp(1700000000, 0)
                .unwrap()
                .naive_utc(),
        );
        let portal = make_portal(&ts, Type::TIMESTAMPTZ);
        assert_eq!(
            get_result(
                &portal,
                Some(&DataType::Timestamp(TimeUnit::Nanosecond, None))
            ),
            ScalarValue::TimestampNanosecond(Some(ts.timestamp_nanos_opt().unwrap()), None)
        );
    }

    // -- DATE --

    #[test]
    fn test_date() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let portal = make_portal(&date, Type::DATE);
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::Date32(Some(
                datafusion::arrow::datatypes::Date32Type::from_naive_date(date)
            ))
        );
    }

    // -- TIME with all unit coercions --

    #[test]
    fn test_deserialise_time_params() {
        let portal = make_portal(
            &NaiveTime::from_num_seconds_from_midnight_opt(1, 0).unwrap(),
            Type::TIME,
        );

        for (arrow_type, expected) in [
            (
                DataType::Time32(TimeUnit::Second),
                ScalarValue::Time32Second(Some(1)),
            ),
            (
                DataType::Time32(TimeUnit::Millisecond),
                ScalarValue::Time32Millisecond(Some(1000)),
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                ScalarValue::Time64Microsecond(Some(1000000)),
            ),
            (
                DataType::Time64(TimeUnit::Nanosecond),
                ScalarValue::Time64Nanosecond(Some(1000000000)),
            ),
        ] {
            assert_eq!(get_result(&portal, Some(&arrow_type)), expected);
        }
    }

    #[test]
    fn test_time_default_nanosecond() {
        let time = NaiveTime::from_num_seconds_from_midnight_opt(1, 0).unwrap();
        let portal = make_portal(&time, Type::TIME);
        // No inferred type: should default to Time64Nanosecond
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::Time64Nanosecond(Some(1_000_000_000))
        );
    }

    // -- INTERVAL coercion --

    #[test]
    fn test_interval_direct() {
        let interval = Interval::new(1, 2, 3_000_000);
        let portal = make_portal(&interval, Type::INTERVAL);
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::new_interval_mdn(1, 2, 3_000_000_000i64)
        );
    }

    #[test]
    fn test_interval_coerce_to_month_day_nano() {
        let interval = Interval::new(1, 2, 3_000_000);
        let portal = make_portal(&interval, Type::INTERVAL);
        assert_eq!(
            get_result(
                &portal,
                Some(&DataType::Interval(IntervalUnit::MonthDayNano))
            ),
            ScalarValue::new_interval_mdn(1, 2, 3_000_000_000i64)
        );
    }

    #[test]
    fn test_interval_coerce_to_year_month() {
        let interval = Interval::new(3, 0, 0);
        let portal = make_portal(&interval, Type::INTERVAL);
        assert_eq!(
            get_result(&portal, Some(&DataType::Interval(IntervalUnit::YearMonth))),
            ScalarValue::IntervalYearMonth(Some(3))
        );
    }

    #[test]
    fn test_interval_coerce_to_year_month_with_days_error() {
        let interval = Interval::new(3, 1, 0);
        let portal = make_portal(&interval, Type::INTERVAL);
        assert!(
            deserialize_parameters(
                &portal,
                &[Some(&DataType::Interval(IntervalUnit::YearMonth))]
            )
            .is_err()
        );
    }

    #[test]
    fn test_interval_coerce_to_day_time() {
        let interval = Interval::new(0, 5, 3_000_000);
        let portal = make_portal(&interval, Type::INTERVAL);
        assert_eq!(
            get_result(&portal, Some(&DataType::Interval(IntervalUnit::DayTime))),
            ScalarValue::new_interval_dt(5, 3000)
        );
    }

    #[test]
    fn test_interval_coerce_to_day_time_with_months_error() {
        let interval = Interval::new(1, 5, 3_000_000);
        let portal = make_portal(&interval, Type::INTERVAL);
        assert!(
            deserialize_parameters(&portal, &[Some(&DataType::Interval(IntervalUnit::DayTime))])
                .is_err()
        );
    }

    #[test]
    fn test_interval_coerce_to_day_time_sub_millis_error() {
        let interval = Interval::new(0, 5, 500);
        let portal = make_portal(&interval, Type::INTERVAL);
        assert!(
            deserialize_parameters(&portal, &[Some(&DataType::Interval(IntervalUnit::DayTime))])
                .is_err()
        );
    }

    #[test]
    fn test_interval_null() {
        let portal = make_null_portal(Type::INTERVAL);
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::IntervalMonthDayNano(None)
        );
    }

    // -- UUID, JSON --
    // These types don't have FromSql<String> support in pgwire, so they
    // fall through to the `_` wildcard branch which calls
    // `portal.parameter::<String>()`. Testing them requires a postgres
    // round-trip, so we skip unit tests for these.

    // -- Advanced types (MONEY, INET, MACADDR) --
    // Same as above: pgwire's FromSql<String> doesn't accept MONEY/INET/MACADDR.

    // -- Null parameters --

    #[test]
    fn test_null_int4() {
        let portal = make_null_portal(Type::INT4);
        assert_eq!(
            get_result(&portal, Some(&DataType::Int32)),
            ScalarValue::Int32(None)
        );
    }

    #[test]
    fn test_null_timestamp() {
        let portal = make_null_portal(Type::TIMESTAMP);
        assert_eq!(
            get_result(
                &portal,
                Some(&DataType::Timestamp(TimeUnit::Millisecond, None))
            ),
            ScalarValue::TimestampMillisecond(None, None)
        );
    }

    // -- Fallback: unknown type as string --

    #[test]
    fn test_unknown_type_string() {
        let portal = make_portal(&"hello".to_string(), Type::NAME);
        assert_eq!(
            get_result(&portal, None),
            ScalarValue::Utf8(Some("hello".to_string()))
        );
    }
}
