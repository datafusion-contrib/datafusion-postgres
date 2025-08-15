use std::iter;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ParamValues;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use futures::{stream, StreamExt};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::QueryResponse;
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::{arrow_schema_to_pg_fields, encode_recordbatch, into_pg_type};

pub async fn encode_dataframe<'a>(
    df: DataFrame,
    format: &Format,
) -> PgWireResult<QueryResponse<'a>> {
    let fields = Arc::new(arrow_schema_to_pg_fields(df.schema().as_arrow(), format)?);

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
        pg_type_hint: Option<&Type>,
        inferenced_type: Option<&DataType>,
    ) -> PgWireResult<Type> {
        if let Some(ty) = pg_type_hint {
            Ok(ty.clone())
        } else if let Some(infer_type) = inferenced_type {
            into_pg_type(infer_type)
        } else {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_string(),
                "XX000".to_string(),
                "Unknown parameter type".to_string(),
            ))))
        }
    }

    let param_len = portal.parameter_len();
    let mut deserialized_params = Vec::with_capacity(param_len);
    for i in 0..param_len {
        let pg_type = get_pg_type(
            portal.statement.parameter_types.get(i),
            inferenced_types.get(i).and_then(|v| v.to_owned()),
        )?;
        match pg_type {
            // enumerate all supported parameter types and deserialize the
            // type to ScalarValue
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
                deserialized_params.push(ScalarValue::Int16(value));
            }
            Type::INT4 => {
                let value = portal.parameter::<i32>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int32(value));
            }
            Type::INT8 => {
                let value = portal.parameter::<i64>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int64(value));
            }
            Type::TEXT => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                // Use Utf8View for TEXT type to match DataFusion's internal schema expectations
                deserialized_params.push(ScalarValue::Utf8View(value));
            }
            Type::VARCHAR => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::BYTEA => {
                let value = portal.parameter::<Vec<u8>>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Binary(value));
            }

            Type::FLOAT4 => {
                let value = portal.parameter::<f32>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Float32(value));
            }
            Type::FLOAT8 => {
                let value = portal.parameter::<f64>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Float64(value));
            }
            Type::NUMERIC => {
                let value = match portal.parameter::<Decimal>(i, &pg_type)? {
                    None => ScalarValue::Decimal128(None, 0, 0),
                    Some(value) => {
                        let precision = match value.mantissa() {
                            0 => 1,
                            m => (m.abs() as f64).log10().floor() as u8 + 1,
                        };
                        let scale = value.scale() as i8;
                        ScalarValue::Decimal128(value.to_i128(), precision, scale)
                    }
                };
                deserialized_params.push(value);
            }
            Type::TIMESTAMP => {
                let value = portal.parameter::<NaiveDateTime>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::TimestampMicrosecond(
                    value.map(|t| t.and_utc().timestamp_micros()),
                    None,
                ));
            }
            Type::TIMESTAMPTZ => {
                let value = portal.parameter::<DateTime<FixedOffset>>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::TimestampMicrosecond(
                    value.map(|t| t.timestamp_micros()),
                    value.map(|t| t.offset().to_string().into()),
                ));
            }
            Type::DATE => {
                let value = portal.parameter::<NaiveDate>(i, &pg_type)?;
                deserialized_params
                    .push(ScalarValue::Date32(value.map(Date32Type::from_naive_date)));
            }
            Type::TIME => {
                let value = portal.parameter::<NaiveTime>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Time64Microsecond(value.map(|t| {
                    t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1_000
                })));
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
                let value = portal.parameter::<String>(i, &pg_type)?;
                // Store interval as string for now (DataFusion has limited interval support)
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            // Array types support
            Type::BOOL_ARRAY => {
                let value = portal.parameter::<Vec<Option<bool>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Boolean).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Boolean,
                )));
            }
            Type::INT2_ARRAY => {
                let value = portal.parameter::<Vec<Option<i16>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Int16).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Int16,
                )));
            }
            Type::INT4_ARRAY => {
                let value = portal.parameter::<Vec<Option<i32>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Int32).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Int32,
                )));
            }
            Type::INT8_ARRAY => {
                let value = portal.parameter::<Vec<Option<i64>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Int64).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Int64,
                )));
            }
            Type::FLOAT4_ARRAY => {
                let value = portal.parameter::<Vec<Option<f32>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Float32).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Float32,
                )));
            }
            Type::FLOAT8_ARRAY => {
                let value = portal.parameter::<Vec<Option<f64>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Float64).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Float64,
                )));
            }
            Type::TEXT_ARRAY => {
                let value = portal.parameter::<Vec<Option<String>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Utf8View).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Utf8View,
                )));
            }
            Type::VARCHAR_ARRAY => {
                let value = portal.parameter::<Vec<Option<String>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Utf8).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Utf8,
                )));
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
            Type::UNKNOWN => {
                // For unknown types, try to deserialize as integer first, then fallback to text
                // This handles cases like NULL arithmetic where DataFusion can't infer types
                match portal.parameter::<i32>(i, &Type::INT4) {
                    Ok(value) => deserialized_params.push(ScalarValue::Int32(value)),
                    Err(_) => {
                        // Fallback to text if integer parsing fails
                        let value = portal.parameter::<String>(i, &Type::TEXT)?;
                        deserialized_params.push(ScalarValue::Utf8View(value));
                    }
                }
            }
            // TODO: add more advanced types (composite types, ranges, etc.)
            _ => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_string(),
                    "XX000".to_string(),
                    format!("Unsupported parameter type: {pg_type}"),
                ))));
            }
        }
    }

    Ok(ParamValues::List(deserialized_params))
}
