use std::error::Error;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::{array::*, datatypes::*};
use bytes::BufMut;
use bytes::BytesMut;
use chrono::{NaiveDate, NaiveDateTime};
#[cfg(feature = "datafusion")]
use datafusion::arrow::{array::*, datatypes::*};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::types::format::FormatOptions;
use pgwire::types::ToSqlText;
use postgres_types::{ToSql, Type};
use rust_decimal::Decimal;
use timezone::Tz;

use crate::error::ToSqlError;
use crate::list_encoder::encode_list;
use crate::struct_encoder::encode_struct;

pub trait Encoder {
    fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized;
}

impl Encoder for DataRowEncoder {
    fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        self.encode_field_with_type_and_format(
            value,
            pg_field.datatype(),
            pg_field.format(),
            pg_field.format_options(),
        )
    }
}

pub(crate) struct EncodedValue {
    pub(crate) bytes: BytesMut,
}

impl std::fmt::Debug for EncodedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedValue").finish()
    }
}

impl ToSql for EncodedValue {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        out.writer().write_all(&self.bytes)?;
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Send + Sync>> {
        self.to_sql(ty, out)
    }
}

impl ToSqlText for EncodedValue {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        out.writer().write_all(&self.bytes)?;
        Ok(postgres_types::IsNull::No)
    }
}

fn get_bool_value(arr: &Arc<dyn Array>, idx: usize) -> Option<bool> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(idx)
    })
}

macro_rules! get_primitive_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> Option<$pt> {
            (!arr.is_null(idx)).then(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<$t>>()
                    .unwrap()
                    .value(idx)
            })
        }
    };
}

get_primitive_value!(get_i8_value, Int8Type, i8);
get_primitive_value!(get_i16_value, Int16Type, i16);
get_primitive_value!(get_i32_value, Int32Type, i32);
get_primitive_value!(get_i64_value, Int64Type, i64);
get_primitive_value!(get_u8_value, UInt8Type, u8);
get_primitive_value!(get_u16_value, UInt16Type, u16);
get_primitive_value!(get_u32_value, UInt32Type, u32);
get_primitive_value!(get_u64_value, UInt64Type, u64);
get_primitive_value!(get_f32_value, Float32Type, f32);
get_primitive_value!(get_f64_value, Float64Type, f64);

fn get_utf8_view_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_binary_view_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&[u8]> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_large_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_binary_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&[u8]> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_large_binary_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&[u8]> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_date32_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDate> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Date32Array>()
        .unwrap()
        .value_as_date(idx)
}

fn get_date64_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDate> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Date64Array>()
        .unwrap()
        .value_as_date(idx)
}

fn get_time32_second_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_time32_millisecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_time64_microsecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}
fn get_time64_nanosecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDateTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap()
        .value_as_datetime(idx)
}

fn get_numeric_128_value(
    arr: &Arc<dyn Array>,
    idx: usize,
    scale: u32,
) -> PgWireResult<Option<Decimal>> {
    if arr.is_null(idx) {
        return Ok(None);
    }

    let array = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
    let value = array.value(idx);
    Decimal::try_from_i128_with_scale(value, scale)
        .map_err(|e| {
            let error_code = match e {
                rust_decimal::Error::ExceedsMaximumPossibleValue => {
                    "22003" // numeric_value_out_of_range
                }
                rust_decimal::Error::LessThanMinimumPossibleValue => {
                    "22003" // numeric_value_out_of_range
                }
                rust_decimal::Error::ScaleExceedsMaximumPrecision(scale) => {
                    return PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "22003".to_string(),
                        format!("Scale {scale} exceeds maximum precision for numeric type"),
                    )));
                }
                _ => "22003", // generic numeric_value_out_of_range
            };
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                error_code.to_string(),
                format!("Numeric value conversion failed: {e}"),
            )))
        })
        .map(Some)
}

pub fn encode_value<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    _arrow_filed: &Field,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    let type_ = pg_field.datatype();

    match arr.data_type() {
        DataType::Null => encoder.encode_field(&None::<i8>, pg_field)?,
        DataType::Boolean => encoder.encode_field(&get_bool_value(arr, idx), pg_field)?,
        DataType::Int8 => encoder.encode_field(&get_i8_value(arr, idx), pg_field)?,
        DataType::Int16 => encoder.encode_field(&get_i16_value(arr, idx), pg_field)?,
        DataType::Int32 => encoder.encode_field(&get_i32_value(arr, idx), pg_field)?,
        DataType::Int64 => encoder.encode_field(&get_i64_value(arr, idx), pg_field)?,
        DataType::UInt8 => {
            encoder.encode_field(&(get_u8_value(arr, idx).map(|x| x as i8)), pg_field)?
        }
        DataType::UInt16 => {
            encoder.encode_field(&(get_u16_value(arr, idx).map(|x| x as i16)), pg_field)?
        }
        DataType::UInt32 => encoder.encode_field(&get_u32_value(arr, idx), pg_field)?,
        DataType::UInt64 => {
            encoder.encode_field(&(get_u64_value(arr, idx).map(|x| x as i64)), pg_field)?
        }
        DataType::Float32 => encoder.encode_field(&get_f32_value(arr, idx), pg_field)?,
        DataType::Float64 => encoder.encode_field(&get_f64_value(arr, idx), pg_field)?,
        DataType::Decimal128(_, s) => {
            encoder.encode_field(&get_numeric_128_value(arr, idx, *s as u32)?, pg_field)?
        }
        DataType::Utf8 => encoder.encode_field(&get_utf8_value(arr, idx), pg_field)?,
        DataType::Utf8View => encoder.encode_field(&get_utf8_view_value(arr, idx), pg_field)?,
        DataType::BinaryView => encoder.encode_field(&get_binary_view_value(arr, idx), pg_field)?,
        DataType::LargeUtf8 => encoder.encode_field(&get_large_utf8_value(arr, idx), pg_field)?,
        DataType::Binary => encoder.encode_field(&get_binary_value(arr, idx), pg_field)?,
        DataType::LargeBinary => {
            encoder.encode_field(&get_large_binary_value(arr, idx), pg_field)?
        }
        DataType::Date32 => encoder.encode_field(&get_date32_value(arr, idx), pg_field)?,
        DataType::Date64 => encoder.encode_field(&get_date64_value(arr, idx), pg_field)?,
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                encoder.encode_field(&get_time32_second_value(arr, idx), pg_field)?
            }
            TimeUnit::Millisecond => {
                encoder.encode_field(&get_time32_millisecond_value(arr, idx), pg_field)?
            }
            _ => {}
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                encoder.encode_field(&get_time64_microsecond_value(arr, idx), pg_field)?
            }
            TimeUnit::Nanosecond => {
                encoder.encode_field(&get_time64_nanosecond_value(arr, idx), pg_field)?
            }
            _ => {}
        },
        DataType::Timestamp(unit, timezone) => match unit {
            TimeUnit::Second => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<NaiveDateTime>, pg_field);
                }
                let ts_array = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());

                    encoder.encode_field(&value, pg_field)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field(&value, pg_field)?;
                }
            }
            TimeUnit::Millisecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<NaiveDateTime>, pg_field);
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field(&value, pg_field)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field(&value, pg_field)?;
                }
            }
            TimeUnit::Microsecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<NaiveDateTime>, pg_field);
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field(&value, pg_field)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field(&value, pg_field)?;
                }
            }
            TimeUnit::Nanosecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<NaiveDateTime>, pg_field);
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field(&value, pg_field)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field(&value, pg_field)?;
                }
            }
        },
        DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_) => {
            if arr.is_null(idx) {
                return encoder.encode_field(&None::<&[i8]>, pg_field);
            }
            let array = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
            let value = encode_list(array, pg_field)?;
            encoder.encode_field(&value, pg_field)?
        }
        DataType::Struct(arrow_fields) => {
            let fields = match type_.kind() {
                postgres_types::Kind::Composite(fields) => fields,
                _ => {
                    return Err(PgWireError::ApiError(ToSqlError::from(format!(
                        "Failed to unwrap a composite type from type {type_}"
                    ))));
                }
            };
            let value = encode_struct(arr, idx, arrow_fields, pg_field)?;
            encoder.encode_field(&value, pg_field)?
        }
        DataType::Dictionary(_, value_type) => {
            if arr.is_null(idx) {
                return encoder.encode_field(&None::<i8>, pg_field);
            }
            // Get the dictionary values and the mapped row index
            macro_rules! get_dict_values_and_index {
                ($key_type:ty) => {
                    arr.as_any()
                        .downcast_ref::<DictionaryArray<$key_type>>()
                        .map(|dict| (dict.values(), dict.keys().value(idx) as usize))
                };
            }

            // Try to extract values using different key types
            let (values, idx) = get_dict_values_and_index!(Int8Type)
                .or_else(|| get_dict_values_and_index!(Int16Type))
                .or_else(|| get_dict_values_and_index!(Int32Type))
                .or_else(|| get_dict_values_and_index!(Int64Type))
                .or_else(|| get_dict_values_and_index!(UInt8Type))
                .or_else(|| get_dict_values_and_index!(UInt16Type))
                .or_else(|| get_dict_values_and_index!(UInt32Type))
                .or_else(|| get_dict_values_and_index!(UInt64Type))
                .ok_or_else(|| {
                    ToSqlError::from(format!(
                        "Unsupported dictionary key type for value type {value_type}"
                    ))
                })?;

            let inner_arrow_field = Field::new(pg_field.name(), *value_type.clone(), true);

            encode_value(encoder, values, idx, &inner_arrow_field, pg_field)?
        }
        _ => {
            return Err(PgWireError::ApiError(ToSqlError::from(format!(
                "Unsupported Datatype {} and array {:?}",
                arr.data_type(),
                &arr
            ))));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use pgwire::api::results::FieldFormat;

    use super::*;

    #[test]
    fn encodes_dictionary_array() {
        #[derive(Default)]
        struct MockEncoder {
            encoded_value: String,
        }

        impl Encoder for MockEncoder {
            fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
            where
                T: ToSql + ToSqlText + Sized,
            {
                let mut bytes = BytesMut::new();
                let _sql_text =
                    value.to_sql_text(pg_field.datatype(), &mut bytes, &FormatOptions::default());
                let string = String::from_utf8(bytes.to_vec());
                self.encoded_value = string.unwrap();
                Ok(())
            }
        }

        let val = "~!@&$[]()@@!!";
        let value = StringArray::from_iter_values([val]);
        let keys = Int8Array::from_iter_values([0, 0, 0, 0]);
        let dict_arr: Arc<dyn Array> =
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::new(value)).unwrap());

        let mut encoder = MockEncoder::default();

        let arrow_field = Field::new("x", DataType::Utf8, true);
        let pg_field = FieldInfo::new("x".to_string(), None, None, Type::TEXT, FieldFormat::Text);
        let result = encode_value(&mut encoder, &dict_arr, 2, &arrow_field, &pg_field);

        assert!(result.is_ok());

        assert!(encoder.encoded_value == val);
    }
}
