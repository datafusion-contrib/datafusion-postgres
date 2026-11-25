use std::str::FromStr;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};

#[cfg(not(feature = "datafusion"))]
use arrow::{array::*, datatypes::*};
use chrono::NaiveTime;
use chrono::{NaiveDate, NaiveDateTime};
#[cfg(feature = "datafusion")]
use datafusion::arrow::{array::*, datatypes::*};
use pg_interval::Interval as PgInterval;
use pgwire::api::results::{CopyEncoder, DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::CopyData;
use pgwire::messages::data::DataRow;
use pgwire::types::ToSqlText;
use postgres_types::{IsNull, ToSql, Type};
use rust_decimal::Decimal;
use timezone::Tz;

use crate::error::ToSqlError;
#[cfg(feature = "postgis")]
use crate::geo_encoder::encode_geo;
use crate::list_encoder::encode_list;
use crate::struct_encoder::encode_struct;

pub trait Encoder {
    type Item;

    fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized;

    fn take_row(&mut self) -> Self::Item;
}

impl Encoder for DataRowEncoder {
    type Item = DataRow;

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

    fn take_row(&mut self) -> Self::Item {
        self.take_row()
    }
}

impl Encoder for CopyEncoder {
    type Item = CopyData;

    fn encode_field<T>(&mut self, value: &T, _pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        self.encode_field(value)
    }

    fn take_row(&mut self) -> Self::Item {
        self.take_copy()
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

fn get_u64_as_decimal_value(arr: &Arc<dyn Array>, idx: usize) -> Option<Decimal> {
    get_u64_value(arr, idx).map(Decimal::from)
}
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

/// Encode a Postgres `oid` column (stored as Arrow `Int32` with `pg.oid_alias
/// = "oid"` metadata) whose result format is binary.
///
/// PostgreSQL's `oid` is unsigned 32-bit, and drivers (tokio-postgres, ...)
/// encode/decode it with `u32`. Arrow stores our catalog oids as `Int32`, so
/// for the binary protocol the value must be re-encoded as `u32`, otherwise the
/// `i32` ToSql impl rejects the OID type.
fn encode_pg_oid_binary<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if arr.is_null(idx) {
        return encoder.encode_field(&None::<u32>, pg_field);
    }
    let value = get_i32_value(arr, idx).unwrap_or(0) as u32;
    encoder.encode_field(&Some(value), pg_field)
}

/// Encode a Postgres internal `"char"` column (stored as a single-character
/// UTF-8 string tagged with the `pg.char` metadata).
///
/// Binary format is the raw byte of the character (decoded by clients as an
/// `i8`/`char`); text format is the character itself.
fn encode_pg_char<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if arr.is_null(idx) {
        return encoder.encode_field(&None::<String>, pg_field);
    }
    let text = get_utf8_value(arr, idx)
        .ok_or_else(|| PgWireError::ApiError(ToSqlError::from("pg.char column must be UTF-8")))?;
    if pg_field.format() == FieldFormat::Binary {
        let byte = text
            .as_bytes()
            .first()
            .copied()
            .ok_or_else(|| PgWireError::ApiError(ToSqlError::from("pg.char value is empty")))?
            as i8;
        encoder.encode_field(&Some(byte), pg_field)
    } else {
        encoder.encode_field(&Some(text.to_string()), pg_field)
    }
}

/// A UUID value encoded in PostgreSQL's text or binary format.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PgUuidValue(pub(crate) [u8; 16]);

impl PgUuidValue {
    fn text(self) -> String {
        let b = self.0;
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6],
            b[7],
            b[8],
            b[9],
            b[10],
            b[11],
            b[12],
            b[13],
            b[14],
            b[15]
        )
    }
}

impl ToSqlText for PgUuidValue {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_slice(self.text().as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PgUuidValue {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.oid(), 2950 | 2951)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(ty) {
            return Err("UUID value bound to a non-UUID result type".into());
        }
        self.to_sql(ty, out)
    }
}

fn encode_pg_uuid<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if arr.is_null(idx) {
        return encoder.encode_field(&None::<PgUuidValue>, pg_field);
    }
    let value = match arr.data_type() {
        DataType::FixedSizeBinary(16) => {
            let array = arr
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("FixedSizeBinary(16) array");
            let bytes = array.value(idx);
            let mut value = [0u8; 16];
            value.copy_from_slice(bytes);
            PgUuidValue(value)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let text = if let Some(array) = arr.as_any().downcast_ref::<StringArray>() {
                array.value(idx)
            } else if let Some(array) = arr.as_any().downcast_ref::<LargeStringArray>() {
                array.value(idx)
            } else {
                arr.as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("UTF-8 array")
                    .value(idx)
            };
            let uuid = uuid::Uuid::parse_str(text.trim()).map_err(|error| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "22P02".to_string(),
                    format!("invalid input syntax for type uuid: {text:?}: {error}"),
                )))
            })?;
            PgUuidValue(*uuid.as_bytes())
        }
        other => {
            return Err(PgWireError::ApiError(ToSqlError::from(format!(
                "UUID column must be UTF-8 or FixedSizeBinary(16), got {other}"
            ))));
        }
    };
    encoder.encode_field(&Some(value), pg_field)
}

/// A PostgreSQL money value in int64 cents.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PgMoneyValue(pub(crate) i64);

impl PgMoneyValue {
    fn text(self) -> String {
        let negative = self.0 < 0;
        let cents = self.0.unsigned_abs();
        let whole = cents / 100;
        let fraction = cents % 100;
        let mut whole = whole.to_string();
        let mut grouped = String::new();
        while whole.len() > 3 {
            let split = whole.len() - 3;
            grouped.insert_str(0, &format!(",{}", &whole[split..]));
            whole.truncate(split);
        }
        grouped.insert_str(0, &whole);
        format!(
            "{}${}.{:02}",
            if negative { "-" } else { "" },
            grouped,
            fraction
        )
    }
}

impl ToSqlText for PgMoneyValue {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_slice(self.text().as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PgMoneyValue {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_i64(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.oid(), 790 | 791)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(ty) {
            return Err("money value bound to a non-money result type".into());
        }
        self.to_sql(ty, out)
    }
}

pub(crate) fn money_cents(value: i128, scale: i8) -> PgWireResult<i64> {
    let cents = if scale == 2 {
        value
    } else if scale < 2 {
        value
            .checked_mul(10i128.pow((2 - scale) as u32))
            .ok_or_else(|| PgWireError::ApiError(ToSqlError::from("money value out of range")))?
    } else {
        value / 10i128.pow((scale - 2) as u32)
    };
    i64::try_from(cents)
        .map_err(|_| PgWireError::ApiError(ToSqlError::from("money value out of range")))
}

fn encode_pg_money<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if arr.is_null(idx) {
        return encoder.encode_field(&None::<PgMoneyValue>, pg_field);
    }
    let (value, scale) = match arr.data_type() {
        DataType::Decimal128(_, scale) => (
            arr.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(idx),
            *scale,
        ),
        other => {
            return Err(PgWireError::ApiError(ToSqlError::from(format!(
                "money column must be Decimal128(19,2), got {other}"
            ))));
        }
    };
    let cents = money_cents(value, scale)?;
    encoder.encode_field(&Some(PgMoneyValue(cents)), pg_field)
}

/// A Decimal256 value encoded as PostgreSQL numeric text/binary.
#[derive(Debug, Clone)]
pub(crate) struct PgNumericValue {
    pub(crate) text: String,
    pub(crate) scale: u32,
}

impl PgNumericValue {
    pub(crate) fn from_i256(value: i256, scale: i8) -> Self {
        let negative = value < i256::ZERO;
        let digits = if negative {
            (-value).to_string()
        } else {
            value.to_string()
        };
        Self {
            text: decimal_text(&digits, negative, scale),
            scale: scale.max(0) as u32,
        }
    }

    fn payload(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Sync + Send>> {
        let (negative, digits) = if let Some(rest) = self.text.strip_prefix('-') {
            (true, rest)
        } else {
            (false, self.text.as_str())
        };
        let mut parts = digits.split('.');
        let integer = parts.next().unwrap_or("0");
        let fraction = parts.next().unwrap_or("");
        let mut all = String::with_capacity(integer.len() + self.scale as usize);
        all.push_str(integer);
        all.push_str(fraction);
        while all.len() < self.scale as usize + 1 {
            all.insert(0, '0');
        }
        let integer_len = all.len() - self.scale as usize;
        let mut integer_digits = all[..integer_len].trim_start_matches('0').to_string();
        if integer_digits.is_empty() {
            integer_digits.push('0');
        }
        let mut fractional = all[integer_len..].to_string();
        while fractional.len() < self.scale as usize {
            fractional.push('0');
        }
        let mut groups: Vec<u16> = Vec::new();
        let mut int_work = integer_digits;
        while int_work.len() % 4 != 0 {
            int_work.insert(0, '0');
        }
        for chunk in int_work.as_bytes().chunks(4) {
            groups.push(std::str::from_utf8(chunk)?.parse::<u16>()?);
        }
        let weight = groups.len() as i32 - 1;
        let mut frac_work = fractional;
        while frac_work.len() % 4 != 0 {
            frac_work.push('0');
        }
        for chunk in frac_work.as_bytes().chunks(4) {
            groups.push(std::str::from_utf8(chunk)?.parse::<u16>()?);
        }
        // Preserve the requested display scale even for zero.
        let mut ndigits = groups.len();
        while ndigits > 0 && groups[ndigits - 1] == 0 && ndigits > (weight + 1).max(0) as usize {
            ndigits -= 1;
        }
        let mut out = BytesMut::with_capacity(8 + ndigits * 2);
        out.put_u16(ndigits as u16);
        out.put_i16(weight as i16);
        out.put_u16(if negative { 0x4000 } else { 0 });
        out.put_u16(self.scale as u16);
        for group in &groups[..ndigits] {
            out.put_u16(*group);
        }
        Ok(out.to_vec())
    }
}

impl ToSqlText for PgNumericValue {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_slice(self.text.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PgNumericValue {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_slice(&self.payload()?);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.oid(), 1700 | 1231)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !Self::accepts(ty) {
            return Err("numeric value bound to a non-numeric result type".into());
        }
        self.to_sql(ty, out)
    }
}

fn decimal_text(digits: &str, negative: bool, scale: i8) -> String {
    let mut text = if scale <= 0 {
        digits.to_string()
    } else {
        let scale = scale as usize;
        let mut digits = digits.to_string();
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        text.insert(0, '-');
    }
    text
}

fn encode_decimal256<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if arr.is_null(idx) {
        return encoder.encode_field(&None::<PgNumericValue>, pg_field);
    }
    let DataType::Decimal256(_, scale) = arr.data_type() else {
        unreachable!()
    };
    let value = arr
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap()
        .value(idx);
    let value = PgNumericValue::from_i256(value, *scale);
    encoder.encode_field(&Some(value), pg_field)
}

/// A pgvector `vector` value that pgwire serializes in the format the client
/// requested: binary (pgvector layout, big-endian `int16` dimension followed by
/// big-endian IEEE float32s) or text (`[1,2,3]`).
#[cfg(feature = "pgvector")]
#[derive(Debug)]
struct PgVectorValue(Vec<f32>);

#[cfg(feature = "pgvector")]
impl postgres_types::ToSql for PgVectorValue {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut as _;
        if !PgVectorValue::accepts(ty) {
            return Err("vector value bound to a non-vector result type".into());
        }
        // pgvector binary layout: big-endian u16 dimension, an unused u16 that
        // must be 0, then big-endian IEEE float32 elements.
        out.put_u16(self.0.len() as u16);
        out.put_u16(0);
        for v in &self.0 {
            out.put_f32(*v);
        }
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        ty.oid() == crate::datatypes::PG_VECTOR_TYPE_OID
    }

    fn to_sql_checked(
        &self,
        ty: &postgres_types::Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

#[cfg(feature = "pgvector")]
impl pgwire::types::ToSqlText for PgVectorValue {
    fn to_sql_text(
        &self,
        _ty: &postgres_types::Type,
        out: &mut BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use bytes::BufMut as _;
        // Element formatting uses Rust's shortest round-trip `Display` for
        // `f32` (`1.0` -> `1`), matching pgvector's `vector_out`.
        let inner = self
            .0
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",");
        out.put_slice(format!("[{inner}]").as_bytes());
        Ok(postgres_types::IsNull::No)
    }
}

/// Encode a pgvector `vector` column (an Arrow `List`/`FixedSizeList` of
/// `Float32` tagged with the `pg.vector` field metadata) for a single row.
///
/// pgwire picks the encoding from the result `FieldInfo` format: text produces
/// `[1,2,3]` (as psql shows), binary produces the pgvector wire layout that
/// typed drivers decode.
#[cfg(feature = "pgvector")]
fn encode_pg_vector<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if arr.is_null(idx) {
        return encoder.encode_field(&None::<PgVectorValue>, pg_field);
    }

    fn row_values(arr: &Arc<dyn Array>, idx: usize) -> PgWireResult<Vec<f32>> {
        let values = match arr.data_type() {
            DataType::FixedSizeList(_, _) => {
                let list = arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                let values = list
                    .values()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        PgWireError::ApiError(ToSqlError::from(
                            "vector FixedSizeList values must be Float32",
                        ))
                    })?;
                let size = list.value_length() as usize;
                // Respect the array's own offset: a sliced FixedSizeListArray
                // (offset > 0, e.g. a batch sliced by LIMIT) starts its rows
                // further into the flattened values.
                let start = list.value_offset(idx) as usize;
                (0..size).map(|i| values.value(start + i)).collect()
            }
            DataType::List(_) => {
                let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
                let value = list.value(idx);
                let values = value
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        PgWireError::ApiError(ToSqlError::from(
                            "vector List values must be Float32",
                        ))
                    })?;
                values.values().to_vec()
            }
            other => {
                return Err(PgWireError::ApiError(ToSqlError::from(format!(
                    "vector column has unsupported arrow type {other}"
                ))));
            }
        };
        Ok(values)
    }

    encoder.encode_field(&Some(PgVectorValue(row_values(arr, idx)?)), pg_field)
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

fn get_time32_second_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap()
        .value_as_time(idx)
}

fn get_time32_millisecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap()
        .value_as_time(idx)
}

fn get_time64_microsecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap()
        .value_as_time(idx)
}
fn get_time64_nanosecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap()
        .value_as_time(idx)
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

/// A JSON value in its Postgres wire representation.
///
/// The text format is the raw JSON document. The binary format is also the raw
/// document for `json`, while `jsonb` prefixes it with the one-byte format
/// version PostgreSQL uses (currently version 1).
#[derive(Debug)]
struct JsonText<'a> {
    text: &'a str,
    jsonb: bool,
}

impl ToSqlText for JsonText<'_> {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_slice(self.text.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for JsonText<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if self.jsonb {
            out.put_u8(1);
        }
        out.put_slice(self.text.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB)
    }

    postgres_types::to_sql_checked!();
}

pub fn encode_value<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    arrow_field: &Field,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    let arrow_type = arrow_field.data_type();

    #[cfg(feature = "postgis")]
    if let Some(geoarrow_type) = geoarrow_schema::GeoArrowType::from_extension_field(arrow_field)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?
    {
        let geoarrow_array: Arc<dyn geoarrow::array::GeoArrowArray> =
            geoarrow::array::from_arrow_array(arr, arrow_field)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        return encode_geo(
            encoder,
            geoarrow_type,
            &geoarrow_array,
            idx,
            arrow_field,
            pg_field,
        );
    }

    // pgvector `vector` columns are tagged with the `pg.vector` field metadata.
    // Route them through the vector encoder (text `[1,2,3]` / pgvector binary)
    // before the generic list handling below, which would otherwise emit them
    // as a Postgres float4[] (`{1,2,3}`). The resolved FieldInfo wire type is
    // the authoritative signal: some optimizer rewrites drop the arrow field
    // metadata while keeping the logical vector type.
    #[cfg(feature = "pgvector")]
    if crate::datatypes::is_pg_vector_field(arrow_field)
        || pg_field.datatype().oid() == crate::datatypes::PG_VECTOR_TYPE_OID
    {
        return encode_pg_vector(encoder, arr, idx, pg_field);
    }

    // Postgres internal `"char"` columns (pg.typtype, ...): wire type CHAR with
    // char-specific binary/text encoding.
    if crate::datatypes::is_pg_char_field(arrow_field) {
        return encode_pg_char(encoder, arr, idx, pg_field);
    }

    // Arrow canonical UUID extension. The storage is FixedSizeBinary(16),
    // but the wire type is UUID and the text format is the canonical
    // hyphenated form (not bytea's `\x...`).
    if pg_field.datatype() == &Type::UUID {
        return encode_pg_uuid(encoder, arr, idx, pg_field);
    }

    // PostgreSQL money is an int64 count of cents on the wire; LakeSoul stores
    // it as an Arrow decimal with scale 2 and a `pg.type=money` marker.
    if pg_field.datatype() == &Type::MONEY {
        return encode_pg_money(encoder, arr, idx, pg_field);
    }

    // Decimal256 has no rust_decimal representation. Encode it as a
    // PostgreSQL numeric payload (binary) or its decimal text (text).
    if matches!(arrow_type, DataType::Decimal256(_, _)) && pg_field.datatype() == &Type::NUMERIC {
        return encode_decimal256(encoder, arr, idx, pg_field);
    }

    // Postgres `oid` columns are `u32` on the wire but stored as Arrow Int32.
    // Over the binary protocol the value must be sent as `u32` (the `i32` ToSql
    // impl does not accept the OID type); text keeps the existing Int32 path.
    if pg_field.format() == FieldFormat::Binary
        && matches!(arrow_type, DataType::Int32)
        && arrow_field
            .metadata()
            .get(crate::datatypes::PG_OID_ALIAS_KEY)
            .is_some_and(|kind| kind == "oid")
    {
        return encode_pg_oid_binary(encoder, arr, idx, pg_field);
    }

    // Postgres `json` / `jsonb` columns: the storage is UTF-8 JSON text with
    // the Arrow canonical JSON extension. `jsonb` adds the one-byte binary
    // format version; `json` (and the text format of both) is the raw
    // document. The resolved FieldInfo type is authoritative so an optimizer
    // rewrite that drops the field metadata still encodes the right format.
    let jsonb = match *pg_field.datatype() {
        Type::JSON => Some(false),
        Type::JSONB => Some(true),
        _ => crate::datatypes::pg_json_type(arrow_field).map(|ty| ty == Type::JSONB),
    };
    if let Some(jsonb) = jsonb {
        let value = match arrow_type {
            DataType::Utf8 => get_utf8_value(arr, idx),
            DataType::Utf8View => get_utf8_view_value(arr, idx),
            DataType::LargeUtf8 => get_large_utf8_value(arr, idx),
            other => {
                return Err(PgWireError::ApiError(ToSqlError::from(format!(
                    "json column {other} is not UTF-8"
                ))));
            }
        };
        let value = value.map(|text| JsonText { text, jsonb });
        encoder.encode_field(&value, pg_field)?;
        return Ok(());
    }

    match arrow_type {
        DataType::Null => encoder.encode_field(&None::<i8>, pg_field)?,
        DataType::Boolean => encoder.encode_field(&get_bool_value(arr, idx), pg_field)?,
        DataType::Int8 => encoder.encode_field(&get_i8_value(arr, idx), pg_field)?,
        DataType::Int16 => encoder.encode_field(&get_i16_value(arr, idx), pg_field)?,
        DataType::Int32 => encoder.encode_field(&get_i32_value(arr, idx), pg_field)?,
        DataType::Int64 => encoder.encode_field(&get_i64_value(arr, idx), pg_field)?,
        DataType::UInt8 => {
            encoder.encode_field(&(get_u8_value(arr, idx).map(|x| x as i16)), pg_field)?
        }
        DataType::UInt16 => {
            encoder.encode_field(&(get_u16_value(arr, idx).map(|x| x as i32)), pg_field)?
        }
        DataType::UInt32 => {
            encoder.encode_field(&get_u32_value(arr, idx).map(|x| x as i64), pg_field)?
        }
        DataType::UInt64 => encoder.encode_field(&get_u64_as_decimal_value(arr, idx), pg_field)?,
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
        DataType::Interval(interval_unit) => match interval_unit {
            IntervalUnit::YearMonth => {
                let interval_array = arr
                    .as_any()
                    .downcast_ref::<IntervalYearMonthArray>()
                    .unwrap();
                let months = IntervalYearMonthType::to_months(interval_array.value(idx));
                encoder.encode_field(&PgInterval::new(months, 0, 0), pg_field)?;
            }
            IntervalUnit::DayTime => {
                let interval_array = arr.as_any().downcast_ref::<IntervalDayTimeArray>().unwrap();
                let (days, millis) = IntervalDayTimeType::to_parts(interval_array.value(idx));
                encoder
                    .encode_field(&PgInterval::new(0, days, millis as i64 * 1000i64), pg_field)?;
            }
            IntervalUnit::MonthDayNano => {
                let interval_array = arr
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .unwrap();
                let (months, days, nanoseconds) =
                    IntervalMonthDayNanoType::to_parts(interval_array.value(idx));

                encoder.encode_field(
                    &PgInterval::new(months, days, nanoseconds / 1000i64),
                    pg_field,
                )?;
            }
        },
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<PgInterval>, pg_field);
                }
                let duration_array = arr.as_any().downcast_ref::<DurationSecondArray>().unwrap();
                let microseconds = duration_array.value(idx) * 1_000_000i64;
                encoder.encode_field(&PgInterval::new(0, 0, microseconds), pg_field)?;
            }
            TimeUnit::Millisecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<PgInterval>, pg_field);
                }
                let duration_array = arr
                    .as_any()
                    .downcast_ref::<DurationMillisecondArray>()
                    .unwrap();
                let microseconds = duration_array.value(idx) * 1_000i64;
                encoder.encode_field(&PgInterval::new(0, 0, microseconds), pg_field)?;
            }
            TimeUnit::Microsecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<PgInterval>, pg_field);
                }
                let duration_array = arr
                    .as_any()
                    .downcast_ref::<DurationMicrosecondArray>()
                    .unwrap();
                let microseconds = duration_array.value(idx);
                encoder.encode_field(&PgInterval::new(0, 0, microseconds), pg_field)?;
            }
            TimeUnit::Nanosecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<PgInterval>, pg_field);
                }
                let duration_array = arr
                    .as_any()
                    .downcast_ref::<DurationNanosecondArray>()
                    .unwrap();
                let microseconds = duration_array.value(idx) / 1_000i64;
                encoder.encode_field(&PgInterval::new(0, 0, microseconds), pg_field)?;
            }
        },
        DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_) => {
            if arr.is_null(idx) {
                return encoder.encode_field(&None::<&[i8]>, pg_field);
            }
            // Extract this row's element slice from the actual list flavour.
            // (FixedSizeList / LargeList were previously downcast to ListArray,
            // which panics -- these are not ListArrays.)
            let array = match arrow_type {
                DataType::FixedSizeList(_, _) => arr
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(idx),
                DataType::LargeList(_) => arr
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .unwrap()
                    .value(idx),
                _ => arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx),
            };
            encode_list(encoder, array, pg_field)?
        }
        DataType::Struct(arrow_fields) => encode_struct(encoder, arr, idx, arrow_fields, pg_field)?,
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
                arr
            ))));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::buffer::NullBuffer;
    use bytes::BytesMut;
    use pgwire::{api::results::FieldFormat, types::format::FormatOptions};
    use postgres_types::Type;

    use super::*;

    #[test]
    fn encodes_dictionary_array() {
        #[derive(Default)]
        struct MockEncoder {
            encoded_value: String,
        }

        impl Encoder for MockEncoder {
            type Item = String;

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

            fn take_row(&mut self) -> Self::Item {
                std::mem::take(&mut self.encoded_value)
            }
        }

        let val = "~!@&$[]()@@!!";
        let value = StringArray::from_iter_values([val]);
        let keys = Int8Array::from_iter_values([0, 0, 0, 0]);
        let dict_arr: Arc<dyn Array> =
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::new(value)).unwrap());

        let mut encoder = MockEncoder::default();

        let arrow_field = Field::new(
            "x",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        let pg_field = FieldInfo::new("x".to_string(), None, None, Type::TEXT, FieldFormat::Text);
        let result = encode_value(&mut encoder, &dict_arr, 2, &arrow_field, &pg_field);

        assert!(result.is_ok());

        assert!(encoder.encoded_value == val);
    }

    #[test]
    fn encode_struct_null_emits_field() {
        // Regression test: encode_struct must call encoder.encode_field for
        // NULL struct values so a NULL indicator is written to the DataRow.
        // Previously it returned Ok(()) without encoding, corrupting the
        // column count.

        #[derive(Default)]
        struct CountingEncoder {
            call_count: usize,
        }

        impl Encoder for CountingEncoder {
            type Item = ();

            fn encode_field<T>(&mut self, _value: &T, _pg_field: &FieldInfo) -> PgWireResult<()>
            where
                T: ToSql + ToSqlText + Sized,
            {
                self.call_count += 1;
                Ok(())
            }

            fn take_row(&mut self) -> Self::Item {}
        }

        let fields = vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ];
        let a = Arc::new(StringArray::from(vec![Some("hello"), Some("x")])) as Arc<dyn Array>;
        let b = Arc::new(StringArray::from(vec![Some("world"), Some("y")])) as Arc<dyn Array>;

        // Row 0: non-null struct, Row 1: null struct
        let null_buffer = NullBuffer::from(vec![true, false]);
        let struct_arr: Arc<dyn Array> = Arc::new(
            StructArray::try_new(fields.clone().into(), vec![a, b], Some(null_buffer)).unwrap(),
        );

        let arrow_field = Field::new("s", DataType::Struct(fields.into()), true);
        let pg_field = FieldInfo::new("s".to_string(), None, None, Type::TEXT, FieldFormat::Text);

        // Encode the NULL row (index 1).
        let mut encoder = CountingEncoder::default();
        let result = encode_value(&mut encoder, &struct_arr, 1, &arrow_field, &pg_field);
        assert!(result.is_ok());
        assert_eq!(
            encoder.call_count, 1,
            "encode_field must be called exactly once for a NULL struct to emit a NULL indicator"
        );
    }

    #[test]
    fn encodes_null_list_as_text_array() {
        // Regression: `ARRAY[NULL]` (a List whose element type is Null) must
        // be encoded as a `text[]` value `{NULL}`, aligning with postgres
        // rather than emitting a SQL NULL.
        #[derive(Default)]
        struct TextEncoder {
            encoded_value: String,
        }

        impl Encoder for TextEncoder {
            type Item = String;

            fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
            where
                T: ToSql + ToSqlText + Sized,
            {
                let mut bytes = BytesMut::new();
                value
                    .to_sql_text(pg_field.datatype(), &mut bytes, &FormatOptions::default())
                    .unwrap();
                self.encoded_value = String::from_utf8(bytes.to_vec()).unwrap();
                Ok(())
            }

            fn take_row(&mut self) -> Self::Item {
                std::mem::take(&mut self.encoded_value)
            }
        }

        // Build a single-row ListArray whose element type is Null, mirroring
        // DataFusion's `array[null]` output.
        let list_field = Arc::new(Field::new_list_field(DataType::Null, true));
        let offsets = arrow::buffer::OffsetBuffer::<i32>::from_lengths([1]);
        let values = Arc::new(NullArray::new(1)) as Arc<dyn Array>;
        let list_arr: Arc<dyn Array> =
            Arc::new(ListArray::new(list_field.clone(), offsets, values, None));

        let arrow_field = Field::new("c", DataType::List(list_field), true);
        let pg_field = FieldInfo::new(
            "c".to_string(),
            None,
            None,
            Type::TEXT_ARRAY,
            FieldFormat::Text,
        );

        let mut encoder = TextEncoder::default();
        let result = encode_value(&mut encoder, &list_arr, 0, &arrow_field, &pg_field);
        assert!(result.is_ok());
        assert_eq!(encoder.encoded_value, "{NULL}");
    }

    #[test]
    fn test_get_time32_second_value() {
        let array = Time32SecondArray::from_iter_values([3723_i32]);
        let array: Arc<dyn Array> = Arc::new(array);
        let value = get_time32_second_value(&array, 0);
        assert_eq!(value, Some(NaiveTime::from_hms_opt(1, 2, 3)).unwrap());
    }

    #[test]
    fn test_get_time32_millisecond_value() {
        let array = Time32MillisecondArray::from_iter_values([3723001_i32]);
        let array: Arc<dyn Array> = Arc::new(array);
        let value = get_time32_millisecond_value(&array, 0);
        assert_eq!(
            value,
            Some(NaiveTime::from_hms_milli_opt(1, 2, 3, 1)).unwrap()
        );
    }

    #[test]
    fn test_get_time64_microsecond_value() {
        let array = Time64MicrosecondArray::from_iter_values([3723001001_i64]);
        let array: Arc<dyn Array> = Arc::new(array);
        let value = get_time64_microsecond_value(&array, 0);
        assert_eq!(
            value,
            Some(NaiveTime::from_hms_micro_opt(1, 2, 3, 1001)).unwrap()
        );
    }

    #[test]
    fn test_get_time64_nanosecond_value() {
        let array = Time64NanosecondArray::from_iter_values([3723001001001_i64]);
        let array: Arc<dyn Array> = Arc::new(array);
        let value = get_time64_nanosecond_value(&array, 0);
        assert_eq!(
            value,
            Some(NaiveTime::from_hms_nano_opt(1, 2, 3, 1001001)).unwrap()
        );
    }

    #[cfg(feature = "pgvector")]
    mod vector {
        use super::*;
        use arrow::buffer::NullBuffer;
        use bytes::BytesMut;
        use pgwire::{api::results::FieldFormat, types::format::FormatOptions};
        use postgres_types::Type;
        use std::collections::HashMap;

        #[derive(Default)]
        struct TextCapture {
            encoded: String,
        }

        impl Encoder for TextCapture {
            type Item = String;

            fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
            where
                T: ToSql + ToSqlText + Sized,
            {
                let mut bytes = BytesMut::new();
                value
                    .to_sql_text(pg_field.datatype(), &mut bytes, &FormatOptions::default())
                    .unwrap();
                self.encoded = String::from_utf8(bytes.to_vec()).unwrap();
                Ok(())
            }

            fn take_row(&mut self) -> Self::Item {
                std::mem::take(&mut self.encoded)
            }
        }

        fn vector_arrow_field(metadata: bool) -> Field {
            let mut field = Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    3,
                ),
                true,
            );
            if metadata {
                field = field.with_metadata(HashMap::from([(
                    crate::datatypes::PG_VECTOR_KEY.to_string(),
                    "vector".to_string(),
                )]));
            }
            field
        }

        #[test]
        fn encodes_vector_fixed_size_list_as_pgvector_text() {
            // Two rows: [1,2,3] and [4.5,0,7]
            let values = Float32Array::from(vec![1.0, 2.0, 3.0, 4.5, 0.0, 7.0]);
            let array: Arc<dyn Array> = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    3,
                    Arc::new(values),
                    None,
                )
                .unwrap(),
            );

            let arrow_field = vector_arrow_field(true);
            let pg_field = FieldInfo::new(
                "embedding".to_string(),
                None,
                None,
                crate::datatypes::pg_vector_type(),
                FieldFormat::Text,
            );

            let mut encoder = TextCapture::default();
            encode_value(&mut encoder, &array, 0, &arrow_field, &pg_field).unwrap();
            assert_eq!(encoder.encoded, "[1,2,3]");

            let mut encoder = TextCapture::default();
            encode_value(&mut encoder, &array, 1, &arrow_field, &pg_field).unwrap();
            assert_eq!(encoder.encoded, "[4.5,0,7]");
        }

        #[test]
        fn encodes_sliced_fixed_size_list_vector() {
            // A sliced FixedSizeListArray has a non-zero offset; row indexing
            // must account for it instead of using `idx * size`.
            let values = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]);
            let array: Arc<dyn Array> = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    3,
                    Arc::new(values),
                    None,
                )
                .unwrap(),
            );
            let sliced = array.slice(1, 2); // rows [4,5,6] and [7,8,9]

            let arrow_field = vector_arrow_field(true);
            let pg_field = FieldInfo::new(
                "embedding".to_string(),
                None,
                None,
                crate::datatypes::pg_vector_type(),
                FieldFormat::Text,
            );

            let mut encoder = TextCapture::default();
            encode_value(&mut encoder, &sliced, 0, &arrow_field, &pg_field).unwrap();
            assert_eq!(encoder.encoded, "[4,5,6]");
        }

        #[derive(Default)]
        struct BinaryCapture {
            encoded: Vec<u8>,
        }

        impl Encoder for BinaryCapture {
            type Item = Vec<u8>;

            fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
            where
                T: ToSql + ToSqlText + Sized,
            {
                use postgres_types::ToSql as _;
                let mut bytes = BytesMut::new();
                value.to_sql(pg_field.datatype(), &mut bytes).unwrap();
                self.encoded = bytes.to_vec();
                Ok(())
            }

            fn take_row(&mut self) -> Self::Item {
                std::mem::take(&mut self.encoded)
            }
        }

        #[test]
        fn encodes_vector_fixed_size_list_as_pgvector_binary() {
            // pgvector binary layout: big-endian u16 dimension, an unused u16
            // (0), then big-endian IEEE float32s. [1,2,3] ->
            // 00 03 | 00 00 | 3f800000 40000000 40400000.
            let values = Float32Array::from(vec![1.0, 2.0, 3.0]);
            let array: Arc<dyn Array> = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    3,
                    Arc::new(values),
                    None,
                )
                .unwrap(),
            );

            let arrow_field = vector_arrow_field(true);
            let pg_field = FieldInfo::new(
                "embedding".to_string(),
                None,
                None,
                crate::datatypes::pg_vector_type(),
                FieldFormat::Binary,
            );

            let mut encoder = BinaryCapture::default();
            encode_value(&mut encoder, &array, 0, &arrow_field, &pg_field).unwrap();
            assert_eq!(
                encoder.encoded,
                vec![
                    0x00, 0x03, // dim = 3
                    0x00, 0x00, // unused
                    0x3f, 0x80, 0x00, 0x00, // 1.0
                    0x40, 0x00, 0x00, 0x00, // 2.0
                    0x40, 0x40, 0x00, 0x00, // 3.0
                ]
            );
        }

        #[test]
        fn encodes_null_vector_as_null() {
            // `NullBuffer::from(Vec<bool>)` treats `true` as valid.
            let nulls = NullBuffer::from(vec![false, true]);
            let values = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
            let array: Arc<dyn Array> = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    3,
                    Arc::new(values),
                    Some(nulls),
                )
                .unwrap(),
            );

            let arrow_field = vector_arrow_field(true);
            let pg_field = FieldInfo::new(
                "embedding".to_string(),
                None,
                None,
                crate::datatypes::pg_vector_type(),
                FieldFormat::Text,
            );

            // Row 0 is NULL: no value bytes.
            let mut encoder = TextCapture::default();
            encode_value(&mut encoder, &array, 0, &arrow_field, &pg_field).unwrap();
            assert!(encoder.encoded.is_empty(), "NULL must emit no bytes");

            // Row 1 is a valid vector.
            let mut encoder = TextCapture::default();
            encode_value(&mut encoder, &array, 1, &arrow_field, &pg_field).unwrap();
            assert_eq!(encoder.encoded, "[4,5,6]");
        }

        #[test]
        fn plain_fixed_size_list_encodes_as_float4_array() {
            // Regression: FixedSizeList columns used to be downcast to
            // ListArray and panic. A non-pgvector fixed-size float list must
            // still encode (as a Postgres array).
            let values = Float32Array::from(vec![1.0, 2.0, 3.0]);
            let array: Arc<dyn Array> = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    3,
                    Arc::new(values),
                    None,
                )
                .unwrap(),
            );

            let arrow_field = vector_arrow_field(false);
            let pg_field = FieldInfo::new(
                "embedding".to_string(),
                None,
                None,
                Type::FLOAT4_ARRAY,
                FieldFormat::Text,
            );

            let mut encoder = TextCapture::default();
            encode_value(&mut encoder, &array, 0, &arrow_field, &pg_field).unwrap();
            assert_eq!(encoder.encoded, "{1.0,2.0,3.0}");
        }
    }
}
