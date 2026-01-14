use bytes::BytesMut;
use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::datatypes::*;
#[cfg(feature = "datafusion")]
use datafusion::arrow::datatypes::*;
use geoarrow::array::{AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::GeoArrowType;
use pgwire::api::results::FieldInfo;
use pgwire::error::{PgWireError, PgWireResult};
use wkb::{writer::WriteOptions, Endianness};

use crate::encoder::{EncodedValue, Encoder};

pub fn encode_geo<T: Encoder>(
    encoder: &mut T,
    geoarrow_type: GeoArrowType,
    arr: &Arc<dyn geoarrow::array::GeoArrowArray>,
    idx: usize,
    _arrow_field: &Field,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    match geoarrow_type {
        geoarrow_schema::GeoArrowType::Point(_) => {
            let array: &geoarrow::array::PointArray = arr.as_point();
            encode_point(encoder, array, idx, pg_field)?;
        }
        _ => todo!("handle other geometry types"),
    }
    Ok(())
}

fn encode_point<T: Encoder>(
    encoder: &mut T,
    array: &geoarrow::array::PointArray,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if array.is_null(idx) {
        encoder.encode_field(&None::<EncodedValue>, pg_field)?;
        return Ok(());
    }

    let point = array
        .value(idx)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let mut bytes = Vec::new();
    let options = WriteOptions {
        endianness: Endianness::LittleEndian,
    };
    wkb::writer::write_point(&mut bytes, &point, &options)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let encoded_value = EncodedValue {
        bytes: BytesMut::from(&bytes[..]),
    };
    encoder.encode_field(&encoded_value, pg_field)
}
