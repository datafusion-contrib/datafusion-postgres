use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::datatypes::*;
#[cfg(feature = "datafusion")]
use datafusion::arrow::datatypes::*;
use geoarrow::array::AsGeoArrowArray;
use geoarrow_schema::GeoArrowType;
use pgwire::api::results::FieldInfo;
use pgwire::error::PgWireResult;

use crate::encoder::Encoder;

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
    _encoder: &mut T,
    _array: &geoarrow::array::PointArray,
    _idx: usize,
    _pg_field: &FieldInfo,
) -> PgWireResult<()> {
    todo!()
}
