use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::datatypes::*;
#[cfg(feature = "datafusion")]
use datafusion::arrow::datatypes::*;
use geo_postgis::ToPostgis;
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::{AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::GeoArrowType;
use pgwire::api::results::FieldInfo;
use pgwire::error::{PgWireError, PgWireResult};

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
    encoder: &mut T,
    array: &geoarrow::array::PointArray,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if array.is_null(idx) {
        return encoder.encode_field(&None::<postgis::ewkb::Point>, pg_field);
    }

    let point = array
        .value(idx)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let geo_point = point.to_point();
    let ewkb_point = geo_point.to_postgis_with_srid(None);

    encoder.encode_field(&ewkb_point, pg_field)
}
