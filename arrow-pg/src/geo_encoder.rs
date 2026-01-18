use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::datatypes::*;
#[cfg(feature = "datafusion")]
use datafusion::arrow::datatypes::*;
use geo_postgis::ToPostgis;
use geoarrow::array::{AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::GeoArrowType;
use pgwire::api::results::FieldInfo;
use pgwire::error::{PgWireError, PgWireResult};

use crate::encoder::Encoder;

use geo_traits::to_geo::{
    ToGeoGeometryCollection, ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint,
    ToGeoMultiPolygon, ToGeoPoint, ToGeoPolygon, ToGeoRect,
};

macro_rules! encode_geo_fn {
    (
        $name:ident,
        $array_type:ty,
        $postgis_type:ty,
        $geoarrow_value_type:ty,
        $to_geo_fn:ident
    ) => {
        fn $name<T: Encoder>(
            encoder: &mut T,
            array: &$array_type,
            idx: usize,
            pg_field: &FieldInfo,
        ) -> PgWireResult<()> {
            if array.is_null(idx) {
                return encoder.encode_field(&None::<$postgis_type>, pg_field);
            }

            let value: $geoarrow_value_type = array
                .value(idx)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let ewkb_value = value.$to_geo_fn().to_postgis_with_srid(None);

            encoder.encode_field(&ewkb_value, pg_field)
        }
    };
}

encode_geo_fn!(
    encode_point,
    geoarrow::array::PointArray,
    postgis::ewkb::Point,
    geoarrow::array::Point<'_>,
    to_point
);

encode_geo_fn!(
    encode_linestring,
    geoarrow::array::LineStringArray,
    postgis::ewkb::LineString,
    geoarrow::array::LineString<'_>,
    to_line_string
);

encode_geo_fn!(
    encode_polygon,
    geoarrow::array::PolygonArray,
    postgis::ewkb::Polygon,
    geoarrow::array::Polygon<'_>,
    to_polygon
);

encode_geo_fn!(
    encode_multipoint,
    geoarrow::array::MultiPointArray,
    postgis::ewkb::MultiPoint,
    geoarrow::array::MultiPoint<'_>,
    to_multi_point
);

encode_geo_fn!(
    encode_multilinestring,
    geoarrow::array::MultiLineStringArray,
    postgis::ewkb::MultiLineString,
    geoarrow::array::MultiLineString<'_>,
    to_multi_line_string
);

encode_geo_fn!(
    encode_multipolygon,
    geoarrow::array::MultiPolygonArray,
    postgis::ewkb::MultiPolygon,
    geoarrow::array::MultiPolygon<'_>,
    to_multi_polygon
);

encode_geo_fn!(
    encode_geometrycollection,
    geoarrow::array::GeometryCollectionArray,
    postgis::ewkb::GeometryCollection,
    geoarrow::array::GeometryCollection<'_>,
    to_geometry_collection
);

fn encode_rect<T: Encoder>(
    encoder: &mut T,
    array: &geoarrow::array::RectArray,
    idx: usize,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    if array.is_null(idx) {
        return encoder.encode_field(&None::<postgis::ewkb::Polygon>, pg_field);
    }

    let rect: geoarrow::array::Rect<'_> = array
        .value(idx)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let geo_rect = rect.to_rect();
    let ewkb_polygon = geo_rect.to_polygon().to_postgis_with_srid(None);

    encoder.encode_field(&ewkb_polygon, pg_field)
}

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
            encode_point(encoder, array, idx, pg_field)
        }
        geoarrow_schema::GeoArrowType::LineString(_) => {
            let array: &geoarrow::array::LineStringArray = arr.as_line_string();
            encode_linestring(encoder, array, idx, pg_field)
        }
        geoarrow_schema::GeoArrowType::Polygon(_) => {
            let array: &geoarrow::array::PolygonArray = arr.as_polygon();
            encode_polygon(encoder, array, idx, pg_field)
        }
        geoarrow_schema::GeoArrowType::MultiPoint(_) => {
            let array: &geoarrow::array::MultiPointArray = arr.as_multi_point();
            encode_multipoint(encoder, array, idx, pg_field)
        }
        geoarrow_schema::GeoArrowType::MultiLineString(_) => {
            let array: &geoarrow::array::MultiLineStringArray = arr.as_multi_line_string();
            encode_multilinestring(encoder, array, idx, pg_field)
        }
        geoarrow_schema::GeoArrowType::MultiPolygon(_) => {
            let array: &geoarrow::array::MultiPolygonArray = arr.as_multi_polygon();
            encode_multipolygon(encoder, array, idx, pg_field)
        }
        geoarrow_schema::GeoArrowType::GeometryCollection(_) => {
            let array: &geoarrow::array::GeometryCollectionArray = arr.as_geometry_collection();
            encode_geometrycollection(encoder, array, idx, pg_field)
        }
        geoarrow_schema::GeoArrowType::Rect(_) => {
            let array: &geoarrow::array::RectArray = arr.as_rect();
            encode_rect(encoder, array, idx, pg_field)
        }
        geo_type => Err(PgWireError::ApiError(
            format!("Unsupported GeoArrowType {:?}", geo_type).into(),
        )),
    }
}
