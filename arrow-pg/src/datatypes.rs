use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::{datatypes::*, record_batch::RecordBatch};
#[cfg(feature = "postgis")]
use arrow_schema::extension::ExtensionType;
#[cfg(feature = "datafusion")]
use datafusion::arrow::{datatypes::*, record_batch::RecordBatch};

use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::types::format::FormatOptions;
use postgres_types::Kind;

use crate::row_encoder::RowEncoder;

#[cfg(feature = "datafusion")]
pub mod df;

pub fn into_pg_type(arrow_type: &DataType) -> PgWireResult<Type> {
    let datatype = match arrow_type {
        DataType::Null => Type::UNKNOWN,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::INT2,
        DataType::Int16 | DataType::UInt8 => Type::INT2,
        DataType::Int32 | DataType::UInt16 => Type::INT4,
        DataType::Int64 | DataType::UInt32 => Type::INT8,
        DataType::UInt64 => Type::NUMERIC,
        DataType::Timestamp(_, tz) => {
            if tz.is_some() {
                Type::TIMESTAMPTZ
            } else {
                Type::TIMESTAMP
            }
        }
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Interval(_) | DataType::Duration(_) => Type::INTERVAL,
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => Type::BYTEA,
        DataType::Float16 | DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::TEXT,
        DataType::List(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => match field.data_type() {
            // Align with PostgreSQL: an array literal without type
            // information such as `ARRAY[NULL]` has a `Null` element type and
            // is reported as `text[]` by postgres.
            DataType::Null => Type::TEXT_ARRAY,
            DataType::Boolean => Type::BOOL_ARRAY,
            DataType::Int8 => Type::INT2_ARRAY,
            DataType::Int16 | DataType::UInt8 => Type::INT2_ARRAY,
            DataType::Int32 | DataType::UInt16 => Type::INT4_ARRAY,
            DataType::Int64 | DataType::UInt32 => Type::INT8_ARRAY,
            DataType::UInt64 | DataType::Decimal128(_, _) => Type::NUMERIC_ARRAY,
            DataType::Timestamp(_, tz) => {
                if tz.is_some() {
                    Type::TIMESTAMPTZ_ARRAY
                } else {
                    Type::TIMESTAMP_ARRAY
                }
            }
            DataType::Time32(_) | DataType::Time64(_) => Type::TIME_ARRAY,
            DataType::Date32 | DataType::Date64 => Type::DATE_ARRAY,
            DataType::Interval(_) | DataType::Duration(_) => Type::INTERVAL_ARRAY,
            DataType::FixedSizeBinary(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView => Type::BYTEA_ARRAY,
            DataType::Float16 | DataType::Float32 => Type::FLOAT4_ARRAY,
            DataType::Float64 => Type::FLOAT8_ARRAY,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::TEXT_ARRAY,
            DataType::Struct(_) => Type::new(
                Type::RECORD_ARRAY.name().into(),
                Type::RECORD_ARRAY.oid(),
                Kind::Array(field_into_pg_type(field)?),
                Type::RECORD_ARRAY.schema().into(),
            ),
            list_type => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Unsupported List Datatype {list_type}"),
                ))));
            }
        },
        DataType::Dictionary(_, value_type) => into_pg_type(value_type.as_ref())?,
        DataType::Struct(fields) => {
            let name: String = fields
                .iter()
                .map(|x| x.name().clone())
                .reduce(|a, b| a + ", " + &b)
                .map(|x| format!("({x})"))
                .unwrap_or("()".to_string());
            let kind = Kind::Composite(
                fields
                    .iter()
                    .map(|x| {
                        field_into_pg_type(x)
                            .map(|_type| postgres_types::Field::new(x.name().clone(), _type))
                    })
                    .collect::<Result<Vec<_>, PgWireError>>()?,
            );
            Type::new(name, Type::RECORD.oid(), kind, Type::RECORD.schema().into())
        }
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {arrow_type}"),
            ))));
        }
    };

    Ok(datatype)
}

/// Field metadata key marking an `Int32` Arrow field as a Postgres oid /
/// oid-alias column.
///
/// This is a **cross-crate contract**: the lower-level `arrow-pg` crate does
/// not depend on `datafusion-pg-catalog`, which owns the canonical
/// `OID_ALIAS_KEY` definition and the list of recognized alias names. The
/// value names the alias kind (`oid`, `regclass`, `regtype`, ...) in
/// lowercase and is matched case-sensitively. Unknown or missing metadata
/// falls back to the ordinary physical Arrow type mapping (`Int32` -> `INT4`),
/// so untagged columns are unaffected.
pub const PG_OID_ALIAS_KEY: &str = "pg.oid_alias";

/// Map a field's [`PG_OID_ALIAS_KEY`] metadata, when present and recognized, to
/// the matching Postgres alias [`Type`] (e.g. `regtype` -> OID 2206).
///
/// Returns `None` when the metadata is absent or names an unrecognized alias,
/// so the caller can fall back to the physical-type mapping (`Int32` ->
/// `INT4`).
fn pg_alias_type(field: &Field) -> Option<Type> {
    let kind = field.metadata().get(PG_OID_ALIAS_KEY)?;
    Some(match kind.as_str() {
        "oid" => Type::OID,
        "regproc" => Type::REGPROC,
        "regprocedure" => Type::REGPROCEDURE,
        "regoper" => Type::REGOPER,
        "regoperator" => Type::REGOPERATOR,
        "regclass" => Type::REGCLASS,
        "regtype" => Type::REGTYPE,
        "regnamespace" => Type::REGNAMESPACE,
        "regrole" => Type::REGROLE,
        "regconfig" => Type::REGCONFIG,
        "regdictionary" => Type::REGDICTIONARY,
        "regcollation" => Type::REGCOLLATION,
        _ => return None,
    })
}

pub fn field_into_pg_type(field: &Arc<Field>) -> PgWireResult<Type> {
    // A `pg.oid_alias`-tagged Int32 field is reported as its Postgres alias
    // type (regtype -> OID 2206, regclass -> OID 2205, ...) instead of the
    // physical INT4 (OID 23), so RowDescription matches what a
    // Postgres-compatible client expects. The metadata is the semantic source
    // of truth; unknown/missing metadata falls through to the physical-type
    // mapping below.
    if let Some(alias_type) = pg_alias_type(field) {
        return Ok(alias_type);
    }

    let arrow_type = field.data_type();

    match field.extension_type_name() {
        // As of arrow 56, there are additional extension logical type that is
        // defined using field metadata, for instance, json or geo.
        //
        // TODO: there is no fixed Geometry/Geography type id, here we use text
        // for placeholder.
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::PointType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::LineStringType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::PolygonType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::MultiPointType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::MultiLineStringType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::MultiPolygonType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::GeometryCollectionType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::GeometryType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::RectType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::WktType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::WkbType::NAME) => Ok(Type::TEXT),

        _ => into_pg_type(arrow_type),
    }
}

pub fn arrow_schema_to_pg_fields(
    schema: &Schema,
    format: &Format,
    data_format_options: Option<Arc<FormatOptions>>,
) -> PgWireResult<Vec<FieldInfo>> {
    let _ = data_format_options;
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let pg_type = field_into_pg_type(f)?;
            let mut field_info =
                FieldInfo::new(f.name().into(), None, None, pg_type, format.format_for(idx));
            if let Some(data_format_options) = &data_format_options {
                field_info = field_info.with_format_options(data_format_options.clone());
            }

            Ok(field_info)
        })
        .collect::<PgWireResult<Vec<FieldInfo>>>()
}

pub fn encode_recordbatch(
    fields: Arc<Vec<FieldInfo>>,
    record_batch: RecordBatch,
) -> Box<impl Iterator<Item = PgWireResult<DataRow>>> {
    let mut row_stream = RowEncoder::new(record_batch, fields);
    Box::new(std::iter::from_fn(move || row_stream.next_row()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid_alias_field(kind: &str) -> Arc<Field> {
        use std::collections::HashMap;
        Arc::new(
            Field::new("c", DataType::Int32, false).with_metadata(HashMap::from([(
                PG_OID_ALIAS_KEY.to_string(),
                kind.to_string(),
            )])),
        )
    }

    #[test]
    fn null_list_is_text_array() {
        // Align with PostgreSQL: `ARRAY[NULL]` has no element type
        // information and is reported as `text[]`.
        let ty = DataType::List(Arc::new(Field::new_list_field(DataType::Null, true)));
        assert_eq!(into_pg_type(&ty).unwrap(), Type::TEXT_ARRAY);
    }

    #[test]
    fn oid_alias_metadata_maps_to_alias_pg_type() {
        // The kinds enumerated in issue #384:
        assert_eq!(
            field_into_pg_type(&oid_alias_field("oid")).unwrap(),
            Type::OID
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regproc")).unwrap(),
            Type::REGPROC
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regprocedure")).unwrap(),
            Type::REGPROCEDURE
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regclass")).unwrap(),
            Type::REGCLASS
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regtype")).unwrap(),
            Type::REGTYPE
        );

        // The remaining recognized oid-alias kinds:
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regoper")).unwrap(),
            Type::REGOPER
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regoperator")).unwrap(),
            Type::REGOPERATOR
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regnamespace")).unwrap(),
            Type::REGNAMESPACE
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regrole")).unwrap(),
            Type::REGROLE
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regconfig")).unwrap(),
            Type::REGCONFIG
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regdictionary")).unwrap(),
            Type::REGDICTIONARY
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regcollation")).unwrap(),
            Type::REGCOLLATION
        );
    }

    #[test]
    fn oid_alias_metadata_reports_correct_oid() {
        // Issue #384: clients key off the RowDescription type OID, not just the
        // Type variant. Pin the OIDs called out explicitly in the issue.
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regtype"))
                .unwrap()
                .oid(),
            2206
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regclass"))
                .unwrap()
                .oid(),
            2205
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regprocedure"))
                .unwrap()
                .oid(),
            2202
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("regproc"))
                .unwrap()
                .oid(),
            24
        );
        assert_eq!(
            field_into_pg_type(&oid_alias_field("oid")).unwrap().oid(),
            26
        );
    }

    #[test]
    fn unknown_oid_alias_metadata_falls_back_to_int4() {
        let field = oid_alias_field("not_a_real_alias");
        assert_eq!(field_into_pg_type(&field).unwrap(), Type::INT4);
    }

    #[test]
    fn missing_oid_alias_metadata_is_int4() {
        // An ordinary Int32 column with no pg.oid_alias metadata is unchanged.
        let field = Arc::new(Field::new("c", DataType::Int32, false));
        assert_eq!(field_into_pg_type(&field).unwrap(), Type::INT4);
    }
}
