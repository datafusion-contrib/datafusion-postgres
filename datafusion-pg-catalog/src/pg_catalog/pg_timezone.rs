use crate::pg_catalog::empty_table::EmptyTable;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub(crate) fn pg_timezone_names() -> EmptyTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("abbrev", DataType::Utf8, true),
        Field::new("utc_offset", DataType::Utf8, true),
        Field::new("is_dst", DataType::Boolean, true),
    ]));

    EmptyTable::new(schema)
}

pub(crate) fn pg_timezone_abbrevs() -> EmptyTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("abbrev", DataType::Utf8, true),
        Field::new("utc_offset", DataType::Utf8, true),
        Field::new("is_dst", DataType::Boolean, true),
    ]));

    EmptyTable::new(schema)
}
