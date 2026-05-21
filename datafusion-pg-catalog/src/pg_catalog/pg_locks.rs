use crate::pg_catalog::empty_table::EmptyTable;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub(crate) fn pg_locks() -> EmptyTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("locktype", DataType::Utf8, true),
        Field::new("database", DataType::Int32, true),
        Field::new("relation", DataType::Int32, true),
        Field::new("page", DataType::Int32, true),
        Field::new("tuple", DataType::Int16, true),
        Field::new("virtualxid", DataType::Utf8, true),
        Field::new("transactionid", DataType::Utf8, true),
        Field::new("classid", DataType::Int32, true),
        Field::new("objid", DataType::Int32, true),
        Field::new("objsubid", DataType::Int16, true),
        Field::new("virtualtransaction", DataType::Utf8, true),
        Field::new("pid", DataType::Int32, true),
        Field::new("mode", DataType::Utf8, true),
        Field::new("granted", DataType::Boolean, true),
        Field::new("fastpath", DataType::Boolean, true),
        Field::new(
            "waitstart",
            DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                Some(Arc::from("UTC")),
            ),
            true,
        ),
    ]));

    EmptyTable::new(schema)
}
