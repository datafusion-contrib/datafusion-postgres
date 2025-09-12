use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::MemTable;
use datafusion::error::Result;

#[derive(Debug, Clone)]
pub(crate) struct PgViewsTable {
    schema: SchemaRef,
}

impl PgViewsTable {
    pub(crate) fn new() -> Self {
        // Define the schema for pg_views
        let schema = Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, true),
            Field::new("viewname", DataType::Utf8, true),
            Field::new("viewowner", DataType::Utf8, true),
            Field::new("definition", DataType::Utf8, true),
        ]));

        Self { schema }
    }

    pub fn try_into_memtable(self) -> Result<MemTable> {
        MemTable::try_new(self.schema, vec![vec![]])
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PgMatviewsTable {
    schema: SchemaRef,
}

impl PgMatviewsTable {
    pub(crate) fn new() -> Self {
        // Define the schema for pg_matviews
        let schema = Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, true),
            Field::new("matviewname", DataType::Utf8, true),
            Field::new("matviewowner", DataType::Utf8, true),
            Field::new("tablespace", DataType::Utf8, true),
            Field::new("hasindexes", DataType::Boolean, true),
            Field::new("ispopulated", DataType::Boolean, true),
            Field::new("definition", DataType::Utf8, true),
        ]));

        Self { schema }
    }

    pub fn try_into_memtable(self) -> Result<MemTable> {
        MemTable::try_new(self.schema, vec![vec![]])
    }
}
