use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::MemTable;
use datafusion::error::Result;

#[derive(Debug, Clone)]
pub(crate) struct EmptyTable {
    schema: SchemaRef,
}

impl EmptyTable {
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }

    pub fn try_into_memtable(self) -> Result<MemTable> {
        MemTable::try_new(self.schema, vec![vec![]])
    }
}
