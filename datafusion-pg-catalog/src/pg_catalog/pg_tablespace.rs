use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Int32Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;

#[derive(Debug, Clone)]
pub(crate) struct PgTablespaceTable {
    schema: SchemaRef,
}

impl PgTablespaceTable {
    pub(crate) fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("xmin", DataType::Int32, true),
            Field::new("spcname", DataType::Utf8, false),
            Field::new("spcowner", DataType::Int32, false),
            Field::new(
                "spcacl",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("spcoptions", DataType::Utf8, true),
        ]));

        Self { schema }
    }

    fn get_data(schema: SchemaRef) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1663, 1664])),
            Arc::new(Int32Array::from(vec![Some(1), Some(1)])),
            Arc::new(StringArray::from(vec!["pg_default", "pg_global"])),
            Arc::new(Int32Array::from(vec![10, 10])),
            Arc::new({
                let mut builder = ListBuilder::new(StringBuilder::new());
                builder.append(false);
                builder.append(false);
                builder.finish()
            }),
            Arc::new(StringArray::from(vec![None::<String>, None::<String>])),
        ];

        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

impl PartitionStream for PgTablespaceTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(async move { Self::get_data(schema) }),
        ))
    }
}
