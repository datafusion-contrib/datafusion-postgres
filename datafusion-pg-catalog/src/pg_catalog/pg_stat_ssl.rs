use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use std::sync::Arc;

use crate::pg_catalog::BACKEND_PID;

#[derive(Debug, Clone)]
pub(crate) struct PgStatSslTable {
    schema: SchemaRef,
}

impl PgStatSslTable {
    pub(crate) fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pid", DataType::Int32, true),
            Field::new("ssl", DataType::Boolean, false),
            Field::new("version", DataType::Utf8, true),
            Field::new("cipher", DataType::Utf8, true),
            Field::new("bits", DataType::Int32, true),
            Field::new("client_dn", DataType::Utf8, true),
            Field::new("client_serial", DataType::Utf8, true),
            Field::new("issuer_dn", DataType::Utf8, true),
        ]));

        Self { schema }
    }

    async fn get_data(this: Self) -> Result<RecordBatch> {
        let pid = vec![BACKEND_PID];
        let ssl = vec![false];
        let version: Vec<Option<String>> = vec![None];
        let cipher: Vec<Option<String>> = vec![None];
        let bits: Vec<Option<i32>> = vec![None];
        let client_dn: Vec<Option<String>> = vec![None];
        let client_serial: Vec<Option<String>> = vec![None];
        let issuer_dn: Vec<Option<String>> = vec![None];

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(pid)),
            Arc::new(BooleanArray::from(ssl)),
            Arc::new(datafusion::arrow::array::StringArray::from(version)),
            Arc::new(datafusion::arrow::array::StringArray::from(cipher)),
            Arc::new(Int32Array::from(bits)),
            Arc::new(datafusion::arrow::array::StringArray::from(client_dn)),
            Arc::new(datafusion::arrow::array::StringArray::from(client_serial)),
            Arc::new(datafusion::arrow::array::StringArray::from(issuer_dn)),
        ];

        let batch = RecordBatch::try_new(this.schema.clone(), arrays)?;
        Ok(batch)
    }
}

impl PartitionStream for PgStatSslTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { PgStatSslTable::get_data(this).await }),
        ))
    }
}
