use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::array::RecordBatch;
#[cfg(feature = "datafusion")]
use datafusion::arrow::array::RecordBatch;

use pgwire::{
    api::results::{DataRowEncoder, FieldInfo},
    error::PgWireResult,
    messages::data::DataRow,
};

use crate::encoder::encode_value;

pub struct RowEncoder {
    rb: RecordBatch,
    curr_idx: usize,
    fields: Arc<Vec<FieldInfo>>,
}

impl RowEncoder {
    pub fn new(rb: RecordBatch, fields: Arc<Vec<FieldInfo>>) -> Self {
        assert_eq!(rb.num_columns(), fields.len());
        Self {
            rb,
            fields,
            curr_idx: 0,
        }
    }

    pub fn next_row(&mut self) -> Option<PgWireResult<DataRow>> {
        if self.curr_idx == self.rb.num_rows() {
            return None;
        }
        let arrow_schema = self.rb.schema_ref();
        let mut encoder = DataRowEncoder::new(self.fields.clone());
        for col in 0..self.rb.num_columns() {
            let array = self.rb.column(col);
            let arrow_field = arrow_schema.field(col);
            let pg_field = &self.fields[col];

            encode_value(&mut encoder, array, self.curr_idx, arrow_field, pg_field).unwrap();
        }
        self.curr_idx += 1;
        Some(encoder.finish())
    }
}
