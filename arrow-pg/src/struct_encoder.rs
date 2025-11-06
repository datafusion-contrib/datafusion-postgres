use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::array::{Array, StructArray};
use arrow_schema::Fields;
#[cfg(feature = "datafusion")]
use datafusion::arrow::array::{Array, StructArray};

use bytes::{BufMut, BytesMut};
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::types::{ToSqlText, QUOTE_CHECK, QUOTE_ESCAPE};
use postgres_types::{IsNull, ToSql};

use crate::encoder::{encode_value, EncodedValue, Encoder};
use crate::error::ToSqlError;

pub(crate) fn encode_struct(
    arr: &Arc<dyn Array>,
    idx: usize,
    arrow_fields: &Fields,
    parent_pg_field_info: &FieldInfo,
) -> PgWireResult<Option<EncodedValue>> {
    let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    if arr.is_null(idx) {
        return Ok(None);
    }

    let fields = match parent_pg_field_info.datatype().kind() {
        postgres_types::Kind::Composite(fields) => fields,
        _ => {
            return Err(PgWireError::ApiError(ToSqlError::from(format!(
                "Failed to unwrap a composite type of {}",
                parent_pg_field_info.datatype()
            ))));
        }
    };

    let mut row_encoder = StructEncoder::new(arrow_fields.len());
    for (i, arr) in arr.columns().iter().enumerate() {
        let field = &fields[i];
        let type_ = field.type_();

        let arrow_field = &arrow_fields[i];

        let mut pg_field = FieldInfo::new(
            field.name().to_string(),
            None,
            None,
            type_.clone(),
            parent_pg_field_info.format(),
        );
        pg_field = pg_field.with_format_options(parent_pg_field_info.format_options().clone());

        encode_value(&mut row_encoder, arr, idx, arrow_field, &pg_field).unwrap();
    }
    Ok(Some(EncodedValue {
        bytes: row_encoder.row_buffer,
    }))
}

pub(crate) struct StructEncoder {
    num_cols: usize,
    curr_col: usize,
    row_buffer: BytesMut,
}

impl StructEncoder {
    pub(crate) fn new(num_cols: usize) -> Self {
        Self {
            num_cols,
            curr_col: 0,
            row_buffer: BytesMut::new(),
        }
    }
}

impl Encoder for StructEncoder {
    fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        let datatype = pg_field.datatype();
        let format = pg_field.format();

        if format == FieldFormat::Text {
            if self.curr_col == 0 {
                self.row_buffer.put_slice(b"(");
            }
            // encode value in an intermediate buf
            let mut buf = BytesMut::new();
            value.to_sql_text(datatype, &mut buf, pg_field.format_options().as_ref())?;
            let encoded_value_as_str = String::from_utf8_lossy(&buf);
            if QUOTE_CHECK.is_match(&encoded_value_as_str) {
                self.row_buffer.put_u8(b'"');
                self.row_buffer.put_slice(
                    QUOTE_ESCAPE
                        .replace_all(&encoded_value_as_str, r#"\$1"#)
                        .as_bytes(),
                );
                self.row_buffer.put_u8(b'"');
            } else {
                self.row_buffer.put_slice(&buf);
            }
            if self.curr_col == self.num_cols - 1 {
                self.row_buffer.put_slice(b")");
            } else {
                self.row_buffer.put_slice(b",");
            }
        } else {
            if self.curr_col == 0 && format == FieldFormat::Binary {
                // Place Number of fields
                self.row_buffer.put_i32(self.num_cols as i32);
            }

            self.row_buffer.put_u32(datatype.oid());
            // remember the position of the 4-byte length field
            let prev_index = self.row_buffer.len();
            // write value length as -1 ahead of time
            self.row_buffer.put_i32(-1);
            let is_null = value.to_sql(datatype, &mut self.row_buffer)?;
            if let IsNull::No = is_null {
                let value_length = self.row_buffer.len() - prev_index - 4;
                let mut length_bytes = &mut self.row_buffer[prev_index..(prev_index + 4)];
                length_bytes.put_i32(value_length as i32);
            }
        }
        self.curr_col += 1;
        Ok(())
    }
}
