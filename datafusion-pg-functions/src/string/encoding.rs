//! PostgreSQL encoding-related string functions:
//!
//! * `pg_client_encoding()` — returns the name of the current client
//!   encoding. In DataFusion we always report `'UTF8'`.
//! * `to_ascii(text [, encoding])` — convert text to ASCII, replacing
//!   non-ASCII characters with `?`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

// ---------------------------------------------------------------------------
// pg_client_encoding() → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgClientEncodingUdf {
    signature: Signature,
}

impl Default for PgClientEncodingUdf {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for PgClientEncodingUdf {
    fn name(&self) -> &str {
        "pg_client_encoding"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "UTF8".to_string(),
        ))))
    }
}

// ---------------------------------------------------------------------------
// to_ascii(text [, encoding]) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToAsciiUdf {
    signature: Signature,
}

impl Default for ToAsciiUdf {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ToAsciiUdf {
    fn name(&self) -> &str {
        "to_ascii"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(arr) => {
                let typed = arr.as_string::<i32>();
                let mut builder = StringBuilder::with_capacity(typed.len(), typed.len() * 20);
                for i in 0..typed.len() {
                    if typed.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(to_ascii_str(typed.value(i)));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(to_ascii_str(s))),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            _ => Err(DataFusionError::Internal(
                "to_ascii: unexpected argument type".into(),
            )),
        }
    }
}

fn to_ascii_str(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii() { c } else { '?' })
        .collect()
}

pub fn create_pg_client_encoding_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(PgClientEncodingUdf::default())
}

pub fn create_to_ascii_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToAsciiUdf::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn run_str(ctx: &SessionContext, sql: &str) -> Option<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        if batches[0].num_rows() == 0 {
            return None;
        }
        let col = batches[0].column(0);
        let arr = col.as_string::<i32>();
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_string())
        }
    }

    #[tokio::test]
    async fn pg_client_encoding_returns_utf8() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_pg_client_encoding_udf());

        assert_eq!(
            run_str(&ctx, "SELECT pg_client_encoding()").await,
            Some("UTF8".into())
        );
    }

    #[tokio::test]
    async fn to_ascii_basic() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_ascii_udf());

        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('hello')").await,
            Some("hello".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('café')").await,
            Some("caf?".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii(CAST(NULL AS TEXT))").await,
            None
        );
    }
}
