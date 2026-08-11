//! PostgreSQL `quote_literal(text)` and `quote_nullable(text)`.
//!
//! ## Semantics (from PostgreSQL docs)
//!
//! * `quote_literal(value)` — Return the given string suitably quoted to be
//!   used as a string literal in an SQL statement string. Embedded
//!   single-quotes and backslashes are properly doubled. Returns `NULL` for
//!   `NULL` input.
//!
//! * `quote_nullable(value)` — Return the given string suitably quoted to be
//!   used as a string literal in an SQL statement string. If the argument is
//!   `NULL`, the result is the unquoted string `"NULL"`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Escape a string for use as a PostgreSQL SQL literal:
/// double every single-quote and every backslash, then wrap in single quotes.
fn pg_quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
}

// ---------------------------------------------------------------------------
// quote_literal(text) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct QuoteLiteralUdf {
    signature: Signature,
}

impl Default for QuoteLiteralUdf {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for QuoteLiteralUdf {
    fn name(&self) -> &str {
        "quote_literal"
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
                        builder.append_value(pg_quote_literal(typed.value(i)));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(pg_quote_literal(s))),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            _ => Err(DataFusionError::Internal(
                "quote_literal: unexpected argument type".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// quote_nullable(text) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct QuoteNullableUdf {
    signature: Signature,
}

impl Default for QuoteNullableUdf {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for QuoteNullableUdf {
    fn name(&self) -> &str {
        "quote_nullable"
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
                        builder.append_value("NULL");
                    } else {
                        builder.append_value(pg_quote_literal(typed.value(i)));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(pg_quote_literal(s))),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some("NULL".into())),
            )),
            _ => Err(DataFusionError::Internal(
                "quote_nullable: unexpected argument type".into(),
            )),
        }
    }
}

pub fn create_quote_literal_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(QuoteLiteralUdf::default())
}

pub fn create_quote_nullable_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(QuoteNullableUdf::default())
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
    async fn quote_literal_basics() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_quote_literal_udf());

        assert_eq!(
            run_str(&ctx, "SELECT quote_literal('hello')").await,
            Some("'hello'".into())
        );
        // NULL propagates
        assert_eq!(
            run_str(&ctx, "SELECT quote_literal(CAST(NULL AS TEXT))").await,
            None
        );
    }

    #[tokio::test]
    async fn quote_nullable_basics() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_quote_nullable_udf());

        assert_eq!(
            run_str(&ctx, "SELECT quote_nullable('hello')").await,
            Some("'hello'".into())
        );
        // NULL becomes the literal string "NULL"
        assert_eq!(
            run_str(&ctx, "SELECT quote_nullable(CAST(NULL AS TEXT))").await,
            Some("NULL".into())
        );
    }
}
