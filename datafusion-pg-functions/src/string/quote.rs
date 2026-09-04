//! PostgreSQL `quote_literal(text)` and `quote_nullable(text)`.
//!
//! <https://www.postgresql.org/docs/current/functions-string.html>
//!
//! ## Postgres compatibility
//!
//! Both functions render their argument as a single-quoted SQL string literal,
//! with embedded single quotes doubled. Backslashes are **not** doubled:
//! PostgreSQL ships with `standard_conforming_strings = on` by default, in
//! which a backslash inside `'...'` is an ordinary character and needs no
//! escaping. (Only the legacy `off` setting, or the `E'...'` form, doubles
//! backslashes — neither of which DataFusion models.)
//!
//! `quote_nullable` differs from `quote_literal` only on `NULL` input: it
//! returns the unquoted string `NULL` rather than a null value.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Render `s` as a single-quoted SQL literal, doubling embedded single quotes.
/// Backslashes are left untouched (standard_conforming_strings = on).
fn pg_quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

// ---------------------------------------------------------------------------
// quote_literal(text) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct QuoteLiteralUDF {
    signature: Signature,
}

impl Default for QuoteLiteralUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for QuoteLiteralUDF {
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
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
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
pub struct QuoteNullableUDF {
    signature: Signature,
}

impl Default for QuoteNullableUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for QuoteNullableUDF {
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
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
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
    ScalarUDF::new_from_impl(QuoteLiteralUDF::default())
}

pub fn create_quote_nullable_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(QuoteNullableUDF::default())
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
        // Embedded single quote is doubled; backslash is NOT doubled (scs=on).
        assert_eq!(
            run_str(&ctx, "SELECT quote_literal('a''b\\c')").await,
            Some("'a''b\\c'".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT quote_literal(CAST(NULL AS TEXT))").await,
            None
        );
    }

    #[tokio::test]
    async fn quote_nullable_null_is_string_null() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_quote_nullable_udf());

        assert_eq!(
            run_str(&ctx, "SELECT quote_nullable('hello')").await,
            Some("'hello'".into())
        );
        // NULL input -> the literal string "NULL", not a null value.
        assert_eq!(
            run_str(&ctx, "SELECT quote_nullable(CAST(NULL AS TEXT))").await,
            Some("NULL".into())
        );
    }

    #[tokio::test]
    async fn quote_nullable_vectorized_batch() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_quote_nullable_udf());
        let df = ctx
            .sql("SELECT quote_nullable(c) FROM (VALUES ('a'), (CAST(NULL AS TEXT))) AS t(c)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = df[0].column(0).as_string::<i32>();
        assert_eq!(arr.value(0), "'a'");
        assert_eq!(arr.value(1), "NULL");
    }
}
