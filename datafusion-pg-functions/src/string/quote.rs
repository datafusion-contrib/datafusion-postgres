//! PostgreSQL `quote_literal(text)` and `quote_nullable(text)`.
//!
//! <https://www.postgresql.org/docs/current/functions-string.html>
//!
//! ## Postgres compatibility
//!
//! Both functions render their argument as an SQL string literal, doubling
//! embedded single quotes. When the value contains a backslash, Postgres
//! emits the `E'...'` escape-string form with **both** single quotes and
//! backslashes doubled (verified on PostgreSQL 18 with the default
//! `standard_conforming_strings = on`): `quote_literal('a\b')` → `E'a\\b'`.
//! Values without backslashes get the plain `'...'` form.
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

/// Render `s` as an SQL literal. If `s` contains a backslash, Postgres emits
/// the `E'...'` form with single quotes and backslashes doubled; otherwise
/// the plain `'...'` form with single quotes doubled.
/// (Both behaviors verified against PostgreSQL 18.)
fn pg_quote_literal(s: &str) -> String {
    let quotes_doubled = s.replace('\'', "''");
    if s.contains('\\') {
        format!("E'{}'", quotes_doubled.replace('\\', "\\\\"))
    } else {
        format!("'{quotes_doubled}'")
    }
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
        // Expectations verified against PostgreSQL 18.4.
        let ctx = SessionContext::new();
        ctx.register_udf(create_quote_literal_udf());

        assert_eq!(
            run_str(&ctx, "SELECT quote_literal('hello')").await,
            Some("'hello'".into())
        );
        // Embedded single quote doubled; no backslash -> plain form.
        assert_eq!(
            run_str(&ctx, "SELECT quote_literal('a''b')").await,
            Some("'a''b'".into())
        );
        // Backslash present -> E'...' form with quotes AND backslashes doubled.
        assert_eq!(
            run_str(&ctx, "SELECT quote_literal('a''b\\c')").await,
            Some("E'a''b\\\\c'".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT quote_literal('plain\\slash')").await,
            Some("E'plain\\\\slash'".into())
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
        // Backslash -> E'' form, same rule as quote_literal.
        assert_eq!(
            run_str(&ctx, "SELECT quote_nullable('a\\b')").await,
            Some("E'a\\\\b'".into())
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
