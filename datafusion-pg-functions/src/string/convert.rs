//! PostgreSQL `to_bin(integer)` and `to_oct(integer)` — integer-to-text
//! conversion in binary and octal bases (PG 18+).
//!
//! <https://www.postgresql.org/docs/18/functions-string.html>
//!
//! ## Postgres compatibility
//!
//! Negative values are rendered using the **two's-complement** representation
//! of the integer's width (matching `to_hex`, the sibling base-conversion
//! function). So `to_bin(-1::int4)` yields 32 ones, `to_oct(-1::int4)` yields
//! `37777777777`. `NULL` propagates to `NULL`. We support both `int4` and
//! `int8` inputs; the width follows the input type.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

// ---------------------------------------------------------------------------
// to_bin(int) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToBinUDF {
    signature: Signature,
}

impl Default for ToBinUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ToBinUDF {
    fn name(&self) -> &str {
        "to_bin"
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
                let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 10);
                match arr.data_type() {
                    DataType::Int32 => {
                        let typed = arr.as_primitive::<Int32Type>();
                        for i in 0..typed.len() {
                            if typed.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(bin_i32(typed.value(i)));
                            }
                        }
                    }
                    DataType::Int64 => {
                        let typed = arr.as_primitive::<Int64Type>();
                        for i in 0..typed.len() {
                            if typed.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(bin_i64(typed.value(i)));
                            }
                        }
                    }
                    other => {
                        return Err(DataFusionError::Internal(format!(
                            "to_bin: unsupported input type {other}"
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(sv) => match sv {
                ScalarValue::Int32(Some(v)) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(bin_i32(*v)))))
                }
                ScalarValue::Int64(Some(v)) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(bin_i64(*v)))))
                }
                ScalarValue::Int32(None) | ScalarValue::Int64(None) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                }
                _ => Err(DataFusionError::Internal(
                    "to_bin: unexpected scalar type".into(),
                )),
            },
        }
    }
}

/// Binary text of an `int4`, two's-complement (32-bit width for negatives).
fn bin_i32(v: i32) -> String {
    format!("{:b}", v as u32)
}

/// Binary text of an `int8`, two's-complement (64-bit width for negatives).
fn bin_i64(v: i64) -> String {
    format!("{:b}", v as u64)
}

// ---------------------------------------------------------------------------
// to_oct(int) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToOctUDF {
    signature: Signature,
}

impl Default for ToOctUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ToOctUDF {
    fn name(&self) -> &str {
        "to_oct"
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
                let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 10);
                match arr.data_type() {
                    DataType::Int32 => {
                        let typed = arr.as_primitive::<Int32Type>();
                        for i in 0..typed.len() {
                            if typed.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(oct_i32(typed.value(i)));
                            }
                        }
                    }
                    DataType::Int64 => {
                        let typed = arr.as_primitive::<Int64Type>();
                        for i in 0..typed.len() {
                            if typed.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(oct_i64(typed.value(i)));
                            }
                        }
                    }
                    other => {
                        return Err(DataFusionError::Internal(format!(
                            "to_oct: unsupported input type {other}"
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(sv) => match sv {
                ScalarValue::Int32(Some(v)) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(oct_i32(*v)))))
                }
                ScalarValue::Int64(Some(v)) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(oct_i64(*v)))))
                }
                ScalarValue::Int32(None) | ScalarValue::Int64(None) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                }
                _ => Err(DataFusionError::Internal(
                    "to_oct: unexpected scalar type".into(),
                )),
            },
        }
    }
}

/// Octal text of an `int4`, two's-complement (32-bit width for negatives).
fn oct_i32(v: i32) -> String {
    format!("{:o}", v as u32)
}

/// Octal text of an `int8`, two's-complement (64-bit width for negatives).
fn oct_i64(v: i64) -> String {
    format!("{:o}", v as u64)
}

pub fn create_to_bin_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToBinUDF::default())
}

pub fn create_to_oct_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToOctUDF::default())
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
    async fn to_bin_basics() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_bin_udf());

        assert_eq!(
            run_str(&ctx, "SELECT to_bin(42)").await,
            Some("101010".into())
        );
        assert_eq!(run_str(&ctx, "SELECT to_bin(0)").await, Some("0".into()));
        // Two's-complement 32-bit: -1 -> 32 ones
        assert_eq!(
            run_str(&ctx, "SELECT to_bin(CAST(-1 AS INT))").await,
            Some("11111111111111111111111111111111".into())
        );
        // -13 -> ...11110011
        assert_eq!(
            run_str(&ctx, "SELECT to_bin(CAST(-13 AS INT))").await,
            Some("11111111111111111111111111110011".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_bin(CAST(NULL AS INT))").await,
            None
        );
    }

    #[tokio::test]
    async fn to_bin_bigint_width() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_bin_udf());
        // int8 -1 -> 64 ones
        assert_eq!(
            run_str(&ctx, "SELECT to_bin(CAST(-1 AS BIGINT))")
                .await
                .map(|s| s.len()),
            Some(64)
        );
    }

    #[tokio::test]
    async fn to_oct_basics() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_oct_udf());

        assert_eq!(run_str(&ctx, "SELECT to_oct(42)").await, Some("52".into()));
        assert_eq!(run_str(&ctx, "SELECT to_oct(0)").await, Some("0".into()));
        // -1 int4 -> 37777777777 (two's-complement)
        assert_eq!(
            run_str(&ctx, "SELECT to_oct(CAST(-1 AS INT))").await,
            Some("37777777777".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_oct(CAST(NULL AS INT))").await,
            None
        );
    }

    #[tokio::test]
    async fn to_bin_vectorized_batch() {
        // Convention #4: a row-wise vectorized batch (array input path).
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_bin_udf());
        let df = ctx
            .sql("SELECT to_bin(c) FROM (VALUES (1), (2), (10), (CAST(NULL AS INT))) AS t(c)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = df[0].column(0).as_string::<i32>();
        let got: Vec<Option<&str>> = (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect();
        assert_eq!(got, vec![Some("1"), Some("10"), Some("1010"), None]);
    }
}
