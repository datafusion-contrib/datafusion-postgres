//! PostgreSQL `to_bin(integer)` and `to_oct(integer)` — integer-to-text
//! conversion in binary and octal bases.
//!
//! PostgreSQL also overloads these on `bigint`. We cover both `int4` and
//! `int8` via separate signature arms.
//!
//! ## Semantics
//!
//! * Negative values get a leading `-` sign followed by the absolute value
//!   in the target base (matching Postgres, **not** two's-complement).
//! * `NULL` input propagates to `NULL`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
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
pub struct ToBinUdf {
    signature: Signature,
}

impl Default for ToBinUdf {
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

impl ScalarUDFImpl for ToBinUdf {
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
                                builder.append_value(&format_bin_i64(typed.value(i) as i64));
                            }
                        }
                    }
                    DataType::Int64 => {
                        let typed = arr.as_primitive::<Int64Type>();
                        for i in 0..typed.len() {
                            if typed.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(&format_bin_i64(typed.value(i)));
                            }
                        }
                    }
                    other => {
                        return Err(DataFusionError::Internal(format!(
                            "to_bin: unsupported input type {other}"
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
            }
            ColumnarValue::Scalar(sv) => match sv {
                ScalarValue::Int32(Some(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    format_bin_i64(*v as i64),
                )))),
                ScalarValue::Int64(Some(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    format_bin_i64(*v),
                )))),
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

fn format_bin_i64(v: i64) -> String {
    if v < 0 {
        format!("-{:b}", v.unsigned_abs())
    } else {
        format!("{v:b}")
    }
}

// ---------------------------------------------------------------------------
// to_oct(int) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToOctUdf {
    signature: Signature,
}

impl Default for ToOctUdf {
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

impl ScalarUDFImpl for ToOctUdf {
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
                                builder.append_value(&format_oct_i64(typed.value(i) as i64));
                            }
                        }
                    }
                    DataType::Int64 => {
                        let typed = arr.as_primitive::<Int64Type>();
                        for i in 0..typed.len() {
                            if typed.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(&format_oct_i64(typed.value(i)));
                            }
                        }
                    }
                    other => {
                        return Err(DataFusionError::Internal(format!(
                            "to_oct: unsupported input type {other}"
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
            }
            ColumnarValue::Scalar(sv) => match sv {
                ScalarValue::Int32(Some(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    format_oct_i64(*v as i64),
                )))),
                ScalarValue::Int64(Some(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    format_oct_i64(*v),
                )))),
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

fn format_oct_i64(v: i64) -> String {
    if v < 0 {
        format!("-{:o}", v.unsigned_abs())
    } else {
        format!("{v:o}")
    }
}

pub fn create_to_bin_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToBinUdf::default())
}

pub fn create_to_oct_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToOctUdf::default())
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

        assert_eq!(run_str(&ctx, "SELECT to_bin(42)").await, Some("101010".into()));
        assert_eq!(run_str(&ctx, "SELECT to_bin(0)").await, Some("0".into()));
        assert_eq!(run_str(&ctx, "SELECT to_bin(-13)").await, Some("-1101".into()));
        assert_eq!(run_str(&ctx, "SELECT to_bin(CAST(NULL AS INT))").await, None);
    }

    #[tokio::test]
    async fn to_oct_basics() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_oct_udf());

        assert_eq!(run_str(&ctx, "SELECT to_oct(42)").await, Some("52".into()));
        assert_eq!(run_str(&ctx, "SELECT to_oct(0)").await, Some("0".into()));
        assert_eq!(run_str(&ctx, "SELECT to_oct(-13)").await, Some("-15".into()));
        assert_eq!(run_str(&ctx, "SELECT to_oct(CAST(NULL AS INT))").await, None);
    }
}
