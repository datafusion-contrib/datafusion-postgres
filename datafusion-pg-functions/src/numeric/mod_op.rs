//! PostgreSQL `mod(y, x)` — the modulus function.
//!
//! DataFusion ships the `%` operator but not the function form `mod(y, x)`,
//! so queries written against Postgres (which documents both forms) fail to
//! resolve. This UDF produces the same result as `y % x` for integer inputs
//! and follows Postgres semantics for the floating-point case
//! (`mod(y, x) = y - x * trunc(y / x)`).
//!
//! # Postgres semantics
//!
//! Per the [PostgreSQL manual](https://www.postgresql.org/docs/current/functions-math.html):
//!
//! > `mod(y, x)` → remainder of `y / x`
//!
//! The result has the same sign as the dividend `y`, matching the SQL
//! standard and Rust's built-in `%` operator for both integer and
//! floating-point operands. Division by zero yields `NULL` (Postgres raises an
//! error; we return NULL because DataFusion does not propagate error rows).

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, AsArray, PrimitiveBuilder,
};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Create the PostgreSQL `mod(y, x)` UDF.
pub fn create_mod_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ModUDF::default())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ModUDF {
    signature: Signature,
}

impl Default for ModUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ModUDF {
    pub fn new() -> Self {
        use DataType::*;
        // Postgres `mod` accepts integer (i8/i16/i32/i64) and floating-point
        // (f32/f64) operands. Both arguments must share a type.
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Int8, Int8]),
                    TypeSignature::Exact(vec![Int16, Int16]),
                    TypeSignature::Exact(vec![Int32, Int32]),
                    TypeSignature::Exact(vec![Int64, Int64]),
                    TypeSignature::Exact(vec![Float32, Float32]),
                    TypeSignature::Exact(vec![Float64, Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ModUDF {
    fn name(&self) -> &str {
        "mod"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 || arg_types[0] != arg_types[1] {
            return exec_err!(
                "mod requires two arguments of the same type, got {:?}",
                arg_types
            );
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        if args.len() != 2 {
            return exec_err!("mod requires exactly 2 arguments, got {}", args.len());
        }
        let y = &args[0];
        let x = &args[1];
        if y.data_type() != x.data_type() {
            return exec_err!(
                "mod requires both operands to share a type; got y={:?}, x={:?}",
                y.data_type(),
                x.data_type()
            );
        }
        let out: ArrayRef = match y.data_type() {
            DataType::Int8 => Arc::new(mod_int::<Int8Type>(y, x)?.finish()),
            DataType::Int16 => Arc::new(mod_int::<Int16Type>(y, x)?.finish()),
            DataType::Int32 => Arc::new(mod_int::<Int32Type>(y, x)?.finish()),
            DataType::Int64 => Arc::new(mod_int::<Int64Type>(y, x)?.finish()),
            DataType::Float32 => Arc::new(mod_float::<Float32Type, _>(y, x, |a, b| {
                a - b * (a / b).trunc()
            })?.finish()),
            DataType::Float64 => Arc::new(mod_float::<Float64Type, _>(y, x, |a, b| {
                a - b * (a / b).trunc()
            })?.finish()),
            other => {
                return exec_err!(
                    "mod only supports integer/float operands, got {other:?}"
                );
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

/// Elementwise integer modulus following Postgres semantics. Both arrays must
/// be the same primitive type; either may be a length-1 broadcast scalar.
fn mod_int<T>(y: &ArrayRef, x: &ArrayRef) -> Result<PrimitiveBuilder<T>>
where
    T: datafusion::arrow::datatypes::ArrowPrimitiveType,
    T::Native: std::ops::Rem<Output = T::Native> + PartialOrd + Default,
{
    let y = y.as_primitive::<T>();
    let x = x.as_primitive::<T>();
    let zero = T::Native::default();
    let len = y.len().max(x.len());
    let mut out = PrimitiveBuilder::<T>::with_capacity(len);
    for i in 0..len {
        let j = if y.len() == 1 { 0 } else { i };
        let k = if x.len() == 1 { 0 } else { i };
        if y.is_null(j) || x.is_null(k) || x.value(k) == zero {
            // Postgres raises an error on integer mod-by-zero; we return NULL
            // because DataFusion cannot raise per-row errors.
            out.append_null();
        } else {
            out.append_value(y.value(j) % x.value(k));
        }
    }
    Ok(out)
}

/// Elementwise floating-point modulus. The closure lets us share the boilerplate
/// between f32 and f64 with the only difference being the arithmetic.
fn mod_float<T, F>(y: &ArrayRef, x: &ArrayRef, op: F) -> Result<PrimitiveBuilder<T>>
where
    T: datafusion::arrow::datatypes::ArrowPrimitiveType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let y = y.as_primitive::<T>();
    let x = x.as_primitive::<T>();
    let len = y.len().max(x.len());
    let mut out = PrimitiveBuilder::<T>::with_capacity(len);
    for i in 0..len {
        let j = if y.len() == 1 { 0 } else { i };
        let k = if x.len() == 1 { 0 } else { i };
        if y.is_null(j) || x.is_null(k) {
            out.append_null();
        } else {
            out.append_value(op(y.value(j), x.value(k)));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn run_i64(ctx: &SessionContext, sql: &str) -> Option<i64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        use datafusion::arrow::array::Array;
        match col.data_type() {
            DataType::Int8 => col.as_primitive::<Int8Type>().iter().next().unwrap().map(|v| v as i64),
            DataType::Int16 => col.as_primitive::<Int16Type>().iter().next().unwrap().map(|v| v as i64),
            DataType::Int32 => col.as_primitive::<Int32Type>().iter().next().unwrap().map(|v| v as i64),
            DataType::Int64 => col.as_primitive::<Int64Type>().iter().next().unwrap(),
            _ => panic!("unexpected integer type {:?}", col.data_type()),
        }
    }

    async fn run_f64(ctx: &SessionContext, sql: &str) -> Option<f64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).as_primitive::<Float64Type>().iter().next().unwrap()
    }

    #[tokio::test]
    async fn mod_matches_postgres_examples() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_mod_udf().into());

        // From the Postgres docs:
        //   mod(9, 4)   = 1
        //   mod(-9, 4)  = -1
        //   mod(9, -4)  = 1
        //   mod(-9, -4) = -1
        assert_eq!(run_i64(&ctx, "SELECT mod(9, 4)").await, Some(1));
        assert_eq!(run_i64(&ctx, "SELECT mod(-9, 4)").await, Some(-1));
        assert_eq!(run_i64(&ctx, "SELECT mod(9, -4)").await, Some(1));
        assert_eq!(run_i64(&ctx, "SELECT mod(-9, -4)").await, Some(-1));
    }

    #[tokio::test]
    async fn mod_float_uses_trunc_quotient() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_mod_udf().into());

        // mod(7.5, 2.5) = 7.5 - 2.5 * trunc(7.5 / 2.5) = 7.5 - 2.5 * 3 = 0
        let got = run_f64(&ctx, "SELECT mod(7.5, 2.5)").await.unwrap();
        assert!(got.abs() < 1e-12);

        // Sign follows dividend.
        let got = run_f64(&ctx, "SELECT mod(-7.5, 2.5)").await.unwrap();
        assert!(got.abs() < 1e-12);

        // mod(10.5, 3.0) = 10.5 - 3.0 * trunc(10.5 / 3.0) = 10.5 - 9.0 = 1.5
        let got = run_f64(&ctx, "SELECT mod(10.5, 3.0)").await.unwrap();
        assert!((got - 1.5).abs() < 1e-12);
    }

    #[tokio::test]
    async fn mod_by_zero_returns_null() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_mod_udf().into());
        assert_eq!(run_i64(&ctx, "SELECT mod(9, 0)").await, None);
    }
}
