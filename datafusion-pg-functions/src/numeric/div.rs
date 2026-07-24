//! PostgreSQL `div(y, x)` — integer quotient function.
//!
//! Per the [PostgreSQL manual](https://www.postgresql.org/docs/current/functions-math.html):
//!
//! > `div(y, x)` → integer quotient `y / x`
//!
//! Returns the integer quotient of `y / x`. Both arguments must be integers.
//! The result follows the same sign rules as Rust's `/` for integer types
//! (truncation toward zero), matching Postgres.

use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, Int8Type, Int16Type, Int32Type, Int64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Create the PostgreSQL `div(y, x)` UDF.
pub fn create_div_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(DivUDF::default())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DivUDF {
    signature: Signature,
}

impl Default for DivUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl DivUDF {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Int8, Int8]),
                    TypeSignature::Exact(vec![Int16, Int16]),
                    TypeSignature::Exact(vec![Int32, Int32]),
                    TypeSignature::Exact(vec![Int64, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DivUDF {
    fn name(&self) -> &str {
        "div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 || arg_types[0] != arg_types[1] {
            return exec_err!(
                "div requires two arguments of the same type, got {:?}",
                arg_types
            );
        }
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        if args.len() != 2 {
            return exec_err!("div requires 2 arguments, got {}", args.len());
        }
        let y = &args[0];
        let x = &args[1];
        if y.data_type() != x.data_type() {
            return exec_err!(
                "div requires both operands to share a type; got {:?} vs {:?}",
                y.data_type(),
                x.data_type()
            );
        }
        let out: ArrayRef = match y.data_type() {
            DataType::Int8 => Arc::new(div_int::<Int8Type>(y, x)?.finish()),
            DataType::Int16 => Arc::new(div_int::<Int16Type>(y, x)?.finish()),
            DataType::Int32 => Arc::new(div_int::<Int32Type>(y, x)?.finish()),
            DataType::Int64 => Arc::new(div_int::<Int64Type>(y, x)?.finish()),
            other => return exec_err!("div only supports integer operands, got {other:?}"),
        };
        Ok(ColumnarValue::Array(out))
    }
}

fn div_int<T>(y: &ArrayRef, x: &ArrayRef) -> Result<PrimitiveBuilder<T>>
where
    T: datafusion::arrow::datatypes::ArrowPrimitiveType,
    T::Native: std::ops::Div<Output = T::Native> + PartialOrd + Default,
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
            // Postgres raises on integer div-by-zero; we return NULL because
            // DataFusion cannot raise per-row errors.
            out.append_null();
        } else {
            out.append_value(y.value(j) / x.value(k));
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
        match col.data_type() {
            DataType::Int8 => col
                .as_primitive::<Int8Type>()
                .iter()
                .next()
                .unwrap()
                .map(|v| v as i64),
            DataType::Int16 => col
                .as_primitive::<Int16Type>()
                .iter()
                .next()
                .unwrap()
                .map(|v| v as i64),
            DataType::Int32 => col
                .as_primitive::<Int32Type>()
                .iter()
                .next()
                .unwrap()
                .map(|v| v as i64),
            DataType::Int64 => col.as_primitive::<Int64Type>().iter().next().unwrap(),
            _ => panic!("unexpected type {:?}", col.data_type()),
        }
    }

    #[tokio::test]
    async fn div_postgres_examples() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_div_udf());

        // From the Postgres docs:
        //   div(9, 4)   = 2
        //   div(-9, 4)  = -2
        //   div(9, -4)  = -2
        //   div(-9, -4) = 2
        assert_eq!(run_i64(&ctx, "SELECT div(9, 4)").await, Some(2));
        assert_eq!(run_i64(&ctx, "SELECT div(-9, 4)").await, Some(-2));
        assert_eq!(run_i64(&ctx, "SELECT div(9, -4)").await, Some(-2));
        assert_eq!(run_i64(&ctx, "SELECT div(-9, -4)").await, Some(2));
    }

    #[tokio::test]
    async fn div_by_zero_returns_null() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_div_udf());
        assert_eq!(run_i64(&ctx, "SELECT div(9, 0)").await, None);
    }
}
