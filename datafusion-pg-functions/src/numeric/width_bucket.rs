//! PostgreSQL `width_bucket(operand, low, high, count)` and
//! `width_bucket(operand, thresholds)` — histogram bucket assignment.
//!
//! Per the [PostgreSQL manual](https://www.postgresql.org/docs/current/functions-math.html):
//!
//! - `width_bucket(operand, low, high, count)` returns an integer indicating
//!   which of `count` equal-width buckets `operand` would fall into, with
//!   buckets numbered from 1. `operand` < `low` returns 0; `operand` >= `high`
//!   returns `count + 1`. `low`, `high`, and `count` must be positive numbers
//!   and `count > 0`.
//! - `width_bucket(operand, thresholds)` returns the index of the first
//!   threshold `operand` is less than, plus 1; if `operand` is below the
//!   lowest threshold it returns 0. `thresholds` must be sorted ascending.
//!
//! Both forms accept any numeric type. DataFusion does not ship either form.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveBuilder};

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{
    DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Create the PostgreSQL `width_bucket` UDF (supports both forms).
pub fn create_width_bucket_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(WidthBucketUDF::default())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct WidthBucketUDF {
    signature: Signature,
}

impl Default for WidthBucketUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl WidthBucketUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // 4-arg equal-width form: (operand, low, high, count)
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int32,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float32,
                        DataType::Float32,
                        DataType::Float32,
                        DataType::Int32,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float32,
                        DataType::Float32,
                        DataType::Float32,
                        DataType::Int64,
                    ]),
                    // 2-arg sorted-thresholds form: (operand, thresholds[])
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for WidthBucketUDF {
    fn name(&self) -> &str {
        "width_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        match arrays.len() {
            4 => equal_width_form(&arrays),
            2 => thresholds_form(&arrays),
            n => exec_err!("width_bucket expects 2 or 4 arguments, got {n}"),
        }
    }
}

/// `width_bucket(operand, low, high, count)` — equal-width form.
fn equal_width_form(args: &[ArrayRef]) -> Result<ColumnarValue> {
    let operand = &args[0];
    let low = &args[1];
    let high = &args[2];
    let count = &args[3];

    if operand.data_type() != low.data_type() || operand.data_type() != high.data_type() {
        return exec_err!(
            "width_bucket requires operand/low/high to share a type; got {:?}/{:?}/{:?}",
            operand.data_type(),
            low.data_type(),
            high.data_type()
        );
    }

    let counts: Option<i64> = match count.data_type() {
        DataType::Int32 => count
            .as_primitive::<Int32Type>()
            .iter()
            .next()
            .flatten()
            .map(|v| v as i64),
        DataType::Int64 => count.as_primitive::<Int64Type>().iter().next().flatten(),
        other => return exec_err!("width_bucket count must be int4/int8, got {other:?}"),
    };
    let Some(count) = counts else {
        return exec_err!("width_bucket count must be a positive integer");
    };
    if count <= 0 {
        return exec_err!("width_bucket count must be a positive integer");
    }

    let mut out: PrimitiveBuilder<Int32Type> = PrimitiveBuilder::with_capacity(operand.len());
    for i in 0..operand.len() {
        let (Some(op), Some(lo), Some(hi)) =
            (scalar_f64(operand, i), scalar_f64(low, 0), scalar_f64(high, 0))
        else {
            out.append_null();
            continue;
        };
        if lo >= hi {
            return exec_err!("width_bucket requires low < high, got low={lo}, high={hi}");
        }
        let bucket = equal_width_bucket_f64(op, lo, hi, count);
        out.append_value(bucket as i32);
    }
    Ok(ColumnarValue::Array(Arc::new(out.finish())))
}

fn equal_width_bucket_f64(op: f64, low: f64, high: f64, count: i64) -> i64 {
    // Postgres semantics: operand < low -> 0; operand >= high -> count+1;
    // otherwise 1 + floor(count * (op - low) / (high - low)).
    if op.is_nan() {
        // Postgres treats NaN the same as "below low" (returns 0).
        return 0;
    }
    if op < low {
        0
    } else if op >= high {
        count + 1
    } else {
        let step = (high - low) / count as f64;
        let b = ((op - low) / step).floor() as i64 + 1;
        // Clamp in case of float drift just at the boundary.
        b.clamp(1, count)
    }
}

/// `width_bucket(operand, thresholds)` — sorted-array form.
fn thresholds_form(args: &[ArrayRef]) -> Result<ColumnarValue> {
    let operand = &args[0];
    let thresholds = &args[1];

    let Some(thresholds_list) = as_f64_list(thresholds) else {
        return exec_err!(
            "width_bucket thresholds must be a sorted list of f64, got {:?}",
            thresholds.data_type()
        );
    };

    let mut out: PrimitiveBuilder<Int32Type> = PrimitiveBuilder::with_capacity(operand.len());
    for i in 0..operand.len() {
        let Some(op) = scalar_f64(operand, i) else {
            out.append_null();
            continue;
        };
        // Postgres: returns the bucket number = (index of first threshold that
        // op <) + 0 if op is below the first threshold. Equivalently, number
        // of thresholds op >=, clamped to [0, len].
        if op.is_nan() {
            out.append_value(0);
            continue;
        }
        let bucket = thresholds_list.iter().filter(|&&t| op >= t).count();
        out.append_value(bucket as i32);
    }
    Ok(ColumnarValue::Array(Arc::new(out.finish())))
}

/// Read row `i` of `arr` as `Option<f64>`. Only meaningful for primitive
/// numeric arrays; non-numeric types return an error upstream.
fn scalar_f64(arr: &ArrayRef, i: usize) -> Option<f64> {
    if arr.is_null(i) {
        return None;
    }
    match arr.data_type() {
        DataType::Float64 => Some(arr.as_primitive::<Float64Type>().value(i)),
        DataType::Float32 => Some(arr.as_primitive::<Float32Type>().value(i) as f64),
        DataType::Int64 => Some(arr.as_primitive::<Int64Type>().value(i) as f64),
        DataType::Int32 => Some(arr.as_primitive::<Int32Type>().value(i) as f64),
        DataType::Int16 => Some(arr.as_primitive::<Int16Type>().value(i) as f64),
        DataType::Int8 => Some(arr.as_primitive::<Int8Type>().value(i) as f64),
        _ => None,
    }
}

fn as_f64_list(arr: &ArrayRef) -> Option<Vec<f64>> {
    let list = arr.as_list::<i32>();
    if list.len() != 1 || list.is_null(0) {
        return None;
    }
    let row = list.value(0);
    let f = row.as_primitive::<Float64Type>();
    Some(f.iter().map(|v| v.unwrap_or(f64::NAN)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn run_i32(ctx: &SessionContext, sql: &str) -> Option<i32> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).as_primitive::<Int32Type>().iter().next().unwrap()
    }

    #[tokio::test]
    async fn equal_width_postgres_examples() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_width_bucket_udf());

        // From the Postgres docs:
        //   width_bucket(5.35, 0.024, 10.06, 5) = 3
        //   width_bucket(-0.4, 0.024, 10.06, 5) = 0   (below low)
        //   width_bucket(11.0, 0.024, 10.06, 5) = 6   (above high)
        assert_eq!(
            run_i32(&ctx, "SELECT width_bucket(5.35, 0.024, 10.06, 5)").await,
            Some(3)
        );
        assert_eq!(
            run_i32(&ctx, "SELECT width_bucket(-0.4, 0.024, 10.06, 5)").await,
            Some(0)
        );
        assert_eq!(
            run_i32(&ctx, "SELECT width_bucket(11.0, 0.024, 10.06, 5)").await,
            Some(6)
        );
    }

    #[tokio::test]
    async fn thresholds_form() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_width_bucket_udf());

        // thresholds = [10, 20, 30] (sorted ascending).
        //   25 >= [10, 20], not >= 30 -> 2
        //   5  >= none -> 0
        //   30 >= all three -> 3
        assert_eq!(
            run_i32(&ctx, "SELECT width_bucket(25, ARRAY[10.0, 20.0, 30.0])").await,
            Some(2)
        );
        assert_eq!(
            run_i32(&ctx, "SELECT width_bucket(5, ARRAY[10.0, 20.0, 30.0])").await,
            Some(0)
        );
        assert_eq!(
            run_i32(&ctx, "SELECT width_bucket(30, ARRAY[10.0, 20.0, 30.0])").await,
            Some(3)
        );
    }

    #[tokio::test]
    async fn null_operand_returns_null() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_width_bucket_udf());
        assert_eq!(
            run_i32(
                &ctx,
                "SELECT width_bucket(CAST(NULL AS DOUBLE), 0.0, 10.0, 5)"
            )
            .await,
            None
        );
    }
}
