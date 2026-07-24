//! PostgreSQL `random_normal(mean, stddev)` — normal-distributed random variate.
//!
//! Per the [PostgreSQL manual](https://www.postgresql.org/docs/current/functions-math.html#FUNCTIONS-MATH-RANDOM-TABLE):
//!
//! > `random_normal([mean, stddev])` → double precision
//! >
//! > Returns a random value from the normal distribution with the given
//! > `mean` (default 0.0) and `stddev` (default 1.0).
//!
//! The implementation uses the Box–Muller transform on top of `rand::random`,
//! matching Postgres's behavior for default arguments. Unlike the radian
//! trigonometric UDFs this function is **volatile** (each call yields a fresh
//! random value).
//!
//! # Argument handling
//!
//! DataFusion UDFs do not currently model optional-with-default arguments
//! directly, so we accept both the zero-arg form (`random_normal()`) and the
//! explicit form (`random_normal(mean, stddev)`). The `mean`-only and
//! `stddev`-only forms are not supported; require callers to pass both or
//! neither.

use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::array::{ArrayRef, AsArray, Float64Builder};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use rand::{Rng, rng};

/// Create the PostgreSQL `random_normal()` UDF.
pub fn create_random_normal_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RandomNormalUDF::default())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RandomNormalUDF {
    signature: Signature,
}

impl Default for RandomNormalUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl RandomNormalUDF {
    pub fn new() -> Self {
        // Accept either `random_normal()` (zero args, defaults),
        // `random_normal(mean, stddev)`, or any-arg variadic form.
        Self {
            signature: Signature::one_of(
                vec![
                    datafusion::logical_expr::TypeSignature::Nullary,
                    datafusion::logical_expr::TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    datafusion::logical_expr::TypeSignature::VariadicAny,
                ],
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for RandomNormalUDF {
    fn name(&self) -> &str {
        "random_normal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (mean, stddev): (f64, f64) = match arrays.len() {
            0 => (0.0, 1.0),
            2 => {
                let m = extract_scalar_f64(&arrays[0])?.unwrap_or(0.0);
                let s = extract_scalar_f64(&arrays[1])?.unwrap_or(1.0);
                if s < 0.0 {
                    return exec_err!("stddev must be non-negative, got {s}");
                }
                (m, s)
            }
            n => return exec_err!("random_normal expects 0 or 2 args, got {n}"),
        };

        // Result length is the batch size from the surrounding context. This
        // matches DataFusion's own `random()` UDF, which uses `args.number_rows`
        // so that `SELECT random() FROM generate_series(1, 100)` returns 100
        // distinct samples rather than a single scalar.
        let len = num_rows.max(1);
        let mut out = Float64Builder::with_capacity(len);
        for _ in 0..len {
            out.append_value(mean + stddev * box_muller());
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Pull the first row of `arr` as `Option<f64>`. Used to extract the scalar
/// `mean`/`stddev` arguments.
fn extract_scalar_f64(arr: &ArrayRef) -> Result<Option<f64>> {
    let a = arr.as_primitive::<Float64Type>();
    if a.len() == 1 && !a.is_null(0) {
        Ok(Some(a.value(0)))
    } else if a.is_empty() || a.is_null(0) {
        Ok(None)
    } else {
        Ok(Some(a.value(0)))
    }
}

/// Standard Box–Muller transform: produces a single N(0, 1) sample.
fn box_muller() -> f64 {
    let mut rng = rng();
    // Two independent uniforms in (0, 1]; rejection-sample u1 to avoid log(0).
    let u1: f64 = loop {
        let v = rng.random::<f64>();
        if v > f64::MIN_POSITIVE {
            break v;
        }
    };
    let u2: f64 = rng.random::<f64>();
    let r = (-2.0 * u1.ln()).sqrt();
    let theta = 2.0 * std::f64::consts::PI * u2;
    // Postgres uses a single sample per call; we discard the second variate.
    r * theta.cos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn run_f64_list(ctx: &SessionContext, sql: &str) -> Vec<Option<f64>> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_primitive::<Float64Type>()
            .iter()
            .collect()
    }

    #[tokio::test]
    async fn default_args_returns_finite_values() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_random_normal_udf());

        let vals = run_f64_list(&ctx, "SELECT random_normal() FROM generate_series(1, 100)").await;
        assert_eq!(vals.len(), 100);
        assert!(vals.iter().all(|v| v.is_some()));
        // Sanity: at least one value should be far enough from 0 to confirm we
        // are not just returning 0 (the probability of all 100 being within
        // 0.001 of 0 is essentially zero).
        assert!(
            vals.iter().filter_map(|v| *v).any(|v| v.abs() > 0.001),
            "samples suspiciously close to zero"
        );
    }

    #[tokio::test]
    async fn explicit_mean_stddev_shifts_distribution() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_random_normal_udf());

        let vals = run_f64_list(
            &ctx,
            "SELECT random_normal(100.0, 0.5) FROM generate_series(1, 1000)",
        )
        .await;
        let mean: f64 = vals.iter().filter_map(|v| *v).sum::<f64>() / vals.len() as f64;
        // Loose bounds: with N=1000 from N(100, 0.5), the sample mean is
        // within ~0.1 of 100 with overwhelming probability.
        assert!((mean - 100.0).abs() < 0.1, "got mean = {mean}");
    }

    #[tokio::test]
    async fn negative_stddev_errors() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_random_normal_udf());

        let res = ctx
            .sql("SELECT random_normal(0.0, -1.0)")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(
            res.is_err(),
            "negative stddev should be rejected at execution"
        );
    }
}
