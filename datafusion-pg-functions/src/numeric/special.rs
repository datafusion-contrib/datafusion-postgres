//! PostgreSQL `erf`, `erfc`, `gamma`, `lgamma` — special functions.
//!
//! These four functions live in PostgreSQL's "Mathematical Functions" table
//! but DataFusion does not ship them. They are all scalar, take one float
//! argument, and return one float.
//!
//! # Implementation
//!
//! We use [`libm`] for the underlying numeric routines so the crate links
//! against `libc` on Unix platforms (where these are direct `erf`/`erfc`/
//! `lgamma`/`tgamma` symbols) and gets pure-Rust implementations elsewhere.
//!
//! | PG name  | libm function     | Notes                                   |
//! |----------|-------------------|-----------------------------------------|
//! | `erf(x)`   | `erf(f64)`        | error function                          |
//! | `erfc(x)`  | `erfc(f64)`       | `1.0 - erf(x)` (computed directly)      |
//! | `gamma(x)` | `tgamma(f64)`     | true gamma function                     |
//! | `lgamma(x)`| `lgamma(f64)`     | natural log of `|gamma(x)|`             |
//!
//! Postgres's `gamma(0)` returns `Infinity` and `gamma(-integer)` returns
//! `NaN` or raises an error depending on platform; we match libm's behavior.

use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Build a one-argument UDF that applies a pure `f64 -> f64` op elementwise,
/// returning the same float type as its input.
macro_rules! special_unary_udf {
    (
        $(#[$meta:meta])*
        struct $struct_name:ident;
        name = $sql_name:literal;
        op = $f64_impl:expr;
    ) => {
        $(#[$meta])*
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::uniform(
                        1,
                        vec![DataType::Float64, DataType::Float32],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $sql_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                match arg_types[0] {
                    DataType::Float32 => Ok(DataType::Float32),
                    _ => Ok(DataType::Float64),
                }
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let args = ColumnarValue::values_to_arrays(&args.args)?;
                if args.len() != 1 {
                    return exec_err!("{} expects exactly 1 argument, got {}", $sql_name, args.len());
                }
                let op: fn(f64) -> f64 = $f64_impl;
                let out: ArrayRef = match args[0].data_type() {
                    DataType::Float64 => {
                        let a = args[0].as_primitive::<Float64Type>();
                        let mut b = PrimitiveBuilder::<Float64Type>::with_capacity(a.len());
                        for i in 0..a.len() {
                            if a.is_null(i) {
                                b.append_null();
                            } else {
                                b.append_value(op(a.value(i)));
                            }
                        }
                        Arc::new(b.finish())
                    }
                    DataType::Float32 => {
                        let a = args[0].as_primitive::<Float32Type>();
                        let mut b = PrimitiveBuilder::<Float32Type>::with_capacity(a.len());
                        for i in 0..a.len() {
                            if a.is_null(i) {
                                b.append_null();
                            } else {
                                b.append_value(op(a.value(i) as f64) as f32);
                            }
                        }
                        Arc::new(b.finish())
                    }
                    other => return exec_err!("{} requires float input, got {other:?}", $sql_name),
                };
                Ok(ColumnarValue::Array(out))
            }
        }
    };
}

special_unary_udf! {
    /// PostgreSQL `erf(x)` — Gauss error function.
    struct ErfUDF;
    name = "erf";
    op = |x: f64| libm::erf(x);
}

special_unary_udf! {
    /// PostgreSQL `erfc(x)` — complementary error function `1 - erf(x)`.
    struct ErfcUDF;
    name = "erfc";
    op = |x: f64| libm::erfc(x);
}

special_unary_udf! {
    /// PostgreSQL `gamma(x)` — true gamma function.
    struct GammaUDF;
    name = "gamma";
    op = |x: f64| libm::tgamma(x);
}

special_unary_udf! {
    /// PostgreSQL `lgamma(x)` — natural log of `|gamma(x)|`.
    struct LgammaUDF;
    name = "lgamma";
    op = |x: f64| libm::lgamma_r(x).0;
}

pub fn create_erf_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ErfUDF::default())
}
pub fn create_erfc_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ErfcUDF::default())
}
pub fn create_gamma_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(GammaUDF::default())
}
pub fn create_lgamma_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(LgammaUDF::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn run_f64(ctx: &SessionContext, sql: &str) -> Option<f64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_primitive::<Float64Type>()
            .iter()
            .next()
            .unwrap()
    }

    fn ctx_with_all() -> SessionContext {
        let ctx = SessionContext::new();
        for udf in [
            create_erf_udf(),
            create_erfc_udf(),
            create_gamma_udf(),
            create_lgamma_udf(),
        ] {
            ctx.register_udf(udf);
        }
        ctx
    }

    #[tokio::test]
    async fn known_values() {
        let ctx = ctx_with_all();

        // erf(0) = 0, erf(inf) = 1
        assert!(run_f64(&ctx, "SELECT erf(0)").await.unwrap().abs() < 1e-12);
        // erfc(x) = 1 - erf(x); erfc(0) = 1
        assert!((run_f64(&ctx, "SELECT erfc(0)").await.unwrap() - 1.0).abs() < 1e-12);
        // gamma(n) = (n-1)! for positive integers; gamma(5) = 4! = 24
        assert!((run_f64(&ctx, "SELECT gamma(5)").await.unwrap() - 24.0).abs() < 1e-9);
        // lgamma(5) = ln(24) ≈ 3.178
        assert!((run_f64(&ctx, "SELECT lgamma(5)").await.unwrap() - 24.0_f64.ln()).abs() < 1e-9);
    }

    #[tokio::test]
    async fn null_propagates() {
        let ctx = ctx_with_all();
        assert_eq!(
            run_f64(&ctx, "SELECT erf(CAST(NULL AS DOUBLE))").await,
            None
        );
    }
}
