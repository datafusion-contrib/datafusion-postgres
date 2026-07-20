//! PostgreSQL `ceiling` and `sign` — thin aliases of DataFusion built-ins.
//!
//! PostgreSQL documents both `ceiling(x)` (as an alias of `ceil(x)`) and
//! `sign(x)`. DataFusion implements the underlying behavior as `ceil` and
//! `signum` respectively but does not register the Postgres alias names, so
//! queries such as `SELECT ceiling(price) FROM ...` fail to resolve.
//!
//! Both functions here are constructed via
//! [`ScalarUDF::with_aliases`](datafusion::logical_expr::ScalarUDF::with_aliases),
//! which delegates every call to the existing DataFusion implementation while
//! exposing the Postgres name to the SQL planner.

use datafusion::logical_expr::ScalarUDF;

/// PostgreSQL `ceiling(numeric)` — identical to `ceil(numeric)`.
///
/// Registered as an alias of DataFusion's built-in [`ceil`] UDF so that
/// `ceiling(x)` and `ceil(x)` resolve to the same plan node.
///
/// [`ceil`]: datafusion::functions::math::ceil
pub fn create_ceiling_udf() -> ScalarUDF {
    (*datafusion::functions::math::ceil())
        .clone()
        .with_aliases(["ceiling"])
}

/// PostgreSQL `sign(numeric)` — identical to DataFusion `signum(numeric)`.
///
/// Registered as an alias of DataFusion's built-in [`signum`] UDF.
///
/// Postgres returns `-1`, `0`, or `+1` with the same sign as the input (and
/// `NULL` for `NULL`); DataFusion's `signum` matches that contract.
///
/// [`signum`]: datafusion::functions::math::signum
pub fn create_sign_udf() -> ScalarUDF {
    (*datafusion::functions::math::signum())
        .clone()
        .with_aliases(["sign"])
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Float64Type;
    use datafusion::prelude::SessionContext;

    /// Run `sql`, return the single f64 cell of the first row.
    async fn run_f64(ctx: &SessionContext, sql: &str) -> Option<f64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        col.as_primitive::<Float64Type>().iter().next().unwrap()
    }

    #[tokio::test]
    async fn ceiling_resolves_and_matches_ceil() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_ceiling_udf().into());

        for x in ["2.4", "-2.4", "0.0", "5.0"] {
            assert_eq!(
                run_f64(&ctx, &format!("SELECT ceil({x})")).await,
                run_f64(&ctx, &format!("SELECT ceiling({x})")).await,
                "mismatch for x={x}",
            );
        }
    }

    #[tokio::test]
    async fn sign_resolves_and_matches_signum() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_sign_udf().into());

        for x in ["-7", "0", "3.5", "-3.5"] {
            assert_eq!(
                run_f64(&ctx, &format!("SELECT signum({x})")).await,
                run_f64(&ctx, &format!("SELECT sign({x})")).await,
                "mismatch for x={x}",
            );
        }
    }
}
