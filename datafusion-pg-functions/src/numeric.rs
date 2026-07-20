//! PostgreSQL mathematical / numeric functions.
//!
//! This module hosts the math UDFs listed in the "Mathematical Functions and
//! Operators" section of [`functions.md`](../functions.md). It is organized as
//! one file per logical group:
//!
//! - [`aliases`]: `ceiling`, `sign` — thin aliases of DataFusion's `ceil` /
//!   `signum`.
//! - [`mod_op`]: `mod(y, x)` — modulus function (DataFusion only ships the
//!   `%` operator).
//! - [`degree_trig`]: the eight degree-variant trig functions (`sind`,
//!   `cosd`, ..., `atan2d`).
//! - [`div`]: integer quotient `div(y, x)`.
//! - [`random_normal`]: normal-distributed random variate.
//! - [`width_bucket`]: histogram bucket assignment.
//! - [`special`]: `erf`, `erfc`, `gamma`, `lgamma` — special functions backed
//!   by `libm` via the [`special-funs`] crate.
//!
//! Functions that DataFusion already provides with Postgres-compatible
//! semantics (`abs`, `acos`, `cbrt`, `gcd`, ...) are *not* re-registered here.
//!
//! [`special-funs`]: https://crates.io/crates/special

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;

pub mod aliases;
pub mod degree_trig;
pub mod div;
pub mod mod_op;
pub mod random_normal;
pub mod special;
pub mod width_bucket;

/// Register every PostgreSQL math UDF provided by this crate against
/// `registry`.
///
/// Returns the number of UDFs that were registered. Functions already
/// provided by DataFusion are not re-registered.
pub fn register(registry: &mut dyn FunctionRegistry) -> usize {
    let udfs: Vec<ScalarUDF> = vec![
        aliases::create_ceiling_udf(),
        aliases::create_sign_udf(),
        mod_op::create_mod_udf(),
        degree_trig::create_sind_udf(),
        degree_trig::create_cosd_udf(),
        degree_trig::create_tand_udf(),
        degree_trig::create_cotd_udf(),
        degree_trig::create_asind_udf(),
        degree_trig::create_acosd_udf(),
        degree_trig::create_atand_udf(),
        degree_trig::create_atan2d_udf(),
        div::create_div_udf(),
        random_normal::create_random_normal_udf(),
        width_bucket::create_width_bucket_udf(),
        special::create_erf_udf(),
        special::create_erfc_udf(),
        special::create_gamma_udf(),
        special::create_lgamma_udf(),
    ];

    let mut count = 0;
    for udf in udfs {
        let _ = registry.register_udf(udf.into());
        count += 1;
    }
    count
}
