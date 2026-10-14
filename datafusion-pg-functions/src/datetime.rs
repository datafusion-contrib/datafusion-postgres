//! PostgreSQL date/time functions.
//!
//! This module hosts the UDFs listed in the "Date/Time Functions and
//! Operators" section of [`functions.md`](../functions.md), one file per
//! function:
//!
//! - [`justify_days`] — promote 30-day periods of an interval to months.
//! - [`justify_hours`] — promote 24-hour periods of an interval to days.
//! - [`justify_interval`] — normalize days and time, with sign adjustment.
//!
//! Shared interval input handling lives in the private `interval` module.
//!
//! See the corresponding section of `functions.md` for the catalog of
//! PostgreSQL built-ins in this category and their implementation status.

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;

mod interval;
pub mod justify_days;
pub mod justify_hours;
pub mod justify_interval;

/// Register every PostgreSQL built-in UDF in the datetime category against
/// `registry`.
///
/// Returns the number of UDFs that were registered.
pub fn register(registry: &mut dyn FunctionRegistry) -> usize {
    let udfs: Vec<ScalarUDF> = vec![
        justify_days::create_justify_days_udf(),
        justify_hours::create_justify_hours_udf(),
        justify_interval::create_justify_interval_udf(),
    ];

    let mut count = 0;
    for udf in udfs {
        let _ = registry.register_udf(udf.into());
        count += 1;
    }
    count
}
