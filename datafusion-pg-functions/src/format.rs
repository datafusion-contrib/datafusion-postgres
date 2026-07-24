//! format functions.
//!
//! See the corresponding section of `functions.md` for the catalog of
//! PostgreSQL built-ins in this category and their implementation status.

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;

/// Register every PostgreSQL built-in UDF in the format category against
/// `registry`.
///
/// Returns the number of UDFs that were registered.
pub fn register(_registry: &mut dyn FunctionRegistry) -> usize {
    let _udfs: Vec<ScalarUDF> = vec![];
    // registry.register_udf(...);
    0
}
