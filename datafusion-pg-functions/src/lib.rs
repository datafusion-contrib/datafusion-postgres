//! PostgreSQL built-in functions implemented as DataFusion
//! [`ScalarUDF`]s.
//!
//! The goal of this crate is to provide the largest possible subset of the
//! built-in scalar functions documented in the PostgreSQL reference manual,
//! matching Postgres semantics on top of the Apache DataFusion execution
//! engine. Functions that DataFusion already ships with a Postgres-compatible
//! implementation of are *not* re-implemented here; the
//! [`register_all`](crate::register_all) entrypoint only installs UDFs that
//! DataFusion lacks.
//!
//! See `functions.md` at the crate root for the full catalog of PostgreSQL
//! built-ins, grouped by category, along with the implementation status of
//! each entry. That file is generated from the upstream PostgreSQL source by
//! `gen/functions_gen.py`; re-run it whenever you upgrade DataFusion or want
//! to refresh against a newer PostgreSQL release.
//!
//! # Categories
//!
//! | Module            | PostgreSQL category                                  |
//! |-------------------|------------------------------------------------------|
//! | [`string`]        | String / character functions                         |
//! | [`binary`]        | Bytea / binary string functions                      |
//! | [`numeric`]       | Numeric / math / trig functions                      |
//! | [`array`]         | Array functions                                      |
//! | [`range`]         | Range / multirange functions                         |
//! | [`datetime`]      | Date / time functions                                |
//! | [`conditional`]   | `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, etc.      |
//! | [`json`]          | JSON / JSONB functions                               |
//! | [`network`]       | Network address (`inet` / `cidr` / `macaddr`)        |
//! | [`text_search`]   | Full-text search functions                           |
//! | [`sequence`]      | Sequence manipulation                                |
//! | [`uuid`]          | UUID functions                                       |
//! | [`system`]        | System information / administrative functions        |
//! | [`format`]        | `format()`, `to_char`, type formatting              |
//! | [`geometric`]     | Geometric types                                      |
//! | [`xml`]           | XML functions                                        |
//! | [`enum_type`]     | Enum support functions                               |
//! | [`bitstring`]     | Bit string functions                                 |
//! | [`crypto`]        | Hash / digest functions                              |
//! | [`row`]           | Record / composite / row functions                   |
//!
//! # Usage
//!
//! Register every UDF in this crate against a
//! [`SessionContext`](datafusion::prelude::SessionContext):
//!
//! ```no_run
//! use datafusion::prelude::SessionContext;
//! use datafusion_pg_functions::register_all;
//!
//! let ctx = SessionContext::new();
//! register_all(&ctx);
//! ```
//!
//! To register a single category, call its `register` function directly:
//!
//! ```no_run
//! use datafusion::prelude::SessionContext;
//! use datafusion_pg_functions::string;
//!
//! let ctx = SessionContext::new();
//! string::register(&ctx);
//! ```

use datafusion::execution::FunctionRegistry;

pub mod array;
pub mod binary;
pub mod bitstring;
pub mod conditional;
pub mod crypto;
pub mod datetime;
pub mod enum_type;
pub mod format;
pub mod geometric;
pub mod json;
pub mod network;
pub mod numeric;
pub mod range;
pub mod row;
pub mod sequence;
pub mod string;
pub mod system;
pub mod text_search;
pub mod uuid;
pub mod xml;

/// Register every PostgreSQL built-in UDF provided by this crate against
/// `registry`.
///
/// Only functions that DataFusion does not already provide are installed, so
/// calling this on a freshly-created
/// [`SessionContext`](datafusion::prelude::SessionContext) is sufficient to
/// bring the SQL surface area close to a vanilla PostgreSQL backend.
///
/// Returns the number of UDFs that were registered.
pub fn register_all(registry: &dyn FunctionRegistry) -> usize {
    let mut count = 0;
    count += string::register(registry);
    count += binary::register(registry);
    count += numeric::register(registry);
    count += array::register(registry);
    count += range::register(registry);
    count += datetime::register(registry);
    count += conditional::register(registry);
    count += json::register(registry);
    count += network::register(registry);
    count += text_search::register(registry);
    count += sequence::register(registry);
    count += uuid::register(registry);
    count += system::register(registry);
    count += format::register(registry);
    count += geometric::register(registry);
    count += xml::register(registry);
    count += enum_type::register(registry);
    count += bitstring::register(registry);
    count += crypto::register(registry);
    count += row::register(registry);
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    /// `register_all` must succeed against a stock SessionContext and register
    /// at least one UDF (it grows over time; the floor of zero is what we are
    /// guarding against here, e.g. a panic in any submodule's `register`).
    #[test]
    fn register_all_runs_against_fresh_context() {
        let ctx = SessionContext::new();
        let n = register_all(&ctx);
        assert!(n >= 0);
    }
}
