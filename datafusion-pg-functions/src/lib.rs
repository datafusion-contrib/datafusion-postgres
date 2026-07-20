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
//! each entry.
//!
//! # Cargo features
//!
//! Each PostgreSQL function category lives behind its own Cargo feature so
//! downstream crates can pay only for what they use. The `math` category is
//! enabled by default; pull in additional categories by enabling the matching
//! feature, or enable `full` for everything.
//!
//! ```toml
//! # Just math (the default)
//! datafusion-pg-functions = "0.1"
//!
//! # Math + string
//! datafusion-pg-functions = { version = "0.1", features = ["string"] }
//!
//! # Everything
//! datafusion-pg-functions = { version = "0.1", features = ["full"] }
//! ```
//!
//! Feature names match the PostgreSQL manual's category names. The single
//! naming exception is `math` (the feature) ↔ [`numeric`] (the module
//! directory); every other feature maps 1:1 to a module of the same name.
//!
//! | Feature         | Module            | PostgreSQL category                          |
//! |-----------------|-------------------|----------------------------------------------|
//! | `math`          | [`numeric`]       | Mathematical Functions and Operators         |
//! | `string`        | [`string`]        | String Functions and Operators               |
//! | `binary`        | [`binary`]        | Binary String Functions and Operators        |
//! | `bitstring`     | [`bitstring`]     | Bit String Functions and Operators           |
//! | `conditional`   | [`conditional`]   | Conditional / Comparison / Logical           |
//! | `datetime`      | [`datetime`]      | Date/Time Functions and Operators            |
//! | `crypto`        | [`crypto`]        | Hash / digest helpers                        |
//! | `array`         | [`array`]         | Array Functions and Operators                |
//! | `range`         | [`range`]         | Range / Multirange Functions and Operators   |
//! | `json`          | [`json`]          | JSON / JSONB Functions and Operators         |
//! | `network`       | [`network`]       | Network Address Functions and Operators      |
//! | `text_search`   | [`text_search`]   | Text Search Functions                        |
//! | `uuid`          | [`uuid`]          | UUID Functions                               |
//! | `xml`           | [`xml`]           | XML Functions                                |
//! | `system`        | [`system`]        | System Information / Administration          |
//! | `format`        | [`format`]        | Data Type Formatting Functions               |
//! | `geometric`     | [`geometric`]     | Geometric Functions and Operators            |
//! | `enum_type`     | [`enum_type`]     | Enum Support Functions                       |
//! | `sequence`      | [`sequence`]      | Sequence Manipulation Functions              |
//! | `row`           | [`row`]           | Row / Record Functions                       |
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
//! let mut ctx = SessionContext::new();
//! register_all(&mut ctx);
//! ```
//!
//! To register a single category, call its `register` function directly.
//! For example, with the `math` feature enabled:
//!
//! ```ignore
//! use datafusion::prelude::SessionContext;
//! use datafusion_pg_functions::numeric;
//!
//! let mut ctx = SessionContext::new();
//! numeric::register(&mut ctx);
//! ```

use datafusion::execution::FunctionRegistry;

// One module per PostgreSQL function category. Each module is gated by its
// matching Cargo feature (see lib.rs docs / Cargo.toml `[features]`).
#[cfg(feature = "math")]
pub mod numeric;
#[cfg(feature = "string")]
pub mod string;
#[cfg(feature = "binary")]
pub mod binary;
#[cfg(feature = "bitstring")]
pub mod bitstring;
#[cfg(feature = "conditional")]
pub mod conditional;
#[cfg(feature = "datetime")]
pub mod datetime;
#[cfg(feature = "crypto")]
pub mod crypto;
#[cfg(feature = "array")]
pub mod array;
#[cfg(feature = "range")]
pub mod range;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "network")]
pub mod network;
#[cfg(feature = "text_search")]
pub mod text_search;
#[cfg(feature = "uuid")]
pub mod uuid;
#[cfg(feature = "xml")]
pub mod xml;
#[cfg(feature = "system")]
pub mod system;
#[cfg(feature = "format")]
pub mod format;
#[cfg(feature = "geometric")]
pub mod geometric;
#[cfg(feature = "enum_type")]
pub mod enum_type;
#[cfg(feature = "sequence")]
pub mod sequence;
#[cfg(feature = "row")]
pub mod row;

/// Register every PostgreSQL built-in UDF provided by this crate against
/// `registry`, across every category whose Cargo feature is enabled.
///
/// Only functions that DataFusion does not already provide are installed, so
/// calling this on a freshly-created
/// [`SessionContext`](datafusion::prelude::SessionContext) brings the SQL
/// surface area closer to a vanilla PostgreSQL backend for whichever
/// categories are compiled in.
///
/// Returns the number of UDFs that were registered.
#[cfg_attr(
    not(any(
        feature = "math",
        feature = "string",
        feature = "binary",
        feature = "bitstring",
        feature = "conditional",
        feature = "datetime",
        feature = "crypto",
        feature = "array",
        feature = "range",
        feature = "json",
        feature = "network",
        feature = "text_search",
        feature = "uuid",
        feature = "xml",
        feature = "system",
        feature = "format",
        feature = "geometric",
        feature = "enum_type",
        feature = "sequence",
        feature = "row",
    )),
    allow(unused_variables, unused_mut)
)]
pub fn register_all(registry: &mut dyn FunctionRegistry) -> usize {
    let mut count = 0;
    #[cfg(feature = "math")]
    {
        count += numeric::register(registry);
    }
    #[cfg(feature = "string")]
    {
        count += string::register(registry);
    }
    #[cfg(feature = "binary")]
    {
        count += binary::register(registry);
    }
    #[cfg(feature = "bitstring")]
    {
        count += bitstring::register(registry);
    }
    #[cfg(feature = "conditional")]
    {
        count += conditional::register(registry);
    }
    #[cfg(feature = "datetime")]
    {
        count += datetime::register(registry);
    }
    #[cfg(feature = "crypto")]
    {
        count += crypto::register(registry);
    }
    #[cfg(feature = "array")]
    {
        count += array::register(registry);
    }
    #[cfg(feature = "range")]
    {
        count += range::register(registry);
    }
    #[cfg(feature = "json")]
    {
        count += json::register(registry);
    }
    #[cfg(feature = "network")]
    {
        count += network::register(registry);
    }
    #[cfg(feature = "text_search")]
    {
        count += text_search::register(registry);
    }
    #[cfg(feature = "uuid")]
    {
        count += uuid::register(registry);
    }
    #[cfg(feature = "xml")]
    {
        count += xml::register(registry);
    }
    #[cfg(feature = "system")]
    {
        count += system::register(registry);
    }
    #[cfg(feature = "format")]
    {
        count += format::register(registry);
    }
    #[cfg(feature = "geometric")]
    {
        count += geometric::register(registry);
    }
    #[cfg(feature = "enum_type")]
    {
        count += enum_type::register(registry);
    }
    #[cfg(feature = "sequence")]
    {
        count += sequence::register(registry);
    }
    #[cfg(feature = "row")]
    {
        count += row::register(registry);
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    /// `register_all` must succeed against a stock SessionContext. With the
    /// default features this registers the math UDFs; with `--no-default-features`
    /// it registers nothing and returns 0.
    #[test]
    fn register_all_runs_against_fresh_context() {
        let mut ctx = SessionContext::new();
        let n = register_all(&mut ctx);
        #[cfg(feature = "math")]
        assert!(n > 0, "default features should register math UDFs");
        #[cfg(not(feature = "math"))]
        assert_eq!(n, 0, "with no features enabled, nothing is registered");
    }

    /// With `default-features = false` and no features selected, the crate
    /// must still compile and `register_all` must be a no-op returning 0.
    #[cfg(not(any(
        feature = "math",
        feature = "string",
        feature = "binary",
        feature = "bitstring",
        feature = "conditional",
        feature = "datetime",
        feature = "crypto",
        feature = "array",
        feature = "range",
        feature = "json",
        feature = "network",
        feature = "text_search",
        feature = "uuid",
        feature = "xml",
        feature = "system",
        feature = "format",
        feature = "geometric",
        feature = "enum_type",
        feature = "sequence",
        feature = "row",
    )))]
    #[test]
    fn no_features_compiles_and_is_noop() {
        let mut ctx = SessionContext::new();
        assert_eq!(register_all(&mut ctx), 0);
    }
}
