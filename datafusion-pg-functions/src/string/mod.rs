//! PostgreSQL string functions.
//!
//! This module hosts the string UDFs listed in the "String Functions and
//! Operators" section of [`functions.md`](../functions.md). It is organized
//! as one file per logical group:
//!
//! - [`convert`]: `to_bin(integer)`, `to_oct(integer)` — integer-to-text
//!   base conversion.
//! - [`quote`]: `quote_literal(text)`, `quote_nullable(text)` — SQL quoting.
//! - [`unicode`]: `normalize(text)`, `casefold(text)`, `unicode_assigned(text)`,
//!   `unistr(text)` — Unicode string operations.
//! - [`format`]: `format(fmt, ...)`, `sprintf(fmt, ...)` — text formatting
//!   with `%s` / `%I` / `%L` specifiers.
//! - [`regexp`]: `regexp_substr(...)`, `regexp_split_to_array(...)` — regex
//!   helpers.
//! - [`encoding`]: `pg_client_encoding()`, `to_ascii(text)` — encoding
//!   utilities.
//!
//! Functions that DataFusion already provides with Postgres-compatible
//! semantics (`concat`, `lower`, `upper`, `length`, `replace`, `trim`, ...)
//! are *not* re-registered here.

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;

pub mod convert;
pub mod encoding;
pub mod format;
pub mod quote;
pub mod regexp;
pub mod unicode;

/// Register every PostgreSQL string UDF provided by this crate against
/// `registry`.
///
/// Returns the number of UDFs that were registered. Functions already
/// provided by DataFusion are not re-registered.
pub fn register(registry: &mut dyn FunctionRegistry) -> usize {
    let udfs: Vec<ScalarUDF> = vec![
        // convert
        convert::create_to_bin_udf(),
        convert::create_to_oct_udf(),
        // quote
        quote::create_quote_literal_udf(),
        quote::create_quote_nullable_udf(),
        // unicode
        unicode::create_normalize_udf(),
        unicode::create_casefold_udf(),
        unicode::create_unicode_assigned_udf(),
        unicode::create_unistr_udf(),
        // format (also registers `sprintf` as an alias)
        format::create_format_udf(),
        // regexp
        regexp::create_regexp_substr_udf(),
        regexp::create_regexp_split_to_array_udf(),
        // encoding
        encoding::create_pg_client_encoding_udf(),
        encoding::create_to_ascii_udf(),
    ];

    let mut count = 0;
    for udf in udfs {
        let _ = registry.register_udf(udf.into());
        count += 1;
    }
    count
}
