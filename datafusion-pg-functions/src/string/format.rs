//! PostgreSQL `format(fmt, ...)` and `sprintf(fmt, ...)` — text formatting.
//!
//! <https://www.postgresql.org/docs/current/functions-string.html>
//!
//! ## Postgres compatibility
//!
//! Implements the documented `format()` grammar exactly:
//! `format(formatstr text [, VARIADIC "any"])` with specifiers
//!
//! * `%[position]s` — format the argument as a simple string (NULL → empty).
//! * `%[position]I` — format the argument as an SQL identifier, double-quoted
//!   unless it is a bare identifier (lowercase ASCII letters/digits/underscore,
//!   not starting with a digit). Deviation from Postgres: the reserved-word
//!   check is not implemented, so `format('%I', 'select')` renders unquoted
//!   where Postgres would quote it.
//! * `%[position]L` — format the argument as an SQL literal (`quote_nullable`).
//! * `%%` — a literal `%`.
//!
//! `[position]` is `N$` for the 1-based argument index. When omitted, the next
//! automatic argument is consumed. Width/precision/flags are **not** part of
//! Postgres' grammar and are rejected with an error. `sprintf` (PG 18+) is an
//! alias of `format`.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Render `s` as an SQL identifier, double-quoting when it is not already a
/// bare identifier (lowercase ASCII letters/digits/underscore, not starting
/// with a digit). Mirrors the common case of Postgres' `quote_ident` — the
/// reserved-word check is not implemented (see the module docs).
fn pg_quote_ident(s: &str) -> String {
    let is_bare = !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !s.bytes().next().unwrap().is_ascii_digit();
    if is_bare {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

/// Render `s` as a single-quoted SQL literal, doubling single quotes.
/// (Backslashes are left as-is — standard_conforming_strings = on.)
fn pg_quote_literal_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn format_spec(spec: char, val: Option<&String>) -> Result<String> {
    match spec {
        's' => Ok(val.map(String::as_str).unwrap_or("").to_string()),
        'I' => Ok(pg_quote_ident(val.map(String::as_str).unwrap_or(""))),
        'L' => match val {
            Some(s) => Ok(pg_quote_literal_value(s)),
            None => Ok("NULL".to_string()),
        },
        other => Err(DataFusionError::Execution(format!(
            "format: unrecognized format specifier '%{other}' \
             (Postgres allows only %s, %I, %L)"
        ))),
    }
}

fn pg_format(fmt: &str, args: &[Option<String>]) -> Result<String> {
    let mut out = String::with_capacity(fmt.len() * 2);
    let mut chars = fmt.chars().peekable();
    let mut auto_idx: usize = 0;

    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        // %% -> literal %
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }

        // Optional positional index: digits followed by '$'.
        let mut pos_idx: Option<usize> = None;
        let mut digit_buf = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                digit_buf.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if !digit_buf.is_empty() {
            if chars.peek() == Some(&'$') {
                chars.next();
                let n: usize = digit_buf.parse().map_err(|_| {
                    DataFusionError::Execution(format!(
                        "format: invalid positional index '{digit_buf}'"
                    ))
                })?;
                if n == 0 {
                    return Err(DataFusionError::Execution(
                        "format: positional index must be >= 1".into(),
                    ));
                }
                pos_idx = Some(n - 1);
            } else {
                // Postgres' grammar has no width/precision: digits that are not
                // part of an `N$` position are not a valid specifier.
                let consumed = chars.next();
                return Err(DataFusionError::Execution(format!(
                    "format: unrecognized format specifier '%{digit_buf}{}' \
                     (width/flags are not supported)",
                    consumed.unwrap_or_default()
                )));
            }
        }

        let spec = chars.next().ok_or_else(|| {
            DataFusionError::Execution("format: incomplete format specifier".into())
        })?;
        let arg_i = pos_idx.unwrap_or_else(|| {
            let i = auto_idx;
            auto_idx += 1;
            i
        });
        let val = args.get(arg_i).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "format: too few arguments (need at least {}, got {})",
                arg_i + 1,
                args.len()
            ))
        })?;
        out.push_str(&format_spec(spec, val.as_ref())?);
    }
    Ok(out)
}

fn scalar_to_opt_string(sv: &ColumnarValue) -> Option<String> {
    match sv {
        ColumnarValue::Scalar(s) => match s {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v.clone(),
            ScalarValue::Int32(Some(v)) => Some(v.to_string()),
            ScalarValue::Int64(Some(v)) => Some(v.to_string()),
            ScalarValue::Float64(Some(v)) => Some(v.to_string()),
            ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
            ScalarValue::Null => None,
            other => Some(other.to_string().trim_end_matches(" NULL").to_string()),
        },
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FormatUDF {
    signature: Signature,
}

impl Default for FormatUDF {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FormatUDF {
    fn name(&self) -> &str {
        "format"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Err(DataFusionError::Execution(
                "format: requires at least a format string argument".into(),
            ));
        }
        let fmt_str = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "format: first argument must be a text format string".into(),
                ));
            }
        };
        let fmt_args: Vec<Option<String>> =
            args.args[1..].iter().map(scalar_to_opt_string).collect();
        let result = pg_format(&fmt_str, &fmt_args)?;
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
    }
}

pub fn create_format_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(FormatUDF::default()).with_aliases(["sprintf"])
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::prelude::SessionContext;

    async fn run_str(ctx: &SessionContext, sql: &str) -> Option<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        if batches[0].num_rows() == 0 {
            return None;
        }
        let arr = batches[0].column(0).as_string::<i32>();
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_string())
        }
    }

    #[tokio::test]
    async fn format_basic_and_positional() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_format_udf());
        assert_eq!(
            run_str(&ctx, "SELECT format('Hello, %s!', 'world')").await,
            Some("Hello, world!".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT format('%2$s %1$s', 'a', 'b')").await,
            Some("b a".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT format('100%%')").await,
            Some("100%".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT format('table %I', 'my table')").await,
            Some("table \"my table\"".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT format('val %L', 'it''s')").await,
            Some("val 'it''s'".into())
        );
    }

    #[tokio::test]
    async fn format_rejects_width() {
        // Postgres' format() grammar has no width — an error at execution, not planning.
        let ctx = SessionContext::new();
        ctx.register_udf(create_format_udf());
        let res = ctx
            .sql("SELECT format('%10s', 'x')")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(res.is_err(), "width should be rejected");
    }

    #[tokio::test]
    async fn sprintf_alias() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_format_udf());
        assert_eq!(
            run_str(&ctx, "SELECT sprintf('Hello, %s!', 'world')").await,
            Some("Hello, world!".into())
        );
    }
}
