//! PostgreSQL `format(fmt, ...)` and `sprintf(fmt, ...)` — text formatting.
//!
//! Supported format specifiers: `%s`, `%I`, `%L`, `%%`, positional (`%2$s`),
//! flags (`-`), width.
//!
//! `sprintf` is a PG 18+ alias of `format`.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

fn pg_format(fmt: &str, args: &[Option<String>]) -> Result<String> {
    let mut out = String::with_capacity(fmt.len() * 2);
    let mut chars = fmt.chars().peekable();
    let mut auto_idx: usize = 0;

    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }

        // Parse optional positional index: digits followed by '$'
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
        if !digit_buf.is_empty() && chars.peek() == Some(&'$') {
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
        } else if !digit_buf.is_empty() {
            // Width specifier
            let width: usize = digit_buf.parse().unwrap_or(0);
            let spec = chars.next().ok_or_else(|| {
                DataFusionError::Execution("format: incomplete format specifier".into())
            })?;
            let arg_i = auto_idx;
            auto_idx += 1;
            let val = args.get(arg_i).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "format: too few arguments (need at least {}, got {})",
                    arg_i + 1,
                    args.len()
                ))
            })?;
            let formatted = format_spec(spec, val)?;
            write_padded(&mut out, &formatted, width, false);
            continue;
        }

        // Parse optional flags
        let left_align = if chars.peek() == Some(&'-') {
            chars.next();
            true
        } else {
            false
        };

        // Parse optional width
        let mut width_str = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                width_str.push(c);
                chars.next();
            } else {
                break;
            }
        }
        let width: usize = width_str.parse().unwrap_or(0);

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
        let formatted = format_spec(spec, val)?;
        write_padded(&mut out, &formatted, width, left_align);
    }
    Ok(out)
}

fn format_spec(spec: char, val: &Option<String>) -> Result<String> {
    match spec {
        's' => Ok(val.as_deref().unwrap_or("").to_string()),
        'I' => {
            let s = val.as_deref().unwrap_or("");
            Ok(pg_quote_ident(s))
        }
        'L' => match val {
            Some(s) => Ok(pg_quote_literal_value(s)),
            None => Ok("NULL".to_string()),
        },
        _ => Err(DataFusionError::Execution(format!(
            "format: unsupported format specifier '%{spec}'"
        ))),
    }
}

fn write_padded(out: &mut String, s: &str, width: usize, left_align: bool) {
    if width == 0 || s.len() >= width {
        out.push_str(s);
        return;
    }
    let pad = width - s.len();
    if left_align {
        out.push_str(s);
        for _ in 0..pad {
            out.push(' ');
        }
    } else {
        for _ in 0..pad {
            out.push(' ');
        }
        out.push_str(s);
    }
}

fn pg_quote_ident(s: &str) -> String {
    let needs_quoting = s.is_empty()
        || s.contains(' ')
        || s.contains('"')
        || s.contains('.')
        || s.chars().next().map_or(true, |c| c.is_ascii_digit())
        || s.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_');
    if needs_quoting {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn pg_quote_literal_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
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
            _ => Some(format!("{s:?}")),
        },
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FormatUdf {
    signature: Signature,
}

impl Default for FormatUdf {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FormatUdf {
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
    ScalarUDF::new_from_impl(FormatUdf::default()).with_aliases(["sprintf"])
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
        let col = batches[0].column(0);
        let arr = col.as_string::<i32>();
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_string())
        }
    }

    #[tokio::test]
    async fn format_basic() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_format_udf());

        assert_eq!(
            run_str(&ctx, "SELECT format('Hello, %s!', 'world')").await,
            Some("Hello, world!".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT format('%s %s', 'a', 'b')").await,
            Some("a b".into())
        );
    }

    #[tokio::test]
    async fn format_percent_escape() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_format_udf());

        assert_eq!(
            run_str(&ctx, "SELECT format('100%%')").await,
            Some("100%".into())
        );
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
