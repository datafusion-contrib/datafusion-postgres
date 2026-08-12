//! PostgreSQL regex-based string functions.
//!
//! * `regexp_substr(text, pattern [, start [, N [, flags [, subexpr]]]])` —
//!   extract the substring matching a regular expression.
//! * `regexp_split_to_array(text, pattern [, flags])` — split a string by a
//!   regular expression pattern and return a `text[]`.
//!
//! <https://www.postgresql.org/docs/current/functions-matching.html>
//!
//! `regexp_matches` (set-returning) and `regexp_split_to_table` are omitted
//! here because they require table-valued function support.
//!
//! ## Postgres compatibility
//!
//! `start` is a 1-based **character** position; it is converted to a byte
//! offset via `char_indices` so multibyte UTF-8 never panics. The `flags`
//! string supports the common Postgres flags (`i`, `c`, `g`, `m`/`n`, `s`,
//! `w`, `p`, `x`). The set-returning `regexp_matches` /
//! `regexp_split_to_table` are out of scope for a `ScalarUDF`.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, ListBuilder, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use regex::Regex;

/// Build a `Regex` from a pattern string and optional Postgres flag letters.
fn build_regex(pattern: &str, flags: &str) -> Result<Regex> {
    let mut pat = String::new();
    for f in flags.chars() {
        match f {
            'i' | 'c' => pat.push_str("(?i)"), // i = case-insensitive; c is no-op default
            'm' | 'n' => pat.push_str("(?m)"),
            's' => pat.push_str("(?s)"),
            'x' => pat.push_str("(?x)"),
            'g' | 'w' | 'p' => {} // global / ascii-/unicode-wildcard: no regex-crate effect
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "regexp: unsupported flag '{f}'"
                )));
            }
        }
    }
    pat.push_str(pattern);
    Regex::new(&pat)
        .map_err(|e| DataFusionError::Execution(format!("regexp: invalid pattern '{pattern}': {e}")))
}

/// Return the byte offset of the `skip`-th (0-based) char, or `None` if the
/// string has fewer than `skip+1` characters. Always lands on a char boundary.
fn char_offset(text: &str, skip: usize) -> Option<usize> {
    text.char_indices().nth(skip).map(|(b, _)| b)
}

// ---------------------------------------------------------------------------
// regexp_substr(text, pattern [, start [, N [, flags [, subexpr]]]]) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpSubstrUDF {
    signature: Signature,
}

impl Default for RegexpSubstrUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int32]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Int32,
                        DataType::Int32,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Int32,
                        DataType::Int32,
                        DataType::Utf8,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Int32,
                        DataType::Int32,
                        DataType::Utf8,
                        DataType::Int32,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpSubstrUDF {
    fn name(&self) -> &str {
        "regexp_substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let start: i32 = match args.args.get(2) {
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(v)))) => *v,
            _ => 1,
        };
        let n: i32 = match args.args.get(3) {
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(v)))) => *v,
            _ => 1,
        };
        let flags: String = match args.args.get(4) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(f)))) => f.clone(),
            _ => String::new(),
        };
        let subexpr: Option<i32> = match args.args.get(5) {
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(v)))) => Some(*v),
            _ => None,
        };

        match (&args.args[0], &args.args[1]) {
            (ColumnarValue::Scalar(ScalarValue::Utf8(None)), _)
            | (_, ColumnarValue::Scalar(ScalarValue::Utf8(None))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            (
                ColumnarValue::Array(text_arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))),
            ) => {
                let re = build_regex(pattern, &flags)?;
                let typed = text_arr.as_string::<i32>();
                let mut builder = StringBuilder::with_capacity(typed.len(), typed.len() * 20);
                for i in 0..typed.len() {
                    if typed.is_null(i) {
                        builder.append_null();
                    } else {
                        match regexp_substr_with_regex(typed.value(i), &re, start, n, subexpr) {
                            Some(s) => builder.append_value(s),
                            None => builder.append_null(),
                        }
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))),
            ) => {
                let re = build_regex(pattern, &flags)?;
                let result = regexp_substr_with_regex(text, &re, start, n, subexpr);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => Err(DataFusionError::Internal(
                "regexp_substr: unsupported argument combination".into(),
            )),
        }
    }
}

/// Find the Nth match (1-based) of `re` in `text` starting at the 1-based
/// character position `start`. `subexpr` selects a capture group (0 = whole).
fn regexp_substr_with_regex(
    text: &str,
    re: &Regex,
    start: i32,
    n: i32,
    subexpr: Option<i32>,
) -> Option<String> {
    if start < 1 || n < 1 {
        return None;
    }
    let off = char_offset(text, (start as usize) - 1)?; // None => start beyond char length
    let search_text = &text[off..];
    let mut count = 0i32;
    for mat in re.find_iter(search_text) {
        count += 1;
        if count == n {
            if let Some(sub) = subexpr {
                if sub == 0 {
                    return Some(mat.as_str().to_string());
                }
                if let Some(caps) = re.captures(mat.as_str()) {
                    return caps.get(sub as usize).map(|m| m.as_str().to_string());
                }
                return None;
            }
            return Some(mat.as_str().to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// regexp_split_to_array(text, pattern [, flags]) → text[]
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpSplitToArrayUDF {
    signature: Signature,
}

impl Default for RegexpSplitToArrayUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpSplitToArrayUDF {
    fn name(&self) -> &str {
        "regexp_split_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let flags: String = match args.args.get(2) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(f)))) => f.clone(),
            _ => String::new(),
        };

        match (&args.args[0], &args.args[1]) {
            (ColumnarValue::Scalar(ScalarValue::Utf8(None)), _)
            | (_, ColumnarValue::Scalar(ScalarValue::Utf8(None))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Null))
            }
            (
                ColumnarValue::Array(text_arr),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))),
            ) => {
                let re = build_regex(pattern, &flags)?;
                let typed = text_arr.as_string::<i32>();
                let mut list = ListBuilder::new(StringBuilder::new());
                for i in 0..typed.len() {
                    if typed.is_null(i) {
                        list.append_null();
                        continue;
                    }
                    for part in re.split(typed.value(i)) {
                        list.values().append_value(part);
                    }
                    list.append(true);
                }
                Ok(ColumnarValue::Array(Arc::new(list.finish()) as ArrayRef))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))),
            ) => {
                let re = build_regex(pattern, &flags)?;
                let parts: Vec<ScalarValue> = re
                    .split(text)
                    .map(|s| ScalarValue::Utf8(Some(s.to_string())))
                    .collect();
                let arr = ScalarValue::new_list(&parts, &DataType::Utf8, true);
                Ok(ColumnarValue::Scalar(ScalarValue::List(arr)))
            }
            _ => Err(DataFusionError::Internal(
                "regexp_split_to_array: unsupported argument combination".into(),
            )),
        }
    }
}

pub fn create_regexp_substr_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpSubstrUDF::default())
}

pub fn create_regexp_split_to_array_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpSplitToArrayUDF::default())
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn regexp_substr_basic() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_regexp_substr_udf());
        assert_eq!(
            run_str(&ctx, "SELECT regexp_substr('hello world', 'wor..')").await,
            Some("world".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT regexp_substr('abc123def', '[0-9]+')").await,
            Some("123".into())
        );
        assert_eq!(run_str(&ctx, "SELECT regexp_substr('hello', '[0-9]+')").await, None);
        assert_eq!(run_str(&ctx, "SELECT regexp_substr(CAST(NULL AS TEXT), 'abc')").await, None);
    }

    #[tokio::test]
    async fn regexp_substr_multibyte_does_not_panic() {
        // Regression: 'start' landing inside a multibyte char must not panic.
        let ctx = SessionContext::new();
        ctx.register_udf(create_regexp_substr_udf());
        // 'café' has 4 chars; start=4 (cast to int) starts at 'é'.
        assert_eq!(
            run_str(&ctx, "SELECT regexp_substr('café', 'é', CAST(4 AS INT))").await,
            Some("é".into())
        );
        // start beyond the last char returns NULL (no panic).
        assert_eq!(
            run_str(&ctx, "SELECT regexp_substr('café', 'x', CAST(5 AS INT))").await,
            None
        );
    }

    #[tokio::test]
    async fn regexp_substr_vectorized_batch() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_regexp_substr_udf());
        let df = ctx
            .sql("SELECT regexp_substr(c, '[0-9]+') FROM (VALUES ('a1b'), ('no digits'), (CAST(NULL AS TEXT))) AS t(c)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = df[0].column(0).as_string::<i32>();
        assert_eq!(arr.value(0), "1");
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }

    #[tokio::test]
    async fn regexp_split_to_array_scalar() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_regexp_split_to_array_udf());
        let df = ctx
            .sql("SELECT regexp_split_to_array('a,b,,c', ',')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let list = df[0].column(0).as_list::<i32>();
        let inner = list.value(0);
        let arr = inner.as_string::<i32>();
        let got: Vec<&str> = (0..arr.len()).map(|i| arr.value(i)).collect();
        assert_eq!(got, vec!["a", "b", "", "c"]);
    }

    #[tokio::test]
    async fn regexp_split_to_array_vectorized_batch() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_regexp_split_to_array_udf());
        let df = ctx
            .sql("SELECT regexp_split_to_array(c, ',') FROM (VALUES ('a,b'), ('x,y,z'), (CAST(NULL AS TEXT))) AS t(c)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let list = df[0].column(0).as_list::<i32>();
        // row 0: [a, b]
        let r0 = list.value(0);
        let arr0 = r0.as_string::<i32>();
        assert_eq!(arr0.len(), 2);
        assert_eq!(arr0.value(0), "a");
        assert_eq!(arr0.value(1), "b");
        // row 1: [x, y, z]
        let r1 = list.value(1);
        let arr1 = r1.as_string::<i32>();
        assert_eq!(arr1.len(), 3);
        assert_eq!(arr1.value(2), "z");
        // row 2: NULL
        assert!(list.is_null(2));
    }
}
