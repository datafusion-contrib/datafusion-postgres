//! PostgreSQL regex-based string functions:
//!
//! * `regexp_substr(text, pattern [, start, N, flags [, subexpr]])` —
//!   extract the substring matching a regular expression.
//! * `regexp_split_to_array(text, pattern [, flags])` — split a string by a
//!   regular expression pattern and return a text array.
//!
//! `regexp_matches` (set-returning) and `regexp_split_to_table` are omitted
//! here because they require table-valued function support.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use regex::Regex;

fn build_regex(pattern: &str, flags: &str) -> Result<Regex> {
    let mut pat = String::new();
    for f in flags.chars() {
        match f {
            'i' => pat.push_str("(?i)"),
            'm' | 'n' => pat.push_str("(?m)"),
            's' => pat.push_str("(?s)"),
            'x' => pat.push_str("(?x)"),
            'g' => {} // global flag handled by caller
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "regexp: unsupported flag '{f}'"
                )));
            }
        }
    }
    pat.push_str(pattern);
    Regex::new(&pat).map_err(|e| {
        DataFusionError::Execution(format!("regexp: invalid pattern '{pattern}': {e}"))
    })
}

// ---------------------------------------------------------------------------
// regexp_substr(text, pattern [, start [, N [, flags [, subexpr]]]]) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpSubstrUdf {
    signature: Signature,
}

impl Default for RegexpSubstrUdf {
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

impl ScalarUDFImpl for RegexpSubstrUdf {
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
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))),
            ) => {
                let re = build_regex(pattern, &flags)?;
                let result = regexp_substr_with_regex(text, &re, start, n, subexpr);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
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
                        let result =
                            regexp_substr_with_regex(typed.value(i), &re, start, n, subexpr);
                        match result {
                            Some(s) => builder.append_value(s),
                            None => builder.append_null(),
                        }
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
            }
            _ => Err(DataFusionError::Internal(
                "regexp_substr: unsupported argument combination".into(),
            )),
        }
    }
}

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
    let start_idx = (start as usize).saturating_sub(1);
    if start_idx > text.len() {
        return None;
    }
    let search_text = &text[start_idx..];
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
pub struct RegexpSplitToArrayUdf {
    signature: Signature,
}

impl Default for RegexpSplitToArrayUdf {
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

impl ScalarUDFImpl for RegexpSplitToArrayUdf {
    fn name(&self) -> &str {
        "regexp_split_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let flags: String = match args.args.get(2) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(f)))) => f.clone(),
            _ => String::new(),
        };

        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))),
            ) => {
                let re = build_regex(pattern, &flags)?;
                let parts: Vec<ScalarValue> = re
                    .split(text)
                    .map(|s| ScalarValue::Utf8(Some(s.to_string())))
                    .collect();
                let list_arr = ScalarValue::new_list(&parts, &DataType::Utf8, true);
                Ok(ColumnarValue::Scalar(ScalarValue::List(list_arr)))
            }
            (ColumnarValue::Scalar(ScalarValue::Utf8(None)), _)
            | (_, ColumnarValue::Scalar(ScalarValue::Utf8(None))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Null))
            }
            _ => Err(DataFusionError::Internal(
                "regexp_split_to_array: unsupported argument combination".into(),
            )),
        }
    }
}

pub fn create_regexp_substr_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpSubstrUdf::default())
}

pub fn create_regexp_split_to_array_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RegexpSplitToArrayUdf::default())
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
        let col = batches[0].column(0);
        let arr = col.as_string::<i32>();
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
        assert_eq!(
            run_str(&ctx, "SELECT regexp_substr('hello', '[0-9]+')").await,
            None
        );
        assert_eq!(
            run_str(&ctx, "SELECT regexp_substr(CAST(NULL AS TEXT), 'abc')").await,
            None
        );
    }
}
