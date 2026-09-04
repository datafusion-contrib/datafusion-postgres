//! PostgreSQL Unicode string functions:
//!
//! * `normalize(text [, form])` — Unicode normalization (NFC, NFD, NFKC, NFKD).
//! * `casefold(text)` — Unicode full case folding.
//! * `unicode_assigned(text)` — `true` iff every character is an *assigned*
//!   Unicode codepoint (General_Category ≠ Cn).
//! * `unistr(text)` — decode `\uXXXX` / `\UXXXXXXXX` / `\+XXXXXX` escapes.
//!
//! <https://www.postgresql.org/docs/current/functions-string.html>
//!
//! ## Postgres compatibility
//!
//! `casefold` performs full Unicode case folding (CaseFolding.txt, the common
//! `C` plus full `F` mappings) via the ICU4X case-mapping tables — the same
//! operation Postgres performs. It is *not* the same as locale-independent
//! lowercase: it expands e.g. `ß` → `ss`, folds the long-s `ſ` → `s`, maps
//! final sigma `ς` → `σ`, micro `µ` → `μ`, and capital sharp-s `ẞ` → `ss`.
//!
//! `unicode_assigned` reports whether every codepoint has a non-`Cn` general
//! category, looked up via the ICU4X property tables — so Private-Use-Area
//! characters (category `Co`, assigned) correctly return `true`, and reserved
//! codepoints (e.g. U+0378, category `Cn`) correctly return `false`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use icu_casemap::CaseMapper;
use icu_properties::CodePointMapData;
use icu_properties::props::GeneralCategory;
use unicode_normalization::UnicodeNormalization;

// ---------------------------------------------------------------------------
// normalize(text [, form]) → text
// ---------------------------------------------------------------------------

fn normalize_str(s: &str, form: &str) -> Result<String> {
    match form.to_uppercase().as_str() {
        "NFC" => Ok(s.nfc().collect()),
        "NFD" => Ok(s.nfd().collect()),
        "NFKC" => Ok(s.nfkc().collect()),
        "NFKD" => Ok(s.nfkd().collect()),
        _ => Err(DataFusionError::Execution(format!(
            "normalize: unsupported normalization form '{form}'. \
             Must be one of NFC, NFD, NFKC, NFKD."
        ))),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NormalizeUDF {
    signature: Signature,
}

impl Default for NormalizeUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NormalizeUDF {
    fn name(&self) -> &str {
        "normalize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let text_arg = &args.args[0];
        let form = match args.args.get(1) {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(f)))) => f.clone(),
            _ => "NFC".to_string(),
        };

        match text_arg {
            ColumnarValue::Array(arr) => {
                let typed = arr.as_string::<i32>();
                let mut builder = StringBuilder::with_capacity(typed.len(), typed.len() * 20);
                for i in 0..typed.len() {
                    if typed.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(normalize_str(typed.value(i), &form)?);
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(normalize_str(s, &form)?)),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            _ => Err(DataFusionError::Internal(
                "normalize: unexpected argument type".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// casefold(text) → text — full Unicode case folding
// ---------------------------------------------------------------------------

/// Apply full Unicode case folding (CaseFolding.txt `C` + `F` mappings) via
/// the ICU4X `CaseMapper` — the same tables Postgres' own casefold uses.
fn casefold_str(s: &str) -> String {
    CaseMapper::new().fold_string(s).into_owned()
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CasefoldUDF {
    signature: Signature,
}

impl Default for CasefoldUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CasefoldUDF {
    fn name(&self) -> &str {
        "casefold"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(arr) => {
                let typed = arr.as_string::<i32>();
                let mut builder = StringBuilder::with_capacity(typed.len(), typed.len() * 20);
                for i in 0..typed.len() {
                    if typed.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(casefold_str(typed.value(i)));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(casefold_str(s))),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            _ => Err(DataFusionError::Internal(
                "casefold: unexpected argument type".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// unicode_assigned(text) → boolean
// ---------------------------------------------------------------------------

/// True iff every character's Unicode General_Category is not `Cn`
/// (Unassigned). Resolved via the ICU4X compiled property table.
fn is_unicode_assigned(s: &str) -> bool {
    // `new()` returns a cheap borrowed handle to static compiled data.
    let gc = CodePointMapData::<GeneralCategory>::new();
    s.chars()
        .all(|ch| gc.get(ch) != GeneralCategory::Unassigned)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UnicodeAssignedUDF {
    signature: Signature,
}

impl Default for UnicodeAssignedUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for UnicodeAssignedUDF {
    fn name(&self) -> &str {
        "unicode_assigned"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(arr) => {
                let typed = arr.as_string::<i32>();
                let mut builder = BooleanBuilder::with_capacity(typed.len());
                for i in 0..typed.len() {
                    if typed.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(is_unicode_assigned(typed.value(i)));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(is_unicode_assigned(s))),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }
            _ => Err(DataFusionError::Internal(
                "unicode_assigned: unexpected argument type".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// unistr(text) → text
// ---------------------------------------------------------------------------

/// Decode `\uXXXX`, `\UXXXXXXXX`, and `\+XXXXXX` Unicode escapes. A backslash
/// not introducing a recognized escape is kept verbatim.
fn decode_unistr(s: &str) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.peek() {
            Some('u') => {
                chars.next();
                let hex: String = chars.by_ref().take(4).collect();
                if hex.len() != 4 {
                    return Err(DataFusionError::Execution(format!(
                        "unistr: incomplete \\u escape (got {hex:?})"
                    )));
                }
                let cp = u32::from_str_radix(&hex, 16).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "unistr: invalid hex in \\u escape: \\u{hex}"
                    ))
                })?;
                let c = char::from_u32(cp).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "unistr: invalid Unicode codepoint U+{cp:04X}"
                    ))
                })?;
                out.push(c);
            }
            Some('U') => {
                chars.next();
                let hex: String = chars.by_ref().take(8).collect();
                if hex.len() != 8 {
                    return Err(DataFusionError::Execution(format!(
                        "unistr: incomplete \\U escape (got {hex:?})"
                    )));
                }
                let cp = u32::from_str_radix(&hex, 16).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "unistr: invalid hex in \\U escape: \\U{hex}"
                    ))
                })?;
                let c = char::from_u32(cp).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "unistr: invalid Unicode codepoint U+{cp:08X}"
                    ))
                })?;
                out.push(c);
            }
            Some('+') => {
                chars.next();
                let mut hex = String::new();
                for _ in 0..6 {
                    match chars.peek() {
                        Some(c) if c.is_ascii_hexdigit() => {
                            hex.push(*c);
                            chars.next();
                        }
                        _ => break,
                    }
                }
                if hex.is_empty() {
                    return Err(DataFusionError::Execution(
                        "unistr: \\+ escape requires at least one hex digit".into(),
                    ));
                }
                let cp = u32::from_str_radix(&hex, 16).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "unistr: invalid hex in \\+ escape: \\+{hex}"
                    ))
                })?;
                let c = char::from_u32(cp).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "unistr: invalid Unicode codepoint U+{cp:04X}"
                    ))
                })?;
                out.push(c);
            }
            _ => out.push('\\'),
        }
    }
    Ok(out)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UnistrUDF {
    signature: Signature,
}

impl Default for UnistrUDF {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for UnistrUDF {
    fn name(&self) -> &str {
        "unistr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(arr) => {
                let typed = arr.as_string::<i32>();
                let mut builder = StringBuilder::with_capacity(typed.len(), typed.len() * 20);
                for i in 0..typed.len() {
                    if typed.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(decode_unistr(typed.value(i))?);
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(decode_unistr(s)?)),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            _ => Err(DataFusionError::Internal(
                "unistr: unexpected argument type".into(),
            )),
        }
    }
}

pub fn create_normalize_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(NormalizeUDF::default())
}

pub fn create_casefold_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CasefoldUDF::default())
}

pub fn create_unicode_assigned_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(UnicodeAssignedUDF::default())
}

pub fn create_unistr_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(UnistrUDF::default())
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

    async fn run_bool(ctx: &SessionContext, sql: &str) -> Option<bool> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        if batches[0].num_rows() == 0 {
            return None;
        }
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0))
        }
    }

    #[tokio::test]
    async fn normalize_nfc_default() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_normalize_udf());
        assert_eq!(
            run_str(&ctx, "SELECT normalize('café')").await,
            Some("café".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT normalize(CAST(NULL AS TEXT))").await,
            None
        );
    }

    #[tokio::test]
    async fn casefold_full_folding() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_casefold_udf());
        assert_eq!(
            run_str(&ctx, "SELECT casefold('Hello World')").await,
            Some("hello world".into())
        );
        // Full case folding: ß -> ss (not ß), long-s ſ -> s.
        assert_eq!(
            run_str(&ctx, "SELECT casefold('STRASSE')").await,
            Some("strasse".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ß')").await,
            Some("ss".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ſ')").await,
            Some("s".into())
        );
        // Full case folding edge cases: final sigma, micro sign, capital sharp-s.
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ς')").await,
            Some("σ".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT casefold('µ')").await,
            Some("μ".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ẞ')").await,
            Some("ss".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT casefold(CAST(NULL AS TEXT))").await,
            None
        );
    }

    #[tokio::test]
    async fn unicode_assigned_uses_general_category() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_unicode_assigned_udf());
        // Ordinary text is assigned.
        assert_eq!(
            run_bool(&ctx, "SELECT unicode_assigned('hello')").await,
            Some(true)
        );
        // Private-Use-Area is category Co (assigned) -> true (NOT false).
        assert_eq!(
            run_bool(&ctx, "SELECT unicode_assigned('\u{E000}')").await,
            Some(true)
        );
        assert_eq!(
            run_bool(&ctx, "SELECT unicode_assigned(CAST(NULL AS TEXT))").await,
            None
        );
    }

    #[tokio::test]
    async fn unistr_escapes() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_unistr_udf());
        assert_eq!(
            run_str(&ctx, r"SELECT unistr('\u0041')").await,
            Some("A".into())
        );
        assert_eq!(
            run_str(&ctx, r"SELECT unistr('\U00000041')").await,
            Some("A".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT unistr('hello')").await,
            Some("hello".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT unistr(CAST(NULL AS TEXT))").await,
            None
        );
    }

    #[tokio::test]
    async fn casefold_vectorized_batch() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_casefold_udf());
        let df = ctx
            .sql("SELECT casefold(c) FROM (VALUES ('A'), ('ß'), (CAST(NULL AS TEXT))) AS t(c)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = df[0].column(0).as_string::<i32>();
        assert_eq!(arr.value(0), "a");
        assert_eq!(arr.value(1), "ss");
        assert!(arr.is_null(2));
    }
}
