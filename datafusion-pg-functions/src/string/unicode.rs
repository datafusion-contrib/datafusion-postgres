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
//! `casefold` applies **simple, per-character Unicode lowercasing** —
//! verified against PostgreSQL 18: `casefold('ß')` → `'ß'` (Postgres does
//! NOT expand to `ss`), `casefold('İ')` → `'i'` (no combining dot — the
//! UnicodeData *simple* mapping), and final sigma is not context-sensitive
//! (`casefold('ΟΔΟΣ')` → `'οδοσ'`). This matches Postgres exactly; it is
//! neither full case folding nor Rust's `to_lowercase` (which expands `İ`).
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

/// Apply Postgres' `casefold`: simple, per-character Unicode lowercasing via
/// the ICU4X `CaseMapper::simple_lowercase` — verified equal to PostgreSQL
/// 18's `casefold()`/`lower()` on every tested character, including `ß`→`ß`,
/// `İ`→`i`, `ẞ`→`ß`, and non-contextual final sigma.
fn casefold_str(s: &str) -> String {
    let cm = CaseMapper::new();
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        out.push(cm.simple_lowercase(c));
    }
    out
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

/// Decode Postgres' Unicode escapes: `\XXXX` (bare, exactly 4 hex digits),
/// `\uXXXX`, `\+XXXXXX` (exactly 6), and `\UXXXXXXXX` (exactly 8).
/// Verified against PostgreSQL 18: any other backslash sequence — including
/// `\n`, `\\` and short `\+` forms — is an error, not a pass-through.
fn decode_unistr(s: &str) -> Result<String> {
    fn push_codepoint(out: &mut String, hex: &str) -> Result<()> {
        let cp = u32::from_str_radix(hex, 16).map_err(|_| {
            DataFusionError::Execution(format!("unistr: invalid hex escape \\{hex}"))
        })?;
        let c = char::from_u32(cp).ok_or_else(|| {
            DataFusionError::Execution(format!("unistr: invalid Unicode codepoint U+{cp:04X}"))
        })?;
        out.push(c);
        Ok(())
    }

    /// Consume exactly `n` hex digits from `chars`; error if fewer.
    fn take_hex<I: Iterator<Item = char>>(chars: &mut I, n: usize) -> Result<String> {
        let mut hex = String::with_capacity(n);
        for _ in 0..n {
            match chars.next() {
                Some(c) if c.is_ascii_hexdigit() => hex.push(c),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unistr: invalid Unicode escape (expected {n} hex digits, \
                         got {hex:?} then {other:?}); escapes must be \\XXXX, \
                         \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX"
                    )));
                }
            }
        }
        Ok(hex)
    }

    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('u') => push_codepoint(&mut out, &take_hex(&mut chars, 4)?)?,
            Some('U') => push_codepoint(&mut out, &take_hex(&mut chars, 8)?)?,
            Some('+') => push_codepoint(&mut out, &take_hex(&mut chars, 6)?)?,
            Some(c) if c.is_ascii_hexdigit() => {
                // Bare \XXXX form: the digit just read is the first of four.
                let mut hex = String::with_capacity(4);
                hex.push(c);
                hex.push_str(&take_hex(&mut chars, 3)?);
                push_codepoint(&mut out, &hex)?;
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "unistr: invalid Unicode escape \\{other:?}; escapes must be \
                     \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX"
                )));
            }
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
    async fn casefold_matches_postgres() {
        // Every expectation verified against PostgreSQL 18.4:
        // casefold = simple per-char lowercase (NOT full case folding).
        let ctx = SessionContext::new();
        ctx.register_udf(create_casefold_udf());
        assert_eq!(
            run_str(&ctx, "SELECT casefold('Hello World')").await,
            Some("hello world".into())
        );
        // ß is NOT expanded to ss (Postgres: casefold('ß') = 'ß').
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ß')").await,
            Some("ß".into())
        );
        // Long-s and final sigma stay as-is (simple per-char mapping).
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ſ')").await,
            Some("ſ".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ς')").await,
            Some("ς".into())
        );
        // Capital sharp-s and micro sign.
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ẞ')").await,
            Some("ß".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT casefold('µ')").await,
            Some("µ".into())
        );
        // Dotted capital I: simple mapping to 'i' (Rust to_lowercase would add U+0307).
        assert_eq!(
            run_str(&ctx, "SELECT casefold('İ')").await,
            Some("i".into())
        );
        // Final sigma is NOT context-sensitive.
        assert_eq!(
            run_str(&ctx, "SELECT casefold('ΟΔΟΣ')").await,
            Some("οδοσ".into())
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
        // Semantics verified against PostgreSQL 18.4.
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
        // Bare \XXXX form (no marker letter).
        assert_eq!(
            run_str(&ctx, "SELECT unistr('a\\0062c')").await,
            Some("abc".into())
        );
        // \+ requires EXACTLY 6 hex digits.
        assert_eq!(
            run_str(&ctx, "SELECT unistr('x\\+000061y')").await,
            Some("xay".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT unistr('hello')").await,
            Some("hello".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT unistr(CAST(NULL AS TEXT))").await,
            None
        );

        // Invalid escapes are errors, not pass-throughs (Postgres behavior):
        // \n, \\, and a short \+ form all error.
        for bad in [
            "SELECT unistr('a\\nb')",
            "SELECT unistr('a\\\\b')",
            "SELECT unistr('x\\+0061y')",
        ] {
            let res = ctx.sql(bad).await.unwrap().collect().await;
            assert!(res.is_err(), "unistr should reject {bad}");
        }
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
        assert_eq!(arr.value(1), "ß");
        assert!(arr.is_null(2));
    }
}
