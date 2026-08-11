//! PostgreSQL Unicode string functions:
//!
//! * `normalize(text [, form])` — Unicode normalization (NFC, NFD, NFKC, NFKD).
//! * `casefold(text)` — Unicode case folding (locale-independent lowercase).
//! * `unicode_assigned(text)` — `true` iff every character is an assigned
//!   Unicode codepoint.
//! * `unistr(text)` — decode `\uXXXX` / `\UXXXXXXXX` / `\+XXXXXX` escape
//!   sequences into the corresponding characters.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanBuilder, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
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
pub struct NormalizeUdf {
    signature: Signature,
}

impl Default for NormalizeUdf {
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

impl ScalarUDFImpl for NormalizeUdf {
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
        let form = if args.args.len() > 1 {
            match &args.args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(f))) => f.clone(),
                _ => "NFC".to_string(),
            }
        } else {
            "NFC".to_string()
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
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
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
// casefold(text) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CasefoldUdf {
    signature: Signature,
}

impl Default for CasefoldUdf {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CasefoldUdf {
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
                        builder.append_value(typed.value(i).to_lowercase());
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(s.to_lowercase())),
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

fn is_unicode_assigned(s: &str) -> bool {
    for ch in s.chars() {
        let cp = ch as u32;
        // Private Use Areas
        if (0xE000..=0xF8FF).contains(&cp)
            || (0xF0000..=0xFFFFD).contains(&cp)
            || (0x100000..=0x10FFFD).contains(&cp)
        {
            return false;
        }
        // Non-characters
        if (0xFDD0..=0xFDEF).contains(&cp) || cp & 0xFFFF >= 0xFFFE {
            return false;
        }
    }
    true
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UnicodeAssignedUdf {
    signature: Signature,
}

impl Default for UnicodeAssignedUdf {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for UnicodeAssignedUdf {
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
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
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
                    if let Some(&c) = chars.peek() {
                        if c.is_ascii_hexdigit() {
                            hex.push(c);
                            chars.next();
                        } else {
                            break;
                        }
                    } else {
                        break;
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
            _ => {
                out.push('\\');
            }
        }
    }
    Ok(out)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UnistrUdf {
    signature: Signature,
}

impl Default for UnistrUdf {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for UnistrUdf {
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
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as _))
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
    ScalarUDF::new_from_impl(NormalizeUdf::default())
}

pub fn create_casefold_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CasefoldUdf::default())
}

pub fn create_unicode_assigned_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(UnicodeAssignedUdf::default())
}

pub fn create_unistr_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(UnistrUdf::default())
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
        let col = batches[0].column(0);
        let arr = col
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
    async fn casefold_basic() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_casefold_udf());

        assert_eq!(
            run_str(&ctx, "SELECT casefold('Hello World')").await,
            Some("hello world".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT casefold(CAST(NULL AS TEXT))").await,
            None
        );
    }

    #[tokio::test]
    async fn unicode_assigned_basic() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_unicode_assigned_udf());

        assert_eq!(
            run_bool(&ctx, "SELECT unicode_assigned('hello')").await,
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
            run_str(&ctx, "SELECT unistr('hello')").await,
            Some("hello".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT unistr(CAST(NULL AS TEXT))").await,
            None
        );
    }
}
