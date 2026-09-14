//! PostgreSQL encoding-related string functions.
//!
//! * `pg_client_encoding()` — name of the current client encoding. DataFusion
//!   only handles UTF-8, so this always returns `'UTF8'`.
//! * `to_ascii(text [, encoding])` — convert text to ASCII using Postgres'
//!   own ISO 8859-1 → ASCII transliteration table.
//!
//! <https://www.postgresql.org/docs/current/functions-string.html>
//!
//! ## Postgres compatibility
//!
//! The table below is Postgres' actual LATIN1 → ASCII mapping, extracted
//! verbatim from a live PostgreSQL 18 server (byte range 0x80–0xFF). It is
//! intentionally idiosyncratic — e.g. `ß` → `B` (not `ss`), `Æ` → `A` (not
//! `AE`), `©` → `C`, `£` → `L` — because Postgres maps every single-byte
//! character to a *single* ASCII character and replaces unmapped characters
//! with a space. General transliteration libraries (`any_ascii`, `deunicode`)
//! produce different mappings (e.g. `ß` → `ss`) and therefore cannot be used.
//!
//! Deviations from Postgres, documented:
//!
//! * Postgres only accepts `to_ascii` when the (server or given) encoding is
//!   a single-byte LATIN-family encoding; on a UTF-8 database the one-argument
//!   form raises an error. We always transliterate with the LATIN1 table and
//!   accept (and ignore) the optional `encoding` argument.
//! * Postgres' LATIN2–LATIN10 / WIN850 tables are not implemented; characters
//!   outside the LATIN1 range (U+0080–U+00FF) map to a space, matching
//!   Postgres' unmapped-character behavior.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

// ---------------------------------------------------------------------------
// pg_client_encoding() → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgClientEncodingUDF {
    signature: Signature,
}

impl Default for PgClientEncodingUDF {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for PgClientEncodingUDF {
    fn name(&self) -> &str {
        "pg_client_encoding"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "UTF8".to_string(),
        ))))
    }
}

// ---------------------------------------------------------------------------
// to_ascii(text [, encoding]) → text
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToAsciiUDF {
    signature: Signature,
}

impl Default for ToAsciiUDF {
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

impl ScalarUDFImpl for ToAsciiUDF {
    fn name(&self) -> &str {
        "to_ascii"
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
                        builder.append_value(to_ascii_str(typed.value(i)));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(Some(to_ascii_str(s))),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            _ => Err(DataFusionError::Internal(
                "to_ascii: unexpected argument type".into(),
            )),
        }
    }
}

/// Transliterate a string to ASCII using Postgres' LATIN1 table: ASCII
/// passes through, U+0080–U+00FF map through `LATIN1_TO_ASCII`, and every
/// other character maps to a space (Postgres' unmapped-character behavior).
fn to_ascii_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        let u = c as u32;
        if u < 0x80 {
            out.push(c);
        } else if (0x80..=0xFF).contains(&u) {
            // Index into the table (u - 0x80); unmapped entries are ' '.
            out.push(LATIN1_TO_ASCII[(u - 0x80) as usize]);
        } else {
            // Outside the LATIN1 range: Postgres' unmapped behavior.
            out.push(' ');
        }
    }
    out
}

/// Postgres' ISO 8859-1 (LATIN1) → ASCII table for bytes 0x80–0xFF, extracted
/// verbatim from PostgreSQL 18.4 (`to_ascii` in a LATIN1 database). Entries
/// marked `' '` are unmapped by Postgres and replaced with a space.
const LATIN1_TO_ASCII: [char; 128] = [
    // 0x80 – 0x8F: C1 controls — unmapped
    ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
    // 0x90 – 0x9F: C1 controls — unmapped
    ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
    // 0xA0 nbsp, 0xA1 ¡, 0xA2 ¢, 0xA3 £, 0xA4 ¤, 0xA5 ¥, 0xA6 ¦, 0xA7 §
    ' ', ' ', 'c', 'L', ' ', 'Y', ' ', ' ',
    // 0xA8 ¨, 0xA9 ©, 0xAA ª, 0xAB «, 0xAC ¬, 0xAD ­, 0xAE ®, 0xAF ¯
    '"', 'C', 'a', ' ', ' ', '-', 'R', ' ',
    // 0xB0 °, 0xB1 ±, 0xB2 ², 0xB3 ³, 0xB4 ´, 0xB5 µ, 0xB6 ¶, 0xB7 ·
    ' ', ' ', ' ', ' ', '\'', 'u', ' ', '.',
    // 0xB8 ¸, 0xB9 ¹, 0xBA º, 0xBB », 0xBC ¼, 0xBD ½, 0xBE ¾, 0xBF ¿
    ',', ' ', ' ', ' ', ' ', ' ', ' ', '?', // 0xC0–0xC6 À Å Æ, 0xC7 Ç
    'A', 'A', 'A', 'A', 'A', 'A', 'A', 'C', // 0xC8–0xCB È Ë, 0xCC–0xCF Ì Ï
    'E', 'E', 'E', 'E', 'I', 'I', 'I', 'I',
    // 0xD0 Ð — unmapped, 0xD1 Ñ, 0xD2–0xD6 Ò Ö, 0xD7 ×
    ' ', 'N', 'O', 'O', 'O', 'O', 'O', 'x',
    // 0xD8 Ø, 0xD9–0xDC Ù Ü, 0xDD Ý, 0xDE Þ, 0xDF ß
    'O', 'U', 'U', 'U', 'U', 'Y', 'T', 'B', // 0xE0–0xE6 à æ, 0xE7 ç
    'a', 'a', 'a', 'a', 'a', 'a', 'a', 'c', // 0xE8–0xEB è ë, 0xEC–0xEF ì ï
    'e', 'e', 'e', 'e', 'i', 'i', 'i', 'i',
    // 0xF0 ð — unmapped, 0xF1 ñ, 0xF2–0xF6 ò ö, 0xF7 ÷
    ' ', 'n', 'o', 'o', 'o', 'o', 'o', '/',
    // 0xF8 ø, 0xF9–0xFC ù ü, 0xFD ý, 0xFE þ, 0xFF ÿ
    'o', 'u', 'u', 'u', 'u', 'y', 't', 'y',
];

pub fn create_pg_client_encoding_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(PgClientEncodingUDF::default())
}

pub fn create_to_ascii_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToAsciiUDF::default())
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
    async fn pg_client_encoding_returns_utf8() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_pg_client_encoding_udf());
        assert_eq!(
            run_str(&ctx, "SELECT pg_client_encoding()").await,
            Some("UTF8".into())
        );
    }

    /// Every expectation below was verified against PostgreSQL 18.4.
    #[tokio::test]
    async fn to_ascii_matches_postgres() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_ascii_udf());
        // Accented letters fold to their base letter.
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('café')").await,
            Some("cafe".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('München')").await,
            Some("Munchen".into())
        );
        // Idiosyncratic-but-real Postgres mappings.
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('ß')").await,
            Some("B".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('Æ')").await,
            Some("A".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('æ')").await,
            Some("a".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('Þ')").await,
            Some("T".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('þ')").await,
            Some("t".into())
        );
        // Symbols.
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('©')").await,
            Some("C".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('£')").await,
            Some("L".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('¥')").await,
            Some("Y".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('×')").await,
            Some("x".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('÷')").await,
            Some("/".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('µ')").await,
            Some("u".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('¿')").await,
            Some("?".into())
        );
        // Unmapped characters become a space (Postgres behavior).
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('°')").await,
            Some(" ".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('¼')").await,
            Some(" ".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('Ð')").await,
            Some(" ".into())
        );
        // Outside the LATIN1 range: unmapped behavior (space).
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii('ā')").await,
            Some(" ".into())
        );
        assert_eq!(
            run_str(&ctx, "SELECT to_ascii(CAST(NULL AS TEXT))").await,
            None
        );
    }

    #[tokio::test]
    async fn to_ascii_vectorized_batch() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_ascii_udf());
        let df = ctx
            .sql("SELECT to_ascii(c) FROM (VALUES ('café'), ('naïve'), (CAST(NULL AS TEXT))) AS t(c)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = df[0].column(0).as_string::<i32>();
        assert_eq!(arr.value(0), "cafe");
        assert_eq!(arr.value(1), "naive");
        assert!(arr.is_null(2));
    }
}
