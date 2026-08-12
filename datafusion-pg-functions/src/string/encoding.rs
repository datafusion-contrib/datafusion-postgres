//! PostgreSQL encoding-related string functions.
//!
//! * `pg_client_encoding()` — name of the current client encoding. DataFusion
//!   only handles UTF-8, so this always returns `'UTF8'`.
//! * `to_ascii(text [, encoding])` — convert text to ASCII by transliterating
//!   Latin accented characters to their ASCII base.
//!
//! <https://www.postgresql.org/docs/current/functions-string.html>
//!
//! ## Postgres compatibility
//!
//! `to_ascii` transliterates accented Latin characters (the Latin-1 Supplement
//! range, plus the common Latin Extended-A letters) to their ASCII base — e.g.
//! `'café' → 'cafe'`, `'München' → 'Munchen'`, `ß → 'ss'` — matching the intent
//! of Postgres' `to_ascii(..., 'LATIN1')`. Any character that cannot be
//! transliterated is omitted. The optional `encoding` argument is accepted for
//! signature compatibility but ignored (input is always UTF-8); Postgres would
//! error on UTF-8 input without an explicit LATIN-family encoding.

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

/// Transliterate a string to ASCII. Latin-1 Supplement and Latin Extended-A
/// letters are mapped to their ASCII base; unmappable characters are dropped.
fn to_ascii_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match transliterate(c) {
            Some(mapped) => out.push_str(mapped),
            None => out.push(c),
        }
    }
    out
}

/// Return the ASCII transliteration of a Latin-range character, or `None` if
/// the character is already ASCII (kept as-is) or has no mapping (dropped).
fn transliterate(c: char) -> Option<&'static str> {
    let u = c as u32;
    // Already-ASCII characters pass through unchanged.
    if u < 0x80 {
        return None;
    }
    // Latin-1 Supplement (U+00C0 .. U+00FF) transliteration.
    let mapped = match u {
        0x00C0 => "A", 0x00C1 => "A", 0x00C2 => "A", 0x00C3 => "A", 0x00C4 => "A", 0x00C5 => "A",
        0x00C6 => "AE", 0x00C7 => "C", 0x00C8 => "E", 0x00C9 => "E", 0x00CA => "E", 0x00CB => "E",
        0x00CC => "I", 0x00CD => "I", 0x00CE => "I", 0x00CF => "I", 0x00D0 => "D", 0x00D1 => "N",
        0x00D2 => "O", 0x00D3 => "O", 0x00D4 => "O", 0x00D5 => "O", 0x00D6 => "O", 0x00D8 => "O",
        0x00D9 => "U", 0x00DA => "U", 0x00DB => "U", 0x00DC => "U", 0x00DD => "Y", 0x00DE => "TH",
        0x00DF => "ss",
        0x00E0 => "a", 0x00E1 => "a", 0x00E2 => "a", 0x00E3 => "a", 0x00E4 => "a", 0x00E5 => "a",
        0x00E6 => "ae", 0x00E7 => "c", 0x00E8 => "e", 0x00E9 => "e", 0x00EA => "e", 0x00EB => "e",
        0x00EC => "i", 0x00ED => "i", 0x00EE => "i", 0x00EF => "i", 0x00F0 => "d", 0x00F1 => "n",
        0x00F2 => "o", 0x00F3 => "o", 0x00F4 => "o", 0x00F5 => "o", 0x00F6 => "o", 0x00F8 => "o",
        0x00F9 => "u", 0x00FA => "u", 0x00FB => "u", 0x00FC => "u", 0x00FD => "y", 0x00FE => "th",
        0x00FF => "y",
        // Latin Extended-A (common Central/Eastern European letters).
        0x0100 | 0x0101 => "A",  // Ā/ā
        0x0102 | 0x0103 => "A",  // Ă/ă
        0x0104 | 0x0105 => "A",  // Ą/ą
        0x0106 | 0x0107 => "C",  // Ć/ć
        0x0108 | 0x0109 => "C",  // Ĉ/ĉ
        0x010A | 0x010B => "C",  // Ċ/ċ
        0x010C | 0x010D => "C",  // Č/č
        0x010E | 0x010F => "D",  // Ď/ď
        0x0110 | 0x0111 => "D",  // Đ/đ
        0x0112 | 0x0113 => "E",  // Ē/ē
        0x0114 | 0x0115 => "E",  // Ĕ/ĕ
        0x0116 | 0x0117 => "E",  // Ė/ė
        0x0118 | 0x0119 => "E",  // Ę/ę
        0x011A | 0x011B => "E",  // Ě/ě
        0x011C | 0x011D => "G",  // Ĝ/ĝ
        0x011E | 0x011F => "G",  // Ğ/ğ
        0x0120 | 0x0121 => "G",  // Ġ/ġ
        0x0122 | 0x0123 => "G",  // Ģ/ģ
        0x0124 | 0x0125 => "H",  // Ĥ/ĥ
        0x0126 | 0x0127 => "H",  // Ħ/ħ
        0x0128 | 0x0129 => "I",  // Ĩ/ĩ
        0x012A | 0x012B => "I",  // Ī/ī
        0x012C | 0x012D => "I",  // Ĭ/ĭ
        0x012E | 0x012F => "I",  // Į/į
        0x0130 => "I",           // İ
        0x0134 | 0x0135 => "J",  // Ĵ/ĵ
        0x0136 | 0x0137 => "K",  // Ķ/ķ
        0x0139 | 0x013A => "L",  // Ĺ/ĺ
        0x013B | 0x013C => "L",  // Ļ/ļ
        0x013D | 0x013E => "L",  // Ľ/ľ
        0x0141 | 0x0142 => "L",  // Ł/ł
        0x0143 | 0x0144 => "N",  // Ń/ń
        0x0145 | 0x0146 => "N",  // Ņ/ņ
        0x0147 | 0x0148 => "N",  // Ň/ň
        0x014A | 0x014B => "NG", // Ŋ/ŋ
        0x014C | 0x014D => "O",  // Ō/ō
        0x014E | 0x014F => "O",  // Ŏ/ŏ
        0x0150 | 0x0151 => "O",  // Ő/ő
        0x0152 => "OE",          // Œ
        0x0153 => "oe",          // œ
        0x0154 | 0x0155 => "R",  // Ŕ/ŕ
        0x0156 | 0x0157 => "R",  // Ŗ/ŗ
        0x0158 | 0x0159 => "R",  // Ř/ř
        0x015A | 0x015B => "S",  // Ś/ś
        0x015C | 0x015D => "S",  // Ŝ/ŝ
        0x015E | 0x015F => "S",  // Ş/ş
        0x0160 | 0x0161 => "S",  // Š/š
        0x0162 | 0x0163 => "T",  // Ţ/ţ
        0x0164 | 0x0165 => "T",  // Ť/ť
        0x0166 | 0x0167 => "T",  // Ŧ/ŧ
        0x0168 | 0x0169 => "U",  // Ũ/ũ
        0x016A | 0x016B => "U",  // Ū/ū
        0x016C | 0x016D => "U",  // Ŭ/ŭ
        0x016E | 0x016F => "U",  // Ů/ů
        0x0170 | 0x0171 => "U",  // Ű/ű
        0x0172 | 0x0173 => "U",  // Ų/ų
        0x0174 | 0x0175 => "W",  // Ŵ/ŵ
        0x0176 | 0x0177 => "Y",  // Ŷ/ŷ
        0x0178 => "Y",           // Ÿ
        0x0179 | 0x017A => "Z",  // Ź/ź
        0x017B | 0x017C => "Z",  // Ż/ż
        0x017D | 0x017E => "Z",  // Ž/ž
        _ => return None,
    };
    Some(mapped)
}

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
        assert_eq!(run_str(&ctx, "SELECT pg_client_encoding()").await, Some("UTF8".into()));
    }

    #[tokio::test]
    async fn to_ascii_transliterates() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_to_ascii_udf());
        // Accented Latin chars are transliterated to their ASCII base (not '?').
        assert_eq!(run_str(&ctx, "SELECT to_ascii('café')").await, Some("cafe".into()));
        assert_eq!(run_str(&ctx, "SELECT to_ascii('München')").await, Some("Munchen".into()));
        assert_eq!(run_str(&ctx, "SELECT to_ascii('hello')").await, Some("hello".into()));
        assert_eq!(run_str(&ctx, "SELECT to_ascii(CAST(NULL AS TEXT))").await, None);
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
