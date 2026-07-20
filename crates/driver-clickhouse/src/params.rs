//! Rewrites this workspace's `?`-placeholder convention
//! (`ParameterStyle::QuestionMark`) into ClickHouse's own typed named
//! parameters, with values sent out-of-band as `param_pN` HTTP query
//! parameters — never spliced into the SQL body text.
//!
//! **Every** `?` is rewritten to `unhex({pN:String})`, and every bound
//! value (`CellValue::Text` and `CellValue::Bytes` alike) is sent as
//! plain hex text with no prefix, not the value's own bytes. This is not
//! what a first read of ClickHouse's parameter docs suggests — the
//! obvious design is `{pN:String}` with the text sent as-is — but that
//! was verified against a real ClickHouse 23.3 server to fail in two
//! ways that matter for this driver's guardrails:
//!
//! - A value containing a literal tab character is rejected outright:
//!   `Code: 457. DB::Exception: Value a<TAB>b cannot be parsed as String
//!   for query parameter 'p1' ... only 1 of 3 bytes was parsed`. This
//!   driver is required to round-trip a value with an embedded tab, so
//!   the naive design cannot satisfy that guardrail at all, not just
//!   awkwardly.
//! - A bare `0x<hex>` value bound to a `{pN:String}` placeholder — the
//!   convention this module originally tried, matching an assumption
//!   carried over from the source project's audit — is *not* decoded to
//!   raw bytes. ClickHouse stores the literal 12-character text
//!   `"0x68656c6c6f"`, not the 5 decoded bytes `"hello"`. `String`
//!   parameters are parsed with the same text-literal grammar
//!   `clickhouse-client` uses for `Values` input, which has no special
//!   case for a `0x` prefix (that syntax exists only for numeric-type
//!   literals).
//!
//! Wrapping every placeholder in `unhex(...)` and sending only
//! hex-digest text sidesteps both problems: hex digits are never tab,
//! backslash, quote, or anything else the `Values` parser treats
//! specially, and `unhex` reconstructs the exact original bytes
//! (verified for embedded tabs, backslashes, single quotes, and a
//! `'; DROP TABLE ...; --`-shaped payload). `CellValue::Text` is hex
//! encoded from its UTF-8 bytes; `CellValue::Bytes` is hex encoded
//! directly — both reconstruct as an exact byte-for-byte ClickHouse
//! `String`, since ClickHouse strings are just byte arrays with no
//! separate binary/text distinction.
//!
//! **Known limitation**: ClickHouse SQL has a ternary operator
//! (`cond ? a : b`) that also uses a bare `?`. This rewriter cannot tell
//! that usage apart from a bind placeholder (same constraint any
//! `?`-placeholder client library has), so a query using the ternary
//! operator cannot be run through `execute_parameterized`/
//! `execute_user_query`'s parameterized path. This is inherent to
//! `ParameterStyle::QuestionMark` being the workspace-wide convention,
//! not a bug in the rewriter.

use db_headless_core::{CellValue, DriverError, DriverErrorKind, DriverResult};

/// Rewrites every `?` outside a single-quoted string literal or a
/// backtick-quoted identifier into a sequentially numbered
/// `unhex({pN:String})` expression, returning the rewritten SQL and the
/// count of placeholders substituted.
pub fn rewrite_question_marks(sql: &str) -> (String, usize) {
    let mut out = String::with_capacity(sql.len() + 16);
    let mut chars = sql.chars().peekable();
    let mut in_single_quote = false;
    let mut in_backtick = false;
    let mut placeholder_count = 0usize;

    while let Some(ch) = chars.next() {
        if in_single_quote {
            out.push(ch);
            if ch == '\\' {
                if let Some(escaped) = chars.next() {
                    out.push(escaped);
                }
                continue;
            }
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    out.push(chars.next().expect("peeked char exists"));
                    continue;
                }
                in_single_quote = false;
            }
            continue;
        }

        if in_backtick {
            out.push(ch);
            if ch == '`' {
                if chars.peek() == Some(&'`') {
                    out.push(chars.next().expect("peeked char exists"));
                    continue;
                }
                in_backtick = false;
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single_quote = true;
                out.push(ch);
            }
            '`' => {
                in_backtick = true;
                out.push(ch);
            }
            '?' => {
                placeholder_count += 1;
                out.push_str(&format!("unhex({{p{placeholder_count}:String}})"));
            }
            _ => out.push(ch),
        }
    }

    (out, placeholder_count)
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Builds the `param_pN=<hex>` query-string pairs ClickHouse's HTTP
/// interface expects, out-of-band from the SQL body — see the module
/// doc comment for why every value is hex-encoded (and every placeholder
/// wrapped in `unhex(...)`) rather than sent as plain text.
///
/// Binding a `CellValue::Null` is a real, documented gap: `unhex(NULL)`
/// would need the placeholder to be `Nullable`, which requires knowing
/// the target column's real type — information this driver does not
/// have (every placeholder is typed `String`). Rather than send
/// something silently wrong, this returns a clear error.
pub fn build_param_query_pairs(values: &[CellValue]) -> DriverResult<Vec<(String, String)>> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let key = format!("param_p{}", index + 1);
            match value {
                CellValue::Text(text) => Ok((key, to_hex(text.as_bytes()))),
                CellValue::Bytes(bytes) => Ok((key, to_hex(bytes))),
                CellValue::Null => Err(DriverError::new(
                    DriverErrorKind::Query,
                    "binding a NULL parameter is not supported yet: ClickHouse HTTP named \
                     parameters are typed (this driver types every placeholder as `String`), \
                     and making a placeholder correctly Nullable requires type information this \
                     driver does not have",
                )),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_sequential_question_marks() {
        let (sql, count) = rewrite_question_marks("SELECT * FROM t WHERE a = ? AND b = ?");
        assert_eq!(
            sql,
            "SELECT * FROM t WHERE a = unhex({p1:String}) AND b = unhex({p2:String})"
        );
        assert_eq!(count, 2);
    }

    #[test]
    fn ignores_question_mark_inside_single_quoted_literal() {
        let (sql, count) = rewrite_question_marks("SELECT '?' AS literal WHERE a = ?");
        assert_eq!(sql, "SELECT '?' AS literal WHERE a = unhex({p1:String})");
        assert_eq!(count, 1);
    }

    #[test]
    fn ignores_question_mark_inside_backtick_identifier() {
        let (sql, count) = rewrite_question_marks("SELECT `weird?col` FROM t WHERE a = ?");
        assert_eq!(
            sql,
            "SELECT `weird?col` FROM t WHERE a = unhex({p1:String})"
        );
        assert_eq!(count, 1);
    }

    #[test]
    fn handles_doubled_single_quote_escape_without_exiting_early() {
        let (sql, count) = rewrite_question_marks("SELECT 'it''s a ? test' WHERE a = ?");
        assert_eq!(sql, "SELECT 'it''s a ? test' WHERE a = unhex({p1:String})");
        assert_eq!(count, 1);
    }

    #[test]
    fn text_param_is_hex_encoded_from_utf8_bytes() {
        let pairs = build_param_query_pairs(&[CellValue::Text("hi".to_string())]).unwrap();
        assert_eq!(pairs, vec![("param_p1".to_string(), "6869".to_string())]);
    }

    #[test]
    fn bytes_param_is_hex_encoded_with_no_prefix() {
        let pairs =
            build_param_query_pairs(&[CellValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef])]).unwrap();
        assert_eq!(
            pairs,
            vec![("param_p1".to_string(), "deadbeef".to_string())]
        );
    }

    #[test]
    fn null_param_is_a_clear_error_not_a_silent_wrong_value() {
        let err = build_param_query_pairs(&[CellValue::Null]).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
        assert!(err.message.to_lowercase().contains("null"));
    }
}
