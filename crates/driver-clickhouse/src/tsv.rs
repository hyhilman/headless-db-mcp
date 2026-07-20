//! Parses `TabSeparatedWithNamesAndTypes` response bodies from ClickHouse's
//! HTTP interface.
//!
//! The wire format: a header line of column names, a second header line of
//! column type names, then one line per data row — all tab-separated. Row
//! separators (`\n`) and cell separators (`\t`) always appear as raw bytes
//! in the response; a literal tab or newline *inside* a value is always
//! backslash-escaped (`\t`, `\n`) by ClickHouse's own formatter, so naively
//! splitting on raw `\t`/`\n` bytes is safe and unambiguous — the escaping
//! is what keeps it that way. Each cell must still be run through
//! [`unescape_text`] to turn those backslash escapes back into the real
//! characters, and `\N` on its own denotes SQL NULL.
//!
//! This module only parses successful response bodies; HTTP-level and
//! ClickHouse-exception error responses are handled by `crate::error`
//! before a body ever reaches here.

use db_headless_core::{CellValue, DriverError, DriverErrorKind, DriverResult};

/// `(column names, column type names, data rows)`.
type ParsedTsv = (Vec<String>, Vec<String>, Vec<Vec<CellValue>>);

/// Reverses ClickHouse's TSV backslash-escaping for a single cell's raw
/// text, turning `\t`/`\n`/`\\`/... back into the literal characters they
/// stand for. An unrecognized escape sequence drops the backslash and
/// keeps the following character literally, matching ClickHouse's own
/// lenient behavior rather than erroring on it.
pub fn unescape_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('0') => out.push('\0'),
            Some('b') => out.push('\u{08}'),
            Some('f') => out.push('\u{0c}'),
            Some('a') => out.push('\u{07}'),
            Some('v') => out.push('\u{0b}'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }

    out
}

/// Parses one already-unescaped-boundary data cell: `\N` alone means SQL
/// NULL, anything else is unescaped text.
pub fn parse_data_cell(raw: &str) -> CellValue {
    if raw == "\\N" {
        CellValue::Null
    } else {
        CellValue::Text(unescape_text(raw))
    }
}

fn split_lines(body: &str) -> Vec<&str> {
    let mut lines: Vec<&str> = body.split('\n').collect();
    if lines.last() == Some(&"") {
        lines.pop();
    }
    lines
}

/// Parses just the two header lines (column names, column type names),
/// ignoring any data rows that follow. Used for the `LIMIT 0` probe query
/// that recovers a typed header ahead of a `JSONEachRow` stream.
pub fn parse_header(body: &str) -> DriverResult<(Vec<String>, Vec<String>)> {
    let lines = split_lines(body);
    let mut iter = lines.into_iter();

    let names_line = iter.next().unwrap_or_default();
    let types_line = iter.next().unwrap_or_default();

    let columns = names_line
        .split('\t')
        .map(unescape_text)
        .collect::<Vec<_>>();
    let column_type_names = types_line
        .split('\t')
        .map(unescape_text)
        .collect::<Vec<_>>();

    if columns.len() != column_type_names.len() {
        return Err(DriverError::new(
            DriverErrorKind::Protocol,
            "TabSeparatedWithNamesAndTypes header had a mismatched number of names and types",
        ));
    }

    Ok((columns, column_type_names))
}

/// Parses a full `TabSeparatedWithNamesAndTypes` body: two header lines
/// followed by zero or more data rows. An entirely empty body (as
/// ClickHouse returns for a DDL statement or an `INSERT` with no result
/// set) parses as no columns and no rows, not an error.
pub fn parse_full(body: &str) -> DriverResult<ParsedTsv> {
    if body.is_empty() {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    }

    let lines = split_lines(body);
    if lines.len() < 2 {
        return Err(DriverError::new(
            DriverErrorKind::Protocol,
            "TabSeparatedWithNamesAndTypes response was missing its header lines",
        ));
    }

    let columns = lines[0].split('\t').map(unescape_text).collect::<Vec<_>>();
    let column_type_names = lines[1].split('\t').map(unescape_text).collect::<Vec<_>>();

    if columns.len() != column_type_names.len() {
        return Err(DriverError::new(
            DriverErrorKind::Protocol,
            "TabSeparatedWithNamesAndTypes header had a mismatched number of names and types",
        ));
    }

    let mut rows = Vec::with_capacity(lines.len().saturating_sub(2));
    for line in &lines[2..] {
        let cells = line.split('\t').map(parse_data_cell).collect::<Vec<_>>();
        rows.push(cells);
    }

    Ok((columns, column_type_names, rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unescapes_tab_and_backslash_and_newline() {
        assert_eq!(unescape_text("a\\tb"), "a\tb");
        assert_eq!(unescape_text("a\\\\b"), "a\\b");
        assert_eq!(unescape_text("a\\nb"), "a\nb");
    }

    #[test]
    fn unknown_escape_drops_the_backslash() {
        assert_eq!(unescape_text("a\\qb"), "aqb");
    }

    #[test]
    fn parse_data_cell_recognizes_null_marker() {
        assert_eq!(parse_data_cell("\\N"), CellValue::Null);
        assert_eq!(
            parse_data_cell("hello"),
            CellValue::Text("hello".to_string())
        );
    }

    #[test]
    fn parse_full_handles_empty_body_as_no_result_set() {
        let (columns, types, rows) = parse_full("").expect("parse");
        assert!(columns.is_empty());
        assert!(types.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn parse_full_round_trips_names_types_and_rows() {
        let body = "id\tval\nUInt32\tString\n1\thello\n2\t\\N\n";
        let (columns, types, rows) = parse_full(body).expect("parse");
        assert_eq!(columns, vec!["id".to_string(), "val".to_string()]);
        assert_eq!(types, vec!["UInt32".to_string(), "String".to_string()]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], CellValue::Text("hello".to_string()));
        assert_eq!(rows[1][1], CellValue::Null);
    }

    #[test]
    fn parse_full_round_trips_embedded_tab_and_backslash() {
        let body = "val\nString\nhas\\ta\\\\tab and backslash\n";
        let (_, _, rows) = parse_full(body).expect("parse");
        assert_eq!(
            rows[0][0],
            CellValue::Text("has\ta\\tab and backslash".to_string())
        );
    }

    #[test]
    fn parse_header_ignores_data_rows() {
        let body = "id\nUInt32\n1\n2\n3\n";
        let (columns, types) = parse_header(body).expect("parse header");
        assert_eq!(columns, vec!["id".to_string()]);
        assert_eq!(types, vec!["UInt32".to_string()]);
    }
}
