//! Parses one `JSONEachRow` line (a single JSON object mapping column
//! name -> value) into a `Vec<CellValue>`, ordered to match an
//! already-known column list.
//!
//! `JSONEachRow` carries no type information of its own — a `Nullable`
//! column's value is either the real value or JSON `null`, and there is
//! nothing else to introspect. Column order in the JSON object itself is
//! not relied on either (`serde_json`'s default `Map` does not preserve
//! insertion order without the `preserve_order` feature, which this crate
//! does not enable): callers already know the true column order and types
//! from the `TabSeparatedWithNamesAndTypes` probe request
//! (`crate::request::fetch_probe_header`), so each row is looked up by
//! name against that known list instead.

use db_headless_core::{CellValue, DriverError, DriverErrorKind, DriverResult};
use serde_json::Value;

fn json_value_to_cell(value: Option<&Value>) -> CellValue {
    match value {
        None | Some(Value::Null) => CellValue::Null,
        Some(Value::String(text)) => CellValue::Text(text.clone()),
        Some(other) => CellValue::Text(other.to_string()),
    }
}

/// Parses a single non-empty `JSONEachRow` line into a row ordered to
/// match `columns`.
pub fn parse_row(line: &str, columns: &[String]) -> DriverResult<Vec<CellValue>> {
    let parsed: Value = serde_json::from_str(line).map_err(|err| {
        DriverError::new(
            DriverErrorKind::Protocol,
            format!("failed to parse a JSONEachRow line: {err}"),
        )
    })?;

    let object = parsed.as_object().ok_or_else(|| {
        DriverError::new(
            DriverErrorKind::Protocol,
            "a JSONEachRow line was not a JSON object",
        )
    })?;

    Ok(columns
        .iter()
        .map(|column| json_value_to_cell(object.get(column)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_row_matching_known_column_order() {
        let columns = vec!["b".to_string(), "a".to_string()];
        let row = parse_row(r#"{"a": "x", "b": "y"}"#, &columns).expect("parse row");
        assert_eq!(
            row,
            vec![
                CellValue::Text("y".to_string()),
                CellValue::Text("x".to_string())
            ]
        );
    }

    #[test]
    fn null_field_becomes_null_cell() {
        let columns = vec!["a".to_string()];
        let row = parse_row(r#"{"a": null}"#, &columns).expect("parse row");
        assert_eq!(row, vec![CellValue::Null]);
    }

    #[test]
    fn missing_field_becomes_null_cell() {
        let columns = vec!["a".to_string(), "missing".to_string()];
        let row = parse_row(r#"{"a": "x"}"#, &columns).expect("parse row");
        assert_eq!(row[1], CellValue::Null);
    }

    #[test]
    fn non_string_values_are_stringified() {
        let columns = vec!["n".to_string(), "flag".to_string()];
        let row = parse_row(r#"{"n": 42, "flag": true}"#, &columns).expect("parse row");
        assert_eq!(row[0], CellValue::Text("42".to_string()));
        assert_eq!(row[1], CellValue::Text("true".to_string()));
    }

    #[test]
    fn invalid_json_is_a_protocol_error() {
        let err = parse_row("not json", &[]).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Protocol);
    }
}
