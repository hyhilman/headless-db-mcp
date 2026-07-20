use serde::{Deserialize, Serialize};

/// The entire wire value model for a single database cell.
///
/// Deliberately minimal (mirrors the source project's `PluginCellValue`):
/// numeric/date typing rides in `QueryResult::column_type_names`, not
/// here. Callers reconstruct typed values from the raw text/bytes using
/// the column's declared type name, so this type never needs to grow a
/// variant when a new database adds a numeric/date flavor.
///
/// JSON representation is a Phase 1 (MCP wire layer) concern, not this
/// crate's — `Bytes` currently round-trips through serde_json as a plain
/// array of byte values; base64-encoding it for the wire happens in
/// `db-headless-mcp-wire`, not here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CellValue {
    Null,
    Text(String),
    Bytes(Vec<u8>),
}

impl CellValue {
    pub fn is_null(&self) -> bool {
        matches!(self, CellValue::Null)
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            CellValue::Text(s) => Some(s),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json() {
        for value in [
            CellValue::Null,
            CellValue::Text("hello".to_string()),
            CellValue::Bytes(vec![0, 1, 2, 255]),
        ] {
            let json = serde_json::to_string(&value).expect("serialize");
            let back: CellValue = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(value, back);
        }
    }

    #[test]
    fn as_text_only_matches_text_variant() {
        assert_eq!(CellValue::Text("x".into()).as_text(), Some("x"));
        assert_eq!(CellValue::Null.as_text(), None);
        assert_eq!(CellValue::Bytes(vec![1]).as_text(), None);
    }
}
