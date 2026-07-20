//! Converts a `redis::Value` (this driver always negotiates RESP2 — see
//! `config::build_connection_info`) into this crate's transfer types.
//!
//! Redis wire values carry no separate "type name" the way a SQL column
//! does, so every conversion here lands on `CellValue::Text`, using a
//! **lossy** UTF-8 decode for binary payloads (`String::from_utf8_lossy`).
//! A key/field/value that is not valid UTF-8 renders with U+FFFD
//! replacement characters rather than failing the whole query — this is a
//! deliberate, documented simplification for a browsing/inspection driver,
//! not a lossless binary-safe round trip. Callers that need exact bytes
//! back should bind the value as a parameter instead of reading it back
//! through this generic conversion.

use redis::Value;
use serde_json::{Map as JsonMap, Value as JsonValue};

use db_headless_core::CellValue;

pub(crate) fn bytes_to_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Renders a single `redis::Value` as one `CellValue::Text`, JSON-encoding
/// any nested structure (`Array`/`Set`/`Map`/`Attribute`/`Push`) rather
/// than trying to flatten it into more cells — see the module doc on
/// `value_to_query_result` for why the generic `execute*` path is
/// deliberately un-opinionated about shape.
pub(crate) fn value_to_cell(value: &Value) -> CellValue {
    match value {
        Value::Nil => CellValue::Null,
        Value::Int(i) => CellValue::Text(i.to_string()),
        Value::BulkString(bytes) => CellValue::Text(bytes_to_text(bytes)),
        Value::SimpleString(s) => CellValue::Text(s.clone()),
        Value::Okay => CellValue::Text("OK".to_string()),
        Value::Double(d) => CellValue::Text(d.to_string()),
        Value::Boolean(b) => CellValue::Text(b.to_string()),
        Value::VerbatimString { text, .. } => CellValue::Text(text.clone()),
        Value::BigNumber(n) => CellValue::Text(n.to_string()),
        Value::ServerError(err) => CellValue::Text(format!("{err:?}")),
        Value::Array(_)
        | Value::Set(_)
        | Value::Map(_)
        | Value::Attribute { .. }
        | Value::Push { .. } => CellValue::Text(value_to_json(value).to_string()),
    }
}

pub(crate) fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Nil => JsonValue::Null,
        Value::Int(i) => JsonValue::from(*i),
        Value::BulkString(bytes) => JsonValue::String(bytes_to_text(bytes)),
        Value::SimpleString(s) => JsonValue::String(s.clone()),
        Value::Okay => JsonValue::String("OK".to_string()),
        Value::Array(items) | Value::Set(items) => {
            JsonValue::Array(items.iter().map(value_to_json).collect())
        }
        Value::Map(pairs) => {
            let mut object = JsonMap::with_capacity(pairs.len());
            for (key, value) in pairs {
                object.insert(json_key(key), value_to_json(value));
            }
            JsonValue::Object(object)
        }
        Value::Attribute { data, .. } => value_to_json(data),
        Value::Double(d) => JsonValue::from(*d),
        Value::Boolean(b) => JsonValue::Bool(*b),
        Value::VerbatimString { text, .. } => JsonValue::String(text.clone()),
        Value::BigNumber(n) => JsonValue::String(n.to_string()),
        Value::Push { data, .. } => JsonValue::Array(data.iter().map(value_to_json).collect()),
        Value::ServerError(err) => JsonValue::String(format!("{err:?}")),
    }
}

fn json_key(value: &Value) -> String {
    match value {
        Value::BulkString(bytes) => bytes_to_text(bytes),
        Value::SimpleString(s) => s.clone(),
        other => value_to_json(other).to_string(),
    }
}

/// Flattens a RESP2-shaped alternating `[k1, v1, k2, v2, ...]` reply
/// (`HGETALL`, `ZRANGE ... WITHSCORES`) into `(key, value)` pairs.
/// Defensively also accepts a RESP3 `Value::Map` directly, even though
/// this driver always requests RESP2 (see `config::build_connection_info`)
/// so a real server should never actually send one.
pub(crate) fn flat_pairs(value: &Value) -> Vec<(Value, Value)> {
    match value {
        Value::Map(pairs) => pairs.clone(),
        Value::Array(items) | Value::Set(items) => items
            .chunks(2)
            .filter(|chunk| chunk.len() == 2)
            .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
            .collect(),
        _ => Vec::new(),
    }
}

/// Normalizes an array-shaped reply (`SMEMBERS`, `LRANGE`, `XRANGE`) into
/// a plain `Vec<Value>`. `Nil` (e.g. a range over a key that does not
/// exist) becomes an empty list rather than a one-element list containing
/// `Nil`.
pub(crate) fn array_items(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(items) | Value::Set(items) => items.clone(),
        Value::Nil => Vec::new(),
        other => vec![other.clone()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_becomes_null_cell() {
        assert_eq!(value_to_cell(&Value::Nil), CellValue::Null);
    }

    #[test]
    fn bulk_string_becomes_text_cell() {
        assert_eq!(
            value_to_cell(&Value::BulkString(b"hello".to_vec())),
            CellValue::Text("hello".to_string())
        );
    }

    #[test]
    fn invalid_utf8_bulk_string_is_lossily_decoded_not_rejected() {
        let cell = value_to_cell(&Value::BulkString(vec![0xff, 0xfe]));
        assert!(matches!(cell, CellValue::Text(_)));
    }

    #[test]
    fn nested_array_is_json_encoded_as_one_cell() {
        let value = Value::Array(vec![Value::Int(1), Value::BulkString(b"two".to_vec())]);
        let cell = value_to_cell(&value);
        assert_eq!(cell, CellValue::Text("[1,\"two\"]".to_string()));
    }

    #[test]
    fn flat_pairs_chunks_alternating_array_into_pairs() {
        let value = Value::Array(vec![
            Value::BulkString(b"field1".to_vec()),
            Value::BulkString(b"value1".to_vec()),
            Value::BulkString(b"field2".to_vec()),
            Value::BulkString(b"value2".to_vec()),
        ]);
        let pairs = flat_pairs(&value);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, Value::BulkString(b"field1".to_vec()));
        assert_eq!(pairs[1].1, Value::BulkString(b"value2".to_vec()));
    }

    #[test]
    fn flat_pairs_drops_a_trailing_unpaired_element() {
        let value = Value::Array(vec![Value::BulkString(b"orphan".to_vec())]);
        assert!(flat_pairs(&value).is_empty());
    }

    #[test]
    fn array_items_treats_nil_as_empty() {
        assert!(array_items(&Value::Nil).is_empty());
    }

    #[test]
    fn array_items_wraps_a_bare_scalar_as_one_element() {
        let items = array_items(&Value::Int(5));
        assert_eq!(items, vec![Value::Int(5)]);
    }
}
