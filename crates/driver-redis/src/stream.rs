//! `stream_rows`: real, cursor-based chunked streaming over one
//! pseudo-table (see `schema.rs`).
//!
//! `query` is a small browse specification, not a Redis command:
//! `<pseudo-table> [MATCH <pattern>] [COUNT <n>]`, e.g.
//! `hash MATCH user:* COUNT 500` or just `string` (pattern defaults to
//! `*`, count to `DEFAULT_SCAN_COUNT`). It is tokenized with the same
//! quote-aware tokenizer `command.rs` uses for raw commands.
//!
//! Iteration always goes through Redis's real
//! `SCAN cursor MATCH pattern COUNT n TYPE <type>` (the `TYPE` filter is
//! server-side, added in Redis 6) in genuine batches. `KEYS *` never
//! appears in this driver: Redis's own documentation calls it
//! dangerous/blocking against a large keyspace, which is exactly the
//! failure mode incremental `SCAN` exists to avoid.
//!
//! Each batch of keys is expanded into properly-shaped rows matching
//! `schema::columns_for` for that pseudo-table (real field/value pairs
//! for a hash, real member/score pairs for a zset, ...) — the opposite of
//! `query.rs`'s generic `execute*` path, which knows nothing about the
//! command ahead of time and so cannot shape rows this way.
//!
//! `RowLimits::EMERGENCY_MAX` is a hard cap on the total number of rows
//! yielded across the entire scan: the loop breaks out as soon as the
//! cap is hit, even if the `SCAN` cursor has not reached `0` yet.

use async_stream::try_stream;
use redis::aio::ConnectionManager;

use db_headless_core::{
    CellValue, DriverError, DriverErrorKind, DriverResult, RowLimits, RowStream, StreamElement,
    StreamHeader,
};

use crate::command;
use crate::convert;
use crate::driver::{not_connected_error, RedisDriver};
use crate::error::map_query_error;
use crate::schema;

const DEFAULT_SCAN_COUNT: usize = 500;

#[derive(Debug)]
struct ScanSpec {
    table: String,
    pattern: String,
    count: usize,
}

fn parse_scan_spec(query: &str) -> DriverResult<ScanSpec> {
    let tokens = command::tokenize(query)?;
    let mut tokens = tokens.into_iter();

    let table = tokens
        .next()
        .ok_or_else(|| {
            DriverError::new(
                DriverErrorKind::Query,
                "stream_rows requires a pseudo-table name (string, hash, list, set, zset, or stream)",
            )
        })?
        .to_lowercase();
    schema::validate_pseudo_table(&table)?;

    let rest: Vec<String> = tokens.collect();
    let mut pattern = "*".to_string();
    let mut count = DEFAULT_SCAN_COUNT;
    let mut index = 0;

    while index < rest.len() {
        match rest[index].to_uppercase().as_str() {
            "MATCH" => {
                let value = rest.get(index + 1).ok_or_else(|| {
                    DriverError::new(DriverErrorKind::Query, "MATCH requires a pattern argument")
                })?;
                pattern = value.clone();
                index += 2;
            }
            "COUNT" => {
                let value = rest.get(index + 1).ok_or_else(|| {
                    DriverError::new(DriverErrorKind::Query, "COUNT requires a numeric argument")
                })?;
                count = value.parse::<usize>().map_err(|_| {
                    DriverError::new(
                        DriverErrorKind::Query,
                        format!("COUNT must be a positive integer, got {value:?}"),
                    )
                })?;
                index += 2;
            }
            other => {
                return Err(DriverError::new(
                    DriverErrorKind::Query,
                    format!("unrecognized stream_rows clause {other:?}; expected MATCH or COUNT"),
                ));
            }
        }
    }

    Ok(ScanSpec {
        table,
        pattern,
        count,
    })
}

pub fn stream_rows<'a>(driver: &'a RedisDriver, query: &'a str) -> RowStream<'a> {
    Box::pin(try_stream! {
        let spec = parse_scan_spec(query)?;
        let mut manager: ConnectionManager = {
            let guard = driver.connection.read().await;
            guard.clone().ok_or_else(not_connected_error)?
        };

        let header_columns = schema::columns_for(&spec.table)?;
        yield StreamElement::Header(StreamHeader {
            columns: header_columns.iter().map(|c| c.name.clone()).collect(),
            column_type_names: header_columns.iter().map(|c| c.data_type.clone()).collect(),
        });

        let mut cursor: u64 = 0;
        let mut total = 0usize;

        loop {
            if total >= RowLimits::EMERGENCY_MAX {
                break;
            }

            let (next_cursor, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(spec.pattern.as_str())
                .arg("COUNT")
                .arg(spec.count)
                .arg("TYPE")
                .arg(spec.table.as_str())
                .query_async(&mut manager)
                .await
                .map_err(map_query_error)?;

            let mut batch: Vec<Vec<CellValue>> = Vec::new();
            for key in &keys {
                let key_text = convert::bytes_to_text(key);
                let mut key_rows = fetch_rows_for_key(&mut manager, &spec.table, key, &key_text).await?;
                batch.append(&mut key_rows);
            }

            if total + batch.len() > RowLimits::EMERGENCY_MAX {
                batch.truncate(RowLimits::EMERGENCY_MAX - total);
            }
            total += batch.len();

            if !batch.is_empty() {
                yield StreamElement::Rows(batch);
            }

            if total >= RowLimits::EMERGENCY_MAX || next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }
    })
}

async fn fetch_rows_for_key(
    manager: &mut ConnectionManager,
    table: &str,
    key: &[u8],
    key_text: &str,
) -> DriverResult<Vec<Vec<CellValue>>> {
    match table {
        "string" => {
            let value: redis::Value = redis::cmd("GET")
                .arg(key)
                .query_async(manager)
                .await
                .map_err(map_query_error)?;
            Ok(vec![vec![
                CellValue::Text(key_text.to_string()),
                convert::value_to_cell(&value),
            ]])
        }
        "hash" => {
            let value: redis::Value = redis::cmd("HGETALL")
                .arg(key)
                .query_async(manager)
                .await
                .map_err(map_query_error)?;
            Ok(convert::flat_pairs(&value)
                .into_iter()
                .map(|(field, val)| {
                    vec![
                        CellValue::Text(key_text.to_string()),
                        convert::value_to_cell(&field),
                        convert::value_to_cell(&val),
                    ]
                })
                .collect())
        }
        "list" => {
            let value: redis::Value = redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .query_async(manager)
                .await
                .map_err(map_query_error)?;
            Ok(convert::array_items(&value)
                .into_iter()
                .enumerate()
                .map(|(index, item)| {
                    vec![
                        CellValue::Text(key_text.to_string()),
                        CellValue::Text(index.to_string()),
                        convert::value_to_cell(&item),
                    ]
                })
                .collect())
        }
        "set" => {
            let value: redis::Value = redis::cmd("SMEMBERS")
                .arg(key)
                .query_async(manager)
                .await
                .map_err(map_query_error)?;
            Ok(convert::array_items(&value)
                .into_iter()
                .map(|member| {
                    vec![
                        CellValue::Text(key_text.to_string()),
                        convert::value_to_cell(&member),
                    ]
                })
                .collect())
        }
        "zset" => {
            let value: redis::Value = redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES")
                .query_async(manager)
                .await
                .map_err(map_query_error)?;
            Ok(convert::flat_pairs(&value)
                .into_iter()
                .map(|(member, score)| {
                    vec![
                        CellValue::Text(key_text.to_string()),
                        convert::value_to_cell(&member),
                        convert::value_to_cell(&score),
                    ]
                })
                .collect())
        }
        "stream" => {
            let value: redis::Value = redis::cmd("XRANGE")
                .arg(key)
                .arg("-")
                .arg("+")
                .query_async(manager)
                .await
                .map_err(map_query_error)?;
            let mut rows = Vec::new();
            for entry in convert::array_items(&value) {
                let entry_items = convert::array_items(&entry);
                let id = entry_items
                    .first()
                    .map(convert::value_to_cell)
                    .unwrap_or(CellValue::Null);
                let fields_value = entry_items
                    .get(1)
                    .cloned()
                    .unwrap_or(redis::Value::Array(Vec::new()));
                let mut fields_object = serde_json::Map::new();
                for (field, val) in convert::flat_pairs(&fields_value) {
                    fields_object.insert(field_json_key(&field), convert::value_to_json(&val));
                }
                rows.push(vec![
                    CellValue::Text(key_text.to_string()),
                    id,
                    CellValue::Text(serde_json::Value::Object(fields_object).to_string()),
                ]);
            }
            Ok(rows)
        }
        other => Err(schema::unknown_pseudo_table(other)),
    }
}

fn field_json_key(value: &redis::Value) -> String {
    match value {
        redis::Value::BulkString(bytes) => convert::bytes_to_text(bytes),
        redis::Value::SimpleString(s) => s.clone(),
        other => convert::value_to_json(other).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bare_table_name_with_defaults() {
        let spec = parse_scan_spec("hash").expect("parse");
        assert_eq!(spec.table, "hash");
        assert_eq!(spec.pattern, "*");
        assert_eq!(spec.count, DEFAULT_SCAN_COUNT);
    }

    #[test]
    fn parses_match_and_count_clauses() {
        let spec = parse_scan_spec("hash MATCH user:* COUNT 250").expect("parse");
        assert_eq!(spec.table, "hash");
        assert_eq!(spec.pattern, "user:*");
        assert_eq!(spec.count, 250);
    }

    #[test]
    fn table_name_is_case_insensitive() {
        let spec = parse_scan_spec("STRING").expect("parse");
        assert_eq!(spec.table, "string");
    }

    #[test]
    fn unknown_table_name_is_a_driver_error() {
        let err = parse_scan_spec("bogus").unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn unrecognized_clause_is_a_driver_error() {
        let err = parse_scan_spec("hash LIMIT 10").unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn match_without_pattern_argument_is_a_driver_error() {
        let err = parse_scan_spec("hash MATCH").unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }
}
