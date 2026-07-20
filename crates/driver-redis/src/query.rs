//! `execute`, `execute_parameterized`, and `execute_user_query`: the
//! "run any raw command" surface. `query` is interpreted as one Redis
//! command line (see `command.rs` for tokenization and parameter
//! binding).
//!
//! The conversion from `redis::Value` to `QueryResult` here is
//! deliberately generic and un-opinionated about shape:
//!
//! - A scalar reply (`Nil`/`Int`/`BulkString`/`SimpleString`/`Okay`/...)
//!   becomes exactly one row with one column named `result`.
//! - An array/set reply becomes one row per top-level element, in a
//!   single column named `value`; a nested/complex element (e.g. a
//!   reply-within-a-reply) is JSON-encoded into that one cell rather than
//!   flattened into more columns.
//!
//! This means `HGETALL` run through `execute*` comes back as one row per
//! flat array element (alternating field, value, field, value, ...), not
//! as real field/value pairs — that generic shape is a deliberate
//! trade-off of running an arbitrary, unknown-in-advance command. Getting
//! real per-type row shapes (a hash's actual field/value pairs, a
//! stream's actual entries) is exactly what the pseudo-table browsing
//! path (`stream_rows`, in `stream.rs`) is for; it knows the command
//! ahead of time and can shape rows accordingly.

use std::time::Duration;
use std::time::Instant;

use redis::aio::ConnectionManager;

use db_headless_core::{CellValue, DriverResult, QueryResult, RowLimits};

use crate::command;
use crate::convert;
use crate::error::map_query_error;

pub(crate) async fn execute(
    manager: &mut ConnectionManager,
    query: &str,
) -> DriverResult<QueryResult> {
    execute_parameterized(manager, query, &[]).await
}

pub(crate) async fn execute_parameterized(
    manager: &mut ConnectionManager,
    query: &str,
    parameters: &[CellValue],
) -> DriverResult<QueryResult> {
    let started = Instant::now();
    let tokens = command::tokenize(query)?;
    let cmd = command::build_command(&tokens, Some(parameters))?;

    let value = cmd
        .query_async::<redis::Value>(manager)
        .await
        .map_err(map_query_error)?;

    let mut result = value_to_query_result(&value);
    result.execution_time = started.elapsed();
    Ok(result)
}

/// Same as `execute_parameterized`, plus a row cap applied by truncating
/// the already-fetched reply. This is a deliberate, documented exception
/// to "never fetch-then-truncate": a single Redis command reply is not a
/// cursor the way a SQL result set is (there is no source-side `LIMIT`
/// concept at the generic command-dispatch level — only the pseudo-table
/// `SCAN` path in `stream.rs` can genuinely fetch incrementally). The cap
/// is clamped to `RowLimits::EMERGENCY_MAX` regardless of what the
/// caller asks for.
pub(crate) async fn execute_user_query(
    manager: &mut ConnectionManager,
    query: &str,
    row_cap: Option<usize>,
    parameters: Option<&[CellValue]>,
    read_only: bool,
) -> DriverResult<QueryResult> {
    if read_only {
        command::require_read_only(query)?;
    }

    let cap = row_cap
        .unwrap_or(RowLimits::EMERGENCY_MAX)
        .min(RowLimits::EMERGENCY_MAX);

    let mut result = execute_parameterized(manager, query, parameters.unwrap_or(&[])).await?;

    if result.rows.len() > cap {
        result.rows.truncate(cap);
        result.is_truncated = true;
    }
    result.rows_affected = result.rows.len() as u64;

    Ok(result)
}

fn value_to_query_result(value: &redis::Value) -> QueryResult {
    match value {
        redis::Value::Array(items) | redis::Value::Set(items) => {
            let rows: Vec<Vec<CellValue>> = items
                .iter()
                .map(|item| vec![convert::value_to_cell(item)])
                .collect();
            QueryResult {
                columns: vec!["value".to_string()],
                column_type_names: vec!["text".to_string()],
                rows_affected: rows.len() as u64,
                rows,
                execution_time: Duration::default(),
                is_truncated: false,
                status_message: None,
                column_meta: None,
            }
        }
        redis::Value::Map(pairs) => {
            let rows: Vec<Vec<CellValue>> = pairs
                .iter()
                .map(|(key, value)| {
                    let encoded = serde_json::json!({
                        "key": convert::value_to_json(key),
                        "value": convert::value_to_json(value),
                    });
                    vec![CellValue::Text(encoded.to_string())]
                })
                .collect();
            QueryResult {
                columns: vec!["value".to_string()],
                column_type_names: vec!["text".to_string()],
                rows_affected: rows.len() as u64,
                rows,
                execution_time: Duration::default(),
                is_truncated: false,
                status_message: None,
                column_meta: None,
            }
        }
        scalar => QueryResult {
            columns: vec!["result".to_string()],
            column_type_names: vec!["text".to_string()],
            rows: vec![vec![convert::value_to_cell(scalar)]],
            rows_affected: 1,
            execution_time: Duration::default(),
            is_truncated: false,
            status_message: None,
            column_meta: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_reply_becomes_one_row_one_result_column() {
        let result = value_to_query_result(&redis::Value::BulkString(b"hello".to_vec()));
        assert_eq!(result.columns, vec!["result".to_string()]);
        assert_eq!(
            result.rows,
            vec![vec![CellValue::Text("hello".to_string())]]
        );
    }

    #[test]
    fn nil_scalar_reply_becomes_one_null_row() {
        let result = value_to_query_result(&redis::Value::Nil);
        assert_eq!(result.rows, vec![vec![CellValue::Null]]);
    }

    #[test]
    fn array_reply_becomes_one_row_per_element_in_value_column() {
        let value = redis::Value::Array(vec![
            redis::Value::BulkString(b"a".to_vec()),
            redis::Value::BulkString(b"b".to_vec()),
            redis::Value::BulkString(b"c".to_vec()),
        ]);
        let result = value_to_query_result(&value);
        assert_eq!(result.columns, vec!["value".to_string()]);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[1], vec![CellValue::Text("b".to_string())]);
    }
}
