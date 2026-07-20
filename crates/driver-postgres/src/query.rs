//! `execute`, `execute_parameterized`, and `execute_user_query`.

use std::time::Instant;

use db_headless_core::{CellValue, DriverResult, QueryResult, RowLimits};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

use crate::convert;
use crate::error::map_query_error;
use crate::params::{as_sql_params, to_params};

const PORTAL_BATCH_SIZE: i32 = 1000;

/// Runs `sql` through the simple query protocol (`Client::simple_query`),
/// which supports multiple `;`-separated statements and takes no bound
/// parameters. Chosen over the extended protocol (`Client::query`) for
/// `execute` specifically to preserve multi-statement script support
/// (e.g. running a whole pasted DDL script) — something the extended
/// protocol cannot do (a `Parse` message may only contain one statement).
///
/// The trade-off: the simple query protocol returns every value as text
/// and exposes no column type OIDs (`tokio_postgres::SimpleColumn` only
/// has a name), so `column_type_names` here is always `"text"` rather
/// than the real Postgres type name. Callers that need real column types
/// should use `execute_parameterized` or `execute_user_query`, which run
/// through the extended protocol and populate `column_type_names` from
/// the server's actual column metadata.
pub async fn execute(client: &Client, sql: &str) -> DriverResult<QueryResult> {
    let started = Instant::now();
    let messages = client.simple_query(sql).await.map_err(map_query_error)?;

    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<CellValue>> = Vec::new();
    let mut rows_affected: u64 = 0;

    for message in messages {
        match message {
            tokio_postgres::SimpleQueryMessage::RowDescription(cols) => {
                columns = cols.iter().map(|c| c.name().to_string()).collect();
                rows.clear();
            }
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    values.push(match row.get(i) {
                        Some(text) => CellValue::Text(text.to_string()),
                        None => CellValue::Null,
                    });
                }
                rows.push(values);
            }
            tokio_postgres::SimpleQueryMessage::CommandComplete(count) => {
                rows_affected = count;
            }
            _ => {}
        }
    }

    let column_type_names = vec!["text".to_string(); columns.len()];

    Ok(QueryResult {
        columns,
        column_type_names,
        rows,
        rows_affected,
        execution_time: started.elapsed(),
        is_truncated: false,
        status_message: None,
        column_meta: None,
    })
}

pub async fn execute_parameterized(
    client: &Client,
    sql: &str,
    parameters: &[CellValue],
) -> DriverResult<QueryResult> {
    let started = Instant::now();
    let bound = to_params(parameters);
    let sql_params = as_sql_params(&bound);

    let rows = client
        .query(sql, &sql_params)
        .await
        .map_err(map_query_error)?;

    let (columns, column_type_names) = match rows.first() {
        Some(row) => convert::column_names_and_types(row.columns()),
        None => (Vec::new(), Vec::new()),
    };

    let mut result_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut values = Vec::with_capacity(row.len());
        for i in 0..row.len() {
            values.push(convert::row_value(row, i)?);
        }
        result_rows.push(values);
    }

    let rows_affected = result_rows.len() as u64;

    Ok(QueryResult {
        columns,
        column_type_names,
        rows: result_rows,
        rows_affected,
        execution_time: started.elapsed(),
        is_truncated: false,
        status_message: None,
        column_meta: None,
    })
}

/// Runs `sql` through a transaction-scoped portal, fetching rows in
/// batches of `PORTAL_BATCH_SIZE` instead of pulling the whole result set
/// into memory (the trait's doc comment explicitly flags "fetch
/// everything then truncate" as an anti-pattern for this method).
///
/// `row_cap` is clamped to `RowLimits::EMERGENCY_MAX` regardless of what
/// the caller asks for. The loop checks `rows.len() >= cap` *before*
/// computing the next fetch size, so a fetch is never issued with size
/// zero (Postgres's `Execute` protocol message treats a max-rows of zero
/// as "fetch all remaining rows", the opposite of what capping means
/// here — an off-by-one on this would silently ignore the cap rather
/// than error, exactly the kind of bug this method exists to avoid).
///
/// `is_truncated` is set whenever the loop stops because it hit the cap,
/// including the edge case where the result set's true size exactly
/// equals the cap: distinguishing "hit the cap with more rows behind it"
/// from "hit the cap exactly at the end" would need one extra fetch spent
/// purely to check for a next row. This is a deliberately conservative
/// approximation — it may mark an exact-cap result as truncated when it
/// was not, but never the reverse.
pub async fn execute_user_query(
    client: &mut Client,
    sql: &str,
    row_cap: Option<usize>,
    parameters: Option<&[CellValue]>,
    read_only: bool,
) -> DriverResult<QueryResult> {
    let started = Instant::now();
    let cap = row_cap
        .unwrap_or(RowLimits::EMERGENCY_MAX)
        .min(RowLimits::EMERGENCY_MAX);

    // `read_only` opens the transaction with Postgres's own `BEGIN READ
    // ONLY`, so the server itself rejects any write inside it — a real
    // engine-level guarantee, not a client-side statement-string check
    // that a CTE, function call, or `SELECT ... FOR UPDATE` could slip past.
    let txn = client
        .build_transaction()
        .read_only(read_only)
        .start()
        .await
        .map_err(map_query_error)?;
    let stmt = txn.prepare(sql).await.map_err(map_query_error)?;

    let owned;
    let sql_params: Vec<&(dyn ToSql + Sync)> = match parameters {
        Some(p) => {
            owned = to_params(p);
            as_sql_params(&owned)
        }
        None => Vec::new(),
    };

    let portal = txn
        .bind(&stmt, &sql_params)
        .await
        .map_err(map_query_error)?;

    let mut columns: Vec<String> = Vec::new();
    let mut column_type_names: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<CellValue>> = Vec::new();
    let mut is_truncated = false;

    loop {
        if rows.len() >= cap {
            is_truncated = true;
            break;
        }

        let fetch_size = (cap - rows.len()).min(PORTAL_BATCH_SIZE as usize) as i32;
        let batch = txn
            .query_portal(&portal, fetch_size)
            .await
            .map_err(map_query_error)?;

        if columns.is_empty() {
            if let Some(first) = batch.first() {
                let (names, types) = convert::column_names_and_types(first.columns());
                columns = names;
                column_type_names = types;
            } else if let Some((names, types)) = stmt_columns(&stmt) {
                columns = names;
                column_type_names = types;
            }
        }

        let batch_len = batch.len();
        for row in &batch {
            let mut values = Vec::with_capacity(row.len());
            for i in 0..row.len() {
                values.push(convert::row_value(row, i)?);
            }
            rows.push(values);
        }

        if batch_len < fetch_size as usize {
            break;
        }
    }

    // Commit, never roll back: this method runs whatever SQL the caller
    // gave it, not just `SELECT`s (the trait puts no such restriction on
    // `execute_user_query`), and a silently-rolled-back INSERT/UPDATE/
    // DELETE would report success while persisting nothing. Committing a
    // read-only statement is a no-op for correctness purposes either way.
    txn.commit().await.map_err(map_query_error)?;

    let rows_affected = rows.len() as u64;

    Ok(QueryResult {
        columns,
        column_type_names,
        rows,
        rows_affected,
        execution_time: started.elapsed(),
        is_truncated,
        status_message: None,
        column_meta: None,
    })
}

pub(crate) fn stmt_columns(stmt: &tokio_postgres::Statement) -> Option<(Vec<String>, Vec<String>)> {
    let columns = stmt.columns();
    if columns.is_empty() {
        return None;
    }
    Some((
        columns.iter().map(|c| c.name().to_string()).collect(),
        columns
            .iter()
            .map(|c| c.type_().name().to_string())
            .collect(),
    ))
}
