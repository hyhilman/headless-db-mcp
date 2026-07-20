//! `execute`, `execute_parameterized`, and `execute_user_query`'s capped
//! path, plus `run_tsv`/`run_tsv_parameterized` — the shared
//! buffered-TSV request helpers `crate::schema`'s introspection queries
//! also run on.
//!
//! `execute`/`execute_parameterized` request
//! `TabSeparatedWithNamesAndTypes` and buffer the whole (typically small,
//! caller-controlled) response — same trade-off `driver-postgres`'s
//! `execute`/`execute_parameterized` make. `execute_user_query` is
//! different: the trait's doc comment explicitly calls out "fetch
//! everything, then truncate" as the anti-pattern to avoid, so *for a
//! real `SELECT`* its capped path reuses the same incremental
//! `JSONEachRow` body streaming as `stream_rows` (`crate::stream`) and
//! simply stops consuming the response once `cap` rows have been
//! collected, dropping the connection rather than reading the rest of a
//! potentially enormous result.
//!
//! But `execute_user_query` is also the one method the generic MCP
//! `execute_query` tool calls for *any* SQL a client sends —
//! `CREATE TABLE`, `INSERT`, `ALTER`, everything, not just `SELECT`s —
//! and the capped path's header probe
//! (`fetch_probe_header`, `SELECT * FROM (<sql>) LIMIT 0 FORMAT ...`)
//! is a real ClickHouse syntax error when `<sql>` is not itself a
//! `SELECT`. `execute_user_query_capped` therefore checks
//! `crate::statement::is_row_returning_statement` first and only takes
//! the capped-streaming path for statements that heuristic recognizes as
//! row-returning; everything else runs through the same buffered
//! `run_tsv`/`run_tsv_parameterized` path `execute`/`execute_parameterized`
//! use (safe to buffer: DDL/DML does not return a large result set, so
//! there is nothing to cap at the source for that case), with the cap
//! applied to whatever rows (if any) come back.

use std::time::Instant;

use db_headless_core::{
    CellValue, DriverError, DriverErrorKind, DriverResult, QueryResult, RowLimits, StreamHeader,
};
use futures_util::StreamExt;

use crate::driver::ClickHouseDriver;
use crate::error::map_reqwest_error;
use crate::jsonl;
use crate::params::{build_param_query_pairs, rewrite_question_marks};
use crate::request::{extract_summary, ConnectedClient};
use crate::statement::is_row_returning_statement;
use crate::tsv;

fn tsv_format_params() -> Vec<(String, String)> {
    vec![
        (
            "default_format".to_string(),
            "TabSeparatedWithNamesAndTypes".to_string(),
        ),
        ("wait_end_of_query".to_string(), "1".to_string()),
    ]
}

fn parameter_count_mismatch(expected: usize, actual: usize) -> DriverError {
    DriverError::new(
        DriverErrorKind::Query,
        format!("query has {expected} `?` placeholder(s) but {actual} parameter(s) were supplied"),
    )
}

pub(crate) struct TsvOutcome {
    pub(crate) columns: Vec<String>,
    pub(crate) column_type_names: Vec<String>,
    pub(crate) rows: Vec<Vec<CellValue>>,
    pub(crate) written_rows: Option<u64>,
}

/// Runs `sql` verbatim through the buffered `TabSeparatedWithNamesAndTypes`
/// path. Used both by `execute` and by every schema-introspection query in
/// `crate::schema` that has no caller-supplied values to bind.
pub(crate) async fn run_tsv(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    sql: &str,
) -> DriverResult<TsvOutcome> {
    let (response, _guard) = driver
        .send_request(connected, sql.to_string(), &tsv_format_params())
        .await?;
    let written_rows = extract_summary(&response).and_then(|summary| summary.written_rows());
    let body = response.text().await.map_err(map_reqwest_error)?;
    let (columns, column_type_names, rows) = tsv::parse_full(&body)?;
    Ok(TsvOutcome {
        columns,
        column_type_names,
        rows,
        written_rows,
    })
}

/// Runs `sql` with `?` placeholders rewritten to ClickHouse's typed named
/// parameters and `parameters` bound out-of-band, through the same
/// buffered TSV path as [`run_tsv`]. Used both by `execute_parameterized`
/// and by every schema-introspection query in `crate::schema` that binds
/// a database/table name as a value (`WHERE name = ?`), never spliced
/// into SQL text.
pub(crate) async fn run_tsv_parameterized(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    sql: &str,
    parameters: &[CellValue],
) -> DriverResult<TsvOutcome> {
    let (rewritten_sql, placeholder_count) = rewrite_question_marks(sql);
    if placeholder_count != parameters.len() {
        return Err(parameter_count_mismatch(
            placeholder_count,
            parameters.len(),
        ));
    }

    let mut params = tsv_format_params();
    params.extend(build_param_query_pairs(parameters)?);

    let (response, _guard) = driver
        .send_request(connected, rewritten_sql, &params)
        .await?;
    let written_rows = extract_summary(&response).and_then(|summary| summary.written_rows());
    let body = response.text().await.map_err(map_reqwest_error)?;
    let (columns, column_type_names, rows) = tsv::parse_full(&body)?;
    Ok(TsvOutcome {
        columns,
        column_type_names,
        rows,
        written_rows,
    })
}

fn rows_affected_from(outcome: &TsvOutcome) -> u64 {
    if outcome.columns.is_empty() && outcome.rows.is_empty() {
        outcome.written_rows.unwrap_or(0)
    } else {
        outcome.rows.len() as u64
    }
}

pub async fn execute(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    sql: &str,
) -> DriverResult<QueryResult> {
    let started = Instant::now();
    let outcome = run_tsv(driver, connected, sql).await?;
    let rows_affected = rows_affected_from(&outcome);

    Ok(QueryResult {
        columns: outcome.columns,
        column_type_names: outcome.column_type_names,
        rows: outcome.rows,
        rows_affected,
        execution_time: started.elapsed(),
        is_truncated: false,
        status_message: None,
        column_meta: None,
    })
}

pub async fn execute_parameterized(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    sql: &str,
    parameters: &[CellValue],
) -> DriverResult<QueryResult> {
    let started = Instant::now();
    let outcome = run_tsv_parameterized(driver, connected, sql, parameters).await?;
    let rows_affected = rows_affected_from(&outcome);

    Ok(QueryResult {
        columns: outcome.columns,
        column_type_names: outcome.column_type_names,
        rows: outcome.rows,
        rows_affected,
        execution_time: started.elapsed(),
        is_truncated: false,
        status_message: None,
        column_meta: None,
    })
}

/// Issues the `SELECT * FROM (<query>) LIMIT 0 FORMAT
/// TabSeparatedWithNamesAndTypes` probe that recovers a typed
/// `StreamHeader` (names *and* types) ahead of a `JSONEachRow` request,
/// which on its own exposes only field names (recoverable from a row's
/// JSON keys) and not even those when the query returns zero rows.
///
/// This assumes `sql` is row-returning (`SELECT`-shaped) — the same
/// assumption every other cursor-based method in this workspace makes
/// (there is no portal to bind a `CREATE TABLE` against in Postgres
/// either). Wrapping a non-`SELECT` statement here will surface as a
/// ClickHouse syntax error from the probe, not silently misbehave.
pub(crate) async fn fetch_probe_header(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    sql: &str,
    extra_params: &[(String, String)],
) -> DriverResult<StreamHeader> {
    let probe_sql = format!("SELECT * FROM ({sql}) LIMIT 0 FORMAT TabSeparatedWithNamesAndTypes");
    let (response, _guard) = driver
        .send_request(connected, probe_sql, extra_params)
        .await?;
    let body = response.text().await.map_err(map_reqwest_error)?;
    let (columns, column_type_names) = tsv::parse_header(&body)?;
    Ok(StreamHeader {
        columns,
        column_type_names,
    })
}

/// `execute_user_query`'s dispatcher: a real `SELECT` (per
/// `is_row_returning_statement`) takes the incremental capped-streaming
/// path (`execute_user_query_capped_streaming`); anything else — DDL,
/// DML, or any statement the heuristic does not recognize — runs through
/// the buffered TSV path instead, since wrapping it in the streaming
/// path's header-probe subquery would be a syntax error, not a
/// degraded-but-working fallback.
pub async fn execute_user_query_capped(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    sql: &str,
    row_cap: Option<usize>,
    parameters: Option<&[CellValue]>,
) -> DriverResult<QueryResult> {
    if is_row_returning_statement(sql) {
        execute_user_query_capped_streaming(driver, connected, sql, row_cap, parameters).await
    } else {
        execute_user_query_capped_buffered(driver, connected, sql, row_cap, parameters).await
    }
}

/// The non-`SELECT` path: runs `sql` through the same buffered
/// `run_tsv`/`run_tsv_parameterized` request `execute`/
/// `execute_parameterized` use (correctly populating `rows_affected`
/// from `X-ClickHouse-Summary`'s `written_rows` for a statement with no
/// result set), then applies `row_cap` to whatever rows did come back.
/// DDL/DML never returns a large result set, so buffering here does not
/// reintroduce the "fetch everything then truncate" problem the capped
/// path exists to avoid for real `SELECT`s.
async fn execute_user_query_capped_buffered(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    sql: &str,
    row_cap: Option<usize>,
    parameters: Option<&[CellValue]>,
) -> DriverResult<QueryResult> {
    let started = Instant::now();
    let cap = row_cap
        .unwrap_or(RowLimits::EMERGENCY_MAX)
        .min(RowLimits::EMERGENCY_MAX);

    let mut outcome = match parameters {
        Some(values) => run_tsv_parameterized(driver, connected, sql, values).await?,
        None => run_tsv(driver, connected, sql).await?,
    };

    let is_truncated = outcome.rows.len() > cap;
    if is_truncated {
        outcome.rows.truncate(cap);
    }

    let rows_affected = if outcome.columns.is_empty() && outcome.rows.is_empty() {
        outcome.written_rows.unwrap_or(0)
    } else {
        outcome.rows.len() as u64
    };

    Ok(QueryResult {
        columns: outcome.columns,
        column_type_names: outcome.column_type_names,
        rows: outcome.rows,
        rows_affected,
        execution_time: started.elapsed(),
        is_truncated,
        status_message: None,
        column_meta: None,
    })
}

/// `execute_user_query`'s capped path for a real `SELECT`: real
/// incremental `JSONEachRow` body consumption via `bytes_stream()`,
/// stopping as soon as `cap` rows have been collected instead of reading
/// (and buffering) the rest of the HTTP response.
async fn execute_user_query_capped_streaming(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    sql: &str,
    row_cap: Option<usize>,
    parameters: Option<&[CellValue]>,
) -> DriverResult<QueryResult> {
    let started = Instant::now();
    let cap = row_cap
        .unwrap_or(RowLimits::EMERGENCY_MAX)
        .min(RowLimits::EMERGENCY_MAX);

    let (rewritten_sql, param_pairs) = match parameters {
        Some(values) => {
            let (rewritten, placeholder_count) = rewrite_question_marks(sql);
            if placeholder_count != values.len() {
                return Err(parameter_count_mismatch(placeholder_count, values.len()));
            }
            (rewritten, build_param_query_pairs(values)?)
        }
        None => (sql.to_string(), Vec::new()),
    };

    let header = fetch_probe_header(driver, connected, &rewritten_sql, &param_pairs).await?;

    let mut stream_params = vec![("default_format".to_string(), "JSONEachRow".to_string())];
    stream_params.extend(param_pairs);
    let (response, _guard) = driver
        .send_request(connected, rewritten_sql, &stream_params)
        .await?;

    let mut byte_stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut rows: Vec<Vec<CellValue>> = Vec::new();
    let mut is_truncated = false;

    'outer: while let Some(chunk) = byte_stream.next().await {
        let bytes = chunk.map_err(map_reqwest_error)?;
        buffer.push_str(&String::from_utf8_lossy(&bytes));

        while let Some(newline_at) = buffer.find('\n') {
            let line = buffer[..newline_at].to_string();
            buffer.drain(..=newline_at);
            if line.trim().is_empty() {
                continue;
            }
            rows.push(jsonl::parse_row(&line, &header.columns)?);
            if rows.len() >= cap {
                is_truncated = true;
                break 'outer;
            }
        }
    }

    if !is_truncated && !buffer.trim().is_empty() {
        rows.push(jsonl::parse_row(&buffer, &header.columns)?);
    }

    let rows_affected = rows.len() as u64;
    Ok(QueryResult {
        columns: header.columns,
        column_type_names: header.column_type_names,
        rows,
        rows_affected,
        execution_time: started.elapsed(),
        is_truncated,
        status_message: None,
        column_meta: None,
    })
}
