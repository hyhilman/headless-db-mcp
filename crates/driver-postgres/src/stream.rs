//! `stream_rows`: real chunked streaming via a transaction-scoped portal.
//!
//! Unlike `execute_user_query` (which accumulates into one `QueryResult`
//! up to a cap), this yields a `StreamHeader` once and then `Rows`
//! batches as they are fetched, without ever holding more than one batch
//! plus what the caller has already consumed in memory. `RowLimits::
//! EMERGENCY_MAX` still applies here as a hard backstop even though this
//! is the "give me everything" API — guardrail #7 is unconditional.
//!
//! Takes the write lock on the driver's connection for the lifetime of
//! the stream, for the same reason `execute_user_query` does: a portal
//! only exists for the duration of the transaction that created it, so
//! the transaction (and therefore exclusive access to the one shared
//! session) must stay open until the stream is fully drained or dropped.

use async_stream::try_stream;
use db_headless_core::{RowLimits, RowStream, StreamElement, StreamHeader};

use crate::convert;
use crate::driver::{not_connected_error, PostgresDriver};
use crate::error::map_query_error;
use crate::query::stmt_columns;

const PORTAL_BATCH_SIZE: i32 = 1000;

pub fn stream_rows<'a>(driver: &'a PostgresDriver, sql: &'a str) -> RowStream<'a> {
    Box::pin(try_stream! {
        let mut guard = driver.client.write().await;
        let connected = guard.as_mut().ok_or_else(not_connected_error)?;

        let txn = connected.client.transaction().await.map_err(map_query_error)?;
        let stmt = txn.prepare(sql).await.map_err(map_query_error)?;
        let portal = txn.bind(&stmt, &[]).await.map_err(map_query_error)?;

        let mut header_sent = false;
        let mut total = 0usize;

        loop {
            if total >= RowLimits::EMERGENCY_MAX {
                break;
            }

            let fetch_size = (RowLimits::EMERGENCY_MAX - total)
                .min(PORTAL_BATCH_SIZE as usize) as i32;
            let batch = txn.query_portal(&portal, fetch_size).await.map_err(map_query_error)?;

            if !header_sent {
                let (columns, column_type_names) = match batch.first() {
                    Some(first) => convert::column_names_and_types(first.columns()),
                    None => stmt_columns(&stmt).unwrap_or_default(),
                };
                yield StreamElement::Header(StreamHeader { columns, column_type_names });
                header_sent = true;
            }

            let batch_len = batch.len();
            if batch_len > 0 {
                let mut rows_out = Vec::with_capacity(batch_len);
                for row in &batch {
                    let mut values = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        values.push(convert::row_value(row, i)?);
                    }
                    rows_out.push(values);
                }
                total += batch_len;
                yield StreamElement::Rows(rows_out);
            }

            if batch_len < fetch_size as usize {
                break;
            }
        }

        // Commit, not roll back: `stream_rows` carries no restriction to
        // `SELECT`-shaped queries, so rolling back here would silently
        // discard a write (e.g. an `INSERT ... RETURNING`) run through
        // this API while still yielding rows and reporting success — the
        // same bug class fixed in `query::execute_user_query`.
        txn.commit().await.map_err(map_query_error)?;
    })
}
