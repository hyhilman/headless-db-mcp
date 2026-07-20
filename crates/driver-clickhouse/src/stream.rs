//! `stream_rows`: real chunked streaming over ClickHouse's HTTP interface,
//! never buffering the full result set.
//!
//! Requests `JSONEachRow` (one JSON object per line) instead of the
//! buffered `TabSeparatedWithNamesAndTypes` format `execute`/
//! `execute_parameterized` use, consuming `reqwest`'s `bytes_stream()`
//! incrementally and splitting on newlines as they arrive rather than
//! reading the whole body first. Rows are batched (`JSON_BATCH_SIZE` per
//! `StreamElement::Rows`) to match the source project's batching rather
//! than yielding one element per row. `RowLimits::EMERGENCY_MAX` is a
//! hard ceiling here too, even though this is the "give me everything"
//! API.
//!
//! `JSONEachRow` has no type information and, for a zero-row result, not
//! even column names — see `crate::query::fetch_probe_header`'s doc
//! comment for the header-recovery strategy this reuses (an extra small
//! `LIMIT 0` round trip against the same query, in exchange for a real
//! typed `StreamHeader`).
//!
//! Unlike `execute_user_query` (`crate::query::execute_user_query_capped`,
//! which is reachable with arbitrary DDL/DML through the generic MCP
//! `execute_query` tool and so has to classify the statement before
//! deciding how to run it — see that module's doc comment), nothing in
//! this workspace calls `stream_rows` with non-`SELECT` SQL: the trait's
//! own contract for it ("every stream starts with exactly one `Header`,
//! followed by zero or more `Rows` batches") is a row-returning-query
//! contract in the first place, with no equivalent of "rows affected" for
//! a statement with no result set. `fetch_probe_header`'s row-returning
//! assumption therefore still applies here unconditionally; a caller that
//! passes DDL/DML gets a clear ClickHouse syntax error from the probe
//! rather than the earlier method's classify-and-branch treatment. If a
//! caller ever needs to `stream_rows` non-`SELECT` SQL, this file is
//! where `crate::statement::is_row_returning_statement` would need to be
//! consulted the same way.

use async_stream::try_stream;
use db_headless_core::{RowLimits, RowStream, StreamElement};
use futures_util::StreamExt;

use crate::driver::{not_connected_error, ClickHouseDriver};
use crate::error::map_reqwest_error;
use crate::jsonl;
use crate::query::fetch_probe_header;

const JSON_BATCH_SIZE: usize = 5_000;

pub fn stream_rows<'a>(driver: &'a ClickHouseDriver, sql: &'a str) -> RowStream<'a> {
    Box::pin(try_stream! {
        let guard = driver.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;

        let header = fetch_probe_header(driver, connected, sql, &[]).await?;
        yield StreamElement::Header(header.clone());

        let (response, _active_guard) = driver
            .send_request(connected, sql.to_string(), &[("default_format".to_string(), "JSONEachRow".to_string())])
            .await?;

        let mut byte_stream = response.bytes_stream();
        let mut buffer = String::new();
        let mut batch: Vec<Vec<db_headless_core::CellValue>> = Vec::with_capacity(JSON_BATCH_SIZE);
        let mut total = 0usize;

        'outer: while let Some(chunk) = byte_stream.next().await {
            let bytes = chunk.map_err(map_reqwest_error)?;
            buffer.push_str(&String::from_utf8_lossy(&bytes));

            while let Some(newline_at) = buffer.find('\n') {
                let line = buffer[..newline_at].to_string();
                buffer.drain(..=newline_at);
                if line.trim().is_empty() {
                    continue;
                }

                let row = jsonl::parse_row(&line, &header.columns)?;
                batch.push(row);
                total += 1;

                if batch.len() >= JSON_BATCH_SIZE {
                    yield StreamElement::Rows(std::mem::take(&mut batch));
                }
                if total >= RowLimits::EMERGENCY_MAX {
                    break 'outer;
                }
            }
        }

        if total < RowLimits::EMERGENCY_MAX && !buffer.trim().is_empty() {
            let row = jsonl::parse_row(&buffer, &header.columns)?;
            batch.push(row);
        }

        if !batch.is_empty() {
            yield StreamElement::Rows(batch);
        }
    })
}
