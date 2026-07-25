use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use db_headless_core::{CellValue, QueryResult, QueryTimeouts};
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::execute_query::{to_cell_value, CellValueArg};
use crate::tools::support::{map_manager_error, parse_arguments, parse_connection_id};

#[derive(Debug, Deserialize)]
struct ExportQueryCsvArgs {
    connection_id: String,
    query: String,
    #[serde(default)]
    parameters: Option<Vec<CellValueArg>>,
    #[serde(default)]
    row_cap: Option<usize>,
    /// Emit a header row of column names before the data rows. Defaults to
    /// true; set false to dump only the records (e.g. when appending to an
    /// existing file that already carries the header).
    #[serde(default = "default_true")]
    include_header: bool,
}

fn default_true() -> bool {
    true
}

/// Dumps a query's result set as CSV text instead of the structured JSON
/// [`ExecuteQueryTool`](crate::tools::ExecuteQueryTool) returns.
///
/// This is the same run-a-query path — same driver call, same row cap, same
/// client-side backstop timeout and cancellation — differing only in how the
/// rows are rendered on the way out: RFC 4180 CSV rather than a JSON array of
/// `CellValue`s. It exists because a CSV blob is what a caller actually wants
/// when the goal is "export these records" — pasteable into a spreadsheet,
/// appendable to a file — where the JSON shape would have to be re-flattened
/// first.
///
/// The row cap (guardrail #7) still applies, so a large table is clipped to
/// `RowLimits::EMERGENCY_MAX` rows; `truncated` in the result says whether
/// that happened, so a caller never mistakes a clipped dump for a complete
/// one.
pub struct ExportQueryCsvTool {
    manager: Arc<ConnectionManager>,
}

impl ExportQueryCsvTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ExportQueryCsvTool {
    fn name(&self) -> &str {
        "export_query_csv"
    }

    fn description(&self) -> &str {
        "Runs a SQL query against a live connection and returns the result set as CSV text (RFC 4180), for dumping or exporting records."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "connection_id": { "type": "string" },
                "query": { "type": "string" },
                "parameters": {
                    "type": "array",
                    "items": { "type": ["string", "null"] },
                    "description": "Bound parameters in order. null binds SQL NULL; a string binds a text value. Binary parameters are not supported through this tool."
                },
                "row_cap": { "type": "integer", "minimum": 0 },
                "include_header": {
                    "type": "boolean",
                    "description": "Emit a header row of column names before the data rows. Defaults to true."
                }
            },
            "required": ["connection_id", "query"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: ExportQueryCsvArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;
        let driver = self.manager.get(connection_id).map_err(map_manager_error)?;

        let parameters: Option<Vec<CellValue>> = args
            .parameters
            .map(|params| params.into_iter().map(to_cell_value).collect());

        let query_future =
            driver.execute_user_query(&args.query, args.row_cap, parameters.as_deref());
        let backstop = Duration::from_secs(QueryTimeouts::CLIENT_BACKSTOP_SECS);

        let result = match tokio::time::timeout(backstop, query_future).await {
            Ok(query_result) => {
                query_result.map_err(|err| McpToolError::Failed(err.to_string()))?
            }
            Err(_elapsed) => {
                if let Err(err) = driver.cancel_query() {
                    tracing::warn!(
                        error = %err,
                        "failed to cancel a query that exceeded the client-side backstop timeout"
                    );
                }
                return Err(McpToolError::Failed(format!(
                    "query exceeded the {}s client-side timeout and was cancelled; this connection's link may be unstable, or the query itself may be missing an index",
                    QueryTimeouts::CLIENT_BACKSTOP_SECS
                )));
            }
        };

        let csv = render_csv(&result, args.include_header);

        Ok(json!({
            "csv": csv,
            "row_count": result.rows.len(),
            "columns": result.columns,
            "truncated": result.is_truncated,
        }))
    }
}

/// Renders a [`QueryResult`] as RFC 4180 CSV text.
///
/// - Fields are separated by `,` and records by `\r\n` (the RFC's CRLF).
/// - A field is quoted (and any `"` inside it doubled) only when it contains
///   a `"`, `,`, `\r`, or `\n`, so ordinary values stay unquoted and readable.
/// - `CellValue::Null` renders as an empty field — the conventional CSV
///   representation of a missing value.
/// - `CellValue::Bytes` renders as `\x`-prefixed lowercase hex (PostgreSQL's
///   own `bytea` hex-output convention), since CSV has no binary literal.
fn render_csv(result: &QueryResult, include_header: bool) -> String {
    let mut out = String::new();

    if include_header {
        write_record(&mut out, result.columns.iter().map(|c| escape_field(c)));
    }

    for row in &result.rows {
        write_record(&mut out, row.iter().map(render_cell));
    }

    out
}

fn write_record(out: &mut String, mut fields: impl Iterator<Item = String>) {
    if let Some(first) = fields.next() {
        out.push_str(&first);
        for field in fields {
            out.push(',');
            out.push_str(&field);
        }
    }
    out.push_str("\r\n");
}

fn render_cell(cell: &CellValue) -> String {
    match cell {
        CellValue::Null => String::new(),
        CellValue::Text(text) => escape_field(text),
        CellValue::Bytes(bytes) => {
            let mut hex = String::with_capacity(2 + bytes.len() * 2);
            hex.push_str("\\x");
            for byte in bytes {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
            }
            escape_field(&hex)
        }
    }
}

fn escape_field(field: &str) -> String {
    let needs_quoting = field
        .chars()
        .any(|c| c == '"' || c == ',' || c == '\n' || c == '\r');

    if !needs_quoting {
        return field.to_string();
    }

    let mut quoted = String::with_capacity(field.len() + 2);
    quoted.push('"');
    for c in field.chars() {
        if c == '"' {
            quoted.push('"');
        }
        quoted.push(c);
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    fn manager_with_mock(config: MockDriverConfig) -> Arc<ConnectionManager> {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(config)));
        Arc::new(manager)
    }

    fn result_with(columns: &[&str], rows: Vec<Vec<CellValue>>, truncated: bool) -> QueryResult {
        let mut result = QueryResult::empty();
        result.columns = columns.iter().map(|c| c.to_string()).collect();
        result.rows = rows;
        result.is_truncated = truncated;
        result
    }

    #[test]
    fn renders_header_and_rows() {
        let result = result_with(
            &["id", "name"],
            vec![
                vec![CellValue::Text("1".into()), CellValue::Text("Alice".into())],
                vec![CellValue::Text("2".into()), CellValue::Text("Bob".into())],
            ],
            false,
        );
        assert_eq!(render_csv(&result, true), "id,name\r\n1,Alice\r\n2,Bob\r\n");
    }

    #[test]
    fn omitting_header_dumps_only_records() {
        let result = result_with(&["id"], vec![vec![CellValue::Text("1".into())]], false);
        assert_eq!(render_csv(&result, false), "1\r\n");
    }

    #[test]
    fn quotes_fields_with_commas_quotes_and_newlines() {
        let result = result_with(
            &["a", "b", "c"],
            vec![vec![
                CellValue::Text("x,y".into()),
                CellValue::Text("she said \"hi\"".into()),
                CellValue::Text("line1\nline2".into()),
            ]],
            false,
        );
        assert_eq!(
            render_csv(&result, false),
            "\"x,y\",\"she said \"\"hi\"\"\",\"line1\nline2\"\r\n"
        );
    }

    #[test]
    fn null_is_an_empty_field_distinct_from_empty_string() {
        let result = result_with(
            &["a", "b"],
            vec![vec![CellValue::Null, CellValue::Text(String::new())]],
            false,
        );
        // Both render as empty text, but the point is Null does not crash and
        // sits in its own column position.
        assert_eq!(render_csv(&result, false), ",\r\n");
    }

    #[test]
    fn bytes_render_as_prefixed_hex() {
        let result = result_with(
            &["blob"],
            vec![vec![CellValue::Bytes(vec![0x00, 0x0f, 0xff])]],
            false,
        );
        assert_eq!(render_csv(&result, false), "\\x000fff\r\n");
    }

    #[test]
    fn empty_result_with_header_is_just_the_header() {
        let result = result_with(&["id", "name"], vec![], false);
        assert_eq!(render_csv(&result, true), "id,name\r\n");
    }

    #[tokio::test]
    async fn exports_a_query_result_end_to_end() {
        let canned = result_with(
            &["id", "name"],
            vec![vec![
                CellValue::Text("1".into()),
                CellValue::Text("A,B".into()),
            ]],
            true,
        );
        let manager = manager_with_mock(MockDriverConfig::with_query_result(canned));
        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = ExportQueryCsvTool::new(manager);
        let result = tool
            .call(Some(json!({
                "connection_id": id.to_string(),
                "query": "SELECT * FROM t"
            })))
            .await
            .expect("export_query_csv succeeds");

        assert_eq!(result["csv"], json!("id,name\r\n1,\"A,B\"\r\n"));
        assert_eq!(result["row_count"], json!(1));
        assert_eq!(result["columns"], json!(["id", "name"]));
        assert_eq!(result["truncated"], json!(true));
    }

    #[tokio::test]
    async fn unknown_connection_id_is_failed_not_invalid_arguments() {
        let manager = manager_with_mock(MockDriverConfig::default());
        let tool = ExportQueryCsvTool::new(manager);

        let err = tool
            .call(Some(json!({
                "connection_id": uuid::Uuid::new_v4().to_string(),
                "query": "SELECT 1"
            })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::Failed(_)));
    }
}
