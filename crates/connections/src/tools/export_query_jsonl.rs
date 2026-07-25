use std::fmt::Write as _;
use std::sync::Arc;

use async_trait::async_trait;
use db_headless_core::{CellValue, QueryResult};
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::execute_query::{to_cell_value, CellValueArg};
use crate::tools::support::{parse_arguments, parse_connection_id, run_user_query};

#[derive(Debug, Deserialize)]
struct ExportQueryJsonlArgs {
    connection_id: String,
    query: String,
    #[serde(default)]
    parameters: Option<Vec<CellValueArg>>,
    #[serde(default)]
    row_cap: Option<usize>,
}

/// Dumps a query's result set as JSON Lines (one JSON object per row,
/// newline-separated) — a file-ready export format that preserves types.
///
/// Every cell in this server is carried as text with its real type in
/// `column_type_names`, so a flat text format (like CSV) would render a
/// number, a boolean, and the text `"1"` indistinguishably and blur SQL
/// `NULL` into an empty field. JSON Lines keeps those apart: each value is
/// rendered as a native JSON number, boolean, string, or `null`, keyed by
/// column name, using that type name to decide which. The envelope also
/// echoes `columns` and `column_type_names` so the full schema travels with
/// the dump.
///
/// Same run-a-query path as `execute_query` (row cap, read-only
/// enforcement, backstop timeout with cancellation); only the rendering
/// differs. `truncated` reports whether the row cap (guardrail #7) clipped
/// the dump, so a partial export is never mistaken for a complete one.
pub struct ExportQueryJsonlTool {
    manager: Arc<ConnectionManager>,
}

impl ExportQueryJsonlTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ExportQueryJsonlTool {
    fn name(&self) -> &str {
        "export_query_jsonl"
    }

    fn description(&self) -> &str {
        "Runs a SQL query against a live connection and returns the result set as JSON Lines (one JSON object per row), preserving numeric/boolean/null types that CSV would flatten to strings."
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
                "row_cap": { "type": "integer", "minimum": 0 }
            },
            "required": ["connection_id", "query"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: ExportQueryJsonlArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;

        let parameters: Option<Vec<CellValue>> = args
            .parameters
            .map(|params| params.into_iter().map(to_cell_value).collect());

        let result = run_user_query(
            &self.manager,
            connection_id,
            &args.query,
            parameters.as_deref(),
            args.row_cap,
        )
        .await?;

        let jsonl = render_jsonl(&result);

        Ok(json!({
            "jsonl": jsonl,
            "row_count": result.rows.len(),
            "columns": result.columns,
            "column_type_names": result.column_type_names,
            "truncated": result.is_truncated,
        }))
    }
}

/// Renders a [`QueryResult`] as JSON Lines: one JSON object per row keyed by
/// column name, records separated by `\n`.
///
/// Objects are built in column order by hand rather than through a
/// `serde_json::Map`, which would reorder keys alphabetically (the crate's
/// default map is a `BTreeMap`) — a dump should read in the column order the
/// query declared.
fn render_jsonl(result: &QueryResult) -> String {
    let mut out = String::new();

    for row in &result.rows {
        out.push('{');
        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let column = result.columns.get(i).map(String::as_str).unwrap_or("");
            let type_name = result
                .column_type_names
                .get(i)
                .map(String::as_str)
                .unwrap_or("");

            // `to_string` on a Value::String escapes the key correctly; a
            // column name with a quote or backslash would otherwise produce
            // invalid JSON.
            let key = Value::String(column.to_string());
            out.push_str(&key.to_string());
            out.push(':');
            out.push_str(&cell_to_json(cell, type_name).to_string());
        }
        out.push('}');
        out.push('\n');
    }

    out
}

/// Maps a single cell to a native JSON value, using the column's declared
/// type name to decide numeric/boolean typing. All cells arrive as text
/// (see [`CellValue`]), so this is where the type name earns its keep.
fn cell_to_json(cell: &CellValue, type_name: &str) -> Value {
    match cell {
        CellValue::Null => Value::Null,
        CellValue::Bytes(bytes) => Value::String(hex_encode(bytes)),
        CellValue::Text(text) => coerce_text(text, type_name),
    }
}

fn coerce_text(text: &str, type_name: &str) -> Value {
    match classify(type_name) {
        TypeClass::Boolean => parse_bool(text).map_or_else(|| string(text), Value::Bool),
        TypeClass::Integer => text
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| text.parse::<u64>().map(Value::from))
            .unwrap_or_else(|_| string(text)),
        TypeClass::Float => match text.parse::<f64>() {
            Ok(f) => serde_json::Number::from_f64(f).map_or_else(|| string(text), Value::Number),
            Err(_) => string(text),
        },
        TypeClass::String => string(text),
    }
}

fn string(text: &str) -> Value {
    Value::String(text.to_string())
}

enum TypeClass {
    Boolean,
    Integer,
    Float,
    String,
}

/// Classifies a driver-reported column type name into how its text value
/// should render in JSON.
///
/// Deliberately conservative: `numeric`/`decimal` stay `String` because a
/// JSON `f64` would silently lose their precision, and anything unrecognized
/// stays `String` rather than being guessed at. Even when a type is
/// classified as numeric or boolean, [`coerce_text`] only emits a typed
/// value if the text actually parses — so a misclassification degrades to a
/// string, never to wrong data.
fn classify(type_name: &str) -> TypeClass {
    let lowered = unwrap_wrappers(&type_name.trim().to_ascii_lowercase());
    let t = lowered.as_str();

    if t == "bool" || t == "boolean" {
        return TypeClass::Boolean;
    }
    // Arbitrary-precision decimals must not become a lossy JSON float.
    if t.contains("decimal") || t.contains("numeric") {
        return TypeClass::String;
    }
    if is_float(t) {
        return TypeClass::Float;
    }
    if is_integer(t) {
        return TypeClass::Integer;
    }
    TypeClass::String
}

/// Peels ClickHouse's `Nullable(...)` / `LowCardinality(...)` wrappers off a
/// type name so the inner type drives classification. Postgres type names
/// have no such wrappers, so this is a no-op for them.
fn unwrap_wrappers(type_name: &str) -> String {
    let mut t = type_name.to_string();
    loop {
        let unwrapped = t
            .strip_prefix("nullable(")
            .or_else(|| t.strip_prefix("lowcardinality("))
            .and_then(|inner| inner.strip_suffix(')'));
        match unwrapped {
            Some(inner) => t = inner.trim().to_string(),
            None => return t,
        }
    }
}

fn is_float(t: &str) -> bool {
    matches!(t, "real" | "double" | "double precision") || t.starts_with("float")
}

fn is_integer(t: &str) -> bool {
    if matches!(
        t,
        "int2"
            | "int4"
            | "int8"
            | "smallint"
            | "integer"
            | "int"
            | "bigint"
            | "serial"
            | "serial2"
            | "serial4"
            | "serial8"
            | "smallserial"
            | "bigserial"
            | "oid"
    ) {
        return true;
    }
    // ClickHouse fixed-width forms: Int8/16/32/64/128/256, UInt8..UInt256.
    // Guarded by an all-digits suffix so `interval` and `int4range` do not
    // slip through the `int` prefix.
    for prefix in ["int", "uint"] {
        if let Some(rest) = t.strip_prefix(prefix) {
            if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()) {
                return true;
            }
        }
    }
    false
}

fn parse_bool(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "yes" | "y" => Some(true),
        "f" | "false" | "0" | "no" | "n" => Some(false),
        _ => None,
    }
}

/// `\x`-prefixed lowercase hex, matching PostgreSQL's own `bytea` output —
/// JSON has no binary literal.
fn hex_encode(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(2 + bytes.len() * 2);
    hex.push_str("\\x");
    for byte in bytes {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
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

    fn result_with(
        columns: &[&str],
        types: &[&str],
        rows: Vec<Vec<CellValue>>,
        truncated: bool,
    ) -> QueryResult {
        let mut result = QueryResult::empty();
        result.columns = columns.iter().map(|c| c.to_string()).collect();
        result.column_type_names = types.iter().map(|t| t.to_string()).collect();
        result.rows = rows;
        result.is_truncated = truncated;
        result
    }

    #[test]
    fn preserves_numeric_boolean_and_null_types() {
        let result = result_with(
            &["id", "active", "note"],
            &["int4", "bool", "text"],
            vec![vec![
                CellValue::Text("1".into()),
                CellValue::Text("t".into()),
                CellValue::Null,
            ]],
            false,
        );
        assert_eq!(
            render_jsonl(&result),
            "{\"id\":1,\"active\":true,\"note\":null}\n"
        );
    }

    #[test]
    fn quoted_string_one_is_not_the_number_one() {
        // The whole point over CSV: text "1" stays a string, int 1 becomes a
        // number.
        let result = result_with(
            &["as_text", "as_int"],
            &["text", "int8"],
            vec![vec![
                CellValue::Text("1".into()),
                CellValue::Text("1".into()),
            ]],
            false,
        );
        assert_eq!(render_jsonl(&result), "{\"as_text\":\"1\",\"as_int\":1}\n");
    }

    #[test]
    fn floats_become_json_numbers() {
        let result = result_with(
            &["ratio"],
            &["float8"],
            vec![vec![CellValue::Text("12.5".into())]],
            false,
        );
        assert_eq!(render_jsonl(&result), "{\"ratio\":12.5}\n");
    }

    #[test]
    fn decimals_stay_strings_to_keep_precision() {
        let result = result_with(
            &["amount"],
            &["numeric"],
            vec![vec![CellValue::Text("9999999999999999999.99".into())]],
            false,
        );
        assert_eq!(
            render_jsonl(&result),
            "{\"amount\":\"9999999999999999999.99\"}\n"
        );
    }

    #[test]
    fn unparseable_value_for_numeric_type_falls_back_to_string() {
        // Classification is guarded by an actual parse, so a bogus value
        // never yields corrupt JSON.
        let result = result_with(
            &["n"],
            &["int4"],
            vec![vec![CellValue::Text("not-a-number".into())]],
            false,
        );
        assert_eq!(render_jsonl(&result), "{\"n\":\"not-a-number\"}\n");
    }

    #[test]
    fn interval_is_not_mistaken_for_an_integer() {
        let result = result_with(
            &["span"],
            &["interval"],
            vec![vec![CellValue::Text("1 day".into())]],
            false,
        );
        assert_eq!(render_jsonl(&result), "{\"span\":\"1 day\"}\n");
    }

    #[test]
    fn clickhouse_nullable_wrapper_is_unwrapped() {
        let result = result_with(
            &["n"],
            &["Nullable(Int32)"],
            vec![vec![CellValue::Text("42".into())]],
            false,
        );
        assert_eq!(render_jsonl(&result), "{\"n\":42}\n");
    }

    #[test]
    fn bytes_render_as_prefixed_hex_string() {
        let result = result_with(
            &["blob"],
            &["bytea"],
            vec![vec![CellValue::Bytes(vec![0x00, 0x0f, 0xff])]],
            false,
        );
        assert_eq!(render_jsonl(&result), "{\"blob\":\"\\\\x000fff\"}\n");
    }

    #[test]
    fn keys_and_string_values_are_escaped() {
        let result = result_with(
            &["od\"d"],
            &["text"],
            vec![vec![CellValue::Text("a\"b\nc".into())]],
            false,
        );
        assert_eq!(render_jsonl(&result), "{\"od\\\"d\":\"a\\\"b\\nc\"}\n");
    }

    #[test]
    fn empty_result_is_empty_string() {
        let result = result_with(&["id"], &["int4"], vec![], false);
        assert_eq!(render_jsonl(&result), "");
    }

    #[tokio::test]
    async fn exports_a_query_result_end_to_end() {
        let canned = result_with(
            &["id", "active"],
            &["int4", "bool"],
            vec![vec![
                CellValue::Text("7".into()),
                CellValue::Text("f".into()),
            ]],
            true,
        );
        let manager = manager_with_mock(MockDriverConfig::with_query_result(canned));
        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = ExportQueryJsonlTool::new(manager);
        let result = tool
            .call(Some(json!({
                "connection_id": id.to_string(),
                "query": "SELECT * FROM t"
            })))
            .await
            .expect("export_query_jsonl succeeds");

        assert_eq!(result["jsonl"], json!("{\"id\":7,\"active\":false}\n"));
        assert_eq!(result["row_count"], json!(1));
        assert_eq!(result["columns"], json!(["id", "active"]));
        assert_eq!(result["column_type_names"], json!(["int4", "bool"]));
        assert_eq!(result["truncated"], json!(true));
    }

    #[tokio::test]
    async fn unknown_connection_id_is_failed_not_invalid_arguments() {
        let manager = manager_with_mock(MockDriverConfig::default());
        let tool = ExportQueryJsonlTool::new(manager);

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
