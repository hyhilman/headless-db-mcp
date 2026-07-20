//! Schema introspection against ClickHouse's own `system.*` tables, plus
//! `create_database`/`drop_database`/`switch_database`'s existence check.
//!
//! ClickHouse exposes richer, more authoritative metadata than Postgres's
//! `information_schema` approximation needed: `system.tables.
//! create_table_query` is the exact `CREATE TABLE`/`CREATE VIEW`
//! statement ClickHouse itself would replay, so `fetch_table_ddl`/
//! `fetch_view_definition` below return it verbatim rather than
//! reconstructing anything by hand the way `driver-postgres` has to.
//!
//! Every place a database/table name is used as a *value* (`WHERE
//! database = ?`) goes through [`crate::query::run_tsv_parameterized`],
//! binding it as an out-of-band HTTP parameter — not spliced into SQL
//! text. Only `create_database`/`drop_database`, which must splice a name
//! into an identifier *position* ClickHouse gives no way to bind, go
//! through [`crate::ident::quote_ident`].

use db_headless_core::{
    CellValue, ColumnInfo, CreateDatabaseRequest, DatabaseMetadata, DriverError, DriverErrorKind,
    DriverResult, IdentityKind, IndexInfo, TableInfo, TableKind, TableMetadata, TriggerInfo,
};

use crate::driver::ClickHouseDriver;
use crate::ident::quote_ident;
use crate::query::{run_tsv, run_tsv_parameterized, TsvOutcome};
use crate::request::ConnectedClient;

fn missing_column(column: &str) -> DriverError {
    DriverError::new(
        DriverErrorKind::Internal,
        format!("a schema-introspection query result was missing column \"{column}\""),
    )
}

fn unexpected_cell_kind(column: &str) -> DriverError {
    DriverError::new(
        DriverErrorKind::Internal,
        format!("column \"{column}\" in a schema-introspection query result had an unexpected value kind"),
    )
}

fn cell_text(row: &[CellValue], idx: usize, column: &str) -> DriverResult<String> {
    match row.get(idx) {
        Some(CellValue::Text(text)) => Ok(text.clone()),
        Some(CellValue::Null) => Ok(String::new()),
        Some(CellValue::Bytes(_)) => Err(unexpected_cell_kind(column)),
        None => Err(missing_column(column)),
    }
}

fn cell_opt_text(row: &[CellValue], idx: usize, column: &str) -> DriverResult<Option<String>> {
    match row.get(idx) {
        Some(CellValue::Text(text)) if text.is_empty() => Ok(None),
        Some(CellValue::Text(text)) => Ok(Some(text.clone())),
        Some(CellValue::Null) => Ok(None),
        Some(CellValue::Bytes(_)) => Err(unexpected_cell_kind(column)),
        None => Err(missing_column(column)),
    }
}

fn cell_bool(row: &[CellValue], idx: usize, column: &str) -> DriverResult<bool> {
    Ok(cell_text(row, idx, column)? == "1")
}

fn cell_opt_i64(row: &[CellValue], idx: usize, column: &str) -> DriverResult<Option<i64>> {
    match cell_opt_text(row, idx, column)? {
        None => Ok(None),
        Some(text) => text
            .parse::<i64>()
            .map(Some)
            .map_err(|_| unexpected_cell_kind(column)),
    }
}

fn cell_opt_u64(row: &[CellValue], idx: usize, column: &str) -> DriverResult<Option<u64>> {
    match cell_opt_text(row, idx, column)? {
        None => Ok(None),
        Some(text) => text
            .parse::<u64>()
            .map(Some)
            .map_err(|_| unexpected_cell_kind(column)),
    }
}

/// Splits a comma-separated expression list on only its top-level commas,
/// so a compound sort-key expression like `cityHash64(a, b), c` splits
/// into `["cityHash64(a, b)", "c"]` rather than incorrectly breaking
/// inside the function call's own argument list.
fn split_top_level_commas(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();

    for ch in input.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }

    parts
}

fn unwrap_nullable(type_name: &str) -> &str {
    type_name
        .strip_prefix("Nullable(")
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or(type_name)
}

/// Extracts the quoted labels out of an `Enum8('a' = 1, 'b' = 2)` /
/// `Enum16(...)` type name (unwrapping a `Nullable(...)` layer first).
/// Returns `None` for any other type. Does not handle an escaped quote
/// inside a label — ClickHouse enum labels containing a literal `'` are
/// rare enough that this is a documented gap rather than a full SQL
/// literal parser.
fn parse_enum_labels(type_name: &str) -> Option<Vec<String>> {
    let inner = unwrap_nullable(type_name);
    let body = inner
        .strip_prefix("Enum8(")
        .or_else(|| inner.strip_prefix("Enum16("))?
        .strip_suffix(')')?;

    let mut labels = Vec::new();
    let mut chars = body.chars();
    while let Some(ch) = chars.next() {
        if ch != '\'' {
            continue;
        }
        let mut label = String::new();
        for next in chars.by_ref() {
            if next == '\'' {
                break;
            }
            label.push(next);
        }
        labels.push(label);
    }

    if labels.is_empty() {
        None
    } else {
        Some(labels)
    }
}

fn table_kind_from_engine(engine: &str) -> TableKind {
    match engine {
        "View" => TableKind::View,
        "MaterializedView" => TableKind::MaterializedView,
        _ => TableKind::Table,
    }
}

pub async fn fetch_databases(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
) -> DriverResult<Vec<String>> {
    let outcome = run_tsv(
        driver,
        connected,
        "SELECT name FROM system.databases ORDER BY name",
    )
    .await?;
    outcome
        .rows
        .iter()
        .map(|row| cell_text(row, 0, "name"))
        .collect()
}

pub async fn fetch_tables(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    database: &str,
) -> DriverResult<Vec<TableInfo>> {
    let sql = "SELECT name, engine, comment, total_rows FROM system.tables \
               WHERE database = ? ORDER BY name";
    let outcome = run_tsv_parameterized(
        driver,
        connected,
        sql,
        &[CellValue::Text(database.to_string())],
    )
    .await?;

    outcome
        .rows
        .iter()
        .map(|row| {
            let name = cell_text(row, 0, "name")?;
            let engine = cell_text(row, 1, "engine")?;
            let comment = cell_opt_text(row, 2, "comment")?;
            let row_count_estimate = cell_opt_i64(row, 3, "total_rows")?;
            Ok(TableInfo {
                name,
                schema: Some(database.to_string()),
                kind: table_kind_from_engine(&engine),
                comment,
                row_count_estimate,
            })
        })
        .collect()
}

pub async fn fetch_columns(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    table: &str,
    database: &str,
) -> DriverResult<Vec<ColumnInfo>> {
    let sql = "SELECT name, type, default_kind, default_expression, comment, is_in_primary_key \
               FROM system.columns WHERE database = ? AND table = ? ORDER BY position";
    let outcome = run_tsv_parameterized(
        driver,
        connected,
        sql,
        &[
            CellValue::Text(database.to_string()),
            CellValue::Text(table.to_string()),
        ],
    )
    .await?;

    outcome
        .rows
        .iter()
        .map(|row| {
            let name = cell_text(row, 0, "name")?;
            let data_type = cell_text(row, 1, "type")?;
            let default_kind = cell_text(row, 2, "default_kind")?;
            let default_expression = cell_opt_text(row, 3, "default_expression")?;
            let comment = cell_opt_text(row, 4, "comment")?;
            let is_primary_key = cell_bool(row, 5, "is_in_primary_key")?;

            let is_nullable = data_type.starts_with("Nullable(");
            let is_generated = default_kind == "MATERIALIZED" || default_kind == "ALIAS";
            let allowed_values = parse_enum_labels(&data_type);

            Ok(ColumnInfo {
                name,
                data_type,
                is_nullable,
                is_primary_key,
                default_value: default_expression,
                extra: None,
                // ClickHouse has no per-column charset/collation
                // metadata: string comparison collation is an ORDER BY
                // clause detail, not a stored column property.
                charset: None,
                collation: None,
                comment,
                // ClickHouse has no identity-column concept.
                identity_kind: None::<IdentityKind>,
                is_generated,
                allowed_values,
            })
        })
        .collect()
}

/// Reports two kinds of ClickHouse index concept as `IndexInfo` entries:
/// the table's implicit primary key / `ORDER BY` key (`system.tables.
/// primary_key`, reported here as a single synthetic `is_primary = true`
/// entry — MergeTree tables have no separate `PRIMARY KEY` constraint the
/// way Postgres does, the sort key *is* the primary key), and any real
/// data-skipping indexes (`system.data_skipping_indices`). This is a
/// deliberate interpretation of "index" for an engine family with no
/// traditional secondary indexes, not a literal 1:1 mapping to SQL
/// `CREATE INDEX` statements.
pub async fn fetch_indexes(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    table: &str,
    database: &str,
) -> DriverResult<Vec<IndexInfo>> {
    let mut indexes = Vec::new();

    let primary_key_outcome = run_tsv_parameterized(
        driver,
        connected,
        "SELECT primary_key FROM system.tables WHERE database = ? AND name = ?",
        &[
            CellValue::Text(database.to_string()),
            CellValue::Text(table.to_string()),
        ],
    )
    .await?;
    if let Some(row) = primary_key_outcome.rows.first() {
        let primary_key = cell_text(row, 0, "primary_key")?;
        if !primary_key.trim().is_empty() {
            indexes.push(IndexInfo {
                name: "primary_key".to_string(),
                columns: split_top_level_commas(&primary_key),
                is_unique: false,
                is_primary: true,
                method: Some("ORDER BY".to_string()),
            });
        }
    }

    let skip_index_outcome = run_tsv_parameterized(
        driver,
        connected,
        "SELECT name, type, expr FROM system.data_skipping_indices \
         WHERE database = ? AND table = ? ORDER BY name",
        &[
            CellValue::Text(database.to_string()),
            CellValue::Text(table.to_string()),
        ],
    )
    .await?;
    for row in &skip_index_outcome.rows {
        let name = cell_text(row, 0, "name")?;
        let index_type = cell_text(row, 1, "type")?;
        let expr = cell_text(row, 2, "expr")?;
        indexes.push(IndexInfo {
            name,
            columns: vec![expr],
            is_unique: false,
            is_primary: false,
            method: Some(index_type),
        });
    }

    Ok(indexes)
}

/// Returns the exact, complete `CREATE TABLE`/`CREATE VIEW` statement
/// ClickHouse itself stores (`system.tables.create_table_query`), used
/// for both `fetch_table_ddl` and `fetch_view_definition` — ClickHouse
/// makes no table/view distinction here, unlike Postgres's
/// `pg_get_viewdef`, which only accepts a view.
pub async fn fetch_create_query(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    name: &str,
    database: &str,
) -> DriverResult<String> {
    let outcome = run_tsv_parameterized(
        driver,
        connected,
        "SELECT create_table_query FROM system.tables WHERE database = ? AND name = ?",
        &[
            CellValue::Text(database.to_string()),
            CellValue::Text(name.to_string()),
        ],
    )
    .await?;

    let row = outcome.rows.first().ok_or_else(|| {
        DriverError::new(
            DriverErrorKind::Query,
            format!("table or view \"{database}\".\"{name}\" was not found"),
        )
    })?;
    cell_text(row, 0, "create_table_query")
}

pub async fn fetch_table_metadata(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    table: &str,
    database: &str,
) -> DriverResult<TableMetadata> {
    let outcome = run_tsv_parameterized(
        driver,
        connected,
        "SELECT name, engine, comment, total_rows FROM system.tables \
         WHERE database = ? AND name = ?",
        &[
            CellValue::Text(database.to_string()),
            CellValue::Text(table.to_string()),
        ],
    )
    .await?;
    let row = outcome.rows.first().ok_or_else(|| {
        DriverError::new(
            DriverErrorKind::Query,
            format!("table \"{database}\".\"{table}\" was not found"),
        )
    })?;
    let info = TableInfo {
        name: cell_text(row, 0, "name")?,
        schema: Some(database.to_string()),
        kind: table_kind_from_engine(&cell_text(row, 1, "engine")?),
        comment: cell_opt_text(row, 2, "comment")?,
        row_count_estimate: cell_opt_i64(row, 3, "total_rows")?,
    };

    let columns = fetch_columns(driver, connected, table, database).await?;
    let indexes = fetch_indexes(driver, connected, table, database).await?;

    Ok(TableMetadata {
        info,
        columns,
        indexes,
        // ClickHouse has no foreign key constraints.
        foreign_keys: Vec::new(),
        // ClickHouse has no trigger concept.
        triggers: Vec::<TriggerInfo>::new(),
    })
}

pub async fn fetch_database_metadata(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    database: &str,
) -> DriverResult<DatabaseMetadata> {
    let size_outcome = run_tsv_parameterized(
        driver,
        connected,
        "SELECT sum(bytes_on_disk) FROM system.parts WHERE database = ? AND active",
        &[CellValue::Text(database.to_string())],
    )
    .await;

    let size_bytes = match size_outcome {
        Ok(outcome) => match outcome.rows.first() {
            Some(row) => cell_opt_u64(row, 0, "bytes_on_disk").unwrap_or(None),
            None => None,
        },
        Err(err) => {
            tracing::debug!(error = %err, database, "system.parts size lookup failed, omitting size");
            None
        }
    };

    Ok(DatabaseMetadata {
        name: database.to_string(),
        // ClickHouse has no schema layer distinct from "database" — see
        // the module doc comment on `DatabaseDriver::supports_schemas`
        // for why this driver relies on the trait's own empty default
        // rather than inventing one.
        schemas: Vec::new(),
        size_bytes,
    })
}

pub async fn validate_database_exists(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    database: &str,
) -> DriverResult<()> {
    let outcome = run_tsv_parameterized(
        driver,
        connected,
        "SELECT 1 FROM system.databases WHERE name = ?",
        &[CellValue::Text(database.to_string())],
    )
    .await?;

    if outcome.rows.is_empty() {
        return Err(DriverError::new(
            DriverErrorKind::Query,
            format!("database \"{database}\" does not exist"),
        ));
    }
    Ok(())
}

/// ClickHouse's `CREATE DATABASE` has no owner or encoding concept the
/// way Postgres's does, so `request.owner`/`request.encoding`/
/// `request.additional_fields` are intentionally not applied here — only
/// `request.name` is meaningful.
pub async fn create_database(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    request: &CreateDatabaseRequest,
) -> DriverResult<()> {
    let sql = format!("CREATE DATABASE {}", quote_ident(&request.name));
    run_tsv(driver, connected, &sql)
        .await
        .map(|_: TsvOutcome| ())
}

pub async fn drop_database(
    driver: &ClickHouseDriver,
    connected: &ConnectedClient,
    name: &str,
) -> DriverResult<()> {
    let sql = format!("DROP DATABASE {}", quote_ident(name));
    run_tsv(driver, connected, &sql)
        .await
        .map(|_: TsvOutcome| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_top_level_commas_ignores_commas_inside_function_calls() {
        let parts = split_top_level_commas("cityHash64(a, b), c");
        assert_eq!(parts, vec!["cityHash64(a, b)".to_string(), "c".to_string()]);
    }

    #[test]
    fn split_top_level_commas_handles_a_single_column() {
        assert_eq!(split_top_level_commas("id"), vec!["id".to_string()]);
    }

    #[test]
    fn parse_enum_labels_extracts_quoted_labels() {
        let labels = parse_enum_labels("Enum8('a' = 1, 'b' = 2)").expect("labels");
        assert_eq!(labels, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn parse_enum_labels_unwraps_nullable() {
        let labels = parse_enum_labels("Nullable(Enum8('x' = 1))").expect("labels");
        assert_eq!(labels, vec!["x".to_string()]);
    }

    #[test]
    fn parse_enum_labels_returns_none_for_non_enum_types() {
        assert_eq!(parse_enum_labels("String"), None);
    }

    #[test]
    fn table_kind_from_engine_recognizes_view_kinds() {
        assert_eq!(table_kind_from_engine("View"), TableKind::View);
        assert_eq!(
            table_kind_from_engine("MaterializedView"),
            TableKind::MaterializedView
        );
        assert_eq!(table_kind_from_engine("MergeTree"), TableKind::Table);
    }
}
