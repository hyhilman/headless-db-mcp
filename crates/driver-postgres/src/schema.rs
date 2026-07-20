//! Schema introspection: `information_schema` and `pg_catalog` queries
//! backing every `fetch_*` method, plus `create_database`/`drop_database`/
//! `switch_schema`.
//!
//! Every place a schema/table/database name is spliced into SQL text
//! (rather than bound as a parameter — Postgres has no other way to
//! parameterize an identifier) goes through [`crate::ident::quote_ident`]
//! or [`crate::ident::quote_qualified`].

use std::collections::HashMap;

use db_headless_core::{
    ColumnInfo, CreateDatabaseRequest, DatabaseMetadata, DriverError, DriverErrorKind,
    DriverResult, ForeignKeyInfo, IdentityKind, IndexInfo, TableInfo, TableKind, TableMetadata,
    TriggerInfo,
};
use tokio_postgres::{Client, Row};

use crate::error::map_query_error;
use crate::ident::{quote_ident, quote_literal, quote_qualified};
use crate::params::{as_sql_params, to_params};

fn column_error(column: &str, err: tokio_postgres::Error) -> DriverError {
    DriverError::new(
        DriverErrorKind::Internal,
        format!("failed to read column \"{column}\" from a schema-introspection query: {err}"),
    )
}

fn get_string(row: &Row, idx: usize, column: &str) -> DriverResult<String> {
    row.try_get::<_, String>(idx)
        .map_err(|e| column_error(column, e))
}

fn get_opt_string(row: &Row, idx: usize, column: &str) -> DriverResult<Option<String>> {
    row.try_get::<_, Option<String>>(idx)
        .map_err(|e| column_error(column, e))
}

fn get_yes_no(row: &Row, idx: usize, column: &str) -> DriverResult<bool> {
    Ok(get_string(row, idx, column)?.eq_ignore_ascii_case("yes"))
}

fn get_opt_i64(row: &Row, idx: usize, column: &str) -> DriverResult<Option<i64>> {
    row.try_get::<_, Option<i64>>(idx)
        .map_err(|e| column_error(column, e))
}

pub async fn fetch_databases(client: &Client) -> DriverResult<Vec<String>> {
    let rows = client
        .query(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
            &[],
        )
        .await
        .map_err(map_query_error)?;

    rows.iter()
        .map(|row| get_string(row, 0, "datname"))
        .collect()
}

pub async fn fetch_schemas(client: &Client) -> DriverResult<Vec<String>> {
    let rows = client
        .query(
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            &[],
        )
        .await
        .map_err(map_query_error)?;

    rows.iter()
        .map(|row| get_string(row, 0, "schema_name"))
        .collect()
}

pub async fn switch_schema(client: &Client, schema: &str) -> DriverResult<()> {
    let sql = format!("SET search_path TO {}", quote_ident(schema));
    client.simple_query(&sql).await.map_err(map_query_error)?;
    Ok(())
}

fn table_kind_from_relkind(relkind: &str) -> TableKind {
    match relkind {
        "v" => TableKind::View,
        "m" => TableKind::MaterializedView,
        _ => TableKind::Table,
    }
}

pub async fn fetch_tables(client: &Client, schema: &str) -> DriverResult<Vec<TableInfo>> {
    let sql = "
        SELECT c.relname,
               c.relkind::text,
               obj_description(c.oid, 'pg_class') AS comment,
               CASE WHEN c.reltuples < 0 THEN NULL ELSE round(c.reltuples)::bigint END AS row_estimate
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relkind IN ('r', 'v', 'm', 'p', 'f')
        ORDER BY c.relname
    ";
    let rows = client
        .query(sql, &[&schema])
        .await
        .map_err(map_query_error)?;

    rows.iter()
        .map(|row| {
            let name = get_string(row, 0, "relname")?;
            let relkind = get_string(row, 1, "relkind")?;
            let comment = get_opt_string(row, 2, "comment")?;
            let row_count_estimate = get_opt_i64(row, 3, "row_estimate")?;
            Ok(TableInfo {
                name,
                schema: Some(schema.to_string()),
                kind: table_kind_from_relkind(&relkind),
                comment,
                row_count_estimate,
            })
        })
        .collect()
}

pub async fn fetch_columns(
    client: &Client,
    table: &str,
    schema: &str,
) -> DriverResult<Vec<ColumnInfo>> {
    let sql = "
        SELECT col.column_name,
               col.data_type,
               col.udt_name,
               col.is_nullable,
               col.column_default,
               col.character_set_name,
               col.collation_name,
               col.is_identity,
               col.identity_generation,
               col.is_generated,
               col_description(
                   (quote_ident(col.table_schema) || '.' || quote_ident(col.table_name))::regclass,
                   col.ordinal_position
               ) AS comment
        FROM information_schema.columns col
        WHERE col.table_name = $1 AND col.table_schema = $2
        ORDER BY col.ordinal_position
    ";
    let rows = client
        .query(sql, &[&table, &schema])
        .await
        .map_err(map_query_error)?;

    let pk_columns = fetch_primary_key_columns(client, table, schema).await?;
    let mut columns = Vec::with_capacity(rows.len());
    let mut enum_lookups: HashMap<String, Vec<String>> = HashMap::new();

    for row in &rows {
        let name = get_string(row, 0, "column_name")?;
        let data_type = get_string(row, 1, "data_type")?;
        let udt_name = get_string(row, 2, "udt_name")?;
        let is_nullable = get_yes_no(row, 3, "is_nullable")?;
        let default_value = get_opt_string(row, 4, "column_default")?;
        let charset = get_opt_string(row, 5, "character_set_name")?;
        let collation = get_opt_string(row, 6, "collation_name")?;
        let is_identity = get_yes_no(row, 7, "is_identity")?;
        let identity_generation = get_opt_string(row, 8, "identity_generation")?;
        let generated = get_string(row, 9, "is_generated")?;
        let comment = get_opt_string(row, 10, "comment")?;

        let identity_kind = if is_identity {
            match identity_generation.as_deref() {
                Some("ALWAYS") => Some(IdentityKind::Always),
                _ => Some(IdentityKind::ByDefault),
            }
        } else {
            None
        };

        let allowed_values = if data_type == "USER-DEFINED" {
            if !enum_lookups.contains_key(&udt_name) {
                let labels = fetch_enum_labels(client, &udt_name).await?;
                enum_lookups.insert(udt_name.clone(), labels);
            }
            enum_lookups
                .get(&udt_name)
                .filter(|labels| !labels.is_empty())
                .cloned()
        } else {
            None
        };

        columns.push(ColumnInfo {
            is_primary_key: pk_columns.contains(&name),
            name,
            data_type: udt_name,
            is_nullable,
            default_value,
            extra: None,
            charset,
            collation,
            comment,
            identity_kind,
            is_generated: generated.eq_ignore_ascii_case("always"),
            allowed_values,
        });
    }

    Ok(columns)
}

async fn fetch_primary_key_columns(
    client: &Client,
    table: &str,
    schema: &str,
) -> DriverResult<std::collections::HashSet<String>> {
    let sql = "
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.constraint_schema = kcu.constraint_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = $1
          AND tc.table_schema = $2
    ";
    let rows = client
        .query(sql, &[&table, &schema])
        .await
        .map_err(map_query_error)?;
    rows.iter()
        .map(|row| get_string(row, 0, "column_name"))
        .collect()
}

async fn fetch_enum_labels(client: &Client, udt_name: &str) -> DriverResult<Vec<String>> {
    let sql = "
        SELECT e.enumlabel
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE t.typname = $1
        ORDER BY e.enumsortorder
    ";
    let rows = client
        .query(sql, &[&udt_name])
        .await
        .map_err(map_query_error)?;
    rows.iter()
        .map(|row| get_string(row, 0, "enumlabel"))
        .collect()
}

pub async fn fetch_indexes(
    client: &Client,
    table: &str,
    schema: &str,
) -> DriverResult<Vec<IndexInfo>> {
    let sql = "
        SELECT ix.relname AS index_name,
               i.indisunique,
               i.indisprimary,
               am.amname AS method,
               array_agg(a.attname::text ORDER BY k.ord) AS columns
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_class ix ON ix.oid = i.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = ix.relam
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
        WHERE t.relname = $1 AND n.nspname = $2
        GROUP BY ix.relname, i.indisunique, i.indisprimary, am.amname
        ORDER BY ix.relname
    ";
    let rows = client
        .query(sql, &[&table, &schema])
        .await
        .map_err(map_query_error)?;

    rows.iter()
        .map(|row| {
            let name = get_string(row, 0, "index_name")?;
            let is_unique = row
                .try_get::<_, bool>(1)
                .map_err(|e| column_error("indisunique", e))?;
            let is_primary = row
                .try_get::<_, bool>(2)
                .map_err(|e| column_error("indisprimary", e))?;
            let method = get_opt_string(row, 3, "method")?;
            let columns = row
                .try_get::<_, Vec<String>>(4)
                .map_err(|e| column_error("columns", e))?;
            Ok(IndexInfo {
                name,
                columns,
                is_unique,
                is_primary,
                method,
            })
        })
        .collect()
}

struct RawForeignKeyRow {
    constraint_name: String,
    column_name: String,
    referenced_table: String,
    referenced_schema: String,
    referenced_column: String,
    on_update: Option<String>,
    on_delete: Option<String>,
}

fn rule_from_confcode(code: &str) -> Option<String> {
    match code {
        "a" => Some("NO ACTION".to_string()),
        "r" => Some("RESTRICT".to_string()),
        "c" => Some("CASCADE".to_string()),
        "n" => Some("SET NULL".to_string()),
        "d" => Some("SET DEFAULT".to_string()),
        _ => None,
    }
}

/// Uses `pg_constraint`'s `conkey`/`confkey` column-number arrays,
/// zipped with `unnest(..., ...) WITH ORDINALITY`, rather than
/// `information_schema.key_column_usage`/`constraint_column_usage`. The
/// information_schema views are documented to not reliably preserve
/// column correspondence order for composite (multi-column) foreign
/// keys; `pg_constraint`'s parallel arrays are Postgres's own source of
/// truth and pair up correctly by construction.
pub async fn fetch_foreign_keys(
    client: &Client,
    table: &str,
    schema: &str,
) -> DriverResult<Vec<ForeignKeyInfo>> {
    let sql = "
        SELECT con.conname AS constraint_name,
               att2.attname AS column_name,
               cl.relname AS referenced_table,
               ns2.nspname AS referenced_schema,
               att.attname AS referenced_column,
               con.confupdtype::text AS update_code,
               con.confdeltype::text AS delete_code
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace relns ON relns.oid = rel.relnamespace
        JOIN pg_class cl ON cl.oid = con.confrelid
        JOIN pg_namespace ns2 ON ns2.oid = cl.relnamespace
        JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS pos(conkey, confkey, ord)
          ON true
        JOIN pg_attribute att2 ON att2.attrelid = con.conrelid AND att2.attnum = pos.conkey
        JOIN pg_attribute att ON att.attrelid = con.confrelid AND att.attnum = pos.confkey
        WHERE con.contype = 'f' AND rel.relname = $1 AND relns.nspname = $2
        ORDER BY con.conname, pos.ord
    ";
    let rows = client
        .query(sql, &[&table, &schema])
        .await
        .map_err(map_query_error)?;

    let mut order: Vec<String> = Vec::new();
    let mut by_name: HashMap<String, ForeignKeyInfo> = HashMap::new();

    for row in &rows {
        let raw = RawForeignKeyRow {
            constraint_name: get_string(row, 0, "constraint_name")?,
            column_name: get_string(row, 1, "column_name")?,
            referenced_table: get_string(row, 2, "referenced_table")?,
            referenced_schema: get_string(row, 3, "referenced_schema")?,
            referenced_column: get_string(row, 4, "referenced_column")?,
            on_update: rule_from_confcode(&get_string(row, 5, "update_code")?),
            on_delete: rule_from_confcode(&get_string(row, 6, "delete_code")?),
        };

        let entry = by_name
            .entry(raw.constraint_name.clone())
            .or_insert_with(|| {
                order.push(raw.constraint_name.clone());
                ForeignKeyInfo {
                    name: raw.constraint_name.clone(),
                    columns: Vec::new(),
                    referenced_table: raw.referenced_table.clone(),
                    referenced_schema: Some(raw.referenced_schema.clone()),
                    referenced_columns: Vec::new(),
                    on_delete: raw.on_delete.clone(),
                    on_update: raw.on_update.clone(),
                }
            });
        entry.columns.push(raw.column_name);
        entry.referenced_columns.push(raw.referenced_column);
    }

    let mut result = Vec::with_capacity(order.len());
    for name in order {
        if let Some(fk) = by_name.remove(&name) {
            result.push(fk);
        }
    }
    Ok(result)
}

pub async fn fetch_triggers(
    client: &Client,
    table: &str,
    schema: &str,
) -> DriverResult<Vec<TriggerInfo>> {
    let sql = "
        SELECT trigger_name, event_manipulation, action_timing, action_statement
        FROM information_schema.triggers
        WHERE event_object_table = $1 AND event_object_schema = $2
        ORDER BY trigger_name, event_manipulation
    ";
    let rows = client
        .query(sql, &[&table, &schema])
        .await
        .map_err(map_query_error)?;

    rows.iter()
        .map(|row| {
            Ok(TriggerInfo {
                name: get_string(row, 0, "trigger_name")?,
                event: get_string(row, 1, "event_manipulation")?,
                timing: get_string(row, 2, "action_timing")?,
                definition: get_opt_string(row, 3, "action_statement")?,
            })
        })
        .collect()
}

/// Reconstructs a `CREATE TABLE` statement from `pg_attribute`/`pg_attrdef`
/// (exact column types via `format_type`, exact default expressions via
/// `pg_get_expr`) plus the primary key constraint, if any.
///
/// Deliberately scoped down: this does **not** include foreign keys,
/// check constraints, non-primary-key unique constraints, secondary
/// indexes, partitioning, table inheritance, storage parameters, or
/// comments. All of those are available from the sibling `fetch_*`
/// methods; folding them into one DDL string correctly is what
/// `pg_dump` exists for; reproducing an important slice of it here (not
/// all of it) is the scope for this driver.
pub async fn fetch_table_ddl(client: &Client, table: &str, schema: &str) -> DriverResult<String> {
    let sql = "
        SELECT a.attname,
               format_type(a.atttypid, a.atttypmod) AS formatted_type,
               a.attnotnull,
               pg_get_expr(d.adbin, d.adrelid) AS default_expr
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE c.relname = $1 AND n.nspname = $2 AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    ";
    let rows = client
        .query(sql, &[&table, &schema])
        .await
        .map_err(map_query_error)?;

    if rows.is_empty() {
        return Err(DriverError::new(
            DriverErrorKind::Query,
            format!("table \"{schema}\".\"{table}\" was not found"),
        ));
    }

    let mut column_lines = Vec::with_capacity(rows.len());
    for row in &rows {
        let name = get_string(row, 0, "attname")?;
        let formatted_type = get_string(row, 1, "formatted_type")?;
        let not_null = row
            .try_get::<_, bool>(2)
            .map_err(|e| column_error("attnotnull", e))?;
        let default_expr = get_opt_string(row, 3, "default_expr")?;

        let mut line = format!("    {} {}", quote_ident(&name), formatted_type);
        if not_null {
            line.push_str(" NOT NULL");
        }
        if let Some(default_expr) = default_expr {
            line.push_str(&format!(" DEFAULT {default_expr}"));
        }
        column_lines.push(line);
    }

    let pk_columns = fetch_primary_key_columns_ordered(client, table, schema).await?;
    if !pk_columns.is_empty() {
        let quoted = pk_columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        column_lines.push(format!("    PRIMARY KEY ({quoted})"));
    }

    Ok(format!(
        "CREATE TABLE {} (\n{}\n);",
        quote_qualified(schema, table),
        column_lines.join(",\n")
    ))
}

async fn fetch_primary_key_columns_ordered(
    client: &Client,
    table: &str,
    schema: &str,
) -> DriverResult<Vec<String>> {
    let sql = "
        SELECT a.attname
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
        WHERE i.indisprimary AND t.relname = $1 AND n.nspname = $2
        ORDER BY k.ord
    ";
    let rows = client
        .query(sql, &[&table, &schema])
        .await
        .map_err(map_query_error)?;
    rows.iter()
        .map(|row| get_string(row, 0, "attname"))
        .collect()
}

pub async fn fetch_view_definition(
    client: &Client,
    view: &str,
    schema: &str,
) -> DriverResult<String> {
    let qualified = db_headless_core::CellValue::Text(quote_qualified(schema, view));
    let bound = to_params(std::slice::from_ref(&qualified));
    let params = as_sql_params(&bound);

    // `$1::regclass` makes Postgres infer the parameter type as `regclass`,
    // which `&str`/`String`'s built-in `ToSql` does not accept (its
    // `accepts` list is text-like types only). `SqlParam` accepts every
    // type and sends the qualified name as a text-format parameter, which
    // `regclass`'s own input function parses exactly like it would parse
    // the same quoted string written directly in SQL.
    let row = client
        .query_one("SELECT pg_get_viewdef($1::regclass, true)", &params)
        .await
        .map_err(map_query_error)?;
    get_string(&row, 0, "pg_get_viewdef")
}

pub async fn fetch_table_metadata(
    client: &Client,
    table: &str,
    schema: &str,
) -> DriverResult<TableMetadata> {
    let tables = fetch_tables(client, schema).await?;
    let info = tables
        .into_iter()
        .find(|t| t.name == table)
        .ok_or_else(|| {
            DriverError::new(
                DriverErrorKind::Query,
                format!("table \"{schema}\".\"{table}\" was not found"),
            )
        })?;

    let columns = fetch_columns(client, table, schema).await?;
    let indexes = fetch_indexes(client, table, schema).await?;
    let foreign_keys = fetch_foreign_keys(client, table, schema).await?;
    let triggers = fetch_triggers(client, table, schema).await?;

    Ok(TableMetadata {
        info,
        columns,
        indexes,
        foreign_keys,
        triggers,
    })
}

pub async fn fetch_database_metadata(
    client: &Client,
    database: &str,
) -> DriverResult<DatabaseMetadata> {
    let schemas = fetch_schemas(client).await?;

    let size_bytes = match client
        .query_one("SELECT pg_database_size($1)", &[&database])
        .await
    {
        Ok(row) => row
            .try_get::<_, i64>(0)
            .ok()
            .and_then(|v| u64::try_from(v).ok()),
        Err(err) => {
            tracing::debug!(error = %err, database, "pg_database_size lookup failed, omitting size");
            None
        }
    };

    Ok(DatabaseMetadata {
        name: database.to_string(),
        schemas,
        size_bytes,
    })
}

pub async fn create_database(client: &Client, request: &CreateDatabaseRequest) -> DriverResult<()> {
    let mut sql = format!("CREATE DATABASE {}", quote_ident(&request.name));
    if let Some(owner) = &request.owner {
        sql.push_str(&format!(" OWNER {}", quote_ident(owner)));
    }
    if let Some(encoding) = &request.encoding {
        sql.push_str(&format!(" ENCODING {}", quote_literal(encoding)));
    }
    client.simple_query(&sql).await.map_err(map_query_error)?;
    Ok(())
}

pub async fn drop_database(client: &Client, name: &str) -> DriverResult<()> {
    let sql = format!("DROP DATABASE {}", quote_ident(name));
    client.simple_query(&sql).await.map_err(map_query_error)?;
    Ok(())
}
