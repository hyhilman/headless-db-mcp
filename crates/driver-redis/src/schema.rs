//! Redis has no tables, columns, indexes, foreign keys, triggers, DDL, or
//! views. To let the rest of the system (schema browsing, the MCP tool
//! surface) treat this driver uniformly with SQL-shaped drivers, it
//! models a fixed set of **pseudo-tables, one per Redis data type**:
//! `string`, `hash`, `list`, `set`, `zset`, `stream`.
//!
//! This is a deliberate simplification, not a real schema: it is not
//! data-dependent (the keyspace is never scanned to decide which
//! pseudo-tables "exist" — all six are always reported, whether or not
//! any key of that type currently exists) and it never changes at
//! runtime. Each pseudo-table's synthetic column list (below) describes
//! the shape of a "row" when browsing that type via `stream_rows`
//! (`stream.rs`) — it is not introspected from any server metadata,
//! because Redis has none to introspect.

use redis::aio::ConnectionManager;

use db_headless_core::{
    ColumnInfo, DriverError, DriverErrorKind, DriverResult, TableInfo, TableKind,
};

pub(crate) const PSEUDO_TABLES: [&str; 6] = ["string", "hash", "list", "set", "zset", "stream"];

const PSEUDO_TABLE_COMMENT: &str =
    "Redis pseudo-table: a synthetic view over one Redis data type, not a real schema object.";

/// Redis's conventional default when the server's real `databases` count
/// cannot be read (see `fetch_databases`).
const DEFAULT_DATABASE_COUNT: i64 = 16;

pub(crate) fn unknown_pseudo_table(name: &str) -> DriverError {
    DriverError::new(
        DriverErrorKind::Query,
        format!(
            "unknown Redis pseudo-table {name:?}; expected one of string, hash, list, set, zset, stream"
        ),
    )
}

pub(crate) fn validate_pseudo_table(name: &str) -> DriverResult<()> {
    if PSEUDO_TABLES.contains(&name) {
        Ok(())
    } else {
        Err(unknown_pseudo_table(name))
    }
}

/// The `schema` argument is accepted (per the `DatabaseDriver` trait
/// signature) but ignored: Redis has no schema concept, so every caller
/// sees the same fixed six pseudo-tables regardless of what it passes.
pub(crate) fn fetch_tables() -> Vec<TableInfo> {
    PSEUDO_TABLES
        .iter()
        .map(|name| TableInfo {
            name: (*name).to_string(),
            schema: None,
            kind: TableKind::Table,
            comment: Some(PSEUDO_TABLE_COMMENT.to_string()),
            row_count_estimate: None,
        })
        .collect()
}

pub(crate) fn columns_for(table: &str) -> DriverResult<Vec<ColumnInfo>> {
    let names: &[&str] = match table {
        "string" => &["key", "value"],
        "hash" => &["key", "field", "value"],
        "list" => &["key", "index", "value"],
        "set" => &["key", "member"],
        "zset" => &["key", "member", "score"],
        "stream" => &["key", "id", "fields"],
        other => return Err(unknown_pseudo_table(other)),
    };

    Ok(names
        .iter()
        .map(|name| ColumnInfo {
            name: (*name).to_string(),
            data_type: "text".to_string(),
            is_nullable: false,
            is_primary_key: false,
            default_value: None,
            extra: None,
            charset: None,
            collation: None,
            comment: None,
            identity_kind: None,
            is_generated: false,
            allowed_values: None,
        })
        .collect())
}

/// Redis's numbered databases (`SELECT 0`..`SELECT n-1`). `CONFIG GET
/// databases` reports the server's real configured count, but some
/// managed Redis deployments disable `CONFIG` entirely (it returns an
/// error, e.g. `ERR unknown command` or `NOPERM`), so a failure here
/// falls back to Redis's own documented default of 16 databases rather
/// than surfacing an error — a database list is a browsing aid, and 16
/// is the correct answer for the overwhelming majority of real servers.
pub(crate) async fn fetch_databases(manager: &mut ConnectionManager) -> DriverResult<Vec<String>> {
    let count = fetch_configured_database_count(manager)
        .await
        .unwrap_or(DEFAULT_DATABASE_COUNT);
    Ok((0..count).map(|n| n.to_string()).collect())
}

async fn fetch_configured_database_count(manager: &mut ConnectionManager) -> Option<i64> {
    let value = redis::cmd("CONFIG")
        .arg("GET")
        .arg("databases")
        .query_async::<redis::Value>(manager)
        .await
        .ok()?;

    match value {
        redis::Value::Array(items) | redis::Value::Set(items) if items.len() >= 2 => {
            database_count_from_value(&items[1])
        }
        redis::Value::Map(pairs) => pairs
            .iter()
            .find_map(|(_, value)| database_count_from_value(value)),
        _ => None,
    }
}

fn database_count_from_value(value: &redis::Value) -> Option<i64> {
    match value {
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.trim().parse().ok(),
        redis::Value::SimpleString(s) => s.trim().parse().ok(),
        redis::Value::Int(i) => Some(*i),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_tables_always_reports_all_six_pseudo_tables() {
        let tables = fetch_tables();
        assert_eq!(tables.len(), 6);
        for name in PSEUDO_TABLES {
            assert!(tables.iter().any(|t| t.name == name));
        }
    }

    #[test]
    fn columns_for_string_matches_documented_shape() {
        let columns = columns_for("string").expect("columns");
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["key", "value"]);
    }

    #[test]
    fn columns_for_hash_matches_documented_shape() {
        let columns = columns_for("hash").expect("columns");
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["key", "field", "value"]);
    }

    #[test]
    fn columns_for_list_matches_documented_shape() {
        let columns = columns_for("list").expect("columns");
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["key", "index", "value"]);
    }

    #[test]
    fn columns_for_set_matches_documented_shape() {
        let columns = columns_for("set").expect("columns");
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["key", "member"]);
    }

    #[test]
    fn columns_for_zset_matches_documented_shape() {
        let columns = columns_for("zset").expect("columns");
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["key", "member", "score"]);
    }

    #[test]
    fn columns_for_stream_matches_documented_shape() {
        let columns = columns_for("stream").expect("columns");
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["key", "id", "fields"]);
    }

    #[test]
    fn columns_for_unknown_table_is_a_driver_error_not_empty_or_a_panic() {
        let err = columns_for("bogus").unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn database_count_from_bulk_string_parses() {
        assert_eq!(
            database_count_from_value(&redis::Value::BulkString(b"16".to_vec())),
            Some(16)
        );
    }

    #[test]
    fn database_count_from_unparsable_value_is_none() {
        assert_eq!(
            database_count_from_value(&redis::Value::BulkString(b"not-a-number".to_vec())),
            None
        );
    }
}
