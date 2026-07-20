//! A cheap, deterministic heuristic for "does this SQL statement return a
//! result set", used by `execute_user_query`'s capped path
//! (`crate::query::execute_user_query_capped`) to decide whether it is
//! safe to run the `SELECT * FROM (<sql>) LIMIT 0 FORMAT
//! TabSeparatedWithNamesAndTypes` header probe (`crate::query::
//! fetch_probe_header`) ahead of incremental `JSONEachRow` streaming.
//!
//! This is **not** a SQL parser. `execute_user_query` is the one method
//! the generic MCP `execute_query` tool calls for *any* SQL a client
//! sends — `SELECT`, `CREATE TABLE`, `INSERT`, `ALTER`, everything — and
//! wrapping a `CREATE TABLE` in that probe's subquery is a syntax error
//! ClickHouse rejects outright (`Code: 62`), not a harmless no-op. A
//! prefix check after skipping whitespace and comments is enough to tell
//! the two cases apart for the statement kinds ClickHouse actually
//! supports; it is not a substitute for real parsing and does not need
//! to be, since a wrong classification here only ever chooses the wrong
//! *code path*, never returns wrong data — see the module-level doc
//! comment on `crate::query::execute_user_query_capped` for what each
//! path does.
//!
//! **Known limitations**, documented rather than glossed over:
//! - Statements that return a row but do not start with one of the
//!   recognized keywords (e.g. `EXISTS TABLE t`, `CHECK TABLE t`) are
//!   misclassified as non-row-returning and run through the buffered
//!   `run_tsv`/`run_tsv_parameterized` path instead of the capped
//!   streaming path. That path still returns the (small) result
//!   correctly; it just does not benefit from incremental capping,
//!   which does not matter for these statements in practice (they never
//!   return large result sets).
//! - A statement embedded inside a string literal that happens to start
//!   with one of these keywords is irrelevant here: this only inspects
//!   the *start* of the whole submitted `sql` text, not values inside it.
//! - Nested/nonstandard comment placement (a comment in the middle of a
//!   keyword, e.g. `SEL/**/ECT`) is not handled; ClickHouse itself barely
//!   tolerates that either.

const ROW_RETURNING_KEYWORDS: [&str; 6] = ["SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN"];

/// Skips leading whitespace and any number of leading `--` line comments
/// or `/* */` block comments, returning whatever text is left.
fn skip_whitespace_and_comments(sql: &str) -> &str {
    let mut rest = sql;

    loop {
        let trimmed = rest.trim_start();

        if let Some(after_marker) = trimmed.strip_prefix("--") {
            rest = match after_marker.find('\n') {
                Some(newline_at) => &after_marker[newline_at + 1..],
                None => "",
            };
            continue;
        }

        if let Some(after_marker) = trimmed.strip_prefix("/*") {
            rest = match after_marker.find("*/") {
                Some(close_at) => &after_marker[close_at + 2..],
                None => "",
            };
            continue;
        }

        return trimmed;
    }
}

fn starts_with_keyword(text: &str, keyword: &str) -> bool {
    if text.len() < keyword.len() {
        return false;
    }
    if !text.as_bytes()[..keyword.len()].eq_ignore_ascii_case(keyword.as_bytes()) {
        return false;
    }
    match text[keyword.len()..].chars().next() {
        None => true,
        Some(next) => !(next.is_alphanumeric() || next == '_'),
    }
}

/// Whether `sql`, after skipping leading whitespace/comments, starts with
/// a ClickHouse keyword that returns a result set (`SELECT`, a `WITH`
/// CTE, `SHOW`, `DESCRIBE`/`DESC`, or `EXPLAIN`). Case-insensitive.
pub(crate) fn is_row_returning_statement(sql: &str) -> bool {
    let rest = skip_whitespace_and_comments(sql);
    ROW_RETURNING_KEYWORDS
        .iter()
        .any(|keyword| starts_with_keyword(rest, keyword))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_select() {
        assert!(is_row_returning_statement("SELECT 1"));
        assert!(is_row_returning_statement("  select * from t"));
    }

    #[test]
    fn recognizes_with_cte() {
        assert!(is_row_returning_statement(
            "WITH x AS (SELECT 1) SELECT * FROM x"
        ));
    }

    #[test]
    fn recognizes_show_describe_and_explain() {
        assert!(is_row_returning_statement("SHOW TABLES"));
        assert!(is_row_returning_statement("DESCRIBE TABLE t"));
        assert!(is_row_returning_statement("DESC t"));
        assert!(is_row_returning_statement("EXPLAIN SELECT 1"));
    }

    #[test]
    fn rejects_ddl_and_dml() {
        assert!(!is_row_returning_statement(
            "CREATE TABLE t (id UInt32) ENGINE = Memory"
        ));
        assert!(!is_row_returning_statement("INSERT INTO t (id) VALUES (1)"));
        assert!(!is_row_returning_statement(
            "ALTER TABLE t ADD COLUMN x String"
        ));
        assert!(!is_row_returning_statement("DROP TABLE t"));
    }

    #[test]
    fn skips_a_leading_line_comment() {
        assert!(is_row_returning_statement("-- a comment\nSELECT 1"));
        assert!(!is_row_returning_statement(
            "-- a comment\nINSERT INTO t (id) VALUES (1)"
        ));
    }

    #[test]
    fn skips_a_leading_block_comment() {
        assert!(is_row_returning_statement("/* comment */ SELECT 1"));
        assert!(!is_row_returning_statement(
            "/* comment */ INSERT INTO t (id) VALUES (1)"
        ));
    }

    #[test]
    fn skips_multiple_mixed_leading_comments() {
        assert!(is_row_returning_statement(
            "-- first\n/* second */\n-- third\nSELECT 1"
        ));
    }

    #[test]
    fn does_not_match_a_keyword_prefix_of_a_longer_identifier() {
        assert!(!is_row_returning_statement("SELECTOR_TABLE_DOES_NOT_EXIST"));
        assert!(!is_row_returning_statement("WITHIN_GROUP_FUNC()"));
    }

    #[test]
    fn is_case_insensitive() {
        assert!(is_row_returning_statement("select 1"));
        assert!(is_row_returning_statement("Select 1"));
    }
}
