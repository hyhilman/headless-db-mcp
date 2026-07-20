//! Safe quoting for identifiers spliced into SQL text.
//!
//! ClickHouse quotes identifiers with backticks, not the double quotes
//! Postgres uses (see `crates/driver-postgres/src/ident.rs`) — an embedded
//! backtick is escaped by doubling it, the same doubling convention
//! Postgres uses for `"`. Anywhere this driver must splice a database or
//! table name into SQL text (`create_database`, `drop_database`) it goes
//! through [`quote_ident`], never raw string interpolation. Most of this
//! driver's other identifier-shaped lookups (`switch_database`'s existence
//! check, `fetch_table_ddl`'s `WHERE name = ?`) bind the name as an
//! out-of-band parameter instead and never need quoting at all — so,
//! unlike `driver-postgres`, there is no `quote_qualified` here: nothing
//! in this driver ever splices a two-part `database.table` reference into
//! SQL text (ClickHouse's `WHERE database = ? AND name = ?` predicates
//! cover every such lookup).

/// Quotes a ClickHouse identifier using the standard backtick-doubling
/// escape (`` ` `` becomes `` `` ``), then wraps the result in backticks.
///
/// This makes the identifier safe to splice into SQL text: a quoted
/// identifier can contain any character, including another backtick, a
/// semicolon, or whitespace, without breaking out of the identifier
/// position.
pub fn quote_ident(name: &str) -> String {
    let mut quoted = String::with_capacity(name.len() + 2);
    quoted.push('`');
    for ch in name.chars() {
        if ch == '`' {
            quoted.push('`');
        }
        quoted.push(ch);
    }
    quoted.push('`');
    quoted
}

/// Quotes a ClickHouse string literal using standard single-quote-doubling
/// escape. Used only for the handful of places (e.g. the `KILL QUERY`
/// command sent by `cancel_query`) that need a string literal rather than
/// an identifier and cannot be bound as an out-of-band HTTP parameter.
pub fn quote_literal(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push('\'');
        }
        quoted.push(ch);
    }
    quoted.push('\'');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_plain_identifier_in_backticks() {
        assert_eq!(quote_ident("events"), "`events`");
    }

    #[test]
    fn doubles_embedded_backticks() {
        assert_eq!(quote_ident("weird`name"), "`weird``name`");
    }

    #[test]
    fn does_not_treat_semicolon_as_special() {
        let quoted = quote_ident("foo`; DROP TABLE events; --");
        assert_eq!(quoted, "`foo``; DROP TABLE events; --`");
        assert!(quoted.starts_with('`') && quoted.ends_with('`'));
    }

    #[test]
    fn quote_literal_doubles_embedded_single_quotes() {
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
    }

    #[test]
    fn quote_literal_neutralizes_statement_terminator() {
        let quoted = quote_literal("'; DROP TABLE events; --");
        assert_eq!(quoted, "'''; DROP TABLE events; --'");
    }
}
