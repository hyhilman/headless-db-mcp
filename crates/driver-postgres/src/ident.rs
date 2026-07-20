//! Safe quoting for identifiers spliced into SQL text.
//!
//! Postgres has no way to bind an identifier (table/schema/column/database
//! name) as a query parameter — parameters only ever stand in for values.
//! Anywhere this driver must splice a name into SQL text (`switch_schema`,
//! `create_database`, `drop_database`, DDL reconstruction) it goes through
//! [`quote_ident`], never raw string interpolation.

/// Quotes a Postgres identifier using the standard double-quote-doubling
/// escape (`"` becomes `""`), then wraps the result in double quotes.
///
/// This makes the identifier safe to splice into SQL text: a quoted
/// identifier can contain any character, including another double quote,
/// a semicolon, or whitespace, without breaking out of the identifier
/// position.
pub fn quote_ident(name: &str) -> String {
    let mut quoted = String::with_capacity(name.len() + 2);
    quoted.push('"');
    for ch in name.chars() {
        if ch == '"' {
            quoted.push('"');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    quoted
}

/// Quotes a two-part `schema.name` reference, quoting each part
/// independently so a `.` inside either part cannot be mistaken for the
/// separator.
pub fn quote_qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

/// Quotes a Postgres string literal using standard single-quote-doubling
/// escape. Used only for the handful of DDL keywords (e.g. `CREATE
/// DATABASE ... ENCODING 'UTF8'`) that take a string literal rather than
/// an identifier and cannot be bound as a query parameter.
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
    fn wraps_plain_identifier_in_double_quotes() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn doubles_embedded_double_quotes() {
        assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    }

    #[test]
    fn does_not_treat_semicolon_as_special() {
        let quoted = quote_ident("foo\"; DROP TABLE users; --");
        assert_eq!(quoted, "\"foo\"\"; DROP TABLE users; --\"");
        assert!(quoted.starts_with('"') && quoted.ends_with('"'));
    }

    #[test]
    fn quote_qualified_quotes_both_parts_independently() {
        assert_eq!(
            quote_qualified("my schema", "my.table"),
            "\"my schema\".\"my.table\""
        );
    }

    #[test]
    fn quote_literal_doubles_embedded_single_quotes() {
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
    }

    #[test]
    fn quote_literal_neutralizes_statement_terminator() {
        let quoted = quote_literal("'; DROP TABLE users; --");
        assert_eq!(quoted, "'''; DROP TABLE users; --'");
    }
}
