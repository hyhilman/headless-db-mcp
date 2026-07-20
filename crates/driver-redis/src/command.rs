//! Tokenizes a raw Redis command line and builds a `redis::Cmd` from it.
//!
//! `execute`/`execute_parameterized`/`execute_user_query` interpret their
//! `query` string as one command line, e.g. `SET mykey myvalue` or
//! `HGETALL myhash`. Tokenization respects single/double-quoted
//! substrings so `SET key "value with spaces"` yields two arguments
//! (`key`, `value with spaces`), not four.
//!
//! A bare, unquoted token that is exactly `?` is a bound-parameter
//! placeholder, filled in order from the caller's `parameters` slice.
//! Placeholder substitution binds each value as a **distinct RESP
//! argument** via `redis::Cmd::arg`, never by re-joining tokens into a
//! string and re-parsing it. This is what makes parameter binding here
//! inherently safe the way SQL bind-parameters are, even though the
//! injection mechanics are completely different from SQL: `Cmd::arg`
//! encodes each argument as an independent RESP bulk string with an
//! explicit byte-length prefix (`$<len>\r\n<bytes>\r\n`), so a value
//! containing whitespace, a newline, or even another command's worth of
//! text can never be reinterpreted as "a new argument" or "a new
//! command" — RESP framing is length-prefixed, not delimiter-based, so
//! there is no delimiter for a malicious value to smuggle. This is the
//! Redis-shaped analog of "never build a command by string-interpolating
//! a parameter value" (SQL injection's guardrail), applied to a protocol
//! that has no notion of statement text to begin with.

use db_headless_core::{CellValue, DriverError, DriverErrorKind, DriverResult};

const PLACEHOLDER: &str = "?";

/// Commands this driver knows never mutate the keyspace, covering the six
/// pseudo-tables (`schema.rs`) plus generic/introspection commands.
///
/// Deny-by-default: any command not on this explicit list is treated as
/// a write and rejected on a `read_only` connection, even if it is
/// actually harmless (e.g. a newer read-only command this list has not
/// caught up with yet). A false rejection only costs a retry; a false
/// permission would defeat the whole point of `read_only`.
const READ_ONLY_COMMANDS: &[&str] = &[
    // string
    "GET",
    "MGET",
    "GETRANGE",
    "STRLEN",
    "SUBSTR",
    // hash
    "HGET",
    "HGETALL",
    "HMGET",
    "HKEYS",
    "HVALS",
    "HLEN",
    "HSTRLEN",
    "HEXISTS",
    "HRANDFIELD",
    "HSCAN",
    // list
    "LRANGE",
    "LLEN",
    "LINDEX",
    "LPOS",
    // set
    "SMEMBERS",
    "SISMEMBER",
    "SMISMEMBER",
    "SCARD",
    "SRANDMEMBER",
    "SSCAN",
    "SINTER",
    "SUNION",
    "SDIFF",
    // sorted set
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZRANGEBYLEX",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZREVRANGEBYLEX",
    "ZSCORE",
    "ZMSCORE",
    "ZCARD",
    "ZCOUNT",
    "ZLEXCOUNT",
    "ZRANK",
    "ZREVRANK",
    "ZSCAN",
    "ZRANDMEMBER",
    // stream
    "XRANGE",
    "XREVRANGE",
    "XLEN",
    "XREAD",
    "XINFO",
    // generic key introspection
    "EXISTS",
    "TYPE",
    "TTL",
    "PTTL",
    "EXPIRETIME",
    "PEXPIRETIME",
    "KEYS",
    "SCAN",
    "DBSIZE",
    "RANDOMKEY",
    "DUMP",
    "OBJECT",
    "BITCOUNT",
    "BITPOS",
    "GETBIT",
    // connection/server (no data mutation)
    "PING",
    "ECHO",
    "TIME",
    "COMMAND",
    "INFO",
    "MEMORY",
];

/// Rejects `query` on a `read_only` connection unless its command verb is
/// on [`READ_ONLY_COMMANDS`]. Runs before parameter binding since the
/// classification only needs the command name, not the bound values.
pub(crate) fn require_read_only(query: &str) -> DriverResult<()> {
    let tokens = tokenize(query)?;
    let name = tokens
        .first()
        .ok_or_else(|| DriverError::new(DriverErrorKind::Query, "empty Redis command"))?
        .to_uppercase();

    if READ_ONLY_COMMANDS.contains(&name.as_str()) {
        Ok(())
    } else {
        Err(DriverError::new(
            DriverErrorKind::Query,
            format!("read-only connection: command '{name}' is not permitted"),
        ))
    }
}

/// Splits `input` into whitespace-separated tokens, treating a
/// single-quoted or double-quoted substring as one token regardless of
/// the whitespace inside it. Quote characters are consumed, not included
/// in the resulting token. An unterminated quote is a `DriverError`, not
/// a silently mis-tokenized command.
pub(crate) fn tokenize(input: &str) -> DriverResult<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut has_current = false;
    let mut chars = input.chars();

    while let Some(ch) = chars.next() {
        match ch {
            c if c.is_whitespace() => {
                if has_current {
                    tokens.push(std::mem::take(&mut current));
                    has_current = false;
                }
            }
            '"' | '\'' => {
                let quote = ch;
                has_current = true;
                loop {
                    match chars.next() {
                        Some(c) if c == quote => break,
                        Some(c) => current.push(c),
                        None => {
                            return Err(DriverError::new(
                                DriverErrorKind::Query,
                                format!("unterminated {quote} quote in Redis command"),
                            ));
                        }
                    }
                }
            }
            other => {
                current.push(other);
                has_current = true;
            }
        }
    }

    if has_current {
        tokens.push(current);
    }

    Ok(tokens)
}

/// Builds a `redis::Cmd` from already-tokenized command text, binding any
/// `?` placeholder tokens from `parameters` in order.
///
/// `CellValue::Null` has no valid encoding as a Redis command *argument*
/// (as opposed to a null *reply*, which is a completely different thing —
/// `Value::Nil`): binding it as an empty string would silently change
/// what the caller asked for, so it is rejected instead.
pub(crate) fn build_command(
    tokens: &[String],
    parameters: Option<&[CellValue]>,
) -> DriverResult<redis::Cmd> {
    let (name, rest) = tokens
        .split_first()
        .ok_or_else(|| DriverError::new(DriverErrorKind::Query, "empty Redis command"))?;

    let mut cmd = redis::cmd(&name.to_uppercase());
    let parameters = parameters.unwrap_or(&[]);
    let mut next_parameter = parameters.iter();

    for token in rest {
        if token == PLACEHOLDER {
            let value = next_parameter.next().ok_or_else(|| {
                DriverError::new(
                    DriverErrorKind::Query,
                    "not enough bound parameters for the `?` placeholders in this command",
                )
            })?;
            bind_parameter(&mut cmd, value)?;
        } else {
            cmd.arg(token.as_str());
        }
    }

    Ok(cmd)
}

fn bind_parameter(cmd: &mut redis::Cmd, value: &CellValue) -> DriverResult<()> {
    match value {
        CellValue::Null => Err(DriverError::new(
            DriverErrorKind::Query,
            "Redis command arguments cannot be null",
        )),
        CellValue::Text(s) => {
            cmd.arg(s.as_str());
            Ok(())
        }
        CellValue::Bytes(b) => {
            cmd.arg(b.as_slice());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_plain_whitespace_separated_command() {
        let tokens = tokenize("SET mykey myvalue").expect("tokenize");
        assert_eq!(tokens, vec!["SET", "mykey", "myvalue"]);
    }

    #[test]
    fn quoted_substring_with_spaces_is_one_token() {
        let tokens = tokenize(r#"SET key "value with spaces""#).expect("tokenize");
        assert_eq!(tokens, vec!["SET", "key", "value with spaces"]);
    }

    #[test]
    fn single_quoted_substring_is_one_token() {
        let tokens = tokenize("SET key 'value with spaces'").expect("tokenize");
        assert_eq!(tokens, vec!["SET", "key", "value with spaces"]);
    }

    #[test]
    fn unterminated_quote_is_a_driver_error() {
        let err = tokenize(r#"SET key "unterminated"#).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn empty_command_is_rejected() {
        let err = build_command(&[], None).err().expect("must be an error");
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn null_parameter_is_rejected_rather_than_becoming_empty_string() {
        let tokens = tokenize("SET mykey ?").expect("tokenize");
        let err = build_command(&tokens, Some(&[CellValue::Null]))
            .err()
            .expect("must be an error");
        assert_eq!(err.kind, DriverErrorKind::Query);
        assert!(err.message.contains("null"));
    }

    #[test]
    fn missing_bound_parameter_is_a_clear_error_not_a_panic() {
        let tokens = tokenize("SET mykey ?").expect("tokenize");
        let err = build_command(&tokens, Some(&[]))
            .err()
            .expect("must be an error");
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn literal_question_mark_value_does_not_reopen_tokenization() {
        let tokens = tokenize("SET mykey ?").expect("tokenize");
        let cmd = build_command(
            &tokens,
            Some(&[CellValue::Text("value ? with spaces".to_string())]),
        )
        .expect("build command");
        let packed = String::from_utf8_lossy(&cmd.get_packed_command()).into_owned();
        assert!(packed.contains("value ? with spaces"));
    }

    #[test]
    fn read_only_command_is_allowed_regardless_of_case() {
        require_read_only("get mykey").expect("lowercase GET is allowed");
        require_read_only("HGETALL myhash").expect("HGETALL is allowed");
    }

    #[test]
    fn write_command_is_rejected() {
        let err = require_read_only("SET mykey myvalue").unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
        assert!(err.message.contains("SET"));
    }

    #[test]
    fn unknown_command_is_rejected_not_silently_allowed() {
        let err = require_read_only("SOMENEWCOMMAND arg").unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn empty_command_is_rejected_by_require_read_only_too() {
        let err = require_read_only("   ").unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }
}
