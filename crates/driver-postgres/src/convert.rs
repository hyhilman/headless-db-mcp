//! Converts `tokio_postgres::Row` cells back into
//! `db_headless_core::CellValue`, matching the driver contract's untyped
//! `Null | Text | Bytes` wire model.
//!
//! `execute_parameterized`, `execute_user_query`, and `stream_rows` all
//! run through the extended query protocol, which this driver always
//! requests in **binary** result format (that is `tokio_postgres`'s own
//! hardcoded choice, not something this crate controls per-column). So
//! every column here is decoded from Postgres's binary wire format via
//! `FromSql`, then rendered back to a text `CellValue` — never returned as
//! a typed Rust value, per the driver contract.
//!
//! Verified to round-trip cleanly through `CellValue::Text`:
//! `bool`, `int2`, `int4`, `int8`, `oid`, `float4`, `float8`, `text`,
//! `varchar`, `bpchar`, `name`, `unknown`, `uuid`, `numeric` (via
//! `rust_decimal`, exact — no `f64` precision loss), `json`/`jsonb`,
//! `date`, `time`, `timestamp`, `timestamptz`. `bytea` round-trips through
//! `CellValue::Bytes` instead (lossless, no text encoding needed).
//!
//! `float4`/`float8` round-trip through Rust's own `f32`/`f64` `Display`,
//! which is not guaranteed byte-for-byte identical to Postgres's own text
//! output for every value (e.g. some values differ in trailing-digit
//! rendering); both parse back to the same underlying binary float, so
//! this is a cosmetic gap, not a correctness one.
//!
//! Known gap, by design rather than oversight: arrays, ranges, composite
//! types, enums, and any other OID not matched below return a
//! `DriverErrorKind::Internal` error naming the column and type instead of
//! guessing at a textual rendering. Silently emitting a wrong or partial
//! string for a type this driver has not verified would violate the
//! "never produce corrupt data" guardrail; failing loudly is the
//! conservative choice until those types get real coverage.

use db_headless_core::{CellValue, DriverError, DriverErrorKind};
use tokio_postgres::types::{FromSql, Json, Type};
use tokio_postgres::Row;

pub fn row_value(row: &Row, idx: usize) -> Result<CellValue, DriverError> {
    let column = &row.columns()[idx];
    let ty = column.type_();

    match *ty {
        Type::BOOL => text_from::<bool>(row, idx, column.name(), ty, |v| {
            if v { "true" } else { "false" }.to_string()
        }),
        Type::INT2 => text_from::<i16>(row, idx, column.name(), ty, |v| v.to_string()),
        Type::INT4 => text_from::<i32>(row, idx, column.name(), ty, |v| v.to_string()),
        Type::OID => text_from::<u32>(row, idx, column.name(), ty, |v| v.to_string()),
        Type::INT8 => text_from::<i64>(row, idx, column.name(), ty, |v| v.to_string()),
        Type::FLOAT4 => text_from::<f32>(row, idx, column.name(), ty, |v| v.to_string()),
        Type::FLOAT8 => text_from::<f64>(row, idx, column.name(), ty, |v| v.to_string()),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            text_from::<String>(row, idx, column.name(), ty, |v| v)
        }
        Type::BYTEA => bytes_from(row, idx, column.name(), ty),
        Type::UUID => text_from::<uuid::Uuid>(row, idx, column.name(), ty, |v| v.to_string()),
        Type::JSON | Type::JSONB => {
            text_from::<Json<serde_json::Value>>(row, idx, column.name(), ty, |v| v.0.to_string())
        }
        Type::NUMERIC => {
            text_from::<rust_decimal::Decimal>(row, idx, column.name(), ty, |v| v.to_string())
        }
        Type::DATE => {
            text_from::<chrono::NaiveDate>(row, idx, column.name(), ty, |v| v.to_string())
        }
        Type::TIME => {
            text_from::<chrono::NaiveTime>(row, idx, column.name(), ty, |v| v.to_string())
        }
        Type::TIMESTAMP => text_from::<chrono::NaiveDateTime>(row, idx, column.name(), ty, |v| {
            v.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
        }),
        Type::TIMESTAMPTZ => {
            text_from::<chrono::DateTime<chrono::Utc>>(row, idx, column.name(), ty, |v| {
                v.to_rfc3339()
            })
        }
        _ => Err(unsupported_type_error(column.name(), ty)),
    }
}

fn unsupported_type_error(column_name: &str, ty: &Type) -> DriverError {
    DriverError::new(
        DriverErrorKind::Internal,
        format!(
            "column \"{column_name}\" has Postgres type \"{}\" (oid {}), which this driver build \
             does not yet support converting to a text cell value",
            ty.name(),
            ty.oid()
        ),
    )
}

fn conversion_error(column_name: &str, ty: &Type, source: tokio_postgres::Error) -> DriverError {
    DriverError::new(
        DriverErrorKind::Internal,
        format!(
            "failed to decode column \"{column_name}\" (type \"{}\"): {source}",
            ty.name()
        ),
    )
}

fn text_from<T>(
    row: &Row,
    idx: usize,
    column_name: &str,
    ty: &Type,
    render: impl FnOnce(T) -> String,
) -> Result<CellValue, DriverError>
where
    T: for<'a> FromSql<'a>,
{
    match row.try_get::<usize, Option<T>>(idx) {
        Ok(Some(value)) => Ok(CellValue::Text(render(value))),
        Ok(None) => Ok(CellValue::Null),
        Err(err) => Err(conversion_error(column_name, ty, err)),
    }
}

fn bytes_from(
    row: &Row,
    idx: usize,
    column_name: &str,
    ty: &Type,
) -> Result<CellValue, DriverError> {
    match row.try_get::<usize, Option<Vec<u8>>>(idx) {
        Ok(Some(value)) => Ok(CellValue::Bytes(value)),
        Ok(None) => Ok(CellValue::Null),
        Err(err) => Err(conversion_error(column_name, ty, err)),
    }
}

pub fn column_names_and_types(
    row_columns: &[tokio_postgres::Column],
) -> (Vec<String>, Vec<String>) {
    let names = row_columns.iter().map(|c| c.name().to_string()).collect();
    let types = row_columns
        .iter()
        .map(|c| c.type_().name().to_string())
        .collect();
    (names, types)
}
