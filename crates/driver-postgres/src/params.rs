//! Binds `db_headless_core::CellValue` as a Postgres query parameter.
//!
//! `tokio_postgres`'s `ToSql` trait is normally implemented per concrete
//! Rust type, matched against a specific Postgres `Type` at bind time.
//! `CellValue` is deliberately untyped (`Null | Text | Bytes`), so
//! `SqlParam` implements `ToSql` by hand, always accepting the server's
//! inferred parameter type and sending the value in whichever wire format
//! is correct for that `CellValue` variant:
//!
//! - `Null` writes nothing and reports `IsNull::Yes`.
//! - `Text` is sent in Postgres **text format** (format code 0) with the
//!   raw UTF-8 bytes of the string. Postgres parses text-format
//!   parameters with the target type's normal input function, which is
//!   exactly the "untyped text parameter" behavior this wire model wants:
//!   the same string binds correctly against an `int4`, a `timestamptz`,
//!   or a `text` column without this driver ever needing to know which.
//! - `Bytes` is sent in Postgres **binary format** (format code 1) with
//!   the raw bytes verbatim. This is only correct when the target column
//!   is `bytea` (binary format for `bytea` *is* the raw byte sequence);
//!   binding `Bytes` against a non-`bytea` column is a caller error, not
//!   something this driver can detect ahead of time since `accepts`
//!   deliberately accepts every type.
//!
//! Getting the per-variant format code right is load-bearing: sending
//! text bytes tagged as binary format (or vice versa) does not error, it
//! silently corrupts the value Postgres reconstructs from the wire bytes.

use bytes::BytesMut;
use db_headless_core::CellValue;
use tokio_postgres::types::{Format, IsNull, ToSql, Type};

#[derive(Debug)]
pub struct SqlParam<'a>(pub &'a CellValue);

impl ToSql for SqlParam<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.0 {
            CellValue::Null => Ok(IsNull::Yes),
            CellValue::Text(s) => {
                out.extend_from_slice(s.as_bytes());
                Ok(IsNull::No)
            }
            CellValue::Bytes(b) => {
                out.extend_from_slice(b);
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        match self.0 {
            CellValue::Bytes(_) => Format::Binary,
            CellValue::Null | CellValue::Text(_) => Format::Text,
        }
    }

    tokio_postgres::types::to_sql_checked!();
}

pub fn to_params(values: &[CellValue]) -> Vec<SqlParam<'_>> {
    values.iter().map(SqlParam).collect()
}

pub fn as_sql_params<'a>(params: &'a [SqlParam<'a>]) -> Vec<&'a (dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(value: &CellValue) -> (IsNull, Vec<u8>, Format) {
        let param = SqlParam(value);
        let mut buf = BytesMut::new();
        let is_null = param.to_sql(&Type::TEXT, &mut buf).expect("encode");
        let format = param.encode_format(&Type::TEXT);
        (is_null, buf.to_vec(), format)
    }

    #[test]
    fn null_writes_nothing_and_reports_is_null() {
        let (is_null, bytes, _) = encode(&CellValue::Null);
        assert!(matches!(is_null, IsNull::Yes));
        assert!(bytes.is_empty());
    }

    #[test]
    fn text_writes_utf8_bytes_in_text_format() {
        let (is_null, bytes, format) = encode(&CellValue::Text("hello ' ; --".to_string()));
        assert!(matches!(is_null, IsNull::No));
        assert_eq!(bytes, b"hello ' ; --");
        assert!(matches!(format, Format::Text));
    }

    #[test]
    fn bytes_writes_raw_bytes_in_binary_format() {
        let (is_null, bytes, format) = encode(&CellValue::Bytes(vec![0, 1, 2, 255]));
        assert!(matches!(is_null, IsNull::No));
        assert_eq!(bytes, vec![0, 1, 2, 255]);
        assert!(matches!(format, Format::Binary));
    }

    #[test]
    fn accepts_every_type() {
        assert!(SqlParam::accepts(&Type::INT4));
        assert!(SqlParam::accepts(&Type::UUID));
        assert!(SqlParam::accepts(&Type::UNKNOWN));
    }
}
