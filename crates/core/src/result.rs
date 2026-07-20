use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::schema::ColumnInfo;
use crate::value::CellValue;

/// The result of any `execute*` call on a `DatabaseDriver`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub column_type_names: Vec<String>,
    pub rows: Vec<Vec<CellValue>>,
    pub rows_affected: u64,
    pub execution_time: Duration,
    /// True when the row count was capped below what the query would
    /// otherwise have returned (see `RowLimits::EMERGENCY_MAX` and
    /// `DatabaseDriver::execute_user_query`).
    pub is_truncated: bool,
    pub status_message: Option<String>,
    pub column_meta: Option<Vec<ColumnInfo>>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            column_type_names: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
            execution_time: Duration::default(),
            is_truncated: false,
            status_message: None,
            column_meta: None,
        }
    }
}

impl Default for QueryResult {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json() {
        let mut result = QueryResult::empty();
        result.columns.push("id".to_string());
        result.column_type_names.push("int4".to_string());
        result.rows.push(vec![CellValue::Text("1".to_string())]);
        result.rows_affected = 1;

        let json = serde_json::to_string(&result).expect("serialize");
        let back: QueryResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.columns, result.columns);
        assert_eq!(back.rows, result.rows);
    }
}
