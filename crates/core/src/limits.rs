/// Hard server-side backstops, independent of any client-requested limit.
///
/// Guardrail #7: enforce this even when a caller asks for more, to
/// prevent an unbounded `SELECT` from exhausting server memory.
pub struct RowLimits;

impl RowLimits {
    /// Absolute maximum rows a single query result may hold in memory,
    /// regardless of what `execute_user_query`'s `row_cap` argument asks
    /// for. Matches the source project's `PluginRowLimits.emergencyMax`.
    pub const EMERGENCY_MAX: usize = 5_000_000;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emergency_max_matches_source_project_constant() {
        assert_eq!(RowLimits::EMERGENCY_MAX, 5_000_000);
    }
}
