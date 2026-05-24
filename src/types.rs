use crate::errors::{PwbError, PwbResult};

pub(crate) type WalBytes = u64;
pub(crate) type QueryId = u64;
pub(crate) type ScopeHash = u64;
pub(crate) type PolicyId = i32;
pub(crate) type EpochMillis = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BudgetMode {
    Off,
    Observe,
    Shadow,
    Reject,
}

impl BudgetMode {
    pub(crate) const fn as_sql_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::Shadow => "shadow",
            Self::Reject => "reject",
        }
    }

    pub(crate) fn parse_sql(input: &str) -> PwbResult<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "observe" => Ok(Self::Observe),
            "shadow" => Ok(Self::Shadow),
            "reject" => Ok(Self::Reject),
            _ => Err(PwbError::InvalidBudgetMode {
                value: input.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ScopeKind {
    Database,
    Role,
    Application,
    Tenant,
    Composite,
}

impl ScopeKind {
    pub(crate) const fn as_sql_str(self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::Role => "role",
            Self::Application => "application",
            Self::Tenant => "tenant",
            Self::Composite => "composite",
        }
    }

    pub(crate) fn parse_sql(input: &str) -> PwbResult<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "database" => Ok(Self::Database),
            "role" => Ok(Self::Role),
            "application" => Ok(Self::Application),
            "tenant" => Ok(Self::Tenant),
            "composite" => Ok(Self::Composite),
            _ => Err(PwbError::InvalidScopeKind {
                value: input.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DecisionKind {
    Allowed,
    WouldReject,
    Rejected,
    NoMatchingPolicy,
    MissingScope,
    InternalErrorFailOpen,
}

impl DecisionKind {
    pub(crate) const fn as_sql_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::WouldReject => "would_reject",
            Self::Rejected => "rejected",
            Self::NoMatchingPolicy => "no_matching_policy",
            Self::MissingScope => "missing_scope",
            Self::InternalErrorFailOpen => "internal_error_fail_open",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StatementClass {
    ReadOnly,
    Write,
    Utility,
    Copy,
    Unknown,
}

impl StatementClass {
    pub(crate) const fn as_sql_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Write => "write",
            Self::Utility => "utility",
            Self::Copy => "copy",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ReasonCode {
    PolicyDisabled,
    PolicyMatched,
    BudgetAvailable,
    BudgetExceeded,
    ObserveMode,
    ShadowMode,
    NoMatchingPolicy,
    MissingScope,
    PredictionUnavailable,
    InternalErrorFailOpen,
}

impl ReasonCode {
    pub(crate) const fn as_sql_str(self) -> &'static str {
        match self {
            Self::PolicyDisabled => "policy_disabled",
            Self::PolicyMatched => "policy_matched",
            Self::BudgetAvailable => "budget_available",
            Self::BudgetExceeded => "budget_exceeded",
            Self::ObserveMode => "observe_mode",
            Self::ShadowMode => "shadow_mode",
            Self::NoMatchingPolicy => "no_matching_policy",
            Self::MissingScope => "missing_scope",
            Self::PredictionUnavailable => "prediction_unavailable",
            Self::InternalErrorFailOpen => "internal_error_fail_open",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ScopeKey {
    pub(crate) kind: ScopeKind,
    pub(crate) value_hash: ScopeHash,
    pub(crate) debug_value: Option<String>,
}

impl ScopeKey {
    pub(crate) fn with_debug_value(
        kind: ScopeKind,
        value_hash: ScopeHash,
        debug_value: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            value_hash,
            debug_value: Some(debug_value.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdmissionContext {
    pub(crate) query_id: Option<QueryId>,
    pub(crate) scope: ScopeKey,
    pub(crate) statement_class: StatementClass,
    pub(crate) predicted_wal_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdmissionDecision {
    pub(crate) kind: DecisionKind,
    pub(crate) policy_id: Option<PolicyId>,
    pub(crate) charged_bytes: WalBytes,
    pub(crate) available_before: WalBytes,
    pub(crate) available_after: WalBytes,
    pub(crate) reason_code: ReasonCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveStatementState {
    pub(crate) decision: AdmissionDecision,
    pub(crate) start_wal_bytes: Option<WalBytes>,
    pub(crate) measurement_kind: WalMeasurementKind,
    pub(crate) query_id: Option<QueryId>,
    pub(crate) scope_kind: ScopeKind,
    pub(crate) scope_hash: ScopeHash,
    pub(crate) statement_class: StatementClass,
    pub(crate) predicted_wal_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalMeasurementKind {
    /// Exact per-backend WAL accounting from the backend-local WAL usage accumulator.
    ExactBackend,
    Unavailable,
}

impl AdmissionDecision {
    pub(crate) const fn allowed(
        policy_id: Option<PolicyId>,
        charged_bytes: WalBytes,
        reason_code: ReasonCode,
    ) -> Self {
        Self {
            kind: DecisionKind::Allowed,
            policy_id,
            charged_bytes,
            available_before: 0,
            available_after: 0,
            reason_code,
        }
    }

    pub(crate) const fn would_reject(policy_id: PolicyId, charged_bytes: WalBytes) -> Self {
        Self {
            kind: DecisionKind::WouldReject,
            policy_id: Some(policy_id),
            charged_bytes,
            available_before: 0,
            available_after: 0,
            reason_code: ReasonCode::BudgetExceeded,
        }
    }

    pub(crate) const fn rejected(policy_id: PolicyId, charged_bytes: WalBytes) -> Self {
        Self {
            kind: DecisionKind::Rejected,
            policy_id: Some(policy_id),
            charged_bytes,
            available_before: 0,
            available_after: 0,
            reason_code: ReasonCode::BudgetExceeded,
        }
    }

    pub(crate) const fn no_matching_policy() -> Self {
        Self {
            kind: DecisionKind::NoMatchingPolicy,
            policy_id: None,
            charged_bytes: 0,
            available_before: 0,
            available_after: 0,
            reason_code: ReasonCode::NoMatchingPolicy,
        }
    }

    pub(crate) const fn internal_error_fail_open() -> Self {
        Self {
            kind: DecisionKind::InternalErrorFailOpen,
            policy_id: None,
            charged_bytes: 0,
            available_before: 0,
            available_after: 0,
            reason_code: ReasonCode::InternalErrorFailOpen,
        }
    }

    pub(crate) const fn with_availability(
        mut self,
        available_before: WalBytes,
        available_after: WalBytes,
    ) -> Self {
        self.available_before = available_before;
        self.available_after = available_after;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryWalProfile {
    pub(crate) calls: u64,
    pub(crate) ewma_wal_bytes: WalBytes,
    pub(crate) max_wal_bytes: WalBytes,
    pub(crate) last_seen_epoch_ms: EpochMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProfileEwmaWeights {
    pub(crate) numerator: u64,
    pub(crate) denominator: u64,
}

impl ProfileEwmaWeights {
    pub(crate) fn new(numerator: u64, denominator: u64) -> PwbResult<Self> {
        if denominator == 0 || numerator == 0 || numerator > denominator {
            return Err(PwbError::Internal {
                message: format!("invalid profile EWMA weights: {numerator}/{denominator}"),
            });
        }

        Ok(Self {
            numerator,
            denominator,
        })
    }
}

impl QueryWalProfile {
    pub(crate) const fn new(first_actual: WalBytes, now_epoch_ms: EpochMillis) -> Self {
        Self {
            calls: 1,
            ewma_wal_bytes: first_actual,
            max_wal_bytes: first_actual,
            last_seen_epoch_ms: now_epoch_ms,
        }
    }

    pub(crate) fn record_observation(
        &mut self,
        actual: WalBytes,
        now_epoch_ms: EpochMillis,
        alpha_numerator: u64,
        alpha_denominator: u64,
    ) {
        debug_assert!(alpha_denominator > 0);
        debug_assert!(alpha_numerator > 0);
        debug_assert!(alpha_numerator <= alpha_denominator);

        // Alpha is validated at configuration/constructor boundaries. This update path is
        // expected to run during statement reconciliation, so it trusts that invariant.
        let old_weight = alpha_denominator - alpha_numerator;
        let weighted_sum = u128::from(self.ewma_wal_bytes) * u128::from(old_weight)
            + u128::from(actual) * u128::from(alpha_numerator);
        let ewma = weighted_sum / u128::from(alpha_denominator);

        self.ewma_wal_bytes = WalBytes::try_from(ewma).map_or(WalBytes::MAX, |value| value);
        self.calls = self.calls.saturating_add(1);
        self.max_wal_bytes = self.max_wal_bytes.max(actual);
        self.last_seen_epoch_ms = now_epoch_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_budget_modes_from_sql_strings() {
        assert_eq!(BudgetMode::parse_sql("off"), Ok(BudgetMode::Off));
        assert_eq!(BudgetMode::parse_sql(" OBSERVE "), Ok(BudgetMode::Observe));
        assert_eq!(BudgetMode::parse_sql("shadow"), Ok(BudgetMode::Shadow));
        assert_eq!(BudgetMode::parse_sql("reject"), Ok(BudgetMode::Reject));

        assert!(matches!(
            BudgetMode::parse_sql("enforce"),
            Err(PwbError::InvalidBudgetMode { value }) if value == "enforce"
        ));
    }

    #[test]
    fn emits_budget_mode_sql_strings() {
        assert_eq!(BudgetMode::Off.as_sql_str(), "off");
        assert_eq!(BudgetMode::Observe.as_sql_str(), "observe");
        assert_eq!(BudgetMode::Shadow.as_sql_str(), "shadow");
        assert_eq!(BudgetMode::Reject.as_sql_str(), "reject");
    }

    #[test]
    fn parses_scope_kinds_from_sql_strings() {
        assert_eq!(ScopeKind::parse_sql("database"), Ok(ScopeKind::Database));
        assert_eq!(ScopeKind::parse_sql("ROLE"), Ok(ScopeKind::Role));
        assert_eq!(
            ScopeKind::parse_sql(" application "),
            Ok(ScopeKind::Application)
        );
        assert_eq!(ScopeKind::parse_sql("tenant"), Ok(ScopeKind::Tenant));
        assert_eq!(ScopeKind::parse_sql("composite"), Ok(ScopeKind::Composite));

        assert!(matches!(
            ScopeKind::parse_sql("user"),
            Err(PwbError::InvalidScopeKind { value }) if value == "user"
        ));
    }

    #[test]
    fn emits_scope_kind_sql_strings() {
        assert_eq!(ScopeKind::Database.as_sql_str(), "database");
        assert_eq!(ScopeKind::Role.as_sql_str(), "role");
        assert_eq!(ScopeKind::Application.as_sql_str(), "application");
        assert_eq!(ScopeKind::Tenant.as_sql_str(), "tenant");
        assert_eq!(ScopeKind::Composite.as_sql_str(), "composite");
    }

    #[test]
    fn updates_query_profile_with_integer_ewma() {
        let mut profile = QueryWalProfile::new(100, 10);

        profile.record_observation(300, 20, 1, 2);

        assert_eq!(profile.calls, 2);
        assert_eq!(profile.ewma_wal_bytes, 200);
        assert_eq!(profile.max_wal_bytes, 300);
        assert_eq!(profile.last_seen_epoch_ms, 20);
    }

    #[test]
    fn updates_query_profile_without_saturating_intermediate_sum() {
        let mut profile = QueryWalProfile::new(WalBytes::MAX - 1, 10);

        profile.record_observation(WalBytes::MAX - 1, 20, 1, 2);

        assert_eq!(profile.ewma_wal_bytes, WalBytes::MAX - 1);
    }
}
