use crate::errors::{PwbError, PwbResult};
use crate::types::{
    DecisionKind, EpochMillis, PolicyId, QueryId, QueryWalProfile, ReasonCode, ScopeHash,
    ScopeKind, StatementClass, WalBytes,
};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PwbSharedState {
    pub(super) magic: u32,
    pub(super) layout_version: u32,
    pub(super) shmem_capacity: u32,
    pub(super) recent_decision_head: u64,
    pub(super) recent_decision_count: u32,
    pub(super) profiles_len: u32,
    pub(super) budget_buckets_len: u32,
    pub(super) profile_restore_state: u8,
    pub(super) _profile_restore_padding: [u8; 7],
    pub(super) profile_restore_started_epoch_ms: EpochMillis,
    pub(super) last_profile_persist_epoch_ms: EpochMillis,
    pub(super) profile_dirty_count: u64,
    pub(super) counters: PwbCounters,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PwbCounters {
    pub(crate) accepted_statements: u64,
    pub(crate) rejected_statements: u64,
    pub(crate) shadow_would_reject_count: u64,
    pub(crate) predicted_wal_bytes: u64,
    pub(crate) actual_wal_bytes: u64,
    pub(crate) absolute_prediction_error: u64,
    pub(crate) scope_debt_bytes: u64,
    pub(crate) missing_actual_wal_count: u64,
    pub(crate) internal_fail_open_count: u64,
    pub(crate) aborted_after_charge_count: u64,
}

impl PwbCounters {
    pub(super) const fn saturating_add_delta(&mut self, delta: CounterDelta) {
        self.accepted_statements = self
            .accepted_statements
            .saturating_add(delta.accepted_statements);
        self.rejected_statements = self
            .rejected_statements
            .saturating_add(delta.rejected_statements);
        self.shadow_would_reject_count = self
            .shadow_would_reject_count
            .saturating_add(delta.shadow_would_reject_count);
        self.predicted_wal_bytes = self
            .predicted_wal_bytes
            .saturating_add(delta.predicted_wal_bytes);
        self.actual_wal_bytes = self.actual_wal_bytes.saturating_add(delta.actual_wal_bytes);
        self.absolute_prediction_error = self
            .absolute_prediction_error
            .saturating_add(delta.absolute_prediction_error);
        self.scope_debt_bytes = self.scope_debt_bytes.saturating_add(delta.scope_debt_bytes);
        self.missing_actual_wal_count = self
            .missing_actual_wal_count
            .saturating_add(delta.missing_actual_wal_count);
        self.internal_fail_open_count = self
            .internal_fail_open_count
            .saturating_add(delta.internal_fail_open_count);
        self.aborted_after_charge_count = self
            .aborted_after_charge_count
            .saturating_add(delta.aborted_after_charge_count);
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PwbRecentDecision {
    pub(super) timestamp_epoch_ms: EpochMillis,
    pub(super) decision_kind: u8,
    pub(super) reason_code: u8,
    pub(super) scope_kind: u8,
    pub(super) statement_class: u8,
    pub(super) has_policy_id: u8,
    pub(super) has_query_id: u8,
    pub(super) has_actual_wal_bytes: u8,
    pub(super) _padding: u8,
    pub(super) policy_id: PolicyId,
    pub(super) query_id: QueryId,
    pub(super) scope_hash: ScopeHash,
    pub(super) predicted_wal_bytes: WalBytes,
    pub(super) actual_wal_bytes: WalBytes,
    pub(super) available_before: WalBytes,
    pub(super) available_after: WalBytes,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PwbProfileEntry {
    pub(super) occupied: u8,
    pub(super) has_scope_hash: u8,
    pub(super) _padding: [u8; 6],
    pub(super) scope_hash: ScopeHash,
    pub(super) query_id: QueryId,
    pub(super) profile: PwbQueryWalProfile,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PwbQueryWalProfile {
    pub(super) calls: u64,
    pub(super) ewma_wal_bytes: WalBytes,
    pub(super) max_wal_bytes: WalBytes,
    pub(super) last_seen_epoch_ms: EpochMillis,
}

impl From<QueryWalProfile> for PwbQueryWalProfile {
    fn from(profile: QueryWalProfile) -> Self {
        Self {
            calls: profile.calls,
            ewma_wal_bytes: profile.ewma_wal_bytes,
            max_wal_bytes: profile.max_wal_bytes,
            last_seen_epoch_ms: profile.last_seen_epoch_ms,
        }
    }
}

impl From<PwbQueryWalProfile> for QueryWalProfile {
    fn from(profile: PwbQueryWalProfile) -> Self {
        Self {
            calls: profile.calls,
            ewma_wal_bytes: profile.ewma_wal_bytes,
            max_wal_bytes: profile.max_wal_bytes,
            last_seen_epoch_ms: profile.last_seen_epoch_ms,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PwbBudgetBucket {
    pub(super) occupied: u8,
    pub(super) _padding: [u8; 3],
    pub(super) policy_id: PolicyId,
    pub(super) scope_hash: ScopeHash,
    pub(super) available_bytes: WalBytes,
    pub(super) max_burst_bytes: WalBytes,
    pub(super) rate_bytes_per_sec: WalBytes,
    pub(super) last_refill_epoch_ms: EpochMillis,
    pub(super) debt_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BudgetBucketState {
    pub(crate) policy_id: PolicyId,
    pub(crate) scope_hash: ScopeHash,
    pub(crate) available_bytes: WalBytes,
    pub(crate) max_burst_bytes: WalBytes,
    pub(crate) rate_bytes_per_sec: WalBytes,
    pub(crate) last_refill_epoch_ms: EpochMillis,
    pub(crate) debt_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryProfileSnapshot {
    pub(crate) scope_hash: Option<ScopeHash>,
    pub(crate) query_id: QueryId,
    pub(crate) profile: QueryWalProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BudgetBucketSnapshot {
    pub(crate) policy_id: PolicyId,
    pub(crate) scope_hash: ScopeHash,
    pub(crate) available_bytes: WalBytes,
    pub(crate) max_burst_bytes: WalBytes,
    pub(crate) rate_bytes_per_sec: WalBytes,
    pub(crate) last_refill_epoch_ms: EpochMillis,
    pub(crate) debt_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CounterDelta {
    pub(crate) accepted_statements: u64,
    pub(crate) rejected_statements: u64,
    pub(crate) shadow_would_reject_count: u64,
    pub(crate) predicted_wal_bytes: u64,
    pub(crate) actual_wal_bytes: u64,
    pub(crate) absolute_prediction_error: u64,
    pub(crate) scope_debt_bytes: u64,
    pub(crate) missing_actual_wal_count: u64,
    pub(crate) internal_fail_open_count: u64,
    pub(crate) aborted_after_charge_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecentDecisionRecord {
    pub(crate) timestamp_epoch_ms: EpochMillis,
    pub(crate) decision_kind: DecisionKind,
    pub(crate) policy_id: Option<PolicyId>,
    pub(crate) scope_kind: ScopeKind,
    pub(crate) scope_hash: ScopeHash,
    pub(crate) query_id: Option<QueryId>,
    pub(crate) statement_class: StatementClass,
    pub(crate) predicted_wal_bytes: WalBytes,
    pub(crate) actual_wal_bytes: Option<WalBytes>,
    pub(crate) available_before: WalBytes,
    pub(crate) available_after: WalBytes,
    pub(crate) reason_code: ReasonCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DecodedProfileEntry {
    pub(super) scope_hash: Option<ScopeHash>,
    pub(super) query_id: QueryId,
    pub(super) profile: PwbQueryWalProfile,
}

impl PwbRecentDecision {
    pub(super) fn encode(record: RecentDecisionRecord) -> Self {
        Self {
            timestamp_epoch_ms: record.timestamp_epoch_ms,
            decision_kind: encode_decision_kind(record.decision_kind),
            reason_code: encode_reason_code(record.reason_code),
            scope_kind: encode_scope_kind(record.scope_kind),
            statement_class: encode_statement_class(record.statement_class),
            has_policy_id: u8::from(record.policy_id.is_some()),
            has_query_id: u8::from(record.query_id.is_some()),
            has_actual_wal_bytes: u8::from(record.actual_wal_bytes.is_some()),
            _padding: 0,
            policy_id: record.policy_id.unwrap_or_default(),
            query_id: record.query_id.unwrap_or_default(),
            scope_hash: record.scope_hash,
            predicted_wal_bytes: record.predicted_wal_bytes,
            actual_wal_bytes: record.actual_wal_bytes.unwrap_or_default(),
            available_before: record.available_before,
            available_after: record.available_after,
        }
    }

    pub(super) fn decode(self) -> PwbResult<RecentDecisionRecord> {
        Ok(RecentDecisionRecord {
            timestamp_epoch_ms: self.timestamp_epoch_ms,
            decision_kind: decode_decision_kind(self.decision_kind)?,
            policy_id: decode_optional(self.has_policy_id, self.policy_id)?,
            scope_kind: decode_scope_kind(self.scope_kind)?,
            scope_hash: self.scope_hash,
            query_id: decode_optional(self.has_query_id, self.query_id)?,
            statement_class: decode_statement_class(self.statement_class)?,
            predicted_wal_bytes: self.predicted_wal_bytes,
            actual_wal_bytes: decode_optional(self.has_actual_wal_bytes, self.actual_wal_bytes)?,
            available_before: self.available_before,
            available_after: self.available_after,
            reason_code: decode_reason_code(self.reason_code)?,
        })
    }
}

impl PwbProfileEntry {
    pub(super) const fn encode(
        scope_hash: Option<ScopeHash>,
        query_id: QueryId,
        profile: PwbQueryWalProfile,
    ) -> Self {
        Self {
            occupied: 1,
            has_scope_hash: if scope_hash.is_some() { 1 } else { 0 },
            _padding: [0; 6],
            scope_hash: match scope_hash {
                Some(scope_hash) => scope_hash,
                None => 0,
            },
            query_id,
            profile,
        }
    }

    pub(super) fn decode(self) -> PwbResult<DecodedProfileEntry> {
        if self.occupied != 1 {
            return Err(PwbError::Internal {
                message: format!("invalid profile occupied flag: {}", self.occupied),
            });
        }

        Ok(DecodedProfileEntry {
            scope_hash: decode_optional(self.has_scope_hash, self.scope_hash)?,
            query_id: self.query_id,
            profile: self.profile,
        })
    }
}

impl PwbBudgetBucket {
    pub(super) const fn encode(bucket: BudgetBucketState) -> Self {
        Self {
            occupied: 1,
            _padding: [0; 3],
            policy_id: bucket.policy_id,
            scope_hash: bucket.scope_hash,
            available_bytes: bucket.available_bytes,
            max_burst_bytes: bucket.max_burst_bytes,
            rate_bytes_per_sec: bucket.rate_bytes_per_sec,
            last_refill_epoch_ms: bucket.last_refill_epoch_ms,
            debt_bytes: bucket.debt_bytes,
        }
    }

    pub(super) fn decode(self) -> PwbResult<BudgetBucketState> {
        if self.occupied != 1 {
            return Err(PwbError::Internal {
                message: format!("invalid budget bucket occupied flag: {}", self.occupied),
            });
        }

        Ok(self.state())
    }

    pub(super) const fn state(self) -> BudgetBucketState {
        // Callers use this only after slot lookup or decode's occupied check.
        BudgetBucketState {
            policy_id: self.policy_id,
            scope_hash: self.scope_hash,
            available_bytes: self.available_bytes,
            max_burst_bytes: self.max_burst_bytes,
            rate_bytes_per_sec: self.rate_bytes_per_sec,
            last_refill_epoch_ms: self.last_refill_epoch_ms,
            debt_bytes: self.debt_bytes,
        }
    }
}

fn decode_optional<T: Copy>(flag: u8, value: T) -> PwbResult<Option<T>> {
    match flag {
        0 => Ok(None),
        1 => Ok(Some(value)),
        _ => Err(PwbError::Internal {
            message: format!("invalid optional field flag in shared memory: {flag}"),
        }),
    }
}

const fn encode_decision_kind(kind: DecisionKind) -> u8 {
    match kind {
        DecisionKind::Allowed => 1,
        DecisionKind::WouldReject => 2,
        DecisionKind::Rejected => 3,
        DecisionKind::NoMatchingPolicy => 4,
        DecisionKind::MissingScope => 5,
        DecisionKind::InternalErrorFailOpen => 6,
    }
}

fn decode_decision_kind(value: u8) -> PwbResult<DecisionKind> {
    match value {
        1 => Ok(DecisionKind::Allowed),
        2 => Ok(DecisionKind::WouldReject),
        3 => Ok(DecisionKind::Rejected),
        4 => Ok(DecisionKind::NoMatchingPolicy),
        5 => Ok(DecisionKind::MissingScope),
        6 => Ok(DecisionKind::InternalErrorFailOpen),
        _ => invalid_enum("decision_kind", value),
    }
}

const fn encode_reason_code(code: ReasonCode) -> u8 {
    match code {
        ReasonCode::PolicyDisabled => 1,
        ReasonCode::PolicyMatched => 2,
        ReasonCode::BudgetAvailable => 3,
        ReasonCode::BudgetExceeded => 4,
        ReasonCode::ObserveMode => 5,
        ReasonCode::ShadowMode => 6,
        ReasonCode::NoMatchingPolicy => 7,
        ReasonCode::MissingScope => 8,
        ReasonCode::PredictionUnavailable => 9,
        ReasonCode::InternalErrorFailOpen => 10,
    }
}

fn decode_reason_code(value: u8) -> PwbResult<ReasonCode> {
    match value {
        1 => Ok(ReasonCode::PolicyDisabled),
        2 => Ok(ReasonCode::PolicyMatched),
        3 => Ok(ReasonCode::BudgetAvailable),
        4 => Ok(ReasonCode::BudgetExceeded),
        5 => Ok(ReasonCode::ObserveMode),
        6 => Ok(ReasonCode::ShadowMode),
        7 => Ok(ReasonCode::NoMatchingPolicy),
        8 => Ok(ReasonCode::MissingScope),
        9 => Ok(ReasonCode::PredictionUnavailable),
        10 => Ok(ReasonCode::InternalErrorFailOpen),
        _ => invalid_enum("reason_code", value),
    }
}

const fn encode_scope_kind(kind: ScopeKind) -> u8 {
    match kind {
        ScopeKind::Database => 1,
        ScopeKind::Role => 2,
        ScopeKind::Application => 3,
        ScopeKind::Tenant => 4,
        ScopeKind::Composite => 5,
    }
}

fn decode_scope_kind(value: u8) -> PwbResult<ScopeKind> {
    match value {
        1 => Ok(ScopeKind::Database),
        2 => Ok(ScopeKind::Role),
        3 => Ok(ScopeKind::Application),
        4 => Ok(ScopeKind::Tenant),
        5 => Ok(ScopeKind::Composite),
        _ => invalid_enum("scope_kind", value),
    }
}

const fn encode_statement_class(class: StatementClass) -> u8 {
    match class {
        StatementClass::ReadOnly => 1,
        StatementClass::Write => 2,
        StatementClass::Utility => 3,
        StatementClass::Copy => 4,
        StatementClass::Unknown => 5,
    }
}

fn decode_statement_class(value: u8) -> PwbResult<StatementClass> {
    match value {
        1 => Ok(StatementClass::ReadOnly),
        2 => Ok(StatementClass::Write),
        3 => Ok(StatementClass::Utility),
        4 => Ok(StatementClass::Copy),
        5 => Ok(StatementClass::Unknown),
        _ => invalid_enum("statement_class", value),
    }
}

fn invalid_enum<T>(field: &'static str, value: u8) -> PwbResult<T> {
    Err(PwbError::Internal {
        message: format!("invalid {field} enum value in shared memory: {value}"),
    })
}

#[cfg(test)]
pub(super) fn test_state(shmem_capacity: u32) -> PwbSharedState {
    PwbSharedState {
        magic: super::MAGIC,
        layout_version: super::LAYOUT_VERSION,
        shmem_capacity,
        recent_decision_head: 0,
        recent_decision_count: 0,
        profiles_len: 0,
        budget_buckets_len: 0,
        profile_restore_state: super::profiles::initial_profile_restore_state(),
        _profile_restore_padding: [0; 7],
        profile_restore_started_epoch_ms: 0,
        last_profile_persist_epoch_ms: 0,
        profile_dirty_count: 0,
        counters: PwbCounters::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNSET_ENUM: u8 = 0;

    #[test]
    fn encodes_and_decodes_recent_decision() {
        let record = RecentDecisionRecord {
            timestamp_epoch_ms: 123,
            decision_kind: DecisionKind::WouldReject,
            policy_id: Some(7),
            scope_kind: ScopeKind::Tenant,
            scope_hash: 99,
            query_id: Some(42),
            statement_class: StatementClass::Write,
            predicted_wal_bytes: 2048,
            actual_wal_bytes: Some(1024),
            available_before: 4096,
            available_after: 2048,
            reason_code: ReasonCode::BudgetExceeded,
        };

        let encoded = PwbRecentDecision::encode(record);
        let decoded = encoded.decode().unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(decoded, record);
    }

    #[test]
    fn encodes_and_decodes_budget_bucket() {
        let bucket = BudgetBucketState {
            policy_id: 7,
            scope_hash: 99,
            available_bytes: 1024,
            max_burst_bytes: 4096,
            rate_bytes_per_sec: 512,
            last_refill_epoch_ms: 123,
            debt_bytes: 64,
        };

        let encoded = PwbBudgetBucket::encode(bucket);
        let decoded = encoded.decode().unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(decoded, bucket);
    }

    #[test]
    fn encodes_and_decodes_scoped_and_global_profiles() {
        let scoped =
            PwbProfileEntry::encode(Some(99), 42, PwbQueryWalProfile::from(profile(100, 1)));
        let global = PwbProfileEntry::encode(None, 42, PwbQueryWalProfile::from(profile(200, 2)));

        let decoded_scoped = scoped.decode().unwrap_or_else(|error| panic!("{error}"));
        let decoded_global = global.decode().unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(decoded_scoped.scope_hash, Some(99));
        assert_eq!(decoded_scoped.query_id, 42);
        assert_eq!(decoded_scoped.profile.ewma_wal_bytes, 100);
        assert_eq!(decoded_global.scope_hash, None);
        assert_eq!(decoded_global.query_id, 42);
        assert_eq!(decoded_global.profile.ewma_wal_bytes, 200);
    }

    #[test]
    fn rejects_invalid_enum_decode() {
        let mut encoded = PwbRecentDecision::encode(RecentDecisionRecord {
            timestamp_epoch_ms: 123,
            decision_kind: DecisionKind::Allowed,
            policy_id: None,
            scope_kind: ScopeKind::Database,
            scope_hash: 99,
            query_id: None,
            statement_class: StatementClass::ReadOnly,
            predicted_wal_bytes: 0,
            actual_wal_bytes: None,
            available_before: 0,
            available_after: 0,
            reason_code: ReasonCode::BudgetAvailable,
        });
        encoded.decision_kind = UNSET_ENUM;

        assert!(encoded.decode().is_err());
    }

    const fn profile(ewma_wal_bytes: WalBytes, last_seen_epoch_ms: EpochMillis) -> QueryWalProfile {
        QueryWalProfile {
            calls: 1,
            ewma_wal_bytes,
            max_wal_bytes: ewma_wal_bytes,
            last_seen_epoch_ms,
        }
    }
}
