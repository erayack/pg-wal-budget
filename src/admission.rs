use crate::budget;
use crate::errors::PwbError;
use crate::policy;
use crate::shmem::{self, CounterDelta, RecentDecisionRecord};
use crate::time;
use crate::types::{
    ActiveStatementState, AdmissionContext, AdmissionDecision, DecisionKind, EpochMillis, PolicyId,
    QueryId, ScopeHash, ScopeKind, StatementClass, WalBytes, WalMeasurementKind,
};

#[derive(Debug)]
pub(crate) enum AdmissionError {
    Internal {
        error: PwbError,
        context: Option<AdmissionErrorContext>,
    },
    Rejected {
        policy_id: i32,
        predicted_wal_bytes: WalBytes,
        available_wal_bytes: WalBytes,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdmissionErrorContext {
    pub(crate) policy_id: Option<PolicyId>,
    pub(crate) scope_kind: Option<ScopeKind>,
    pub(crate) scope_hash: Option<ScopeHash>,
    pub(crate) query_id: Option<QueryId>,
    pub(crate) statement_class: StatementClass,
    pub(crate) predicted_wal_bytes: Option<WalBytes>,
}

impl AdmissionError {
    pub(crate) const fn internal(error: PwbError) -> Self {
        Self::Internal {
            error,
            context: None,
        }
    }

    pub(crate) const fn internal_with_context(
        error: PwbError,
        context: AdmissionErrorContext,
    ) -> Self {
        Self::Internal {
            error,
            context: Some(context),
        }
    }

    pub(crate) const fn internal_from_admission_context(
        error: PwbError,
        context: &AdmissionContext,
    ) -> Self {
        Self::internal_with_context(
            error,
            AdmissionErrorContext::from_admission_context(context),
        )
    }
}

impl AdmissionErrorContext {
    pub(crate) const fn from_statement(
        query_id: Option<QueryId>,
        statement_class: StatementClass,
    ) -> Self {
        Self {
            policy_id: None,
            scope_kind: None,
            scope_hash: None,
            query_id,
            statement_class,
            predicted_wal_bytes: None,
        }
    }

    pub(crate) const fn from_admission_context(context: &AdmissionContext) -> Self {
        Self {
            policy_id: None,
            scope_kind: Some(context.scope.kind),
            scope_hash: Some(context.scope.value_hash),
            query_id: context.query_id,
            statement_class: context.statement_class,
            predicted_wal_bytes: Some(context.predicted_wal_bytes),
        }
    }

    pub(crate) const fn with_policy_id(mut self, policy_id: PolicyId) -> Self {
        self.policy_id = Some(policy_id);
        self
    }
}

pub(crate) fn admit_context(
    context: &AdmissionContext,
) -> Result<ActiveStatementState, AdmissionError> {
    let now_epoch_ms = time::current_epoch_ms();

    let Some(effective_policy) = policy::effective_policy_for_scope(&context.scope)
        .map_err(|error| AdmissionError::internal_from_admission_context(error, context))?
    else {
        let decision = AdmissionDecision::no_matching_policy();
        record_admission_decision(context, decision, now_epoch_ms);
        return Ok(active_statement_from_context(context, decision));
    };

    match budget::admit_statement(context, &effective_policy, now_epoch_ms) {
        Ok(decision) => {
            record_admission_decision(context, decision, now_epoch_ms);
            Ok(active_statement_from_context(context, decision))
        }
        Err(PwbError::BudgetExceeded {
            policy_id,
            predicted_wal_bytes,
            available_wal_bytes,
        }) => {
            let decision = AdmissionDecision::rejected(policy_id, predicted_wal_bytes)
                .with_availability(available_wal_bytes, available_wal_bytes);
            record_admission_decision(context, decision, now_epoch_ms);
            Err(AdmissionError::Rejected {
                policy_id,
                predicted_wal_bytes,
                available_wal_bytes,
            })
        }
        Err(error) => Err(AdmissionError::internal_with_context(
            error,
            AdmissionErrorContext::from_admission_context(context)
                .with_policy_id(effective_policy.policy_id),
        )),
    }
}

pub(crate) fn record_internal_fail_open(context: Option<AdmissionErrorContext>) {
    let now_epoch_ms = time::current_epoch_ms();
    let decision = AdmissionDecision::internal_error_fail_open();
    let predicted_wal_bytes = context
        .and_then(|context| context.predicted_wal_bytes)
        .unwrap_or(0);
    let _ = shmem::add_counters(counter_delta_for_decision(decision, predicted_wal_bytes));
    let _ = shmem::record_recent_decision(internal_fail_open_recent_decision(
        context,
        decision,
        now_epoch_ms,
    ));
}

pub(crate) fn record_missing_actual_wal() {
    let _ = shmem::add_counters(CounterDelta {
        missing_actual_wal_count: 1,
        ..CounterDelta::default()
    });
}

const fn active_statement_from_context(
    context: &AdmissionContext,
    decision: AdmissionDecision,
) -> ActiveStatementState {
    ActiveStatementState {
        decision,
        start_wal_bytes: None,
        measurement_kind: WalMeasurementKind::Unavailable,
        query_id: context.query_id,
        scope_kind: context.scope.kind,
        scope_hash: context.scope.value_hash,
        statement_class: context.statement_class,
        predicted_wal_bytes: context.predicted_wal_bytes,
    }
}

fn counter_delta_for_decision(
    decision: AdmissionDecision,
    predicted_wal_bytes: WalBytes,
) -> CounterDelta {
    match decision.kind {
        DecisionKind::Allowed | DecisionKind::NoMatchingPolicy | DecisionKind::MissingScope => {
            CounterDelta {
                accepted_statements: 1,
                predicted_wal_bytes,
                ..CounterDelta::default()
            }
        }
        DecisionKind::WouldReject => CounterDelta {
            accepted_statements: 1,
            shadow_would_reject_count: 1,
            predicted_wal_bytes,
            ..CounterDelta::default()
        },
        DecisionKind::Rejected => CounterDelta {
            rejected_statements: 1,
            predicted_wal_bytes,
            ..CounterDelta::default()
        },
        DecisionKind::InternalErrorFailOpen => CounterDelta {
            accepted_statements: 1,
            internal_fail_open_count: 1,
            predicted_wal_bytes,
            ..CounterDelta::default()
        },
    }
}

fn record_admission_decision(
    context: &AdmissionContext,
    decision: AdmissionDecision,
    now_epoch_ms: EpochMillis,
) {
    let delta = counter_delta_for_decision(decision, context.predicted_wal_bytes);
    let recent_decision = RecentDecisionRecord {
        timestamp_epoch_ms: now_epoch_ms,
        decision_kind: decision.kind,
        policy_id: decision.policy_id,
        scope_kind: context.scope.kind,
        scope_hash: context.scope.value_hash,
        query_id: context.query_id,
        statement_class: context.statement_class,
        predicted_wal_bytes: context.predicted_wal_bytes,
        actual_wal_bytes: None,
        available_before: decision.available_before,
        available_after: decision.available_after,
        reason_code: decision.reason_code,
    };
    let _ = shmem::record_admission_telemetry(delta, recent_decision, &context.scope, now_epoch_ms);
}

fn internal_fail_open_recent_decision(
    context: Option<AdmissionErrorContext>,
    decision: AdmissionDecision,
    now_epoch_ms: EpochMillis,
) -> RecentDecisionRecord {
    let scope_kind = context
        .and_then(|context| context.scope_kind)
        .unwrap_or(ScopeKind::Composite);
    let scope_hash = context.and_then(|context| context.scope_hash).unwrap_or(0);
    let query_id = context.and_then(|context| context.query_id);
    let policy_id = context.and_then(|context| context.policy_id);
    let statement_class =
        context.map_or(StatementClass::Unknown, |context| context.statement_class);
    let predicted_wal_bytes = context
        .and_then(|context| context.predicted_wal_bytes)
        .unwrap_or(0);

    RecentDecisionRecord {
        timestamp_epoch_ms: now_epoch_ms,
        decision_kind: decision.kind,
        policy_id,
        scope_kind,
        scope_hash,
        query_id,
        statement_class,
        predicted_wal_bytes,
        actual_wal_bytes: None,
        available_before: 0,
        available_after: 0,
        reason_code: decision.reason_code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ScopeKey;

    #[test]
    fn fail_open_recent_decision_preserves_sentinel_context_when_none_available() {
        let decision = AdmissionDecision::internal_error_fail_open();
        let record = internal_fail_open_recent_decision(None, decision, 123);

        assert_eq!(record.timestamp_epoch_ms, 123);
        assert_eq!(record.decision_kind, DecisionKind::InternalErrorFailOpen);
        assert_eq!(record.reason_code, decision.reason_code);
        assert_eq!(record.policy_id, None);
        assert_eq!(record.scope_kind, ScopeKind::Composite);
        assert_eq!(record.scope_hash, 0);
        assert_eq!(record.query_id, None);
        assert_eq!(record.statement_class, StatementClass::Unknown);
        assert_eq!(record.predicted_wal_bytes, 0);
    }

    #[test]
    fn fail_open_recent_decision_records_statement_context_when_scope_is_missing() {
        let decision = AdmissionDecision::internal_error_fail_open();
        let context = AdmissionErrorContext::from_statement(Some(42), StatementClass::Write);
        let record = internal_fail_open_recent_decision(Some(context), decision, 123);

        assert_eq!(record.scope_kind, ScopeKind::Composite);
        assert_eq!(record.scope_hash, 0);
        assert_eq!(record.policy_id, None);
        assert_eq!(record.query_id, Some(42));
        assert_eq!(record.statement_class, StatementClass::Write);
        assert_eq!(record.predicted_wal_bytes, 0);
    }

    #[test]
    fn fail_open_recent_decision_records_full_admission_context() {
        let decision = AdmissionDecision::internal_error_fail_open();
        let context = AdmissionContext {
            query_id: Some(42),
            scope: ScopeKey {
                kind: ScopeKind::Tenant,
                value_hash: 99,
                debug_value: Some("tenant-a".to_string()),
            },
            statement_class: StatementClass::Write,
            predicted_wal_bytes: 2048,
        };
        let record = internal_fail_open_recent_decision(
            Some(AdmissionErrorContext::from_admission_context(&context).with_policy_id(7)),
            decision,
            123,
        );

        assert_eq!(record.policy_id, Some(7));
        assert_eq!(record.scope_kind, ScopeKind::Tenant);
        assert_eq!(record.scope_hash, 99);
        assert_eq!(record.query_id, Some(42));
        assert_eq!(record.statement_class, StatementClass::Write);
        assert_eq!(record.predicted_wal_bytes, 2048);
    }

    #[test]
    fn fail_open_counter_delta_includes_prediction_when_context_has_prediction() {
        let delta = counter_delta_for_decision(
            AdmissionDecision::internal_error_fail_open(),
            AdmissionErrorContext {
                policy_id: Some(7),
                scope_kind: Some(ScopeKind::Tenant),
                scope_hash: Some(99),
                query_id: Some(42),
                statement_class: StatementClass::Write,
                predicted_wal_bytes: Some(2048),
            }
            .predicted_wal_bytes
            .unwrap_or(0),
        );

        assert_eq!(delta.accepted_statements, 1);
        assert_eq!(delta.internal_fail_open_count, 1);
        assert_eq!(delta.predicted_wal_bytes, 2048);
    }
}
