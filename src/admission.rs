use crate::budget;
use crate::errors::PwbError;
use crate::policy;
use crate::shmem::{self, CounterDelta, RecentDecisionRecord};
use crate::time;
use crate::types::{
    ActiveStatementState, AdmissionContext, AdmissionDecision, DecisionKind, EpochMillis,
    ScopeKind, StatementClass, WalBytes, WalMeasurementKind,
};

#[derive(Debug)]
pub(crate) enum AdmissionError {
    Internal(PwbError),
    Rejected {
        policy_id: i32,
        predicted_wal_bytes: WalBytes,
        available_wal_bytes: WalBytes,
    },
}

pub(crate) fn admit_context(
    context: &AdmissionContext,
) -> Result<ActiveStatementState, AdmissionError> {
    let now_epoch_ms = time::current_epoch_ms();

    let Some(effective_policy) =
        policy::effective_policy_for_scope(&context.scope).map_err(AdmissionError::Internal)?
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
        Err(error) => Err(AdmissionError::Internal(error)),
    }
}

pub(crate) fn record_internal_fail_open() {
    let now_epoch_ms = time::current_epoch_ms();
    let decision = AdmissionDecision::internal_error_fail_open();
    let _ = shmem::add_counters(counter_delta_for_decision(decision, 0));
    let _ = shmem::record_recent_decision(RecentDecisionRecord {
        timestamp_epoch_ms: now_epoch_ms,
        decision_kind: decision.kind,
        policy_id: decision.policy_id,
        scope_kind: ScopeKind::Composite,
        scope_hash: 0,
        query_id: None,
        statement_class: StatementClass::Unknown,
        predicted_wal_bytes: 0,
        actual_wal_bytes: None,
        available_before: 0,
        available_after: 0,
        reason_code: decision.reason_code,
    });
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
