use crate::errors::{PwbError, PwbResult};
use crate::shmem::{self, BudgetBucketState};
use crate::time;
use crate::types::{
    AdmissionContext, AdmissionDecision, BudgetMode, EpochMillis, PolicyId, ReasonCode, ScopeHash,
    WalBytes,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectivePolicy {
    pub(crate) policy_id: PolicyId,
    pub(crate) enabled: bool,
    pub(crate) mode: BudgetMode,
    pub(crate) wal_rate_bytes_per_sec: WalBytes,
    pub(crate) wal_burst_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RefillResult {
    available_bytes: WalBytes,
    last_refill_epoch_ms: EpochMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueueWait {
    policy_id: PolicyId,
    predicted_wal_bytes: WalBytes,
    available_wal_bytes: WalBytes,
    wait_ms: EpochMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChargeAttempt {
    Admitted(AdmissionDecision),
    WouldWait(QueueWait),
}

pub(crate) fn admit_statement(
    context: &AdmissionContext,
    policy: &EffectivePolicy,
    now_epoch_ms: EpochMillis,
) -> PwbResult<AdmissionDecision> {
    if !policy.enabled || matches!(policy.mode, BudgetMode::Off) {
        return Ok(non_charging_admission(
            policy.policy_id,
            ReasonCode::PolicyDisabled,
        ));
    }

    if matches!(policy.mode, BudgetMode::Observe) {
        return Ok(non_charging_admission(
            policy.policy_id,
            ReasonCode::ObserveMode,
        ));
    }

    match policy.mode {
        BudgetMode::Shadow => admit_shadow_statement(context, policy, now_epoch_ms),
        BudgetMode::Reject => shmem::with_budget_bucket(
            policy.policy_id,
            context.scope.value_hash,
            || initial_bucket_state(policy, context.scope.value_hash, now_epoch_ms),
            |bucket| admit_with_bucket(context.predicted_wal_bytes, policy, now_epoch_ms, bucket),
        ),
        BudgetMode::Queue => admit_queue_statement(context, policy, now_epoch_ms),
        BudgetMode::Off | BudgetMode::Observe => unreachable!("handled before bucket admission"),
    }
}

pub(crate) fn refund_charged_bytes(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    bytes: WalBytes,
) -> PwbResult<()> {
    shmem::with_existing_budget_bucket(policy_id, scope_hash, |bucket| {
        bucket.available_bytes =
            refund_available(bucket.available_bytes, bucket.max_burst_bytes, bytes);
        Ok(())
    })?;
    Ok(())
}

pub(crate) fn record_underprediction_debt(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    bytes: WalBytes,
) -> PwbResult<()> {
    shmem::with_existing_budget_bucket(policy_id, scope_hash, |bucket| {
        bucket.debt_bytes = bucket.debt_bytes.saturating_add(bytes);
        Ok(())
    })?;
    Ok(())
}

const fn initial_bucket_state(
    policy: &EffectivePolicy,
    scope_hash: ScopeHash,
    now_epoch_ms: EpochMillis,
) -> BudgetBucketState {
    BudgetBucketState {
        policy_id: policy.policy_id,
        scope_hash,
        available_bytes: policy.wal_burst_bytes,
        max_burst_bytes: policy.wal_burst_bytes,
        rate_bytes_per_sec: policy.wal_rate_bytes_per_sec,
        last_refill_epoch_ms: now_epoch_ms,
        debt_bytes: 0,
    }
}

const fn non_charging_admission(policy_id: PolicyId, reason_code: ReasonCode) -> AdmissionDecision {
    AdmissionDecision::allowed(Some(policy_id), 0, reason_code)
}

fn admit_shadow_statement(
    context: &AdmissionContext,
    policy: &EffectivePolicy,
    now_epoch_ms: EpochMillis,
) -> PwbResult<AdmissionDecision> {
    if !shmem::is_available() {
        return admit_shadow_with_ephemeral_bucket(context, policy, now_epoch_ms);
    }

    shmem::with_existing_budget_bucket(policy.policy_id, context.scope.value_hash, |bucket| {
        admit_with_bucket(context.predicted_wal_bytes, policy, now_epoch_ms, bucket)
    })?
    .map_or_else(
        || admit_shadow_with_ephemeral_bucket(context, policy, now_epoch_ms),
        Ok,
    )
}

fn admit_shadow_with_ephemeral_bucket(
    context: &AdmissionContext,
    policy: &EffectivePolicy,
    now_epoch_ms: EpochMillis,
) -> PwbResult<AdmissionDecision> {
    let mut bucket = initial_bucket_state(policy, context.scope.value_hash, now_epoch_ms);
    admit_with_bucket(
        context.predicted_wal_bytes,
        policy,
        now_epoch_ms,
        &mut bucket,
    )
}

fn admit_with_bucket(
    predicted_wal_bytes: WalBytes,
    policy: &EffectivePolicy,
    now_epoch_ms: EpochMillis,
    bucket: &mut BudgetBucketState,
) -> PwbResult<AdmissionDecision> {
    refresh_bucket_policy(bucket, policy);

    let refilled = refill_available_bytes(
        bucket.available_bytes,
        bucket.max_burst_bytes,
        bucket.rate_bytes_per_sec,
        bucket.last_refill_epoch_ms,
        now_epoch_ms,
    );
    bucket.available_bytes = refilled.available_bytes;
    bucket.last_refill_epoch_ms = refilled.last_refill_epoch_ms;

    let available_before = bucket.available_bytes;

    match policy.mode {
        BudgetMode::Shadow => {
            let decision = if can_afford(available_before, predicted_wal_bytes) {
                AdmissionDecision::allowed(Some(policy.policy_id), 0, ReasonCode::ShadowMode)
            } else {
                AdmissionDecision::would_reject(policy.policy_id, predicted_wal_bytes)
            };
            Ok(decision.with_availability(available_before, bucket.available_bytes))
        }
        BudgetMode::Reject => {
            admit_charge_attempt(predicted_wal_bytes, policy, available_before, bucket)
                .and_then(charge_attempt_to_reject_decision)
        }
        BudgetMode::Off | BudgetMode::Observe | BudgetMode::Queue => {
            unreachable!("handled before single-shot bucket admission")
        }
    }
}

fn admit_queue_statement(
    context: &AdmissionContext,
    policy: &EffectivePolicy,
    mut now_epoch_ms: EpochMillis,
) -> PwbResult<AdmissionDecision> {
    if context.predicted_wal_bytes > policy.wal_burst_bytes {
        return Err(PwbError::BudgetExceeded {
            policy_id: policy.policy_id,
            predicted_wal_bytes: context.predicted_wal_bytes,
            available_wal_bytes: policy.wal_burst_bytes,
        });
    }

    loop {
        let attempt = shmem::with_budget_bucket(
            policy.policy_id,
            context.scope.value_hash,
            || initial_bucket_state(policy, context.scope.value_hash, now_epoch_ms),
            |bucket| {
                refresh_and_attempt_charge(
                    context.predicted_wal_bytes,
                    policy,
                    now_epoch_ms,
                    bucket,
                )
            },
        )?;

        match attempt {
            ChargeAttempt::Admitted(decision) => return Ok(decision),
            ChargeAttempt::WouldWait(wait) => {
                time::sleep_ms_interruptible(wait.wait_ms);
                now_epoch_ms = time::current_epoch_ms();
            }
        }
    }
}

fn refresh_and_attempt_charge(
    predicted_wal_bytes: WalBytes,
    policy: &EffectivePolicy,
    now_epoch_ms: EpochMillis,
    bucket: &mut BudgetBucketState,
) -> PwbResult<ChargeAttempt> {
    refresh_bucket_policy(bucket, policy);

    let refilled = refill_available_bytes(
        bucket.available_bytes,
        bucket.max_burst_bytes,
        bucket.rate_bytes_per_sec,
        bucket.last_refill_epoch_ms,
        now_epoch_ms,
    );
    bucket.available_bytes = refilled.available_bytes;
    bucket.last_refill_epoch_ms = refilled.last_refill_epoch_ms;

    admit_charge_attempt(predicted_wal_bytes, policy, bucket.available_bytes, bucket)
}

fn admit_charge_attempt(
    predicted_wal_bytes: WalBytes,
    policy: &EffectivePolicy,
    available_before: WalBytes,
    bucket: &mut BudgetBucketState,
) -> PwbResult<ChargeAttempt> {
    if !can_afford(available_before, predicted_wal_bytes) {
        return Ok(ChargeAttempt::WouldWait(QueueWait {
            policy_id: policy.policy_id,
            predicted_wal_bytes,
            available_wal_bytes: available_before,
            wait_ms: wait_ms_for_deficit(
                predicted_wal_bytes - available_before,
                policy.wal_rate_bytes_per_sec,
            )?,
        }));
    }

    bucket.available_bytes = charge_available(available_before, predicted_wal_bytes);
    Ok(ChargeAttempt::Admitted(
        AdmissionDecision::allowed(
            Some(policy.policy_id),
            predicted_wal_bytes,
            ReasonCode::BudgetAvailable,
        )
        .with_availability(available_before, bucket.available_bytes),
    ))
}

const fn charge_attempt_to_reject_decision(attempt: ChargeAttempt) -> PwbResult<AdmissionDecision> {
    match attempt {
        ChargeAttempt::Admitted(decision) => Ok(decision),
        ChargeAttempt::WouldWait(wait) => Err(PwbError::BudgetExceeded {
            policy_id: wait.policy_id,
            predicted_wal_bytes: wait.predicted_wal_bytes,
            available_wal_bytes: wait.available_wal_bytes,
        }),
    }
}

fn refresh_bucket_policy(bucket: &mut BudgetBucketState, policy: &EffectivePolicy) {
    bucket.max_burst_bytes = policy.wal_burst_bytes;
    bucket.rate_bytes_per_sec = policy.wal_rate_bytes_per_sec;
    bucket.available_bytes = bucket.available_bytes.min(bucket.max_burst_bytes);
}

fn refill_available_bytes(
    available: WalBytes,
    max_burst: WalBytes,
    rate_per_sec: WalBytes,
    last_refill_epoch_ms: EpochMillis,
    now_epoch_ms: EpochMillis,
) -> RefillResult {
    if now_epoch_ms <= last_refill_epoch_ms {
        return RefillResult {
            available_bytes: available,
            last_refill_epoch_ms,
        };
    }

    let elapsed_ms = now_epoch_ms - last_refill_epoch_ms;
    let refill = (u128::from(rate_per_sec) * u128::from(elapsed_ms)) / 1000;
    let refill = WalBytes::try_from(refill).map_or(WalBytes::MAX, |value| value);

    if refill == 0 {
        return RefillResult {
            available_bytes: available,
            last_refill_epoch_ms,
        };
    }

    RefillResult {
        available_bytes: available.saturating_add(refill).min(max_burst),
        last_refill_epoch_ms: now_epoch_ms,
    }
}

const fn can_afford(available: WalBytes, predicted: WalBytes) -> bool {
    predicted <= available
}

const fn charge_available(available: WalBytes, predicted: WalBytes) -> WalBytes {
    available - predicted
}

fn refund_available(available: WalBytes, max_burst: WalBytes, refund: WalBytes) -> WalBytes {
    available.saturating_add(refund).min(max_burst)
}

fn wait_ms_for_deficit(deficit: WalBytes, rate_per_sec: WalBytes) -> PwbResult<EpochMillis> {
    if deficit == 0 {
        return Ok(0);
    }

    if rate_per_sec == 0 {
        return Err(PwbError::Internal {
            message: "queue policy has zero WAL refill rate".to_string(),
        });
    }

    let numerator = u128::from(deficit) * 1000;
    let wait_ms = numerator.div_ceil(u128::from(rate_per_sec));
    Ok(EpochMillis::try_from(wait_ms).unwrap_or(EpochMillis::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ScopeKey, ScopeKind, StatementClass};

    const POLICY_ID: PolicyId = 7;
    const SCOPE_HASH: ScopeHash = 99;

    fn policy(mode: BudgetMode) -> EffectivePolicy {
        EffectivePolicy {
            policy_id: POLICY_ID,
            enabled: true,
            mode,
            wal_rate_bytes_per_sec: 1000,
            wal_burst_bytes: 5000,
        }
    }

    fn context(predicted_wal_bytes: WalBytes) -> AdmissionContext {
        AdmissionContext {
            query_id: None,
            scope: ScopeKey {
                kind: ScopeKind::Tenant,
                value_hash: SCOPE_HASH,
                debug_value: None,
            },
            statement_class: StatementClass::Write,
            predicted_wal_bytes,
        }
    }

    fn bucket(available_bytes: WalBytes) -> BudgetBucketState {
        BudgetBucketState {
            policy_id: POLICY_ID,
            scope_hash: SCOPE_HASH,
            available_bytes,
            max_burst_bytes: 5000,
            rate_bytes_per_sec: 1000,
            last_refill_epoch_ms: 1000,
            debt_bytes: 0,
        }
    }

    #[test]
    fn refill_adds_elapsed_rate_bytes() {
        let result = refill_available_bytes(0, 10_000, 1000, 1000, 3000);

        assert_eq!(result.available_bytes, 2000);
        assert_eq!(result.last_refill_epoch_ms, 3000);
    }

    #[test]
    fn refill_clamps_to_burst() {
        let result = refill_available_bytes(4900, 5000, 1000, 1000, 3000);

        assert_eq!(result.available_bytes, 5000);
        assert_eq!(result.last_refill_epoch_ms, 3000);
    }

    #[test]
    fn refill_ignores_non_monotonic_time() {
        let result = refill_available_bytes(100, 5000, 1000, 3000, 1000);

        assert_eq!(result.available_bytes, 100);
        assert_eq!(result.last_refill_epoch_ms, 3000);
    }

    #[test]
    fn refill_preserves_timestamp_when_fractional_refill_is_zero() {
        let result = refill_available_bytes(100, 5000, 1, 1000, 1500);

        assert_eq!(result.available_bytes, 100);
        assert_eq!(result.last_refill_epoch_ms, 1000);
    }

    #[test]
    fn saturating_refill_does_not_overflow() {
        let result =
            refill_available_bytes(WalBytes::MAX - 1, WalBytes::MAX, WalBytes::MAX, 0, 2000);

        assert_eq!(result.available_bytes, WalBytes::MAX);
        assert_eq!(result.last_refill_epoch_ms, 2000);
    }

    #[test]
    fn charge_subtracts_when_affordable() {
        assert!(can_afford(1000, 400));
        assert_eq!(charge_available(1000, 400), 600);
    }

    #[test]
    fn refund_clamps_to_burst() {
        assert_eq!(refund_available(4500, 5000, 1000), 5000);
    }

    #[test]
    fn disabled_policy_allows_without_charge() {
        let admission = non_charging_admission(POLICY_ID, ReasonCode::PolicyDisabled);

        assert_eq!(
            admission,
            AdmissionDecision::allowed(Some(POLICY_ID), 0, ReasonCode::PolicyDisabled)
        );
    }

    #[test]
    fn shadow_under_budget_allows_without_decrementing() {
        let mut bucket = bucket(3000);
        let admission = admit_with_bucket(2000, &policy(BudgetMode::Shadow), 1000, &mut bucket)
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            admission,
            AdmissionDecision::allowed(Some(POLICY_ID), 0, ReasonCode::ShadowMode)
                .with_availability(3000, 3000)
        );
        assert_eq!(bucket.available_bytes, 3000);
    }

    #[test]
    fn shadow_over_budget_would_reject_without_decrementing() {
        let mut bucket = bucket(1000);
        let admission = admit_with_bucket(2000, &policy(BudgetMode::Shadow), 1000, &mut bucket)
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            admission,
            AdmissionDecision::would_reject(POLICY_ID, 2000).with_availability(1000, 1000)
        );
        assert_eq!(bucket.available_bytes, 1000);
    }

    #[test]
    fn shadow_ephemeral_bucket_uses_full_burst_without_decrementing() {
        let admission =
            admit_shadow_with_ephemeral_bucket(&context(6000), &policy(BudgetMode::Shadow), 1000)
                .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            admission,
            AdmissionDecision::would_reject(POLICY_ID, 6000).with_availability(5000, 5000)
        );
        assert_eq!(admission.available_before, 5000);
        assert_eq!(admission.available_after, 5000);
    }

    #[test]
    fn policy_refresh_clamps_available_to_new_burst() {
        let mut bucket = bucket(5000);
        let mut policy = policy(BudgetMode::Reject);
        policy.wal_burst_bytes = 2000;
        policy.wal_rate_bytes_per_sec = 250;

        refresh_bucket_policy(&mut bucket, &policy);

        assert_eq!(bucket.available_bytes, 2000);
        assert_eq!(bucket.max_burst_bytes, 2000);
        assert_eq!(bucket.rate_bytes_per_sec, 250);
    }

    #[test]
    fn reject_under_budget_charges_prediction() {
        let mut bucket = bucket(3000);
        let admission = admit_with_bucket(2000, &policy(BudgetMode::Reject), 1000, &mut bucket)
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            admission,
            AdmissionDecision::allowed(Some(POLICY_ID), 2000, ReasonCode::BudgetAvailable)
                .with_availability(3000, 1000)
        );
        assert_eq!(bucket.available_bytes, 1000);
        assert_eq!(admission.available_before, 3000);
        assert_eq!(admission.available_after, 1000);
    }

    #[test]
    fn reject_over_budget_returns_error_without_decrementing() {
        let mut bucket = bucket(1000);
        let error = match admit_with_bucket(2000, &policy(BudgetMode::Reject), 1000, &mut bucket) {
            Ok(admission) => panic!("expected budget exceeded, got {admission:?}"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            PwbError::BudgetExceeded {
                policy_id: POLICY_ID,
                predicted_wal_bytes: 2000,
                available_wal_bytes: 1000,
            }
        );
        assert_eq!(bucket.available_bytes, 1000);
    }

    #[test]
    fn queue_under_budget_charges_prediction() {
        let mut bucket = bucket(3000);
        let attempt =
            refresh_and_attempt_charge(2000, &policy(BudgetMode::Queue), 1000, &mut bucket)
                .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            attempt,
            ChargeAttempt::Admitted(
                AdmissionDecision::allowed(Some(POLICY_ID), 2000, ReasonCode::BudgetAvailable)
                    .with_availability(3000, 1000)
            )
        );
        assert_eq!(bucket.available_bytes, 1000);
    }

    #[test]
    fn queue_over_budget_reports_wait_without_decrementing() {
        let mut bucket = bucket(1000);
        let attempt =
            refresh_and_attempt_charge(2000, &policy(BudgetMode::Queue), 1000, &mut bucket)
                .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            attempt,
            ChargeAttempt::WouldWait(QueueWait {
                policy_id: POLICY_ID,
                predicted_wal_bytes: 2000,
                available_wal_bytes: 1000,
                wait_ms: 1000,
            })
        );
        assert_eq!(bucket.available_bytes, 1000);
    }

    #[test]
    fn queue_rejects_predictions_that_exceed_burst_before_bucket_access() {
        let error = match admit_queue_statement(&context(6000), &policy(BudgetMode::Queue), 1000) {
            Ok(admission) => panic!("expected budget exceeded, got {admission:?}"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            PwbError::BudgetExceeded {
                policy_id: POLICY_ID,
                predicted_wal_bytes: 6000,
                available_wal_bytes: 5000,
            }
        );
    }

    #[test]
    fn wait_ms_for_deficit_rounds_up_fractional_milliseconds() {
        let wait_ms = wait_ms_for_deficit(1, 3).unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(wait_ms, 334);
    }

    #[test]
    fn wait_ms_for_deficit_rejects_zero_rate() {
        let error = match wait_ms_for_deficit(1, 0) {
            Ok(wait_ms) => panic!("expected error, got {wait_ms}"),
            Err(error) => error,
        };

        assert!(matches!(error, PwbError::Internal { .. }));
    }
}
