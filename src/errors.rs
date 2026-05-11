#![allow(dead_code)]

use core::fmt;

use crate::types::{PolicyId, StatementClass, WalBytes};

pub(crate) const WAL_BUDGET_EXCEEDED_SQLSTATE: &str = "P0001";
pub(crate) const INVALID_PARAMETER_SQLSTATE: &str = "22023";
pub(crate) const INTERNAL_ERROR_SQLSTATE: &str = "XX000";

pub(crate) type PwbResult<T> = Result<T, PwbError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PwbError {
    InvalidBudgetMode {
        value: String,
    },
    InvalidScopeKind {
        value: String,
    },
    InvalidPolicyValue {
        field: &'static str,
        value: String,
        reason: &'static str,
    },
    InsufficientPrivilege {
        operation: &'static str,
    },
    MissingScope,
    PredictionUnavailable {
        statement_class: StatementClass,
    },
    BudgetExceeded {
        policy_id: PolicyId,
        predicted_wal_bytes: WalBytes,
        available_wal_bytes: WalBytes,
    },
    Internal {
        message: String,
    },
}

impl PwbError {
    pub(crate) const fn sqlstate(&self) -> &'static str {
        match self {
            Self::InvalidBudgetMode { .. }
            | Self::InvalidScopeKind { .. }
            | Self::InvalidPolicyValue { .. } => INVALID_PARAMETER_SQLSTATE,
            Self::InsufficientPrivilege { .. } => "42501",
            Self::MissingScope
            | Self::PredictionUnavailable { .. }
            | Self::BudgetExceeded { .. } => WAL_BUDGET_EXCEEDED_SQLSTATE,
            Self::Internal { .. } => INTERNAL_ERROR_SQLSTATE,
        }
    }

    pub(crate) const fn message(&self) -> &'static str {
        match self {
            Self::InvalidBudgetMode { .. }
            | Self::InvalidScopeKind { .. }
            | Self::InvalidPolicyValue { .. } => "invalid pg_wal_budget policy value",
            Self::InsufficientPrivilege { .. } => {
                "insufficient privilege for pg_wal_budget operation"
            }
            Self::MissingScope => "pg_wal_budget could not determine statement scope",
            Self::PredictionUnavailable { .. } => {
                "pg_wal_budget could not predict statement WAL usage"
            }
            Self::BudgetExceeded { .. } => "pg_wal_budget rejected statement: WAL budget exceeded",
            Self::Internal { .. } => "pg_wal_budget internal error",
        }
    }
}

impl fmt::Display for PwbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBudgetMode { value } => {
                write!(formatter, "invalid budget mode: {value}")
            }
            Self::InvalidScopeKind { value } => {
                write!(formatter, "invalid scope kind: {value}")
            }
            Self::InvalidPolicyValue {
                field,
                value,
                reason,
            } => write!(
                formatter,
                "invalid policy value for {field}: {value} ({reason})"
            ),
            Self::InsufficientPrivilege { operation } => {
                write!(formatter, "insufficient privilege to {operation}")
            }
            Self::MissingScope => formatter.write_str("missing scope"),
            Self::PredictionUnavailable { statement_class } => write!(
                formatter,
                "prediction unavailable for statement class {}",
                statement_class.as_sql_str()
            ),
            Self::BudgetExceeded {
                policy_id,
                predicted_wal_bytes,
                available_wal_bytes,
            } => write!(
                formatter,
                "WAL budget exceeded for policy {policy_id}: predicted {predicted_wal_bytes} bytes, available {available_wal_bytes} bytes"
            ),
            Self::Internal { message } => write!(formatter, "internal error: {message}"),
        }
    }
}

impl std::error::Error for PwbError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_stable_postgres_error_contract() {
        let error = PwbError::BudgetExceeded {
            policy_id: 7,
            predicted_wal_bytes: 1024,
            available_wal_bytes: 512,
        };

        assert_eq!(error.sqlstate(), WAL_BUDGET_EXCEEDED_SQLSTATE);
        assert_eq!(
            error.message(),
            "pg_wal_budget rejected statement: WAL budget exceeded"
        );
    }

    #[test]
    fn invalid_policy_values_use_invalid_parameter_sqlstate() {
        let error = PwbError::InvalidBudgetMode {
            value: "enforce".to_string(),
        };

        assert_eq!(error.sqlstate(), INVALID_PARAMETER_SQLSTATE);
        assert_eq!(error.message(), "invalid pg_wal_budget policy value");
    }

    #[test]
    fn insufficient_privilege_uses_privilege_sqlstate() {
        let error = PwbError::InsufficientPrivilege {
            operation: "set trusted tenant scope",
        };

        assert_eq!(error.sqlstate(), "42501");
        assert_eq!(
            error.message(),
            "insufficient privilege for pg_wal_budget operation"
        );
    }
}
