use core::fmt;

use pgrx::{PgLogLevel, PgSqlErrorCode, ereport};

use crate::types::{PolicyId, WalBytes};

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
    pub(crate) const fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::InvalidBudgetMode { .. }
            | Self::InvalidScopeKind { .. }
            | Self::InvalidPolicyValue { .. } => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            Self::InsufficientPrivilege { .. } => PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            Self::MissingScope | Self::BudgetExceeded { .. } => {
                PgSqlErrorCode::ERRCODE_RAISE_EXCEPTION
            }
            Self::Internal { .. } => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
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
            Self::BudgetExceeded { .. } => "pg_wal_budget rejected statement: WAL budget exceeded",
            Self::Internal { .. } => "pg_wal_budget internal error",
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn raise<T>(error: PwbError) -> T {
    let message = error.message();
    let detail = error.to_string();
    let sql_error_code = error.sql_error_code();

    ereport!(
        PgLogLevel::ERROR,
        sql_error_code,
        format!("{message}: {detail}")
    );
    unreachable!("ereport(ERROR) should not return");
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

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_RAISE_EXCEPTION
        );
        assert_eq!(
            error.message(),
            "pg_wal_budget rejected statement: WAL budget exceeded"
        );
    }

    #[test]
    fn invalid_policy_values_use_invalid_parameter_error_code() {
        let error = PwbError::InvalidBudgetMode {
            value: "enforce".to_string(),
        };

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
        );
        assert_eq!(error.message(), "invalid pg_wal_budget policy value");
    }

    #[test]
    fn insufficient_privilege_uses_privilege_error_code() {
        let error = PwbError::InsufficientPrivilege {
            operation: "set trusted tenant scope",
        };

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE
        );
        assert_eq!(
            error.message(),
            "insufficient privilege for pg_wal_budget operation"
        );
    }

    #[test]
    fn internal_errors_use_internal_error_code() {
        let error = PwbError::Internal {
            message: "shared memory unavailable".to_string(),
        };

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
        );
        assert_eq!(error.message(), "pg_wal_budget internal error");
    }
}
