//! Killing a query by id from any client protocol:
//! `CALL system.runtime.kill_query(query_id => '…', message => '…')` (or the
//! two arguments by position). Only the query's owner — the user its client
//! named — may kill it (no authentication is assumed). The query fails with
//! `ADMINISTRATIVELY_KILLED`; its streams are dropped by its protocol, which
//! stops its operators and returns their memory to the pool.

use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart, Statement, Value,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::registry::Failure;
use super::{Governance, USER_CANCELED};

/// Stable error name of a killed query.
pub const ADMINISTRATIVELY_KILLED: &str = "ADMINISTRATIVELY_KILLED";

/// The procedure's name, as `catalog.schema.procedure`.
const PROCEDURE: [&str; 3] = ["system", "runtime", "kill_query"];

/// A parsed `kill_query` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KillRequest {
    pub query_id: String,
    pub message: Option<String>,
}

/// Why a query could not be killed or canceled. Names and messages follow
/// the reference engine's procedure (431), except [`KillError::NotOwner`]:
/// its default access control lets anyone kill any query.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KillError {
    /// The call's argument list does not fit the procedure
    /// (`INVALID_ARGUMENTS`).
    #[error("{0}")]
    InvalidArguments(String),
    /// An argument is not a text literal (`TYPE_MISMATCH`).
    #[error("{0}")]
    NotText(String),
    /// `query_id` is `NULL` (`INVALID_PROCEDURE_ARGUMENT`).
    #[error("query_id cannot be null")]
    NullQueryId,
    #[error("Target query not found: {0}")]
    NotFound(String),
    #[error("Target query is not running: {0}")]
    NotRunning(String),
    /// The caller is not the user the query runs as.
    #[error("Access Denied: Cannot kill query owned by {0}")]
    NotOwner(String),
}

/// The `kill_query` call `statement` makes, if it is one.
pub fn kill_request(statement: &Statement) -> Option<Result<KillRequest, KillError>> {
    let Statement::Call(function) = statement else {
        return None;
    };
    let parts: Vec<String> = function
        .name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) if ident.quote_style.is_none() => ident.value.to_lowercase(),
            ObjectNamePart::Identifier(ident) => ident.value.clone(),
            ObjectNamePart::Function(f) => f.name.value.clone(),
        })
        .collect();
    if parts != PROCEDURE {
        return None;
    }
    Some(arguments(&function.args))
}

/// [`kill_request`] of a statement text: `None` unless it is exactly one
/// `kill_query` call.
pub fn kill_request_sql(sql: &str) -> Option<Result<KillRequest, KillError>> {
    // Cheap test first: every other statement skips the parse.
    if !sql.to_ascii_lowercase().contains("kill_query") {
        return None;
    }
    let statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    match statements.as_slice() {
        [statement] => kill_request(statement),
        _ => None,
    }
}

fn invalid(message: impl Into<String>) -> KillError {
    KillError::InvalidArguments(message.into())
}

/// A text literal or `NULL`.
fn text(arg: &FunctionArgExpr) -> Result<Option<String>, KillError> {
    let not_text = || KillError::NotText(format!("kill_query arguments must be text literals, not {arg}"));
    let FunctionArgExpr::Expr(Expr::Value(value)) = arg else {
        return Err(not_text());
    };
    match &value.value {
        Value::SingleQuotedString(text) => Ok(Some(text.clone())),
        Value::Null => Ok(None),
        _ => Err(not_text()),
    }
}

fn arguments(args: &FunctionArguments) -> Result<KillRequest, KillError> {
    let list = match args {
        FunctionArguments::List(list) if list.duplicate_treatment.is_none() && list.clauses.is_empty() => {
            list.args.as_slice()
        }
        FunctionArguments::None => &[],
        _ => return Err(invalid("Invalid arguments for procedure")),
    };
    let mut query_id = None;
    let mut message = None;
    for (position, arg) in list.iter().enumerate() {
        let (name, value) = match arg {
            FunctionArg::Named { name, arg, .. } => (name.value.to_lowercase(), arg),
            FunctionArg::Unnamed(arg) => match position {
                0 => ("query_id".to_owned(), arg),
                1 => ("message".to_owned(), arg),
                _ => return Err(invalid("Too many arguments for procedure")),
            },
            FunctionArg::ExprNamed { name, .. } => {
                return Err(invalid(format!(
                    "Unknown argument name: {}",
                    name.to_string().to_uppercase()
                )));
            }
        };
        let slot = match name.as_str() {
            "query_id" => &mut query_id,
            "message" => &mut message,
            other => {
                return Err(invalid(format!(
                    "Unknown argument name: {}",
                    other.to_uppercase()
                )));
            }
        };
        if slot.is_some() {
            return Err(invalid(format!(
                "Duplicate procedure argument: {}",
                name.to_uppercase()
            )));
        }
        *slot = Some(text(value)?);
    }
    let query_id = query_id
        .ok_or_else(|| invalid("Required procedure argument 'QUERY_ID' is missing"))?
        .ok_or(KillError::NullQueryId)?;
    Ok(KillRequest {
        query_id,
        message: message.flatten(),
    })
}

impl Governance {
    /// Kills the query `request` names, on behalf of `user`, who must own it.
    pub fn kill(&self, request: &KillRequest, user: &str) -> Result<(), KillError> {
        let message = request.message.as_deref().map_or_else(
            || "Query killed. No message provided.".to_owned(),
            |m| format!("Query killed. Message: {m}"),
        );
        self.stop(
            &request.query_id,
            user,
            Failure::new(ADMINISTRATIVELY_KILLED, "USER_ERROR", message),
        )
    }

    /// Cancels the query `id` on behalf of `user`, who must own it
    /// (`USER_CANCELED`), whatever protocol runs it.
    pub fn cancel(&self, id: &str, user: &str) -> Result<(), KillError> {
        self.stop(
            id,
            user,
            Failure::new(USER_CANCELED, "USER_ERROR", "Query was canceled"),
        )
    }

    fn stop(&self, id: &str, user: &str, failure: Failure) -> Result<(), KillError> {
        let record = self
            .registry
            .get(id)
            .ok_or_else(|| KillError::NotFound(id.to_owned()))?;
        let owner = record.user();
        if owner != user {
            return Err(KillError::NotOwner(owner));
        }
        let name = failure.name.clone();
        if !record.fail(failure) {
            return Err(KillError::NotRunning(id.to_owned()));
        }
        tracing::info!(query_id = id, error = %name, "query stopped by its owner");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::{Protocol, QueryState};

    fn request(sql: &str) -> Option<Result<KillRequest, KillError>> {
        kill_request_sql(sql)
    }

    #[test]
    fn calls_are_recognized_by_name_and_arguments() {
        let named = request("CALL system.runtime.kill_query(query_id => 'q1', message => 'too long')");
        assert_eq!(
            named,
            Some(Ok(KillRequest {
                query_id: "q1".into(),
                message: Some("too long".into())
            }))
        );
        let positional = request("call SYSTEM.Runtime.KILL_QUERY('q1');");
        assert_eq!(
            positional,
            Some(Ok(KillRequest {
                query_id: "q1".into(),
                message: None
            }))
        );
        assert_eq!(
            request("CALL system.runtime.kill_query(message => NULL, query_id => 'q1')"),
            Some(Ok(KillRequest {
                query_id: "q1".into(),
                message: None
            }))
        );
        // Other statements, other procedures, several statements.
        for sql in [
            "SELECT 'kill_query'",
            "CALL system.runtime.other('q1')",
            "CALL runtime.kill_query('q1')",
            "CALL \"SYSTEM\".runtime.kill_query('q1')",
            "CALL system.runtime.kill_query('q1'); SELECT 1",
            "SELECT 1",
        ] {
            assert_eq!(request(sql), None, "{sql}");
        }
    }

    #[test]
    fn bad_arguments_are_refused_as_the_reference_engine_words_it() {
        for (sql, error) in [
            (
                "CALL system.runtime.kill_query()",
                invalid("Required procedure argument 'QUERY_ID' is missing"),
            ),
            ("CALL system.runtime.kill_query(NULL)", KillError::NullQueryId),
            (
                "CALL system.runtime.kill_query('a', 'b', 'c')",
                invalid("Too many arguments for procedure"),
            ),
            (
                "CALL system.runtime.kill_query(id => 'a')",
                invalid("Unknown argument name: ID"),
            ),
            (
                "CALL system.runtime.kill_query(query_id => 'a', query_id => 'b')",
                invalid("Duplicate procedure argument: QUERY_ID"),
            ),
            (
                "CALL system.runtime.kill_query(1)",
                KillError::NotText("kill_query arguments must be text literals, not 1".into()),
            ),
        ] {
            assert_eq!(request(sql), Some(Err(error)), "{sql}");
        }
        assert_eq!(KillError::NullQueryId.to_string(), "query_id cannot be null");
    }

    #[test]
    fn only_the_owner_kills_a_running_query() {
        let g = Governance::default();
        let record = g.register(Protocol::Postgres, "ana", None, "SELECT 1");
        let id = record.id().to_owned();
        let kill = |message: Option<&str>| KillRequest {
            query_id: id.clone(),
            message: message.map(str::to_owned),
        };
        assert_eq!(g.kill(&kill(None), "bob"), Err(KillError::NotOwner("ana".into())));
        assert!(!record.cancellation().is_cancelled());
        assert_eq!(
            g.kill(
                &KillRequest {
                    query_id: "nope".into(),
                    message: None
                },
                "ana"
            ),
            Err(KillError::NotFound("nope".into()))
        );
        g.kill(&kill(Some("too long")), "ana").unwrap();
        assert!(record.cancellation().is_cancelled());
        assert_eq!(record.state(), QueryState::Failed);
        let failure = record.failure().unwrap();
        assert_eq!(failure.name, ADMINISTRATIVELY_KILLED);
        assert_eq!(failure.kind, "USER_ERROR");
        assert_eq!(failure.message, "Query killed. Message: too long");
        assert_eq!(g.kill(&kill(None), "ana"), Err(KillError::NotRunning(id.clone())));
    }

    #[test]
    fn cancel_by_id_records_user_canceled() {
        let g = Governance::default();
        let record = g.register(Protocol::Http, "ana", None, "SELECT 1");
        assert_eq!(
            g.cancel(record.id(), "bob"),
            Err(KillError::NotOwner("ana".into()))
        );
        g.cancel(record.id(), "ana").unwrap();
        assert_eq!(record.failure().unwrap().name, USER_CANCELED);
        let killed = g.register(Protocol::Http, "ana", None, "SELECT 1");
        g.kill(
            &KillRequest {
                query_id: killed.id().to_owned(),
                message: None,
            },
            "ana",
        )
        .unwrap();
        assert_eq!(
            killed.failure().unwrap().message,
            "Query killed. No message provided."
        );
    }
}
