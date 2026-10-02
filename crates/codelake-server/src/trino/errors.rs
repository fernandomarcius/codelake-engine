//! Trino error codes. Clients branch on `errorType` (the Python client raises
//! `TrinoUserError` for `USER_ERROR`) and on `errorName`, so the mapping from
//! `DataFusion` errors follows Trino's `StandardErrorCode`.
//!
//! The mapping looks at the error's structure first (parser, schema and Arrow
//! errors), then at its message through [`MESSAGE_RULES`], a table that grows
//! with each engine message worth a Trino name. A missing table is refined into
//! `CATALOG_NOT_FOUND` / `SCHEMA_NOT_FOUND` by asking a [`Namespace`] which part
//! of the name is missing, as Trino reports it.
//!
//! The Trino function library (`codelake_core::trino_functions`) raises errors
//! whose message starts with the Trino name (`INVALID_FUNCTION_ARGUMENT: …`):
//! that name wins, and the client sees the message after it, as Trino words
//! it. Errors this module would call `GENERIC_INTERNAL_ERROR` take the
//! library's classification ([`trino_error_name`]) when it knows better.

use std::sync::Arc;

use codelake_core::governance::{Failure, GovernanceError, KillError};
use codelake_core::subquery::SubqueryMultipleRows;
use codelake_core::trino_functions::trino_error_name;
use datafusion::arrow::error::ArrowError;
use datafusion::common::SchemaError;
use datafusion::error::DataFusionError;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Code {
    GenericUserError,
    SyntaxError,
    AbandonedQuery,
    UserCanceled,
    PermissionDenied,
    NotFound,
    FunctionNotFound,
    InvalidFunctionArgument,
    DivisionByZero,
    InvalidCastArgument,
    NotSupported,
    InvalidSessionProperty,
    NumericValueOutOfRange,
    QueryTextTooLarge,
    SubqueryMultipleRows,
    ExceededFunctionMemoryLimit,
    CatalogNotFound,
    MissingCatalogName,
    SchemaNotFound,
    TableNotFound,
    ColumnNotFound,
    SchemaAlreadyExists,
    TableAlreadyExists,
    ColumnAlreadyExists,
    TypeMismatch,
    InvalidLiteral,
    AmbiguousName,
    InvalidParameterUsage,
    AdministrativelyKilled,
    InvalidProcedureArgument,
    InvalidArguments,
    QueryQueueFull,
    ExceededTimeLimit,
    ExceededLocalMemoryLimit,
    GenericInternalError,
}

impl Code {
    fn parts(self) -> (i32, &'static str, &'static str) {
        match self {
            Self::GenericUserError => (0, "GENERIC_USER_ERROR", "USER_ERROR"),
            Self::SyntaxError => (1, "SYNTAX_ERROR", "USER_ERROR"),
            Self::AbandonedQuery => (2, "ABANDONED_QUERY", "USER_ERROR"),
            Self::UserCanceled => (3, "USER_CANCELED", "USER_ERROR"),
            Self::PermissionDenied => (4, "PERMISSION_DENIED", "USER_ERROR"),
            Self::NotFound => (5, "NOT_FOUND", "USER_ERROR"),
            Self::FunctionNotFound => (6, "FUNCTION_NOT_FOUND", "USER_ERROR"),
            Self::InvalidFunctionArgument => (7, "INVALID_FUNCTION_ARGUMENT", "USER_ERROR"),
            Self::DivisionByZero => (8, "DIVISION_BY_ZERO", "USER_ERROR"),
            Self::InvalidCastArgument => (9, "INVALID_CAST_ARGUMENT", "USER_ERROR"),
            Self::NotSupported => (13, "NOT_SUPPORTED", "USER_ERROR"),
            Self::InvalidSessionProperty => (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
            Self::NumericValueOutOfRange => (19, "NUMERIC_VALUE_OUT_OF_RANGE", "USER_ERROR"),
            Self::QueryTextTooLarge => (35, "QUERY_TEXT_TOO_LARGE", "USER_ERROR"),
            Self::SubqueryMultipleRows => (28, "SUBQUERY_MULTIPLE_ROWS", "USER_ERROR"),
            Self::ExceededFunctionMemoryLimit => (37, "EXCEEDED_FUNCTION_MEMORY_LIMIT", "USER_ERROR"),
            Self::CatalogNotFound => (44, "CATALOG_NOT_FOUND", "USER_ERROR"),
            Self::MissingCatalogName => (56, "MISSING_CATALOG_NAME", "USER_ERROR"),
            Self::SchemaNotFound => (45, "SCHEMA_NOT_FOUND", "USER_ERROR"),
            Self::TableNotFound => (46, "TABLE_NOT_FOUND", "USER_ERROR"),
            Self::ColumnNotFound => (47, "COLUMN_NOT_FOUND", "USER_ERROR"),
            Self::SchemaAlreadyExists => (49, "SCHEMA_ALREADY_EXISTS", "USER_ERROR"),
            Self::TableAlreadyExists => (50, "TABLE_ALREADY_EXISTS", "USER_ERROR"),
            Self::ColumnAlreadyExists => (51, "COLUMN_ALREADY_EXISTS", "USER_ERROR"),
            Self::TypeMismatch => (58, "TYPE_MISMATCH", "USER_ERROR"),
            Self::InvalidLiteral => (59, "INVALID_LITERAL", "USER_ERROR"),
            Self::AmbiguousName => (62, "AMBIGUOUS_NAME", "USER_ERROR"),
            Self::InvalidParameterUsage => (80, "INVALID_PARAMETER_USAGE", "USER_ERROR"),
            Self::InvalidProcedureArgument => (30, "INVALID_PROCEDURE_ARGUMENT", "USER_ERROR"),
            Self::InvalidArguments => (76, "INVALID_ARGUMENTS", "USER_ERROR"),
            Self::AdministrativelyKilled => (38, "ADMINISTRATIVELY_KILLED", "USER_ERROR"),
            Self::GenericInternalError => (65_536, "GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR"),
            Self::QueryQueueFull => (131_074, "QUERY_QUEUE_FULL", "INSUFFICIENT_RESOURCES"),
            Self::ExceededTimeLimit => (131_075, "EXCEEDED_TIME_LIMIT", "INSUFFICIENT_RESOURCES"),
            Self::ExceededLocalMemoryLimit => {
                (131_079, "EXCEEDED_LOCAL_MEMORY_LIMIT", "INSUFFICIENT_RESOURCES")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ErrorLocation {
    line_number: u32,
    column_number: u32,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct FailureInfo {
    #[serde(rename = "type")]
    kind: String,
    message: String,
    suppressed: Vec<FailureInfo>,
    stack: Vec<String>,
}

/// The `error` object of a Trino `QueryResults`, boxed to keep `Result`s small.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct QueryError(Box<ErrorBody>);

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    message: String,
    error_code: i32,
    error_name: &'static str,
    error_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_location: Option<ErrorLocation>,
    failure_info: FailureInfo,
}

impl QueryError {
    pub(crate) fn new(code: Code, message: impl Into<String>) -> Self {
        let (error_code, error_name, error_type) = code.parts();
        let message = message.into();
        Self(Box::new(ErrorBody {
            failure_info: FailureInfo {
                kind: "io.trino.spi.TrinoException".into(),
                message: message.clone(),
                suppressed: Vec::new(),
                stack: Vec::new(),
            },
            message,
            error_code,
            error_name,
            error_type,
            error_location: None,
        }))
    }

    pub(crate) fn name(&self) -> &'static str {
        self.0.error_name
    }

    pub(crate) fn kind(&self) -> &'static str {
        self.0.error_type
    }

    pub(crate) fn message(&self) -> &str {
        &self.0.message
    }

    /// This error as the query registry records it, keeping the error itself
    /// so the protocol answers with it again.
    pub(crate) fn failure(&self) -> Failure {
        Failure::new(self.name(), self.kind(), self.message()).with_detail(Arc::new(self.clone()))
    }

    /// The error of a failure recorded in the registry: the protocol's own
    /// error when this protocol recorded it, else one built from its name
    /// (governance failures, a query failed elsewhere).
    pub(crate) fn from_failure(failure: &Failure) -> Self {
        if let Some(error) = failure.detail::<Self>() {
            return error.clone();
        }
        let code = match failure.name.as_str() {
            "QUERY_QUEUE_FULL" => Code::QueryQueueFull,
            "EXCEEDED_TIME_LIMIT" => Code::ExceededTimeLimit,
            "USER_CANCELED" => Code::UserCanceled,
            "ADMINISTRATIVELY_KILLED" => Code::AdministrativelyKilled,
            "ABANDONED_QUERY" => Code::AbandonedQuery,
            name => named(name).unwrap_or(if failure.kind == "USER_ERROR" {
                Code::GenericUserError
            } else {
                Code::GenericInternalError
            }),
        };
        Self::new(code, failure.message.clone())
    }

    pub(crate) fn from_governance(error: &GovernanceError) -> Self {
        Self::from_failure(&error.failure())
    }

    /// A `kill_query` call that failed.
    pub(crate) fn from_kill(error: &KillError) -> Self {
        let code = match error {
            KillError::InvalidArguments(_) => Code::InvalidArguments,
            KillError::NullQueryId => Code::InvalidProcedureArgument,
            KillError::NotText(_) => Code::TypeMismatch,
            KillError::NotFound(_) => Code::NotFound,
            KillError::NotRunning(_) => Code::NotSupported,
            KillError::NotOwner(_) => Code::PermissionDenied,
        };
        Self::new(code, error.to_string())
    }

    #[cfg(test)]
    pub(crate) fn location(&self) -> Option<(u32, u32)> {
        self.0.error_location.map(|l| (l.line_number, l.column_number))
    }

    fn at(mut self, location: Option<ErrorLocation>) -> Self {
        self.0.error_location = location;
        self
    }

    /// Maps an engine error on `sql` as Trino reports it: the missing part of a
    /// table name, and the position of syntax and semantic errors (semantic
    /// messages start with `line L:C: `, like Trino's analyzer).
    pub(crate) fn from_engine(error: &DataFusionError, sql: &str, names: &dyn Namespace) -> Self {
        let root = error.find_root();
        if let DataFusionError::SQL(..) = root {
            let text = root.to_string();
            let location = parser_location(&text).or_else(|| text.contains("EOF").then(|| end_of(sql)));
            return Self::new(Code::SyntaxError, text).at(location);
        }
        // Trino 431 reports the overflow of a decimal `sum` as an internal
        // error, with the overflow's own message (`Decimal overflow`).
        if codelake_core::overflow::is_trino_internal_error(error) {
            let message = match root {
                DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
                    ArrowError::ArithmeticOverflow(message) => message.clone(),
                    _ => raw_message(root),
                },
                _ => raw_message(root),
            };
            return Self::new(Code::GenericInternalError, message);
        }
        let (code, message) = library_error(root).unwrap_or_else(|| {
            let code = match code_of(root) {
                Code::GenericInternalError => {
                    named(trino_error_name(error)).unwrap_or(Code::GenericInternalError)
                }
                code => code,
            };
            (code, raw_message(root))
        });
        let (code, message) = match code {
            Code::TableNotFound => missing_table(&message, names).unwrap_or((code, message)),
            _ => (code, message),
        };
        let location = error
            .diagnostic()
            .and_then(|d| d.span)
            .filter(|span| span.start.line > 0 && span.start.column > 0)
            .map(|span| ErrorLocation {
                line_number: u32::try_from(span.start.line).unwrap_or(u32::MAX),
                column_number: u32::try_from(span.start.column).unwrap_or(u32::MAX),
            });
        let message = match location {
            Some(l) => format!("line {}:{}: {message}", l.line_number, l.column_number),
            None => message,
        };
        Self::new(code, message).at(location)
    }
}

/// Which catalogs and schemas exist, to name the missing part of a table name.
pub(crate) trait Namespace {
    fn has_catalog(&self, catalog: &str) -> bool;
    fn has_schema(&self, catalog: &str, schema: &str) -> bool;
}

/// A namespace where every catalog and schema exists: a missing table stays
/// `TABLE_NOT_FOUND`.
pub(crate) struct Everything;

impl Namespace for Everything {
    fn has_catalog(&self, _: &str) -> bool {
        true
    }
    fn has_schema(&self, _: &str, _: &str) -> bool {
        true
    }
}

/// `DataFusion` names a missing table by its resolved name:
/// `table 'catalog.schema.table' not found`.
fn missing_table(message: &str, names: &dyn Namespace) -> Option<(Code, String)> {
    let name = message.strip_prefix("table '")?.strip_suffix("' not found")?;
    let mut parts = name.splitn(3, '.');
    let (catalog, schema, table) = (parts.next()?, parts.next()?, parts.next()?);
    Some(if !names.has_catalog(catalog) {
        (
            Code::CatalogNotFound,
            format!("Catalog '{catalog}' does not exist"),
        )
    } else if !names.has_schema(catalog, schema) {
        (Code::SchemaNotFound, format!("Schema '{schema}' does not exist"))
    } else {
        (
            Code::TableNotFound,
            format!("Table '{catalog}.{schema}.{table}' does not exist"),
        )
    })
}

/// Position just after the last character, where Trino reports "found EOF".
fn end_of(sql: &str) -> ErrorLocation {
    let line = sql.matches('\n').count() + 1;
    let last = sql.rsplit('\n').next().unwrap_or_default();
    ErrorLocation {
        line_number: u32::try_from(line).unwrap_or(u32::MAX),
        column_number: u32::try_from(last.chars().count() + 1).unwrap_or(u32::MAX),
    }
}

/// `sqlparser` reports positions as "… at Line: 1, Column: 8".
fn parser_location(message: &str) -> Option<ErrorLocation> {
    let (_, tail) = message.rsplit_once("Line: ")?;
    let (line, tail) = tail.split_once(", Column: ")?;
    let column: String = tail.chars().take_while(char::is_ascii_digit).collect();
    let location = ErrorLocation {
        line_number: line.trim().parse().ok()?,
        column_number: column.parse().ok()?,
    };
    (location.line_number > 0).then_some(location)
}

/// The code for a Trino error name, for the names the function library uses.
fn named(name: &str) -> Option<Code> {
    Some(match name {
        "GENERIC_USER_ERROR" => Code::GenericUserError,
        "SYNTAX_ERROR" => Code::SyntaxError,
        "FUNCTION_NOT_FOUND" => Code::FunctionNotFound,
        "INVALID_FUNCTION_ARGUMENT" => Code::InvalidFunctionArgument,
        "DIVISION_BY_ZERO" => Code::DivisionByZero,
        "INVALID_CAST_ARGUMENT" => Code::InvalidCastArgument,
        "NOT_SUPPORTED" => Code::NotSupported,
        "NUMERIC_VALUE_OUT_OF_RANGE" => Code::NumericValueOutOfRange,
        "EXCEEDED_FUNCTION_MEMORY_LIMIT" => Code::ExceededFunctionMemoryLimit,
        "COLUMN_NOT_FOUND" => Code::ColumnNotFound,
        "TYPE_MISMATCH" => Code::TypeMismatch,
        "INVALID_LITERAL" => Code::InvalidLiteral,
        "GENERIC_INTERNAL_ERROR" => Code::GenericInternalError,
        "EXCEEDED_LOCAL_MEMORY_LIMIT" => Code::ExceededLocalMemoryLimit,
        _ => return None,
    })
}

/// An error of the Trino function library: `NAME: message`, with `NAME` a
/// Trino error name. Returns the code and the message after the name.
fn library_error(error: &DataFusionError) -> Option<(Code, String)> {
    let message = match error {
        DataFusionError::Plan(message)
        | DataFusionError::Execution(message)
        | DataFusionError::NotImplemented(message) => message.clone(),
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            ArrowError::ComputeError(message) | ArrowError::InvalidArgumentError(message) => message.clone(),
            _ => return None,
        },
        DataFusionError::External(inner) => inner.to_string(),
        _ => return None,
    };
    let (name, rest) = message.split_once(": ")?;
    let code = named(name)?;
    Some((code, rest.to_owned()))
}

/// The message without `DataFusion`'s variant prefix ("Error during planning: ").
fn raw_message(error: &DataFusionError) -> String {
    match error {
        DataFusionError::Plan(message)
        | DataFusionError::Execution(message)
        | DataFusionError::NotImplemented(message)
        | DataFusionError::ResourcesExhausted(message) => message.clone(),
        other => other.to_string(),
    }
}

/// How a message rule matches the lower-cased message.
#[derive(Debug, Clone, Copy)]
enum Pattern {
    StartsWith(&'static str),
    Contains(&'static str),
    /// Starts with the first text and contains the second.
    Both(&'static str, &'static str),
}

impl Pattern {
    fn matches(self, message: &str) -> bool {
        match self {
            Self::StartsWith(prefix) => message.starts_with(prefix),
            Self::Contains(part) => message.contains(part),
            Self::Both(prefix, part) => message.starts_with(prefix) && message.contains(part),
        }
    }
}

/// Engine messages with a Trino name, checked in order on planning and
/// execution errors (`DataFusion`'s in-memory catalog and the connectors'
/// writable catalogs word them slightly differently).
const MESSAGE_RULES: &[(Pattern, Code)] = &[
    (
        Pattern::Both("table ", "already exists"),
        Code::TableAlreadyExists,
    ),
    (Pattern::Both("view ", "already exists"), Code::TableAlreadyExists),
    (
        Pattern::Both("schema ", "already exists"),
        Code::SchemaAlreadyExists,
    ),
    (
        Pattern::Both("column ", "already exists"),
        Code::ColumnAlreadyExists,
    ),
    (Pattern::Both("table '", "' not found"), Code::TableNotFound),
    (Pattern::Both("table ", "doesn't exist"), Code::TableNotFound),
    (Pattern::Both("table ", "does not exist"), Code::TableNotFound),
    (Pattern::Both("view ", "doesn't exist"), Code::TableNotFound),
    (Pattern::Both("view ", "does not exist"), Code::TableNotFound),
    (Pattern::Both("schema ", "doesn't exist"), Code::SchemaNotFound),
    (Pattern::Both("schema ", "does not exist"), Code::SchemaNotFound),
    (
        Pattern::StartsWith("failed to resolve schema"),
        Code::SchemaNotFound,
    ),
    (
        Pattern::StartsWith("failed to resolve catalog"),
        Code::CatalogNotFound,
    ),
    (Pattern::Both("catalog ", "not found"), Code::CatalogNotFound),
    (Pattern::StartsWith("invalid function '"), Code::FunctionNotFound),
    (
        Pattern::Contains("failed to coerce arguments to satisfy a call to"),
        Code::FunctionNotFound,
    ),
    // A decimal product whose scale passes 38 (Trino: `DECIMAL scale must be
    // in range [0, precision (38)]`), a decimal quotient that cannot be
    // rescaled (Trino: `Decimal overflow`).
    (
        Pattern::Both(
            "cannot get result type for decimal operation",
            "would exceed max scale",
        ),
        Code::InvalidFunctionArgument,
    ),
    (
        Pattern::Both("cannot get result type for decimal operation", "overflow"),
        Code::NumericValueOutOfRange,
    ),
    (
        Pattern::StartsWith("cannot coerce arithmetic expression"),
        Code::TypeMismatch,
    ),
    (Pattern::StartsWith("cannot infer common"), Code::TypeMismatch),
    // A type Trino does not have (`timestamp(13)`).
    (Pattern::StartsWith("unknown type: "), Code::TypeMismatch),
    (
        Pattern::StartsWith("cannot get result type for"),
        Code::TypeMismatch,
    ),
];

fn by_message(message: &str) -> Option<Code> {
    let lower = message.to_lowercase();
    MESSAGE_RULES
        .iter()
        .find(|(pattern, _)| pattern.matches(&lower))
        .map(|(_, code)| *code)
}

/// Arrow's cast errors: a number that does not fit the target numeric type is
/// Trino's `NUMERIC_VALUE_OUT_OF_RANGE`; text that is not a number is
/// `INVALID_CAST_ARGUMENT`.
fn cast_code(message: &str) -> Code {
    if message.starts_with("Can't cast value")
        || message.contains("Overflowing on")
        || message.contains("overflows")
    {
        Code::NumericValueOutOfRange
    } else {
        Code::InvalidCastArgument
    }
}

fn code_of(error: &DataFusionError) -> Code {
    match error {
        DataFusionError::SQL(..) => Code::SyntaxError,
        DataFusionError::SchemaError(schema_error, _) => match schema_error.as_ref() {
            SchemaError::FieldNotFound { .. } => Code::ColumnNotFound,
            SchemaError::AmbiguousReference { .. } => Code::AmbiguousName,
            _ => Code::GenericUserError,
        },
        DataFusionError::Plan(message) => by_message(message).unwrap_or(Code::GenericUserError),
        DataFusionError::Execution(message) => by_message(message).unwrap_or(Code::GenericInternalError),
        DataFusionError::NotImplemented(_) => Code::NotSupported,
        DataFusionError::ResourcesExhausted(_) => Code::ExceededLocalMemoryLimit,
        DataFusionError::External(error) if error.is::<SubqueryMultipleRows>() => Code::SubqueryMultipleRows,
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            ArrowError::DivideByZero => Code::DivisionByZero,
            ArrowError::ArithmeticOverflow(_) => Code::NumericValueOutOfRange,
            ArrowError::CastError(message) => cast_code(message),
            ArrowError::ParseError(_) => Code::InvalidCastArgument,
            // A decimal cast to a precision too small for the value.
            ArrowError::InvalidArgumentError(message)
                if message.contains("too large to store in a Decimal") =>
            {
                Code::InvalidCastArgument
            }
            _ => Code::GenericInternalError,
        },
        _ => Code::GenericInternalError,
    }
}

impl From<&DataFusionError> for QueryError {
    fn from(error: &DataFusionError) -> Self {
        Self::from_engine(error, "", &Everything)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Codes and names of the reference engine's `kill_query` (431).
    #[test]
    fn kill_errors_use_the_reference_codes() {
        let wire = |error: KillError| {
            let value = serde_json::to_value(QueryError::from_kill(&error)).unwrap();
            (
                value["errorCode"].as_i64().unwrap(),
                value["errorName"].as_str().unwrap().to_owned(),
                value["message"].as_str().unwrap().to_owned(),
            )
        };
        assert_eq!(
            wire(KillError::NotFound("q".into())),
            (5, "NOT_FOUND".into(), "Target query not found: q".into())
        );
        assert_eq!(
            wire(KillError::NotRunning("q".into())),
            (
                13,
                "NOT_SUPPORTED".into(),
                "Target query is not running: q".into()
            )
        );
        assert_eq!(
            wire(KillError::NullQueryId),
            (
                30,
                "INVALID_PROCEDURE_ARGUMENT".into(),
                "query_id cannot be null".into()
            )
        );
        assert_eq!(wire(KillError::InvalidArguments("x".into())).0, 76);
        assert_eq!(wire(KillError::NotText("x".into())).1, "TYPE_MISMATCH");
        assert_eq!(wire(KillError::NotOwner("ana".into())).0, 4);
        let killed = QueryError::from_failure(&Failure::new("ADMINISTRATIVELY_KILLED", "USER_ERROR", "m"));
        let killed = serde_json::to_value(killed).unwrap();
        assert_eq!(
            (killed["errorCode"].as_i64(), killed["errorType"].as_str()),
            (Some(38), Some("USER_ERROR"))
        );
    }

    #[test]
    fn failures_round_trip_through_the_registry() {
        let error = QueryError::new(Code::SyntaxError, "bad").at(Some(ErrorLocation {
            line_number: 1,
            column_number: 2,
        }));
        let failure = error.failure();
        assert_eq!(failure.name, "SYNTAX_ERROR");
        assert_eq!(QueryError::from_failure(&failure), error, "the detail is kept");
        let queue = QueryError::from_failure(&Failure::new(
            "QUERY_QUEUE_FULL",
            "INSUFFICIENT_RESOURCES",
            "full",
        ));
        assert_eq!(
            (queue.name(), queue.kind(), queue.message()),
            ("QUERY_QUEUE_FULL", "INSUFFICIENT_RESOURCES", "full")
        );
        let time =
            QueryError::from_governance(&GovernanceError::TimeLimit(std::time::Duration::from_secs(2)));
        assert_eq!(time.name(), "EXCEEDED_TIME_LIMIT");
        assert_eq!(time.message(), "Query exceeded maximum time limit of 2.00s");
        let other = QueryError::from_failure(&Failure::new("SOMETHING", "USER_ERROR", "x"));
        assert_eq!(other.name(), "GENERIC_USER_ERROR");
    }

    fn map(sql: &str) -> QueryError {
        let error = datafusion::sql::parser::DFParser::parse_sql(sql).unwrap_err();
        QueryError::from_engine(&error, sql, &Everything)
    }

    #[test]
    fn syntax_errors_carry_their_location() {
        let error = map("SELECT a FROM t ORDER x");
        assert_eq!(error.name(), "SYNTAX_ERROR");
        assert_eq!(error.kind(), "USER_ERROR");
        assert_eq!(error.location().unwrap().0, 1);
    }

    #[test]
    fn end_of_input_is_located_after_the_last_character() {
        assert_eq!(map("SELECT 1 +").location(), Some((1, 11)));
        assert_eq!(map("SELECT 1\n  FROM").location().map(|l| l.0), Some(2));
        assert_eq!(
            end_of("SELECT ação +"),
            ErrorLocation {
                line_number: 1,
                column_number: 14
            }
        );
    }

    /// Errors of the function library keep their Trino name, and the client
    /// sees Trino's message without it.
    #[test]
    fn function_library_errors_keep_their_trino_name() {
        let cases = [
            (
                DataFusionError::Execution("INVALID_FUNCTION_ARGUMENT: Invalid format: \"x\"".into()),
                "INVALID_FUNCTION_ARGUMENT",
                7,
                "Invalid format: \"x\"",
            ),
            (
                DataFusionError::Context(
                    "while running".into(),
                    Box::new(DataFusionError::Execution(
                        "NUMERIC_VALUE_OUT_OF_RANGE: Value 300 exceeds MAX_BYTE".into(),
                    )),
                ),
                "NUMERIC_VALUE_OUT_OF_RANGE",
                19,
                "Value 300 exceeds MAX_BYTE",
            ),
            (
                DataFusionError::Plan(
                    "FUNCTION_NOT_FOUND: Unexpected parameters (varchar) for function abs".into(),
                ),
                "FUNCTION_NOT_FOUND",
                6,
                "Unexpected parameters (varchar) for function abs",
            ),
            (
                DataFusionError::from(ArrowError::ComputeError("INVALID_LITERAL: bad".into())),
                "INVALID_LITERAL",
                59,
                "bad",
            ),
            (
                DataFusionError::Execution("EXCEEDED_FUNCTION_MEMORY_LIMIT: too big".into()),
                "EXCEEDED_FUNCTION_MEMORY_LIMIT",
                37,
                "too big",
            ),
        ];
        for (error, name, code, message) in cases {
            let mapped = QueryError::from(&error);
            assert_eq!(mapped.name(), name, "{error}");
            assert_eq!(mapped.kind(), "USER_ERROR", "{error}");
            assert_eq!(mapped.0.error_code, code, "{error}");
            assert_eq!(mapped.message(), message, "{error}");
        }
        // A message that only looks like it: not a Trino name.
        let other = QueryError::from(&DataFusionError::Execution("boom: x".into()));
        assert_eq!(other.name(), "GENERIC_INTERNAL_ERROR");
        assert_eq!(other.message(), "boom: x");
        // What this module calls internal, the library may classify.
        let decimal = QueryError::from(&DataFusionError::from(ArrowError::InvalidArgumentError(
            "12345 is too large to store in a Decimal128 of precision 3. Max is 999".into(),
        )));
        assert_eq!(decimal.name(), "INVALID_CAST_ARGUMENT");
    }

    #[test]
    fn datafusion_errors_map_to_trino_codes() {
        let cases = [
            (
                DataFusionError::Plan("table 'a.b.c' not found".into()),
                "TABLE_NOT_FOUND",
            ),
            (DataFusionError::NotImplemented("x".into()), "NOT_SUPPORTED"),
            (
                DataFusionError::ResourcesExhausted("x".into()),
                "EXCEEDED_LOCAL_MEMORY_LIMIT",
            ),
            (
                DataFusionError::from(ArrowError::DivideByZero),
                "DIVISION_BY_ZERO",
            ),
            (
                DataFusionError::Execution("boom".into()),
                "GENERIC_INTERNAL_ERROR",
            ),
            (
                DataFusionError::Context(
                    "ctx".into(),
                    Box::new(DataFusionError::Plan("table 't' not found".into())),
                ),
                "TABLE_NOT_FOUND",
            ),
            (
                DataFusionError::Execution("Table 'datafusion.s.t' already exists".into()),
                "TABLE_ALREADY_EXISTS",
            ),
            (
                DataFusionError::Plan("table hive.s.t already exists".into()),
                "TABLE_ALREADY_EXISTS",
            ),
            (
                DataFusionError::Execution("Schema 's' already exists".into()),
                "SCHEMA_ALREADY_EXISTS",
            ),
            (
                DataFusionError::Plan("schema s already exists".into()),
                "SCHEMA_ALREADY_EXISTS",
            ),
            (
                DataFusionError::Execution("Table 'datafusion.s.nope' doesn't exist.".into()),
                "TABLE_NOT_FOUND",
            ),
            (
                DataFusionError::Plan("table hive.s.t does not exist".into()),
                "TABLE_NOT_FOUND",
            ),
            (
                DataFusionError::Execution("Schema 'x' doesn't exist.".into()),
                "SCHEMA_NOT_FOUND",
            ),
            (
                DataFusionError::Plan("Invalid function 'nofn'.\nDid you mean 'nvl'?".into()),
                "FUNCTION_NOT_FOUND",
            ),
            (
                DataFusionError::Plan(
                    "Cannot coerce arithmetic expression Int64 + Utf8 to valid types".into(),
                ),
                "TYPE_MISMATCH",
            ),
            (
                DataFusionError::from(ArrowError::CastError("Can't cast value 300 to type Int8".into())),
                "NUMERIC_VALUE_OUT_OF_RANGE",
            ),
            (
                DataFusionError::from(ArrowError::CastError(
                    "Cannot cast string 'abc' to value of Int32 type".into(),
                )),
                "INVALID_CAST_ARGUMENT",
            ),
            (
                DataFusionError::from(ArrowError::ArithmeticOverflow("Int64 + Int64".into())),
                "NUMERIC_VALUE_OUT_OF_RANGE",
            ),
            (
                DataFusionError::SchemaError(
                    Box::new(SchemaError::AmbiguousReference {
                        field: Box::new(datafusion::common::Column::from_name("a")),
                    }),
                    Box::new(None),
                ),
                "AMBIGUOUS_NAME",
            ),
        ];
        for (error, name) in cases {
            assert_eq!(QueryError::from(&error).name(), name, "{error}");
        }
    }

    #[test]
    fn decimal_errors_map_to_trino_codes() {
        let cases = [
            // Trino 431: `decimal(5,2) * decimal(38,38)`, `decimal(5,2) /
            // decimal(38,38)` and a narrowing decimal cast.
            (
                DataFusionError::Plan(
                    "Cannot get result type for decimal operation Decimal128(5, 2) * Decimal128(38, 38): \
                     Invalid argument error: Output scale of Decimal128(5, 2) * Decimal128(38, 38) would \
                     exceed max scale of 38"
                        .into(),
                ),
                "INVALID_FUNCTION_ARGUMENT",
            ),
            (
                DataFusionError::Plan(
                    "Cannot get result type for decimal operation Decimal128(5, 2) / Decimal128(38, 38): \
                     Arithmetic overflow: Overflow happened on: 10 ^ 42"
                        .into(),
                ),
                "NUMERIC_VALUE_OUT_OF_RANGE",
            ),
            (
                DataFusionError::from(ArrowError::InvalidArgumentError(
                    "777.4 is too large to store in a Decimal128 of precision 3. Max is 99.9".into(),
                )),
                "INVALID_CAST_ARGUMENT",
            ),
        ];
        for (error, name) in cases {
            assert_eq!(QueryError::from(&error).name(), name, "{error}");
        }
    }

    struct Only(&'static str, &'static str);

    impl Namespace for Only {
        fn has_catalog(&self, catalog: &str) -> bool {
            catalog == self.0
        }
        fn has_schema(&self, catalog: &str, schema: &str) -> bool {
            catalog == self.0 && schema == self.1
        }
    }

    #[test]
    fn missing_tables_name_the_missing_part() {
        let names = Only("datafusion", "public");
        let error = |name: &str| {
            QueryError::from_engine(
                &DataFusionError::Plan(format!("table '{name}' not found")),
                "",
                &names,
            )
        };
        assert_eq!(error("nocat.s.t").name(), "CATALOG_NOT_FOUND");
        assert_eq!(error("datafusion.nope.t").name(), "SCHEMA_NOT_FOUND");
        let table = error("datafusion.public.t");
        assert_eq!(table.name(), "TABLE_NOT_FOUND");
        assert_eq!(table.message(), "Table 'datafusion.public.t' does not exist");
    }

    #[test]
    fn serializes_like_trino() {
        let json = serde_json::to_value(QueryError::new(Code::UserCanceled, "Query was canceled")).unwrap();
        assert_eq!(json["errorCode"], 3);
        assert_eq!(json["errorName"], "USER_CANCELED");
        assert_eq!(json["errorType"], "USER_ERROR");
        assert_eq!(json["failureInfo"]["type"], "io.trino.spi.TrinoException");
        assert!(json.get("errorLocation").is_none());
    }
}
