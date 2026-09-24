//! Trino error codes. Clients branch on `errorType` (the Python client raises
//! `TrinoUserError` for `USER_ERROR`) and on `errorName`, so the mapping from
//! `DataFusion` errors follows Trino's `StandardErrorCode`.

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
    DivisionByZero,
    InvalidCastArgument,
    NotSupported,
    InvalidSessionProperty,
    CatalogNotFound,
    TableNotFound,
    ColumnNotFound,
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
            Self::DivisionByZero => (8, "DIVISION_BY_ZERO", "USER_ERROR"),
            Self::InvalidCastArgument => (9, "INVALID_CAST_ARGUMENT", "USER_ERROR"),
            Self::NotSupported => (13, "NOT_SUPPORTED", "USER_ERROR"),
            Self::InvalidSessionProperty => (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
            Self::CatalogNotFound => (44, "CATALOG_NOT_FOUND", "USER_ERROR"),
            Self::TableNotFound => (46, "TABLE_NOT_FOUND", "USER_ERROR"),
            Self::ColumnNotFound => (47, "COLUMN_NOT_FOUND", "USER_ERROR"),
            Self::GenericInternalError => (65_536, "GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR"),
            Self::QueryQueueFull => (131_074, "QUERY_QUEUE_FULL", "INSUFFICIENT_RESOURCES"),
            Self::ExceededTimeLimit => (131_075, "EXCEEDED_TIME_LIMIT", "INSUFFICIENT_RESOURCES"),
            Self::ExceededLocalMemoryLimit => {
                (131_079, "EXCEEDED_LOCAL_MEMORY_LIMIT", "INSUFFICIENT_RESOURCES")
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
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

    fn at(mut self, location: Option<ErrorLocation>) -> Self {
        self.0.error_location = location;
        self
    }
}

/// `sqlparser` reports positions as "… at Line: 1, Column: 8".
fn parser_location(message: &str) -> Option<ErrorLocation> {
    let (_, tail) = message.rsplit_once("Line: ")?;
    let (line, tail) = tail.split_once(", Column: ")?;
    let column: String = tail.chars().take_while(char::is_ascii_digit).collect();
    Some(ErrorLocation {
        line_number: line.trim().parse().ok()?,
        column_number: column.parse().ok()?,
    })
}

fn code_of(error: &DataFusionError) -> Code {
    match error {
        DataFusionError::SQL(..) => Code::SyntaxError,
        DataFusionError::SchemaError(schema_error, _) => match schema_error.as_ref() {
            SchemaError::FieldNotFound { .. } => Code::ColumnNotFound,
            _ => Code::GenericUserError,
        },
        DataFusionError::Plan(message) => {
            if message.starts_with("table '") && message.ends_with("not found") {
                Code::TableNotFound
            } else if message.contains("catalog") && message.contains("not found") {
                Code::CatalogNotFound
            } else {
                Code::GenericUserError
            }
        }
        DataFusionError::NotImplemented(_) => Code::NotSupported,
        DataFusionError::ResourcesExhausted(_) => Code::ExceededLocalMemoryLimit,
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            ArrowError::DivideByZero => Code::DivisionByZero,
            ArrowError::CastError(_) | ArrowError::ParseError(_) => Code::InvalidCastArgument,
            _ => Code::GenericInternalError,
        },
        _ => Code::GenericInternalError,
    }
}

impl From<&DataFusionError> for QueryError {
    fn from(error: &DataFusionError) -> Self {
        let root = error.find_root();
        let message = root.to_string();
        let location = matches!(root, DataFusionError::SQL(..))
            .then(|| parser_location(&message))
            .flatten();
        Self::new(code_of(root), message).at(location)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(sql: &str) -> QueryError {
        let error = datafusion::sql::parser::DFParser::parse_sql(sql).unwrap_err();
        QueryError::from(&error)
    }

    #[test]
    fn syntax_errors_carry_their_location() {
        let error = map("SELECT a FROM t ORDER x");
        assert_eq!(error.name(), "SYNTAX_ERROR");
        assert_eq!(error.kind(), "USER_ERROR");
        let location = error
            .0
            .error_location
            .clone()
            .unwrap_or_else(|| panic!("no location: {error:?}"));
        assert_eq!(location.line_number, 1);
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
        ];
        for (error, name) in cases {
            assert_eq!(QueryError::from(&error).name(), name, "{error}");
        }
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
