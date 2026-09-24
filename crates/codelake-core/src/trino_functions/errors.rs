//! Trino error names (`errorName` in the client protocol) for engine errors.
//!
//! Functions of this library raise errors whose message starts with the Trino
//! name (`INVALID_FUNCTION_ARGUMENT: …`, see `support`). Errors raised by
//! Arrow kernels and `DataFusion` itself are classified by kind, so a client
//! sees the same name Trino would report for the same failure.

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;

/// Names this library puts at the start of its error messages.
const PREFIXES: [&str; 9] = [
    "INVALID_FUNCTION_ARGUMENT",
    "NUMERIC_VALUE_OUT_OF_RANGE",
    "INVALID_CAST_ARGUMENT",
    "DIVISION_BY_ZERO",
    "FUNCTION_NOT_FOUND",
    "NOT_SUPPORTED",
    "TYPE_MISMATCH",
    "INVALID_LITERAL",
    "EXCEEDED_FUNCTION_MEMORY_LIMIT",
];

/// Trino error names `try(…)` turns into NULL (Trino's `TryFunction`).
const CAUGHT_BY_TRY: [&str; 4] = [
    "DIVISION_BY_ZERO",
    "INVALID_CAST_ARGUMENT",
    "INVALID_FUNCTION_ARGUMENT",
    "NUMERIC_VALUE_OUT_OF_RANGE",
];

fn prefixed(message: &str) -> Option<&'static str> {
    let head = message.split(':').next().unwrap_or_default().trim();
    PREFIXES.iter().copied().find(|name| *name == head)
}

fn arrow_error_name(error: &ArrowError) -> &'static str {
    match error {
        ArrowError::DivideByZero => "DIVISION_BY_ZERO",
        ArrowError::ArithmeticOverflow(_) => "NUMERIC_VALUE_OUT_OF_RANGE",
        // Arrow reports a value that does not fit the target type as
        // "Can't cast value …"; Trino calls that an out-of-range number.
        ArrowError::CastError(message) if message.starts_with("Can't cast value") => {
            "NUMERIC_VALUE_OUT_OF_RANGE"
        }
        ArrowError::CastError(_) | ArrowError::ParseError(_) => "INVALID_CAST_ARGUMENT",
        ArrowError::ExternalError(inner) => match inner.downcast_ref::<DataFusionError>() {
            Some(error) => trino_error_name(error),
            None => "GENERIC_INTERNAL_ERROR",
        },
        ArrowError::InvalidArgumentError(message) if message.contains("too large to store in a Decimal") => {
            "INVALID_CAST_ARGUMENT"
        }
        ArrowError::ComputeError(message) | ArrowError::InvalidArgumentError(message) => {
            prefixed(message).unwrap_or("GENERIC_INTERNAL_ERROR")
        }
        _ => "GENERIC_INTERNAL_ERROR",
    }
}

/// Trino `errorName` for an error raised while planning or running a query.
pub fn trino_error_name(error: &DataFusionError) -> &'static str {
    let root = error.find_root();
    match root {
        DataFusionError::ArrowError(inner, _) => arrow_error_name(inner),
        DataFusionError::ResourcesExhausted(_) => "EXCEEDED_LOCAL_MEMORY_LIMIT",
        DataFusionError::SQL(..) => "SYNTAX_ERROR",
        DataFusionError::NotImplemented(message) => prefixed(message).unwrap_or("NOT_SUPPORTED"),
        DataFusionError::External(inner) => match inner.downcast_ref::<ArrowError>() {
            Some(arrow) => arrow_error_name(arrow),
            None => prefixed(&root.message()).unwrap_or("GENERIC_INTERNAL_ERROR"),
        },
        DataFusionError::SchemaError(..) => "COLUMN_NOT_FOUND",
        DataFusionError::Plan(message) => prefixed(message).unwrap_or_else(|| {
            if message.contains("Invalid function") || message.contains("No function matches") {
                "FUNCTION_NOT_FOUND"
            } else {
                "GENERIC_USER_ERROR"
            }
        }),
        other => {
            let message = other.message();
            if let Some(name) = prefixed(&message) {
                name
            } else if message.contains("Divide by zero") {
                "DIVISION_BY_ZERO"
            } else {
                "GENERIC_INTERNAL_ERROR"
            }
        }
    }
}

/// True when Trino's `try(…)` returns NULL for this error instead of failing.
pub(crate) fn caught_by_try(error: &DataFusionError) -> bool {
    CAUGHT_BY_TRY.contains(&trino_error_name(error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_message_prefix_and_the_arrow_kind() {
        let own = DataFusionError::Execution("INVALID_FUNCTION_ARGUMENT: bad".into());
        assert_eq!(trino_error_name(&own), "INVALID_FUNCTION_ARGUMENT");
        let wrapped = DataFusionError::Context("while planning".into(), Box::new(own));
        assert_eq!(trino_error_name(&wrapped), "INVALID_FUNCTION_ARGUMENT");
        let zero = DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None);
        assert_eq!(trino_error_name(&zero), "DIVISION_BY_ZERO");
        let cast = DataFusionError::ArrowError(
            Box::new(ArrowError::CastError(
                "Cannot cast string '1.5' to value of Int32 type".into(),
            )),
            None,
        );
        assert_eq!(trino_error_name(&cast), "INVALID_CAST_ARGUMENT");
        let range = DataFusionError::ArrowError(
            Box::new(ArrowError::CastError(
                "Can't cast value 3000000000 to type Int32".into(),
            )),
            None,
        );
        assert_eq!(trino_error_name(&range), "NUMERIC_VALUE_OUT_OF_RANGE");
        let plan = DataFusionError::Plan("FUNCTION_NOT_FOUND: Unexpected parameters".into());
        assert_eq!(trino_error_name(&plan), "FUNCTION_NOT_FOUND");
        assert_eq!(
            trino_error_name(&DataFusionError::ResourcesExhausted("pool".into())),
            "EXCEEDED_LOCAL_MEMORY_LIMIT"
        );
    }

    #[test]
    fn try_catches_only_user_errors() {
        assert!(caught_by_try(&DataFusionError::Execution(
            "DIVISION_BY_ZERO: x".into()
        )));
        assert!(caught_by_try(&DataFusionError::ArrowError(
            Box::new(ArrowError::DivideByZero),
            None
        )));
        assert!(!caught_by_try(&DataFusionError::ResourcesExhausted(
            "pool".into()
        )));
        assert!(!caught_by_try(&DataFusionError::Internal("bug".into())));
        assert!(!caught_by_try(&DataFusionError::Execution(
            "FUNCTION_NOT_FOUND: x".into()
        )));
    }
}
