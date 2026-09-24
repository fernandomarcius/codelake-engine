//! Table procedures, following Trino's `ALTER TABLE <table> EXECUTE <name>(…)`.
//!
//! A catalog declares the procedures it runs ([`TableProcedure`]) with typed
//! arguments, the way it declares session properties. The engine parses the
//! statement, evaluates each argument as a constant expression, binds the
//! values here (defaults, names, types) and hands the catalog a validated
//! [`ProcedureCall`]. Error messages are Trino's, so clients that match on them
//! keep working.

use std::collections::BTreeMap;

use crate::session_properties::{PropertyType, parse_data_size, parse_duration};

/// One argument a table procedure accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcedureArgument {
    /// Lower-case name, used with `name => value`.
    pub name: String,
    pub property_type: PropertyType,
    /// Value used when the call omits the argument.
    pub default: String,
    pub description: String,
}

impl ProcedureArgument {
    pub fn new(name: &str, property_type: PropertyType, default: &str, description: &str) -> Self {
        Self {
            name: name.to_ascii_lowercase(),
            property_type,
            default: default.to_owned(),
            description: description.to_owned(),
        }
    }
}

/// A procedure registered by a catalog (`OPTIMIZE` for Hive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableProcedure {
    /// Upper-case name; calls match it case-insensitively.
    pub name: String,
    /// In positional order.
    pub arguments: Vec<ProcedureArgument>,
}

impl TableProcedure {
    pub fn new(name: &str, arguments: Vec<ProcedureArgument>) -> Self {
        Self {
            name: name.to_ascii_uppercase(),
            arguments,
        }
    }
}

/// The value of one argument, after evaluating its expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgumentValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Varchar(String),
    /// Any other type, kept as its display text for the error message.
    Other(String),
}

impl ArgumentValue {
    fn display(&self) -> String {
        match self {
            Self::Null => "null".into(),
            Self::Boolean(b) => b.to_string(),
            Self::Integer(i) => i.to_string(),
            Self::Varchar(s) | Self::Other(s) => s.clone(),
        }
    }
}

/// An argument as written in the call: `value` or `name => value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawArgument {
    pub name: Option<String>,
    pub value: ArgumentValue,
}

/// A validated call: every declared argument has a value (given or default),
/// normalized like a session property (`true`/`false`, trimmed sizes, …).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcedureCall {
    pub procedure: String,
    arguments: BTreeMap<String, String>,
}

impl ProcedureCall {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.arguments.get(name).map(String::as_str)
    }
}

/// Error message when `name` is not one of `procedures` (Trino's text).
pub fn not_registered(name: &str) -> String {
    format!("Table procedure not registered: {}", name.to_ascii_uppercase())
}

/// Finds a procedure by name, case-insensitively.
pub fn find<'a>(procedures: &'a [TableProcedure], name: &str) -> Result<&'a TableProcedure, String> {
    procedures
        .iter()
        .find(|p| p.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| not_registered(name))
}

/// Checks `value` against `property_type`; returns the normalized text or the
/// parser's reason.
fn normalize(property_type: PropertyType, value: &str) -> Result<String, String> {
    match property_type {
        PropertyType::DataSize => parse_data_size(value).map(|_| value.to_owned()),
        PropertyType::Duration => parse_duration(value)
            .map(|_| value.to_owned())
            .ok_or_else(|| format!("duration is not a valid data duration string: {value}")),
        PropertyType::Enum(allowed) => {
            let upper = value.trim().to_ascii_uppercase();
            if allowed.contains(&upper.as_str()) {
                Ok(upper)
            } else {
                Err(format!("Invalid value [{value}]. Valid values: {allowed:?}"))
            }
        }
        PropertyType::Boolean | PropertyType::Integer | PropertyType::Varchar => Ok(value.to_owned()),
    }
}

/// Binds evaluated arguments to `procedure` of `catalog`.
pub fn bind(
    catalog: &str,
    procedure: &TableProcedure,
    arguments: Vec<RawArgument>,
) -> Result<ProcedureCall, String> {
    let named = arguments.iter().filter(|a| a.name.is_some()).count();
    if named != 0 && named != arguments.len() {
        return Err("Named and positional arguments cannot be mixed".into());
    }
    if named == 0 && arguments.len() > procedure.arguments.len() {
        return Err("Too many positional arguments".into());
    }
    let property = |name: &str| {
        format!(
            "catalog '{catalog}' table procedure '{}' property '{name}'",
            procedure.name
        )
    };
    let mut given: BTreeMap<String, ArgumentValue> = BTreeMap::new();
    for (position, argument) in arguments.into_iter().enumerate() {
        let name = match argument.name {
            Some(name) => {
                let name = name.to_ascii_lowercase();
                if given.contains_key(&name) {
                    return Err(format!("Duplicate named argument: {name}"));
                }
                if !procedure.arguments.iter().any(|a| a.name == name) {
                    return Err(format!(
                        "Catalog '{catalog}' table procedure '{}' property '{name}' does not exist",
                        procedure.name
                    ));
                }
                name
            }
            None => procedure
                .arguments
                .get(position)
                .map(|a| a.name.clone())
                .ok_or("Too many positional arguments")?,
        };
        given.insert(name, argument.value);
    }
    let mut bound = BTreeMap::new();
    for declared in &procedure.arguments {
        let text = match given.remove(&declared.name) {
            None => declared.default.clone(),
            Some(ArgumentValue::Null) => {
                return Err(format!(
                    "Invalid null value for {} from [null]",
                    property(&declared.name)
                ));
            }
            Some(value) => {
                let expected = declared.property_type.name();
                match (&value, expected) {
                    (ArgumentValue::Varchar(s), "varchar") => s.clone(),
                    (ArgumentValue::Boolean(b), "boolean") => b.to_string(),
                    (ArgumentValue::Integer(i), "integer") => i.to_string(),
                    _ => {
                        return Err(format!(
                            "Invalid value for {}: Cannot convert [{}] to {expected}",
                            property(&declared.name),
                            value.display()
                        ));
                    }
                }
            }
        };
        let value = normalize(declared.property_type, &text).map_err(|reason| {
            format!(
                "Unable to set {} to ['{text}']: {reason}",
                property(&declared.name)
            )
        })?;
        bound.insert(declared.name.clone(), value);
    }
    Ok(ProcedureCall {
        procedure: procedure.name.clone(),
        arguments: bound,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn optimize() -> TableProcedure {
        TableProcedure::new(
            "optimize",
            vec![ProcedureArgument::new(
                "file_size_threshold",
                PropertyType::DataSize,
                "100MB",
                "Only compact files smaller than given threshold in bytes",
            )],
        )
    }

    fn named(name: &str, value: ArgumentValue) -> RawArgument {
        RawArgument {
            name: Some(name.into()),
            value,
        }
    }

    fn positional(value: ArgumentValue) -> RawArgument {
        RawArgument { name: None, value }
    }

    fn text(s: &str) -> ArgumentValue {
        ArgumentValue::Varchar(s.into())
    }

    #[test]
    fn defaults_named_and_positional() {
        let procedure = optimize();
        assert_eq!(procedure.name, "OPTIMIZE");
        let call = bind("hive", &procedure, vec![]).unwrap();
        assert_eq!(call.procedure, "OPTIMIZE");
        assert_eq!(call.get("file_size_threshold"), Some("100MB"));
        let call = bind(
            "hive",
            &procedure,
            vec![named("FILE_SIZE_THRESHOLD", text("10MB"))],
        )
        .unwrap();
        assert_eq!(call.get("file_size_threshold"), Some("10MB"));
        let call = bind("hive", &procedure, vec![positional(text("1kB"))]).unwrap();
        assert_eq!(call.get("file_size_threshold"), Some("1kB"));
        assert_eq!(call.get("nope"), None);
    }

    #[test]
    fn errors_match_trino() {
        let procedure = optimize();
        let err = |args| bind("hive", &procedure, args).unwrap_err();
        assert_eq!(
            err(vec![named("foo", text("1MB"))]),
            "Catalog 'hive' table procedure 'OPTIMIZE' property 'foo' does not exist"
        );
        assert_eq!(
            err(vec![named("file_size_threshold", text("abc"))]),
            "Unable to set catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['abc']: size is not a valid data size string: abc"
        );
        assert_eq!(
            err(vec![named("file_size_threshold", text("10mb"))]),
            "Unable to set catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['10mb']: Unknown unit: mb"
        );
        assert_eq!(
            err(vec![named("file_size_threshold", ArgumentValue::Integer(10))]),
            "Invalid value for catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold': Cannot convert [10] to varchar"
        );
        assert_eq!(
            err(vec![named("file_size_threshold", ArgumentValue::Null)]),
            "Invalid null value for catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold' from [null]"
        );
        assert_eq!(
            err(vec![
                named("file_size_threshold", text("1MB")),
                named("file_size_threshold", text("2MB"))
            ]),
            "Duplicate named argument: file_size_threshold"
        );
        assert_eq!(
            err(vec![positional(text("1MB")), positional(text("2MB"))]),
            "Too many positional arguments"
        );
        assert_eq!(
            err(vec![
                positional(text("1MB")),
                named("file_size_threshold", text("2MB"))
            ]),
            "Named and positional arguments cannot be mixed"
        );
        assert_eq!(
            err(vec![positional(ArgumentValue::Other("1.5".into()))]),
            "Invalid value for catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold': Cannot convert [1.5] to varchar"
        );
    }

    #[test]
    fn typed_arguments() {
        let procedure = TableProcedure::new(
            "tune",
            vec![
                ProcedureArgument::new("on", PropertyType::Boolean, "false", ""),
                ProcedureArgument::new("n", PropertyType::Integer, "1", ""),
                ProcedureArgument::new("mode", PropertyType::Enum(&["FAST", "SAFE"]), "SAFE", ""),
                ProcedureArgument::new("wait", PropertyType::Duration, "1s", ""),
                ProcedureArgument::new("label", PropertyType::Varchar, "", ""),
            ],
        );
        let call = bind(
            "c",
            &procedure,
            vec![
                positional(ArgumentValue::Boolean(true)),
                positional(ArgumentValue::Integer(7)),
                positional(text("fast")),
                positional(text("2m")),
                positional(text("x")),
            ],
        )
        .unwrap();
        assert_eq!(call.get("on"), Some("true"));
        assert_eq!(call.get("n"), Some("7"));
        assert_eq!(call.get("mode"), Some("FAST"));
        assert_eq!(call.get("wait"), Some("2m"));
        assert_eq!(call.get("label"), Some("x"));
        assert!(bind("c", &procedure, vec![positional(text("true"))]).is_err());
        assert!(bind("c", &procedure, vec![named("n", ArgumentValue::Boolean(true))]).is_err());
        assert!(bind("c", &procedure, vec![named("mode", text("slow"))]).is_err());
        assert!(bind("c", &procedure, vec![named("wait", text("soon"))]).is_err());
    }

    #[test]
    fn lookup_is_case_insensitive() {
        let procedures = vec![optimize()];
        assert!(find(&procedures, "Optimize").is_ok());
        assert_eq!(
            find(&procedures, "foo").unwrap_err(),
            "Table procedure not registered: FOO"
        );
        assert_eq!(ArgumentValue::Null.display(), "null");
        assert_eq!(ArgumentValue::Boolean(false).display(), "false");
    }
}
