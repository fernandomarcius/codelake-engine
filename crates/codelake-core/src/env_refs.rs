//! `${ENV:NAME}` references in the configuration file.
//!
//! Any string value of `codelake.toml` may hold one or more references to
//! environment variables, resolved once, when the file is loaded
//! ([`crate::Config::load`]):
//!
//! ```toml
//! [[source]]
//! name = "erp"
//! kind = "postgres"
//! host = "${ENV:ERP_HOST}"
//! database = "erp_${ENV:STAGE}"
//! ```
//!
//! Rules:
//! - `NAME` matches `[A-Za-z_][A-Za-z0-9_]*`.
//! - A variable that is unset or empty is an error that names the variable
//!   and the key (`source[0].host`); every missing variable is reported at
//!   once. A resolved value never appears in an error message.
//! - Every `${ENV:` starts a reference; there is no escape for it, as in
//!   catalog-properties files. Any other `$` is literal.
//! - Only string values are resolved; numbers and booleans are written as
//!   literals. Keys (table names) are never resolved.
//! - Resolution is not recursive: a value that itself contains `${ENV:…}` is
//!   inserted as it is.
//!
//! References are for non-secret values (hosts, paths, listen addresses,
//! sizes). Secrets keep their `*_env` keys (`password_env = "ERP_PASSWORD"`),
//! whose values the configuration holds as `SecretString` and never prints.

use std::fmt::Write as _;

/// Start of a reference.
pub const REFERENCE_START: &str = "${ENV:";

/// Whether `name` is a valid environment variable name for a reference.
pub fn is_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `NAME` when the whole of `text` is exactly one `${ENV:NAME}` reference.
pub fn whole_reference(text: &str) -> Option<&str> {
    let name = text.strip_prefix(REFERENCE_START)?.strip_suffix('}')?;
    is_env_name(name).then_some(name)
}

/// A reference that cannot be resolved. Never carries a value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReferenceError {
    #[error("has an unclosed ${{ENV:…}} reference")]
    Unclosed,
    #[error("references an invalid environment variable name (expected [A-Za-z_][A-Za-z0-9_]*)")]
    InvalidName,
    #[error("references environment variable {0}, which is not set (or is empty)")]
    Missing(String),
}

/// One piece of a string value: literal text or a reference.
#[derive(Debug, PartialEq, Eq)]
enum Piece<'a> {
    Text(&'a str),
    Reference(&'a str),
}

/// Splits `text` into literal pieces and references.
fn pieces(text: &str) -> Result<Vec<Piece<'_>>, ReferenceError> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find(REFERENCE_START) {
        out.push(Piece::Text(&rest[..at]));
        let tail = &rest[at + REFERENCE_START.len()..];
        let end = tail.find('}').ok_or(ReferenceError::Unclosed)?;
        let name = &tail[..end];
        if !is_env_name(name) {
            return Err(ReferenceError::InvalidName);
        }
        out.push(Piece::Reference(name));
        rest = &tail[end + 1..];
    }
    out.push(Piece::Text(rest));
    out.retain(|p| !matches!(p, Piece::Text("")));
    Ok(out)
}

/// Names referenced by `text`, in order (repeats kept).
pub fn references(text: &str) -> Result<Vec<&str>, ReferenceError> {
    Ok(pieces(text)?
        .into_iter()
        .filter_map(|p| match p {
            Piece::Reference(name) => Some(name),
            Piece::Text(_) => None,
        })
        .collect())
}

/// Resolves every reference of `text` with `env`. Unset and empty
/// variables are both [`ReferenceError::Missing`].
pub fn interpolate(text: &str, env: &dyn Fn(&str) -> Option<String>) -> Result<String, ReferenceError> {
    let mut out = String::with_capacity(text.len());
    for piece in pieces(text)? {
        match piece {
            Piece::Text(t) => out.push_str(t),
            Piece::Reference(name) => match env(name) {
                Some(value) if !value.is_empty() => out.push_str(&value),
                _ => return Err(ReferenceError::Missing(name.to_owned())),
            },
        }
    }
    Ok(out)
}

/// Whether any string of the document holds `${ENV:`.
pub fn has_references(value: &toml::Value) -> bool {
    match value {
        toml::Value::String(s) => s.contains(REFERENCE_START),
        toml::Value::Array(items) => items.iter().any(has_references),
        toml::Value::Table(table) => table.values().any(has_references),
        _ => false,
    }
}

/// Resolved values, kept only to redact them from later error messages.
#[derive(Default)]
pub struct Resolved {
    values: Vec<(String, String)>,
}

impl std::fmt::Debug for Resolved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resolved")
            .field("names", &self.values.iter().map(|(n, _)| n).collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl Resolved {
    /// `message` with every resolved value of three or more characters
    /// replaced by its `${ENV:NAME}` reference. Deserialization errors quote
    /// the offending value (`invalid type: string "…"`); this keeps them
    /// from quoting what came from the environment.
    pub fn redact(&self, message: &str) -> String {
        let mut values: Vec<&(String, String)> = self
            .values
            .iter()
            .filter(|(_, v)| v.chars().count() >= 3)
            .collect();
        values.sort_by_key(|(_, v)| std::cmp::Reverse(v.len()));
        let mut out = message.to_owned();
        for (name, value) in values {
            out = out.replace(value.as_str(), &format!("${{ENV:{name}}}"));
        }
        out
    }
}

fn key_path(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_owned()
    } else {
        format!("{path}.{key}")
    }
}

fn resolve_value(
    value: &mut toml::Value,
    path: &str,
    env: &dyn Fn(&str) -> Option<String>,
    resolved: &mut Resolved,
    errors: &mut Vec<String>,
) {
    match value {
        toml::Value::String(text) => {
            let pieces = match pieces(text) {
                Ok(pieces) => pieces,
                Err(err) => {
                    errors.push(format!("`{path}` {err}"));
                    return;
                }
            };
            if !pieces.iter().any(|p| matches!(p, Piece::Reference(_))) {
                return;
            }
            let mut out = String::with_capacity(text.len());
            let mut failed = false;
            for piece in pieces {
                match piece {
                    Piece::Text(t) => out.push_str(t),
                    Piece::Reference(name) => match env(name) {
                        Some(v) if !v.is_empty() => {
                            out.push_str(&v);
                            resolved.values.push((name.to_owned(), v));
                        }
                        _ => {
                            failed = true;
                            errors.push(format!("`{path}` {}", ReferenceError::Missing(name.to_owned())));
                        }
                    },
                }
            }
            if !failed {
                *text = out;
            }
        }
        toml::Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                let mut item_path = String::with_capacity(path.len() + 4);
                let _ = write!(item_path, "{path}[{index}]");
                resolve_value(item, &item_path, env, resolved, errors);
            }
        }
        toml::Value::Table(table) => {
            for (key, item) in table.iter_mut() {
                resolve_value(item, &key_path(path, key), env, resolved, errors);
            }
        }
        _ => {}
    }
}

/// Resolves every reference of a parsed document in place. Errors name the
/// key (`source[1].host`) and the variable, never a value, and are all
/// reported together.
pub fn resolve_document(
    document: &mut toml::Value,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Resolved, Vec<String>> {
    let mut resolved = Resolved::default();
    let mut errors = Vec::new();
    resolve_value(document, "", env, &mut resolved, &mut errors);
    if errors.is_empty() {
        Ok(resolved)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(name: &str) -> Option<String> {
        match name {
            "HOST" => Some("db.internal".to_owned()),
            "STAGE" => Some("hml".to_owned()),
            "EMPTY" => Some(String::new()),
            "NESTED" => Some("${ENV:HOST}".to_owned()),
            _ => None,
        }
    }

    fn document(text: &str) -> toml::Value {
        toml::Value::Table(toml::from_str::<toml::Table>(text).unwrap())
    }

    #[test]
    fn interpolates_whole_and_embedded_references() {
        assert_eq!(interpolate("${ENV:HOST}", &env).unwrap(), "db.internal");
        assert_eq!(interpolate("erp_${ENV:STAGE}_x", &env).unwrap(), "erp_hml_x");
        assert_eq!(
            interpolate("${ENV:HOST}:${ENV:STAGE}", &env).unwrap(),
            "db.internal:hml"
        );
        assert_eq!(interpolate("no refs", &env).unwrap(), "no refs");
        assert_eq!(interpolate("", &env).unwrap(), "");
    }

    #[test]
    fn other_dollar_signs_are_literal() {
        assert_eq!(interpolate("a$b$", &env).unwrap(), "a$b$");
        assert_eq!(interpolate("${OTHER}", &env).unwrap(), "${OTHER}");
        assert_eq!(interpolate("x$${y", &env).unwrap(), "x$${y");
        assert_eq!(interpolate("$${ENV:HOST}", &env).unwrap(), "$db.internal");
        assert_eq!(interpolate("$$", &env).unwrap(), "$$");
    }

    #[test]
    fn resolution_is_not_recursive() {
        assert_eq!(interpolate("${ENV:NESTED}", &env).unwrap(), "${ENV:HOST}");
    }

    #[test]
    fn errors_name_the_variable_only() {
        assert_eq!(
            interpolate("${ENV:UNSET}", &env),
            Err(ReferenceError::Missing("UNSET".to_owned()))
        );
        assert_eq!(
            interpolate("${ENV:EMPTY}", &env),
            Err(ReferenceError::Missing("EMPTY".to_owned()))
        );
        assert_eq!(interpolate("${ENV:HOST", &env), Err(ReferenceError::Unclosed));
        assert_eq!(interpolate("${ENV:9X}", &env), Err(ReferenceError::InvalidName));
        assert_eq!(interpolate("${ENV:}", &env), Err(ReferenceError::InvalidName));
        assert_eq!(interpolate("${ENV:A-B}", &env), Err(ReferenceError::InvalidName));
    }

    #[test]
    fn whole_reference_and_listing() {
        assert_eq!(whole_reference("${ENV:X_1}"), Some("X_1"));
        assert_eq!(whole_reference("a${ENV:X}"), None);
        assert_eq!(whole_reference("${ENV:X}b"), None);
        assert_eq!(whole_reference("${ENV:1}"), None);
        assert_eq!(
            references("${ENV:A}-${ENV:B}-$${ENV:C}").unwrap(),
            ["A", "B", "C"]
        );
    }

    #[test]
    fn documents_resolve_every_string_and_report_every_missing_variable() {
        let mut doc = document(
            r#"
            top = "${ENV:HOST}"
            port = 5432
            [section]
            list = ["${ENV:STAGE}", "${ENV:MISSING_A}", 3]
            [[source]]
            host = "${ENV:MISSING_B}"
            "${ENV:HOST}" = "keys are not resolved"
            "#,
        );
        let errors = resolve_document(&mut doc, &env).unwrap_err();
        assert_eq!(
            errors,
            [
                "`section.list[1]` references environment variable MISSING_A, which is not set (or is empty)",
                "`source[0].host` references environment variable MISSING_B, which is not set (or is empty)",
            ]
        );
        let mut doc = document("a = \"${ENV:HOST}\"\n[t]\nb = [\"x${ENV:STAGE}\"]\n\"${ENV:HOST}\" = 1\n");
        let resolved = resolve_document(&mut doc, &env).unwrap();
        assert_eq!(doc["a"].as_str(), Some("db.internal"));
        assert_eq!(doc["t"]["b"][0].as_str(), Some("xhml"));
        assert!(doc["t"].get("${ENV:HOST}").is_some());
        assert!(!format!("{resolved:?}").contains("db.internal"));
        assert!(has_references(&document("a = [\"${ENV:X}\"]")));
        assert!(!has_references(&document("a = [\"$X\"]")));
    }

    #[test]
    fn redaction_replaces_resolved_values_longest_first() {
        let mut doc = document("a = \"${ENV:HOST}\"\nb = \"${ENV:STAGE}\"\n");
        let resolved = resolve_document(&mut doc, &env).unwrap();
        assert_eq!(
            resolved.redact("invalid type: string \"db.internal\" near hml"),
            "invalid type: string \"${ENV:HOST}\" near ${ENV:STAGE}"
        );
    }
}
