//! Client session carried in `X-Trino-*` headers, and the statements that change
//! it (`USE`, `SET SESSION`, `RESET SESSION`). Trino keeps no server-side
//! session: the server answers with `X-Trino-Set-*` headers and the client sends
//! the new values on every later request.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use axum::http::HeaderMap;
use codelake_core::{PropertyRegistry, SessionProperties};

use super::errors::{Code, QueryError};

pub(crate) const USER: &str = "x-trino-user";
pub(crate) const CATALOG: &str = "x-trino-catalog";
pub(crate) const SCHEMA: &str = "x-trino-schema";
pub(crate) const SOURCE: &str = "x-trino-source";
pub(crate) const SESSION: &str = "x-trino-session";
pub(crate) const SET_CATALOG: &str = "x-trino-set-catalog";
pub(crate) const SET_SCHEMA: &str = "x-trino-set-schema";
pub(crate) const SET_SESSION: &str = "x-trino-set-session";
pub(crate) const CLEAR_SESSION: &str = "x-trino-clear-session";

fn invalid_property(error: codelake_core::session_properties::PropertyError) -> QueryError {
    QueryError::new(Code::InvalidSessionProperty, error.0)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ClientSession {
    pub(crate) user: String,
    pub(crate) source: Option<String>,
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) properties: SessionProperties,
}

fn header(headers: &HeaderMap, name: &str) -> Result<Option<String>, QueryError> {
    headers
        .get(name)
        .map(|value| {
            value.to_str().map(|v| v.trim().to_owned()).map_err(|_| {
                QueryError::new(Code::GenericUserError, format!("header {name} is not valid text"))
            })
        })
        .transpose()
        .map(|v| v.filter(|v| !v.is_empty()))
}

impl ClientSession {
    pub(crate) fn from_headers(headers: &HeaderMap, registry: &PropertyRegistry) -> Result<Self, QueryError> {
        let user = header(headers, USER)?
            .ok_or_else(|| QueryError::new(Code::GenericUserError, "User must be set (X-Trino-User)"))?;
        let catalog = header(headers, CATALOG)?;
        let schema = header(headers, SCHEMA)?;
        if schema.is_some() && catalog.is_none() {
            return Err(QueryError::new(
                Code::GenericUserError,
                "Schema is set but catalog is not (X-Trino-Catalog)",
            ));
        }
        let mut raw = BTreeMap::new();
        for value in headers.get_all(SESSION) {
            let text = value
                .to_str()
                .map_err(|_| QueryError::new(Code::GenericUserError, "X-Trino-Session is not valid text"))?;
            for pair in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                let (name, value) = pair.split_once('=').ok_or_else(|| {
                    QueryError::new(
                        Code::GenericUserError,
                        format!("Invalid X-Trino-Session entry: {pair}"),
                    )
                })?;
                raw.insert(name.trim().to_owned(), percent_decode(value.trim())?);
            }
        }
        Ok(Self {
            user,
            source: header(headers, SOURCE)?,
            catalog,
            schema,
            properties: registry.session(raw).map_err(invalid_property)?,
        })
    }
}

fn percent_decode(text: &str) -> Result<String, QueryError> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes
                    .get(i + 1..i + 3)
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| {
                        QueryError::new(Code::GenericUserError, format!("Invalid encoding: {text}"))
                    })?;
                out.push(hex);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(out)
        .map_err(|_| QueryError::new(Code::GenericUserError, format!("Invalid encoding: {text}")))
}

pub(crate) fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'*') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Statements that only change the client session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Control {
    Use { catalog: Option<String>, schema: String },
    SetSession { name: String, value: String },
    ResetSession { name: String },
}

impl Control {
    pub(crate) fn update_type(&self) -> &'static str {
        match self {
            Self::Use { .. } => "USE",
            Self::SetSession { .. } => "SET SESSION",
            Self::ResetSession { .. } => "RESET SESSION",
        }
    }
}

/// Splits `a."B".c` into normalized parts: unquoted names are lowercased,
/// quoted ones kept verbatim (`""` escapes a quote).
fn qualified_name(text: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = text.trim().chars().peekable();
    loop {
        let mut part = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next()? {
                    '"' if chars.peek() == Some(&'"') => {
                        chars.next();
                        part.push('"');
                    }
                    '"' => break,
                    c => part.push(c),
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c.is_alphanumeric() || c == '_' {
                    part.push(c.to_ascii_lowercase());
                    chars.next();
                } else {
                    break;
                }
            }
        }
        if part.is_empty() {
            return None;
        }
        parts.push(part);
        match chars.next() {
            None => return Some(parts),
            Some('.') => {}
            Some(_) => return None,
        }
    }
}

/// `'text'` with `''` escapes, or a bare literal (`true`, `1h`, `10`).
fn literal(text: &str) -> Option<String> {
    let text = text.trim();
    if let Some(inner) = text.strip_prefix('\'').and_then(|t| t.strip_suffix('\'')) {
        // Every quote inside must be doubled.
        let unescaped = inner.replace("''", "");
        (!unescaped.contains('\'')).then(|| inner.replace("''", "'"))
    } else if !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_')
    {
        Some(text.to_owned())
    } else {
        None
    }
}

fn strip_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let head = text.get(..keyword.len())?;
    let rest = &text[keyword.len()..];
    (head.eq_ignore_ascii_case(keyword) && rest.starts_with(char::is_whitespace)).then(|| rest.trim_start())
}

fn syntax(message: &str) -> QueryError {
    QueryError::new(Code::SyntaxError, message.to_owned())
}

/// Recognizes a session-control statement; `None` means "plan it normally".
pub(crate) fn control(
    sql: &str,
    session: &ClientSession,
    registry: &PropertyRegistry,
) -> Result<Option<Control>, QueryError> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    if let Some(rest) = strip_keyword(sql, "USE") {
        let parts = qualified_name(rest).ok_or_else(|| syntax("USE expects [catalog.]schema"))?;
        return match parts.as_slice() {
            [schema] => {
                if session.catalog.is_none() {
                    return Err(QueryError::new(
                        Code::GenericUserError,
                        "Catalog must be specified when session catalog is not set",
                    ));
                }
                Ok(Some(Control::Use {
                    catalog: None,
                    schema: schema.clone(),
                }))
            }
            [catalog, schema] => Ok(Some(Control::Use {
                catalog: Some(catalog.clone()),
                schema: schema.clone(),
            })),
            _ => Err(syntax("USE expects [catalog.]schema")),
        };
    }
    if let Some(rest) = strip_keyword(sql, "SET").and_then(|r| strip_keyword(r, "SESSION")) {
        let (name, value) = rest
            .split_once('=')
            .ok_or_else(|| syntax("SET SESSION expects name = value"))?;
        let name = qualified_name(name)
            .ok_or_else(|| syntax("invalid session property name"))?
            .join(".");
        let value = literal(value).ok_or_else(|| syntax("SET SESSION value must be a literal"))?;
        let (value, _) = registry.validate(&name, &value).map_err(invalid_property)?;
        return Ok(Some(Control::SetSession { name, value }));
    }
    if let Some(rest) = strip_keyword(sql, "RESET").and_then(|r| strip_keyword(r, "SESSION")) {
        let name = qualified_name(rest)
            .ok_or_else(|| syntax("invalid session property name"))?
            .join(".");
        registry.lookup(&name).map_err(invalid_property)?;
        return Ok(Some(Control::ResetSession { name }));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::time::Duration;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, HeaderValue::from_str(value).unwrap());
        }
        map
    }

    fn registry() -> PropertyRegistry {
        PropertyRegistry::default()
    }

    #[test]
    fn session_from_headers() {
        let session = ClientSession::from_headers(
            &headers(&[
                ("X-Trino-User", "dbt"),
                ("X-Trino-Catalog", "hive"),
                ("X-Trino-Schema", "streaming"),
                ("X-Trino-Source", "dbt-trino-1.8"),
                ("X-Trino-Session", "query_max_run_time=1h,scale_writers=false"),
                ("X-Trino-Session", "query_max_execution_time=30m"),
            ]),
            &registry(),
        )
        .unwrap();
        assert_eq!(session.user, "dbt");
        assert_eq!(session.catalog.as_deref(), Some("hive"));
        assert_eq!(session.schema.as_deref(), Some("streaming"));
        assert_eq!(session.source.as_deref(), Some("dbt-trino-1.8"));
        assert_eq!(session.properties.max_run_time(), Some(Duration::from_mins(30)));
        assert_eq!(session.properties.ignored(), ["scale_writers"]);
    }

    #[test]
    fn session_errors() {
        let registry = registry();
        assert!(ClientSession::from_headers(&headers(&[]), &registry).is_err());
        let unknown = ClientSession::from_headers(
            &headers(&[("X-Trino-User", "u"), ("X-Trino-Session", "nope=1")]),
            &registry,
        );
        assert_eq!(unknown.unwrap_err().name(), "INVALID_SESSION_PROPERTY");
        let bad = ClientSession::from_headers(
            &headers(&[
                ("X-Trino-User", "u"),
                ("X-Trino-Session", "query_max_run_time=soon"),
            ]),
            &registry,
        );
        assert_eq!(bad.unwrap_err().name(), "INVALID_SESSION_PROPERTY");
        let schema_only = ClientSession::from_headers(
            &headers(&[("X-Trino-User", "u"), ("X-Trino-Schema", "s")]),
            &registry,
        );
        assert!(schema_only.is_err());
    }

    #[test]
    fn percent_coding_round_trips() {
        assert_eq!(percent_decode("1%20h+x").unwrap(), "1 h x");
        assert!(percent_decode("%zz").is_err());
        assert_eq!(percent_encode("a b,c=é"), "a%20b%2Cc%3D%C3%A9");
        assert_eq!(percent_decode(&percent_encode("a b,c=é")).unwrap(), "a b,c=é");
    }

    #[test]
    fn literals() {
        assert_eq!(literal("'it''s'").as_deref(), Some("it's"));
        assert_eq!(literal("'bad'quote'"), None);
        assert_eq!(literal("true").as_deref(), Some("true"));
        assert_eq!(literal("1 + 1"), None);
    }

    #[test]
    fn control_statements() {
        let registry = registry();
        let none = ClientSession::default();
        let with_catalog = ClientSession {
            catalog: Some("hive".into()),
            ..ClientSession::default()
        };
        assert_eq!(
            control("use Hive.\"Raw Data\";", &none, &registry).unwrap(),
            Some(Control::Use {
                catalog: Some("hive".into()),
                schema: "Raw Data".into()
            })
        );
        assert_eq!(
            control("USE streaming", &with_catalog, &registry).unwrap(),
            Some(Control::Use {
                catalog: None,
                schema: "streaming".into()
            })
        );
        assert!(control("USE streaming", &none, &registry).is_err());
        assert_eq!(
            control("SET SESSION query_max_run_time = '2h'", &none, &registry).unwrap(),
            Some(Control::SetSession {
                name: "query_max_run_time".into(),
                value: "2h".into()
            })
        );
        assert_eq!(
            control("set session scale_writers=FALSE", &none, &registry).unwrap(),
            Some(Control::SetSession {
                name: "scale_writers".into(),
                value: "false".into()
            })
        );
        assert_eq!(
            control("SET SESSION bogus = 1", &none, &registry)
                .unwrap_err()
                .name(),
            "INVALID_SESSION_PROPERTY"
        );
        assert_eq!(
            control("RESET SESSION scale_writers", &none, &registry).unwrap(),
            Some(Control::ResetSession {
                name: "scale_writers".into()
            })
        );
        assert!(control("RESET SESSION bogus", &none, &registry).is_err());
        assert_eq!(control("SELECT 1", &none, &registry).unwrap(), None);
        assert_eq!(control("user_table_scan", &none, &registry).unwrap(), None);
        assert_eq!(control("SETTINGS", &none, &registry).unwrap(), None);
    }
}
