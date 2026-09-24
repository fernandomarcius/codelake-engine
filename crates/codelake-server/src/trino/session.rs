//! Client session carried in `X-Trino-*` headers, and the statements that change
//! it (`USE`, `SET SESSION`, `RESET SESSION`). Trino keeps no server-side
//! session: the server answers with `X-Trino-Set-*` headers and the client sends
//! the new values on every later request. Prepared statements travel the same
//! way (see [`super::prepared`]).

use std::collections::BTreeMap;
use std::fmt::Write as _;

use axum::http::HeaderMap;
use codelake_core::{PropertyRegistry, SessionProperties};

use super::errors::{Code, Namespace, QueryError};
use super::prepared;
use super::types::Encoding;

pub(crate) const USER: &str = "x-trino-user";
pub(crate) const CATALOG: &str = "x-trino-catalog";
pub(crate) const SCHEMA: &str = "x-trino-schema";
pub(crate) const SOURCE: &str = "x-trino-source";
pub(crate) const SESSION: &str = "x-trino-session";
pub(crate) const SET_CATALOG: &str = "x-trino-set-catalog";
pub(crate) const SET_SCHEMA: &str = "x-trino-set-schema";
pub(crate) const SET_SESSION: &str = "x-trino-set-session";
pub(crate) const CLEAR_SESSION: &str = "x-trino-clear-session";
pub(crate) const CLIENT_CAPABILITIES: &str = "x-trino-client-capabilities";
pub(crate) const TIME_ZONE: &str = "x-trino-time-zone";
pub(crate) const PREPARED_STATEMENT: &str = "x-trino-prepared-statement";
pub(crate) const ADDED_PREPARE: &str = "x-trino-added-prepare";
pub(crate) const DEALLOCATED_PREPARE: &str = "x-trino-deallocated-prepare";

fn invalid_property(error: codelake_core::session_properties::PropertyError) -> QueryError {
    QueryError::new(Code::InvalidSessionProperty, error.0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClientSession {
    pub(crate) user: String,
    pub(crate) source: Option<String>,
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) properties: SessionProperties,
    /// From `X-Trino-Client-Capabilities`.
    pub(crate) encoding: Encoding,
    /// From `X-Trino-Prepared-Statement`: name → SQL.
    pub(crate) prepared: BTreeMap<String, String>,
    /// From `X-Trino-Time-Zone`, as Trino names the zone.
    pub(crate) time_zone: Option<String>,
}

impl Default for ClientSession {
    fn default() -> Self {
        Self {
            user: String::new(),
            source: None,
            catalog: None,
            schema: None,
            properties: SessionProperties::default(),
            encoding: Encoding::MODERN,
            prepared: BTreeMap::new(),
            time_zone: None,
        }
    }
}

/// Why the session headers were refused.
#[derive(Debug)]
pub(crate) enum SessionError {
    /// Malformed headers: HTTP 400, as Trino answers them.
    BadRequest(String),
    /// Well-formed headers the query cannot run with (an unknown property):
    /// the query fails.
    Query(QueryError),
}

fn header(headers: &HeaderMap, name: &str) -> Result<Option<String>, SessionError> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(|v| v.trim().to_owned())
                .map_err(|_| SessionError::BadRequest(format!("Header {name} is not valid text")))
        })
        .transpose()
        .map(|v| v.filter(|v| !v.is_empty()))
}

fn texts<'a>(headers: &'a HeaderMap, name: &str) -> Result<Vec<&'a str>, SessionError> {
    headers
        .get_all(name)
        .iter()
        .map(|value| {
            value
                .to_str()
                .map_err(|_| SessionError::BadRequest(format!("Header {name} is not valid text")))
        })
        .collect()
}

impl ClientSession {
    pub(crate) fn from_headers(
        headers: &HeaderMap,
        registry: &PropertyRegistry,
    ) -> Result<Self, SessionError> {
        let user = header(headers, USER)?
            .ok_or_else(|| SessionError::BadRequest("User must be set (X-Trino-User)".into()))?;
        let catalog = header(headers, CATALOG)?;
        let schema = header(headers, SCHEMA)?;
        if schema.is_some() && catalog.is_none() {
            return Err(SessionError::BadRequest(
                "Schema is set but catalog is not (X-Trino-Catalog)".into(),
            ));
        }
        let mut raw = BTreeMap::new();
        for text in texts(headers, SESSION)? {
            for pair in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                let (name, value) = pair.split_once('=').ok_or_else(|| {
                    SessionError::BadRequest(format!("Invalid X-Trino-Session header: {pair}"))
                })?;
                let value = percent_decode(value.trim()).map_err(|_| {
                    SessionError::BadRequest(format!("Invalid X-Trino-Session header: {pair}"))
                })?;
                raw.insert(name.trim().to_owned(), value);
            }
        }
        let parametric_datetime = texts(headers, CLIENT_CAPABILITIES)?
            .iter()
            .flat_map(|text| text.split(','))
            .any(|capability| capability.trim().eq_ignore_ascii_case("PARAMETRIC_DATETIME"));
        let prepared = prepared::parse_header(texts(headers, PREPARED_STATEMENT)?.into_iter())
            .map_err(SessionError::BadRequest)?;
        let time_zone = header(headers, TIME_ZONE)?
            .map(|zone| {
                codelake_core::trino_time_zone(&zone).ok_or_else(|| {
                    SessionError::Query(QueryError::new(
                        Code::NotSupported,
                        format!("Time zone not supported: {zone}"),
                    ))
                })
            })
            .transpose()?;
        Ok(Self {
            user,
            source: header(headers, SOURCE)?,
            catalog,
            schema,
            properties: registry
                .session(raw)
                .map_err(|e| SessionError::Query(invalid_property(e)))?,
            encoding: Encoding { parametric_datetime },
            prepared,
            time_zone,
        })
    }
}

pub(crate) fn percent_decode(text: &str) -> Result<String, QueryError> {
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

/// `text` after `keyword` (case-insensitive) and the whitespace that follows it.
pub(crate) fn strip_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let head = text.get(..keyword.len())?;
    let rest = &text[keyword.len()..];
    (head.eq_ignore_ascii_case(keyword) && rest.starts_with(char::is_whitespace)).then(|| rest.trim_start())
}

fn syntax(message: &str) -> QueryError {
    QueryError::new(Code::SyntaxError, message.to_owned())
}

/// `USE` of a catalog or schema that does not exist fails, as in Trino, so
/// the client's session never points at nothing.
fn check_exists(catalog: &str, schema: &str, names: &dyn Namespace) -> Result<(), QueryError> {
    if !names.has_catalog(catalog) {
        return Err(QueryError::new(
            Code::NotFound,
            format!("Catalog does not exist: {catalog}"),
        ));
    }
    if !names.has_schema(catalog, schema) {
        return Err(QueryError::new(
            Code::NotFound,
            format!("Schema does not exist: {catalog}.{schema}"),
        ));
    }
    Ok(())
}

/// Recognizes a session-control statement; `None` means "plan it normally".
pub(crate) fn control(
    sql: &str,
    session: &ClientSession,
    registry: &PropertyRegistry,
    names: &dyn Namespace,
) -> Result<Option<Control>, QueryError> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    if let Some(rest) = strip_keyword(sql, "USE") {
        let parts = qualified_name(rest).ok_or_else(|| syntax("USE expects [catalog.]schema"))?;
        return match parts.as_slice() {
            [schema] => {
                let Some(catalog) = &session.catalog else {
                    return Err(QueryError::new(
                        Code::GenericUserError,
                        "Catalog must be specified when session catalog is not set",
                    ));
                };
                check_exists(catalog, schema, names)?;
                Ok(Some(Control::Use {
                    catalog: None,
                    schema: schema.clone(),
                }))
            }
            [catalog, schema] => {
                check_exists(catalog, schema, names)?;
                Ok(Some(Control::Use {
                    catalog: Some(catalog.clone()),
                    schema: schema.clone(),
                }))
            }
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
    use crate::trino::errors::Everything;
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

    fn query_error(result: Result<ClientSession, SessionError>) -> &'static str {
        match result {
            Err(SessionError::Query(error)) => error.name(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn session_errors() {
        let registry = registry();
        assert!(matches!(
            ClientSession::from_headers(&headers(&[]), &registry),
            Err(SessionError::BadRequest(_))
        ));
        let unknown = ClientSession::from_headers(
            &headers(&[("X-Trino-User", "u"), ("X-Trino-Session", "nope=1")]),
            &registry,
        );
        assert_eq!(query_error(unknown), "INVALID_SESSION_PROPERTY");
        let bad = ClientSession::from_headers(
            &headers(&[
                ("X-Trino-User", "u"),
                ("X-Trino-Session", "query_max_run_time=soon"),
            ]),
            &registry,
        );
        assert_eq!(query_error(bad), "INVALID_SESSION_PROPERTY");
        for malformed in [
            &[("X-Trino-User", "u"), ("X-Trino-Schema", "s")][..],
            &[("X-Trino-User", "u"), ("X-Trino-Session", "no_equals")][..],
            &[("X-Trino-User", "u"), ("X-Trino-Prepared-Statement", "no_equals")][..],
        ] {
            assert!(matches!(
                ClientSession::from_headers(&headers(malformed), &registry),
                Err(SessionError::BadRequest(_))
            ));
        }
    }

    /// Trino 431 names the zone as it prints it and refuses unknown ones
    /// (`NOT_SUPPORTED: Time zone not supported: Mars/Olympus`).
    #[test]
    fn the_session_zone_comes_from_its_header() {
        let registry = registry();
        let zone = |value: &str| {
            ClientSession::from_headers(
                &headers(&[("X-Trino-User", "u"), ("X-Trino-Time-Zone", value)]),
                &registry,
            )
        };
        let plain = ClientSession::from_headers(&headers(&[("X-Trino-User", "u")]), &registry).unwrap();
        assert_eq!(plain.time_zone, None);
        for (value, expected) in [
            ("America/Sao_Paulo", "America/Sao_Paulo"),
            ("-03:00", "-03:00"),
            ("Etc/GMT+3", "-03:00"),
            ("UTC", "UTC"),
        ] {
            assert_eq!(
                zone(value).unwrap().time_zone.as_deref(),
                Some(expected),
                "{value}"
            );
        }
        for unknown in ["Mars/Olympus", "america/sao_paulo"] {
            assert_eq!(query_error(zone(unknown)), "NOT_SUPPORTED", "{unknown}");
        }
    }

    #[test]
    fn capabilities_and_prepared_statements_come_from_headers() {
        let registry = registry();
        let plain = ClientSession::from_headers(&headers(&[("X-Trino-User", "u")]), &registry).unwrap();
        assert!(!plain.encoding.parametric_datetime);
        let modern = ClientSession::from_headers(
            &headers(&[
                ("X-Trino-User", "u"),
                (
                    "X-Trino-Client-Capabilities",
                    "PATH, PARAMETRIC_DATETIME,SESSION_AUTHORIZATION",
                ),
                ("X-Trino-Prepared-Statement", "s1=SELECT+%3F,s2=SELECT%201"),
            ]),
            &registry,
        )
        .unwrap();
        assert!(modern.encoding.parametric_datetime);
        assert_eq!(modern.prepared["s1"], "SELECT ?");
        assert_eq!(modern.prepared["s2"], "SELECT 1");
    }

    struct Only;

    impl Namespace for Only {
        fn has_catalog(&self, catalog: &str) -> bool {
            catalog == "hive"
        }
        fn has_schema(&self, _: &str, schema: &str) -> bool {
            schema == "raw"
        }
    }

    #[test]
    fn use_requires_an_existing_catalog_and_schema() {
        let registry = registry();
        let none = ClientSession::default();
        assert!(control("USE hive.raw", &none, &registry, &Only).is_ok());
        let catalog = control("USE nocat.raw", &none, &registry, &Only).unwrap_err();
        assert_eq!(catalog.name(), "NOT_FOUND");
        assert_eq!(catalog.message(), "Catalog does not exist: nocat");
        let schema = control("USE hive.nope", &none, &registry, &Only).unwrap_err();
        assert_eq!(schema.message(), "Schema does not exist: hive.nope");
        let in_session = ClientSession {
            catalog: Some("hive".into()),
            ..ClientSession::default()
        };
        assert_eq!(
            control("USE nope", &in_session, &registry, &Only)
                .unwrap_err()
                .name(),
            "NOT_FOUND"
        );
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
            control("use Hive.\"Raw Data\";", &none, &registry, &Everything).unwrap(),
            Some(Control::Use {
                catalog: Some("hive".into()),
                schema: "Raw Data".into()
            })
        );
        assert_eq!(
            control("USE streaming", &with_catalog, &registry, &Everything).unwrap(),
            Some(Control::Use {
                catalog: None,
                schema: "streaming".into()
            })
        );
        assert!(control("USE streaming", &none, &registry, &Everything).is_err());
        assert_eq!(
            control(
                "SET SESSION query_max_run_time = '2h'",
                &none,
                &registry,
                &Everything
            )
            .unwrap(),
            Some(Control::SetSession {
                name: "query_max_run_time".into(),
                value: "2h".into()
            })
        );
        assert_eq!(
            control("set session scale_writers=FALSE", &none, &registry, &Everything).unwrap(),
            Some(Control::SetSession {
                name: "scale_writers".into(),
                value: "false".into()
            })
        );
        assert_eq!(
            control("SET SESSION bogus = 1", &none, &registry, &Everything)
                .unwrap_err()
                .name(),
            "INVALID_SESSION_PROPERTY"
        );
        assert_eq!(
            control("RESET SESSION scale_writers", &none, &registry, &Everything).unwrap(),
            Some(Control::ResetSession {
                name: "scale_writers".into()
            })
        );
        assert!(control("RESET SESSION bogus", &none, &registry, &Everything).is_err());
        assert_eq!(control("SELECT 1", &none, &registry, &Everything).unwrap(), None);
        assert_eq!(
            control("user_table_scan", &none, &registry, &Everything).unwrap(),
            None
        );
        assert_eq!(control("SETTINGS", &none, &registry, &Everything).unwrap(), None);
    }
}
