//! Client session carried in `X-Trino-*` headers, and the statements that change
//! it (`USE`, `SET SESSION`, `RESET SESSION`). Trino keeps no server-side
//! session: the server answers with `X-Trino-Set-*` headers and the client sends
//! the new values on every later request. Prepared statements travel the same
//! way (see [`super::prepared`]).

use std::collections::BTreeMap;
use std::fmt::Write as _;

use axum::http::{HeaderMap, HeaderName};
use codelake_core::{PropertyRegistry, SessionProperties};

use super::errors::{Code, Namespace, QueryError};
use super::prepared;
use super::types::Encoding;

/// The protocol's header names: the configured prefix (`[server.trino]
/// header_prefix`, default `X-CodeLake-`, ISS-011) and each header's
/// suffix, in lower case as `http` compares them.
#[derive(Debug, Clone)]
pub(crate) struct HeaderNames {
    /// The prefix as configured, for messages.
    prefix: String,
    pub(crate) user: HeaderName,
    pub(crate) catalog: HeaderName,
    pub(crate) schema: HeaderName,
    pub(crate) source: HeaderName,
    pub(crate) session: HeaderName,
    pub(crate) set_catalog: HeaderName,
    pub(crate) set_schema: HeaderName,
    pub(crate) set_session: HeaderName,
    pub(crate) clear_session: HeaderName,
    pub(crate) client_capabilities: HeaderName,
    pub(crate) time_zone: HeaderName,
    pub(crate) prepared_statement: HeaderName,
    pub(crate) added_prepare: HeaderName,
    pub(crate) deallocated_prepare: HeaderName,
}

impl HeaderNames {
    /// Names under `prefix` (checked by the configuration: `X-<token>-`).
    ///
    /// # Panics
    ///
    /// When `prefix` plus a suffix is not a valid header name; the
    /// configuration refuses such a prefix before the server starts.
    #[allow(
        clippy::expect_used,
        reason = "the configuration check validates the prefix first"
    )]
    pub(crate) fn new(prefix: &str) -> Self {
        let name = |suffix: &str| {
            HeaderName::from_bytes(format!("{prefix}{suffix}").to_ascii_lowercase().as_bytes())
                .expect("checked header prefix")
        };
        Self {
            prefix: prefix.to_owned(),
            user: name("User"),
            catalog: name("Catalog"),
            schema: name("Schema"),
            source: name("Source"),
            session: name("Session"),
            set_catalog: name("Set-Catalog"),
            set_schema: name("Set-Schema"),
            set_session: name("Set-Session"),
            clear_session: name("Clear-Session"),
            client_capabilities: name("Client-Capabilities"),
            time_zone: name("Time-Zone"),
            prepared_statement: name("Prepared-Statement"),
            added_prepare: name("Added-Prepare"),
            deallocated_prepare: name("Deallocated-Prepare"),
        }
    }

    /// `<prefix><suffix>` as written in the configuration, for messages.
    pub(crate) fn shown(&self, suffix: &str) -> String {
        format!("{}{suffix}", self.prefix)
    }
}

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
        names: &HeaderNames,
    ) -> Result<Self, SessionError> {
        let user = header(headers, names.user.as_str())?
            .ok_or_else(|| SessionError::BadRequest(format!("User must be set ({})", names.shown("User"))))?;
        let catalog = header(headers, names.catalog.as_str())?;
        let schema = header(headers, names.schema.as_str())?;
        if schema.is_some() && catalog.is_none() {
            return Err(SessionError::BadRequest(format!(
                "Schema is set but catalog is not ({})",
                names.shown("Catalog")
            )));
        }
        let mut raw = BTreeMap::new();
        for text in texts(headers, names.session.as_str())? {
            for pair in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                let (name, value) = pair.split_once('=').ok_or_else(|| {
                    SessionError::BadRequest(format!("Invalid {} header: {pair}", names.shown("Session")))
                })?;
                let value = percent_decode(value.trim()).map_err(|_| {
                    SessionError::BadRequest(format!("Invalid {} header: {pair}", names.shown("Session")))
                })?;
                raw.insert(name.trim().to_owned(), value);
            }
        }
        let parametric_datetime = texts(headers, names.client_capabilities.as_str())?
            .iter()
            .flat_map(|text| text.split(','))
            .any(|capability| capability.trim().eq_ignore_ascii_case("PARAMETRIC_DATETIME"));
        let prepared = prepared::parse_header(
            texts(headers, names.prepared_statement.as_str())?.into_iter(),
            &names.shown("Prepared-Statement"),
        )
        .map_err(SessionError::BadRequest)?;
        // `SET TIME ZONE` sets the session's zone in place of the client's.
        let time_zone = raw
            .remove(TIME_ZONE_ID)
            .or(header(headers, names.time_zone.as_str())?)
            .map(|zone| zone_name(&zone).map_err(SessionError::Query))
            .transpose()?;
        Ok(Self {
            user,
            source: header(headers, names.source.as_str())?,
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

/// Session property `SET TIME ZONE` sets and `SET TIME ZONE LOCAL` clears:
/// the session's zone, in place of `X-Trino-Time-Zone`.
pub(crate) const TIME_ZONE_ID: &str = "time_zone_id";

/// The zone as Trino names it, or the error Trino answers.
fn zone_name(zone: &str) -> Result<String, QueryError> {
    codelake_core::trino_time_zone(zone)
        .ok_or_else(|| QueryError::new(Code::NotSupported, format!("Time zone not supported: {zone}")))
}

/// Statements that only change the client session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Control {
    Use {
        catalog: Option<String>,
        schema: String,
    },
    SetSession {
        name: String,
        value: String,
    },
    ResetSession {
        name: String,
    },
    /// `SET TIME ZONE 'zone'`, or `LOCAL` (`None`): back to the client's zone.
    SetTimeZone {
        zone: Option<String>,
    },
}

impl Control {
    pub(crate) fn update_type(&self) -> &'static str {
        match self {
            Self::Use { .. } => "USE",
            Self::SetSession { .. } => "SET SESSION",
            Self::ResetSession { .. } => "RESET SESSION",
            Self::SetTimeZone { .. } => "SET TIME ZONE",
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
    if let Some(rest) = strip_keyword(sql, "SET")
        .and_then(|r| strip_keyword(r, "TIME"))
        .and_then(|r| strip_keyword(r, "ZONE"))
    {
        if rest.eq_ignore_ascii_case("LOCAL") {
            return Ok(Some(Control::SetTimeZone { zone: None }));
        }
        let zone = rest
            .starts_with('\'')
            .then(|| literal(rest))
            .flatten()
            .ok_or_else(|| {
                QueryError::new(
                    Code::NotSupported,
                    "SET TIME ZONE takes a zone name ('America/Sao_Paulo', '-03:00') or LOCAL",
                )
            })?;
        return Ok(Some(Control::SetTimeZone {
            zone: Some(zone_name(&zone)?),
        }));
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
            &HeaderNames::new("X-Trino-"),
        )
        .unwrap();
        assert_eq!(session.user, "dbt");
        assert_eq!(session.catalog.as_deref(), Some("hive"));
        assert_eq!(session.schema.as_deref(), Some("streaming"));
        assert_eq!(session.source.as_deref(), Some("dbt-trino-1.8"));
        assert_eq!(session.properties.get("query_max_execution_time"), Some("30m"));
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
            ClientSession::from_headers(&headers(&[]), &registry, &HeaderNames::new("X-Trino-")),
            Err(SessionError::BadRequest(_))
        ));
        let unknown = ClientSession::from_headers(
            &headers(&[("X-Trino-User", "u"), ("X-Trino-Session", "nope=1")]),
            &registry,
            &HeaderNames::new("X-Trino-"),
        );
        assert_eq!(query_error(unknown), "INVALID_SESSION_PROPERTY");
        let bad = ClientSession::from_headers(
            &headers(&[
                ("X-Trino-User", "u"),
                ("X-Trino-Session", "query_max_run_time=soon"),
            ]),
            &registry,
            &HeaderNames::new("X-Trino-"),
        );
        assert_eq!(query_error(bad), "INVALID_SESSION_PROPERTY");
        for malformed in [
            &[("X-Trino-User", "u"), ("X-Trino-Schema", "s")][..],
            &[("X-Trino-User", "u"), ("X-Trino-Session", "no_equals")][..],
            &[("X-Trino-User", "u"), ("X-Trino-Prepared-Statement", "no_equals")][..],
        ] {
            assert!(matches!(
                ClientSession::from_headers(&headers(malformed), &registry, &HeaderNames::new("X-Trino-")),
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
                &HeaderNames::new("X-Trino-"),
            )
        };
        let plain = ClientSession::from_headers(
            &headers(&[("X-Trino-User", "u")]),
            &registry,
            &HeaderNames::new("X-Trino-"),
        )
        .unwrap();
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

    /// `SET TIME ZONE` answers `X-Trino-Set-Session: time_zone_id=…`; the
    /// client sends it back, and it wins over `X-Trino-Time-Zone`, as in
    /// Trino 431. It is not a property `SHOW SESSION` lists.
    #[test]
    fn the_zone_set_in_the_session_wins_over_the_header() {
        let registry = registry();
        let session = |pairs: &[(&'static str, &'static str)]| {
            ClientSession::from_headers(&headers(pairs), &registry, &HeaderNames::new("X-Trino-"))
        };
        let set = session(&[
            ("X-Trino-User", "u"),
            ("X-Trino-Time-Zone", "America/Sao_Paulo"),
            ("X-Trino-Session", "time_zone_id=Asia%2FTokyo"),
        ])
        .unwrap();
        assert_eq!(set.time_zone.as_deref(), Some("Asia/Tokyo"));
        assert_eq!(set.properties.get(TIME_ZONE_ID), None);
        assert!(set.properties.ignored().is_empty());
        let without_header =
            session(&[("X-Trino-User", "u"), ("X-Trino-Session", "time_zone_id=-03:00")]).unwrap();
        assert_eq!(without_header.time_zone.as_deref(), Some("-03:00"));
        assert_eq!(
            query_error(session(&[
                ("X-Trino-User", "u"),
                ("X-Trino-Session", "time_zone_id=Mars/Olympus")
            ])),
            "NOT_SUPPORTED"
        );
    }

    #[test]
    fn capabilities_and_prepared_statements_come_from_headers() {
        let registry = registry();
        let plain = ClientSession::from_headers(
            &headers(&[("X-Trino-User", "u")]),
            &registry,
            &HeaderNames::new("X-Trino-"),
        )
        .unwrap();
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
            &HeaderNames::new("X-Trino-"),
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
        assert_eq!(
            control(
                "SET TIME ZONE 'America/Sao_Paulo';",
                &none,
                &registry,
                &Everything
            )
            .unwrap(),
            Some(Control::SetTimeZone {
                zone: Some("America/Sao_Paulo".into())
            })
        );
        assert_eq!(
            control("set time zone '-03:00'", &none, &registry, &Everything).unwrap(),
            Some(Control::SetTimeZone {
                zone: Some("-03:00".into())
            })
        );
        assert_eq!(
            control("SET TIME ZONE local", &none, &registry, &Everything).unwrap(),
            Some(Control::SetTimeZone { zone: None })
        );
        for refused in [
            "SET TIME ZONE 'Mars/Olympus'",
            "SET TIME ZONE INTERVAL '-03:00' HOUR TO MINUTE",
            "SET TIME ZONE America/Sao_Paulo",
        ] {
            assert_eq!(
                control(refused, &none, &registry, &Everything)
                    .unwrap_err()
                    .name(),
                "NOT_SUPPORTED",
                "{refused}"
            );
        }
        assert_eq!(control("SELECT 1", &none, &registry, &Everything).unwrap(), None);
        assert_eq!(
            control("user_table_scan", &none, &registry, &Everything).unwrap(),
            None
        );
        assert_eq!(control("SETTINGS", &none, &registry, &Everything).unwrap(), None);
    }
}
