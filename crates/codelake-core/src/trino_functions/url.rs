//! URL functions (Trino "URL functions").
//!
//! Trino parses with `java.net.URI`: no normalization, NULL for text that is
//! not a valid URI, decoded path/query/fragment, and the raw query for
//! `url_extract_parameter`.

use std::fmt::Write as _;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::ScalarUDF;

use super::support::{
    At, Func, UTF8, as_str, coerce_each, int_array, invalid_argument, string_array, strings,
};

#[derive(Debug, Default, PartialEq, Eq)]
struct Uri<'a> {
    scheme: Option<&'a str>,
    host: Option<&'a str>,
    port: Option<i64>,
    path: &'a str,
    query: Option<&'a str>,
    fragment: Option<&'a str>,
}

/// Characters `java.net.URI` never accepts unescaped.
fn has_illegal_char(text: &str) -> bool {
    text.chars()
        .any(|c| c.is_control() || matches!(c, ' ' | '"' | '<' | '>' | '\\' | '^' | '`' | '{' | '|' | '}'))
}

fn valid_escapes(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if !(bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit))
            {
                return false;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    true
}

fn parse_uri(text: &str) -> Option<Uri<'_>> {
    if text.is_empty() || has_illegal_char(text) || !valid_escapes(text) {
        return None;
    }
    let (rest, fragment) = match text.split_once('#') {
        Some((r, f)) => (r, Some(f)),
        None => (text, None),
    };
    if fragment.is_some_and(|f| f.contains('#')) {
        return None;
    }
    let (scheme, rest) = match rest.find(':') {
        Some(i)
            if rest[..i].chars().next().is_some_and(|c| c.is_ascii_alphabetic())
                && rest[..i]
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
                && !rest[..i].contains(['/', '?']) =>
        {
            (Some(&rest[..i]), &rest[i + 1..])
        }
        _ => (None, rest),
    };
    let (rest, query) = match rest.split_once('?') {
        Some((r, q)) => (r, Some(q)),
        None => (rest, None),
    };
    let mut uri = Uri {
        scheme,
        query,
        fragment,
        ..Uri::default()
    };
    if let Some(after) = rest.strip_prefix("//") {
        let end = after.find('/').unwrap_or(after.len());
        let authority = &after[..end];
        uri.path = &after[end..];
        let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
        let (host, port) = match host_port.rsplit_once(':') {
            Some((h, p)) if !h.ends_with(']') || h.starts_with('[') => {
                if p.is_empty() {
                    (h, None)
                } else {
                    (h, Some(p.parse::<i64>().ok()?))
                }
            }
            _ => (host_port, None),
        };
        uri.host = (!host.is_empty()).then_some(host);
        uri.port = port;
    } else if scheme.is_some() && !rest.starts_with('/') {
        // An opaque URI (`mailto:x@y.com`) has no path and no query.
        uri.path = "";
        uri.query = None;
    } else {
        uri.path = rest;
    }
    Some(uri)
}

/// Percent-decoding (`+` stays `+`); `None` for invalid UTF-8.
fn percent_decode(text: &str, plus_is_space: bool) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = text.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' if plus_is_space => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn map_uri(
    call: &super::support::Call<'_>,
    extract: fn(&Uri<'_>) -> Option<String>,
) -> Result<datafusion::arrow::array::ArrayRef> {
    let urls = strings(call.arg(0)?)?;
    Ok(string_array(
        (0..call.rows)
            .map(|i| parse_uri(urls.at(i)?).and_then(|u| extract(&u)))
            .collect(),
    ))
}

fn parameter(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then(|| percent_decode(value, true).unwrap_or_else(|| value.to_owned()))
    })
}

fn url_encode(text: &str) -> String {
    let mut out = String::new();
    for b in text.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'*' | b'_' => out.push(char::from(b)),
            b' ' => out.push('+'),
            other => {
                let _ = write!(out, "%{other:02X}");
            }
        }
    }
    out
}

fn one(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    coerce_each(name, types, &[as_str])
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        Func::new("url_extract_protocol", one, UTF8, |c| {
            // Trino returns '' for an absent component (`nullToEmpty`).
            map_uri(c, |u| Some(u.scheme.unwrap_or_default().to_owned()))
        })
        .udf(),
        Func::new("url_extract_host", one, UTF8, |c| {
            map_uri(c, |u| Some(u.host.unwrap_or_default().to_owned()))
        })
        .udf(),
        Func::new("url_extract_path", one, UTF8, |c| {
            map_uri(c, |u| percent_decode(u.path, false))
        })
        .udf(),
        Func::new("url_extract_query", one, UTF8, |c| {
            map_uri(c, |u| percent_decode(u.query.unwrap_or_default(), false))
        })
        .udf(),
        Func::new("url_extract_fragment", one, UTF8, |c| {
            map_uri(c, |u| percent_decode(u.fragment.unwrap_or_default(), false))
        })
        .udf(),
        Func::new(
            "url_extract_port",
            one,
            |_| Ok(DataType::Int64),
            |c| {
                let urls = strings(c.arg(0)?)?;
                Ok(int_array(
                    (0..c.rows)
                        .map(|i| parse_uri(urls.at(i)?).and_then(|u| u.port))
                        .collect(),
                ))
            },
        )
        .udf(),
        Func::new(
            "url_extract_parameter",
            |n, t| coerce_each(n, t, &[as_str, as_str]),
            UTF8,
            |c| {
                let urls = strings(c.arg(0)?)?;
                let names = strings(c.arg(1)?)?;
                Ok(string_array(
                    (0..c.rows)
                        .map(|i| {
                            let uri = parse_uri(urls.at(i)?)?;
                            parameter(uri.query?, names.at(i)?)
                        })
                        .collect(),
                ))
            },
        )
        .udf(),
        Func::new("url_encode", one, UTF8, |c| {
            let text = strings(c.arg(0)?)?;
            Ok(string_array(
                (0..c.rows).map(|i| text.at(i).map(url_encode)).collect(),
            ))
        })
        .udf(),
        Func::new("url_decode", one, UTF8, |c| {
            let text = strings(c.arg(0)?)?;
            let out = (0..c.rows)
                .map(|i| {
                    text.at(i)
                        .map(|t| {
                            percent_decode(t, true).ok_or_else(|| {
                                invalid_argument(format!(
                                    "URLDecoder: Illegal hex characters in escape (%) pattern: {t}"
                                ))
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        })
        .udf(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_like_java_net_uri() {
        let uri = parse_uri("https://user@example.com:8080/a%20b/c?x=1&y=%C3%A7#frag").unwrap();
        assert_eq!(uri.scheme, Some("https"));
        assert_eq!(uri.host, Some("example.com"));
        assert_eq!(uri.port, Some(8080));
        assert_eq!(percent_decode(uri.path, false).as_deref(), Some("/a b/c"));
        assert_eq!(parameter(uri.query.unwrap(), "y").as_deref(), Some("ç"));
        assert_eq!(uri.fragment, Some("frag"));
        assert!(parse_uri("http://a b").is_none());
        assert!(parse_uri("http://a/%zz").is_none());
        let relative = parse_uri("/path/only?q").unwrap();
        assert_eq!(relative.host, None);
        assert_eq!(relative.path, "/path/only");
    }

    #[test]
    fn encoding() {
        assert_eq!(url_encode("a b/ç*"), "a+b%2F%C3%A7*");
        assert_eq!(percent_decode("a+b%2F", true).as_deref(), Some("a b/"));
    }
}
