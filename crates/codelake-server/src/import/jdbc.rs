//! JDBC connection URLs of the PostgreSQL and SQL Server drivers, reduced to
//! what a `[[source]]` needs. Credentials embedded in a URL (`user:pass@`,
//! `password=`) are returned apart so the caller can treat them as secrets.
//!
//! Errors never quote the URL: it may carry a password.

use std::collections::BTreeMap;

/// Host, port, database and the remaining driver properties of a URL.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct JdbcUrl {
    pub(crate) host: String,
    pub(crate) port: Option<u16>,
    pub(crate) database: Option<String>,
    /// Driver properties, keys lower-cased (both drivers match them without
    /// regard to case). Holds `user` and `password` when the URL carries them.
    pub(crate) properties: BTreeMap<String, String>,
}

impl JdbcUrl {
    /// Removes and returns a property.
    pub(crate) fn take(&mut self, key: &str) -> Option<String> {
        self.properties.remove(key)
    }
}

fn parse_port(port: &str) -> Result<u16, &'static str> {
    port.parse::<u16>()
        .ok()
        .filter(|&p| p > 0)
        .ok_or("connection-url has an invalid port")
}

/// Splits `host[:port]`, with bracketed IPv6 hosts.
fn host_port(authority: &str) -> Result<(String, Option<u16>), &'static str> {
    if let Some(bracketed) = authority.strip_prefix('[') {
        let (host, after) = bracketed
            .split_once(']')
            .ok_or("connection-url has an invalid IPv6 host")?;
        let port = match after {
            "" => None,
            rest => Some(parse_port(
                rest.strip_prefix(':')
                    .ok_or("connection-url has an invalid port")?,
            )?),
        };
        return Ok((host.to_owned(), port));
    }
    match authority.rsplit_once(':') {
        Some((host, port)) => Ok((host.to_owned(), Some(parse_port(port)?))),
        None => Ok((authority.to_owned(), None)),
    }
}

/// Decodes `%XX` sequences of a URL query component.
fn percent_decode(text: &str) -> Result<String, &'static str> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = text
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or("connection-url has an invalid percent escape")?;
            out.push(hex);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "connection-url is not valid UTF-8 after decoding")
}

/// `jdbc:postgresql://host[:port]/database[?key=value&...]`.
pub(crate) fn postgres(url: &str) -> Result<JdbcUrl, &'static str> {
    let rest = url
        .strip_prefix("jdbc:postgresql://")
        .ok_or("connection-url does not start with jdbc:postgresql:")?;
    let (location, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, database) = location.split_once('/').unwrap_or((location, ""));
    let mut properties = BTreeMap::new();
    // The driver has no user-info syntax, but a URL written by hand may.
    let authority = match authority.rsplit_once('@') {
        Some((userinfo, host)) => {
            let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
            properties.insert("user".to_owned(), percent_decode(user)?);
            if !password.is_empty() {
                properties.insert("password".to_owned(), percent_decode(password)?);
            }
            host
        }
        None => authority,
    };
    if authority.contains(',') {
        return Err("connection-url lists several hosts (failover), which a source does not support");
    }
    let (host, port) = host_port(authority)?;
    if host.is_empty() {
        return Err("connection-url has no host");
    }
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        properties.insert(percent_decode(key)?.to_ascii_lowercase(), percent_decode(value)?);
    }
    let database = percent_decode(database)?;
    Ok(JdbcUrl {
        host,
        port,
        database: (!database.is_empty()).then_some(database),
        properties,
    })
}

/// Splits `;`-separated `key=value` pairs, honoring `{...}` braces around a
/// value (which may then contain `;`, and `}}` for a literal `}`).
fn sqlserver_pairs(text: &str) -> Result<Vec<(String, String)>, &'static str> {
    let mut pairs = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let Some((key, after)) = rest.split_once('=') else {
            if rest.trim().is_empty() {
                break;
            }
            return Err("connection-url has a property without a value");
        };
        let key = key.trim().to_ascii_lowercase();
        let (value, next) = if let Some(braced) = after.strip_prefix('{') {
            let mut value = String::new();
            let mut chars = braced.char_indices();
            let mut end = None;
            while let Some((at, c)) = chars.next() {
                if c == '}' {
                    if braced[at + 1..].starts_with('}') {
                        value.push('}');
                        chars.next();
                    } else {
                        end = Some(at + 1);
                        break;
                    }
                } else {
                    value.push(c);
                }
            }
            let end = end.ok_or("connection-url has an unclosed `{` in a property value")?;
            let tail = &braced[end..];
            (value, tail.strip_prefix(';').unwrap_or(tail))
        } else {
            match after.split_once(';') {
                Some((value, next)) => (value.to_owned(), next),
                None => (after.to_owned(), ""),
            }
        };
        if !key.is_empty() {
            pairs.push((key, value));
        }
        rest = next;
    }
    Ok(pairs)
}

/// `jdbc:sqlserver://[host[\instance][:port]][;key=value...]`.
pub(crate) fn sqlserver(url: &str) -> Result<JdbcUrl, &'static str> {
    let rest = url
        .strip_prefix("jdbc:sqlserver://")
        .ok_or("connection-url does not start with jdbc:sqlserver:")?;
    let (authority, props) = rest.split_once(';').unwrap_or((rest, ""));
    let mut properties = BTreeMap::new();
    for (key, value) in sqlserver_pairs(props)? {
        properties.insert(key, value);
    }
    let authority = if authority.is_empty() {
        properties.remove("servername").unwrap_or_default()
    } else {
        authority.to_owned()
    };
    let (server, port) = host_port(&authority)?;
    let (host, instance) = match server.split_once('\\') {
        Some((host, instance)) => (host.to_owned(), Some(instance.to_owned())),
        None => (server, properties.remove("instancename")),
    };
    if host.is_empty() {
        return Err("connection-url has no host");
    }
    let port = match (port, properties.remove("portnumber")) {
        (Some(port), _) => Some(port),
        (None, Some(port)) => Some(parse_port(&port)?),
        (None, None) => None,
    };
    if instance.is_some_and(|i| !i.is_empty()) && port.is_none() {
        return Err("connection-url names an instance without a port; a source connects to host:port");
    }
    let database = properties
        .remove("databasename")
        .or_else(|| properties.remove("database"));
    if let Some(user) = properties.remove("username") {
        properties.entry("user".to_owned()).or_insert(user);
    }
    Ok(JdbcUrl {
        host,
        port,
        database: database.filter(|d| !d.is_empty()),
        properties,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_url_with_port_database_and_parameters() {
        let mut url =
            postgres("jdbc:postgresql://db.local:6543/erp?sslmode=require&ApplicationName=x").unwrap();
        assert_eq!(url.host, "db.local");
        assert_eq!(url.port, Some(6543));
        assert_eq!(url.database.as_deref(), Some("erp"));
        assert_eq!(url.take("sslmode").as_deref(), Some("require"));
        assert_eq!(url.take("applicationname").as_deref(), Some("x"));
    }

    #[test]
    fn postgres_url_defaults_and_ipv6() {
        let url = postgres("jdbc:postgresql://[::1]/").unwrap();
        assert_eq!(url.host, "::1");
        assert_eq!(url.port, None);
        assert_eq!(url.database, None);
        let url = postgres("jdbc:postgresql://[::1]:5433/x").unwrap();
        assert_eq!(url.port, Some(5433));
    }

    #[test]
    fn postgres_credentials_in_the_url_are_extracted() {
        let url = postgres("jdbc:postgresql://u%40x:p%3Ass@h/db?password=q").unwrap();
        assert_eq!(url.host, "h");
        assert_eq!(url.properties["user"], "u@x");
        // The query parameter comes last and wins, as in the driver.
        assert_eq!(url.properties["password"], "q");
        let url = postgres("jdbc:postgresql://u:p%3Ass@h/db").unwrap();
        assert_eq!(url.properties["password"], "p:ss");
    }

    #[test]
    fn postgres_url_errors_never_quote_the_url() {
        for bad in [
            "jdbc:mysql://h/db",
            "jdbc:postgresql://h1,h2/db",
            "jdbc:postgresql://h:99999/db",
            "jdbc:postgresql://h:0/db",
            "jdbc:postgresql:///db",
            "jdbc:postgresql://h/db?password=%zz-secret",
        ] {
            let err = postgres(bad).unwrap_err();
            assert!(!err.contains("secret") && !err.contains("//"), "{err}");
        }
    }

    #[test]
    fn sqlserver_url_properties_are_case_insensitive() {
        let mut url = sqlserver(
            "jdbc:sqlserver://sql.local:1444;DatabaseName=Sales;encrypt=false;TrustServerCertificate=true;",
        )
        .unwrap();
        assert_eq!(url.host, "sql.local");
        assert_eq!(url.port, Some(1444));
        assert_eq!(url.database.as_deref(), Some("Sales"));
        assert_eq!(url.take("encrypt").as_deref(), Some("false"));
        assert_eq!(url.take("trustservercertificate").as_deref(), Some("true"));
        assert!(url.properties.is_empty());
    }

    #[test]
    fn sqlserver_url_braced_values_and_credentials() {
        let url = sqlserver("jdbc:sqlserver://h;user=sa;password={a;b}}c};database=d").unwrap();
        assert_eq!(url.properties["password"], "a;b}c");
        assert_eq!(url.properties["user"], "sa");
        assert_eq!(url.database.as_deref(), Some("d"));
        let url = sqlserver("jdbc:sqlserver://;serverName=h;portNumber=1500;userName=u").unwrap();
        assert_eq!((url.host.as_str(), url.port), ("h", Some(1500)));
        assert_eq!(url.properties["user"], "u");
    }

    #[test]
    fn sqlserver_named_instance_needs_a_port() {
        assert!(sqlserver("jdbc:sqlserver://h\\inst;databaseName=d").is_err());
        let url = sqlserver("jdbc:sqlserver://h\\inst:1500;databaseName=d").unwrap();
        assert_eq!((url.host.as_str(), url.port), ("h", Some(1500)));
    }

    #[test]
    fn sqlserver_url_errors_never_quote_the_url() {
        for bad in [
            "jdbc:postgresql://h/db",
            "jdbc:sqlserver://h;password={never-closed",
            "jdbc:sqlserver://h;justakey",
            "jdbc:sqlserver://;databaseName=d",
        ] {
            let err = sqlserver(bad).unwrap_err();
            assert!(!err.contains("never-closed") && !err.contains("//"), "{err}");
        }
    }
}
