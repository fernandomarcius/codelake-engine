//! Maps one catalog-properties file (`connector.name=…`) to a `[[source]]`.
//!
//! Secret values (passwords, access keys, credentials embedded in a URL) never
//! enter the source table: the table names an environment variable and the
//! value travels apart, wrapped in [`SecretString`], to the `.env` file. A
//! secret that already is `${ENV:NAME}` becomes a reference to `NAME` and is
//! not copied at all.
//!
//! A non-secret value with `${ENV:NAME}` references (a host, a database, a
//! user) is written as it is: `codelake.toml` resolves them when the server
//! loads it ([`codelake_core::env_refs`]). The import never reads the
//! environment. Where the import must understand a value to map it (a port,
//! a boolean, the scheme of a warehouse) a reference is refused.

use std::collections::BTreeMap;

use codelake_core::env_refs;
use codelake_core::parquet_time_zone::ParquetTimeZone;
use secrecy::SecretString;
use toml::{Table, Value};

use super::jdbc::{self, JdbcUrl};

/// A secret the `.env` file must carry.
#[derive(Debug)]
pub(crate) struct EnvSecret {
    pub(crate) var: String,
    pub(crate) value: SecretString,
}

/// A catalog that became a source.
#[derive(Debug)]
pub(crate) struct Imported {
    pub(crate) connector: String,
    pub(crate) source: Table,
    pub(crate) secrets: Vec<EnvSecret>,
    /// Secrets referenced through `${ENV:NAME}`: their `*_env` key names
    /// the variable, and the env file does not carry it.
    pub(crate) references: Vec<String>,
    /// Variables of `${ENV:NAME}` references kept in non-secret values,
    /// resolved when the server loads the configuration.
    pub(crate) value_references: Vec<String>,
    /// Remarks for the operator: names and settings only, never values.
    pub(crate) notes: Vec<String>,
}

/// Why a catalog was not imported. Never quotes a value.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Skipped {
    pub(crate) connector: String,
    pub(crate) reason: String,
}

/// Naming of one import run.
pub(crate) struct Context<'a> {
    /// Prefix of generated environment variable names (`CODELAKE_`).
    pub(crate) env_prefix: &'a str,
    /// Zone of hive catalogs without `hive.parquet.time-zone`: the
    /// reference engine's JVM time zone, given by the operator.
    pub(crate) reference_time_zone: Option<ParquetTimeZone>,
}

/// Whether a property holds a credential, by its name.
pub(crate) fn is_sensitive_key(key: &str) -> bool {
    const MARKERS: [&str; 8] = [
        "password",
        "secret",
        "access-key",
        "token",
        "credential",
        "private-key",
        "passphrase",
        "key-store",
    ];
    let key = key.to_ascii_lowercase();
    MARKERS.iter().any(|m| key.contains(m))
}

/// A non-secret value, kept as written: its `${ENV:NAME}` references are
/// checked and recorded, and resolved later by the server.
fn keep(key: &str, value: String, references: &mut Vec<String>) -> Result<String, String> {
    let names = env_refs::references(&value).map_err(|err| format!("`{key}` {err}"))?;
    references.extend(names.into_iter().map(str::to_owned));
    Ok(value)
}

/// Refuses a host or URL that carries credentials (`user:pass@host`,
/// `scheme://user:pass@host/…`). The message never quotes the value.
fn no_credentials(key: &str, value: &str) -> Result<(), String> {
    let authority = match value.split_once("://") {
        Some((_, rest)) => rest.split('/').next().unwrap_or_default(),
        None => value,
    };
    if authority.contains('@') {
        Err(format!(
            "`{key}` embeds credentials (`…@host`); pass them through the credential settings instead"
        ))
    } else {
        Ok(())
    }
}

/// A value the import must read (a port, a boolean): no reference allowed.
fn literal<'v>(key: &str, value: &'v str) -> Result<&'v str, String> {
    if value.contains(env_refs::REFERENCE_START) {
        Err(format!(
            "`{key}` uses ${{ENV:…}} where the import needs a literal value; write it literally"
        ))
    } else {
        Ok(value)
    }
}

/// Private-use characters that stand for `${ENV:…}` references while a URL is
/// parsed: they are none of the separators a JDBC URL uses (`:` `/` `;` `=`
/// `?` `&` `@`), so a reference can sit anywhere a literal could.
const MASK_OPEN: char = '\u{e000}';
const MASK_CLOSE: char = '\u{e001}';

/// Replaces each reference with `MASK_OPEN index MASK_CLOSE`.
fn mask(value: &str) -> Result<(String, Vec<String>), String> {
    if value.contains([MASK_OPEN, MASK_CLOSE]) {
        return Err("connection-url holds reserved characters".to_owned());
    }
    let mut out = String::with_capacity(value.len());
    let mut originals = Vec::new();
    let mut rest = value;
    while let Some(at) = rest.find(env_refs::REFERENCE_START) {
        let end = rest[at..]
            .find('}')
            .ok_or("`connection-url` has an unclosed ${ENV:…} reference")?;
        let reference = &rest[at..=at + end];
        env_refs::whole_reference(reference)
            .ok_or("`connection-url` references an invalid environment variable name")?;
        out.push_str(&rest[..at]);
        out.push(MASK_OPEN);
        out.push_str(&originals.len().to_string());
        out.push(MASK_CLOSE);
        originals.push(reference.to_owned());
        rest = &rest[at + end + 1..];
    }
    out.push_str(rest);
    Ok((out, originals))
}

/// Puts the references back into a masked piece of the URL.
fn unmask(value: &str, originals: &[String]) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(at) = rest.find(MASK_OPEN) {
        out.push_str(&rest[..at]);
        let after = &rest[at + MASK_OPEN.len_utf8()..];
        let end = after.find(MASK_CLOSE).unwrap_or(after.len());
        match after[..end].parse::<usize>().ok().and_then(|i| originals.get(i)) {
            Some(original) => out.push_str(original),
            None => out.push_str(&rest[at..at + MASK_OPEN.len_utf8() + end]),
        }
        rest = after.get(end + MASK_CLOSE.len_utf8()..).unwrap_or("");
    }
    out.push_str(rest);
    out
}

/// Upper-case environment variable name for a catalog's secret.
fn env_var(ctx: &Context<'_>, catalog: &str, suffix: &str) -> String {
    let catalog: String = catalog
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("{}{catalog}_{suffix}", ctx.env_prefix)
}

/// Properties of one catalog, consumed as they are mapped so that whatever
/// is left can be reported as ignored.
struct Props {
    map: BTreeMap<String, String>,
}

impl Props {
    fn take(&mut self, key: &str) -> Option<String> {
        self.map.remove(key)
    }

    /// Takes the first of several alias keys; the others are dropped.
    fn take_any(&mut self, keys: &[&str]) -> Option<String> {
        let mut found = None;
        for key in keys {
            if let Some(value) = self.map.remove(*key) {
                found.get_or_insert(value);
            }
        }
        found
    }

    fn ignored(self) -> Vec<String> {
        self.map.into_keys().collect()
    }
}

/// Builds the result of a secret setting: a reference when the value is
/// `${ENV:NAME}`, otherwise a generated variable that the `.env` file carries.
struct Secrets<'a> {
    ctx: &'a Context<'a>,
    catalog: &'a str,
    generated: Vec<EnvSecret>,
    references: Vec<String>,
    value_references: Vec<String>,
    /// Generated variables whose value only a literal reader of the env
    /// file keeps (see [`literal_only`]).
    literal_only: Vec<String>,
}

/// Whether a value would be changed by the env-file readers that interpret
/// their input: Compose `env_file` (`$` interpolation, ` #` comments, quotes,
/// trimmed whitespace) and systemd `EnvironmentFile` (quotes, backslash
/// escapes, trimmed whitespace). `docker --env-file` and `kubectl create
/// secret --from-env-file` read it literally.
pub(crate) fn literal_only(value: &str) -> bool {
    value.contains(['$', '#', '"', '\'', '\\', '`']) || value.trim() != value || value.contains('\t')
}

impl Secrets<'_> {
    fn bind(&mut self, key: &str, value: String, suffix: &str) -> Result<String, String> {
        if let Some(name) = env_refs::whole_reference(&value) {
            self.references.push(name.to_owned());
            return Ok(name.to_owned());
        }
        if value.contains(env_refs::REFERENCE_START) {
            return Err(format!(
                "`{key}` mixes ${{ENV:…}} with literal text; use one whole reference or a plain value"
            ));
        }
        if value.is_empty() {
            return Err(format!("`{key}` is empty"));
        }
        if value.contains(['\n', '\r', '\0']) {
            return Err(format!(
                "`{key}` holds a line break or NUL, which an env file cannot carry"
            ));
        }
        let var = env_var(self.ctx, self.catalog, suffix);
        if literal_only(&value) {
            self.literal_only.push(var.clone());
        }
        self.generated.push(EnvSecret {
            var: var.clone(),
            value: SecretString::from(value),
        });
        Ok(var)
    }
}

fn bool_value(key: &str, value: &str) -> Result<bool, String> {
    match literal(key, value)?.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("`{key}` must be true or false")),
    }
}

/// Maps a catalog file, or explains why it cannot be.
pub(crate) fn map_catalog(
    catalog: &str,
    properties: BTreeMap<String, String>,
    ctx: &Context<'_>,
) -> Result<Imported, Skipped> {
    let mut props = Props { map: properties };
    let connector = props.take("connector.name").unwrap_or_default();
    let skip = |reason: String| Skipped {
        connector: if connector.is_empty() {
            "?".to_owned()
        } else {
            connector.clone()
        },
        reason,
    };
    if connector.is_empty() {
        return Err(skip("no connector.name".to_owned()));
    }
    if !is_catalog_name(catalog) {
        return Err(skip(
            "the file name is not a valid source name ([a-z_][a-z0-9_]*, max 63 chars)".to_owned(),
        ));
    }
    let mut secrets = Secrets {
        ctx,
        catalog,
        generated: Vec::new(),
        references: Vec::new(),
        value_references: Vec::new(),
        literal_only: Vec::new(),
    };
    let mut notes = Vec::new();
    let mut source = Table::new();
    source.insert("name".into(), Value::String(catalog.to_owned()));
    let mapped = match connector.as_str() {
        "sqlserver" => sqlserver(&mut props, &mut secrets, &mut source, &mut notes),
        "postgresql" => postgres(&mut props, &mut secrets, &mut source, &mut notes),
        "hive" => hive(&mut props, &mut secrets, &mut source, &mut notes),
        other => Err(format!("connector `{other}` is not supported by the import")),
    };
    mapped.map_err(skip)?;
    if !secrets.literal_only.is_empty() {
        notes.push(format!(
            "the value of {} holds `$`, `#`, a quote, a backslash or edge whitespace: pass the env file \
             with `docker --env-file` or `kubectl create secret generic --from-env-file` (read \
             literally), not Compose `env_file` or systemd `EnvironmentFile` (they change it), or \
             reference the secret with ${{ENV:…}}",
            secrets.literal_only.join(", ")
        ));
    }
    let ignored = props.ignored();
    let sensitive: Vec<&String> = ignored.iter().filter(|k| is_sensitive_key(k)).collect();
    if !sensitive.is_empty() {
        notes.push(format!(
            "credential settings not carried over: {}",
            sensitive
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let plain: Vec<&String> = ignored.iter().filter(|k| !is_sensitive_key(k)).collect();
    if !plain.is_empty() {
        notes.push(format!(
            "ignored settings: {}",
            plain.iter().map(|k| k.as_str()).collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(Imported {
        connector,
        source,
        secrets: secrets.generated,
        references: secrets.references,
        value_references: secrets.value_references,
        notes,
    })
}

fn is_catalog_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && name.len() <= 63
        && !matches!(name, "datafusion" | "information_schema" | "system")
}

/// `connection-url`, `connection-user` and `connection-password` of a JDBC
/// catalog, with credentials the URL itself carries.
struct JdbcSettings {
    url: JdbcUrl,
    user: String,
    password_env: String,
}

fn jdbc_settings(
    props: &mut Props,
    secrets: &mut Secrets<'_>,
    parse: fn(&str) -> Result<JdbcUrl, &'static str>,
) -> Result<JdbcSettings, String> {
    let raw_url = props.take("connection-url").ok_or("no connection-url")?;
    // References are masked while the URL is parsed, so one can stand for a
    // host, a database, a user or a password; one standing for a port or
    // another value the import reads fails the parse.
    let (masked, originals) = mask(&raw_url)?;
    let mut url = parse(&masked).map_err(|problem| {
        if originals.is_empty() {
            problem.to_owned()
        } else {
            format!("{problem} (a ${{ENV:…}} reference stands where the import needs a literal?)")
        }
    })?;
    url.host = unmask(&url.host, &originals);
    url.database = url.database.map(|d| unmask(&d, &originals));
    for value in url.properties.values_mut() {
        *value = unmask(value, &originals);
    }
    let url_password = url.take("password");
    let url_user = url.take("user");
    let user = match props.take("connection-user") {
        Some(user) => user,
        None => url_user.ok_or("no connection-user (and no user in connection-url)")?,
    };
    if user.is_empty() {
        return Err("connection-user is empty".to_owned());
    }
    let user = keep("connection-user", user, &mut secrets.value_references)?;
    let password_env = match (props.take("connection-password"), url_password) {
        (Some(password), _) => secrets.bind("connection-password", password, "PASSWORD")?,
        (None, Some(password)) => secrets.bind("connection-url password", password, "PASSWORD")?,
        (None, None) => {
            return Err(
                "no connection-password (and none in connection-url); only password authentication is imported"
                    .to_owned(),
            );
        }
    };
    no_credentials("connection-url host", &url.host)?;
    url.host = keep(
        "connection-url host",
        std::mem::take(&mut url.host),
        &mut secrets.value_references,
    )?;
    if let Some(database) = url.database.take() {
        url.database = Some(keep(
            "connection-url database",
            database,
            &mut secrets.value_references,
        )?);
    }
    Ok(JdbcSettings {
        url,
        user,
        password_env,
    })
}

fn insert_connection(source: &mut Table, kind: &str, settings: &JdbcSettings, database: String) {
    source.insert("kind".into(), Value::String(kind.to_owned()));
    source.insert("host".into(), Value::String(settings.url.host.clone()));
    if let Some(port) = settings.url.port {
        source.insert("port".into(), Value::Integer(i64::from(port)));
    }
    source.insert("database".into(), Value::String(database));
    source.insert("user".into(), Value::String(settings.user.clone()));
    source.insert(
        "password_env".into(),
        Value::String(settings.password_env.clone()),
    );
}

fn url_leftovers(url: &JdbcUrl, notes: &mut Vec<String>) {
    let names: Vec<&str> = url.properties.keys().map(String::as_str).collect();
    if !names.is_empty() {
        notes.push(format!("ignored connection-url properties: {}", names.join(", ")));
    }
}

fn sqlserver(
    props: &mut Props,
    secrets: &mut Secrets<'_>,
    source: &mut Table,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let mut settings = jdbc_settings(props, secrets, jdbc::sqlserver)?;
    let database = settings
        .url
        .database
        .clone()
        .ok_or("connection-url has no databaseName; a source reads one database")?;
    // The JDBC driver (12.x) encrypts by default.
    let encrypt = match settings
        .url
        .take("encrypt")
        .map(|e| literal("encrypt", &e).map(str::to_ascii_lowercase))
        .transpose()?
    {
        None => "required",
        Some(e) if e == "true" || e == "mandatory" => "required",
        Some(e) if e == "false" || e == "optional" || e == "no" => "off",
        Some(e) if e == "strict" => {
            notes.push("encrypt=strict (TDS 8) imported as `required` (TDS 7.x with TLS)".to_owned());
            "required"
        }
        Some(_) => return Err("connection-url has an unknown encrypt value".to_owned()),
    };
    let trust = match settings.url.take("trustservercertificate") {
        Some(value) => bool_value("trustServerCertificate", &value)?,
        None => false,
    };
    // With encrypt=false the JDBC driver still encrypts the login, without
    // validating the certificate; `off` does the same only with
    // `trust_server_certificate`.
    let trust = trust || encrypt == "off";
    insert_connection(source, "sqlserver", &settings, database);
    source.insert("encrypt".into(), Value::String(encrypt.to_owned()));
    if trust {
        source.insert("trust_server_certificate".into(), Value::Boolean(true));
    }
    if encrypt == "off" {
        notes.push(
            "encrypt = \"off\": data travel in clear text; the login is encrypted without \
             certificate validation (trust_server_certificate = true), as with the JDBC driver"
                .to_owned(),
        );
    }
    url_leftovers(&settings.url, notes);
    Ok(())
}

fn postgres(
    props: &mut Props,
    secrets: &mut Secrets<'_>,
    source: &mut Table,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    const MODES: [&str; 5] = ["disable", "prefer", "require", "verify-ca", "verify-full"];
    let mut settings = jdbc_settings(props, secrets, jdbc::postgres)?;
    let database = settings
        .url
        .database
        .clone()
        .ok_or("connection-url has no database")?;
    let ssl = settings.url.take("ssl");
    let sslmode = match settings
        .url
        .take("sslmode")
        .map(|m| literal("sslmode", &m).map(str::to_owned))
        .transpose()?
    {
        Some(mode) if MODES.contains(&mode.as_str()) => mode,
        Some(_) => return Err("connection-url has an unknown sslmode".to_owned()),
        // `ssl=true` without a mode means verify-full in the driver.
        None if ssl.as_deref() == Some("true") => "verify-full".to_owned(),
        None => {
            notes.push(
                "no sslmode in connection-url: imported the driver's default `prefer`; prefer verify-full"
                    .to_owned(),
            );
            "prefer".to_owned()
        }
    };
    insert_connection(source, "postgres", &settings, database);
    source.insert("sslmode".into(), Value::String(sslmode));
    postgres_decimals(props, source)?;
    url_leftovers(&settings.url, notes);
    Ok(())
}

/// The reference engine's `decimal-mapping`, `decimal-rounding-mode` and
/// `decimal-default-scale` (ISS-060), when the catalog sets them; their
/// values in lower case.
fn postgres_decimals(props: &mut Props, source: &mut Table) -> Result<(), String> {
    const MAPPINGS: [&str; 2] = ["STRICT", "ALLOW_OVERFLOW"];
    const MODES: [&str; 8] = [
        "UP",
        "DOWN",
        "CEILING",
        "FLOOR",
        "HALF_UP",
        "HALF_DOWN",
        "HALF_EVEN",
        "UNNECESSARY",
    ];
    for (key, setting, values) in [
        ("decimal-mapping", "decimal_mapping", &MAPPINGS[..]),
        ("decimal-rounding-mode", "decimal_rounding_mode", &MODES[..]),
    ] {
        if let Some(value) = props.take(key) {
            let value = literal(key, &value)?.trim().to_ascii_uppercase();
            if !values.contains(&value.as_str()) {
                return Err(format!("{key} is not one of {}", values.join(", ")));
            }
            source.insert(setting.into(), Value::String(value.to_ascii_lowercase()));
        }
    }
    if let Some(scale) = props.take("decimal-default-scale") {
        let scale = literal("decimal-default-scale", &scale)?
            .trim()
            .parse::<u8>()
            .ok()
            .filter(|scale| *scale <= 38)
            .ok_or("decimal-default-scale is not an integer from 0 to 38")?;
        source.insert("decimal_default_scale".into(), Value::Integer(i64::from(scale)));
    }
    Ok(())
}

/// Local directory or `s3://` URL of a file metastore's catalog directory.
fn warehouse(dir: &str) -> Result<String, String> {
    if let Some(path) = dir.strip_prefix("file://") {
        // file:///data → /data; file://host/data is not local.
        return if path.starts_with('/') {
            Ok(path.to_owned())
        } else {
            Err("hive.metastore.catalog.dir names a remote file:// host".to_owned())
        };
    }
    if let Some(path) = dir.strip_prefix("file:") {
        return if path.starts_with('/') {
            Ok(path.to_owned())
        } else {
            Err("hive.metastore.catalog.dir must be an absolute path".to_owned())
        };
    }
    if dir.starts_with('/') {
        return Ok(dir.to_owned());
    }
    for scheme in ["s3://", "s3a://", "s3n://"] {
        if let Some(rest) = dir.strip_prefix(scheme) {
            if rest.is_empty() || rest.starts_with('/') {
                return Err("hive.metastore.catalog.dir has no bucket".to_owned());
            }
            return Ok(format!("s3://{rest}"));
        }
    }
    Err("hive.metastore.catalog.dir uses a file system other than a local path or S3".to_owned())
}

fn hive(
    props: &mut Props,
    secrets: &mut Secrets<'_>,
    source: &mut Table,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    match props.take("hive.metastore").as_deref() {
        Some("file") => {}
        // Known metastore kinds are named; anything else is not quoted.
        Some(other @ ("thrift" | "glue")) => {
            return Err(format!(
                "hive.metastore={other} is not supported; only the file metastore is"
            ));
        }
        Some(_) => {
            return Err("hive.metastore is not a supported kind; only the file metastore is".to_owned());
        }
        None => return Err("hive.metastore is not set (the default is a thrift metastore)".to_owned()),
    }
    let dir = props
        .take("hive.metastore.catalog.dir")
        .ok_or("no hive.metastore.catalog.dir")?;
    no_credentials("hive.metastore.catalog.dir", &dir)?;
    let location = warehouse(&dir).map_err(|problem| {
        if dir.contains(env_refs::REFERENCE_START) {
            format!(
                "{problem}; with ${{ENV:…}}, write the scheme or the leading `/` literally \
                 (`/${{ENV:X}}`, `s3://${{ENV:X}}`) so the import can tell local from S3"
            )
        } else {
            problem
        }
    })?;
    let location = keep(
        "hive.metastore.catalog.dir",
        location,
        &mut secrets.value_references,
    )?;
    let s3 = location.starts_with("s3://");
    source.insert("kind".into(), Value::String("hive".to_owned()));
    source.insert("warehouse".into(), Value::String(location));
    hive_s3(props, secrets, source, notes, s3)?;
    if let Some(precision) = props.take("hive.timestamp-precision") {
        let precision = literal("hive.timestamp-precision", &precision)?
            .trim()
            .to_ascii_uppercase();
        if !matches!(
            precision.as_str(),
            "MILLISECONDS" | "MICROSECONDS" | "NANOSECONDS"
        ) {
            return Err(
                "hive.timestamp-precision is not MILLISECONDS, MICROSECONDS or NANOSECONDS".to_owned(),
            );
        }
        source.insert("timestamp_precision".into(), Value::String(precision));
    }
    hive_time_zone(props, secrets.ctx, source, notes)
}

/// `hive.parquet.time-zone` → `parquet_time_zone`. Without it, the
/// reference engine converts `INT96` timestamps with its JVM time zone, which
/// the catalog file does not show: the operator names it
/// (`--reference-time-zone`), or the catalog is skipped. A guessed zone
/// would shift every `INT96` timestamp silently.
fn hive_time_zone(
    props: &mut Props,
    ctx: &Context<'_>,
    source: &mut Table,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let zone = if let Some(zone) = props.take("hive.parquet.time-zone") {
        literal("hive.parquet.time-zone", &zone)?
            .trim()
            .parse::<ParquetTimeZone>()
            .map_err(|_| "hive.parquet.time-zone is not an IANA time zone name".to_owned())?
    } else {
        let zone = ctx.reference_time_zone.ok_or(
            "hive.parquet.time-zone is not set: the reference engine then uses its JVM time zone, \
             which the catalog file does not show; pass --reference-time-zone <IANA zone>",
        )?;
        notes.push(
            "hive.parquet.time-zone not set: parquet_time_zone is the --reference-time-zone".to_owned(),
        );
        zone
    };
    source.insert("parquet_time_zone".into(), Value::String(zone.name().to_owned()));
    Ok(())
}

/// S3 settings of a hive catalog: keys to the env file, endpoint, region and
/// addressing into the source. A local warehouse ignores them with a note.
fn hive_s3(
    props: &mut Props,
    secrets: &mut Secrets<'_>,
    source: &mut Table,
    notes: &mut Vec<String>,
    s3: bool,
) -> Result<(), String> {
    let access = props.take_any(&["hive.s3.aws-access-key", "s3.aws-access-key"]);
    let secret = props.take_any(&["hive.s3.aws-secret-key", "s3.aws-secret-key"]);
    let endpoint = props.take_any(&["hive.s3.endpoint", "s3.endpoint"]);
    let ssl = props.take("hive.s3.ssl.enabled");
    let region = props.take_any(&["hive.s3.region", "s3.region"]);
    let path_style = props.take_any(&["hive.s3.path-style-access", "s3.path-style-access"]);
    if s3 {
        match (access, secret) {
            (Some(access), Some(secret)) => {
                let access = secrets.bind("hive.s3.aws-access-key", access, "S3_ACCESS_KEY_ID")?;
                let secret = secrets.bind("hive.s3.aws-secret-key", secret, "S3_SECRET_ACCESS_KEY")?;
                source.insert("access_key_id_env".into(), Value::String(access));
                source.insert("secret_access_key_env".into(), Value::String(secret));
            }
            (None, None) => notes.push("no static S3 keys: the ambient credential chain is used".to_owned()),
            _ => return Err("set both S3 access key and secret key, or neither".to_owned()),
        }
        let ssl = match ssl {
            Some(value) => bool_value("hive.s3.ssl.enabled", &value)?,
            None => true,
        };
        if let Some(endpoint) = endpoint {
            no_credentials("hive.s3.endpoint", &endpoint)?;
            let endpoint = keep("hive.s3.endpoint", endpoint, &mut secrets.value_references)?;
            let endpoint = if endpoint.starts_with("http://")
                || endpoint.starts_with("https://")
                || endpoint.starts_with(env_refs::REFERENCE_START)
            {
                // A leading reference is taken as the whole URL, scheme included.
                endpoint
            } else {
                format!("{}://{endpoint}", if ssl { "https" } else { "http" })
            };
            source.insert("s3_endpoint".into(), Value::String(endpoint));
        }
        if let Some(region) = region {
            source.insert(
                "s3_region".into(),
                Value::String(keep("hive.s3.region", region, &mut secrets.value_references)?),
            );
        }
        if let Some(path_style) = path_style {
            source.insert(
                "s3_path_style".into(),
                Value::Boolean(bool_value("hive.s3.path-style-access", &path_style)?),
            );
        }
        notes.push(
            "S3 warehouse: needs a build with the `hive-s3` feature (not in the release image)".to_owned(),
        );
    } else {
        let set: Vec<&str> = [
            ("s3 access key", access.is_some()),
            ("s3 secret key", secret.is_some()),
            ("s3 endpoint", endpoint.is_some()),
        ]
        .iter()
        .filter(|(_, set)| *set)
        .map(|(name, _)| *name)
        .collect();
        if !set.is_empty() {
            notes.push(format!("local warehouse: ignored {}", set.join(", ")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    const SECRET: &str = "Very-Secret-Value-9";

    fn ctx() -> Context<'static> {
        Context {
            env_prefix: "CODELAKE_",
            reference_time_zone: Some(ParquetTimeZone::UTC),
        }
    }

    fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn import(catalog: &str, pairs: &[(&str, &str)]) -> Result<Imported, Skipped> {
        map_catalog(catalog, props(pairs), &ctx())
    }

    fn rendered(imported: &Imported) -> String {
        toml::to_string(&imported.source).unwrap()
    }

    #[test]
    fn sqlserver_catalog_becomes_a_source_with_a_password_reference() {
        let imported = import(
            "erp1",
            &[
                ("connector.name", "sqlserver"),
                (
                    "connection-url",
                    "jdbc:sqlserver://sql.local:1433;databaseName=Sales;encrypt=false;trustServerCertificate=true",
                ),
                ("connection-user", "reader"),
                ("connection-password", SECRET),
            ],
        )
        .unwrap();
        let s = &imported.source;
        assert_eq!(s["kind"].as_str(), Some("sqlserver"));
        assert_eq!(s["host"].as_str(), Some("sql.local"));
        assert_eq!(s["port"].as_integer(), Some(1433));
        assert_eq!(s["database"].as_str(), Some("Sales"));
        assert_eq!(s["user"].as_str(), Some("reader"));
        assert_eq!(s["password_env"].as_str(), Some("CODELAKE_ERP1_PASSWORD"));
        assert_eq!(s["encrypt"].as_str(), Some("off"));
        assert_eq!(s["trust_server_certificate"].as_bool(), Some(true));
        assert_eq!(imported.secrets.len(), 1);
        assert_eq!(imported.secrets[0].var, "CODELAKE_ERP1_PASSWORD");
        assert_eq!(imported.secrets[0].value.expose_secret(), SECRET);
        assert!(!rendered(&imported).contains(SECRET));
        assert!(!format!("{imported:?}").contains(SECRET));
    }

    #[test]
    fn sqlserver_encryption_follows_the_driver_default() {
        let base = [
            ("connector.name", "sqlserver"),
            ("connection-url", "jdbc:sqlserver://h;databaseName=d"),
            ("connection-user", "u"),
            ("connection-password", SECRET),
        ];
        let imported = import("a", &base).unwrap();
        assert_eq!(imported.source["encrypt"].as_str(), Some("required"));
        assert!(
            imported.source.get("port").is_none(),
            "the source default applies"
        );
        assert!(imported.source.get("trust_server_certificate").is_none());
        let mut off = base.to_vec();
        off[1] = (
            "connection-url",
            "jdbc:sqlserver://h;databaseName=d;encrypt=false",
        );
        let imported = import("a", &off).unwrap();
        assert_eq!(imported.source["encrypt"].as_str(), Some("off"));
        assert_eq!(
            imported.source["trust_server_certificate"].as_bool(),
            Some(true),
            "login-only encryption without validation, as the JDBC driver"
        );
    }

    #[test]
    fn password_inside_the_url_goes_to_the_env_file() {
        let imported = import(
            "src",
            &[
                ("connector.name", "sqlserver"),
                (
                    "connection-url",
                    &format!("jdbc:sqlserver://h:1433;databaseName=d;user=sa;password={{{SECRET}}}"),
                ),
            ],
        )
        .unwrap();
        assert_eq!(imported.source["user"].as_str(), Some("sa"));
        assert_eq!(imported.secrets[0].value.expose_secret(), SECRET);
        let shown = format!("{}{:?}", rendered(&imported), imported.notes);
        assert!(!shown.contains(SECRET), "{shown}");
    }

    #[test]
    fn env_reference_is_kept_as_a_reference() {
        let imported = import(
            "pg",
            &[
                ("connector.name", "postgresql"),
                (
                    "connection-url",
                    "jdbc:postgresql://h:5432/erp?sslmode=verify-full",
                ),
                ("connection-user", "${ENV:PG_USER}"),
                ("connection-password", "${ENV:PG_PASSWORD}"),
            ],
        )
        .unwrap();
        assert_eq!(imported.source["password_env"].as_str(), Some("PG_PASSWORD"));
        // Resolved by the server when it loads the configuration.
        assert_eq!(imported.source["user"].as_str(), Some("${ENV:PG_USER}"));
        assert_eq!(imported.value_references, vec!["PG_USER"]);
        assert_eq!(imported.source["sslmode"].as_str(), Some("verify-full"));
        assert!(imported.secrets.is_empty(), "a reference is not copied");
        assert_eq!(imported.references, vec!["PG_PASSWORD"]);
    }

    #[test]
    fn references_where_a_literal_is_needed_or_mixed_with_a_secret_skip_the_catalog() {
        let imported = import(
            "pg",
            &[
                ("connector.name", "postgresql"),
                (
                    "connection-url",
                    "jdbc:postgresql://${ENV:PG_HOST}:5432/erp_${ENV:STAGE}",
                ),
                ("connection-user", "u"),
                ("connection-password", SECRET),
            ],
        )
        .unwrap();
        assert_eq!(imported.source["host"].as_str(), Some("${ENV:PG_HOST}"));
        assert_eq!(imported.source["database"].as_str(), Some("erp_${ENV:STAGE}"));
        assert_eq!(imported.value_references, vec!["PG_HOST", "STAGE"]);
        for (url, expected) in [
            ("jdbc:postgresql://h:${ENV:PORT}/erp", "invalid port"),
            ("jdbc:postgresql://h/erp?sslmode=${ENV:MODE}", "needs a literal"),
            ("jdbc:postgresql://h/${ENV:DB", "unclosed"),
            (
                "jdbc:postgresql://h/${ENV:9}",
                "invalid environment variable name",
            ),
        ] {
            let skipped = import(
                "pg",
                &[
                    ("connector.name", "postgresql"),
                    ("connection-url", url),
                    ("connection-user", "u"),
                    ("connection-password", SECRET),
                ],
            )
            .unwrap_err();
            assert!(skipped.reason.contains(expected), "{url}: {skipped:?}");
        }
        let skipped = import(
            "pg",
            &[
                ("connector.name", "postgresql"),
                ("connection-url", "jdbc:postgresql://h/erp"),
                ("connection-user", "u"),
                ("connection-password", "pre${ENV:X}"),
            ],
        )
        .unwrap_err();
        assert!(skipped.reason.contains("mixes"), "{skipped:?}");
        assert!(!skipped.reason.contains("pre"), "{skipped:?}");
    }

    #[test]
    fn env_reference_inside_the_url_stays_a_reference() {
        let imported = import(
            "src",
            &[
                ("connector.name", "sqlserver"),
                (
                    "connection-url",
                    "jdbc:sqlserver://h:1433;databaseName=${ENV:SRC_DB};user=sa;password=${ENV:SRC_PW}",
                ),
            ],
        )
        .unwrap();
        assert_eq!(imported.source["password_env"].as_str(), Some("SRC_PW"));
        assert_eq!(imported.source["database"].as_str(), Some("${ENV:SRC_DB}"));
        assert_eq!(imported.references, vec!["SRC_PW"]);
        assert!(imported.secrets.is_empty());
        let skipped = import(
            "src",
            &[
                ("connector.name", "sqlserver"),
                (
                    "connection-url",
                    "jdbc:sqlserver://h;databaseName=d;trustServerCertificate=${ENV:TRUST}",
                ),
                ("connection-user", "u"),
                ("connection-password", SECRET),
            ],
        )
        .unwrap_err();
        assert!(skipped.reason.contains("needs a literal"), "{skipped:?}");
    }

    #[test]
    fn postgres_without_sslmode_imports_the_driver_default_with_a_note() {
        let imported = import(
            "erp",
            &[
                ("connector.name", "postgresql"),
                ("connection-url", "jdbc:postgresql://h/erp"),
                ("connection-user", "u"),
                ("connection-password", SECRET),
                ("case-insensitive-name-matching", "true"),
            ],
        )
        .unwrap();
        assert_eq!(imported.source["sslmode"].as_str(), Some("prefer"));
        assert!(imported.notes.iter().any(|n| n.contains("prefer")));
        assert!(
            imported
                .notes
                .iter()
                .any(|n| n.contains("ignored settings: case-insensitive-name-matching"))
        );
    }

    #[test]
    fn postgres_decimal_settings_are_imported() {
        let base = [
            ("connector.name", "postgresql"),
            ("connection-url", "jdbc:postgresql://h/erp?sslmode=verify-full"),
            ("connection-user", "u"),
            ("connection-password", SECRET),
        ];
        let imported = import("hub", &base).unwrap();
        for key in [
            "decimal_mapping",
            "decimal_rounding_mode",
            "decimal_default_scale",
        ] {
            assert!(imported.source.get(key).is_none(), "{key}");
        }
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut props = base.to_vec();
            props.extend_from_slice(extra);
            import("hub", &props)
        };
        let imported = with(&[
            ("decimal-mapping", "ALLOW_OVERFLOW"),
            ("decimal-rounding-mode", "half_even"),
            ("decimal-default-scale", "4"),
        ])
        .unwrap();
        assert_eq!(
            imported.source["decimal_mapping"].as_str(),
            Some("allow_overflow")
        );
        assert_eq!(
            imported.source["decimal_rounding_mode"].as_str(),
            Some("half_even")
        );
        assert_eq!(imported.source["decimal_default_scale"].as_integer(), Some(4));
        assert!(
            !imported.notes.iter().any(|n| n.contains("decimal")),
            "{:?}",
            imported.notes
        );
        for (key, value, reason) in [
            (
                "decimal-mapping",
                "LOOSE",
                "decimal-mapping is not one of STRICT, ALLOW_OVERFLOW",
            ),
            (
                "decimal-rounding-mode",
                "NEAREST",
                "decimal-rounding-mode is not one of UP",
            ),
            (
                "decimal-default-scale",
                "39",
                "decimal-default-scale is not an integer from 0 to 38",
            ),
            (
                "decimal-default-scale",
                "-1",
                "decimal-default-scale is not an integer from 0 to 38",
            ),
        ] {
            let skipped = with(&[(key, value)]).unwrap_err();
            assert!(skipped.reason.contains(reason), "{skipped:?}");
        }
    }

    #[test]
    fn local_hive_file_metastore() {
        for (dir, expected) in [
            ("file:/data/wh", "/data/wh"),
            ("file:///data/wh", "/data/wh"),
            ("/data/wh", "/data/wh"),
        ] {
            let imported = import(
                "hive",
                &[
                    ("connector.name", "hive"),
                    ("hive.metastore", "file"),
                    ("hive.metastore.catalog.dir", dir),
                    ("hive.security", "allow-all"),
                    ("hive.timestamp-precision", "microseconds"),
                ],
            )
            .unwrap();
            assert_eq!(imported.source["kind"].as_str(), Some("hive"));
            assert_eq!(imported.source["warehouse"].as_str(), Some(expected));
            assert_eq!(
                imported.source["timestamp_precision"].as_str(),
                Some("MICROSECONDS")
            );
            assert!(imported.secrets.is_empty());
            assert!(imported.notes.iter().any(|n| n.contains("hive.security")));
        }
    }

    #[test]
    fn hive_parquet_time_zone_is_carried_over_or_must_be_named() {
        let base = [
            ("connector.name", "hive"),
            ("hive.metastore", "file"),
            ("hive.metastore.catalog.dir", "/wh"),
        ];
        let with = |zone: &str| {
            let mut pairs = base.to_vec();
            pairs.push(("hive.parquet.time-zone", zone));
            import("lake", &pairs)
        };
        let imported = with(" America/Sao_Paulo ").unwrap();
        assert_eq!(
            imported.source["parquet_time_zone"].as_str(),
            Some("America/Sao_Paulo")
        );
        assert!(imported.notes.is_empty(), "{:?}", imported.notes);
        for bad in ["-03:00", "Brasilia", "${ENV:TZ}"] {
            let skipped = with(bad).unwrap_err();
            assert!(skipped.reason.contains("hive.parquet.time-zone"), "{skipped:?}");
            assert!(!skipped.reason.contains(bad), "{skipped:?}");
        }
        // Unset: the operator names the reference engine's zone, or the
        // catalog is skipped rather than guessed.
        let imported = import("lake", &base).unwrap();
        assert_eq!(imported.source["parquet_time_zone"].as_str(), Some("UTC"));
        assert!(imported.notes.iter().any(|n| n.contains("--reference-time-zone")));
        let ctx = Context {
            env_prefix: "CODELAKE_",
            reference_time_zone: None,
        };
        let skipped = map_catalog("lake", props(&base), &ctx).unwrap_err();
        assert!(
            skipped.reason.contains("pass --reference-time-zone"),
            "{skipped:?}"
        );
    }

    #[test]
    fn s3_hive_file_metastore_keeps_keys_out_of_the_source() {
        let imported = import(
            "lake",
            &[
                ("connector.name", "hive"),
                ("hive.metastore", "file"),
                ("hive.metastore.catalog.dir", "s3a://bucket/warehouse"),
                ("hive.s3.aws-access-key", "AKIA-EXAMPLE-ACCESS"),
                ("hive.s3.aws-secret-key", SECRET),
                ("hive.s3.endpoint", "minio:9000"),
                ("hive.s3.ssl.enabled", "false"),
                ("hive.s3.path-style-access", "true"),
                ("hive.s3.region", "sa-east-1"),
            ],
        )
        .unwrap();
        let s = &imported.source;
        assert_eq!(s["warehouse"].as_str(), Some("s3://bucket/warehouse"));
        assert_eq!(s["s3_endpoint"].as_str(), Some("http://minio:9000"));
        assert_eq!(s["s3_path_style"].as_bool(), Some(true));
        assert_eq!(s["s3_region"].as_str(), Some("sa-east-1"));
        assert_eq!(
            s["access_key_id_env"].as_str(),
            Some("CODELAKE_LAKE_S3_ACCESS_KEY_ID")
        );
        assert_eq!(
            s["secret_access_key_env"].as_str(),
            Some("CODELAKE_LAKE_S3_SECRET_ACCESS_KEY")
        );
        let text = rendered(&imported);
        assert!(!text.contains(SECRET) && !text.contains("AKIA-EXAMPLE"), "{text}");
        assert_eq!(imported.secrets.len(), 2);
    }

    #[test]
    fn half_s3_credentials_are_refused() {
        let skipped = import(
            "lake",
            &[
                ("connector.name", "hive"),
                ("hive.metastore", "file"),
                ("hive.metastore.catalog.dir", "s3://b/p"),
                ("hive.s3.aws-access-key", "k"),
            ],
        )
        .unwrap_err();
        assert!(skipped.reason.contains("both"), "{skipped:?}");
    }

    #[test]
    fn unsupported_catalogs_are_skipped_with_a_reason() {
        let cases: [(&[(&str, &str)], &str); 8] = [
            (
                &[
                    ("connector.name", "hive"),
                    ("hive.metastore.uri", "thrift://meta:9083"),
                ],
                "hive.metastore is not set",
            ),
            (
                &[("connector.name", "hive"), ("hive.metastore", "thrift")],
                "hive.metastore=thrift is not supported",
            ),
            (
                &[("connector.name", "hive"), ("hive.metastore", SECRET)],
                "hive.metastore is not a supported kind",
            ),
            (
                &[("connector.name", "iceberg"), ("iceberg.catalog.type", "jdbc")],
                "connector `iceberg` is not supported",
            ),
            (
                &[
                    ("connector.name", "mongodb"),
                    ("mongodb.connection-url", "mongodb://u:p@h/"),
                ],
                "connector `mongodb` is not supported",
            ),
            (&[("hive.metastore", "file")], "no connector.name"),
            (
                &[
                    ("connector.name", "hive"),
                    ("hive.metastore", "file"),
                    ("hive.metastore.catalog.dir", "hdfs://nn/wh"),
                ],
                "other than a local path or S3",
            ),
            (
                &[
                    ("connector.name", "sqlserver"),
                    ("connection-url", "jdbc:sqlserver://h:1433"),
                    ("connection-user", "u"),
                    ("connection-password", SECRET),
                ],
                "no databaseName",
            ),
        ];
        for (pairs, expected) in cases {
            let skipped = import("c", pairs).unwrap_err();
            assert!(skipped.reason.contains(expected), "{expected}: {skipped:?}");
            assert!(!skipped.reason.contains(SECRET));
        }
    }

    #[test]
    fn invalid_catalog_names_are_skipped() {
        for name in ["Bad-Name", "9x", "system", "information_schema"] {
            let skipped = import(name, &[("connector.name", "hive")]).unwrap_err();
            assert!(skipped.reason.contains("not a valid source name"), "{skipped:?}");
        }
    }

    #[test]
    fn missing_credentials_are_reported() {
        let skipped = import(
            "c",
            &[
                ("connector.name", "postgresql"),
                ("connection-url", "jdbc:postgresql://h/d"),
                ("connection-user", "u"),
            ],
        )
        .unwrap_err();
        assert!(skipped.reason.contains("no connection-password"), "{skipped:?}");
        let skipped = import(
            "c",
            &[
                ("connector.name", "postgresql"),
                ("connection-url", "jdbc:postgresql://h/d"),
                ("connection-password", SECRET),
            ],
        )
        .unwrap_err();
        assert!(skipped.reason.contains("no connection-user"), "{skipped:?}");
    }

    #[test]
    fn secrets_that_only_a_literal_reader_keeps_are_named() {
        for (value, flagged) in [
            ("plain-Secret_1", false),
            ("pa$$word", true),
            ("abc #tail", true),
            ("\"quoted\"", true),
            ("it's", true),
            ("back\\slash", true),
            (" edge", true),
            ("edge ", true),
        ] {
            assert_eq!(literal_only(value), flagged, "{value:?}");
        }
        let imported = import(
            "c",
            &[
                ("connector.name", "postgresql"),
                ("connection-url", "jdbc:postgresql://h/d"),
                ("connection-user", "u"),
                ("connection-password", "s3cr$t #x"),
            ],
        )
        .unwrap();
        let notes = imported.notes.join("; ");
        assert!(
            notes.contains("the value of CODELAKE_C_PASSWORD holds"),
            "{notes}"
        );
        assert!(!notes.contains("s3cr$t"), "{notes}");
    }

    #[test]
    fn secrets_with_line_breaks_cannot_go_to_an_env_file() {
        let skipped = import(
            "c",
            &[
                ("connector.name", "postgresql"),
                ("connection-url", "jdbc:postgresql://h/d"),
                ("connection-user", "u"),
                ("connection-password", "a\nb"),
            ],
        )
        .unwrap_err();
        assert!(skipped.reason.contains("line break"), "{skipped:?}");
    }

    #[test]
    fn unknown_credential_settings_are_named_not_shown() {
        let imported = import(
            "lake",
            &[
                ("connector.name", "hive"),
                ("hive.metastore", "file"),
                ("hive.metastore.catalog.dir", "/wh"),
                ("hive.s3.session-token", SECRET),
            ],
        )
        .unwrap();
        let notes = imported.notes.join("\n");
        assert!(notes.contains("credential settings not carried over: hive.s3.session-token"));
        assert!(!notes.contains(SECRET));
    }

    #[test]
    fn credentials_embedded_in_a_host_or_url_are_refused_without_quoting_them() {
        let cases: [&[(&str, &str)]; 3] = [
            &[
                ("connector.name", "sqlserver"),
                (
                    "connection-url",
                    "jdbc:sqlserver://sa:Very-Secret-Value-9@h:1433;databaseName=d",
                ),
                ("connection-user", "u"),
                ("connection-password", SECRET),
            ],
            &[
                ("connector.name", "hive"),
                ("hive.metastore", "file"),
                (
                    "hive.metastore.catalog.dir",
                    "s3://AKIA:Very-Secret-Value-9@bucket/wh",
                ),
            ],
            &[
                ("connector.name", "hive"),
                ("hive.metastore", "file"),
                ("hive.metastore.catalog.dir", "s3://bucket/wh"),
                ("hive.s3.endpoint", "http://k:Very-Secret-Value-9@minio:9000"),
            ],
        ];
        for pairs in cases {
            let skipped = import("c", pairs).unwrap_err();
            assert!(skipped.reason.contains("embeds credentials"), "{skipped:?}");
            assert!(!skipped.reason.contains(SECRET), "{skipped:?}");
        }
    }

    #[test]
    fn hive_references_keep_the_scheme_literal() {
        let imported = import(
            "lake",
            &[
                ("connector.name", "hive"),
                ("hive.metastore", "file"),
                ("hive.metastore.catalog.dir", "s3://${ENV:BUCKET}/wh"),
                ("hive.s3.endpoint", "${ENV:S3_URL}"),
                ("hive.s3.aws-access-key", "${ENV:KEY_ID}"),
                ("hive.s3.aws-secret-key", "${ENV:KEY_SECRET}"),
            ],
        )
        .unwrap();
        let s = &imported.source;
        assert_eq!(s["warehouse"].as_str(), Some("s3://${ENV:BUCKET}/wh"));
        assert_eq!(s["s3_endpoint"].as_str(), Some("${ENV:S3_URL}"));
        assert_eq!(s["access_key_id_env"].as_str(), Some("KEY_ID"));
        assert_eq!(s["secret_access_key_env"].as_str(), Some("KEY_SECRET"));
        assert!(imported.secrets.is_empty());
        let skipped = import(
            "lake",
            &[
                ("connector.name", "hive"),
                ("hive.metastore", "file"),
                ("hive.metastore.catalog.dir", "${ENV:WAREHOUSE}"),
            ],
        )
        .unwrap_err();
        assert!(skipped.reason.contains("literally"), "{skipped:?}");
    }

    #[test]
    fn sensitive_key_names() {
        for key in [
            "connection-password",
            "hive.s3.aws-secret-key",
            "hive.s3.aws-access-key",
            "s3.session-token",
            "connection-credential-file",
        ] {
            assert!(is_sensitive_key(key), "{key}");
        }
        for key in ["connection-url", "connection-user", "hive.s3.endpoint"] {
            assert!(!is_sensitive_key(key), "{key}");
        }
    }

    #[test]
    fn env_var_names_are_upper_case_with_the_prefix() {
        let ctx = Context {
            env_prefix: "SHADOW_",
            reference_time_zone: None,
        };
        assert_eq!(env_var(&ctx, "erp_2", "PASSWORD"), "SHADOW_ERP_2_PASSWORD");
    }
}
