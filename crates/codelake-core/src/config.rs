//! Configuration model and fail-fast validation.
//!
//! Rules enforced here:
//! - Configuration lives outside the artifact (a TOML file plus environment variables).
//! - Secrets are never written in the file: sources reference *environment variable
//!   names* and the values are resolved into [`SecretString`] at startup.
//! - Validation reports **every** problem at once, so an operator fixes the file in
//!   one pass instead of restarting the process once per mistake.

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use secrecy::SecretString;
use serde::Deserialize;

use crate::bytes::ByteSize;

/// Root configuration document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub memory: MemoryConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    /// Distributed tracing over OTLP. Off unless `otlp_endpoint` is set.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    #[serde(default, rename = "source")]
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// PostgreSQL wire protocol listener (clients, BI tools).
    pub pg_listen: SocketAddr,
    /// Operational HTTP listener: `/livez`, `/readyz`, `/healthz`, `/metrics`.
    pub http_listen: SocketAddr,
    /// Hard cap on concurrent client connections. `0` is rejected: unbounded
    /// connections are unbounded memory.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Seconds to keep serving in-flight work after SIGTERM before exiting.
    #[serde(default = "default_shutdown_grace_secs")]
    pub shutdown_grace_secs: u64,
    pub tls: Option<TlsConfig>,
    /// Explicit opt-in to expose the unauthenticated PostgreSQL listener beyond
    /// loopback (e.g. behind a sidecar proxy or a Kubernetes `NetworkPolicy`).
    #[serde(default)]
    pub allow_unauthenticated_network: bool,
    /// Trino client protocol (`/v1/statement`). Disabled when absent.
    pub trino: Option<TrinoProtocolConfig>,
}

/// Trino client protocol listener, for Trino clients (JDBC, the CLI, the
/// Python client, dbt-trino) and plain HTTP scripts.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrinoProtocolConfig {
    pub listen: SocketAddr,
    /// Queries accepted and not yet finished; more are refused with
    /// `QUERY_QUEUE_FULL`.
    #[serde(default = "default_trino_max_active_queries")]
    pub max_active_queries: usize,
    /// A query whose client stops polling for this long is canceled
    /// (`ABANDONED_QUERY`), as with Trino's `query.client.timeout`.
    #[serde(default = "default_trino_client_timeout_secs")]
    pub client_timeout_secs: u64,
    /// Completed queries kept in `system.runtime.queries`.
    #[serde(default = "default_trino_history_size")]
    pub history_size: usize,
    #[serde(default = "default_trino_history_retention_secs")]
    pub history_retention_secs: u64,
    /// Longest SQL text accepted, in characters; longer statements fail with
    /// `QUERY_TEXT_TOO_LARGE`, as with Trino's `query.max-length`.
    #[serde(default = "default_trino_max_query_length")]
    pub max_query_length: usize,
}

const fn default_trino_max_active_queries() -> usize {
    100
}

const fn default_trino_client_timeout_secs() -> u64 {
    300
}

const fn default_trino_history_size() -> usize {
    100
}

const fn default_trino_history_retention_secs() -> u64 {
    900
}

/// Trino's `query.max-length` default.
const fn default_trino_max_query_length() -> usize {
    1_000_000
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryConfig {
    /// Upper bound for memory tracked by the query engine (joins, sorts,
    /// aggregations). Above it, operators spill to `spill_dir` or fail the query;
    /// the process is never allowed to grow unbounded.
    pub limit: ByteSize,
    /// Directory for spill files. Use a dedicated volume with its own size limit.
    pub spill_dir: PathBuf,
    /// Maximum disk used for spill across all queries.
    pub spill_limit: ByteSize,
    /// Cache for Parquet/Iceberg file metadata.
    #[serde(default = "default_metadata_cache")]
    pub metadata_cache_limit: ByteSize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    /// Rows per Arrow record batch. Smaller batches lower peak memory per operator.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Parallelism per query. Defaults to the number of CPUs.
    pub target_partitions: Option<usize>,
    /// Seconds allowed for connecting to every source at startup.
    #[serde(default = "default_startup_timeout_secs")]
    pub source_startup_timeout_secs: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            target_partitions: None,
            source_startup_timeout_secs: default_startup_timeout_secs(),
        }
    }
}

/// `[telemetry]`: OpenTelemetry traces exported over OTLP.
///
/// Disabled by default. When `otlp_endpoint` is absent no exporter, no
/// background thread and no OpenTelemetry layer exist; the engine's spans only
/// feed the JSON logs, as before.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Base URL of an OTLP/HTTP receiver, e.g. `http://otel-collector:4318`.
    /// Spans go to `<endpoint>/v1/traces`. Plain `http://` only: point it at a
    /// collector agent on the node or in the pod, which handles TLS upstream.
    pub otlp_endpoint: Option<String>,
    /// OTLP transport. Only `http/protobuf` is supported; `grpc` is refused
    /// instead of silently falling back.
    #[serde(default = "default_otlp_protocol")]
    pub protocol: String,
    /// `service.name` resource attribute.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Fraction of new traces recorded, `0.0`–`1.0`. A query that arrives with
    /// a W3C `traceparent` follows the caller's sampling decision instead.
    #[serde(default = "default_sampling_ratio")]
    pub sampling_ratio: f64,
    /// Seconds allowed for one export request before it is dropped.
    #[serde(default = "default_export_timeout_secs")]
    pub export_timeout_secs: u64,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            protocol: default_otlp_protocol(),
            service_name: default_service_name(),
            sampling_ratio: default_sampling_ratio(),
            export_timeout_secs: default_export_timeout_secs(),
        }
    }
}

/// Why `endpoint` cannot be an OTLP/HTTP base URL, if it cannot: the
/// exporter must be able to use every value validation accepts, and
/// `/v1/traces` is appended to it, so a query or fragment would swallow that
/// path. Accepted: `http://host[:port][/path]`, host a DNS name, an IPv4
/// address or a bracketed IPv6 address, port 1–65535, no credentials.
fn check_otlp_endpoint(endpoint: &str) -> Result<(), String> {
    let Some(rest) = endpoint.strip_prefix("http://") else {
        return Err(if endpoint.starts_with("https://") {
            "uses https: TLS to the collector is not supported; export to a collector agent \
             over http:// on the node or in the pod"
                .to_owned()
        } else {
            "must be an http:// URL (OTLP/HTTP, port 4318 by default)".to_owned()
        });
    };
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    if authority.is_empty() || rest.contains(|c: char| c.is_whitespace() || c.is_control()) {
        return Err("has no host".to_owned());
    }
    if rest.contains(['?', '#']) {
        return Err("must not have a query or fragment: `/v1/traces` is appended to it".to_owned());
    }
    if authority.contains('@') {
        return Err("must not carry credentials".to_owned());
    }
    if !path.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("has an invalid path".to_owned());
    }
    let (host, port) = match authority.strip_prefix('[') {
        Some(bracketed) => {
            let (host, after) = bracketed.split_once(']').ok_or("has an invalid IPv6 host")?;
            if host.parse::<std::net::Ipv6Addr>().is_err() {
                return Err("has an invalid IPv6 host".to_owned());
            }
            match after {
                "" => (host, None),
                port => (host, Some(port.strip_prefix(':').ok_or("has an invalid port")?)),
            }
        }
        None => match authority.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        },
    };
    if host.is_empty() {
        return Err("has no host".to_owned());
    }
    if !authority.starts_with('[') {
        let label_ok = |label: &str| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        };
        // URL parsers read a host whose last label is numeric as IPv4.
        let numeric = host
            .rsplit('.')
            .next()
            .is_some_and(|l| l.bytes().all(|b| b.is_ascii_digit()));
        if !host.split('.').all(label_ok) || (numeric && host.parse::<std::net::Ipv4Addr>().is_err()) {
            return Err(format!("has an invalid host `{host}`"));
        }
    }
    if let Some(port) = port
        && !(port.bytes().all(|b| b.is_ascii_digit()) && port.parse::<u16>().is_ok_and(|p| p > 0))
    {
        return Err(format!("has an invalid port `{port}` (1-65535)"));
    }
    Ok(())
}

impl TelemetryConfig {
    /// `<endpoint>/v1/traces`, when tracing is enabled.
    pub fn traces_url(&self) -> Option<String> {
        self.otlp_endpoint.as_ref().map(|endpoint| {
            let base = endpoint.trim_end_matches('/');
            if base.ends_with("/v1/traces") {
                base.to_owned()
            } else {
                format!("{base}/v1/traces")
            }
        })
    }
}

/// One federated source, exposed to SQL as catalog `<name>`.
///
/// No `deny_unknown_fields` here: serde does not support it together with
/// `flatten`. Unknown keys are rejected by [`SourceKind`] instead.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub name: String,
    /// When `false`, a source that cannot be reached at startup is skipped with a
    /// warning instead of aborting the process.
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(flatten)]
    pub kind: SourceKind,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceKind {
    Postgres {
        host: String,
        #[serde(default = "default_pg_port")]
        port: u16,
        database: String,
        user: String,
        /// Name of the environment variable holding the password.
        password_env: String,
        /// `disable`, `prefer`, `require`, `verify-ca` or `verify-full`.
        #[serde(default = "default_sslmode")]
        sslmode: String,
    },
    Mysql {
        host: String,
        #[serde(default = "default_mysql_port")]
        port: u16,
        database: String,
        user: String,
        /// Name of the environment variable holding the password.
        password_env: String,
        /// `disabled`, `preferred` or `required`.
        #[serde(default = "default_mysql_sslmode")]
        sslmode: String,
    },
    Sqlserver {
        host: String,
        #[serde(default = "default_mssql_port")]
        port: u16,
        database: String,
        user: String,
        /// Name of the environment variable holding the password.
        password_env: String,
        /// `required` (default), `on` or `off`. `off` sends credentials in clear
        /// text: only for isolated test networks.
        #[serde(default = "default_mssql_encrypt")]
        encrypt: String,
        /// Accept the server certificate without validation (self-signed
        /// development servers). Never in production.
        #[serde(default)]
        trust_server_certificate: bool,
        /// Maximum pooled connections to this server.
        #[serde(default = "default_pool_size")]
        pool_size: u32,
    },
    Mongodb {
        /// Name of the environment variable holding the full connection string.
        connection_string_env: String,
    },
    /// Parquet warehouse in Trino's file-metastore layout (`hive.metastore=file`).
    Hive {
        /// Absolute local path (`/data/warehouse`, `file:/data/warehouse`) or
        /// `s3://bucket/prefix`.
        warehouse: String,
        /// S3-compatible endpoint (`MinIO`); omit for AWS S3 or local disk.
        s3_endpoint: Option<String>,
        #[serde(default = "default_region")]
        s3_region: String,
        access_key_id_env: Option<String>,
        secret_access_key_env: Option<String>,
        #[serde(default)]
        s3_path_style: bool,
        /// Precision of the catalog's `timestamp` columns, Trino's
        /// `hive.timestamp-precision`: reads round to it, writes round to it,
        /// and DDL of another precision is refused, as in Trino.
        #[serde(default)]
        timestamp_precision: HiveTimestampPrecision,
    },
    Iceberg {
        /// Iceberg REST catalog URI.
        catalog_uri: String,
        warehouse: Option<String>,
        /// S3-compatible endpoint (`MinIO`); omit for AWS S3.
        s3_endpoint: Option<String>,
        #[serde(default = "default_region")]
        s3_region: String,
        /// Environment variable names for static credentials. Omit both to use
        /// the ambient credential chain (IRSA, instance profile, ...).
        access_key_id_env: Option<String>,
        secret_access_key_env: Option<String>,
        /// Path-style addressing, required by most `MinIO` deployments.
        #[serde(default)]
        s3_path_style: bool,
    },
}

impl SourceKind {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Postgres { .. } => "postgres",
            Self::Mysql { .. } => "mysql",
            Self::Sqlserver { .. } => "sqlserver",
            Self::Mongodb { .. } => "mongodb",
            Self::Iceberg { .. } => "iceberg",
            Self::Hive { .. } => "hive",
        }
    }

    /// Environment variables this source needs, in declaration order.
    pub fn secret_env_vars(&self) -> Vec<&str> {
        match self {
            Self::Postgres { password_env, .. }
            | Self::Mysql { password_env, .. }
            | Self::Sqlserver { password_env, .. } => {
                vec![password_env.as_str()]
            }
            Self::Mongodb {
                connection_string_env,
            } => vec![connection_string_env.as_str()],
            Self::Iceberg {
                access_key_id_env,
                secret_access_key_env,
                ..
            }
            | Self::Hive {
                access_key_id_env,
                secret_access_key_env,
                ..
            } => access_key_id_env
                .iter()
                .chain(secret_access_key_env)
                .map(String::as_str)
                .collect(),
        }
    }
}

const fn default_max_connections() -> usize {
    256
}
const fn default_shutdown_grace_secs() -> u64 {
    30
}
const fn default_batch_size() -> usize {
    8192
}
const fn default_startup_timeout_secs() -> u64 {
    60
}
const fn default_metadata_cache() -> ByteSize {
    ByteSize::mib(64)
}
const fn default_pg_port() -> u16 {
    5432
}
const fn default_mysql_port() -> u16 {
    3306
}
const fn default_mssql_port() -> u16 {
    1433
}
const fn default_pool_size() -> u32 {
    10
}
const fn default_true() -> bool {
    true
}
fn default_sslmode() -> String {
    "verify-full".to_owned()
}
fn default_mysql_sslmode() -> String {
    "required".to_owned()
}
fn default_mssql_encrypt() -> String {
    "required".to_owned()
}
/// Trino's `hive.timestamp-precision` (default `MILLISECONDS`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HiveTimestampPrecision {
    #[default]
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl HiveTimestampPrecision {
    /// Fractional digits of the precision (3, 6, 9).
    pub fn digits(self) -> u8 {
        match self {
            Self::Milliseconds => 3,
            Self::Microseconds => 6,
            Self::Nanoseconds => 9,
        }
    }

    /// The name Trino prints in messages.
    pub fn name(self) -> &'static str {
        match self {
            Self::Milliseconds => "MILLISECONDS",
            Self::Microseconds => "MICROSECONDS",
            Self::Nanoseconds => "NANOSECONDS",
        }
    }
}

fn default_region() -> String {
    "us-east-1".to_owned()
}
fn default_otlp_protocol() -> String {
    OTLP_PROTOCOLS[0].to_owned()
}
fn default_service_name() -> String {
    "codelake".to_owned()
}
const fn default_sampling_ratio() -> f64 {
    1.0
}
const fn default_export_timeout_secs() -> u64 {
    10
}

/// Every validation problem found, reported together.
#[derive(Debug, thiserror::Error)]
#[error("invalid configuration:\n  - {}", .0.join("\n  - "))]
pub struct ConfigErrors(pub Vec<String>);

/// Errors while loading configuration from disk.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("cannot read config file {path}: {source}")]
    Read { path: PathBuf, source: std::io::Error },
    #[error("cannot parse config file {path}: {source}")]
    Parse {
        path: PathBuf,
        source: Box<toml::de::Error>,
    },
    #[error(transparent)]
    Invalid(#[from] ConfigErrors),
}

/// Secrets resolved from the environment, keyed by environment variable name.
#[derive(Debug, Default)]
pub struct Secrets(BTreeMap<String, SecretString>);

impl Secrets {
    pub fn get(&self, env_var: &str) -> Option<&SecretString> {
        self.0.get(env_var)
    }
}

/// Configuration that passed validation, with its secrets resolved.
#[derive(Debug)]
pub struct ValidatedConfig {
    pub config: Config,
    pub secrets: Secrets,
}

const MIN_MEMORY_LIMIT: ByteSize = ByteSize::mib(64);
const MAX_BATCH_SIZE: usize = 1 << 20;
const PG_SSL_MODES: [&str; 5] = ["disable", "prefer", "require", "verify-ca", "verify-full"];
const MYSQL_SSL_MODES: [&str; 3] = ["disabled", "preferred", "required"];
const MSSQL_ENCRYPT: [&str; 3] = ["required", "on", "off"];
const MAX_POOL_SIZE: u32 = 256;
const OTLP_PROTOCOLS: [&str; 1] = ["http/protobuf"];
const MAX_EXPORT_TIMEOUT_SECS: u64 = 300;
const MAX_SERVICE_NAME: usize = 255;

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && name.len() <= 63
}

fn check_sql_source(
    name: &str,
    host: &str,
    (field, value): (&str, &str),
    allowed: &[&str],
    errors: &mut Vec<String>,
) {
    if !allowed.contains(&value) {
        errors.push(format!(
            "source `{name}`: {field} `{value}` is not one of {allowed:?}"
        ));
    }
    if host.trim().is_empty() {
        errors.push(format!("source `{name}`: host is empty"));
    }
}

impl Config {
    /// Parses and validates a config file, resolving secrets from the process
    /// environment.
    pub fn load(path: &Path) -> Result<ValidatedConfig, LoadError> {
        let raw = std::fs::read_to_string(path).map_err(|source| LoadError::Read {
            path: path.to_owned(),
            source,
        })?;
        let config: Config = toml::from_str(&raw).map_err(|source| LoadError::Parse {
            path: path.to_owned(),
            source: Box::new(source),
        })?;
        Ok(config.validate(&|name| std::env::var(name).ok(), &|p| p.is_file())?)
    }

    /// Validates the whole document. `env` and `file_exists` are injected so the
    /// rules are testable without touching the real process environment.
    pub fn validate(
        self,
        env: &dyn Fn(&str) -> Option<String>,
        file_exists: &dyn Fn(&Path) -> bool,
    ) -> Result<ValidatedConfig, ConfigErrors> {
        let mut errors = Vec::new();
        self.validate_server(file_exists, &mut errors);
        self.validate_memory(&mut errors);
        self.validate_execution(&mut errors);
        self.validate_telemetry(&mut errors);
        let secrets = self.validate_sources(env, &mut errors);
        if errors.is_empty() {
            Ok(ValidatedConfig {
                config: self,
                secrets,
            })
        } else {
            Err(ConfigErrors(errors))
        }
    }

    fn validate_server(&self, file_exists: &dyn Fn(&Path) -> bool, errors: &mut Vec<String>) {
        let server = &self.server;
        if server.max_connections == 0 {
            errors.push("server.max_connections must be > 0 (0 means unlimited)".into());
        }
        // The PostgreSQL wire listener has no client authentication yet (see
        // ROADMAP M1). Until it does, exposing it beyond loopback would hand every
        // federated source to anyone who reaches the port.
        if !server.pg_listen.ip().is_loopback() && !server.allow_unauthenticated_network {
            errors.push(format!(
                "server.pg_listen {} is not loopback and client authentication is not implemented yet; \
                 bind to 127.0.0.1 or set server.allow_unauthenticated_network = true inside an isolated network",
                server.pg_listen
            ));
        }
        if server.pg_listen == server.http_listen {
            errors.push("server.pg_listen and server.http_listen must differ".into());
        }
        if let Some(trino) = &server.trino {
            Self::validate_trino(server, trino, errors);
        }
        // datafusion-postgres silently falls back to plaintext when the TLS files
        // cannot be loaded. Refuse to start instead of downgrading.
        if let Some(tls) = &server.tls {
            for (field, path) in [("cert_path", &tls.cert_path), ("key_path", &tls.key_path)] {
                if !file_exists(path) {
                    errors.push(format!("server.tls.{field} {} does not exist", path.display()));
                }
            }
        }
    }

    fn validate_trino(server: &ServerConfig, trino: &TrinoProtocolConfig, errors: &mut Vec<String>) {
        // Same rule as the PostgreSQL listener: no client authentication yet.
        if !trino.listen.ip().is_loopback() && !server.allow_unauthenticated_network {
            errors.push(format!(
                "server.trino.listen {} is not loopback and client authentication is not implemented yet; \
                 bind to 127.0.0.1 or set server.allow_unauthenticated_network = true inside an isolated network",
                trino.listen
            ));
        }
        if trino.listen == server.pg_listen || trino.listen == server.http_listen {
            errors
                .push("server.trino.listen must differ from server.pg_listen and server.http_listen".into());
        }
        if trino.max_active_queries == 0 {
            errors.push("server.trino.max_active_queries must be > 0".into());
        }
        if trino.client_timeout_secs == 0 {
            errors.push("server.trino.client_timeout_secs must be > 0".into());
        }
        if trino.max_query_length == 0 {
            errors.push("server.trino.max_query_length must be > 0".into());
        }
    }

    fn validate_memory(&self, errors: &mut Vec<String>) {
        let memory = &self.memory;
        if memory.limit < MIN_MEMORY_LIMIT {
            errors.push(format!(
                "memory.limit {} is below the minimum of {MIN_MEMORY_LIMIT}",
                memory.limit
            ));
        }
        if memory.spill_limit.as_u64() == 0 {
            errors.push(
                "memory.spill_limit must be > 0; without spill, large joins fail instead of degrading".into(),
            );
        }
        if !memory.spill_dir.is_absolute() {
            errors.push(format!(
                "memory.spill_dir {} must be an absolute path",
                memory.spill_dir.display()
            ));
        }
        if memory.metadata_cache_limit >= memory.limit {
            errors.push("memory.metadata_cache_limit must be smaller than memory.limit".into());
        }
    }

    fn validate_execution(&self, errors: &mut Vec<String>) {
        let exec = &self.execution;
        if exec.batch_size == 0 || exec.batch_size > MAX_BATCH_SIZE {
            errors.push(format!(
                "execution.batch_size must be between 1 and {MAX_BATCH_SIZE}"
            ));
        }
        if exec.target_partitions == Some(0) {
            errors.push("execution.target_partitions must be > 0".into());
        }
        if exec.source_startup_timeout_secs == 0 {
            errors.push("execution.source_startup_timeout_secs must be > 0".into());
        }
    }

    fn validate_telemetry(&self, errors: &mut Vec<String>) {
        let telemetry = &self.telemetry;
        if !OTLP_PROTOCOLS.contains(&telemetry.protocol.as_str()) {
            errors.push(format!(
                "telemetry.protocol `{}` is not supported; use one of {OTLP_PROTOCOLS:?}",
                telemetry.protocol
            ));
        }
        if !(0.0..=1.0).contains(&telemetry.sampling_ratio) {
            errors.push(format!(
                "telemetry.sampling_ratio {} must be between 0.0 and 1.0",
                telemetry.sampling_ratio
            ));
        }
        let name = telemetry.service_name.trim();
        if name.is_empty() || name.len() > MAX_SERVICE_NAME {
            errors.push(format!(
                "telemetry.service_name must be 1 to {MAX_SERVICE_NAME} characters"
            ));
        }
        if telemetry.export_timeout_secs == 0 || telemetry.export_timeout_secs > MAX_EXPORT_TIMEOUT_SECS {
            errors.push(format!(
                "telemetry.export_timeout_secs must be between 1 and {MAX_EXPORT_TIMEOUT_SECS}"
            ));
        }
        if let Some(endpoint) = &telemetry.otlp_endpoint
            && let Err(problem) = check_otlp_endpoint(endpoint)
        {
            // Never echo what may be a password in the user-info part.
            let shown = if endpoint.contains('@') {
                "<hidden>"
            } else {
                endpoint
            };
            errors.push(format!("telemetry.otlp_endpoint `{shown}` {problem}"));
        }
    }

    fn validate_sources(&self, env: &dyn Fn(&str) -> Option<String>, errors: &mut Vec<String>) -> Secrets {
        let mut seen = HashSet::new();
        let mut secrets = BTreeMap::new();
        for source in &self.sources {
            let name = &source.name;
            if !is_identifier(name) {
                errors.push(format!(
                    "source `{name}`: name must match [a-z_][a-z0-9_]* (max 63 chars)"
                ));
            }
            if name == "datafusion" || name == "information_schema" || name == "system" {
                errors.push(format!("source `{name}`: name is reserved"));
            }
            if !seen.insert(name.as_str()) {
                errors.push(format!("source `{name}`: duplicate name"));
            }
            match &source.kind {
                SourceKind::Postgres { sslmode, host, .. } => {
                    check_sql_source(name, host, ("sslmode", sslmode), &PG_SSL_MODES, errors);
                }
                SourceKind::Mysql { sslmode, host, .. } => {
                    check_sql_source(name, host, ("sslmode", sslmode), &MYSQL_SSL_MODES, errors);
                }
                SourceKind::Sqlserver {
                    encrypt,
                    host,
                    pool_size,
                    ..
                } => {
                    check_sql_source(name, host, ("encrypt", encrypt), &MSSQL_ENCRYPT, errors);
                    if *pool_size == 0 || *pool_size > MAX_POOL_SIZE {
                        errors.push(format!(
                            "source `{name}`: pool_size must be between 1 and {MAX_POOL_SIZE}"
                        ));
                    }
                }
                SourceKind::Iceberg {
                    catalog_uri,
                    access_key_id_env,
                    secret_access_key_env,
                    ..
                } => {
                    if !(catalog_uri.starts_with("http://") || catalog_uri.starts_with("https://")) {
                        errors.push(format!("source `{name}`: catalog_uri must be an http(s) URL"));
                    }
                    if access_key_id_env.is_some() != secret_access_key_env.is_some() {
                        errors.push(format!(
                            "source `{name}`: set both access_key_id_env and secret_access_key_env, or neither"
                        ));
                    }
                }
                SourceKind::Hive {
                    warehouse,
                    access_key_id_env,
                    secret_access_key_env,
                    ..
                } => {
                    let local = warehouse.starts_with('/') || warehouse.starts_with("file:");
                    if !local && !warehouse.starts_with("s3://") {
                        errors.push(format!(
                            "source `{name}`: warehouse must be an absolute path, file: or s3:// URL"
                        ));
                    }
                    if access_key_id_env.is_some() != secret_access_key_env.is_some() {
                        errors.push(format!(
                            "source `{name}`: set both access_key_id_env and secret_access_key_env, or neither"
                        ));
                    }
                }
                SourceKind::Mongodb { .. } => {}
            }
            for var in source.kind.secret_env_vars() {
                match env(var) {
                    Some(value) if !value.is_empty() => {
                        secrets.insert(var.to_owned(), SecretString::from(value));
                    }
                    _ => errors.push(format!("source `{name}`: environment variable {var} is not set")),
                }
            }
        }
        Secrets(secrets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    const VALID: &str = r#"
        [server]
        pg_listen = "127.0.0.1:5432"
        http_listen = "0.0.0.0:8080"

        [memory]
        limit = "2GiB"
        spill_dir = "/var/lib/codelake/spill"
        spill_limit = "20GiB"

        [[source]]
        name = "erp"
        kind = "postgres"
        host = "db.internal"
        database = "erp"
        user = "reader"
        password_env = "ERP_PG_PASSWORD"

        [[source]]
        name = "lake"
        kind = "iceberg"
        catalog_uri = "http://iceberg-rest:8181"
        s3_endpoint = "http://minio:9000"
        access_key_id_env = "LAKE_KEY"
        secret_access_key_env = "LAKE_SECRET"
        s3_path_style = true

        [[source]]
        name = "crm"
        kind = "mysql"
        host = "mysql.internal"
        database = "crm"
        user = "reader"
        password_env = "CRM_MYSQL_PASSWORD"

        [[source]]
        name = "erp1"
        kind = "sqlserver"
        host = "erp.internal"
        database = "ERP"
        user = "codelake_reader"
        password_env = "ERP1_MSSQL_PASSWORD"

        [[source]]
        name = "events"
        kind = "mongodb"
        required = false
        connection_string_env = "EVENTS_MONGO_URI"
    "#;

    #[expect(clippy::unnecessary_wraps, reason = "must match the env lookup signature")]
    fn full_env(name: &str) -> Option<String> {
        Some(format!("value-of-{name}"))
    }

    fn parse(doc: &str) -> Config {
        toml::from_str(doc).unwrap()
    }

    fn errors_of(doc: &str, env: &dyn Fn(&str) -> Option<String>) -> Vec<String> {
        parse(doc).validate(env, &|_| true).unwrap_err().0
    }

    #[test]
    fn valid_config_resolves_every_secret_and_applies_defaults() {
        let validated = parse(VALID).validate(&full_env, &|_| true).unwrap();
        let cfg = &validated.config;
        assert_eq!(cfg.memory.limit, ByteSize::gib(2));
        assert_eq!(cfg.server.max_connections, 256);
        assert_eq!(
            cfg.server.shutdown_grace_secs, 30,
            "a zero grace would cut in-flight queries"
        );
        assert_eq!(cfg.execution.source_startup_timeout_secs, 60);
        assert_eq!(cfg.memory.metadata_cache_limit, ByteSize::mib(64));
        match &cfg.sources[1].kind {
            SourceKind::Iceberg {
                s3_region,
                s3_path_style,
                ..
            } => {
                assert_eq!(s3_region, "us-east-1");
                assert!(*s3_path_style);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(cfg.execution.batch_size, 8192);
        assert_eq!(cfg.sources.len(), 5);
        assert!(cfg.sources[2].required);
        assert!(!cfg.sources[4].required);
        match &cfg.sources[3].kind {
            SourceKind::Sqlserver {
                port,
                encrypt,
                trust_server_certificate,
                pool_size,
                ..
            } => {
                assert_eq!(*port, 1433);
                assert_eq!(encrypt, "required", "encryption must be the default");
                assert!(!trust_server_certificate, "certificates are validated by default");
                assert_eq!(*pool_size, 10);
            }
            other => panic!("unexpected {other:?}"),
        }
        match &cfg.sources[2].kind {
            SourceKind::Mysql { port, sslmode, .. } => {
                assert_eq!(*port, 3306);
                assert_eq!(sslmode, "required", "TLS must be the default");
            }
            other => panic!("unexpected {other:?}"),
        }
        match &cfg.sources[0].kind {
            SourceKind::Postgres { port, sslmode, .. } => {
                assert_eq!(*port, 5432);
                assert_eq!(sslmode, "verify-full", "TLS verification must be the default");
            }
            other => panic!("unexpected {other:?}"),
        }
        for var in [
            "ERP_PG_PASSWORD",
            "LAKE_KEY",
            "LAKE_SECRET",
            "CRM_MYSQL_PASSWORD",
            "EVENTS_MONGO_URI",
        ] {
            let secret = validated.secrets.get(var).unwrap();
            assert_eq!(secret.expose_secret(), format!("value-of-{var}"));
        }
    }

    #[test]
    fn reports_all_missing_secrets_at_once() {
        let errors = errors_of(VALID, &|_| None);
        assert_eq!(errors.len(), 6, "{errors:#?}");
        assert!(errors.iter().all(|e| e.contains("is not set")));
    }

    #[test]
    fn empty_secret_counts_as_missing() {
        let errors = errors_of(VALID, &|name| {
            (name != "ERP_PG_PASSWORD")
                .then(|| "x".to_owned())
                .or(Some(String::new()))
        });
        assert_eq!(
            errors,
            vec!["source `erp`: environment variable ERP_PG_PASSWORD is not set"]
        );
    }

    #[test]
    fn debug_output_never_leaks_secrets() {
        let validated = parse(VALID).validate(&full_env, &|_| true).unwrap();
        let rendered = format!("{validated:?}");
        assert!(!rendered.contains("value-of-ERP_PG_PASSWORD"), "{rendered}");
    }

    #[test]
    fn rejects_unknown_fields() {
        let doc = VALID.replace("[memory]", "[memory]\nlimt = \"1GiB\"");
        assert!(toml::from_str::<Config>(&doc).is_err());
    }

    #[test]
    fn rejects_unknown_fields_inside_a_source() {
        let doc = VALID.replace("user = \"reader\"", "user = \"reader\"\npasword_env = \"typo\"");
        assert!(toml::from_str::<Config>(&doc).is_err());
    }

    #[test]
    fn rejects_unsafe_limits() {
        let doc = VALID
            .replace("limit = \"2GiB\"", "limit = \"1MiB\"")
            .replace("spill_limit = \"20GiB\"", "spill_limit = 0")
            .replace("/var/lib/codelake/spill", "relative/spill")
            .replace("[server]", "[server]\nmax_connections = 0");
        let errors = errors_of(&doc, &full_env);
        assert!(
            errors.iter().any(|e| e.contains("memory.limit 1MiB is below")),
            "{errors:#?}"
        );
        assert!(errors.iter().any(|e| e.contains("spill_limit must be > 0")));
        assert!(errors.iter().any(|e| e.contains("must be an absolute path")));
        assert!(errors.iter().any(|e| e.contains("max_connections must be > 0")));
        assert!(errors.iter().any(|e| e.contains("metadata_cache_limit")));
    }

    #[test]
    fn memory_limit_at_minimum_is_accepted() {
        let doc = VALID.replace(
            "limit = \"2GiB\"",
            "limit = \"64MiB\"\nmetadata_cache_limit = \"1MiB\"",
        );
        assert!(parse(&doc).validate(&full_env, &|_| true).is_ok());
    }

    #[test]
    fn rejects_bad_execution_settings() {
        let doc = VALID.replace(
            "[memory]",
            "[execution]\nbatch_size = 0\ntarget_partitions = 0\nsource_startup_timeout_secs = 0\n\n[memory]",
        );
        let errors = errors_of(&doc, &full_env);
        assert_eq!(errors.len(), 3, "{errors:#?}");
        let doc = VALID.replace("[memory]", "[execution]\nbatch_size = 1048577\n\n[memory]");
        assert_eq!(errors_of(&doc, &full_env).len(), 1);
        let doc = VALID.replace("[memory]", "[execution]\nbatch_size = 1048576\n\n[memory]");
        assert!(parse(&doc).validate(&full_env, &|_| true).is_ok());
    }

    #[test]
    fn rejects_same_listener_for_pg_and_http() {
        let doc = VALID.replace("0.0.0.0:8080", "127.0.0.1:5432");
        let errors = errors_of(&doc, &full_env);
        assert_eq!(
            errors,
            vec!["server.pg_listen and server.http_listen must differ"]
        );
    }

    #[test]
    fn refuses_unauthenticated_network_listener_without_opt_in() {
        let doc = VALID.replace("127.0.0.1:5432", "0.0.0.0:5432");
        let errors = errors_of(&doc, &full_env);
        assert_eq!(errors.len(), 1, "{errors:#?}");
        assert!(errors[0].contains("client authentication is not implemented"));

        let doc = doc.replace("[server]", "[server]\nallow_unauthenticated_network = true");
        assert!(parse(&doc).validate(&full_env, &|_| true).is_ok());
    }

    #[test]
    fn trino_listener_is_validated() {
        let with =
            |section: &str| VALID.replace("[memory]", &format!("[server.trino]\n{section}\n\n[memory]"));
        let ok = with("listen = \"127.0.0.1:8081\"");
        let validated = parse(&ok).validate(&full_env, &|_| true).unwrap();
        let trino = validated.config.server.trino.unwrap();
        assert_eq!(trino.max_active_queries, 100);
        assert_eq!(trino.client_timeout_secs, 300);
        assert_eq!(trino.max_query_length, 1_000_000);

        let errors = errors_of(&with("listen = \"0.0.0.0:8081\""), &full_env);
        assert_eq!(errors.len(), 1, "{errors:#?}");
        assert!(errors[0].contains("server.trino.listen"));

        let errors = errors_of(&with("listen = \"127.0.0.1:5432\""), &full_env);
        assert_eq!(
            errors,
            vec!["server.trino.listen must differ from server.pg_listen and server.http_listen"]
        );

        let errors = errors_of(
            &with(
                "listen = \"127.0.0.1:8081\"\nmax_active_queries = 0\nclient_timeout_secs = 0\nmax_query_length = 0",
            ),
            &full_env,
        );
        assert_eq!(errors.len(), 3, "{errors:#?}");
    }

    #[test]
    fn refuses_tls_files_that_do_not_exist() {
        let doc = VALID.replace(
            "[memory]",
            "[server.tls]\ncert_path = \"/tls/cert.pem\"\nkey_path = \"/tls/key.pem\"\n\n[memory]",
        );
        let errors = parse(&doc).validate(&full_env, &|_| false).unwrap_err().0;
        assert_eq!(errors.len(), 2, "{errors:#?}");
        assert!(parse(&doc).validate(&full_env, &|_| true).is_ok());
    }

    #[test]
    fn rejects_bad_source_definitions() {
        let doc = format!(
            "{VALID}\n[[source]]\nname = \"erp\"\nkind = \"postgres\"\nhost = \" \"\ndatabase = \"d\"\nuser = \"u\"\npassword_env = \"P\"\nsslmode = \"allow\"\n\
             [[source]]\nname = \"my\"\nkind = \"mysql\"\nhost = \"h\"\ndatabase = \"d\"\nuser = \"u\"\npassword_env = \"P\"\nsslmode = \"verify-full\"\n\
             [[source]]\nname = \"ms\"\nkind = \"sqlserver\"\nhost = \"h\"\ndatabase = \"d\"\nuser = \"u\"\npassword_env = \"P\"\nencrypt = \"maybe\"\npool_size = 0\n\
             [[source]]\nname = \"Bad-Name\"\nkind = \"mongodb\"\nconnection_string_env = \"M\"\n\
             [[source]]\nname = \"information_schema\"\nkind = \"mongodb\"\nconnection_string_env = \"M\"\n\
             [[source]]\nname = \"half\"\nkind = \"iceberg\"\ncatalog_uri = \"thrift://x\"\naccess_key_id_env = \"K\"\n"
        );
        let errors = errors_of(&doc, &full_env);
        for expected in [
            "`erp`: duplicate name",
            "`erp`: sslmode `allow`",
            "`erp`: host is empty",
            "`my`: sslmode `verify-full`",
            "`ms`: encrypt `maybe`",
            "`ms`: pool_size must be between 1 and 256",
            "`Bad-Name`: name must match",
            "`information_schema`: name is reserved",
            "`half`: catalog_uri must be an http(s) URL",
            "`half`: set both access_key_id_env",
        ] {
            assert!(
                errors.iter().any(|e| e.contains(expected)),
                "missing `{expected}` in {errors:#?}"
            );
        }
    }

    #[test]
    fn hive_sources_validate_their_warehouse() {
        let doc = format!(
            "{VALID}\n[[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"file:/data/DATAWAREHOUSE\"\n\
             [[source]]\nname = \"lake2\"\nkind = \"hive\"\nwarehouse = \"s3://lake/wh\"\ns3_endpoint = \"http://minio:9000\"\n\
             [[source]]\nname = \"bad\"\nkind = \"hive\"\nwarehouse = \"relative\"\naccess_key_id_env = \"K\"\n"
        );
        let errors = errors_of(&doc, &full_env);
        assert_eq!(errors.len(), 2, "{errors:#?}");
        assert!(
            errors.iter().all(|e| e.starts_with("source `bad`")),
            "{errors:#?}"
        );
        let cfg = parse(&doc);
        assert_eq!(cfg.sources.last().unwrap().kind.label(), "hive");
    }

    #[test]
    fn hive_timestamp_precision_defaults_to_milliseconds_like_trino() {
        let doc = format!(
            "{VALID}\n[[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"/wh\"\n\
             [[source]]\nname = \"hive_us\"\nkind = \"hive\"\nwarehouse = \"/wh\"\ntimestamp_precision = \"MICROSECONDS\"\n"
        );
        let precisions: Vec<HiveTimestampPrecision> = parse(&doc)
            .sources
            .iter()
            .filter_map(|source| match &source.kind {
                SourceKind::Hive {
                    timestamp_precision, ..
                } => Some(*timestamp_precision),
                _ => None,
            })
            .collect();
        assert_eq!(
            precisions,
            vec![
                HiveTimestampPrecision::Milliseconds,
                HiveTimestampPrecision::Microseconds
            ]
        );
        assert_eq!(HiveTimestampPrecision::Microseconds.digits(), 6);
        assert_eq!(HiveTimestampPrecision::Nanoseconds.name(), "NANOSECONDS");
        let wrong = format!(
            "{VALID}\n[[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"/wh\"\ntimestamp_precision = \"SECONDS\"\n"
        );
        assert!(toml::from_str::<Config>(&wrong).is_err());
    }

    #[test]
    fn telemetry_is_off_by_default() {
        let validated = parse(VALID).validate(&full_env, &|_| true).unwrap();
        let telemetry = &validated.config.telemetry;
        assert_eq!(telemetry.otlp_endpoint, None);
        assert_eq!(telemetry.traces_url(), None);
        assert_eq!(telemetry.protocol, "http/protobuf");
        assert_eq!(telemetry.service_name, "codelake");
        assert!((telemetry.sampling_ratio - 1.0).abs() < f64::EPSILON);
        assert_eq!(telemetry.export_timeout_secs, 10);
    }

    #[test]
    fn telemetry_accepts_an_http_collector() {
        for (endpoint, url) in [
            (
                "http://otel-collector:4318",
                "http://otel-collector:4318/v1/traces",
            ),
            ("http://127.0.0.1:4318/", "http://127.0.0.1:4318/v1/traces"),
            ("http://c:4318/v1/traces", "http://c:4318/v1/traces"),
            ("http://otel_agent/otlp", "http://otel_agent/otlp/v1/traces"),
            ("http://[::1]:4318", "http://[::1]:4318/v1/traces"),
            ("http://[fe80::1]", "http://[fe80::1]/v1/traces"),
            (
                "http://collector.local:65535/",
                "http://collector.local:65535/v1/traces",
            ),
        ] {
            let doc = format!(
                "{VALID}\n[telemetry]\notlp_endpoint = \"{endpoint}\"\nservice_name = \"lake\"\nsampling_ratio = 0.0\nexport_timeout_secs = 300\n"
            );
            let validated = parse(&doc).validate(&full_env, &|_| true).unwrap();
            assert_eq!(validated.config.telemetry.traces_url().as_deref(), Some(url));
        }
    }

    #[test]
    fn telemetry_rejects_every_bad_setting_at_once() {
        let doc = format!(
            "{VALID}\n[telemetry]\notlp_endpoint = \"https://collector:4318\"\nprotocol = \"grpc\"\n\
             service_name = \" \"\nsampling_ratio = 1.5\nexport_timeout_secs = 0\n"
        );
        let errors = errors_of(&doc, &full_env);
        assert_eq!(errors.len(), 5, "{errors:#?}");
        for expected in [
            "telemetry.protocol `grpc` is not supported",
            "telemetry.sampling_ratio 1.5 must be between",
            "telemetry.service_name must be",
            "telemetry.export_timeout_secs must be between 1 and 300",
            "TLS to the collector is not supported",
        ] {
            assert!(
                errors.iter().any(|e| e.contains(expected)),
                "missing `{expected}` in {errors:#?}"
            );
        }
        for (endpoint, expected) in [
            ("otel:4318", "must be an http:// URL"),
            ("http://", "has no host"),
            ("http:///v1/traces", "has no host"),
            ("http://a b", "has no host"),
            ("http://:4318", "has no host"),
            ("http://collector:99999", "has an invalid port"),
            ("http://collector:0", "has an invalid port"),
            ("http://collector:", "has an invalid port"),
            ("http://collector:43a8", "has an invalid port"),
            ("http://collector:+4318", "has an invalid port"),
            (
                "http://127.0.0.1:4318?token=abc",
                "must not have a query or fragment",
            ),
            (
                "http://127.0.0.1:4318/otlp#frag",
                "must not have a query or fragment",
            ),
            ("http://user:secret@collector:4318", "must not carry credentials"),
            ("http://coll ector:4318", "has no host"),
            ("http://collector..local:4318", "has an invalid host"),
            ("http://999.0.0.1:4318", "has an invalid host"),
            ("http://a:b:4318", "has an invalid port"),
            ("http://[::1", "has an invalid IPv6 host"),
            ("http://[zz]:4318", "has an invalid IPv6 host"),
            ("http://[::1]4318", "has an invalid port"),
        ] {
            let doc = format!("{VALID}\n[telemetry]\notlp_endpoint = \"{endpoint}\"\n");
            let errors = errors_of(&doc, &full_env);
            assert_eq!(errors.len(), 1, "{endpoint}: {errors:#?}");
            assert!(errors[0].contains(expected), "{endpoint}: {errors:#?}");
        }
        let nan = format!("{VALID}\n[telemetry]\nsampling_ratio = nan\n");
        assert_eq!(errors_of(&nan, &full_env).len(), 1);
        let long = format!("{VALID}\n[telemetry]\nservice_name = \"{}\"\n", "s".repeat(256));
        assert_eq!(errors_of(&long, &full_env).len(), 1);
        let unknown = format!("{VALID}\n[telemetry]\nendpoint = \"http://x:4318\"\n");
        assert!(
            toml::from_str::<Config>(&unknown).is_err(),
            "typos must not disable tracing silently"
        );
    }

    #[test]
    fn identifier_rules() {
        assert!(is_identifier("erp"));
        assert!(is_identifier("_x9"));
        assert!(!is_identifier("9x"));
        assert!(!is_identifier(""));
        assert!(!is_identifier("a-b"));
        assert!(is_identifier(&"a".repeat(63)));
        assert!(!is_identifier(&"a".repeat(64)));
    }

    #[test]
    fn load_reports_missing_file_and_parse_errors() {
        let missing = Config::load(Path::new("/definitely/not/here.toml")).unwrap_err();
        assert!(matches!(missing, LoadError::Read { .. }));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "[server\n").unwrap();
        assert!(matches!(
            Config::load(&path).unwrap_err(),
            LoadError::Parse { .. }
        ));
    }

    #[test]
    fn secret_env_vars_per_kind() {
        let cfg = parse(VALID);
        let vars: Vec<Vec<&str>> = cfg.sources.iter().map(|s| s.kind.secret_env_vars()).collect();
        assert_eq!(
            vars,
            vec![
                vec!["ERP_PG_PASSWORD"],
                vec!["LAKE_KEY", "LAKE_SECRET"],
                vec!["CRM_MYSQL_PASSWORD"],
                vec!["ERP1_MSSQL_PASSWORD"],
                vec!["EVENTS_MONGO_URI"]
            ]
        );
        let labels: Vec<&str> = cfg.sources.iter().map(|s| s.kind.label()).collect();
        assert_eq!(
            labels,
            vec!["postgres", "iceberg", "mysql", "sqlserver", "mongodb"]
        );
    }
}
