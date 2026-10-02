//! Configuration model and fail-fast validation.
//!
//! Rules enforced here:
//! - Configuration lives outside the artifact (a TOML file plus environment variables).
//! - Secrets are never written in the file: sources reference *environment variable
//!   names* and the values are resolved into [`SecretString`] at startup.
//! - Non-secret string values may reference the environment as `${ENV:NAME}`,
//!   resolved once when the file is loaded ([`crate::env_refs`]).
//! - Validation reports **every** problem at once, so an operator fixes the file in
//!   one pass instead of restarting the process once per mistake.

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use secrecy::SecretString;
use serde::Deserialize;

use crate::bytes::ByteSize;
use crate::env_refs;

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
    /// Query governance: admission queue, per-user and resource-group
    /// concurrency, time limits and the query registry.
    #[serde(default)]
    pub governance: GovernanceConfig,
    #[serde(default, rename = "source")]
    pub sources: Vec<SourceConfig>,
    /// Distributed execution (ROADMAP M4, ADR 0011): this node's role.
    /// Absent: a single node. Needs a build with the `distributed` feature.
    pub distributed: Option<DistributedConfig>,
}

/// A node of a distributed deployment. Coordinator and workers read the same
/// configuration file apart from this section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DistributedConfig {
    pub role: DistributedRole,
    /// Worker: the address its execution service listens on. Unauthenticated,
    /// like the PostgreSQL listener: beyond loopback only with
    /// `server.allow_unauthenticated_network`.
    #[serde(default = "default_worker_listen")]
    pub listen: SocketAddr,
    /// Coordinator: the workers' service URLs (`http://host:port`).
    #[serde(default)]
    pub workers: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributedRole {
    /// Plans every query and serves clients; distributes what workers can run.
    Coordinator,
    /// Runs the stages coordinators send; serves no clients.
    Worker,
}

fn default_worker_listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 50051))
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
    /// The soft open-files limit the server sets at startup. Absent: the
    /// hard limit (`ulimit -Hn`), as high as the process may go (ADR 0018).
    pub open_files_limit: Option<u64>,
    /// Descriptors never given to client connections: source pools, spill
    /// and data files, logs (ADR 0018).
    #[serde(default = "default_reserved_files")]
    pub reserved_files: u64,
    /// HTTP listeners (ops and `[server.trino]`): a connection with no request
    /// in flight is closed after this many seconds without a new one, as with
    /// Trino's `http-server.net.max-idle-time` (ADR 0018).
    #[serde(default = "default_http_idle_timeout_secs")]
    pub http_idle_timeout_secs: u64,
    /// HTTP listeners: seconds a request head may take to arrive in full,
    /// counted from the connection's opening for its first request and from
    /// the first byte for the next ones (ADR 0018).
    #[serde(default = "default_http_header_read_timeout_secs")]
    pub http_header_read_timeout_secs: u64,
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
    /// Deprecated: the query registry moved to the core, configure
    /// `[governance] history_size`. When set here, it still applies.
    pub history_size: Option<usize>,
    /// Deprecated: see `history_size`; `[governance] history_retention_secs`.
    pub history_retention_secs: Option<u64>,
    /// Longest SQL text accepted, in characters; longer statements fail with
    /// `QUERY_TEXT_TOO_LARGE`, as with Trino's `query.max-length`.
    #[serde(default = "default_trino_max_query_length")]
    pub max_query_length: usize,
    /// Prefix of the protocol's headers (`<prefix>User`, `<prefix>Catalog`,
    /// …; ISS-011). Default `X-CodeLake-`; clients built for another engine
    /// send that engine's prefix, set here.
    #[serde(default = "default_header_prefix")]
    pub header_prefix: String,
    /// Version the server reports (`/v1/info`, `version()`); default
    /// `CodeLake`'s. Some clients check it.
    pub reported_version: Option<String>,
    /// Open connections; more are answered `503` at once. Absent: every
    /// descriptor the open-files limit leaves to this listener (ADR 0018).
    pub max_connections: Option<usize>,
}

fn default_header_prefix() -> String {
    "X-CodeLake-".into()
}

/// Whether `prefix` makes valid header names: `X-`, then letters, digits and
/// `-`, ending in `-`.
fn valid_header_prefix(prefix: &str) -> bool {
    prefix.len() > 3
        && prefix.get(..2).is_some_and(|x| x.eq_ignore_ascii_case("x-"))
        && prefix.ends_with('-')
        && prefix.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

const fn default_trino_max_active_queries() -> usize {
    100
}

const fn default_trino_client_timeout_secs() -> u64 {
    300
}

/// Trino's `query.max-length` default.
const fn default_trino_max_query_length() -> usize {
    1_000_000
}

/// `[governance]`: how many queries run and wait, per node, per user and per
/// resource group, and for how long (see [`crate::governance`]). It applies
/// to every client protocol, with the user each client names (no
/// authentication is assumed).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernanceConfig {
    /// Queries executing at once on this node; more wait in the queue.
    #[serde(default = "default_max_running_queries")]
    pub max_running_queries: usize,
    /// Queries waiting for a slot; more are refused (`QUERY_QUEUE_FULL`).
    /// `0` refuses whatever cannot run at once.
    #[serde(default = "default_max_queued_queries")]
    pub max_queued_queries: usize,
    /// Queries one user runs at once, across resource groups.
    #[serde(default = "default_max_running_per_user")]
    pub max_running_per_user: usize,
    /// Queries one user has waiting.
    #[serde(default = "default_max_queued_per_user")]
    pub max_queued_per_user: usize,
    /// Longest a query waits for a slot before it fails (`EXCEEDED_TIME_LIMIT`).
    #[serde(default = "default_queue_timeout_secs")]
    pub queue_timeout_secs: u64,
    /// Default of the `query_max_run_time` session property: wall time from
    /// submission, queueing included. Duration text (`1h`, `30m`).
    #[serde(default = "default_query_time")]
    pub query_max_run_time: String,
    /// Default of `query_max_execution_time`: time from admission.
    #[serde(default = "default_query_time")]
    pub query_max_execution_time: String,
    /// Completed queries kept in `system.runtime.queries`.
    #[serde(default = "default_history_size")]
    pub history_size: usize,
    /// Seconds a completed query stays listed.
    #[serde(default = "default_history_retention_secs")]
    pub history_retention_secs: u64,
    /// Longest SQL text the registry keeps per query, in characters; longer
    /// texts are recorded truncated (the query itself is not refused).
    #[serde(default = "default_max_recorded_query_length")]
    pub max_recorded_query_length: usize,
    /// Resource groups, matched in order; a query that matches none only
    /// counts against the node and per-user limits.
    #[serde(default, rename = "resource_group")]
    pub resource_groups: Vec<ResourceGroupConfig>,
}

impl Default for GovernanceConfig {
    fn default() -> Self {
        Self {
            max_running_queries: default_max_running_queries(),
            max_queued_queries: default_max_queued_queries(),
            max_running_per_user: default_max_running_per_user(),
            max_queued_per_user: default_max_queued_per_user(),
            queue_timeout_secs: default_queue_timeout_secs(),
            query_max_run_time: default_query_time(),
            query_max_execution_time: default_query_time(),
            history_size: default_history_size(),
            history_retention_secs: default_history_retention_secs(),
            max_recorded_query_length: default_max_recorded_query_length(),
            resource_groups: Vec::new(),
        }
    }
}

/// `[[governance.resource_group]]`: a named share of the node. A query
/// belongs to the first group whose selectors all match: its user is in
/// `users` or matches `user_pattern` (when either is given), and its source
/// (the HTTP protocol's source header, PostgreSQL's `application_name`)
/// matches `source_pattern` (when given).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceGroupConfig {
    pub name: String,
    #[serde(default)]
    pub users: Vec<String>,
    /// Regular expression matched against the whole user name.
    pub user_pattern: Option<String>,
    /// Regular expression matched against the whole source.
    pub source_pattern: Option<String>,
    pub max_running: usize,
    #[serde(default)]
    pub max_queued: usize,
}

const fn default_max_running_queries() -> usize {
    32
}

const fn default_max_queued_queries() -> usize {
    1000
}

const fn default_max_running_per_user() -> usize {
    16
}

const fn default_max_queued_per_user() -> usize {
    500
}

const fn default_queue_timeout_secs() -> u64 {
    600
}

/// The default of both time limits: effectively none, as the session
/// properties' own default.
fn default_query_time() -> String {
    "100d".to_owned()
}

const fn default_history_size() -> usize {
    100
}

const fn default_history_retention_secs() -> u64 {
    900
}

const fn default_max_recorded_query_length() -> usize {
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
    /// Parallelism per query, and the number of runtime workers of the server
    /// (ADR 0003). Defaults to the number of CPUs.
    pub target_partitions: Option<usize>,
    /// Seconds allowed for connecting to every source at startup.
    #[serde(default = "default_startup_timeout_secs")]
    pub source_startup_timeout_secs: u64,
    /// Distinct join keys a hash join's dynamic filter lists, per partition
    /// of the join; past it, or past `dynamic_filter_max_key_bytes`, only
    /// their min/max remain. A federated source receives the lists as an
    /// `IN` only while those of every partition hold this many keys together.
    #[serde(default = "default_dynamic_filter_max_keys")]
    pub dynamic_filter_max_keys: usize,
    /// Size of those keys per build partition.
    #[serde(default = "default_dynamic_filter_max_key_bytes")]
    pub dynamic_filter_max_key_bytes: ByteSize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            target_partitions: None,
            source_startup_timeout_secs: default_startup_timeout_secs(),
            dynamic_filter_max_keys: default_dynamic_filter_max_keys(),
            dynamic_filter_max_key_bytes: default_dynamic_filter_max_key_bytes(),
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
        /// The reference engine's `decimal-mapping`: what a `numeric` without
        /// precision, or with more than 38 digits, is. `strict` (default)
        /// leaves such a column out of its relation; `allow_overflow` makes it
        /// a `decimal(38, s)` whose values are rounded to `s` digits by
        /// `decimal_rounding_mode` (ISS-060).
        #[serde(default)]
        decimal_mapping: DecimalMapping,
        /// The reference engine's `decimal-rounding-mode` (`unnecessary`, the
        /// default: a value with more fraction digits than the column's scale
        /// fails the query instead of being rounded).
        #[serde(default)]
        decimal_rounding_mode: DecimalRoundingMode,
        /// The reference engine's `decimal-default-scale` (0 to 38, default 0):
        /// the scale of a `numeric` without precision under `allow_overflow`.
        #[serde(default)]
        decimal_default_scale: u8,
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
        /// IANA time zone of the catalog's Parquet `INT96` timestamps, the
        /// reference engine's `hive.parquet.time-zone`: writes convert the
        /// wall-clock time from it to UTC, reads convert back
        /// ([`crate::parquet_time_zone`]).
        /// Default `America/Sao_Paulo`. It must be the zone of every other
        /// engine that writes or reads the warehouse: a file another engine
        /// wrote in UTC reads shifted by the zone's offset.
        #[serde(default = "default_parquet_time_zone")]
        parquet_time_zone: String,
        /// Names of the metastore's files and markers (ISS-011): `CodeLake`'s
        /// by default; a warehouse another engine shares or wrote names them
        /// as that engine does (`[source.metastore_names]`).
        #[serde(default)]
        metastore_names: MetastoreNames,
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
const fn default_reserved_files() -> u64 {
    256
}
/// Trino's `http-server.net.max-idle-time` default (airlift `HttpServerConfig`).
const fn default_http_idle_timeout_secs() -> u64 {
    200
}
const fn default_http_header_read_timeout_secs() -> u64 {
    30
}
const fn default_batch_size() -> usize {
    8192
}
const fn default_startup_timeout_secs() -> u64 {
    60
}
/// `DataFusion`'s defaults (`hash_join_inlist_pushdown_max_distinct_values`
/// and `hash_join_inlist_pushdown_max_size`).
const fn default_dynamic_filter_max_keys() -> usize {
    150
}
const fn default_dynamic_filter_max_key_bytes() -> ByteSize {
    ByteSize::b(128 * 1024)
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
/// What a PostgreSQL `numeric` without precision, or with more than 38
/// digits, is: the reference engine's `decimal-mapping` (default `STRICT`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecimalMapping {
    /// Left out of its relation, as the reference engine leaves it.
    #[default]
    Strict,
    /// A `decimal(38, s)`: `s` is `decimal_default_scale` without precision,
    /// the smaller of the declared scale and it with more than 38 digits; an
    /// array of `numeric` without precision is an `array(decimal(38, 0))`.
    AllowOverflow,
}

/// How a value with more fraction digits than its decimal column's scale is
/// rounded: the reference engine's `decimal-rounding-mode`, Java's
/// `RoundingMode` (default `UNNECESSARY`: such a value fails the query).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecimalRoundingMode {
    /// Away from zero.
    Up,
    /// Towards zero.
    Down,
    /// Towards positive infinity.
    Ceiling,
    /// Towards negative infinity.
    Floor,
    /// To the nearest, a tie away from zero.
    HalfUp,
    /// To the nearest, a tie towards zero.
    HalfDown,
    /// To the nearest, a tie to the even neighbor.
    HalfEven,
    /// Never: the value fails the query.
    #[default]
    Unnecessary,
}

/// Largest `decimal_default_scale`: a decimal's largest precision.
const MAX_DECIMAL_SCALE: u8 = 38;

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

/// Names the hive file metastore writes and reads (ISS-011). The defaults
/// are `CodeLake`'s; another engine sharing the warehouse needs its own names
/// here, and `codelake hive migrate-names` renames the files of an existing
/// warehouse.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetastoreNames {
    /// Metadata file of a table or view, and suffix of a schema's
    /// (`.<schema><metadata_file>`).
    pub metadata_file: String,
    /// Directory of a table's ownership files.
    pub permissions_dir: String,
    /// Marker of a view's SQL (`/* <marker>: … */`) and its comment.
    pub view_marker: String,
    /// Table parameter set to `true` on a view.
    pub view_parameter: String,
    /// Table parameter holding the writer's version.
    pub version_parameter: String,
    /// `writerVersion` of new metadata (an engine sharing the warehouse may
    /// refuse metadata of another version).
    pub writer_version: String,
}

impl Default for MetastoreNames {
    fn default() -> Self {
        Self {
            metadata_file: ".codelakeSchema".into(),
            permissions_dir: ".codelakePermissions".into(),
            view_marker: "CodeLake View".into(),
            view_parameter: "codelake_view".into(),
            version_parameter: "codelake_version".into(),
            writer_version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

impl MetastoreNames {
    /// Every problem of these names, for the configuration check.
    #[must_use]
    pub fn problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        for (key, value) in [
            ("metadata_file", &self.metadata_file),
            ("permissions_dir", &self.permissions_dir),
        ] {
            if !value.starts_with('.') || value.len() < 2 || value.contains(['/', '\\']) {
                problems.push(format!(
                    "metastore_names.{key} must be a hidden file name (`.name`, no `/`)"
                ));
            }
        }
        if self.metadata_file == self.permissions_dir {
            problems.push("metastore_names.metadata_file and permissions_dir must differ".into());
        }
        for (key, value) in [
            ("view_marker", &self.view_marker),
            ("view_parameter", &self.view_parameter),
            ("version_parameter", &self.version_parameter),
            ("writer_version", &self.writer_version),
        ] {
            if value.trim().is_empty() || value.contains("*/") {
                problems.push(format!("metastore_names.{key} must not be empty nor hold `*/`"));
            }
        }
        problems
    }
}

fn default_parquet_time_zone() -> String {
    crate::parquet_time_zone::DEFAULT_PARQUET_TIME_ZONE.to_owned()
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
    /// `${ENV:NAME}` references that cannot be resolved.
    #[error("config file {path}: {errors}")]
    References { path: PathBuf, errors: ConfigErrors },
    /// The document does not deserialize once its references are resolved.
    #[error("cannot parse config file {path} (after resolving ${{ENV:…}} references): {message}")]
    Resolved { path: PathBuf, message: String },
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
    /// Parses and validates a config file, resolving `${ENV:NAME}`
    /// references ([`crate::env_refs`]) and secrets from the process
    /// environment.
    pub fn load(path: &Path) -> Result<ValidatedConfig, LoadError> {
        let raw = std::fs::read_to_string(path).map_err(|source| LoadError::Read {
            path: path.to_owned(),
            source,
        })?;
        let env = |name: &str| std::env::var(name).ok();
        let config = Self::parse(path, &raw, &env)?;
        Ok(config.validate(&env, &|p| p.is_file())?)
    }

    /// Parses a document, resolving its `${ENV:NAME}` references with `env`
    /// first. `path` only labels errors. A document without references is
    /// parsed as written, so its errors keep their line numbers; with
    /// references, errors name the key instead, and never quote a value that
    /// came from the environment.
    pub fn parse(path: &Path, raw: &str, env: &dyn Fn(&str) -> Option<String>) -> Result<Self, LoadError> {
        let parse_err = |source| LoadError::Parse {
            path: path.to_owned(),
            source: Box::new(source),
        };
        let mut document = toml::Value::Table(toml::from_str::<toml::Table>(raw).map_err(parse_err)?);
        if !env_refs::has_references(&document) {
            return toml::from_str(raw).map_err(parse_err);
        }
        let resolved =
            env_refs::resolve_document(&mut document, env).map_err(|errors| LoadError::References {
                path: path.to_owned(),
                errors: ConfigErrors(errors),
            })?;
        document
            .try_into()
            .map_err(|err: toml::de::Error| LoadError::Resolved {
                path: path.to_owned(),
                message: resolved.redact(err.message()),
            })
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
        self.validate_governance(&mut errors);
        self.validate_distributed(&mut errors);
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
        Self::validate_connections(server, errors);
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

    /// Descriptor and HTTP connection settings (ADR 0018). The budget each
    /// listener gets depends on the limit the process obtains, which the
    /// server checks again at startup.
    fn validate_connections(server: &ServerConfig, errors: &mut Vec<String>) {
        if let Some(limit) = server.open_files_limit {
            let pg = u64::try_from(server.max_connections).unwrap_or(u64::MAX);
            let floor = server.reserved_files.saturating_add(pg);
            if limit <= floor {
                errors.push(format!(
                    "server.open_files_limit {limit} must exceed server.reserved_files + server.max_connections ({floor})"
                ));
            }
        }
        if server.http_idle_timeout_secs == 0 {
            errors.push("server.http_idle_timeout_secs must be > 0".into());
        }
        if server.http_header_read_timeout_secs == 0 {
            errors.push("server.http_header_read_timeout_secs must be > 0".into());
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
        if !valid_header_prefix(&trino.header_prefix) {
            errors.push(format!(
                "server.trino.header_prefix `{}` must be `X-`, letters, digits or `-`, ending in `-`",
                trino.header_prefix
            ));
        }
        if trino
            .reported_version
            .as_deref()
            .is_some_and(|v| v.trim().is_empty())
        {
            errors.push("server.trino.reported_version must not be empty".into());
        }
        if trino.max_query_length == 0 {
            errors.push("server.trino.max_query_length must be > 0".into());
        }
        if trino.max_connections == Some(0) {
            errors.push("server.trino.max_connections must be > 0".into());
        }
    }

    /// Completed queries kept by the registry and for how long: the
    /// deprecated `[server.trino]` values win when set, so configurations
    /// written before the registry moved keep their meaning.
    pub fn query_history(&self) -> (usize, u64) {
        let trino = self.server.trino.as_ref();
        (
            trino
                .and_then(|t| t.history_size)
                .unwrap_or(self.governance.history_size),
            trino
                .and_then(|t| t.history_retention_secs)
                .unwrap_or(self.governance.history_retention_secs),
        )
    }

    fn validate_distributed(&self, errors: &mut Vec<String>) {
        let Some(distributed) = &self.distributed else {
            return;
        };
        match distributed.role {
            DistributedRole::Coordinator => {
                if distributed.workers.is_empty() {
                    errors.push("distributed.workers must name at least one worker for a coordinator".into());
                }
                for url in &distributed.workers {
                    let scheme_ok = url.starts_with("http://") || url.starts_with("https://");
                    if !scheme_ok || url.len() <= "https://".len() {
                        errors.push(format!(
                            "distributed.workers: {url} is not an http(s)://host:port URL"
                        ));
                    }
                }
            }
            DistributedRole::Worker => {
                if !distributed.workers.is_empty() {
                    errors.push("distributed.workers is for coordinators; a worker lists none".into());
                }
                // The execution service has no client authentication: whoever
                // reaches it can run plans over every file the node can read.
                if !distributed.listen.ip().is_loopback() && !self.server.allow_unauthenticated_network {
                    errors.push(format!(
                        "distributed.listen {} is not loopback and workers do not authenticate their callers; \
                         bind to 127.0.0.1 or set server.allow_unauthenticated_network = true inside an isolated network",
                        distributed.listen
                    ));
                }
            }
        }
    }

    fn validate_governance(&self, errors: &mut Vec<String>) {
        let governance = &self.governance;
        for (field, value) in [
            ("max_running_queries", governance.max_running_queries),
            ("max_running_per_user", governance.max_running_per_user),
            ("max_recorded_query_length", governance.max_recorded_query_length),
        ] {
            if value == 0 {
                errors.push(format!("governance.{field} must be > 0"));
            }
        }
        if governance.queue_timeout_secs == 0 {
            errors.push("governance.queue_timeout_secs must be > 0".into());
        }
        for (field, value) in [
            ("query_max_run_time", &governance.query_max_run_time),
            ("query_max_execution_time", &governance.query_max_execution_time),
        ] {
            if crate::session_properties::parse_duration(value).is_none_or(|d| d.is_zero()) {
                errors.push(format!(
                    "governance.{field} `{value}` is not a positive duration (e.g. `1h`, `30m`)"
                ));
            }
        }
        let mut names = HashSet::new();
        for group in &governance.resource_groups {
            let name = group.name.trim();
            if name.is_empty() || name.eq_ignore_ascii_case(crate::governance::GLOBAL_GROUP) {
                errors.push(format!(
                    "governance.resource_group name `{}` is empty or reserved",
                    group.name
                ));
            } else if !names.insert(name) {
                errors.push(format!("governance.resource_group `{name}` is defined twice"));
            }
            if group.users.is_empty() && group.user_pattern.is_none() && group.source_pattern.is_none() {
                errors.push(format!(
                    "governance.resource_group `{name}` needs users, user_pattern or source_pattern"
                ));
            }
            if group.max_running == 0 {
                errors.push(format!(
                    "governance.resource_group `{name}`: max_running must be > 0"
                ));
            }
            for (field, pattern) in [
                ("user_pattern", &group.user_pattern),
                ("source_pattern", &group.source_pattern),
            ] {
                if let Some(pattern) = pattern
                    && let Err(err) = crate::governance::whole_match(pattern)
                {
                    errors.push(format!("governance.resource_group `{name}`: {field}: {err}"));
                }
            }
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
                SourceKind::Postgres {
                    sslmode,
                    host,
                    decimal_default_scale,
                    ..
                } => {
                    check_sql_source(name, host, ("sslmode", sslmode), &PG_SSL_MODES, errors);
                    if *decimal_default_scale > MAX_DECIMAL_SCALE {
                        errors.push(format!(
                            "source `{name}`: decimal_default_scale must be between 0 and {MAX_DECIMAL_SCALE}"
                        ));
                    }
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
                    parquet_time_zone,
                    metastore_names,
                    ..
                } => {
                    if let Err(err) = parquet_time_zone.parse::<crate::parquet_time_zone::ParquetTimeZone>() {
                        errors.push(format!("source `{name}`: parquet_time_zone {err}"));
                    }
                    for problem in metastore_names.problems() {
                        errors.push(format!("source `{name}`: {problem}"));
                    }
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

    const WITH_REFERENCES: &str = r#"
        [server]
        pg_listen = "${ENV:PG_LISTEN}"
        http_listen = "127.0.0.1:8080"

        [memory]
        limit = "${ENV:MEMORY_LIMIT}"
        spill_dir = "${ENV:DATA}/spill"
        spill_limit = "20GiB"

        [[source]]
        name = "erp"
        kind = "postgres"
        host = "${ENV:ERP_HOST}"
        database = "erp_${ENV:STAGE}"
        user = "reader"
        password_env = "ERP_PG_PASSWORD"

        [[source]]
        name = "lake"
        kind = "hive"
        warehouse = "${ENV:DATA}/warehouse"
        timestamp_precision = "${ENV:PRECISION}"
    "#;

    fn reference_env(name: &str) -> Option<String> {
        match name {
            "PG_LISTEN" => Some("127.0.0.1:15432".to_owned()),
            "MEMORY_LIMIT" => Some("1GiB".to_owned()),
            "DATA" => Some("/srv/codelake".to_owned()),
            "ERP_HOST" => Some("pg.hml.internal".to_owned()),
            "STAGE" => Some("hml".to_owned()),
            "PRECISION" => Some("MICROSECONDS".to_owned()),
            "ERP_PG_PASSWORD" => Some("pw".to_owned()),
            _ => None,
        }
    }

    #[test]
    fn env_references_resolve_in_every_string_value_at_load() {
        let config = Config::parse(Path::new("c.toml"), WITH_REFERENCES, &reference_env).unwrap();
        assert_eq!(config.server.pg_listen.port(), 15432);
        assert_eq!(config.memory.limit, ByteSize::gib(1));
        assert_eq!(config.memory.spill_dir, PathBuf::from("/srv/codelake/spill"));
        match &config.sources[0].kind {
            SourceKind::Postgres { host, database, .. } => {
                assert_eq!(host, "pg.hml.internal");
                assert_eq!(database, "erp_hml");
            }
            other => panic!("unexpected {other:?}"),
        }
        match &config.sources[1].kind {
            SourceKind::Hive {
                warehouse,
                timestamp_precision,
                ..
            } => {
                assert_eq!(warehouse, "/srv/codelake/warehouse");
                assert_eq!(*timestamp_precision, HiveTimestampPrecision::Microseconds);
            }
            other => panic!("unexpected {other:?}"),
        }
        config.validate(&reference_env, &|_| true).unwrap();
    }

    #[test]
    fn missing_env_references_are_reported_together_with_their_keys() {
        let env = |name: &str| {
            if name == "ERP_HOST" || name == "DATA" {
                None
            } else {
                reference_env(name)
            }
        };
        let err = Config::parse(Path::new("c.toml"), WITH_REFERENCES, &env).unwrap_err();
        let text = err.to_string();
        for expected in [
            "`memory.spill_dir` references environment variable DATA",
            "`source[0].host` references environment variable ERP_HOST",
            "`source[1].warehouse` references environment variable DATA",
        ] {
            assert!(text.contains(expected), "{expected}: {text}");
        }
        assert!(text.contains("c.toml"), "{text}");
        assert!(matches!(err, LoadError::References { .. }));
    }

    #[test]
    fn a_resolved_value_never_appears_in_a_parse_error() {
        let doc = VALID.replace(
            "http_listen = \"0.0.0.0:8080\"",
            "http_listen = \"0.0.0.0:8080\"\nmax_connections = \"${ENV:CONNS}\"",
        );
        let env = |name: &str| (name == "CONNS").then(|| "Not-A-Number-Value".to_owned());
        let err = Config::parse(Path::new("c.toml"), &doc, &env)
            .unwrap_err()
            .to_string();
        assert!(!err.contains("Not-A-Number-Value"), "{err}");
        assert!(err.contains("${ENV:CONNS}"), "{err}");
    }

    #[test]
    fn a_document_without_references_keeps_line_numbers_in_errors() {
        let doc = VALID.replace("[memory]", "[memory]\nlimt = \"1GiB\"");
        let err = Config::parse(Path::new("c.toml"), &doc, &|_| None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("line"), "{err}");
        // `$` outside `${ENV:…}` is literal text, not a lookup.
        let doc = VALID.replace("db.internal", "db$${HOST}");
        let config = Config::parse(Path::new("c.toml"), &doc, &|_| None).unwrap();
        match &config.sources[0].kind {
            SourceKind::Postgres { host, .. } => assert_eq!(host, "db$${HOST}"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn load_reads_the_file_and_resolves_references() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("codelake.toml");
        std::fs::write(&path, WITH_REFERENCES).unwrap();
        // The test process does not define these variables.
        let err = Config::load(&path).unwrap_err().to_string();
        assert!(err.contains("PG_LISTEN"), "{err}");
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
    fn dynamic_filter_keys_are_read() {
        let execution = parse(VALID).execution;
        assert_eq!(execution.dynamic_filter_max_keys, 150);
        assert_eq!(execution.dynamic_filter_max_key_bytes, ByteSize::b(128 * 1024));
        let doc = VALID.replace(
            "[memory]",
            "[execution]\ndynamic_filter_max_keys = 5000\ndynamic_filter_max_key_bytes = \"1MiB\"\n\n[memory]",
        );
        let execution = parse(&doc).execution;
        assert_eq!(execution.dynamic_filter_max_keys, 5000);
        assert_eq!(execution.dynamic_filter_max_key_bytes, ByteSize::mib(1));
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
    fn governance_defaults_and_validation() {
        let validated = parse(VALID).validate(&full_env, &|_| true).unwrap();
        let governance = &validated.config.governance;
        assert_eq!(governance.max_running_queries, 32);
        assert_eq!(governance.max_queued_queries, 1000);
        assert_eq!(governance.max_running_per_user, 16);
        assert_eq!(governance.queue_timeout_secs, 600);
        assert_eq!(governance.query_max_run_time, "100d");
        assert_eq!(validated.config.query_history(), (100, 900));

        let with = |section: &str| VALID.replace("[memory]", &format!("{section}\n\n[memory]"));
        let ok = with(
            "[governance]\nmax_queued_queries = 0\nquery_max_run_time = \"1h\"\n\
             [[governance.resource_group]]\nname = \"etl\"\nusers = [\"dbt\"]\nmax_running = 2",
        );
        let validated = parse(&ok).validate(&full_env, &|_| true).unwrap();
        let group = &validated.config.governance.resource_groups[0];
        assert_eq!((group.max_running, group.max_queued), (2, 0));

        let errors = errors_of(
            &with(
                "[governance]\nmax_running_queries = 0\nmax_running_per_user = 0\nqueue_timeout_secs = 0\n\
                 query_max_execution_time = \"soon\"\n\
                 [[governance.resource_group]]\nname = \"global\"\nmax_running = 0\n\
                 [[governance.resource_group]]\nname = \"a\"\nuser_pattern = \"(\"\nmax_running = 1\n\
                 [[governance.resource_group]]\nname = \"a\"\nusers = [\"x\"]\nmax_running = 1",
            ),
            &full_env,
        );
        for expected in [
            "governance.max_running_queries must be > 0",
            "governance.max_running_per_user must be > 0",
            "governance.queue_timeout_secs must be > 0",
            "governance.query_max_execution_time `soon` is not a positive duration",
            "name `global` is empty or reserved",
            "`global` needs users, user_pattern or source_pattern",
            "`global`: max_running must be > 0",
            "`a`: user_pattern:",
            "`a` is defined twice",
        ] {
            assert!(
                errors.iter().any(|e| e.contains(expected)),
                "{expected} not in {errors:#?}"
            );
        }
    }

    #[test]
    fn deprecated_trino_history_settings_still_apply() {
        let doc = VALID.replace(
            "[memory]",
            "[server.trino]\nlisten = \"127.0.0.1:8081\"\nhistory_size = 7\n\n[governance]\nhistory_retention_secs = 60\n\n[memory]",
        );
        let validated = parse(&doc).validate(&full_env, &|_| true).unwrap();
        assert_eq!(validated.config.query_history(), (7, 60));
    }

    #[test]
    fn connection_settings_have_defaults_and_are_validated() {
        let validated = parse(VALID).validate(&full_env, &|_| true).unwrap();
        let server = &validated.config.server;
        assert_eq!(server.open_files_limit, None);
        assert_eq!(server.reserved_files, 256);
        assert_eq!(server.http_idle_timeout_secs, 200);
        assert_eq!(server.http_header_read_timeout_secs, 30);

        let with = |keys: &str| {
            VALID.replace(
                "http_listen = \"0.0.0.0:8080\"",
                &format!("http_listen = \"0.0.0.0:8080\"\n{keys}"),
            )
        };
        // The limit must leave room for the reserve and the PostgreSQL
        // connections (256 + 256 by default).
        assert_eq!(
            errors_of(&with("open_files_limit = 512"), &full_env),
            vec![
                "server.open_files_limit 512 must exceed server.reserved_files + server.max_connections (512)"
            ]
        );
        let validated = parse(&with("open_files_limit = 513"))
            .validate(&full_env, &|_| true)
            .unwrap();
        assert_eq!(validated.config.server.open_files_limit, Some(513));
        let small = "reserved_files = 10\nmax_connections = 5";
        assert!(
            parse(&with(&format!("{small}\nopen_files_limit = 16")))
                .validate(&full_env, &|_| true)
                .is_ok()
        );
        assert_eq!(
            errors_of(&with(&format!("{small}\nopen_files_limit = 15")), &full_env),
            vec![
                "server.open_files_limit 15 must exceed server.reserved_files + server.max_connections (15)"
            ]
        );
        let validated = parse(&with(
            "http_idle_timeout_secs = 1\nhttp_header_read_timeout_secs = 1",
        ))
        .validate(&full_env, &|_| true)
        .unwrap();
        assert_eq!(validated.config.server.http_idle_timeout_secs, 1);
        assert_eq!(validated.config.server.http_header_read_timeout_secs, 1);
        assert_eq!(
            errors_of(
                &with("http_idle_timeout_secs = 0\nhttp_header_read_timeout_secs = 0"),
                &full_env
            ),
            vec![
                "server.http_idle_timeout_secs must be > 0",
                "server.http_header_read_timeout_secs must be > 0",
            ]
        );
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

        assert_eq!(trino_max_connections(&with("listen = \"127.0.0.1:8081\"")), None);
        let one = with("listen = \"127.0.0.1:8081\"\nmax_connections = 1");
        assert_eq!(trino_max_connections(&one), Some(1));
        assert_eq!(
            errors_of(
                &with("listen = \"127.0.0.1:8081\"\nmax_connections = 0"),
                &full_env
            ),
            vec!["server.trino.max_connections must be > 0"]
        );
    }

    fn trino_max_connections(doc: &str) -> Option<usize> {
        let validated = parse(doc).validate(&full_env, &|_| true).unwrap();
        validated.config.server.trino.unwrap().max_connections
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
    fn distributed_deploy_examples_are_valid() {
        for (file, role) in [
            (
                include_str!("../../../deploy/k8s/distributed/coordinator.toml"),
                DistributedRole::Coordinator,
            ),
            (
                include_str!("../../../deploy/k8s/distributed/worker.toml"),
                DistributedRole::Worker,
            ),
        ] {
            let config = parse(file);
            assert_eq!(config.distributed.as_ref().map(|d| d.role), Some(role));
            let validated = config.validate(&|_| None, &|_| true);
            assert!(validated.is_ok(), "{role:?}: {:?}", validated.err());
        }
    }

    #[test]
    fn distributed_roles_are_checked() {
        let coordinator = format!(
            "{VALID}\n[distributed]\nrole = \"coordinator\"\nworkers = [\"http://worker-0:50051\", \"worker-1\"]\n"
        );
        let errors = errors_of(&coordinator, &full_env);
        assert_eq!(errors.len(), 1, "{errors:#?}");
        assert!(errors[0].contains("worker-1 is not an http(s)"), "{errors:#?}");
        let lonely = format!("{VALID}\n[distributed]\nrole = \"coordinator\"\n");
        assert!(errors_of(&lonely, &full_env)[0].contains("at least one worker"));
        let exposed = format!("{VALID}\n[distributed]\nrole = \"worker\"\nlisten = \"0.0.0.0:50051\"\n");
        assert!(
            errors_of(&exposed, &full_env)
                .iter()
                .any(|e| e.contains("workers do not authenticate")),
        );
        let worker = format!("{VALID}\n[distributed]\nrole = \"worker\"\n");
        assert!(parse(&worker).validate(&full_env, &|_| true).is_ok());
        assert_eq!(
            parse(&worker).distributed.map(|d| (d.role, d.listen.port())),
            Some((DistributedRole::Worker, 50051))
        );
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
    fn hive_parquet_time_zone_is_an_iana_zone_per_catalog() {
        let doc = format!(
            "{VALID}\n[[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"/wh\"\n\
             [[source]]\nname = \"hive_utc\"\nkind = \"hive\"\nwarehouse = \"/wh\"\nparquet_time_zone = \"UTC\"\n"
        );
        let zones: Vec<String> = parse(&doc)
            .sources
            .iter()
            .filter_map(|source| match &source.kind {
                SourceKind::Hive {
                    parquet_time_zone, ..
                } => Some(parquet_time_zone.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(zones, vec!["America/Sao_Paulo".to_owned(), "UTC".to_owned()]);
        assert!(parse(&doc).validate(&full_env, &|_| true).is_ok());
        let wrong = format!(
            "{VALID}\n[[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"/wh\"\nparquet_time_zone = \"-03:00\"\n\
             [[source]]\nname = \"hive_br\"\nkind = \"hive\"\nwarehouse = \"/wh\"\nparquet_time_zone = \"Brasilia\"\n"
        );
        let errors = errors_of(&wrong, &full_env);
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(
            errors[0].contains("source `hive`: parquet_time_zone `-03:00` is not an IANA time zone name"),
            "{errors:?}"
        );
        assert!(
            errors[1].contains("source `hive_br`: parquet_time_zone `Brasilia`"),
            "{errors:?}"
        );
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

    /// A PostgreSQL catalog's decimals as the reference engine's
    /// `decimal-mapping`, `decimal-rounding-mode` and `decimal-default-scale`
    /// (defaults `STRICT`, `UNNECESSARY`, 0).
    #[test]
    fn postgres_decimal_mapping_defaults_to_strict_like_trino() {
        let pg = |keys: &str| {
            format!(
                "{VALID}\n[[source]]\nname = \"hub\"\nkind = \"postgres\"\nhost = \"h\"\ndatabase = \"d\"\n\
                 user = \"u\"\npassword_env = \"P\"\n{keys}\n"
            )
        };
        let decimals = |doc: &str| {
            parse(doc)
                .sources
                .iter()
                .find_map(|source| match &source.kind {
                    SourceKind::Postgres {
                        decimal_mapping,
                        decimal_rounding_mode,
                        decimal_default_scale,
                        ..
                    } if source.name == "hub" => {
                        Some((*decimal_mapping, *decimal_rounding_mode, *decimal_default_scale))
                    }
                    _ => None,
                })
                .unwrap()
        };
        assert_eq!(
            decimals(&pg("")),
            (DecimalMapping::Strict, DecimalRoundingMode::Unnecessary, 0)
        );
        let modes = [
            ("up", DecimalRoundingMode::Up),
            ("down", DecimalRoundingMode::Down),
            ("ceiling", DecimalRoundingMode::Ceiling),
            ("floor", DecimalRoundingMode::Floor),
            ("half_up", DecimalRoundingMode::HalfUp),
            ("half_down", DecimalRoundingMode::HalfDown),
            ("half_even", DecimalRoundingMode::HalfEven),
            ("unnecessary", DecimalRoundingMode::Unnecessary),
        ];
        for (name, mode) in modes {
            let doc = pg(&format!(
                "decimal_mapping = \"allow_overflow\"\ndecimal_rounding_mode = \"{name}\"\ndecimal_default_scale = 38"
            ));
            assert_eq!(
                decimals(&doc),
                (DecimalMapping::AllowOverflow, mode, 38),
                "{name}"
            );
            parse(&doc).validate(&full_env, &|_| true).unwrap();
        }
        let errors = errors_of(&pg("decimal_default_scale = 39"), &full_env);
        assert_eq!(
            errors,
            ["source `hub`: decimal_default_scale must be between 0 and 38"]
        );
        for wrong in [
            "decimal_mapping = \"STRICT\"",
            "decimal_mapping = \"overflow\"",
            "decimal_rounding_mode = \"HALF_UP\"",
            "decimal_default_scale = -1",
        ] {
            assert!(toml::from_str::<Config>(&pg(wrong)).is_err(), "{wrong}");
        }
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
    #[test]
    fn metastore_names_default_to_codelakes_and_are_checked() {
        let names = MetastoreNames::default();
        assert_eq!(names.metadata_file, ".codelakeSchema");
        assert!(names.problems().is_empty());
        let doc = format!(
            "{VALID}\n[[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"/wh\"\n\
             [source.metastore_names]\nmetadata_file = \"schema\"\npermissions_dir = \"schema\"\nview_marker = \"x */\"\n"
        );
        let errors = parse(&doc)
            .validate(&full_env, &|_| true)
            .unwrap_err()
            .0
            .join("\n");
        assert!(
            errors.contains("metastore_names.metadata_file must be a hidden file name"),
            "{errors}"
        );
        assert!(
            errors.contains("metadata_file and permissions_dir must differ"),
            "{errors}"
        );
        assert!(
            errors.contains("metastore_names.view_marker must not be empty"),
            "{errors}"
        );
        let unknown = format!(
            "{VALID}\n[[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"/wh\"\n[source.metastore_names]\nmetadata = \".x\"\n"
        );
        assert!(Config::parse(std::path::Path::new("t.toml"), &unknown, &full_env).is_err());
    }
}
