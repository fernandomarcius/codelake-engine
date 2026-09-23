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
fn default_region() -> String {
    "us-east-1".to_owned()
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
            if name == "datafusion" || name == "information_schema" {
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
