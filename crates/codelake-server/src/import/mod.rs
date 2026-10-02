//! `codelake config import`: turns a directory of catalog-properties files
//! (`<catalog>.properties` with `connector.name=…`, the format query engines
//! of the Hive/JDBC family read) into `[[source]]` entries of a `codelake.toml`.
//!
//! Secret values never reach the TOML file, standard output or standard
//! error: the TOML names environment variables, and their values go to an
//! env file written with mode `0600`. The report lists every catalog with
//! its status and names only (settings, variables), never values.
//!
//! `${ENV:NAME}` in a catalog file (the syntax catalog-properties files use
//! for environment variables) is never resolved here: in a secret it becomes
//! the `*_env` key (`password_env = "NAME"`), elsewhere it is written as it
//! is and the server resolves it when it loads `codelake.toml`
//! ([`codelake_core::env_refs`]). The import does not read the environment.

mod jdbc;
mod mapping;
mod properties;

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Path, PathBuf};

use clap::Args;
use codelake_core::env_refs;
use codelake_core::parquet_time_zone::ParquetTimeZone;
use secrecy::ExposeSecret;
use toml::Value;

use mapping::{Context, EnvSecret, Imported, Skipped};

/// Arguments of `codelake config import`.
#[derive(Debug, Args)]
pub(crate) struct ImportArgs {
    /// Directory with one `<catalog>.properties` file per catalog.
    #[arg(long)]
    pub(crate) from: PathBuf,
    /// TOML file to write.
    #[arg(long)]
    pub(crate) out: PathBuf,
    /// Env file for the secret values (written with mode 0600).
    #[arg(long)]
    pub(crate) env_out: PathBuf,
    /// Prefix of the generated environment variable names.
    #[arg(long, default_value = "CODELAKE_")]
    pub(crate) env_prefix: String,
    /// Existing configuration whose `[server]`, `[memory]`, … sections (and
    /// sources) the imported sources are appended to. Without it, a minimal
    /// loopback-only header is written.
    #[arg(long)]
    pub(crate) base: Option<PathBuf>,
    /// Replace `--out` and `--env-out` when they exist.
    #[arg(long)]
    pub(crate) force: bool,
    /// IANA time zone of the reference engine's JVM, for hive catalogs
    /// without `hive.parquet.time-zone` (their `INT96` timestamps use it).
    /// Without it, such catalogs are skipped.
    #[arg(long)]
    pub(crate) reference_time_zone: Option<String>,
}

/// What an import did, for the exit code.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub(crate) imported: usize,
    pub(crate) skipped: usize,
}

/// An import that wrote nothing.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ImportError {
    #[error("cannot read {path}: {source}")]
    Read { path: PathBuf, source: std::io::Error },
    #[error("cannot write {path}: {source}")]
    Write { path: PathBuf, source: std::io::Error },
    #[error("{0} exists; pass --force to replace it")]
    Exists(PathBuf),
    #[error("--env-prefix must match [A-Z_][A-Z0-9_]* (or be empty)")]
    Prefix,
    #[error("--reference-time-zone {0}")]
    TimeZone(String),
    #[error("{0} has no *.properties file")]
    Empty(PathBuf),
    #[error("base configuration {path}: {problem}")]
    Base { path: PathBuf, problem: String },
    #[error("the generated configuration is invalid:\n  - {}", .0.join("\n  - "))]
    Generated(Vec<String>),
}

/// Written when no `--base` is given: loopback listeners only.
const DEFAULT_HEADER: &str = r#"[server]
pg_listen = "127.0.0.1:5432"
http_listen = "127.0.0.1:8080"

[memory]
# Keep about 75% of the container's memory limit (docs/adr/0003-memory-model.md).
limit = "3GiB"
spill_dir = "/var/lib/codelake/spill"
spill_limit = "40GiB"
"#;

/// Keys of a source in the order they are written; any other key follows.
const KEY_ORDER: [&str; 21] = [
    "name",
    "kind",
    "host",
    "port",
    "database",
    "user",
    "password_env",
    "encrypt",
    "trust_server_certificate",
    "sslmode",
    "decimal_mapping",
    "decimal_rounding_mode",
    "decimal_default_scale",
    "warehouse",
    "s3_endpoint",
    "s3_region",
    "s3_path_style",
    "access_key_id_env",
    "secret_access_key_env",
    "timestamp_precision",
    "parquet_time_zone",
];

enum Entry {
    Imported(Imported),
    Skipped(Skipped),
}

struct Catalog {
    name: String,
    file: String,
    entry: Entry,
}

fn valid_prefix(prefix: &str) -> bool {
    let mut chars = prefix.chars();
    match chars.next() {
        None => true,
        Some(c) => {
            (c.is_ascii_uppercase() || c == '_')
                && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        }
    }
}

/// Reads and maps every `*.properties` file of `dir`, in name order.
fn read_catalogs(dir: &Path, ctx: &Context<'_>) -> Result<Vec<Catalog>, ImportError> {
    let read_err = |source| ImportError::Read {
        path: dir.to_owned(),
        source,
    };
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(read_err)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|e| e == "properties") && path.is_file())
        .collect();
    files.sort();
    if files.is_empty() {
        return Err(ImportError::Empty(dir.to_owned()));
    }
    let mut catalogs = Vec::with_capacity(files.len());
    for path in files {
        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let file = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let bytes = std::fs::read(&path).map_err(|source| ImportError::Read {
            path: path.clone(),
            source,
        })?;
        let entry = match properties::parse(&properties::decode(&bytes)) {
            Ok(props) => match mapping::map_catalog(&name, props, ctx) {
                Ok(imported) => Entry::Imported(imported),
                Err(skipped) => Entry::Skipped(skipped),
            },
            Err(err) => Entry::Skipped(Skipped {
                connector: "?".to_owned(),
                reason: format!("malformed file, {err}"),
            }),
        };
        catalogs.push(Catalog { name, file, entry });
    }
    Ok(catalogs)
}

/// Source names the base configuration already declares.
fn base_sources(path: &Path, text: &str) -> Result<BTreeSet<String>, ImportError> {
    let base_err = |problem: String| ImportError::Base {
        path: path.to_owned(),
        problem,
    };
    let doc: toml::Table = toml::from_str(text).map_err(|e| base_err(e.message().to_owned()))?;
    let mut names = BTreeSet::new();
    if let Some(sources) = doc.get("source") {
        let sources = sources
            .as_array()
            .ok_or_else(|| base_err("`source` must be an array of tables".to_owned()))?;
        for source in sources {
            if let Some(name) = source.get("name").and_then(Value::as_str) {
                names.insert(name.to_owned());
            }
        }
    }
    Ok(names)
}

fn render_source(catalog: &Catalog, imported: &Imported) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "\n# {} (connector.name={})",
        catalog.file, imported.connector
    );
    for note in &imported.notes {
        let _ = writeln!(out, "# note: {note}");
    }
    out.push_str("[[source]]\n");
    let source = &imported.source;
    let ordered = KEY_ORDER
        .iter()
        .filter_map(|k| source.get_key_value(*k))
        .chain(source.iter().filter(|(k, _)| !KEY_ORDER.contains(&k.as_str())));
    for (key, value) in ordered {
        let _ = writeln!(out, "{key} = {value}");
    }
    out
}

/// The env file: `NAME=value` lines, no quoting, read literally by
/// `docker --env-file` and `kubectl create secret generic --from-env-file`.
/// Compose `env_file` and systemd `EnvironmentFile` interpret some
/// characters; the report names every variable whose value they would change
/// (`mapping::literal_only`).
fn render_env(secrets: &[&EnvSecret]) -> String {
    let mut out = String::from(
        "# Secrets for the sources of the generated configuration. Keep this file\n\
         # out of version control. One NAME=value per line, value unquoted.\n",
    );
    for secret in secrets {
        out.push_str(&secret.var);
        out.push('=');
        out.push_str(secret.value.expose_secret());
        out.push('\n');
    }
    out
}

/// Writes through a temporary file in the same directory, then renames it,
/// so a reader never sees a partial file. `mode` applies before any byte is
/// written.
fn write_atomically(path: &Path, content: &str, mode: Option<u32>) -> Result<(), ImportError> {
    let write_err = |source| ImportError::Write {
        path: path.to_owned(),
        source,
    };
    let file_name = path
        .file_name()
        .ok_or_else(|| write_err(std::io::Error::other("not a file path")))?;
    let tmp = path.with_file_name(format!(
        ".{}.tmp-{}",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    if mode.is_some() {
        return Err(write_err(std::io::Error::other(
            "restrictive file modes need a Unix platform",
        )));
    }
    let result = (|| {
        let mut file = options.open(&tmp)?;
        // The mode given to open() is reduced by the umask, never widened;
        // set it exactly.
        #[cfg(unix)]
        if let Some(mode) = mode {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
        }
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result.map_err(write_err)
}

/// Runs the import and writes the report to `report`.
pub(crate) fn run(args: &ImportArgs, report: &mut dyn Write) -> Result<Outcome, ImportError> {
    if !valid_prefix(&args.env_prefix) {
        return Err(ImportError::Prefix);
    }
    for path in [&args.out, &args.env_out] {
        if !args.force && path.exists() {
            return Err(ImportError::Exists(path.clone()));
        }
    }
    let (header, mut taken) = match &args.base {
        Some(path) => {
            let text = std::fs::read_to_string(path).map_err(|source| ImportError::Read {
                path: path.clone(),
                source,
            })?;
            let names = base_sources(path, &text)?;
            (text, names)
        }
        None => (DEFAULT_HEADER.to_owned(), BTreeSet::new()),
    };
    let reference_time_zone = args
        .reference_time_zone
        .as_deref()
        .map(str::parse::<ParquetTimeZone>)
        .transpose()
        .map_err(ImportError::TimeZone)?;
    let ctx = Context {
        env_prefix: &args.env_prefix,
        reference_time_zone,
    };
    let mut catalogs = read_catalogs(&args.from, &ctx)?;
    for catalog in &mut catalogs {
        if let Entry::Imported(imported) = &catalog.entry
            && !taken.insert(catalog.name.clone())
        {
            catalog.entry = Entry::Skipped(Skipped {
                connector: imported.connector.clone(),
                reason: "a source with this name is already in the base configuration".to_owned(),
            });
        }
    }

    let mut toml_text = format!(
        "# Generated by `codelake config import --from {}`. Review before use.\n\
         # Secrets are not in this file: every *_env key names a variable whose\n\
         # value is in the env file (or must be provided by the environment).\n\n",
        args.from.display()
    );
    toml_text.push_str(&header);
    let mut sources_text = String::new();
    let mut secrets: Vec<&EnvSecret> = Vec::new();
    let mut references: BTreeSet<&str> = BTreeSet::new();
    for catalog in &catalogs {
        if let Entry::Imported(imported) = &catalog.entry {
            sources_text.push_str(&render_source(catalog, imported));
            secrets.extend(imported.secrets.iter());
            references.extend(imported.references.iter().map(String::as_str));
            references.extend(imported.value_references.iter().map(String::as_str));
        }
    }
    toml_text.push_str(&sources_text);
    // A placeholder can stand for a reference in a string, not in a size or
    // an address: with references in the base, only the imported sources are
    // checked here, under the default header.
    let base_has_references = header.contains(env_refs::REFERENCE_START);
    if base_has_references {
        check_generated(&format!("{DEFAULT_HEADER}{sources_text}"))?;
    } else {
        check_generated(&toml_text)?;
    }
    write_atomically(&args.out, &toml_text, None)?;
    write_atomically(&args.env_out, &render_env(&secrets), Some(0o600))?;

    let outcome = Outcome {
        imported: catalogs
            .iter()
            .filter(|c| matches!(c.entry, Entry::Imported(_)))
            .count(),
        skipped: catalogs
            .iter()
            .filter(|c| matches!(c.entry, Entry::Skipped(_)))
            .count(),
    };
    write_report(report, args, &catalogs, &secrets, &references, &outcome)
        .and_then(|()| {
            if base_has_references {
                writeln!(
                    report,
                    "The base configuration has ${{ENV:…}} references: validate the result with \
                     `codelake check-config` where they are set."
                )
            } else {
                Ok(())
            }
        })
        .map_err(|source| ImportError::Write {
            path: PathBuf::from("<stdout>"),
            source,
        })?;
    Ok(outcome)
}

/// The generated document must load: every field known, every rule that does
/// not depend on the environment or the file system satisfied. Every
/// variable (`${ENV:…}` and `*_env`) is taken as set to a placeholder.
fn check_generated(text: &str) -> Result<(), ImportError> {
    let placeholder = |_: &str| Some("set".to_owned());
    let config = codelake_core::Config::parse(Path::new("<generated>"), text, &placeholder)
        .map_err(|e| ImportError::Generated(vec![e.to_string()]))?;
    config
        .validate(&placeholder, &|_| true)
        .map(|_| ())
        .map_err(|errors| ImportError::Generated(errors.0))
}

fn write_report(
    out: &mut dyn Write,
    args: &ImportArgs,
    catalogs: &[Catalog],
    secrets: &[&EnvSecret],
    references: &BTreeSet<&str>,
    outcome: &Outcome,
) -> std::io::Result<()> {
    let width = catalogs
        .iter()
        .map(|c| c.name.len())
        .max()
        .unwrap_or(0)
        .max("catalog".len());
    writeln!(
        out,
        "{:width$}  {:<10}  {:<8}  details",
        "catalog", "connector", "status"
    )?;
    for catalog in catalogs {
        let (connector, status, details) = match &catalog.entry {
            Entry::Imported(imported) => {
                let vars: Vec<&str> = imported
                    .secrets
                    .iter()
                    .map(|s| s.var.as_str())
                    .chain(imported.references.iter().map(String::as_str))
                    .collect();
                let mut details = if vars.is_empty() {
                    "no secrets".to_owned()
                } else {
                    format!("secrets: {}", vars.join(", "))
                };
                let values: BTreeSet<&str> = imported.value_references.iter().map(String::as_str).collect();
                if !values.is_empty() {
                    let _ = write!(
                        details,
                        "; ${{ENV:…}} in values: {}",
                        values.into_iter().collect::<Vec<_>>().join(", ")
                    );
                }
                for note in &imported.notes {
                    let _ = write!(details, "; {note}");
                }
                (imported.connector.as_str(), "imported", details)
            }
            Entry::Skipped(skipped) => (skipped.connector.as_str(), "skipped", skipped.reason.clone()),
        };
        writeln!(
            out,
            "{:width$}  {connector:<10}  {status:<8}  {details}",
            catalog.name
        )?;
    }
    writeln!(
        out,
        "\n{} imported, {} skipped. Wrote {} and {} (mode 0600, {} variable(s)).",
        outcome.imported,
        outcome.skipped,
        args.out.display(),
        args.env_out.display(),
        secrets.len()
    )?;
    if !references.is_empty() {
        writeln!(
            out,
            "Referenced through ${{ENV:…}} and not in the env file; set them where the server runs: {}",
            references.iter().copied().collect::<Vec<_>>().join(", ")
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "Import-Test-Secret-7";

    fn fixture(dir: &Path) {
        std::fs::write(
            dir.join("src.properties"),
            format!(
                "# reference catalog\nconnector.name=sqlserver\n\
                 connection-url=jdbc:sqlserver://mssql:1433;\\\n    databaseName=shadow;encrypt=false\n\
                 connection-user=sa\nconnection-password={SECRET}\n"
            ),
        )
        .unwrap();
        std::fs::write(
            dir.join("hive.properties"),
            "connector.name=hive\nhive.metastore=file\nhive.metastore.catalog.dir=file:/warehouse\n\
             hive.parquet.time-zone=UTC\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("meta.properties"),
            "connector.name=hive\nhive.metastore.uri=thrift://meta:9083\n",
        )
        .unwrap();
        std::fs::write(dir.join("broken.properties"), "k=\\u12").unwrap();
        std::fs::write(dir.join("notes.txt"), "not a catalog").unwrap();
    }

    fn args(dir: &Path) -> ImportArgs {
        ImportArgs {
            from: dir.join("catalog"),
            out: dir.join("codelake.toml"),
            env_out: dir.join("codelake.env"),
            env_prefix: "CODELAKE_".to_owned(),
            base: None,
            force: false,
            reference_time_zone: None,
        }
    }

    fn setup() -> (tempfile::TempDir, ImportArgs) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("catalog")).unwrap();
        fixture(&dir.path().join("catalog"));
        let args = args(dir.path());
        (dir, args)
    }

    #[test]
    fn imports_a_directory_and_keeps_secrets_out_of_toml_and_report() {
        let (_dir, args) = setup();
        let mut report = Vec::new();
        let outcome = run(&args, &mut report).unwrap();
        assert_eq!(
            outcome,
            Outcome {
                imported: 2,
                skipped: 2
            }
        );
        let report = String::from_utf8(report).unwrap();
        let toml_text = std::fs::read_to_string(&args.out).unwrap();
        assert!(!report.contains(SECRET), "{report}");
        assert!(!toml_text.contains(SECRET), "{toml_text}");
        assert!(report.contains("meta"), "{report}");
        assert!(report.contains("hive.metastore is not set"), "{report}");
        assert!(report.contains("line 1: malformed"), "{report}");
        assert!(report.contains("CODELAKE_SRC_PASSWORD"), "{report}");

        let env = std::fs::read_to_string(&args.env_out).unwrap();
        assert!(
            env.contains(&format!("CODELAKE_SRC_PASSWORD={SECRET}\n")),
            "{env}"
        );

        // The generated file loads, and its secret comes from the env file.
        let config: codelake_core::Config = toml::from_str(&toml_text).unwrap();
        assert_eq!(config.sources.len(), 2);
        let validated = config
            .validate(
                &|name| (name == "CODELAKE_SRC_PASSWORD").then(|| SECRET.to_owned()),
                &|_| true,
            )
            .unwrap();
        assert!(validated.secrets.get("CODELAKE_SRC_PASSWORD").is_some());
        assert!(
            toml_text.contains("name = \"src\"\nkind = \"sqlserver\""),
            "{toml_text}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn env_file_is_owner_only_even_when_replaced() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_dir, mut args) = setup();
        std::fs::write(&args.env_out, "old").unwrap();
        std::fs::set_permissions(&args.env_out, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(run(&args, &mut Vec::new()), Err(ImportError::Exists(_))));
        args.force = true;
        run(&args, &mut Vec::new()).unwrap();
        let mode = std::fs::metadata(&args.env_out).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "{mode:o}");
        let leftovers: Vec<_> = std::fs::read_dir(args.env_out.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn base_configuration_is_kept_and_name_clashes_are_skipped() {
        let (dir, mut args) = setup();
        let base = dir.path().join("base.toml");
        std::fs::write(
            &base,
            "[server]\npg_listen = \"127.0.0.1:15432\"\nhttp_listen = \"127.0.0.1:18080\"\n\n\
             [memory]\nlimit = \"1GiB\"\nspill_dir = \"/tmp/spill\"\nspill_limit = \"2GiB\"\n\n\
             [[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"/shadow\"\n",
        )
        .unwrap();
        args.base = Some(base);
        let mut report = Vec::new();
        let outcome = run(&args, &mut report).unwrap();
        assert_eq!(outcome.imported, 1);
        let report = String::from_utf8(report).unwrap();
        assert!(report.contains("already in the base configuration"), "{report}");
        let text = std::fs::read_to_string(&args.out).unwrap();
        assert!(
            text.contains("127.0.0.1:15432") && text.contains("/shadow"),
            "{text}"
        );
    }

    #[test]
    fn invalid_prefix_and_empty_directory_write_nothing() {
        let (dir, mut args) = setup();
        args.env_prefix = "lower".to_owned();
        assert!(matches!(run(&args, &mut Vec::new()), Err(ImportError::Prefix)));
        args.env_prefix = String::new();
        let empty = dir.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        args.from = empty;
        assert!(matches!(run(&args, &mut Vec::new()), Err(ImportError::Empty(_))));
        assert!(!args.out.exists() && !args.env_out.exists());
    }

    #[test]
    fn a_base_with_references_is_kept_as_written() {
        let (dir, mut args) = setup();
        let base = dir.path().join("base.toml");
        std::fs::write(
            &base,
            "[server]\npg_listen = \"${ENV:PG_LISTEN}\"\nhttp_listen = \"127.0.0.1:18080\"\n\n\
             [memory]\nlimit = \"${ENV:MEMORY_LIMIT}\"\nspill_dir = \"/tmp/spill\"\nspill_limit = \"2GiB\"\n",
        )
        .unwrap();
        args.base = Some(base);
        let mut report = Vec::new();
        run(&args, &mut report).unwrap();
        let report = String::from_utf8(report).unwrap();
        assert!(report.contains("codelake check-config"), "{report}");
        let text = std::fs::read_to_string(&args.out).unwrap();
        assert!(text.contains("limit = \"${ENV:MEMORY_LIMIT}\""), "{text}");
        let env = |name: &str| match name {
            "PG_LISTEN" => Some("127.0.0.1:15432".to_owned()),
            "MEMORY_LIMIT" => Some("1GiB".to_owned()),
            _ => None,
        };
        let config = codelake_core::Config::parse(&args.out, &text, &env).unwrap();
        assert_eq!(config.memory.limit, codelake_core::ByteSize::gib(1));
    }

    #[test]
    fn an_invalid_base_is_refused() {
        let (dir, mut args) = setup();
        let base = dir.path().join("base.toml");
        std::fs::write(&base, "[server\n").unwrap();
        args.base = Some(base.clone());
        assert!(matches!(
            run(&args, &mut Vec::new()),
            Err(ImportError::Base { .. })
        ));
        std::fs::write(&base, "[server]\npg_listen = \"0.0.0.0:5432\"\nhttp_listen = \"127.0.0.1:8080\"\n[memory]\nlimit = \"1GiB\"\nspill_dir = \"/s\"\nspill_limit = \"1GiB\"\n").unwrap();
        let err = run(&args, &mut Vec::new()).unwrap_err();
        assert!(err.to_string().contains("client authentication"), "{err}");
        assert!(!args.out.exists());
    }

    #[test]
    fn references_reach_the_generated_file_and_resolve_when_it_loads() {
        let (_dir, args) = setup();
        std::fs::write(
            args.from.join("src.properties"),
            "connector.name=sqlserver\n\
             connection-url=jdbc:sqlserver://${ENV:SRC_HOST}:1433;databaseName=shadow;encrypt=false\n\
             connection-user=${ENV:SRC_USER}\nconnection-password=${ENV:SRC_PASSWORD}\n",
        )
        .unwrap();
        let mut report = Vec::new();
        run(&args, &mut report).unwrap();
        let report = String::from_utf8(report).unwrap();
        assert!(report.contains("secrets: SRC_PASSWORD"), "{report}");
        assert!(
            report.contains("${ENV:…} in values: SRC_HOST, SRC_USER"),
            "{report}"
        );
        assert!(
            report.contains("set them where the server runs: SRC_HOST, SRC_PASSWORD, SRC_USER"),
            "{report}"
        );
        let text = std::fs::read_to_string(&args.out).unwrap();
        assert!(text.contains("host = \"${ENV:SRC_HOST}\""), "{text}");
        let env = std::fs::read_to_string(&args.env_out).unwrap();
        assert!(!env.contains("SRC_"), "{env}");
        let values = |name: &str| match name {
            "SRC_HOST" => Some("mssql.test".to_owned()),
            "SRC_USER" => Some("reader".to_owned()),
            "SRC_PASSWORD" => Some("pw".to_owned()),
            _ => None,
        };
        let config = codelake_core::Config::parse(&args.out, &text, &values).unwrap();
        let source = config.sources.iter().find(|s| s.name == "src").unwrap();
        match &source.kind {
            codelake_core::SourceKind::Sqlserver { host, user, .. } => {
                assert_eq!((host.as_str(), user.as_str()), ("mssql.test", "reader"));
            }
            other => panic!("unexpected {other:?}"),
        }
        config.validate(&values, &|_| true).unwrap();
    }

    #[test]
    fn a_hive_catalog_without_its_time_zone_needs_the_reference_zone() {
        let (_dir, mut args) = setup();
        std::fs::write(
            args.from.join("hive.properties"),
            "connector.name=hive\nhive.metastore=file\nhive.metastore.catalog.dir=/warehouse\n",
        )
        .unwrap();
        let mut report = Vec::new();
        let outcome = run(&args, &mut report).unwrap();
        assert_eq!(outcome.imported, 1);
        let report = String::from_utf8(report).unwrap();
        assert!(report.contains("pass --reference-time-zone"), "{report}");

        args.force = true;
        args.reference_time_zone = Some("Brasilia".to_owned());
        let err = run(&args, &mut Vec::new()).unwrap_err();
        assert!(matches!(err, ImportError::TimeZone(_)), "{err}");
        args.reference_time_zone = Some("America/Sao_Paulo".to_owned());
        assert_eq!(run(&args, &mut Vec::new()).unwrap().imported, 2);
        let text = std::fs::read_to_string(&args.out).unwrap();
        assert!(
            text.contains("warehouse = \"/warehouse\"\nparquet_time_zone = \"America/Sao_Paulo\""),
            "{text}"
        );
    }

    #[test]
    fn prefix_rules() {
        assert!(valid_prefix(""));
        assert!(valid_prefix("CODELAKE_"));
        assert!(valid_prefix("_X9"));
        assert!(!valid_prefix("9X"));
        assert!(!valid_prefix("x"));
    }
}
