//! Workload manifests (`bench/workloads/*.toml`): the SQL both engines run.
//!
//! Statements are templates. Placeholders:
//! `{cat}` catalog, `{tpch}` TPC-H schema, `{work}` per-engine scratch schema
//! (engines share the warehouse, so their writes must not collide) and `{i}`
//! the repetition index (warmup included), for statements that must differ
//! between repetitions.

use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    workload: String,
    #[serde(default, rename = "case")]
    cases: Vec<CaseSpec>,
}

/// One benchmark case as written in a manifest.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaseSpec {
    pub(crate) id: String,
    /// Timed statement, inline...
    pub(crate) sql: Option<String>,
    /// ...or in a file relative to the manifest.
    pub(crate) sql_file: Option<PathBuf>,
    /// Untimed, once before the first repetition.
    #[serde(default)]
    pub(crate) setup: Vec<String>,
    /// Untimed, before every repetition (e.g. `DROP TABLE IF EXISTS`).
    #[serde(default)]
    pub(crate) before_each: Vec<String>,
    /// Untimed, once after the last repetition; its result is what the eval
    /// compares. Without it, the first measured repetition's result is used.
    pub(crate) verify: Option<String>,
    /// Untimed, once at the end.
    #[serde(default)]
    pub(crate) teardown: Vec<String>,
    /// Creates or drops tables: needs DDL routed to the Hive catalog, which the
    /// PostgreSQL wire path of `CodeLake` does not do yet (ROADMAP M1.1).
    #[serde(default)]
    pub(crate) writes: bool,
    /// Overrides the global repetition count (short queries need more samples).
    pub(crate) reps: Option<usize>,
    pub(crate) warmup: Option<usize>,
}

/// A case ready to run: SQL loaded, workload name attached.
#[derive(Debug, Clone)]
pub(crate) struct Case {
    pub(crate) workload: String,
    pub(crate) spec: CaseSpec,
    pub(crate) sql: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkloadError {
    #[error("{path}: {cause}")]
    Io { path: PathBuf, cause: std::io::Error },
    #[error("{path}: {cause}")]
    Parse { path: PathBuf, cause: toml::de::Error },
    #[error("{path}: case `{id}` needs exactly one of `sql` or `sql_file`")]
    NoSql { path: PathBuf, id: String },
    #[error("duplicate case id `{0}`")]
    Duplicate(String),
    #[error("unknown placeholder in `{0}`")]
    Placeholder(String),
    #[error("repetition override `{0}`: expected `<case id>=<reps>` or `<case id>=<reps>/<warmup>`")]
    OverrideSyntax(String),
    #[error("repetition override for unknown case `{0}`")]
    OverrideCase(String),
}

fn read(path: &Path) -> Result<String, WorkloadError> {
    std::fs::read_to_string(path).map_err(|cause| WorkloadError::Io {
        path: path.to_owned(),
        cause,
    })
}

/// Loads `<dir>/<name>.toml` for each requested workload, in order.
pub(crate) fn load(dir: &Path, names: &[String]) -> Result<Vec<Case>, WorkloadError> {
    let mut cases: Vec<Case> = Vec::new();
    for name in names {
        let path = dir.join(format!("{name}.toml"));
        let manifest: Manifest = toml::from_str(&read(&path)?).map_err(|cause| WorkloadError::Parse {
            path: path.clone(),
            cause,
        })?;
        let base = path.parent().unwrap_or(dir);
        for spec in manifest.cases {
            let sql = match (&spec.sql, &spec.sql_file) {
                (Some(sql), None) => sql.clone(),
                (None, Some(file)) => read(&base.join(file))?,
                _ => {
                    return Err(WorkloadError::NoSql {
                        path: path.clone(),
                        id: spec.id.clone(),
                    });
                }
            };
            if cases.iter().any(|c| c.spec.id == spec.id) {
                return Err(WorkloadError::Duplicate(spec.id));
            }
            cases.push(Case {
                workload: manifest.workload.clone(),
                sql: sql.trim().trim_end_matches(';').trim().to_owned(),
                spec,
            });
        }
    }
    Ok(cases)
}

/// Applies run-time repetition overrides (`--case-reps`, `BENCH_CASE_REPS`):
/// `<case id>=<reps>` or `<case id>=<reps>/<warmup>`. They win over the
/// manifest, so a large scale can run its longest cases fewer times without
/// editing the workload files; the run records them. An unknown id is an error
/// (a typo must not silently keep the long default).
pub(crate) fn apply_overrides(cases: &mut [Case], overrides: &[String]) -> Result<(), WorkloadError> {
    for text in overrides.iter().map(|o| o.trim()).filter(|o| !o.is_empty()) {
        let syntax = || WorkloadError::OverrideSyntax(text.to_owned());
        let (id, counts) = text.split_once('=').ok_or_else(syntax)?;
        let (reps, warmup) = match counts.split_once('/') {
            Some((reps, warmup)) => (reps, Some(warmup)),
            None => (counts, None),
        };
        let reps: usize = reps.trim().parse().map_err(|_| syntax())?;
        let warmup: Option<usize> = warmup
            .map(|w| w.trim().parse().map_err(|_| syntax()))
            .transpose()?;
        if reps == 0 {
            return Err(syntax());
        }
        let case = cases
            .iter_mut()
            .find(|c| c.spec.id == id.trim())
            .ok_or_else(|| WorkloadError::OverrideCase(id.trim().to_owned()))?;
        case.spec.reps = Some(reps);
        if warmup.is_some() {
            case.spec.warmup = warmup;
        }
    }
    Ok(())
}

/// Values for the placeholders of one engine run.
#[derive(Debug, Clone)]
pub(crate) struct Vars {
    pub(crate) catalog: String,
    pub(crate) tpch: String,
    pub(crate) work: String,
}

impl Vars {
    pub(crate) fn render(&self, template: &str, i: usize) -> Result<String, WorkloadError> {
        let out = template
            .replace("{cat}", &self.catalog)
            .replace("{tpch}", &self.tpch)
            .replace("{work}", &self.work)
            .replace("{i}", &i.to_string());
        if out.contains('{') {
            return Err(WorkloadError::Placeholder(template.to_owned()));
        }
        Ok(out)
    }
}

/// TPC-H source schema in Trino's `tpch` connector and target schema in the
/// warehouse for a scale (`tiny`, `1`, `10`, ...).
pub(crate) fn tpch_schemas(scale: &str) -> (String, String) {
    if scale == "tiny" {
        ("tiny".to_owned(), "tpch_tiny".to_owned())
    } else {
        (format!("sf{scale}"), format!("tpch_sf{scale}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> Vars {
        Vars {
            catalog: "hive".into(),
            tpch: "tpch_sf1".into(),
            work: "bench_trino".into(),
        }
    }

    #[test]
    fn templates_render_known_placeholders_only() {
        assert_eq!(
            vars()
                .render("SELECT * FROM {cat}.{tpch}.t WHERE k = {i} -- {work}", 3)
                .unwrap(),
            "SELECT * FROM hive.tpch_sf1.t WHERE k = 3 -- bench_trino"
        );
        assert!(vars().render("SELECT {oops}", 0).is_err());
    }

    #[test]
    fn manifests_load_inline_and_file_sql() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("q.sql"), "SELECT 1;\n").unwrap();
        std::fs::write(
            dir.path().join("w.toml"),
            "workload = \"w\"\n[[case]]\nid = \"a\"\nsql = \"SELECT 2\"\nreps = 3\n\
             [[case]]\nid = \"b\"\nsql_file = \"q.sql\"\nwrites = true\n",
        )
        .unwrap();
        let cases = load(dir.path(), &["w".into()]).unwrap();
        assert_eq!(cases.len(), 2);
        assert_eq!(cases[0].sql, "SELECT 2");
        assert_eq!(cases[0].spec.reps, Some(3));
        assert_eq!(cases[1].sql, "SELECT 1");
        assert!(cases[1].spec.writes);

        std::fs::write(
            dir.path().join("bad.toml"),
            "workload = \"w\"\n[[case]]\nid = \"a\"\n",
        )
        .unwrap();
        assert!(matches!(
            load(dir.path(), &["bad".into()]),
            Err(WorkloadError::NoSql { .. })
        ));
        assert!(matches!(
            load(dir.path(), &["w".into(), "w".into()]),
            Err(WorkloadError::Duplicate(_))
        ));
    }

    #[test]
    fn overrides_replace_reps_and_optionally_warmup() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("w.toml"),
            "workload = \"w\"\n[[case]]\nid = \"a\"\nsql = \"SELECT 1\"\nreps = 3\nwarmup = 2\n\
             [[case]]\nid = \"b\"\nsql = \"SELECT 2\"\n",
        )
        .unwrap();
        let mut cases = load(dir.path(), &["w".into()]).unwrap();
        apply_overrides(&mut cases, &["a=1/0".into(), " b=2 ".into(), String::new()]).unwrap();
        assert_eq!((cases[0].spec.reps, cases[0].spec.warmup), (Some(1), Some(0)));
        assert_eq!((cases[1].spec.reps, cases[1].spec.warmup), (Some(2), None));

        for bad in ["a", "a=x", "a=0", "a=1/x"] {
            assert!(matches!(
                apply_overrides(&mut cases, &[bad.into()]),
                Err(WorkloadError::OverrideSyntax(_))
            ));
        }
        assert!(matches!(
            apply_overrides(&mut cases, &["zz=1".into()]),
            Err(WorkloadError::OverrideCase(_))
        ));
    }

    #[test]
    fn tpch_scale_names() {
        assert_eq!(tpch_schemas("1"), ("sf1".into(), "tpch_sf1".into()));
        assert_eq!(tpch_schemas("tiny"), ("tiny".into(), "tpch_tiny".into()));
    }
}
