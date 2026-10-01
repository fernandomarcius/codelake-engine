//! Renames the metastore files and markers of a local warehouse from the
//! names another engine wrote to a source's `metastore_names`
//! (`codelake hive migrate-names`, ISS-011).
//!
//! For every schema file (`.<schema><metadata file>`) and every table or view
//! (`<schema>/<table>/<metadata file>`): the metadata is rewritten under the
//! new name with its checksum (`writerVersion`, the version and view
//! parameters, a view's comment and SQL markers), then the old file and its
//! checksum are removed; a table's permissions directory is renamed. Data
//! files are not touched.
//!
//! Hidden directories (a name starting with `.`) are not schemas or tables,
//! as the warehouse's listing skips them: a backup copy of a table
//! (`.bkp_<date>`) keeps its old names.
//!
//! The whole plan is computed first; it is refused, with nothing changed,
//! when a new name already exists, a metadata file does not parse, a table
//! has partitions (their metadata files are not migrated: the engine does
//! not read partitioned tables) or a directory the plan changes cannot be
//! written. Numbers are rewritten exactly as they were read (statistics keep
//! every digit). Without `apply` only the plan is returned.

use std::path::{Path, PathBuf};

use codelake_core::config::MetastoreNames;
use serde_json::Value;

use super::crc::{crc_name, hadoop_crc};

/// One change of the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Metadata rewritten under a new name (the old file and its checksum go).
    Rewrite { from: PathBuf, to: PathBuf },
    /// Permissions directory renamed.
    Rename { from: PathBuf, to: PathBuf },
}

/// Why a migration was refused.
#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error("{0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("{0} already exists: nothing changed")]
    Exists(PathBuf),
    #[error("{0} is not valid metadata JSON ({1}): nothing changed")]
    Json(PathBuf, serde_json::Error),
    #[error(
        "{0} is a partitioned table: its partition metadata is not migrated, so the plan is \
         refused (nothing changed)"
    )]
    Partitioned(PathBuf),
    #[error("{0} cannot be written ({1}): nothing changed")]
    NotWritable(PathBuf, std::io::Error),
    #[error("old and new names are the same: nothing to do")]
    Same,
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> MigrateError + '_ {
    move |e| MigrateError::Io(path.to_path_buf(), e)
}

fn entries(dir: &Path) -> Result<Vec<(String, PathBuf, bool)>, MigrateError> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(io(dir))? {
        let entry = entry.map_err(io(dir))?;
        let path = entry.path();
        let is_dir = entry.file_type().map_err(io(&path))?.is_dir();
        out.push((entry.file_name().to_string_lossy().into_owned(), path, is_dir));
    }
    out.sort();
    Ok(out)
}

/// The plan (see the module documentation), applied when `apply`.
///
/// # Errors
///
/// A new name that exists, a metadata file that does not parse, or an I/O
/// failure; nothing is changed by a refused plan.
pub fn migrate_names(
    root: &Path,
    old: &MetastoreNames,
    new: &MetastoreNames,
    apply: bool,
) -> Result<Vec<Step>, MigrateError> {
    if old.metadata_file == new.metadata_file && old.permissions_dir == new.permissions_dir {
        return Err(MigrateError::Same);
    }
    let mut steps = Vec::new();
    for (name, path, is_dir) in entries(root)? {
        if !is_dir {
            if let Some(schema) = name
                .strip_prefix('.')
                .and_then(|n| n.strip_suffix(old.metadata_file.as_str()))
                .filter(|s| !s.is_empty() && !s.starts_with('.'))
            {
                let to = root.join(format!(".{schema}{}", new.metadata_file));
                steps.push(Step::Rewrite { from: path, to });
            }
            continue;
        }
        if name.starts_with('.') {
            continue;
        }
        for (table, table_dir, table_is_dir) in entries(&path)? {
            if !table_is_dir || table.starts_with('.') {
                continue;
            }
            let metadata = table_dir.join(&old.metadata_file);
            if metadata.is_file() {
                steps.push(Step::Rewrite {
                    from: metadata,
                    to: table_dir.join(&new.metadata_file),
                });
            }
            let permissions = table_dir.join(&old.permissions_dir);
            if permissions.is_dir() {
                steps.push(Step::Rename {
                    from: permissions,
                    to: table_dir.join(&new.permissions_dir),
                });
            }
        }
    }
    // Check everything before changing anything.
    let mut rewritten = Vec::new();
    for step in &steps {
        let (Step::Rewrite { to, .. } | Step::Rename { to, .. }) = step;
        if to.exists() {
            return Err(MigrateError::Exists(to.clone()));
        }
        if let Step::Rewrite { from, .. } = step {
            let bytes = std::fs::read(from).map_err(io(from))?;
            let mut json: Value =
                serde_json::from_slice(&bytes).map_err(|e| MigrateError::Json(from.clone(), e))?;
            if json
                .get("partitionColumns")
                .and_then(Value::as_array)
                .is_some_and(|columns| !columns.is_empty())
            {
                return Err(MigrateError::Partitioned(from.clone()));
            }
            rewrite_markers(&mut json, old, new);
            let bytes = serde_json::to_vec_pretty(&json).map_err(|e| MigrateError::Json(from.clone(), e))?;
            rewritten.push(bytes);
        }
    }
    if !apply {
        return Ok(steps);
    }
    // Every directory the plan writes to, before writing anything.
    let mut dirs: Vec<&Path> = steps
        .iter()
        .filter_map(|step| {
            let (Step::Rewrite { from, .. } | Step::Rename { from, .. }) = step;
            from.parent()
        })
        .collect();
    dirs.sort_unstable();
    dirs.dedup();
    for dir in dirs {
        check_writable(dir)?;
    }
    // New files and checksums first: an interrupted run leaves the old
    // metadata readable under the old names. A write that fails removes the
    // ones already written, so the warehouse is left as it was.
    let mut written: Vec<&Path> = Vec::new();
    for (step, bytes) in steps.iter().filter_map(rewrite_target).zip(&rewritten) {
        if let Err(err) = write_with_crc(step, bytes) {
            for path in written.into_iter().chain([step]) {
                let _ = std::fs::remove_file(path);
                let _ = std::fs::remove_file(sibling(path, &crc_name(&file_name(path))));
            }
            return Err(err);
        }
        written.push(step);
    }
    for step in &steps {
        match step {
            Step::Rewrite { from, .. } => {
                let old_crc = sibling(from, &crc_name(&file_name(from)));
                std::fs::remove_file(from).map_err(io(from))?;
                if old_crc.exists() {
                    std::fs::remove_file(&old_crc).map_err(io(&old_crc))?;
                }
            }
            Step::Rename { from, to } => std::fs::rename(from, to).map_err(io(from))?,
        }
    }
    Ok(steps)
}

fn rewrite_target(step: &Step) -> Option<&Path> {
    match step {
        Step::Rewrite { to, .. } => Some(to),
        Step::Rename { .. } => None,
    }
}

/// Whether files can be created and removed in `dir`: a probe file is
/// created there and removed (permissions alone do not tell a read-only
/// mount or an immutable directory).
fn check_writable(dir: &Path) -> Result<(), MigrateError> {
    let probe = dir.join(format!(".codelake-migrate-{}", uuid::Uuid::new_v4().simple()));
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .map_err(|e| MigrateError::NotWritable(dir.to_path_buf(), e))?;
    std::fs::remove_file(&probe).map_err(|e| MigrateError::NotWritable(dir.to_path_buf(), e))
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn sibling(path: &Path, name: &str) -> PathBuf {
    path.with_file_name(name)
}

fn write_with_crc(path: &Path, bytes: &[u8]) -> Result<(), MigrateError> {
    std::fs::write(path, bytes).map_err(io(path))?;
    let crc = sibling(path, &crc_name(&file_name(path)));
    std::fs::write(&crc, hadoop_crc(bytes)).map_err(io(&crc))
}

/// The writer version, the version and view parameters, and a view's comment
/// and SQL markers, from the old names to the new ones.
fn rewrite_markers(json: &mut Value, old: &MetastoreNames, new: &MetastoreNames) {
    let Some(object) = json.as_object_mut() else {
        return;
    };
    if object.contains_key("writerVersion") {
        object.insert("writerVersion".into(), Value::String(new.writer_version.clone()));
    }
    if let Some(Value::Object(parameters)) = object.get_mut("parameters") {
        if parameters.remove(&old.version_parameter).is_some() {
            parameters.insert(
                new.version_parameter.clone(),
                Value::String(new.writer_version.clone()),
            );
        }
        if let Some(flag) = parameters.remove(&old.view_parameter) {
            parameters.insert(new.view_parameter.clone(), flag);
        }
        if parameters.get("comment").and_then(Value::as_str) == Some(old.view_marker.as_str()) {
            parameters.insert("comment".into(), Value::String(new.view_marker.clone()));
        }
    }
    let old_prefix = format!("/* {}: ", old.view_marker);
    if let Some(Value::String(text)) = object.get_mut("viewOriginalText")
        && let Some(rest) = text.strip_prefix(old_prefix.as_str())
    {
        *text = format!("/* {}: {rest}", new.view_marker);
    }
    if let Some(Value::String(text)) = object.get_mut("viewExpandedText")
        && *text == format!("/* {} */", old.view_marker)
    {
        *text = format!("/* {} */", new.view_marker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A copy of the fixture warehouse Trino 431 wrote (tables, a view, their
    /// checksums), migrated to `CodeLake`'s names and read back.
    #[tokio::test]
    async fn migrates_a_warehouse_the_reference_engine_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let fixture =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431/warehouse");
        copy_dir(&fixture, dir.path());
        let old = crate::hive::reference_names();
        let new = MetastoreNames::default();
        let plan = migrate_names(dir.path(), &old, &new, false).unwrap();
        assert!(!plan.is_empty());
        assert!(
            dir.path().join(".pilot.trinoSchema").exists(),
            "a dry run changes nothing"
        );
        let applied = migrate_names(dir.path(), &old, &new, true).unwrap();
        assert_eq!(applied, plan);
        assert!(!dir.path().join(".pilot.trinoSchema").exists());
        assert!(dir.path().join(".pilot.codelakeSchema").is_file());
        assert!(dir.path().join("..pilot.codelakeSchema.crc").is_file());
        let view: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("pilot/v1/.codelakeSchema")).unwrap())
                .unwrap();
        assert_eq!(view["parameters"]["codelake_view"], "true");
        assert!(
            view["viewOriginalText"]
                .as_str()
                .unwrap()
                .starts_with("/* CodeLake View: ")
        );
        assert_eq!(view["writerVersion"], new.writer_version.as_str());
        // The engine reads the migrated warehouse with its default names.
        let store = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let metastore = super::super::metastore::FileMetastore::new(
            std::sync::Arc::new(store),
            object_store::path::Path::default(),
        );
        assert!(
            metastore
                .list_schemas()
                .await
                .unwrap()
                .contains(&"pilot".to_owned())
        );
        let tables = metastore.list_tables("pilot").await.unwrap();
        assert!(
            tables.contains(&"t1".to_owned()) && tables.contains(&"v1".to_owned()),
            "{tables:?}"
        );
        // Again: nothing left under the old names.
        assert!(migrate_names(dir.path(), &old, &new, true).unwrap().is_empty());
    }

    #[test]
    fn an_existing_new_name_refuses_the_whole_plan() {
        let dir = tempfile::tempdir().unwrap();
        let fixture =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431/warehouse");
        copy_dir(&fixture, dir.path());
        std::fs::write(dir.path().join("pilot/t1/.codelakeSchema"), b"{}").unwrap();
        let err = migrate_names(
            dir.path(),
            &crate::hive::reference_names(),
            &MetastoreNames::default(),
            true,
        )
        .unwrap_err();
        assert!(matches!(err, MigrateError::Exists(_)), "{err}");
        assert!(dir.path().join(".pilot.trinoSchema").exists(), "nothing changed");
    }

    fn fixture_copy() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let fixture =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431/warehouse");
        copy_dir(&fixture, dir.path());
        dir
    }

    /// Every file under `dir` with its bytes.
    fn snapshot(dir: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        let mut files = std::collections::BTreeMap::new();
        for (_, path, is_dir) in entries(dir).unwrap() {
            if is_dir {
                files.extend(snapshot(&path));
            } else {
                files.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
        files
    }

    /// Statistics as Trino 431 wrote them after `ANALYZE`: a double whose
    /// shortest form has 17 digits, exponents, an integer past 2^53.
    const STATISTICS: &str = r#""columnStatistics" : {
    "x" : {
      "doubleStatistics" : {
        "min" : 0.1,
        "max" : 1.23456789E7
      }
    },
    "b" : {
      "integerStatistics" : {
        "min" : 2,
        "max" : 9007199254740993
      }
    },
    "d" : {
      "doubleStatistics" : {
        "min" : 1.0E-7,
        "max" : 13591.293610249173
      }
    }
  }
}
"#;

    #[test]
    fn numbers_keep_every_digit_there_and_back() {
        let dir = fixture_copy();
        let file = dir.path().join("pilot/t1/.trinoSchema");
        let text = std::fs::read_to_string(&file).unwrap();
        let start = text.find("\"columnStatistics\"").unwrap();
        std::fs::write(&file, format!("{}{STATISTICS}", &text[..start])).unwrap();
        let (trino, codelake) = (crate::hive::reference_names(), MetastoreNames::default());
        let migrated = dir.path().join("pilot/t1/.codelakeSchema");
        let mut passes = Vec::new();
        for _ in 0..2 {
            migrate_names(dir.path(), &trino, &codelake, true).unwrap();
            passes.push(std::fs::read(&migrated).unwrap());
            migrate_names(dir.path(), &codelake, &trino, true).unwrap();
            passes.push(std::fs::read(&file).unwrap());
        }
        // Each value is the number Trino wrote (`13591.293610249173` was read
        // as `…171`).
        let json: Value = serde_json::from_slice(&passes[0]).unwrap();
        let statistics = &json["columnStatistics"];
        for (column, kind, bound, written) in [
            ("d", "doubleStatistics", "max", "13591.293610249173"),
            ("d", "doubleStatistics", "min", "1.0E-7"),
            ("x", "doubleStatistics", "min", "0.1"),
            ("x", "doubleStatistics", "max", "1.23456789E7"),
        ] {
            let value = statistics[column][kind][bound].as_f64().unwrap();
            assert_eq!(
                value.to_bits(),
                written.parse::<f64>().unwrap().to_bits(),
                "{column}.{bound}: {value}"
            );
        }
        assert_eq!(
            statistics["b"]["integerStatistics"]["max"].as_u64(),
            Some(9_007_199_254_740_993)
        );
        let migrated_text = String::from_utf8(passes[0].clone()).unwrap();
        assert!(migrated_text.contains("13591.293610249173"), "{migrated_text}");
        // There and back, twice: the same bytes under each name.
        assert_eq!(passes[0], passes[2], "new names");
        assert_eq!(passes[1], passes[3], "old names");
    }

    /// Production held `erp_raw_b1/.bkp_20260909_…__dbt_tmp`: a hidden copy
    /// of a table, with an empty metadata file, refused the whole plan.
    #[test]
    fn hidden_directories_in_a_schema_are_not_tables() {
        let dir = fixture_copy();
        let hidden = dir.path().join("pilot/.bkp_20260909__dbt_tmp");
        std::fs::create_dir_all(hidden.join(".trinoPermissions")).unwrap();
        std::fs::write(hidden.join(".trinoSchema"), b"").unwrap();
        let (old, new) = (crate::hive::reference_names(), MetastoreNames::default());
        let plan = migrate_names(dir.path(), &old, &new, true).unwrap();
        assert!(
            plan.iter().all(|step| {
                let (Step::Rewrite { from, .. } | Step::Rename { from, .. }) = step;
                !from.starts_with(&hidden)
            }),
            "{plan:?}"
        );
        assert_eq!(plan.len(), 5, "{plan:?}");
        assert!(hidden.join(".trinoSchema").is_file(), "left as it was");
        assert!(hidden.join(".trinoPermissions").is_dir(), "left as it was");
    }

    #[test]
    fn partitioned_tables_refuse_the_whole_plan() {
        let dir = fixture_copy();
        let file = dir.path().join("pilot/t1/.trinoSchema");
        let text = std::fs::read_to_string(&file).unwrap().replace(
            "\"partitionColumns\" : [ ]",
            r#""partitionColumns" : [ { "name" : "p", "type" : "string", "properties" : { } } ]"#,
        );
        std::fs::write(&file, text).unwrap();
        let before = snapshot(dir.path());
        let err = migrate_names(
            dir.path(),
            &crate::hive::reference_names(),
            &MetastoreNames::default(),
            true,
        )
        .unwrap_err();
        assert!(
            matches!(&err, MigrateError::Partitioned(path) if *path == file),
            "{err}"
        );
        assert!(
            err.to_string().contains("partition metadata is not migrated"),
            "{err}"
        );
        assert_eq!(snapshot(dir.path()), before, "nothing changed");
    }

    /// A write that fails halfway (here the third new checksum, whose name a
    /// directory takes) removes what the run wrote: the warehouse is left as
    /// it was, readable under the old names.
    #[test]
    fn a_write_that_fails_halfway_changes_nothing() {
        let dir = fixture_copy();
        std::fs::create_dir(dir.path().join("pilot/v1/..codelakeSchema.crc")).unwrap();
        let before = snapshot(dir.path());
        let err = migrate_names(
            dir.path(),
            &crate::hive::reference_names(),
            &MetastoreNames::default(),
            true,
        )
        .unwrap_err();
        assert!(matches!(err, MigrateError::Io(..)), "{err}");
        assert_eq!(snapshot(dir.path()), before, "nothing changed");
        assert!(dir.path().join("pilot/v1/..codelakeSchema.crc").is_dir());
    }

    #[test]
    fn writable_directories_keep_no_probe() {
        let dir = tempfile::tempdir().unwrap();
        check_writable(dir.path()).unwrap();
        assert!(snapshot(dir.path()).is_empty());
        let missing = dir.path().join("missing");
        let err = check_writable(&missing).unwrap_err();
        assert!(
            matches!(&err, MigrateError::NotWritable(path, _) if *path == missing),
            "{err}"
        );
        assert!(err.to_string().contains("cannot be written"), "{err}");
    }

    /// A directory nobody can write into, not even root (the tests run as
    /// root in the toolbelt): the plan is refused before any file changes.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_directory_that_cannot_be_written_is_refused() {
        let err = check_writable(Path::new("/proc")).unwrap_err();
        assert!(matches!(err, MigrateError::NotWritable(..)), "{err}");
    }

    fn copy_dir(from: &Path, to: &Path) {
        for entry in std::fs::read_dir(from).unwrap().flatten() {
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                std::fs::create_dir_all(&target).unwrap();
                copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
}
