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
//! The whole plan is computed first; it is refused, with nothing changed,
//! when a new name already exists or a metadata file does not parse. Without
//! `apply` only the plan is returned.

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
        for (_, table_dir, table_is_dir) in entries(&path)? {
            if !table_is_dir {
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
            rewrite_markers(&mut json, old, new);
            let bytes = serde_json::to_vec_pretty(&json).map_err(|e| MigrateError::Json(from.clone(), e))?;
            rewritten.push(bytes);
        }
    }
    if !apply {
        return Ok(steps);
    }
    let mut contents = rewritten.into_iter();
    for step in &steps {
        match step {
            Step::Rewrite { from, to } => {
                let bytes = contents.next().unwrap_or_default();
                // New file and checksum first: an interrupted run leaves the
                // old metadata readable under the old names.
                write_with_crc(to, &bytes)?;
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
