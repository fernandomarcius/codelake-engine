//! Repository automation that must behave identically on a laptop and in CI.
//!
//! ```text
//! cargo xtask arch                         # architecture rules (dependency direction)
//! cargo xtask bench-gate <criterion-dir> <base> <head>   # criterion regression gate
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::process::ExitCode;

use cargo_metadata::{DependencyKind, MetadataCommand};

/// Allowed internal dependencies. Anything not listed is a violation: the
/// engine core must never learn about concrete connectors or the server.
const ALLOWED: &[(&str, &[&str])] = &[
    ("codelake-core", &[]),
    ("codelake-connectors", &["codelake-core"]),
    ("codelake-server", &["codelake-core", "codelake-connectors"]),
    ("codelake-bench", &["codelake-core", "codelake-connectors"]),
    ("xtask", &[]),
];

/// Crates that must never appear in the core's normal dependency closure.
const FORBIDDEN_IN_CORE: &[&str] = &[
    "tokio-postgres",
    "mysql_async",
    "mongodb",
    "tiberius",
    "iceberg",
    "axum",
    "pgwire",
];

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("arch") => arch(),
        Some("bench-gate") if args.len() == 4 => bench_gate(Path::new(&args[1]), &args[2], &args[3]),
        _ => Err(
            "usage: cargo xtask arch | cargo xtask bench-gate <criterion-dir> <base-name> <head-name>".into(),
        ),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn arch() -> Result<(), String> {
    let metadata = MetadataCommand::new()
        .exec()
        .map_err(|e| format!("cargo metadata failed: {e}"))?;
    let members: Vec<_> = metadata.workspace_packages();
    let mut violations = Vec::new();

    for package in &members {
        let name = package.name.as_str();
        let Some((_, allowed)) = ALLOWED.iter().find(|(crate_name, _)| *crate_name == name) else {
            violations.push(format!(
                "{name}: new workspace crate without an entry in xtask ALLOWED"
            ));
            continue;
        };
        for dep in package
            .dependencies
            .iter()
            .filter(|d| d.kind == DependencyKind::Normal && d.path.is_some())
        {
            if !allowed.contains(&dep.name.as_str()) {
                violations.push(format!("{name} must not depend on {}", dep.name));
            }
        }
    }

    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or("cargo metadata returned no resolve graph")?;
    let core = members
        .iter()
        .find(|p| p.name.as_str() == "codelake-core")
        .ok_or("codelake-core not found")?;
    let nodes: BTreeMap<_, _> = resolve.nodes.iter().map(|n| (&n.id, n)).collect();
    let mut stack = vec![&core.id];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some(node) = nodes.get(id) else { continue };
        for dep in &node.deps {
            if dep.dep_kinds.iter().any(|k| k.kind == DependencyKind::Normal) {
                let dep_name = metadata[&dep.pkg].name.as_str();
                if FORBIDDEN_IN_CORE.contains(&dep_name) {
                    violations.push(format!("codelake-core pulls in forbidden crate {dep_name}"));
                }
                stack.push(&dep.pkg);
            }
        }
    }

    if violations.is_empty() {
        println!("architecture rules: OK ({} crates checked)", members.len());
        Ok(())
    } else {
        Err(format!(
            "architecture violations:\n  - {}",
            violations.join("\n  - ")
        ))
    }
}

/// Percent change that fails the build / only warns (see docs/gates.md).
const BLOCK_PCT: f64 = 25.0;
const WARN_PCT: f64 = 10.0;

/// Mean time per benchmark for one named criterion baseline
/// (`<criterion-dir>/<group>/<bench>/<baseline>/estimates.json`).
fn read_estimates(dir: &Path, baseline: &str) -> Result<BTreeMap<String, f64>, String> {
    let mut out = BTreeMap::new();
    if !dir.exists() {
        return Ok(out);
    }
    let files = walk(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    for path in files {
        let Some(parent) = path.parent() else { continue };
        if path.file_name().is_none_or(|f| f != "estimates.json")
            || parent.file_name().is_none_or(|d| d != baseline)
        {
            continue;
        }
        let raw = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let json: serde_json::Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
        let mean = json["mean"]["point_estimate"]
            .as_f64()
            .ok_or_else(|| format!("{}: missing mean.point_estimate", path.display()))?;
        let bench = parent
            .parent()
            .and_then(|p| p.strip_prefix(dir).ok())
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        out.insert(bench, mean);
    }
    Ok(out)
}

fn walk(dir: &Path) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            files.extend(walk(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
}

/// Classification of a relative slowdown (positive = slower), see docs/gates.md.
fn verdict(pct: f64) -> &'static str {
    if pct > BLOCK_PCT {
        "BLOCK"
    } else if pct > WARN_PCT {
        "WARNING"
    } else {
        "PASS"
    }
}

fn bench_gate(dir: &Path, base_name: &str, head_name: &str) -> Result<(), String> {
    let base = read_estimates(dir, base_name)?;
    let new = read_estimates(dir, head_name)?;
    if new.is_empty() {
        return Err(format!(
            "no `{head_name}` criterion results under {}",
            dir.display()
        ));
    }
    let mut blocked = Vec::new();
    for (bench, new_mean) in &new {
        let Some(base_mean) = base.get(bench) else {
            println!("NEW      {bench}: no baseline");
            continue;
        };
        let pct = (new_mean - base_mean) / base_mean * 100.0;
        let verdict = verdict(pct);
        if verdict == "BLOCK" {
            blocked.push(bench.clone());
        }
        println!("{verdict:<8} {bench}: {pct:+.1}% ({base_mean:.0}ns -> {new_mean:.0}ns)");
    }
    if blocked.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "performance regression above {BLOCK_PCT}% in: {}",
            blocked.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_thresholds_match_the_policy() {
        assert_eq!(verdict(-40.0), "PASS");
        assert_eq!(verdict(10.0), "PASS");
        assert_eq!(verdict(10.1), "WARNING");
        assert_eq!(verdict(25.0), "WARNING");
        assert_eq!(verdict(25.1), "BLOCK");
    }

    fn write(dir: &Path, rel: &str, mean: f64) {
        let path = dir.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, format!("{{\"mean\":{{\"point_estimate\":{mean}}}}}")).unwrap();
    }

    #[test]
    fn gate_blocks_regressions_and_tolerates_new_benchmarks() {
        let dir = std::env::temp_dir().join(format!("xtask-bench-{}", std::process::id()));
        write(&dir, "g/a/base/estimates.json", 100.0);
        write(&dir, "g/a/head/estimates.json", 105.0);
        write(&dir, "g/new/head/estimates.json", 1.0);
        assert!(bench_gate(&dir, "base", "head").is_ok());

        write(&dir, "g/a/head/estimates.json", 140.0);
        let err = bench_gate(&dir, "base", "head").unwrap_err();
        assert!(err.contains("g/a"), "{err}");

        assert!(bench_gate(&dir, "base", "missing").is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
