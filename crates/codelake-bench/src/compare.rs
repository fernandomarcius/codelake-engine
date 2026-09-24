//! The eval: every case's `CodeLake` result against Trino's, then one
//! [`Baseline`] with both sides' metrics. Trino 431 is the reference: the goal
//! is a drop-in replacement, so "different from Trino" is a failure even when
//! both answers are defensible (e.g. decimal scale rules).

use crate::canonical::compare;
use crate::model::{
    Baseline, CaseEntry, CaseRun, Comparable, Correctness, EngineRun, Processes, RunStatus, SCHEMA_VERSION,
    Side, Verdict,
};

fn correctness(codelake: Option<&CaseRun>, trino: Option<&CaseRun>) -> Correctness {
    let verdict = |status, divergences| Correctness {
        status,
        divergences,
        notes: Vec::new(),
    };
    let (Some(cl), Some(tr)) = (codelake, trino) else {
        let missing = if codelake.is_none() { "codelake" } else { "trino" };
        return verdict(Verdict::Missing, vec![format!("case did not run on {missing}")]);
    };
    let mut errors = Vec::new();
    for (engine, run) in [("codelake", cl), ("trino", tr)] {
        if run.status == RunStatus::Error {
            errors.push(format!(
                "{engine} failed: {}",
                run.error.as_deref().unwrap_or("unknown error")
            ));
        }
    }
    if !errors.is_empty() {
        return verdict(Verdict::Error, errors);
    }
    match (&cl.result, &tr.result) {
        (Some(c), Some(t)) => {
            let cmp = compare(t, c);
            Correctness {
                status: if cmp.equal { Verdict::Ok } else { Verdict::Fail },
                divergences: cmp.divergences,
                notes: cmp.notes,
            }
        }
        _ => verdict(Verdict::Error, vec!["no result captured".into()]),
    }
}

fn setting(env: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    env.get(key)
        .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_owned))
        .unwrap_or_default()
}

/// Merges one Trino run and the `CodeLake` runs (one per driver phase).
pub(crate) fn merge(
    trino: &EngineRun,
    codelake: &[EngineRun],
    env: serde_json::Map<String, serde_json::Value>,
    commit: &str,
    date: &str,
) -> Baseline {
    let find_cl = |id: &str| codelake.iter().flat_map(|r| &r.cases).find(|c| c.id == id);
    let mut ids: Vec<(&str, &str)> = trino
        .cases
        .iter()
        .map(|c| (c.id.as_str(), c.workload.as_str()))
        .collect();
    for case in codelake.iter().flat_map(|r| &r.cases) {
        if !ids.iter().any(|(id, _)| *id == case.id) {
            ids.push((case.id.as_str(), case.workload.as_str()));
        }
    }
    let cases = ids
        .into_iter()
        .map(|(id, workload)| {
            let cl = find_cl(id);
            let tr = trino.cases.iter().find(|c| c.id == id);
            CaseEntry {
                id: id.to_owned(),
                workload: workload.to_owned(),
                correctness: correctness(cl, tr),
                codelake: cl.map(Side::from),
                trino: tr.map(Side::from),
            }
        })
        .collect();
    let codelake_process = codelake
        .iter()
        .find(|r| r.process.ready_ms.is_some())
        .or_else(|| codelake.first())
        .map(|r| r.process.clone())
        .unwrap_or_default();
    Baseline {
        schema_version: SCHEMA_VERSION,
        commit: commit.to_owned(),
        date: date.to_owned(),
        comparable: Comparable {
            scale: trino.scale.clone(),
            cpus: setting(&env, "cpus"),
            memory_mib: setting(&env, "memory_mib"),
            reps: trino.reps,
            warmup: trino.warmup,
        },
        env,
        process: Processes {
            codelake: codelake_process,
            trino: trino.process.clone(),
        },
        cases,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::canonical::{CanonicalResult, Cell, Class, Column};
    use crate::model::ProcessMetrics;

    pub(crate) fn case(id: &str, value: &str, p50: u64) -> CaseRun {
        CaseRun {
            id: id.into(),
            workload: "tpch".into(),
            driver: "test".into(),
            status: RunStatus::Ok,
            error: None,
            warmup: 1,
            latencies_us: vec![p50],
            p50_us: Some(p50),
            p95_us: Some(p50),
            cpu_usec_p50: Some(p50 * 2),
            peak_anon_bytes: Some(64 << 20),
            spill_peak_bytes: None,
            result: Some(CanonicalResult::new(
                vec![Column {
                    name: "v".into(),
                    engine_type: "decimal(15,2)".into(),
                    class: Class::Number,
                }],
                vec![vec![Cell::from_text(Class::Number, value)]],
            )),
        }
    }

    pub(crate) fn engine_run(engine: &str, cases: Vec<CaseRun>) -> EngineRun {
        EngineRun {
            schema_version: SCHEMA_VERSION,
            engine: engine.into(),
            driver: "test".into(),
            engine_version: None,
            scale: "1".into(),
            reps: 5,
            warmup: 1,
            process: ProcessMetrics {
                ready_ms: Some(1500),
                idle_anon_bytes: Some(100 << 20),
                container_peak_bytes: None,
            },
            cases,
        }
    }

    pub(crate) fn baseline() -> Baseline {
        let trino = engine_run(
            "trino",
            vec![
                case("q1", "1.5", 2000),
                case("q2", "25.51", 3000),
                case("only_trino", "1", 10),
            ],
        );
        let mut broken = case("q3", "1", 10);
        broken.status = RunStatus::Error;
        broken.error = Some("boom".into());
        let codelake = engine_run(
            "codelake",
            vec![case("q1", "1.50", 1000), case("q2", "25.507251", 1500), broken],
        );
        let mut env = serde_json::Map::new();
        env.insert("cpus".into(), "4".into());
        env.insert("memory_mib".into(), "4096".into());
        merge(&trino, &[codelake], env, "abc1234", "2026-09-24")
    }

    #[test]
    fn eval_classifies_each_case() {
        let b = baseline();
        let status = |id: &str| b.cases.iter().find(|c| c.id == id).unwrap().correctness.status;
        assert_eq!(status("q1"), Verdict::Ok);
        assert_eq!(status("q2"), Verdict::Fail);
        assert_eq!(status("q3"), Verdict::Missing);
        assert_eq!(status("only_trino"), Verdict::Missing);
        assert_eq!(b.comparable.cpus, "4");
        assert_eq!(b.comparable.memory_mib, "4096");
        assert_eq!(b.process.codelake.ready_ms, Some(1500));
        let q2 = b.cases.iter().find(|c| c.id == "q2").unwrap();
        assert!(q2.correctness.divergences[0].contains("25.507251"), "{q2:?}");
    }

    #[test]
    fn an_engine_error_is_reported_as_error() {
        let mut failed = case("q1", "1", 10);
        failed.status = RunStatus::Error;
        failed.error = Some("resources exhausted".into());
        let c = correctness(Some(&failed), Some(&case("q1", "1", 10)));
        assert_eq!(c.status, Verdict::Error);
        assert!(c.divergences[0].contains("resources exhausted"));
    }
}
