//! File formats: one [`EngineRun`] per engine phase (written by `run`), and
//! one [`Baseline`] per commit (written by `compare`, versioned under
//! `bench/baselines/`, read by `cargo xtask bench-gate --e2e`).

use serde::{Deserialize, Serialize};

use crate::canonical::CanonicalResult;

pub(crate) const SCHEMA_VERSION: u32 = 1;

/// Process-level metrics of one engine container.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProcessMetrics {
    /// Container start → first successful read of a table (the readiness probe).
    pub(crate) ready_ms: Option<u64>,
    /// Anonymous memory a few seconds after ready, before any workload.
    pub(crate) idle_anon_bytes: Option<u64>,
    /// cgroup `memory.peak` at the end of the phase (includes page cache).
    pub(crate) container_peak_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunStatus {
    Ok,
    Error,
}

/// Measurements of one case on one engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CaseRun {
    pub(crate) id: String,
    pub(crate) workload: String,
    pub(crate) driver: String,
    pub(crate) status: RunStatus,
    pub(crate) error: Option<String>,
    pub(crate) warmup: usize,
    /// Wall time of every measured repetition, microseconds.
    pub(crate) latencies_us: Vec<u64>,
    pub(crate) p50_us: Option<u64>,
    pub(crate) p95_us: Option<u64>,
    /// Median container CPU time per repetition, microseconds.
    pub(crate) cpu_usec_p50: Option<u64>,
    /// Highest sampled anonymous memory over the measured repetitions.
    pub(crate) peak_anon_bytes: Option<u64>,
    pub(crate) spill_peak_bytes: Option<u64>,
    /// Result compared by the eval (the `verify` statement's, or the first
    /// measured repetition's).
    pub(crate) result: Option<CanonicalResult>,
}

/// Everything one engine phase measured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct EngineRun {
    pub(crate) schema_version: u32,
    pub(crate) engine: String,
    pub(crate) driver: String,
    pub(crate) engine_version: Option<String>,
    pub(crate) scale: String,
    pub(crate) reps: usize,
    pub(crate) warmup: usize,
    pub(crate) process: ProcessMetrics,
    pub(crate) cases: Vec<CaseRun>,
}

/// Parameters that must match for two baselines to be compared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Comparable {
    pub(crate) scale: String,
    pub(crate) cpus: String,
    pub(crate) memory_mib: String,
    pub(crate) reps: usize,
    pub(crate) warmup: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum Verdict {
    /// Same rows as Trino.
    Ok,
    /// Different rows: a wrong result (or a Trino incompatibility).
    Fail,
    /// The statement failed on one or both engines.
    Error,
    /// The case did not run on one of the engines.
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Correctness {
    pub(crate) status: Verdict,
    pub(crate) divergences: Vec<String>,
    pub(crate) notes: Vec<String>,
}

/// Metrics of one side of a case, without the rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Side {
    pub(crate) driver: String,
    pub(crate) reps: usize,
    pub(crate) p50_us: Option<u64>,
    pub(crate) p95_us: Option<u64>,
    pub(crate) cpu_usec_p50: Option<u64>,
    pub(crate) peak_anon_bytes: Option<u64>,
    pub(crate) spill_peak_bytes: Option<u64>,
    pub(crate) error: Option<String>,
}

impl From<&CaseRun> for Side {
    fn from(run: &CaseRun) -> Self {
        Self {
            driver: run.driver.clone(),
            reps: run.latencies_us.len(),
            p50_us: run.p50_us,
            p95_us: run.p95_us,
            cpu_usec_p50: run.cpu_usec_p50,
            peak_anon_bytes: run.peak_anon_bytes,
            spill_peak_bytes: run.spill_peak_bytes,
            error: run.error.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CaseEntry {
    pub(crate) id: String,
    pub(crate) workload: String,
    pub(crate) correctness: Correctness,
    pub(crate) codelake: Option<Side>,
    pub(crate) trino: Option<Side>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Processes {
    pub(crate) codelake: ProcessMetrics,
    pub(crate) trino: ProcessMetrics,
}

/// One measured run of both engines at one commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Baseline {
    pub(crate) schema_version: u32,
    pub(crate) commit: String,
    pub(crate) date: String,
    pub(crate) comparable: Comparable,
    /// Free-form facts about the machine and versions (from `run.sh`).
    pub(crate) env: serde_json::Map<String, serde_json::Value>,
    pub(crate) process: Processes,
    pub(crate) cases: Vec<CaseEntry>,
}

/// Nearest-rank percentile (`p` in 1..=100) of unsorted samples.
pub(crate) fn percentile(samples: &[u64], p: usize) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (p * sorted.len()).div_ceil(100).clamp(1, sorted.len());
    sorted.get(rank - 1).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles() {
        assert_eq!(percentile(&[], 50), None);
        assert_eq!(percentile(&[7], 95), Some(7));
        assert_eq!(percentile(&[5, 1, 4, 2, 3], 50), Some(3));
        assert_eq!(percentile(&[5, 1, 4, 2, 3], 95), Some(5));
        let hundred: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&hundred, 50), Some(50));
        assert_eq!(percentile(&hundred, 95), Some(95));
        assert_eq!(percentile(&[1, 2], 50), Some(1));
    }
}
