//! Markdown report of one [`Baseline`] (`bench/reports/<date>-<commit>.md`).
//!
//! Timing and memory of a case are shown only when its results match Trino's:
//! a fast wrong answer is not a result.

use std::fmt::Write as _;

use crate::canonical::{FLOAT_ABS_TOL, FLOAT_REL_TOL};
use crate::model::{Baseline, CaseEntry, Side, Verdict};
use crate::probe::SAMPLE_EVERY;

/// Exact for the magnitudes involved (µs below ~71 min, KiB below 4 TiB).
fn as_f64(value: u64) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn ms(us: Option<u64>) -> String {
    us.map_or_else(|| "—".into(), |us| format!("{}.{}", us / 1000, (us % 1000) / 100))
}

fn mib(bytes: Option<u64>) -> String {
    bytes.map_or_else(
        || "—".into(),
        |b| {
            let tenths = b * 10 / (1024 * 1024);
            format!("{}.{}", tenths / 10, tenths % 10)
        },
    )
}

/// `codelake / trino`, below 1 means `CodeLake` is lower (faster, smaller).
fn ratio(codelake: Option<u64>, trino: Option<u64>) -> Option<f64> {
    match (codelake, trino) {
        (Some(c), Some(t)) if t > 0 => Some(as_f64(c) / as_f64(t)),
        _ => None,
    }
}

fn ratio_text(value: Option<f64>) -> String {
    value.map_or_else(|| "—".into(), |r| format!("{r:.2}"))
}

fn kib(bytes: Option<u64>) -> Option<u64> {
    bytes.map(|b| b / 1024)
}

fn geomean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let count = f64::from(u32::try_from(values.len()).unwrap_or(u32::MAX));
    Some((values.iter().map(|v| v.ln()).sum::<f64>() / count).exp())
}

fn status_text(status: Verdict) -> &'static str {
    match status {
        Verdict::Ok => "OK",
        Verdict::Fail => "**FAIL**",
        Verdict::Error => "**ERROR**",
        Verdict::Missing => "MISSING",
    }
}

fn env_text(baseline: &Baseline, key: &str) -> String {
    baseline.env.get(key).map_or_else(
        || "—".into(),
        |v| v.as_str().map_or_else(|| v.to_string(), str::to_owned),
    )
}

fn row(out: &mut String, entry: &CaseEntry) {
    let cl = entry.codelake.as_ref();
    let tr = entry.trino.as_ref();
    let driver = cl.map_or("—", |s| s.driver.as_str());
    let reps = tr.map_or(0, |s| s.reps);
    if entry.correctness.status == Verdict::Ok {
        let get = |s: Option<&Side>, f: fn(&Side) -> Option<u64>| s.and_then(f);
        let _ = writeln!(
            out,
            "| {} | {} | {driver} | OK | {reps} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            entry.id,
            entry.workload,
            ms(get(cl, |s| s.p50_us)),
            ms(get(tr, |s| s.p50_us)),
            ratio_text(ratio(get(cl, |s| s.p50_us), get(tr, |s| s.p50_us))),
            ms(get(cl, |s| s.p95_us)),
            ms(get(tr, |s| s.p95_us)),
            mib(get(cl, |s| s.peak_anon_bytes)),
            mib(get(tr, |s| s.peak_anon_bytes)),
            ratio_text(ratio(
                kib(get(cl, |s| s.peak_anon_bytes)),
                kib(get(tr, |s| s.peak_anon_bytes))
            )),
            ms(get(cl, |s| s.cpu_usec_p50)),
            ms(get(tr, |s| s.cpu_usec_p50)),
            mib(get(cl, |s| s.spill_peak_bytes)),
        );
    } else {
        let _ = writeln!(
            out,
            "| {} | {} | {driver} | {} | {reps} | — | — | — | — | — | — | — | — | — | — | — |",
            entry.id,
            entry.workload,
            status_text(entry.correctness.status),
        );
    }
}

pub(crate) fn render(baseline: &Baseline) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# CodeLake × Trino 431 — {} — `{}`\n",
        baseline.date, baseline.commit
    );
    let _ = writeln!(
        out,
        "All numbers below were **measured** by `scripts/bench/run.sh` on the machine described in \
         *Environment*, with the machine reserved for the run (no concurrent builds). Nothing is \
         estimated or extrapolated. How to read this report: `docs/benchmarks.md`.\n"
    );
    correctness_section(&mut out, baseline);
    summary_section(&mut out, baseline);
    process_section(&mut out, baseline);
    cases_section(&mut out, baseline);
    env_section(&mut out, baseline);
    out
}

fn correctness_section(out: &mut String, b: &Baseline) {
    let count = |v: Verdict| b.cases.iter().filter(|c| c.correctness.status == v).count();
    let _ = writeln!(out, "## Correctness (eval against Trino 431)\n");
    let _ = writeln!(
        out,
        "| OK | FAIL | ERROR | MISSING | total |\n|---|---|---|---|---|\n| {} | {} | {} | {} | {} |\n",
        count(Verdict::Ok),
        count(Verdict::Fail),
        count(Verdict::Error),
        count(Verdict::Missing),
        b.cases.len()
    );
    let _ = writeln!(
        out,
        "Rows are compared as multisets in a canonical order; decimals, integers, text and dates must \
         match exactly; only `double`/`real` values get a tolerance (relative {FLOAT_REL_TOL:e}, \
         absolute {FLOAT_ABS_TOL:e}). A case that is not **OK** shows no timing.\n"
    );
    let failing: Vec<&CaseEntry> = b
        .cases
        .iter()
        .filter(|c| c.correctness.status != Verdict::Ok)
        .collect();
    if !failing.is_empty() {
        let _ = writeln!(out, "### Divergences\n");
        for entry in failing {
            let _ = writeln!(
                out,
                "- **{}** ({})",
                entry.id,
                status_text(entry.correctness.status)
            );
            for d in &entry.correctness.divergences {
                let _ = writeln!(out, "  - {}", d.replace('|', "\\|"));
            }
        }
        out.push('\n');
    }
    let noted: Vec<&CaseEntry> = b
        .cases
        .iter()
        .filter(|c| !c.correctness.notes.is_empty())
        .collect();
    if !noted.is_empty() {
        let _ = writeln!(out, "### Notes (values equal, presentation differs)\n");
        for entry in noted {
            for n in &entry.correctness.notes {
                let _ = writeln!(out, "- **{}**: {}", entry.id, n.replace('|', "\\|"));
            }
        }
        out.push('\n');
    }
}

fn summary_section(out: &mut String, b: &Baseline) {
    let _ = writeln!(out, "## Summary\n");
    let mut by_workload: Vec<&str> = Vec::new();
    for case in &b.cases {
        if !by_workload.contains(&case.workload.as_str()) {
            by_workload.push(case.workload.as_str());
        }
    }
    let _ = writeln!(
        out,
        "| workload | cases OK | geomean p50 ratio | geomean peak-memory ratio |\n|---|---|---|---|"
    );
    for workload in by_workload {
        let ok: Vec<&CaseEntry> = b
            .cases
            .iter()
            .filter(|c| c.workload == workload && c.correctness.status == Verdict::Ok)
            .collect();
        let total = b.cases.iter().filter(|c| c.workload == workload).count();
        let latency: Vec<f64> = ok
            .iter()
            .filter_map(|c| {
                ratio(
                    c.codelake.as_ref().and_then(|s| s.p50_us),
                    c.trino.as_ref().and_then(|s| s.p50_us),
                )
            })
            .collect();
        let memory: Vec<f64> = ok
            .iter()
            .filter_map(|c| {
                ratio(
                    kib(c.codelake.as_ref().and_then(|s| s.peak_anon_bytes)),
                    kib(c.trino.as_ref().and_then(|s| s.peak_anon_bytes)),
                )
            })
            .collect();
        let _ = writeln!(
            out,
            "| {workload} | {}/{total} | {} | {} |",
            ok.len(),
            ratio_text(geomean(&latency)),
            ratio_text(geomean(&memory)),
        );
    }
    let _ = writeln!(
        out,
        "\nRatios are CodeLake / Trino over the cases that are OK: **below 1.00 CodeLake is faster \
         or smaller**.\n"
    );
}

fn process_section(out: &mut String, b: &Baseline) {
    let _ = writeln!(out, "## Process\n");
    let (cl, tr) = (&b.process.codelake, &b.process.trino);
    let _ = writeln!(
        out,
        "| metric | CodeLake | Trino | ratio |\n|---|---|---|---|\n\
         | time to ready (container start → first read of `region`), ms | {} | {} | {} |\n\
         | idle anonymous memory (≈ RSS), MiB | {} | {} | {} |\n\
         | container `memory.peak` over the phase (incl. page cache), MiB | {} | {} | {} |\n",
        cl.ready_ms.map_or_else(|| "—".into(), |v| v.to_string()),
        tr.ready_ms.map_or_else(|| "—".into(), |v| v.to_string()),
        ratio_text(ratio(cl.ready_ms, tr.ready_ms)),
        mib(cl.idle_anon_bytes),
        mib(tr.idle_anon_bytes),
        ratio_text(ratio(kib(cl.idle_anon_bytes), kib(tr.idle_anon_bytes))),
        mib(cl.container_peak_bytes),
        mib(tr.container_peak_bytes),
        ratio_text(ratio(kib(cl.container_peak_bytes), kib(tr.container_peak_bytes))),
    );
}

fn cases_section(out: &mut String, b: &Baseline) {
    let _ = writeln!(out, "## Per case\n");
    let _ = writeln!(
        out,
        "Latency in ms (client-side wall time until the last row), memory in MiB (peak anonymous \
         memory of the engine container, sampled every {} ms), CPU in ms (container CPU time per \
         repetition, median). `driver` is how CodeLake was reached: `pg-wire` (network, like Trino) \
         or `embedded` (engine inside the bench process — **not a like-for-like comparison**, used \
         for writes until a network protocol routes DDL to the Hive catalog).\n",
        SAMPLE_EVERY.as_millis()
    );
    let _ = writeln!(
        out,
        "| case | workload | CL driver | correct | reps | p50 CL | p50 Trino | p50 ratio | p95 CL | p95 Trino | \
         peak mem CL | peak mem Trino | mem ratio | CPU CL | CPU Trino | spill CL |\n\
         |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
    );
    for entry in &b.cases {
        row(out, entry);
    }
    out.push('\n');
}

fn env_section(out: &mut String, b: &Baseline) {
    let _ = writeln!(out, "## Environment\n\n| key | value |\n|---|---|");
    let _ = writeln!(out, "| commit | `{}` |", b.commit);
    let scale = &b.comparable.scale;
    let scale = if scale.parse::<f64>().is_ok() {
        format!("SF{scale}")
    } else {
        scale.clone()
    };
    let _ = writeln!(out, "| scale | TPC-H {scale} |");
    let _ = writeln!(
        out,
        "| limits per engine container | `--cpus={}` `--memory={}m` (swap off) |",
        b.comparable.cpus, b.comparable.memory_mib
    );
    let _ = writeln!(
        out,
        "| repetitions | warmup {} + {} measured (per-case overrides in the workload files); p95 is nearest-rank |",
        b.comparable.warmup, b.comparable.reps
    );
    for key in b.env.keys() {
        if key != "cpus" && key != "memory_mib" {
            let _ = writeln!(out, "| {key} | {} |", env_text(b, key).replace('|', "\\|"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_hides_timing_of_failed_cases() {
        let baseline = crate::compare::tests::baseline();
        let text = render(&baseline);
        assert!(
            text.contains("| q1 | tpch | test | OK | 1 | 1.0 | 2.0 | 0.50 |"),
            "{text}"
        );
        assert!(text.contains("| q2 | tpch | test | **FAIL** | 1 | — |"), "{text}");
        assert!(text.contains("25.507251"), "{text}");
        assert!(text.contains("| tpch | 1/4 | 0.50 | 1.00 |"), "{text}");
        assert!(text.contains(
            "| time to ready (container start → first read of `region`), ms | 1500 | 1500 | 1.00 |"
        ));
        assert!(text.contains("`--cpus=4` `--memory=4096m`"), "{text}");
    }

    #[test]
    fn units_and_ratios() {
        assert_eq!(ms(Some(1_234_567)), "1234.5");
        assert_eq!(ms(None), "—");
        assert_eq!(mib(Some(3 * 1024 * 1024 / 2)), "1.5");
        assert_eq!(ratio(Some(1), Some(0)), None);
        let g = geomean(&[0.5, 2.0]).unwrap();
        assert!((g - 1.0).abs() < 1e-12);
        assert_eq!(geomean(&[]), None);
    }
}
