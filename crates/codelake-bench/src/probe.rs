//! Resource probes read from the engine container's cgroup (v2), so both
//! engines are measured the same way, from outside the process:
//!
//! - memory: `anon` of `memory.stat` (anonymous memory, i.e. RSS without the
//!   page cache of the Parquet files) sampled every [`SAMPLE_EVERY`];
//! - CPU: `usage_usec` of `cpu.stat` before and after a statement;
//! - spill: `CodeLake`'s spill gauge (its `/metrics`, or the engine itself when
//!   embedded). Trino's default configuration has spill disabled.

use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use codelake_core::Engine;

/// Memory sampling period. Short spikes between samples are missed; the
/// same period applies to both engines.
pub(crate) const SAMPLE_EVERY: Duration = Duration::from_millis(20);
/// Spill is polled less often: over HTTP it costs a request.
const SPILL_EVERY_N_SAMPLES: u32 = 5;

/// cgroup v2 directories of the engine: one container, or every pod of a
/// distributed engine (coordinator and workers), read as one sum.
#[derive(Debug, Clone)]
pub(crate) struct Cgroup {
    dirs: Vec<PathBuf>,
}

fn stat_field(text: &str, field: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(' ')?;
        (key == field).then(|| value.trim().parse().ok()).flatten()
    })
}

fn read(dir: &Path, file: &str) -> Option<String> {
    std::fs::read_to_string(dir.join(file)).ok()
}

impl Cgroup {
    pub(crate) fn at(dir: PathBuf) -> Self {
        Self { dirs: vec![dir] }
    }

    /// Several cgroups summed (`--cgroup-dir`, repeated).
    pub(crate) fn sum(dirs: Vec<PathBuf>) -> Self {
        Self { dirs }
    }

    /// Finds the cgroup of container `id` under a mounted cgroup root, for the
    /// cgroupfs (`docker/<id>`) and systemd (`system.slice/docker-<id>.scope`)
    /// drivers.
    pub(crate) fn find(root: &Path, id: &str) -> Option<Self> {
        [
            root.join("docker").join(id),
            root.join("system.slice").join(format!("docker-{id}.scope")),
        ]
        .into_iter()
        .find(|dir| dir.join("memory.stat").exists())
        .map(Self::at)
    }

    /// The sum of `value` over every directory; `None` when one is missing.
    fn total(&self, value: impl Fn(&Path) -> Option<u64>) -> Option<u64> {
        self.dirs.iter().map(|dir| value(dir)).sum()
    }

    /// Anonymous memory of the container, bytes.
    pub(crate) fn anon_bytes(&self) -> Option<u64> {
        self.total(|dir| stat_field(&read(dir, "memory.stat")?, "anon"))
    }

    /// Container-lifetime high-water mark including page cache, bytes.
    pub(crate) fn peak_bytes(&self) -> Option<u64> {
        self.total(|dir| read(dir, "memory.peak")?.trim().parse().ok())
    }

    /// Total CPU time consumed by the container, microseconds.
    pub(crate) fn cpu_usec(&self) -> Option<u64> {
        self.total(|dir| stat_field(&read(dir, "cpu.stat")?, "usage_usec"))
    }
}

/// Where spill usage comes from.
#[derive(Debug, Clone)]
pub(crate) enum SpillProbe {
    None,
    /// `CodeLake`'s `/metrics` (`host:port`), gauge `codelake_spill_used_bytes`.
    Metrics(String),
    Engine(Arc<Engine>),
}

fn http_get(addr: &str, path: &str) -> Option<String> {
    let mut stream = std::net::TcpStream::connect(addr).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
    write!(stream, "GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

fn metrics_gauge(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let value = line.strip_prefix(name)?.strip_prefix(' ')?;
        // Prometheus renders gauges as floats; spill sizes are whole bytes.
        value.trim().split('.').next()?.parse().ok()
    })
}

impl SpillProbe {
    pub(crate) fn used_bytes(&self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Metrics(addr) => metrics_gauge(&http_get(addr, "/metrics")?, "codelake_spill_used_bytes"),
            Self::Engine(engine) => Some(engine.memory().spill_used_bytes),
        }
    }
}

/// Peaks observed while one statement ran.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Peaks {
    pub(crate) anon_bytes: Option<u64>,
    pub(crate) spill_bytes: Option<u64>,
}

/// Background sampler on its own OS thread: an engine saturating the async
/// runtime (embedded mode) must not delay the samples.
#[derive(Debug)]
pub(crate) struct Sampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<Peaks>,
}

fn max_opt(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, None) => x,
        (None, y) => y,
    }
}

impl Sampler {
    pub(crate) fn start(cgroup: Option<Cgroup>, spill: SpillProbe) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut peaks = Peaks::default();
            let mut tick = 0_u32;
            loop {
                let anon = cgroup.as_ref().and_then(Cgroup::anon_bytes);
                peaks.anon_bytes = max_opt(peaks.anon_bytes, anon);
                if tick.is_multiple_of(SPILL_EVERY_N_SAMPLES) {
                    peaks.spill_bytes = max_opt(peaks.spill_bytes, spill.used_bytes());
                }
                tick = tick.wrapping_add(1);
                if flag.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(SAMPLE_EVERY);
            }
            peaks
        });
        Self { stop, handle }
    }

    /// Stops sampling (after one last sample) and returns the peaks.
    pub(crate) fn stop(self) -> Peaks {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cgroup_files_are_parsed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let id = "abc123";
        let cg = root.join("docker").join(id);
        std::fs::create_dir_all(&cg).unwrap();
        std::fs::write(cg.join("memory.stat"), "anon 4096\nfile 8192\nanon_thp 0\n").unwrap();
        std::fs::write(cg.join("memory.peak"), "123456\n").unwrap();
        std::fs::write(cg.join("cpu.stat"), "usage_usec 777\nuser_usec 700\n").unwrap();

        assert!(Cgroup::find(root, "missing").is_none());
        let cgroup = Cgroup::find(root, id).unwrap();
        assert_eq!(cgroup.anon_bytes(), Some(4096));
        assert_eq!(cgroup.peak_bytes(), Some(123_456));
        assert_eq!(cgroup.cpu_usec(), Some(777));

        let sampler = Sampler::start(Some(cgroup), SpillProbe::None);
        std::fs::write(cg.join("memory.stat"), "anon 9000\n").unwrap();
        std::thread::sleep(SAMPLE_EVERY * 10);
        std::fs::write(cg.join("memory.stat"), "anon 10\n").unwrap();
        let peaks = sampler.stop();
        assert_eq!(peaks.anon_bytes, Some(9000));
        assert_eq!(peaks.spill_bytes, None);
    }

    #[test]
    fn prometheus_gauges_are_read() {
        let text = "# TYPE codelake_spill_used_bytes gauge\ncodelake_spill_used_bytes 1048576\ncodelake_spill_used_bytes_total 5\n";
        assert_eq!(metrics_gauge(text, "codelake_spill_used_bytes"), Some(1_048_576));
        assert_eq!(
            metrics_gauge("codelake_spill_used_bytes 12.0\n", "codelake_spill_used_bytes"),
            Some(12)
        );
        assert_eq!(metrics_gauge("", "codelake_spill_used_bytes"), None);
    }
}
