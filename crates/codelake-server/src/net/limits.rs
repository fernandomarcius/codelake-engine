//! The process's open-files limit and how the client listeners share it
//! (ADR 0018).
//!
//! Every connection is a descriptor. With the soft limit a container usually
//! starts with (1024) and no connection limit, ~1000 idle keep-alive clients
//! used every descriptor: the kernel still completed new TCP handshakes, but
//! `accept` failed, and every new client — `SELECT 1`, the health checks —
//! waited until it timed out. So the server raises its soft limit to the hard
//! one at startup, keeps [`reserved`](codelake_core::config::ServerConfig::reserved_files)
//! descriptors away from clients, and gives each listener a bounded share of
//! the rest: past its share a listener answers at once with an error instead
//! of leaving the client hanging.

use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
use tracing::{info, warn};

/// Connections the ops listener (probes, `/metrics`) holds at once: a few
/// probes and scrapers; its own share, so a crowd of query clients never
/// takes the descriptors the probes need.
pub(crate) const OPS_MAX_CONNECTIONS: usize = 32;

/// Soft limit asked for when the hard one is unlimited (`RLIM_INFINITY`):
/// Linux never allows an unlimited `RLIMIT_NOFILE`, and 2^20 is its default
/// `fs.nr_open`.
const UNLIMITED_HARD_TARGET: u64 = 1 << 20;

/// The open-files limits before and after [`raise_open_files`]; `None` is
/// unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpenFiles {
    pub(crate) soft_before: Option<u64>,
    pub(crate) soft: Option<u64>,
    pub(crate) hard: Option<u64>,
}

/// The soft limit to set: the hard limit, or `cap` when given (never above
/// the hard limit, which an unprivileged process cannot raise).
pub(crate) fn target_soft(hard: Option<u64>, cap: Option<u64>) -> u64 {
    match (hard, cap) {
        (Some(hard), Some(cap)) => hard.min(cap),
        (Some(hard), None) => hard,
        (None, Some(cap)) => cap,
        (None, None) => UNLIMITED_HARD_TARGET,
    }
}

/// Sets the soft `RLIMIT_NOFILE` to [`target_soft`] and logs both limits.
/// A refusal (a platform below the target, such as macOS's `OPEN_MAX`) is
/// logged and the process keeps the limit it had.
pub(crate) fn raise_open_files(cap: Option<u64>) -> OpenFiles {
    let before = getrlimit(Resource::Nofile);
    let target = target_soft(before.maximum, cap);
    if before.current != Some(target) {
        let wanted = Rlimit {
            current: Some(target),
            maximum: before.maximum,
        };
        if let Err(err) = setrlimit(Resource::Nofile, wanted) {
            warn!(
                error = %err,
                target,
                soft = ?before.current,
                hard = ?before.maximum,
                "cannot set the open files limit; keeping the current one"
            );
        }
    }
    let after = getrlimit(Resource::Nofile);
    let limits = OpenFiles {
        soft_before: before.current,
        soft: after.current,
        hard: after.maximum,
    };
    info!(
        soft_before = ?limits.soft_before,
        soft = ?limits.soft,
        hard = ?limits.hard,
        cap = ?cap,
        "open files limit"
    );
    limits
}

/// The soft open-files limit now (`None`: unlimited).
pub(crate) fn current_soft() -> Option<u64> {
    getrlimit(Resource::Nofile).current
}

/// Descriptors open in this process now, where the platform lists them.
pub(crate) fn open_descriptors() -> Option<usize> {
    ["/proc/self/fd", "/dev/fd"]
        .iter()
        .find_map(|dir| std::fs::read_dir(dir).ok())
        .map(Iterator::count)
}

/// How many connections each listener may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Budget {
    pub(crate) ops: usize,
    pub(crate) pg: usize,
    /// `None` when the Trino protocol listener is off.
    pub(crate) trino: Option<usize>,
}

/// The Trino protocol listener's claim on the descriptors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrinoShare {
    /// The listener is off.
    Off,
    /// What the other listeners and the reserve leave.
    Rest,
    /// `[server.trino] max_connections`.
    AtMost(usize),
}

impl TrinoShare {
    pub(crate) fn of(trino: Option<&codelake_core::TrinoProtocolConfig>) -> Self {
        match trino {
            None => Self::Off,
            Some(trino) => trino.max_connections.map_or(Self::Rest, Self::AtMost),
        }
    }
}

/// Splits `soft` descriptors: `reserved` stay out of reach of clients, the
/// ops listener gets [`OPS_MAX_CONNECTIONS`], PostgreSQL `pg`, and the Trino
/// protocol listener what is left, or its configured maximum when that
/// fits. Fails when the limit cannot hold them: better a clear error at
/// startup than clients hanging later.
pub(crate) fn budget(
    soft: Option<u64>,
    reserved: u64,
    pg: usize,
    trino: TrinoShare,
) -> Result<Budget, String> {
    // Semaphore permits are bounded; an unlimited soft limit is that bound.
    let permits = tokio::sync::Semaphore::MAX_PERMITS;
    let soft = soft.map_or(permits, |s| usize::try_from(s).unwrap_or(permits));
    let reserved = usize::try_from(reserved).unwrap_or(usize::MAX);
    let needed = reserved.saturating_add(OPS_MAX_CONNECTIONS).saturating_add(pg);
    let Some(left) = soft.checked_sub(needed) else {
        return Err(format!(
            "the open files limit {soft} cannot hold server.reserved_files ({reserved}) + the ops listener \
             ({OPS_MAX_CONNECTIONS}) + server.max_connections ({pg}): raise the limit (`ulimit -n`, \
             server.open_files_limit) or lower them"
        ));
    };
    let trino = match trino {
        TrinoShare::Off => None,
        _ if left == 0 => {
            return Err(format!(
                "the open files limit {soft} leaves no descriptor for the Trino protocol listener after \
                 server.reserved_files ({reserved}), the ops listener ({OPS_MAX_CONNECTIONS}) and \
                 server.max_connections ({pg})"
            ));
        }
        TrinoShare::Rest => Some(left.min(permits)),
        TrinoShare::AtMost(max) if max > left => {
            return Err(format!(
                "server.trino.max_connections {max} exceeds the {left} descriptors the open files limit \
                 {soft} leaves after server.reserved_files ({reserved}), the ops listener \
                 ({OPS_MAX_CONNECTIONS}) and server.max_connections ({pg})"
            ));
        }
        TrinoShare::AtMost(max) => Some(max),
    };
    Ok(Budget {
        ops: OPS_MAX_CONNECTIONS,
        pg,
        trino,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soft_limit_goes_to_the_hard_one_unless_capped() {
        assert_eq!(target_soft(Some(524_288), None), 524_288);
        assert_eq!(target_soft(Some(524_288), Some(4096)), 4096);
        // The cap never asks for more than the hard limit allows.
        assert_eq!(target_soft(Some(1024), Some(4096)), 1024);
        assert_eq!(target_soft(None, Some(4096)), 4096);
        assert_eq!(target_soft(None, None), 1 << 20);
    }

    #[test]
    fn the_trino_listener_gets_what_the_others_leave() {
        // 1024 - 256 reserved - 32 ops - 256 PostgreSQL.
        assert_eq!(
            budget(Some(1024), 256, 256, TrinoShare::Rest),
            Ok(Budget {
                ops: 32,
                pg: 256,
                trino: Some(480),
            })
        );
        assert_eq!(
            budget(Some(1024), 256, 256, TrinoShare::Off),
            Ok(Budget {
                ops: 32,
                pg: 256,
                trino: None,
            })
        );
        assert_eq!(
            budget(Some(1024), 256, 256, TrinoShare::AtMost(480)).map(|b| b.trino),
            Ok(Some(480))
        );
        assert_eq!(
            budget(Some(1024), 256, 256, TrinoShare::AtMost(10)).map(|b| b.trino),
            Ok(Some(10))
        );
        // Unlimited: bounded by what a semaphore can count.
        assert_eq!(
            budget(None, 256, 256, TrinoShare::Rest).map(|b| b.trino),
            Ok(Some(tokio::sync::Semaphore::MAX_PERMITS - 544))
        );
    }

    #[test]
    fn a_limit_too_low_is_refused_at_startup() {
        let err = budget(Some(1024), 256, 256, TrinoShare::AtMost(481)).unwrap_err();
        assert!(err.contains("max_connections 481 exceeds the 480"), "{err}");
        // Exactly the fixed shares: nothing left for the Trino listener…
        let err = budget(Some(544), 256, 256, TrinoShare::Rest).unwrap_err();
        assert!(err.contains("leaves no descriptor"), "{err}");
        // … which is fine when it is off.
        assert_eq!(
            budget(Some(544), 256, 256, TrinoShare::Off).map(|b| b.pg),
            Ok(256)
        );
        let err = budget(Some(543), 256, 256, TrinoShare::Off).unwrap_err();
        assert!(err.contains("cannot hold"), "{err}");
    }

    #[test]
    fn open_descriptors_are_counted() {
        // At least stdin, stdout and stderr.
        assert!(open_descriptors().unwrap() >= 3);
        assert!(current_soft().is_none_or(|soft| soft > 0));
    }
}
