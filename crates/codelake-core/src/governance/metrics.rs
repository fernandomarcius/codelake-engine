//! Prometheus metrics of query governance, through the `metrics` facade (the
//! server installs the exporter; without one these calls do nothing).
//!
//! | metric | type | labels |
//! |---|---|---|
//! | `codelake_queries_running` | gauge | — |
//! | `codelake_queries_queued` | gauge | — |
//! | `codelake_queries_suspended` | gauge | — |
//! | `codelake_resource_group_running` | gauge | `resource_group` |
//! | `codelake_resource_group_queued` | gauge | `resource_group` |
//! | `codelake_queries_rejected_total` | counter | `scope` (`node`, `resource_group`, `user`) |
//! | `codelake_queries_timed_out_total` | counter | `phase` (`queued`, `running`) |
//! | `codelake_queries_completed_total` | counter | `protocol`, `state` (`FINISHED`, `FAILED`) |
//!
//! Users are never a label: their number is unbounded.

use super::registry::{Protocol, QueryState};

pub const RUNNING: &str = "codelake_queries_running";
pub const QUEUED: &str = "codelake_queries_queued";
pub const SUSPENDED: &str = "codelake_queries_suspended";
pub const GROUP_RUNNING: &str = "codelake_resource_group_running";
pub const GROUP_QUEUED: &str = "codelake_resource_group_queued";
pub const REJECTED: &str = "codelake_queries_rejected_total";
pub const TIMED_OUT: &str = "codelake_queries_timed_out_total";
pub const COMPLETED: &str = "codelake_queries_completed_total";

/// Where a query ran out of time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Queued,
    Running,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
        }
    }
}

/// Slots in use on the node (`group` `None`) or in one resource group.
pub(crate) fn slots(group: Option<&str>, running: f64, queued: f64) {
    match group {
        None => {
            metrics::gauge!(RUNNING).set(running);
            metrics::gauge!(QUEUED).set(queued);
        }
        Some(group) => {
            metrics::gauge!(GROUP_RUNNING, "resource_group" => group.to_owned()).set(running);
            metrics::gauge!(GROUP_QUEUED, "resource_group" => group.to_owned()).set(queued);
        }
    }
}

/// Queries on the node that gave their running slot back while their client
/// does not read.
pub(crate) fn suspended(count: f64) {
    metrics::gauge!(SUSPENDED).set(count);
}

pub(crate) fn rejected(scope: &'static str) {
    metrics::counter!(REJECTED, "scope" => scope).increment(1);
}

pub(crate) fn timed_out(phase: Phase) {
    metrics::counter!(TIMED_OUT, "phase" => phase.as_str()).increment(1);
}

pub(crate) fn completed(protocol: Protocol, state: QueryState) {
    metrics::counter!(COMPLETED, "protocol" => protocol.as_str(), "state" => state.as_str()).increment(1);
}
