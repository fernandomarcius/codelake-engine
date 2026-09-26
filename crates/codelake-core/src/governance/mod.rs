//! Query governance, shared by every client protocol (ADR 0016).
//!
//! - [`QueryRegistry`]: every governed query from submission to its end, and
//!   a bounded history, shown as `system.runtime.queries`.
//! - [`Admission`]: a bounded queue in front of a bounded number of running
//!   queries, per node, per resource group and per user (the user the client
//!   names; no authentication is assumed).
//! - [`TimeLimits`]: `query_max_run_time` (from submission, queueing
//!   included) and `query_max_execution_time` (from admission), with
//!   configured defaults. A protocol enforces them by dropping the query's
//!   streams at the deadline, which stops its operators and returns their
//!   memory to the pool.
//!
//! - Killing a query by id from any protocol
//!   (`CALL system.runtime.kill_query(…)`, [`Governance::kill`]) or canceling
//!   it as its owner ([`Governance::cancel`]).
//!
//! A protocol registers a query ([`Governance::register`]), asks for a place
//! ([`Governance::enter`]: a running slot, a queued one, or a refusal), waits
//! for its slot ([`Governance::admit`]) and holds the [`Permit`] while the
//! query executes.

mod admission;
mod kill;
pub mod metrics;
mod registry;
mod table;

use std::sync::Arc;
use std::time::{Duration, Instant};

pub use admission::{Admission, GLOBAL_GROUP, Permit, Refusal, Scope, Ticket, whole_match};
pub use kill::{ADMINISTRATIVELY_KILLED, KillError, KillRequest, kill_request, kill_request_sql};
pub use metrics::Phase;
pub use registry::{Failure, Protocol, QueryInfo, QueryRecord, QueryRegistry, QueryState, RegistryLimits};
pub use table::{QueriesTable, system_catalog};

use crate::config::GovernanceConfig;
use crate::session_properties::{
    QUERY_MAX_EXECUTION_TIME, QUERY_MAX_RUN_TIME, SessionProperties, format_duration, parse_duration,
};

/// Stable error names, as clients of the HTTP statement protocol know them.
pub const QUERY_QUEUE_FULL: &str = "QUERY_QUEUE_FULL";
pub const EXCEEDED_TIME_LIMIT: &str = "EXCEEDED_TIME_LIMIT";
pub const USER_CANCELED: &str = "USER_CANCELED";
/// Error type of the governance errors.
pub const INSUFFICIENT_RESOURCES: &str = "INSUFFICIENT_RESOURCES";

/// Why governance stopped a query.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GovernanceError {
    /// Every queue the query could wait in is full.
    #[error("Too many queued queries for {} (limit {})", .0.scope, .0.limit)]
    QueueFull(Refusal),
    /// The query waited in the queue for longer than the queue allows.
    #[error("Query exceeded maximum queued time of {}", format_duration(*.0))]
    QueuedTooLong(Duration),
    /// `query_max_run_time` or `query_max_execution_time` ran out.
    #[error("Query exceeded maximum time limit of {}", format_duration(*.0))]
    TimeLimit(Duration),
    /// The query was canceled (by its client) while it waited.
    #[error("Query was canceled")]
    Canceled,
}

impl GovernanceError {
    /// Stable error name (`QUERY_QUEUE_FULL`, `EXCEEDED_TIME_LIMIT`, …).
    pub fn name(&self) -> &'static str {
        match self {
            Self::QueueFull(_) => QUERY_QUEUE_FULL,
            Self::QueuedTooLong(_) | Self::TimeLimit(_) => EXCEEDED_TIME_LIMIT,
            Self::Canceled => USER_CANCELED,
        }
    }

    /// Error type (`INSUFFICIENT_RESOURCES`, `USER_ERROR`).
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Canceled => "USER_ERROR",
            _ => INSUFFICIENT_RESOURCES,
        }
    }

    pub fn failure(&self) -> Failure {
        Failure::new(self.name(), self.kind(), self.to_string())
    }
}

/// The time limits of one query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeLimits {
    /// From submission, queueing included.
    pub run_time: Duration,
    /// From admission.
    pub execution_time: Duration,
}

impl TimeLimits {
    /// The instant the query must be over, and the limit that sets it.
    pub fn deadline(&self, info: &QueryInfo) -> (Instant, Duration) {
        let run = far(info.created_at, self.run_time);
        let execution = far(info.admitted_at.unwrap_or_else(Instant::now), self.execution_time);
        if execution < run {
            (execution, self.execution_time)
        } else {
            (run, self.run_time)
        }
    }
}

/// `at + after`, or a year from now when that does not fit an `Instant`.
fn far(at: Instant, after: Duration) -> Instant {
    at.checked_add(after)
        .or_else(|| Instant::now().checked_add(Duration::from_hours(365 * 24)))
        .unwrap_or(at)
}

/// Governance of one node: see the module documentation.
#[derive(Debug)]
pub struct Governance {
    registry: Arc<QueryRegistry>,
    admission: Arc<Admission>,
    queue_timeout: Duration,
    defaults: TimeLimits,
}

impl Default for Governance {
    fn default() -> Self {
        Self::new(&GovernanceConfig::default(), None)
    }
}

impl Governance {
    /// `history` overrides the configured `(size, retention seconds)` of the
    /// registry's history (see [`crate::Config::query_history`]).
    pub fn new(config: &GovernanceConfig, history: Option<(usize, u64)>) -> Self {
        let (history_size, retention) =
            history.unwrap_or((config.history_size, config.history_retention_secs));
        let hundred_days = Duration::from_hours(100 * 24);
        let time = |text: &str| {
            parse_duration(text)
                .filter(|d| !d.is_zero())
                .unwrap_or(hundred_days)
        };
        Self {
            registry: Arc::new(QueryRegistry::new(RegistryLimits {
                history_size,
                history_retention: Duration::from_secs(retention),
                max_query_length: config.max_recorded_query_length,
            })),
            admission: Arc::new(Admission::new(config)),
            queue_timeout: Duration::from_secs(config.queue_timeout_secs),
            defaults: TimeLimits {
                run_time: time(&config.query_max_run_time),
                execution_time: time(&config.query_max_execution_time),
            },
        }
    }

    pub fn registry(&self) -> &Arc<QueryRegistry> {
        &self.registry
    }

    pub fn admission(&self) -> &Arc<Admission> {
        &self.admission
    }

    /// Configured defaults of the time limits.
    pub fn default_limits(&self) -> TimeLimits {
        self.defaults
    }

    /// Registers a query in state `QUEUED`.
    pub fn register(
        &self,
        protocol: Protocol,
        user: &str,
        source: Option<&str>,
        sql: &str,
    ) -> Arc<QueryRecord> {
        self.registry.register(protocol, user, source, sql)
    }

    /// The limits of a query: the session's values, else the configured
    /// defaults.
    pub fn limits(&self, run_time: Option<Duration>, execution_time: Option<Duration>) -> TimeLimits {
        TimeLimits {
            run_time: run_time.unwrap_or(self.defaults.run_time),
            execution_time: execution_time.unwrap_or(self.defaults.execution_time),
        }
    }

    /// [`Governance::limits`] from session properties.
    pub fn session_limits(&self, properties: &SessionProperties) -> TimeLimits {
        let get = |name| properties.get(name).and_then(parse_duration);
        self.limits(get(QUERY_MAX_RUN_TIME), get(QUERY_MAX_EXECUTION_TIME))
    }

    /// Takes a running or queued slot for `record`, in the resource group its
    /// user and source select; a refusal fails the record.
    pub fn enter(&self, record: &QueryRecord) -> Result<Ticket, GovernanceError> {
        let info = record.snapshot();
        let group = self.admission.classify(&info.user, info.source.as_deref());
        record.set_resource_group(self.admission.group_name(group));
        self.admission.enter(&info.user, group).map_err(|refusal| {
            metrics::rejected(refusal.scope.label());
            let error = GovernanceError::QueueFull(refusal);
            tracing::info!(query_id = record.id(), error = %error, "query refused");
            record.fail(error.failure());
            error
        })
    }

    /// Waits for `ticket`'s running slot, at most the queue timeout and what
    /// is left of the run time; stops early when the record fails (its client
    /// canceled it). A failure is recorded on the record, unless it was the
    /// cancellation that stopped the wait.
    pub async fn admit(
        &self,
        ticket: Ticket,
        record: &QueryRecord,
        limits: &TimeLimits,
    ) -> Result<Permit, GovernanceError> {
        let created = record.snapshot().created_at;
        let queue_deadline = far(created, self.queue_timeout);
        let run_deadline = far(created, limits.run_time);
        let (deadline, error) = if queue_deadline < run_deadline {
            (queue_deadline, GovernanceError::QueuedTooLong(self.queue_timeout))
        } else {
            (run_deadline, GovernanceError::TimeLimit(limits.run_time))
        };
        let cancel = record.cancellation();
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(GovernanceError::Canceled),
            permit = ticket.ready() => Ok(permit),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                metrics::timed_out(Phase::Queued);
                tracing::info!(query_id = record.id(), error = %error, "query timed out in the queue");
                record.fail(error.failure());
                Err(error)
            }
        };
        if outcome.is_ok() {
            record.admitted();
        }
        outcome
    }

    /// Admits `record` at once, without a slot of its own: it runs in a
    /// running slot an earlier query of the same client still holds, in that
    /// query's `resource_group` (a PostgreSQL connection's statement while a
    /// cursor or portal of the connection is open, ADR 0016). The client
    /// executes one statement at a time, so the slot still bounds what it
    /// runs at once; memory stays bounded by the pool.
    pub fn admit_alongside(&self, record: &QueryRecord, resource_group: Arc<str>) {
        record.set_resource_group(resource_group);
        record.admitted();
    }

    /// The error for a query whose time `limit` ran out while it executed,
    /// counted in the metrics. The caller records it (with its own detail).
    pub fn time_limit_exceeded(&self, record: &QueryRecord, limit: Duration) -> GovernanceError {
        metrics::timed_out(Phase::Running);
        let error = GovernanceError::TimeLimit(limit);
        tracing::info!(query_id = record.id(), error = %error, "query timed out");
        error
    }

    /// Forgets completed queries beyond the history limits.
    pub fn sweep(&self) {
        self.registry.sweep();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GovernanceConfig;

    fn governance(running: usize, queued: usize, queue_timeout_secs: u64) -> Governance {
        Governance::new(
            &GovernanceConfig {
                max_running_queries: running,
                max_queued_queries: queued,
                queue_timeout_secs,
                ..GovernanceConfig::default()
            },
            None,
        )
    }

    #[tokio::test]
    async fn a_query_admitted_alongside_takes_no_slot() {
        let g = governance(1, 0, 60);
        let holder = g.register(Protocol::Postgres, "u", None, "DECLARE c CURSOR FOR SELECT 1");
        let permit = g
            .admit(g.enter(&holder).unwrap(), &holder, &g.default_limits())
            .await;
        let permit = permit.unwrap();
        let alongside = g.register(Protocol::Postgres, "u", None, "SELECT 2");
        g.admit_alongside(&alongside, Arc::from("etl"));
        let info = alongside.snapshot();
        assert!(info.admitted_at.is_some(), "admitted");
        assert_eq!(&*info.resource_group, "etl");
        assert_eq!(g.admission().load(), (1, 0), "no slot of its own");
        drop(permit);
        assert_eq!(g.admission().load(), (0, 0));
    }

    #[tokio::test]
    async fn a_full_queue_fails_the_record_with_queue_full() {
        let g = governance(1, 0, 60);
        let first = g.register(Protocol::Http, "u", None, "SELECT 1");
        let _running = g.enter(&first).unwrap();
        let second = g.register(Protocol::Postgres, "u", None, "SELECT 2");
        let error = g.enter(&second).unwrap_err();
        assert_eq!(error.name(), QUERY_QUEUE_FULL);
        assert_eq!(
            error.to_string(),
            "Too many queued queries for this node (limit 0)"
        );
        let failure = second.failure().unwrap();
        assert_eq!(failure.name, QUERY_QUEUE_FULL);
        assert_eq!(failure.kind, INSUFFICIENT_RESOURCES);
        assert!(second.cancellation().is_cancelled());
    }

    #[tokio::test]
    async fn queue_timeout_fails_the_waiting_query() {
        let g = governance(1, 5, 1);
        let first = g.register(Protocol::Http, "u", None, "SELECT 1");
        let _running = g
            .admit(g.enter(&first).unwrap(), &first, &g.default_limits())
            .await;
        let second = g.register(Protocol::Http, "u", None, "SELECT 2");
        let ticket = g.enter(&second).unwrap();
        let error = g.admit(ticket, &second, &g.default_limits()).await.unwrap_err();
        assert_eq!(error, GovernanceError::QueuedTooLong(Duration::from_secs(1)));
        assert_eq!(error.to_string(), "Query exceeded maximum queued time of 1.00s");
        assert_eq!(second.failure().unwrap().name, EXCEEDED_TIME_LIMIT);
        assert_eq!(g.admission().load(), (1, 0), "the queued place was given back");
    }

    #[tokio::test]
    async fn run_time_counts_the_wait_in_the_queue() {
        let g = governance(1, 5, 600);
        let first = g.register(Protocol::Http, "u", None, "SELECT 1");
        let _running = g
            .admit(g.enter(&first).unwrap(), &first, &g.default_limits())
            .await;
        let second = g.register(Protocol::Http, "u", None, "SELECT 2");
        let limits = g.limits(Some(Duration::from_millis(200)), None);
        let error = g
            .admit(g.enter(&second).unwrap(), &second, &limits)
            .await
            .unwrap_err();
        assert_eq!(error, GovernanceError::TimeLimit(Duration::from_millis(200)));
    }

    #[tokio::test]
    async fn canceling_a_waiting_query_leaves_the_queue() {
        let g = Arc::new(governance(1, 5, 600));
        let first = g.register(Protocol::Http, "u", None, "SELECT 1");
        let running = g
            .admit(g.enter(&first).unwrap(), &first, &g.default_limits())
            .await
            .unwrap();
        let second = g.register(Protocol::Http, "u", None, "SELECT 2");
        let ticket = g.enter(&second).unwrap();
        let waiter = {
            let (g, second) = (Arc::clone(&g), Arc::clone(&second));
            tokio::spawn(async move { g.admit(ticket, &second, &g.default_limits()).await })
        };
        tokio::task::yield_now().await;
        assert_eq!(g.admission().load(), (1, 1));
        second.fail(Failure::new(USER_CANCELED, "USER_ERROR", "Query was canceled"));
        assert_eq!(waiter.await.unwrap().unwrap_err(), GovernanceError::Canceled);
        assert_eq!(g.admission().load(), (1, 0));
        drop(running);
        assert_eq!(g.admission().load(), (0, 0));
    }

    #[tokio::test]
    async fn admission_is_recorded() {
        let g = governance(4, 5, 600);
        let record = g.register(Protocol::Postgres, "u", None, "SELECT 1");
        assert!(record.snapshot().admitted_at.is_none());
        let _permit = g
            .admit(g.enter(&record).unwrap(), &record, &g.default_limits())
            .await
            .unwrap();
        assert!(record.snapshot().admitted_at.is_some());
        assert_eq!(&*record.snapshot().resource_group, GLOBAL_GROUP);
    }

    #[test]
    fn deadline_is_the_earlier_of_run_and_execution_time() {
        let g = governance(4, 5, 600);
        let record = g.register(Protocol::Http, "u", None, "SELECT 1");
        record.admitted();
        let info = record.snapshot();
        let limits = g.limits(Some(Duration::from_secs(10)), Some(Duration::from_secs(5)));
        assert_eq!(limits.deadline(&info).1, Duration::from_secs(5));
        let limits = g.limits(Some(Duration::from_secs(5)), Some(Duration::from_secs(10)));
        assert_eq!(limits.deadline(&info).1, Duration::from_secs(5));
        assert!(limits.deadline(&info).0 <= info.created_at + Duration::from_secs(5));
        // Defaults: 100 days, without overflowing.
        let defaults = g.default_limits();
        assert_eq!(defaults.run_time, Duration::from_hours(100 * 24));
        let _ = defaults.deadline(&info);
    }

    #[test]
    fn session_values_override_the_defaults() {
        let g = Governance::new(
            &GovernanceConfig {
                query_max_run_time: "1h".into(),
                ..GovernanceConfig::default()
            },
            None,
        );
        assert_eq!(g.default_limits().run_time, Duration::from_hours(1));
        let registry = crate::session_properties::PropertyRegistry::default();
        let session = registry
            .session(
                [(QUERY_MAX_EXECUTION_TIME.to_owned(), "30s".to_owned())]
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        let limits = g.session_limits(&session);
        assert_eq!(limits.run_time, Duration::from_hours(1));
        assert_eq!(limits.execution_time, Duration::from_secs(30));
    }
}
