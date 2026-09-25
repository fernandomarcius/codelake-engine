//! The node's query registry: every governed query of every client protocol,
//! from submission to its end, plus a bounded history of completed ones.
//!
//! Lifecycle: `QUEUED` when submitted (and while it waits for an admission
//! slot), `PLANNING` while it is analyzed, `RUNNING` while it produces rows,
//! `FINISHING` once the last row is produced and the client has not fetched
//! it yet, then `FINISHED` or `FAILED`. States only move forward.
//!
//! Bounds: completed queries are kept for `history_retention`, at most
//! `history_size` of them, enforced on every insertion; unfinished ones are
//! bounded by admission and by each protocol's own caps; each keeps at most
//! `max_query_length` characters of SQL.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use tokio_util::sync::CancellationToken;

use super::metrics;

/// The client protocol a query came through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// PostgreSQL wire protocol.
    Postgres,
    /// HTTP statement protocol (`/v1/statement`).
    Http,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgresql",
            Self::Http => "http",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    Queued,
    Planning,
    Running,
    Finishing,
    Finished,
    Failed,
}

impl QueryState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Planning => "PLANNING",
            Self::Running => "RUNNING",
            Self::Finishing => "FINISHING",
            Self::Finished => "FINISHED",
            Self::Failed => "FAILED",
        }
    }

    pub fn is_done(self) -> bool {
        matches!(self, Self::Finished | Self::Failed)
    }
}

/// Why a query failed, as listed in `system.runtime.queries`: a stable error
/// name (`EXCEEDED_TIME_LIMIT`), its type (`USER_ERROR`,
/// `INSUFFICIENT_RESOURCES`, …) and the message. `detail` keeps the
/// protocol's own error, so the protocol that recorded it can answer with it
/// again.
#[derive(Clone)]
pub struct Failure {
    pub name: String,
    pub kind: String,
    pub message: String,
    pub detail: Option<Arc<dyn Any + Send + Sync>>,
}

impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Failure")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl Failure {
    pub fn new(name: impl Into<String>, kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: kind.into(),
            message: message.into(),
            detail: None,
        }
    }

    #[must_use]
    pub fn with_detail(mut self, detail: Arc<dyn Any + Send + Sync>) -> Self {
        self.detail = Some(detail);
        self
    }

    /// The protocol error kept in `detail`, when it is a `T`.
    pub fn detail<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.detail.as_deref().and_then(|d| d.downcast_ref::<T>())
    }
}

/// What the registry knows about one query.
#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub protocol: Protocol,
    pub user: String,
    pub source: Option<String>,
    /// Shared, so snapshots do not copy the text.
    pub query: Arc<str>,
    /// Resource group the query was admitted in (`global` when none matched).
    pub resource_group: Arc<str>,
    pub state: QueryState,
    pub created: SystemTime,
    pub created_at: Instant,
    pub started: Option<SystemTime>,
    /// When the query left the queue.
    pub admitted_at: Option<Instant>,
    pub ended: Option<SystemTime>,
    pub ended_at: Option<Instant>,
    /// Last time the client was seen (a request of the query's protocol).
    pub last_heartbeat: SystemTime,
    pub heartbeat_at: Instant,
    pub queued: Duration,
    pub analysis: Duration,
    pub planning: Duration,
    pub rows: u64,
    pub failure: Option<Failure>,
}

/// One registered query: its [`QueryInfo`] and the token that stops it.
pub struct QueryRecord {
    id: String,
    info: Mutex<QueryInfo>,
    cancel: CancellationToken,
}

impl std::fmt::Debug for QueryRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRecord")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl QueryRecord {
    fn new(id: String, protocol: Protocol, user: &str, source: Option<&str>, query: &str) -> Self {
        let now = SystemTime::now();
        let at = Instant::now();
        Self {
            id,
            info: Mutex::new(QueryInfo {
                protocol,
                user: user.to_owned(),
                source: source.map(str::to_owned),
                query: Arc::from(query),
                resource_group: Arc::from(super::GLOBAL_GROUP),
                state: QueryState::Queued,
                created: now,
                created_at: at,
                started: None,
                admitted_at: None,
                ended: None,
                ended_at: None,
                last_heartbeat: now,
                heartbeat_at: at,
                queued: Duration::ZERO,
                analysis: Duration::ZERO,
                planning: Duration::ZERO,
                rows: 0,
                failure: None,
            }),
            cancel: CancellationToken::new(),
        }
    }

    /// A record outside any registry, for tests of protocol code.
    pub fn detached(id: &str, protocol: Protocol, user: &str, query: &str) -> Arc<Self> {
        Arc::new(Self::new(id.to_owned(), protocol, user, None, query))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn info(&self) -> MutexGuard<'_, QueryInfo> {
        // A panic while holding this lock leaves plain data behind; keep going.
        self.info
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn snapshot(&self) -> QueryInfo {
        self.info().clone()
    }

    pub fn state(&self) -> QueryState {
        self.info().state
    }

    pub fn is_done(&self) -> bool {
        self.info().state.is_done()
    }

    pub fn user(&self) -> String {
        self.info().user.clone()
    }

    /// When the query finished or failed.
    pub fn ended_at(&self) -> Option<Instant> {
        self.info().ended_at
    }

    pub fn heartbeat(&self) {
        let mut info = self.info();
        info.heartbeat_at = Instant::now();
        info.last_heartbeat = SystemTime::now();
    }

    pub(crate) fn set_resource_group(&self, group: Arc<str>) {
        self.info().resource_group = group;
    }

    /// The query left the queue (see [`super::Governance::admit`]).
    pub(crate) fn admitted(&self) {
        self.info().admitted_at = Some(Instant::now());
    }

    /// Moves forward only; a finished or failed query never changes state.
    pub fn set_state(&self, state: QueryState) {
        let protocol = {
            let mut info = self.info();
            if info.state.is_done() {
                return;
            }
            if info.state == QueryState::Queued && state != QueryState::Queued {
                info.queued = info.created_at.elapsed();
                info.started = Some(SystemTime::now());
            }
            info.state = state;
            if !state.is_done() {
                return;
            }
            info.ended = Some(SystemTime::now());
            info.ended_at = Some(Instant::now());
            info.protocol
        };
        metrics::completed(protocol, state);
    }

    pub fn record_timing(&self, analysis: Duration, planning: Duration) {
        let mut info = self.info();
        info.analysis = analysis;
        info.planning = planning;
    }

    pub fn add_rows(&self, rows: u64) {
        let mut info = self.info();
        info.rows = info.rows.saturating_add(rows);
    }

    /// Records the first failure (later ones are consequences of it) and
    /// cancels the query. Returns whether this failure was the one recorded.
    pub fn fail(&self, failure: Failure) -> bool {
        {
            let mut info = self.info();
            if info.state.is_done() {
                return false;
            }
            info.failure = Some(failure);
        }
        self.set_state(QueryState::Failed);
        self.cancel.cancel();
        true
    }

    pub fn finish(&self) {
        self.set_state(QueryState::Finished);
    }

    pub fn failure(&self) -> Option<Failure> {
        self.info().failure.clone()
    }

    /// Canceled when the query fails, whoever fails it.
    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

/// Registry sizes, from [`crate::config::GovernanceConfig`].
#[derive(Debug, Clone, Copy)]
pub struct RegistryLimits {
    pub history_size: usize,
    pub history_retention: Duration,
    /// SQL characters kept per query.
    pub max_query_length: usize,
}

pub struct QueryRegistry {
    queries: Mutex<HashMap<String, Arc<QueryRecord>>>,
    limits: RegistryLimits,
}

impl std::fmt::Debug for QueryRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRegistry")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl QueryRegistry {
    pub fn new(limits: RegistryLimits) -> Self {
        Self {
            queries: Mutex::new(HashMap::new()),
            limits,
        }
    }

    fn map(&self) -> MutexGuard<'_, HashMap<String, Arc<QueryRecord>>> {
        self.queries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Creates and registers a query, forgetting completed ones beyond the
    /// history limits first, so the registry stays bounded between sweeps.
    pub fn register(
        &self,
        protocol: Protocol,
        user: &str,
        source: Option<&str>,
        query: &str,
    ) -> Arc<QueryRecord> {
        let text = truncated(query, self.limits.max_query_length);
        let record = Arc::new(QueryRecord::new(
            crate::trace::next_query_id(),
            protocol,
            user,
            source,
            text,
        ));
        let mut map = self.map();
        Self::trim_history(&mut map, &self.limits);
        map.insert(record.id.clone(), Arc::clone(&record));
        record
    }

    /// Keeps the newest `history_size` completed queries younger than
    /// `history_retention`.
    fn trim_history(map: &mut HashMap<String, Arc<QueryRecord>>, limits: &RegistryLimits) {
        let mut done: Vec<(String, Instant)> = map
            .iter()
            .filter_map(|(id, q)| q.ended_at().map(|at| (id.clone(), at)))
            .collect();
        if done.len() <= limits.history_size
            && done
                .iter()
                .all(|(_, at)| at.elapsed() <= limits.history_retention)
        {
            return;
        }
        done.sort_by_key(|(_, at)| std::cmp::Reverse(*at));
        for (index, (id, at)) in done.into_iter().enumerate() {
            if index >= limits.history_size || at.elapsed() > limits.history_retention {
                map.remove(&id);
            }
        }
    }

    /// Forgets completed queries beyond the history limits.
    pub fn sweep(&self) {
        Self::trim_history(&mut self.map(), &self.limits);
    }

    pub fn get(&self, id: &str) -> Option<Arc<QueryRecord>> {
        self.map().get(id).cloned()
    }

    pub fn contains(&self, id: &str) -> bool {
        self.map().contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.map().len()
    }

    pub fn is_empty(&self) -> bool {
        self.map().is_empty()
    }

    pub fn snapshot(&self) -> Vec<(String, QueryInfo)> {
        let queries: Vec<_> = self.map().values().cloned().collect();
        queries.iter().map(|q| (q.id.clone(), q.snapshot())).collect()
    }
}

/// The first `max` characters of `text`.
fn truncated(text: &str, max: usize) -> &str {
    text.char_indices().nth(max).map_or(text, |(end, _)| &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(history_size: usize) -> QueryRegistry {
        QueryRegistry::new(RegistryLimits {
            history_size,
            history_retention: Duration::from_mins(1),
            max_query_length: 8,
        })
    }

    #[test]
    fn state_moves_forward_only() {
        let registry = registry(10);
        let q = registry.register(Protocol::Http, "u", None, "SELECT 1");
        q.set_state(QueryState::Running);
        assert!(q.snapshot().started.is_some());
        q.finish();
        q.set_state(QueryState::Running);
        assert!(!q.fail(Failure::new("GENERIC_USER_ERROR", "USER_ERROR", "late")));
        let info = q.snapshot();
        assert_eq!(info.state, QueryState::Finished);
        assert!(info.failure.is_none());
        assert!(info.ended.is_some());
        assert!(!q.cancellation().is_cancelled());
    }

    #[test]
    fn the_first_failure_wins_and_cancels() {
        let registry = registry(10);
        let q = registry.register(Protocol::Postgres, "u", Some("psql"), "SELECT 1");
        let token = q.cancellation();
        assert!(q.fail(Failure::new("USER_CANCELED", "USER_ERROR", "canceled")));
        assert!(!q.fail(Failure::new(
            "EXCEEDED_TIME_LIMIT",
            "INSUFFICIENT_RESOURCES",
            "late"
        )));
        assert!(token.is_cancelled());
        assert_eq!(q.failure().unwrap().name, "USER_CANCELED");
        assert_eq!(q.state(), QueryState::Failed);
    }

    #[test]
    fn failure_keeps_the_protocol_detail() {
        let failure = Failure::new("X", "USER_ERROR", "m").with_detail(Arc::new(7_u32));
        assert_eq!(failure.detail::<u32>(), Some(&7));
        assert_eq!(failure.detail::<String>(), None);
    }

    #[test]
    fn history_is_bounded_on_register() {
        let registry = registry(2);
        for _ in 0..20 {
            registry.register(Protocol::Http, "u", None, "SELECT 1").finish();
        }
        // history_size = 2, plus the query just registered.
        assert_eq!(registry.len(), 3);
        let running = registry.register(Protocol::Http, "u", None, "SELECT 2");
        for _ in 0..5 {
            registry.register(Protocol::Http, "u", None, "SELECT 1").finish();
        }
        assert!(registry.contains(running.id()), "unfinished queries stay");
    }

    #[test]
    fn query_text_is_truncated_to_the_limit() {
        let registry = registry(2);
        let q = registry.register(Protocol::Postgres, "u", None, "SELECT 'ação', 1234567");
        assert_eq!(&*q.snapshot().query, "SELECT '");
        let short = registry.register(Protocol::Postgres, "u", None, "SELECT");
        assert_eq!(&*short.snapshot().query, "SELECT");
        assert_eq!(truncated("ação!", 3), "açã");
    }

    #[test]
    fn ids_are_unique() {
        let registry = registry(10);
        let a = registry.register(Protocol::Http, "u", None, "SELECT 1");
        let b = registry.register(Protocol::Http, "u", None, "SELECT 1");
        assert_ne!(a.id(), b.id());
        assert_eq!(a.id().split('_').count(), 4, "{}", a.id());
    }
}
