//! Paging index of the HTTP statement protocol: the queries a client may
//! still page or cancel, by id, with their paging state. What is known about
//! each query (state, times, failure) lives in the engine's query registry
//! (`codelake_core::governance`), which `system.runtime.queries` shows; an
//! entry here only adds the protocol's side.
//!
//! A client that stops polling for `client_timeout` abandons its query, which
//! is then canceled (`ABANDONED_QUERY`). Accepted-and-unfinished queries are
//! capped by `max_active`; a completed query stays reachable as long as the
//! registry keeps it in its history, so the index never outgrows it. A failed
//! query drops its buffered results at once ([`Release`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use codelake_core::governance::{QueryInfo, QueryRecord, QueryRegistry, QueryState};

use super::errors::{Code, QueryError};

pub(crate) type Info = QueryInfo;
pub(crate) type State = QueryState;

pub(crate) struct Query<E> {
    pub(crate) id: String,
    /// Unguessable part of every URI of this query.
    pub(crate) slug: String,
    record: Arc<QueryRecord>,
    pub(crate) exec: tokio::sync::Mutex<E>,
    /// Requests paging this query right now (holding or awaiting `exec`).
    fetchers: AtomicUsize,
}

impl<E> std::fmt::Debug for Query<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Query")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Paging state a failed query no longer needs (its buffered results).
pub(crate) trait Release {
    fn release(&mut self);
}

impl Release for () {
    fn release(&mut self) {}
}

impl<E: Release> Query<E> {
    pub(crate) fn new(record: Arc<QueryRecord>, exec: E) -> Self {
        Self {
            id: record.id().to_owned(),
            slug: uuid::Uuid::new_v4().simple().to_string(),
            record,
            exec: tokio::sync::Mutex::new(exec),
            fetchers: AtomicUsize::new(0),
        }
    }

    pub(crate) fn record(&self) -> &Arc<QueryRecord> {
        &self.record
    }

    pub(crate) fn snapshot(&self) -> Info {
        self.record.snapshot()
    }

    pub(crate) fn is_done(&self) -> bool {
        self.record.is_done()
    }

    pub(crate) fn heartbeat(&self) {
        self.record.heartbeat();
    }

    /// Moves forward only; a finished or failed query never changes state.
    pub(crate) fn set_state(&self, state: State) {
        self.record.set_state(state);
    }

    pub(crate) fn record_timing(&self, analysis: Duration, planning: Duration) {
        self.record.record_timing(analysis, planning);
    }

    pub(crate) fn add_rows(&self, rows: u64) {
        self.record.add_rows(rows);
    }

    /// Records the first failure (later ones are consequences of it), stops
    /// the execution and drops the buffered results. When pages are being
    /// requested right now, the last of those requests releases them as it
    /// ends ([`Query::leave_fetch`]), so they all answer the same page.
    pub(crate) fn fail(&self, error: &QueryError) {
        self.record.fail(error.failure());
        // Also when the record had failed already (governance fails records
        // itself): a finished query keeps its last page for a retry.
        if self.record.state() == State::Failed {
            self.release_if_idle();
        }
    }

    /// A page request starts.
    pub(crate) fn enter_fetch(&self) {
        self.fetchers.fetch_add(1, Ordering::SeqCst);
    }

    /// A page request ended (its `exec` guard dropped): the client was there
    /// until now, so the heartbeat moves to the end of the request. The last
    /// request out of a failed query releases what the failure could not.
    pub(crate) fn leave_fetch(&self) {
        self.heartbeat();
        if self.fetchers.fetch_sub(1, Ordering::SeqCst) == 1 && self.error().is_some() {
            self.release_if_idle();
        }
    }

    fn release_if_idle(&self) {
        if self.fetchers.load(Ordering::SeqCst) == 0
            && let Ok(mut exec) = self.exec.try_lock()
        {
            exec.release();
        }
    }

    pub(crate) fn finish(&self) {
        self.record.finish();
    }

    pub(crate) fn error(&self) -> Option<QueryError> {
        self.record.failure().map(|f| QueryError::from_failure(&f))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) max_active: usize,
    pub(crate) client_timeout: Duration,
    /// Longest SQL text accepted, in characters (Trino's `query.max-length`).
    pub(crate) max_query_length: usize,
}

pub(crate) struct Registry<E> {
    queries: Mutex<HashMap<String, Arc<Query<E>>>>,
    /// The engine's registry: an entry here lives as long as its record there.
    records: Arc<QueryRegistry>,
    limits: Limits,
}

impl<E> std::fmt::Debug for Registry<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl<E: Release> Registry<E> {
    pub(crate) fn new(records: Arc<QueryRegistry>, limits: Limits) -> Self {
        Self {
            queries: Mutex::new(HashMap::new()),
            records,
            limits,
        }
    }

    fn map(&self) -> MutexGuard<'_, HashMap<String, Arc<Query<E>>>> {
        self.queries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Indexes a query, forgetting completed ones the engine's registry no
    /// longer lists; refuses it beyond `max_active` unfinished queries.
    pub(crate) fn insert(&self, query: Arc<Query<E>>) -> Result<(), QueryError> {
        let mut map = self.map();
        self.trim(&mut map);
        let active = map.values().filter(|q| !q.is_done()).count();
        if active >= self.limits.max_active {
            return Err(QueryError::new(
                Code::QueryQueueFull,
                format!("Too many queued queries (limit {})", self.limits.max_active),
            ));
        }
        map.insert(query.id.clone(), query);
        Ok(())
    }

    fn trim(&self, map: &mut HashMap<String, Arc<Query<E>>>) {
        map.retain(|id, query| !query.is_done() || self.records.contains(id));
    }

    pub(crate) fn get(&self, id: &str) -> Option<Arc<Query<E>>> {
        self.map().get(id).cloned()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map().len()
    }

    /// Cancels abandoned queries and forgets old completed ones.
    ///
    /// A query is abandoned when no page request came for `client_timeout`.
    /// A request still being answered is the client waiting, not gone: a page
    /// can take longer than the timeout when the node is busy (a spilling
    /// aggregation kept every worker for seconds, and its client was told it
    /// had abandoned the query while it waited for the answer).
    pub(crate) fn sweep(&self) {
        let queries: Vec<_> = self.map().values().cloned().collect();
        for query in &queries {
            let info = query.snapshot();
            let fetching = query.fetchers.load(Ordering::SeqCst) > 0;
            if !info.state.is_done() && !fetching && info.heartbeat_at.elapsed() > self.limits.client_timeout
            {
                query.fail(&QueryError::new(
                    Code::AbandonedQuery,
                    format!(
                        "Query {} was abandoned by the client, as it may have exited or stopped checking for query results",
                        query.id
                    ),
                ));
            }
        }
        self.records.sweep();
        self.trim(&mut self.map());
    }
}

#[cfg(test)]
mod tests {
    use codelake_core::governance::{Failure, Protocol, RegistryLimits};

    use super::*;

    fn records(history_size: usize) -> Arc<QueryRegistry> {
        Arc::new(QueryRegistry::new(RegistryLimits {
            history_size,
            history_retention: Duration::from_mins(1),
            max_query_length: 1000,
        }))
    }

    fn registry(records: &Arc<QueryRegistry>, max_active: usize) -> Registry<()> {
        Registry::new(
            Arc::clone(records),
            Limits {
                max_active,
                client_timeout: Duration::from_millis(50),
                max_query_length: 1000,
            },
        )
    }

    fn query(records: &QueryRegistry) -> Arc<Query<()>> {
        Arc::new(Query::new(
            records.register(Protocol::Http, "u", None, "SELECT 1"),
            (),
        ))
    }

    #[test]
    fn state_moves_forward_only() {
        let records = records(10);
        let q = query(&records);
        q.set_state(State::Running);
        assert!(q.snapshot().started.is_some());
        q.finish();
        q.set_state(State::Running);
        q.fail(&QueryError::new(Code::GenericUserError, "late"));
        let info = q.snapshot();
        assert_eq!(info.state, State::Finished);
        assert!(info.failure.is_none());
        assert!(info.ended.is_some());
    }

    #[test]
    fn active_limit_rejects_with_queue_full() {
        let records = records(10);
        let registry = registry(&records, 1);
        registry.insert(query(&records)).unwrap();
        let err = registry.insert(query(&records)).unwrap_err();
        assert_eq!(err.name(), "QUERY_QUEUE_FULL");
    }

    #[test]
    fn sweep_cancels_abandoned_and_trims_history() {
        let records = records(2);
        let registry = registry(&records, 10);
        let abandoned = query(&records);
        let cancel = abandoned.record().cancellation();
        registry.insert(Arc::clone(&abandoned)).unwrap();
        for _ in 0..3 {
            let done = query(&records);
            done.finish();
            registry.insert(done).unwrap();
        }
        std::thread::sleep(Duration::from_millis(60));
        registry.sweep();
        assert_eq!(abandoned.error().unwrap().name(), "ABANDONED_QUERY");
        assert!(cancel.is_cancelled());
        // history_size = 2: the abandoned one plus the newest finished one stay.
        assert_eq!(records.len(), 2);
        assert_eq!(registry.len(), 2, "the index follows the registry");
    }

    /// A client waiting for a page is not gone, however long the page takes;
    /// the timeout counts from the end of its last request.
    #[test]
    fn a_page_request_in_progress_keeps_the_query_alive() {
        let records = records(10);
        let registry = registry(&records, 10);
        let waiting = query(&records);
        registry.insert(Arc::clone(&waiting)).unwrap();
        waiting.enter_fetch();
        std::thread::sleep(Duration::from_millis(60));
        registry.sweep();
        assert!(
            waiting.error().is_none(),
            "abandoned while its page was being built"
        );
        waiting.leave_fetch();
        registry.sweep();
        assert!(waiting.error().is_none(), "the request just ended");
        std::thread::sleep(Duration::from_millis(60));
        registry.sweep();
        assert_eq!(waiting.error().unwrap().name(), "ABANDONED_QUERY");
    }

    #[test]
    fn history_is_bounded_on_insert() {
        let records = records(2);
        let registry = registry(&records, 10);
        for _ in 0..20 {
            let done = query(&records);
            done.finish();
            registry.insert(done).unwrap();
        }
        // history_size = 2, plus the query just registered; no sweep needed.
        assert_eq!(records.len(), 3);
        assert!(registry.len() <= 3);
    }

    struct Buffer(bool);

    impl Release for Buffer {
        fn release(&mut self) {
            self.0 = true;
        }
    }

    #[test]
    fn failing_releases_the_buffers() {
        let records = records(10);
        let q = Query::new(
            records.register(Protocol::Http, "u", None, "SELECT 1"),
            Buffer(false),
        );
        q.fail(&QueryError::new(Code::UserCanceled, "canceled"));
        assert!(q.exec.try_lock().unwrap().0);
        let finished = Query::new(
            records.register(Protocol::Http, "u", None, "SELECT 1"),
            Buffer(false),
        );
        finished.finish();
        finished.fail(&QueryError::new(Code::UserCanceled, "late"));
        assert!(!finished.exec.try_lock().unwrap().0);
    }

    /// Governance fails a record itself (queue timeout); failing the query
    /// afterwards still drops its buffers, and the first failure is kept.
    #[test]
    fn a_record_failed_elsewhere_still_releases() {
        let records = records(10);
        let record = records.register(Protocol::Http, "u", None, "SELECT 1");
        let q = Query::new(Arc::clone(&record), Buffer(false));
        record.fail(Failure::new(
            "EXCEEDED_TIME_LIMIT",
            "INSUFFICIENT_RESOURCES",
            "Query exceeded maximum queued time of 1.00s",
        ));
        assert_eq!(q.error().unwrap().name(), "EXCEEDED_TIME_LIMIT");
        q.fail(&QueryError::new(Code::UserCanceled, "late"));
        assert!(q.exec.try_lock().unwrap().0);
        assert_eq!(q.error().unwrap().name(), "EXCEEDED_TIME_LIMIT");
    }
}
