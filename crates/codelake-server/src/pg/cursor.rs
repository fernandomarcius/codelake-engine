//! Cursors (`DECLARE … CURSOR`, `FETCH`, `CLOSE`) under governance.
//!
//! `datafusion-postgres` keeps a declared cursor's result stream in a portal
//! of the connection; `FETCH` reads it and `CLOSE` drops it. The `DECLARE`
//! is the governed statement: it keeps its record, admission slot and
//! deadline while the cursor is open ([`super::Governed`] governs the portal's
//! stream as it does a query's), so `FETCH` and `CLOSE` take none of their
//! own — a `FETCH` queued behind its own cursor's slot would never run.
//!
//! `FETCH` is answered here rather than by `datafusion-postgres`, which
//! collects the rows it asks for before sending the first (every row left,
//! for `FETCH ALL`): that would end the governed statement, slot freed and
//! time limit over, with the rows in memory outside the pool.
//!
//! A cursor lives in its transaction, as in `PostgreSQL`: the end of the
//! transaction — `COMMIT`, `ROLLBACK`, the end of a simple-protocol message
//! outside a transaction block, an extended-protocol `Sync` outside one —
//! closes the connection's cursors and portals, which ends their statements,
//! frees their slots and returns their memory to the pool. Only a cursor
//! declared `WITH HOLD` outlives the transaction that commits it, until it is
//! closed or read to the end. So a `DECLARE` without `WITH HOLD` outside a
//! transaction block fails (SQLSTATE `25P01`), as in `PostgreSQL`, instead of
//! declaring a cursor its own transaction closes at once. [`Cursors`] keeps
//! what the portals do not: which cursors are held and whether the
//! connection is in a transaction block.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Declare, DeclareType, FetchDirection, Statement, Value};
use datafusion_postgres::hooks::{HookClient, QueryHook};
use datafusion_postgres::pgwire::api::portal::{Format, Portal, PortalExecutionState};
use datafusion_postgres::pgwire::api::results::{FieldInfo, QueryResponse, Response};
use datafusion_postgres::pgwire::api::store::{MemPortalStore, PortalStore};
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore, PgWireConnectionState};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::data::DataRow;
use datafusion_postgres::pgwire::messages::response::TransactionStatus;
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use super::{DfStatement, lock, user_error};

type CursorPortal = Arc<Portal<DfStatement>>;
type Store = MemPortalStore<DfStatement>;

/// The connection's portals, where `datafusion-postgres` keeps its cursors.
fn portals<C>(client: &C) -> Option<&Store>
where
    C: ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    client.portal_store().as_any().downcast_ref()
}

/// What `PostgreSQL` knows of a cursor that its portal does not.
struct Cursor {
    /// Declared `WITH HOLD`: outlives the transaction that commits it.
    hold: bool,
    /// Declared in the transaction in progress, not committed yet.
    pending: AtomicBool,
}

/// A connection's cursors, and where the connection stands in its
/// transaction; kept in its session extensions.
#[derive(Default)]
pub(super) struct Cursors(Mutex<Book>);

#[derive(Default)]
struct Book {
    cursors: HashMap<String, Arc<Cursor>>,
    /// In a transaction block (`BEGIN`), as `pgwire` tracks it when a
    /// message starts and the message's own `BEGIN`, `COMMIT` and
    /// `ROLLBACK` then change it.
    explicit: bool,
    /// The transaction block failed: its end is a rollback.
    aborted: bool,
    /// The simple-protocol message being run holds more than one statement:
    /// `PostgreSQL` runs them in one implicit transaction block, where a
    /// cursor may be declared.
    implicit_block: bool,
}

impl Cursors {
    pub(super) fn of(client: &(impl ClientInfo + ?Sized)) -> Arc<Self> {
        client.session_extensions().get_or_insert_with(Self::default)
    }

    /// A message of `statements` statements starts, the connection's
    /// transaction status as `pgwire` tracks it.
    fn begin_message(&self, status: TransactionStatus, statements: usize) {
        let mut book = lock(&self.0);
        book.explicit = status != TransactionStatus::Idle;
        book.aborted = status == TransactionStatus::Error;
        book.implicit_block = statements > 1;
    }

    /// `BEGIN`: a transaction block starts (a failed one stays failed).
    fn begin(&self) {
        let mut book = lock(&self.0);
        if !book.aborted {
            book.explicit = true;
        }
    }

    /// Checks the cursors `declares` make and records them, before
    /// `datafusion-postgres` declares them.
    fn declare(&self, declares: &[Declare]) -> PgWireResult<()> {
        let mut book = lock(&self.0);
        let mut made = Vec::new();
        for declare in declares {
            if declare.declare_type != Some(DeclareType::Cursor) {
                continue; // `datafusion-postgres` refuses it
            }
            let Some(name) = declare.names.first() else {
                continue;
            };
            let hold = declare.hold == Some(true);
            if !hold && !book.explicit && !book.implicit_block {
                return Err(user_error(
                    "25P01",
                    "DECLARE CURSOR can only be used in transaction blocks",
                ));
            }
            made.push((name.value.clone(), hold));
        }
        for (name, hold) in made {
            let cursor = Cursor {
                hold,
                pending: AtomicBool::new(true),
            };
            book.cursors.insert(name, Arc::new(cursor));
        }
        Ok(())
    }

    /// The transaction in progress ends, committed or rolled back (a failed
    /// one always is): every portal of the connection is closed but the
    /// `WITH HOLD` cursors it commits or an earlier transaction committed.
    fn end_transaction(&self, store: Option<&Store>, commit: bool) {
        let mut book = lock(&self.0);
        let commit = commit && !book.aborted;
        book.explicit = false;
        book.aborted = false;
        book.cursors
            .retain(|_, cursor| cursor.hold && (commit || !cursor.pending.load(Ordering::SeqCst)));
        let Some(store) = store else {
            book.cursors.clear();
            return;
        };
        let kept: Vec<CursorPortal> = book
            .cursors
            .keys()
            .filter_map(|name| store.get_portal(name))
            .collect();
        book.cursors
            .retain(|name, _| kept.iter().any(|portal| portal.name == *name));
        for cursor in book.cursors.values() {
            cursor.pending.store(false, Ordering::SeqCst);
        }
        // Dropping the other portals drops their streams: their statements
        // end, their slots and memory are freed.
        store.clear_portals();
        for portal in kept {
            store.put_portal(portal);
        }
    }
}

/// A simple-protocol message of `statements` statements starts. Outside a
/// transaction block no cursor of an earlier transaction is left open but
/// the held ones (one whose message was dropped by a cancel request, before
/// its end was seen).
pub(super) fn begin_simple<C>(client: &C, statements: usize)
where
    C: ClientInfo + ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    let status = client.transaction_status();
    let cursors = Cursors::of(client);
    if status == TransactionStatus::Idle {
        cursors.begin_message(status, 0);
        cursors.end_transaction(portals(client), false);
    }
    cursors.begin_message(status, statements);
}

/// A simple-protocol message ends, `ok` or failed: outside a transaction
/// block, so does its implicit transaction.
pub(super) fn end_simple<C>(client: &C, ok: bool)
where
    C: ClientInfo + ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    let cursors = Cursors::of(client);
    let implicit = {
        let mut book = lock(&cursors.0);
        book.implicit_block = false;
        !book.explicit
    };
    if implicit {
        cursors.end_transaction(portals(client), ok);
    }
}

/// An extended-protocol `Execute` starts.
pub(super) fn begin_execute(client: &(impl ClientInfo + ?Sized)) {
    Cursors::of(client).begin_message(client.transaction_status(), 1);
}

/// An extended-protocol `Sync`: outside a transaction block it ends the
/// implicit transaction of the messages before it, rolled back when one
/// failed (the connection then waits for this `Sync`).
pub(super) fn sync<C>(client: &C)
where
    C: ClientInfo + ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    if client.transaction_status() == TransactionStatus::Idle {
        let failed = matches!(client.state(), PgWireConnectionState::AwaitingSync);
        let cursors = Cursors::of(client);
        cursors.begin_message(TransactionStatus::Idle, 1);
        cursors.end_transaction(portals(client), !failed);
    }
}

/// Keeps [`Cursors`] up to date with the statements `datafusion-postgres`
/// answers after it (`BEGIN`, `COMMIT`, `ROLLBACK`, `DECLARE`): the
/// statements of a simple-protocol message run one after the other, and a
/// `COMMIT` among them closes the cursors before the next runs.
#[derive(Debug)]
pub(crate) struct CursorHook;

impl CursorHook {
    fn observe(statement: &Statement, client: &dyn HookClient) -> Option<PgWireResult<Response>> {
        let cursors = Cursors::of(client);
        match statement {
            Statement::StartTransaction { .. } => cursors.begin(),
            Statement::Commit { .. } => cursors.end_transaction(Some(client.portal_store()), true),
            Statement::Rollback { .. } => cursors.end_transaction(Some(client.portal_store()), false),
            Statement::Declare { stmts } => return cursors.declare(stmts).err().map(Err),
            _ => {}
        }
        None
    }
}

#[async_trait]
impl QueryHook for CursorHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        Self::observe(statement, client)
    }

    async fn handle_extended_parse_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        Self::observe(statement, client)
    }
}

/// Names of the cursors `statements` declare.
pub(super) fn declared_names<'a>(statements: impl IntoIterator<Item = &'a Statement>) -> Vec<String> {
    statements
        .into_iter()
        .filter_map(|statement| match statement {
            Statement::Declare { stmts } => Some(stmts),
            _ => None,
        })
        .flatten()
        .filter_map(|declare| declare.names.first().map(|name| name.value.clone()))
        .collect()
}

/// The cursors a statement declares, with the portals their names held
/// before it ran, to tell the ones it made.
pub(super) struct Declared {
    names: Vec<String>,
    before: Vec<Option<CursorPortal>>,
}

impl Declared {
    pub(super) fn before<C>(client: &C, names: Vec<String>) -> Self
    where
        C: ClientPortalStore + ?Sized,
        C::PortalStore: PortalStore,
    {
        let store = portals(client);
        let before = names
            .iter()
            .map(|name| store.and_then(|store| store.get_portal(name)))
            .collect();
        Self { names, before }
    }

    /// Names and portals of the cursors the statement made.
    fn made<'a, C>(&'a self, client: &C) -> Vec<(&'a str, CursorPortal)>
    where
        C: ClientPortalStore + ?Sized,
        C::PortalStore: PortalStore,
    {
        let Some(store) = portals(client) else {
            return Vec::new();
        };
        self.names
            .iter()
            .zip(&self.before)
            .filter_map(|(name, before)| {
                let portal = store.get_portal(name)?;
                let new = before.as_ref().is_none_or(|before| !Arc::ptr_eq(before, &portal));
                new.then_some((name.as_str(), portal))
            })
            .collect()
    }

    /// Execution states of the cursors the statement made, locked for their
    /// streams to be governed.
    pub(super) async fn states<C>(&self, client: &C) -> Vec<OwnedMutexGuard<PortalExecutionState>>
    where
        C: ClientPortalStore + ?Sized,
        C::PortalStore: PortalStore,
    {
        let mut states = Vec::new();
        for (_, portal) in self.made(client) {
            states.push(portal.state().lock_owned().await);
        }
        states
    }

    /// `outcome` of the statement; when it failed, the cursors it made are
    /// dropped, so none is left open ungoverned.
    pub(super) fn settle<C, T>(&self, client: &C, outcome: PgWireResult<T>) -> PgWireResult<T>
    where
        C: ClientPortalStore + ?Sized,
        C::PortalStore: PortalStore,
    {
        if outcome.is_err()
            && let Some(store) = portals(client)
        {
            for (name, _) in self.made(client) {
                store.rm_portal(name);
            }
        }
        outcome
    }
}

/// `FETCH` from a cursor, its rows sent as the cursor's stream yields them,
/// in the result `format` its client asked for; `None` when `statement` is
/// not a `FETCH`.
pub(super) async fn fetch<C>(
    client: &C,
    statement: &Statement,
    format: &Format,
) -> Option<PgWireResult<Response>>
where
    C: ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    let Statement::Fetch { name, direction, .. } = statement else {
        return None;
    };
    Some(fetch_from(client, &name.value, direction, format).await)
}

async fn fetch_from<C>(
    client: &C,
    name: &str,
    direction: &FetchDirection,
    format: &Format,
) -> PgWireResult<Response>
where
    C: ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    let max_rows = fetch_count(direction)?;
    let portal = portals(client)
        .and_then(|store| store.get_portal(name))
        .ok_or_else(|| user_error("34000", format!("cursor \"{name}\" does not exist")))?;
    let state = portal.state();
    let (schema, tag) = match &*state.lock().await {
        PortalExecutionState::Initial => return Err(PgWireError::PortalNotStarted),
        PortalExecutionState::Finished => (Arc::new(Vec::new()), "SELECT".to_owned()),
        PortalExecutionState::Suspended(response) => {
            (response.row_schema(), response.command_tag().to_owned())
        }
    };
    text_only(format, schema.len())?;
    let mut response = QueryResponse::new(schema, cursor_rows(state, max_rows));
    response.set_command_tag(&tag);
    Ok(Response::Query(response))
}

/// The columns a `FETCH` answers with, for `Describe`: its cursor's, as
/// `PostgreSQL` describes them; none when the cursor does not exist (the
/// `FETCH` then fails) or was read to the end. `format` is the one the
/// client asked results in (text for a statement). `None` when `statement`
/// is not a `FETCH`.
pub(super) async fn fetch_fields<C>(
    client: &C,
    statement: &Statement,
    format: &Format,
) -> Option<PgWireResult<Vec<FieldInfo>>>
where
    C: ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    let Statement::Fetch { name, .. } = statement else {
        return None;
    };
    let Some(portal) = portals(client).and_then(|store| store.get_portal(&name.value)) else {
        return Some(Ok(Vec::new()));
    };
    let fields = match &*portal.state().lock().await {
        PortalExecutionState::Suspended(response) => response.row_schema().to_vec(),
        PortalExecutionState::Initial | PortalExecutionState::Finished => Vec::new(),
    };
    Some(text_only(format, fields.len()).map(|()| fields))
}

/// Refuses results in binary: a cursor's rows were encoded as text when it
/// was declared, and a client decoding them as binary would read wrong
/// values.
fn text_only(format: &Format, columns: usize) -> PgWireResult<()> {
    let binary = match format {
        Format::UnifiedText => false,
        Format::UnifiedBinary => columns > 0,
        Format::Individual(codes) => (0..codes.len()).any(|column| format.is_binary(column)),
    };
    if binary {
        return Err(user_error(
            "0A000",
            "FETCH returns a cursor's rows in text format only",
        ));
    }
    Ok(())
}

/// Rows a `FETCH` asks for, `0` for all that are left. As
/// `datafusion-postgres` reads it: a cursor only scans forward.
fn fetch_count(direction: &FetchDirection) -> PgWireResult<usize> {
    match direction {
        FetchDirection::Next | FetchDirection::Forward { limit: None } => Ok(1),
        FetchDirection::Forward { limit: Some(count) } | FetchDirection::Count { limit: count } => {
            Ok(match &count.value {
                Value::Number(count, _) => count.parse().unwrap_or(0),
                _ => 0,
            })
        }
        FetchDirection::ForwardAll | FetchDirection::All => Ok(0),
        _ => Err(user_error("42000", "cursor can only scan forward")),
    }
}

/// Up to `max_rows` rows (all when `0`) of a cursor, read one at a time
/// from its stream; the cursor is finished when its stream is.
fn cursor_rows(
    state: Arc<AsyncMutex<PortalExecutionState>>,
    max_rows: usize,
) -> impl Stream<Item = PgWireResult<DataRow>> + Send + 'static {
    futures::stream::unfold(Some((state, 0_usize)), move |cursor| async move {
        let (state, sent) = cursor?;
        if max_rows != 0 && sent >= max_rows {
            return None;
        }
        let mut guard = state.lock().await;
        let next = match &mut *guard {
            PortalExecutionState::Suspended(response) => response.data_rows().next().await,
            PortalExecutionState::Finished => None,
            PortalExecutionState::Initial => Some(Err(PgWireError::PortalNotStarted)),
        };
        match next {
            None => {
                *guard = PortalExecutionState::Finished;
                None
            }
            Some(Ok(row)) => {
                drop(guard);
                Some((Ok(row), Some((state, sent + 1))))
            }
            // The error is the last item: the governed stream failed.
            Some(Err(error)) => Some((Err(error), None)),
        }
    })
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion_postgres::pgwire::api::stmt::StoredStatement;

    use super::*;

    fn parse(sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap()
    }

    #[test]
    fn names_of_declared_cursors() {
        let statements =
            parse("DECLARE c1 CURSOR FOR SELECT 1; SELECT 2; DECLARE \"Big\" CURSOR FOR SELECT 3");
        assert_eq!(declared_names(&statements), ["c1", "Big"]);
        assert!(declared_names(&parse("SELECT 1; FETCH 1 FROM c1")).is_empty());
    }

    fn declares(sql: &str) -> Vec<Declare> {
        match parse(sql).remove(0) {
            Statement::Declare { stmts } => stmts,
            other => panic!("{other:?}"),
        }
    }

    /// A store holding portals named `names`, as cursors or protocol
    /// portals.
    fn store_with(names: &[&str]) -> Store {
        let store = Store::new();
        for name in names {
            let statement = StoredStatement::new((*name).to_owned(), (String::new(), None), Vec::new());
            let portal = Portal::new_cursor((*name).to_owned(), Arc::new(statement));
            store.put_portal(Arc::new(portal));
        }
        store
    }

    fn open(store: &Store, names: &[&str]) -> Vec<String> {
        names
            .iter()
            .filter(|name| store.get_portal(name).is_some())
            .map(|name| (*name).to_owned())
            .collect()
    }

    fn code(error: PgWireError) -> String {
        match error {
            PgWireError::UserError(info) => info.code,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn cursors_are_declared_in_transaction_blocks() {
        let cursors = Cursors::default();
        cursors.begin_message(TransactionStatus::Idle, 1);
        let refused = cursors.declare(&declares("DECLARE c CURSOR FOR SELECT 1"));
        assert_eq!(refused.map_err(code), Err("25P01".to_owned()));
        assert!(
            cursors
                .declare(&declares("DECLARE c CURSOR WITHOUT HOLD FOR SELECT 1"))
                .is_err()
        );
        cursors
            .declare(&declares("DECLARE h CURSOR WITH HOLD FOR SELECT 1"))
            .unwrap();
        // A message of several statements is an implicit transaction block.
        cursors.begin_message(TransactionStatus::Idle, 2);
        cursors
            .declare(&declares("DECLARE m CURSOR FOR SELECT 1"))
            .unwrap();
        cursors.begin_message(TransactionStatus::Idle, 1);
        cursors.begin();
        cursors
            .declare(&declares("DECLARE b CURSOR FOR SELECT 1"))
            .unwrap();
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors
            .declare(&declares("DECLARE t CURSOR FOR SELECT 1"))
            .unwrap();
        let mut names: Vec<String> = lock(&cursors.0).cursors.keys().cloned().collect();
        names.sort();
        assert_eq!(names, ["b", "h", "m", "t"]);
    }

    #[test]
    fn transaction_ends_close_every_portal_but_committed_held_cursors() {
        let all = ["c", "h", "old", "p", ""];
        let cursors = Cursors::default();
        let store = store_with(&all);
        cursors.begin_message(TransactionStatus::Idle, 1);
        cursors
            .declare(&declares("DECLARE old CURSOR WITH HOLD FOR SELECT 1"))
            .unwrap();
        cursors.end_transaction(Some(&store), true);
        assert_eq!(
            open(&store, &all),
            ["old"],
            "a committed transaction keeps its held cursor"
        );

        let store = store_with(&all);
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors
            .declare(&declares("DECLARE c CURSOR FOR SELECT 1"))
            .unwrap();
        cursors
            .declare(&declares("DECLARE h CURSOR WITH HOLD FOR SELECT 1"))
            .unwrap();
        cursors.end_transaction(Some(&store), false);
        assert_eq!(
            open(&store, &all),
            ["old"],
            "a rollback closes the held cursors it declared, not those committed before"
        );

        let store = store_with(&all);
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors
            .declare(&declares("DECLARE h CURSOR WITH HOLD FOR SELECT 1"))
            .unwrap();
        cursors.end_transaction(Some(&store), true);
        assert_eq!(open(&store, &all), ["h", "old"]);
        // Committed: a later rollback keeps it.
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors.end_transaction(Some(&store), false);
        assert_eq!(open(&store, &all), ["h", "old"]);

        // The COMMIT of a failed transaction is a rollback.
        let store = store_with(&all);
        cursors.begin_message(TransactionStatus::Error, 1);
        cursors
            .declare(&declares("DECLARE c CURSOR WITH HOLD FOR SELECT 1"))
            .unwrap();
        cursors.end_transaction(Some(&store), true);
        assert_eq!(open(&store, &all), ["h", "old"]);
        // A cursor whose portal is gone (closed) is forgotten.
        let store = store_with(&["h"]);
        cursors.end_transaction(Some(&store), true);
        assert_eq!(lock(&cursors.0).cursors.len(), 1);
    }

    #[test]
    fn fetch_counts() {
        let count = |sql: &str| match &parse(sql)[0] {
            Statement::Fetch { direction, .. } => fetch_count(direction).map_err(|e| e.to_string()),
            other => panic!("{other:?}"),
        };
        assert_eq!(count("FETCH NEXT FROM c"), Ok(1));
        assert_eq!(count("FETCH 7 FROM c"), Ok(7));
        assert_eq!(count("FETCH FORWARD 2000 FROM c"), Ok(2000));
        assert_eq!(count("FETCH ALL FROM c"), Ok(0));
        assert_eq!(count("FETCH FORWARD ALL FROM c"), Ok(0));
        assert!(count("FETCH PRIOR FROM c").is_err());
        assert!(count("FETCH BACKWARD 2 FROM c").is_err());
        assert!(count("FETCH ABSOLUTE 3 FROM c").is_err());
    }

    #[test]
    fn cursor_rows_are_text_only() {
        let refused = |format: &Format, columns| text_only(format, columns).is_err();
        assert!(!refused(&Format::UnifiedText, 2));
        assert!(refused(&Format::UnifiedBinary, 2));
        assert!(!refused(&Format::UnifiedBinary, 0), "no column to send in binary");
        assert!(!refused(&Format::Individual(vec![0, 0]), 2));
        assert!(refused(&Format::Individual(vec![0, 1]), 2));
    }

    fn row(value: u8) -> DataRow {
        DataRow::new((&[value][..]).into(), 1)
    }

    fn suspended(rows: Vec<PgWireResult<DataRow>>) -> Arc<AsyncMutex<PortalExecutionState>> {
        let response = QueryResponse::new(Arc::new(Vec::new()), futures::stream::iter(rows));
        Arc::new(AsyncMutex::new(PortalExecutionState::Suspended(response)))
    }

    async fn values(
        state: &Arc<AsyncMutex<PortalExecutionState>>,
        max_rows: usize,
    ) -> Vec<Result<Vec<u8>, String>> {
        cursor_rows(Arc::clone(state), max_rows)
            .map(|row| row.map(|row| row.data.to_vec()).map_err(|e| e.to_string()))
            .collect()
            .await
    }

    #[tokio::test]
    async fn fetches_read_on_where_the_last_stopped() {
        let state = suspended((1..=5).map(|value| Ok(row(value))).collect());
        assert_eq!(values(&state, 2).await, [Ok(vec![1]), Ok(vec![2])]);
        assert!(matches!(*state.lock().await, PortalExecutionState::Suspended(_)));
        assert_eq!(values(&state, 0).await, [Ok(vec![3]), Ok(vec![4]), Ok(vec![5])]);
        assert!(matches!(*state.lock().await, PortalExecutionState::Finished));
        assert!(
            values(&state, 0).await.is_empty(),
            "a finished cursor has no rows"
        );
    }

    #[tokio::test]
    async fn a_failed_stream_ends_the_fetch() {
        let state = suspended(vec![Ok(row(1)), Err(user_error("57014", "limit")), Ok(row(2))]);
        let fetched = values(&state, 0).await;
        assert_eq!(fetched.len(), 2, "{fetched:?}");
        assert_eq!(fetched[0], Ok(vec![1]));
        assert!(
            fetched[1].as_ref().is_err_and(|e| e.contains("limit")),
            "{fetched:?}"
        );
    }
}
