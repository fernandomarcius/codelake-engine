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
//! what the portals do not: which cursors are held, the row each stands on
//! (`FETCH 0` reads it again, as in `PostgreSQL`) and whether the connection
//! is in a transaction block.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use codelake_core::governance::{Failure, QueryRecord};
use datafusion::common::ParamValues;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    CloseCursor, Declare, DeclareType, DiscardObject, FetchDirection, FetchPosition, Ident, Statement, Value,
    ValueWithSpan,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::tokenizer::{Span, Token, Tokenizer, Word};
use datafusion_postgres::arrow_pg::datatypes::df::encode_dataframe;
use datafusion_postgres::hooks::{HookClient, QueryHook};
use datafusion_postgres::pgwire::api::portal::{Format, Portal, PortalExecutionState};
use datafusion_postgres::pgwire::api::results::{FieldInfo, QueryResponse, Response, Tag};
use datafusion_postgres::pgwire::api::stmt::StoredStatement;
use datafusion_postgres::pgwire::api::store::{MemPortalStore, PortalStore};
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore, PgWireConnectionState};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::data::DataRow;
use datafusion_postgres::pgwire::messages::response::TransactionStatus;
use datafusion_postgres::pgwire::types::format::FormatOptions;
use futures::stream::BoxStream;
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
    /// Not declared `NO SCROLL`: `FETCH 0` may read the current row again.
    scroll: bool,
    /// Declared in the transaction in progress, not committed yet.
    pending: AtomicBool,
    /// The row the cursor stands on: the last one a `FETCH` returned, unless
    /// a later `FETCH` moved past it or found the end.
    current: Mutex<Option<DataRow>>,
    /// Its columns, for a `FETCH` once its stream is finished.
    columns: Mutex<Option<Arc<Vec<FieldInfo>>>>,
    /// Its stream failed (canceled, killed, past its time limit, an error):
    /// it cannot be read any more, as `PostgreSQL` marks a failed portal.
    failed: AtomicBool,
    /// The governed statement that declared it.
    record: Mutex<Option<Arc<QueryRecord>>>,
}

impl Cursor {
    fn new(hold: bool, scroll: bool) -> Self {
        Self {
            hold,
            scroll,
            pending: AtomicBool::new(true),
            current: Mutex::new(None),
            columns: Mutex::new(None),
            failed: AtomicBool::new(false),
            record: Mutex::new(None),
        }
    }

    fn stand_on(&self, row: Option<DataRow>) {
        *lock(&self.current) = row;
    }
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

    /// Checks the cursors `declares` make, as `PostgreSQL` does, and the
    /// engine can answer, and records them; planned and started in `store`
    /// by [`declare`].
    fn declare(
        &self,
        declares: &[Declare],
        store: Option<&Store>,
    ) -> PgWireResult<Vec<(String, Arc<Cursor>)>> {
        let mut book = lock(&self.0);
        let made: Vec<(String, Arc<Cursor>)> = book
            .checked(declares, store)?
            .into_iter()
            .map(|(name, cursor)| (name, Arc::new(cursor)))
            .collect();
        for (name, cursor) in &made {
            book.cursors.insert(name.clone(), Arc::clone(cursor));
        }
        Ok(made)
    }

    /// `CLOSE`: `PostgreSQL`'s answers, a missing cursor included (34000,
    /// where `datafusion-postgres` answers nothing).
    fn close(&self, store: &Store, cursor: &CloseCursor) -> PgWireResult<Response> {
        let mut book = lock(&self.0);
        match cursor {
            CloseCursor::All => {
                store.clear_portals();
                book.cursors.clear();
                Ok(Response::Execution(Tag::new("CLOSE CURSOR ALL")))
            }
            CloseCursor::Specific { name } => {
                let name = cursor_name(name);
                if store.get_portal(&name).is_none() {
                    return Err(user_error("34000", format!("cursor \"{name}\" does not exist")));
                }
                store.rm_portal(&name);
                book.cursors.remove(&name);
                Ok(Response::Execution(Tag::new("CLOSE CURSOR")))
            }
        }
    }

    /// `DISCARD`: `ALL` closes every cursor and portal of the connection,
    /// held ones too, and resets its `statement_timeout`, outside a
    /// transaction block only (`25001`), as `PostgreSQL` does; the engine
    /// keeps no plans, sequences or temporary tables of a connection, so
    /// the others have nothing to discard.
    fn discard(&self, client: &mut dyn HookClient, object: DiscardObject) -> PgWireResult<Response> {
        let tag = match object {
            DiscardObject::ALL => "DISCARD ALL",
            DiscardObject::PLANS => "DISCARD PLANS",
            DiscardObject::SEQUENCES => "DISCARD SEQUENCES",
            DiscardObject::TEMP => "DISCARD TEMP",
        };
        if object == DiscardObject::ALL {
            let mut book = lock(&self.0);
            if book.explicit || book.implicit_block {
                return Err(user_error(
                    "25001",
                    "DISCARD ALL cannot run inside a transaction block",
                ));
            }
            client.portal_store().clear_portals();
            book.cursors.clear();
            drop(book);
            client.metadata_mut().remove(super::STATEMENT_TIMEOUT);
        }
        Ok(Response::Execution(Tag::new(tag)))
    }

    /// The cursor `name` (a portal of the extended protocol has one too).
    fn cursor(&self, name: &str) -> Arc<Cursor> {
        let mut book = lock(&self.0);
        let cursor = book
            .cursors
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(Cursor::new(false, true)));
        Arc::clone(cursor)
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

impl Book {
    /// The cursors `declares` make in `store`, refused when `PostgreSQL`
    /// refuses them or the engine could not answer them right.
    fn checked(&self, declares: &[Declare], store: Option<&Store>) -> PgWireResult<Vec<(String, Cursor)>> {
        let mut made = Vec::new();
        for declare in declares {
            if declare.declare_type != Some(DeclareType::Cursor) {
                return Err(user_error(
                    "42601",
                    format!("unsupported DECLARE type: {:?}", declare.declare_type),
                ));
            }
            let Some(name) = declare.names.first().map(cursor_name) else {
                return Err(user_error("42601", "cursor name is required"));
            };
            // Its rows are encoded as text: a client decoding them as binary
            // would read wrong values.
            if declare.binary == Some(true) {
                return Err(user_error(
                    "0A000",
                    "BINARY cursors are not supported: a cursor's rows are sent as text",
                ));
            }
            let hold = declare.hold == Some(true);
            if !hold && !self.explicit && !self.implicit_block {
                return Err(user_error(
                    "25P01",
                    "DECLARE CURSOR can only be used in transaction blocks",
                ));
            }
            // Declared again, it would replace the cursor, dropping its rows.
            if store.is_some_and(|store| store.get_portal(&name).is_some()) {
                return Err(user_error("42P03", format!("cursor \"{name}\" already exists")));
            }
            made.push((name, Cursor::new(hold, declare.scroll != Some(false))));
        }
        Ok(made)
    }
}

/// Refuses a `DECLARE` sent alone, before it takes an admission slot, when
/// its cursor would be refused (`25P01`, `42P03`, `0A000`): it would
/// otherwise wait for a slot the connection's open cursor may hold. (The
/// statements of a longer message are checked as they run: one may close
/// or declare a cursor another names.)
pub(super) fn check_declare<C>(client: &C, statement: &Statement) -> PgWireResult<()>
where
    C: ClientInfo + ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    let Statement::Declare { stmts } = statement else {
        return Ok(());
    };
    let cursors = Cursors::of(client);
    let book = lock(&cursors.0);
    book.checked(stmts, portals(client)).map(|_| ())
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
    async fn observe(
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let cursors = Cursors::of(client);
        match statement {
            Statement::Discard { object_type } => return Some(cursors.discard(client, *object_type)),
            Statement::Close { cursor } => return Some(cursors.close(client.portal_store(), cursor)),
            Statement::Fetch { into: Some(_), .. } => return Some(Err(into_error())),
            // A `FETCH` among other statements: its rows are read before the
            // next statement runs, held in the pool (a single `FETCH` streams
            // them, [`fetch`]).
            Statement::Fetch { name, direction, .. } => {
                let store = Some(client.portal_store());
                let fetch = open(
                    &cursors,
                    store,
                    &cursor_name(name),
                    direction,
                    &Format::UnifiedText,
                )
                .await;
                let pool = &session_context.runtime_env().memory_pool;
                return Some(match fetch {
                    Ok(fetch) => fetch.collected(pool).await,
                    Err(error) => Err(error),
                });
            }
            Statement::StartTransaction { .. } => cursors.begin(),
            Statement::Commit { .. } => cursors.end_transaction(Some(client.portal_store()), true),
            Statement::Rollback { .. } => cursors.end_transaction(Some(client.portal_store()), false),
            Statement::Declare { stmts } => {
                return Some(declare(&cursors, stmts, session_context, client).await);
            }
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
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        Self::observe(statement, session_context, client).await
    }

    /// `DISCARD`, which `DataFusion` does not plan, is answered here when
    /// it runs.
    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        matches!(statement, Statement::Discard { .. }).then(|| Ok(empty_plan()))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        Self::observe(statement, session_context, client).await
    }
}

/// A cursor's name as `PostgreSQL` reads it: folded to lower case unless
/// quoted (`DECLARE Big` declares `big`).
fn cursor_name(name: &Ident) -> String {
    if name.quote_style.is_some() {
        name.value.clone()
    } else {
        name.value.to_ascii_lowercase()
    }
}

/// `DECLARE … CURSOR`: each cursor checked ([`Cursors::declare`]), its
/// query planned as a query of the session is (operands checked,
/// [`crate::hooks::plan_checked`]) and its stream kept in a portal of the
/// connection, rows encoded in text as a query's are, for `FETCH` to read.
async fn declare(
    cursors: &Cursors,
    declares: &[Declare],
    session_context: &SessionContext,
    client: &mut dyn HookClient,
) -> PgWireResult<Response> {
    let made = cursors.declare(declares, Some(client.portal_store()))?;
    let options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
    for ((name, _), declare) in made.iter().zip(declares) {
        let outcome = async {
            let query = declare
                .for_query
                .as_ref()
                .ok_or_else(|| user_error("42601", "DECLARE CURSOR requires a FOR query"))?
                .to_string();
            let df = crate::hooks::plan_checked(session_context, &query).await?;
            let rows = encode_dataframe(df, &Format::UnifiedText, Some(Arc::clone(&options))).await?;
            let statement = StoredStatement::new(name.clone(), (query, None), Vec::new());
            let portal = Portal::new_cursor(name.clone(), Arc::new(statement));
            portal.start(rows).await;
            client.portal_store().put_portal(Arc::new(portal));
            Ok(())
        }
        .await;
        if let Err(error) = outcome {
            lock(&cursors.0).cursors.remove(name);
            return Err(error);
        }
    }
    Ok(Response::Execution(Tag::new("DECLARE CURSOR")))
}

/// The plan of a statement answered without one.
pub(super) fn empty_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(datafusion::common::DFSchema::empty()),
    })
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
        .filter_map(|declare| declare.names.first().map(cursor_name))
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

    /// Ties the cursors the statement made to its `record`, which a failure
    /// of theirs fails.
    pub(super) fn attach<C>(&self, client: &C, record: &Arc<QueryRecord>)
    where
        C: ClientInfo + ClientPortalStore + ?Sized,
        C::PortalStore: PortalStore,
    {
        let cursors = Cursors::of(client);
        for (name, _) in self.made(client) {
            *lock(&cursors.cursor(name).record) = Some(Arc::clone(record));
        }
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
    C: ClientInfo + ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    let Statement::Fetch {
        name,
        direction,
        into,
        ..
    } = statement
    else {
        return None;
    };
    if into.is_some() {
        return Some(Err(into_error()));
    }
    let cursors = Cursors::of(client);
    let fetch = open(&cursors, portals(client), &cursor_name(name), direction, format).await;
    Some(fetch.map(Fetch::streamed))
}

/// `FETCH … INTO`, which `PostgreSQL`'s SQL does not have (`PL/pgSQL` does).
fn into_error() -> PgWireError {
    user_error("42601", "syntax error at or near \"INTO\"")
}

/// How far a `FETCH` moves a cursor that only scans forward.
#[derive(Debug, PartialEq, Eq)]
enum Count {
    /// Up to this many rows (at least one).
    Rows(usize),
    /// Every row left.
    All,
    /// None: the current row again (`FETCH 0`, `FETCH RELATIVE 0`).
    Current,
}

/// A `FETCH` about to read: its cursor's columns and where its rows come
/// from.
struct Fetch {
    columns: Arc<Vec<FieldInfo>>,
    rows: FetchRows,
}

/// A `FETCH`'s command tag (`FETCH n`), as `PostgreSQL`'s.
const FETCH_TAG: &str = "FETCH";

enum FetchRows {
    /// The row the cursor stands on, when it stands on one.
    Current(Option<DataRow>),
    /// Rows from the cursor's stream, `max` at most.
    Read {
        state: Arc<AsyncMutex<PortalExecutionState>>,
        max: Option<usize>,
        cursor: Arc<Cursor>,
    },
}

impl Fetch {
    fn rows(self) -> (Arc<Vec<FieldInfo>>, BoxStream<'static, PgWireResult<DataRow>>) {
        let rows = match self.rows {
            FetchRows::Current(row) => futures::stream::iter(row.map(Ok)).boxed(),
            FetchRows::Read { state, max, cursor } => cursor_rows(state, max, cursor).boxed(),
        };
        (self.columns, rows)
    }

    /// The rows, sent as the cursor's stream yields them.
    fn streamed(self) -> Response {
        let (columns, rows) = self.rows();
        let mut response = QueryResponse::new(columns, rows);
        response.set_command_tag(FETCH_TAG);
        Response::Query(response)
    }

    /// The rows, read before the response is made (a `FETCH` among other
    /// statements of a message: the next one runs before they are sent).
    /// Until they are sent they are held in `pool`, the engine's memory
    /// pool; rows that do not fit fail the `FETCH` with `53200`
    /// (`out_of_memory`) rather than be held outside the pool, and its
    /// cursor cannot be read again (the rows read are gone).
    async fn collected(self, pool: &Arc<dyn MemoryPool>) -> PgWireResult<Response> {
        let cursor = match &self.rows {
            FetchRows::Read { cursor, .. } => Some(Arc::clone(cursor)),
            FetchRows::Current(_) => None,
        };
        let (columns, mut rows) = self.rows();
        let reservation = MemoryConsumer::new("PostgreSQL FETCH among other statements").register(pool);
        let mut held: Vec<DataRow> = Vec::new();
        let mut bytes = 0_usize;
        while let Some(row) = rows.next().await {
            let row = row?;
            bytes += row.data.capacity();
            // Room for the row reserved before the vector grows to hold it.
            let slots = if held.len() == held.capacity() {
                (held.capacity() * 2).max(8)
            } else {
                held.capacity()
            };
            let needed = bytes + slots * std::mem::size_of::<DataRow>();
            if reservation.try_resize(needed).is_err() {
                let error = out_of_memory(held.len());
                if let Some(cursor) = cursor {
                    cursor.failed.store(true, Ordering::SeqCst);
                    // Its statement fails too: its stream is dropped, its
                    // slot and memory given back.
                    if let (Some(record), PgWireError::UserError(info)) =
                        (lock(&cursor.record).as_ref(), &error)
                    {
                        record.fail(Failure::new(
                            "EXCEEDED_LOCAL_MEMORY_LIMIT",
                            "INSUFFICIENT_RESOURCES",
                            info.message.clone(),
                        ));
                    }
                }
                return Err(error);
            }
            held.reserve_exact(slots - held.len());
            held.push(row);
        }
        let rows = HeldRows {
            rows: held.into_iter(),
            _reservation: reservation,
        };
        let mut response = QueryResponse::new(columns, rows);
        response.set_command_tag(FETCH_TAG);
        Ok(Response::Query(response))
    }
}

/// Rows a `FETCH` read before its response, with their place in the pool,
/// given back once they are sent (or the response dropped).
struct HeldRows {
    rows: std::vec::IntoIter<DataRow>,
    _reservation: MemoryReservation,
}

impl Stream for HeldRows {
    type Item = PgWireResult<DataRow>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.get_mut().rows.next().map(Ok))
    }
}

/// The error of a `FETCH` among other statements whose rows do not fit in
/// the memory pool, after `rows` did.
fn out_of_memory(rows: usize) -> PgWireError {
    user_error(
        "53200",
        format!(
            "out of memory: a FETCH among other statements holds its rows until they are sent, \
             and the memory pool is full after {rows} rows; send the FETCH alone to stream its rows"
        ),
    )
}

/// Prepares a `FETCH` of the cursor `name`: checked (count, cursor, result
/// format) before any row is read.
async fn open(
    cursors: &Cursors,
    store: Option<&Store>,
    name: &str,
    direction: &FetchDirection,
    format: &Format,
) -> PgWireResult<Fetch> {
    let count = fetch_count(direction)?;
    let portal = store
        .and_then(|store| store.get_portal(name))
        .ok_or_else(|| user_error("34000", format!("cursor \"{name}\" does not exist")))?;
    let cursor = cursors.cursor(name);
    if cursor.failed.load(Ordering::SeqCst) {
        return Err(user_error("55000", format!("portal \"{name}\" cannot be run")));
    }
    let state = portal.state();
    let columns = match &*state.lock().await {
        PortalExecutionState::Initial => return Err(PgWireError::PortalNotStarted),
        PortalExecutionState::Finished => lock(&cursor.columns).clone().unwrap_or_default(),
        PortalExecutionState::Suspended(response) => {
            let columns = response.row_schema();
            *lock(&cursor.columns) = Some(Arc::clone(&columns));
            columns
        }
    };
    text_only(format, columns.len())?;
    let rows = match count {
        Count::Current => {
            let current = lock(&cursor.current).clone();
            if current.is_some() && !cursor.scroll {
                return Err(user_error("55000", "cursor can only scan forward"));
            }
            FetchRows::Current(current)
        }
        Count::Rows(rows) => {
            cursor.stand_on(None);
            FetchRows::Read {
                state,
                max: Some(rows),
                cursor,
            }
        }
        Count::All => {
            cursor.stand_on(None);
            FetchRows::Read {
                state,
                max: None,
                cursor,
            }
        }
    };
    Ok(Fetch { columns, rows })
}

/// The columns a `FETCH` answers with, for `Describe`: its cursor's, as
/// `PostgreSQL` describes them; none when the cursor does not exist (the
/// `FETCH` then fails). `format` is the one the client asked results in
/// (text for a statement). `None` when `statement` is not a `FETCH`.
pub(super) async fn fetch_fields<C>(
    client: &C,
    statement: &Statement,
    format: &Format,
) -> Option<PgWireResult<Vec<FieldInfo>>>
where
    C: ClientInfo + ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    let Statement::Fetch { name, .. } = statement else {
        return None;
    };
    let name = cursor_name(name);
    let Some(portal) = portals(client).and_then(|store| store.get_portal(&name)) else {
        return Some(Ok(Vec::new()));
    };
    let fields = match &*portal.state().lock().await {
        PortalExecutionState::Suspended(response) => response.row_schema().to_vec(),
        PortalExecutionState::Finished => Cursors::of(client)
            .cursor(&name)
            .columns
            .lock()
            .map(|columns| columns.as_deref().cloned().unwrap_or_default())
            .unwrap_or_default(),
        PortalExecutionState::Initial => Vec::new(),
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

/// How far a `FETCH` moves its cursor, as `PostgreSQL` reads it; a cursor
/// here only scans forward (`55000` for a move backward, as `PostgreSQL`
/// answers for a `NO SCROLL` cursor). A count that is not a whole number
/// is an error, never "every row".
fn fetch_count(direction: &FetchDirection) -> PgWireResult<Count> {
    let forward = |rows: i64| match rows {
        0 => Ok(Count::Current),
        rows if rows > 0 => usize::try_from(rows).map(Count::Rows).map_err(|_| backward()),
        _ => Err(backward()),
    };
    match direction {
        FetchDirection::Next | FetchDirection::Forward { limit: None } => Ok(Count::Rows(1)),
        FetchDirection::Forward { limit: Some(count) } | FetchDirection::Count { limit: count } => {
            forward(number(count)?)
        }
        FetchDirection::Backward { limit: Some(count) } => forward(-number(count)?),
        FetchDirection::ForwardAll | FetchDirection::All => Ok(Count::All),
        FetchDirection::Relative { limit } => match number(limit)? {
            0 => Ok(Count::Current),
            rows if rows < 0 => Err(backward()),
            _ => Err(unsupported("FETCH RELATIVE")),
        },
        FetchDirection::Absolute { .. } => Err(unsupported("FETCH ABSOLUTE")),
        FetchDirection::First => Err(unsupported("FETCH FIRST")),
        FetchDirection::Last => Err(unsupported("FETCH LAST")),
        FetchDirection::Prior | FetchDirection::Backward { limit: None } | FetchDirection::BackwardAll => {
            Err(backward())
        }
    }
}

fn backward() -> PgWireError {
    user_error("55000", "cursor can only scan forward")
}

fn unsupported(what: &str) -> PgWireError {
    user_error(
        "0A000",
        format!("{what} is not supported: cursors only scan forward"),
    )
}

/// A `FETCH` count: a whole number of rows, negative for backward.
fn number(count: &ValueWithSpan) -> PgWireResult<i64> {
    let invalid = || user_error("42601", format!("invalid FETCH count: {count}"));
    match &count.value {
        Value::Number(text, _) => text.parse().map_err(|_| invalid()),
        _ => Err(invalid()),
    }
}

/// A message the SQL parser does not read (`error`) that holds a `FETCH` or
/// a `MOVE`, answered at once: sent on, it would wait in the queue for an
/// admission slot its own cursor may hold, then time out. A single `FETCH`
/// is read here as `PostgreSQL` reads it (a negative count, no count, no
/// `FROM`: `FETCH -1 FROM c`, `FETCH FROM c`, `FETCH c`); `MOVE` is not
/// supported. `None` when the message holds neither.
pub(super) fn unparsed(sql: &str, error: &impl std::fmt::Display) -> Option<PgWireResult<Statement>> {
    let tokens: Vec<Token> = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .ok()?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    let mut tokens = tokens.as_slice();
    while let [rest @ .., Token::SemiColon] = tokens {
        tokens = rest;
    }
    let statements: Vec<&[Token]> = tokens.split(|token| *token == Token::SemiColon).collect();
    let starts =
        |statement: &[Token], keyword: &str| statement.first().is_some_and(|token| is(token, keyword));
    if statements.iter().any(|statement| starts(statement, "MOVE")) {
        return Some(Err(unsupported("MOVE")));
    }
    match statements.as_slice() {
        [statement] if starts(statement, "FETCH") => Some(read_fetch(&statement[1..])),
        _ if statements.iter().any(|statement| starts(statement, "FETCH")) => {
            Some(Err(user_error("42601", format!("syntax error: {error}"))))
        }
        _ => None,
    }
}

/// `sql` with its `ABORT` statements (`PostgreSQL`'s `ROLLBACK`, which the
/// SQL parser does not read) written as `ROLLBACK`, so they end their
/// transaction as it does; `None` when it has none.
pub(super) fn abort_as_rollback(sql: &str) -> Option<String> {
    if !sql.to_ascii_uppercase().contains("ABORT") {
        return None;
    }
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    let mut starts = Vec::new();
    let mut first = true;
    for token in &tokens {
        match &token.token {
            Token::Whitespace(_) => {}
            Token::SemiColon => first = true,
            other => {
                if first && is(other, "ABORT") {
                    starts.push(offset(sql, token.span.start.line, token.span.start.column)?);
                }
                first = false;
            }
        }
    }
    if starts.is_empty() {
        return None;
    }
    let mut rewritten = sql.to_owned();
    for start in starts.into_iter().rev() {
        rewritten.replace_range(start..start + "ABORT".len(), "ROLLBACK");
    }
    Some(rewritten)
}

/// Byte offset in `sql` of the tokenizer's `line` and `column` (from 1, in
/// characters).
fn offset(sql: &str, line: u64, column: u64) -> Option<usize> {
    let (mut at_line, mut at_column) = (1, 1);
    for (index, character) in sql.char_indices() {
        if (at_line, at_column) == (line, column) {
            return Some(index);
        }
        if character == '\n' {
            at_line += 1;
            at_column = 1;
        } else {
            at_column += 1;
        }
    }
    None
}

/// Whether `token` is the (unquoted) keyword `keyword`.
fn is(token: &Token, keyword: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
}

/// A count after a `FETCH` direction, `-` or `+` signed.
fn signed_count(tokens: &[Token]) -> Option<(ValueWithSpan, &[Token])> {
    let (sign, tokens) = match tokens {
        [Token::Minus, rest @ ..] => ("-", rest),
        [Token::Plus, rest @ ..] => ("", rest),
        _ => ("", tokens),
    };
    match tokens {
        [Token::Number(number, _), rest @ ..] => Some((
            Value::Number(format!("{sign}{number}"), false).with_empty_span(),
            rest,
        )),
        _ => None,
    }
}

/// A `FETCH` after its keyword, as `PostgreSQL`'s grammar has it:
/// `[direction] [FROM | IN] cursor_name`.
fn read_fetch(tokens: &[Token]) -> PgWireResult<Statement> {
    let syntax = || user_error("42601", "syntax error in FETCH");
    let (direction, rest) = match tokens {
        [first, rest @ ..] if is(first, "NEXT") => (FetchDirection::Next, rest),
        [first, rest @ ..] if is(first, "PRIOR") => (FetchDirection::Prior, rest),
        [first, rest @ ..] if is(first, "FIRST") => (FetchDirection::First, rest),
        [first, rest @ ..] if is(first, "LAST") => (FetchDirection::Last, rest),
        [first, rest @ ..] if is(first, "ALL") => (FetchDirection::All, rest),
        [first, rest @ ..] if is(first, "ABSOLUTE") => {
            let (limit, rest) = signed_count(rest).ok_or_else(syntax)?;
            (FetchDirection::Absolute { limit }, rest)
        }
        [first, rest @ ..] if is(first, "RELATIVE") => {
            let (limit, rest) = signed_count(rest).ok_or_else(syntax)?;
            (FetchDirection::Relative { limit }, rest)
        }
        [first, all, rest @ ..] if is(first, "FORWARD") && is(all, "ALL") => {
            (FetchDirection::ForwardAll, rest)
        }
        [first, all, rest @ ..] if is(first, "BACKWARD") && is(all, "ALL") => {
            (FetchDirection::BackwardAll, rest)
        }
        [first, rest @ ..] if is(first, "FORWARD") => match signed_count(rest) {
            Some((limit, rest)) => (FetchDirection::Forward { limit: Some(limit) }, rest),
            None => (FetchDirection::Forward { limit: None }, rest),
        },
        [first, rest @ ..] if is(first, "BACKWARD") => match signed_count(rest) {
            Some((limit, rest)) => (FetchDirection::Backward { limit: Some(limit) }, rest),
            None => (FetchDirection::Backward { limit: None }, rest),
        },
        _ => match signed_count(tokens) {
            Some((limit, rest)) => (FetchDirection::Count { limit }, rest),
            None => (FetchDirection::Next, tokens),
        },
    };
    let (position, rest) = match rest {
        [from, rest @ ..] if is(from, "FROM") => (FetchPosition::From, rest),
        [from, rest @ ..] if is(from, "IN") => (FetchPosition::In, rest),
        _ => (FetchPosition::From, rest),
    };
    let [
        Token::Word(Word {
            value, quote_style, ..
        }),
    ] = rest
    else {
        return Err(syntax());
    };
    Ok(Statement::Fetch {
        name: Ident {
            value: value.clone(),
            quote_style: *quote_style,
            span: Span::empty(),
        },
        direction,
        position,
        into: None,
    })
}

/// Up to `max` rows (all when `None`) of a cursor, read one at a time from
/// its stream; the cursor is finished when its stream is. `cursor` then
/// stands on the last row returned, or on none after the end.
fn cursor_rows(
    state: Arc<AsyncMutex<PortalExecutionState>>,
    max: Option<usize>,
    cursor: Arc<Cursor>,
) -> impl Stream<Item = PgWireResult<DataRow>> + Send + 'static {
    futures::stream::unfold(Some((state, 0_usize)), move |step| {
        let cursor = Arc::clone(&cursor);
        async move {
            let (state, sent) = step?;
            if max.is_some_and(|max| sent >= max) {
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
                    cursor.stand_on(None);
                    None
                }
                Some(Ok(row)) => {
                    drop(guard);
                    let sent = sent + 1;
                    if max == Some(sent) {
                        cursor.stand_on(Some(row.clone()));
                    }
                    Some((Ok(row), Some((state, sent))))
                }
                // The error is the last item: the governed stream failed.
                Some(Err(error)) => {
                    cursor.failed.store(true, Ordering::SeqCst);
                    Some((Err(error), None))
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use futures::TryStreamExt;

    use super::*;

    fn parse(sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap()
    }

    #[test]
    fn names_of_declared_cursors() {
        let statements = parse(
            "DECLARE c1 CURSOR FOR SELECT 1; SELECT 2; DECLARE \"Big\" CURSOR FOR SELECT 3; \
             DECLARE Small CURSOR FOR SELECT 4",
        );
        // Folded to lower case unless quoted, as PostgreSQL reads them.
        assert_eq!(declared_names(&statements), ["c1", "Big", "small"]);
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

    fn open_names(store: &Store, names: &[&str]) -> Vec<String> {
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
        let refused = cursors.declare(&declares("DECLARE c CURSOR FOR SELECT 1"), None);
        assert_eq!(refused.map(|_| ()).map_err(code), Err("25P01".to_owned()));
        assert!(
            cursors
                .declare(&declares("DECLARE c CURSOR WITHOUT HOLD FOR SELECT 1"), None)
                .is_err()
        );
        cursors
            .declare(&declares("DECLARE h CURSOR WITH HOLD FOR SELECT 1"), None)
            .unwrap();
        // A message of several statements is an implicit transaction block.
        cursors.begin_message(TransactionStatus::Idle, 2);
        cursors
            .declare(&declares("DECLARE m CURSOR FOR SELECT 1"), None)
            .unwrap();
        cursors.begin_message(TransactionStatus::Idle, 1);
        cursors.begin();
        cursors
            .declare(&declares("DECLARE b CURSOR FOR SELECT 1"), None)
            .unwrap();
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors
            .declare(&declares("DECLARE t CURSOR FOR SELECT 1"), None)
            .unwrap();
        let mut names: Vec<String> = lock(&cursors.0).cursors.keys().cloned().collect();
        names.sort();
        assert_eq!(names, ["b", "h", "m", "t"]);
    }

    #[test]
    fn declares_postgresql_refuses_or_the_engine_cannot_answer() {
        let cursors = Cursors::default();
        cursors.begin_message(TransactionStatus::Transaction, 1);
        let store = store_with(&["c"]);
        let declare = |sql| {
            cursors
                .declare(&declares(sql), Some(&store))
                .map(|_| ())
                .map_err(code)
        };
        assert_eq!(declare("DECLARE c CURSOR FOR SELECT 1"), Err("42P03".to_owned()));
        // Unquoted names are folded to lower case, quoted ones kept.
        assert_eq!(declare("DECLARE C CURSOR FOR SELECT 1"), Err("42P03".to_owned()));
        assert_eq!(declare("DECLARE \"C\" CURSOR FOR SELECT 1"), Ok(()));
        assert_eq!(
            declare("DECLARE b BINARY CURSOR FOR SELECT 1"),
            Err("0A000".to_owned())
        );
        assert_eq!(declare("DECLARE d CURSOR FOR SELECT 1"), Ok(()));
        let closed = cursors.close(
            &store,
            &CloseCursor::Specific {
                name: Ident::new("x"),
            },
        );
        assert_eq!(closed.map(|_| ()).map_err(code), Err("34000".to_owned()));
        assert!(
            cursors
                .close(
                    &store,
                    &CloseCursor::Specific {
                        name: Ident::new("C")
                    }
                )
                .is_ok(),
            "CLOSE C closes c"
        );
        assert!(store.get_portal("c").is_none());
        let quoted = cursors.close(
            &store,
            &CloseCursor::Specific {
                name: Ident::with_quote('"', "D"),
            },
        );
        assert_eq!(quoted.map(|_| ()).map_err(code), Err("34000".to_owned()));
    }

    #[test]
    fn transaction_ends_close_every_portal_but_committed_held_cursors() {
        let all = ["c", "h", "old", "p", ""];
        let cursors = Cursors::default();
        let store = store_with(&all);
        cursors.begin_message(TransactionStatus::Idle, 1);
        cursors
            .declare(&declares("DECLARE old CURSOR WITH HOLD FOR SELECT 1"), None)
            .unwrap();
        cursors.end_transaction(Some(&store), true);
        assert_eq!(
            open_names(&store, &all),
            ["old"],
            "a committed transaction keeps its held cursor"
        );

        let store = store_with(&all);
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors
            .declare(&declares("DECLARE c CURSOR FOR SELECT 1"), None)
            .unwrap();
        cursors
            .declare(&declares("DECLARE h CURSOR WITH HOLD FOR SELECT 1"), None)
            .unwrap();
        cursors.end_transaction(Some(&store), false);
        assert_eq!(
            open_names(&store, &all),
            ["old"],
            "a rollback closes the held cursors it declared, not those committed before"
        );

        let store = store_with(&all);
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors
            .declare(&declares("DECLARE h CURSOR WITH HOLD FOR SELECT 1"), None)
            .unwrap();
        cursors.end_transaction(Some(&store), true);
        assert_eq!(open_names(&store, &all), ["h", "old"]);
        // Committed: a later rollback keeps it.
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors.end_transaction(Some(&store), false);
        assert_eq!(open_names(&store, &all), ["h", "old"]);

        // The COMMIT of a failed transaction is a rollback.
        let store = store_with(&all);
        cursors.begin_message(TransactionStatus::Error, 1);
        cursors
            .declare(&declares("DECLARE c CURSOR WITH HOLD FOR SELECT 1"), None)
            .unwrap();
        cursors.end_transaction(Some(&store), true);
        assert_eq!(open_names(&store, &all), ["h", "old"]);
        // A cursor whose portal is gone (closed) is forgotten.
        let store = store_with(&["h"]);
        cursors.end_transaction(Some(&store), true);
        assert_eq!(lock(&cursors.0).cursors.len(), 1);
    }

    #[test]
    fn fetch_counts() {
        let count = |sql: &str| match &parse(sql)[0] {
            Statement::Fetch { direction, .. } => fetch_count(direction).map_err(code),
            other => panic!("{other:?}"),
        };
        assert_eq!(count("FETCH NEXT FROM c"), Ok(Count::Rows(1)));
        assert_eq!(count("FETCH 7 FROM c"), Ok(Count::Rows(7)));
        assert_eq!(count("FETCH FORWARD 2000 FROM c"), Ok(Count::Rows(2000)));
        assert_eq!(count("FETCH ALL FROM c"), Ok(Count::All));
        assert_eq!(count("FETCH FORWARD ALL FROM c"), Ok(Count::All));
        // Zero rows forward: the current row again, never every row.
        assert_eq!(count("FETCH 0 FROM c"), Ok(Count::Current));
        assert_eq!(count("FETCH FORWARD 0 FROM c"), Ok(Count::Current));
        assert_eq!(count("FETCH RELATIVE 0 FROM c"), Ok(Count::Current));
        // A count that is not a whole number is an error, never every row.
        assert_eq!(count("FETCH 1.5 FROM c"), Err("42601".to_owned()));
        assert_eq!(
            count("FETCH 99999999999999999999999 FROM c"),
            Err("42601".to_owned())
        );
        assert_eq!(count("FETCH FORWARD 2.0 FROM c"), Err("42601".to_owned()));
        // Backward: as PostgreSQL answers for a NO SCROLL cursor.
        for sql in [
            "FETCH PRIOR FROM c",
            "FETCH BACKWARD 2 FROM c",
            "FETCH BACKWARD ALL FROM c",
        ] {
            assert_eq!(count(sql), Err("55000".to_owned()), "{sql}");
        }
        assert_eq!(count("FETCH BACKWARD 0 FROM c"), Ok(Count::Current));
        for sql in [
            "FETCH ABSOLUTE 3 FROM c",
            "FETCH RELATIVE 2 FROM c",
            "FETCH FIRST FROM c",
            "FETCH LAST FROM c",
        ] {
            assert_eq!(count(sql), Err("0A000".to_owned()), "{sql}");
        }
    }

    /// The `FETCH` the SQL parser does not read, as read here: its cursor
    /// and its count.
    fn unparsed_fetch(sql: &str) -> Result<(String, Result<Count, String>), String> {
        assert!(
            Parser::parse_sql(&PostgreSqlDialect {}, sql).is_err(),
            "{sql} parses"
        );
        match unparsed(sql, &"parse error").expect(sql).map_err(code)? {
            Statement::Fetch { name, direction, .. } => {
                Ok((name.value, fetch_count(&direction).map_err(code)))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn aborts_are_rollbacks() {
        for (sql, want) in [
            ("ABORT", Some("ROLLBACK")),
            ("abort work", Some("ROLLBACK work")),
            (
                "  Abort TRANSACTION AND NO CHAIN;",
                Some("  ROLLBACK TRANSACTION AND NO CHAIN;"),
            ),
            (
                "SELECT 'é abort'; -- ABORT\n\tABORT; SELECT abort FROM t",
                Some("SELECT 'é abort'; -- ABORT\n\tROLLBACK; SELECT abort FROM t"),
            ),
            ("/* é */ ABORT", Some("/* é */ ROLLBACK")),
            ("SELECT 'ABORT'", None),
            ("SELECT abort FROM t", None),
            ("\"abort\"", None),
            ("ROLLBACK", None),
        ] {
            assert_eq!(abort_as_rollback(sql).as_deref(), want, "{sql}");
        }
        for sql in ["ABORT", "abort work", "BEGIN; ABORT AND CHAIN"] {
            let rewritten = abort_as_rollback(sql).unwrap();
            let statements = parse(&rewritten);
            assert!(
                matches!(statements.last(), Some(Statement::Rollback { .. })),
                "{sql}: {statements:?}"
            );
        }
    }

    #[test]
    fn fetches_the_parser_does_not_read() {
        let backward = || Err("55000".to_owned());
        for (sql, name, count) in [
            ("FETCH -1 FROM c", "c", backward()),
            ("fetch -3 in c;", "c", backward()),
            ("FETCH FORWARD -1 FROM c", "c", backward()),
            ("FETCH RELATIVE -1 c", "c", backward()),
            ("FETCH BACKWARD FROM c", "c", backward()),
            ("FETCH BACKWARD -2 FROM c", "c", Ok(Count::Rows(2))),
            ("FETCH FROM c", "c", Ok(Count::Rows(1))),
            ("FETCH c", "c", Ok(Count::Rows(1))),
            ("FETCH NEXT c", "c", Ok(Count::Rows(1))),
            (
                "FETCH FORWARD FROM \"My Cursor\"",
                "My Cursor",
                Ok(Count::Rows(1)),
            ),
            ("FETCH +2 c", "c", Ok(Count::Rows(2))),
            ("FETCH 2 c", "c", Ok(Count::Rows(2))),
            ("FETCH ALL c", "c", Ok(Count::All)),
        ] {
            assert_eq!(unparsed_fetch(sql), Ok((name.to_owned(), count)), "{sql}");
        }
        for sql in [
            "FETCH 1 FROM",
            "FETCH -x FROM c",
            "FETCH 1 FROM a b",
            "FETCH ABSOLUTE FROM c",
        ] {
            assert_eq!(unparsed_fetch(sql), Err("42601".to_owned()), "{sql}");
        }
        let refused =
            |sql: &str| unparsed(sql, &"parse error").map(|outcome| outcome.map(|_| ()).map_err(code));
        assert_eq!(refused("MOVE 1 IN c"), Some(Err("0A000".to_owned())));
        assert_eq!(refused("BEGIN; FETCH -1 FROM c"), Some(Err("42601".to_owned())));
        assert_eq!(refused("SELEC 1"), None);
        assert_eq!(refused("SELECT 'FETCH' FROM ("), None);
    }

    /// A cursor `c` over rows `1..=rows`, declared in a transaction block.
    async fn cursor_over(rows: u8, declare: &str) -> (Cursors, Store) {
        cursor_streaming((1..=rows).map(|value| Ok(row(value))).collect(), declare).await
    }

    /// A cursor `c` whose stream yields `rows`, declared in a transaction
    /// block.
    async fn cursor_streaming(rows: Vec<PgWireResult<DataRow>>, declare: &str) -> (Cursors, Store) {
        let cursors = Cursors::default();
        cursors.begin_message(TransactionStatus::Transaction, 1);
        cursors.declare(&declares(declare), None).unwrap();
        let store = store_with(&["c"]);
        let response = QueryResponse::new(Arc::new(Vec::new()), futures::stream::iter(rows));
        store.get_portal("c").unwrap().start(response).await;
        (cursors, store)
    }

    #[tokio::test]
    async fn a_failed_cursor_cannot_be_read_again() {
        let rows = vec![Ok(row(1)), Err(user_error("57014", "canceled")), Ok(row(2))];
        let (cursors, store) = cursor_streaming(rows, "DECLARE c CURSOR FOR SELECT 1").await;
        assert_eq!(
            fetched(&cursors, &store, "FETCH 5 FROM c").await,
            Err("57014".to_owned())
        );
        for sql in ["FETCH 1 FROM c", "FETCH 0 FROM c", "FETCH ALL FROM c"] {
            assert_eq!(
                fetched(&cursors, &store, sql).await,
                Err("55000".to_owned()),
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn fetches_are_tagged_fetch() {
        let (cursors, store) = cursor_over(3, "DECLARE c CURSOR FOR SELECT 1").await;
        for collected in [false, true] {
            let Statement::Fetch { name, direction, .. } = &parse("FETCH 1 FROM c")[0] else {
                unreachable!();
            };
            let fetch = open(
                &cursors,
                Some(&store),
                &name.value,
                direction,
                &Format::UnifiedText,
            )
            .await
            .unwrap();
            let response = if collected {
                fetch.collected(&pool(1 << 20)).await.unwrap()
            } else {
                fetch.streamed()
            };
            let Response::Query(response) = response else {
                panic!("not a query response");
            };
            assert_eq!(response.command_tag(), "FETCH");
        }
    }

    /// Values of the rows `sql` fetches, read as a `FETCH` alone (streamed).
    async fn fetched(cursors: &Cursors, store: &Store, sql: &str) -> Result<Vec<u8>, String> {
        read(cursors, store, sql, false).await
    }

    /// Values of the rows `sql` fetches, read as a `FETCH` alone (streamed)
    /// or, when `collected`, among other statements ([`CursorHook`]).
    async fn read(cursors: &Cursors, store: &Store, sql: &str, collected: bool) -> Result<Vec<u8>, String> {
        let Statement::Fetch { name, direction, .. } = &parse(sql)[0] else {
            panic!("{sql}");
        };
        let fetch = open(cursors, Some(store), &name.value, direction, &Format::UnifiedText)
            .await
            .map_err(code)?;
        let response = if collected {
            fetch.collected(&pool(1 << 20)).await.map_err(code)?
        } else {
            fetch.streamed()
        };
        let Response::Query(response) = response else {
            panic!("{sql}");
        };
        response
            .data_rows
            .map(|row| row.map(|row| row.data[0]).map_err(code))
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn fetch_zero_reads_the_current_row_again() {
        let (cursors, store) = cursor_over(10, "DECLARE c CURSOR FOR SELECT 1").await;
        for (sql, collected, rows) in [
            ("FETCH 0 FROM c", false, vec![]),
            ("FETCH 2 FROM c", false, vec![1, 2]),
            ("FETCH 0 FROM c", false, vec![2]),
            ("FETCH FORWARD 0 FROM c", true, vec![2]),
            ("FETCH RELATIVE 0 FROM c", false, vec![2]),
            ("FETCH NEXT FROM c", true, vec![3]),
            ("FETCH 0 FROM c", true, vec![3]),
            ("FETCH 3 FROM c", false, vec![4, 5, 6]),
            ("FETCH 0 FROM c", false, vec![6]),
            // Exactly the rows left: on the last one, as PostgreSQL.
            ("FETCH 4 FROM c", false, vec![7, 8, 9, 10]),
            ("FETCH 0 FROM c", false, vec![10]),
            // Past the end: on no row.
            ("FETCH 1 FROM c", false, vec![]),
            ("FETCH 0 FROM c", false, vec![]),
            ("FETCH ALL FROM c", false, vec![]),
        ] {
            assert_eq!(read(&cursors, &store, sql, collected).await, Ok(rows), "{sql}");
        }
        let (cursors, store) = cursor_over(5, "DECLARE c CURSOR FOR SELECT 1").await;
        assert_eq!(fetched(&cursors, &store, "FETCH 2 FROM c").await, Ok(vec![1, 2]));
        assert_eq!(
            read(&cursors, &store, "FETCH ALL FROM c", true).await,
            Ok(vec![3, 4, 5])
        );
        assert_eq!(fetched(&cursors, &store, "FETCH 0 FROM c").await, Ok(vec![]));
    }

    #[tokio::test]
    async fn fetch_zero_of_a_no_scroll_cursor_on_a_row_fails() {
        let (cursors, store) = cursor_over(3, "DECLARE c NO SCROLL CURSOR FOR SELECT 1").await;
        assert_eq!(fetched(&cursors, &store, "FETCH 0 FROM c").await, Ok(vec![]));
        assert_eq!(fetched(&cursors, &store, "FETCH 1 FROM c").await, Ok(vec![1]));
        assert_eq!(
            fetched(&cursors, &store, "FETCH 0 FROM c").await,
            Err("55000".to_owned())
        );
        assert_eq!(
            fetched(&cursors, &store, "FETCH 1.5 FROM c").await,
            Err("42601".to_owned())
        );
        assert_eq!(fetched(&cursors, &store, "FETCH 5 FROM c").await, Ok(vec![2, 3]));
        assert_eq!(
            fetched(&cursors, &store, "FETCH 1 FROM x").await,
            Err("34000".to_owned())
        );
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

    /// A memory pool of `bytes`.
    fn pool(bytes: usize) -> Arc<dyn MemoryPool> {
        Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(bytes))
    }

    /// A `FETCH` among other statements holds the rows it read in the pool
    /// until they are sent; rows that do not fit fail it (`53200`) and its
    /// cursor, never held outside the pool.
    #[tokio::test]
    async fn rows_read_before_their_response_are_held_in_the_pool() {
        let (cursors, store) = cursor_over(100, "DECLARE c CURSOR FOR SELECT 1").await;
        let pool = pool(1 << 20);
        let fetch = |sql: &str| {
            let Statement::Fetch { name, direction, .. } = parse(sql).remove(0) else {
                panic!("{sql}");
            };
            let (cursors, store) = (&cursors, &store);
            async move {
                open(
                    cursors,
                    Some(store),
                    &name.value,
                    &direction,
                    &Format::UnifiedText,
                )
                .await
                .unwrap()
            }
        };
        let response = fetch("FETCH 10 FROM c").await.collected(&pool).await.unwrap();
        let held = pool.reserved();
        assert!(
            held >= 10 * std::mem::size_of::<DataRow>(),
            "{held} bytes reserved"
        );
        let Response::Query(response) = response else {
            panic!("not a query response");
        };
        let values: Vec<u8> = response.data_rows.map(|row| row.unwrap().data[0]).collect().await;
        assert_eq!(values, (1..=10).collect::<Vec<_>>());
        assert_eq!(pool.reserved(), 0, "given back once sent");

        // A pool too small for the rows: refused, and the cursor with it.
        let small = self::pool(2 * std::mem::size_of::<DataRow>() * 8);
        let refused = fetch("FETCH ALL FROM c").await.collected(&small).await;
        assert_eq!(refused.map(|_| ()).map_err(code), Err("53200".to_owned()));
        assert_eq!(small.reserved(), 0);
        assert_eq!(
            fetched(&cursors, &store, "FETCH 1 FROM c").await,
            Err("55000".to_owned())
        );
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
        let max = (max_rows > 0).then_some(max_rows);
        cursor_rows(Arc::clone(state), max, Arc::new(Cursor::new(false, true)))
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
