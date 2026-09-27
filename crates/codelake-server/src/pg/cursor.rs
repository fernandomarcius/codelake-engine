//! Cursors (`DECLARE … CURSOR`, `FETCH`, `CLOSE`) under governance.
//!
//! The server declares a cursor as `PostgreSQL` names it (folded to lower
//! case unless quoted) and keeps its result stream in a portal of the
//! connection, as `datafusion-postgres` would; `FETCH` and `MOVE` read it
//! and `CLOSE` drops it. The `DECLARE`
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
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Span, Token, TokenWithSpan, Tokenizer, Word};
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
    /// The subtransaction it was declared in ([`Book::subtransactions`]):
    /// a `ROLLBACK TO SAVEPOINT` of a savepoint set before closes it.
    subtransaction: u64,
}

impl Cursor {
    fn new(hold: bool, scroll: bool, subtransaction: u64) -> Self {
        Self {
            hold,
            scroll,
            subtransaction,
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
#[expect(
    clippy::struct_excessive_bools,
    reason = "PostgreSQL's transaction flags, each set on its own"
)]
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
    /// The transaction block's savepoints, oldest first, each with the
    /// subtransaction it began.
    savepoints: Vec<(String, u64)>,
    /// The last subtransaction begun (by `SAVEPOINT`, or `ROLLBACK TO
    /// SAVEPOINT`, which begins its savepoint's again), counted from the
    /// connection's first: what was declared since a savepoint has a number
    /// at least its own.
    subtransactions: u64,
    /// The subtransaction in which the transaction last ran a statement
    /// that may write ([`reads_only`]): the engine's writes are not
    /// transactional, so a `ROLLBACK TO SAVEPOINT` could not undo it.
    last_write: Option<u64>,
    /// A statement set the transaction status to one `pgwire` cannot tell
    /// from its answer (`ROLLBACK TO SAVEPOINT` in a failed block, `AND
    /// CHAIN`): the message ends in the status this book says
    /// ([`restated`]).
    restated: bool,
    /// An extended-protocol `Execute` of the transaction completed before
    /// the one being run, in the same pipeline (`PostgreSQL`'s
    /// `XACT_FLAGS_PIPELINING`): what cannot run in a transaction block
    /// cannot run here either.
    pipelined: bool,
}

impl Cursors {
    pub(super) fn of(client: &(impl ClientInfo + ?Sized)) -> Arc<Self> {
        client.session_extensions().get_or_insert_with(Self::default)
    }

    /// A message of `statements` statements starts, the connection's
    /// transaction status as `pgwire` tracks it.
    fn begin_message(&self, status: TransactionStatus, statements: usize) {
        let mut book = lock(&self.0);
        book.restated = false;
        if status == TransactionStatus::Idle {
            book.savepoints.clear();
            book.last_write = None;
        }
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
            if book.pipelined {
                return Err(user_error(
                    "25001",
                    "DISCARD ALL cannot be executed within a pipeline",
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
        let subtransaction = book.subtransactions;
        let cursor = book
            .cursors
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(Cursor::new(false, true, subtransaction)));
        Arc::clone(cursor)
    }

    /// `SAVEPOINT`: a subtransaction begins, in a transaction block only
    /// (`25P01`), as in `PostgreSQL`.
    fn savepoint(&self, name: &Ident) -> PgWireResult<Response> {
        let mut book = lock(&self.0);
        if !book.explicit {
            return Err(outside_block("SAVEPOINT"));
        }
        book.subtransactions += 1;
        let subtransaction = book.subtransactions;
        book.savepoints.push((cursor_name(name), subtransaction));
        Ok(Response::Execution(Tag::new("SAVEPOINT")))
    }

    /// Where the savepoint `name` stands: the last set of that name
    /// (`3B001` when there is none, `25P01` outside a transaction block).
    fn find_savepoint(book: &Book, name: &Ident, statement: &str) -> PgWireResult<usize> {
        if !book.explicit {
            return Err(outside_block(statement));
        }
        let name = cursor_name(name);
        book.savepoints
            .iter()
            .rposition(|(set, _)| *set == name)
            .ok_or_else(|| user_error("3B001", format!("savepoint \"{name}\" does not exist")))
    }

    /// `RELEASE SAVEPOINT`: the savepoint and those set after it are gone;
    /// what was declared since stays open, as in `PostgreSQL`.
    fn release(&self, name: &Ident) -> PgWireResult<Response> {
        let mut book = lock(&self.0);
        let at = Self::find_savepoint(&book, name, "RELEASE SAVEPOINT")?;
        book.savepoints.truncate(at);
        Ok(Response::Execution(Tag::new("RELEASE")))
    }

    /// `ROLLBACK TO SAVEPOINT`, as `PostgreSQL` does it for cursors: the
    /// cursors declared since the savepoint are closed (their statements end,
    /// slot and memory given back), those declared before stay open where
    /// they stand (a move is not undone), the savepoints set after it are
    /// gone, and a failed transaction block is no longer failed. The
    /// engine's writes are not transactional: after a statement that may
    /// write ([`reads_only`]) since the savepoint it fails with `0A000`
    /// rather than leave the write in place as if undone.
    fn rollback_to(&self, client: &mut dyn HookClient, name: &Ident) -> PgWireResult<Response> {
        let store = client.portal_store();
        let mut book = lock(&self.0);
        let at = Self::find_savepoint(&book, name, "ROLLBACK TO SAVEPOINT")?;
        let (set, since) = book.savepoints[at].clone();
        if book.last_write.is_some_and(|written| written >= since) {
            return Err(user_error(
                "0A000",
                format!(
                    "ROLLBACK TO SAVEPOINT \"{set}\" cannot undo the statements since it that may \
                     write: the engine's writes are not transactional"
                ),
            ));
        }
        let closed: Vec<String> = book
            .cursors
            .iter()
            .filter(|(_, cursor)| cursor.subtransaction >= since)
            .map(|(name, _)| name.clone())
            .collect();
        for name in closed {
            store.rm_portal(&name);
            book.cursors.remove(&name);
        }
        book.savepoints.truncate(at + 1);
        book.subtransactions += 1;
        let subtransaction = book.subtransactions;
        book.savepoints[at].1 = subtransaction;
        book.aborted = false;
        book.restated = true;
        drop(book);
        client.set_transaction_status(TransactionStatus::Transaction);
        Ok(Response::Execution(Tag::new("ROLLBACK")))
    }

    /// `COMMIT AND CHAIN` (`commit`) or `ROLLBACK AND CHAIN`: the
    /// transaction block ends, as `COMMIT` or `ROLLBACK` would end it (a
    /// failed one is rolled back), and a new one begins at once, in a
    /// transaction block only (`25P01`), as in `PostgreSQL`.
    fn chain(&self, client: &mut dyn HookClient, commit: bool) -> PgWireResult<Response> {
        let (explicit, aborted) = {
            let book = lock(&self.0);
            (book.explicit, book.aborted)
        };
        if !explicit {
            let statement = if commit {
                "COMMIT AND CHAIN"
            } else {
                "ROLLBACK AND CHAIN"
            };
            return Err(outside_block(statement));
        }
        self.end_transaction(Some(client.portal_store()), commit);
        let mut book = lock(&self.0);
        book.explicit = true;
        book.restated = true;
        drop(book);
        client.set_transaction_status(TransactionStatus::Transaction);
        let tag = if commit && !aborted { "COMMIT" } else { "ROLLBACK" };
        Ok(Response::Execution(Tag::new(tag)))
    }

    /// The transaction in progress ends, committed or rolled back (a failed
    /// one always is): every portal of the connection is closed but the
    /// `WITH HOLD` cursors it commits or an earlier transaction committed.
    fn end_transaction(&self, store: Option<&Store>, commit: bool) {
        let mut book = lock(&self.0);
        let commit = commit && !book.aborted;
        book.explicit = false;
        book.aborted = false;
        book.savepoints.clear();
        book.last_write = None;
        book.pipelined = false;
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
            made.push((
                name,
                Cursor::new(hold, declare.scroll != Some(false), self.subtransactions),
            ));
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

/// `statement` can only be used in transaction blocks (`25P01`).
fn outside_block(statement: &str) -> PgWireError {
    user_error(
        "25P01",
        format!("{statement} can only be used in transaction blocks"),
    )
}

/// The transaction status a message or an `Execute` ends in, when one of
/// its statements set it where `pgwire` cannot follow ([`Cursors::rollback_to`],
/// [`Cursors::chain`]): the one this connection's book says.
pub(super) fn restated(client: &(impl ClientInfo + ?Sized)) -> Option<TransactionStatus> {
    let cursors = Cursors::of(client);
    let mut book = lock(&cursors.0);
    if !std::mem::take(&mut book.restated) {
        return None;
    }
    Some(match (book.explicit, book.aborted) {
        (false, _) => TransactionStatus::Idle,
        (true, false) => TransactionStatus::Transaction,
        (true, true) => TransactionStatus::Error,
    })
}

/// `statements` are about to run (`None`: a message the SQL parser does not
/// read): when one may write ([`reads_only`]), a `ROLLBACK TO SAVEPOINT` of
/// a savepoint set before cannot undo it.
pub(super) fn will_run(client: &(impl ClientInfo + ?Sized), statements: Option<&[Statement]>) {
    if !statements.is_some_and(|statements| statements.iter().all(reads_only)) {
        let cursors = Cursors::of(client);
        let mut book = lock(&cursors.0);
        book.last_write = Some(book.subtransactions);
    }
}

/// Whether `statement` leaves every table as it was: a query (no `SELECT
/// INTO`, no data-modifying `WITH`), a cursor statement, `EXPLAIN` without
/// `ANALYZE`, `SHOW`, `SET`, transaction control, `PREPARE`, a procedure
/// `CALL` (`kill_query`). Anything else may write.
fn reads_only(statement: &Statement) -> bool {
    match statement {
        Statement::Query(query) => query_reads_only(query),
        Statement::Declare { stmts } => stmts
            .iter()
            .all(|declare| declare.for_query.as_deref().is_none_or(query_reads_only)),
        Statement::Explain {
            analyze, statement, ..
        } => !*analyze || reads_only(statement),
        Statement::Fetch { .. }
        | Statement::Close { .. }
        | Statement::ExplainTable { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowViews { .. }
        | Statement::Set(_)
        | Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::Discard { .. }
        | Statement::Deallocate { .. }
        | Statement::Prepare { .. }
        | Statement::Call(_) => true,
        _ => false,
    }
}

fn query_reads_only(query: &datafusion::sql::sqlparser::ast::Query) -> bool {
    query
        .with
        .as_ref()
        .is_none_or(|with| with.cte_tables.iter().all(|cte| query_reads_only(&cte.query)))
        && set_reads_only(&query.body)
}

fn set_reads_only(body: &datafusion::sql::sqlparser::ast::SetExpr) -> bool {
    use datafusion::sql::sqlparser::ast::SetExpr;
    match body {
        SetExpr::Select(select) => select.into.is_none(),
        SetExpr::Values(_) | SetExpr::Table(_) => true,
        SetExpr::Query(query) => query_reads_only(query),
        SetExpr::SetOperation { left, right, .. } => set_reads_only(left) && set_reads_only(right),
        _ => false,
    }
}

/// An extended-protocol `Execute` starts.
pub(super) fn begin_execute(client: &(impl ClientInfo + ?Sized)) {
    Cursors::of(client).begin_message(client.transaction_status(), 1);
}

/// An extended-protocol `Execute` of `statement` completed (its portal not
/// suspended): as in `PostgreSQL`, the statements after it before the next
/// `Sync` run in a pipeline, where `DISCARD ALL` fails with `25001`. A
/// transaction statement or a `DISCARD ALL` ends the transaction it ran in
/// instead (`PostgreSQL` commits it at once).
pub(super) fn executed(client: &(impl ClientInfo + ?Sized), statement: Option<&Statement>) {
    let ends = matches!(
        statement,
        Some(
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }
                | Statement::Discard {
                    object_type: DiscardObject::ALL
                }
        )
    );
    if !ends {
        lock(&Cursors::of(client).0).pipelined = true;
    }
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
    /// `bound` is the statement's plan and its parameters' values, on the
    /// extended protocol.
    async fn observe(
        statement: &Statement,
        bound: Option<(&LogicalPlan, &ParamValues)>,
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
                    false,
                )
                .await;
                let pool = &session_context.runtime_env().memory_pool;
                return Some(match fetch {
                    Ok(fetch) => fetch.collected(pool).await,
                    Err(error) => Err(error),
                });
            }
            Statement::Savepoint { name } => return Some(cursors.savepoint(name)),
            Statement::ReleaseSavepoint { name } => return Some(cursors.release(name)),
            Statement::Rollback {
                savepoint: Some(name),
                ..
            } => return Some(cursors.rollback_to(client, name)),
            Statement::Commit { chain: true, .. } => return Some(cursors.chain(client, true)),
            Statement::Rollback { chain: true, .. } => return Some(cursors.chain(client, false)),
            // Answered here for its tag: `START TRANSACTION` or `BEGIN`, as
            // written (`datafusion-postgres` answers `BEGIN` to both).
            Statement::StartTransaction { begin, .. } => {
                cursors.begin();
                let tag = Tag::new(if *begin { "BEGIN" } else { "START TRANSACTION" });
                return Some(Ok(if client.transaction_status() == TransactionStatus::Idle {
                    Response::TransactionStart(tag)
                } else {
                    Response::Execution(tag)
                }));
            }
            Statement::Commit { .. } => cursors.end_transaction(Some(client.portal_store()), true),
            Statement::Rollback { .. } => cursors.end_transaction(Some(client.portal_store()), false),
            Statement::Declare { stmts } => {
                return Some(declare(&cursors, stmts, bound, session_context, client).await);
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
        Self::observe(statement, None, session_context, client).await
    }

    /// `DISCARD`, which `DataFusion` does not plan, is answered here when
    /// it runs. A `DECLARE` of one cursor is planned as its query, whose
    /// parameters (`$1`) the client then binds, as `PostgreSQL` plans it
    /// (psycopg3's server-side cursors send them); it is described with no
    /// columns ([`declares_one`]).
    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if let Some(query) = declares_one(statement) {
            let mut query = datafusion::sql::parser::Statement::Statement(Box::new(Statement::Query(
                Box::new(query.clone()),
            )));
            // `IS [NOT] DISTINCT FROM` grouped as PostgreSQL groups it, as
            // every other statement of the session (`hooks::plan_checked`).
            codelake_core::regroup_distinct_from(&mut query);
            return Some(
                session_context
                    .state()
                    .statement_to_plan(query)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error))),
            );
        }
        matches!(
            statement,
            Statement::Discard { .. } | Statement::Savepoint { .. } | Statement::ReleaseSavepoint { .. }
        )
        .then(|| Ok(empty_plan()))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        logical_plan: &LogicalPlan,
        params: &ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        Self::observe(statement, Some((logical_plan, params)), session_context, client).await
    }
}

/// `PostgreSQL`'s longest identifier, in bytes (`NAMEDATALEN - 1`).
const MAX_IDENTIFIER_BYTES: usize = 63;

/// A cursor's name as `PostgreSQL` reads it: folded to lower case unless
/// quoted (`DECLARE Big` declares `big`), and cut to 63 bytes, on a
/// character boundary, as it cuts every identifier (a `FETCH` naming the
/// cursor in full finds it too).
fn cursor_name(name: &Ident) -> String {
    let mut name = if name.quote_style.is_some() {
        name.value.clone()
    } else {
        name.value.to_ascii_lowercase()
    };
    if name.len() > MAX_IDENTIFIER_BYTES {
        let mut end = MAX_IDENTIFIER_BYTES;
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        name.truncate(end);
    }
    name
}

/// The query of a `DECLARE` of one cursor, planned on the extended protocol
/// with its parameters ([`CursorHook`]).
pub(super) fn declares_one(statement: &Statement) -> Option<&datafusion::sql::sqlparser::ast::Query> {
    match statement {
        Statement::Declare { stmts } => match stmts.as_slice() {
            [declare] => declare.for_query.as_deref(),
            _ => None,
        },
        _ => None,
    }
}

/// Whether `params` holds any value.
fn has_values(params: &ParamValues) -> bool {
    match params {
        ParamValues::List(values) => !values.is_empty(),
        ParamValues::Map(values) => !values.is_empty(),
    }
}

/// `DECLARE … CURSOR`: each cursor checked ([`Cursors::declare`]), its
/// query planned as a query of the session is (operands checked,
/// [`crate::hooks::plan_checked`]) and its stream kept in a portal of the
/// connection, rows encoded in text as a query's are, for `FETCH` to read.
/// On the extended protocol (`bound`), a query with parameters runs the
/// plan made when it was parsed, with the values bound to them.
async fn declare(
    cursors: &Cursors,
    declares: &[Declare],
    bound: Option<(&LogicalPlan, &ParamValues)>,
    session_context: &SessionContext,
    client: &mut dyn HookClient,
) -> PgWireResult<Response> {
    let made = cursors.declare(declares, Some(client.portal_store()))?;
    let options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
    let bound = bound.filter(|(_, params)| declares.len() == 1 && has_values(params));
    for ((name, _), declare) in made.iter().zip(declares) {
        let outcome = async {
            let query = declare
                .for_query
                .as_ref()
                .ok_or_else(|| user_error("42601", "DECLARE CURSOR requires a FOR query"))?
                .to_string();
            let df = match bound {
                Some((plan, params)) => {
                    let plan = plan
                        .clone()
                        .replace_params_with_values(params)
                        .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                    crate::hooks::run_checked(session_context, plan).await?
                }
                None => crate::hooks::plan_checked(session_context, &query).await?,
            };
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
/// in the result `format` its client asked for, or `MOVE` when `sql`, the
/// statement's text, is one ([`moves`]); `None` when `statement` is not a
/// `FETCH`.
///
/// `held_in` is the memory pool when the `FETCH` is a protocol portal
/// executed with a row limit (JDBC's fetch size), which the client may leave
/// suspended while it runs other statements, other `FETCH`es of the same
/// cursor among them: as `PostgreSQL` does, the `FETCH` then reads all its
/// rows from the cursor at its first `Execute`, held in the pool until they
/// are sent ([`Fetch::collected`]), so no later statement takes rows that
/// are its. Read as they were sent, the rows of a `FETCH 4` suspended after
/// one went to a `FETCH 1` run in between, and the portal resumed past them.
pub(super) async fn fetch<C>(
    client: &C,
    sql: &str,
    statement: &Statement,
    format: &Format,
    held_in: Option<&Arc<dyn MemoryPool>>,
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
    let moving = moves(sql);
    let fetch = open(
        &cursors,
        portals(client),
        &cursor_name(name),
        direction,
        format,
        moving,
    )
    .await;
    Some(match (fetch, held_in) {
        (Ok(fetch), _) if moving => fetch.moved().await,
        (Ok(fetch), Some(pool)) => fetch.collected(pool).await,
        (Ok(fetch), None) => Ok(fetch.streamed()),
        (Err(error), _) => Err(error),
    })
}

/// `FETCH` or `MOVE` among other statements of a simple-protocol message
/// ([`statements`]): its rows read before the next statement runs, held in
/// `pool` ([`Fetch::collected`]), as a `FETCH` the SQL parser reads there is
/// ([`CursorHook`]).
pub(super) async fn fetch_among<C>(
    client: &C,
    sql: &str,
    statement: &Statement,
    pool: &Arc<dyn MemoryPool>,
) -> PgWireResult<Response>
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
        return Err(user_error("42601", format!("not a FETCH: {statement}")));
    };
    if into.is_some() {
        return Err(into_error());
    }
    let moving = moves(sql);
    let fetch = open(
        &Cursors::of(client),
        portals(client),
        &cursor_name(name),
        direction,
        &Format::UnifiedText,
        moving,
    )
    .await?;
    if moving {
        fetch.moved().await
    } else {
        fetch.collected(pool).await
    }
}

/// Whether `sql` is a `MOVE`: [`unparsed`] reads it as the `FETCH` of the
/// same direction, and its text tells them apart (a `FETCH` the SQL parser
/// reads has the text it writes, `FETCH …`).
pub(super) fn moves(sql: &str) -> bool {
    Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .ok()
        .and_then(|tokens| {
            tokens
                .into_iter()
                .find(|token| !matches!(token, Token::Whitespace(_)))
        })
        .is_some_and(|token| is(&token, "MOVE"))
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

    /// `MOVE`: the cursor moved as this `FETCH` would move it, the rows it
    /// reads dropped at once; answers how many rows it moved over (`MOVE n`,
    /// `1` for the current row).
    async fn moved(self) -> PgWireResult<Response> {
        let count = match self.rows {
            FetchRows::Current(row) => usize::from(row.is_some()),
            FetchRows::Read { state, max, cursor } => {
                let mut rows = std::pin::pin!(cursor_rows(state, max, cursor));
                let mut count = 0;
                while let Some(row) = rows.next().await {
                    row?;
                    count += 1;
                }
                count
            }
        };
        Ok(Response::Execution(Tag::new("MOVE").with_rows(count)))
    }

    /// The rows, sent as the cursor's stream yields them.
    fn streamed(self) -> Response {
        let (columns, rows) = self.rows();
        let mut response = QueryResponse::new(columns, rows);
        response.set_command_tag(FETCH_TAG);
        Response::Query(response)
    }

    /// The rows, read before the response is made (a `FETCH` among other
    /// statements of a message: the next one runs before they are sent; a
    /// `FETCH` portal with a row limit: another `FETCH` may run before the
    /// portal is resumed, [`fetch`]).
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

/// Prepares a `FETCH` of the cursor `name`, or a `MOVE` when `moving`:
/// checked (count, cursor, result format) before any row is read. A `MOVE`
/// sends no row: no result format to check, and it may stand again on the
/// current row of a `NO SCROLL` cursor, as in `PostgreSQL`.
async fn open(
    cursors: &Cursors,
    store: Option<&Store>,
    name: &str,
    direction: &FetchDirection,
    format: &Format,
    moving: bool,
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
    if !moving {
        text_only(format, columns.len())?;
    }
    let rows = match count {
        Count::Current => {
            let current = lock(&cursor.current).clone();
            if current.is_some() && !cursor.scroll && !moving {
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
/// `FETCH` then fails) or `sql` is a `MOVE`. `format` is the one the client
/// asked results in (text for a statement). `None` when `statement` is not
/// a `FETCH`.
pub(super) async fn fetch_fields<C>(
    client: &C,
    sql: &str,
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
    if moves(sql) {
        return Some(Ok(Vec::new()));
    }
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

/// A `FETCH` count: a whole number of rows, negative for backward, that
/// `PostgreSQL`'s grammar reads as an integer (`int4`): a larger one is a
/// syntax error there (`42601`), never "every row".
fn number(count: &ValueWithSpan) -> PgWireResult<i64> {
    let invalid = || user_error("42601", format!("syntax error at or near \"{count}\""));
    match &count.value {
        Value::Number(text, _) => text
            .parse::<i64>()
            .ok()
            .filter(|rows| rows.unsigned_abs() <= u64::from(i32::MAX.unsigned_abs()))
            .ok_or_else(invalid),
        _ => Err(invalid()),
    }
}

/// A statement the SQL parser does not read (`error`) that is a `FETCH` or
/// a `MOVE`, answered at once, on either protocol ([`statements`]); `None`
/// when it is neither. A message of several statements is refused
/// (`42601`), as `PostgreSQL` refuses one in a prepared statement.
pub(super) fn unparsed(sql: &str, error: &impl std::fmt::Display) -> Option<PgWireResult<Statement>> {
    Some(match statements(sql, error)? {
        Ok(mut statements) if statements.len() == 1 => Ok(statements.remove(0).1),
        Ok(_) => Err(user_error(
            "42601",
            "cannot insert multiple commands into a prepared statement",
        )),
        Err(error) => Err(error),
    })
}

/// The statements of a message the SQL parser does not read (`error`)
/// because it holds a `FETCH` or a `MOVE` the parser does not read, each
/// with its text, to be run one after the other as `PostgreSQL` runs them.
/// A `FETCH` is read here as `PostgreSQL` reads it when the parser cannot
/// (a negative count, no count, no `FROM`: `FETCH -1 FROM c`, `FETCH FROM
/// c`, `FETCH c`), and a `MOVE` as the `FETCH` of the same direction
/// ([`moves`] tells them apart, from the statement's text). A statement
/// neither reads fails the whole message (`42601`) before any of it runs,
/// as `PostgreSQL` parses a message whole. `None` when the message holds no
/// `FETCH` or `MOVE`.
pub(super) fn statements(
    sql: &str,
    error: &impl std::fmt::Display,
) -> Option<PgWireResult<Vec<(String, Statement)>>> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    // Each statement's text and its tokens, empty statements (`;;`) left
    // out.
    let mut pieces: Vec<(String, Vec<Token>)> = Vec::new();
    let mut start = 0;
    let mut piece = Vec::new();
    for token in tokens
        .into_iter()
        .chain(std::iter::once(TokenWithSpan::wrap(Token::SemiColon)))
    {
        match token.token {
            Token::Whitespace(_) => {}
            Token::SemiColon => {
                let end = if token.span == Span::empty() {
                    sql.len()
                } else {
                    offset(sql, token.span.start.line, token.span.start.column)?
                };
                if !piece.is_empty() {
                    pieces.push((sql[start..end].to_owned(), std::mem::take(&mut piece)));
                }
                start = (end + 1).min(sql.len());
            }
            other => piece.push(other),
        }
    }
    let starts = |tokens: &[Token], keyword: &str| tokens.first().is_some_and(|token| is(token, keyword));
    let cursor = |tokens: &[Token]| starts(tokens, "FETCH") || starts(tokens, "MOVE");
    if !pieces.iter().any(|(_, tokens)| cursor(tokens)) {
        return None;
    }
    let syntax = || user_error("42601", format!("syntax error: {error}"));
    let read = |(text, tokens): (String, Vec<Token>)| {
        let parsed = Parser::parse_sql(&PostgreSqlDialect {}, &text)
            .ok()
            .filter(|parsed| parsed.len() == 1)
            .and_then(|mut parsed| parsed.pop());
        let statement = match parsed {
            Some(statement) if !starts(&tokens, "MOVE") => statement,
            _ if cursor(&tokens) => read_fetch(&tokens[1..])?,
            _ => return Err(syntax()),
        };
        Ok((text, statement))
    };
    Some(pieces.into_iter().map(read).collect())
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

    #[test]
    fn cursor_names_are_cut_as_postgresql_cuts_identifiers() {
        let name = |sql: &str| declared_names(&parse(sql)).remove(0);
        let long = "C".repeat(70);
        assert_eq!(
            name(&format!("DECLARE {long} CURSOR FOR SELECT 1")),
            "c".repeat(63)
        );
        assert_eq!(
            name(&format!("DECLARE \"{long}\" CURSOR FOR SELECT 1")),
            "C".repeat(63)
        );
        // Never inside a character: "é" would end at byte 64.
        let accented = format!("{}é", "a".repeat(62));
        assert_eq!(
            name(&format!("DECLARE \"{accented}\" CURSOR FOR SELECT 1")),
            "a".repeat(62)
        );
        let exact = "b".repeat(63);
        assert_eq!(name(&format!("DECLARE {exact} CURSOR FOR SELECT 1")), exact);
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
        // Counts are PostgreSQL's int4: a larger one is a syntax error.
        assert_eq!(count("FETCH 2147483647 FROM c"), Ok(Count::Rows(2_147_483_647)));
        for sql in [
            "FETCH 2147483648 FROM c",
            "FETCH 9223372036854775807 FROM c",
            "FETCH FORWARD 2147483648 FROM c",
            "FETCH BACKWARD 2147483648 FROM c",
        ] {
            assert_eq!(count(sql), Err("42601".to_owned()), "{sql}");
        }
        assert_eq!(count("FETCH BACKWARD 2147483647 FROM c"), Err("55000".to_owned()));
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
        assert_eq!(refused("MOVE 1 IN c"), Some(Ok(())));
        assert_eq!(refused("BEGIN; MOVE 1 IN c"), Some(Err("42601".to_owned())));
        assert_eq!(
            refused("FETCH -2147483648 FROM c").map(|_| unparsed_fetch("FETCH -2147483648 FROM c")),
            Some(Ok(("c".to_owned(), Err("42601".to_owned()))))
        );
        assert!(moves("  move 1 IN c") && moves("/* x */ MOVE c"));
        assert!(!moves("FETCH 1 FROM c") && !moves("SELECT 'MOVE'") && !moves("\"move\""));
        assert_eq!(refused("BEGIN; FETCH -1 FROM c"), Some(Err("42601".to_owned())));
        assert_eq!(refused("SELEC 1"), None);
        assert_eq!(refused("SELECT 'FETCH' FROM ("), None);
    }

    #[test]
    fn messages_with_unparsed_fetches_are_read_statement_by_statement() {
        let read = |sql: &str| {
            statements(sql, &"parse error").map(|outcome| {
                outcome
                    .map(|statements| {
                        statements
                            .into_iter()
                            .map(|(text, statement)| {
                                (
                                    text.trim().to_owned(),
                                    matches!(statement, Statement::Fetch { .. }),
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                    .map_err(code)
            })
        };
        let text = |text: &str, fetch: bool| (text.to_owned(), fetch);
        assert_eq!(
            read("SELECT 'é;x'; FETCH FROM c ;; MOVE 2 IN c; /* ; */ FETCH 1 FROM c;"),
            Some(Ok(vec![
                text("SELECT 'é;x'", false),
                text("FETCH FROM c", true),
                text("MOVE 2 IN c", true),
                text("/* ; */ FETCH 1 FROM c", true),
            ]))
        );
        assert_eq!(read("FETCH c"), Some(Ok(vec![text("FETCH c", true)])));
        // A statement neither reads fails the message before any of it runs.
        assert_eq!(read("FETCH c; SELEC 1"), Some(Err("42601".to_owned())));
        assert_eq!(read("FETCH c; FETCH -x FROM c"), Some(Err("42601".to_owned())));
        assert_eq!(read("SELECT 1; SELEC 2"), None);
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

    /// A `FETCH` read at once (a portal with a row limit): the cursor is
    /// past its rows before any is sent, and they are held until they are.
    #[tokio::test]
    async fn a_fetch_read_at_once_takes_its_rows_first() {
        let (cursors, store) = cursor_over(5, "DECLARE c CURSOR FOR SELECT 1").await;
        let Statement::Fetch { name, direction, .. } = &parse("FETCH 3 FROM c")[0] else {
            unreachable!();
        };
        let fetch = open(
            &cursors,
            Some(&store),
            &name.value,
            direction,
            &Format::UnifiedText,
            false,
        )
        .await
        .unwrap();
        let Ok(Response::Query(mut held)) = fetch.collected(&pool(1 << 20)).await else {
            panic!("not a query response");
        };
        assert_eq!(fetched(&cursors, &store, "FETCH 1 FROM c").await, Ok(vec![4]));
        let rows: Vec<_> = held
            .data_rows()
            .map(|row| row.unwrap().data.to_vec())
            .collect()
            .await;
        assert_eq!(rows.len(), 3);
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
                false,
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
        let fetch = open(
            cursors,
            Some(store),
            &name.value,
            direction,
            &Format::UnifiedText,
            false,
        )
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

    /// How many rows `MOVE` `sql` moved over (its tag, `MOVE n`).
    async fn moved(cursors: &Cursors, store: &Store, sql: &str) -> Result<usize, String> {
        let Some(Ok(Statement::Fetch { name, direction, .. })) = unparsed(sql, &"parse error") else {
            panic!("{sql}");
        };
        assert!(moves(sql), "{sql}");
        let fetch = open(
            cursors,
            Some(store),
            &name.value,
            &direction,
            &Format::UnifiedBinary,
            true,
        )
        .await
        .map_err(code)?;
        let Response::Execution(tag) = fetch.moved().await.map_err(code)? else {
            panic!("{sql}");
        };
        (0..=20)
            .find(|rows| tag == Tag::new("MOVE").with_rows(*rows))
            .ok_or_else(|| format!("{tag:?}"))
    }

    #[tokio::test]
    async fn moves_as_postgresql_does() {
        let (cursors, store) = cursor_over(10, "DECLARE c NO SCROLL CURSOR FOR SELECT 1").await;
        for (sql, want) in [
            ("MOVE 0 IN c", Ok(0)),
            ("MOVE 2 IN c", Ok(2)),
            // On a row: 1, with no error for a NO SCROLL cursor.
            ("MOVE 0 IN c", Ok(1)),
            ("MOVE RELATIVE 0 IN c", Ok(1)),
            ("MOVE c", Ok(1)),
            ("MOVE FORWARD 2 FROM c", Ok(2)),
            ("MOVE -1 IN c", Err("55000")),
            ("MOVE 20 IN c", Ok(5)),
            ("MOVE 0 IN c", Ok(0)),
            ("MOVE ALL IN c", Ok(0)),
            ("MOVE 1 IN x", Err("34000")),
        ] {
            let got = moved(&cursors, &store, sql).await;
            assert_eq!(got, want.map_err(str::to_owned), "{sql}");
        }
        let (cursors, store) = cursor_over(10, "DECLARE c NO SCROLL CURSOR FOR SELECT 1").await;
        assert!(moved(&cursors, &store, "MOVE 3 IN c").await.is_ok());
        assert_eq!(fetched(&cursors, &store, "FETCH 2 FROM c").await, Ok(vec![4, 5]));
        assert_eq!(
            fetched(&cursors, &store, "FETCH 0 FROM c").await,
            Err("55000".to_owned())
        );
        let (cursors, store) = cursor_over(10, "DECLARE c CURSOR FOR SELECT 1").await;
        assert!(moved(&cursors, &store, "MOVE 3 IN c").await.is_ok());
        assert_eq!(fetched(&cursors, &store, "FETCH 0 FROM c").await, Ok(vec![3]));
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
                    false,
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
        cursor_rows(Arc::clone(state), max, Arc::new(Cursor::new(false, true, 0)))
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
