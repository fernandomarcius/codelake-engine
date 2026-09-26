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

use std::sync::Arc;

use datafusion::sql::sqlparser::ast::{FetchDirection, Statement, Value};
use datafusion_postgres::pgwire::api::ClientPortalStore;
use datafusion_postgres::pgwire::api::portal::{Format, Portal, PortalExecutionState};
use datafusion_postgres::pgwire::api::results::{FieldInfo, QueryResponse, Response};
use datafusion_postgres::pgwire::api::store::{MemPortalStore, PortalStore};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::data::DataRow;
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, OwnedMutexGuard};

use super::{DfStatement, user_error};

type CursorPortal = Arc<Portal<DfStatement>>;

/// The connection's portals, where `datafusion-postgres` keeps its cursors.
fn portals<C>(client: &C) -> Option<&MemPortalStore<DfStatement>>
where
    C: ClientPortalStore + ?Sized,
    C::PortalStore: PortalStore,
{
    client.portal_store().as_any().downcast_ref()
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
    state: Arc<Mutex<PortalExecutionState>>,
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

    fn suspended(rows: Vec<PgWireResult<DataRow>>) -> Arc<Mutex<PortalExecutionState>> {
        let response = QueryResponse::new(Arc::new(Vec::new()), futures::stream::iter(rows));
        Arc::new(Mutex::new(PortalExecutionState::Suspended(response)))
    }

    async fn values(
        state: &Arc<Mutex<PortalExecutionState>>,
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
