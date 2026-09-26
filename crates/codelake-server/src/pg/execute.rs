//! `Execute` of the extended protocol, with the rows streamed to the client.
//!
//! `pgwire`'s default `on_execute` collects every row a portal yields into a
//! vector before sending the first (`Portal::fetch`): with no row limit (what
//! JDBC, psycopg3 and `tokio-postgres`' `query` send) a whole result sits in
//! server memory outside the pool, and the governed statement ends — slot
//! freed, time limit over — while its client has read nothing. This is the
//! same handler with that one step changed: rows go to the connection's sink
//! as the stream yields them, so the sink's back-pressure reaches the
//! operators and the statement runs, governed, until its rows are sent.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_postgres::pgwire::api::portal::{Portal, PortalExecutionState};
use datafusion_postgres::pgwire::api::query::{
    ExtendedQueryHandler, send_execution_response, send_partial_query_response,
};
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{
    ClientInfo, ClientPortalStore, ConnectionHandle, DEFAULT_NAME, PgWireConnectionState, copy,
};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::pgwire::messages::extendedquery::Execute;
use datafusion_postgres::pgwire::messages::response::{EmptyQueryResponse, TransactionStatus};
use futures::future::{Either, select};
use futures::{Sink, SinkExt};

/// Runs the portal `message` names (when not started yet) and sends up to
/// `max_rows` of its rows (all when `0`), as `pgwire`'s default does, one
/// row at a time.
pub(super) async fn execute<H, C>(handler: &H, client: &mut C, message: Execute) -> PgWireResult<()>
where
    H: ExtendedQueryHandler + ?Sized,
    C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::PortalStore: PortalStore<Statement = H::Statement>,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
        return Err(PgWireError::NotReadyForQuery);
    }
    let mut transaction_status = client.transaction_status();
    client.set_state(PgWireConnectionState::QueryInProgress);

    let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
    // As `pgwire` reads it: a negative limit is no limit.
    let max_rows = usize::try_from(message.max_rows).unwrap_or(0);
    let Some(portal) = client.portal_store().get_portal(portal_name) else {
        // As `PostgreSQL` answers it (`pgwire` says `26000`): the portal may
        // have been closed by the end of its transaction.
        return Err(super::user_error(
            "34000",
            format!("portal \"{portal_name}\" does not exist"),
        ));
    };

    let initial = matches!(*portal.state().lock().await, PortalExecutionState::Initial);
    let has_rows = if initial {
        let cancel = match client.session_extensions().get::<Arc<ConnectionHandle>>() {
            Some(handle) => Some(handle.start_query().await),
            None => None,
        };
        let response = match cancel {
            Some(cancel) => match select(handler.do_query(client, portal.as_ref(), max_rows), cancel).await {
                Either::Left((result, _)) => result,
                Either::Right(_) => Err(PgWireError::QueryCanceled),
            },
            None => handler.do_query(client, portal.as_ref(), max_rows).await,
        }?;
        send_response(client, portal.as_ref(), response, &mut transaction_status).await?
    } else {
        // Suspended or finished: send what is left.
        true
    };

    if has_rows {
        send_rows(client, portal.as_ref(), max_rows).await?;
    }

    if !matches!(client.state(), PgWireConnectionState::CopyInProgress(_)) {
        client.set_state(PgWireConnectionState::ReadyForQuery);
        client.set_transaction_status(transaction_status);
    }
    Ok(())
}

/// Sends what executing the portal answered; `true` when it has rows to send.
async fn send_response<C, S: Clone>(
    client: &mut C,
    portal: &Portal<S>,
    response: Response,
    transaction_status: &mut TransactionStatus,
) -> PgWireResult<bool>
where
    C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::PortalStore: PortalStore<Statement = S>,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    Ok(match response {
        Response::Query(results) => {
            portal.start(results).await;
            true
        }
        Response::EmptyQuery => {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse::new()))
                .await?;
            false
        }
        Response::Execution(tag) => {
            send_execution_response(client, tag).await?;
            false
        }
        Response::TransactionStart(tag) => {
            send_execution_response(client, tag).await?;
            *transaction_status = transaction_status.to_in_transaction_state();
            false
        }
        Response::TransactionEnd(tag) => {
            send_execution_response(client, tag).await?;
            *transaction_status = transaction_status.to_idle_state();
            client.portal_store().rm_portal(DEFAULT_NAME);
            false
        }
        Response::Error(error) => {
            client
                .send(PgWireBackendMessage::ErrorResponse((*error).into()))
                .await?;
            *transaction_status = transaction_status.to_error_state();
            false
        }
        Response::CopyIn(result) => {
            client.set_state(PgWireConnectionState::CopyInProgress(true));
            copy::send_copy_in_response(client, result).await?;
            false
        }
        Response::CopyOut(result) => {
            copy::send_copy_out_response(client, result).await?;
            false
        }
        Response::CopyBoth(result) => {
            client.set_state(PgWireConnectionState::CopyInProgress(true));
            copy::send_copy_both_response(client, result).await?;
            false
        }
    })
}

/// Sends up to `max_rows` of the portal's rows (all when `0`) as its stream
/// yields them, then `PortalSuspended` or `CommandComplete`.
async fn send_rows<C, S: Clone>(client: &mut C, portal: &Portal<S>, max_rows: usize) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let state = portal.state();
    let mut state = state.lock().await;
    match &mut *state {
        PortalExecutionState::Initial => return Err(PgWireError::PortalNotStarted),
        PortalExecutionState::Finished => {
            client
                .send(PgWireBackendMessage::CommandComplete(
                    Tag::new("SELECT").with_rows(0).into(),
                ))
                .await?;
        }
        PortalExecutionState::Suspended(response) => {
            match send_partial_query_response(client, response, max_rows).await {
                Ok(true) => {}
                Ok(false) => *state = PortalExecutionState::Finished,
                Err(error) => {
                    // The stream failed (or the client went away): drop
                    // it, which ends the statement and frees its memory.
                    *state = PortalExecutionState::Finished;
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}
