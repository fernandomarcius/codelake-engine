//! Executes federated SQL on SQL Server and streams Arrow batches back.
//!
//! Memory: rows are converted into batches of `batch_size` rows and handed over
//! through a channel of capacity [`IN_FLIGHT_BATCHES`], so at most that many
//! batches exist per query no matter how large the remote result is.
//!
//! Tracing: every remote statement is one `sqlserver.query` span (a child of
//! the engine's `codelake.remote` span) with the SQL sent — literals redacted,
//! plus its hash — the server and database, time waiting for a pooled
//! connection, rows read, retries, and the SQL Server error number on failure.
//!
//! Lost connections: a read whose connection drops (reset, abort, broken pipe
//! or an early end of the stream) **before any batch reached the consumer**
//! is run again on a new connection, up to [`MAX_RETRIES`] times. Once a batch
//! has been handed over nothing is retried: the rows already delivered cannot
//! be taken back, and reading the result again from the start would repeat
//! them, or silently end the stream short. The error then surfaces as it
//! always did.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::sql::{AstAnalyzer, LogicalOptimizer, SQLExecutor};
use futures::{StreamExt, TryStreamExt};
use tiberius::error::IoErrorKind;
use tokio::sync::mpsc;
use tracing::field::Empty;
use tracing::{Instrument, Span};

use super::dialect::{self, TsqlDialect};
use super::pool::Pool;
use super::types::BatchBuilder;
use codelake_core::federation_policy::restrict_federation;
use codelake_core::trace::{self, TARGET, field};

/// Batches buffered between the SQL Server reader and the engine.
const IN_FLIGHT_BATCHES: usize = 2;

/// Reads run again on a new connection after the connection was lost before
/// the consumer got a batch (so a read is attempted up to `MAX_RETRIES + 1`
/// times).
const MAX_RETRIES: usize = 2;

/// Wait before each retry, in order.
const RETRY_BACKOFF: [Duration; MAX_RETRIES] = [Duration::from_millis(200), Duration::from_secs(1)];

pub(crate) struct SqlServerExecutor {
    context: String,
    pool: Pool,
    batch_size: usize,
    target: Target,
}

/// Where statements go, for span attributes (never credentials).
#[derive(Debug, Clone)]
pub(crate) struct Target {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) database: String,
}

impl std::fmt::Debug for SqlServerExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlServerExecutor")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

impl SqlServerExecutor {
    pub(crate) fn new(context: String, pool: Pool, batch_size: usize, target: Target) -> Self {
        Self {
            context,
            pool,
            batch_size,
            target,
        }
    }
}

/// Span of one remote statement. Created when the stream is first polled,
/// so its parent is the span polling it and its duration is the real work.
fn query_span(target: &Target, sql: &str) -> Span {
    let span = tracing::info_span!(
        target: TARGET,
        "sqlserver.query",
        otel.kind = "client",
        otel.status_code = Empty,
        otel.status_description = Empty,
        db.system.name = "microsoft.sql_server",
        db.namespace = target.database.as_str(),
        server.address = target.host.as_str(),
        server.port = target.port,
        db.query.text = Empty,
        codelake.sql.hash = Empty,
        codelake.pool.wait_ms = Empty,
        codelake.retries = Empty,
        db.response.returned_rows = Empty,
        db.response.status_code = Empty,
        codelake.cancelled = Empty,
    );
    trace::record_sql(&span, sql, &datafusion::sql::sqlparser::dialect::MsSqlDialect {});
    span
}

/// The hash of the redacted SQL: what the span records as `codelake.sql.hash`,
/// the only trace of the statement a log line may carry (empty when the SQL
/// does not tokenize).
fn sql_fingerprint(sql: &str) -> String {
    trace::redact_sql(sql, &datafusion::sql::sqlparser::dialect::MsSqlDialect {})
        .map(|redacted| trace::sql_hash(&redacted))
        .unwrap_or_default()
}

/// Records the SQL Server error number (`Msg 208`, …) on the current span.
///
/// A server error keeps SQL Server's own message, prefixed with its number:
/// `sqlserver: error 245 (state 1, class 16): Conversion failed …`. tiberius'
/// rendering wraps that message in quotes (`'…' on server …`), and the values
/// the message itself quotes then no longer pair up, which leaves them
/// outside every quoted segment of the error text.
fn tds_error(err: tiberius::error::Error) -> DataFusionError {
    if let tiberius::error::Error::Server(token) = &err {
        Span::current().record("db.response.status_code", token.code());
        return DataFusionError::Execution(format!(
            "sqlserver: error {} (state {}, class {}): {}",
            token.code(),
            token.state(),
            token.class(),
            token.message()
        ));
    }
    external(err)
}

fn external(err: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

/// Refuses anything but a single `SELECT`: generated SQL must never carry a
/// second statement, whatever identifiers or aliases went into it.
pub(crate) fn validate(sql: &str) -> Result<()> {
    use datafusion::sql::sqlparser::ast::Statement as Ast;
    use datafusion::sql::sqlparser::dialect::MsSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    let statements = Parser::parse_sql(&MsSqlDialect {}, sql).map_err(|e| {
        DataFusionError::Plan(format!("sqlserver: generated SQL does not parse ({e}): {sql}"))
    })?;
    match statements.as_slice() {
        [Ast::Query(_)] => Ok(()),
        _ => Err(DataFusionError::Plan(format!(
            "sqlserver: generated SQL must be exactly one query: {sql}"
        ))),
    }
}

/// How a remote read ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Outcome {
    rows: u64,
    /// `false` when the consumer went away first (cancel, satisfied `LIMIT`).
    complete: bool,
}

/// The io errors with which a lost connection shows up: the peer reset it
/// (`ConnectionReset`, `ConnectionAborted`, `BrokenPipe`) or closed it in the
/// middle of a message (`UnexpectedEof`).
///
/// tiberius reports a failure of the socket as `Error::Io { kind, .. }`; one
/// met while it reads a packet is re-wrapped as `BrokenPipe` with the original
/// text (`Connection reset by peer (os error 104)`), and the end of the
/// stream inside a message as `UnexpectedEof`. Timeouts are not here: a slow
/// source is not a lost connection and running it again would only wait more.
fn is_lost(kind: IoErrorKind) -> bool {
    matches!(
        kind,
        IoErrorKind::ConnectionReset
            | IoErrorKind::ConnectionAborted
            | IoErrorKind::BrokenPipe
            | IoErrorKind::UnexpectedEof
    )
}

/// The kind of the io error when `err` says the connection to SQL Server was
/// lost, `None` for every other failure (a server error, a conversion, a
/// pool that could not give a connection, ...).
fn lost_connection(err: &DataFusionError) -> Option<IoErrorKind> {
    let DataFusionError::External(inner) = err else {
        return None;
    };
    match inner.downcast_ref::<tiberius::error::Error>()? {
        tiberius::error::Error::Io { kind, .. } if is_lost(*kind) => Some(*kind),
        _ => None,
    }
}

/// What to do with a read whose connection was lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Run it again on a new connection after this pause.
    Retry(Duration),
    /// Surface the error: a batch already reached the consumer.
    Delivered,
    /// Surface the error: every attempt lost its connection.
    Exhausted,
}

/// Decides for a connection lost after `retries` retries, with `delivered`
/// batches already handed to the consumer.
///
/// A delivered batch cannot be taken back: a second read would repeat its
/// rows, or end the stream short without telling. Only a read that has
/// delivered nothing may start over.
fn next_step(delivered: u64, retries: usize) -> Step {
    if delivered > 0 {
        Step::Delivered
    } else {
        RETRY_BACKOFF
            .get(retries)
            .map_or(Step::Exhausted, |pause| Step::Retry(*pause))
    }
}

/// What one attempt to read the result did before it ended.
#[derive(Debug, Default)]
struct Attempt {
    /// Rows read from the connection (delivered or not).
    rows: u64,
    /// Batches the consumer's channel accepted.
    batches: u64,
    /// Time waiting for a pooled connection.
    waited_ms: u64,
    /// When the statement went out on the connection.
    started: Option<Instant>,
}

/// Where a statement goes and which one it is, for logs (never the SQL).
struct Remote<'a> {
    target: &'a Target,
    /// Hash of the redacted SQL, the value of `codelake.sql.hash` on the span.
    hash: &'a str,
}

const CANCELLED: Outcome = Outcome {
    rows: 0,
    complete: false,
};

/// Reads the result, on a new connection each time the previous one is lost
/// before the consumer got anything (see the module documentation).
async fn run(
    pool: Pool,
    sql: String,
    schema: SchemaRef,
    batch_size: usize,
    remote: Remote<'_>,
    tx: &Sender,
) -> Result<Outcome> {
    tracing::debug!("sqlserver query");
    let mut retries = 0_usize;
    let mut waited_ms = 0_u64;
    let result = loop {
        let mut attempt = Attempt::default();
        let result = read_once(&pool, &sql, &schema, batch_size, tx, &mut attempt).await;
        waited_ms = waited_ms.saturating_add(attempt.waited_ms);
        let err = match result {
            Ok(outcome) => break Ok(outcome),
            Err(err) => err,
        };
        let Some(kind) = lost_connection(&err) else {
            break Err(err);
        };
        let step = next_step(attempt.batches, retries);
        tracing::warn!(
            server = remote.target.host.as_str(),
            port = remote.target.port,
            database = remote.target.database.as_str(),
            sql_hash = remote.hash,
            kind = ?kind,
            attempt = %format_args!("{}/{}", retries + 1, MAX_RETRIES + 1),
            retrying = matches!(step, Step::Retry(_)),
            rows_read = attempt.rows,
            batches_delivered = attempt.batches,
            read_ms = attempt.started.map_or(0, |at| u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX)),
            error = %err,
            "sqlserver connection lost while reading"
        );
        let Step::Retry(pause) = step else {
            break Err(err);
        };
        tokio::select! {
            () = tokio::time::sleep(pause) => {}
            () = tx.closed() => break Ok(CANCELLED),
        }
        retries += 1;
    };
    let span = Span::current();
    span.record("codelake.pool.wait_ms", waited_ms);
    span.record(field::RETRIES, u64::try_from(retries).unwrap_or(u64::MAX));
    result
}

/// One attempt: takes a connection, sends the statement and hands the rows
/// over in batches. `attempt` tells the caller how far it got when it fails.
async fn read_once(
    pool: &Pool,
    sql: &str,
    schema: &SchemaRef,
    batch_size: usize,
    tx: &Sender,
    attempt: &mut Attempt,
) -> Result<Outcome> {
    let waiting = Instant::now();
    // Not worth a connection for a consumer that already left.
    let acquired = tokio::select! {
        conn = pool.get_owned() => conn,
        () = tx.closed() => return Ok(CANCELLED),
    };
    attempt.waited_ms = u64::try_from(waiting.elapsed().as_millis()).unwrap_or(u64::MAX);
    let mut conn = acquired.map_err(|e| DataFusionError::Execution(format!("sqlserver pool: {e:?}")))?;
    conn.poisoned = true;
    let mut builder = BatchBuilder::new(Arc::clone(schema), batch_size)?;
    attempt.started = Some(Instant::now());
    {
        let query = async {
            let mut rows = conn
                .client
                .simple_query(sql)
                .await
                .map_err(tds_error)?
                .into_row_stream();
            while let Some(row) = rows.try_next().await.map_err(tds_error)? {
                if attempt.rows == 0 {
                    builder.check_columns(row.columns().iter().map(tiberius::Column::name))?;
                }
                attempt.rows += 1;
                // Consumer gone (query cancelled or LIMIT satisfied): stop; the
                // connection keeps `poisoned` and is discarded by the pool.
                if let Some(batch) = builder.push(row)? {
                    if tx.send(Ok(batch)).await.is_err() {
                        return Ok(false);
                    }
                    attempt.batches += 1;
                }
            }
            Ok::<bool, DataFusionError>(true)
        };
        // A slow remote aggregate sends nothing for a long time: notice the
        // cancellation while waiting instead of at the next batch.
        tokio::select! {
            finished = query => {
                if !finished? {
                    return Ok(Outcome { rows: attempt.rows, ..CANCELLED });
                }
            }
            () = tx.closed() => return Ok(Outcome { rows: attempt.rows, ..CANCELLED }),
        }
    }
    conn.poisoned = false;
    if let Some(batch) = builder.flush()? {
        let _ = tx.send(Ok(batch)).await;
    }
    Ok(Outcome {
        rows: attempt.rows,
        complete: true,
    })
}

type Sender = mpsc::Sender<Result<datafusion::arrow::array::RecordBatch>>;

/// Starts the remote query when the stream is first polled. A hash join
/// creates its probe-side stream before the build side finishes; taking a
/// pooled connection that early could exhaust the pool while waiting.
fn lazy_stream(
    pool: Pool,
    sql: String,
    schema: SchemaRef,
    batch_size: usize,
    target: Target,
) -> impl futures::Stream<Item = Result<datafusion::arrow::array::RecordBatch>> {
    futures::stream::once(async move {
        let span = query_span(&target, &sql);
        let hash = sql_fingerprint(&sql);
        let (tx, rx) = mpsc::channel(IN_FLIGHT_BATCHES);
        let worker = tokio::spawn(
            {
                let tx = tx.clone();
                async move {
                    let remote = Remote {
                        target: &target,
                        hash: &hash,
                    };
                    run(pool, sql, schema, batch_size, remote, &tx).await
                }
            }
            .instrument(span.clone()),
        );
        // Supervisor: an error or a panic in the worker must reach the
        // consumer, never look like a clean end of stream. It also closes
        // the span with the outcome.
        tokio::spawn(async move {
            let failure = match worker.await {
                Ok(Ok(outcome)) => {
                    span.record(field::ROWS, outcome.rows);
                    if !outcome.complete {
                        span.record(field::CANCELLED, true);
                    }
                    None
                }
                Ok(Err(err)) => Some(err),
                Err(join) => Some(DataFusionError::Execution(format!(
                    "sqlserver reader failed: {join}"
                ))),
            };
            if let Some(err) = failure {
                trace::record_error(&span, &err);
                let _ = tx.send(Err(err)).await;
            }
        });
        futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|item| (item, rx)) })
    })
    .flatten()
}

#[async_trait]
impl SQLExecutor for SqlServerExecutor {
    fn name(&self) -> &'static str {
        "sqlserver"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.context.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(TsqlDialect)
    }

    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        let policy = dialect::policy();
        Some(Box::new(move |plan: LogicalPlan| {
            restrict_federation(plan, policy.as_ref())
        }))
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        Some(Box::new(|statement: Statement| dialect::rewrite(statement)))
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        validate(query)?;
        let stream = lazy_stream(
            self.pool.clone(),
            query.to_owned(),
            Arc::clone(&schema),
            self.batch_size,
            self.target.clone(),
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream.boxed())))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "sqlserver: tables are listed by the catalog, not the executor".into(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        Err(DataFusionError::NotImplemented(format!(
            "sqlserver: schema of {table_name} is resolved by the catalog"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::error::DataFusionError;
    use tiberius::error::{Error, IoErrorKind};

    use super::{MAX_RETRIES, RETRY_BACKOFF, Step, lost_connection, next_step, validate};

    fn io(kind: IoErrorKind) -> DataFusionError {
        DataFusionError::External(Box::new(Error::Io {
            kind,
            message: "Connection reset by peer (os error 104)".to_owned(),
        }))
    }

    #[test]
    fn a_connection_that_went_away_is_lost() {
        for kind in [
            IoErrorKind::ConnectionReset,
            IoErrorKind::ConnectionAborted,
            IoErrorKind::BrokenPipe,
            IoErrorKind::UnexpectedEof,
        ] {
            assert_eq!(lost_connection(&io(kind)), Some(kind), "{kind:?}");
        }
    }

    #[test]
    fn other_failures_are_not_a_lost_connection() {
        // A slow or refused connection, a bad certificate or address: running
        // the read again would not help, or would wait longer for nothing.
        for kind in [
            IoErrorKind::TimedOut,
            IoErrorKind::ConnectionRefused,
            IoErrorKind::PermissionDenied,
            IoErrorKind::InvalidData,
            IoErrorKind::Other,
        ] {
            assert_eq!(lost_connection(&io(kind)), None, "{kind:?}");
        }
        let others = [
            DataFusionError::External(Box::new(Error::Protocol("unexpected token".into()))),
            DataFusionError::External(Box::new(Error::Tls("handshake".to_owned()))),
            DataFusionError::External(Box::new(Error::Conversion("bad value".into()))),
            DataFusionError::External(Box::new(Error::Utf8)),
            // An error of the pool or of a SQL Server statement is rendered as text.
            DataFusionError::Execution("sqlserver pool: TimedOut".to_owned()),
            DataFusionError::Execution(
                "sqlserver: error 208 (state 1, class 16): Invalid object name".to_owned(),
            ),
            // Another error that merely mentions a reset.
            DataFusionError::Execution("Connection reset by peer (os error 104)".to_owned()),
            DataFusionError::External("connection reset".into()),
        ];
        for err in &others {
            assert_eq!(lost_connection(err), None, "{err}");
        }
    }

    #[test]
    fn nothing_is_retried_once_a_batch_was_delivered() {
        for retries in 0..=MAX_RETRIES {
            assert_eq!(next_step(1, retries), Step::Delivered);
            assert_eq!(next_step(7, retries), Step::Delivered);
        }
    }

    #[test]
    fn a_read_with_no_delivered_batch_is_retried_with_backoff_then_gives_up() {
        assert_eq!(next_step(0, 0), Step::Retry(Duration::from_millis(200)));
        assert_eq!(next_step(0, 1), Step::Retry(Duration::from_secs(1)));
        assert_eq!(next_step(0, 2), Step::Exhausted);
        assert_eq!(RETRY_BACKOFF.len(), MAX_RETRIES);
    }

    #[test]
    fn only_single_queries_are_sent() {
        assert!(validate("SELECT [a] FROM [dbo].[t] WHERE [a] > 1").is_ok());
        assert!(validate("SELECT [a] FROM [dbo].[t]; DROP TABLE [dbo].[t]").is_err());
        assert!(validate("SELECT [x]; DROP TABLE t;--] FROM [dbo].[t]").is_err());
        assert!(validate("DELETE FROM [dbo].[t]").is_err());
    }
}
