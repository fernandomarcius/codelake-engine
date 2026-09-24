//! Executes federated SQL on SQL Server and streams Arrow batches back.
//!
//! Memory: rows are converted into batches of `batch_size` rows and handed over
//! through a channel of capacity [`IN_FLIGHT_BATCHES`], so at most that many
//! batches exist per query no matter how large the remote result is.

use std::sync::Arc;

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
use tokio::sync::mpsc;

use super::dialect::{self, TsqlDialect};
use super::pool::Pool;
use super::types::BatchBuilder;
use codelake_core::federation_policy::restrict_federation;

/// Batches buffered between the SQL Server reader and the engine.
const IN_FLIGHT_BATCHES: usize = 2;

pub(crate) struct SqlServerExecutor {
    context: String,
    pool: Pool,
    batch_size: usize,
}

impl std::fmt::Debug for SqlServerExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlServerExecutor")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

impl SqlServerExecutor {
    pub(crate) fn new(context: String, pool: Pool, batch_size: usize) -> Self {
        Self {
            context,
            pool,
            batch_size,
        }
    }
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

async fn run(pool: Pool, sql: String, schema: SchemaRef, batch_size: usize, tx: &Sender) -> Result<()> {
    let mut conn = pool
        .get_owned()
        .await
        .map_err(|e| DataFusionError::Execution(format!("sqlserver pool: {e:?}")))?;
    conn.poisoned = true;
    let mut builder = BatchBuilder::new(schema, batch_size)?;
    {
        let query = async {
            let mut rows = conn
                .client
                .simple_query(sql.as_str())
                .await
                .map_err(external)?
                .into_row_stream();
            while let Some(row) = rows.try_next().await.map_err(external)? {
                // Consumer gone (query cancelled or LIMIT satisfied): stop; the
                // connection keeps `poisoned` and is discarded by the pool.
                if let Some(batch) = builder.push(row)?
                    && tx.send(Ok(batch)).await.is_err()
                {
                    return Ok(false);
                }
            }
            Ok::<bool, DataFusionError>(true)
        };
        // A slow remote aggregate sends nothing for a long time: notice the
        // cancellation while waiting instead of at the next batch.
        tokio::select! {
            finished = query => {
                if !finished? {
                    return Ok(());
                }
            }
            () = tx.closed() => return Ok(()),
        }
    }
    conn.poisoned = false;
    if let Some(batch) = builder.flush()? {
        let _ = tx.send(Ok(batch)).await;
    }
    Ok(())
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
) -> impl futures::Stream<Item = Result<datafusion::arrow::array::RecordBatch>> {
    futures::stream::once(async move {
        let (tx, rx) = mpsc::channel(IN_FLIGHT_BATCHES);
        let worker = tokio::spawn({
            let tx = tx.clone();
            async move { run(pool, sql, schema, batch_size, &tx).await }
        });
        // Supervisor: an error or a panic in the worker must reach the
        // consumer, never look like a clean end of stream.
        tokio::spawn(async move {
            let failure = match worker.await {
                Ok(Ok(())) => None,
                Ok(Err(err)) => Some(err),
                Err(join) => Some(DataFusionError::Execution(format!(
                    "sqlserver reader failed: {join}"
                ))),
            };
            if let Some(err) = failure {
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
        tracing::debug!(context = %self.context, sql = %query, "sqlserver query");
        let stream = lazy_stream(
            self.pool.clone(),
            query.to_owned(),
            Arc::clone(&schema),
            self.batch_size,
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
    use super::validate;

    #[test]
    fn only_single_queries_are_sent() {
        assert!(validate("SELECT [a] FROM [dbo].[t] WHERE [a] > 1").is_ok());
        assert!(validate("SELECT [a] FROM [dbo].[t]; DROP TABLE [dbo].[t]").is_err());
        assert!(validate("SELECT [x]; DROP TABLE t;--] FROM [dbo].[t]").is_err());
        assert!(validate("DELETE FROM [dbo].[t]").is_err());
    }
}
