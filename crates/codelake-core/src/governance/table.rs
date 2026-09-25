//! `system.runtime.queries`: the registry as a table, with the columns the
//! HTTP statement protocol's clients expect (ADR 0012). Queries of every
//! protocol are listed.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Int64Array, ListBuilder, RecordBatch, StringArray, StringBuilder, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{
    CatalogProvider, MemTable, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, Session,
    TableProvider,
};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use super::GLOBAL_GROUP;
use super::registry::{QueryInfo, QueryRegistry};

fn millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// `system.runtime.queries`.
pub struct QueriesTable {
    registry: Arc<QueryRegistry>,
    schema: SchemaRef,
}

impl std::fmt::Debug for QueriesTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueriesTable")
    }
}

impl QueriesTable {
    pub fn new(registry: Arc<QueryRegistry>) -> Self {
        let timestamp = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_id", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("user", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, true),
            Field::new("query", DataType::Utf8, false),
            Field::new(
                "resource_group_id",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("queued_time_ms", DataType::Int64, true),
            Field::new("analysis_time_ms", DataType::Int64, true),
            Field::new("planning_time_ms", DataType::Int64, true),
            Field::new("created", timestamp.clone(), true),
            Field::new("started", timestamp.clone(), true),
            Field::new("last_heartbeat", timestamp.clone(), true),
            Field::new("end", timestamp, true),
            Field::new("error_type", DataType::Utf8, true),
            Field::new("error_code", DataType::Utf8, true),
        ]));
        Self { registry, schema }
    }

    pub(crate) fn batch(&self) -> Result<RecordBatch> {
        let mut rows = self.registry.snapshot();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let text = |f: &dyn Fn(&QueryInfo) -> Option<String>| -> ArrayRef {
            Arc::new(rows.iter().map(|(_, i)| f(i)).collect::<StringArray>())
        };
        let number = |f: &dyn Fn(&QueryInfo) -> Duration| -> ArrayRef {
            Arc::new(
                rows.iter()
                    .map(|(_, i)| Some(duration_ms(f(i))))
                    .collect::<Int64Array>(),
            )
        };
        let time = |f: &dyn Fn(&QueryInfo) -> Option<SystemTime>| -> ArrayRef {
            Arc::new(
                rows.iter()
                    .map(|(_, i)| f(i).map(millis))
                    .collect::<TimestampMillisecondArray>()
                    .with_timezone("UTC"),
            )
        };
        // `["global"]`, or `["global", "<group>"]` for a configured group.
        let mut groups = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        )));
        for (_, info) in &rows {
            groups.values().append_value(GLOBAL_GROUP);
            if &*info.resource_group != GLOBAL_GROUP {
                groups.values().append_value(&*info.resource_group);
            }
            groups.append(true);
        }
        let columns = vec![
            Arc::new(
                rows.iter()
                    .map(|(id, _)| Some(id.as_str()))
                    .collect::<StringArray>(),
            ) as ArrayRef,
            text(&|i| Some(i.state.as_str().to_owned())),
            text(&|i| Some(i.user.clone())),
            text(&|i| i.source.clone()),
            text(&|i| Some(i.query.to_string())),
            Arc::new(groups.finish()),
            number(&|i| i.queued),
            number(&|i| i.analysis),
            number(&|i| i.planning),
            time(&|i| Some(i.created)),
            time(&|i| i.started),
            time(&|i| Some(i.last_heartbeat)),
            time(&|i| i.ended),
            text(&|i| i.failure.as_ref().map(|e| e.kind.clone())),
            text(&|i| i.failure.as_ref().map(|e| e.name.clone())),
        ];
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

#[async_trait]
impl TableProvider for QueriesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // A snapshot taken at scan time, like other system tables.
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![self.batch()?]])?;
        table.scan(state, projection, filters, limit).await
    }
}

/// Catalog `system` with schema `runtime` holding `queries`. Protocols may
/// add schemas of their own to it.
pub fn system_catalog(registry: &Arc<QueryRegistry>) -> Result<Arc<MemoryCatalogProvider>> {
    let runtime = MemorySchemaProvider::new();
    runtime.register_table(
        "queries".into(),
        Arc::new(QueriesTable::new(Arc::clone(registry))),
    )?;
    let catalog = MemoryCatalogProvider::new();
    catalog.register_schema("runtime", Arc::new(runtime))?;
    Ok(Arc::new(catalog))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::registry::{Failure, Protocol, RegistryLimits};
    use datafusion::arrow::array::{Array, AsArray};

    #[test]
    fn table_lists_queries_of_every_protocol() {
        let registry = Arc::new(QueryRegistry::new(RegistryLimits {
            history_size: 10,
            history_retention: Duration::from_mins(1),
            max_query_length: 100,
        }));
        let http = registry.register(Protocol::Http, "ana", Some("cli"), "SELECT 1");
        http.fail(Failure::new("SYNTAX_ERROR", "USER_ERROR", "bad"));
        let pg = registry.register(Protocol::Postgres, "bob", None, "SELECT 2");
        pg.set_resource_group(Arc::from("etl"));
        let table = QueriesTable::new(Arc::clone(&registry));
        let batch = table.batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 15);
        let by_user = |user: &str| {
            let users = batch.column(2).as_string::<i32>();
            (0..batch.num_rows()).find(|&i| users.value(i) == user).unwrap()
        };
        let state = batch.column(1).as_string::<i32>();
        let code = batch.column(14).as_string::<i32>();
        let kind = batch.column(13).as_string::<i32>();
        let (ana, bob) = (by_user("ana"), by_user("bob"));
        assert_eq!(state.value(ana), "FAILED");
        assert_eq!(code.value(ana), "SYNTAX_ERROR");
        assert_eq!(kind.value(ana), "USER_ERROR");
        assert_eq!(state.value(bob), "QUEUED");
        assert!(code.is_null(bob));
        let groups = batch.column(5).as_list::<i32>();
        let names = |row: usize| -> Vec<String> {
            let values = groups.value(row);
            let values = values.as_string::<i32>();
            (0..values.len()).map(|i| values.value(i).to_owned()).collect()
        };
        assert_eq!(names(ana), ["global"]);
        assert_eq!(names(bob), ["global", "etl"]);
    }
}
