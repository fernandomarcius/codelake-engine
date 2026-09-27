//! Trino's `system.metadata` and `system.jdbc` tables, which clients query to
//! discover what exists: dbt-trino lists relations through
//! `system.metadata.materialized_views`, the JDBC driver answers
//! `DatabaseMetaData` from `system.jdbc.*`.
//!
//! Nothing is fixed here: every scan walks the catalogs registered in the
//! querying session (plus `information_schema` when the session has it), so
//! catalogs attached, created or renamed later show up. Columns and types are
//! those of Trino 431 (`DESCRIBE system.jdbc.columns` on a real server).
//!
//! Filters on the catalog, schema and table name columns (`=`, `IN`, `LIKE`)
//! limit which relations are resolved — resolving a table can mean a round
//! trip to its source. They are reported as inexact, so the engine applies
//! them again: pruning never changes a result.
//!
//! What `CodeLake` does not have yet is listed empty, as Trino lists it for a
//! connector without the feature: materialized views, procedures, user
//! defined types, table and column comments, and catalog properties (no
//! connector declares `WITH (…)` properties besides the Hive `format`).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::information_schema::{INFORMATION_SCHEMA, InformationSchemaProvider};
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    Session, TableProvider,
};
use datafusion::common::ScalarValue;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::{
    ColumnarValue, Expr, Operator, ScalarUDF, TableProviderFilterPushDown, Volatility, create_udf,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use tracing::warn;

/// `version()` as Trino answers it: the server version, here the Trino
/// release whose protocol is implemented.
pub(crate) fn version_function(version: &str) -> Arc<ScalarUDF> {
    let version = version.to_owned();
    Arc::new(create_udf(
        "version",
        Vec::new(),
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(version.clone()))))),
    ))
}

/// Registers the `metadata` and `jdbc` schemas in the `system` catalog.
/// `connectors` names the connector serving each attached catalog.
pub(crate) fn register(
    system: &MemoryCatalogProvider,
    connectors: Vec<(String, &'static str)>,
) -> Result<()> {
    let connectors = Arc::new(connectors.into_iter().collect::<BTreeMap<_, _>>());
    for (schema_name, kinds) in [("metadata", Kind::METADATA), ("jdbc", Kind::JDBC)] {
        let schema = MemorySchemaProvider::new();
        for kind in kinds {
            schema.register_table(
                kind.name().to_owned(),
                Arc::new(SystemTable {
                    kind: *kind,
                    schema: kind.schema(),
                    connectors: Arc::clone(&connectors),
                }),
            )?;
        }
        system.register_schema(schema_name, Arc::new(schema))?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Catalogs,
    SchemaProperties,
    TableProperties,
    ColumnProperties,
    MaterializedViewProperties,
    AnalyzeProperties,
    MaterializedViews,
    TableComments,
    JdbcCatalogs,
    JdbcSchemas,
    JdbcTables,
    JdbcColumns,
    TableTypes,
    Types,
    Procedures,
    ProcedureColumns,
    PseudoColumns,
    SuperTables,
    SuperTypes,
    Udts,
    Attributes,
}

/// Column types of the system tables (Trino `varchar`, `bigint`, `boolean`,
/// `timestamp(9) with time zone`).
#[derive(Debug, Clone, Copy)]
enum ColumnType {
    Varchar,
    Bigint,
    Boolean,
    TimestampTz,
}

use ColumnType::{Bigint as BIGINT, Boolean as BOOLEAN, Varchar as VARCHAR};

impl ColumnType {
    fn arrow(self) -> DataType {
        match self {
            Self::Varchar => DataType::Utf8,
            Self::Bigint => DataType::Int64,
            Self::Boolean => DataType::Boolean,
            Self::TimestampTz => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        }
    }
}

type Columns = &'static [(&'static str, ColumnType)];

const MATERIALIZED_VIEWS_COLUMNS: Columns = &[
    ("catalog_name", VARCHAR),
    ("schema_name", VARCHAR),
    ("name", VARCHAR),
    ("storage_catalog", VARCHAR),
    ("storage_schema", VARCHAR),
    ("storage_table", VARCHAR),
    ("freshness", VARCHAR),
    ("last_fresh_time", ColumnType::TimestampTz),
    ("comment", VARCHAR),
    ("definition", VARCHAR),
];
const CATALOGS_COLUMNS: Columns = &[
    ("catalog_name", VARCHAR),
    ("connector_id", VARCHAR),
    ("connector_name", VARCHAR),
];

const TABLE_COMMENTS_COLUMNS: Columns = &[
    ("catalog_name", VARCHAR),
    ("schema_name", VARCHAR),
    ("table_name", VARCHAR),
    ("comment", VARCHAR),
];

const JDBC_CATALOGS_COLUMNS: Columns = &[("table_cat", VARCHAR)];

const JDBC_SCHEMAS_COLUMNS: Columns = &[("table_schem", VARCHAR), ("table_catalog", VARCHAR)];

const JDBC_TABLES_COLUMNS: Columns = &[
    ("table_cat", VARCHAR),
    ("table_schem", VARCHAR),
    ("table_name", VARCHAR),
    ("table_type", VARCHAR),
    ("remarks", VARCHAR),
    ("type_cat", VARCHAR),
    ("type_schem", VARCHAR),
    ("type_name", VARCHAR),
    ("self_referencing_col_name", VARCHAR),
    ("ref_generation", VARCHAR),
];

const JDBC_COLUMNS_COLUMNS: Columns = &[
    ("table_cat", VARCHAR),
    ("table_schem", VARCHAR),
    ("table_name", VARCHAR),
    ("column_name", VARCHAR),
    ("data_type", BIGINT),
    ("type_name", VARCHAR),
    ("column_size", BIGINT),
    ("buffer_length", BIGINT),
    ("decimal_digits", BIGINT),
    ("num_prec_radix", BIGINT),
    ("nullable", BIGINT),
    ("remarks", VARCHAR),
    ("column_def", VARCHAR),
    ("sql_data_type", BIGINT),
    ("sql_datetime_sub", BIGINT),
    ("char_octet_length", BIGINT),
    ("ordinal_position", BIGINT),
    ("is_nullable", VARCHAR),
    ("scope_catalog", VARCHAR),
    ("scope_schema", VARCHAR),
    ("scope_table", VARCHAR),
    ("source_data_type", BIGINT),
    ("is_autoincrement", VARCHAR),
    ("is_generatedcolumn", VARCHAR),
];

const TABLE_TYPES_COLUMNS: Columns = &[("table_type", VARCHAR)];

const TYPES_COLUMNS: Columns = &[
    ("type_name", VARCHAR),
    ("data_type", BIGINT),
    ("precision", BIGINT),
    ("literal_prefix", VARCHAR),
    ("literal_suffix", VARCHAR),
    ("create_params", VARCHAR),
    ("nullable", BIGINT),
    ("case_sensitive", BOOLEAN),
    ("searchable", BIGINT),
    ("unsigned_attribute", BOOLEAN),
    ("fixed_prec_scale", BOOLEAN),
    ("auto_increment", BOOLEAN),
    ("local_type_name", VARCHAR),
    ("minimum_scale", BIGINT),
    ("maximum_scale", BIGINT),
    ("sql_data_type", BIGINT),
    ("sql_datetime_sub", BIGINT),
    ("num_prec_radix", BIGINT),
];

const PROCEDURES_COLUMNS: Columns = &[
    ("procedure_cat", VARCHAR),
    ("procedure_schem", VARCHAR),
    ("procedure_name", VARCHAR),
    ("remarks", VARCHAR),
    ("procedure_type", BIGINT),
    ("specific_name", VARCHAR),
];

const PROCEDURE_COLUMNS_COLUMNS: Columns = &[
    ("procedure_cat", VARCHAR),
    ("procedure_schem", VARCHAR),
    ("procedure_name", VARCHAR),
    ("column_name", VARCHAR),
    ("column_type", BIGINT),
    ("data_type", BIGINT),
    ("type_name", VARCHAR),
    ("precision", BIGINT),
    ("length", BIGINT),
    ("scale", BIGINT),
    ("radix", BIGINT),
    ("nullable", BIGINT),
    ("remarks", VARCHAR),
    ("column_def", VARCHAR),
    ("sql_data_type", BIGINT),
    ("sql_datetime_sub", BIGINT),
    ("char_octet_length", BIGINT),
    ("ordinal_position", BIGINT),
    ("is_nullable", VARCHAR),
    ("specific_name", VARCHAR),
];

const PSEUDO_COLUMNS_COLUMNS: Columns = &[
    ("table_cat", VARCHAR),
    ("table_schem", VARCHAR),
    ("table_name", VARCHAR),
    ("column_name", VARCHAR),
    ("data_type", BIGINT),
    ("column_size", BIGINT),
    ("decimal_digits", BIGINT),
    ("num_prec_radix", BIGINT),
    ("column_usage", VARCHAR),
    ("remarks", VARCHAR),
    ("char_octet_length", BIGINT),
    ("is_nullable", BIGINT),
];

const SUPER_TABLES_COLUMNS: Columns = &[
    ("table_cat", VARCHAR),
    ("table_schem", VARCHAR),
    ("table_name", VARCHAR),
    ("supertable_name", VARCHAR),
];

const SUPER_TYPES_COLUMNS: Columns = &[
    ("type_cat", VARCHAR),
    ("type_schem", VARCHAR),
    ("type_name", VARCHAR),
    ("supertype_cat", VARCHAR),
    ("supertype_schem", VARCHAR),
    ("supertype_name", VARCHAR),
];

const UDTS_COLUMNS: Columns = &[
    ("type_cat", VARCHAR),
    ("type_schem", VARCHAR),
    ("type_name", VARCHAR),
    ("class_name", VARCHAR),
    ("data_type", VARCHAR),
    ("remarks", VARCHAR),
    ("base_type", VARCHAR),
];

const ATTRIBUTES_COLUMNS: Columns = &[
    ("type_cat", VARCHAR),
    ("type_schem", VARCHAR),
    ("type_name", VARCHAR),
    ("attr_name", VARCHAR),
    ("data_type", BIGINT),
    ("attr_type_name", VARCHAR),
    ("attr_size", BIGINT),
    ("decimal_digits", BIGINT),
    ("num_prec_radix", BIGINT),
    ("nullable", BIGINT),
    ("remarks", VARCHAR),
    ("attr_def", VARCHAR),
    ("sql_data_type", BIGINT),
    ("sql_datetime_sub", BIGINT),
    ("char_octet_length", BIGINT),
    ("ordinal_position", BIGINT),
    ("is_nullable", VARCHAR),
    ("scope_catalog", VARCHAR),
    ("scope_schema", VARCHAR),
    ("scope_table", VARCHAR),
    ("source_data_type", BIGINT),
];

/// Columns of the `system.metadata.*_properties` tables.
const PROPERTIES: Columns = &[
    ("catalog_name", VARCHAR),
    ("property_name", VARCHAR),
    ("default_value", VARCHAR),
    ("type", VARCHAR),
    ("description", VARCHAR),
];

impl Kind {
    const METADATA: &[Self] = &[
        Self::Catalogs,
        Self::SchemaProperties,
        Self::TableProperties,
        Self::ColumnProperties,
        Self::MaterializedViewProperties,
        Self::AnalyzeProperties,
        Self::MaterializedViews,
        Self::TableComments,
    ];
    const JDBC: &[Self] = &[
        Self::JdbcCatalogs,
        Self::JdbcSchemas,
        Self::JdbcTables,
        Self::JdbcColumns,
        Self::TableTypes,
        Self::Types,
        Self::Procedures,
        Self::ProcedureColumns,
        Self::PseudoColumns,
        Self::SuperTables,
        Self::SuperTypes,
        Self::Udts,
        Self::Attributes,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Catalogs | Self::JdbcCatalogs => "catalogs",
            Self::SchemaProperties => "schema_properties",
            Self::TableProperties => "table_properties",
            Self::ColumnProperties => "column_properties",
            Self::MaterializedViewProperties => "materialized_view_properties",
            Self::AnalyzeProperties => "analyze_properties",
            Self::MaterializedViews => "materialized_views",
            Self::TableComments => "table_comments",
            Self::JdbcSchemas => "schemas",
            Self::JdbcTables => "tables",
            Self::JdbcColumns => "columns",
            Self::TableTypes => "table_types",
            Self::Types => "types",
            Self::Procedures => "procedures",
            Self::ProcedureColumns => "procedure_columns",
            Self::PseudoColumns => "pseudo_columns",
            Self::SuperTables => "super_tables",
            Self::SuperTypes => "super_types",
            Self::Udts => "udts",
            Self::Attributes => "attributes",
        }
    }

    fn columns(self) -> Columns {
        match self {
            Self::Catalogs => CATALOGS_COLUMNS,
            Self::TableComments => TABLE_COMMENTS_COLUMNS,
            Self::JdbcCatalogs => JDBC_CATALOGS_COLUMNS,
            Self::JdbcSchemas => JDBC_SCHEMAS_COLUMNS,
            Self::JdbcTables => JDBC_TABLES_COLUMNS,
            Self::JdbcColumns => JDBC_COLUMNS_COLUMNS,
            Self::TableTypes => TABLE_TYPES_COLUMNS,
            Self::Types => TYPES_COLUMNS,
            Self::Procedures => PROCEDURES_COLUMNS,
            Self::ProcedureColumns => PROCEDURE_COLUMNS_COLUMNS,
            Self::PseudoColumns => PSEUDO_COLUMNS_COLUMNS,
            Self::SuperTables => SUPER_TABLES_COLUMNS,
            Self::SuperTypes => SUPER_TYPES_COLUMNS,
            Self::Udts => UDTS_COLUMNS,
            Self::Attributes => ATTRIBUTES_COLUMNS,
            Self::SchemaProperties
            | Self::TableProperties
            | Self::ColumnProperties
            | Self::MaterializedViewProperties
            | Self::AnalyzeProperties => PROPERTIES,
            Self::MaterializedViews => MATERIALIZED_VIEWS_COLUMNS,
        }
    }

    fn schema(self) -> SchemaRef {
        Arc::new(Schema::new(
            self.columns()
                .iter()
                .map(|(name, column_type)| Field::new(*name, column_type.arrow(), true))
                .collect::<Vec<_>>(),
        ))
    }

    /// Columns holding the catalog, schema and table names, used for pruning.
    fn name_columns(self) -> [Option<&'static str>; 3] {
        match self {
            Self::Catalogs => [Some("catalog_name"), None, None],
            Self::TableComments => [Some("catalog_name"), Some("schema_name"), Some("table_name")],
            Self::JdbcCatalogs => [Some("table_cat"), None, None],
            Self::JdbcSchemas => [Some("table_catalog"), Some("table_schem"), None],
            Self::JdbcTables | Self::JdbcColumns => {
                [Some("table_cat"), Some("table_schem"), Some("table_name")]
            }
            _ => [None, None, None],
        }
    }
}

/// One `system.metadata` or `system.jdbc` table.
#[derive(Debug)]
struct SystemTable {
    kind: Kind,
    schema: SchemaRef,
    connectors: Arc<BTreeMap<String, &'static str>>,
}

#[async_trait]
impl TableProvider for SystemTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| DataFusionError::Internal("system tables need a SessionState".into()))?;
        let rows = Rows {
            kind: self.kind,
            schema: Arc::clone(&self.schema),
            catalogs: Arc::clone(session.catalog_list()),
            information_schema: session.config().information_schema(),
            names: Names::from_filters(self.kind.name_columns(), filters),
            connectors: Arc::clone(&self.connectors),
        };
        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(rows)],
            projection,
            Vec::new(),
            false,
            limit,
        )?))
    }
}

/// A predicate on one name column that can prune relations.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Pattern {
    Equals(Vec<String>),
    Like {
        pattern: Vec<char>,
        escape: Option<char>,
    },
}

impl Pattern {
    fn matches(&self, name: &str) -> bool {
        match self {
            Self::Equals(values) => values.iter().any(|v| v == name),
            Self::Like { pattern, escape } => like(pattern, *escape, &name.chars().collect::<Vec<_>>()),
        }
    }
}

/// SQL `LIKE`: `%` any run, `_` any character, `escape` makes the next one literal.
fn like(pattern: &[char], escape: Option<char>, text: &[char]) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some((&c, rest)) if Some(c) == escape => match rest.split_first() {
            Some((&literal, rest)) => text.first() == Some(&literal) && like(rest, escape, &text[1..]),
            None => false,
        },
        Some(('%', rest)) => (0..=text.len()).any(|skip| like(rest, escape, &text[skip..])),
        Some(('_', rest)) => !text.is_empty() && like(rest, escape, &text[1..]),
        Some((&c, rest)) => text.first() == Some(&c) && like(rest, escape, &text[1..]),
    }
}

fn text_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
            _,
        ) => Some(value.clone()),
        _ => None,
    }
}

/// The column an expression reads unchanged (a cast between string types
/// keeps the value).
fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Cast(cast)
            if matches!(
                cast.field.data_type(),
                DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
            ) =>
        {
            column_name(&cast.expr)
        }
        _ => None,
    }
}

/// The name column and pattern a filter pins, if it is one this module understands.
fn pattern_of(filter: &Expr) -> Option<(&str, Pattern)> {
    match filter {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let (column, value) = match (column_name(&binary.left), text_literal(&binary.right)) {
                (Some(column), Some(value)) => (column, value),
                _ => (column_name(&binary.right)?, text_literal(&binary.left)?),
            };
            Some((column, Pattern::Equals(vec![value])))
        }
        Expr::InList(list) if !list.negated => {
            let values = list.list.iter().map(text_literal).collect::<Option<Vec<_>>>()?;
            Some((column_name(&list.expr)?, Pattern::Equals(values)))
        }
        Expr::Like(like) if !like.negated && !like.case_insensitive => Some((
            column_name(&like.expr)?,
            Pattern::Like {
                pattern: text_literal(&like.pattern)?.chars().collect(),
                escape: like.escape_char,
            },
        )),
        _ => None,
    }
}

/// Patterns every catalog, schema and table name must match.
#[derive(Debug, Clone, Default)]
struct Names {
    levels: [Vec<Pattern>; 3],
}

impl Names {
    fn from_filters(columns: [Option<&'static str>; 3], filters: &[Expr]) -> Self {
        let mut names = Self::default();
        for (column, pattern) in filters.iter().filter_map(pattern_of) {
            if let Some(level) = columns.iter().position(|c| *c == Some(column)) {
                names.levels[level].push(pattern);
            }
        }
        names
    }

    fn keeps(&self, level: usize, name: &str) -> bool {
        self.levels[level].iter().all(|pattern| pattern.matches(name))
    }
}

/// The rows of one scan, produced when the plan runs.
#[derive(Debug)]
struct Rows {
    kind: Kind,
    schema: SchemaRef,
    catalogs: Arc<dyn CatalogProviderList>,
    information_schema: bool,
    names: Names,
    connectors: Arc<BTreeMap<String, &'static str>>,
}

impl PartitionStream for Rows {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let rows = Self {
            kind: self.kind,
            schema: Arc::clone(&self.schema),
            catalogs: Arc::clone(&self.catalogs),
            information_schema: self.information_schema,
            names: self.names.clone(),
            connectors: Arc::clone(&self.connectors),
        };
        let schema = Arc::clone(&self.schema);
        let batch = futures::stream::once(async move { rows.batch(&context).await });
        Box::pin(RecordBatchStreamAdapter::new(schema, batch))
    }
}

type Row = Vec<ScalarValue>;

/// Called with each relation: catalog, schema, table and its provider, or
/// `None` when the source lists the table in a form `CodeLake` does not
/// implement (a Hive table in ORC, say): it is listed, without columns. Any
/// other failure (a source that does not answer) fails the scan, rather than
/// answering an incomplete listing.
type Visit<'a> = dyn FnMut(&str, &str, &str, Option<Arc<dyn TableProvider>>) -> Result<()> + Send + 'a;

fn text(value: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(value.to_owned()))
}

fn number(value: Option<i64>) -> ScalarValue {
    ScalarValue::Int64(value)
}

const NULL_TEXT: ScalarValue = ScalarValue::Utf8(None);
const NULL_NUMBER: ScalarValue = ScalarValue::Int64(None);

/// Bytes a row holds, for the memory reservation.
fn row_size(row: &Row) -> usize {
    row.iter().map(ScalarValue::size).sum()
}

impl Rows {
    /// Accumulates the rows, reserving their memory from the query's pool,
    /// and hands them over as one batch.
    async fn batch(&self, context: &TaskContext) -> Result<RecordBatch> {
        let reservation =
            MemoryConsumer::new(format!("system.{}", self.kind.name())).register(context.memory_pool());
        let mut rows: Vec<Row> = Vec::new();
        self.collect(&mut |row: Row| {
            reservation.try_grow(row_size(&row))?;
            rows.push(row);
            Ok(())
        })
        .await?;
        let columns = (0..self.schema.fields().len())
            .map(|index| {
                let field = self.schema.field(index);
                if rows.is_empty() {
                    return Ok(datafusion::arrow::array::new_empty_array(field.data_type()));
                }
                ScalarValue::iter_to_array(rows.iter().map(|row| row[index].clone()))
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .catalogs
            .catalog_names()
            .into_iter()
            .filter(|name| self.names.keeps(0, name))
            .collect();
        names.sort();
        names
    }

    /// Schemas of `catalog`, sorted, including `information_schema`.
    fn schemas(&self, catalog: &dyn CatalogProvider) -> Vec<(String, Arc<dyn SchemaProvider>)> {
        let mut schemas: Vec<(String, Arc<dyn SchemaProvider>)> = catalog
            .schema_names()
            .into_iter()
            .filter(|name| self.names.keeps(1, name))
            .filter_map(|name| catalog.schema(&name).map(|schema| (name, schema)))
            .collect();
        if self.information_schema
            && self.names.keeps(1, INFORMATION_SCHEMA)
            && !schemas.iter().any(|(name, _)| name == INFORMATION_SCHEMA)
        {
            schemas.push((
                INFORMATION_SCHEMA.to_owned(),
                Arc::new(InformationSchemaProvider::new(Arc::clone(&self.catalogs))),
            ));
        }
        schemas.sort_by(|a, b| a.0.cmp(&b.0));
        schemas
    }

    /// Every (catalog, schema, table) that passes the name filters, sorted.
    async fn relations(&self, each: &mut Visit<'_>) -> Result<()> {
        for catalog_name in self.catalog_names() {
            let Some(catalog) = self.catalogs.catalog(&catalog_name) else {
                continue;
            };
            for (schema_name, schema) in self.schemas(catalog.as_ref()) {
                let mut tables = schema.table_names();
                tables.sort();
                for table_name in tables.into_iter().filter(|t| self.names.keeps(2, t)) {
                    match schema.table(&table_name).await {
                        Ok(Some(table)) => each(&catalog_name, &schema_name, &table_name, Some(table))?,
                        // Dropped since the listing.
                        Ok(None) => {}
                        Err(error @ DataFusionError::NotImplemented(_)) => {
                            warn!(%catalog_name, %schema_name, %table_name, %error, "table listed but unreadable");
                            each(&catalog_name, &schema_name, &table_name, None)?;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Ok(())
    }

    async fn collect(&self, emit: &mut (dyn FnMut(Row) -> Result<()> + Send)) -> Result<()> {
        match self.kind {
            Kind::Catalogs => {
                for name in self.catalog_names() {
                    let connector = self
                        .connectors
                        .get(&name)
                        .copied()
                        .unwrap_or(match name.as_str() {
                            "system" => "system",
                            // A catalog no connector serves is DataFusion's in-memory one.
                            _ => "memory",
                        });
                    emit(vec![text(&name), text(&name), text(connector)])?;
                }
            }
            Kind::JdbcCatalogs => {
                for name in self.catalog_names() {
                    emit(vec![text(&name)])?;
                }
            }
            Kind::JdbcSchemas => {
                for catalog_name in self.catalog_names() {
                    if let Some(catalog) = self.catalogs.catalog(&catalog_name) {
                        for (schema, _) in self.schemas(catalog.as_ref()) {
                            emit(vec![text(&schema), text(&catalog_name)])?;
                        }
                    }
                }
            }
            Kind::JdbcTables => {
                self.relations(&mut |catalog, schema, table, provider| {
                    let view = provider.is_some_and(|p| is_view(schema, p.as_ref()));
                    let kind = if view { "VIEW" } else { "TABLE" };
                    let mut row = vec![text(catalog), text(schema), text(table), text(kind)];
                    row.resize(10, NULL_TEXT);
                    emit(row)
                })
                .await?;
            }
            Kind::TableComments => {
                self.relations(&mut |catalog, schema, table, _| {
                    emit(vec![text(catalog), text(schema), text(table), NULL_TEXT])
                })
                .await?;
            }
            Kind::JdbcColumns => {
                self.relations(&mut |catalog, schema, table, provider| {
                    let Some(provider) = provider else {
                        return Ok(());
                    };
                    let view = is_view(schema, provider.as_ref());
                    for (position, field) in provider.schema().fields().iter().enumerate() {
                        emit(column_row([catalog, schema, table], position, field, view))?;
                    }
                    Ok(())
                })
                .await?;
            }
            Kind::TableTypes => {
                for table_type in ["TABLE", "VIEW"] {
                    emit(vec![text(table_type)])?;
                }
            }
            Kind::Types => {
                for row in type_rows() {
                    emit(row)?;
                }
            }
            // Features `CodeLake` has none of: empty, as in Trino without them.
            Kind::SchemaProperties
            | Kind::TableProperties
            | Kind::ColumnProperties
            | Kind::MaterializedViewProperties
            | Kind::AnalyzeProperties
            | Kind::MaterializedViews
            | Kind::Procedures
            | Kind::ProcedureColumns
            | Kind::PseudoColumns
            | Kind::SuperTables
            | Kind::SuperTypes
            | Kind::Udts
            | Kind::Attributes => {}
        }
        Ok(())
    }
}

/// A view for Trino: `information_schema` relations are tables there, while
/// `DataFusion` implements them as views.
fn is_view(schema: &str, table: &dyn TableProvider) -> bool {
    schema != INFORMATION_SCHEMA && table.table_type() == TableType::View
}

/// One `system.jdbc.columns` row. View columns are nullable, as in Trino;
/// table columns follow the table's schema.
fn column_row([catalog, schema, table]: [&str; 3], position: usize, field: &Field, view: bool) -> Row {
    let column = jdbc_column(field.data_type());
    let nullable = view || field.is_nullable();
    vec![
        text(catalog),
        text(schema),
        text(table),
        text(field.name()),
        number(Some(column.code)),
        text(&column.name),
        number(column.size),
        number(Some(0)),
        number(column.digits),
        number(column.radix),
        number(Some(i64::from(nullable))),
        NULL_TEXT,
        NULL_TEXT,
        NULL_NUMBER,
        NULL_NUMBER,
        number(column.octets),
        number(i64::try_from(position + 1).ok()),
        text(if nullable { "YES" } else { "NO" }),
        NULL_TEXT,
        NULL_TEXT,
        NULL_TEXT,
        NULL_NUMBER,
        NULL_TEXT,
        NULL_TEXT,
    ]
}

/// How `system.jdbc.columns` describes a column type (`java.sql.Types` code,
/// Trino display name, size, fractional digits, radix, octet length).
#[derive(Debug, PartialEq, Eq)]
struct JdbcColumn {
    code: i64,
    name: String,
    size: Option<i64>,
    digits: Option<i64>,
    radix: Option<i64>,
    octets: Option<i64>,
}

const UNBOUNDED: i64 = 2_147_483_647;

fn precision(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// Characters of the fraction of a `p`-digit time value (`.` plus digits).
fn fraction(p: i64) -> i64 {
    if p > 0 { p + 1 } else { 0 }
}

/// Trino's `Type.getDisplayName()` for the Trino type an Arrow type is sent
/// as (see `types::trino_type`): `decimal(10,2)`, `map(varchar, integer)`.
fn display_name(data_type: &DataType) -> String {
    jdbc_column(data_type).name
}

/// Values observed on Trino 431 (`SELECT * FROM system.jdbc.columns`).
fn jdbc_column(data_type: &DataType) -> JdbcColumn {
    let plain = |code: i64, name: &str, size: Option<i64>, radix: Option<i64>| JdbcColumn {
        code,
        name: name.to_owned(),
        size,
        digits: None,
        radix,
        octets: None,
    };
    match data_type {
        DataType::Boolean => plain(16, "boolean", None, None),
        DataType::Int8 => plain(-6, "tinyint", Some(3), Some(10)),
        DataType::Int16 | DataType::UInt8 => plain(5, "smallint", Some(5), Some(10)),
        DataType::Int32 | DataType::UInt16 => plain(4, "integer", Some(10), Some(10)),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => plain(-5, "bigint", Some(19), Some(10)),
        DataType::Float32 => plain(7, "real", Some(24), Some(2)),
        DataType::Float64 => plain(8, "double", Some(53), Some(2)),
        DataType::Decimal128(p, s) => JdbcColumn {
            digits: Some(i64::from(*s)),
            ..plain(3, &format!("decimal({p},{s})"), Some(i64::from(*p)), Some(10))
        },
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            JdbcColumn {
                octets: Some(UNBOUNDED),
                ..plain(-3, "varbinary", Some(UNBOUNDED), None)
            }
        }
        DataType::Date32 | DataType::Date64 => plain(91, "date", Some(14), None),
        DataType::Timestamp(unit, None) => {
            let p = precision(*unit);
            JdbcColumn {
                digits: Some(p),
                ..plain(93, &format!("timestamp({p})"), Some(21 + fraction(p)), None)
            }
        }
        DataType::Timestamp(unit, Some(_)) => {
            let p = precision(*unit);
            JdbcColumn {
                digits: Some(p),
                ..plain(
                    2014,
                    &format!("timestamp({p}) with time zone"),
                    Some(55 + fraction(p)),
                    None,
                )
            }
        }
        DataType::Time32(unit) | DataType::Time64(unit) => {
            let p = precision(*unit);
            JdbcColumn {
                digits: Some(p),
                ..plain(92, &format!("time({p})"), Some(8 + fraction(p)), None)
            }
        }
        DataType::List(field) | DataType::LargeList(field) => plain(
            2003,
            &format!("array({})", display_name(field.data_type())),
            None,
            None,
        ),
        DataType::Map(entries, _) => {
            let name = match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => format!(
                    "map({}, {})",
                    display_name(fields[0].data_type()),
                    display_name(fields[1].data_type())
                ),
                other => format!("map({})", display_name(other)),
            };
            plain(2000, &name, None, None)
        }
        DataType::Struct(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|f| format!("{} {}", f.name(), display_name(f.data_type())))
                .collect();
            plain(2000, &format!("row({})", fields.join(", ")), None, None)
        }
        DataType::Null => plain(2000, "unknown", None, None),
        // Everything else travels as text (`types::trino_type`).
        _ => JdbcColumn {
            octets: Some(UNBOUNDED),
            ..plain(12, "varchar", Some(UNBOUNDED), None)
        },
    }
}

/// A `system.jdbc.types` entry: name, code, precision, searchable, radix.
type TypeInfo = (&'static str, i64, Option<i64>, i64, Option<i64>);

/// `system.jdbc.types`: the Trino types `CodeLake` produces, with the values
/// Trino 431 lists for them (parametric types by their base name).
fn type_rows() -> Vec<Row> {
    // (name, code, precision, searchable, radix)
    let types: [TypeInfo; 18] = [
        ("boolean", 16, None, 3, None),
        ("tinyint", -6, Some(3), 3, Some(10)),
        ("smallint", 5, Some(5), 3, Some(10)),
        ("integer", 4, Some(10), 3, Some(10)),
        ("bigint", -5, Some(19), 3, Some(10)),
        ("real", 7, Some(24), 3, Some(2)),
        ("double", 8, Some(53), 3, Some(2)),
        ("decimal", 2000, None, 0, None),
        ("varchar", 2000, None, 0, None),
        ("varbinary", -3, Some(UNBOUNDED), 3, None),
        ("date", 91, Some(14), 3, None),
        ("time", 2000, None, 0, None),
        ("timestamp", 2000, None, 0, None),
        ("timestamp with time zone", 2000, None, 0, None),
        ("array", 2003, None, 0, None),
        ("map", 2000, None, 0, None),
        ("row", 2000, None, 0, None),
        ("unknown", 2000, None, 3, None),
    ];
    types
        .into_iter()
        .map(|(name, code, precision, searchable, radix)| {
            vec![
                text(name),
                number(Some(code)),
                number(precision),
                NULL_TEXT,
                NULL_TEXT,
                NULL_TEXT,
                number(Some(1)),
                ScalarValue::Boolean(Some(false)),
                number(Some(searchable)),
                ScalarValue::Boolean(None),
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Boolean(None),
                NULL_TEXT,
                number(Some(0)),
                number(Some(0)),
                NULL_NUMBER,
                NULL_NUMBER,
                number(radix),
            ]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Fields;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn context() -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let system = MemoryCatalogProvider::new();
        register(&system, vec![("lake".into(), "hive")]).unwrap();
        ctx.register_catalog("system", Arc::new(system));
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        text.lines()
            .filter(|l| l.starts_with('|'))
            .skip(1)
            .map(|l| {
                l.split('|')
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    }

    #[tokio::test]
    async fn catalogs_come_from_the_session() {
        let ctx = context();
        assert_eq!(
            rows(&ctx, "SELECT * FROM system.metadata.catalogs ORDER BY 1").await,
            ["datafusion datafusion memory", "system system system"]
        );
        assert_eq!(
            rows(&ctx, "SELECT table_cat FROM system.jdbc.catalogs").await,
            ["datafusion", "system"]
        );
        assert_eq!(
            rows(
                &ctx,
                "SELECT count(*) FROM system.metadata.materialized_views WHERE catalog_name = 'datafusion'"
            )
            .await,
            ["0"]
        );
    }

    #[tokio::test]
    async fn tables_and_columns_follow_ddl() {
        let ctx = context();
        ctx.sql("CREATE SCHEMA datafusion.s").await.unwrap();
        ctx.sql("CREATE TABLE datafusion.s.t_1 (a bigint, b varchar, d decimal(10,2), ts timestamp)")
            .await
            .unwrap();
        ctx.sql("CREATE VIEW datafusion.s.v AS SELECT a FROM datafusion.s.t_1")
            .await
            .unwrap();
        ctx.sql("CREATE TABLE datafusion.s.tx1 (a bigint)").await.unwrap();
        assert_eq!(
            rows(
                &ctx,
                "SELECT table_cat, table_schem, table_name, table_type FROM system.jdbc.tables \
                 WHERE table_schem = 's'"
            )
            .await,
            [
                "datafusion s t_1 TABLE",
                "datafusion s tx1 TABLE",
                "datafusion s v VIEW"
            ]
        );
        // `_` is a wildcard unless escaped.
        assert_eq!(
            rows(
                &ctx,
                "SELECT table_name FROM system.jdbc.tables WHERE table_name LIKE 't\\_1' ESCAPE '\\'"
            )
            .await,
            ["t_1"]
        );
        assert_eq!(
            rows(
                &ctx,
                "SELECT column_name, data_type, type_name, column_size, decimal_digits, ordinal_position \
                 FROM system.jdbc.columns WHERE table_cat = 'datafusion' AND table_name IN ('t_1')"
            )
            .await,
            [
                "a -5 bigint 19 1",
                "b 12 varchar 2147483647 2",
                "d 3 decimal(10,2) 10 2 3",
                "ts 93 timestamp(9) 31 9 4",
            ]
        );
        assert_eq!(
            rows(
                &ctx,
                "SELECT table_schem FROM system.jdbc.schemas WHERE table_catalog = 'datafusion' ORDER BY 1"
            )
            .await,
            ["information_schema", "public", "s"]
        );
    }

    /// Lists `orc` (a format `CodeLake` does not read) and `down` (a source
    /// failure) depending on `broken`.
    #[derive(Debug)]
    struct Unreadable {
        broken: &'static str,
    }

    #[async_trait]
    impl SchemaProvider for Unreadable {
        fn table_names(&self) -> Vec<String> {
            vec![self.broken.to_owned()]
        }
        async fn table(&self, _: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            match self.broken {
                "orc" => Err(DataFusionError::NotImplemented(
                    "only PARQUET hive tables are supported".into(),
                )),
                _ => Err(DataFusionError::Execution("connection refused".into())),
            }
        }
        fn table_exist(&self, _: &str) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn unsupported_tables_are_listed_and_failures_fail() {
        let ctx = context();
        let lake = MemoryCatalogProvider::new();
        lake.register_schema("raw", Arc::new(Unreadable { broken: "orc" }))
            .unwrap();
        ctx.register_catalog("lake", Arc::new(lake));
        assert_eq!(
            rows(&ctx, "SELECT table_name, table_type FROM system.jdbc.tables WHERE table_cat = 'lake' AND table_schem = 'raw'").await,
            ["orc TABLE"]
        );
        assert!(
            rows(
                &ctx,
                "SELECT column_name FROM system.jdbc.columns WHERE table_cat = 'lake' AND table_schem = 'raw'"
            )
            .await
            .is_empty()
        );
        let down = MemoryCatalogProvider::new();
        down.register_schema("raw", Arc::new(Unreadable { broken: "down" }))
            .unwrap();
        ctx.register_catalog("down", Arc::new(down));
        let error = ctx
            .sql("SELECT * FROM system.jdbc.tables WHERE table_cat = 'down'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("connection refused"), "{error}");
    }

    #[test]
    fn like_patterns() {
        let pattern = |p: &str| p.chars().collect::<Vec<_>>();
        let text = |t: &str| t.chars().collect::<Vec<_>>();
        assert!(like(&pattern("t\\_1"), Some('\\'), &text("t_1")));
        assert!(!like(&pattern("t\\_1"), Some('\\'), &text("tx1")));
        assert!(like(&pattern("t_1"), None, &text("tx1")));
        assert!(like(&pattern("%ção%"), None, &text("ação!")));
        assert!(!like(&pattern("a%b"), None, &text("ab c")));
        assert!(!like(&pattern("a\\"), Some('\\'), &text("a")));
    }

    #[test]
    fn nested_type_names_match_trino() {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        );
        assert_eq!(
            display_name(&DataType::Map(Arc::new(entries), false)),
            "map(varchar, integer)"
        );
        let row = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Utf8, true),
        ]));
        assert_eq!(display_name(&row), "row(x integer, y varchar)");
        assert_eq!(
            jdbc_column(&DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))),
            JdbcColumn {
                code: 2014,
                name: "timestamp(3) with time zone".into(),
                size: Some(59),
                digits: Some(3),
                radix: None,
                octets: None,
            }
        );
        assert_eq!(
            jdbc_column(&DataType::Time64(TimeUnit::Microsecond)).size,
            Some(15)
        );
    }
}
