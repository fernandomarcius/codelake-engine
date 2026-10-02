//! What a PostgreSQL source holds: its schemas, the relations a query can read
//! and their columns, read from `pg_catalog` as the reference engine reads them.
//!
//! - Schemas: every one but the source's `information_schema` and the TOAST and
//!   temporary ones; `pg_catalog` is listed, without relations.
//! - Relations: tables, partitioned tables and their partitions, views,
//!   materialized views and foreign tables (`relkind` `r`, `p`, `v`, `m`,
//!   `f`), outside the system schemas (`pg_*`, `information_schema`). They are
//!   not filtered by privilege: reading one the user may not read fails at
//!   the source (`permission denied`), as on the reference engine.
//! - Columns: in order, with the Arrow type of [`super::types`] (and the
//!   precision it declares besides); an array column whose elements have no
//!   type there is left out, and so is a `numeric` without precision or with
//!   more than 38 digits under the catalog's `decimal_mapping = "strict"`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bb8_postgres::tokio_postgres::Client;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::{
    Error as DbError, GenericError,
};

use codelake_core::catalog::NameSource;

use super::Decimals;
use super::pool::{PgPool, db_error};
use super::types::{Column, arrow_type, declared};

const SCHEMAS: &str = "
SELECT nspname::text
FROM pg_catalog.pg_namespace
WHERE nspname <> 'information_schema' AND nspname !~ '^pg_(toast|temp_)'";

/// The relations a query can read, by schema.
const RELATIONS: &str = "
SELECT n.nspname::text, c.relname::text
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'";

/// The relations of `RELATIONS` in schema `$1` named one of `$2`.
const FIND: &str = "
SELECT c.relname::text
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
  AND n.nspname = $1 AND c.relname = ANY($2)";

/// The columns of relation `$2` of schema `$1`: name, type, its kind
/// (`typtype`) and category, an array's element type and kind and its
/// dimensions (declared, at least 1: a view does not keep them), a
/// composite's fields.
const COLUMNS: &str = "
SELECT a.attname::text,
       pg_catalog.format_type(a.atttypid, a.atttypmod),
       t.typtype::text,
       t.typcategory::text,
       pg_catalog.format_type(e.oid, a.atttypmod),
       e.typtype::text,
       greatest(a.attndims, 1)::int4,
       a.attnotnull,
       ARRAY(SELECT f.attname::text FROM pg_catalog.pg_attribute f
             WHERE f.attrelid = t.typrelid AND f.attnum > 0 AND NOT f.attisdropped ORDER BY f.attnum),
       ARRAY(SELECT pg_catalog.format_type(f.atttypid, f.atttypmod) FROM pg_catalog.pg_attribute f
             WHERE f.attrelid = t.typrelid AND f.attnum > 0 AND NOT f.attisdropped ORDER BY f.attnum)
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_type e ON e.oid = t.typelem AND t.typcategory = 'A'
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum";

/// Every listed schema with its relations, sorted.
pub(super) async fn list(client: &Client) -> Result<BTreeMap<String, Vec<String>>, GenericError> {
    let mut listing: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in client.query(SCHEMAS, &[]).await.map_err(db_error)? {
        listing.insert(row.try_get(0)?, Vec::new());
    }
    for row in client.query(RELATIONS, &[]).await.map_err(db_error)? {
        let schema: String = row.try_get(0)?;
        listing.entry(schema).or_default().push(row.try_get(1)?);
    }
    for tables in listing.values_mut() {
        tables.sort();
    }
    Ok(listing)
}

/// The relation of `schema` that `table` names: itself, else its lower-case
/// form (the rule of `codelake_core::catalog`).
async fn find(client: &Client, schema: &str, table: &str) -> Result<Option<String>, GenericError> {
    let lower = table.to_lowercase();
    let candidates = [table, lower.as_str()];
    let found: Vec<String> = client
        .query(FIND, &[&schema, &&candidates[..]])
        .await
        .map_err(db_error)?
        .iter()
        .map(|row| row.try_get(0))
        .collect::<Result<_, _>>()?;
    Ok(candidates
        .into_iter()
        .find(|candidate| found.iter().any(|name| name == candidate))
        .map(str::to_owned))
}

/// The Arrow schema of relation `schema`.`table`.
pub(super) async fn schema_of(
    client: &Client,
    schema: &str,
    table: &str,
    decimals: Decimals,
) -> Result<Schema, DbError> {
    let unreadable = |source: GenericError| DbError::UnableToGetSchema { source };
    let rows = client
        .query(COLUMNS, &[&schema, &table])
        .await
        .map_err(|e| unreadable(db_error(e)))?;
    if rows.is_empty() && find(client, schema, table).await.map_err(unreadable)?.is_none() {
        return Err(DbError::UndefinedTable {
            table_name: format!("{schema}.{table}"),
            source: format!("relation \"{schema}.{table}\" does not exist").into(),
        });
    }
    let mut fields = Vec::with_capacity(rows.len());
    let columns = rows.len();
    for row in rows {
        let read = || -> Result<Column, bb8_postgres::tokio_postgres::Error> {
            let names: Vec<String> = row.try_get(8)?;
            let types: Vec<String> = row.try_get(9)?;
            Ok(Column {
                name: row.try_get(0)?,
                type_name: row.try_get(1)?,
                kind: row.try_get(2)?,
                category: row.try_get(3)?,
                element: row.try_get(4)?,
                element_kind: row.try_get(5)?,
                dimensions: row.try_get(6)?,
                not_null: row.try_get(7)?,
                fields: names.into_iter().zip(types).collect(),
            })
        };
        let column = read().map_err(|e| unreadable(Box::new(e)))?;
        let Some(data_type) =
            arrow_type(&column, decimals).map_err(|type_name| DbError::UnsupportedDataType {
                data_type: type_name,
                field_name: column.name.clone(),
            })?
        else {
            continue;
        };
        let field = Field::new(&column.name, data_type, !column.not_null);
        fields.push(match declared(&column) {
            Some(declared) => field.with_metadata(declared.metadata()),
            None => field,
        });
    }
    if columns > 0 && fields.is_empty() {
        return Err(DbError::UnableToGetSchema {
            source: Box::new(NoSupportedColumns::new(format!("{schema}.{table}"), columns)),
        });
    }
    Ok(Schema::new(fields))
}

/// A relation whose every column is left out: the reference engine refuses it
/// (`NOT_FOUND`), and so does a query naming it; a listing of the catalog
/// lists it without columns.
#[derive(Debug)]
pub(super) struct NoSupportedColumns {
    relation: String,
    columns: usize,
}

impl NoSupportedColumns {
    pub(super) fn new(relation: impl Into<String>, columns: usize) -> Self {
        Self {
            relation: relation.into(),
            columns,
        }
    }
}

impl fmt::Display for NoSupportedColumns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NOT_FOUND: Table '{}' has no supported columns (all {} columns are not supported)",
            self.relation, self.columns
        )
    }
}

impl std::error::Error for NoSupportedColumns {}

/// The names of a source, for `codelake_core::catalog::LiveListing`.
pub(super) struct PgNames {
    pool: Arc<PgPool>,
}

impl PgNames {
    pub(super) fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }
}

impl fmt::Debug for PgNames {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PgNames")
    }
}

fn external(error: GenericError) -> DataFusionError {
    DataFusionError::External(error)
}

#[async_trait]
impl NameSource for PgNames {
    async fn list(&self) -> DfResult<BTreeMap<String, Vec<String>>> {
        let client = self.pool.client().await.map_err(external)?;
        list(&client).await.map_err(external)
    }

    async fn find(&self, schema: &str, table: &str) -> DfResult<Option<String>> {
        let client = self.pool.client().await.map_err(external)?;
        find(&client, schema, table).await.map_err(external)
    }
}
