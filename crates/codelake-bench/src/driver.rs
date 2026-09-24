//! Client drivers. The timed part of a query is [`Driver::execute`]: send the
//! statement and receive every row. Turning rows into canonical cells happens
//! afterwards ([`Raw::canonicalize`]) and is not timed.
//!
//! - [`Driver::Postgres`]: `CodeLake` over the PostgreSQL wire (simple query
//!   protocol, text results), as BI tools and drivers use it.
//! - [`Driver::Trino`]: the Trino client REST protocol (`POST /v1/statement`,
//!   follow `nextUri`), as dbt-trino and scripts use it. Works against `CodeLake`
//!   unchanged once its Trino protocol endpoint ships (ROADMAP M1.3).
//! - [`Driver::Embedded`]: a `CodeLake` [`Engine`] inside the bench process,
//!   running `Engine::sql` (DDL routed to the Hive catalog). No client
//!   protocol at all, so it is **not** a like-for-like comparison with Trino;
//!   used only for writes until one of the network protocols routes DDL.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use codelake_core::Engine;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use serde_json::Value;

use crate::canonical::{CanonicalResult, Cell, Class, Column, hex};

#[derive(Debug, thiserror::Error)]
pub(crate) enum DriverError {
    #[error("{0}")]
    Query(String),
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    #[error("connection: {0}")]
    Connect(String),
}

/// Rows exactly as the protocol delivered them.
#[derive(Debug)]
pub(crate) enum Raw {
    /// Column names, type names (when known) and text values.
    Postgres {
        names: Vec<String>,
        types: Option<Vec<String>>,
        rows: Vec<Vec<Option<String>>>,
    },
    /// `(name, type)` and JSON values.
    Trino {
        columns: Vec<(String, String)>,
        rows: Vec<Vec<Value>>,
    },
    Arrow(Vec<RecordBatch>),
}

impl Raw {
    pub(crate) fn canonicalize(&self) -> Result<CanonicalResult, DriverError> {
        match self {
            Self::Postgres { names, types, rows } => Ok(canonical_postgres(names, types.as_deref(), rows)),
            Self::Trino { columns, rows } => Ok(canonical_trino(columns, rows)),
            Self::Arrow(batches) => canonical_arrow(batches),
        }
    }
}

fn guess_class(values: impl Iterator<Item = Option<String>>) -> Class {
    let mut class = None;
    for value in values.flatten() {
        let this = if crate::canonical::normalize_decimal(&value).is_some() {
            Class::Number
        } else if value.trim().parse::<f64>().is_ok() {
            Class::Float
        } else {
            return Class::Text;
        };
        class = Some(match (class, this) {
            (Some(Class::Float), _) | (_, Class::Float) => Class::Float,
            _ => Class::Number,
        });
    }
    class.unwrap_or(Class::Text)
}

fn canonical_postgres(
    names: &[String],
    types: Option<&[String]>,
    rows: &[Vec<Option<String>>],
) -> CanonicalResult {
    let columns: Vec<Column> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let known = types.and_then(|t| t.get(i));
            let class = known.map_or_else(
                || guess_class(rows.iter().map(|r| r.get(i).cloned().flatten())),
                |t| Class::from_postgres(t),
            );
            Column {
                name: name.clone(),
                engine_type: known.cloned().unwrap_or_else(|| "unknown".into()),
                class,
            }
        })
        .collect();
    let cells = rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(&columns)
                .map(|(value, column)| {
                    value
                        .as_deref()
                        .map_or(Cell::Null, |v| Cell::from_text(column.class, v))
                })
                .collect()
        })
        .collect();
    CanonicalResult::new(columns, cells)
}

fn trino_cell(class: Class, value: &Value) -> Cell {
    match value {
        Value::Null => Cell::Null,
        Value::Bool(b) => Cell::Bool(*b),
        Value::Number(n) => Cell::from_text(class, &n.to_string()),
        Value::String(s) if class == Class::Binary => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_or_else(|_| Cell::Text(s.clone()), |bytes| Cell::Text(hex(&bytes))),
        Value::String(s) => Cell::from_text(class, s),
        other => Cell::Text(other.to_string()),
    }
}

fn canonical_trino(columns: &[(String, String)], rows: &[Vec<Value>]) -> CanonicalResult {
    let columns: Vec<Column> = columns
        .iter()
        .map(|(name, engine_type)| Column {
            name: name.clone(),
            engine_type: engine_type.clone(),
            class: Class::from_trino(engine_type),
        })
        .collect();
    let cells = rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(&columns)
                .map(|(v, c)| trino_cell(c.class, v))
                .collect()
        })
        .collect();
    CanonicalResult::new(columns, cells)
}

fn canonical_arrow(batches: &[RecordBatch]) -> Result<CanonicalResult, DriverError> {
    let Some(first) = batches.first() else {
        return Ok(CanonicalResult::new(Vec::new(), Vec::new()));
    };
    let schema = first.schema();
    let columns: Vec<Column> = schema
        .fields()
        .iter()
        .map(|f| Column {
            name: f.name().clone(),
            engine_type: f.data_type().to_string(),
            class: Class::from_arrow(f.data_type()),
        })
        .collect();
    let options = FormatOptions::default();
    let mut rows = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DriverError::Query(e.to_string()))?;
        for row in 0..batch.num_rows() {
            let cells = batch
                .columns()
                .iter()
                .zip(&formatters)
                .zip(&columns)
                .map(|((array, formatter), column)| {
                    if array.is_null(row) {
                        Cell::Null
                    } else {
                        Cell::from_text(column.class, &formatter.value(row).to_string())
                    }
                })
                .collect();
            rows.push(cells);
        }
    }
    Ok(CanonicalResult::new(columns, rows))
}

/// Trino client protocol over plain HTTP.
#[derive(Debug, Clone)]
pub(crate) struct TrinoClient {
    http: reqwest::Client,
    base: String,
    user: String,
}

impl TrinoClient {
    pub(crate) fn new(base: &str, user: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: base.trim_end_matches('/').to_owned(),
            user: user.to_owned(),
        }
    }

    async fn json(&self, request: reqwest::RequestBuilder) -> Result<Value, DriverError> {
        let response = request
            .header("X-Trino-User", &self.user)
            .header("X-Trino-Source", "codelake-bench")
            .send()
            .await
            .map_err(|e| DriverError::Connect(e.to_string()))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| DriverError::Connect(e.to_string()))?;
        if !status.is_success() {
            return Err(DriverError::Query(format!(
                "HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }
        serde_json::from_slice(&body).map_err(|e| DriverError::Query(format!("invalid JSON: {e}")))
    }

    async fn run(&self, sql: &str, next: &mut Option<String>) -> Result<Raw, DriverError> {
        let mut page = self
            .json(
                self.http
                    .post(format!("{}/v1/statement", self.base))
                    .body(sql.to_owned()),
            )
            .await?;
        let mut columns = Vec::new();
        let mut rows = Vec::new();
        loop {
            if let Some(error) = page.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("query failed");
                return Err(DriverError::Query(message.to_owned()));
            }
            if columns.is_empty()
                && let Some(cols) = page.get("columns").and_then(Value::as_array)
            {
                columns = cols
                    .iter()
                    .map(|c| {
                        let text = |k: &str| c.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
                        (text("name"), text("type"))
                    })
                    .collect();
            }
            if let Some(data) = page.get("data").and_then(Value::as_array) {
                rows.extend(data.iter().map(|r| r.as_array().cloned().unwrap_or_default()));
            }
            match page.get("nextUri").and_then(Value::as_str) {
                Some(uri) => {
                    *next = Some(uri.to_owned());
                    page = self.json(self.http.get(uri)).await?;
                }
                None => break,
            }
        }
        *next = None;
        Ok(Raw::Trino { columns, rows })
    }

    pub(crate) async fn execute(&self, sql: &str, timeout: Duration) -> Result<Raw, DriverError> {
        let mut next = None;
        if let Ok(result) = tokio::time::timeout(timeout, self.run(sql, &mut next)).await {
            return result;
        }
        if let Some(uri) = next {
            // Cancel on the server; ignore failures, the query is abandoned anyway.
            let _ = self
                .http
                .delete(uri)
                .header("X-Trino-User", &self.user)
                .send()
                .await;
        }
        Err(DriverError::Timeout(timeout))
    }
}

/// `CodeLake` over the PostgreSQL wire protocol.
#[derive(Debug)]
pub(crate) struct PgClient {
    client: tokio_postgres::Client,
    /// Column type names per SQL text, from a one-off `Parse` (not timed).
    types: HashMap<String, Option<Vec<String>>>,
}

impl PgClient {
    pub(crate) async fn connect(conninfo: &str) -> Result<Self, DriverError> {
        let (client, connection) = tokio_postgres::connect(conninfo, tokio_postgres::NoTls)
            .await
            .map_err(|e| DriverError::Connect(e.to_string()))?;
        tokio::spawn(connection);
        Ok(Self {
            client,
            types: HashMap::new(),
        })
    }

    /// Learns result column types through the extended protocol, so text
    /// results can be classified exactly. Best effort: DDL cannot be prepared.
    pub(crate) async fn learn_types(&mut self, sql: &str) {
        if self.types.contains_key(sql) {
            return;
        }
        let types = self
            .client
            .prepare(sql)
            .await
            .ok()
            .map(|s| s.columns().iter().map(|c| c.type_().name().to_owned()).collect());
        self.types.insert(sql.to_owned(), types);
    }

    pub(crate) async fn execute(&self, sql: &str, timeout: Duration) -> Result<Raw, DriverError> {
        let messages = tokio::time::timeout(timeout, self.client.simple_query(sql))
            .await
            .map_err(|_| DriverError::Timeout(timeout))?
            .map_err(|e| {
                DriverError::Query(
                    e.as_db_error()
                        .map_or_else(|| e.to_string(), |d| d.message().to_owned()),
                )
            })?;
        let mut names = Vec::new();
        let mut rows = Vec::new();
        for message in messages {
            match message {
                tokio_postgres::SimpleQueryMessage::RowDescription(columns) if names.is_empty() => {
                    names = columns.iter().map(|c| c.name().to_owned()).collect();
                }
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    if names.is_empty() {
                        names = row.columns().iter().map(|c| c.name().to_owned()).collect();
                    }
                    rows.push((0..row.len()).map(|i| row.get(i).map(str::to_owned)).collect());
                }
                _ => {}
            }
        }
        let types = self
            .types
            .get(sql)
            .cloned()
            .flatten()
            .filter(|t| t.len() == names.len());
        Ok(Raw::Postgres { names, types, rows })
    }
}

/// A `CodeLake` engine in this process.
#[derive(Debug, Clone)]
pub(crate) struct EmbeddedClient {
    pub(crate) engine: Arc<Engine>,
}

impl EmbeddedClient {
    /// Builds the engine exactly like `codelake serve` does.
    pub(crate) async fn start(config: &std::path::Path) -> Result<Self, DriverError> {
        let validated =
            codelake_core::Config::load(config).map_err(|e| DriverError::Connect(e.to_string()))?;
        let codelake_core::ValidatedConfig { config, secrets } = validated;
        std::fs::create_dir_all(&config.memory.spill_dir).map_err(|e| DriverError::Connect(e.to_string()))?;
        let mut engine = Engine::new(&config.memory, &config.execution)
            .map_err(|e| DriverError::Connect(e.to_string()))?;
        let timeout = Duration::from_secs(config.execution.source_startup_timeout_secs);
        for source in &config.sources {
            let connector = codelake_connectors::build(source, &secrets)
                .map_err(|e| DriverError::Connect(e.to_string()))?;
            engine
                .attach(connector, source.required, timeout)
                .await
                .map_err(|e| DriverError::Connect(e.to_string()))?;
        }
        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    pub(crate) async fn execute(&self, sql: &str, timeout: Duration) -> Result<Raw, DriverError> {
        let run = async {
            let frame = self.engine.sql(sql).await?;
            frame.collect().await
        };
        tokio::time::timeout(timeout, run)
            .await
            .map_err(|_| DriverError::Timeout(timeout))?
            .map(Raw::Arrow)
            .map_err(|e| DriverError::Query(e.to_string()))
    }
}

/// The driver of one run.
#[derive(Debug)]
pub(crate) enum Driver {
    Postgres(PgClient),
    Trino(TrinoClient),
    Embedded(EmbeddedClient),
}

impl Driver {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Postgres(_) => "pg-wire",
            Self::Trino(_) => "trino-http",
            Self::Embedded(_) => "embedded",
        }
    }

    /// Untimed preparation for `sql` (PostgreSQL: learn column types).
    pub(crate) async fn prepare(&mut self, sql: &str) {
        if let Self::Postgres(pg) = self {
            pg.learn_types(sql).await;
        }
    }

    pub(crate) async fn execute(&self, sql: &str, timeout: Duration) -> Result<Raw, DriverError> {
        match self {
            Self::Postgres(pg) => pg.execute(sql, timeout).await,
            Self::Trino(trino) => trino.execute(sql, timeout).await,
            Self::Embedded(embedded) => embedded.execute(sql, timeout).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Decimal128Array, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn the_same_rows_from_every_protocol_are_equal() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(15, 2), true),
            Field::new("ratio", DataType::Float64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(
                    Decimal128Array::from(vec![Some(1050), None])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
                Arc::new(Float64Array::from(vec![0.5, 1e-5])),
                Arc::new(StringArray::from(vec!["ação", "b"])),
            ],
        )
        .unwrap();
        let arrow = Raw::Arrow(vec![batch]).canonicalize().unwrap();

        let trino = Raw::Trino {
            columns: vec![
                ("k".into(), "bigint".into()),
                ("amount".into(), "decimal(15,2)".into()),
                ("ratio".into(), "double".into()),
                ("name".into(), "varchar".into()),
            ],
            rows: vec![
                vec![
                    Value::from(2),
                    Value::Null,
                    serde_json::json!(1.0e-5),
                    Value::from("b"),
                ],
                vec![
                    Value::from(1),
                    Value::from("10.50"),
                    serde_json::json!(0.5),
                    Value::from("ação"),
                ],
            ],
        }
        .canonicalize()
        .unwrap();

        let text = |s: &str| Some(s.to_owned());
        let pg = Raw::Postgres {
            names: vec!["k".into(), "amount".into(), "ratio".into(), "name".into()],
            types: Some(vec![
                "int8".into(),
                "numeric".into(),
                "float8".into(),
                "text".into(),
            ]),
            rows: vec![
                vec![text("1"), text("10.5000"), text("0.5"), text("ação")],
                vec![text("2"), None, text("1e-05"), text("b")],
            ],
        }
        .canonicalize()
        .unwrap();

        for other in [&arrow, &pg] {
            let cmp = crate::canonical::compare(&trino, other);
            assert!(cmp.equal && cmp.notes.is_empty(), "{cmp:?}");
        }
    }

    #[test]
    fn untyped_postgres_text_is_classified_by_content() {
        let text = |s: &str| Some(s.to_owned());
        assert_eq!(
            guess_class([text("1"), None, text("2.5")].into_iter()),
            Class::Number
        );
        assert_eq!(guess_class([text("1"), text("1e-05")].into_iter()), Class::Float);
        assert_eq!(guess_class([text("1"), text("x")].into_iter()), Class::Text);
        assert_eq!(guess_class(std::iter::empty()), Class::Text);
    }

    #[test]
    fn trino_varbinary_is_compared_as_hex() {
        let cell = trino_cell(Class::Binary, &Value::from("AAAq"));
        assert_eq!(cell, Cell::Text("00002a".into()));
    }
}
