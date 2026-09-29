//! `SHOW CREATE TABLE` and `SHOW CREATE VIEW` compared with the reference
//! engine: the statement text (names quoted as it quotes them, types in SQL syntax,
//! `WITH (format = …)` of a hive table), its `varchar(n)` column, and the
//! errors on a relation of the other kind or a missing one.

use serde_json::{Value, json};

use super::{Pair, Res, expect_same, verdict};

const HIVE_SETUP: &[&str] = &[
    "CREATE SCHEMA hive.sem_sct",
    "CREATE TABLE hive.sem_sct.t (id bigint, n integer, s smallint, ti tinyint, b boolean, r real, d double, dc decimal(12,3), dcl decimal(30,5), v varchar, v10 varchar(10), ch char(3), dt date, ts timestamp, vb varbinary, \"from\" bigint, \"a b\" integer, \"a:b@c\" integer, \"Upper\" integer, \"end\" varchar) WITH (format = 'PARQUET')",
    "CREATE TABLE hive.sem_sct.c WITH (format = 'PARQUET') AS SELECT CAST(1.5 AS decimal(2,1)) AS x, 1 AS y, 'abc' AS z",
    "CREATE VIEW hive.sem_sct.v AS SELECT id, dc * 2 AS dc2 FROM hive.sem_sct.t WHERE id > 1",
    "CREATE VIEW hive.sem_sct.vi SECURITY INVOKER AS SELECT id, v FROM hive.sem_sct.t",
];

const HIVE_QUERIES: &[&str] = &[
    "SHOW CREATE TABLE hive.sem_sct.t",
    "SHOW CREATE TABLE hive.sem_sct.c",
    "SHOW CREATE VIEW hive.sem_sct.v",
    "SHOW CREATE VIEW hive.sem_sct.vi",
    // A view is not a table, nor a table a view.
    "SHOW CREATE TABLE hive.sem_sct.v",
    "SHOW CREATE VIEW hive.sem_sct.t",
    // Missing relation, schema or catalog.
    "SHOW CREATE TABLE hive.sem_sct.nope",
    "SHOW CREATE TABLE hive.nope.t",
    "SHOW CREATE TABLE nocat.sem_sct.t",
    "SHOW CREATE VIEW hive.sem_sct.nope",
];

#[tokio::test]
async fn hive_relations_show_create_as_the_reference() -> Res<()> {
    expect_same(HIVE_SETUP, HIVE_QUERIES, None).await
}

/// The answer with the in-memory catalog's name (`memory`, `datafusion`)
/// replaced in the statement, and the length of its `varchar(n)` column
/// taken from the replaced text.
fn without_catalog(answer: &Value, catalog: &str) -> Value {
    let Some(text) = answer["rows"][0][0].as_str() else {
        return answer.clone();
    };
    let text = text.replace(catalog, "CATALOG");
    json!({
        "types": [format!("varchar({})", text.chars().count())],
        "names": answer["names"],
        "rows": [[text]],
    })
}

/// Types the in-memory catalogs of both engines create, beyond hive's.
#[tokio::test]
async fn memory_relations_show_create_as_the_reference() -> Res<()> {
    let pair = Pair::start().await?;
    pair.setup(&[
        "CREATE SCHEMA {c}.sem_scm",
        "CREATE TABLE {c}.sem_scm.t (id bigint, tz timestamp(3) with time zone, t6 timestamp(6), dcl decimal(38,5), \"código\" integer, arr array(integer), mp map(varchar, bigint), rw row(x integer, \"y z\" varchar))",
        "CREATE VIEW {c}.sem_scm.v AS SELECT id, dcl FROM {c}.sem_scm.t",
    ])
    .await?;
    let mut wrong = Vec::new();
    for query in [
        "SHOW CREATE TABLE {c}.sem_scm.t",
        "SHOW CREATE VIEW {c}.sem_scm.v",
    ] {
        let (trino, codelake) = pair.both(query, None).await?;
        let (trino, codelake) = (
            without_catalog(&trino, "memory"),
            without_catalog(&codelake, "datafusion"),
        );
        if trino.get("error").is_some() || trino != codelake {
            wrong.push(format!("{query}\n  trino    {trino}\n  codelake {codelake}"));
        }
    }
    verdict(&wrong)
}
