//! Types `CREATE TABLE AS` stores for text computed from literals, as the
//! nightly monitoring models write it: one input of `UNION ALL` per source
//! table, each an aggregation over SQL Server naming its table in a literal
//! (`'extrato' AS tabela`) and a status in a `CASE` of literals. Checked
//! against Trino 431 reading the same SQL Server into the same metastore:
//! each engine stores its own table, and each reads the type the other
//! stored.
//!
//! Trino stores `varchar(longest literal)`; the engine stored unbounded
//! `varchar` (`string` in the metastore) as soon as a union had three
//! inputs whose first two agreed.

use std::sync::Arc;

use crate::common;
use crate::join_regressions::{Trino, note, open, shared, sorted, try_cl};
use crate::sqlserver_dynamic_filter::{SA_PASSWORD, exec, sqlserver, start_mssql};
use codelake_connectors::sqlserver::{SqlServerConnector, SqlServerParams};
use codelake_core::ByteSize;
use codelake_core::config::ExecutionConfig;
use secrecy::SecretString;

const SEED: [(&str, &str); 2] = [
    (
        "warehouse",
        "CREATE TABLE dbo.extrato (extratoid INT NOT NULL, empresaid INT NULL, deleted BIT NOT NULL);
         INSERT INTO dbo.extrato VALUES (1, 1, 0), (2, 1, 0), (2, 1, 0), (3, 2, 1);
         CREATE TABLE dbo.pessoas (pessoasid INT NOT NULL, gestorid INT NULL, deleted BIT NOT NULL);
         INSERT INTO dbo.pessoas VALUES (1, 1, 0), (2, 13, 0);
         CREATE TABLE dbo.vendreclogparc (id INT NOT NULL, obraid VARCHAR(10) NULL, deleted BIT NOT NULL);
         INSERT INTO dbo.vendreclogparc VALUES (1, 'OB1', 0), (1, 'OB1', 0);",
    ),
    (
        "erp1",
        "CREATE TABLE dbo.itensvenda (empresa_itv INT NULL, obra_itv VARCHAR(10) NULL);
         INSERT INTO dbo.itensvenda VALUES (1, 'OB1'), (2, 'OB2');",
    ),
];

/// One input per table, the first two with literals of one length.
fn branch(table: &str, id: &str, group: &str) -> String {
    format!(
        "SELECT '{table}' AS tabela, CAST({group} AS varchar) AS grupo, COUNT(*) AS ids, \
                CASE WHEN COUNT(*) > COUNT(DISTINCT {id}) THEN 'ALERTA' ELSE 'OK' END AS status \
         FROM warehouse.dbo.{table} WHERE CAST(deleted AS integer) = 0 GROUP BY {group}"
    )
}

/// `(table, query)`: what each engine stores (as `<table>_<engine>`).
fn tables() -> Vec<(&'static str, String)> {
    let integrity = [
        branch("extrato", "extratoid", "empresaid"),
        branch("pessoas", "pessoasid", "gestorid"),
        branch("vendreclogparc", "id", "obraid"),
    ]
    .join(" UNION ALL ");
    vec![
        ("integridade", integrity),
        // Tables of two catalogs joined, one input per pair; the first two
        // inputs agree on every type.
        (
            "conciliacao",
            "SELECT * FROM (SELECT 'itensvenda' AS tabela, COALESCE(o.obra_itv, d.obraid) AS grupo, \
                    CASE WHEN o.obra_itv IS NULL THEN 'CRITICO' ELSE 'OK' END AS status \
             FROM erp1.dbo.itensvenda o FULL OUTER JOIN warehouse.dbo.vendreclogparc d ON o.obra_itv = d.obraid) \
             UNION ALL SELECT * FROM (SELECT 'itensvendb' AS tabela, COALESCE(o.obra_itv, d.obraid) AS grupo, \
                    CASE WHEN d.obraid IS NULL THEN 'CRITICO' ELSE 'ALERTA' END AS status \
             FROM erp1.dbo.itensvenda o FULL OUTER JOIN warehouse.dbo.vendreclogparc d ON o.obra_itv = d.obraid) \
             UNION ALL SELECT * FROM (SELECT 'itensrecebidas' AS tabela, CAST(gestorid AS varchar) AS grupo, 'OK' AS status \
             FROM warehouse.dbo.pessoas)"
                .to_owned(),
        ),
        // `varchar(10)` of a column with unbounded text: unbounded.
        (
            "mistura",
            "SELECT obraid AS grupo FROM warehouse.dbo.vendreclogparc \
             UNION ALL SELECT obra_itv FROM erp1.dbo.itensvenda \
             UNION ALL SELECT CAST(empresaid AS varchar) FROM warehouse.dbo.extrato"
                .to_owned(),
        ),
    ]
}

fn columns(table: &str) -> String {
    format!(
        "SELECT column_name, data_type FROM hive.information_schema.columns \
         WHERE table_schema = 'utt' AND table_name = '{table}' ORDER BY ordinal_position"
    )
}

#[tokio::test]
async fn text_literals_of_every_input_store_trinos_varchar_length() {
    let mssql = start_mssql().await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    for (database, seed) in SEED {
        exec(&host, port, "master", &format!("CREATE DATABASE {database}")).await;
        exec(&host, port, database, seed).await;
    }
    let shared = shared().await;
    let catalog = |database: &str| {
        format!(
            "connector.name=sqlserver\n\
             connection-url=jdbc:sqlserver://{host}:{port};database={database};encrypt=false\n\
             connection-user=sa\n\
             connection-password={SA_PASSWORD}\n"
        )
    };
    let trino = Trino::start_with(
        &shared,
        "text-types",
        &[("warehouse", catalog("warehouse")), ("erp1", catalog("erp1"))],
    )
    .await;
    let spill = tempfile::tempdir().unwrap();
    let mut engine = open(
        &shared.warehouse(),
        spill.path(),
        ByteSize::mib(256),
        &ExecutionConfig::default(),
        &[],
    )
    .await;
    common::attach(&mut engine, sqlserver(&host, port)).await;
    let erp1 = SqlServerConnector::new(
        "erp1".into(),
        SqlServerParams {
            host: host.clone(),
            port,
            database: "erp1".into(),
            user: "sa".into(),
            password: SecretString::from(SA_PASSWORD),
            encrypt: "required".into(),
            trust_server_certificate: true,
            pool_size: 2,
        },
    );
    common::attach(&mut engine, Arc::new(erp1)).await;

    trino.batch(&["CREATE SCHEMA hive.utt"]).await;
    let mut failures = Vec::new();
    for (table, query) in tables() {
        // As dbt writes it.
        let create = |engine: &str| {
            format!(
                "create table \"hive\".\"utt\".\"{table}_{engine}\"\n WITH (format = 'PARQUET')\n as (\n{query}\n )"
            )
        };
        trino.batch(&[&create("tr")]).await;
        if let Err(error) = try_cl(&engine, &create("cl")).await {
            failures.push(format!("{table}: {error}"));
            continue;
        }
        let stored_by_trino = trino.batch(&[&columns(&format!("{table}_tr"))]).await.remove(0);
        let read_by_trino = trino.batch(&[&columns(&format!("{table}_cl"))]).await.remove(0);
        let stored = try_cl(&engine, &columns(&format!("{table}_cl"))).await;
        let read = try_cl(&engine, &columns(&format!("{table}_tr"))).await;
        note(
            table,
            format!("trino stored {stored_by_trino:?}, read the engine's {read_by_trino:?}"),
        );
        if read_by_trino != stored_by_trino {
            failures.push(format!(
                "{table}: trino stored {stored_by_trino:?}, reads the engine's table as {read_by_trino:?}"
            ));
        }
        for (what, answer) in [("stored", stored), ("read trino's", read)] {
            match answer {
                Ok(rows) if rows == stored_by_trino => {}
                other => failures.push(format!(
                    "{table}: the engine {what} {other:?}, trino {stored_by_trino:?}"
                )),
            }
        }
        let rows = |engine: &str| format!("SELECT * FROM hive.utt.{table}_{engine}");
        let theirs = sorted(trino.batch(&[&rows("tr")]).await.remove(0));
        match try_cl(&engine, &rows("cl")).await.map(sorted) {
            Ok(ours) if ours == theirs && !ours.is_empty() => {}
            other => failures.push(format!("{table}: rows {other:?}, trino {theirs:?}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
