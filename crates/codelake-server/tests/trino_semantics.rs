//! Trino semantics in the engine, against a real Trino 431.
//!
//! Every [`Case`] carries the golden Trino 431 answered (`trinodb/trino:431`,
//! `memory` catalog): column types, names and rows exactly as the Trino
//! protocol sends them, or the error name and message. Covered:
//!
//! - decimal arithmetic and aggregates (types, half-up rounding, overflow,
//!   38 digits, scale 0, negatives), the shapes of TPC-H Q01, Q08, Q14, Q17;
//! - casts from text to `timestamp(p)` and `timestamp(p) with time zone`;
//! - the `information_schema` tables dbt reads.
//!
//! `trino_431_answers_every_golden` runs every case on a real Trino 431
//! (`codelake-sem-trino-<pid>`), so each golden is Trino's observed
//! behaviour; `codelake_answers_every_golden` runs them on the `codelake`
//! binary. `{c}` is the catalog (`memory` in Trino, `datafusion` in
//! `CodeLake`), `{l}` a few TPC-H `lineitem` rows.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    reason = "test crate"
)]

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};

const BIN: &str = env!("CARGO_BIN_EXE_codelake");

struct Case {
    id: &'static str,
    /// Statements run once before the first case that names them.
    setup: &'static [&'static str],
    sql: &'static str,
    /// `{"types", "names", "rows"}` or `{"error", "message"}` (a `null`
    /// error name: only the message is compared; Trino 431 reports a `sum`
    /// overflow as `GENERIC_INTERNAL_ERROR`).
    expect: &'static str,
}

const LINEITEM: &str = "(VALUES (1, CAST(17 AS decimal(15,2)), CAST(21168.23 AS decimal(15,2)), CAST(0.04 AS decimal(15,2)), CAST(0.02 AS decimal(15,2))), (1, CAST(36 AS decimal(15,2)), CAST(45983.16 AS decimal(15,2)), CAST(0.09 AS decimal(15,2)), CAST(0.06 AS decimal(15,2))), (2, CAST(8 AS decimal(15,2)), CAST(13309.60 AS decimal(15,2)), CAST(0.10 AS decimal(15,2)), CAST(0.02 AS decimal(15,2))), (2, CAST(28 AS decimal(15,2)), CAST(28955.64 AS decimal(15,2)), CAST(0.09 AS decimal(15,2)), CAST(0.06 AS decimal(15,2))), (2, CAST(24 AS decimal(15,2)), CAST(22824.48 AS decimal(15,2)), CAST(0.10 AS decimal(15,2)), CAST(0.04 AS decimal(15,2))), (3, CAST(32 AS decimal(15,2)), CAST(49620.16 AS decimal(15,2)), CAST(0.07 AS decimal(15,2)), CAST(0.02 AS decimal(15,2))), (3, CAST(38 AS decimal(15,2)), CAST(44694.46 AS decimal(15,2)), CAST(0.00 AS decimal(15,2)), CAST(0.05 AS decimal(15,2)))) l(k, qty, price, disc, tax)";

/// A schema with a table of every type both engines create, and a view.
const IS_SETUP: &[&str] = &[
    "CREATE SCHEMA {c}.sem_is",
    "CREATE TABLE {c}.sem_is.t (a bigint, b integer, c smallint, d tinyint, e boolean, f real, g double, h decimal(10,2), i decimal(38,0), j varchar, m date, n timestamp, o timestamp(6))",
    "CREATE VIEW {c}.sem_is.v AS SELECT a, h, n FROM {c}.sem_is.t",
];

const CASES: &[Case] = &[
    Case {
        id: "dec_q01_shape",
        setup: &[],
        sql: "SELECT sum(qty), sum(price), sum(price * (1 - disc)), sum(price * (1 - disc) * (1 + tax)), avg(qty), avg(price), avg(disc), count(*) FROM {l} GROUP BY k ORDER BY k",
        expect: r#"{"types":["decimal(38, 2)","decimal(38, 2)","decimal(38, 4)","decimal(38, 6)","decimal(15, 2)","decimal(15, 2)","decimal(15, 2)","bigint"],"names":["_col0","_col1","_col2","_col3","_col4","_col5","_col6","_col7"],"rows":[["53.00","67151.39","62166.1764","65083.286952","26.50","33575.70","0.07",2],["60.00","65089.72","58870.3044","61512.536424","20.00","21696.57","0.10",3],["70.00","94314.62","90841.2088","93998.866776","35.00","47157.31","0.04",2]]}"#,
    },
    Case {
        id: "dec_q14_shape",
        setup: &[],
        sql: "SELECT 100.00 * sum(CASE WHEN k = 2 THEN price * (1 - disc) ELSE 0 END) / sum(price * (1 - disc)) AS promo FROM {l}",
        expect: r#"{"types":["decimal(38, 6)"],"names":["promo"],"rows":[["27.785042"]]}"#,
    },
    Case {
        id: "dec_q08_shape",
        setup: &[],
        sql: "SELECT sum(CASE WHEN k = 3 THEN price * (1 - disc) ELSE 0 END) / sum(price * (1 - disc)) AS share FROM {l}",
        expect: r#"{"types":["decimal(38, 4)"],"names":["share"],"rows":[["0.4287"]]}"#,
    },
    Case {
        id: "dec_q17_shape",
        setup: &[],
        sql: "SELECT sum(price) / 7.0 AS avg_yearly FROM {l} WHERE qty < 0.2 * 150",
        expect: r#"{"types":["decimal(38, 2)"],"names":["avg_yearly"],"rows":[["12322.56"]]}"#,
    },
    Case {
        id: "dec_avg_window",
        setup: &[],
        sql: "SELECT avg(price) OVER (ORDER BY price ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w FROM {l} ORDER BY price",
        expect: r#"{"types":["decimal(15, 2)"],"names":["w"],"rows":[["13309.60"],["17238.92"],["21996.36"],["25890.06"],["36825.05"],["45338.81"],["47801.66"]]}"#,
    },
    Case {
        id: "dec_avg_distinct",
        setup: &[],
        sql: "SELECT avg(DISTINCT disc), sum(DISTINCT disc) FROM {l}",
        expect: r#"{"types":["decimal(15, 2)","decimal(38, 2)"],"names":["_col0","_col1"],"rows":[["0.06","0.30"]]}"#,
    },
    Case {
        id: "dec_operators",
        setup: &[],
        sql: "SELECT CAST(-1 AS DECIMAL(5,1)) / CAST(4 AS DECIMAL(5,0)), CAST(1 AS DECIMAL(5,2)) / CAST(8 AS DECIMAL(5,0)), CAST(-1 AS DECIMAL(5,2)) / CAST(8 AS DECIMAL(5,0)), CAST(1.25 AS DECIMAL(5,2)) / 2, CAST(10 AS DECIMAL(5,0)) / CAST(3 AS DECIMAL(5,0)), CAST(20 AS DECIMAL(5,0)) / CAST(3 AS DECIMAL(5,0)), CAST(-20 AS DECIMAL(5,0)) / CAST(3 AS DECIMAL(5,0)), CAST(7.5 AS DECIMAL(5,1)) % CAST(2 AS DECIMAL(5,0)), CAST(-7.5 AS DECIMAL(5,1)) % 2, CAST(1 AS DECIMAL(12,2)) + 1, CAST(1 AS DECIMAL(12,2)) * CAST(3 AS SMALLINT), CAST(12.5 AS DECIMAL(12,2)) * CAST(3 AS TINYINT), CAST(1 AS DECIMAL(12,2)) - CAST(2 AS BIGINT)",
        expect: r#"{"types":["decimal(5, 1)","decimal(5, 2)","decimal(5, 2)","decimal(5, 2)","decimal(5, 0)","decimal(5, 0)","decimal(5, 0)","decimal(5, 1)","decimal(5, 1)","decimal(13, 2)","decimal(17, 2)","decimal(15, 2)","decimal(22, 2)"],"names":["_col0","_col1","_col2","_col3","_col4","_col5","_col6","_col7","_col8","_col9","_col10","_col11","_col12"],"rows":[["-0.3","0.13","-0.13","0.63","3","7","-7","1.5","-1.5","2.00","3.00","37.50","-1.00"]]}"#,
    },
    Case {
        id: "dec_literals",
        setup: &[],
        sql: "SELECT 1.5, 100.00, 007.50, 0.2, .5, 0.00, -1.5, 1e5, 2.5E-1, 1.5 * 2, 0.1 + 0.2",
        expect: r#"{"types":["decimal(2, 1)","decimal(5, 2)","decimal(3, 2)","decimal(1, 1)","decimal(1, 1)","decimal(2, 2)","decimal(2, 1)","double","double","decimal(12, 1)","decimal(2, 1)"],"names":["_col0","_col1","_col2","_col3","_col4","_col5","_col6","_col7","_col8","_col9","_col10"],"rows":[["1.5","100.00","7.50","0.2","0.5","0.00","-1.5",100000.0,0.25,"3.0","0.3"]]}"#,
    },
    Case {
        id: "dec_38_digits",
        setup: &[],
        sql: "SELECT CAST(1 AS DECIMAL(38,0)) / CAST(3 AS DECIMAL(38,18)), CAST(1 AS DECIMAL(38,1)) / CAST(3 AS DECIMAL(38,19)), CAST(1 AS DECIMAL(20,10)) * CAST(1 AS DECIMAL(20,10)), CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) - 1, -CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) + 1",
        expect: r#"{"types":["decimal(38, 18)","decimal(38, 19)","decimal(38, 20)","decimal(38, 0)","decimal(38, 0)"],"names":["_col0","_col1","_col2","_col3","_col4"],"rows":[["0.333333333333333333","0.3333333333333333333","1.00000000000000000000","99999999999999999999999999999999999998","-99999999999999999999999999999999999998"]]}"#,
    },
    Case {
        id: "dec_avg_sum_bounds",
        setup: &[],
        sql: "SELECT avg(x), avg(-x), count(*) FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) t(x)",
        expect: r#"{"types":["decimal(38, 0)","decimal(38, 0)","bigint"],"names":["_col0","_col1","_col2"],"rows":[["99999999999999999999999999999999999999","-99999999999999999999999999999999999999",2]]}"#,
    },
    Case {
        id: "dec_avg_half_up",
        setup: &[],
        sql: "SELECT avg(x), avg(y), sum(x) FROM (VALUES (CAST(-1.25 AS DECIMAL(5,2)), CAST(1 AS DECIMAL(3,0))), (CAST(-1.26 AS DECIMAL(5,2)), CAST(2 AS DECIMAL(3,0)))) t(x, y)",
        expect: r#"{"types":["decimal(5, 2)","decimal(3, 0)","decimal(38, 2)"],"names":["_col0","_col1","_col2"],"rows":[["-1.26","2","-2.51"]]}"#,
    },
    Case {
        id: "dec_scale_zero_nulls",
        setup: &[],
        sql: "SELECT x / y, x % y, x * y FROM (VALUES (CAST(7 AS DECIMAL(3,0)), CAST(2 AS DECIMAL(3,0))), (CAST(NULL AS DECIMAL(3,0)), CAST(2 AS DECIMAL(3,0))), (CAST(-7 AS DECIMAL(3,0)), CAST(2 AS DECIMAL(3,0)))) t(x, y)",
        expect: r#"{"types":["decimal(3, 0)","decimal(3, 0)","decimal(6, 0)"],"names":["_col0","_col1","_col2"],"rows":[["4","1","14"],[null,null,null],["-4","-1","-14"]]}"#,
    },
    Case {
        id: "dec_err_sum_overflow",
        setup: &[],
        sql: "SELECT sum(x) FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) t(x)",
        expect: r#"{"error":null,"message":"Decimal overflow"}"#,
    },
    Case {
        id: "dec_err_add_overflow",
        setup: &[],
        sql: "SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) + 1",
        expect: r#"{"error":"NUMERIC_VALUE_OUT_OF_RANGE","message":"Decimal overflow"}"#,
    },
    Case {
        id: "dec_err_rescale",
        setup: &[],
        sql: "SELECT CAST(0 AS DECIMAL(38,0)) / CAST(3 AS DECIMAL(38,19))",
        expect: r#"{"error":"NUMERIC_VALUE_OUT_OF_RANGE","message":"Decimal overflow"}"#,
    },
    Case {
        id: "dec_err_division_by_zero",
        setup: &[],
        sql: "SELECT CAST(1 AS DECIMAL(5,0)) / CAST(0 AS DECIMAL(5,0))",
        expect: r#"{"error":"DIVISION_BY_ZERO","message":"Division by zero"}"#,
    },
    Case {
        id: "dec_err_modulus_by_zero",
        setup: &[],
        sql: "SELECT CAST(1 AS DECIMAL(5,2)) % CAST(0 AS DECIMAL(5,0))",
        expect: r#"{"error":"DIVISION_BY_ZERO","message":"Division by zero"}"#,
    },
    Case {
        id: "ts_casts",
        setup: &[],
        sql: "SELECT CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP), CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP(3) WITH TIME ZONE), CAST('2026-01-01 10:00:00.1235' AS TIMESTAMP), CAST('2026-01-01 10:00:00.123456 +02:00' AS TIMESTAMP(6) WITH TIME ZONE), CAST('2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE), CAST('2026-01-01 23:59:59.9995' AS TIMESTAMP), CAST('2026-01-01 10:00:00.5' AS TIMESTAMP(0)), CAST('2026-1-1 1:2:3' AS TIMESTAMP), CAST(' 2026-01-01 10:00 ' AS TIMESTAMP(6)), CAST('2026-01-01 10:00:00 Etc/GMT+3' AS TIMESTAMP WITH TIME ZONE), CAST('2026-01-01 10:00:00 +00:00' AS TIMESTAMP(0) WITH TIME ZONE)",
        expect: r#"{"types":["timestamp(3)","timestamp(3) with time zone","timestamp(3)","timestamp(6) with time zone","timestamp(3) with time zone","timestamp(3)","timestamp(0)","timestamp(3)","timestamp(6)","timestamp(3) with time zone","timestamp(0) with time zone"],"names":["_col0","_col1","_col2","_col3","_col4","_col5","_col6","_col7","_col8","_col9","_col10"],"rows":[["2026-01-01 10:00:00.000","2026-01-01 10:00:00.000 America/Sao_Paulo","2026-01-01 10:00:00.124","2026-01-01 10:00:00.123456 +02:00","2026-01-01 10:00:00.000 UTC","2026-01-02 00:00:00.000","2026-01-01 10:00:01","2026-01-01 01:02:03.000","2026-01-01 10:00:00.000000","2026-01-01 10:00:00.000 -03:00","2026-01-01 10:00:00 UTC"]]}"#,
    },
    Case {
        id: "ts_cast_column",
        setup: &[],
        sql: "SELECT CAST(x AS TIMESTAMP) FROM (VALUES ('2026-01-01 10:00:00 America/Sao_Paulo'), ('2026-06-01 10:00:00.5 +02:00'), ('2026-01-01 10:00:00'), (NULL)) t(x) ORDER BY 1",
        expect: r#"{"types":["timestamp(3)"],"names":["_col0"],"rows":[["2026-01-01 10:00:00.000"],["2026-01-01 10:00:00.000"],["2026-06-01 10:00:00.500"],[null]]}"#,
    },
    Case {
        id: "ts_err_t_separator",
        setup: &[],
        sql: "SELECT CAST('2026-01-01T10:00:00' AS TIMESTAMP)",
        expect: r#"{"error":"INVALID_CAST_ARGUMENT","message":"Value cannot be cast to timestamp: 2026-01-01T10:00:00"}"#,
    },
    Case {
        id: "ts_err_unknown_zone",
        setup: &[],
        sql: "SELECT CAST('2026-01-01 10:00:00 Mars/Olympus' AS TIMESTAMP)",
        expect: r#"{"error":"INVALID_CAST_ARGUMENT","message":"Value cannot be cast to timestamp: 2026-01-01 10:00:00 Mars/Olympus"}"#,
    },
    Case {
        id: "ts_err_column",
        setup: &[],
        sql: "SELECT CAST(x AS TIMESTAMP) FROM (VALUES ('garbage')) t(x)",
        expect: r#"{"error":"INVALID_CAST_ARGUMENT","message":"Value cannot be cast to timestamp: garbage"}"#,
    },
    Case {
        id: "is_columns",
        setup: IS_SETUP,
        sql: "SELECT table_catalog = '{c}', table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type FROM {c}.information_schema.columns WHERE table_schema = 'sem_is' ORDER BY table_name, ordinal_position",
        expect: r#"{"types":["boolean","varchar","varchar","varchar","bigint","varchar","varchar","varchar"],"names":["_col0","table_schema","table_name","column_name","ordinal_position","column_default","is_nullable","data_type"],"rows":[[true,"sem_is","t","a",1,null,"YES","bigint"],[true,"sem_is","t","b",2,null,"YES","integer"],[true,"sem_is","t","c",3,null,"YES","smallint"],[true,"sem_is","t","d",4,null,"YES","tinyint"],[true,"sem_is","t","e",5,null,"YES","boolean"],[true,"sem_is","t","f",6,null,"YES","real"],[true,"sem_is","t","g",7,null,"YES","double"],[true,"sem_is","t","h",8,null,"YES","decimal(10,2)"],[true,"sem_is","t","i",9,null,"YES","decimal(38,0)"],[true,"sem_is","t","j",10,null,"YES","varchar"],[true,"sem_is","t","m",11,null,"YES","date"],[true,"sem_is","t","n",12,null,"YES","timestamp(3)"],[true,"sem_is","t","o",13,null,"YES","timestamp(6)"],[true,"sem_is","v","a",1,null,"YES","bigint"],[true,"sem_is","v","h",2,null,"YES","decimal(10,2)"],[true,"sem_is","v","n",3,null,"YES","timestamp(3)"]]}"#,
    },
    Case {
        id: "is_columns_shape",
        setup: IS_SETUP,
        sql: "SELECT * FROM {c}.information_schema.columns WHERE table_schema = 'no_such_schema'",
        expect: r#"{"types":["varchar","varchar","varchar","varchar","bigint","varchar","varchar","varchar"],"names":["table_catalog","table_schema","table_name","column_name","ordinal_position","column_default","is_nullable","data_type"],"rows":[]}"#,
    },
    Case {
        id: "is_tables",
        setup: IS_SETUP,
        sql: "SELECT table_catalog = '{c}', table_schema, table_name, table_type FROM {c}.information_schema.tables WHERE table_schema IN ('sem_is', 'information_schema') ORDER BY table_schema, table_name",
        expect: r#"{"types":["boolean","varchar","varchar","varchar"],"names":["_col0","table_schema","table_name","table_type"],"rows":[[true,"information_schema","applicable_roles","BASE TABLE"],[true,"information_schema","columns","BASE TABLE"],[true,"information_schema","enabled_roles","BASE TABLE"],[true,"information_schema","roles","BASE TABLE"],[true,"information_schema","schemata","BASE TABLE"],[true,"information_schema","table_privileges","BASE TABLE"],[true,"information_schema","tables","BASE TABLE"],[true,"information_schema","views","BASE TABLE"],[true,"sem_is","t","BASE TABLE"],[true,"sem_is","v","VIEW"]]}"#,
    },
    Case {
        id: "is_tables_shape",
        setup: IS_SETUP,
        sql: "SELECT * FROM {c}.information_schema.tables WHERE table_schema = 'no_such_schema'",
        expect: r#"{"types":["varchar","varchar","varchar","varchar"],"names":["table_catalog","table_schema","table_name","table_type"],"rows":[]}"#,
    },
    Case {
        id: "is_views",
        setup: IS_SETUP,
        sql: "SELECT table_catalog = '{c}', table_schema, table_name FROM {c}.information_schema.views WHERE table_schema = 'sem_is'",
        expect: r#"{"types":["boolean","varchar","varchar"],"names":["_col0","table_schema","table_name"],"rows":[[true,"sem_is","v"]]}"#,
    },
    Case {
        id: "is_views_shape",
        setup: IS_SETUP,
        sql: "SELECT * FROM {c}.information_schema.views WHERE table_schema = 'no_such_schema'",
        expect: r#"{"types":["varchar","varchar","varchar","varchar"],"names":["table_catalog","table_schema","table_name","view_definition"],"rows":[]}"#,
    },
    Case {
        id: "is_schemata",
        setup: IS_SETUP,
        sql: "SELECT catalog_name = '{c}', schema_name FROM {c}.information_schema.schemata WHERE schema_name IN ('sem_is', 'information_schema') ORDER BY schema_name",
        expect: r#"{"types":["boolean","varchar"],"names":["_col0","schema_name"],"rows":[[true,"information_schema"],[true,"sem_is"]]}"#,
    },
    Case {
        id: "is_schemata_shape",
        setup: IS_SETUP,
        sql: "SELECT * FROM {c}.information_schema.schemata WHERE schema_name = 'no_such_schema'",
        expect: r#"{"types":["varchar","varchar"],"names":["catalog_name","schema_name"],"rows":[]}"#,
    },
    Case {
        id: "is_info_schema_columns",
        setup: IS_SETUP,
        sql: "SELECT table_name, column_name, ordinal_position, is_nullable, data_type FROM {c}.information_schema.columns WHERE table_schema = 'information_schema' ORDER BY table_name, ordinal_position",
        expect: r#"{"types":["varchar","varchar","bigint","varchar","varchar"],"names":["table_name","column_name","ordinal_position","is_nullable","data_type"],"rows":[["applicable_roles","grantee",1,"YES","varchar"],["applicable_roles","grantee_type",2,"YES","varchar"],["applicable_roles","role_name",3,"YES","varchar"],["applicable_roles","is_grantable",4,"YES","varchar"],["columns","table_catalog",1,"YES","varchar"],["columns","table_schema",2,"YES","varchar"],["columns","table_name",3,"YES","varchar"],["columns","column_name",4,"YES","varchar"],["columns","ordinal_position",5,"YES","bigint"],["columns","column_default",6,"YES","varchar"],["columns","is_nullable",7,"YES","varchar"],["columns","data_type",8,"YES","varchar"],["enabled_roles","role_name",1,"YES","varchar"],["roles","role_name",1,"YES","varchar"],["schemata","catalog_name",1,"YES","varchar"],["schemata","schema_name",2,"YES","varchar"],["table_privileges","grantor",1,"YES","varchar"],["table_privileges","grantor_type",2,"YES","varchar"],["table_privileges","grantee",3,"YES","varchar"],["table_privileges","grantee_type",4,"YES","varchar"],["table_privileges","table_catalog",5,"YES","varchar"],["table_privileges","table_schema",6,"YES","varchar"],["table_privileges","table_name",7,"YES","varchar"],["table_privileges","privilege_type",8,"YES","varchar"],["table_privileges","is_grantable",9,"YES","varchar"],["table_privileges","with_hierarchy",10,"YES","varchar"],["tables","table_catalog",1,"YES","varchar"],["tables","table_schema",2,"YES","varchar"],["tables","table_name",3,"YES","varchar"],["tables","table_type",4,"YES","varchar"],["views","table_catalog",1,"YES","varchar"],["views","table_schema",2,"YES","varchar"],["views","table_name",3,"YES","varchar"],["views","view_definition",4,"YES","varchar"]]}"#,
    },
    Case {
        id: "is_empty_tables",
        setup: IS_SETUP,
        sql: "SELECT (SELECT count(*) FROM {c}.information_schema.table_privileges), (SELECT count(*) FROM {c}.information_schema.roles), (SELECT count(*) FROM {c}.information_schema.enabled_roles), (SELECT count(*) FROM {c}.information_schema.applicable_roles)",
        expect: r#"{"types":["bigint","bigint","bigint","bigint"],"names":["_col0","_col1","_col2","_col3"],"rows":[[0,0,0,0]]}"#,
    },
];

// ---------------------------------------------------------------- servers

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct CodeLake {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for CodeLake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl CodeLake {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (pg, http, port) = (free_port(), free_port(), free_port());
        let path = dir.path().join("codelake.toml");
        let mut file = std::fs::File::create(&path).unwrap();
        write!(
            file,
            r#"
[server]
pg_listen = "127.0.0.1:{pg}"
http_listen = "127.0.0.1:{http}"
shutdown_grace_secs = 1

[server.trino]
listen = "127.0.0.1:{port}"

[memory]
limit = "512MiB"
spill_dir = "{spill}"
spill_limit = "512MiB"
metadata_cache_limit = "8MiB"
"#,
            spill = dir.path().join("spill").display()
        )
        .unwrap();
        let child = Command::new(BIN)
            .args(["serve", "--config"])
            .arg(&path)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            child,
            port,
            _dir: dir,
        };
        wait_info(&server.base()).await;
        server
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

async fn wait_info(base: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(response) = reqwest::get(format!("{base}/v1/info")).await
            && response.status() == 200
            && let Ok(body) = response.text().await
            && body.contains("\"starting\":false")
        {
            return;
        }
        assert!(Instant::now() < deadline, "{base} did not start");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Counts the workers the reference server's coordinator lists as active: it
/// logs "SERVER STARTED" before its own worker registers, and a query sent in
/// between fails with "No nodes available to run query".
const ACTIVE_WORKERS: &str = "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'";

async fn trino_431() -> (ContainerAsync<GenericImage>, String) {
    const JVM: &str = "-server\n-Xmx1G\n-XX:+UseG1GC\n-XX:G1HeapRegionSize=32M\n\
        -XX:+ExplicitGCInvokesConcurrent\n-XX:+ExitOnOutOfMemoryError\n\
        -XX:-OmitStackTraceInFastThrow\n-XX:ReservedCodeCacheSize=150M\n\
        -Djdk.attach.allowAttachSelf=true\n-Djdk.nio.maxCachedBufferSize=2000000\n";
    let container = GenericImage::new("trinodb/trino", "431")
        .with_exposed_port(ContainerPort::Tcp(8080))
        .with_wait_for(WaitFor::message_on_either_std("SERVER STARTED"))
        .with_copy_to(
            CopyTargetOptions::new("/etc/trino/jvm.config").with_mode(0o644),
            JVM.as_bytes().to_vec(),
        )
        .with_container_name(format!("codelake-sem-trino-{}", std::process::id()))
        .with_startup_timeout(Duration::from_secs(240))
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(8080).await.unwrap();
    let base = format!("http://{host}:{port}");
    wait_info(&base).await;
    let deadline = Instant::now() + Duration::from_secs(90);
    while run(&base, "system", ACTIVE_WORKERS).await["rows"][0][0]
        .as_u64()
        .is_none_or(|workers| workers == 0)
    {
        assert!(Instant::now() < deadline, "Trino never listed an active worker");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // The memory connector answers NO_NODES_AVAILABLE until the node registers.
    let deadline = Instant::now() + Duration::from_secs(60);
    while run(
        &base,
        "memory",
        "SELECT count(*) FROM memory.information_schema.schemata",
    )
    .await
    .get("error")
    .is_some()
    {
        assert!(Instant::now() < deadline, "Trino never became ready");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (container, base)
}

// ---------------------------------------------------------------- protocol

async fn fetch(request: reqwest::RequestBuilder) -> Value {
    let response = request.send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("not JSON ({e}): {body}"))
}

/// A statement followed to its end: `{"types", "names", "rows"}` or
/// `{"error", "message"}`.
async fn run(base: &str, catalog: &str, sql: &str) -> Value {
    let client = reqwest::Client::new();
    let mut page = fetch(
        client
            .post(format!("{base}/v1/statement"))
            .header("X-Trino-User", "semantics")
            .header("X-Trino-Catalog", catalog)
            .header("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
            .timeout(Duration::from_secs(120))
            .body(sql.to_owned()),
    )
    .await;
    let mut columns: Option<Vec<Value>> = None;
    let mut rows: Vec<Value> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(240);
    loop {
        if columns.is_none() {
            columns = page.get("columns").and_then(Value::as_array).cloned();
        }
        if let Some(data) = page.get("data").and_then(Value::as_array) {
            rows.extend(data.iter().cloned());
        }
        if let Some(error) = page.get("error") {
            return json!({"error": error["errorName"], "message": error["message"]});
        }
        let Some(next) = page.get("nextUri").and_then(Value::as_str).map(str::to_owned) else {
            break;
        };
        assert!(Instant::now() < deadline, "{sql} did not finish");
        page = fetch(client.get(next).header("X-Trino-User", "semantics")).await;
    }
    let columns = columns.unwrap_or_default();
    json!({
        "types": columns.iter().map(|c| c["type"].clone()).collect::<Vec<_>>(),
        "names": columns.iter().map(|c| c["name"].clone()).collect::<Vec<_>>(),
        "rows": rows,
    })
}

fn sql(template: &str, catalog: &str) -> String {
    template.replace("{c}", catalog).replace("{l}", LINEITEM)
}

/// Whether `actual` is the golden `expected`. With `exact_errors` off (the
/// `CodeLake` side), an error matches by name and by containing Trino's
/// message: `CodeLake` prefixes it with the failing component
/// (`Arrow error: ...`), and words a division by zero as Arrow does, the same
/// for integers and decimals.
fn matches(expected: &Value, actual: &Value, exact_errors: bool) -> bool {
    let Some(error) = expected.get("error") else {
        return actual == expected;
    };
    let names_match = error.is_null() || actual.get("error") == Some(error);
    let (want, got) = (
        expected["message"].as_str().unwrap_or_default(),
        actual["message"].as_str().unwrap_or_default(),
    );
    let messages_match = if exact_errors {
        want == got
    } else {
        got.contains(want) || error == "DIVISION_BY_ZERO"
    };
    actual.get("error").is_some() && names_match && messages_match
}

/// Runs every case and returns the ones whose answer differs from the golden.
async fn mismatches(base: &str, catalog: &str, exact_errors: bool) -> Vec<String> {
    let mut prepared: Vec<&[&str]> = Vec::new();
    let mut wrong = Vec::new();
    for case in CASES {
        if !prepared.iter().any(|done| std::ptr::eq(*done, case.setup)) {
            for statement in case.setup {
                let outcome = run(base, catalog, &sql(statement, catalog)).await;
                assert!(outcome.get("error").is_none(), "{statement}: {outcome}");
            }
            prepared.push(case.setup);
        }
        let expected: Value = serde_json::from_str(case.expect).unwrap();
        let actual = run(base, catalog, &sql(case.sql, catalog)).await;
        if !matches(&expected, &actual, exact_errors) {
            wrong.push(format!(
                "{}:\n  expected {expected}\n  actual   {actual}",
                case.id
            ));
        }
    }
    wrong
}

#[tokio::test]
async fn trino_431_answers_every_golden() {
    let (_trino, base) = trino_431().await;
    let wrong = mismatches(&base, "memory", true).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[tokio::test]
async fn codelake_answers_every_golden() {
    let server = CodeLake::start().await;
    let wrong = mismatches(&server.base(), "datafusion", false).await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
