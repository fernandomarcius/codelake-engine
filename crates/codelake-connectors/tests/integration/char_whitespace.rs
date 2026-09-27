//! `char(n)` values that end in whitespace other than a space (tab, line
//! feed, carriage return, vertical tab, form feed, U+001C, U+0085, U+00A0,
//! U+2003, U+3000), read from a hive table Trino wrote and through views
//! (written by Trino and by the engine), against Trino 431: only trailing
//! spaces are padding, every other character is part of the value.

use super::{Rows, Shared, cl_try, note};

const TRINO_SETUP: &[&str] = &[
    "CREATE TABLE hive.s.ws (id bigint, ch char(5)) WITH (format = 'PARQUET')",
    "INSERT INTO hive.s.ws VALUES (1, 'a' || chr(9)), (2, 'a' || chr(10)), (3, 'a' || chr(13)), \
     (4, 'a' || chr(11)), (5, 'a' || chr(12)), (6, 'a' || chr(12288)), (7, 'a' || chr(160)), \
     (8, 'a' || chr(8195)), (9, 'a' || chr(28)), (10, 'a'), (11, 'a' || chr(9) || 'b'), \
     (12, 'a' || chr(133)), (13, 'a  ')",
    "CREATE VIEW hive.s.wsv AS SELECT id, ch FROM hive.s.ws",
    "CREATE VIEW hive.s.wsv2 AS SELECT id, ch, CAST(ch AS varchar) AS v FROM hive.s.ws",
];

/// The view the engine writes over the same table.
const ENGINE_VIEW: &str = "CREATE VIEW hive.s.ewv AS SELECT id, ch FROM hive.s.ws";

/// The same SQL on both engines, for each relation holding the values.
const QUERIES: &[&str] = &[
    "SELECT id, to_hex(to_utf8(CAST(ch AS varchar))), length(ch) FROM {r} ORDER BY 1",
    "SELECT id FROM {r} WHERE ch = 'a' ORDER BY 1",
    "SELECT id FROM {r} WHERE ch = 'a' || chr(9) ORDER BY 1",
    "SELECT id FROM {r} WHERE ch < 'a' ORDER BY 1",
    "SELECT ch = 'a', count(*) FROM {r} GROUP BY 1 ORDER BY 1",
    "SELECT count(DISTINCT ch) FROM {r}",
    "SELECT a.id, b.id FROM hive.s.ws a JOIN {r} b ON a.ch = b.ch ORDER BY 1, 2",
    "SELECT id, to_hex(to_utf8(ltrim(ch))), to_hex(to_utf8(rtrim(ch))), length(ltrim(ch)) FROM {r} ORDER BY 1",
];

const RELATIONS: &[&str] = &["hive.s.ws", "hive.s.wsv", "hive.s.ewv"];

fn compare(
    wrong: &mut Vec<String>,
    sql: &str,
    codelake: &Result<Rows, String>,
    trino: &Result<Rows, String>,
) {
    note("char_whitespace", sql, (codelake, trino));
    if codelake != trino {
        wrong.push(format!("{sql}\n  codelake {codelake:?}\n  trino    {trino:?}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1t_char_values_ending_in_whitespace_like_trino_431() {
    let s = Shared::new("codelake-w1t-char-whitespace").await;
    s.trino.ok("CREATE SCHEMA hive.s").await;
    for statement in TRINO_SETUP {
        s.trino.ok(statement).await;
    }
    cl_try(&s.engine, ENGINE_VIEW)
        .await
        .unwrap_or_else(|e| panic!("{ENGINE_VIEW}: {e}"));
    let mut wrong = Vec::new();
    let statements: Vec<String> = RELATIONS
        .iter()
        .flat_map(|r| QUERIES.iter().map(move |q| q.replace("{r}", r)))
        .chain(std::iter::once(
            "SELECT id, to_hex(to_utf8(v)), length(ch) FROM hive.s.wsv2 ORDER BY 1".to_owned(),
        ))
        .collect();
    let answers = s.trino.sql_batch(&statements).await;
    for (sql, trino) in statements.iter().zip(answers) {
        let codelake = cl_try(&s.engine, sql).await;
        compare(&mut wrong, sql, &codelake, &trino);
    }
    assert!(
        wrong.is_empty(),
        "{} differences:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}
