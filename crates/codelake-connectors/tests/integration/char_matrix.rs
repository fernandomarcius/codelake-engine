//! `char(n)` semantics against Trino 431: every operation of a matrix, over
//! every kind of operand and every origin of the char value, answers Trino's
//! rows (`codelake_core::char_semantics`).
//!
//! - operations: comparisons (`=`, `<>`, `<`, `>=`, `IS DISTINCT FROM`,
//!   `IN`, `BETWEEN`, simple `CASE`), values in a common type (`CASE`,
//!   `coalesce`, `nullif`, `greatest`, `||`, and with the result of a text
//!   function whose length Trino derives), set operations (`UNION`,
//!   `UNION ALL` + `GROUP BY`, `INTERSECT`, `EXCEPT`), joins (`ON =`, `ON <`,
//!   `USING`), subqueries (`IN`, `NOT IN`, `= ANY`, scalar);
//! - the other operand: `char(3)`, `char(5)`, unbounded `varchar`,
//!   `varchar(3)` (tables Trino wrote), text literals and `NULL`;
//! - the origin of the `char(5)` value: a hive table Trino wrote, the same
//!   rows in an in-memory table of the engine, literals (`VALUES`), a
//!   subquery (`max(ch)`), a view Trino wrote, a hive table the engine wrote
//!   (`CREATE TABLE … AS`), read by both.
//!
//! Where Trino fails, the engine must fail too (a char concatenated with
//! unbounded text). The engine refuses, where Trino answers, only what
//! [`DOCUMENTED`] lists (ADR 0015). Every answer is appended to the evidence
//! logs `target/w1rt-evidence/char_matrix_{rows,sets}.log`, the counts by
//! origin and operation to `char_matrix_{rows,sets}_summary.log`.

use std::collections::BTreeMap;

use super::{Rows, Shared, cl, cl_try, note};

/// Refusals of the engine where Trino answers (ADR 0015).
const DOCUMENTED: &[&str] = &[];

/// The tables, written by Trino. `cht` gets two files: a filter must never
/// skip one by the statistics of its stored (unpadded) values.
const TRINO_SETUP: &[&str] = &[
    "CREATE TABLE hive.s.cht (id bigint, ch char(5)) WITH (format = 'PARQUET')",
    "INSERT INTO hive.s.cht VALUES (1, 'ab'), (3, NULL)",
    "INSERT INTO hive.s.cht VALUES (2, 'zz'), (4, 'a')",
    "CREATE TABLE hive.s.c3 (id bigint, c char(3)) WITH (format = 'PARQUET')",
    "INSERT INTO hive.s.c3 VALUES (1, 'ab'), (2, 'a  '), (3, 'zz')",
    "CREATE TABLE hive.s.vct (id bigint, s varchar) WITH (format = 'PARQUET')",
    "INSERT INTO hive.s.vct VALUES (1, 'ab'), (2, 'ab  '), (3, 'cd'), (4, NULL), (5, 'a')",
    "CREATE TABLE hive.s.v3 (id bigint, s varchar(3)) WITH (format = 'PARQUET')",
    "INSERT INTO hive.s.v3 VALUES (1, 'ab'), (2, 'a')",
    "CREATE VIEW hive.s.chv AS SELECT id, ch FROM hive.s.cht",
];

/// The same rows as `hive.s.cht` in the engine's in-memory catalog.
const MEMORY_SETUP: &[&str] = &[
    "CREATE TABLE mcht (id bigint, ch char(5))",
    "INSERT INTO mcht VALUES (1, 'ab'), (3, NULL), (2, 'zz'), (4, 'a')",
];

const LITERAL_ROWS: &str = "(SELECT * FROM (VALUES (BIGINT '1', CAST('ab' AS char(5))), (BIGINT '2', CAST('zz' AS char(5))), \
                            (BIGINT '3', CAST(NULL AS char(5))), (BIGINT '4', CAST('a' AS char(5)))) AS t(id, ch))";

/// Origin of the `char(5)` value: name, the engine's relation, Trino's.
fn origins() -> Vec<(&'static str, String, String)> {
    let both = |s: &str| (s.to_owned(), s.to_owned());
    let mut out = Vec::new();
    for (name, (codelake, trino)) in [
        ("hive", both("hive.s.cht")),
        ("memory", ("mcht".to_owned(), "hive.s.cht".to_owned())),
        ("literal", both(LITERAL_ROWS)),
        (
            "subquery",
            both("(SELECT id, max(ch) AS ch FROM hive.s.cht GROUP BY id)"),
        ),
        ("view", both("hive.s.chv")),
        ("ctas", both("hive.s.cht_cl")),
    ] {
        out.push((name, codelake, trino));
    }
    out
}

/// The other operand, from a table: name, relation, column.
const OTHERS: &[(&str, &str, &str)] = &[
    ("char(3)", "hive.s.c3", "c"),
    ("char(5)", "hive.s.cht", "ch"),
    ("varchar", "hive.s.vct", "s"),
    ("varchar(3)", "hive.s.v3", "s"),
];

/// The other operand, a literal.
const LITERALS: &[&str] = &["'ab'", "'ab  '", "'a'", "'abcdefg'", "'ab' || chr(9)", "NULL"];

/// Operations of one row: `{a}` the char value, `{b}` the other operand.
const ROW_OPS: &[(&str, &str)] = &[
    ("=", "{a} = {b}"),
    ("= reversed", "{b} = {a}"),
    ("<>", "{a} <> {b}"),
    ("<", "{a} < {b}"),
    (">= reversed", "{b} >= {a}"),
    ("IS DISTINCT FROM", "{a} IS DISTINCT FROM {b}"),
    ("IN", "{a} IN ({b}, 'zz')"),
    ("NOT IN", "{a} NOT IN ({b}, 'q')"),
    ("BETWEEN", "{a} BETWEEN {b} AND 'zz'"),
    ("simple CASE", "CASE {a} WHEN {b} THEN 1 ELSE 0 END"),
    ("CASE value", "CASE WHEN x.id = 1 THEN {a} ELSE {b} END"),
    ("coalesce", "coalesce({a}, {b})"),
    ("nullif", "nullif({a}, {b})"),
    ("greatest", "greatest({a}, {b})"),
    ("||", "{a} || {b}"),
    ("LIKE", "{a} LIKE 'ab%'"),
];

/// A char value output with the result of a text function of the other
/// operand: the common type's length comes from the length the function's
/// result has (Trino has no char version of these, so the other operand is
/// never char).
const FUNCTION_OPS: &[(&str, &str)] = &[
    (
        "CASE replace",
        "CASE WHEN x.id = 1 THEN {a} ELSE replace({b}, 'a', 'bc') END",
    ),
    ("coalesce replace", "coalesce({a}, replace({b}, 'b'))"),
    (
        "CASE regexp_replace",
        "CASE WHEN x.id = 1 THEN {a} ELSE regexp_replace({b}, 'b', 'yy') END",
    ),
    (
        "coalesce regexp_replace",
        "coalesce({a}, regexp_replace({b}, 'b'))",
    ),
    (
        "CASE soundex",
        "CASE WHEN x.id = 1 THEN {a} ELSE soundex({b}) END",
    ),
    (
        "CASE url_encode",
        "CASE WHEN x.id = 1 THEN {a} ELSE url_encode({b}) END",
    ),
    ("coalesce url_decode", "coalesce({a}, url_decode({b}))"),
    (
        "coalesce url_extract_host",
        "coalesce({a}, url_extract_host({b}))",
    ),
    (
        "coalesce json_extract_scalar",
        "coalesce({a}, json_extract_scalar({b}, '$.a'))",
    ),
    ("coalesce from_utf8", "coalesce({a}, from_utf8(to_utf8({b})))"),
    (
        "CASE element_at split",
        "CASE WHEN x.id = 1 THEN {a} ELSE element_at(split({b}, ','), 1) END",
    ),
    ("coalesce split subscript", "coalesce({a}, split({b}, ',')[1])"),
    (
        "coalesce regexp_extract_all",
        "coalesce({a}, regexp_extract_all({b}, '.')[1])",
    ),
    (
        "CASE to_iso8601",
        "CASE WHEN x.id = 1 THEN {a} ELSE to_iso8601(DATE '2020-01-01') END",
    ),
];

/// Operations over sets: `{C}` the char relation (columns `id`, `ch`), `{Y}`
/// the other (`y`, column `{y}`).
const SET_OPS: &[(&str, &str)] = &[
    (
        "UNION",
        "SELECT v FROM (SELECT ch AS v FROM {C} x UNION SELECT y.{y} FROM {Y} y) ORDER BY 1",
    ),
    (
        "UNION ALL GROUP BY",
        "SELECT v, count(*) FROM (SELECT ch AS v FROM {C} x UNION ALL SELECT y.{y} FROM {Y} y) GROUP BY v ORDER BY 1",
    ),
    (
        "count DISTINCT",
        "SELECT count(DISTINCT v) FROM (SELECT ch AS v FROM {C} x UNION ALL SELECT y.{y} FROM {Y} y)",
    ),
    (
        "INTERSECT",
        "SELECT v FROM (SELECT ch AS v FROM {C} x INTERSECT SELECT y.{y} FROM {Y} y) ORDER BY 1",
    ),
    (
        "EXCEPT",
        "SELECT v FROM (SELECT ch AS v FROM {C} x EXCEPT SELECT y.{y} FROM {Y} y) ORDER BY 1",
    ),
    (
        "EXCEPT reversed",
        "SELECT v FROM (SELECT y.{y} AS v FROM {Y} y EXCEPT SELECT ch FROM {C} x) ORDER BY 1",
    ),
    (
        "JOIN ON =",
        "SELECT x.id, y.id FROM {C} x JOIN {Y} y ON x.ch = y.{y} ORDER BY 1, 2",
    ),
    (
        "JOIN ON <",
        "SELECT x.id, y.id FROM {C} x JOIN {Y} y ON x.ch < y.{y} ORDER BY 1, 2",
    ),
    (
        "JOIN USING",
        "SELECT ch, length(ch) FROM {C} x JOIN (SELECT y.{y} AS ch FROM {Y} y) w USING (ch) ORDER BY 1",
    ),
    (
        "IN subquery",
        "SELECT id FROM {C} x WHERE ch IN (SELECT y.{y} FROM {Y} y) ORDER BY 1",
    ),
    (
        "IN subquery reversed",
        "SELECT id FROM {Y} y WHERE y.{y} IN (SELECT ch FROM {C} x) ORDER BY 1",
    ),
    (
        "NOT IN subquery",
        "SELECT id FROM {Y} y WHERE y.{y} NOT IN (SELECT ch FROM {C} x WHERE ch IS NOT NULL) ORDER BY 1",
    ),
    (
        "= ANY",
        "SELECT id FROM {C} x WHERE ch = ANY (SELECT y.{y} FROM {Y} y) ORDER BY 1",
    ),
    // Without a filter in the subquery: `DataFusion` 54 fails to plan
    // `< ALL (SELECT c FROM hive_table WHERE c IS NOT NULL)` for any text
    // column (ProjectionPushdown: "Input field name c does not match").
    (
        "< ALL",
        "SELECT id FROM {C} x WHERE ch < ALL (SELECT y.{y} FROM {Y} y) ORDER BY 1",
    ),
    (
        "> ANY",
        "SELECT id FROM {C} x WHERE ch > ANY (SELECT y.{y} FROM {Y} y) ORDER BY 1",
    ),
    (
        "scalar subquery",
        "SELECT id FROM {Y} y WHERE y.{y} = (SELECT max(ch) FROM {C} x WHERE id = 1) ORDER BY 1",
    ),
];

/// One query of the matrix, and the operation of each column it compares
/// (`None`: a column that identifies the row).
struct Case {
    origin: &'static str,
    columns: Vec<Option<String>>,
    codelake: String,
    trino: String,
}

/// Row operations, all but `||` together in one query per origin and
/// operand (`||` fails in Trino with unbounded text, which would fail the
/// others).
fn row_cases() -> Vec<Case> {
    let mut out = Vec::new();
    for (origin, codelake, trino) in origins() {
        let mut add = |columns: Vec<Option<String>>, template: String| {
            out.push(Case {
                origin,
                columns,
                codelake: template.replace("{C}", &codelake),
                trino: template.replace("{C}", &trino),
            });
        };
        let operands = OTHERS
            .iter()
            .map(|(other, relation, column)| {
                (
                    (*other).to_owned(),
                    format!("y.{column}"),
                    format!(" CROSS JOIN {relation} y"),
                    "x.id, y.id",
                )
            })
            .chain(LITERALS.iter().map(|literal| {
                (
                    format!("literal {literal}"),
                    (*literal).to_owned(),
                    String::new(),
                    "x.id",
                )
            }));
        for (other, b, join, ids) in operands {
            let keys = ids.split(", ").count();
            let mut groups = vec![
                ROW_OPS.iter().filter(|(op, _)| *op != "||").collect::<Vec<_>>(),
                ROW_OPS.iter().filter(|(op, _)| *op == "||").collect(),
            ];
            if !other.starts_with("char") {
                // Trino cannot choose between `json_extract_scalar`'s
                // varchar and json versions for an untyped `NULL` (it fails
                // the whole query, not a char question).
                groups.push(
                    FUNCTION_OPS
                        .iter()
                        .filter(|(op, _)| other != "literal NULL" || !op.contains("json_extract_scalar"))
                        .collect(),
                );
            }
            for group in groups {
                let exprs: Vec<String> = group
                    .iter()
                    .map(|(_, e)| e.replace("{a}", "x.ch").replace("{b}", &b))
                    .collect();
                let mut columns = vec![None; keys];
                columns.extend(group.iter().map(|(op, _)| Some(format!("{op} / {other}"))));
                let order: Vec<String> = (1..=keys).map(|k| k.to_string()).collect();
                add(
                    columns,
                    format!(
                        "SELECT {ids}, {} FROM {{C}} x{join} ORDER BY {}",
                        exprs.join(", "),
                        order.join(", ")
                    ),
                );
            }
        }
        add(
            vec![None, Some("VALUES / literal".into())],
            "SELECT v, count(*) FROM (SELECT ch AS v FROM {C} x UNION ALL SELECT * FROM (VALUES ('ab '), ('a'))) \
             GROUP BY v ORDER BY 1"
                .into(),
        );
        add(
            ["typeof", "typeof max", "typeof coalesce", "typeof ||"]
                .into_iter()
                .map(|c| Some(c.to_owned()))
                .collect(),
            "SELECT typeof(ch), typeof(max(ch)), typeof(coalesce(ch, 'x')), typeof(ch || 'x') FROM {C} x \
             GROUP BY ch ORDER BY 1 LIMIT 1"
                .into(),
        );
    }
    out
}

/// Set operations, joins and subqueries: one query each.
fn set_cases() -> Vec<Case> {
    let mut out = Vec::new();
    for (origin, codelake, trino) in origins() {
        for (other, relation, column) in OTHERS {
            for (op, template) in SET_OPS {
                let template = template.replace("{Y}", relation).replace("{y}", column);
                out.push(Case {
                    origin,
                    columns: vec![Some(format!("{op} / {other}"))],
                    codelake: template.replace("{C}", &codelake),
                    trino: template.replace("{C}", &trino),
                });
            }
        }
    }
    out
}

/// What became of one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Outcome {
    /// Same rows.
    Same,
    /// Both refused.
    BothRefuse,
    /// The engine refused, as [`DOCUMENTED`] says, where Trino answers.
    Documented,
    /// Anything else: a difference.
    Different,
}

/// Column `index` of every row.
fn column(rows: &Rows, index: usize) -> Vec<Option<&String>> {
    rows.iter().map(|row| row.get(index)).collect()
}

/// The outcome of each operation of `case`: the rows of its column (and
/// the columns that identify them) compared.
fn outcomes(
    case: &Case,
    codelake: &Result<Rows, String>,
    trino: &Result<Rows, String>,
) -> Vec<(String, Outcome)> {
    let keys: Vec<usize> = (0..case.columns.len())
        .filter(|i| case.columns[*i].is_none())
        .collect();
    // A query of one operation compares whole rows.
    let whole = case.columns.len() == 1;
    case.columns
        .iter()
        .enumerate()
        .filter_map(|(i, op)| Some((i, op.clone()?)))
        .map(|(i, op)| {
            let result = match (codelake, trino) {
                (Ok(a), Ok(b)) => {
                    let same = if whole {
                        a == b
                    } else {
                        a.len() == b.len()
                            && keys.iter().all(|k| column(a, *k) == column(b, *k))
                            && column(a, i) == column(b, i)
                    };
                    if same { Outcome::Same } else { Outcome::Different }
                }
                (Err(_), Err(_)) => Outcome::BothRefuse,
                (Err(e), Ok(_)) if DOCUMENTED.iter().any(|d| e.contains(d)) => Outcome::Documented,
                _ => Outcome::Different,
            };
            (op, result)
        })
        .collect()
}

/// A warehouse with the tables of the module documentation, Trino on it.
async fn setup(name: &str) -> Shared {
    let s = Shared::new(name).await;
    s.trino.ok("CREATE SCHEMA hive.s").await;
    for statement in TRINO_SETUP {
        s.trino.ok(statement).await;
    }
    for statement in MEMORY_SETUP {
        cl(&s.engine, statement).await;
    }
    cl(
        &s.engine,
        "CREATE TABLE hive.s.cht_cl AS SELECT id, ch FROM hive.s.cht",
    )
    .await;
    s
}

/// Runs `cases` on both engines; every operation must answer Trino's rows
/// (or be refused by both, or as [`DOCUMENTED`]).
async fn run(s: &Shared, probe: &str, cases: &[Case]) {
    let statements: Vec<String> = cases.iter().map(|c| c.trino.clone()).collect();
    let answers = s.trino.sql_batch(&statements).await;
    assert_eq!(answers.len(), cases.len(), "one Trino answer per case");
    let mut summary: BTreeMap<(&str, String), BTreeMap<Outcome, usize>> = BTreeMap::new();
    let mut differences = Vec::new();
    let mut operations = 0;
    for (case, trino) in cases.iter().zip(&answers) {
        let codelake = cl_try(&s.engine, &case.codelake).await;
        for (op, result) in outcomes(case, &codelake, trino) {
            operations += 1;
            note(
                probe,
                &format!("{} | {op} | {}", case.origin, case.codelake),
                (&result, &codelake, trino),
            );
            let class = op.split(" / ").next().unwrap_or_default().to_owned();
            *summary
                .entry((case.origin, class))
                .or_default()
                .entry(result)
                .or_default() += 1;
            if result == Outcome::Different {
                differences.push(format!(
                    "{} | {op} | {}: CodeLake {codelake:?} / Trino {trino:?}",
                    case.origin, case.codelake
                ));
            }
        }
    }
    let summary_probe = format!("{probe}_summary");
    for ((origin, class), counts) in &summary {
        note(&summary_probe, &format!("{origin} | {class}"), counts);
    }
    note(
        &summary_probe,
        "queries, operations, differences",
        (cases.len(), operations, differences.len()),
    );
    assert!(
        differences.is_empty(),
        "{} of {operations} operations differ from Trino 431: {differences:#?}",
        differences.len()
    );
}

/// Row operations of the matrix (see the module documentation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1t_char_matrix_row_operations_like_trino_431() {
    let s = setup("codelake-w1t-char-rows").await;
    run(&s, "char_matrix_rows", &row_cases()).await;
}

/// Set operations, joins and subqueries of the matrix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1t_char_matrix_set_operations_like_trino_431() {
    let s = setup("codelake-w1t-char-sets").await;
    run(&s, "char_matrix_sets", &set_cases()).await;
}
