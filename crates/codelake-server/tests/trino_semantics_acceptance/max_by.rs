//! `max_by` and `min_by` with a row or an array as the key, compared with
//! the reference engine over table columns: rows compared field by field,
//! arrays element by element, NULL keys skipped, a NULL field the comparison
//! reaches refused, NaN picked only when every key is NaN, and the `n` form.
//! The first query is the shape a production model uses (the latest
//! `dataregistro` by load time, then by itself).

use super::expect_same;

const SETUP: &[&str] = &[
    "CREATE SCHEMA {c}.sem_mxb",
    "CREATE TABLE {c}.sem_mxb.t (g integer, x integer, dataregistro timestamp(3), loaded timestamp(3), d double, a array(integer))",
    "INSERT INTO {c}.sem_mxb.t VALUES \
     (1, 1, TIMESTAMP '2024-01-01 10:00:00.000', TIMESTAMP '2024-01-05 00:00:00.000', 1.5, ARRAY[1, 2]), \
     (1, 2, TIMESTAMP '2024-01-02 10:00:00.000', NULL, nan(), ARRAY[1, 3]), \
     (1, 3, TIMESTAMP '2024-01-03 10:00:00.000', TIMESTAMP '2024-01-04 00:00:00.000', -2.0, ARRAY[1, 2, 0]), \
     (2, 4, TIMESTAMP '2024-02-01 10:00:00.000', NULL, 0.5, ARRAY[0]), \
     (2, 5, TIMESTAMP '2024-02-03 10:00:00.000', NULL, 3.0, NULL), \
     (3, 6, NULL, NULL, NULL, ARRAY[1, NULL])",
];

const QUERIES: &[&str] = &[
    "SELECT g, MAX_BY(CAST(dataregistro AS timestamp(3)), ROW(COALESCE(CAST(loaded AS timestamp(3)), \
     CAST(dataregistro AS timestamp(3))), CAST(dataregistro AS timestamp(3)))) AS dr_at_lastapplied \
     FROM {c}.sem_mxb.t GROUP BY g ORDER BY g",
    "SELECT g, min_by(x, ROW(COALESCE(loaded, dataregistro), dataregistro)) AS first_x \
     FROM {c}.sem_mxb.t GROUP BY g ORDER BY g",
    // A row whose second field decides; the NaN field ranks last either way.
    "SELECT g, max_by(x, ROW(g, d)) AS hi, min_by(x, ROW(g, d)) AS lo FROM {c}.sem_mxb.t GROUP BY g ORDER BY g",
    // Arrays: a prefix before the longer array; the NULL array is skipped.
    "SELECT g, max_by(x, a) AS hi, min_by(x, a) AS lo FROM {c}.sem_mxb.t WHERE g < 3 GROUP BY g ORDER BY g",
    "SELECT g, max_by(x, ROW(dataregistro, x), 2) AS top, min_by(x, ROW(dataregistro, x), 2) AS bottom \
     FROM {c}.sem_mxb.t GROUP BY g ORDER BY g",
    "SELECT max_by(x, d, 5) AS top, min_by(x, d, 5) AS bottom FROM {c}.sem_mxb.t",
    // A NaN key (the other is NULL) is picked when it is the only one.
    "SELECT max_by(x, d) AS hi, min_by(x, d) AS lo FROM {c}.sem_mxb.t WHERE x IN (2, 6)",
    // The only key of group 3 has a NULL field it is never compared on.
    "SELECT max_by(x, ROW(x, loaded)) AS sole FROM {c}.sem_mxb.t WHERE g = 3",
    // Two keys equal up to a NULL field: refused.
    "SELECT max_by(x, ROW(g, loaded)) AS refused FROM {c}.sem_mxb.t WHERE g = 2",
    "SELECT min_by(x, ROW(g, loaded), 2) AS refused FROM {c}.sem_mxb.t WHERE g = 2",
    "SELECT max_by(x, a) AS refused FROM {c}.sem_mxb.t WHERE x IN (1, 6)",
    "SELECT max_by(x, ARRAY[a[1], x]) AS arrays FROM {c}.sem_mxb.t WHERE g = 1",
];

#[tokio::test]
async fn row_and_array_keys_order_as_the_reference() -> super::Res<()> {
    expect_same(SETUP, QUERIES, None).await
}
