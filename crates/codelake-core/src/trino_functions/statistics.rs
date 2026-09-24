//! Statistical math functions: `normal_cdf`, `inverse_normal_cdf`,
//! `wilson_interval_lower`, `wilson_interval_upper`, `cosine_similarity`.
//!
//! Trino computes the normal distribution with Commons Math (`Erf`); here
//! `erf`/`erfc` come from `libm` and the quantile is Acklam's approximation
//! refined by two Halley steps. Results agree with Trino within a few units in
//! the last place (the golden cases state the tolerance); the Wilson interval
//! follows Trino's operation order and matches bit for bit.

use std::f64::consts::{PI, SQRT_2};

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, MapArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ScalarUDF;

use super::support::{
    At, Call, DOUBLE, Func, as_f64, as_i64, coerce_each, float_array, floats, ints, invalid_argument,
    not_found,
};

fn map_f64_3(call: &Call<'_>, function: impl Fn(f64, f64, f64) -> Result<f64>) -> Result<ArrayRef> {
    let (first, second, third) = (
        floats(call.arg(0)?)?,
        floats(call.arg(1)?)?,
        floats(call.arg(2)?)?,
    );
    let out = (0..call.rows)
        .map(|row| match (first.at(row), second.at(row), third.at(row)) {
            (Some(x), Some(y), Some(z)) => function(x, y, z).map(Some),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(float_array(out))
}

fn normal_cdf_value(mean: f64, sd: f64, value: f64) -> Result<f64> {
    if sd <= 0.0 {
        return Err(invalid_argument("standardDeviation must be > 0"));
    }
    Ok(f64::midpoint(1.0, libm::erf((value - mean) / (sd * SQRT_2))))
}

/// Standard normal quantile. The upper half uses the symmetry
/// `Q(p) = -Q(1 - p)` (`1 - p` is exact there), so the refinement always runs
/// where `erfc` is small and keeps its precision.
fn standard_normal_quantile(p: f64) -> f64 {
    if p > 0.5 {
        -lower_quantile(1.0 - p)
    } else {
        lower_quantile(p)
    }
}

/// Acklam's rational approximation refined by two Halley steps, for p <= 0.5.
fn lower_quantile(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    const LOW: f64 = 0.024_25;
    let tail = |t: f64| {
        (((((C[0] * t + C[1]) * t + C[2]) * t + C[3]) * t + C[4]) * t + C[5])
            / ((((D[0] * t + D[1]) * t + D[2]) * t + D[3]) * t + 1.0)
    };
    let mut x = if p < LOW {
        tail((-2.0 * p.ln()).sqrt())
    } else if p <= 1.0 - LOW {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        -tail((-2.0 * (1.0 - p).ln()).sqrt())
    };
    for _ in 0..2 {
        let error = 0.5 * libm::erfc(-x / SQRT_2) - p;
        let u = error * (2.0 * PI).sqrt() * (x * x / 2.0).exp();
        x -= u / (1.0 + x * u / 2.0);
    }
    x
}

fn inverse_normal_cdf_value(mean: f64, sd: f64, p: f64) -> Result<f64> {
    if !(p > 0.0 && p < 1.0) {
        return Err(invalid_argument("p must be 0 > p > 1"));
    }
    if sd <= 0.0 {
        return Err(invalid_argument("sd must be > 0"));
    }
    Ok(mean + sd * standard_normal_quantile(p))
}

/// Wilson score interval bound, in Trino's operation order.
fn wilson(successes: i64, trials: i64, z: f64, lower: bool) -> Result<f64> {
    if successes < 0 {
        return Err(invalid_argument("number of successes must not be negative"));
    }
    if trials <= 0 {
        return Err(invalid_argument("number of trials must be positive"));
    }
    if successes > trials {
        return Err(invalid_argument(
            "number of successes must not be larger than number of trials",
        ));
    }
    if z < 0.0 {
        return Err(invalid_argument("z-score must not be negative"));
    }
    let s = super::cast::i64_to_f64(successes);
    let n = super::cast::i64_to_f64(trials);
    let p = s / n;
    let z2 = z * z;
    let left = p + z2 / (2.0 * n);
    let right = z * (p * (1.0 - p) / n + z2 / (4.0 * n * n)).sqrt();
    let denominator = 1.0 + z2 / n;
    Ok(if lower {
        (left - right) / denominator
    } else {
        (left + right) / denominator
    })
}

fn wilson_function(name: &'static str, invoke: super::support::InvokeFn) -> ScalarUDF {
    Func::new(
        name,
        |n, t| coerce_each(n, t, &[as_i64, as_i64, as_f64]),
        DOUBLE,
        invoke,
    )
    .udf()
}

fn wilson_column(call: &Call<'_>, lower: bool) -> Result<ArrayRef> {
    let (s, n, z) = (ints(call.arg(0)?)?, ints(call.arg(1)?)?, floats(call.arg(2)?)?);
    let out = (0..call.rows)
        .map(|i| match (s.at(i), n.at(i), z.at(i)) {
            (Some(s), Some(n), Some(z)) => wilson(s, n, z, lower).map(Some),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(float_array(out))
}

fn is_varchar_double_map(t: &DataType) -> bool {
    let DataType::Map(entries, _) = t else {
        return false;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return false;
    };
    fields.len() == 2
        && super::support::is_string(fields[0].data_type())
        && super::support::is_numeric(fields[1].data_type())
}

fn map_rows(array: &ArrayRef) -> Result<&MapArray> {
    array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Internal("expected map".into()))
}

/// Entries of one map row as (key, value) pairs; `None` for a NULL map.
/// Entries of a `map(varchar, double)` row.
type Entries = Vec<(String, Option<f64>)>;

fn entries(map: &MapArray, row: usize) -> Result<Option<Entries>> {
    if map.is_null(row) {
        return Ok(None);
    }
    let entries = map.value(row);
    let keys = cast(entries.column(0), &DataType::Utf8)?;
    let keys = super::support::strings(&keys)?;
    let values = cast(entries.column(1), &DataType::Float64)?;
    let values = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Internal("map values".into()))?;
    Ok(Some(
        (0..entries.len())
            .map(|i| (keys.value(i).to_owned(), values.at(i)))
            .collect(),
    ))
}

fn l2_norm(entries: &[(String, Option<f64>)]) -> Option<f64> {
    let mut sum = 0.0;
    for (_, value) in entries {
        let v = (*value)?;
        sum += v * v;
    }
    Some(sum.sqrt())
}

/// `dot(x, y) / (‖x‖ ‖y‖)` over the keys both maps share; NULL values make it NULL.
fn cosine(x: &[(String, Option<f64>)], y: &[(String, Option<f64>)]) -> Option<f64> {
    let mut dot = 0.0;
    for (key, value) in x {
        if let Some((_, other)) = y.iter().find(|(k, _)| k == key) {
            dot += (*value)? * (*other)?;
        }
    }
    Some(dot / (l2_norm(x)? * l2_norm(y)?))
}

fn cosine_similarity() -> ScalarUDF {
    Func::new(
        "cosine_similarity",
        |name, types| match types {
            [a, b] if is_varchar_double_map(a) && is_varchar_double_map(b) => Ok(types.to_vec()),
            _ => Err(not_found(name, types)),
        },
        DOUBLE,
        |call| {
            let (x, y) = (map_rows(call.arg(0)?)?, map_rows(call.arg(1)?)?);
            let out = (0..call.rows)
                .map(|i| {
                    Ok(match (entries(x, i)?, entries(y, i)?) {
                        (Some(a), Some(b)) => cosine(&a, &b),
                        _ => None,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(float_array(out))
        },
    )
    .udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        Func::new(
            "normal_cdf",
            |n, t| coerce_each(n, t, &[as_f64, as_f64, as_f64]),
            DOUBLE,
            |call| map_f64_3(call, normal_cdf_value),
        )
        .udf(),
        Func::new(
            "inverse_normal_cdf",
            |n, t| coerce_each(n, t, &[as_f64, as_f64, as_f64]),
            DOUBLE,
            |call| map_f64_3(call, inverse_normal_cdf_value),
        )
        .udf(),
        wilson_function("wilson_interval_lower", |call| wilson_column(call, true)),
        wilson_function("wilson_interval_upper", |call| wilson_column(call, false)),
        cosine_similarity(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wilson_matches_trino_bit_for_bit() {
        assert_eq!(
            wilson(1, 5, 1.96, true).unwrap().to_bits(),
            0.036_223_160_969_787_456_f64.to_bits()
        );
        assert_eq!(
            wilson(1, 5, 1.96, false).unwrap().to_bits(),
            0.624_471_735_881_461_2_f64.to_bits()
        );
        assert_eq!(
            wilson(0, 10, 2.576, true).unwrap().to_bits(),
            3.336_853_732_056_613e-17_f64.to_bits()
        );
        assert!(wilson(6, 5, 1.96, true).is_err());
        assert!(wilson(1, 0, 1.96, true).is_err());
    }

    #[test]
    fn normal_quantile_inverts_the_cdf() {
        for p in [1e-10, 0.01, 0.024_25, 0.3, 0.5, 0.9, 0.975, 0.999_999] {
            let x = standard_normal_quantile(p);
            let back = normal_cdf_value(0.0, 1.0, x).unwrap();
            assert!((back - p).abs() <= 1e-15_f64.max(p * 1e-13), "{p}: {back}");
        }
        assert_eq!(standard_normal_quantile(0.5).to_bits(), 0.0_f64.to_bits());
        assert!(inverse_normal_cdf_value(0.0, 1.0, 1.0).is_err());
        assert!(normal_cdf_value(0.0, 0.0, 1.0).is_err());
    }

    #[test]
    fn cosine_uses_shared_keys() {
        let x = vec![("a".to_owned(), Some(1.0)), ("b".to_owned(), Some(2.0))];
        let y = vec![("a".to_owned(), Some(1.0)), ("c".to_owned(), Some(3.0))];
        let value = cosine(&x, &y).unwrap();
        assert!((value - 1.0 / 50_f64.sqrt()).abs() < 1e-15);
        assert!(
            cosine(&[("a".to_owned(), Some(0.0))], &[("a".to_owned(), Some(1.0))])
                .unwrap()
                .is_nan()
        );
    }
}
