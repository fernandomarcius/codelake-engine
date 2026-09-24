//! Conditional expressions and `typeof`.
//!
//! `if(condition, a [, b])` is planned as this function and rewritten to
//! `CASE` before type coercion (see `rewrite`), so only the chosen branch is
//! evaluated. The eager implementation below is a fallback for plans built
//! without the analyzer.

use datafusion::arrow::array::{ArrayRef, new_null_array};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::type_coercion::binary::type_union_resolution;
use datafusion::logical_expr::{Operator, ScalarUDF};

use super::support::{Call, Func, as_bool, bools, not_found, string_array, trino_type_name};

fn coerce_if(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    let (condition, branches) = match types {
        [c, rest @ ..] if (1..=2).contains(&rest.len()) => (c, rest),
        _ => return Err(not_found(name, types)),
    };
    let condition = as_bool(condition).ok_or_else(|| not_found(name, types))?;
    let common = type_union_resolution(branches).ok_or_else(|| not_found(name, types))?;
    let mut out = vec![condition];
    out.extend(std::iter::repeat_n(common, branches.len()));
    Ok(out)
}

fn invoke_if(call: &Call<'_>) -> Result<ArrayRef> {
    let condition = bools(call.arg(0)?)?;
    let then = call.arg(1)?;
    let otherwise = match call.args.get(2) {
        Some(b) => b.clone(),
        None => new_null_array(then.data_type(), call.rows),
    };
    Ok(zip(&super::support::nulls_as_false(condition), then, &otherwise)?)
}

pub(crate) fn if_function() -> ScalarUDF {
    Func::new(
        "if",
        coerce_if,
        |r| {
            r.types
                .get(1)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("missing argument".into()))
        },
        invoke_if,
    )
    .udf()
}

pub(crate) fn typeof_function() -> ScalarUDF {
    Func::new(
        "typeof",
        |name, types| match types {
            [t] => Ok(vec![t.clone()]),
            _ => Err(not_found(name, types)),
        },
        |_| Ok(DataType::Utf8),
        |call| {
            let name = if call
                .fields
                .first()
                .is_some_and(|f| super::support::is_json_field(f))
            {
                "json".to_owned()
            } else {
                trino_type_name(call.arg(0)?.data_type())
            };
            Ok(string_array(vec![Some(name); call.rows.max(1)]))
        },
    )
    .udf()
}

/// `mod(n, m)`: planned as this function, rewritten to `n % m` (truncated
/// remainder, the dividend's sign), whose result type it reports.
pub(crate) fn mod_function() -> ScalarUDF {
    Func::new(
        "mod",
        |name, types| match types {
            [a, b] if super::support::is_numeric(a) && super::support::is_numeric(b) => Ok(types.to_vec()),
            _ => Err(not_found(name, types)),
        },
        |r| match r.types {
            [a, b] => BinaryTypeCoercer::new(a, &Operator::Modulo, b).get_result_type(),
            _ => Err(DataFusionError::Internal("mod arguments".into())),
        },
        |_| {
            Err(DataFusionError::Internal(
                "mod is rewritten to the % operator".into(),
            ))
        },
    )
    .udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![if_function(), typeof_function(), mod_function()]
}
