//! Engine-internal scalar functions.
//!
//! `codelake_varbinary('00ff…')` carries a binary literal through the plan.
//! The `DataFusion` unparser cannot render binary literals, so a predicate such
//! as `lsn > X'0001'` could never reach a SQL source. Wrapping the bytes in this
//! function lets each source dialect render its own literal syntax (T-SQL
//! `0x0001`), while local evaluation decodes the same bytes.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryBuilder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, exec_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, lit,
};

pub const VARBINARY_LITERAL: &str = "codelake_varbinary";

/// Identity around a timestamp cast that widens a column's unit and must
/// stay exact (see `crate::coercion`); pushdown policies recognize it.
pub use crate::coercion::EXACT_CAST;

/// `codelake_superset(<predicate>)`: a predicate a source may evaluate
/// instead of the engine because it keeps *at least* the rows the engine
/// keeps (see `crate::federation_policy`); the exact predicate stays above
/// it. Removed before the SQL is rendered; evaluated locally it is the
/// predicate itself.
pub const SUPERSET_FILTER: &str = "codelake_superset";

/// `predicate` marked as a superset filter ([`SUPERSET_FILTER`]).
pub fn superset_filter(predicate: Expr) -> Expr {
    static UDF: std::sync::LazyLock<Arc<ScalarUDF>> = std::sync::LazyLock::new(|| {
        Arc::new(ScalarUDF::new_from_impl(SupersetFilter {
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        }))
    });
    UDF.call(vec![predicate])
}

/// The predicate inside [`superset_filter`], if `expr` is one.
pub fn as_superset_filter(expr: &Expr) -> Option<&Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.name() != SUPERSET_FILTER {
        return None;
    }
    if let [predicate] = function.args.as_slice() {
        Some(predicate)
    } else {
        None
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SupersetFilter {
    signature: Signature,
}

impl ScalarUDFImpl for SupersetFilter {
    fn name(&self) -> &'static str {
        SUPERSET_FILTER
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [predicate] = args.args.as_slice() {
            Ok(predicate.clone())
        } else {
            exec_err!("{SUPERSET_FILTER} expects one argument")
        }
    }
}

/// Decodes an even-length hexadecimal string (either case).
pub fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    // `from_str_radix` accepts a leading sign: check the digits first.
    if !hex.len().is_multiple_of(2) || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            hex.get(i..i + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
        })
        .collect()
}

pub fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// `codelake_varbinary(<hex>)` for the given bytes.
pub fn binary_literal_expr(bytes: &[u8]) -> Expr {
    varbinary_literal().call(vec![lit(encode_hex(bytes))])
}

/// If `expr` is `codelake_varbinary('<hex>')`, returns the hex text.
pub fn as_binary_literal(expr: &Expr) -> Option<&str> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.name() != VARBINARY_LITERAL {
        return None;
    }
    match function.args.as_slice() {
        [Expr::Literal(ScalarValue::Utf8(Some(hex)), _)] if decode_hex(hex).is_some() => Some(hex),
        _ => None,
    }
}

pub fn varbinary_literal() -> ScalarUDF {
    ScalarUDF::new_from_impl(VarbinaryLiteral {
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct VarbinaryLiteral {
    signature: Signature,
}

impl ScalarUDFImpl for VarbinaryLiteral {
    fn name(&self) -> &'static str {
        VARBINARY_LITERAL
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("{VARBINARY_LITERAL} expects one argument");
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(hex))) => {
                if let Some(bytes) = decode_hex(hex) {
                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))))
                } else {
                    exec_err!("{VARBINARY_LITERAL}: `{hex}` is not hexadecimal")
                }
            }
            ColumnarValue::Array(array) => {
                let Some(strings) = array.as_any().downcast_ref::<StringArray>() else {
                    return exec_err!("{VARBINARY_LITERAL} expects Utf8");
                };
                let mut builder = BinaryBuilder::with_capacity(strings.len(), strings.len());
                for value in strings {
                    match value.map(|hex| (hex, decode_hex(hex))) {
                        None => builder.append_null(),
                        Some((_, Some(bytes))) => builder.append_value(bytes),
                        Some((hex, None)) => {
                            return exec_err!("{VARBINARY_LITERAL}: `{hex}` is not hexadecimal");
                        }
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            ColumnarValue::Scalar(other) => exec_err!("{VARBINARY_LITERAL} expects Utf8, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::SessionContext;

    #[test]
    fn hex_round_trip() {
        assert_eq!(decode_hex("00ff10"), Some(vec![0, 255, 16]));
        assert_eq!(decode_hex("00FF"), Some(vec![0, 255]));
        assert_eq!(decode_hex(""), Some(vec![]));
        assert_eq!(decode_hex("0"), None);
        assert_eq!(decode_hex("zz"), None);
        assert_eq!(decode_hex("+a"), None, "a sign is not a hex digit");
        assert_eq!(encode_hex(&[0, 255, 16]), "00ff10");
    }

    #[test]
    fn recognises_only_valid_literals() {
        assert_eq!(as_binary_literal(&binary_literal_expr(&[1, 2])), Some("0102"));
        assert_eq!(
            as_binary_literal(&varbinary_literal().call(vec![lit("xyz")])),
            None
        );
        assert_eq!(as_binary_literal(&lit("0102")), None);
    }

    #[tokio::test]
    async fn evaluates_scalars_arrays_and_rejects_garbage() {
        let ctx = SessionContext::new();
        ctx.register_udf(varbinary_literal());
        let batches = ctx
            .sql("SELECT codelake_varbinary('0a0b') AS b, codelake_varbinary(x) AS c FROM (VALUES ('ff'), (NULL)) AS t(x)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let b = batches[0].column(0).as_binary::<i32>();
        let c = batches[0].column(1).as_binary::<i32>();
        assert_eq!(b.value(0), &[10, 11]);
        assert_eq!(c.value(0), &[255]);
        assert!(c.is_null(1));
        let err = ctx
            .sql("SELECT codelake_varbinary('nothex')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not hexadecimal"), "{err}");
    }
}
