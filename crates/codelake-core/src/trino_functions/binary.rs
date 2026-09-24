//! Binary functions (Trino "Binary functions and operators").
//!
//! `to_hex` prints upper case; big-endian and IEEE 754 decoders demand the
//! exact byte count; digests take and return `varbinary`.

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE};
use datafusion::arrow::array::{ArrayRef, Float32Array, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ScalarUDF;
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};

use super::support::{
    At, Call, Func, VARBINARY, as_bin, as_f64, as_i64, as_str, binaries, binary_array, coerce_each,
    int_array, ints, invalid_argument, string_array, strings,
};

fn map_bytes(call: &Call<'_>, f: impl Fn(&[u8]) -> Result<Vec<u8>>) -> Result<ArrayRef> {
    let b = binaries(call.arg(0)?)?;
    let out = (0..call.rows)
        .map(|i| b.at(i).map(&f).transpose())
        .collect::<Result<Vec<_>>>()?;
    Ok(binary_array(&out))
}

fn bin1(name: &'static str, f: fn(&Call<'_>) -> Result<ArrayRef>) -> ScalarUDF {
    Func::new(
        name,
        |name, types| coerce_each(name, types, &[as_bin]),
        VARBINARY,
        f,
    )
    .udf()
}

pub(crate) fn to_hex() -> ScalarUDF {
    Func::new(
        "to_hex",
        |name, types| coerce_each(name, types, &[as_bin]),
        |_| Ok(DataType::Utf8),
        |call| {
            let b = binaries(call.arg(0)?)?;
            Ok(string_array(
                (0..call.rows)
                    .map(|i| {
                        b.at(i)
                            .map(|v| crate::functions::encode_hex(v).to_ascii_uppercase())
                    })
                    .collect(),
            ))
        },
    )
    .udf()
}

pub(crate) fn from_hex() -> ScalarUDF {
    Func::new(
        "from_hex",
        |name, types| coerce_each(name, types, &[as_str]),
        VARBINARY,
        |call| {
            let s = strings(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    s.at(i)
                        .map(|text| {
                            if text.len() % 2 != 0 {
                                return Err(invalid_argument(format!("invalid input length {}", text.len())));
                            }
                            crate::functions::decode_hex(text)
                                .ok_or_else(|| invalid_argument("invalid hex character"))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(binary_array(&out))
        },
    )
    .udf()
}

fn exact<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    bytes.try_into().map_err(|_| {
        invalid_argument(format!(
            "expected {N}-byte input, but got instead: {}",
            bytes.len()
        ))
    })
}

pub(crate) fn from_big_endian_64() -> ScalarUDF {
    Func::new(
        "from_big_endian_64",
        |name, types| coerce_each(name, types, &[as_bin]),
        |_| Ok(DataType::Int64),
        |call| {
            let b = binaries(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| b.at(i).map(|v| exact::<8>(v).map(i64::from_be_bytes)).transpose())
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

pub(crate) fn from_big_endian_32() -> ScalarUDF {
    Func::new(
        "from_big_endian_32",
        |name, types| coerce_each(name, types, &[as_bin]),
        |_| Ok(DataType::Int32),
        |call| {
            let b = binaries(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    b.at(i)
                        .map(|v| exact::<4>(v).map(|a| i64::from(i32::from_be_bytes(a))))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

pub(crate) fn to_big_endian_64() -> ScalarUDF {
    Func::new(
        "to_big_endian_64",
        |name, types| coerce_each(name, types, &[as_i64]),
        VARBINARY,
        |call| {
            let n = ints(call.arg(0)?)?;
            let out: Vec<Option<Vec<u8>>> = (0..call.rows)
                .map(|i| n.at(i).map(|v| v.to_be_bytes().to_vec()))
                .collect();
            Ok(binary_array(&out))
        },
    )
    .udf()
}

pub(crate) fn to_big_endian_32() -> ScalarUDF {
    Func::new(
        "to_big_endian_32",
        |name, types| match types {
            [
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                // Coerced already (return types see coerced arguments).
                | DataType::Int64
                | DataType::Null,
            ] => Ok(vec![DataType::Int64]),
            _ => Err(super::support::not_found(name, types)),
        },
        VARBINARY,
        |call| {
            let n = ints(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    n.at(i)
                        .map(|v| {
                            i32::try_from(v)
                                .map(|v| v.to_be_bytes().to_vec())
                                .map_err(|_| invalid_argument("value out of integer range"))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(binary_array(&out))
        },
    )
    .udf()
}

pub(crate) fn to_ieee754_64() -> ScalarUDF {
    Func::new(
        "to_ieee754_64",
        |name, types| coerce_each(name, types, &[as_f64]),
        VARBINARY,
        |call| {
            let values = cast(call.arg(0)?, &DataType::Float64)?;
            let values = super::support::floats(&values)?;
            let out: Vec<Option<Vec<u8>>> = (0..call.rows)
                .map(|i| values.at(i).map(|v| v.to_bits().to_be_bytes().to_vec()))
                .collect();
            Ok(binary_array(&out))
        },
    )
    .udf()
}

pub(crate) fn to_ieee754_32() -> ScalarUDF {
    Func::new(
        "to_ieee754_32",
        |name, types| match types {
            [DataType::Float32 | DataType::Null] => Ok(vec![DataType::Float32]),
            _ => Err(super::support::not_found(name, types)),
        },
        VARBINARY,
        |call| {
            let values = call
                .arg(0)?
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| DataFusionError::Internal("expected Float32".into()))?;
            let out: Vec<Option<Vec<u8>>> = (0..call.rows)
                .map(|i| {
                    use datafusion::arrow::array::Array;
                    (!values.is_null(i)).then(|| values.value(i).to_bits().to_be_bytes().to_vec())
                })
                .collect();
            Ok(binary_array(&out))
        },
    )
    .udf()
}

pub(crate) fn from_ieee754_64() -> ScalarUDF {
    Func::new(
        "from_ieee754_64",
        |name, types| coerce_each(name, types, &[as_bin]),
        |_| Ok(DataType::Float64),
        |call| {
            let b = binaries(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    b.at(i)
                        .map(|v| {
                            if v.len() != 8 {
                                return Err(invalid_argument(
                                    "Input floating-point value must be exactly 8 bytes long",
                                ));
                            }
                            exact::<8>(v).map(|a| f64::from_bits(u64::from_be_bytes(a)))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(std::sync::Arc::new(Float64Array::from(out)))
        },
    )
    .udf()
}

pub(crate) fn from_ieee754_32() -> ScalarUDF {
    Func::new(
        "from_ieee754_32",
        |name, types| coerce_each(name, types, &[as_bin]),
        |_| Ok(DataType::Float32),
        |call| {
            let b = binaries(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    b.at(i)
                        .map(|v| {
                            if v.len() != 4 {
                                return Err(invalid_argument(
                                    "Input floating-point value must be exactly 4 bytes long",
                                ));
                            }
                            exact::<4>(v).map(|a| f32::from_bits(u32::from_be_bytes(a)))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(std::sync::Arc::new(Float32Array::from(out)))
        },
    )
    .udf()
}

pub(crate) fn digest<D: Digest>(bytes: &[u8]) -> Vec<u8> {
    D::digest(bytes).to_vec()
}

/// Standard HMAC (RFC 2104) over a block-size-`B` digest.
fn hmac<D: Digest, const B: usize>(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut block = [0_u8; B];
    if key.len() > B {
        let hashed = D::digest(key);
        block[..hashed.len()].copy_from_slice(&hashed);
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let inner_pad: Vec<u8> = block.iter().map(|b| b ^ 0x36).collect();
    let outer_pad: Vec<u8> = block.iter().map(|b| b ^ 0x5c).collect();
    let mut inner = D::new();
    inner.update(&inner_pad);
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = D::new();
    outer.update(&outer_pad);
    outer.update(inner);
    outer.finalize().to_vec()
}

fn hmac_invoke(call: &Call<'_>, f: fn(&[u8], &[u8]) -> Vec<u8>) -> Result<ArrayRef> {
    let message = binaries(call.arg(0)?)?;
    let key = binaries(call.arg(1)?)?;
    let out = (0..call.rows)
        .map(|i| match (key.at(i), message.at(i)) {
            // Java's `SecretKeySpec` rejects an empty key; Trino reports it as is.
            (Some([]), Some(_)) => Err(DataFusionError::Execution("Empty key".into())),
            (Some(k), Some(m)) => Ok(Some(f(k, m))),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(binary_array(&out))
}

fn hmacs() -> Vec<ScalarUDF> {
    vec![
        Func::new(
            "hmac_md5",
            |name, types| coerce_each(name, types, &[as_bin, as_bin]),
            VARBINARY,
            |call| hmac_invoke(call, hmac::<Md5, 64>),
        )
        .udf(),
        Func::new(
            "hmac_sha1",
            |name, types| coerce_each(name, types, &[as_bin, as_bin]),
            VARBINARY,
            |call| hmac_invoke(call, hmac::<Sha1, 64>),
        )
        .udf(),
        Func::new(
            "hmac_sha256",
            |name, types| coerce_each(name, types, &[as_bin, as_bin]),
            VARBINARY,
            |call| hmac_invoke(call, hmac::<Sha256, 64>),
        )
        .udf(),
        Func::new(
            "hmac_sha512",
            |name, types| coerce_each(name, types, &[as_bin, as_bin]),
            VARBINARY,
            |call| hmac_invoke(call, hmac::<Sha512, 128>),
        )
        .udf(),
    ]
}

/// Trino `xxhash64(VARBINARY)`: the 64-bit hash, big-endian.
pub(crate) fn xxhash64_bytes(bytes: &[u8]) -> u64 {
    twox_hash::XxHash64::oneshot(0, bytes)
}

pub(crate) fn crc32() -> ScalarUDF {
    Func::new(
        "crc32",
        |name, types| coerce_each(name, types, &[as_bin]),
        |_| Ok(DataType::Int64),
        |call| {
            let b = binaries(call.arg(0)?)?;
            Ok(int_array(
                (0..call.rows)
                    .map(|i| b.at(i).map(|v| i64::from(crc32fast::hash(v))))
                    .collect(),
            ))
        },
    )
    .udf()
}

/// Decoding accepts missing padding, as Java's decoder does.
const LENIENT: base64::engine::GeneralPurposeConfig = base64::engine::GeneralPurposeConfig::new()
    .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent);
static STANDARD_DECODER: base64::engine::GeneralPurpose =
    base64::engine::GeneralPurpose::new(&base64::alphabet::STANDARD, LENIENT);
static URL_SAFE_DECODER: base64::engine::GeneralPurpose =
    base64::engine::GeneralPurpose::new(&base64::alphabet::URL_SAFE, LENIENT);

fn base64_decode(engine: &base64::engine::GeneralPurpose, text: &str) -> Result<Vec<u8>> {
    engine
        .decode(text)
        .map_err(|e| invalid_argument(format!("Illegal base64 input: {e}")))
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    let mut all = vec![
        to_hex(),
        from_hex(),
        from_big_endian_64(),
        from_big_endian_32(),
        to_big_endian_64(),
        to_big_endian_32(),
        to_ieee754_64(),
        to_ieee754_32(),
        from_ieee754_64(),
        from_ieee754_32(),
        bin1("md5", |call| map_bytes(call, |v| Ok(digest::<Md5>(v)))),
        bin1("sha1", |call| map_bytes(call, |v| Ok(digest::<Sha1>(v)))),
        bin1("sha256", |call| map_bytes(call, |v| Ok(digest::<Sha256>(v)))),
        bin1("sha512", |call| map_bytes(call, |v| Ok(digest::<Sha512>(v)))),
        bin1("xxhash64", |call| {
            map_bytes(call, |v| Ok(xxhash64_bytes(v).to_be_bytes().to_vec()))
        }),
        crc32(),
        Func::new(
            "to_base64",
            |name, types| coerce_each(name, types, &[as_bin]),
            |_| Ok(DataType::Utf8),
            |call| {
                let b = binaries(call.arg(0)?)?;
                Ok(string_array(
                    (0..call.rows)
                        .map(|i| b.at(i).map(|v| STANDARD.encode(v)))
                        .collect(),
                ))
            },
        )
        .udf(),
        Func::new(
            "to_base64url",
            |name, types| coerce_each(name, types, &[as_bin]),
            |_| Ok(DataType::Utf8),
            |call| {
                let b = binaries(call.arg(0)?)?;
                Ok(string_array(
                    (0..call.rows)
                        .map(|i| b.at(i).map(|v| URL_SAFE.encode(v)))
                        .collect(),
                ))
            },
        )
        .udf(),
        Func::new(
            "from_base64",
            |name, types| coerce_each(name, types, &[as_str]),
            VARBINARY,
            |call| {
                let s = strings(call.arg(0)?)?;
                let out = (0..call.rows)
                    .map(|i| s.at(i).map(|t| base64_decode(&STANDARD_DECODER, t)).transpose())
                    .collect::<Result<Vec<_>>>()?;
                Ok(binary_array(&out))
            },
        )
        .udf(),
        Func::new(
            "from_base64url",
            |name, types| coerce_each(name, types, &[as_str]),
            VARBINARY,
            |call| {
                let s = strings(call.arg(0)?)?;
                let out = (0..call.rows)
                    .map(|i| s.at(i).map(|t| base64_decode(&URL_SAFE_DECODER, t)).transpose())
                    .collect::<Result<Vec<_>>>()?;
                Ok(binary_array(&out))
            },
        )
        .udf(),
    ];
    all.extend(hmacs());
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_rfc_4231_case_2() {
        let mac = hmac::<Sha256, 64>(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            crate::functions::encode_hex(&mac),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn xxhash64_of_known_input() {
        // Reference value of XXH64("abc", seed 0).
        assert_eq!(xxhash64_bytes(b"abc"), 0x44bc_2cf5_ad77_0999);
        assert_eq!(exact::<2>(&[1, 2]).unwrap(), [1, 2]);
        assert!(exact::<8>(&[1]).is_err());
    }
}
