//! Plumbing shared by every Trino function: a table-driven scalar UDF,
//! argument coercion helpers, column accessors and Trino-style errors.
//!
//! Errors raised by these functions start with the Trino error name
//! (`INVALID_FUNCTION_ARGUMENT: …`), so client protocols can report the same
//! `errorName` Trino would and golden tests can compare it.

use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

fn trino_error(name: &str, message: impl Display) -> DataFusionError {
    DataFusionError::Execution(format!("{name}: {message}"))
}

pub(crate) fn invalid_argument(message: impl Display) -> DataFusionError {
    trino_error("INVALID_FUNCTION_ARGUMENT", message)
}

pub(crate) fn out_of_range(message: impl Display) -> DataFusionError {
    trino_error("NUMERIC_VALUE_OUT_OF_RANGE", message)
}

pub(crate) fn invalid_cast(message: impl Display) -> DataFusionError {
    trino_error("INVALID_CAST_ARGUMENT", message)
}

/// Planning error for arguments no overload accepts.
pub(crate) fn not_found(name: &str, types: &[DataType]) -> DataFusionError {
    let list = types.iter().map(trino_type_name).collect::<Vec<_>>().join(", ");
    DataFusionError::Plan(format!(
        "FUNCTION_NOT_FOUND: Unexpected parameters ({list}) for function {name}"
    ))
}

/// Approximate Trino spelling of an Arrow type, for error messages.
pub(crate) fn trino_type_name(t: &DataType) -> String {
    match t {
        DataType::Null => "unknown".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float32 => "real".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "varbinary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::Timestamp(unit, tz) => {
            let p = precision_of(*unit);
            if tz.is_some() {
                format!("timestamp({p}) with time zone")
            } else {
                format!("timestamp({p})")
            }
        }
        DataType::List(f) | DataType::LargeList(f) => format!("array({})", trino_type_name(f.data_type())),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "map({}, {})",
                trino_type_name(fields[0].data_type()),
                trino_type_name(fields[1].data_type())
            ),
            other => other.to_string(),
        },
        DataType::Struct(fields) => {
            let items: Vec<String> = fields
                .iter()
                .map(|f| {
                    let anonymous = f
                        .name()
                        .strip_prefix('c')
                        .is_some_and(|n| n.bytes().all(|b| b.is_ascii_digit()));
                    if anonymous {
                        trino_type_name(f.data_type())
                    } else {
                        format!("{} {}", f.name(), trino_type_name(f.data_type()))
                    }
                })
                .collect();
            format!("row({})", items.join(", "))
        }
        DataType::Time32(TimeUnit::Second) => "time(0)".into(),
        DataType::Time32(_) => "time(3)".into(),
        DataType::Time64(TimeUnit::Microsecond) => "time(6)".into(),
        DataType::Time64(_) => "time(9)".into(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// The json type
// ---------------------------------------------------------------------------

/// Field metadata key naming the Trino type of a column Arrow cannot spell.
pub(crate) const TRINO_TYPE_KEY: &str = "codelake.trino_type";

/// Arrow has no JSON type: a `json` value is `varchar` text whose field
/// carries `codelake.trino_type = json`, so casts can tell it from `varchar`.
pub(crate) fn json_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true).with_metadata(
        [(TRINO_TYPE_KEY.to_owned(), "json".to_owned())]
            .into_iter()
            .collect(),
    )
}

pub(crate) fn is_json_field(field: &Field) -> bool {
    field.metadata().get(TRINO_TYPE_KEY).is_some_and(|t| t == "json")
}

// ---------------------------------------------------------------------------
// Type predicates and coercion targets
// ---------------------------------------------------------------------------

pub(crate) fn is_string(t: &DataType) -> bool {
    match t {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, v) => is_string(v),
        _ => false,
    }
}

pub(crate) fn is_integer(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
    )
}

pub(crate) fn is_float(t: &DataType) -> bool {
    matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64)
}

pub(crate) fn is_decimal(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

pub(crate) fn is_numeric(t: &DataType) -> bool {
    is_integer(t) || is_float(t) || is_decimal(t) || matches!(t, DataType::UInt64)
}

pub(crate) fn is_binary(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_)
    )
}

/// Target for a `varchar` parameter.
pub(crate) fn as_str(t: &DataType) -> Option<DataType> {
    (is_string(t) || t.is_null()).then_some(DataType::Utf8)
}

/// Target for a `bigint` parameter (Trino widens smaller integers).
pub(crate) fn as_i64(t: &DataType) -> Option<DataType> {
    (is_integer(t) || t.is_null()).then_some(DataType::Int64)
}

/// Target for a `double` parameter.
pub(crate) fn as_f64(t: &DataType) -> Option<DataType> {
    (is_numeric(t) || t.is_null()).then_some(DataType::Float64)
}

/// Target for a `varbinary` parameter.
pub(crate) fn as_bin(t: &DataType) -> Option<DataType> {
    (is_binary(t) || t.is_null()).then_some(DataType::Binary)
}

pub(crate) fn as_bool(t: &DataType) -> Option<DataType> {
    (matches!(t, DataType::Boolean) || t.is_null()).then_some(DataType::Boolean)
}

/// Timestamps keep their unit and zone; `NULL` becomes `timestamp(3)`.
pub(crate) fn as_timestamp(t: &DataType) -> Option<DataType> {
    match t {
        DataType::Timestamp(..) => Some(t.clone()),
        DataType::Null => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
        _ => None,
    }
}

/// Coercion target of one parameter.
pub(crate) type Target = fn(&DataType) -> Option<DataType>;

/// Applies one target per argument; any mismatch is `FUNCTION_NOT_FOUND`.
pub(crate) fn coerce_each(name: &str, types: &[DataType], targets: &[Target]) -> Result<Vec<DataType>> {
    if types.len() != targets.len() {
        return Err(not_found(name, types));
    }
    types
        .iter()
        .zip(targets)
        .map(|(t, target)| target(t).ok_or_else(|| not_found(name, types)))
        .collect()
}

/// Several accepted shapes: the first that matches wins.
pub(crate) fn coerce_any(name: &str, types: &[DataType], shapes: &[&[Target]]) -> Result<Vec<DataType>> {
    shapes
        .iter()
        .find_map(|shape| coerce_each(name, types, shape).ok())
        .ok_or_else(|| not_found(name, types))
}

// ---------------------------------------------------------------------------
// Column accessors
// ---------------------------------------------------------------------------

fn downcast<'a, T: 'static>(array: &'a ArrayRef, what: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Internal(format!("expected {what} argument, got {}", array.data_type()))
    })
}

pub(crate) fn strings(array: &ArrayRef) -> Result<&StringArray> {
    downcast(array, "Utf8")
}

pub(crate) fn ints(array: &ArrayRef) -> Result<&Int64Array> {
    downcast(array, "Int64")
}

pub(crate) fn floats(array: &ArrayRef) -> Result<&Float64Array> {
    downcast(array, "Float64")
}

pub(crate) fn binaries(array: &ArrayRef) -> Result<&BinaryArray> {
    downcast(array, "Binary")
}

pub(crate) fn bools(array: &ArrayRef) -> Result<&BooleanArray> {
    downcast(array, "Boolean")
}

pub(crate) fn dates(array: &ArrayRef) -> Result<&Date32Array> {
    downcast(array, "Date32")
}

/// Value at a row, `None` when NULL.
pub(crate) trait At {
    type Item<'a>
    where
        Self: 'a;
    fn at(&self, row: usize) -> Option<Self::Item<'_>>;
}

impl At for StringArray {
    type Item<'a> = &'a str;
    fn at(&self, row: usize) -> Option<&str> {
        (!self.is_null(row)).then(|| self.value(row))
    }
}

impl At for BinaryArray {
    type Item<'a> = &'a [u8];
    fn at(&self, row: usize) -> Option<&[u8]> {
        (!self.is_null(row)).then(|| self.value(row))
    }
}

impl At for Int64Array {
    type Item<'a> = i64;
    fn at(&self, row: usize) -> Option<i64> {
        (!self.is_null(row)).then(|| self.value(row))
    }
}

impl At for Float64Array {
    type Item<'a> = f64;
    fn at(&self, row: usize) -> Option<f64> {
        (!self.is_null(row)).then(|| self.value(row))
    }
}

impl At for BooleanArray {
    type Item<'a> = bool;
    fn at(&self, row: usize) -> Option<bool> {
        (!self.is_null(row)).then(|| self.value(row))
    }
}

impl At for Date32Array {
    type Item<'a> = i32;
    fn at(&self, row: usize) -> Option<i32> {
        (!self.is_null(row)).then(|| self.value(row))
    }
}

/// A timestamp column as raw values in its own unit.
#[derive(Debug)]
pub(crate) struct Timestamps {
    pub(crate) values: Int64Array,
    pub(crate) unit: TimeUnit,
    pub(crate) zone: Option<Arc<str>>,
}

impl Timestamps {
    pub(crate) fn at(&self, row: usize) -> Option<i64> {
        self.values.at(row)
    }
}

pub(crate) fn timestamps(array: &ArrayRef) -> Result<Timestamps> {
    let DataType::Timestamp(unit, zone) = array.data_type() else {
        return Err(DataFusionError::Internal(format!(
            "expected timestamp argument, got {}",
            array.data_type()
        )));
    };
    let values = cast(array, &DataType::Int64)?;
    let values = downcast::<Int64Array>(&values, "Int64")?.clone();
    Ok(Timestamps {
        values,
        unit: *unit,
        zone: zone.clone(),
    })
}

/// Builds a timestamp array from raw values in `unit`.
pub(crate) fn timestamp_array(
    values: Vec<Option<i64>>,
    unit: TimeUnit,
    zone: Option<Arc<str>>,
) -> Result<ArrayRef> {
    let raw: ArrayRef = Arc::new(Int64Array::from(values));
    Ok(cast(&raw, &DataType::Timestamp(unit, zone))?)
}

/// A boolean mask whose NULLs count as false. (Arrow's
/// `prep_null_mask_filter` panics on a mask without a null buffer.)
pub(crate) fn nulls_as_false(mask: &BooleanArray) -> BooleanArray {
    if mask.null_count() == 0 {
        return mask.clone();
    }
    datafusion::arrow::compute::prep_null_mask_filter(mask)
}

pub(crate) fn string_array(values: Vec<Option<String>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

pub(crate) fn int_array(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

pub(crate) fn float_array(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

pub(crate) fn bool_array(values: Vec<Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from(values))
}

pub(crate) fn binary_array(values: &[Option<Vec<u8>>]) -> ArrayRef {
    Arc::new(BinaryArray::from_iter(values.iter().map(|v| v.as_deref())))
}

/// Trino precision (fractional second digits) of an Arrow time unit.
pub(crate) fn precision_of(unit: TimeUnit) -> u32 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// Values of `unit` per second.
pub(crate) fn per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// String value of a literal argument, if it is one.
pub(crate) fn literal_str(scalar: Option<&ScalarValue>) -> Option<&str> {
    match scalar? {
        ScalarValue::Utf8(v) | ScalarValue::Utf8View(v) | ScalarValue::LargeUtf8(v) => v.as_deref(),
        _ => None,
    }
}

/// Session time zone (`datafusion.execution.time_zone`); Trino's default here is UTC.
pub(crate) fn session_zone(config: &ConfigOptions) -> Arc<str> {
    match config.execution.time_zone.as_deref() {
        None | Some("" | "+00:00" | "Z") => Arc::from("UTC"),
        Some(zone) => Arc::from(zone),
    }
}

// ---------------------------------------------------------------------------
// Table-driven scalar UDF
// ---------------------------------------------------------------------------

/// Argument types (already coerced) and literal values, for return types.
#[derive(Debug)]
pub(crate) struct Ret<'a> {
    pub(crate) types: &'a [DataType],
    pub(crate) scalars: &'a [Option<&'a ScalarValue>],
    pub(crate) zone: &'a str,
}

impl Ret<'_> {
    pub(crate) fn literal_str(&self, i: usize) -> Option<&str> {
        literal_str(self.scalars.get(i).copied().flatten())
    }
}

/// One invocation: arguments as arrays of `rows` rows.
#[derive(Debug)]
pub(crate) struct Call<'a> {
    pub(crate) args: Vec<ArrayRef>,
    /// Argument fields (their metadata tells `json` from `varchar`).
    pub(crate) fields: Vec<FieldRef>,
    pub(crate) rows: usize,
    pub(crate) return_type: &'a DataType,
    pub(crate) zone: Arc<str>,
}

impl Call<'_> {
    pub(crate) fn arg(&self, i: usize) -> Result<&ArrayRef> {
        self.args
            .get(i)
            .ok_or_else(|| DataFusionError::Internal(format!("missing argument {i}")))
    }
}

pub(crate) type CoerceFn = fn(&str, &[DataType]) -> Result<Vec<DataType>>;
pub(crate) type ReturnFn = fn(&Ret<'_>) -> Result<DataType>;
pub(crate) type InvokeFn = fn(&Call<'_>) -> Result<ArrayRef>;
pub(crate) type SimplifyFn = fn(&Func, &[Expr], &SimplifyContext) -> Result<Option<Expr>>;

/// A scalar function described by plain function pointers.
#[derive(Debug, Clone)]
pub(crate) struct Func {
    name: &'static str,
    aliases: Vec<String>,
    signature: Signature,
    coerce: CoerceFn,
    ret: ReturnFn,
    invoke: InvokeFn,
    simplify: Option<SimplifyFn>,
    zone: Arc<str>,
    zone_sensitive: bool,
    returns_json: bool,
}

impl PartialEq for Func {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.aliases == other.aliases
            && self.signature == other.signature
            && self.zone == other.zone
    }
}

impl Eq for Func {}

impl Hash for Func {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.zone.hash(state);
    }
}

impl Func {
    pub(crate) fn new(name: &'static str, coerce: CoerceFn, ret: ReturnFn, invoke: InvokeFn) -> Self {
        Self {
            name,
            aliases: Vec::new(),
            signature: Signature::user_defined(Volatility::Immutable),
            coerce,
            ret,
            invoke,
            simplify: None,
            zone: Arc::from("UTC"),
            zone_sensitive: false,
            returns_json: false,
        }
    }

    /// The result is Trino `json` (see [`json_field`]).
    pub(crate) fn returns_json(mut self) -> Self {
        self.returns_json = true;
        self
    }

    pub(crate) fn alias(mut self, alias: &str) -> Self {
        self.aliases.push(alias.to_owned());
        self
    }

    pub(crate) fn volatility(mut self, volatility: Volatility) -> Self {
        self.signature = Signature::user_defined(volatility);
        self
    }

    /// Return type or result depends on the session time zone.
    pub(crate) fn zone_sensitive(mut self) -> Self {
        self.zone_sensitive = true;
        self
    }

    pub(crate) fn simplify_with(mut self, simplify: SimplifyFn) -> Self {
        self.simplify = Some(simplify);
        self
    }

    pub(crate) fn zone(&self) -> &Arc<str> {
        &self.zone
    }

    pub(crate) fn udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }

    fn return_data_type(&self, raw: &[DataType], scalars: &[Option<&ScalarValue>]) -> Result<DataType> {
        let types = (self.coerce)(self.name, raw)?;
        (self.ret)(&Ret {
            types: &types,
            scalars,
            zone: &self.zone,
        })
    }
}

impl ScalarUDFImpl for Func {
    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        (self.coerce)(self.name, arg_types)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.return_data_type(arg_types, &[])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let raw: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let data_type = self.return_data_type(&raw, args.scalar_arguments)?;
        if self.returns_json && data_type == DataType::Utf8 {
            return Ok(Arc::new(json_field(self.name)));
        }
        Ok(Arc::new(Field::new(self.name, data_type, true)))
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        if !self.zone_sensitive {
            return None;
        }
        let mut updated = self.clone();
        updated.zone = session_zone(config);
        Some(ScalarUDF::new_from_impl(updated))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if let Some(simplify) = self.simplify
            && let Some(expr) = simplify(self, &args, info)?
        {
            return Ok(ExprSimplifyResult::Simplified(expr));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar =
            !args.args.is_empty() && args.args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let rows = if all_scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|a| a.to_array(rows))
            .collect::<Result<Vec<_>>>()?;
        let call = Call {
            args: arrays,
            fields: args.arg_fields.clone(),
            rows,
            return_type: args.return_field.data_type(),
            zone: session_zone(&args.config_options),
        };
        // A defect in one function must fail the query, not the process.
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.invoke)(&call))).map_err(
            |_| DataFusionError::Internal(format!("function {} panicked; this is a defect", self.name)),
        )??;
        let out = if out.data_type() == call.return_type {
            out
        } else {
            cast(&out, call.return_type)?
        };
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

pub(crate) const UTF8: ReturnFn = |_| Ok(DataType::Utf8);
pub(crate) const BIGINT: ReturnFn = |_| Ok(DataType::Int64);
pub(crate) const BOOLEAN: ReturnFn = |_| Ok(DataType::Boolean);
pub(crate) const DOUBLE: ReturnFn = |_| Ok(DataType::Float64);
pub(crate) const VARBINARY: ReturnFn = |_| Ok(DataType::Binary);
