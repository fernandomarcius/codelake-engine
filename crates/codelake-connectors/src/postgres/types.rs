//! Arrow types of PostgreSQL columns, and the form array columns travel in.
//!
//! A column that is not an array keeps the type the upstream provider gave
//! it (the rows are decoded by the upstream row decoder, which must agree), a
//! type it does not know is refused, as before. Arrays follow the reference
//! engine (its `array-mapping=AS_ARRAY`): an array of a type it maps is a
//! list of that type, nested once per declared dimension; any other array is
//! left out of the relation, as it leaves it out:
//!
//! | Elements | List of |
//! |---|---|
//! | `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean` | the same type |
//! | `numeric(p, s)` | `decimal(p, s)` (as a column, below) |
//! | `text`, `varchar`, `char`, `"char"`, `name`, `uuid`, `json`, `jsonb`, `money`, enums | text |
//! | `date`, `time(p)`, `timestamp(p)`, `timestamptz(p)` | the same type |
//!
//! Left out (also by the reference engine): `bytea[]`, `interval[]`, network
//! addresses, geometric and range types, composites, `xml[]`. Left out here,
//! mapped there (ISS-060): `oid[]` (`bigint`), `timetz[]` and `bit[]`.
//!
//! A `numeric(p, s)` with `p` up to 38 is a `decimal(p, s)`. One without
//! precision, or with more than 38 digits, follows the catalog's
//! `decimal_mapping`, the reference engine's `decimal-mapping` ([`Decimals`]):
//! `strict` (the default, as there) leaves the column out of its relation, an
//! array of them too; `allow_overflow` makes it a `decimal(38, s)`, `s` being
//! `decimal_default_scale` without precision and the smaller of the declared
//! scale and it beyond 38 digits. An array of `numeric` without precision is
//! an `array(decimal(38, 0))` whatever the default scale, as there (Trino 431
//! with PostgreSQL 17). A composite with such a field is left out.
//!
//! Dates and times have the reference engine's precision: `timestamp(p)`,
//! `timestamp(p) with time zone` and `time(p)` as PostgreSQL declares them
//! (6, its microsecond, without `p`), carried by the smallest Arrow unit
//! holding `p` digits (seconds, milliseconds, microseconds); a `p` other
//! than 0, 3 or 6 is also recorded in the field ([`declared`]).
//!
//! The upstream row decoder reads arrays of numbers, booleans and `text`
//! only, and those without `NULL` elements (`error deserializing column`),
//! and fails on the others (`unsupported data type: _date`). It reads
//! timestamps as nanoseconds since 1970, which skips a `timestamp` before
//! 1970 and turns a `timestamptz` outside 1677–2262 into 1970-01-01; and a
//! `numeric` through a 96-bit decimal, which rounds past 28 digits. So
//! [`readable`] wraps a statement whose result has a list, a timestamp or a
//! decimal, and the source sends each of them in a form the decoder reads
//! exactly: a list as JSON (`to_json`), which the decoder turns into the
//! list the schema declares; a timestamp as its microseconds since 1970 in
//! UTC (`bigint`); a decimal as its text, and a list of decimals as a list of
//! texts. [`restore`] turns them into the columns the schema declares, and a
//! time into its declared unit. A decimal with more fraction digits than its
//! scale is rounded by the catalog's `decimal_rounding_mode` (`unnecessary`,
//! the default: it fails the query); one with more integer digits than its
//! `decimal(p, s)` holds, after rounding, `NaN` or `±Infinity` fails the
//! query, naming the column. Never rounded otherwise, never cut.

use std::borrow::Cow;
use std::fmt::Write as _;
use std::sync::Arc;

use std::cmp::Ordering;

use codelake_core::trino_types::Declared;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Builder, ListArray, PrimitiveArray, RecordBatch,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Int64Type, IntervalUnit, Schema, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};

use super::{DecimalMapping, DecimalRoundingMode, Decimals};

/// A decimal's largest precision.
const MAX_PRECISION: u8 = 38;

/// A column as `pg_catalog` describes it.
#[derive(Debug, Clone, Default)]
pub(super) struct Column {
    pub name: String,
    /// `format_type`: `numeric(10,2)`, `timestamp(3) with time zone`, `integer[]`.
    pub type_name: String,
    /// `typtype`: `b` base, `e` enum, `c` composite, `d` domain…
    pub kind: String,
    /// `typcategory`: `A` for arrays.
    pub category: String,
    /// An array's element type, as `format_type` names it with the column's modifier.
    pub element: Option<String>,
    pub element_kind: Option<String>,
    /// Declared dimensions of an array, at least 1.
    pub dimensions: i32,
    pub not_null: bool,
    /// A composite's fields: name and `format_type`.
    pub fields: Vec<(String, String)>,
}

/// The column's Arrow type; `Ok(None)` leaves it out of the relation,
/// `Err` names a type the engine cannot read.
pub(super) fn arrow_type(column: &Column, decimals: Decimals) -> Result<Option<DataType>, String> {
    if column.category == "A" {
        return Ok(array_type(column, decimals));
    }
    match column.kind.as_str() {
        "e" => Ok(Some(DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        ))),
        "c" if column.fields.iter().any(|(_, type_name)| wide_numeric(type_name)) => Ok(None),
        "c" => column
            .fields
            .iter()
            .map(|(name, type_name)| {
                scalar(type_name)
                    .map(|data_type| Field::new(name, data_type, true))
                    .ok_or_else(|| type_name.clone())
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|fields| Some(DataType::Struct(Fields::from(fields)))),
        _ if base_name(&column.type_name) == "numeric" => {
            Ok(numeric_type(&column.type_name, decimals, false))
        }
        _ => scalar(&column.type_name)
            .map(Some)
            .ok_or_else(|| column.type_name.clone()),
    }
}

/// Whether `type_name` is a `numeric` without precision or with more than
/// 38 digits.
fn wide_numeric(type_name: &str) -> bool {
    base_name(type_name) == "numeric"
        && numeric_precision(type_name).is_none_or(|(precision, _)| precision > u32::from(MAX_PRECISION))
}

/// The decimal of a `numeric` column (`element`: of an array's elements):
/// `decimal(p, s)` up to 38 digits; otherwise as `decimals` maps it, `None`
/// leaving it out (see the module's documentation).
fn numeric_type(type_name: &str, decimals: Decimals, element: bool) -> Option<DataType> {
    let declared = numeric_precision(type_name);
    if let Some((precision, scale)) = declared
        && precision <= u32::from(MAX_PRECISION)
    {
        return Some(DataType::Decimal128(
            u8::try_from(precision).ok()?,
            i8::try_from(scale).ok()?,
        ));
    }
    if decimals.mapping == DecimalMapping::Strict {
        return None;
    }
    let default_scale = decimals.default_scale.min(MAX_PRECISION);
    let scale = match declared {
        // An array's elements, whatever the default scale: the reference
        // engine's `array(decimal(38, 0))` (Trino 431 with PostgreSQL 17).
        None if element => 0,
        None => default_scale,
        Some((_, scale)) => u8::try_from(scale.max(0)).map_or(default_scale, |s| s.min(default_scale)),
    };
    Some(DataType::Decimal128(MAX_PRECISION, i8::try_from(scale).ok()?))
}

/// `format_type`'s name without precision or length: `timestamp(3) with
/// time zone` is `timestamp with time zone`, `character varying(10)` is
/// `character varying`.
fn base_name(type_name: &str) -> Cow<'_, str> {
    match (type_name.find('('), type_name.find(')')) {
        (Some(open), Some(close)) if open < close => {
            Cow::Owned(format!("{}{}", &type_name[..open], &type_name[close + 1..]))
        }
        _ => Cow::Borrowed(type_name),
    }
}

/// `(p, s)` of `numeric(p,s)` / `numeric(p)`; `None` without precision.
fn numeric_precision(type_name: &str) -> Option<(u32, i32)> {
    let inner = type_name.split_once('(')?.1.split_once(')')?.0;
    let mut parts = inner.split(',').map(str::trim);
    let precision = parts.next()?.parse().ok()?;
    let scale = parts.next().map_or(Some(0), |s| s.parse().ok())?;
    Some((precision, scale))
}

/// Fraction digits of a `time(p)` / `timestamp(p)` as `format_type` names
/// it: `p`, or 6 (a microsecond, PostgreSQL's finest) without it.
fn datetime_precision(type_name: &str) -> u8 {
    type_name
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .and_then(|(digits, _)| digits.trim().parse::<u8>().ok())
        .map_or(6, |p| p.min(6))
}

/// The smallest Arrow unit holding `precision` fraction digits.
fn unit_of(precision: u8) -> TimeUnit {
    match precision {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        _ => TimeUnit::Microsecond,
    }
}

fn time_type(precision: u8) -> DataType {
    match unit_of(precision) {
        TimeUnit::Microsecond => DataType::Time64(TimeUnit::Microsecond),
        unit => DataType::Time32(unit),
    }
}

/// The Trino type a column declares beyond its Arrow type: `timestamp(p)`,
/// `timestamp(p) with time zone` or `time(p)` with `p` 1, 2, 4 or 5.
pub(super) fn declared(column: &Column) -> Option<Declared> {
    if column.category == "A" {
        return None;
    }
    let precision = datetime_precision(&column.type_name);
    if matches!(precision, 0 | 3 | 6) {
        return None;
    }
    match base_name(&column.type_name).as_ref() {
        "timestamp without time zone" | "timestamp with time zone" => Some(Declared::Timestamp(precision)),
        "time without time zone" => Some(Declared::Time(precision)),
        _ => None,
    }
}

/// Arrow type of a type that is not an array, enum nor composite.
fn scalar(type_name: &str) -> Option<DataType> {
    Some(match base_name(type_name).as_ref() {
        "smallint" => DataType::Int16,
        "integer" => DataType::Int32,
        "bigint" | "money" => DataType::Int64,
        "oid" | "xid" | "regproc" => DataType::UInt32,
        // Up to 38 digits: the default mapping leaves a wider one out.
        "numeric" => numeric_type(type_name, Decimals::default(), false)?,
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        "\"char\"" => DataType::Int8,
        "character" | "character varying" | "text" | "bpchar" | "uuid" | "name" | "inet" | "cidr"
        | "macaddr" | "xml" | "json" | "aclitem" | "pg_node_tree" => DataType::Utf8,
        "bytea" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" | "bit" | "bit varying"
        | "geometry" | "geography" => DataType::Binary,
        "date" => DataType::Date32,
        "time without time zone" => time_type(datetime_precision(type_name)),
        "timestamp without time zone" => DataType::Timestamp(unit_of(datetime_precision(type_name)), None),
        "timestamp with time zone" => {
            DataType::Timestamp(unit_of(datetime_precision(type_name)), Some("UTC".into()))
        }
        "boolean" => DataType::Boolean,
        "tsvector" | "tsquery" => DataType::LargeUtf8,
        "point" => DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2),
        "int4range" => DataType::Struct(Fields::from(vec![
            Field::new("lower", DataType::Int32, true),
            Field::new("upper", DataType::Int32, true),
        ])),
        name if name.starts_with("interval") => DataType::Interval(IntervalUnit::MonthDayNano),
        _ => return None,
    })
}

/// Arrow type of an array's elements, when the reference engine maps them.
fn element_type(type_name: &str, kind: Option<&str>, decimals: Decimals) -> Option<DataType> {
    if kind == Some("e") {
        return Some(DataType::Utf8);
    }
    Some(match base_name(type_name).as_ref() {
        "numeric" => numeric_type(type_name, decimals, true)?,
        "character" | "character varying" | "text" | "bpchar" | "\"char\"" | "name" | "uuid" | "json"
        | "jsonb" | "money" => DataType::Utf8,
        "smallint"
        | "integer"
        | "bigint"
        | "real"
        | "double precision"
        | "boolean"
        | "date"
        | "time without time zone"
        | "timestamp without time zone"
        | "timestamp with time zone" => scalar(type_name)?,
        _ => return None,
    })
}

fn array_type(column: &Column, decimals: Decimals) -> Option<DataType> {
    let mut data_type = element_type(
        column.element.as_deref()?,
        column.element_kind.as_deref(),
        decimals,
    )?;
    for _ in 0..column.dimensions.max(1) {
        data_type = DataType::List(Arc::new(Field::new("item", data_type, true)));
    }
    Some(data_type)
}

/// Microseconds from 1970-01-01 00:00 of the `timestamp` `value`, exact
/// whatever the server's version (before 14 `extract` answers a `double`:
/// only the time of day goes through it, where it is exact).
fn micros_of(value: &str) -> String {
    format!(
        "(({value})::date - DATE '1970-01-01')::int8 * 86400000000 \
         + round(extract(epoch FROM ({value})::time) * 1000000)::int8"
    )
}

/// The type of the innermost elements of a list (of lists).
fn innermost(item: &Field) -> &DataType {
    let mut innermost = item.data_type();
    while let DataType::List(inner) = innermost {
        innermost = inner.data_type();
    }
    innermost
}

/// How column `column` of the wrapped statement is sent: as it is; as JSON
/// when it is a list (text and decimal elements cast to `text` first, so
/// JSON holds them as strings: a `json[]` element stays its text, a decimal
/// keeps every digit); a timestamp as its microseconds since 1970 in UTC; a
/// decimal as its text.
fn sent_as(data_type: &DataType, column: &str) -> Option<String> {
    match data_type {
        DataType::List(item) => Some(
            if matches!(innermost(item), DataType::Utf8 | DataType::Decimal128(..)) {
                format!("to_json({column}::text[])")
            } else {
                format!("to_json({column})")
            },
        ),
        DataType::Timestamp(_, None) => Some(micros_of(column)),
        DataType::Timestamp(_, Some(_)) => Some(micros_of(&format!("{column} AT TIME ZONE 'UTC'"))),
        DataType::Decimal128(..) => Some(format!("{column}::text")),
        _ => None,
    }
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `sql`, or, when its result (`projected`, by position) has a column
/// [`sent_as`] sends otherwise, `sql` under a select that sends each column
/// so and every column under its projected name.
pub(super) fn readable<'a>(sql: &'a str, projected: Option<&Schema>) -> Cow<'a, str> {
    let Some(schema) = projected else {
        return Cow::Borrowed(sql);
    };
    let fields = schema.fields();
    if !fields.iter().any(|f| sent_as(f.data_type(), "c").is_some()) {
        return Cow::Borrowed(sql);
    }
    let mut select = String::from("SELECT ");
    let mut positions = String::new();
    for (index, field) in fields.iter().enumerate() {
        let column = format!("codelake_rows.c{}", index + 1);
        let value = sent_as(field.data_type(), &column).unwrap_or(column);
        if index > 0 {
            select.push_str(", ");
            positions.push_str(", ");
        }
        let _ = write!(select, "{value} AS {}", quoted(field.name()));
        let _ = write!(positions, "c{}", index + 1);
    }
    Cow::Owned(format!("{select} FROM ({sql}) AS codelake_rows({positions})"))
}

/// The schema the row decoder reads a [`readable`] statement's answer in:
/// `projected`, with each list of decimals a list of texts (as sent).
pub(super) fn decoded(projected: &Schema) -> Schema {
    fn texts(data_type: &DataType) -> DataType {
        match data_type {
            DataType::List(item) => DataType::List(Arc::new(
                item.as_ref().clone().with_data_type(texts(item.data_type())),
            )),
            DataType::Decimal128(..) => DataType::Utf8,
            other => other.clone(),
        }
    }
    let fields: Vec<Field> = projected
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            list @ DataType::List(_) => field.as_ref().clone().with_data_type(texts(list)),
            _ => field.as_ref().clone(),
        })
        .collect();
    Schema::new_with_metadata(fields, projected.metadata().clone())
}

/// The unscaled value of `text`, a `numeric` as PostgreSQL spells it, in a
/// `decimal(precision, scale)`: rounded to `scale` fraction digits as
/// `rounding` says (`Unnecessary`: an error), or an error naming `column`
/// when it does not fit then.
fn decimal_value(
    text: &str,
    precision: u8,
    scale: i8,
    column: &str,
    rounding: DecimalRoundingMode,
) -> Result<i128, String> {
    let refuse = |why: &str| {
        format!(
            "the PostgreSQL value {text} of column \"{column}\" does not fit decimal({precision},{scale}): {why}"
        )
    };
    let scale = u8::try_from(scale).map_err(|_| refuse("a negative scale"))?;
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let is_digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if integer.is_empty() && fraction.is_empty() || !is_digits(integer) || !is_digits(fraction) {
        return Err(refuse("a decimal holds only finite numbers"));
    }
    let integer = integer.trim_start_matches('0');
    let fraction = fraction.trim_end_matches('0');
    let integer_digits = precision.saturating_sub(scale);
    let too_long = || refuse(&format!("more than {integer_digits} integer digits"));
    if integer.len() > usize::from(integer_digits) {
        return Err(too_long());
    }
    let (kept, dropped) = fraction.split_at(fraction.len().min(usize::from(scale)));
    // `dropped` is not zero: the fraction's trailing zeros are gone.
    let away = match dropped.as_bytes() {
        [] => false,
        [first, rest @ ..] => {
            let half = first.cmp(&b'5');
            let past_half = !rest.is_empty();
            // The last digit kept; ASCII digits have their value's parity.
            let odd = integer
                .bytes()
                .chain(kept.bytes())
                .next_back()
                .is_some_and(|digit| digit % 2 == 1);
            match rounding {
                DecimalRoundingMode::Unnecessary => {
                    return Err(refuse(&format!("more than {scale} fraction digits")));
                }
                DecimalRoundingMode::Up => true,
                DecimalRoundingMode::Down => false,
                DecimalRoundingMode::Ceiling => !negative,
                DecimalRoundingMode::Floor => negative,
                DecimalRoundingMode::HalfUp => half != Ordering::Less,
                DecimalRoundingMode::HalfDown => {
                    half == Ordering::Greater || half == Ordering::Equal && past_half
                }
                DecimalRoundingMode::HalfEven => {
                    half == Ordering::Greater || half == Ordering::Equal && (past_half || odd)
                }
            }
        }
    };
    let unscaled = format!("{integer}{kept:0<width$}", width = usize::from(scale));
    let mut magnitude: i128 = if unscaled.is_empty() {
        0
    } else {
        unscaled.parse().map_err(|_| refuse("not a number"))?
    };
    if away {
        magnitude += 1;
        if magnitude >= 10_i128.pow(u32::from(precision)) {
            return Err(too_long());
        }
    }
    Ok(if negative { -magnitude } else { magnitude })
}

/// `texts`, the text of each decimal of a column, as `decimal(precision, scale)`.
fn decimals_of(
    texts: &ArrayRef,
    (precision, scale): (u8, i8),
    column: &str,
    rounding: DecimalRoundingMode,
) -> Result<ArrayRef, String> {
    let mut values = Decimal128Builder::with_capacity(texts.len());
    for text in texts.as_string::<i32>() {
        match text {
            Some(text) => values.append_value(decimal_value(text, precision, scale, column, rounding)?),
            None => values.append_null(),
        }
    }
    let values = values
        .finish()
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(values))
}

/// `column`, a list (of lists) of texts, as the list of decimals `target` is.
fn decimal_lists(
    column: &ArrayRef,
    target: &DataType,
    name: &str,
    rounding: DecimalRoundingMode,
) -> Result<ArrayRef, String> {
    match (column.data_type(), target) {
        (DataType::Utf8, DataType::Decimal128(precision, scale)) => {
            decimals_of(column, (*precision, *scale), name, rounding)
        }
        (DataType::List(_), DataType::List(item)) => {
            let list = column.as_list::<i32>();
            let values = decimal_lists(list.values(), item.data_type(), name, rounding)?;
            let list = ListArray::try_new(
                Arc::clone(item),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )
            .map_err(|e| e.to_string())?;
            Ok(Arc::new(list))
        }
        (found, _) => Err(format!(
            "the PostgreSQL list of column \"{name}\" was read as {found}, not as texts"
        )),
    }
}

/// `values` (microseconds since 1970) in `unit`, exactly.
fn timestamps_in(
    values: &PrimitiveArray<Int64Type>,
    unit: TimeUnit,
    zone: Option<Arc<str>>,
    column: &str,
) -> Result<ArrayRef, String> {
    let convert = |micros: i64| -> Result<i64, String> {
        let exact = |factor: i64| (micros % factor == 0).then(|| micros / factor);
        match unit {
            TimeUnit::Second => exact(1_000_000),
            TimeUnit::Millisecond => exact(1_000),
            TimeUnit::Microsecond => Some(micros),
            TimeUnit::Nanosecond => micros.checked_mul(1_000),
        }
        .ok_or_else(|| format!("the PostgreSQL timestamp of column \"{column}\" does not fit its type ({micros} microseconds)"))
    };
    let converted: Vec<Option<i64>> = values
        .iter()
        .map(|value| value.map(convert).transpose())
        .collect::<Result<_, _>>()?;
    Ok(match unit {
        TimeUnit::Second => {
            Arc::new(PrimitiveArray::<TimestampSecondType>::from(converted).with_timezone_opt(zone))
        }
        TimeUnit::Millisecond => {
            Arc::new(PrimitiveArray::<TimestampMillisecondType>::from(converted).with_timezone_opt(zone))
        }
        TimeUnit::Microsecond => {
            Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(converted).with_timezone_opt(zone))
        }
        TimeUnit::Nanosecond => {
            Arc::new(PrimitiveArray::<TimestampNanosecondType>::from(converted).with_timezone_opt(zone))
        }
    })
}

/// Column `column` as the projected field `field` declares it, when the
/// source sent it in another form ([`sent_as`], or a time in nanoseconds).
fn restored(
    column: &ArrayRef,
    field: &Field,
    rounding: DecimalRoundingMode,
) -> Result<Option<ArrayRef>, String> {
    let name = field.name();
    Ok(match (column.data_type(), field.data_type()) {
        (DataType::Utf8, DataType::Decimal128(precision, scale)) => {
            Some(decimals_of(column, (*precision, *scale), name, rounding)?)
        }
        (DataType::List(_), target @ DataType::List(item))
            if matches!(innermost(item), DataType::Decimal128(..)) =>
        {
            Some(decimal_lists(column, target, name, rounding)?)
        }
        (DataType::Int64, DataType::Timestamp(unit, zone)) => Some(timestamps_in(
            column.as_primitive::<Int64Type>(),
            *unit,
            zone.clone(),
            name,
        )?),
        (DataType::Time64(_), target @ (DataType::Time32(_) | DataType::Time64(_)))
            if column.data_type() != target =>
        {
            Some(cast(column, target).map_err(|e| e.to_string())?)
        }
        _ => None,
    })
}

/// `batch`, a decoded answer to a [`readable`] statement, with each column
/// the source sent in another form as `projected` declares it, decimals
/// rounded as `rounding` says.
pub(super) fn restore(
    batch: RecordBatch,
    projected: Option<&Schema>,
    rounding: DecimalRoundingMode,
) -> Result<RecordBatch, String> {
    let Some(projected) = projected.filter(|p| p.fields().len() == batch.num_columns()) else {
        return Ok(batch);
    };
    let mut changed = false;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for ((column, field), wanted) in batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .zip(projected.fields())
    {
        if let Some(column) = restored(column, wanted, rounding)? {
            changed = true;
            fields.push(wanted.as_ref().clone());
            columns.push(column);
        } else {
            fields.push(field.as_ref().clone());
            columns.push(Arc::clone(column));
        }
    }
    if !changed {
        return Ok(batch);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRICT: Decimals = Decimals {
        mapping: DecimalMapping::Strict,
        rounding_mode: DecimalRoundingMode::Unnecessary,
        default_scale: 0,
    };

    fn column(type_name: &str) -> Column {
        Column {
            name: "c".into(),
            type_name: type_name.into(),
            kind: "b".into(),
            dimensions: 1,
            ..Column::default()
        }
    }

    fn array(element: &str, dimensions: i32) -> Column {
        Column {
            name: "c".into(),
            type_name: format!("{element}[]"),
            kind: "b".into(),
            category: "A".into(),
            element: Some(element.into()),
            element_kind: Some("b".into()),
            dimensions,
            ..Column::default()
        }
    }

    fn list(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }

    #[test]
    fn scalars_keep_the_upstream_types() {
        let cases = [
            ("smallint", DataType::Int16),
            ("integer", DataType::Int32),
            ("bigint", DataType::Int64),
            ("money", DataType::Int64),
            ("numeric(10,2)", DataType::Decimal128(10, 2)),
            ("numeric(5)", DataType::Decimal128(5, 0)),
            ("numeric(38,2)", DataType::Decimal128(38, 2)),
            ("real", DataType::Float32),
            ("double precision", DataType::Float64),
            ("character varying(10)", DataType::Utf8),
            ("character(3)", DataType::Utf8),
            ("text", DataType::Utf8),
            ("uuid", DataType::Utf8),
            ("bytea", DataType::Binary),
            ("date", DataType::Date32),
            ("time without time zone", DataType::Time64(TimeUnit::Microsecond)),
            (
                "time(5) without time zone",
                DataType::Time64(TimeUnit::Microsecond),
            ),
            (
                "time(3) without time zone",
                DataType::Time32(TimeUnit::Millisecond),
            ),
            (
                "time(1) without time zone",
                DataType::Time32(TimeUnit::Millisecond),
            ),
            ("time(0) without time zone", DataType::Time32(TimeUnit::Second)),
            (
                "timestamp without time zone",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "timestamp(4) without time zone",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "timestamp(2) without time zone",
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            (
                "timestamp(0) without time zone",
                DataType::Timestamp(TimeUnit::Second, None),
            ),
            (
                "timestamp with time zone",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                "timestamp(3) with time zone",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ),
            (
                "timestamp(0) with time zone",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            ),
            ("interval", DataType::Interval(IntervalUnit::MonthDayNano)),
            ("boolean", DataType::Boolean),
            ("tsvector", DataType::LargeUtf8),
            ("oid", DataType::UInt32),
            ("\"char\"", DataType::Int8),
            (
                "point",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2),
            ),
            (
                "int4range",
                DataType::Struct(Fields::from(vec![
                    Field::new("lower", DataType::Int32, true),
                    Field::new("upper", DataType::Int32, true),
                ])),
            ),
        ];
        for (type_name, expected) in cases {
            assert_eq!(
                arrow_type(&column(type_name), STRICT),
                Ok(Some(expected)),
                "{type_name}"
            );
        }
        assert_eq!(arrow_type(&column("jsonb"), STRICT), Err("jsonb".to_owned()));
        assert_eq!(
            arrow_type(&column("time with time zone"), STRICT),
            Err("time with time zone".to_owned())
        );
    }

    #[test]
    fn enums_and_composites() {
        let mood = Column {
            kind: "e".into(),
            ..column("mood")
        };
        assert_eq!(
            arrow_type(&mood, STRICT),
            Ok(Some(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Utf8)
            )))
        );
        let pair = Column {
            kind: "c".into(),
            fields: vec![("a".into(), "integer".into()), ("b".into(), "text".into())],
            ..column("pair")
        };
        assert_eq!(
            arrow_type(&pair, STRICT),
            Ok(Some(DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]))))
        );
        let odd = Column {
            fields: vec![("a".into(), "jsonb".into())],
            ..pair
        };
        assert_eq!(arrow_type(&odd, STRICT), Err("jsonb".to_owned()));
    }

    #[test]
    fn arrays_follow_the_reference_engine() {
        let cases = [
            ("date", DataType::Date32),
            (
                "timestamp(3) without time zone",
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            (
                "timestamp without time zone",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                "timestamp with time zone",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                "timestamp(0) with time zone",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            ),
            ("time without time zone", DataType::Time64(TimeUnit::Microsecond)),
            (
                "time(3) without time zone",
                DataType::Time32(TimeUnit::Millisecond),
            ),
            ("numeric(10,2)", DataType::Decimal128(10, 2)),
            ("text", DataType::Utf8),
            ("character varying(10)", DataType::Utf8),
            ("character(3)", DataType::Utf8),
            ("\"char\"", DataType::Utf8),
            ("name", DataType::Utf8),
            ("uuid", DataType::Utf8),
            ("json", DataType::Utf8),
            ("jsonb", DataType::Utf8),
            ("money", DataType::Utf8),
            ("smallint", DataType::Int16),
            ("integer", DataType::Int32),
            ("bigint", DataType::Int64),
            ("real", DataType::Float32),
            ("double precision", DataType::Float64),
            ("boolean", DataType::Boolean),
        ];
        for (element, item) in cases {
            assert_eq!(
                arrow_type(&array(element, 1), STRICT),
                Ok(Some(list(item))),
                "{element}[]"
            );
        }
        for element in [
            "numeric",
            "bytea",
            "interval",
            "inet",
            "oid",
            "xml",
            "point",
            "int4range",
        ] {
            assert_eq!(arrow_type(&array(element, 1), STRICT), Ok(None), "{element}[]");
        }
        let mood = Column {
            element_kind: Some("e".into()),
            ..array("mood", 1)
        };
        assert_eq!(arrow_type(&mood, STRICT), Ok(Some(list(DataType::Utf8))));
        assert_eq!(
            arrow_type(&array("integer", 2), STRICT),
            Ok(Some(list(list(DataType::Int32))))
        );
        assert_eq!(
            arrow_type(&array("integer", 0), STRICT),
            Ok(Some(list(DataType::Int32)))
        );
        let missing = Column {
            element: None,
            ..array("integer", 1)
        };
        assert_eq!(arrow_type(&missing, STRICT), Ok(None));
    }

    #[test]
    fn lists_travel_as_json() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let sql = "SELECT id, label FROM t";
        assert!(matches!(readable(sql, Some(&schema)), Cow::Borrowed(s) if s == sql));
        assert!(matches!(readable(sql, None), Cow::Borrowed(s) if s == sql));

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("days", list(DataType::Date32), true),
            Field::new("tags \"x\"", list(DataType::Utf8), true),
            Field::new("grid", list(list(DataType::Int32)), true),
            Field::new("names", list(list(DataType::Utf8)), true),
            Field::new("amounts", list(DataType::Decimal128(10, 2)), true),
        ]);
        assert_eq!(
            readable("SELECT * FROM t", Some(&schema)),
            "SELECT codelake_rows.c1 AS \"id\", to_json(codelake_rows.c2) AS \"days\", \
             to_json(codelake_rows.c3::text[]) AS \"tags \"\"x\"\"\", to_json(codelake_rows.c4) AS \"grid\", \
             to_json(codelake_rows.c5::text[]) AS \"names\", to_json(codelake_rows.c6::text[]) AS \"amounts\" \
             FROM (SELECT * FROM t) AS codelake_rows(c1, c2, c3, c4, c5, c6)"
        );
    }

    #[test]
    fn precisions_other_than_0_3_6_are_declared() {
        let cases = [
            ("timestamp(1) without time zone", Some(Declared::Timestamp(1))),
            ("timestamp(2) with time zone", Some(Declared::Timestamp(2))),
            ("timestamp(5) without time zone", Some(Declared::Timestamp(5))),
            ("time(4) without time zone", Some(Declared::Time(4))),
            ("timestamp(3) without time zone", None),
            ("timestamp(6) with time zone", None),
            ("timestamp without time zone", None),
            ("time(0) without time zone", None),
            ("numeric(10,2)", None),
            ("character varying(5)", None),
        ];
        for (type_name, expected) in cases {
            assert_eq!(declared(&column(type_name)), expected, "{type_name}");
        }
        assert_eq!(declared(&array("timestamp(2) without time zone", 1)), None);
        assert_eq!(datetime_precision("timestamp(9) without time zone"), 6);
        assert_eq!(datetime_precision("timestamp(x) without time zone"), 6);
    }

    #[test]
    fn timestamps_and_decimals_travel_in_exact_forms() {
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "tz",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("n", DataType::Decimal128(38, 20), true),
            Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
        ]);
        assert_eq!(
            readable("SELECT * FROM t", Some(&schema)),
            "SELECT ((codelake_rows.c1)::date - DATE '1970-01-01')::int8 * 86400000000 \
             + round(extract(epoch FROM (codelake_rows.c1)::time) * 1000000)::int8 AS \"ts\", \
             ((codelake_rows.c2 AT TIME ZONE 'UTC')::date - DATE '1970-01-01')::int8 * 86400000000 \
             + round(extract(epoch FROM (codelake_rows.c2 AT TIME ZONE 'UTC')::time) * 1000000)::int8 AS \"tz\", \
             codelake_rows.c3::text AS \"n\", codelake_rows.c4 AS \"t\" \
             FROM (SELECT * FROM t) AS codelake_rows(c1, c2, c3, c4)"
        );
    }

    #[test]
    fn decimals_fit_exactly_or_fail_naming_the_column() {
        let fits = |text: &str| decimal_value(text, 38, 20, "vl", DecimalRoundingMode::Unnecessary);
        assert_eq!(fits("1.5"), Ok(150_000_000_000_000_000_000));
        assert_eq!(fits("-1.50"), Ok(-150_000_000_000_000_000_000));
        assert_eq!(fits("0"), Ok(0));
        assert_eq!(fits("-0.000"), Ok(0));
        assert_eq!(fits("007"), Ok(700_000_000_000_000_000_000));
        assert_eq!(fits(".5"), Ok(50_000_000_000_000_000_000));
        assert_eq!(fits("5."), Ok(500_000_000_000_000_000_000));
        // 18 integer and 20 fraction digits: the widest value.
        assert_eq!(
            fits("999999999999999999.99999999999999999999"),
            Ok(99_999_999_999_999_999_999_999_999_999_999_999_999)
        );
        assert_eq!(
            fits("123456789012345678.12345678901234567890"),
            Ok(12_345_678_901_234_567_812_345_678_901_234_567_890)
        );
        // Trailing zeros past the scale lose nothing.
        assert_eq!(
            fits("0.1000000000000000000000000"),
            Ok(10_000_000_000_000_000_000)
        );
        assert_eq!(
            fits("100000000000000000000"),
            Err(
                "the PostgreSQL value 100000000000000000000 of column \"vl\" does not fit \
                 decimal(38,20): more than 18 integer digits"
                    .to_owned()
            )
        );
        assert_eq!(
            fits("1000000000000000000"),
            Err(
                "the PostgreSQL value 1000000000000000000 of column \"vl\" does not fit \
                 decimal(38,20): more than 18 integer digits"
                    .to_owned()
            )
        );
        assert_eq!(
            fits("0.123456789012345678901"),
            Err(
                "the PostgreSQL value 0.123456789012345678901 of column \"vl\" does not fit \
                 decimal(38,20): more than 20 fraction digits"
                    .to_owned()
            )
        );
        for special in ["NaN", "Infinity", "-Infinity", "", "-", ".", "1e5", "+1", "1.2.3"] {
            assert_eq!(
                fits(special),
                Err(format!(
                    "the PostgreSQL value {special} of column \"vl\" does not fit decimal(38,20): \
                     a decimal holds only finite numbers"
                )),
                "{special}"
            );
        }
        assert_eq!(
            decimal_value("12.5", 3, 1, "c", DecimalRoundingMode::Unnecessary),
            Ok(125)
        );
        assert!(decimal_value("123.5", 3, 1, "c", DecimalRoundingMode::Unnecessary).is_err());
        assert_eq!(
            decimal_value("-99", 2, 0, "c", DecimalRoundingMode::Unnecessary),
            Ok(-99)
        );
        assert!(decimal_value("1", 2, -1, "c", DecimalRoundingMode::Unnecessary).is_err());
    }

    #[test]
    fn decoded_columns_get_their_declared_types() {
        use datafusion::arrow::array::{
            Decimal128Array, Int32Array, Int64Array, StringArray, Time32MillisecondArray,
            Time64MicrosecondArray, Time64NanosecondArray, TimestampMillisecondArray, TimestampSecondArray,
        };
        let projected = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("n", DataType::Decimal128(10, 2), true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "tz",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            )
            .with_metadata(Declared::Timestamp(2).metadata()),
            Field::new("t3", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("t6", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new("ns", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]);
        let decoded = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("n", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
            Field::new("tz", DataType::Int64, true),
            Field::new("t3", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new("t6", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new("ns", DataType::Int64, true),
        ]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("-12.5"), None])),
            Arc::new(Int64Array::from(vec![Some(-315_619_200_000_000), None])),
            Arc::new(Int64Array::from(vec![Some(253_402_300_799_120_000), None])),
            Arc::new(Time64NanosecondArray::from(vec![Some(45_296_123_000_000), None])),
            Arc::new(Time64NanosecondArray::from(vec![Some(45_296_123_456_000), None])),
            Arc::new(Int64Array::from(vec![Some(-1), None])),
        ];
        let batch = RecordBatch::try_new(Arc::new(decoded), columns).unwrap();
        let restored = restore(batch, Some(&projected), DecimalRoundingMode::Unnecessary).unwrap();
        assert_eq!(restored.schema().as_ref(), &projected);
        let n = restored
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!((n.value(0), n.is_null(1)), (-1250, true));
        let ts = restored
            .column(2)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert_eq!((ts.value(0), ts.is_null(1)), (-315_619_200, true));
        let tz = restored
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(tz.value(0), 253_402_300_799_120);
        let t3 = restored
            .column(4)
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();
        assert_eq!(t3.value(0), 45_296_123);
        let t6 = restored
            .column(5)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(t6.value(0), 45_296_123_456);
        let ns = restored
            .column(6)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ns.value(0), -1_000);

        // A batch whose columns need nothing, or without a projected schema, is kept.
        let plain = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(plain.clone()),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        assert_eq!(
            restore(batch.clone(), Some(&plain), DecimalRoundingMode::Unnecessary).unwrap(),
            batch
        );
        assert_eq!(
            restore(batch.clone(), None, DecimalRoundingMode::Unnecessary).unwrap(),
            batch
        );
        assert_eq!(
            restore(batch.clone(), Some(&projected), DecimalRoundingMode::Unnecessary).unwrap(),
            batch
        );
    }

    /// A value the declared type cannot hold fails, naming its column.
    #[test]
    fn values_the_declared_type_cannot_hold_fail() {
        use datafusion::arrow::array::{Int64Array, StringArray};
        let one = |data_type: DataType, column: ArrayRef| {
            let projected = Schema::new(vec![Field::new("c", data_type, true)]);
            let decoded = Schema::new(vec![Field::new("c", column.data_type().clone(), true)]);
            restore(
                RecordBatch::try_new(Arc::new(decoded), vec![column]).unwrap(),
                Some(&projected),
                DecimalRoundingMode::Unnecessary,
            )
        };
        let error = one(
            DataType::Decimal128(38, 20),
            Arc::new(StringArray::from(vec!["NaN"])),
        )
        .unwrap_err();
        assert!(error.contains("NaN of column \"c\""), "{error}");
        let error = one(
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Arc::new(Int64Array::from(vec![1_500])),
        )
        .unwrap_err();
        assert!(error.contains("column \"c\""), "{error}");
        let error = one(
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            Arc::new(Int64Array::from(vec![i64::MAX])),
        )
        .unwrap_err();
        assert!(error.contains("column \"c\""), "{error}");
    }

    fn decimals(mapping: DecimalMapping, default_scale: u8) -> Decimals {
        Decimals {
            mapping,
            rounding_mode: DecimalRoundingMode::Unnecessary,
            default_scale,
        }
    }

    /// The reference engine's `decimal-mapping`, as Trino 431 maps the
    /// columns of a PostgreSQL 17 table (`SHOW COLUMNS`): `STRICT` leaves a
    /// `numeric` without precision or with more than 38 digits out, and an
    /// array of them; `ALLOW_OVERFLOW` makes it `decimal(38, s)`.
    #[test]
    fn numerics_follow_the_decimal_mapping() {
        let overflow = |scale| decimals(DecimalMapping::AllowOverflow, scale);
        let decimal = |p, s| Ok(Some(DataType::Decimal128(p, s)));
        let decimal_list = |p, s| Ok(Some(list(DataType::Decimal128(p, s))));
        for type_name in ["numeric", "numeric(50,10)", "numeric(40)", "numeric(1000,0)"] {
            assert_eq!(arrow_type(&column(type_name), STRICT), Ok(None), "{type_name}");
            assert_eq!(
                arrow_type(&array(type_name, 1), STRICT),
                Ok(None),
                "{type_name}[]"
            );
        }
        assert_eq!(arrow_type(&column("numeric(38,2)"), STRICT), decimal(38, 2));
        assert_eq!(
            arrow_type(&array("numeric(38,2)", 1), STRICT),
            decimal_list(38, 2)
        );
        // Trino 431, decimal-default-scale 4 (SHOW COLUMNS).
        let cases = [
            ("numeric", decimal(38, 4)),
            ("numeric(10,2)", decimal(10, 2)),
            ("numeric(38,2)", decimal(38, 2)),
            ("numeric(50,10)", decimal(38, 4)),
            ("numeric(40)", decimal(38, 0)),
            ("numeric(1000,0)", decimal(38, 0)),
        ];
        for (type_name, expected) in cases {
            assert_eq!(
                arrow_type(&column(type_name), overflow(4)),
                expected,
                "{type_name}"
            );
        }
        assert_eq!(arrow_type(&array("numeric", 1), overflow(4)), decimal_list(38, 0));
        assert_eq!(
            arrow_type(&array("numeric(50,10)", 1), overflow(4)),
            decimal_list(38, 4)
        );
        assert_eq!(
            arrow_type(&array("numeric(10,2)", 1), overflow(4)),
            decimal_list(10, 2)
        );
        assert_eq!(
            arrow_type(&array("numeric", 2), overflow(4)),
            Ok(Some(list(list(DataType::Decimal128(38, 0)))))
        );
        // Other default scales: the declared scale bounds it beyond 38 digits.
        for (scale, unbounded, wide) in [(0, 0, 0), (1, 1, 1), (20, 20, 10), (37, 37, 10), (38, 38, 10)] {
            assert_eq!(
                arrow_type(&column("numeric"), overflow(scale)),
                decimal(38, unbounded)
            );
            assert_eq!(
                arrow_type(&column("numeric(50,10)"), overflow(scale)),
                decimal(38, wide)
            );
            assert_eq!(
                arrow_type(&array("numeric(50,10)", 1), overflow(scale)),
                decimal_list(38, wide)
            );
            assert_eq!(
                arrow_type(&array("numeric", 1), overflow(scale)),
                decimal_list(38, 0)
            );
        }
        // Beyond a decimal's largest scale, the largest.
        assert_eq!(arrow_type(&column("numeric"), overflow(60)), decimal(38, 38));
        // A composite with such a field is left out, with either mapping.
        let composite = |field: &str| Column {
            kind: "c".into(),
            fields: vec![("a".into(), "integer".into()), ("b".into(), field.into())],
            ..column("pair")
        };
        for mapping in [STRICT, overflow(4)] {
            assert_eq!(arrow_type(&composite("numeric"), mapping), Ok(None));
            assert_eq!(arrow_type(&composite("numeric(39,2)"), mapping), Ok(None));
            assert_eq!(
                arrow_type(&composite("numeric(10,2)"), mapping),
                Ok(Some(DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Decimal128(10, 2), true),
                ]))))
            );
        }
        assert!(wide_numeric("numeric"));
        assert!(wide_numeric("numeric(39)"));
        assert!(!wide_numeric("numeric(38,0)"));
        assert!(!wide_numeric("integer"));
        assert_eq!(numeric_precision("numeric(1000,5)"), Some((1000, 5)));
    }

    /// Rounding to the scale as Trino 431 rounds a PostgreSQL 17 `numeric`
    /// read with `decimal-mapping=ALLOW_OVERFLOW` (each mode at scale 0, the
    /// answers it gave); then the value must fit the precision.
    #[test]
    fn decimals_round_as_the_reference_engine() {
        use DecimalRoundingMode::{Ceiling, Down, Floor, HalfDown, HalfEven, HalfUp, Unnecessary, Up};
        const NINES: &str = "99999999999999999999999999999999999999";
        let nines: i128 = NINES.parse().unwrap();
        let values = ["1.5", "2.5", "-2.5", "0.125", "0.00004999", "-0.00005"];
        let expected: [(DecimalRoundingMode, [i128; 6]); 7] = [
            (Up, [2, 3, -3, 1, 1, -1]),
            (Down, [1, 2, -2, 0, 0, 0]),
            (Ceiling, [2, 3, -2, 1, 1, 0]),
            (Floor, [1, 2, -3, 0, 0, -1]),
            (HalfUp, [2, 3, -3, 0, 0, 0]),
            (HalfDown, [1, 2, -2, 0, 0, 0]),
            (HalfEven, [2, 2, -2, 0, 0, 0]),
        ];
        for (mode, answers) in expected {
            for (value, answer) in values.iter().zip(answers) {
                assert_eq!(
                    decimal_value(value, 38, 0, "n", mode),
                    Ok(answer),
                    "{value} {mode:?}"
                );
            }
            // Rounding past the precision overflows (Trino: "Decimal overflow").
            let tie = format!("{NINES}.5");
            let rounded = decimal_value(&tie, 38, 0, "n", mode);
            if matches!(mode, Down | Floor | HalfDown) {
                assert_eq!(rounded, Ok(nines), "{mode:?}");
            } else {
                assert_eq!(
                    rounded,
                    Err(format!(
                        "the PostgreSQL value {tie} of column \"n\" does not fit decimal(38,0): \
                         more than 38 integer digits"
                    )),
                    "{mode:?}"
                );
            }
            assert_eq!(decimal_value("1.000000", 38, 0, "n", mode), Ok(1));
            assert_eq!(decimal_value(NINES, 38, 0, "n", mode), Ok(nines));
        }
        // Without a rounding mode, a digit past the scale fails (Trino: "Rounding necessary").
        for value in values {
            assert_eq!(
                decimal_value(value, 38, 0, "n", Unnecessary),
                Err(format!(
                    "the PostgreSQL value {value} of column \"n\" does not fit decimal(38,0): \
                     more than 0 fraction digits"
                ))
            );
        }
        // HALF_UP at scale 4 (Trino 431: 123.4568, 0.1235, -0.0001, 0.0000).
        let half_up = |value: &str| decimal_value(value, 38, 4, "n", HalfUp);
        assert_eq!(half_up("123.456789"), Ok(1_234_568));
        assert_eq!(half_up("0.12345678901"), Ok(1_235));
        assert_eq!(half_up("-0.00005"), Ok(-1));
        assert_eq!(half_up("0.00004999"), Ok(0));
        assert_eq!(half_up("99.99995"), Ok(1_000_000));
        assert_eq!(
            half_up("100000000000000000000"),
            Ok(1_000_000_000_000_000_000_000_000)
        );
        assert!(half_up("10000000000000000000000000000000000000").is_err());
        assert!(half_up("NaN").is_err());
        // HALF_EVEN looks at the last kept fraction digit, and past the half.
        assert_eq!(decimal_value("0.125", 38, 2, "n", HalfEven), Ok(12));
        assert_eq!(decimal_value("0.135", 38, 2, "n", HalfEven), Ok(14));
        assert_eq!(decimal_value("0.1251", 38, 2, "n", HalfEven), Ok(13));
        assert_eq!(decimal_value("0.1251", 38, 2, "n", HalfDown), Ok(13));
        assert_eq!(decimal_value("0.125", 38, 2, "n", HalfDown), Ok(12));
        assert_eq!(decimal_value("0.5", 38, 0, "n", HalfEven), Ok(0));
        assert_eq!(decimal_value("1.125", 38, 2, "n", HalfEven), Ok(112));
        assert_eq!(decimal_value("2.135", 38, 2, "n", HalfEven), Ok(214));
        assert_eq!(decimal_value("-3.5", 38, 0, "n", HalfEven), Ok(-4));
        assert_eq!(decimal_value("10.5", 38, 0, "n", HalfEven), Ok(10));
        // decimal(38,38): no integer digit (Trino: "Decimal overflow" for 1.5).
        assert!(decimal_value("1.5", 38, 38, "n", HalfUp).is_err());
        assert_eq!(decimal_value("0.5", 38, 38, "n", HalfUp), Ok(5 * 10_i128.pow(37)));
        assert_eq!(decimal_value("9.95", 2, 1, "n", HalfUp), Err(
            "the PostgreSQL value 9.95 of column \"n\" does not fit decimal(2,1): more than 1 integer digits"
                .to_owned()
        ));
        assert_eq!(decimal_value("9.94", 2, 1, "n", HalfUp), Ok(99));
    }

    /// A list of decimals travels as a list of texts, which the row decoder
    /// reads as such.
    #[test]
    fn lists_of_decimals_travel_as_texts() {
        let projected = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("amounts", list(DataType::Decimal128(38, 0)), true),
            Field::new("grid", list(list(DataType::Decimal128(10, 2))), true),
            Field::new("tags", list(DataType::Utf8), true),
            Field::new("n", DataType::Decimal128(38, 4), true),
        ]);
        let decoded = decoded(&projected);
        let types: Vec<&DataType> = decoded.fields().iter().map(|f| f.data_type()).collect();
        assert_eq!(
            types,
            [
                &DataType::Int32,
                &list(DataType::Utf8),
                &list(list(DataType::Utf8)),
                &list(DataType::Utf8),
                &DataType::Decimal128(38, 4),
            ]
        );
        assert_eq!(
            readable("SELECT * FROM t", Some(&projected)),
            "SELECT codelake_rows.c1 AS \"id\", to_json(codelake_rows.c2::text[]) AS \"amounts\", \
             to_json(codelake_rows.c3::text[]) AS \"grid\", to_json(codelake_rows.c4::text[]) AS \"tags\", \
             codelake_rows.c5::text AS \"n\" FROM (SELECT * FROM t) AS codelake_rows(c1, c2, c3, c4, c5)"
        );
    }

    /// The decoded lists of texts become the declared lists of decimals,
    /// rounded as the catalog says.
    #[test]
    fn decoded_decimal_lists_are_restored_and_rounded() {
        use datafusion::arrow::array::{Decimal128Array, ListBuilder, StringBuilder};
        let projected = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("amounts", list(DataType::Decimal128(38, 0)), true),
            Field::new("grid", list(list(DataType::Decimal128(10, 2))), true),
            Field::new("tags", list(DataType::Utf8), true),
            Field::new("n", DataType::Decimal128(38, 4), true),
        ]);
        let decoded = decoded(&projected);
        let mut amounts = ListBuilder::new(StringBuilder::new());
        amounts.values().append_value("1.5");
        amounts.values().append_null();
        amounts.append(true);
        amounts.append(false);
        amounts.values().append_value("-2.5");
        amounts.append(true);
        let mut grid = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
        grid.values().values().append_value("1.25");
        grid.values().append(true);
        grid.append(true);
        grid.append(false);
        grid.append(true);
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("x");
        tags.append(true);
        tags.append(true);
        tags.append(true);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(amounts.finish()),
            Arc::new(grid.finish()),
            Arc::new(tags.finish()),
            Arc::new(datafusion::arrow::array::StringArray::from(vec![
                Some("0.00005"),
                None,
                Some("1"),
            ])),
        ];
        // A decimal arrives as text: the decoder types it by what PostgreSQL sent.
        let mut fields: Vec<Field> = decoded.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields[4] = Field::new("n", DataType::Utf8, true);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let restored = restore(batch.clone(), Some(&projected), DecimalRoundingMode::HalfUp).unwrap();
        assert_eq!(restored.schema().as_ref(), &projected);
        let amounts = restored.column(1).as_list::<i32>();
        assert!(amounts.is_null(1));
        let first = amounts.value(0);
        let first = first.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!((first.value(0), first.is_null(1)), (2, true));
        let third = amounts.value(2);
        assert_eq!(
            third.as_any().downcast_ref::<Decimal128Array>().unwrap().value(0),
            -3
        );
        let grid = restored.column(2).as_list::<i32>();
        assert!(grid.is_null(1));
        let inner = grid.value(0);
        let inner = inner.as_list::<i32>().value(0);
        assert_eq!(
            inner.as_any().downcast_ref::<Decimal128Array>().unwrap().value(0),
            125
        );
        let n = restored
            .column(4)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!((n.value(0), n.is_null(1), n.value(2)), (1, true, 10_000));
        assert_eq!(restored.column(3), batch.column(3));

        // Without a rounding mode an element with more digits fails, naming its column.
        let error = restore(batch, Some(&projected), DecimalRoundingMode::Unnecessary).unwrap_err();
        assert_eq!(
            error,
            "the PostgreSQL value 1.5 of column \"amounts\" does not fit decimal(38,0): more than 0 fraction digits"
        );
        // A list the decoder did not read as texts is an error, not a guess.
        let odd = Schema::new(vec![Field::new("amounts", list(DataType::Int32), true)]);
        let mut ints = ListBuilder::new(datafusion::arrow::array::Int32Builder::new());
        ints.values().append_value(1);
        ints.append(true);
        let batch = RecordBatch::try_new(Arc::new(odd), vec![Arc::new(ints.finish()) as ArrayRef]).unwrap();
        let wanted = Schema::new(vec![Field::new(
            "amounts",
            list(DataType::Decimal128(10, 2)),
            true,
        )]);
        let error = restore(batch, Some(&wanted), DecimalRoundingMode::HalfUp).unwrap_err();
        assert!(error.contains("column \"amounts\""), "{error}");
    }

    #[test]
    fn names_without_precision() {
        assert_eq!(base_name("character varying(10)"), "character varying");
        assert_eq!(
            base_name("timestamp(3) with time zone"),
            "timestamp with time zone"
        );
        assert_eq!(base_name("text"), "text");
        assert_eq!(base_name("odd)("), "odd)(");
        assert_eq!(base_name("a)b(c"), "a)b(c");
        assert_eq!(numeric_precision("numeric(10, 2)"), Some((10, 2)));
        assert_eq!(numeric_precision("numeric(7)"), Some((7, 0)));
        assert_eq!(numeric_precision("numeric"), None);
        assert_eq!(numeric_precision("numeric(x)"), None);
    }
}
