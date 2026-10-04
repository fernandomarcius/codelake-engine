//! Plans that cross a process boundary (distributed execution, M4): a
//! `datafusion-proto` codec for what the core puts in physical plans that
//! `DataFusion` cannot serialize by itself.
//!
//! - **Functions** the core's rules insert that are not in the session's
//!   registry, or whose instance carries parameters: `codelake_to_decimal(p,s)`,
//!   the timestamp and declared-type casts, the overflow-checked operators, and
//!   the checked `sum`/`avg`, which keep the names `sum` and `avg` (resolving
//!   them by name would silently give back the unchecked ones). Every other
//!   function is written as nothing, and the receiving side resolves it by name
//!   in its own registry (built with the query's settings, so zone-sensitive
//!   functions see the session zone).
//! - **Execution plans**: the core's thirteen nodes (output accounting, join and
//!   spill guards, aggregation order, batch sizing, view retyping, the federation barrier).
//!
//! The format is private to one `CodeLake` version: a coordinator and its
//! workers run the same build.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, Operator, ScalarUDF};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf;
use prost::Message;

use crate::casts::ToDecimal;
use crate::constant_keys::ForgetOrderExec;
use crate::declared::TrinoTyped;
use crate::federation_guard::FederationFilterBarrier;
use crate::join_guard::{
    JoinInputRuntime, NestedLoopJoinWithoutFallback, NullAwareProbeBarrier, NullKeysKept,
};
use crate::join_spill::{JoinSideBudget, SortMergeWithinCeiling};
use crate::output::{AccountedMergeExec, ReservedOutputExec};
use crate::overflow::portable as overflow;
use crate::plain_views::RetypeExec;
use crate::timestamp_casts::TimestampCast;
use crate::timestamps::TextToTimestamp;

/// First bytes of everything this codec writes.
const MAGIC: &[u8; 4] = b"CLK1";

fn err(what: &str) -> DataFusionError {
    DataFusionError::Internal(format!("CodeLake plan codec: {what}"))
}

/// Bytes being written: [`MAGIC`], a tag, then the fields.
struct Out(Vec<u8>);

impl Out {
    fn new(tag: u8) -> Self {
        let mut bytes = MAGIC.to_vec();
        bytes.push(tag);
        Self(bytes)
    }

    fn u8(mut self, v: u8) -> Self {
        self.0.push(v);
        self
    }

    fn i8(self, v: i8) -> Self {
        self.u8(v.to_le_bytes()[0])
    }

    fn bool(self, v: bool) -> Self {
        self.u8(u8::from(v))
    }

    fn u64(mut self, v: u64) -> Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }

    fn bytes(self, v: &[u8]) -> Self {
        let mut out = self.u64(v.len() as u64);
        out.0.extend_from_slice(v);
        out
    }

    fn str(self, v: &str) -> Self {
        self.bytes(v.as_bytes())
    }

    fn data_type(self, v: &DataType) -> Result<Self> {
        let proto = protobuf::ArrowType::try_from(v).map_err(|e| err(&e.to_string()))?;
        Ok(self.bytes(&proto.encode_to_vec()))
    }

    fn schema(self, v: &Schema) -> Result<Self> {
        let proto = protobuf::Schema::try_from(v).map_err(|e| err(&e.to_string()))?;
        Ok(self.bytes(&proto.encode_to_vec()))
    }

    fn write(self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.0);
    }
}

/// Bytes being read, after [`MAGIC`] and the tag.
struct In<'a>(&'a [u8]);

impl<'a> In<'a> {
    /// The tag of `buf`, or `None` when this codec did not write it.
    fn open(buf: &'a [u8]) -> Option<(u8, Self)> {
        let rest = buf.strip_prefix(MAGIC.as_slice())?;
        let (&tag, rest) = rest.split_first()?;
        Some((tag, Self(rest)))
    }

    fn u8(&mut self) -> Result<u8> {
        let (&v, rest) = self.0.split_first().ok_or_else(|| err("truncated"))?;
        self.0 = rest;
        Ok(v)
    }

    fn i8(&mut self) -> Result<i8> {
        Ok(i8::from_le_bytes([self.u8()?]))
    }

    fn bool(&mut self) -> Result<bool> {
        Ok(self.u8()? != 0)
    }

    fn u64(&mut self) -> Result<u64> {
        let (head, rest) = self.0.split_at_checked(8).ok_or_else(|| err("truncated"))?;
        self.0 = rest;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(head);
        Ok(u64::from_le_bytes(bytes))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = usize::try_from(self.u64()?).map_err(|_| err("length"))?;
        let (head, rest) = self.0.split_at_checked(len).ok_or_else(|| err("truncated"))?;
        self.0 = rest;
        Ok(head)
    }

    fn str(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| err("text"))
    }

    fn data_type(&mut self) -> Result<DataType> {
        let proto = protobuf::ArrowType::decode(self.bytes()?).map_err(|e| err(&e.to_string()))?;
        DataType::try_from(&proto).map_err(|e| err(&e.to_string()))
    }

    fn schema(&mut self) -> Result<Schema> {
        let proto = protobuf::Schema::decode(self.bytes()?).map_err(|e| err(&e.to_string()))?;
        Schema::try_from(&proto).map_err(|e| err(&e.to_string()))
    }
}

const OPERATORS: [Operator; 5] = [
    Operator::Plus,
    Operator::Minus,
    Operator::Multiply,
    Operator::Divide,
    Operator::Modulo,
];

fn operator_code(op: Operator) -> Result<u8> {
    OPERATORS
        .iter()
        .position(|o| *o == op)
        .and_then(|p| u8::try_from(p).ok())
        .ok_or_else(|| err("operator"))
}

fn operator(code: u8) -> Result<Operator> {
    OPERATORS
        .get(usize::from(code))
        .copied()
        .ok_or_else(|| err("operator"))
}

// Function tags.
const TO_DECIMAL: u8 = 1;
const TEXT_TO_TIMESTAMP: u8 = 2;
const TRINO_TYPED: u8 = 3;
const TIMESTAMP_CAST: u8 = 4;
const DECIMAL_BINARY: u8 = 5;
const CHECKED_BINARY: u8 = 6;
const CHECKED_NEGATE: u8 = 7;
const CHECKED_ABS: u8 = 8;
// Aggregate tags.
const CHECKED_SUM: u8 = 1;
const TRINO_AVG: u8 = 2;
const REAL_RESULT: u8 = 3;
// Plan tags.
const ACCOUNTED_MERGE: u8 = 1;
const RESERVED_OUTPUT: u8 = 2;
const FEDERATION_BARRIER: u8 = 3;
const SLICE: u8 = 4;
const SIZED_BATCHES: u8 = 5;
const JOIN_SIDE_BUDGET: u8 = 6;
const SORT_MERGE_WITHIN_CEILING: u8 = 7;
const RETYPE: u8 = 8;
const NULL_AWARE_PROBE_BARRIER: u8 = 9;
const NESTED_LOOP_WITHOUT_FALLBACK: u8 = 10;
const JOIN_INPUT_RUNTIME: u8 = 11;
const NULL_KEYS_KEPT: u8 = 12;
const FORGET_ORDER: u8 = 13;

fn encode_udf(udf: &ScalarUDF) -> Result<Option<Out>> {
    let inner = udf.inner();
    if let Some(f) = inner.downcast_ref::<ToDecimal>() {
        let (p, s, safe) = f.portable();
        return Ok(Some(Out::new(TO_DECIMAL).u8(p).i8(s).bool(safe)));
    }
    if let Some(f) = inner.downcast_ref::<TextToTimestamp>() {
        let (target, safe) = f.portable();
        return Ok(Some(Out::new(TEXT_TO_TIMESTAMP).data_type(target)?.bool(safe)));
    }
    if let Some(f) = inner.downcast_ref::<TrinoTyped>() {
        let (mode, lenient) = f.portable();
        return Ok(Some(Out::new(TRINO_TYPED).u8(mode).bool(lenient)));
    }
    if let Some(f) = inner.downcast_ref::<TimestampCast>() {
        let (kind, target, safe) = f.portable();
        return Ok(Some(
            Out::new(TIMESTAMP_CAST).u8(kind).data_type(target)?.bool(safe),
        ));
    }
    if let Some(f) = inner.downcast_ref::<overflow::DecimalBinary>() {
        return Ok(Some(Out::new(DECIMAL_BINARY).u8(operator_code(f.op())?)));
    }
    if let Some(f) = inner.downcast_ref::<overflow::CheckedBinary>() {
        return Ok(Some(Out::new(CHECKED_BINARY).u8(operator_code(f.op())?)));
    }
    if inner.downcast_ref::<overflow::CheckedNegate>().is_some() {
        return Ok(Some(Out::new(CHECKED_NEGATE)));
    }
    if inner.downcast_ref::<overflow::CheckedAbs>().is_some() {
        return Ok(Some(Out::new(CHECKED_ABS)));
    }
    Ok(None)
}

fn decode_udf(buf: &[u8]) -> Result<Option<Arc<ScalarUDF>>> {
    let Some((tag, mut i)) = In::open(buf) else {
        return Ok(None);
    };
    let udf = match tag {
        TO_DECIMAL => Arc::new(ToDecimal::from_portable(i.u8()?, i.i8()?, i.bool()?)),
        TEXT_TO_TIMESTAMP => {
            let target = i.data_type()?;
            Arc::new(ScalarUDF::new_from_impl(TextToTimestamp::new(target, i.bool()?)))
        }
        TRINO_TYPED => Arc::new(TrinoTyped::from_portable(i.u8()?, i.bool()?).ok_or_else(|| err("mode"))?),
        TIMESTAMP_CAST => {
            let kind = i.u8()?;
            let target = i.data_type()?;
            Arc::new(TimestampCast::from_portable(kind, target, i.bool()?).ok_or_else(|| err("kind"))?)
        }
        DECIMAL_BINARY => overflow::decimal_op(operator(i.u8()?)?).ok_or_else(|| err("operator"))?,
        CHECKED_BINARY => overflow::checked_op(operator(i.u8()?)?).ok_or_else(|| err("operator"))?,
        CHECKED_NEGATE => overflow::checked_negate(),
        CHECKED_ABS => overflow::checked_abs(),
        _ => return internal_err!("CodeLake plan codec: unknown function tag {tag}"),
    };
    Ok(Some(udf))
}

fn builtin_aggregate(name: &str) -> Result<Arc<AggregateUDF>> {
    match name {
        "sum" => Ok(datafusion::functions_aggregate::sum::sum_udaf()),
        "avg" => Ok(datafusion::functions_aggregate::average::avg_udaf()),
        other => internal_err!("CodeLake plan codec: no built-in aggregate {other}"),
    }
}

fn encode_udaf(udaf: &AggregateUDF) -> Option<Out> {
    let inner = udaf.inner();
    if let Some(f) = inner.downcast_ref::<overflow::CheckedSum>() {
        return Some(Out::new(CHECKED_SUM).bool(f.is_trino()));
    }
    if inner.downcast_ref::<overflow::TrinoAvg>().is_some() {
        return Some(Out::new(TRINO_AVG));
    }
    if let Some(f) = inner.downcast_ref::<overflow::RealResult>() {
        return Some(Out::new(REAL_RESULT).str(f.inner().name()));
    }
    None
}

fn decode_udaf(buf: &[u8]) -> Result<Option<Arc<AggregateUDF>>> {
    let Some((tag, mut i)) = In::open(buf) else {
        return Ok(None);
    };
    let udaf = match tag {
        CHECKED_SUM => overflow::checked_sum(i.bool()?),
        TRINO_AVG => overflow::trino_avg(),
        REAL_RESULT => overflow::real_result(builtin_aggregate(i.str()?)?),
        _ => return internal_err!("CodeLake plan codec: unknown aggregate tag {tag}"),
    };
    Ok(Some(udaf))
}

fn single_input(inputs: &[Arc<dyn ExecutionPlan>]) -> Result<Arc<dyn ExecutionPlan>> {
    if let [only] = inputs {
        return Ok(Arc::clone(only));
    }
    internal_err!("CodeLake plan codec: expected one input, got {}", inputs.len())
}

/// The codec; see the module documentation.
#[derive(Debug, Default, Clone, Copy)]
pub struct CoreCodec;

impl CoreCodec {
    fn encode_plan(self, node: &Arc<dyn ExecutionPlan>) -> Result<Option<Out>> {
        let tag = |t| Ok(Some(Out::new(t)));
        if node.is::<AccountedMergeExec>() {
            return tag(ACCOUNTED_MERGE);
        }
        if node.is::<ReservedOutputExec>() {
            return tag(RESERVED_OUTPUT);
        }
        if node.is::<FederationFilterBarrier>() {
            return tag(FEDERATION_BARRIER);
        }
        if node.is::<crate::computed_batches::SliceExec>() {
            return tag(SLICE);
        }
        if node.is::<crate::computed_batches::SizedBatchesExec>() {
            return tag(SIZED_BATCHES);
        }
        if let Some(n) = node.downcast_ref::<JoinSideBudget>() {
            return Ok(Some(Out::new(JOIN_SIDE_BUDGET).u64(n.budget() as u64)));
        }
        if node.is::<ForgetOrderExec>() {
            return tag(FORGET_ORDER);
        }
        if let Some(n) = node.downcast_ref::<SortMergeWithinCeiling>() {
            return Ok(Some(Out::new(SORT_MERGE_WITHIN_CEILING).u64(n.budget() as u64)));
        }
        if node.is::<RetypeExec>() {
            return Ok(Some(Out::new(RETYPE).schema(node.schema().as_ref())?));
        }
        if node.is::<NullAwareProbeBarrier>() {
            return tag(NULL_AWARE_PROBE_BARRIER);
        }
        if node.is::<NestedLoopJoinWithoutFallback>() {
            return tag(NESTED_LOOP_WITHOUT_FALLBACK);
        }
        if node.is::<JoinInputRuntime>() {
            return tag(JOIN_INPUT_RUNTIME);
        }
        if let Some(n) = node.downcast_ref::<NullKeysKept>() {
            let mut out = Out::new(NULL_KEYS_KEPT).u64(n.keys().len() as u64);
            for key in n.keys() {
                out = out.bytes(&serialize_physical_expr(key, &self)?.encode_to_vec());
            }
            return Ok(Some(out));
        }
        Ok(None)
    }

    fn decode_plan(
        self,
        tag: u8,
        mut i: In<'_>,
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = single_input(inputs)?;
        Ok(match tag {
            ACCOUNTED_MERGE => Arc::new(AccountedMergeExec::new(input)),
            RESERVED_OUTPUT => Arc::new(ReservedOutputExec::new(input)),
            FEDERATION_BARRIER => Arc::new(FederationFilterBarrier::new(input)),
            SLICE => crate::computed_batches::portable::slice(input),
            SIZED_BATCHES => crate::computed_batches::portable::sized(input),
            JOIN_SIDE_BUDGET => {
                let budget = usize::try_from(i.u64()?).map_err(|_| err("budget"))?;
                Arc::new(JoinSideBudget::new(input, budget))
            }
            SORT_MERGE_WITHIN_CEILING => {
                let budget = usize::try_from(i.u64()?).map_err(|_| err("budget"))?;
                Arc::new(SortMergeWithinCeiling::new(input, budget))
            }
            FORGET_ORDER => Arc::new(ForgetOrderExec::new(input)),
            RETYPE => RetypeExec::portable(input, Arc::new(i.schema()?))?,
            NULL_AWARE_PROBE_BARRIER => Arc::new(NullAwareProbeBarrier::new(input)),
            NESTED_LOOP_WITHOUT_FALLBACK => Arc::new(NestedLoopJoinWithoutFallback::new(input)),
            JOIN_INPUT_RUNTIME => Arc::new(JoinInputRuntime::new(input)),
            NULL_KEYS_KEPT => {
                let count = i.u64()?;
                let schema = input.schema();
                let mut keys: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
                for _ in 0..count {
                    let proto =
                        protobuf::PhysicalExprNode::decode(i.bytes()?).map_err(|e| err(&e.to_string()))?;
                    keys.push(parse_physical_expr(&proto, ctx, &schema, &self)?);
                }
                Arc::new(NullKeysKept::new(input, keys))
            }
            _ => return internal_err!("CodeLake plan codec: unknown plan tag {tag}"),
        })
    }
}

impl PhysicalExtensionCodec for CoreCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some((tag, i)) = In::open(buf) else {
            return internal_err!("CodeLake plan codec: not a CodeLake plan");
        };
        self.decode_plan(tag, i, inputs, ctx)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(out) = self.encode_plan(&node)? else {
            return internal_err!("CodeLake plan codec: {} is not a CodeLake plan", node.name());
        };
        out.write(buf);
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        decode_udf(buf)?.map_or_else(
            || internal_err!("CodeLake plan codec: function {name} is not in the registry"),
            Ok,
        )
    }

    /// Nothing for a function the core does not own: the receiver resolves
    /// it by name.
    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(out) = encode_udf(node)? {
            out.write(buf);
        }
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        decode_udaf(buf)?.map_or_else(
            || internal_err!("CodeLake plan codec: aggregate {name} is not in the registry"),
            Ok,
        )
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(out) = encode_udaf(node) {
            out.write(buf);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_udf(udf: &ScalarUDF) -> Arc<ScalarUDF> {
        let mut buf = Vec::new();
        CoreCodec.try_encode_udf(udf, &mut buf).unwrap();
        assert!(!buf.is_empty(), "{} was not encoded", udf.name());
        CoreCodec.try_decode_udf(udf.name(), &buf).unwrap()
    }

    #[test]
    fn parametrized_functions_come_back_with_their_parameters() {
        let to_decimal = ToDecimal::from_portable(15, 2, true);
        let back = round_trip_udf(&to_decimal);
        assert_eq!(
            back.inner().downcast_ref::<ToDecimal>().unwrap().portable(),
            (15, 2, true)
        );

        let typed = TrinoTyped::from_portable(2, true).unwrap();
        let back = round_trip_udf(&typed);
        assert_eq!(
            back.inner().downcast_ref::<TrinoTyped>().unwrap().portable(),
            (2, true)
        );

        let target = DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None);
        let cast = TimestampCast::from_portable(2, target.clone(), false).unwrap();
        let back = round_trip_udf(&cast);
        let (kind, back_target, safe) = back.inner().downcast_ref::<TimestampCast>().unwrap().portable();
        assert_eq!((kind, back_target, safe), (2, &target, false));

        for op in OPERATORS {
            let checked = overflow::checked_op(op).unwrap();
            assert!(Arc::ptr_eq(&round_trip_udf(&checked), &checked));
            let decimal = overflow::decimal_op(op).unwrap();
            assert!(Arc::ptr_eq(&round_trip_udf(&decimal), &decimal));
        }
    }

    #[test]
    fn checked_aggregates_do_not_come_back_as_the_builtins_of_their_name() {
        for trino in [false, true] {
            let sum = overflow::checked_sum(trino);
            let mut buf = Vec::new();
            CoreCodec.try_encode_udaf(&sum, &mut buf).unwrap();
            let back = CoreCodec.try_decode_udaf("sum", &buf).unwrap();
            assert!(Arc::ptr_eq(&back, &sum));
        }
        let real = overflow::real_result(builtin_aggregate("avg").unwrap());
        let mut buf = Vec::new();
        CoreCodec.try_encode_udaf(&real, &mut buf).unwrap();
        let back = CoreCodec.try_decode_udaf("avg", &buf).unwrap();
        assert!(back.inner().downcast_ref::<overflow::RealResult>().is_some());
    }

    #[test]
    fn functions_the_core_does_not_own_are_left_to_the_registry() {
        let abs = datafusion::functions::math::abs();
        let mut buf = Vec::new();
        CoreCodec.try_encode_udf(&abs, &mut buf).unwrap();
        assert!(buf.is_empty());
        let sum = datafusion::functions_aggregate::sum::sum_udaf();
        CoreCodec.try_encode_udaf(&sum, &mut buf).unwrap();
        assert!(buf.is_empty());
    }
}
