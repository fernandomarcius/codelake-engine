//! One function per name, in every session state derived from the engine's.
//!
//! A registry registers a function under its name and each of its aliases,
//! and the engine registers its functions over `DataFusion`'s built-ins. The
//! engine's session state resolves every name to the last function registered
//! under it, but a replaced built-in stays in the function maps under the
//! names the engine did not take:
//!
//! - it keeps its own name and loses an alias (`to_char`, alias
//!   `date_format`; `character_length`, alias `length`);
//! - it loses its own name and keeps an alias (`datepart` of `date_part`,
//!   `mean` of `avg`).
//!
//! `SessionStateBuilder::new_from_existing`, which every derived state goes
//! through (a view's plan, a distributed task, `SET` of a runtime setting),
//! registers every entry of the maps again, in hash order: the built-in takes
//! the name back in about half the processes. A view reading
//! `date_format(ts, '%Y-%m-%d %H:%i:%s')` then ran `to_char`, for which `%i`
//! is not a specifier ("Cast error: Format error"), while the same expression
//! outside the view answered.
//!
//! [`settle`] leaves every name claimed by one function only, so that
//! registering the entries again, in any order, gives the same maps:
//!
//! - a function that lost its own name is removed, with the names it kept
//!   (`datepart`, `today`, `mean`, `list_sort`…: none is a name of the SQL the
//!   engine speaks);
//! - a function that kept its own name drops the aliases other functions
//!   took ([`Unaliased`]). Only scalar functions need it: for another kind it
//!   is an error, raised when the engine is built.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{ExprSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::udf_eq::UdfEq;
use datafusion::logical_expr::{
    AggregateUDF, ColumnarValue, Documentation, Expr, ExpressionPlacement, HigherOrderUDF, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping, WindowUDF,
};

/// Settles one kind of function: the plan for the map `$map` of `$state`,
/// applied with the registry's `$deregister` and `$register`.
macro_rules! settle_kind {
    ($state:ident, $map:ident, $narrow:expr, $deregister:ident, $register:ident) => {
        if let Some(plan) = plan($state.$map(), $narrow)? {
            for name in &plan.removed {
                $state.$deregister(name)?;
            }
            for f in plan.kept {
                $state.$register(f)?;
            }
        }
    };
}

/// Leaves every name of `state`'s function maps (scalar, aggregate, window,
/// higher-order) claimed by the one function it resolves to (see the module
/// docs). A settled state is left as it is.
///
/// # Errors
///
/// A function other than a scalar one kept its own name and lost an alias.
pub(crate) fn settle(state: &mut SessionState) -> Result<()> {
    settle_kind!(
        state,
        scalar_functions,
        &|f: &Arc<ScalarUDF>, aliases| Some(Arc::new(ScalarUDF::new_from_impl(Unaliased::new(f, aliases)))),
        deregister_udf,
        register_udf
    );
    settle_kind!(
        state,
        aggregate_functions,
        &|_: &Arc<AggregateUDF>, _| None,
        deregister_udaf,
        register_udaf
    );
    settle_kind!(
        state,
        window_functions,
        &|_: &Arc<WindowUDF>, _| None,
        deregister_udwf,
        register_udwf
    );
    settle_kind!(
        state,
        higher_order_functions,
        &|_: &Arc<HigherOrderUDF>, _| None,
        deregister_higher_order_function,
        register_higher_order_function
    );
    Ok(())
}

/// A registered function's name and aliases, for every kind.
trait Named {
    fn name(&self) -> &str;
    fn aliases(&self) -> &[String];
}

macro_rules! named {
    ($($kind:ty),*) => {$(
        impl Named for $kind {
            fn name(&self) -> &str {
                <$kind>::name(self)
            }
            fn aliases(&self) -> &[String] {
                <$kind>::aliases(self)
            }
        }
    )*};
}

named!(ScalarUDF, AggregateUDF, WindowUDF, HigherOrderUDF);

/// How to settle one function map.
struct Plan<F> {
    /// Names no kept function claims, to deregister.
    removed: Vec<String>,
    /// Every kept function, to register again: deregistering a name also
    /// deregisters the aliases of its function, which a kept function may
    /// own.
    kept: Vec<Arc<F>>,
}

/// A function with only the aliases given, `None` when its kind cannot drop
/// an alias.
type Narrow<F> = dyn Fn(&Arc<F>, Vec<String>) -> Option<Arc<F>>;

/// The plan that settles `map`, `None` when it is settled.
fn plan<F: Named>(map: &HashMap<String, Arc<F>>, narrow: &Narrow<F>) -> Result<Option<Plan<F>>> {
    let owns = |name: &str, f: &Arc<F>| map.get(name).is_some_and(|owner| Arc::ptr_eq(owner, f));
    let mut seen = HashSet::new();
    let mut kept = Vec::new();
    let mut narrowed = false;
    for f in map.values() {
        if !seen.insert(Arc::as_ptr(f)) {
            continue;
        }
        let (name, aliases) = (f.name(), f.aliases());
        if !owns(name, f) {
            continue;
        }
        let (own, lost): (Vec<&String>, Vec<&String>) = aliases.iter().partition(|alias| owns(alias, f));
        if lost.is_empty() {
            kept.push(Arc::clone(f));
            continue;
        }
        let Some(f) = narrow(f, own.into_iter().cloned().collect()) else {
            return Err(DataFusionError::Internal(format!(
                "function {name} cannot give up the names {lost:?}, which other functions took"
            )));
        };
        kept.push(f);
        narrowed = true;
    }
    let claimed: HashSet<&str> = kept
        .iter()
        .flat_map(|f| std::iter::once(f.name()).chain(f.aliases().iter().map(String::as_str)))
        .collect();
    let removed: Vec<String> = map
        .keys()
        .filter(|name| !claimed.contains(name.as_str()))
        .cloned()
        .collect();
    if removed.is_empty() && !narrowed {
        return Ok(None);
    }
    Ok(Some(Plan { removed, kept }))
}

/// A scalar function with only some of its aliases: everything else is the
/// function's.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Unaliased {
    inner: UdfEq<Arc<dyn ScalarUDFImpl>>,
    aliases: Vec<String>,
}

impl Unaliased {
    /// `f` with only `aliases` (a subset of its own).
    pub(crate) fn new(f: &ScalarUDF, aliases: Vec<String>) -> Self {
        let inner = f
            .inner()
            .downcast_ref::<Self>()
            .map_or_else(|| Arc::clone(f.inner()), |narrowed| Arc::clone(&narrowed.inner));
        Self {
            inner: inner.into(),
            aliases,
        }
    }
}

#[warn(
    clippy::missing_trait_methods,
    reason = "delegates, so it implements every method"
)]
impl ScalarUDFImpl for Unaliased {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        #[expect(deprecated, reason = "delegates every method")]
        self.inner.display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.inner.schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    /// The function updated for `config`, with the same aliases.
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        let updated = self.inner.with_updated_config(config)?;
        Some(ScalarUDF::new_from_impl(Self::new(
            &updated,
            self.aliases.clone(),
        )))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        #[expect(deprecated, reason = "delegates every method")]
        self.inner.is_nullable(args, schema)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.inner.simplify(args, info)
    }

    fn preimage(&self, args: &[Expr], lit_expr: &Expr, info: &SimplifyContext) -> Result<PreimageResult> {
        self.inner.preimage(args, lit_expr, info)
    }

    fn short_circuits(&self) -> bool {
        self.inner.short_circuits()
    }

    fn conditional_arguments<'a>(&self, args: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.inner.conditional_arguments(args)
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        self.inner.evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, inputs)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.inner.preserves_lex_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn struct_field_mapping(&self, literal_args: &[Option<ScalarValue>]) -> Option<StructFieldMapping> {
        self.inner.struct_field_mapping(literal_args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.inner.placement(args)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::LazyLock;

    use datafusion::arrow::datatypes::Field;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::{DocSection, Volatility, lit};

    use super::*;

    static DOC: LazyLock<Documentation> =
        LazyLock::new(|| Documentation::builder(DocSection::default(), "probe", "probe()").build());

    /// A function whose every method answers something no default does.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct Probe {
        name: &'static str,
        aliases: Vec<String>,
        signature: Signature,
        updated: bool,
    }

    fn probe(name: &'static str, aliases: &[&str]) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::new_from_impl(Probe {
            name,
            aliases: aliases.iter().map(|a| (*a).to_owned()).collect(),
            signature: Signature::exact(vec![DataType::Int64], Volatility::Volatile),
            updated: false,
        }))
    }

    impl ScalarUDFImpl for Probe {
        fn name(&self) -> &str {
            self.name
        }
        fn aliases(&self) -> &[String] {
            &self.aliases
        }
        fn display_name(&self, args: &[Expr]) -> Result<String> {
            Ok(format!("shown {}", args.len()))
        }
        fn schema_name(&self, _: &[Expr]) -> Result<String> {
            Ok("schema".into())
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }
        fn with_updated_config(&self, _: &ConfigOptions) -> Option<ScalarUDF> {
            let updated = Self {
                updated: true,
                ..self.clone()
            };
            (!self.updated).then(|| ScalarUDF::new_from_impl(updated))
        }
        fn return_field_from_args(&self, _: ReturnFieldArgs<'_>) -> Result<FieldRef> {
            Ok(Arc::new(Field::new("field", DataType::Int8, false)))
        }
        fn is_nullable(&self, _: &[Expr], _: &dyn ExprSchema) -> bool {
            false
        }
        fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(7))))
        }
        fn simplify(&self, _: Vec<Expr>, _: &SimplifyContext) -> Result<ExprSimplifyResult> {
            Ok(ExprSimplifyResult::Simplified(lit(1)))
        }
        fn preimage(&self, _: &[Expr], _: &Expr, _: &SimplifyContext) -> Result<PreimageResult> {
            Ok(PreimageResult::Range {
                expr: lit(2),
                interval: Box::new(Interval::make(Some(1_i64), Some(2_i64))?),
            })
        }
        fn short_circuits(&self) -> bool {
            true
        }
        fn conditional_arguments<'a>(&self, args: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
            Some((args.iter().collect(), vec![]))
        }
        fn evaluate_bounds(&self, _: &[&Interval]) -> Result<Interval> {
            Interval::make(Some(3_i64), Some(4_i64))
        }
        fn propagate_constraints(&self, _: &Interval, _: &[&Interval]) -> Result<Option<Vec<Interval>>> {
            Ok(None)
        }
        fn output_ordering(&self, _: &[ExprProperties]) -> Result<SortProperties> {
            Ok(SortProperties::Singleton)
        }
        fn preserves_lex_ordering(&self, _: &[ExprProperties]) -> Result<bool> {
            Ok(true)
        }
        fn coerce_types(&self, _: &[DataType]) -> Result<Vec<DataType>> {
            Ok(vec![DataType::Int64])
        }
        fn struct_field_mapping(&self, _: &[Option<ScalarValue>]) -> Option<StructFieldMapping> {
            Some(StructFieldMapping {
                field_accessor: probe("accessor", &[]),
                fields: vec![(vec![], 5)],
            })
        }
        fn documentation(&self) -> Option<&Documentation> {
            Some(&DOC)
        }
        fn placement(&self, _: &[ExpressionPlacement]) -> ExpressionPlacement {
            ExpressionPlacement::MoveTowardsLeafNodes
        }
    }

    #[test]
    #[expect(deprecated, reason = "the wrapper delegates the deprecated methods too")]
    fn unaliased_answers_as_its_function_with_fewer_aliases() {
        let f = probe("p", &["a", "b"]);
        let u = Unaliased::new(&f, vec!["a".into()]);
        let args = [lit(1)];
        let interval = Interval::make(Some(0_i64), Some(9_i64)).unwrap();
        let field: FieldRef = Arc::new(Field::new("x", DataType::Int64, true));
        let properties = [ExprProperties::new_unknown()];
        assert_eq!(u.name(), "p");
        assert_eq!(u.aliases(), ["a"]);
        assert_eq!(u.display_name(&args).unwrap(), "shown 1");
        assert_eq!(u.schema_name(&args).unwrap(), "schema");
        assert_eq!(u.signature(), f.signature());
        assert_eq!(u.return_type(&[]).unwrap(), DataType::Utf8);
        let returned = u
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[],
                scalar_arguments: &[],
            })
            .unwrap();
        assert_eq!(returned.name(), "field");
        assert!(!u.is_nullable(&args, &datafusion::common::DFSchema::empty()));
        let invoked = u
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::clone(&field),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();
        assert!(matches!(
            invoked,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(7)))
        ));
        let context = SimplifyContext::default();
        assert!(matches!(
            u.simplify(vec![], &context).unwrap(),
            ExprSimplifyResult::Simplified(e) if e == lit(1)
        ));
        assert!(matches!(
            u.preimage(&args, &lit(0), &context).unwrap(),
            PreimageResult::Range { expr, .. } if expr == lit(2)
        ));
        assert!(u.short_circuits());
        assert_eq!(u.conditional_arguments(&args), Some((vec![&args[0]], vec![])));
        assert_eq!(
            u.evaluate_bounds(&[&interval]).unwrap(),
            Interval::make(Some(3_i64), Some(4_i64)).unwrap()
        );
        assert_eq!(u.propagate_constraints(&interval, &[&interval]).unwrap(), None);
        assert_eq!(u.output_ordering(&properties).unwrap(), SortProperties::Singleton);
        assert!(u.preserves_lex_ordering(&properties).unwrap());
        assert_eq!(u.coerce_types(&[]).unwrap(), vec![DataType::Int64]);
        let mapping = u.struct_field_mapping(&[]).unwrap();
        assert_eq!(
            (mapping.field_accessor.name(), mapping.fields),
            ("accessor", vec![(vec![], 5)])
        );
        assert_eq!(u.documentation().map(|d| d.description.as_str()), Some("probe"));
        assert_eq!(u.placement(&[]), ExpressionPlacement::MoveTowardsLeafNodes);
    }

    /// An update keeps the aliases; narrowing twice does not wrap twice.
    #[test]
    fn unaliased_keeps_its_aliases_through_updates() {
        let f = probe("p", &["a", "b"]);
        let u = Unaliased::new(&f, vec!["a".into()]);
        let updated = u.with_updated_config(&ConfigOptions::default()).unwrap();
        assert_eq!(updated.aliases(), ["a"]);
        let inner = &updated.inner().downcast_ref::<Unaliased>().unwrap().inner;
        assert!(inner.downcast_ref::<Probe>().unwrap().updated);
        // The probe updates once.
        assert!(
            updated
                .inner()
                .with_updated_config(&ConfigOptions::default())
                .is_none()
        );

        let again = Unaliased::new(&ScalarUDF::new_from_impl(u), vec![]);
        assert!(again.inner.downcast_ref::<Probe>().is_some());
        assert_eq!(again.aliases(), [] as [String; 0]);
    }

    /// Each name and the name and aliases of the function it resolves to.
    fn resolved(state: &SessionState) -> BTreeMap<String, (String, Vec<String>)> {
        state
            .scalar_functions()
            .iter()
            .map(|(k, f)| (k.clone(), (f.name().to_owned(), f.aliases().to_vec())))
            .collect()
    }

    fn state_with(functions: &[Arc<ScalarUDF>]) -> SessionState {
        let mut state = SessionStateBuilder::new().build();
        for f in functions {
            state.register_udf(Arc::clone(f)).unwrap();
        }
        state
    }

    fn entry(name: &str, aliases: &[&str]) -> (String, Vec<String>) {
        (name.to_owned(), aliases.iter().map(|a| (*a).to_owned()).collect())
    }

    /// Every order of registering `state`'s functions again, rotations and
    /// their reverse, gives the maps `state` has.
    fn assert_any_order_keeps(state: &SessionState) {
        let mut functions: Vec<Arc<ScalarUDF>> = state.scalar_functions().values().cloned().collect();
        functions.sort_by(|a, b| a.name().cmp(b.name()).then(a.aliases().cmp(b.aliases())));
        for start in 0..functions.len() {
            functions.rotate_left(1);
            for reverse in [false, true] {
                let mut order = functions.clone();
                if reverse {
                    order.reverse();
                }
                let mut again = state.clone();
                for f in order {
                    again.register_udf(f).unwrap();
                }
                assert_eq!(
                    resolved(&again),
                    resolved(state),
                    "rotation {start}, reversed {reverse}"
                );
            }
        }
        let rebuilt = SessionStateBuilder::new_from_existing(state.clone()).build();
        assert_eq!(resolved(&rebuilt), resolved(state));
    }

    /// A function that kept its name drops the aliases other functions took;
    /// one that lost its name goes, with the aliases it kept.
    #[test]
    fn settling_leaves_one_function_per_name() {
        let state = state_with(&[
            probe("builtin", &["taken", "kept"]),
            probe("lost", &["orphan"]),
            probe("taken", &[]),
            probe("lost", &[]),
        ]);
        let before = resolved(&state);
        assert_eq!(before["taken"], entry("taken", &[]));
        assert_eq!(before["orphan"], entry("lost", &["orphan"]));
        let mut settled = state;
        settle(&mut settled).unwrap();
        let expected: BTreeMap<String, (String, Vec<String>)> = [
            ("builtin", entry("builtin", &["kept"])),
            ("kept", entry("builtin", &["kept"])),
            ("taken", entry("taken", &[])),
            ("lost", entry("lost", &[])),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();
        assert_eq!(resolved(&settled), expected);
        assert_any_order_keeps(&settled);
        // Settled: nothing more to do.
        let narrow = |_: &Arc<ScalarUDF>, _| None;
        assert!(plan(settled.scalar_functions(), &narrow).unwrap().is_none());
    }

    /// A lost alias alone (no name to remove) is enough to settle.
    #[test]
    fn a_lost_alias_alone_is_settled() {
        let mut state = state_with(&[probe("builtin", &["taken"]), probe("taken", &[])]);
        settle(&mut state).unwrap();
        assert_eq!(resolved(&state)["builtin"], entry("builtin", &[]));
        assert_eq!(resolved(&state)["taken"], entry("taken", &[]));
        assert_any_order_keeps(&state);
    }

    /// Only scalar functions can drop an alias: another kind is an error.
    #[test]
    fn an_aggregate_that_lost_an_alias_is_an_error() {
        let mut state = SessionStateBuilder::new().build();
        let avg = datafusion::functions_aggregate::average::avg_udaf();
        let count = datafusion::functions_aggregate::count::count_udaf();
        state.register_udaf(avg).unwrap();
        state
            .register_udaf(Arc::new(count.as_ref().clone().with_aliases(["mean"])))
            .unwrap();
        let err = settle(&mut state).unwrap_err().to_string();
        assert!(
            err.contains("function avg cannot give up the names [\"mean\"]"),
            "{err}"
        );
    }
}
