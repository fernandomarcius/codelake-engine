//! Trino 431 function library.
//!
//! `CodeLake` answers Trino SQL, so a function called by a Trino name behaves
//! like Trino's: same result type, same NULL handling, same errors (the
//! message starts with Trino's error name, e.g. `INVALID_FUNCTION_ARGUMENT:`).
//! Where `DataFusion` ships a function with the same name and different
//! semantics (`substr`, `greatest`, `concat`, `date_trunc`, …), the Trino
//! version replaces it. Every function is checked against a real Trino 431 by
//! the golden fixtures in `tests/fixtures/trino431-functions`
//! (see `scripts/golden/regenerate-functions.sh`).
//!
//! Pieces, all installed by [`install`]:
//! - scalar and aggregate UDFs, one module per Trino documentation category;
//! - a function rewrite that swaps built-ins the SQL planner creates directly
//!   (`EXTRACT`, `POSITION`, `SUBSTRING … FROM`) and turns `if(…)` into `CASE`;
//! - an analyzer rule giving `CAST` Trino semantics where Arrow differs
//!   (see `cast`);
//! - [`rewrite_statement`]: AST rewrites for Trino syntax (numeric literals,
//!   `DECIMAL '…'`, `AT TIME ZONE`, `listagg … WITHIN GROUP`, ranking
//!   functions as `bigint`);
//!
//! Rules the engine implements once for every query, and the library
//! therefore does not: decimal arithmetic (`crate::overflow`), `TIMESTAMP '…'`
//! literals and `timestamp` types (`crate::literals`), `varchar(n)` / `char(n)`
//! (`crate::declared`), and the `avg` of a decimal (`crate::overflow::avg`,
//! which the library's `avg` calls).
//! - [`physical_rules`]: `try(…)` evaluation that turns failing rows into NULL;
//! - [`trino_error_name`]: the Trino `errorName` of any engine error.
//!
//! Federation: these functions exist only in the engine. A SQL source must not
//! receive them (see [`is_trino_function`]); SQL Server's pushdown policy
//! already keeps every function local.

mod aggregate;
mod aggregate_more;
mod aggregate_types;
mod array;
mod array_agg;
mod binary;
mod cast;
mod cast_type;
mod collections;
mod conditional;
mod datetime;
mod datetime_format;
mod decimal;
#[cfg(test)]
mod engine_tests;
mod errors;
#[cfg(test)]
mod golden;
mod json;
mod json_cast;
mod keys;
mod lateral;
mod literals;
mod math;
mod nan;
mod preprocess;
mod regexp;
mod rewrite;
mod sketch;
mod statistics;
mod string;
mod support;
mod syntax;
mod try_expr;
mod url;
mod zeros;
mod zone;

pub(crate) use collections::SUBSCRIPT;
pub(crate) use try_expr::{BARRIER, GUARDED_TRY, TRY};

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, LogicalPlan, ScalarUDF};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::Statement;

pub use errors::trino_error_name;

/// Every scalar function of the library.
fn scalar_udfs() -> Vec<ScalarUDF> {
    let mut all = Vec::new();
    all.extend(datetime::udfs());
    all.extend(datetime_format::udfs());
    all.extend(string::udfs());
    all.extend(regexp::udfs());
    all.extend(binary::udfs());
    all.extend(math::udfs());
    all.extend(statistics::udfs());
    all.extend(conditional::udfs());
    all.extend(array::udfs());
    all.extend(collections::udfs());
    all.extend(json::udfs());
    all.extend(json_cast::udfs());
    all.extend(url::udfs());
    all.push(try_expr::try_udf());
    all.extend(try_expr::planning_udfs());
    all.extend(cast_type::udfs());
    all.extend(decimal::udfs());
    all.extend(nan::udfs());
    all
}

/// Every aggregate function of the library.
fn aggregate_udfs() -> Vec<AggregateUDF> {
    let mut all = aggregate::udafs();
    all.extend(aggregate_more::udafs());
    all.extend(array_agg::udafs());
    all.extend(aggregate_types::udafs());
    all
}

static NAMES: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let mut names = HashSet::new();
    for udf in scalar_udfs() {
        names.insert(udf.name().to_owned());
        names.extend(udf.aliases().iter().cloned());
    }
    for udaf in aggregate_udfs() {
        names.insert(udaf.name().to_owned());
        names.extend(udaf.aliases().iter().cloned());
    }
    names.insert(cast::CAST.to_owned());
    names.insert(cast::TRY_CAST.to_owned());
    names
});

/// True for names this library defines. A pushdown policy must keep calls to
/// them in the engine: a remote database would evaluate its own function of
/// the same name, with its own semantics.
pub fn is_trino_function(name: &str) -> bool {
    NAMES.contains(name)
}

/// Session defaults Trino semantics rely on: the session zone is UTC unless
/// configured (`timestamp with time zone` needs a zone), and `ORDER BY`
/// without `NULLS FIRST|LAST` puts NULLs last in both directions (queries,
/// windows and ordered aggregates; `DataFusion` puts them first for `DESC`).
pub fn session_config(config: SessionConfig) -> SessionConfig {
    let config = config.set_str("datafusion.sql_parser.default_null_ordering", "nulls_last");
    if config.options().execution.time_zone.is_some() {
        return config;
    }
    config.set_str("datafusion.execution.time_zone", "UTC")
}

/// Registers the library on a session.
pub fn install(ctx: &mut SessionContext) -> Result<()> {
    let config = ctx.state().config().options().clone();
    let mut overrides = Vec::new();
    for udf in scalar_udfs() {
        let udf = udf.inner().with_updated_config(&config).unwrap_or(udf);
        let udf = Arc::new(udf);
        overrides.push(Arc::clone(&udf));
        FunctionRegistry::register_udf(ctx, udf)?;
    }
    for udaf in aggregate_udfs() {
        FunctionRegistry::register_udaf(ctx, Arc::new(udaf))?;
    }
    ctx.register_function_rewrite(Arc::new(rewrite::TrinoRewrites::new(overrides)))?;
    ctx.add_analyzer_rule(Arc::new(cast::TrinoCasts));
    Ok(())
}

/// The rule that must run before `simplify_expressions` (see `zeros`): the
/// engine inserts it there.
pub fn float_zeros() -> Arc<dyn datafusion::optimizer::OptimizerRule + Send + Sync> {
    Arc::new(zeros::FloatZeros)
}

/// Name of the rule [`float_zeros`] returns.
pub const FLOAT_ZEROS: &str = zeros::FLOAT_ZEROS;

/// Logical optimizer rules of the library, to run after the engine's own
/// (float keys and window arguments, see `keys`).
pub fn optimizer_rules() -> Vec<Arc<dyn datafusion::optimizer::OptimizerRule + Send + Sync>> {
    keys::TrinoKeys::new()
        .map(|rule| Arc::new(rule) as Arc<dyn datafusion::optimizer::OptimizerRule + Send + Sync>)
        .into_iter()
        .collect()
}

/// Physical optimizer rules of the library, to run after the engine's own
/// (`try(…)` evaluation, see `try_expr`).
pub fn physical_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![Arc::new(try_expr::TrinoTry)]
}

/// Rewrites Trino syntax the SQL parser rejects (`VALUES 1, 2`,
/// `CAST(x AS array(T))`); apply before parsing.
pub fn preprocess_sql(sql: &str) -> std::borrow::Cow<'_, str> {
    preprocess::preprocess(sql)
}

/// Runs the analyzer on a query plan before it becomes a `DataFrame`.
///
/// The library's analyzer rewrites give Trino result types (decimal
/// arithmetic, casts), which the SQL planner's schema does not know yet: a
/// frame built from the analyzed plan reports the types its batches have.
/// DDL, DML and `EXPLAIN` plans are returned as they are.
pub fn analyze(ctx: &SessionContext, plan: LogicalPlan) -> Result<LogicalPlan> {
    if matches!(
        plan,
        LogicalPlan::Ddl(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Extension(_)
    ) {
        return Ok(plan);
    }
    let state = ctx.state();
    state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
}

/// Rewrites Trino syntax in a parsed statement (see the module docs).
pub fn rewrite_statement(statement: &mut Statement) -> Result<()> {
    syntax::rewrite_statement(statement)
}
