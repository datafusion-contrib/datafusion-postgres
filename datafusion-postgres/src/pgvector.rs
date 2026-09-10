//! pgvector support for the `datafusion-postgres` frontend, gated behind the
//! `pgvector` Cargo feature.
//!
//! Two integration points live here, deliberately kept out of the generic
//! `pg_catalog` compatibility layer:
//!
//! * [`PgVectorExprPlanner`] -- a DataFusion [`ExprPlanner`] that rewrites the
//!   pgvector distance operators (`<->`, `<#>`, `<=>`) into DataFusion's
//!   built-in array distance functions (`array_distance`, `inner_product`,
//!   `cosine_distance`) while planning SQL, so they work in every expression
//!   position (projection, `WHERE`, `ORDER BY`, subqueries, ...).
//! * [`PgVectorInsertHook`] -- a [`QueryHook`] that rewrites
//!   `INSERT ... VALUES ('[1,2,3]')` string literals into `ARRAY[...]` for
//!   `vector(n)` columns, which need the target table's schema.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::{Expr, LogicalPlan, ScalarUDF};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast::{
    Array, BinaryOperator, DataType as SQLDataType, Expr as SQLExpr, ObjectName, SetExpr,
    Statement, TableObject, UnaryOperator, Value, ValueWithSpan,
};
use pgwire::api::ClientInfo;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};

use crate::hooks::{HookClient, QueryHook};

/// Install the pgvector expression planner into `session_context`.
///
/// Looks up the distance UDFs the planner rewrites onto; if they are missing
/// (e.g. a context without the default nested functions) installation is
/// skipped so the server keeps working, just without pgvector operators.
pub fn install(session_context: &SessionContext) {
    let state = session_context.state();
    let (Ok(array_distance), Ok(inner_product), Ok(cosine_distance)) = (
        state.udf("array_distance"),
        state.udf("inner_product"),
        state.udf("cosine_distance"),
    ) else {
        return;
    };

    let planner = Arc::new(PgVectorExprPlanner {
        array_distance,
        inner_product,
        cosine_distance,
    });

    // Append to (do not replace) the existing expression planners: DataFusion
    // registers its own (e.g. the nested-function array literal planner) that
    // must keep working.
    let mut planners = state.expr_planners().to_vec();
    planners.push(planner);

    // The distance operators are only tokenized by the Postgres SQL dialect
    // (DataFusion's default Generic dialect rejects `<->`). Queries sent over
    // the simple protocol are re-serialized and parsed by DataFusion, so the
    // session parser must use the Postgres dialect for the planner to see them.
    let mut config = session_context.copied_config();
    let _ = config
        .options_mut()
        .set("datafusion.sql_parser.dialect", "postgres");

    let state_ref = session_context.state_ref();
    let existing = state_ref.read().clone();
    let new_state = SessionStateBuilder::new_from_existing(existing)
        .with_config(config)
        .with_expr_planners(planners)
        .build();
    *state_ref.write() = new_state;
}

/// A DataFusion [`ExprPlanner`] implementing the pgvector distance operators.
#[derive(Debug)]
pub struct PgVectorExprPlanner {
    array_distance: Arc<ScalarUDF>,
    inner_product: Arc<ScalarUDF>,
    cosine_distance: Arc<ScalarUDF>,
}

impl ExprPlanner for PgVectorExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &datafusion::common::DFSchema,
    ) -> datafusion::error::Result<PlannerResult<RawBinaryExpr>> {
        // (function, whether the result must be negated)
        let (func, negate) = match &expr.op {
            BinaryOperator::LtDashGt => (&self.array_distance, false),
            BinaryOperator::Spaceship => (&self.cosine_distance, false),
            // `<#>` is the *negative* inner product.
            BinaryOperator::Custom(name) if name == "<#>" => (&self.inner_product, true),
            _ => return Ok(PlannerResult::Original(expr)),
        };

        let left = coerce_vector_operand(expr.left);
        let right = coerce_vector_operand(expr.right);
        let call =
            Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(func), vec![left, right]));

        let planned = if negate {
            Expr::Negative(Box::new(call))
        } else {
            call
        };
        Ok(PlannerResult::Planned(planned))
    }
}

/// True when `data_type` is a float vector list (`List(Float32)` /
/// `FixedSizeList(Float32, n)`).
fn is_float_list(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeList(field, _) | DataType::List(field) => {
            field.data_type() == &DataType::Float32
        }
        _ => false,
    }
}

/// The UTF-8 text of a string scalar literal, if it is one.
fn utf8_literal(scalar: &ScalarValue) -> Option<&str> {
    match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s),
        _ => None,
    }
}

/// Parse `[1,2,3]` into a `List(Float32)` scalar literal.
fn parse_vector_list(text: &str) -> Option<ScalarValue> {
    let values = parse_vector_floats(text)?;
    let scalars: Vec<ScalarValue> = values
        .into_iter()
        .map(|v| ScalarValue::Float32(Some(v)))
        .collect();
    Some(ScalarValue::List(ScalarValue::new_list_nullable(
        &scalars,
        &DataType::Float32,
    )))
}

/// Normalize an operator operand: turn a pgvector string literal (`'[1,2,3]'`)
/// or a cast of one (`'[1,2,3]'::vector`) into an `ARRAY[...]`-style list
/// literal. Anything else is returned unchanged.
fn coerce_vector_operand(expr: Expr) -> Expr {
    // `'[1,2,3]'::vector` -- the cast target is a float list.
    if let Expr::Cast(cast) = expr {
        if is_float_list(cast.field.data_type())
            && let Expr::Literal(scalar, _) = cast.expr.as_ref()
            && let Some(text) = utf8_literal(scalar)
            && let Some(list) = parse_vector_list(text)
        {
            return Expr::Literal(list, None);
        }
        return Expr::Cast(cast);
    }

    // Bare `'[1,2,3]'`.
    if let Expr::Literal(scalar, metadata) = &expr
        && let Some(text) = utf8_literal(scalar)
        && let Some(list) = parse_vector_list(text)
    {
        return Expr::Literal(list, metadata.clone());
    }

    expr
}

// ---------------------------------------------------------------------------
// INSERT ... VALUES ('[1,2,3]') support
// ---------------------------------------------------------------------------

/// A [`QueryHook`] rewriting pgvector string literals in `INSERT ... VALUES`
/// against the target table's schema.
#[derive(Debug)]
pub struct PgVectorInsertHook;

#[async_trait]
impl QueryHook for PgVectorInsertHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let mut statement = statement.clone();
        if !rewrite_insert(session_context, &mut statement).await {
            return None;
        }

        let query = statement.to_string();
        let timeout = crate::client::get_statement_timeout(client);
        let result = async {
            let df = match timeout {
                Some(duration) => tokio::time::timeout(duration, session_context.sql(&query))
                    .await
                    .map_err(|_| {
                        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "57014".to_string(),
                            "canceling statement due to statement timeout".to_string(),
                        )))
                    })?
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?,
                None => session_context
                    .sql(&query)
                    .await
                    .map_err(|e| PgWireError::ApiError(Box::new(e)))?,
            };
            let batches = df
                .collect()
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            let rows_affected = batches
                .first()
                .and_then(|batch| batch.column_by_name("count"))
                .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                .map_or(0, |array| array.value(0) as usize);
            Ok::<_, PgWireError>(Response::Execution(
                Tag::new("INSERT").with_oid(0).with_rows(rows_affected),
            ))
        }
        .await;

        Some(result)
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let mut statement = sql.clone();
        if !rewrite_insert(session_context, &mut statement).await {
            return None;
        }

        let state = session_context.state();
        let plan = state
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
                statement,
            )))
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)));
        Some(plan)
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// Rewrite the pgvector string literals of an `INSERT ... VALUES` statement so
/// DataFusion can write them into `vector` columns.
///
/// Returns `true` if any value was rewritten. When the target table cannot be
/// resolved, no column is a vector column, or no value parses as a vector
/// literal, the statement is left untouched and `false` is returned.
async fn rewrite_insert(session_context: &SessionContext, statement: &mut Statement) -> bool {
    let Statement::Insert(insert) = statement else {
        return false;
    };
    let TableObject::TableName(table_name) = &insert.table else {
        return false;
    };
    let Some(source) = insert.source.as_mut() else {
        return false;
    };
    let SetExpr::Values(values) = source.body.as_mut() else {
        return false;
    };
    // DataFusion INSERT only supports single-part column names.
    if insert.columns.iter().any(|col| col.0.len() != 1) {
        return false;
    }

    let Ok(provider) = session_context
        .table_provider(object_name_to_table_reference(table_name))
        .await
    else {
        return false;
    };
    let target_schema = provider.schema();

    // Map each provided value position to the Arrow type of the target column.
    let target_types: Vec<Option<DataType>> = if insert.columns.is_empty() {
        target_schema
            .fields()
            .iter()
            .map(|field| Some(field.data_type().clone()))
            .collect()
    } else {
        insert
            .columns
            .iter()
            .map(|col| {
                let ident = col.0[0].as_ident()?;
                target_schema
                    .fields()
                    .iter()
                    .find(|field| field.name().eq_ignore_ascii_case(&ident.value))
                    .map(|field| field.data_type().clone())
            })
            .collect()
    };

    let mut changed = false;
    for row in &mut values.rows {
        for (pos, target_type) in target_types.iter().enumerate() {
            let Some(target_type) = target_type else {
                continue;
            };
            // Only float vector/list columns accept the bracket-string form.
            let Some(dim) = vector_dimension(target_type) else {
                continue;
            };
            let Some(expr) = row.content.get_mut(pos) else {
                continue;
            };
            let Some(text) = vector_literal_text(expr) else {
                continue;
            };
            let Some(array) = vector_literal_to_array(&text) else {
                continue;
            };
            // When the column fixes a dimension, honor it: leave mismatched
            // values for DataFusion to reject rather than inserting silently.
            let count = match &array {
                SQLExpr::Array(array) => array.elem.len(),
                _ => unreachable!("vector_literal_to_array returns an Array"),
            };
            if dim.is_some_and(|expected| expected as usize != count) {
                continue;
            }
            *expr = array;
            changed = true;
        }
    }
    changed
}

/// The Arrow [`DataType`] of a pgvector `vector` column, if `field_type` is one.
///
/// `Some(Some(n))` for `vector(n)` (`FixedSizeList(Float32, n)`),
/// `Some(None)` for a dimension-less `vector` (`List(Float32)`), `None`
/// otherwise.
fn vector_dimension(field_type: &DataType) -> Option<Option<i32>> {
    match field_type {
        DataType::FixedSizeList(field, n) if field.data_type() == &DataType::Float32 => {
            Some(Some(*n))
        }
        DataType::List(field) if field.data_type() == &DataType::Float32 => Some(None),
        _ => None,
    }
}

/// Convert a table-name `ObjectName` into a DataFusion table reference.
fn object_name_to_table_reference(name: &ObjectName) -> datafusion::common::TableReference {
    let parts = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect::<Vec<String>>();
    match parts.as_slice() {
        [catalog, schema, table] => datafusion::common::TableReference::full(
            catalog.as_str(),
            schema.as_str(),
            table.as_str(),
        ),
        [schema, table] => {
            datafusion::common::TableReference::partial(schema.as_str(), table.as_str())
        }
        [table] => datafusion::common::TableReference::bare(table.as_str()),
        _ => datafusion::common::TableReference::bare(name.to_string().as_str()),
    }
}

/// Return the pgvector literal text carried by `expr` (a bare `'[1,2,3]'` or
/// `'[1,2,3]'::vector`), or `None` if the expression is not one of those.
fn vector_literal_text(expr: &SQLExpr) -> Option<String> {
    match expr {
        SQLExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text),
            ..
        }) => Some(text.clone()),
        SQLExpr::Cast {
            expr: inner,
            data_type,
            ..
        } if is_vector_sql_type(data_type) => {
            if let SQLExpr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(text),
                ..
            }) = inner.as_ref()
            {
                Some(text.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// True when `data_type` is the pgvector `vector` type name.
fn is_vector_sql_type(data_type: &SQLDataType) -> bool {
    let SQLDataType::Custom(name, _) = data_type else {
        return false;
    };
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("vector"))
}

/// Parse the floats of a pgvector literal `[1,-2.5,3]`.
fn parse_vector_floats(text: &str) -> Option<Vec<f32>> {
    let text = text.trim();
    if !(text.starts_with('[') && text.ends_with(']') && text.len() >= 2) {
        return None;
    }
    let inner = &text[1..text.len() - 1];
    if inner.trim().is_empty() {
        return None;
    }
    let mut values = Vec::new();
    for part in inner.split(',') {
        values.push(part.trim().parse::<f32>().ok()?);
    }
    Some(values)
}

/// Build a SQL `ARRAY[<floats>]` literal from a pgvector literal string.
fn vector_literal_to_array(text: &str) -> Option<SQLExpr> {
    let values = parse_vector_floats(text)?;
    let mut elems = Vec::with_capacity(values.len());
    for value in values {
        elems.push(float_literal(value));
    }
    Some(SQLExpr::Array(Array {
        elem: elems,
        named: true,
    }))
}

/// A float SQL literal for `value` (negative values become a unary minus).
fn float_literal(value: f32) -> SQLExpr {
    let rendered = value.to_string();
    let (negative, digits) = match rendered.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, rendered.as_str()),
    };
    let number = SQLExpr::Value(Value::Number(digits.to_string(), false).with_empty_span());
    if negative {
        SQLExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(number),
        }
    } else {
        number
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_vector_literal_floats() {
        assert_eq!(
            parse_vector_floats("[1, -2.5, 3]"),
            Some(vec![1.0, -2.5, 3.0])
        );
    }

    #[test]
    fn rejects_non_vector_literals() {
        assert_eq!(parse_vector_floats("[a,b]"), None);
        assert_eq!(parse_vector_floats("1,2,3"), None);
        assert_eq!(parse_vector_floats("[]"), None);
    }

    #[test]
    fn detects_float_list_columns() {
        assert!(is_float_list(&DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new_list_field(DataType::Float32, true)
        ))));
        assert!(!is_float_list(&DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new_list_field(DataType::Int32, true)
        ))));
    }
}
