use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::common::metadata::ScalarAndMetadata;
use datafusion::common::{DFSchema, ParamValues};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::prelude::{Expr, SessionContext};
use datafusion::sql::sqlparser::ast::{ExprWithAlias, Statement};
use pgwire::api::results::{Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};

use super::QueryHook;

/// Prepared statement storage: name -> (parsed statement, logical plan)
type PreparedStatements =
    Arc<RwLock<HashMap<String, (Statement, LogicalPlan)>>>;

/// Hook for handling PREPARE, EXECUTE, DEALLOCATE statements
pub struct PrepareExecuteHook {
    prepared_statements: PreparedStatements,
}

impl PrepareExecuteHook {
    pub fn new() -> Self {
        PrepareExecuteHook {
            prepared_statements: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn clone_statements(&self) -> PreparedStatements {
        self.prepared_statements.clone()
    }
}

impl Default for PrepareExecuteHook {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueryHook for PrepareExecuteHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            // PREPARE stmt [(param_types)] AS inner_statement
            Statement::Prepare {
                name,
                data_types: _,
                statement,
            } => {
                let stmt_name = name.to_string();

                // Convert inner statement to LogicalPlan
                let plan_result = session_context
                    .state()
                    .statement_to_plan(datafusion::sql::parser::Statement::Statement(
                        Box::new(*statement.clone()),
                    ))
                    .await;

                match plan_result {
                    Ok(plan) => {
                        // Store the prepared statement
                        let mut stmts = self.prepared_statements.write().unwrap();
                        stmts.insert(stmt_name, (*statement.clone(), plan));

                        Some(Ok(Response::Execution(Tag::new("PREPARE"))))
                    }
                    Err(e) => Some(Err(PgWireError::ApiError(Box::new(e)))),
                }
            }

            // EXECUTE stmt [(params)]
            Statement::Execute {
                name,
                parameters,
                using: _,
                has_parentheses: _,
                immediate: _,
                into: _,
                output: _,
                default: _,
            } => {
                let stmt_name = match name {
                    Some(obj_name) => obj_name.to_string(),
                    None => {
                        return Some(Err(PgWireError::UserError(Box::new(
                            pgwire::error::ErrorInfo::new(
                                "ERROR".to_string(),
                                "42P08".to_string(),
                                "EXECUTE requires a prepared statement name".to_string(),
                            ),
                        ))));
                    }
                };

                // Retrieve the prepared statement plan
                let plan_clone = {
                    let stmts = self.prepared_statements.read().unwrap();
                    stmts.get(&stmt_name).map(|(_, plan)| plan.clone())
                };

                match plan_clone {
                    Some(plan) => {
                        // Build ParamValues from EXECUTE parameters
                        let param_values_result: Result<ParamValues, PgWireError> =
                            if parameters.is_empty() {
                                Ok(ParamValues::List(vec![]))
                            } else {
                                let state = session_context.state();
                                let empty_schema = DFSchema::empty();
                                let exec_props = state.execution_props().clone();
                                let simplify_ctx = SimplifyContext::new(&exec_props);
                                let simplifier = ExprSimplifier::new(simplify_ctx);

                                let scalar_params: Result<Vec<ScalarAndMetadata>, PgWireError> =
                                    parameters
                                        .iter()
                                        .enumerate()
                                        .map(|(i, sql_expr)| {
                                            // Convert sqlparser Expr to DataFusion Expr
                                            let expr_with_alias = ExprWithAlias {
                                                expr: sql_expr.clone(),
                                                alias: None,
                                            };
                                            let df_expr = state
                                                .create_logical_expr_from_sql_expr(
                                                    expr_with_alias,
                                                    &empty_schema,
                                                )
                                                .map_err(|e| {
                                                    PgWireError::ApiError(Box::new(e))
                                                })?;

                                            // Evaluate/simplify to a literal scalar
                                            match simplifier.simplify(df_expr).map_err(|e| {
                                                PgWireError::ApiError(Box::new(e))
                                            })? {
                                                Expr::Literal(scalar, metadata) => {
                                                    Ok(ScalarAndMetadata::new(scalar, metadata))
                                                }
                                                other => Err(PgWireError::UserError(Box::new(
                                                    pgwire::error::ErrorInfo::new(
                                                        "ERROR".to_string(),
                                                        "22023".to_string(),
                                                        format!(
                                                            "Parameter ${} is not a constant expression: {}",
                                                            i + 1,
                                                            other
                                                        ),
                                                    ),
                                                ))),
                                            }
                                        })
                                        .collect();

                                scalar_params.map(ParamValues::List)
                            };

                        let param_values = match param_values_result {
                            Ok(pv) => pv,
                            Err(e) => return Some(Err(e)),
                        };

                        // Substitute parameter values into the logical plan
                        let plan = match plan.replace_params_with_values(&param_values) {
                            Ok(p) => p,
                            Err(e) => return Some(Err(PgWireError::ApiError(Box::new(e)))),
                        };

                        let df_result = session_context.execute_logical_plan(plan).await;

                        match df_result {
                            Ok(df) => {
                                let metadata = std::collections::HashMap::new();
                                let format_options = Arc::new(
                                    pgwire::types::format::FormatOptions::from_client_metadata(
                                        &metadata,
                                    ),
                                );
                                let resp = arrow_pg::datatypes::df::encode_dataframe(
                                    df,
                                    &pgwire::api::portal::Format::UnifiedText,
                                    Some(format_options),
                                )
                                .await;

                                Some(resp.map(Response::Query))
                            }
                            Err(e) => Some(Err(PgWireError::ApiError(Box::new(e)))),
                        }
                    }
                    None => Some(Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "ERROR".to_string(),
                            "26000".to_string(),
                            format!("Prepared statement '{}' does not exist", stmt_name),
                        ),
                    )))),
                }
            }

            // DEALLOCATE { name | ALL }
            Statement::Deallocate { name, prepare: _ } => {
                let mut stmts = self.prepared_statements.write().unwrap();

                if name.value.to_uppercase() == "ALL" {
                    stmts.clear();
                } else {
                    stmts.remove(&name.to_string());
                }

                Some(Ok(Response::Execution(Tag::new("DEALLOCATE"))))
            }

            _ => None,
        }
    }

    async fn handle_extended_parse_query(
        &self,
        _sql: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        None
    }
}
