use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::metadata::ScalarAndMetadata;
use datafusion::common::{DFSchema, ParamValues};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::prelude::{Expr, SessionContext};
use datafusion::sql::sqlparser::ast::{DataType as SqlDataType, ExprWithAlias, Statement};
use pgwire::api::results::{Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};

use super::QueryHook;

struct PreparedStatementInfo {
    plan: LogicalPlan,
    parameter_types: Vec<SqlDataType>,
}

/// Prepared statement storage: name -> PreparedStatementInfo
type PreparedStatements = Arc<RwLock<HashMap<String, PreparedStatementInfo>>>;

/// Convert a sqlparser DataType to an Arrow DataType for parameter type validation.
fn sql_type_to_arrow(sql_type: &SqlDataType) -> Option<ArrowDataType> {
    match sql_type {
        SqlDataType::Boolean | SqlDataType::Bool => Some(ArrowDataType::Boolean),
        SqlDataType::TinyInt(_) => Some(ArrowDataType::Int8),
        SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => Some(ArrowDataType::Int16),
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => {
            Some(ArrowDataType::Int32)
        }
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => Some(ArrowDataType::Int64),
        SqlDataType::Float(_) | SqlDataType::Real | SqlDataType::Float4 => {
            Some(ArrowDataType::Float32)
        }
        SqlDataType::Double(_) | SqlDataType::DoublePrecision | SqlDataType::Float8 => {
            Some(ArrowDataType::Float64)
        }
        SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::Char(_)
        | SqlDataType::String(_) => Some(ArrowDataType::Utf8),
        SqlDataType::Bytea => Some(ArrowDataType::Binary),
        SqlDataType::Date => Some(ArrowDataType::Date32),
        _ => None,
    }
}

/// Check if two Arrow DataTypes are compatible for parameter passing.
fn types_compatible(expected: &ArrowDataType, actual: &ArrowDataType) -> bool {
    if expected == actual {
        return true;
    }
    matches!(
        (expected, actual),
        (ArrowDataType::Int64, ArrowDataType::Int8)
            | (ArrowDataType::Int64, ArrowDataType::Int16)
            | (ArrowDataType::Int64, ArrowDataType::Int32)
            | (ArrowDataType::Int32, ArrowDataType::Int8)
            | (ArrowDataType::Int32, ArrowDataType::Int16)
            | (ArrowDataType::Int16, ArrowDataType::Int8)
            | (ArrowDataType::Float64, ArrowDataType::Float32)
            | (ArrowDataType::Float32, ArrowDataType::Int8)
            | (ArrowDataType::Float32, ArrowDataType::Int16)
            | (ArrowDataType::Float32, ArrowDataType::Int32)
            | (ArrowDataType::Float64, ArrowDataType::Int8)
            | (ArrowDataType::Float64, ArrowDataType::Int16)
            | (ArrowDataType::Float64, ArrowDataType::Int32)
            | (ArrowDataType::Float64, ArrowDataType::Int64)
    )
}

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
                data_types,
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
                        let mut stmts = self.prepared_statements.write().unwrap();
                        stmts.insert(
                            stmt_name,
                            PreparedStatementInfo {
                                plan,
                                parameter_types: data_types.clone(),
                            },
                        );

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

                // Retrieve the prepared statement info
                let info = {
                    let stmts = self.prepared_statements.read().unwrap();
                    stmts.get(&stmt_name).map(|info| {
                        (info.plan.clone(), info.parameter_types.clone())
                    })
                };

                match info {
                    Some((plan, declared_types)) => {
                        // Validate parameter count when types were explicitly declared
                        if !declared_types.is_empty()
                            && parameters.len() != declared_types.len()
                        {
                            return Some(Err(PgWireError::UserError(Box::new(
                                pgwire::error::ErrorInfo::new(
                                    "ERROR".to_string(),
                                    "07001".to_string(),
                                    format!(
                                        "wrong number of parameters for prepared statement: expected {}, got {}",
                                        declared_types.len(),
                                        parameters.len()
                                    ),
                                ),
                            ))));
                        }

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
                                            let (scalar, metadata) = match simplifier
                                                .simplify(df_expr)
                                                .map_err(|e| PgWireError::ApiError(Box::new(e)))?
                                            {
                                                Expr::Literal(scalar, metadata) => {
                                                    (scalar, metadata)
                                                }
                                                other => {
                                                    return Err(PgWireError::UserError(Box::new(
                                                        pgwire::error::ErrorInfo::new(
                                                            "ERROR".to_string(),
                                                            "22023".to_string(),
                                                            format!(
                                                                "Parameter ${} is not a constant expression: {}",
                                                                i + 1,
                                                                other
                                                            ),
                                                        ),
                                                    )));
                                                }
                                            };

                                            // Type validation against declared types
                                            if let Some(sql_type) = declared_types.get(i) {
                                                if let Some(expected_type) =
                                                    sql_type_to_arrow(sql_type)
                                                {
                                                    let actual_type = scalar.data_type();
                                                    if !types_compatible(&expected_type, &actual_type) {
                                                        return match scalar.cast_to(&expected_type) {
                                                            Ok(coerced) => Ok(
                                                                ScalarAndMetadata::new(
                                                                    coerced, metadata,
                                                                ),
                                                            ),
                                                            Err(_) => {
                                                                Err(PgWireError::UserError(Box::new(
                                                                    pgwire::error::ErrorInfo::new(
                                                                        "ERROR".to_string(),
                                                                        "22023".to_string(),
                                                                        format!(
                                                                            "parameter ${} has invalid type: expected {}, got {}",
                                                                            i + 1,
                                                                            sql_type,
                                                                            actual_type
                                                                        ),
                                                                    ),
                                                                )))
                                                            }
                                                        };
                                                    }
                                                }
                                            }

                                            Ok(ScalarAndMetadata::new(scalar, metadata))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use pgwire::api::query::SimpleQueryHandler;
    use pgwire::api::results::Response;

    use crate::handlers::DfSessionService;
    use crate::testing::MockClient;

    #[tokio::test]
    async fn test_execute_wrong_parameter_count() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE count_stmt (INT) AS SELECT $1",
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Response::Execution(_)));

        let result = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE count_stmt(1, 2)",
        )
        .await;

        assert!(result.is_err(), "EXECUTE with wrong parameter count should fail");
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("07001") || err_msg.contains("wrong number"),
            "Error should reference SQLSTATE 07001: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_execute_parameter_type_mismatch() {
        let session_context = Arc::new(SessionContext::new());
        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE type_stmt (INT) AS SELECT $1",
        )
        .await
        .unwrap();
        assert!(matches!(results[0], Response::Execution(_)));

        let result = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE type_stmt('not_a_number')",
        )
        .await;

        assert!(
            result.is_err(),
            "EXECUTE with incompatible type should fail"
        );
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("22023") || err_msg.contains("invalid type") || err_msg.contains("invalid parameter"),
            "Error should reference SQLSTATE 22023 or type mismatch: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_execute_correct_parameter_count() {
        let session_context = Arc::new(SessionContext::new());

        session_context
            .sql("CREATE TABLE count_test (id BIGINT, val VARCHAR)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        session_context
            .sql("INSERT INTO count_test VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let service = DfSessionService::new(session_context);
        let mut client = MockClient::new();

        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "PREPARE valid_stmt (BIGINT) AS SELECT id FROM count_test WHERE id = $1",
        )
        .await
        .unwrap();
        assert!(matches!(results[0], Response::Execution(_)));

        let results = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "EXECUTE valid_stmt(1)",
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Response::Query(_)));
    }
}
