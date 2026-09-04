//! Schema-aware rewrite that lets `INSERT ... VALUES ('[1,2,3]')` write into a
//! pgvector `vector` column.
//!
//! DataFusion plans `INSERT` by coercing every provided value to the target
//! column's type with [`Expr::cast_to`], which only allows casts DataFusion
//! already knows. There is no `Utf8 -> FixedSizeList(Float32, n)` cast, so a
//! bare pgvector literal `'[1,2,3]'` fails to plan with "Cannot automatically
//! convert Utf8 to FixedSizeList(...)".
//!
//! Unlike the operator rules (which run before the query has a schema), this
//! rewrite is invoked from the server handlers with the live `SessionContext`,
//! so it can resolve the INSERT target table's schema. For every value that is
//! bound to a pgvector `vector` column (Arrow `List(Float32)` /
//! `FixedSizeList(Float32, n)`), the string literal is replaced with an
//! `ARRAY[...]` of floats. DataFusion then inserts via its supported
//! `List -> FixedSizeList(Float32, n)` coercion (mismatched dimensions surface
//! as a cast error at runtime, matching pgvector's dimension enforcement).
//!
//! The rewrite is conservative:
//! * only `INSERT ... VALUES` statements whose target table resolves are
//!   touched;
//! * only positions whose target column is a float vector/list type;
//! * only string literals that actually parse as a numeric `[...]` vector
//!   (optionally written as `'[...]'::vector`).
//!
//! Everything else is left untouched, so a plain text column receiving a
//! bracket-looking string still works.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr, ObjectName, SetExpr, Statement, TableObject, Value, ValueWithSpan,
};

use super::rules::RewriteVectorOperators;

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

/// Convert a table-name `ObjectName` into a [`TableReference`].
fn object_name_to_table_reference(name: &ObjectName) -> TableReference {
    let parts = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect::<Vec<String>>();
    match parts.as_slice() {
        [catalog, schema, table] => {
            TableReference::full(catalog.as_str(), schema.as_str(), table.as_str())
        }
        [schema, table] => TableReference::partial(schema.as_str(), table.as_str()),
        [table] => TableReference::bare(table.as_str()),
        _ => TableReference::bare(name.to_string().as_str()),
    }
}

/// Return the pgvector literal text carried by `expr` (a bare `'[1,2,3]'` or
/// `'[1,2,3]'::vector`), or `None` if the expression is not one of those.
fn vector_literal_text(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text),
            ..
        }) => Some(text.clone()),
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } if RewriteVectorOperators::is_vector_data_type(data_type) => {
            if let Expr::Value(ValueWithSpan {
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

/// Rewrite the pgvector string literals of an `INSERT ... VALUES` statement so
/// DataFusion can write them into `vector` columns.
///
/// Returns `true` if any value was rewritten. When the target table cannot be
/// resolved, the columns are not vector columns, or no value parses as a vector
/// literal, the statement is left untouched and `false` is returned so the
/// caller can fall back to DataFusion's normal (erroring) handling.
pub async fn rewrite_vector_insert(
    session_context: &SessionContext,
    statement: &mut Statement,
) -> bool {
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
    // Only single-part column names are supported by DataFusion INSERT anyway.
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
            let Some(array) = RewriteVectorOperators::vector_literal_to_array(&text) else {
                continue;
            };
            // When the column fixes a dimension, honor it: leave mismatched
            // values for DataFusion to reject rather than inserting silently.
            let count = match &array {
                Expr::Array(array) => array.elem.len(),
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

#[cfg(all(test, feature = "pgvector"))]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{FixedSizeListArray, Float32Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn vector_field() -> Field {
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Float32, true)), 3),
            false,
        )
        .with_metadata(std::collections::HashMap::from([(
            "pg.vector".to_string(),
            "vector".to_string(),
        )]))
    }

    async fn register_items(ctx: &SessionContext) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            vector_field(),
        ]));
        let id = Int64Array::from(vec![0]); // placeholder row; only the schema matters
        let values = Float32Array::from(vec![0.0, 0.0, 0.0]);
        let embedding = FixedSizeListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Float32, true)),
            3,
            Arc::new(values),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(id), Arc::new(embedding)]).unwrap();
        ctx.register_batch("items", batch).unwrap();
    }

    async fn parse_insert(ctx: &SessionContext, sql: &str) -> (bool, String) {
        let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let mut statement = stmts.remove(0);
        let changed = rewrite_vector_insert(ctx, &mut statement).await;
        (changed, statement.to_string())
    }

    #[tokio::test]
    async fn rewrites_vector_string_to_array_literal() {
        let ctx = SessionContext::new();
        register_items(&ctx).await;

        let (changed, sql) = parse_insert(
            &ctx,
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]')",
        )
        .await;
        assert!(changed, "vector literal must be rewritten");
        assert!(
            sql.contains("VALUES (1, ARRAY[1, 2, 3])") || sql.contains("VALUES (1, ARRAY[1,2,3])"),
            "unexpected rewrite: {sql}"
        );
    }

    #[tokio::test]
    async fn rewrites_vector_cast_literal() {
        let ctx = SessionContext::new();
        register_items(&ctx).await;

        let (changed, sql) = parse_insert(
            &ctx,
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'::vector)",
        )
        .await;
        assert!(changed, "vector cast literal must be rewritten");
        assert!(sql.contains("ARRAY[1, 2, 3]"), "unexpected rewrite: {sql}");
    }

    #[tokio::test]
    async fn leaves_text_and_wrong_dimension_inserts_alone() {
        let ctx = SessionContext::new();
        register_items(&ctx).await;

        // A non-vector target (table missing) is untouched.
        let (changed, _) = parse_insert(
            &ctx,
            "INSERT INTO nope (id, embedding) VALUES (1, '[1,2,3]')",
        )
        .await;
        assert!(!changed);

        // A bracket string into a plain text column is untouched.
        ctx.register_batch(
            "logs",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("tag", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![0])),
                    Arc::new(datafusion::arrow::array::StringArray::from(vec![""])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let (changed, _) =
            parse_insert(&ctx, "INSERT INTO logs (id, tag) VALUES (1, '[1,2,3]')").await;
        assert!(!changed, "text column must not be rewritten");

        // A vector column with a mismatched dimension is untouched (DataFusion
        // will reject it as a cast error rather than silently truncating).
        let (changed, _) = parse_insert(
            &ctx,
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2]')",
        )
        .await;
        assert!(!changed);
    }

    #[tokio::test]
    async fn insert_vector_literal_stores_row() {
        let ctx = SessionContext::new();
        register_items(&ctx).await;

        let sql = parse_insert(
            &ctx,
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]')",
        )
        .await
        .1;
        let affected = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let count = affected[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1, "one row must be inserted");

        let batches = ctx
            .sql("SELECT id FROM items WHERE id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        // And the nearest-neighbour query finds it at distance 0.
        let batches = ctx
            .sql("SELECT id FROM items ORDER BY array_distance(embedding, ARRAY[1.0, 2.0, 3.0]) LIMIT 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let id = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(id, 1);
    }

    #[tokio::test]
    async fn insert_multiple_rows() {
        let ctx = SessionContext::new();
        register_items(&ctx).await;

        let sql = parse_insert(
            &ctx,
            "INSERT INTO items (id, embedding) VALUES (1, '[1,0,0]'), (2, '[0,1,0]')",
        )
        .await
        .1;
        let affected = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        let count = affected[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 2);

        // Order by distance to [1,0,0]: the placeholder row 0 ([0,0,0]) sorts
        // between row 1 (exact match) and row 2.
        let batches = ctx
            .sql("SELECT id FROM items ORDER BY array_distance(embedding, ARRAY[1.0, 0.0, 0.0])")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ids: Vec<i64> = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ids, vec![1, 0, 2]);
    }
}
