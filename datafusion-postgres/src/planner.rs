use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::Expr;
use pgwire::api::Type;

fn extract_placeholder_cast_types(plan: &LogicalPlan) -> Result<HashMap<String, Option<DataType>>> {
    let mut placeholder_types = HashMap::new();
    let mut casted_placeholders = HashSet::new();

    plan.apply(|node| {
        for expr in node.expressions() {
            let _ = expr.apply(|e| {
                if let Expr::Cast(cast) = e
                    && let Expr::Placeholder(ph) = &*cast.expr
                {
                    placeholder_types.insert(ph.id.clone(), Some(cast.field.data_type().clone()));
                    casted_placeholders.insert(ph.id.clone());
                }

                if let Expr::Placeholder(ph) = e
                    && !casted_placeholders.contains(&ph.id)
                    && !placeholder_types.contains_key(&ph.id)
                {
                    placeholder_types.insert(ph.id.clone(), None);
                }

                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(placeholder_types)
}

pub fn get_inferred_parameter_types(
    plan: &LogicalPlan,
) -> Result<HashMap<String, Option<DataType>>> {
    let param_types = plan.get_parameter_types()?;

    let has_none = param_types.values().any(|v| v.is_none());

    if !has_none {
        Ok(param_types)
    } else {
        let cast_types = extract_placeholder_cast_types(plan)?;

        let mut merged = param_types;

        for (id, opt_type) in cast_types {
            merged
                .entry(id)
                .and_modify(|existing| {
                    if existing.is_none() {
                        *existing = opt_type.clone();
                    }
                })
                .or_insert(opt_type);
        }

        Ok(merged)
    }
}

/// For each prepared-statement parameter whose type was resolved from a
/// semantically-typed catalog column, return the Postgres wire type the client
/// must use to bind it.
///
/// DataFusion records the compared/assigned column's field -- including its
/// metadata -- on the placeholder. Two cases need an override over the physical
/// Arrow type mapping:
///
/// * `pg.oid_alias` columns (`pg_type.oid = $1`): report `OID`, not the `INT4`
///   the Int32 storage would imply, so drivers can bind their `u32` OID;
/// * `pg.vector` columns (`INSERT ... VALUES ($1, $2)` into a `vector(n)
///   column): report the pgvector `vector` type (OID 16385) instead of the
///   physical `float4[]`, so drivers can binary-encode the vector.
///
/// See the `pg.oid_alias` / `pg.vector` cross-crate contracts in arrow-pg.
pub fn parameter_override_types(plan: &LogicalPlan) -> HashMap<String, Type> {
    let mut overrides = HashMap::new();

    let _ = plan.apply(|node| {
        for expr in node.expressions() {
            let _ = expr.apply(|e| {
                if let Some((id, field)) = placeholder_field(e) {
                    // oid-alias kinds. Only `oid` (stored as Int32) gets an
                    // override -- the `reg*` aliases are stored/displayed as
                    // their name strings and keep the plain TEXT mapping so
                    // clients can bind them with ordinary string values.
                    if field.data_type() == &DataType::Int32
                        && let Some(kind) =
                            field.metadata().get(arrow_pg::datatypes::PG_OID_ALIAS_KEY)
                        && let Some(ty) = arrow_pg::datatypes::pg_type_for_alias_kind(kind.as_str())
                    {
                        overrides.insert(id.to_string(), ty);
                    }
                    // pgvector `vector`
                    #[cfg(feature = "pgvector")]
                    if arrow_pg::datatypes::is_pg_vector_field(field) {
                        overrides.insert(id.to_string(), arrow_pg::datatypes::pg_vector_type());
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(TreeNodeRecursion::Continue)
    });

    overrides
}

/// Unwrap a `$N` placeholder (possibly wrapped in a single cast) and return its
/// id (e.g. `"$1"`) together with the inferred field, if DataFusion resolved
/// one from the compared/assigned column.
fn placeholder_field(expr: &Expr) -> Option<(&str, &datafusion::arrow::datatypes::Field)> {
    let placeholder = match expr {
        Expr::Placeholder(ph) => ph,
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::Placeholder(ph) => ph,
            _ => return None,
        },
        _ => return None,
    };
    Some((placeholder.id.as_str(), placeholder.field.as_deref()?))
}

/// Sort a parameter-type map (`$1`, `$2`, ...) into positional order, returning
/// each entry's placeholder id alongside its inferred type.
pub fn ordered_parameter_entries(
    params: &HashMap<String, Option<DataType>>,
) -> Vec<(String, Option<DataType>)> {
    let mut entries = params.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| {
        key.trim_start_matches('$')
            .parse::<u32>()
            .unwrap_or(u32::MAX)
    });
    entries
        .into_iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
