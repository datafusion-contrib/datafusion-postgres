use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::Array;
use datafusion::sql::sqlparser::ast::ArrayElemTypeDef;
use datafusion::sql::sqlparser::ast::BinaryOperator;
use datafusion::sql::sqlparser::ast::CastKind;
use datafusion::sql::sqlparser::ast::DataType;
use datafusion::sql::sqlparser::ast::Expr;
use datafusion::sql::sqlparser::ast::Function;
use datafusion::sql::sqlparser::ast::FunctionArg;
use datafusion::sql::sqlparser::ast::FunctionArgExpr;
use datafusion::sql::sqlparser::ast::FunctionArgumentList;
use datafusion::sql::sqlparser::ast::FunctionArguments;
use datafusion::sql::sqlparser::ast::Ident;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::ast::ObjectNamePart;
use datafusion::sql::sqlparser::ast::OrderByKind;
use datafusion::sql::sqlparser::ast::Query;
use datafusion::sql::sqlparser::ast::Select;
use datafusion::sql::sqlparser::ast::SelectItem;
use datafusion::sql::sqlparser::ast::SelectItemQualifiedWildcardKind;
use datafusion::sql::sqlparser::ast::SetExpr;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::ast::TableWithJoins;
use datafusion::sql::sqlparser::ast::UnaryOperator;
use datafusion::sql::sqlparser::ast::Value;
use datafusion::sql::sqlparser::ast::ValueWithSpan;
use datafusion::sql::sqlparser::ast::VisitMut;
use datafusion::sql::sqlparser::ast::VisitorMut;

pub trait SqlStatementRewriteRule: Send + Sync + Debug {
    fn rewrite(&self, s: Statement) -> Statement;
}

/// Rewrite rule for adding alias to duplicated projection
///
/// This rule is to deal with sql like `SELECT n.oid, n.* FROM n`, which is a
/// valid statement in postgres. But datafusion treat it as illegal because of
/// duplicated column oid in projection.
///
/// This rule will add alias to column, when there is a wildcard found in
/// projection.
#[derive(Debug)]
pub struct AliasDuplicatedProjectionRewrite;

impl AliasDuplicatedProjectionRewrite {
    // Rewrites a SELECT statement to alias explicit columns from the same table as a qualified wildcard.
    fn rewrite_select_with_alias(select: &mut Box<Select>) {
        // 1. Collect all table aliases from qualified wildcards.
        let mut wildcard_tables = Vec::new();
        let mut has_simple_wildcard = false;
        for p in &select.projection {
            match p {
                SelectItem::QualifiedWildcard(name, _) => match name {
                    SelectItemQualifiedWildcardKind::ObjectName(objname) => {
                        // for n.oid,
                        let idents = objname
                            .0
                            .iter()
                            .map(|v| v.as_ident().unwrap().value.clone())
                            .collect::<Vec<_>>()
                            .join(".");

                        wildcard_tables.push(idents);
                    }
                    SelectItemQualifiedWildcardKind::Expr(_expr) => {
                        // FIXME:
                    }
                },
                SelectItem::Wildcard(_) => {
                    has_simple_wildcard = true;
                }
                _ => {}
            }
        }

        // If there are no qualified wildcards, there's nothing to do.
        if wildcard_tables.is_empty() && !has_simple_wildcard {
            return;
        }

        // 2. Rewrite the projection, adding aliases to matching columns.
        let mut new_projection = vec![];
        for p in select.projection.drain(..) {
            match p {
                SelectItem::UnnamedExpr(expr) => {
                    let alias_partial = match &expr {
                        // Case for `oid` (unqualified identifier)
                        Expr::Identifier(ident) => Some(ident.clone()),
                        // Case for `n.oid` (compound identifier)
                        Expr::CompoundIdentifier(idents) => {
                            // compare every ident but the last
                            if idents.len() > 1 {
                                let table_name = &idents[..idents.len() - 1]
                                    .iter()
                                    .map(|i| i.value.clone())
                                    .collect::<Vec<_>>()
                                    .join(".");
                                if wildcard_tables.iter().any(|name| name == table_name) {
                                    Some(idents[idents.len() - 1].clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    if let Some(name) = alias_partial {
                        let alias = format!("__alias_{name}");
                        new_projection.push(SelectItem::ExprWithAlias {
                            expr,
                            alias: Ident::new(alias),
                        });
                    } else {
                        new_projection.push(SelectItem::UnnamedExpr(expr));
                    }
                }
                // Preserve existing aliases and wildcards.
                _ => new_projection.push(p),
            }
        }
        select.projection = new_projection;
    }
}

impl SqlStatementRewriteRule for AliasDuplicatedProjectionRewrite {
    fn rewrite(&self, mut statement: Statement) -> Statement {
        if let Statement::Query(query) = &mut statement {
            if let SetExpr::Select(select) = query.body.as_mut() {
                Self::rewrite_select_with_alias(select);
            }
        }

        statement
    }
}

/// Prepend qualifier for order by or filter when there is qualified wildcard
///
/// Postgres allows unqualified identifier in ORDER BY and FILTER but it's not
/// accepted by datafusion.
#[derive(Debug)]
pub struct ResolveUnqualifiedIdentifer;

impl ResolveUnqualifiedIdentifer {
    fn rewrite_unqualified_identifiers(query: &mut Box<Query>) {
        if let SetExpr::Select(select) = query.body.as_mut() {
            // Step 1: Find all table aliases from FROM and JOIN clauses.
            let table_aliases = Self::get_table_aliases(&select.from);

            // Step 2: Check for a single qualified wildcard in the projection.
            let qualified_wildcard_alias = Self::get_qualified_wildcard_alias(&select.projection);
            if qualified_wildcard_alias.is_none() || table_aliases.is_empty() {
                return; // Conditions not met.
            }

            let wildcard_alias = qualified_wildcard_alias.unwrap();

            // Step 2.5: Collect all projection aliases to avoid rewriting them
            let projection_aliases = Self::get_projection_aliases(&select.projection);

            // Step 3: Rewrite expressions in the WHERE and ORDER BY clauses.
            if let Some(selection) = &mut select.selection {
                Self::rewrite_expr(selection, &wildcard_alias, &table_aliases, &projection_aliases);
            }

            if let Some(OrderByKind::Expressions(order_by_exprs)) =
                query.order_by.as_mut().map(|o| &mut o.kind)
            {
                for order_by_expr in order_by_exprs {
                    Self::rewrite_expr(&mut order_by_expr.expr, &wildcard_alias, &table_aliases, &projection_aliases);
                }
            }
        }
    }

    fn get_table_aliases(tables: &[TableWithJoins]) -> HashSet<String> {
        let mut aliases = HashSet::new();
        for table_with_joins in tables {
            if let TableFactor::Table {
                alias: Some(alias), ..
            } = &table_with_joins.relation
            {
                aliases.insert(alias.name.value.clone());
            }
            for join in &table_with_joins.joins {
                if let TableFactor::Table {
                    alias: Some(alias), ..
                } = &join.relation
                {
                    aliases.insert(alias.name.value.clone());
                }
            }
        }
        aliases
    }

    fn get_qualified_wildcard_alias(projection: &[SelectItem]) -> Option<String> {
        let mut qualified_wildcards = projection
            .iter()
            .filter_map(|item| {
                if let SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(objname),
                    _,
                ) = item
                {
                    Some(
                        objname
                            .0
                            .iter()
                            .map(|v| v.as_ident().unwrap().value.clone())
                            .collect::<Vec<_>>()
                            .join("."),
                    )
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if qualified_wildcards.len() == 1 {
            Some(qualified_wildcards.remove(0))
        } else {
            None
        }
    }

    fn get_projection_aliases(projection: &[SelectItem]) -> HashSet<String> {
        let mut aliases = HashSet::new();
        for item in projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    aliases.insert(alias.value.clone());
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    aliases.insert(ident.value.clone());
                }
                _ => {}
            }
        }
        aliases
    }

    fn rewrite_expr(expr: &mut Expr, wildcard_alias: &str, table_aliases: &HashSet<String>, projection_aliases: &HashSet<String>) {
        match expr {
            Expr::Identifier(ident) => {
                // If the identifier is not a table alias itself and not already aliased in projection, rewrite it.
                if !table_aliases.contains(&ident.value) && !projection_aliases.contains(&ident.value) {
                    *expr = Expr::CompoundIdentifier(vec![
                        Ident::new(wildcard_alias.to_string()),
                        ident.clone(),
                    ]);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::rewrite_expr(left, wildcard_alias, table_aliases, projection_aliases);
                Self::rewrite_expr(right, wildcard_alias, table_aliases, projection_aliases);
            }
            // Add more cases for other expression types as needed (e.g., `InList`, `Between`, etc.)
            _ => {}
        }
    }
}

impl SqlStatementRewriteRule for ResolveUnqualifiedIdentifer {
    fn rewrite(&self, mut statement: Statement) -> Statement {
        if let Statement::Query(query) = &mut statement {
            Self::rewrite_unqualified_identifiers(query);
        }

        statement
    }
}

/// Remove datafusion unsupported type annotations
/// it also removes pg_catalog as qualifier
#[derive(Debug)]
pub struct RemoveUnsupportedTypes {
    unsupported_types: HashSet<String>,
}

impl Default for RemoveUnsupportedTypes {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoveUnsupportedTypes {
    pub fn new() -> Self {
        let mut unsupported_types = HashSet::new();

        for item in [
            "regclass",
            "regproc",
            "regtype",
            "regtype[]",
            "regnamespace",
            "oid",
        ] {
            unsupported_types.insert(item.to_owned());
            unsupported_types.insert(format!("pg_catalog.{item}"));
        }

        Self { unsupported_types }
    }
}

struct RemoveUnsupportedTypesVisitor<'a> {
    unsupported_types: &'a HashSet<String>,
}

impl VisitorMut for RemoveUnsupportedTypesVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            // This is the key part: identify constants with type annotations.
            Expr::TypedString { value, data_type } => {
                if self
                    .unsupported_types
                    .contains(data_type.to_string().to_lowercase().as_str())
                {
                    *expr =
                        Expr::Value(Value::SingleQuotedString(value.to_string()).with_empty_span());
                }
            }
            Expr::Cast {
                data_type,
                expr: value,
                ..
            } => {
                if self
                    .unsupported_types
                    .contains(data_type.to_string().to_lowercase().as_str())
                {
                    *expr = *value.clone();
                }
            }
            // Add more match arms for other expression types (e.g., `Function`, `InList`) as needed.
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RemoveUnsupportedTypes {
    fn rewrite(&self, mut statement: Statement) -> Statement {
        let mut visitor = RemoveUnsupportedTypesVisitor {
            unsupported_types: &self.unsupported_types,
        };
        let _ = statement.visit(&mut visitor);
        statement
    }
}

/// Rewrite Postgres's ANY operator to array_contains
#[derive(Debug)]
pub struct RewriteArrayAnyAllOperation;

struct RewriteArrayAnyAllOperationVisitor;

impl RewriteArrayAnyAllOperationVisitor {
    fn any_to_array_cofntains(&self, left: &Expr, right: &Expr) -> Expr {
        let array = if let Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(array_literal),
            ..
        }) = right
        {
            let array_literal = array_literal.trim();
            if array_literal.starts_with('{') && array_literal.ends_with('}') {
                let items = array_literal.trim_matches(|c| c == '{' || c == '}' || c == ' ');
                let items = items.split(',').map(|s| s.trim()).filter(|s| !s.is_empty());

                // For now, we assume the data type is string
                let elems = items
                    .map(|s| {
                        Expr::Value(Value::SingleQuotedString(s.to_string()).with_empty_span())
                    })
                    .collect();
                Expr::Array(Array {
                    elem: elems,
                    named: true,
                })
            } else {
                right.clone()
            }
        } else {
            right.clone()
        };

        Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new("array_contains")]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(array)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(left.clone())),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })
    }
}

impl VisitorMut for RewriteArrayAnyAllOperationVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => match compare_op {
                BinaryOperator::Eq => {
                    *expr = self.any_to_array_cofntains(left.as_ref(), right.as_ref());
                }
                BinaryOperator::NotEq => {
                    // TODO:left not equals to any element in array
                }
                _ => {}
            },
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => match compare_op {
                BinaryOperator::Eq => {
                    // TODO: left equals to every element in array
                }
                BinaryOperator::NotEq => {
                    *expr = Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: Box::new(self.any_to_array_cofntains(left.as_ref(), right.as_ref())),
                    }
                }
                _ => {}
            },
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RewriteArrayAnyAllOperation {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RewriteArrayAnyAllOperationVisitor;

        let _ = s.visit(&mut visitor);

        s
    }
}

/// Prepend qualifier to table_name
///
/// Postgres has pg_catalog in search_path by default so it allow access to
/// `pg_namespace` without `pg_catalog.` qualifier
#[derive(Debug)]
pub struct PrependUnqualifiedPgTableName;

struct PrependUnqualifiedPgTableNameVisitor;

impl VisitorMut for PrependUnqualifiedPgTableNameVisitor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table { name, args, .. } = table_factor {
            // not a table function
            if args.is_none() && name.0.len() == 1 {
                if let ObjectNamePart::Identifier(ident) = &name.0[0] {
                    if ident.value.starts_with("pg_") {
                        *name = ObjectName(vec![
                            ObjectNamePart::Identifier(Ident::new("pg_catalog")),
                            name.0[0].clone(),
                        ]);
                    }
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for PrependUnqualifiedPgTableName {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = PrependUnqualifiedPgTableNameVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

#[derive(Debug)]
pub struct FixArrayLiteral;

struct FixArrayLiteralVisitor;

impl FixArrayLiteralVisitor {
    fn is_string_type(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Text | DataType::Varchar(_) | DataType::Char(_) | DataType::String(_)
        )
    }
}

impl VisitorMut for FixArrayLiteralVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Cast {
            kind,
            expr,
            data_type,
            ..
        } = expr
        {
            if kind == &CastKind::DoubleColon {
                if let DataType::Array(arr) = data_type {
                    // cast some to
                    if let Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(array_literal),
                        ..
                    }) = expr.as_ref()
                    {
                        let items =
                            array_literal.trim_matches(|c| c == '{' || c == '}' || c == ' ');
                        let items = items.split(',').map(|s| s.trim()).filter(|s| !s.is_empty());

                        let is_text = match arr {
                            ArrayElemTypeDef::AngleBracket(dt) => Self::is_string_type(dt.as_ref()),
                            ArrayElemTypeDef::SquareBracket(dt, _) => {
                                Self::is_string_type(dt.as_ref())
                            }
                            ArrayElemTypeDef::Parenthesis(dt) => Self::is_string_type(dt.as_ref()),
                            _ => false,
                        };

                        let elems = items
                            .map(|s| {
                                if is_text {
                                    Expr::Value(
                                        Value::SingleQuotedString(s.to_string()).with_empty_span(),
                                    )
                                } else {
                                    Expr::Value(
                                        Value::Number(s.to_string(), false).with_empty_span(),
                                    )
                                }
                            })
                            .collect();
                        *expr = Box::new(Expr::Array(Array {
                            elem: elems,
                            named: true,
                        }));
                    }
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for FixArrayLiteral {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = FixArrayLiteralVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Remove qualifier from unsupported items
///
/// This rewriter removes qualifier from following items:
/// 1. type cast: for example: `pg_catalog.text`
/// 2. function name: for example: `pg_catalog.array_to_string`,
/// 3. table function name
#[derive(Debug)]
pub struct RemoveQualifier;

struct RemoveQualifierVisitor;

impl VisitorMut for RemoveQualifierVisitor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        // remove table function qualifier
        if let TableFactor::Table { name, args, .. } = table_factor {
            if args.is_some() {
                //  multiple idents in name, which means it's a qualified table name
                if name.0.len() > 1 {
                    if let Some(last_ident) = name.0.pop() {
                        *name = ObjectName(vec![last_ident]);
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Cast { data_type, .. } => {
                // rewrite custom pg_catalog. qualified types
                let data_type_str = data_type.to_string();
                match data_type_str.as_str() {
                    "pg_catalog.text" => {
                        *data_type = DataType::Text;
                    }
                    "pg_catalog.int2[]" => {
                        *data_type = DataType::Array(ArrayElemTypeDef::SquareBracket(
                            Box::new(DataType::Int16),
                            None,
                        ));
                    }
                    _ => {}
                }
            }
            Expr::Function(function) => {
                // remove qualifier from pg_catalog.function
                let name = &mut function.name;
                if name.0.len() > 1 {
                    if let Some(last_ident) = name.0.pop() {
                        *name = ObjectName(vec![last_ident]);
                    }
                }
            }

            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RemoveQualifier {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RemoveQualifierVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Replace `current_user` with `session_user()`
#[derive(Debug)]
pub struct CurrentUserVariableToSessionUserFunctionCall;

struct CurrentUserVariableToSessionUserFunctionCallVisitor;

impl VisitorMut for CurrentUserVariableToSessionUserFunctionCallVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Identifier(ident) = expr {
            if ident.quote_style.is_none() && ident.value.to_lowercase() == "current_user" {
                *expr = Expr::Function(Function {
                    name: ObjectName::from(vec![Ident::new("session_user")]),
                    args: FunctionArguments::None,
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                });
            }
        }

        if let Expr::Function(func) = expr {
            let fname = func
                .name
                .0
                .iter()
                .map(|ident| ident.to_string())
                .collect::<Vec<String>>()
                .join(".");
            if fname.to_lowercase() == "current_user" {
                func.name = ObjectName::from(vec![Ident::new("session_user")])
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for CurrentUserVariableToSessionUserFunctionCall {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = CurrentUserVariableToSessionUserFunctionCallVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Fix collate and regex calls
#[derive(Debug)]
pub struct FixCollate;

struct FixCollateVisitor;

impl VisitorMut for FixCollateVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Collate { expr: inner, .. } => {
                *expr = inner.as_ref().clone();
            }
            Expr::BinaryOp { op, .. } => {
                if let BinaryOperator::PGCustomBinaryOperator(ops) = op {
                    if *ops == ["pg_catalog", "~"] {
                        *op = BinaryOperator::PGRegexMatch;
                    }
                }
            }
            _ => {}
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for FixCollate {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = FixCollateVisitor;

        let _ = s.visit(&mut visitor);
        s
    }
}

/// Datafusion doesn't support subquery on projection
#[derive(Debug)]
pub struct RemoveSubqueryFromProjection;

struct RemoveSubqueryFromProjectionVisitor;

impl VisitorMut for RemoveSubqueryFromProjectionVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            for projection in &mut select.projection {
                match projection {
                    SelectItem::UnnamedExpr(expr) => {
                        if let Expr::Subquery(_) = expr {
                            *expr = Expr::Value(Value::Null.with_empty_span());
                        }
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        if let Expr::Subquery(_) = expr {
                            *expr = Expr::Value(Value::Null.with_empty_span());
                        }
                    }
                    _ => {}
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for RemoveSubqueryFromProjection {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = RemoveSubqueryFromProjectionVisitor;
        let _ = s.visit(&mut visitor);

        s
    }
}

/// `select version()` should return column named `version` not `version()`
#[derive(Debug)]
pub struct FixVersionColumnName;

struct FixVersionColumnNameVisitor;

impl VisitorMut for FixVersionColumnNameVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            for projection in &mut select.projection {
                if let SelectItem::UnnamedExpr(Expr::Function(f)) = projection {
                    if f.name.0.len() == 1 {
                        if let ObjectNamePart::Identifier(part) = &f.name.0[0] {
                            if part.value == "version" {
                                if let FunctionArguments::List(args) = &f.args {
                                    if args.args.is_empty() {
                                        *projection = SelectItem::ExprWithAlias {
                                            expr: Expr::Function(f.clone()),
                                            alias: Ident::new("version"),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl SqlStatementRewriteRule for FixVersionColumnName {
    fn rewrite(&self, mut s: Statement) -> Statement {
        let mut visitor = FixVersionColumnNameVisitor;
        let _ = s.visit(&mut visitor);

        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::sqlparser::parser::ParserError;
    use std::sync::Arc;

    fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let dialect = PostgreSqlDialect {};

        Parser::parse_sql(&dialect, sql)
    }

    fn rewrite(mut s: Statement, rules: &[Arc<dyn SqlStatementRewriteRule>]) -> Statement {
        for rule in rules {
            s = rule.rewrite(s);
        }

        s
    }

    macro_rules! assert_rewrite {
        ($rules:expr, $orig:expr, $rewt:expr) => {
            let sql = $orig;
            let statement = parse(sql).expect("Failed to parse").remove(0);

            let statement = rewrite(statement, $rules);
            assert_eq!(statement.to_string(), $rewt);
        };
    }

    #[test]
    fn test_alias_rewrite() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(AliasDuplicatedProjectionRewrite)];

        assert_rewrite!(
            &rules,
            "SELECT n.oid, n.* FROM pg_catalog.pg_namespace n",
            "SELECT n.oid AS __alias_oid, n.* FROM pg_catalog.pg_namespace AS n"
        );

        assert_rewrite!(
            &rules,
            "SELECT oid, * FROM pg_catalog.pg_namespace",
            "SELECT oid AS __alias_oid, * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT t1.oid, t2.* FROM tbl1 AS t1 JOIN tbl2 AS t2 ON t1.id = t2.id",
            "SELECT t1.oid, t2.* FROM tbl1 AS t1 JOIN tbl2 AS t2 ON t1.id = t2.id"
        );

        let sql = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace' ORDER BY nspsname";
        let statement = parse(sql).expect("Failed to parse").remove(0);

        let statement = rewrite(statement, &rules);
        assert_eq!(
            statement.to_string(),
            "SELECT n.oid AS __alias_oid, n.*, d.description FROM pg_catalog.pg_namespace AS n LEFT OUTER JOIN pg_catalog.pg_description AS d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY nspsname"
        );
    }

    #[test]
    fn test_qualifier_prepend() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(ResolveUnqualifiedIdentifer)];

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE nspname = 'pg_catalog' ORDER BY nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace AS n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname",
            "SELECT * FROM pg_catalog.pg_namespace ORDER BY nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace' ORDER BY nspsname",
            "SELECT n.oid, n.*, d.description FROM pg_catalog.pg_namespace AS n LEFT OUTER JOIN pg_catalog.pg_description AS d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY n.nspsname"
        );

        assert_rewrite!(&rules,
            "SELECT i.*,i.indkey as keys,c.relname,c.relnamespace,c.relam,c.reltablespace,tc.relname as tabrelname,dsc.description FROM pg_catalog.pg_index i INNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid INNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid WHERE i.indrelid=1 ORDER BY tabrelname, c.relname",
            "SELECT i.*, i.indkey AS keys, c.relname, c.relnamespace, c.relam, c.reltablespace, tc.relname AS tabrelname, dsc.description FROM pg_catalog.pg_index AS i INNER JOIN pg_catalog.pg_class AS c ON c.oid = i.indexrelid INNER JOIN pg_catalog.pg_class AS tc ON tc.oid = i.indrelid LEFT OUTER JOIN pg_catalog.pg_description AS dsc ON i.indexrelid = dsc.objoid WHERE i.indrelid = 1 ORDER BY tabrelname, c.relname"
        );
    }

    #[test]
    fn test_remove_unsupported_types() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![
            Arc::new(RemoveQualifier),
            Arc::new(RemoveUnsupportedTypes::new()),
        ];

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pg_catalog'::regclass ORDER BY n.nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace AS n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.oid = 1 AND n.nspname = 'pg_catalog'::regclass ORDER BY n.nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace AS n WHERE n.oid = 1 AND n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname",
            "SELECT n.oid, n.*, d.description FROM pg_catalog.pg_namespace AS n LEFT OUTER JOIN pg_catalog.pg_description AS d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace' ORDER BY nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT n.* FROM pg_catalog.pg_namespace n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname",
            "SELECT n.* FROM pg_catalog.pg_namespace AS n WHERE n.nspname = 'pg_catalog' ORDER BY n.nspname"
        );

        assert_rewrite!(
            &rules,
            "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
    FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
    LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
    WHERE c.oid = '16386'",
            "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::TEXT END, c.relpersistence, c.relreplident, am.amname FROM pg_catalog.pg_class AS c LEFT JOIN pg_catalog.pg_class AS tc ON (c.reltoastrelid = tc.oid) LEFT JOIN pg_catalog.pg_am AS am ON (c.relam = am.oid) WHERE c.oid = '16386'"
        );
    }

    #[test]
    fn test_any_to_array_contains() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RewriteArrayAnyAllOperation)];

        assert_rewrite!(
            &rules,
            "SELECT a = ANY(current_schemas(true))",
            "SELECT array_contains(current_schemas(true), a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a <> ALL(current_schemas(true))",
            "SELECT NOT array_contains(current_schemas(true), a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a = ANY('{r, l, e}')",
            "SELECT array_contains(ARRAY['r', 'l', 'e'], a)"
        );

        assert_rewrite!(
            &rules,
            "SELECT a FROM tbl WHERE a = ANY(current_schemas(true))",
            "SELECT a FROM tbl WHERE array_contains(current_schemas(true), a)"
        );
    }

    #[test]
    fn test_prepend_unqualified_table_name() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(PrependUnqualifiedPgTableName)];

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_namespace",
            "SELECT * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_namespace",
            "SELECT * FROM pg_catalog.pg_namespace"
        );

        assert_rewrite!(
            &rules,
            "SELECT typtype, typname, pg_type.oid FROM pg_catalog.pg_type LEFT JOIN pg_namespace as ns ON ns.oid = oid",
            "SELECT typtype, typname, pg_type.oid FROM pg_catalog.pg_type LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = oid"
        );
    }

    #[test]
    fn test_array_literal_fix() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(FixArrayLiteral)];

        assert_rewrite!(
            &rules,
            "SELECT '{a, abc}'::text[]",
            "SELECT ARRAY['a', 'abc']::TEXT[]"
        );

        assert_rewrite!(
            &rules,
            "SELECT '{1, 2}'::int[]",
            "SELECT ARRAY[1, 2]::INT[]"
        );

        assert_rewrite!(
            &rules,
            "SELECT '{t, f}'::bool[]",
            "SELECT ARRAY[t, f]::BOOL[]"
        );
    }

    #[test]
    fn test_remove_qualifier_from_table_function() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(RemoveQualifier)];

        assert_rewrite!(
            &rules,
            "SELECT * FROM pg_catalog.pg_get_keywords()",
            "SELECT * FROM pg_get_keywords()"
        );
    }

    #[test]
    fn test_current_user() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(CurrentUserVariableToSessionUserFunctionCall)];

        assert_rewrite!(&rules, "SELECT current_user", "SELECT session_user");

        assert_rewrite!(&rules, "SELECT CURRENT_USER", "SELECT session_user");

        assert_rewrite!(
            &rules,
            "SELECT is_null(current_user)",
            "SELECT is_null(session_user)"
        );
    }

    #[test]
    fn test_collate_fix() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(FixCollate)];

        assert_rewrite!(&rules, "SELECT c.oid, c.relname FROM pg_catalog.pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(tablename)$' COLLATE pg_catalog.default AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3;", "SELECT c.oid, c.relname FROM pg_catalog.pg_class AS c WHERE c.relname ~ '^(tablename)$' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3");
    }

    #[test]
    fn test_remove_subquery() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> =
            vec![Arc::new(RemoveSubqueryFromProjection)];

        assert_rewrite!(&rules,
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation LIMIT 1) AS attcollation, a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a WHERE a.attrelid = '16384' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum;",
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), NULL, a.attnotnull, NULL AS attcollation, a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = '16384' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum");
    }

    #[test]
    fn test_version_rewrite() {
        let rules: Vec<Arc<dyn SqlStatementRewriteRule>> = vec![Arc::new(FixVersionColumnName)];

        assert_rewrite!(&rules, "SELECT version()", "SELECT version() AS version");

        // Make sure we don't rewrite things we should leave alone
        assert_rewrite!(&rules, "SELECT version() as foo", "SELECT version() AS foo");
        assert_rewrite!(&rules, "SELECT version(foo)", "SELECT version(foo)");
        assert_rewrite!(&rules, "SELECT foo.version()", "SELECT foo.version()");
    }
}
