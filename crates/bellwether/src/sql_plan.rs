//! SQL `SELECT` queries translated into [`ConsumerPlan`] DAGs (`WP15b`).
//!
//! Agents and pipeline authors read and write SQL, while relational replay
//! prices typed [`ConsumerPlan`] DAGs (`bellwether_replay::relational`). This
//! module parses a SQL `SELECT` query into a `sqlparser` AST and translates
//! it deterministically into a `ConsumerPlan`, without ever executing the
//! SQL string.
//!
//! # Supported SQL vocabulary
//!
//! - Common table expressions (`WITH t AS (...)`): each CTE is translated
//!   once; a CTE referenced by two or more consumers becomes a shared node
//!   in [`ConsumerPlan::nodes`] (`consumers[i] >= 2`, evaluated once under
//!   logical reuse).
//! - `FROM table [AS alias]` and derived subqueries `(SELECT ...) [AS alias]`.
//!   A single scanned table becomes dataset `"main"` when `main_table` is not
//!   set or names it; otherwise `main_table` (or `"main"`) maps to `"main"`
//!   and every other table maps to its SQL identifier.
//! - `JOIN ... ON` / `USING`: `INNER JOIN`, `LEFT [OUTER] JOIN`
//!   ([`PlanNode::Join`]), and `LEFT SEMI JOIN` / `SEMI JOIN`
//!   ([`PlanNode::SemiJoin`]). Joining against the virtual table `keys`
//!   (`JOIN keys ON t.col = keys.col` or `JOIN keys USING (col)`) produces a
//!   [`PlanNode::SemiJoin`] with [`KeysFrom::Run`].
//! - `WHERE` predicates:
//!   - `col IN (:keys)` / `col IN ($keys)` / `col IN ($1)` / `col IN (?)`
//!     / `(a, b) IN (:keys)` -> [`PlanNode::SemiJoin`] (or [`Predicate::SemiJoin`])
//!     with [`KeysFrom::Run`];
//!   - `col IN (SELECT k FROM keys)` or `EXISTS (SELECT 1 FROM keys WHERE ...)`
//!     -> [`KeysFrom::Run`];
//!   - `col IN (SELECT other_col FROM other_table ...)` or `EXISTS (...)`
//!     -> [`PlanNode::SemiJoin`] with [`KeysFrom::Node`];
//!   - `col = val` / `col IN (v1, v2, ...)` -> [`Predicate::In`] (canonical-encoded
//!     against the dataset schema when available, or [`KeysFrom::Run`] when
//!     `keys_from_run` is set);
//!   - `col BETWEEN lo AND hi` (or `col >= lo AND col <= hi`) ->
//!     [`Predicate::Between`];
//!   - conjunctions `p AND q` -> [`Predicate::And`].
//! - `QUALIFY row_number() OVER (PARTITION BY k1, ... ORDER BY ts DESC) = 1`
//!   (or a subquery filtering `rn = 1` on a `row_number()` column, or
//!   `arg_max(col, ts)` grouped by keys) -> [`PlanNode::Latest`] (or
//!   [`PlanNode::AsOf`] when `as_of` is set or `ts <= :at` / `ts <= $at`
//!   bounds the version column).
//! - Window functions `fn(...) OVER (PARTITION BY k1, ...)` ->
//!   [`PlanNode::Window`] (with `keys: Some(KeysFrom::Run)` when the
//!   partition columns are restricted to the run's keys).
//! - `GROUP BY k1, ...` with `COUNT(*)`, `COUNT(col)`, `SUM(col)`,
//!   `MIN(col)`, `MAX(col)` -> [`PlanNode::Aggregate`].
//! - `SELECT col1, col2, ...` -> [`PlanNode::Project`].
//!
//! # Refusals
//!
//! Any SQL construct outside [`ConsumerPlan`]'s closed operator vocabulary
//! (`UNION`, `INTERSECT`, `EXCEPT`, `HAVING`, `ORDER BY` without a window or
//! latest-per-key, `LIMIT`, `OFFSET`, `DISTINCT`, `RIGHT`/`FULL`/`CROSS`
//! joins, `OR`, `NOT`, `IS NULL`, non-equi joins, correlated subqueries,
//! unsupported functions) is refused by name with [`Error::InvalidArgument`]
//! (`field: "sql"`), stating the exact clause and what `ConsumerPlan`
//! supports instead—never silently dropped.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use bellwether_core::encoding::{self, Value as EncValue};
use bellwether_replay::{check_shape, MAIN_DATASET, PLAN_VERSION};
use bellwether_types::{
    AggregateFn, ConsumerPlan, Error, Hash, JoinKind, KeysFrom, Measure, PlanNode, Predicate,
    Result,
};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    BinaryOperator, Cte, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, NamedWindowDefinition,
    NamedWindowExpr, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, UnaryOperator, Value as SqlValue, WindowSpec, WindowType,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Virtual table name that represents the recorded run's key set (`KeysFrom::Run`).
pub const KEYS_TABLE: &str = "keys";

/// Options for translating a SQL query into a [`ConsumerPlan`].
#[derive(Clone, Debug, Default)]
pub struct PlanFromSqlOptions {
    /// Table name in the SQL query that maps to the `"main"` dataset.
    /// When `None` and the query scans only one dataset (besides `keys`),
    /// that table becomes `"main"`.
    pub main_table: Option<String>,
    /// Map lateral/literal `IN (...)` or `=` key predicates to `KeysFrom::Run`.
    pub keys_from_run: bool,
    /// Translate latest-per-key patterns (`QUALIFY row_number() ... = 1`,
    /// `arg_max`) into [`PlanNode::AsOf`] instead of [`PlanNode::Latest`].
    pub as_of: bool,
    /// Optional dataset schemas (keyed by dataset name, `"main"` for the root
    /// dataset) used to expand `SELECT *` and encode literal values at exact
    /// dataset types.
    pub schemas: BTreeMap<String, SchemaRef>,
}

/// Request to [`crate::Engine::plan_from_sql`].
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanFromSqlRequest {
    /// The SQL `SELECT` query text.
    pub sql: String,
    /// The SQL table name that maps to `"main"` (optional; inferred when the
    /// query scans a single table).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_table: Option<String>,
    /// Map literal `IN (...)` predicates to `KeysFrom::Run`.
    #[serde(default)]
    pub keys_from_run: bool,
    /// Translate latest-per-key (`QUALIFY row_number() ... = 1` or `arg_max`)
    /// into `PlanNode::AsOf` instead of `PlanNode::Latest`.
    #[serde(default)]
    pub as_of: bool,
    /// Validate the resulting plan against the engine's bound datasets.
    #[serde(default = "default_true")]
    pub validate: bool,
    /// Store the resulting plan in the engine's cache when writable.
    #[serde(default = "default_true")]
    pub cache: bool,
}

fn default_true() -> bool {
    true
}

/// Result of [`crate::Engine::plan_from_sql`], `bellwether plan from-sql --json`,
/// and MCP `observe.plan_from_sql`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanFromSqlResult {
    /// BLAKE3 hash of the canonical JSON of `plan`.
    pub hash: Hash,
    /// Whether `plan` was stored in the index's cache (`cache/<hash>.bin`).
    pub cached: bool,
    /// Dataset names scanned by `plan`, sorted (`"main"` first when present).
    pub datasets: Vec<String>,
    /// The translated consumer plan.
    pub plan: ConsumerPlan,
}

fn refuse(what: impl std::fmt::Display, supported: impl std::fmt::Display) -> Error {
    Error::InvalidArgument {
        field: "sql",
        reason: format!(
            "SQL {what} is not in ConsumerPlan's operator vocabulary; ConsumerPlan supports {supported}"
        ),
    }
}

fn syntax_err(msg: impl std::fmt::Display) -> Error {
    Error::InvalidArgument {
        field: "sql",
        reason: format!("cannot parse SQL query: {msg}"),
    }
}

/// Translates a SQL `SELECT` query into a [`ConsumerPlan`] DAG.
///
/// Never executes the SQL string. Refuses any SQL construct outside
/// `ConsumerPlan`'s closed operator vocabulary by name with
/// [`Error::InvalidArgument`] (`field: "sql"`).
pub fn plan_from_sql(sql: &str, opts: &PlanFromSqlOptions) -> Result<ConsumerPlan> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidArgument {
            field: "sql",
            reason: "the SQL query is empty; pass a SELECT query".into(),
        });
    }
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, trimmed).map_err(syntax_err)?;
    if stmts.is_empty() {
        return Err(Error::InvalidArgument {
            field: "sql",
            reason: "the SQL query contains no statement; pass one SELECT query".into(),
        });
    }
    if stmts.len() > 1 {
        return Err(refuse(
            format!("multiple statements ({})", stmts.len()),
            "a single SELECT query (use `propose --before-plan` and `--after-plan` for two queries)",
        ));
    }
    let stmt = &stmts[0];
    let Statement::Query(query) = stmt else {
        let name = stmt_name(stmt);
        return Err(refuse(
            format!("statement `{name}`"),
            "a read-only `SELECT` query (with optional `WITH` CTEs)",
        ));
    };

    // Determine the set of base tables scanned across the whole query so we
    // can map the single scanned table (or `opts.main_table`) to `"main"`.
    let mut cte_names = BTreeSet::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            cte_names.insert(ident_str(&cte.alias.name));
        }
    }
    let mut base_tables = BTreeSet::new();
    collect_query_tables(query, &cte_names, &mut base_tables);
    base_tables.remove(KEYS_TABLE);

    let main_table = resolve_main_table(opts.main_table.as_deref(), &base_tables)?;

    let mut builder = PlanBuilder {
        opts,
        main_table,
        nodes: Vec::new(),
        ctes: BTreeMap::new(),
        cte_defs: BTreeMap::new(),
    };
    let root = builder.translate_query(query)?;
    builder.finalize(&root)
}

fn stmt_name(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Query(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::Drop { .. } => "DROP",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Truncate { .. } => "TRUNCATE",
        Statement::Merge { .. } => "MERGE",
        Statement::Copy { .. } => "COPY",
        Statement::Explain { .. } | Statement::ExplainTable { .. } => "EXPLAIN",
        _ => "non-SELECT statement",
    }
}

fn resolve_main_table(
    explicit: Option<&str>,
    base_tables: &BTreeSet<String>,
) -> Result<Option<String>> {
    if let Some(m) = explicit {
        let m = m.trim();
        if m.is_empty() {
            return Err(Error::InvalidArgument {
                field: "sql",
                reason: "main_table must not be empty".into(),
            });
        }
        return Ok(Some(m.to_string()));
    }
    if base_tables.contains(MAIN_DATASET) {
        return Ok(Some(MAIN_DATASET.to_string()));
    }
    if base_tables.len() == 1 {
        return Ok(base_tables.iter().next().cloned());
    }
    Ok(None)
}

fn collect_query_tables(query: &Query, cte_names: &BTreeSet<String>, out: &mut BTreeSet<String>) {
    if let Some(with) = &query.with {
        let mut local_ctes = cte_names.clone();
        for cte in &with.cte_tables {
            collect_query_tables(&cte.query, &local_ctes, out);
            local_ctes.insert(ident_str(&cte.alias.name));
        }
    }
    collect_set_expr_tables(&query.body, cte_names, out);
}

fn collect_set_expr_tables(
    body: &SetExpr,
    cte_names: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    match body {
        SetExpr::Select(sel) => {
            for twj in &sel.from {
                collect_table_factor_tables(&twj.relation, cte_names, out);
                for j in &twj.joins {
                    collect_table_factor_tables(&j.relation, cte_names, out);
                }
            }
            if let Some(selection) = &sel.selection {
                collect_expr_tables(selection, cte_names, out);
            }
        }
        SetExpr::Query(q) => collect_query_tables(q, cte_names, out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_tables(left, cte_names, out);
            collect_set_expr_tables(right, cte_names, out);
        }
        _ => {}
    }
}

fn collect_table_factor_tables(
    tf: &TableFactor,
    cte_names: &BTreeSet<String>,
    out: &mut BTreeSet<String>,
) {
    match tf {
        TableFactor::Table { name, .. } => {
            let raw = object_table_name(name);
            if !cte_names.contains(&raw) && !raw.eq_ignore_ascii_case(KEYS_TABLE) {
                out.insert(raw);
            }
        }
        TableFactor::Derived { subquery, .. } => collect_query_tables(subquery, cte_names, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_factor_tables(&table_with_joins.relation, cte_names, out);
            for j in &table_with_joins.joins {
                collect_table_factor_tables(&j.relation, cte_names, out);
            }
        }
        _ => {}
    }
}

fn collect_expr_tables(expr: &Expr, cte_names: &BTreeSet<String>, out: &mut BTreeSet<String>) {
    match expr {
        Expr::InSubquery { subquery, .. }
        | Expr::Exists { subquery, .. }
        | Expr::Subquery(subquery) => collect_query_tables(subquery, cte_names, out),
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_tables(left, cte_names, out);
            collect_expr_tables(right, cte_names, out);
        }
        Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
            collect_expr_tables(inner, cte_names, out);
        }
        _ => {}
    }
}

fn ident_str(ident: &Ident) -> String {
    ident.value.clone()
}

fn object_table_name(name: &ObjectName) -> String {
    name.0.last().map(|i| i.value.clone()).unwrap_or_default()
}

/// Pending `row_number() OVER (PARTITION BY ... ORDER BY ts DESC) AS alias` from
/// a subquery or current SELECT that may be turned into `Latest`/`AsOf` if an
/// outer query filters `WHERE alias = 1`.
#[derive(Clone, Debug)]
struct PendingRowNumber {
    alias: String,
    part_cols: Vec<String>,
    ts_col: String,
    as_of: bool,
}
#[derive(Clone, Debug)]
struct ScanOrigin {
    node: u32,
    dataset: String,
    qualifier: String,
}

/// Information about a translated relation (table scan, CTE, subquery, join).
#[derive(Clone, Debug)]
struct RelOut {
    /// Node index in `PlanBuilder::nodes`.
    node: u32,
    /// Output columns in order (`(qualifier_or_none, column_name, dataset_if_scan)`).
    columns: Vec<ColInfo>,
    /// Direct scans contributing to this relation (used to resolve columns when
    /// schema is not provided upfront).
    scan_origins: Vec<ScanOrigin>,
    /// Columns restricted by `KeysFrom::Run` on this relation.
    run_key_cols: Option<Vec<String>>,
    /// Optional pending `row_number()` window from a subquery.
    pending_rn: Option<PendingRowNumber>,
}

#[derive(Clone, Debug)]
struct ColInfo {
    /// Optional table alias or table name for qualified references (`e.repo_id`).
    qualifier: Option<String>,
    /// Additional table aliases that merged into this column (e.g. from equi-join `ON e.id = r.id`).
    alt_qualifiers: Vec<String>,
    /// Output column name as seen by parent operators.
    name: String,
    /// Underlying dataset name (`"main"`, `"repos"`, ...) if known.
    dataset: Option<String>,
    /// Underlying column name in that dataset.
    source_col: String,
    /// Scan node index that supplies this column.
    scan_node: Option<u32>,
}

impl RelOut {
    fn find_col(&self, qualifier: Option<&str>, col_name: &str) -> Result<&ColInfo> {
        let mut matches = self.columns.iter().filter(|c| {
            c.name.eq_ignore_ascii_case(col_name)
                && match qualifier {
                    Some(q) => {
                        c.qualifier
                            .as_deref()
                            .is_some_and(|cq| cq.eq_ignore_ascii_case(q))
                            || c.alt_qualifiers.iter().any(|aq| aq.eq_ignore_ascii_case(q))
                    }
                    None => true,
                }
        });
        match (matches.next(), matches.next()) {
            (Some(c), None) => Ok(c),
            (Some(_), Some(_)) => Err(Error::InvalidArgument {
                field: "sql",
                reason: format!(
                    "column reference {col_name:?} is ambiguous across joined relations; qualify \
                     it with a table name or alias"
                ),
            }),
            (None, _) => {
                let avail: Vec<String> = self
                    .columns
                    .iter()
                    .map(|c| match &c.qualifier {
                        Some(q) => format!("{q}.{}", c.name),
                        None => c.name.clone(),
                    })
                    .collect();
                let shown = match qualifier {
                    Some(q) => format!("{q}.{col_name}"),
                    None => col_name.to_string(),
                };
                Err(Error::InvalidArgument {
                    field: "sql",
                    reason: format!(
                        "column {shown:?} is not available in this scope{}",
                        if avail.is_empty() {
                            String::new()
                        } else {
                            format!(" (available: {})", avail.join(", "))
                        }
                    ),
                })
            }
        }
    }
}
struct PlanBuilder<'a> {
    opts: &'a PlanFromSqlOptions,
    main_table: Option<String>,
    nodes: Vec<PlanNode>,
    /// Translated CTEs by name (built lazily or on definition so that a CTE
    /// referenced multiple times shares the same node index!).
    ctes: BTreeMap<String, RelOut>,
    /// Raw CTE AST definitions in scope.
    cte_defs: BTreeMap<String, Cte>,
}

impl PlanBuilder<'_> {
    fn push_node(&mut self, node: PlanNode) -> u32 {
        let idx = u32::try_from(self.nodes.len()).unwrap_or(u32::MAX);
        self.nodes.push(node);
        idx
    }

    fn dataset_name(&self, raw_table: &str) -> String {
        if raw_table == MAIN_DATASET {
            return MAIN_DATASET.to_string();
        }
        if let Some(m) = &self.main_table {
            if raw_table.eq_ignore_ascii_case(m) {
                return MAIN_DATASET.to_string();
            }
        }
        raw_table.to_string()
    }

    fn schema_for(&self, dataset: &str) -> Option<&SchemaRef> {
        self.opts.schemas.get(dataset)
    }

    fn column_data_type(&self, rel: &RelOut, col_name: &str) -> Option<DataType> {
        let info = rel.find_col(None, col_name).ok()?;
        let ds = info.dataset.as_deref()?;
        let schema = self.schema_for(ds)?;
        let field = schema.field_with_name(&info.source_col).ok()?;
        Some(field.data_type().clone())
    }

    fn translate_query(&mut self, query: &Query) -> Result<RelOut> {
        if let Some(limit) = &query.limit {
            return Err(refuse(
                format!("`LIMIT {limit}`"),
                "`QUALIFY row_number() OVER (PARTITION BY <key> ORDER BY <ts> DESC) = 1` for latest-per-key, or key/range predicates (`WHERE col IN (:keys)`, `WHERE col BETWEEN lo AND hi`)",
            ));
        }
        if !query.limit_by.is_empty() {
            return Err(refuse(
                "`LIMIT ... BY`",
                "`QUALIFY row_number() OVER (PARTITION BY <key> ORDER BY <ts> DESC) = 1` (`PlanNode::Latest`)",
            ));
        }
        if let Some(offset) = &query.offset {
            return Err(refuse(
                format!("`{offset}`"),
                "key and range filters (`WHERE col IN (...)`, `WHERE col BETWEEN ...`)",
            ));
        }
        if let Some(fetch) = &query.fetch {
            return Err(refuse(
                format!("`{fetch}`"),
                "`QUALIFY row_number() OVER (PARTITION BY <key> ORDER BY <ts> DESC) = 1` (`PlanNode::Latest`)",
            ));
        }
        if !query.locks.is_empty() {
            return Err(refuse(
                "`FOR UPDATE` / lock clause",
                "read-only SELECT operators",
            ));
        }
        if query.settings.is_some() {
            return Err(refuse("`SETTINGS` clause", "standard SELECT operators"));
        }
        if let Some(order_by) = &query.order_by {
            if !order_by.exprs.is_empty() {
                return Err(refuse(
                    format!("top-level `{order_by}`"),
                    "`OVER (PARTITION BY ... ORDER BY ...)` for window/latest operators (`ConsumerPlan` relations are unordered sets/multisets)",
                ));
            }
        }

        let saved_defs = self.cte_defs.clone();
        let saved_ctes = self.ctes.clone();
        if let Some(with) = &query.with {
            if with.recursive {
                return Err(refuse(
                    "`WITH RECURSIVE`",
                    "non-recursive CTEs (`WITH name AS (SELECT ...)`), which become DAG nodes in `ConsumerPlan`",
                ));
            }
            for cte in &with.cte_tables {
                let name = ident_str(&cte.alias.name);
                if self.cte_defs.contains_key(&name) {
                    return Err(Error::InvalidArgument {
                        field: "sql",
                        reason: format!("CTE {name:?} is defined twice in the same WITH clause"),
                    });
                }
                self.cte_defs.insert(name, cte.clone());
            }
        }

        let res = self.translate_set_expr(&query.body);
        self.cte_defs = saved_defs;
        self.ctes = saved_ctes;
        res
    }

    fn translate_set_expr(&mut self, body: &SetExpr) -> Result<RelOut> {
        match body {
            SetExpr::Select(sel) => self.translate_select(sel),
            SetExpr::Query(q) => self.translate_query(q),
            SetExpr::SetOperation { op, .. } => Err(refuse(
                format!("`{op}` set operation"),
                "a single SELECT query (use `JOIN`, `SEMI JOIN`, or a shared CTE instead of `UNION`/`INTERSECT`/`EXCEPT`)",
            )),
            SetExpr::Values(_) => Err(refuse(
                "`VALUES` clause",
                "`WHERE col IN (...)` for literal key sets or `FROM <table>` for dataset scans",
            )),
            other => Err(refuse(
                format!("`{other}`"),
                "`SELECT ... FROM ...`",
            )),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn translate_select(&mut self, sel: &Select) -> Result<RelOut> {
        if let Some(distinct) = &sel.distinct {
            return Err(refuse(
                format!("`SELECT {distinct}`"),
                "`GROUP BY <columns>` with an aggregate (such as `COUNT(*)`) or `QUALIFY row_number() OVER (PARTITION BY <columns> ORDER BY <ts> DESC) = 1`",
            ));
        }
        if let Some(top) = &sel.top {
            return Err(refuse(
                format!("`SELECT {top}`"),
                "`QUALIFY row_number() OVER (PARTITION BY <key> ORDER BY <ts> DESC) = 1`",
            ));
        }
        if sel.into.is_some() {
            return Err(refuse("`SELECT INTO`", "read-only `SELECT` queries"));
        }
        if !sel.lateral_views.is_empty() {
            return Err(refuse(
                "`LATERAL VIEW`",
                "standard `FROM` and `JOIN` clauses",
            ));
        }
        if sel.prewhere.is_some() {
            return Err(refuse("`PREWHERE`", "`WHERE` predicates"));
        }
        if !sel.cluster_by.is_empty() {
            return Err(refuse(
                "`CLUSTER BY`",
                "`GROUP BY` or `OVER (PARTITION BY ...)`",
            ));
        }
        if !sel.sort_by.is_empty() {
            return Err(refuse(
                "`SORT BY`",
                "`OVER (PARTITION BY ... ORDER BY ...)`",
            ));
        }
        if let Some(having) = &sel.having {
            return Err(refuse(
                format!("`HAVING {having}`"),
                "filtering before aggregation in `WHERE` (`PlanNode::Filter` / `PlanNode::SemiJoin`); `ConsumerPlan` does not price post-aggregation `HAVING` filters",
            ));
        }
        if sel.connect_by.is_some() {
            return Err(refuse("`CONNECT BY`", "standard `JOIN`s and CTEs"));
        }
        if sel.from.is_empty() {
            return Err(refuse(
                "`SELECT` without a `FROM` clause",
                "`SELECT ... FROM <table>`",
            ));
        }

        // 1. Translate FROM and JOINs
        let mut rel = self.translate_from(&sel.from)?;

        // 2. Translate WHERE selection (if any)
        let mut as_of_from_where = false;
        if let Some(where_expr) = &sel.selection {
            if let Some(rn_col) = match_rn_eq_one(where_expr) {
                if let Some(pending) = self.take_pending_row_number(&mut rel, &rn_col)? {
                    rel = self.apply_latest_or_asof(
                        rel,
                        pending.part_cols,
                        pending.ts_col,
                        self.opts.as_of || pending.as_of,
                    )?;
                } else {
                    rel = self.apply_where(rel, where_expr, &mut as_of_from_where)?;
                }
            } else {
                rel = self.apply_where(rel, where_expr, &mut as_of_from_where)?;
            }
        }

        // 3. Check for GROUP BY / Aggregate (including `arg_max(col, ts)` -> Latest/AsOf)
        let group_exprs = match &sel.group_by {
            GroupByExpr::All(_) => {
                return Err(refuse(
                    "`GROUP BY ALL`",
                    "explicit `GROUP BY col1, col2, ...`",
                ))
            }
            GroupByExpr::Expressions(exprs, modifiers) => {
                if !modifiers.is_empty() {
                    return Err(refuse(
                        "`GROUP BY` with `ROLLUP`/`CUBE`/`GROUPING SETS`",
                        "plain `GROUP BY col1, col2, ...`",
                    ));
                }
                exprs
            }
        };

        let has_agg_in_select = select_has_aggregates(&sel.projection);
        let arg_max_ts = self.detect_arg_max(&sel.projection, group_exprs, &rel)?;

        if let Some((key_cols, ts_col, projected_cols)) = arg_max_ts {
            rel = self.apply_latest_or_asof(
                rel,
                key_cols,
                ts_col,
                self.opts.as_of || as_of_from_where,
            )?;
            if !sel.distribute_by.is_empty() {
                rel = self.apply_exchange(rel, &sel.distribute_by)?;
            }
            return self.apply_project_cols(rel, &projected_cols);
        }

        if !group_exprs.is_empty() || has_agg_in_select {
            rel = self.apply_aggregate(rel, group_exprs, &sel.projection)?;
            if !sel.distribute_by.is_empty() {
                rel = self.apply_exchange(rel, &sel.distribute_by)?;
            }
            if let Some(qualify) = &sel.qualify {
                return Err(refuse(
                    format!("`QUALIFY {qualify}` together with `GROUP BY`"),
                    "`QUALIFY row_number() OVER (...) = 1` over a scan or filter without `GROUP BY`",
                ));
            }
            return Ok(rel);
        }

        // 4. Check for QUALIFY (e.g. `QUALIFY row_number() OVER (PARTITION BY id ORDER BY ts DESC) = 1`)
        if let Some(qualify) = &sel.qualify {
            let (part_cols, ts_col, is_asof) =
                self.parse_qualify_latest(&rel, qualify, &sel.named_window)?;
            rel = self.apply_latest_or_asof(
                rel,
                part_cols,
                ts_col,
                self.opts.as_of || as_of_from_where || is_asof,
            )?;
        }

        // 5. Check for Window functions in SELECT projection (`OVER (PARTITION BY ...)`)
        let windows = self.extract_select_windows(&rel, &sel.projection, &sel.named_window)?;
        for win in windows {
            match win {
                WindowExtract::Latest {
                    part_cols,
                    ts_col,
                    as_of,
                    alias,
                } => {
                    if sel.qualify.is_none() {
                        if let Some(alias) = alias {
                            rel = self.apply_window(rel, part_cols.clone())?;
                            rel.pending_rn = Some(PendingRowNumber {
                                alias,
                                part_cols,
                                ts_col,
                                as_of: self.opts.as_of || as_of_from_where || as_of,
                            });
                        } else {
                            rel = self.apply_window(rel, part_cols)?;
                        }
                    }
                }
                WindowExtract::Partition { part_cols } => {
                    rel = self.apply_window(rel, part_cols)?;
                }
            }
        }

        // 6. DISTRIBUTE BY -> PlanNode::Exchange
        if !sel.distribute_by.is_empty() {
            rel = self.apply_exchange(rel, &sel.distribute_by)?;
        }

        // 7. SELECT projection -> PlanNode::Project
        self.apply_select_projection(rel, &sel.projection, &sel.named_window)
    }

    fn record_scan_col_ref(&mut self, scan_idx: Option<u32>, col_name: &str) {
        let Some(idx) = scan_idx else { return };
        let Some(PlanNode::Scan { dataset, columns }) = self.nodes.get_mut(idx as usize) else {
            return;
        };
        if col_name.is_empty() || columns.iter().any(|c| c == col_name) {
            return;
        }
        let ds = dataset.clone();
        columns.push(col_name.to_string());
        if let Some(schema) = self.opts.schemas.get(&ds) {
            let order: BTreeMap<&str, usize> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| (f.name().as_str(), i))
                .collect();
            columns.sort_by_key(|c| order.get(c.as_str()).copied().unwrap_or(usize::MAX));
        }
    }

    fn translate_from(&mut self, from: &[TableWithJoins]) -> Result<RelOut> {
        if from.is_empty() {
            return Err(refuse(
                "`SELECT` without a `FROM` clause",
                "`SELECT ... FROM <table>`",
            ));
        }
        if from.len() > 1 {
            return Err(refuse(
                "comma-separated tables in `FROM` (implicit cross join)",
                "explicit `INNER JOIN ... ON ...`, `LEFT JOIN ... ON ...`, or `SEMI JOIN ... ON ...`",
            ));
        }
        let twj = &from[0];
        let mut rel = self.translate_table_factor(&twj.relation)?;
        for join in &twj.joins {
            rel = self.translate_join(rel, &join.relation, &join.join_operator)?;
        }
        Ok(rel)
    }

    #[allow(clippy::too_many_lines)]
    fn translate_table_factor(&mut self, tf: &TableFactor) -> Result<RelOut> {
        match tf {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if args.is_some() {
                    return Err(refuse(
                        format!("table-valued function `{name}(...)`"),
                        "plain dataset table names or CTEs in `FROM`",
                    ));
                }
                let raw_name = object_table_name(name);
                let qualifier = alias
                    .as_ref()
                    .map_or_else(|| raw_name.clone(), |a| ident_str(&a.name));
                if raw_name.eq_ignore_ascii_case(KEYS_TABLE)
                    && !self.cte_defs.contains_key(&raw_name)
                {
                    return Err(refuse(
                        "`FROM keys` as the primary scanned table",
                        "scanning a dataset table in `FROM` and filtering/joining against `keys` (`WHERE col IN (:keys)` or `JOIN keys ON ...`)",
                    ));
                }
                // Check if `raw_name` is a CTE in scope
                if self.cte_defs.contains_key(&raw_name) {
                    let cte_rel = if let Some(existing) = self.ctes.get(&raw_name) {
                        existing.clone()
                    } else {
                        let cte_def = self
                            .cte_defs
                            .get(&raw_name)
                            .cloned()
                            .ok_or_else(|| Error::Other(format!("missing CTE {raw_name}")))?;
                        let mut built = self.translate_query(&cte_def.query)?;
                        if !cte_def.alias.columns.is_empty() {
                            if cte_def.alias.columns.len() != built.columns.len() {
                                return Err(Error::InvalidArgument {
                                    field: "sql",
                                    reason: format!(
                                        "CTE {raw_name:?} declares {} column alias(es), but its query produces {} column(s)",
                                        cte_def.alias.columns.len(),
                                        built.columns.len()
                                    ),
                                });
                            }
                            for (col, col_alias) in
                                built.columns.iter_mut().zip(&cte_def.alias.columns)
                            {
                                col.name = ident_str(col_alias);
                            }
                        }
                        self.ctes.insert(raw_name.clone(), built.clone());
                        built
                    };
                    let mut out = cte_rel;
                    for c in &mut out.columns {
                        c.qualifier = Some(qualifier.clone());
                        c.alt_qualifiers.clear();
                    }
                    return Ok(out);
                }

                let ds = self.dataset_name(&raw_name);
                let initial_cols: Vec<String> = self
                    .schema_for(&ds)
                    .map(|s| s.fields().iter().map(|f| f.name().clone()).collect())
                    .unwrap_or_default();
                let scan_idx = self.push_node(PlanNode::Scan {
                    dataset: ds.clone(),
                    columns: Vec::new(),
                });
                let columns = initial_cols
                    .into_iter()
                    .map(|col_name| ColInfo {
                        qualifier: Some(qualifier.clone()),
                        alt_qualifiers: Vec::new(),
                        name: col_name.clone(),
                        dataset: Some(ds.clone()),
                        source_col: col_name,
                        scan_node: Some(scan_idx),
                    })
                    .collect();
                Ok(RelOut {
                    node: scan_idx,
                    columns,
                    scan_origins: vec![ScanOrigin {
                        node: scan_idx,
                        dataset: ds,
                        qualifier,
                    }],
                    run_key_cols: None,
                    pending_rn: None,
                })
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    return Err(refuse(
                        "`LATERAL` subquery",
                        "non-lateral subqueries, CTEs, and `JOIN ... ON`",
                    ));
                }
                let mut out = self.translate_query(subquery)?;
                if let Some(a) = alias {
                    let q = ident_str(&a.name);
                    if !a.columns.is_empty() {
                        if a.columns.len() != out.columns.len() {
                            return Err(Error::InvalidArgument {
                                field: "sql",
                                reason: format!(
                                    "subquery alias {q:?} declares {} column(s), but the subquery outputs {} column(s)",
                                    a.columns.len(),
                                    out.columns.len()
                                ),
                            });
                        }
                        for (c, ca) in out.columns.iter_mut().zip(&a.columns) {
                            c.name = ident_str(ca);
                        }
                    }
                    for c in &mut out.columns {
                        c.qualifier = Some(q.clone());
                        c.alt_qualifiers.clear();
                    }
                    out.scan_origins.clear();
                }
                Ok(out)
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.translate_from(std::slice::from_ref(table_with_joins.as_ref())),
            other => Err(refuse(
                format!("table factor `{other}`"),
                "table scans (`FROM table`), CTEs, or derived subqueries (`FROM (SELECT ...)`)",
            )),
        }
    }

    fn ensure_col_on_scan_rel(
        &mut self,
        rel: &mut RelOut,
        qualifier: Option<&str>,
        col_name: &str,
    ) -> Result<ColInfo> {
        if let Ok(c) = rel.find_col(qualifier, col_name) {
            let c = c.clone();
            self.record_scan_col_ref(c.scan_node, &c.source_col);
            return Ok(c);
        }
        if !rel.scan_origins.is_empty() {
            let mut matching = rel.scan_origins.iter().filter(|so| match qualifier {
                Some(q) => {
                    q.eq_ignore_ascii_case(&so.qualifier) || q.eq_ignore_ascii_case(&so.dataset)
                }
                None => true,
            });
            if let (Some(so), None) = (matching.next(), matching.next()) {
                let scan_idx = so.node;
                let ds = so.dataset.clone();
                let qual = so.qualifier.clone();
                if let Some(schema) = self.schema_for(&ds) {
                    if schema.field_with_name(col_name).is_err() {
                        return Err(Error::InvalidArgument {
                            field: "sql",
                            reason: format!(
                                "scans dataset {ds}: {}",
                                encoding::not_in_schema(schema, col_name)
                            ),
                        });
                    }
                }
                let info = ColInfo {
                    qualifier: Some(qual),
                    alt_qualifiers: Vec::new(),
                    name: col_name.to_string(),
                    dataset: Some(ds),
                    source_col: col_name.to_string(),
                    scan_node: Some(scan_idx),
                };
                self.record_scan_col_ref(Some(scan_idx), col_name);
                rel.columns.push(info.clone());
                return Ok(info);
            }
        }
        rel.find_col(qualifier, col_name).cloned()
    }
    fn resolve_col_mut(&mut self, rel: &mut RelOut, expr: &Expr) -> Result<ColInfo> {
        match expr {
            Expr::Identifier(ident) => self.ensure_col_on_scan_rel(rel, None, &ident.value),
            Expr::CompoundIdentifier(parts) => match parts.as_slice() {
                [qual, col] | [_, qual, col] => {
                    self.ensure_col_on_scan_rel(rel, Some(&qual.value), &col.value)
                }
                _ => Err(refuse(
                    format!("column expression `{expr}`"),
                    "column references (`col` or `table.col`)",
                )),
            },
            Expr::Nested(inner) => self.resolve_col_mut(rel, inner),
            _ => Err(refuse(
                format!("expression `{expr}` where a column name is required"),
                "direct column references (`col` or `table.col`)",
            )),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn translate_join(
        &mut self,
        mut left: RelOut,
        right_tf: &TableFactor,
        op: &JoinOperator,
    ) -> Result<RelOut> {
        enum Kind {
            Inner,
            Left,
            Semi,
        }
        let (kind, constraint) = match op {
            JoinOperator::Inner(c) => (Kind::Inner, c),
            JoinOperator::LeftOuter(c) => (Kind::Left, c),
            JoinOperator::LeftSemi(c) | JoinOperator::RightSemi(c) => (Kind::Semi, c),
            JoinOperator::RightOuter(_) => {
                return Err(refuse(
                    "`RIGHT JOIN`",
                    "`INNER JOIN` or `LEFT JOIN` (swap the two tables to write a `RIGHT JOIN` as a `LEFT JOIN`)",
                ))
            }
            JoinOperator::FullOuter(_) => {
                return Err(refuse("`FULL OUTER JOIN`", "`INNER JOIN` or `LEFT JOIN`"))
            }
            JoinOperator::CrossJoin => {
                return Err(refuse(
                    "`CROSS JOIN`",
                    "equi-joins (`INNER JOIN ... ON a = b` or `LEFT JOIN ... ON a = b`)",
                ))
            }
            JoinOperator::LeftAnti(_) | JoinOperator::RightAnti(_) => {
                return Err(refuse(
                    "`ANTI JOIN`",
                    "`INNER JOIN`, `LEFT JOIN`, or `SEMI JOIN`",
                ))
            }
            other => {
                return Err(refuse(
                    format!("join operator `{other:?}`"),
                    "`INNER JOIN`, `LEFT JOIN`, or `SEMI JOIN`",
                ))
            }
        };

        // Check if right_tf is the virtual `keys` table (`JOIN keys ON ...` or `JOIN keys USING (...)`)
        if is_keys_table_factor(right_tf, &self.cte_defs) {
            if matches!(kind, Kind::Left) {
                return Err(refuse(
                    "`LEFT JOIN keys`",
                    "`INNER JOIN keys` or `SEMI JOIN keys` or `WHERE col IN (:keys)`",
                ));
            }
            let keys_alias =
                table_factor_alias_or_name(right_tf).unwrap_or_else(|| KEYS_TABLE.to_string());
            let left_cols = self.extract_keys_join_cols(&mut left, &keys_alias, constraint)?;
            let node = self.push_node(PlanNode::SemiJoin {
                input: left.node,
                columns: left_cols.clone(),
                keys: KeysFrom::Run,
            });
            left.node = node;
            left.run_key_cols = Some(left_cols);
            return Ok(left);
        }

        let mut right = self.translate_table_factor(right_tf)?;
        let pairs = self.extract_join_pairs(&mut left, &mut right, constraint)?;
        if pairs.is_empty() {
            return Err(refuse(
                "a join without equality column pairs",
                "equi-joins (`JOIN ... ON left.col = right.col` or `USING (col)`)",
            ));
        }

        match kind {
            Kind::Semi => {
                let left_cols: Vec<String> = pairs.iter().map(|(l, _)| l.clone()).collect();
                let right_cols: Vec<String> = pairs.iter().map(|(_, r)| r.clone()).collect();
                let node = self.push_node(PlanNode::SemiJoin {
                    input: left.node,
                    columns: left_cols,
                    keys: KeysFrom::Node {
                        node: right.node,
                        columns: right_cols,
                    },
                });
                left.node = node;
                Ok(left)
            }
            Kind::Inner | Kind::Left => {
                let join_kind = match kind {
                    Kind::Inner => JoinKind::Inner,
                    Kind::Left => JoinKind::Left,
                    Kind::Semi => unreachable!(),
                };
                let node = self.push_node(PlanNode::Join {
                    left: left.node,
                    right: right.node,
                    on: pairs.clone(),
                    kind: join_kind,
                });
                let mut combined = left.columns;
                for rc in right.columns {
                    if let Some((a, _)) = pairs.iter().find(|(a, b)| a == b && *b == rc.name) {
                        if let Some(lc) = combined.iter_mut().find(|c| c.name == *a) {
                            if let Some(rq) = rc.qualifier {
                                if !lc.alt_qualifiers.contains(&rq) {
                                    lc.alt_qualifiers.push(rq);
                                }
                            }
                            for aq in rc.alt_qualifiers {
                                if !lc.alt_qualifiers.contains(&aq) {
                                    lc.alt_qualifiers.push(aq);
                                }
                            }
                        }
                        continue;
                    }
                    combined.push(rc);
                }
                let mut scan_origins = left.scan_origins;
                scan_origins.extend(right.scan_origins);
                let run_key_cols = left.run_key_cols.or(right.run_key_cols);
                Ok(RelOut {
                    node,
                    columns: combined,
                    scan_origins,
                    run_key_cols,
                    pending_rn: None,
                })
            }
        }
    }

    fn extract_keys_join_cols(
        &mut self,
        left: &mut RelOut,
        keys_alias: &str,
        constraint: &JoinConstraint,
    ) -> Result<Vec<String>> {
        match constraint {
            JoinConstraint::Using(idents) => {
                let mut cols = Vec::with_capacity(idents.len());
                for id in idents {
                    let c = self.ensure_col_on_scan_rel(left, None, &id.value)?;
                    cols.push(c.name);
                }
                Ok(cols)
            }
            JoinConstraint::On(expr) => {
                let mut eqs = Vec::new();
                collect_and_conjuncts(expr, &mut eqs);
                let mut cols = Vec::with_capacity(eqs.len());
                for eq in eqs {
                    let Expr::BinaryOp {
                        left: l_expr,
                        op: BinaryOperator::Eq,
                        right: r_expr,
                    } = eq
                    else {
                        return Err(refuse(
                            format!("join condition `{eq}`"),
                            "equality predicates (`ON left.col = keys.col` or `USING (col)`)",
                        ));
                    };
                    let (lq, lc) = split_col_ref(l_expr)?;
                    let (rq, rc) = split_col_ref(r_expr)?;
                    let left_col = if lq.is_some_and(|q| {
                        q.eq_ignore_ascii_case(keys_alias) || q.eq_ignore_ascii_case(KEYS_TABLE)
                    }) {
                        self.ensure_col_on_scan_rel(left, rq, rc)?.name
                    } else if rq.is_some_and(|q| {
                        q.eq_ignore_ascii_case(keys_alias) || q.eq_ignore_ascii_case(KEYS_TABLE)
                    }) {
                        self.ensure_col_on_scan_rel(left, lq, lc)?.name
                    } else if let Ok(c) = self.ensure_col_on_scan_rel(left, lq, lc) {
                        c.name
                    } else {
                        self.ensure_col_on_scan_rel(left, rq, rc)?.name
                    };
                    if !cols.contains(&left_col) {
                        cols.push(left_col);
                    }
                }
                Ok(cols)
            }
            JoinConstraint::Natural => Err(refuse(
                "`NATURAL JOIN`",
                "explicit `JOIN ... ON ...` or `JOIN ... USING (...)`",
            )),
            JoinConstraint::None => Err(refuse(
                "`JOIN` without `ON` or `USING`",
                "`JOIN ... ON left.col = right.col` or `JOIN ... USING (col)`",
            )),
        }
    }

    fn extract_join_pairs(
        &mut self,
        left: &mut RelOut,
        right: &mut RelOut,
        constraint: &JoinConstraint,
    ) -> Result<Vec<(String, String)>> {
        match constraint {
            JoinConstraint::Using(idents) => {
                let mut pairs = Vec::with_capacity(idents.len());
                for id in idents {
                    let lc = self.ensure_col_on_scan_rel(left, None, &id.value)?;
                    let rc = self.ensure_col_on_scan_rel(right, None, &id.value)?;
                    pairs.push((lc.name, rc.name));
                }
                Ok(pairs)
            }
            JoinConstraint::On(expr) => {
                let mut eqs = Vec::new();
                collect_and_conjuncts(expr, &mut eqs);
                let mut pairs = Vec::with_capacity(eqs.len());
                for eq in eqs {
                    let Expr::BinaryOp {
                        left: a,
                        op: BinaryOperator::Eq,
                        right: b,
                    } = eq
                    else {
                        return Err(refuse(
                            format!("non-equi join condition `{eq}`"),
                            "equality join predicates (`ON left.col = right.col`)",
                        ));
                    };
                    let (aq, ac) = split_col_ref(a)?;
                    let (bq, bc) = split_col_ref(b)?;
                    // Determine which side is left and which is right
                    let a_matches_left = matches_rel(left, aq, ac);
                    let b_matches_right = matches_rel(right, bq, bc);
                    let a_matches_right = matches_rel(right, aq, ac);
                    let b_matches_left = matches_rel(left, bq, bc);

                    let (l_col, r_col) = if a_matches_left && b_matches_right {
                        let lc = self.ensure_col_on_scan_rel(left, aq, ac)?;
                        let rc = self.ensure_col_on_scan_rel(right, bq, bc)?;
                        (lc.name, rc.name)
                    } else if a_matches_right && b_matches_left {
                        let lc = self.ensure_col_on_scan_rel(left, bq, bc)?;
                        let rc = self.ensure_col_on_scan_rel(right, aq, ac)?;
                        (lc.name, rc.name)
                    } else {
                        let lc = self.ensure_col_on_scan_rel(left, aq, ac)?;
                        let rc = self.ensure_col_on_scan_rel(right, bq, bc)?;
                        (lc.name, rc.name)
                    };
                    pairs.push((l_col, r_col));
                }
                Ok(pairs)
            }
            JoinConstraint::Natural => Err(refuse(
                "`NATURAL JOIN`",
                "explicit `JOIN ... ON ...` or `JOIN ... USING (...)`",
            )),
            JoinConstraint::None => Err(refuse(
                "`JOIN` without `ON` or `USING`",
                "`JOIN ... ON left.col = right.col` or `JOIN ... USING (col)`",
            )),
        }
    }

    fn take_pending_row_number(
        &mut self,
        rel: &mut RelOut,
        rn_col: &str,
    ) -> Result<Option<PendingRowNumber>> {
        let Some(pending) = &rel.pending_rn else {
            return Ok(None);
        };
        if !pending.alias.eq_ignore_ascii_case(rn_col) {
            return Ok(None);
        }
        let taken = rel
            .pending_rn
            .take()
            .ok_or_else(|| Error::Other("missing pending_rn".into()))?;
        // Remove the synthetic `rn` column from `rel.columns`
        rel.columns
            .retain(|c| !c.name.eq_ignore_ascii_case(&taken.alias));
        // If `rel.node` is a `PlanNode::Project` whose input is `PlanNode::Window`,
        // unwrap back to the window's input so `apply_latest_or_asof` replaces `Window` with `Latest`/`AsOf`!
        if let Some(PlanNode::Project { input, .. }) = self.nodes.get(rel.node as usize).cloned() {
            if let Some(PlanNode::Window { input: win_in, .. }) =
                self.nodes.get(input as usize).cloned()
            {
                rel.node = win_in;
            } else {
                rel.node = input;
            }
        } else if let Some(PlanNode::Window { input, .. }) =
            self.nodes.get(rel.node as usize).cloned()
        {
            rel.node = input;
        }
        Ok(Some(taken))
    }

    #[allow(clippy::too_many_lines)]
    fn apply_where(
        &mut self,
        mut rel: RelOut,
        where_expr: &Expr,
        as_of_from_where: &mut bool,
    ) -> Result<RelOut> {
        let mut conjuncts = Vec::new();
        collect_and_conjuncts(where_expr, &mut conjuncts);

        // First pass: merge `col >= lo` and `col <= hi` pairs on the same column into `BETWEEN`
        let mut range_lo: BTreeMap<String, (&Expr, &Expr)> = BTreeMap::new();
        let mut range_hi: BTreeMap<String, (&Expr, &Expr)> = BTreeMap::new();
        let mut consumed = vec![false; conjuncts.len()];

        for (i, c) in conjuncts.iter().enumerate() {
            if let Some((col_expr, bound, is_lo, is_param)) = match_range_bound(c) {
                if is_param && !is_lo {
                    // e.g. `ts <= :at` or `ts <= $at` -> marks `as_of_from_where = true`
                    let _ = self.resolve_col_mut(&mut rel, col_expr)?;
                    *as_of_from_where = true;
                    consumed[i] = true;
                    continue;
                }
                if let Ok(col_info) = self.resolve_col_mut(&mut rel, col_expr) {
                    if is_lo {
                        range_lo.insert(col_info.name, (col_expr, bound));
                    } else {
                        range_hi.insert(col_info.name, (col_expr, bound));
                    }
                }
            }
        }

        let mut predicates: Vec<Predicate> = Vec::new();

        for (i, c) in conjuncts.iter().enumerate() {
            if consumed[i] {
                continue;
            }
            if let Some((col_expr, _, is_lo, _)) = match_range_bound(c) {
                let col_info = self.resolve_col_mut(&mut rel, col_expr)?;
                if let (Some((_, lo_expr)), Some((_, hi_expr))) = (
                    range_lo.get(&col_info.name).copied(),
                    range_hi.get(&col_info.name).copied(),
                ) {
                    if is_lo {
                        let pred = self.build_between_predicate(
                            &mut rel,
                            &col_info.name,
                            lo_expr,
                            hi_expr,
                        )?;
                        predicates.push(pred);
                    }
                    continue;
                }
                return Err(refuse(
                    format!("one-sided range predicate `{c}`"),
                    "two-sided range predicates (`col BETWEEN lo AND hi` or `col >= lo AND col <= hi`), or `ts <= :at` with `QUALIFY row_number() ... = 1` for as-of lookups",
                ));
            }

            match self.translate_where_conjunct(&mut rel, c)? {
                WhereStep::SemiJoin { columns, keys } => {
                    if !predicates.is_empty() {
                        rel = self.flush_filter_predicates(rel, &mut predicates);
                    }
                    if matches!(keys, KeysFrom::Run) {
                        rel.run_key_cols = Some(columns.clone());
                    }
                    rel.node = self.push_semi_join_or_pushdown(rel.node, columns, keys);
                }
                WhereStep::Pred(p) => {
                    predicates.push(p);
                }
            }
        }

        if !predicates.is_empty() {
            rel = self.flush_filter_predicates(rel, &mut predicates);
        }
        Ok(rel)
    }

    fn flush_filter_predicates(
        &mut self,
        mut rel: RelOut,
        predicates: &mut Vec<Predicate>,
    ) -> RelOut {
        let items = std::mem::take(predicates);
        let predicate = if items.len() == 1 {
            items
                .into_iter()
                .next()
                .unwrap_or(Predicate::And { all: Vec::new() })
        } else {
            Predicate::And { all: items }
        };
        let node = self.push_node(PlanNode::Filter {
            input: rel.node,
            predicate,
        });
        rel.node = node;
        rel
    }

    /// Pushes a `SemiJoin` node over `input_node`, or if `input_node` is a `Join`
    /// and `columns` are the join's left key columns, pushes the `SemiJoin` into
    /// the join's left input (`join.json` convention) so the join is narrowed from
    /// its left side and `why` never blames `PlanShape` (`semi_join` above `join`).
    fn push_semi_join_or_pushdown(
        &mut self,
        input_node: u32,
        columns: Vec<String>,
        keys: KeysFrom,
    ) -> u32 {
        if matches!(keys, KeysFrom::Run | KeysFrom::Values { .. }) {
            if let Some(PlanNode::Join {
                left,
                right,
                on,
                kind,
            }) = self.nodes.get(input_node as usize).cloned()
            {
                let left_join_cols: Vec<String> = on.iter().map(|(l, _)| l.clone()).collect();
                if left_join_cols == columns {
                    let sj_idx = self.push_node(PlanNode::SemiJoin {
                        input: left,
                        columns,
                        keys,
                    });
                    if let Some(slot) = self.nodes.get_mut(input_node as usize) {
                        *slot = PlanNode::Join {
                            left: sj_idx,
                            right,
                            on,
                            kind,
                        };
                    }
                    return input_node;
                }
            }
        }
        self.push_node(PlanNode::SemiJoin {
            input: input_node,
            columns,
            keys,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn translate_where_conjunct(&mut self, rel: &mut RelOut, expr: &Expr) -> Result<WhereStep> {
        match expr {
            Expr::Nested(inner) => self.translate_where_conjunct(rel, inner),
            Expr::InList {
                expr: target,
                list,
                negated,
            } => {
                if *negated {
                    return Err(refuse(
                        format!("`{expr}` (`NOT IN`)"),
                        "positive membership predicates (`col IN (...)` or `col IN (:keys)`)",
                    ));
                }
                let cols = self.extract_target_columns(rel, target)?;
                if list.is_empty() {
                    return Err(Error::InvalidArgument {
                        field: "sql",
                        reason: format!("`{expr}` has an empty `IN ()` list"),
                    });
                }
                // Check if the list is a single placeholder (`:keys`, `$keys`, `$1`, `?`)
                if list.len() == 1 && is_run_keys_placeholder(&list[0]) {
                    return Ok(WhereStep::SemiJoin {
                        columns: cols,
                        keys: KeysFrom::Run,
                    });
                }
                if self.opts.keys_from_run {
                    return Ok(WhereStep::SemiJoin {
                        columns: cols,
                        keys: KeysFrom::Run,
                    });
                }
                let values = self.encode_in_list_tuples(rel, &cols, list)?;
                Ok(WhereStep::Pred(Predicate::In {
                    columns: cols,
                    values,
                }))
            }
            Expr::InSubquery {
                expr: target,
                subquery,
                negated,
            } => {
                if *negated {
                    return Err(refuse(
                        format!("`{expr}` (`NOT IN (SELECT ...)`)"),
                        "positive semi-join subqueries (`col IN (SELECT ...)`)",
                    ));
                }
                let cols = self.extract_target_columns(rel, target)?;
                if is_select_from_keys_table(subquery, &self.cte_defs) {
                    return Ok(WhereStep::SemiJoin {
                        columns: cols,
                        keys: KeysFrom::Run,
                    });
                }
                let sub_rel = self.translate_query(subquery)?;
                if sub_rel.columns.len() != cols.len() {
                    return Err(Error::InvalidArgument {
                        field: "sql",
                        reason: format!(
                            "`IN (SELECT ...)` subquery outputs {} column(s), but the left-hand side names {} column(s) ({})",
                            sub_rel.columns.len(),
                            cols.len(),
                            cols.join(", ")
                        ),
                    });
                }
                let sub_cols: Vec<String> =
                    sub_rel.columns.iter().map(|c| c.name.clone()).collect();
                Ok(WhereStep::SemiJoin {
                    columns: cols,
                    keys: KeysFrom::Node {
                        node: sub_rel.node,
                        columns: sub_cols,
                    },
                })
            }
            Expr::Exists { subquery, negated } => {
                if *negated {
                    return Err(refuse(
                        "`NOT EXISTS (...)`",
                        "positive `EXISTS (...)` or `SEMI JOIN`",
                    ));
                }
                self.translate_exists_subquery(rel, subquery)
            }
            Expr::Between {
                expr: target,
                negated,
                low,
                high,
            } => {
                if *negated {
                    return Err(refuse(
                        format!("`{expr}` (`NOT BETWEEN`)"),
                        "positive range predicates (`col BETWEEN lo AND hi`)",
                    ));
                }
                let col_info = self.resolve_col_mut(rel, target)?;
                let pred = self.build_between_predicate(rel, &col_info.name, low, high)?;
                Ok(WhereStep::Pred(pred))
            }
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Eq => {
                    // Check `col = :keys` or `:keys = col` or `col = literal`
                    let (col_expr, val_expr) = if is_col_ref(left) && !is_col_ref(right) {
                        (left.as_ref(), right.as_ref())
                    } else if is_col_ref(right) && !is_col_ref(left) {
                        (right.as_ref(), left.as_ref())
                    } else if matches!(left.as_ref(), Expr::Tuple(_)) {
                        (left.as_ref(), right.as_ref())
                    } else {
                        return Err(refuse(
                            format!("equality predicate `{expr}` between two columns or expressions in `WHERE`"),
                            "`JOIN ... ON left.col = right.col` for joining tables, or `col = <literal>` / `col IN (:keys)` for filtering",
                        ));
                    };
                    let cols = self.extract_target_columns(rel, col_expr)?;
                    if is_run_keys_placeholder(val_expr) || self.opts.keys_from_run {
                        return Ok(WhereStep::SemiJoin {
                            columns: cols,
                            keys: KeysFrom::Run,
                        });
                    }
                    let values =
                        self.encode_in_list_tuples(rel, &cols, std::slice::from_ref(val_expr))?;
                    Ok(WhereStep::Pred(Predicate::In {
                        columns: cols,
                        values,
                    }))
                }
                BinaryOperator::Or => Err(refuse(
                    format!("`OR` disjunction in `{expr}`"),
                    "`col IN (v1, v2, ...)` for multiple values of the same column, or `AND` conjunctions",
                )),
                BinaryOperator::NotEq => Err(refuse(
                    format!("inequality `{expr}`"),
                    "`col IN (...)` or `col BETWEEN lo AND hi`",
                )),
                BinaryOperator::Lt | BinaryOperator::Gt => Err(refuse(
                    format!("strict inequality `{expr}`"),
                    "inclusive range bounds (`col BETWEEN lo AND hi` or `col >= lo AND col <= hi`)",
                )),
                _ => Err(refuse(
                    format!("predicate `{expr}`"),
                    "`col IN (:keys)`, `col IN (...)`, `col = val`, `col BETWEEN lo AND hi`, and `AND` conjunctions",
                )),
            },
            Expr::IsNull(_) | Expr::IsNotNull(_) => Err(refuse(
                format!("`{expr}`"),
                "key membership (`col IN (...)`), semi-joins, and inclusive range predicates (`BETWEEN`)",
            )),
            Expr::Like { .. } | Expr::ILike { .. } | Expr::RLike { .. } => Err(refuse(
                format!("pattern match `{expr}`"),
                "text lookups via `bellwether locate --contains/--has/--prefix` or `bellwether why --contains/--has/--prefix` (B5 text index)",
            )),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                ..
            } => Err(refuse(
                format!("`{expr}` (`NOT`)"),
                "positive key and range filters (`IN`, `SEMI JOIN`, `BETWEEN`)",
            )),
            other => Err(refuse(
                format!("predicate `{other}`"),
                "`col IN (:keys)`, `col IN (...)`, `col = val`, `col BETWEEN lo AND hi`, and `AND` conjunctions",
            )),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn translate_exists_subquery(
        &mut self,
        outer_rel: &mut RelOut,
        subquery: &Query,
    ) -> Result<WhereStep> {
        let SetExpr::Select(sel) = subquery.body.as_ref() else {
            return Err(refuse(
                format!("`EXISTS ({subquery})`"),
                "`EXISTS (SELECT 1 FROM <table> WHERE outer.col = inner.col)`",
            ));
        };
        if sel.from.len() != 1 || !sel.from[0].joins.is_empty() {
            return Err(refuse(
                format!("`EXISTS ({subquery})` with multiple tables"),
                "`col IN (SELECT col FROM ...)` or `SEMI JOIN`",
            ));
        }
        let Some(where_expr) = &sel.selection else {
            return Err(refuse(
                format!("uncorrelated `EXISTS ({subquery})` without `WHERE`"),
                "`EXISTS (SELECT 1 FROM <table> WHERE outer.key = inner.key)`",
            ));
        };
        if is_keys_table_factor(&sel.from[0].relation, &self.cte_defs) {
            let keys_alias = table_factor_alias_or_name(&sel.from[0].relation)
                .unwrap_or_else(|| KEYS_TABLE.to_string());
            let mut eqs = Vec::new();
            collect_and_conjuncts(where_expr, &mut eqs);
            let mut cols = Vec::with_capacity(eqs.len());
            for eq in eqs {
                let Expr::BinaryOp {
                    left: l_expr,
                    op: BinaryOperator::Eq,
                    right: r_expr,
                } = eq
                else {
                    return Err(refuse(
                        format!("`EXISTS` condition `{eq}`"),
                        "equality predicates (`outer.col = keys.col`)",
                    ));
                };
                let (lq, lc) = split_col_ref(l_expr)?;
                let (rq, rc) = split_col_ref(r_expr)?;
                let outer_col = if lq.is_some_and(|q| {
                    q.eq_ignore_ascii_case(&keys_alias) || q.eq_ignore_ascii_case(KEYS_TABLE)
                }) {
                    self.ensure_col_on_scan_rel(outer_rel, rq, rc)?.name
                } else {
                    self.ensure_col_on_scan_rel(outer_rel, lq, lc)?.name
                };
                if !cols.contains(&outer_col) {
                    cols.push(outer_col);
                }
            }
            return Ok(WhereStep::SemiJoin {
                columns: cols,
                keys: KeysFrom::Run,
            });
        }

        let mut inner_rel = self.translate_table_factor(&sel.from[0].relation)?;
        let mut eqs = Vec::new();
        collect_and_conjuncts(where_expr, &mut eqs);
        let mut outer_cols = Vec::new();
        let mut inner_cols = Vec::new();
        let mut inner_filters = Vec::new();

        for eq in eqs {
            if let Expr::BinaryOp {
                left: a,
                op: BinaryOperator::Eq,
                right: b,
            } = eq
            {
                if is_col_ref(a) && is_col_ref(b) {
                    let (aq, ac) = split_col_ref(a)?;
                    let (bq, bc) = split_col_ref(b)?;
                    if matches_rel(outer_rel, aq, ac) && matches_rel(&inner_rel, bq, bc) {
                        outer_cols.push(self.ensure_col_on_scan_rel(outer_rel, aq, ac)?.name);
                        inner_cols.push(self.ensure_col_on_scan_rel(&mut inner_rel, bq, bc)?.name);
                        continue;
                    } else if matches_rel(&inner_rel, aq, ac) && matches_rel(outer_rel, bq, bc) {
                        outer_cols.push(self.ensure_col_on_scan_rel(outer_rel, bq, bc)?.name);
                        inner_cols.push(self.ensure_col_on_scan_rel(&mut inner_rel, aq, ac)?.name);
                        continue;
                    }
                }
            }
            inner_filters.push(eq.clone());
        }

        if outer_cols.is_empty() {
            return Err(refuse(
                format!("`EXISTS ({subquery})` without an outer=inner key equality"),
                "`EXISTS (SELECT 1 FROM other WHERE outer.key = other.key)` or `col IN (SELECT key FROM other)`",
            ));
        }
        if !inner_filters.is_empty() {
            let combined_inner = inner_filters
                .into_iter()
                .reduce(|acc, e| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: BinaryOperator::And,
                    right: Box::new(e),
                })
                .ok_or_else(|| Error::Other("empty inner filters".into()))?;
            let mut dummy_asof = false;
            inner_rel = self.apply_where(inner_rel, &combined_inner, &mut dummy_asof)?;
        }
        Ok(WhereStep::SemiJoin {
            columns: outer_cols,
            keys: KeysFrom::Node {
                node: inner_rel.node,
                columns: inner_cols,
            },
        })
    }

    fn extract_target_columns(&mut self, rel: &mut RelOut, target: &Expr) -> Result<Vec<String>> {
        match target {
            Expr::Tuple(exprs) => {
                if exprs.is_empty() {
                    return Err(Error::InvalidArgument {
                        field: "sql",
                        reason: "empty tuple `()` in predicate".into(),
                    });
                }
                let mut cols = Vec::with_capacity(exprs.len());
                for e in exprs {
                    cols.push(self.resolve_col_mut(rel, e)?.name);
                }
                Ok(cols)
            }
            Expr::Nested(inner) => self.extract_target_columns(rel, inner),
            _ => Ok(vec![self.resolve_col_mut(rel, target)?.name]),
        }
    }

    fn encode_in_list_tuples(
        &self,
        rel: &RelOut,
        cols: &[String],
        list: &[Expr],
    ) -> Result<Vec<Vec<u8>>> {
        let col_types: Vec<Option<DataType>> =
            cols.iter().map(|c| self.column_data_type(rel, c)).collect();
        let mut out = Vec::with_capacity(list.len());
        for item in list {
            if cols.len() == 1 {
                let mut buf = Vec::new();
                encode_sql_literal(item, &cols[0], col_types[0].as_ref(), &mut buf)?;
                out.push(buf);
            } else {
                let tuple_items = match item {
                    Expr::Tuple(t) => t.as_slice(),
                    _ => {
                        return Err(Error::InvalidArgument {
                            field: "sql",
                            reason: format!(
                                "composite `IN` on ({}) requires tuple elements `(v1, v2, ...)`, got `{item}`",
                                cols.join(", ")
                            ),
                        })
                    }
                };
                if tuple_items.len() != cols.len() {
                    return Err(Error::InvalidArgument {
                        field: "sql",
                        reason: format!(
                            "tuple `{item}` has {} element(s), but the column list has {} ({})",
                            tuple_items.len(),
                            cols.len(),
                            cols.join(", ")
                        ),
                    });
                }
                let mut buf = Vec::new();
                for ((e, col_name), dt) in tuple_items.iter().zip(cols).zip(&col_types) {
                    encode_sql_literal(e, col_name, dt.as_ref(), &mut buf)?;
                }
                out.push(buf);
            }
        }
        Ok(out)
    }

    fn build_between_predicate(
        &self,
        rel: &mut RelOut,
        col_name: &str,
        low: &Expr,
        high: &Expr,
    ) -> Result<Predicate> {
        let dt = self.column_data_type(rel, col_name);
        let mut lo = Vec::new();
        let mut hi = Vec::new();
        encode_sql_literal(low, col_name, dt.as_ref(), &mut lo)?;
        encode_sql_literal(high, col_name, dt.as_ref(), &mut hi)?;
        if lo > hi {
            return Err(Error::InvalidArgument {
                field: "sql",
                reason: format!(
                    "`BETWEEN {low} AND {high}` on column {col_name:?} has lower bound above upper bound"
                ),
            });
        }
        Ok(Predicate::Between {
            column: col_name.to_string(),
            lo,
            hi,
        })
    }

    #[allow(clippy::type_complexity)]
    fn detect_arg_max(
        &mut self,
        projection: &[SelectItem],
        group_exprs: &[Expr],
        rel: &RelOut,
    ) -> Result<Option<(Vec<String>, String, Vec<(String, String)>)>> {
        let mut found_ts: Option<String> = None;
        let mut temp_rel = rel.clone();
        let mut projected_cols: Vec<(String, String)> = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Some((val_col, ts_col)) = self.match_arg_max_call(&mut temp_rel, expr)? {
                        if let Some(prev) = &found_ts {
                            if !prev.eq_ignore_ascii_case(&ts_col) {
                                return Err(Error::InvalidArgument {
                                    field: "sql",
                                    reason: format!(
                                        "multiple `arg_max` calls order by different timestamp columns ({prev:?} and {ts_col:?})"
                                    ),
                                });
                            }
                        }
                        found_ts = Some(ts_col);
                        if let Some(vc) = val_col {
                            projected_cols.push((vc.clone(), vc));
                        }
                    } else if let Ok(c) = self.resolve_col_mut(&mut temp_rel, expr) {
                        projected_cols.push((c.name.clone(), c.name));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let out_name = ident_str(alias);
                    if let Some((val_col, ts_col)) = self.match_arg_max_call(&mut temp_rel, expr)? {
                        if let Some(prev) = &found_ts {
                            if !prev.eq_ignore_ascii_case(&ts_col) {
                                return Err(Error::InvalidArgument {
                                    field: "sql",
                                    reason: format!(
                                        "multiple `arg_max` calls order by different timestamp columns ({prev:?} and {ts_col:?})"
                                    ),
                                });
                            }
                        }
                        found_ts = Some(ts_col);
                        if let Some(vc) = val_col {
                            projected_cols.push((vc, out_name));
                        }
                    } else if let Ok(c) = self.resolve_col_mut(&mut temp_rel, expr) {
                        projected_cols.push((c.name, out_name));
                    }
                }
                _ => {}
            }
        }

        let Some(ts_col) = found_ts else {
            return Ok(None);
        };
        if group_exprs.is_empty() {
            return Err(Error::InvalidArgument {
                field: "sql",
                reason: "`arg_max` requires `GROUP BY <key_columns>` to define the key for latest-per-key".into(),
            });
        }
        let mut key_cols = Vec::with_capacity(group_exprs.len());
        for g in group_exprs {
            let c = self.resolve_col_mut(&mut temp_rel, g)?;
            if !key_cols.contains(&c.name) {
                key_cols.push(c.name);
            }
        }
        Ok(Some((key_cols, ts_col, projected_cols)))
    }

    fn match_arg_max_call(
        &mut self,
        rel: &mut RelOut,
        expr: &Expr,
    ) -> Result<Option<(Option<String>, String)>> {
        let Expr::Function(func) = expr else {
            return Ok(None);
        };
        let name = object_table_name(&func.name).to_ascii_lowercase();
        if name != "arg_max" && name != "argmax" && name != "max_by" {
            return Ok(None);
        }
        let FunctionArguments::List(arg_list) = &func.args else {
            return Err(Error::InvalidArgument {
                field: "sql",
                reason: format!("`{name}` requires two arguments: `{name}(value_col, ts_col)`"),
            });
        };
        if arg_list.args.len() != 2 {
            return Err(Error::InvalidArgument {
                field: "sql",
                reason: format!("`{name}` requires two arguments: `{name}(value_col, ts_col)`"),
            });
        }
        let val_col = match &arg_list.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                Some(self.resolve_col_mut(rel, e)?.name)
            }
            other => {
                return Err(refuse(
                    format!("`{name}` argument `{other}`"),
                    "column name or `*` as first argument of `arg_max(col, ts)`",
                ))
            }
        };
        let ts_col = match &arg_list.args[1] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => self.resolve_col_mut(rel, e)?.name,
            other => return Err(refuse(
                format!("`{name}` timestamp argument `{other}`"),
                "a timestamp or integer version column as second argument of `arg_max(col, ts)`",
            )),
        };
        Ok(Some((val_col, ts_col)))
    }

    fn apply_aggregate(
        &mut self,
        mut rel: RelOut,
        group_exprs: &[Expr],
        projection: &[SelectItem],
    ) -> Result<RelOut> {
        let mut group_by = Vec::with_capacity(group_exprs.len());
        let mut out_cols = Vec::with_capacity(group_exprs.len());
        for g in group_exprs {
            let c = self.resolve_col_mut(&mut rel, g)?;
            if !group_by.contains(&c.name) {
                group_by.push(c.name.clone());
                out_cols.push(ColInfo {
                    qualifier: None,
                    alt_qualifiers: Vec::new(),
                    name: c.name,
                    dataset: c.dataset,
                    source_col: c.source_col,
                    scan_node: c.scan_node,
                });
            }
        }

        let mut measures: Vec<Measure> = Vec::new();
        for item in projection {
            let expr = match item {
                SelectItem::UnnamedExpr(e) => e,
                SelectItem::ExprWithAlias { expr: e, alias } => {
                    // If this is a group_by column with an alias, check if alias differs
                    if let Ok(c) = self.resolve_col_mut(&mut rel, e) {
                        if group_by.contains(&c.name) {
                            let a_str = ident_str(alias);
                            if !a_str.eq_ignore_ascii_case(&c.name) {
                                return Err(refuse(
                                    format!("renaming `GROUP BY` column `{}` to `{a_str}`", c.name),
                                    "keeping the `GROUP BY` column name (`PlanNode::Aggregate` outputs the grouping columns under their dataset names)",
                                ));
                            }
                            continue;
                        }
                    }
                    e
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    return Err(refuse(
                        "`SELECT *` with `GROUP BY`",
                        "explicit `GROUP BY` columns and aggregate functions (`COUNT`, `SUM`, `MIN`, `MAX`)",
                    ))
                }
            };

            if let Ok(c) = self.resolve_col_mut(&mut rel, expr) {
                if group_by.contains(&c.name) {
                    continue;
                }
                return Err(Error::InvalidArgument {
                    field: "sql",
                    reason: format!(
                        "column {:?} in `SELECT` is neither in `GROUP BY` nor inside an aggregate function (`COUNT`, `SUM`, `MIN`, `MAX`)",
                        c.name
                    ),
                });
            }

            let m = self.parse_aggregate_measure(&mut rel, expr)?;
            measures.push(m);
        }

        if measures.is_empty() {
            // A `GROUP BY a, b` without aggregate functions in `SELECT` still groups rows;
            // `ConsumerPlan`'s `PlanNode::Aggregate` requires at least `count`.
            measures.push(Measure {
                function: AggregateFn::Count,
                column: None,
            });
        }

        let node = self.push_node(PlanNode::Aggregate {
            input: rel.node,
            group_by,
            measures,
        });
        Ok(RelOut {
            node,
            columns: out_cols,
            scan_origins: Vec::new(),
            run_key_cols: rel.run_key_cols,
            pending_rn: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    fn parse_aggregate_measure(&mut self, rel: &mut RelOut, expr: &Expr) -> Result<Measure> {
        let Expr::Function(func) = expr else {
            return Err(refuse(
                format!("aggregate expression `{expr}`"),
                "`COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, or `MAX(col)`",
            ));
        };
        if func.over.is_some() {
            return Err(refuse(
                format!("window function `{func}` inside an aggregate query"),
                "a separate CTE or subquery for window functions before or after `GROUP BY`",
            ));
        }
        if func.filter.is_some() {
            return Err(refuse(
                format!("`{func}` (`FILTER (WHERE ...)` on aggregate)"),
                "filtering rows in `WHERE` before aggregation",
            ));
        }
        let name = object_table_name(&func.name).to_ascii_lowercase();
        let FunctionArguments::List(arg_list) = &func.args else {
            if name == "count" && matches!(func.args, FunctionArguments::None) {
                return Ok(Measure {
                    function: AggregateFn::Count,
                    column: None,
                });
            }
            return Err(refuse(
                format!("aggregate call `{func}`"),
                "`COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, or `MAX(col)`",
            ));
        };
        if matches!(
            arg_list.duplicate_treatment,
            Some(DuplicateTreatment::Distinct)
        ) {
            return Err(refuse(
                format!("`{func}` (`DISTINCT` aggregate)"),
                "`COUNT(*)`, `COUNT(col)`, `SUM(col)`, `MIN(col)`, or `MAX(col)` (for approximate `COUNT(DISTINCT col)` use `bellwether sketch cardinality`)",
            ));
        }
        if !arg_list.clauses.is_empty() {
            return Err(refuse(
                format!("aggregate clause in `{func}`"),
                "plain `COUNT`, `SUM`, `MIN`, or `MAX`",
            ));
        }

        match name.as_str() {
            "count" => {
                if arg_list.args.is_empty() {
                    return Ok(Measure {
                        function: AggregateFn::Count,
                        column: None,
                    });
                }
                if arg_list.args.len() != 1 {
                    return Err(refuse(
                        format!("`{func}` with multiple arguments"),
                        "`COUNT(*)` or `COUNT(col)`",
                    ));
                }
                match &arg_list.args[0] {
                    FunctionArg::Unnamed(
                        FunctionArgExpr::Wildcard
                        | FunctionArgExpr::Expr(Expr::Value(SqlValue::Number(_, _))),
                    ) => Ok(Measure {
                        function: AggregateFn::Count,
                        column: None,
                    }),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        let c = self.resolve_col_mut(rel, e)?;
                        Ok(Measure {
                            function: AggregateFn::Count,
                            column: Some(c.name),
                        })
                    }
                    other => Err(refuse(
                        format!("`COUNT({other})`"),
                        "`COUNT(*)` or `COUNT(col)`",
                    )),
                }
            }
            "sum" | "min" | "max" => {
                let afn = match name.as_str() {
                    "sum" => AggregateFn::Sum,
                    "min" => AggregateFn::Min,
                    "max" => AggregateFn::Max,
                    _ => unreachable!(),
                };
                if arg_list.args.len() != 1 {
                    return Err(refuse(
                        format!("`{func}`"),
                        format!("`{}(col)` over a single column", name.to_ascii_uppercase()),
                    ));
                }
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = &arg_list.args[0] else {
                    return Err(refuse(
                        format!("`{func}`"),
                        format!("`{}(col)` over a column name", name.to_ascii_uppercase()),
                    ));
                };
                let c = self.resolve_col_mut(rel, e)?;
                Ok(Measure {
                    function: afn,
                    column: Some(c.name),
                })
            }
            _ => Err(refuse(
                format!("aggregate function `{}`", func.name),
                "`COUNT`, `SUM`, `MIN`, and `MAX` (or `arg_max(col, ts)` for latest-per-key)",
            )),
        }
    }

    fn parse_qualify_latest(
        &mut self,
        rel: &RelOut,
        qualify: &Expr,
        named_windows: &[NamedWindowDefinition],
    ) -> Result<(Vec<String>, String, bool)> {
        let mut temp_rel = rel.clone();
        let mut conjuncts = Vec::new();
        collect_and_conjuncts(qualify, &mut conjuncts);
        let mut is_asof = false;
        let mut latest_spec: Option<(Vec<String>, String)> = None;

        for c in conjuncts {
            if let Some((_, _, false, true)) = match_range_bound(c) {
                is_asof = true;
                continue;
            }
            if let Some(rn_col) = match_rn_eq_one(c) {
                if let Some(pending) = &rel.pending_rn {
                    if pending.alias.eq_ignore_ascii_case(&rn_col) {
                        for pc in &pending.part_cols {
                            let _ = self.ensure_col_on_scan_rel(&mut temp_rel, None, pc)?;
                        }
                        let _ =
                            self.ensure_col_on_scan_rel(&mut temp_rel, None, &pending.ts_col)?;
                        latest_spec = Some((pending.part_cols.clone(), pending.ts_col.clone()));
                        is_asof |= pending.as_of;
                        continue;
                    }
                }
            }
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = c
            else {
                return Err(refuse(
                    format!("`QUALIFY {c}`"),
                    "`QUALIFY row_number() OVER (PARTITION BY <keys> ORDER BY <ts> DESC) = 1`",
                ));
            };
            let func_expr = if is_literal_one(right) {
                left.as_ref()
            } else if is_literal_one(left) {
                right.as_ref()
            } else {
                return Err(refuse(
                    format!("`QUALIFY {c}`"),
                    "`QUALIFY row_number() OVER (PARTITION BY <keys> ORDER BY <ts> DESC) = 1`",
                ));
            };
            let Expr::Function(func) = unwrap_nested(func_expr) else {
                return Err(refuse(
                    format!("`QUALIFY {c}`"),
                    "`QUALIFY row_number() OVER (PARTITION BY <keys> ORDER BY <ts> DESC) = 1`",
                ));
            };
            match self.parse_window_function(&mut temp_rel, func, named_windows)? {
                WindowExtract::Latest {
                    part_cols,
                    ts_col,
                    as_of,
                    ..
                } => {
                    latest_spec = Some((part_cols, ts_col));
                    is_asof |= as_of;
                }
                WindowExtract::Partition { .. } => {
                    return Err(refuse(
                        format!("`QUALIFY {c}` without `ORDER BY <ts> DESC`"),
                        "`QUALIFY row_number() OVER (PARTITION BY <keys> ORDER BY <ts> DESC) = 1`",
                    ))
                }
            }
        }

        latest_spec.map(|(p, t)| (p, t, is_asof)).ok_or_else(|| {
            refuse(
                format!("`QUALIFY {qualify}`"),
                "`QUALIFY row_number() OVER (PARTITION BY <keys> ORDER BY <ts> DESC) = 1`",
            )
        })
    }

    fn extract_select_windows(
        &mut self,
        rel: &RelOut,
        projection: &[SelectItem],
        named_windows: &[NamedWindowDefinition],
    ) -> Result<Vec<WindowExtract>> {
        let mut temp_rel = rel.clone();
        let mut out = Vec::new();
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(e) => {
                    if let Some(w) =
                        self.find_window_in_expr(&mut temp_rel, e, None, named_windows)?
                    {
                        out.push(w);
                    }
                }
                SelectItem::ExprWithAlias { expr: e, alias } => {
                    let a = Some(ident_str(alias));
                    if let Some(w) = self.find_window_in_expr(&mut temp_rel, e, a, named_windows)? {
                        out.push(w);
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }

    fn find_window_in_expr(
        &mut self,
        rel: &mut RelOut,
        expr: &Expr,
        alias: Option<String>,
        named_windows: &[NamedWindowDefinition],
    ) -> Result<Option<WindowExtract>> {
        match expr {
            Expr::Function(func) if func.over.is_some() => {
                let mut w = self.parse_window_function(rel, func, named_windows)?;
                if let WindowExtract::Latest {
                    alias: ref mut a, ..
                } = &mut w
                {
                    *a = alias;
                }
                Ok(Some(w))
            }
            Expr::Nested(inner) => self.find_window_in_expr(rel, inner, alias, named_windows),
            _ => Ok(None),
        }
    }

    fn parse_window_function(
        &mut self,
        rel: &mut RelOut,
        func: &Function,
        named_windows: &[NamedWindowDefinition],
    ) -> Result<WindowExtract> {
        let Some(over) = &func.over else {
            return Err(Error::Other(
                "expected OVER clause on window function".into(),
            ));
        };
        let spec = resolve_window_spec(over, named_windows)?;
        // Record any columns referenced in function arguments so scan projection includes them
        if let FunctionArguments::List(arg_list) = &func.args {
            for arg in &arg_list.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                    if is_col_ref(e) {
                        let _ = self.resolve_col_mut(rel, e)?;
                    }
                }
            }
        }
        let mut part_cols = Vec::with_capacity(spec.partition_by.len());
        for p in &spec.partition_by {
            let c = self.resolve_col_mut(rel, p)?;
            if !part_cols.contains(&c.name) {
                part_cols.push(c.name);
            }
        }
        let fn_name = object_table_name(&func.name).to_ascii_lowercase();
        if (fn_name == "row_number" || fn_name == "rank" || fn_name == "dense_rank")
            && spec.order_by.len() == 1
            && !part_cols.is_empty()
        {
            let ob: &OrderByExpr = &spec.order_by[0];
            if ob.asc == Some(false) {
                let ts_col = self.resolve_col_mut(rel, &ob.expr)?.name;
                return Ok(WindowExtract::Latest {
                    part_cols,
                    ts_col,
                    as_of: false,
                    alias: None,
                });
            }
        }
        // Also record any columns in `spec.order_by`
        for ob in &spec.order_by {
            if is_col_ref(&ob.expr) {
                let _ = self.resolve_col_mut(rel, &ob.expr)?;
            }
        }
        Ok(WindowExtract::Partition { part_cols })
    }

    fn apply_latest_or_asof(
        &mut self,
        mut rel: RelOut,
        part_cols: Vec<String>,
        ts_col: String,
        as_of: bool,
    ) -> Result<RelOut> {
        for c in &part_cols {
            let _ = self.ensure_col_on_scan_rel(&mut rel, None, c)?;
        }
        let _ = self.ensure_col_on_scan_rel(&mut rel, None, &ts_col)?;
        let node = if as_of {
            self.push_node(PlanNode::AsOf {
                input: rel.node,
                columns: part_cols,
                ts: ts_col,
            })
        } else {
            self.push_node(PlanNode::Latest {
                input: rel.node,
                columns: part_cols,
                ts: ts_col,
            })
        };
        rel.node = node;
        Ok(rel)
    }

    fn apply_window(&mut self, mut rel: RelOut, part_cols: Vec<String>) -> Result<RelOut> {
        for c in &part_cols {
            let _ = self.ensure_col_on_scan_rel(&mut rel, None, c)?;
        }
        // If the input is a direct `Scan` and `rel.run_key_cols` matches `part_cols`
        // (e.g. a `SemiJoin(Run)` was pushed right before the window on the same columns),
        // check if the immediate node is `SemiJoin { input, columns, keys: KeysFrom::Run }`
        // on the same columns: if so, fold it into `PlanNode::Window { input, partition_by, keys: Some(KeysFrom::Run) }`!
        if let Some(PlanNode::SemiJoin {
            input: sj_input,
            columns: sj_cols,
            keys: KeysFrom::Run,
        }) = self.nodes.get(rel.node as usize).cloned()
        {
            if sj_cols == part_cols && self.nodes.len() == (rel.node as usize) + 1 {
                self.nodes.pop();
                let node = self.push_node(PlanNode::Window {
                    input: sj_input,
                    partition_by: part_cols.clone(),
                    keys: Some(KeysFrom::Run),
                });
                rel.node = node;
                return Ok(rel);
            }
        }
        let keys = rel
            .run_key_cols
            .as_ref()
            .filter(|rk| **rk == part_cols)
            .map(|_| KeysFrom::Run);
        let node = self.push_node(PlanNode::Window {
            input: rel.node,
            partition_by: part_cols,
            keys,
        });
        rel.node = node;
        Ok(rel)
    }

    fn apply_exchange(&mut self, mut rel: RelOut, distribute_by: &[Expr]) -> Result<RelOut> {
        let mut by = Vec::with_capacity(distribute_by.len());
        for e in distribute_by {
            let c = self.resolve_col_mut(&mut rel, e)?;
            if !by.contains(&c.name) {
                by.push(c.name);
            }
        }
        let node = self.push_node(PlanNode::Exchange {
            input: rel.node,
            by,
        });
        rel.node = node;
        Ok(rel)
    }

    fn apply_project_cols(
        &mut self,
        mut rel: RelOut,
        projected: &[(String, String)],
    ) -> Result<RelOut> {
        if projected.is_empty() {
            return Ok(rel);
        }
        let mut cols = Vec::with_capacity(projected.len());
        let mut out_infos = Vec::with_capacity(projected.len());
        for (src_name, alias_name) in projected {
            let c = self.ensure_col_on_scan_rel(&mut rel, None, src_name)?;
            if !alias_name.eq_ignore_ascii_case(&c.name) {
                return Err(refuse(
                    format!("column alias `AS {alias_name}` on `{}`", c.name),
                    "keeping dataset column names in `SELECT` (`ConsumerPlan`'s `PlanNode::Project` selects columns by their dataset name)",
                ));
            }
            if !cols.contains(&c.name) {
                cols.push(c.name.clone());
                out_infos.push(c);
            }
        }
        // Omit redundant `Project` when the input node already outputs the exact same column list
        // and is a `Latest` or `AsOf` node over a scan with those exact columns.
        let node = self.push_node(PlanNode::Project {
            input: rel.node,
            columns: cols,
        });
        rel.node = node;
        rel.columns = out_infos;
        Ok(rel)
    }

    #[allow(clippy::too_many_lines)]
    fn apply_select_projection(
        &mut self,
        mut rel: RelOut,
        projection: &[SelectItem],
        named_windows: &[NamedWindowDefinition],
    ) -> Result<RelOut> {
        let mut wildcard = false;
        let mut proj_cols: Vec<String> = Vec::new();
        let mut out_infos: Vec<ColInfo> = Vec::new();

        for item in projection {
            match item {
                SelectItem::Wildcard(_) => {
                    wildcard = true;
                    if rel.columns.is_empty() {
                        if let Some(so) = rel.scan_origins.first() {
                            return Err(Error::InvalidArgument {
                                field: "sql",
                                reason: format!(
                                    "`SELECT *` over dataset {:?} requires a known dataset schema; list the columns explicitly (`SELECT col1, col2 FROM ...`) or run on an indexed dataset",
                                    so.dataset
                                ),
                            });
                        }
                    }
                    for c in rel.columns.clone() {
                        self.record_scan_col_ref(c.scan_node, &c.source_col);
                        if !proj_cols.contains(&c.name) {
                            proj_cols.push(c.name.clone());
                            out_infos.push(c);
                        }
                    }
                }
                SelectItem::QualifiedWildcard(obj_name, _) => {
                    wildcard = true;
                    let q = object_table_name(obj_name);
                    let matching: Vec<ColInfo> = rel
                        .columns
                        .iter()
                        .filter(|c| {
                            c.qualifier
                                .as_deref()
                                .is_some_and(|cq| cq.eq_ignore_ascii_case(&q))
                        })
                        .cloned()
                        .collect();
                    if matching.is_empty() {
                        return Err(Error::InvalidArgument {
                            field: "sql",
                            reason: format!(
                                "`SELECT {q}.*` matches no columns in scope ( ensure dataset schema is known or list columns explicitly)"
                            ),
                        });
                    }
                    for c in matching {
                        self.record_scan_col_ref(c.scan_node, &c.source_col);
                        if !proj_cols.contains(&c.name) {
                            proj_cols.push(c.name.clone());
                            out_infos.push(c);
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    if self
                        .find_window_in_expr(&mut rel, expr, None, named_windows)?
                        .is_some()
                    {
                        continue;
                    }
                    let c = self.resolve_col_mut(&mut rel, expr)?;
                    if !proj_cols.contains(&c.name) {
                        proj_cols.push(c.name.clone());
                        out_infos.push(c);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let alias_str = ident_str(alias);
                    if self
                        .find_window_in_expr(
                            &mut rel,
                            expr,
                            Some(alias_str.clone()),
                            named_windows,
                        )?
                        .is_some()
                    {
                        continue;
                    }
                    let c = self.resolve_col_mut(&mut rel, expr)?;
                    if !alias_str.eq_ignore_ascii_case(&c.name) {
                        return Err(refuse(
                            format!("column alias `AS {alias_str}` on `{}`", c.name),
                            "keeping dataset column names in `SELECT` (`ConsumerPlan`'s `PlanNode::Project` selects columns by their dataset name)",
                        ));
                    }
                    if !proj_cols.contains(&c.name) {
                        proj_cols.push(c.name.clone());
                        out_infos.push(c);
                    }
                }
            }
        }

        if wildcard {
            // For `SELECT *`, no `PlanNode::Project` wrapper is needed if all columns of the input
            // are already kept (e.g. after `Scan`, `Filter`, `SemiJoin`, `Latest`, `AsOf`).
            rel.columns = out_infos;
            return Ok(rel);
        }

        if proj_cols.is_empty() {
            // e.g. `SELECT row_number() OVER (...) FROM ...` where only a window function was selected:
            // keep the input columns.
            return Ok(rel);
        }

        // If the input node is `Latest` or `AsOf` and `proj_cols` already equals all columns of `rel`,
        // still emit `Project` when explicit columns were listed unless `Latest`/`AsOf` is the root
        // with identical columns? Wait: in `bellwether_bench::relational::plans()`, `semijoin.json`,
        // `join.json`, `window.json` all end with `PlanNode::Project`, while `latest.json` was
        // `SELECT * FROM repos WHERE repo_id IN (:keys) QUALIFY row_number() ... = 1` (wildcard!).
        // When explicit columns are listed (`SELECT repo_id, type FROM ...`), emitting `PlanNode::Project`
        // matches `semijoin.json`, `join.json`, and `window.json`!
        // However, if `rel.node` is a bare `PlanNode::Scan` with the exact same columns in the same order,
        // a `Project` over a bare `Scan` with identical columns is redundant unless needed; wait:
        // does `SELECT a, b FROM t` need `Project` over `Scan(t, [a, b])` or is `Project` fine?
        // Let's check: in `bellwether_replay`, `Project` has zero overhead and `Scan` + `Project` is valid.
        // Wait: what if `SELECT a, b FROM t` is just a scan or scan+project? Wait: if someone writes
        // `SELECT repo_id FROM events`, in `relational.rs` tests, a simple scan is `Scan`, or if there are
        // other operators (`Filter`, `SemiJoin`, `Join`, `Window`), `Project` projects the selected columns
        // when `proj_cols` is a strict subset of input columns OR always when explicit columns are selected!
        let all_input_cols_same = rel.columns.len() == proj_cols.len()
            && rel
                .columns
                .iter()
                .zip(&proj_cols)
                .all(|(c, p)| c.name == *p);
        let is_bare_scan = matches!(
            self.nodes.get(rel.node as usize),
            Some(PlanNode::Scan { .. })
        );
        if is_bare_scan && all_input_cols_same {
            rel.columns = out_infos;
            return Ok(rel);
        }

        let node = self.push_node(PlanNode::Project {
            input: rel.node,
            columns: proj_cols,
        });
        rel.node = node;
        rel.columns = out_infos;
        Ok(rel)
    }

    fn finalize(mut self, root_rel: &RelOut) -> Result<ConsumerPlan> {
        // Ensure every Scan node has at least one column; if any Scan node has empty columns
        // (e.g. `COUNT(*)` over a table without other column references), populate from schema
        // or return a clear error.
        for (idx, node) in self.nodes.iter_mut().enumerate() {
            if let PlanNode::Scan { dataset, columns } = node {
                if columns.is_empty() {
                    if let Some(schema) = self.opts.schemas.get(dataset) {
                        if let Some(first) = schema.fields().first() {
                            columns.push(first.name().clone());
                        }
                    }
                }
                if columns.is_empty() {
                    return Err(Error::InvalidArgument {
                        field: "sql",
                        reason: format!(
                            "scan of dataset {dataset:?} (node {idx}) references no columns; name at least one column in `SELECT`, `WHERE`, or `GROUP BY`"
                        ),
                    });
                }
            }
        }

        // Compact unreachable nodes (e.g. an unused CTE) so `check_shape` succeeds,
        // while preserving topological order and shared-node DAG fanout (`consumers[i] >= 2`).
        let plan = compact_plan(&self.nodes, root_rel.node)?;
        check_shape(&plan).map_err(Error::from)?;
        Ok(plan)
    }
}

enum WhereStep {
    SemiJoin {
        columns: Vec<String>,
        keys: KeysFrom,
    },
    Pred(Predicate),
}

enum WindowExtract {
    Latest {
        part_cols: Vec<String>,
        ts_col: String,
        as_of: bool,
        alias: Option<String>,
    },
    Partition {
        part_cols: Vec<String>,
    },
}

fn collect_and_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_and_conjuncts(left, out);
            collect_and_conjuncts(right, out);
        }
        Expr::Nested(inner) => collect_and_conjuncts(inner, out),
        other => out.push(other),
    }
}

fn unwrap_nested(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

fn is_col_ref(expr: &Expr) -> bool {
    matches!(
        unwrap_nested(expr),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
    )
}

fn split_col_ref(expr: &Expr) -> Result<(Option<&str>, &str)> {
    match unwrap_nested(expr) {
        Expr::Identifier(id) => Ok((None, &id.value)),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [q, c] | [_, q, c] => Ok((Some(&q.value), &c.value)),
            _ => Err(refuse(
                format!("column reference `{expr}`"),
                "`col` or `table.col`",
            )),
        },
        other => Err(refuse(
            format!("expression `{other}` where a column name is required"),
            "`col` or `table.col`",
        )),
    }
}

fn matches_rel(rel: &RelOut, qualifier: Option<&str>, col_name: &str) -> bool {
    if rel.find_col(qualifier, col_name).is_ok() {
        return true;
    }
    if !rel.scan_origins.is_empty() {
        return match qualifier {
            Some(q) => rel.scan_origins.iter().any(|so| {
                q.eq_ignore_ascii_case(&so.qualifier) || q.eq_ignore_ascii_case(&so.dataset)
            }),
            None => rel.scan_origins.len() == 1,
        };
    }
    false
}

fn is_keys_table_factor(tf: &TableFactor, cte_defs: &BTreeMap<String, Cte>) -> bool {
    if let TableFactor::Table { name, .. } = tf {
        let raw = object_table_name(name);
        return raw.eq_ignore_ascii_case(KEYS_TABLE) && !cte_defs.contains_key(&raw);
    }
    false
}

fn table_factor_alias_or_name(tf: &TableFactor) -> Option<String> {
    match tf {
        TableFactor::Table { name, alias, .. } => Some(
            alias
                .as_ref()
                .map_or_else(|| object_table_name(name), |a| ident_str(&a.name)),
        ),
        TableFactor::Derived { alias, .. } => alias.as_ref().map(|a| ident_str(&a.name)),
        _ => None,
    }
}

fn is_select_from_keys_table(query: &Query, cte_defs: &BTreeMap<String, Cte>) -> bool {
    let SetExpr::Select(sel) = query.body.as_ref() else {
        return false;
    };
    sel.from.len() == 1
        && sel.from[0].joins.is_empty()
        && is_keys_table_factor(&sel.from[0].relation, cte_defs)
}

fn is_run_keys_placeholder(expr: &Expr) -> bool {
    match unwrap_nested(expr) {
        Expr::Value(SqlValue::Placeholder(s)) => {
            let t = s.trim_start_matches([':', '$', '?']);
            t.is_empty()
                || t.eq_ignore_ascii_case("keys")
                || t.eq_ignore_ascii_case("run")
                || t.chars().all(|c| c.is_ascii_digit())
        }
        Expr::Identifier(id) => {
            let v = id.value.as_str();
            v.starts_with(':') || v.starts_with('$') || v == "?"
        }
        Expr::Function(func) => {
            let name = object_table_name(&func.name);
            name.eq_ignore_ascii_case("run_keys")
        }
        _ => false,
    }
}

fn is_asof_placeholder(expr: &Expr) -> bool {
    match unwrap_nested(expr) {
        Expr::Value(SqlValue::Placeholder(s)) => {
            let t = s.trim_start_matches([':', '$', '?']);
            t.is_empty()
                || t.eq_ignore_ascii_case("at")
                || t.eq_ignore_ascii_case("as_of")
                || t.eq_ignore_ascii_case("asof")
                || t.chars().all(|c| c.is_ascii_digit())
        }
        Expr::Identifier(id) => {
            let v = id.value.as_str();
            v.starts_with(':') || v.starts_with('$') || v == "?"
        }
        _ => false,
    }
}

fn is_literal_one(expr: &Expr) -> bool {
    matches!(
        unwrap_nested(expr),
        Expr::Value(SqlValue::Number(n, _)) if n == "1"
    )
}

fn match_rn_eq_one(expr: &Expr) -> Option<String> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = unwrap_nested(expr)
    else {
        return None;
    };
    if is_literal_one(right) {
        if let Ok((_, col)) = split_col_ref(left) {
            return Some(col.to_string());
        }
    } else if is_literal_one(left) {
        if let Ok((_, col)) = split_col_ref(right) {
            return Some(col.to_string());
        }
    }
    None
}

/// Matches `col >= lo`, `lo <= col`, `col <= hi`, or `hi >= col`.
/// Returns `(col_expr, bound_expr, is_lower_bound, is_asof_placeholder)`.
fn match_range_bound(expr: &Expr) -> Option<(&Expr, &Expr, bool, bool)> {
    let Expr::BinaryOp { left, op, right } = unwrap_nested(expr) else {
        return None;
    };
    match op {
        BinaryOperator::GtEq => {
            if is_col_ref(left) && !is_col_ref(right) {
                Some((left, right, true, is_asof_placeholder(right)))
            } else if is_col_ref(right) && !is_col_ref(left) {
                Some((right, left, false, is_asof_placeholder(left)))
            } else {
                None
            }
        }
        BinaryOperator::LtEq => {
            if is_col_ref(left) && !is_col_ref(right) {
                Some((left, right, false, is_asof_placeholder(right)))
            } else if is_col_ref(right) && !is_col_ref(left) {
                Some((right, left, true, is_asof_placeholder(left)))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn select_has_aggregates(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            expr_has_aggregate(e)
        }
        _ => false,
    })
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match unwrap_nested(expr) {
        Expr::Function(func) if func.over.is_none() => {
            let name = object_table_name(&func.name).to_ascii_lowercase();
            matches!(
                name.as_str(),
                "count" | "sum" | "min" | "max" | "avg" | "arg_max" | "argmax" | "max_by"
            )
        }
        _ => false,
    }
}

fn resolve_window_spec<'a>(
    over: &'a WindowType,
    named_windows: &'a [NamedWindowDefinition],
) -> Result<&'a WindowSpec> {
    match over {
        WindowType::WindowSpec(spec) => Ok(spec),
        WindowType::NamedWindow(ident) => resolve_named_window(&ident.value, named_windows),
    }
}

fn resolve_named_window<'a>(
    name: &str,
    named_windows: &'a [NamedWindowDefinition],
) -> Result<&'a WindowSpec> {
    for nw in named_windows {
        if nw.0.value.eq_ignore_ascii_case(name) {
            match &nw.1 {
                NamedWindowExpr::WindowSpec(spec) => return Ok(spec),
                NamedWindowExpr::NamedWindow(other) => {
                    return resolve_named_window(&other.value, named_windows)
                }
            }
        }
    }
    Err(Error::InvalidArgument {
        field: "sql",
        reason: format!("unknown named window `{name}` in `OVER {name}`"),
    })
}

fn encode_sql_literal(
    expr: &Expr,
    col_name: &str,
    declared: Option<&DataType>,
    out: &mut Vec<u8>,
) -> Result<()> {
    let e = unwrap_nested(expr);
    match e {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            if let Expr::Value(SqlValue::Number(n, _)) = unwrap_nested(inner) {
                let neg = format!("-{n}");
                return encode_number_literal(&neg, col_name, declared, out);
            }
            Err(refuse(
                format!("literal expression `{expr}`"),
                "scalar literals (integers, decimals, strings, booleans, or RFC 3339 / YYYY-MM-DD timestamps)",
            ))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr: inner,
        }
        | Expr::Cast { expr: inner, .. } => encode_sql_literal(inner, col_name, declared, out),
        Expr::Value(val) => match val {
            SqlValue::Number(n, _) => encode_number_literal(n, col_name, declared, out),
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                encode_string_or_temporal_literal(s, col_name, declared, out)
            }
            SqlValue::Boolean(b) => {
                let v = EncValue::Boolean(*b);
                match declared {
                    Some(dt) => encoding::encode_value_as(&v, dt, out),
                    None => encoding::encode_value(&v, out),
                }
                .map_err(|err| Error::InvalidArgument {
                    field: "sql",
                    reason: format!("column {col_name:?}: {err}"),
                })
            }
            SqlValue::Null => Err(Error::InvalidArgument {
                field: "sql",
                reason: format!(
                    "NULL literal on column {col_name:?} matches no row under SQL three-valued logic (ADR 014)"
                ),
            }),
            other => Err(refuse(
                format!("literal `{other}`"),
                "scalar literals (integers, decimals, strings, booleans, or timestamps)",
            )),
        },
        Expr::TypedString { data_type, value } => {
            let dt_str = data_type.to_string().to_ascii_lowercase();
            if dt_str.contains("date") && !dt_str.contains("time") {
                let default_dt = DataType::Date32;
                let target = declared.unwrap_or(&default_dt);
                return encode_string_or_temporal_literal(value, col_name, Some(target), out);
            }
            if dt_str.contains("timestamp") {
                let default_dt = DataType::Timestamp(TimeUnit::Millisecond, None);
                let target = declared.unwrap_or(&default_dt);
                return encode_string_or_temporal_literal(value, col_name, Some(target), out);
            }
            encode_string_or_temporal_literal(value, col_name, declared, out)
        }
        other => Err(refuse(
            format!("non-literal value `{other}` in predicate on {col_name:?}"),
            "scalar literals or `:keys` placeholder",
        )),
    }
}

#[allow(clippy::too_many_lines)]
fn encode_number_literal(
    n: &str,
    col_name: &str,
    declared: Option<&DataType>,
    out: &mut Vec<u8>,
) -> Result<()> {
    if let Some(dt) = declared {
        match dt {
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                let (val, scale) = parse_decimal_literal(n, col_name)?;
                return encoding::encode_value_as(
                    &EncValue::Decimal256 { value: val, scale },
                    dt,
                    out,
                )
                .map_err(|e| Error::InvalidArgument {
                    field: "sql",
                    reason: format!("column {col_name:?}: {e}"),
                });
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                let f = n.parse::<f64>().map_err(|_| Error::InvalidArgument {
                    field: "sql",
                    reason: format!("{n:?} is not a valid float for column {col_name:?}"),
                })?;
                return encoding::encode_value_as(&EncValue::Float(f), dt, out).map_err(|e| {
                    Error::InvalidArgument {
                        field: "sql",
                        reason: format!("column {col_name:?}: {e}"),
                    }
                });
            }
            DataType::Timestamp(u, _) => {
                let v = n.parse::<i64>().map_err(|_| Error::InvalidArgument {
                    field: "sql",
                    reason: format!("{n:?} is not a valid integer timestamp for {col_name:?}"),
                })?;
                return encoding::encode_value_as(
                    &EncValue::Timestamp { value: v, unit: *u },
                    dt,
                    out,
                )
                .map_err(|e| Error::InvalidArgument {
                    field: "sql",
                    reason: format!("column {col_name:?}: {e}"),
                });
            }
            DataType::Date32 => {
                let v = n.parse::<i32>().map_err(|_| Error::InvalidArgument {
                    field: "sql",
                    reason: format!("{n:?} is not a valid Date32 day count for {col_name:?}"),
                })?;
                return encoding::encode_value_as(&EncValue::Date32(v), dt, out).map_err(|e| {
                    Error::InvalidArgument {
                        field: "sql",
                        reason: format!("column {col_name:?}: {e}"),
                    }
                });
            }
            DataType::Date64 => {
                let v = n.parse::<i64>().map_err(|_| Error::InvalidArgument {
                    field: "sql",
                    reason: format!(
                        "{n:?} is not a valid Date64 millisecond value for {col_name:?}"
                    ),
                })?;
                return encoding::encode_value_as(&EncValue::Date64(v), dt, out).map_err(|e| {
                    Error::InvalidArgument {
                        field: "sql",
                        reason: format!("column {col_name:?}: {e}"),
                    }
                });
            }
            _ => {}
        }
    }

    if n.contains('.') {
        let (val, scale) = parse_decimal_literal(n, col_name)?;
        let v = EncValue::Decimal256 { value: val, scale };
        match declared {
            Some(dt) => encoding::encode_value_as(&v, dt, out),
            None => encoding::encode_value(&v, out),
        }
        .map_err(|e| Error::InvalidArgument {
            field: "sql",
            reason: format!("column {col_name:?}: {e}"),
        })
    } else if let Ok(i) = n.parse::<i64>() {
        let v = EncValue::Int(i);
        match declared {
            Some(dt) => encoding::encode_value_as(&v, dt, out),
            None => encoding::encode_value(&v, out),
        }
        .map_err(|e| Error::InvalidArgument {
            field: "sql",
            reason: format!("column {col_name:?}: {e}"),
        })
    } else if let Ok(u) = n.parse::<u64>() {
        let v = EncValue::UInt(u);
        match declared {
            Some(dt) => encoding::encode_value_as(&v, dt, out),
            None => encoding::encode_value(&v, out),
        }
        .map_err(|e| Error::InvalidArgument {
            field: "sql",
            reason: format!("column {col_name:?}: {e}"),
        })
    } else {
        Err(Error::InvalidArgument {
            field: "sql",
            reason: format!("numeric literal {n:?} on column {col_name:?} is out of range"),
        })
    }
}

fn parse_decimal_literal(s: &str, col_name: &str) -> Result<(arrow::datatypes::i256, i8)> {
    let s = s.trim();
    let (digits, scale) = match s.split_once('.') {
        Some((int_part, frac_part)) => {
            let scale = i8::try_from(frac_part.len()).map_err(|_| Error::InvalidArgument {
                field: "sql",
                reason: format!(
                    "decimal literal {s:?} on {col_name:?} has too many fractional digits"
                ),
            })?;
            (format!("{int_part}{frac_part}"), scale)
        }
        None => (s.to_string(), 0i8),
    };
    let val =
        arrow::datatypes::i256::from_string(&digits).ok_or_else(|| Error::InvalidArgument {
            field: "sql",
            reason: format!("{s:?} is not a valid decimal literal on {col_name:?}"),
        })?;
    Ok((val, scale))
}

fn encode_string_or_temporal_literal(
    s: &str,
    col_name: &str,
    declared: Option<&DataType>,
    out: &mut Vec<u8>,
) -> Result<()> {
    if let Some(dt) = declared {
        match dt {
            DataType::Timestamp(u, _) => {
                let v = crate::index::instant_in_unit("sql", s, col_name, dt)?;
                return encoding::encode_value_as(
                    &EncValue::Timestamp { value: v, unit: *u },
                    dt,
                    out,
                )
                .map_err(|e| Error::InvalidArgument {
                    field: "sql",
                    reason: format!("column {col_name:?}: {e}"),
                });
            }
            DataType::Date32 => {
                let v = crate::index::instant_in_unit("sql", s, col_name, dt)?;
                let days = i32::try_from(v).map_err(|_| Error::InvalidArgument {
                    field: "sql",
                    reason: format!("date {s:?} is out of range for {col_name:?} (Date32)"),
                })?;
                return encoding::encode_value_as(&EncValue::Date32(days), dt, out).map_err(|e| {
                    Error::InvalidArgument {
                        field: "sql",
                        reason: format!("column {col_name:?}: {e}"),
                    }
                });
            }
            DataType::Date64 => {
                let v = crate::index::instant_in_unit("sql", s, col_name, dt)?;
                return encoding::encode_value_as(&EncValue::Date64(v), dt, out).map_err(|e| {
                    Error::InvalidArgument {
                        field: "sql",
                        reason: format!("column {col_name:?}: {e}"),
                    }
                });
            }
            _ => {}
        }
    }

    let v = EncValue::Utf8(s);
    match declared {
        Some(dt) => encoding::encode_value_as(&v, dt, out),
        None => encoding::encode_value(&v, out),
    }
    .map_err(|e| Error::InvalidArgument {
        field: "sql",
        reason: format!("column {col_name:?}: {e}"),
    })
}

fn dfs_compact(u: u32, nodes: &[PlanNode], visited: &mut [bool], post_order: &mut Vec<u32>) {
    if visited[u as usize] {
        return;
    }
    visited[u as usize] = true;
    for c in bellwether_replay::children(&nodes[u as usize]) {
        dfs_compact(c, nodes, visited, post_order);
    }
    post_order.push(u);
}

fn remap_pred(p: Predicate, remap: &[u32]) -> Predicate {
    match p {
        Predicate::SemiJoin { columns, keys_from } => Predicate::SemiJoin {
            columns,
            keys_from: match keys_from {
                KeysFrom::Node { node, columns } => KeysFrom::Node {
                    node: remap[node as usize],
                    columns,
                },
                other => other,
            },
        },
        Predicate::And { all } => Predicate::And {
            all: all.into_iter().map(|q| remap_pred(q, remap)).collect(),
        },
        other => other,
    }
}

/// Removes any nodes not reachable from `root` and remaps node indices in
/// topological post-order while preserving shared-node DAG structure.
#[allow(clippy::too_many_lines)]
fn compact_plan(nodes: &[PlanNode], root: u32) -> Result<ConsumerPlan> {
    let n = nodes.len();
    if root as usize >= n {
        return Err(Error::Other(format!(
            "plan root {root} is out of bounds for {n} nodes"
        )));
    }
    let mut visited = vec![false; n];
    let mut post_order = Vec::with_capacity(n);

    dfs_compact(root, nodes, &mut visited, &mut post_order);

    let mut remap = vec![u32::MAX; n];
    for (new_idx, &old_idx) in post_order.iter().enumerate() {
        remap[old_idx as usize] = u32::try_from(new_idx).unwrap_or(u32::MAX);
    }

    let remap_keys = |k: KeysFrom| -> KeysFrom {
        match k {
            KeysFrom::Node { node, columns } => KeysFrom::Node {
                node: remap[node as usize],
                columns,
            },
            other => other,
        }
    };
    let mut new_nodes = Vec::with_capacity(post_order.len());
    for &old_idx in &post_order {
        let node = match nodes[old_idx as usize].clone() {
            PlanNode::Scan { dataset, columns } => PlanNode::Scan { dataset, columns },
            PlanNode::Filter { input, predicate } => PlanNode::Filter {
                input: remap[input as usize],
                predicate: remap_pred(predicate, &remap),
            },
            PlanNode::Latest { input, columns, ts } => PlanNode::Latest {
                input: remap[input as usize],
                columns,
                ts,
            },
            PlanNode::AsOf { input, columns, ts } => PlanNode::AsOf {
                input: remap[input as usize],
                columns,
                ts,
            },
            PlanNode::SemiJoin {
                input,
                columns,
                keys,
            } => PlanNode::SemiJoin {
                input: remap[input as usize],
                columns,
                keys: remap_keys(keys),
            },
            PlanNode::Join {
                left,
                right,
                on,
                kind,
            } => PlanNode::Join {
                left: remap[left as usize],
                right: remap[right as usize],
                on,
                kind,
            },
            PlanNode::Window {
                input,
                partition_by,
                keys,
            } => PlanNode::Window {
                input: remap[input as usize],
                partition_by,
                keys: keys.map(remap_keys),
            },
            PlanNode::Aggregate {
                input,
                group_by,
                measures,
            } => PlanNode::Aggregate {
                input: remap[input as usize],
                group_by,
                measures,
            },
            PlanNode::Exchange { input, by } => PlanNode::Exchange {
                input: remap[input as usize],
                by,
            },
            PlanNode::Project { input, columns } => PlanNode::Project {
                input: remap[input as usize],
                columns,
            },
        };
        new_nodes.push(node);
    }

    Ok(ConsumerPlan {
        version: PLAN_VERSION,
        nodes: new_nodes,
        root: remap[root as usize],
    })
}

impl crate::engine::Engine {
    /// Translates a SQL `SELECT` query into a [`ConsumerPlan`] DAG, validates
    /// it against the engine's bound datasets when `req.validate` is set, and
    /// stores its canonical JSON in the engine's cache (`cache/<hash>.bin`)
    /// when `req.cache` is set and the store is writable.
    ///
    /// Never executes the SQL string.
    pub fn plan_from_sql(&self, req: &PlanFromSqlRequest) -> Result<PlanFromSqlResult> {
        crate::engine::guard(|| self.plan_from_sql_inner(req))
    }

    fn plan_from_sql_inner(&self, req: &PlanFromSqlRequest) -> Result<PlanFromSqlResult> {
        let mut schemas = BTreeMap::new();

        // Collect main dataset schema if available (from pinned index or source)
        let pinned = self.pin()?;
        let main_source = self.source(pinned.as_ref()).ok();
        if let Some(p) = &pinned {
            schemas.insert(
                MAIN_DATASET.to_string(),
                SchemaRef::clone(p.planner.schema()),
            );
        } else if let Some(src) = &main_source {
            if let Ok(s) = src.schema(self.root()) {
                schemas.insert(MAIN_DATASET.to_string(), s);
            }
        }

        // Also collect schemas of bound datasets in `self.options().datasets`
        for (name, root) in &self.options().datasets {
            let ds_opts = crate::engine::EngineOptions {
                format: self.options().format,
                clock: self.options().clock.clone(),
                threads: self.options().threads,
                ..crate::engine::EngineOptions::default()
            };
            if let Ok(ds_engine) = Self::open(root, ds_opts) {
                if ds_engine.root() == self.root() {
                    if let Some(s) = schemas.get(MAIN_DATASET).cloned() {
                        schemas.insert(name.clone(), s);
                    }
                } else if let Ok(ds_pinned) = ds_engine.pin() {
                    if let Some(p) = &ds_pinned {
                        schemas.insert(name.clone(), SchemaRef::clone(p.planner.schema()));
                    } else if let Ok(ds_src) = ds_engine.source(None) {
                        if let Ok(s) = ds_src.schema(ds_engine.root()) {
                            schemas.insert(name.clone(), s);
                        }
                    }
                }
            }
        }

        let opts = PlanFromSqlOptions {
            main_table: req.main_table.clone(),
            keys_from_run: req.keys_from_run,
            as_of: req.as_of,
            schemas,
        };
        let plan = plan_from_sql(&req.sql, &opts)?;

        // Determine the datasets scanned by `plan` (`main` first when present, then others sorted).
        let mut scanned_set: BTreeSet<String> = plan
            .nodes
            .iter()
            .filter_map(|n| match n {
                PlanNode::Scan { dataset, .. } => Some(dataset.clone()),
                _ => None,
            })
            .collect();
        let mut datasets = Vec::with_capacity(scanned_set.len());
        if scanned_set.remove(MAIN_DATASET) {
            datasets.push(MAIN_DATASET.to_string());
        }
        datasets.extend(scanned_set);

        if req.validate {
            let plan_datasets = self.datasets_for(&[], std::slice::from_ref(&plan))?;
            let src = match main_source {
                Some(s) => s,
                None => self.source(pinned.as_ref())?,
            };
            let replayer = crate::fix::replayer(self, &*src, pinned.as_ref())?;
            let replayer = plan_datasets.attach(replayer)?;
            replayer.check_plan(&plan).map_err(Error::from)?;
            let _ = self.finish_tables()?;
        }

        let (hash, _) = bellwether_replay::plan_payload(&plan)?;
        let mut cached = false;
        if req.cache && self.writable_store().is_some() {
            let cache = self.cache()?;
            bellwether_replay::store_plan(&cache, &plan)?;
            cached = true;
        }

        Ok(PlanFromSqlResult {
            hash,
            cached,
            datasets,
            plan,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bellwether_replay::check_shape;

    fn opts() -> PlanFromSqlOptions {
        PlanFromSqlOptions::default()
    }

    #[test]
    fn scan_filter_in_and_between_group_by_and_project() {
        // (a) scan + filter (IN (:keys) and range BETWEEN) + group by + project
        let sql = "
            SELECT repo_id, COUNT(*), SUM(amount)
            FROM events
            WHERE repo_id IN (:keys) AND created_at BETWEEN 1000 AND 5000
            GROUP BY repo_id
        ";
        let plan = plan_from_sql(sql, &opts()).unwrap();
        check_shape(&plan).unwrap();
        assert_eq!(plan.version, 1);
        assert_eq!(
            plan.nodes,
            vec![
                PlanNode::Scan {
                    dataset: "main".into(),
                    columns: vec!["repo_id".into(), "created_at".into(), "amount".into()],
                },
                PlanNode::SemiJoin {
                    input: 0,
                    columns: vec!["repo_id".into()],
                    keys: KeysFrom::Run,
                },
                PlanNode::Filter {
                    input: 1,
                    predicate: Predicate::Between {
                        column: "created_at".into(),
                        lo: {
                            let mut b = Vec::new();
                            encoding::encode_value(&EncValue::Int(1000), &mut b).unwrap();
                            b
                        },
                        hi: {
                            let mut b = Vec::new();
                            encoding::encode_value(&EncValue::Int(5000), &mut b).unwrap();
                            b
                        },
                    },
                },
                PlanNode::Aggregate {
                    input: 2,
                    group_by: vec!["repo_id".into()],
                    measures: vec![
                        Measure {
                            function: AggregateFn::Count,
                            column: None,
                        },
                        Measure {
                            function: AggregateFn::Sum,
                            column: Some("amount".into()),
                        },
                    ],
                },
            ]
        );
        assert_eq!(plan.root, 3);

        // Also test scan + filter + project without group by
        let sql_proj = "
            SELECT repo_id, type
            FROM events
            WHERE repo_id IN (:keys) AND amount BETWEEN 10 AND 200
        ";
        let plan_proj = plan_from_sql(sql_proj, &opts()).unwrap();
        check_shape(&plan_proj).unwrap();
        assert_eq!(
            plan_proj
                .nodes
                .iter()
                .map(bellwether_replay::op_name)
                .collect::<Vec<_>>(),
            ["scan", "semi_join", "filter", "project"]
        );
    }

    #[test]
    fn two_table_join_translates_to_join_dag() {
        // (b) a two-table join (events JOIN repos ON ...)
        let mut o = opts();
        o.main_table = Some("events".into());
        let sql = "
            SELECT e.repo_id, e.type, r.name
            FROM events e
            JOIN repos r ON e.repo_id = r.repo_id
            WHERE e.repo_id IN (:keys)
        ";
        let plan = plan_from_sql(sql, &o).unwrap();
        check_shape(&plan).unwrap();
        assert_eq!(
            plan.nodes,
            vec![
                PlanNode::Scan {
                    dataset: "main".into(),
                    columns: vec!["repo_id".into(), "type".into()],
                },
                PlanNode::SemiJoin {
                    input: 0,
                    columns: vec!["repo_id".into()],
                    keys: KeysFrom::Run,
                },
                PlanNode::Scan {
                    dataset: "repos".into(),
                    columns: vec!["repo_id".into(), "name".into()],
                },
                PlanNode::Join {
                    left: 1,
                    right: 2,
                    on: vec![("repo_id".into(), "repo_id".into())],
                    kind: JoinKind::Inner,
                },
                PlanNode::Project {
                    input: 3,
                    columns: vec!["repo_id".into(), "type".into(), "name".into()],
                },
            ]
        );
    }

    #[test]
    fn cte_referenced_twice_produces_shared_dag_node() {
        // (c) a CTE referenced twice producing a shared DAG node (consumers[i] == 2)
        let sql = "
            WITH filtered AS (
                SELECT repo_id, type, amount
                FROM events
                WHERE repo_id IN (:keys)
            )
            SELECT a.repo_id, a.type
            FROM filtered a
            WHERE a.repo_id IN (SELECT repo_id FROM filtered)
        ";
        let plan = plan_from_sql(sql, &opts()).unwrap();
        let order = check_shape(&plan).unwrap();
        let mut consumers = vec![0u32; plan.nodes.len()];
        for &v in &order {
            for c in bellwether_replay::children(&plan.nodes[v as usize]) {
                consumers[c as usize] += 1;
            }
        }
        // The CTE root node must have `consumers[i] == 2`!
        assert!(
            consumers.contains(&2),
            "expected a shared node with 2 consumers, got {consumers:?} for nodes {:?}",
            plan.nodes
        );
    }

    #[test]
    fn qualify_row_number_translates_to_latest_and_asof() {
        // (d) QUALIFY row_number() OVER (PARTITION BY id ORDER BY ts DESC) = 1
        let sql = "
            SELECT repo_id, name, created_at, stars
            FROM repos
            WHERE repo_id IN (:keys)
            QUALIFY row_number() OVER (PARTITION BY repo_id ORDER BY created_at DESC) = 1
        ";
        let plan = plan_from_sql(sql, &opts()).unwrap();
        check_shape(&plan).unwrap();
        assert!(
            plan.nodes.iter().any(|n| matches!(
                n,
                PlanNode::Latest { columns, ts, .. }
                if columns == &vec!["repo_id".to_string()] && ts == "created_at"
            )),
            "expected Latest node in {:?}",
            plan.nodes
        );

        // With `as_of: true`, translates to `PlanNode::AsOf`
        let mut asof_opts = opts();
        asof_opts.as_of = true;
        let plan_asof = plan_from_sql(sql, &asof_opts).unwrap();
        check_shape(&plan_asof).unwrap();
        assert!(
            plan_asof.nodes.iter().any(|n| matches!(
                n,
                PlanNode::AsOf { columns, ts, .. }
                if columns == &vec!["repo_id".to_string()] && ts == "created_at"
            )),
            "expected AsOf node in {:?}",
            plan_asof.nodes
        );

        // With `created_at <= :at` in WHERE, translates to `PlanNode::AsOf` automatically
        let sql_with_at = "
            SELECT repo_id, name, created_at
            FROM repos
            WHERE repo_id IN (:keys) AND created_at <= :at
            QUALIFY row_number() OVER (PARTITION BY repo_id ORDER BY created_at DESC) = 1
        ";
        let plan_at = plan_from_sql(sql_with_at, &opts()).unwrap();
        check_shape(&plan_at).unwrap();
        assert!(
            plan_at.nodes.iter().any(|n| matches!(
                n,
                PlanNode::AsOf { columns, ts, .. }
                if columns == &vec!["repo_id".to_string()] && ts == "created_at"
            )),
            "expected AsOf node in {:?}",
            plan_at.nodes
        );

        // Subquery with `row_number() OVER (...) AS rn` and outer `WHERE rn = 1`
        let sql_subq = "
            SELECT repo_id, name
            FROM (
                SELECT repo_id, name, created_at,
                       row_number() OVER (PARTITION BY repo_id ORDER BY created_at DESC) AS rn
                FROM repos
                WHERE repo_id IN (:keys)
            ) t
            WHERE rn = 1
        ";
        let plan_subq = plan_from_sql(sql_subq, &opts()).unwrap();
        check_shape(&plan_subq).unwrap();
        assert!(
            plan_subq.nodes.iter().any(|n| matches!(
                n,
                PlanNode::Latest { columns, ts, .. }
                if columns == &vec!["repo_id".to_string()] && ts == "created_at"
            )),
            "expected Latest node in {:?}",
            plan_subq.nodes
        );
    }

    #[test]
    fn unsupported_sql_is_refused_by_name() {
        // (f) unsupported SQL (UNION, HAVING, LIMIT, SELECT without FROM) refused by name
        let cases = [
            ("SELECT id FROM a UNION SELECT id FROM b", "UNION"),
            (
                "SELECT repo_id, COUNT(*) FROM events GROUP BY repo_id HAVING COUNT(*) > 5",
                "HAVING",
            ),
            ("SELECT repo_id FROM events LIMIT 10", "LIMIT"),
            ("SELECT 1", "without a `FROM` clause"),
            ("SELECT DISTINCT repo_id FROM events", "DISTINCT"),
            ("SELECT repo_id FROM events ORDER BY repo_id", "ORDER BY"),
            (
                "SELECT repo_id FROM events WHERE repo_id = 1 OR repo_id = 2",
                "OR",
            ),
        ];
        for (sql, expected) in cases {
            let err = plan_from_sql(sql, &opts()).unwrap_err();
            match &err {
                Error::InvalidArgument { field, reason } => {
                    assert_eq!(*field, "sql", "{sql}");
                    assert!(
                        reason.contains(expected),
                        "for {sql:?}, expected {expected:?} in {reason:?}"
                    );
                    assert!(
                        reason.contains("ConsumerPlan"),
                        "for {sql:?}, expected actionable ConsumerPlan guidance in {reason:?}"
                    );
                }
                other => panic!("expected InvalidArgument for {sql:?}, got {other:?}"),
            }
        }
    }
}
