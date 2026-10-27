//! The executable DataFusion scan (feature `datafusion`): the
//! `FileScanConfig` with one `ParquetAccessPlan` per file, a table provider
//! whose scan no optimizer prunes, and the residual as a logical plan.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use bellwether_core::encoding::Scalar;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::provider_as_source;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    lit, Expr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder, SortExpr, TableType,
};
use datafusion::object_store::ObjectStore;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, ReplaceChildrenOptions,
};
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use super::{udf, ScanConfig, POSITION_COLUMN, ROW_COLUMN};
use crate::layout::partition_scalar;
use crate::residual::{column_type, value_type, Cond, Stage};
use crate::{AdapterError, Result};

const SCAN: &str = "bw";
const BOUND: &str = "__bellwether_bound";
const RANK: &str = "__bellwether_rank";

fn exec_err(e: impl fmt::Display) -> AdapterError {
    AdapterError::Execution(e.to_string())
}

/// The object store URL and the in-store path of a scan location.
fn split_location(location: &str) -> (String, String) {
    if let Some((scheme, rest)) = location.split_once("://") {
        if scheme == "file" {
            return ("file://".into(), rest.to_string());
        }
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        return (format!("{scheme}://{authority}"), path.to_string());
    }
    ("file://".into(), location.to_string())
}

impl ScanConfig {
    /// The object store URL every scanned file is read through.
    pub fn object_store_url(&self) -> Result<ObjectStoreUrl> {
        let mut url: Option<String> = None;
        for f in &self.files {
            let (u, _) = split_location(&f.location);
            match &url {
                Some(prev) if *prev != u => {
                    return Err(AdapterError::Unsupported {
                        engine: "datafusion",
                        what: "a plan over two object stores".into(),
                        reason: format!("reads {prev} and {u}, and one scan reads one store"),
                        next: "relocate the files to one store".into(),
                    })
                }
                _ => url = Some(u),
            }
        }
        ObjectStoreUrl::parse(url.unwrap_or_else(|| "file://".into())).map_err(exec_err)
    }

    fn file_fields(&self) -> Vec<FieldRef> {
        self.schema
            .iter()
            .filter(|c| !self.table_partition_cols.contains(&c.name))
            .map(|c| Arc::new(Field::new(&c.name, c.data_type.clone(), true)))
            .collect()
    }

    fn table_schema(&self) -> Result<TableSchema> {
        let file_schema = Arc::new(Schema::new(self.file_fields()));
        let mut parts: Vec<FieldRef> = Vec::new();
        for c in &self.table_partition_cols {
            let dt = column_type(&self.schema, c, "a partition column")?;
            parts.push(Arc::new(Field::new(c, dt.clone(), true)));
        }
        let mut virtuals: Vec<FieldRef> = Vec::new();
        if self.ordered {
            parts.push(Arc::new(Field::new(
                POSITION_COLUMN,
                DataType::UInt32,
                false,
            )));
            virtuals.push(Arc::new(
                Field::new(ROW_COLUMN, DataType::Int64, true)
                    .with_extension_type(parquet::arrow::RowNumber),
            ));
        }
        Ok(TableSchema::builder(file_schema)
            .with_table_partition_cols(parts)
            .with_virtual_columns(Fields::from(virtuals))
            .build())
    }

    /// The executable scan: every file with its `ParquetAccessPlan`, its
    /// `metadata_size_hint`, and its partition values, the Parquet options
    /// of [`super::ParquetOptions`], projected to `projection` (indices into
    /// the table schema: file columns, partition columns, then the hidden
    /// position and row-number columns of an ordered plan).
    pub fn file_scan_config(&self, projection: Option<Vec<usize>>) -> Result<FileScanConfig> {
        let table_schema = self.table_schema()?;
        let mut options = TableParquetOptions::default();
        options.global.pruning = self.parquet.pruning;
        options.global.enable_page_index = self.parquet.enable_page_index;
        options.global.bloom_filter_on_read = self.parquet.bloom_filter_on_read;
        options.global.pushdown_filters = self.parquet.pushdown_filters;
        let source = ParquetSource::new(table_schema).with_table_parquet_options(options);
        let types = self
            .table_partition_cols
            .iter()
            .map(|c| {
                column_type(&self.schema, c, "a partition column").map(|t| value_type(t).clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let mut partitioned = Vec::with_capacity(self.files.len());
        for f in &self.files {
            let (_, path) = split_location(&f.location);
            let mut values = Vec::with_capacity(types.len() + 1);
            for ((c, v), t) in self
                .table_partition_cols
                .iter()
                .zip(&f.partition_values)
                .zip(&types)
            {
                values.push(match partition_scalar(c, v.as_deref(), t)? {
                    Some(s) => scalar_value(&s, t)?,
                    None => ScalarValue::try_from(t).map_err(exec_err)?,
                });
            }
            if let Some(pos) = f.position {
                values.push(ScalarValue::UInt32(Some(pos)));
            }
            let mut access = ParquetAccessPlan::new_none(f.access.len());
            for (i, a) in f.access.iter().enumerate() {
                if *a == "scan" {
                    access.scan(i);
                }
            }
            let mut pf = PartitionedFile::new(path, f.size)
                .with_partition_values(values)
                .with_extension(access);
            if let Some(h) = f.metadata_size_hint {
                pf = pf.with_metadata_size_hint(usize::try_from(h).map_err(exec_err)?);
            }
            partitioned.push(pf);
        }
        let groups: Vec<FileGroup> = self
            .file_groups
            .iter()
            .map(|g| FileGroup::new(g.iter().map(|&i| partitioned[i].clone()).collect()))
            .collect();
        let builder = FileScanConfigBuilder::new(self.object_store_url()?, Arc::new(source))
            .with_file_groups(groups)
            .with_projection_indices(projection)
            .map_err(exec_err)?;
        Ok(builder.build())
    }
}

/// `s`, a value of dataset type `dt`, as a DataFusion literal of exactly
/// that type.
pub(crate) fn scalar_value(s: &Scalar, dt: &DataType) -> Result<ScalarValue> {
    let mismatch = || AdapterError::Plan(format!("a {s:?} value for a {dt} column"));
    let tz = |t: &Option<Arc<str>>| t.clone();
    #[allow(clippy::cast_possible_truncation)] // decode range-checked every value
    Ok(match (s, value_type(dt)) {
        (Scalar::Boolean(b), DataType::Boolean) => ScalarValue::Boolean(Some(*b)),
        (Scalar::Int(v), DataType::Int8) => ScalarValue::Int8(Some(*v as i8)),
        (Scalar::Int(v), DataType::Int16) => ScalarValue::Int16(Some(*v as i16)),
        (Scalar::Int(v), DataType::Int32) => ScalarValue::Int32(Some(*v as i32)),
        (Scalar::Int(v), DataType::Int64) => ScalarValue::Int64(Some(*v)),
        (Scalar::UInt(v), DataType::UInt8) => ScalarValue::UInt8(Some(*v as u8)),
        (Scalar::UInt(v), DataType::UInt16) => ScalarValue::UInt16(Some(*v as u16)),
        (Scalar::UInt(v), DataType::UInt32) => ScalarValue::UInt32(Some(*v as u32)),
        (Scalar::UInt(v), DataType::UInt64) => ScalarValue::UInt64(Some(*v)),
        (Scalar::Float(f), DataType::Float16) => ScalarValue::Float16(Some(udf::F16::from_f64(*f))),
        (Scalar::Float(f), DataType::Float32) => ScalarValue::Float32(Some(*f as f32)),
        (Scalar::Float(f), DataType::Float64) => ScalarValue::Float64(Some(*f)),
        (Scalar::Decimal128 { value, .. }, DataType::Decimal128(p, s)) => {
            ScalarValue::Decimal128(Some(*value), *p, *s)
        }
        (Scalar::Decimal128 { value, .. }, DataType::Decimal256(p, s)) => {
            ScalarValue::Decimal256(Some(arrow::datatypes::i256::from_i128(*value)), *p, *s)
        }
        (Scalar::Decimal256 { value, .. }, DataType::Decimal256(p, s)) => {
            ScalarValue::Decimal256(Some(*value), *p, *s)
        }
        (Scalar::Utf8(v), DataType::Utf8) => ScalarValue::Utf8(Some(v.clone())),
        (Scalar::Utf8(v), DataType::LargeUtf8) => ScalarValue::LargeUtf8(Some(v.clone())),
        (Scalar::Utf8(v), DataType::Utf8View) => ScalarValue::Utf8View(Some(v.clone())),
        (Scalar::Binary(v), DataType::Binary) => ScalarValue::Binary(Some(v.clone())),
        (Scalar::Binary(v), DataType::LargeBinary) => ScalarValue::LargeBinary(Some(v.clone())),
        (Scalar::Binary(v), DataType::BinaryView) => ScalarValue::BinaryView(Some(v.clone())),
        (Scalar::Binary(v), DataType::FixedSizeBinary(n)) => {
            ScalarValue::FixedSizeBinary(*n, Some(v.clone()))
        }
        (Scalar::Date32(d), DataType::Date32) => ScalarValue::Date32(Some(*d)),
        (Scalar::Date64(d), DataType::Date64) => ScalarValue::Date64(Some(*d)),
        (Scalar::Time { value, .. }, DataType::Time32(TimeUnit::Second)) => {
            ScalarValue::Time32Second(Some(*value as i32))
        }
        (Scalar::Time { value, .. }, DataType::Time32(TimeUnit::Millisecond)) => {
            ScalarValue::Time32Millisecond(Some(*value as i32))
        }
        (Scalar::Time { value, .. }, DataType::Time64(TimeUnit::Microsecond)) => {
            ScalarValue::Time64Microsecond(Some(*value))
        }
        (Scalar::Time { value, .. }, DataType::Time64(TimeUnit::Nanosecond)) => {
            ScalarValue::Time64Nanosecond(Some(*value))
        }
        (Scalar::Timestamp { value, .. }, DataType::Timestamp(u, z)) => match u {
            TimeUnit::Second => ScalarValue::TimestampSecond(Some(*value), tz(z)),
            TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(*value), tz(z)),
            TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(*value), tz(z)),
            TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(*value), tz(z)),
        },
        _ => return Err(mismatch()),
    })
}

/// The scan as a table: its `scan` builds [`ScanConfig::file_scan_config`]
/// and, with engine pruning off, hides it behind a leaf no optimizer pushes
/// a filter, a limit, or a repartitioning into.
#[derive(Debug)]
struct PinnedTable {
    config: ScanConfig,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for PinnedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let fsc = self
            .config
            .file_scan_config(projection.cloned())
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
        let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(fsc);
        if self.config.parquet.pruning {
            Ok(exec)
        } else {
            Ok(Arc::new(PinnedScanExec {
                properties: Arc::clone(exec.properties()),
                inner: exec,
            }))
        }
    }
}

/// A leaf over the pinned scan: it pushes nothing down and repartitions
/// nothing, so the scan reads exactly its access plans.
#[derive(Debug)]
struct PinnedScanExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for PinnedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BellwetherPinnedScan")
    }
}

impl ExecutionPlan for PinnedScanExec {
    fn name(&self) -> &'static str {
        "BellwetherPinnedScan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>,
    ) -> datafusion::error::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

fn scan_col(name: &str) -> Expr {
    Expr::Column(Column::new(Some(TableReference::bare(SCAN)), name))
}

fn is_float(dt: &DataType) -> bool {
    matches!(
        value_type(dt),
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// A key expression compared as canonical bytes compare: a float column
/// as its canonical order key (`udf::canonical_order`), since DataFusion's
/// `=` and hash join compare `-0.0` equal to `+0.0`, and the plan does
/// not; any other column as it is.
fn key_expr(e: Expr, dt: &DataType) -> Expr {
    if is_float(dt) {
        udf::canonical_order(value_type(dt)).call(vec![e])
    } else {
        e
    }
}

/// A literal compared against [`key_expr`] of a `dt` column.
fn key_lit(s: &Scalar, dt: &DataType) -> Result<Expr> {
    match s {
        Scalar::Float(f) if is_float(dt) => {
            Ok(lit(ScalarValue::UInt64(Some(udf::float_order(*f)))))
        }
        _ => scalar_value(s, dt).map(lit),
    }
}

/// Registers `store` as the object store the scan reads through (a
/// [`super::CountingStore`] to measure a run).
pub fn register(
    ctx: &SessionContext,
    config: &ScanConfig,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let url = config.object_store_url()?;
    ctx.register_object_store(url.as_ref(), store);
    Ok(())
}

/// The residual over the pinned scan as one logical plan, stage by stage
/// in the plan's order (see the module docs of [`super`]).
#[allow(clippy::too_many_lines)]
pub async fn logical_plan(config: &ScanConfig, ctx: &SessionContext) -> Result<LogicalPlan> {
    let table_schema = config.table_schema()?;
    let schema = Arc::clone(table_schema.table_schema());
    let provider = Arc::new(PinnedTable {
        config: config.clone(),
        schema: Arc::clone(&schema),
    });
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let keep = |b: LogicalPlanBuilder| -> Result<LogicalPlanBuilder> {
        b.project(columns.iter().map(|c| scan_col(c)).collect::<Vec<_>>())
            .map_err(exec_err)
    };
    let mut b =
        LogicalPlanBuilder::scan(SCAN, provider_as_source(provider), None).map_err(exec_err)?;
    let dataset = &config.schema;
    for (n, stage) in config.compiled.stages.iter().enumerate() {
        b = match stage {
            Stage::Filter(c) => match c {
                Cond::In {
                    columns: keys,
                    types,
                    rows,
                    ..
                } => {
                    let use_join = config.keys_file.is_some()
                        || keys.len() > 1
                        || rows.len() > crate::duckdb::INLINE_IN_MAX;
                    if rows.is_empty() && config.keys_file.is_none() {
                        b.filter(lit(false)).map_err(exec_err)?
                    } else if !use_join {
                        let lits = rows
                            .iter()
                            .map(|r| key_lit(&r[0], &types[0]))
                            .collect::<Result<Vec<_>>>()?;
                        b.filter(key_expr(scan_col(&keys[0]), &types[0]).in_list(lits, false))
                            .map_err(exec_err)?
                    } else {
                        let alias = format!("bw_k{n}");
                        let right = if let Some(path) = &config.keys_file {
                            let p = path.display().to_string();
                            ctx.read_parquet(p, ParquetReadOptions::default())
                                .await
                                .map_err(exec_err)?
                                .into_unoptimized_plan()
                        } else {
                            let values = rows
                                .iter()
                                .map(|r| {
                                    r.iter()
                                        .zip(types)
                                        .map(|(s, t)| scalar_value(s, t).map(lit))
                                        .collect::<Result<Vec<_>>>()
                                })
                                .collect::<Result<Vec<_>>>()?;
                            let v = LogicalPlanBuilder::values(values).map_err(exec_err)?;
                            let names: Vec<Expr> = keys
                                .iter()
                                .enumerate()
                                .map(|(i, k)| {
                                    Expr::Column(Column::from_name(format!("column{}", i + 1)))
                                        .alias(k)
                                })
                                .collect();
                            v.project(names)
                                .map_err(exec_err)?
                                .build()
                                .map_err(exec_err)?
                        };
                        let right = LogicalPlanBuilder::new(right)
                            .alias(alias.as_str())
                            .map_err(exec_err)?
                            .build()
                            .map_err(exec_err)?;
                        let lk = keys
                            .iter()
                            .zip(types)
                            .map(|(k, t)| key_expr(scan_col(k), t))
                            .collect::<Vec<_>>();
                        let rk = keys
                            .iter()
                            .zip(types)
                            .map(|(k, t)| {
                                key_expr(
                                    Expr::Column(Column::new(
                                        Some(TableReference::bare(alias.as_str())),
                                        k,
                                    )),
                                    t,
                                )
                            })
                            .collect::<Vec<_>>();
                        keep(
                            b.join_with_expr_keys(right, JoinType::LeftSemi, (lk, rk), None)
                                .map_err(exec_err)?,
                        )?
                    }
                }
                Cond::Between {
                    column,
                    data_type,
                    lo,
                    hi,
                } => b
                    .filter(
                        key_expr(scan_col(column), data_type)
                            .between(key_lit(lo, data_type)?, key_lit(hi, data_type)?),
                    )
                    .map_err(exec_err)?,
                Cond::Text {
                    column,
                    operator,
                    values,
                } => {
                    let dt = value_type(column_type(dataset, column, "Text")?);
                    let f = udf::text(&format!("bellwether_text_{n}"), dt, *operator, values);
                    b.filter(f.call(vec![scan_col(column)])).map_err(exec_err)?
                }
                Cond::ExprIn {
                    expr, inputs, raw, ..
                } => {
                    let arrow_schema = crate::residual::arrow_schema(dataset);
                    let typed = bellwether_core::expr::Typed::parse(expr, &arrow_schema)
                        .map_err(|e| AdapterError::Plan(e.to_string()))?;
                    let ins = inputs
                        .iter()
                        .map(|c| column_type(dataset, c, "ExprIn").map(|t| (c.clone(), t.clone())))
                        .collect::<Result<Vec<_>>>()?;
                    let set: HashSet<Vec<u8>> = raw.iter().cloned().collect();
                    let f = udf::expr_in(&format!("bellwether_expr_{n}"), typed, &ins, set);
                    b.filter(f.call(inputs.iter().map(|c| scan_col(c)).collect()))
                        .map_err(exec_err)?
                }
                Cond::ContainsAny {
                    column,
                    element,
                    raw,
                    ..
                } => {
                    let dt = column_type(dataset, column, "ContainsAny")?;
                    let set: HashSet<Vec<u8>> = raw.iter().cloned().collect();
                    let f = udf::contains(
                        &format!("bellwether_contains_{n}"),
                        dt,
                        element.clone(),
                        vec![set],
                    );
                    b.filter(f.call(vec![scan_col(column)])).map_err(exec_err)?
                }
                Cond::ContainsAll {
                    column,
                    element,
                    raw,
                    ..
                } => {
                    let dt = column_type(dataset, column, "ContainsAll")?;
                    let groups = raw
                        .iter()
                        .map(|g| g.iter().cloned().collect::<HashSet<_>>())
                        .collect();
                    let f = udf::contains(
                        &format!("bellwether_contains_{n}"),
                        dt,
                        element.clone(),
                        groups,
                    );
                    b.filter(f.call(vec![scan_col(column)])).map_err(exec_err)?
                }
                Cond::Live {
                    column,
                    data_type,
                    tombstone,
                } => {
                    let c = key_expr(scan_col(column), data_type);
                    b.filter(
                        c.clone()
                            .is_not_null()
                            .and(c.not_eq(key_lit(tombstone, data_type)?)),
                    )
                    .map_err(exec_err)?
                }
            },
            Stage::NotOlderThan {
                columns: keys,
                types,
                ts_column,
                ts_type,
                bounds,
            } => {
                let rows = bounds
                    .iter()
                    .filter(|bd| bd.key.iter().all(Option::is_some))
                    .map(|bd| {
                        let mut row = bd
                            .key
                            .iter()
                            .zip(types)
                            .map(|(k, t)| {
                                let k = k
                                    .as_ref()
                                    .ok_or_else(|| AdapterError::Plan("null key".into()))?;
                                scalar_value(k, t).map(lit)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        row.push(lit(scalar_value(&bd.ts, ts_type)?));
                        Ok(row)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if rows.is_empty() {
                    b
                } else {
                    let alias = format!("bw_b{n}");
                    let mut names: Vec<Expr> = keys
                        .iter()
                        .enumerate()
                        .map(|(i, k)| {
                            Expr::Column(Column::from_name(format!("column{}", i + 1))).alias(k)
                        })
                        .collect();
                    names.push(
                        Expr::Column(Column::from_name(format!("column{}", keys.len() + 1)))
                            .alias(BOUND),
                    );
                    let right = LogicalPlanBuilder::values(rows)
                        .map_err(exec_err)?
                        .project(names)
                        .map_err(exec_err)?
                        .alias(alias.as_str())
                        .map_err(exec_err)?
                        .build()
                        .map_err(exec_err)?;
                    let rcol = |c: &str| {
                        Expr::Column(Column::new(Some(TableReference::bare(alias.as_str())), c))
                    };
                    let lk = keys
                        .iter()
                        .zip(types)
                        .map(|(k, t)| key_expr(scan_col(k), t))
                        .collect::<Vec<_>>();
                    let rk = keys
                        .iter()
                        .zip(types)
                        .map(|(k, t)| key_expr(rcol(k), t))
                        .collect::<Vec<_>>();
                    let joined = b
                        .join_with_expr_keys(right, JoinType::Left, (lk, rk), None)
                        .map_err(exec_err)?
                        .filter(
                            rcol(BOUND)
                                .is_null()
                                .or(key_expr(scan_col(ts_column), ts_type)
                                    .gt_eq(key_expr(rcol(BOUND), ts_type))),
                        )
                        .map_err(exec_err)?;
                    keep(joined)?
                }
            }
            Stage::LatestPerKey {
                columns: keys,
                ts_column,
            } => {
                let mut not_null = scan_col(ts_column).is_not_null();
                for k in keys {
                    not_null = not_null.and(scan_col(k).is_not_null());
                }
                let partition = keys
                    .iter()
                    .map(|k| {
                        column_type(dataset, k, "LatestPerKey").map(|t| key_expr(scan_col(k), t))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let rank = datafusion::functions_window::expr_fn::rank()
                    .partition_by(partition)
                    .order_by(vec![SortExpr::new(
                        key_expr(
                            scan_col(ts_column),
                            column_type(dataset, ts_column, "LatestPerKey")?,
                        ),
                        false,
                        true,
                    )])
                    .build()
                    .map_err(exec_err)?
                    .alias(RANK);
                let ranked = b
                    .filter(not_null)
                    .map_err(exec_err)?
                    .window(vec![rank])
                    .map_err(exec_err)?
                    .filter(Expr::Column(Column::from_name(RANK)).eq(lit(1u64)))
                    .map_err(exec_err)?;
                keep(ranked)?
            }
        };
    }
    if config.ordered {
        b = b
            .sort(vec![
                SortExpr::new(scan_col(POSITION_COLUMN), true, false),
                SortExpr::new(scan_col(ROW_COLUMN), true, false),
            ])
            .map_err(exec_err)?;
    }
    b.project(
        config
            .projection
            .iter()
            .map(|c| scan_col(c))
            .collect::<Vec<_>>(),
    )
    .map_err(exec_err)?
    .build()
    .map_err(exec_err)
}

/// Runs the scan and its residual in `ctx` (register a
/// [`super::CountingStore`] with [`register`] first to measure it).
pub async fn execute(
    config: &ScanConfig,
    ctx: &SessionContext,
) -> Result<SendableRecordBatchStream> {
    let plan = logical_plan(config, ctx).await?;
    ctx.execute_logical_plan(plan)
        .await
        .map_err(exec_err)?
        .execute_stream()
        .await
        .map_err(exec_err)
}
