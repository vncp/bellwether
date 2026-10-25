//! [`SqlSource`]: a `Source` over one or more tables (ADR 027).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bellwether_source::canonical::{encode_as, normalize_float_bounds};
use bellwether_types::{ColumnStats, Error, FileFingerprint, Hash, Result, RowGroupMeta, Source};
use serde::{Deserialize, Serialize};

use crate::cast::conform;
use crate::dialect::{Declared, Dialect};
use crate::driver::{self, collect, Driver};
use crate::url::{redact, EngineKind, SnapshotPolicy, SqlUrl, TableName};

/// The largest unit read by default, in rows: `read_columns` holds one
/// unit's projected columns (ADR 018), so a table with more rows is cut
/// into key ranges (`range=<column>&units=<n>`).
pub const DEFAULT_MAX_UNIT_ROWS: u64 = 16 * 1024 * 1024;

/// How a [`SqlSource`] reads.
#[derive(Clone, Debug)]
pub struct SqlOptions {
    /// Dataset types by column name, replacing the declared ones (for
    /// example the folder a table is reconciled against): every value is
    /// converted exactly or refused. A column named here must exist.
    pub schema: Option<SchemaRef>,
    /// The largest unit, in rows ([`DEFAULT_MAX_UNIT_ROWS`]).
    pub max_unit_rows: u64,
}

impl Default for SqlOptions {
    fn default() -> Self {
        Self {
            schema: None,
            max_unit_rows: DEFAULT_MAX_UNIT_ROWS,
        }
    }
}

/// What a run over a table read and how it held the table still: the
/// part of a result a human reads to trust a table-backed answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRun {
    /// The root (`sql://<engine>/<tables>`).
    pub root: String,
    /// The engine.
    pub engine: EngineKind,
    /// How each table was cut: `one unit per table`, `declared
    /// partitions`, or `key ranges of <column>`.
    pub units_by: String,
    /// Units discovered (each a "file" of one "row group").
    pub units: u64,
    /// Rows over every unit: a count from the engine.
    pub rows: u64,
    /// The snapshot the run held, or `None` when it held none and re-read
    /// every unit's identity at the end instead.
    pub snapshot: Option<String>,
    /// Whether every unit's identity was re-read when the run ended (and
    /// found unchanged; a change is an error, never a result).
    pub rechecked: bool,
    /// The source identity: BLAKE3 over the root and every unit's path and
    /// identity, in path order (contract v3 backlog #10).
    pub identity: Hash,
}

/// One unit: the rows of a table, a declared partition, or a key range.
#[derive(Clone, Debug)]
struct Unit {
    table: TableName,
    /// The `FROM` target.
    from: String,
    /// The `WHERE` predicate selecting the unit, if any.
    filter: Option<String>,
    path: String,
}

#[derive(Debug)]
struct Discovered {
    units: Vec<(Unit, RowGroupMeta)>,
    units_by: String,
}

struct Inner {
    driver: Box<dyn Driver>,
    /// The snapshot held, if any.
    snapshot: Option<&'static str>,
    end: Option<&'static str>,
    schemas: Option<(SchemaRef, SchemaRef)>,
    discovered: Option<Arc<Discovered>>,
    rechecked: bool,
    /// Something was read since the last re-read of the identities.
    dirty: bool,
}

/// A `Source` over SQL tables: each table's declared partitions, key
/// ranges, or the whole table is a unit standing in for a file of one row
/// group (`docs/format.md`, "SQL sources"; ADR 027).
pub struct SqlSource {
    url: SqlUrl,
    root: String,
    dialect: Dialect,
    options: SqlOptions,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for SqlSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlSource")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// The driver for `url`'s engine, as [`SqlSource::connect`] opens it,
/// except that a DuckDB database file that does not exist is created (for
/// loading a table; a source never creates one). The driver runs whatever
/// SQL it is given: only for SQL the caller trusts.
pub fn open_driver(url: &SqlUrl) -> Result<Box<dyn Driver>> {
    driver::open(url, true)
}

/// Bytes of one non-null value of a fixed-width type.
fn fixed_width(dt: &DataType) -> Option<u64> {
    Some(match dt {
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_) => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(..) => 8,
        DataType::Decimal128(..) => 16,
        _ => return None,
    })
}

/// A boundary literal as a path segment: the value without its type
/// keyword and quotes, a space as `T`.
fn shown(literal: &str) -> String {
    let inner = match (literal.find('\''), literal.rfind('\'')) {
        (Some(a), Some(b)) if b > a => &literal[a + 1..b],
        _ => literal,
    };
    inner.replace(' ', "T").replace('/', "%2F")
}

impl SqlSource {
    /// Connects to the root `url` (`sql://...`, module docs of
    /// [`crate::url`]) and, unless `snapshot=reread`, begins the run's
    /// snapshot. Nothing is read until the first `discover`, `row_groups`,
    /// or `schema`.
    pub fn connect(url: &str, options: SqlOptions) -> Result<Self> {
        let parsed = SqlUrl::parse(url)?;
        let driver = driver::open(&parsed, false)?;
        Self::with_driver(parsed, driver, options)
    }

    /// [`SqlSource::connect`] over a driver the caller opened: the driver
    /// boundary, for another way to reach an engine or a test.
    pub fn with_driver(
        url: SqlUrl,
        mut driver: Box<dyn Driver>,
        options: SqlOptions,
    ) -> Result<Self> {
        let dialect = Dialect(url.engine);
        let mut snapshot = None;
        let mut end = None;
        if url.snapshot == SnapshotPolicy::Transaction {
            if let Some(s) = dialect.snapshot() {
                let allowed = match dialect.snapshot_probe() {
                    None => true,
                    Some(probe) => {
                        let native =
                            Arc::new(Schema::new(vec![Field::new("on", DataType::Int64, true)]));
                        let rows = collect(&mut *driver, probe, &native)?;
                        one_int(&rows, &native, "the snapshot probe")? == Some(1)
                    }
                };
                if allowed {
                    for stmt in s.begin {
                        driver.execute(stmt)?;
                    }
                    snapshot = Some(s.label);
                    end = Some(s.end);
                }
            }
        }
        Ok(Self {
            root: url.root(),
            dialect,
            options,
            inner: Mutex::new(Inner {
                driver,
                snapshot,
                end,
                schemas: None,
                discovered: None,
                rechecked: false,
                dirty: false,
            }),
            url,
        })
    }

    /// The root a record names (`sql://<engine>/<tables>`).
    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    /// The parsed root.
    #[must_use]
    pub fn url(&self) -> &SqlUrl {
        &self.url
    }

    /// The snapshot this run holds, if any.
    pub fn snapshot(&self) -> Result<Option<&'static str>> {
        Ok(self.lock()?.snapshot)
    }

    /// The source configuration a cache is keyed by.
    #[must_use]
    pub fn config(&self) -> String {
        format!("sql:{}{}", self.url.engine, self.url.unit_config())
    }

    /// The statements a human runs to give each table an index on
    /// `columns`: what an attest-only apply of a key names (ADR 027).
    #[must_use]
    pub fn create_index_statements(&self, columns: &[String]) -> Vec<String> {
        self.url.create_index_statements(columns)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| Error::Other(format!("{}: the connection lock is poisoned", self.root)))
    }

    fn check_root(&self, root: &str) -> Result<()> {
        if root == self.root || redact(root) == self.root {
            Ok(())
        } else {
            Err(Error::Source(format!(
                "this source reads {}, not {root}",
                self.root
            )))
        }
    }

    // ---------------------------------------------------------------
    // Schema
    // ---------------------------------------------------------------

    fn declared(&self, inner: &mut Inner, t: &TableName) -> Result<Schema> {
        if self.url.engine == EngineKind::FlightSql {
            let s = inner.driver.table_schema(t)?;
            for f in s.fields() {
                if !bellwether_source::canonical::is_supported(f.data_type()) {
                    return Err(unsupported_column(t, f.name(), &f.data_type().to_string()));
                }
            }
            return Ok(s);
        }
        let (sql, shape) = self.dialect.columns_query(t);
        let what = format!("the columns of {t}");
        let batches = collect(&mut *inner.driver, &sql, &shape)?;
        let mut fields = Vec::new();
        for b in batches {
            let b = conform(&b, &shape, &what)?;
            let text = |i: usize| -> Result<Vec<Option<String>>> {
                Ok(b.column(i)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| Error::Source(format!("{what}: not text")))?
                    .iter()
                    .map(|v| v.map(str::to_string))
                    .collect())
            };
            let int = |i: usize| -> Result<Vec<Option<i64>>> {
                Ok(b.column(i)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| Error::Source(format!("{what}: not an integer")))?
                    .iter()
                    .collect())
            };
            let (names, types, nulls) = (text(0)?, text(1)?, text(2)?);
            let (prec, scale, dtp) = (int(3)?, int(4)?, int(5)?);
            for i in 0..b.num_rows() {
                let name = names[i].clone().ok_or_else(|| {
                    Error::Source(format!("{what}: the catalog lists a column with no name"))
                })?;
                let d = Declared {
                    spelled: types[i].clone().unwrap_or_default(),
                    nullable: nulls[i].as_deref() != Some("NO"),
                    precision: prec[i],
                    scale: scale[i],
                    datetime_precision: dtp[i],
                    name,
                };
                let field = self
                    .dialect
                    .native_type(&d)
                    .map_err(|why| unsupported_column(t, &d.name, &why))?;
                if fields.iter().any(|f: &Field| f.name() == field.name()) {
                    return Err(Error::Source(format!(
                        "{what}: the catalog lists column {:?} twice",
                        field.name()
                    )));
                }
                fields.push(field);
            }
        }
        if fields.is_empty() {
            return Err(Error::Source(format!(
                "{}: no table {t} (or it has no columns); check the schema and table names in \
                 the root",
                self.root
            )));
        }
        Ok(Schema::new(fields))
    }

    /// `(native, dataset)`: the declared types, and the types the source
    /// reports and converts to.
    fn schemas(&self, inner: &mut Inner) -> Result<(SchemaRef, SchemaRef)> {
        if let Some(s) = &inner.schemas {
            return Ok(s.clone());
        }
        let mut native: Option<Schema> = None;
        for t in &self.url.tables {
            let s = self.declared(inner, t)?;
            match &native {
                None => native = Some(s),
                Some(first) => {
                    let spell = |s: &Schema| -> Vec<(String, String)> {
                        s.fields()
                            .iter()
                            .map(|f| (f.name().clone(), f.data_type().to_string()))
                            .collect()
                    };
                    if spell(first) != spell(&s) {
                        return Err(Error::Source(format!(
                            "{}: {t} has columns {:?}, but {} has {:?}; every table of one root \
                             holds the same columns and types",
                            self.root,
                            spell(&s),
                            self.url.tables[0],
                            spell(first)
                        )));
                    }
                }
            }
        }
        let native = Arc::new(native.ok_or_else(|| Error::Source("no table".into()))?);
        let mut fields: Vec<Field> = native
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), f.is_nullable()))
            .collect();
        if let Some(over) = &self.options.schema {
            for o in over.fields() {
                let Some(f) = fields.iter_mut().find(|f| f.name() == o.name()) else {
                    return Err(Error::Source(format!(
                        "{}: the dataset schema names column {:?}, which the table does not \
                         have",
                        self.root,
                        o.name()
                    )));
                };
                *f = Field::new(o.name(), o.data_type().clone(), f.is_nullable());
            }
        }
        for f in &fields {
            if !bellwether_source::canonical::is_supported(f.data_type()) {
                return Err(unsupported_column(
                    &self.url.tables[0],
                    f.name(),
                    &f.data_type().to_string(),
                ));
            }
        }
        let out = (native, Arc::new(Schema::new(fields)));
        inner.schemas = Some(out.clone());
        Ok(out)
    }

    // ---------------------------------------------------------------
    // Units
    // ---------------------------------------------------------------

    #[allow(clippy::many_single_char_names, clippy::too_many_lines)]
    fn units_of(
        &self,
        inner: &mut Inner,
        t: &TableName,
        native: &SchemaRef,
    ) -> Result<(Vec<Unit>, String)> {
        let base = format!("{}/{}", self.root_of(), t);
        if let Some(sql) = self.dialect.partitions_query(t) {
            let shape = Arc::new(Schema::new(vec![
                Field::new("schema", DataType::Utf8, true),
                Field::new("table", DataType::Utf8, true),
            ]));
            let mut parts = Vec::new();
            for b in collect(&mut *inner.driver, &sql, &shape)? {
                let b = conform(&b, &shape, &format!("the partitions of {t}"))?;
                let (s, n) = (b.column(0), b.column(1));
                let (s, n) = (
                    s.as_any().downcast_ref::<arrow::array::StringArray>(),
                    n.as_any().downcast_ref::<arrow::array::StringArray>(),
                );
                let (Some(s), Some(n)) = (s, n) else {
                    return Err(Error::Source(format!("the partitions of {t}: not text")));
                };
                for i in 0..b.num_rows() {
                    if s.is_null(i) || n.is_null(i) {
                        return Err(Error::Source(format!(
                            "the partitions of {t}: a partition with no name"
                        )));
                    }
                    let child = TableName {
                        schema: s.value(i).to_string(),
                        table: n.value(i).to_string(),
                    };
                    parts.push(Unit {
                        from: self.dialect.table(&child),
                        filter: None,
                        path: format!("{base}/partition={child}"),
                        table: t.clone(),
                    });
                }
            }
            if !parts.is_empty() {
                return Ok((parts, "declared partitions".into()));
            }
        }
        let from = self.dialect.table(t);
        let Some(range) = &self.url.range else {
            return Ok((
                vec![Unit {
                    table: t.clone(),
                    from,
                    filter: None,
                    path: format!("{base}/all"),
                }],
                "one unit per table".into(),
            ));
        };
        let col = &range.column;
        let field = native.field_with_name(col).map_err(|_| {
            Error::Source(format!(
                "{}: range={col} is not a column of {t}; its columns are {}",
                self.root,
                native
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        let q = self.dialect.ident(col);
        let count_shape = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let rows = collect(
            &mut *inner.driver,
            &format!(
                "SELECT {} FROM {from}",
                Dialect::bigint(&format!("COUNT({q})"))
            ),
            &count_shape,
        )?;
        let total = one_int(&rows, &count_shape, &format!("the count of {t}.{col}"))?
            .ok_or_else(|| Error::Source(format!("the count of {t}.{col} is null")))?;
        let total = u64::try_from(total)
            .map_err(|_| Error::Source(format!("the count of {t}.{col} is negative")))?;
        // Boundaries: the values at ranks k * total / units, from one
        // ordered scan of the column, holding one value per boundary.
        let n = u64::from(range.units);
        let wanted: Vec<u64> = (1..n).map(|k| k * total / n).collect();
        let mut literals: Vec<String> = Vec::new();
        let value_shape = Arc::new(Schema::new(vec![field.clone()]));
        let mut seen = 0u64;
        let mut next = 0usize;
        let dialect = self.dialect;
        inner.driver.query(
            &format!("SELECT {q} FROM {from} WHERE {q} IS NOT NULL ORDER BY {q}"),
            &value_shape,
            &mut |b: RecordBatch| {
                if b.num_columns() != 1 {
                    return Err(Error::Source(format!(
                        "the values of {t}.{col}: {} columns returned",
                        b.num_columns()
                    )));
                }
                let col_values: &ArrayRef = b.column(0);
                let len = b.num_rows() as u64;
                while next < wanted.len() && wanted[next] < seen + len {
                    let row = usize::try_from(wanted[next] - seen).unwrap_or(usize::MAX);
                    let lit = dialect.literal(col_values, row)?;
                    if literals.last() != Some(&lit) {
                        literals.push(lit);
                    }
                    next += 1;
                }
                seen += len;
                Ok(())
            },
        )?;
        if seen != total {
            return Err(Error::Source(format!(
                "{}: {t}.{col} counted {total} non-null values but its ordered scan returned \
                 {seen}; the table changed while its units were cut, or the driver returned a \
                 partial result",
                self.root
            )));
        }
        let mut units = vec![Unit {
            table: t.clone(),
            from: from.clone(),
            filter: Some(format!("{q} IS NULL")),
            path: format!("{base}/{col}=null"),
        }];
        let mut lo: Option<&String> = None;
        for hi in literals.iter().map(Some).chain(std::iter::once(None)) {
            let (filter, name) = match (lo, hi) {
                (None, None) => (format!("{q} IS NOT NULL"), "[-inf,+inf)".to_string()),
                (None, Some(h)) => (format!("{q} < {h}"), format!("[-inf,{})", shown(h))),
                (Some(l), Some(h)) => (
                    format!("{q} >= {l} AND {q} < {h}"),
                    format!("[{},{})", shown(l), shown(h)),
                ),
                (Some(l), None) => (format!("{q} >= {l}"), format!("[{},+inf)", shown(l))),
            };
            units.push(Unit {
                table: t.clone(),
                from: from.clone(),
                filter: Some(filter),
                path: format!("{base}/{col}={name}"),
            });
            lo = hi;
        }
        Ok((units, format!("key ranges of {col}")))
    }

    fn root_of(&self) -> String {
        format!("sql://{}", self.url.engine)
    }

    /// The aggregate query of one unit, its row count, its statistics,
    /// and its identity string.
    #[allow(clippy::too_many_lines)]
    fn measure(&self, inner: &mut Inner, unit: &Unit) -> Result<(u64, Vec<ColumnStats>, String)> {
        let (native, dataset) = self.schemas(inner)?;
        let d = self.dialect;
        let mut exprs = vec![Dialect::bigint("COUNT(*)")];
        let mut nat = vec![Field::new("rows", DataType::Int64, true)];
        let mut ds = vec![Field::new("rows", DataType::Int64, true)];
        if let Some(v) = &self.url.version {
            let f = native.field_with_name(v).map_err(|_| {
                Error::Source(format!(
                    "{}: version={v} is not a column of {}",
                    self.root, unit.table
                ))
            })?;
            exprs.push(format!("MAX({})", d.ident(v)));
            nat.push(f.clone());
            ds.push(
                dataset
                    .field_with_name(v)
                    .map_err(|e| Error::Source(e.to_string()))?
                    .clone(),
            );
        }
        // Per column: [min, max,] nulls, [octets].
        let mut layout = Vec::with_capacity(native.fields().len());
        for (n, f) in native.fields().iter().zip(dataset.fields()) {
            let mm = d.min_max(n.name(), n.data_type());
            if let Some((min, max)) = &mm {
                exprs.push(min.clone());
                exprs.push(max.clone());
                nat.push(n.as_ref().clone().with_name("min"));
                nat.push(n.as_ref().clone().with_name("max"));
                ds.push(Field::new("min", f.data_type().clone(), true));
                ds.push(Field::new("max", f.data_type().clone(), true));
            }
            exprs.push(Dialect::bigint(&format!(
                "COUNT(*) - COUNT({})",
                d.ident(n.name())
            )));
            nat.push(Field::new("nulls", DataType::Int64, true));
            ds.push(Field::new("nulls", DataType::Int64, true));
            let width = fixed_width(f.data_type());
            if width.is_none() {
                exprs.push(Dialect::bigint(&format!("SUM({})", d.octets(n.name()))));
                nat.push(Field::new("octets", DataType::Int64, true));
                ds.push(Field::new("octets", DataType::Int64, true));
            }
            layout.push((mm.is_some(), width));
        }
        let mut sql = format!("SELECT {} FROM {}", exprs.join(", "), unit.from);
        if let Some(w) = &unit.filter {
            let _ = write!(sql, " WHERE {w}");
        }
        let what = format!("the statistics of {}", unit.path);
        let nat = Arc::new(Schema::new(nat));
        let ds = Arc::new(Schema::new(ds));
        let batches = collect(&mut *inner.driver, &sql, &nat)?;
        let rows_in: usize = batches.iter().map(RecordBatch::num_rows).sum();
        if rows_in != 1 {
            return Err(Error::Source(format!(
                "{what}: the engine returned {rows_in} rows for one aggregate row"
            )));
        }
        let b = batches
            .iter()
            .find(|b| b.num_rows() == 1)
            .ok_or_else(|| Error::Source(format!("{what}: no row")))?;
        let b = conform(b, &ds, &what)?;
        let int = |i: usize, name: &str| -> Result<Option<u64>> {
            let a = b
                .column(i)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| Error::Source(format!("{what}: {name} is not an integer")))?;
            if a.is_null(0) {
                return Ok(None);
            }
            u64::try_from(a.value(0))
                .map(Some)
                .map_err(|_| Error::Source(format!("{what}: {name} is negative ({})", a.value(0))))
        };
        let rows = int(0, "the row count")?
            .ok_or_else(|| Error::Source(format!("{what}: the row count is null")))?;
        let mut i = 1;
        let version = if self.url.version.is_some() {
            let enc = encode_as(b.column(i).as_ref(), 0, ds.field(i).data_type())?;
            i += 1;
            Some(enc.map_or_else(|| "null".to_string(), |e| Hash::of(&e).to_hex()))
        } else {
            None
        };
        let mut stats = Vec::with_capacity(layout.len());
        for (f, (has_mm, width)) in dataset.fields().iter().zip(&layout) {
            let (mut min, mut max) = (None, None);
            if *has_mm {
                min = encode_as(b.column(i).as_ref(), 0, f.data_type())?;
                max = encode_as(b.column(i + 1).as_ref(), 0, f.data_type())?;
                i += 2;
            }
            let nulls = int(i, "a null count")?
                .ok_or_else(|| Error::Source(format!("{what}: a null count is null")))?;
            i += 1;
            if nulls > rows {
                return Err(Error::Source(format!(
                    "{what}: column {:?} has {nulls} nulls in {rows} rows",
                    f.name()
                )));
            }
            let present = rows - nulls;
            if *has_mm && (min.is_some() != (present > 0) || max.is_some() != (present > 0)) {
                return Err(Error::Source(format!(
                    "{what}: column {:?} has {present} non-null values but bounds {} and {}",
                    f.name(),
                    if min.is_some() { "present" } else { "absent" },
                    if max.is_some() { "present" } else { "absent" },
                )));
            }
            if let (Some(lo), Some(hi)) = (&min, &max) {
                if lo > hi {
                    return Err(Error::Source(format!(
                        "{what}: column {:?} has a minimum above its maximum",
                        f.name()
                    )));
                }
            }
            let (min, max) = normalize_float_bounds(min, max);
            let bytes = if let Some(w) = width {
                present.checked_mul(*w)
            } else {
                let s = int(i, "a byte count")?;
                i += 1;
                match (s, present) {
                    (None, 0) => Some(0),
                    (None, _) => {
                        return Err(Error::Source(format!(
                            "{what}: column {:?} has values but no byte count",
                            f.name()
                        )))
                    }
                    (Some(s), _) => Some(s),
                }
            };
            stats.push(ColumnStats {
                column: f.name().clone(),
                min,
                max,
                null_count: Some(nulls),
                distinct_count: None,
                has_bloom: false,
                bytes,
            });
        }
        let identity = if let Some(v) = version {
            format!("v1;rows={rows};version={v}")
        } else {
            let digest = Hash::of_record(&(rows, &stats))?;
            format!("v1;rows={rows};stats={digest}")
        };
        Ok((rows, stats, identity))
    }

    fn discover_locked(&self, inner: &mut Inner) -> Result<Arc<Discovered>> {
        if let Some(d) = &inner.discovered {
            return Ok(Arc::clone(d));
        }
        let (native, _) = self.schemas(inner)?;
        let mut out = Vec::new();
        let mut units_by = String::new();
        for t in &self.url.tables {
            let (units, by) = self.units_of(inner, t, &native)?;
            units_by = by;
            let mut sum = 0u64;
            for unit in &units {
                let (rows, stats, identity) = self.measure(inner, unit)?;
                if rows > self.options.max_unit_rows {
                    return Err(Error::Source(format!(
                        "{}: unit {} holds {rows} rows, more than the {} one read may hold \
                         (ADR 018); cut the table into key ranges with \
                         range=<column>&units=<n> so each holds fewer",
                        self.root, unit.path, self.options.max_unit_rows
                    )));
                }
                sum += rows;
                let bytes = stats.iter().filter_map(|s| s.bytes).sum();
                let fp = FileFingerprint {
                    path: unit.path.clone(),
                    size: rows,
                    tail_blake3: Hash::of(identity.as_bytes()),
                    etag: Some(identity),
                };
                out.push((
                    unit.clone(),
                    RowGroupMeta {
                        id: None,
                        file: fp,
                        index_in_file: 0,
                        rows,
                        bytes,
                        stats,
                        byte_range: None,
                    },
                ));
            }
            if units.len() > 1 {
                // The units must partition the table: every row in exactly
                // one. A partial or overlapping cut is refused.
                let shape = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
                let rows = collect(
                    &mut *inner.driver,
                    &format!(
                        "SELECT {} FROM {}",
                        Dialect::bigint("COUNT(*)"),
                        self.dialect.table(t)
                    ),
                    &shape,
                )?;
                let total = one_int(&rows, &shape, &format!("the count of {t}"))?;
                if total != i64::try_from(sum).ok() {
                    return Err(Error::Source(format!(
                        "{}: the units of {t} hold {sum} rows but the table holds {}; the units \
                         do not partition the table, so nothing read from them is complete",
                        self.root,
                        total.map_or_else(|| "null".into(), |t| t.to_string())
                    )));
                }
            }
        }
        out.sort_by(|a, b| a.0.path.cmp(&b.0.path));
        let d = Arc::new(Discovered {
            units: out,
            units_by,
        });
        inner.discovered = Some(Arc::clone(&d));
        Ok(d)
    }

    fn unit<'d>(
        &self,
        d: &'d Discovered,
        file: &FileFingerprint,
    ) -> Result<&'d (Unit, RowGroupMeta)> {
        let found = d
            .units
            .iter()
            .find(|(u, _)| u.path == file.path)
            .ok_or_else(|| Error::Source(format!("{}: no unit {}", self.root, file.path)))?;
        if found.1.file != *file {
            return Err(Error::Source(format!(
                "{}: the identity given ({:?}) is not the one this run discovered ({:?}); \
                 discover again",
                file.path, file.etag, found.1.file.etag
            )));
        }
        Ok(found)
    }

    /// The source identity: BLAKE3 over the root and every unit's path and
    /// identity, in path order. Contract v3 backlog #10 proposes
    /// `Source::identity`; this is its first implementation.
    pub fn identity(&self) -> Result<Hash> {
        let mut inner = self.lock()?;
        let d = self.discover_locked(&mut inner)?;
        identity_of(&self.root, &d)
    }

    /// Ends the run: with a snapshot held, nothing to check (every read saw
    /// one state); without one, every unit's identity is re-read and
    /// compared to the one discovered, and any change, new unit, or vanished
    /// unit is refused with [`Error::SourceChanged`]. Returns what the run
    /// read. A run that discovered nothing has nothing to check.
    pub fn finish(&self) -> Result<TableRun> {
        let mut inner = self.lock()?;
        let Some(d) = inner.discovered.clone() else {
            return Ok(TableRun {
                root: self.root.clone(),
                engine: self.url.engine,
                units_by: String::new(),
                units: 0,
                rows: 0,
                snapshot: inner.snapshot.map(str::to_string),
                rechecked: false,
                identity: Hash::of(self.root.as_bytes()),
            });
        };
        if inner.snapshot.is_none() && (inner.dirty || !inner.rechecked) {
            self.recheck(&mut inner, &d)?;
            inner.rechecked = true;
            inner.dirty = false;
        }
        Ok(TableRun {
            root: self.root.clone(),
            engine: self.url.engine,
            units_by: d.units_by.clone(),
            units: d.units.len() as u64,
            rows: d.units.iter().map(|(_, m)| m.rows).sum(),
            snapshot: inner.snapshot.map(str::to_string),
            rechecked: inner.rechecked,
            identity: identity_of(&self.root, &d)?,
        })
    }

    fn recheck(&self, inner: &mut Inner, d: &Discovered) -> Result<()> {
        let (native, _) = self.schemas(inner)?;
        let mut changed: Vec<String> = Vec::new();
        let before: BTreeMap<&str, &RowGroupMeta> =
            d.units.iter().map(|(u, m)| (u.path.as_str(), m)).collect();
        let mut now_paths = Vec::new();
        for t in &self.url.tables {
            // Partitions are listed again; key ranges are the same
            // predicates (they cover every value, so any change lands in
            // one of them).
            let units: Vec<Unit> = if self.dialect.partitions_query(t).is_some() {
                self.units_of(inner, t, &native)?.0
            } else {
                d.units
                    .iter()
                    .filter(|(u, _)| u.table == *t)
                    .map(|(u, _)| u.clone())
                    .collect()
            };
            for u in units {
                // A unit that can no longer be measured changed.
                let (_, _, identity) = self.measure(inner, &u).unwrap_or_default();
                match before.get(u.path.as_str()) {
                    Some(m) if m.file.etag.as_deref() == Some(identity.as_str()) => {}
                    _ => changed.push(u.path.clone()),
                }
                now_paths.push(u.path);
            }
        }
        for p in before.keys() {
            if !now_paths.iter().any(|n| n == p) {
                changed.push((*p).to_string());
            }
        }
        changed.sort();
        changed.dedup();
        if changed.is_empty() {
            return Ok(());
        }
        Err(Error::SourceChanged {
            message: format!(
                "{} changed during the run, which held no snapshot: the identity of {} of its \
                 {} units ({}) differs at the end from the one read at the start, so the answer \
                 would mix two states of the table and is refused; run it again when the table \
                 is quiet, or give it a snapshot (snapshot=transaction on an engine with one)",
                self.root,
                changed.len(),
                d.units.len(),
                changed
                    .iter()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
                    + if changed.len() > 3 { ", ..." } else { "" }
            ),
            root: self.root.clone(),
            units: changed,
        })
    }
}

fn identity_of(root: &str, d: &Discovered) -> Result<Hash> {
    let units: Vec<(&str, Option<&str>)> = d
        .units
        .iter()
        .map(|(u, m)| (u.path.as_str(), m.file.etag.as_deref()))
        .collect();
    Hash::of_record(&(root, units))
}

fn unsupported_column(t: &TableName, column: &str, why: &str) -> Error {
    Error::Source(format!("{t}: column {column:?} cannot be read: {why}"))
}

/// The one integer of a one-row, one-column answer.
fn one_int(batches: &[RecordBatch], shape: &SchemaRef, what: &str) -> Result<Option<i64>> {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if rows != 1 {
        return Err(Error::Source(format!(
            "{what}: the engine returned {rows} rows for one"
        )));
    }
    let b = batches
        .iter()
        .find(|b| b.num_rows() == 1)
        .ok_or_else(|| Error::Source(format!("{what}: no row")))?;
    let b = conform(b, shape, what)?;
    let a = b
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| Error::Source(format!("{what}: not an integer")))?;
    Ok((!a.is_null(0)).then(|| a.value(0)))
}

impl Drop for SqlSource {
    fn drop(&mut self) {
        if let Ok(inner) = self.inner.get_mut() {
            if let Some(end) = inner.end.take() {
                // A read-only run: its transaction is rolled back.
                let _ = inner.driver.execute(end);
            }
        }
    }
}

impl Source for SqlSource {
    fn discover(&self, root: &str) -> Result<Vec<FileFingerprint>> {
        self.check_root(root)?;
        let mut inner = self.lock()?;
        inner.dirty = true;
        let d = self.discover_locked(&mut inner)?;
        Ok(d.units.iter().map(|(_, m)| m.file.clone()).collect())
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        let mut inner = self.lock()?;
        let d = self.discover_locked(&mut inner)?;
        Ok(vec![self.unit(&d, file)?.1.clone()])
    }

    fn read_columns(&self, rg: &RowGroupMeta, columns: &[String]) -> Result<RecordBatch> {
        let mut inner = self.lock()?;
        inner.dirty = true;
        let d = self.discover_locked(&mut inner)?;
        let (unit, meta) = self.unit(&d, &rg.file)?;
        if rg.index_in_file != 0 {
            return Err(Error::Source(format!(
                "{}: a unit has one row group, not {}",
                unit.path,
                rg.index_in_file + 1
            )));
        }
        let rows = usize::try_from(meta.rows)
            .map_err(|_| Error::Source(format!("{}: too many rows", unit.path)))?;
        if columns.is_empty() {
            return RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                Vec::new(),
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .map_err(|e| Error::Source(e.to_string()));
        }
        let (native, dataset) = self.schemas(&mut inner)?;
        let pick = |s: &Schema| -> Result<SchemaRef> {
            Ok(Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|c| {
                        s.field_with_name(c).cloned().map_err(|_| {
                            Error::KeySpec(format!("column {c:?} not in source schema"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )))
        };
        let (nat, want) = (pick(&native)?, pick(&dataset)?);
        let cols: Vec<String> = columns.iter().map(|c| self.dialect.ident(c)).collect();
        let mut sql = format!("SELECT {} FROM {}", cols.join(", "), unit.from);
        if let Some(w) = &unit.filter {
            let _ = write!(sql, " WHERE {w}");
        }
        let what = format!("reading {}", unit.path);
        let mut parts = Vec::new();
        let mut got = 0usize;
        let mut over = false;
        let read = inner.driver.query(&sql, &nat, &mut |b| {
            got += b.num_rows();
            if got > rows {
                // Stop reading: the unit is not the one discovered.
                over = true;
                return Err(Error::Other(String::new()));
            }
            parts.push(conform(&b, &want, &what)?);
            Ok(())
        });
        if !over {
            read?;
        }
        if got != rows {
            let message = format!(
                "{what}: the engine returned {}{got} rows, and the unit's identity says {rows}; \
                 the table changed during a run that held no snapshot, or the driver returned a \
                 partial result, so the unit is not read as complete; run it again",
                if over { "more than " } else { "" }
            );
            return Err(if inner.snapshot.is_none() {
                Error::SourceChanged {
                    root: self.root.clone(),
                    units: vec![unit.path.clone()],
                    message,
                }
            } else {
                Error::Source(message)
            });
        }
        let schema = Arc::new(Schema::new(
            want.fields()
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        concat_batches(&schema, &parts).map_err(|e| Error::Source(format!("{what}: {e}")))
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        self.check_root(root)?;
        let mut inner = self.lock()?;
        Ok(self.schemas(&mut inner)?.1)
    }
}
