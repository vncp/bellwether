//! Every path of the source over SQLite and DuckDB, in process: schema,
//! units, statistics against an oracle over the rows read, projected
//! reads, nulls, decimals at two scales under one dataset scale,
//! timestamps in two units, snapshots, and identities.
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::type_complexity,
    clippy::cast_possible_wrap
)]

mod common;

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Decimal128Type, Field, Int64Type, Schema, TimeUnit};
use bellwether_source::canonical::{encode_as, normalize_float_bounds};
use bellwether_source_sql::{SqlOptions, SqlSource, TableRun};
use bellwether_types::{Error, Source};
use common::*;

const ROWS: i64 = 200;

fn name(i: i64) -> Option<String> {
    (i % 11 != 0).then(|| format!("n{}", i % 7))
}
/// Cents.
fn amount(i: i64) -> Option<i64> {
    (i % 13 != 0).then(|| i * 100 + 25 * (i % 4))
}
const BASE_MS: i64 = 1_704_067_200_000; // 2024-01-01

fn make_sqlite(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("t.sqlite");
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR(20), amount DECIMAL(10,2), \
           ts TIMESTAMP(3), ts2 TIMESTAMP, flag BOOLEAN, score DOUBLE)",
        ],
    );
    let c = rusqlite::Connection::open(&db).unwrap();
    for i in 1..=ROWS {
        let amt = amount(i).map(|a| format!("{}.{:02}", a / 100, a % 100));
        let ts2 = format!(
            "2024-01-01 00:{:02}:{:02}.{:06}",
            (i * 1000) / 60_000,
            ((i * 1000) / 1000) % 60,
            1
        );
        c.execute(
            "INSERT INTO t VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                i,
                name(i),
                amt,
                BASE_MS + i * 1000,
                ts2,
                i % 2,
                f64::from(i32::try_from(i).unwrap()) / 3.0
            ],
        )
        .unwrap();
    }
    db
}

fn make_duckdb(dir: &Path) -> Option<std::path::PathBuf> {
    duckdb_driver()?;
    let db = dir.join("t.duckdb");
    duck_exec(
        &db,
        &[
            "CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR, amount DECIMAL(10,2), \
             ts TIMESTAMP_MS, ts2 TIMESTAMP, flag BOOLEAN, score DOUBLE)",
            "INSERT INTO t SELECT i, CASE WHEN i % 11 = 0 THEN NULL ELSE 'n' || (i % 7) END, \
             CASE WHEN i % 13 = 0 THEN NULL ELSE CAST((i * 100 + 25 * (i % 4)) / 100.0 AS \
             DECIMAL(10,2)) END, CAST(make_timestamp(1704067200000000 + i * 1000000) AS \
             TIMESTAMP_MS), make_timestamp(1704067200000000 + i * 1000000 + 1), i % 2 = 1, \
             i / 3.0 FROM range(1, 201) r(i)",
        ],
    );
    Some(db)
}

/// `(engine name, root builder)` for every engine available here.
fn engines(dir: &Path) -> Vec<(&'static str, Box<dyn Fn(&str) -> String>)> {
    let mut out: Vec<(&'static str, Box<dyn Fn(&str) -> String>)> = Vec::new();
    let s = make_sqlite(dir);
    out.push(("sqlite", Box::new(move |extra| sqlite_root(&s, "t", extra))));
    if let Some(d) = make_duckdb(dir) {
        out.push(("duckdb", Box::new(move |extra| duck_root(&d, "t", extra))));
    }
    out
}

fn all_rows(src: &SqlSource, columns: &[&str]) -> Vec<RecordBatch> {
    let cols: Vec<String> = columns.iter().map(ToString::to_string).collect();
    src.discover(src.root())
        .unwrap()
        .iter()
        .flat_map(|fp| src.row_groups(fp).unwrap())
        .map(|rg| src.read_columns(&rg, &cols).unwrap())
        .collect()
}

#[test]
fn the_schema_declares_scale_and_unit() {
    let (_d, dir) = tmp();
    for (engine, root) in engines(&dir) {
        let src = SqlSource::connect(&root(""), SqlOptions::default()).unwrap();
        let s = src.schema(src.root()).unwrap();
        let types: Vec<String> = s
            .fields()
            .iter()
            .map(|f| f.data_type().to_string())
            .collect();
        let ts2 = "Timestamp(µs)";
        assert_eq!(types[0], "Int64", "{engine}");
        assert_eq!(types[1], "Utf8", "{engine}");
        assert_eq!(types[2], "Decimal128(10, 2)", "{engine}");
        assert_eq!(types[3], "Timestamp(ms)", "{engine}");
        assert_eq!(types[4], ts2, "{engine}");
        assert_eq!(types[5], "Boolean", "{engine}");
        assert_eq!(types[6], "Float64", "{engine}");
        assert_eq!(src.root(), format!("sql://{engine}/main.t"));
    }
}

/// The statistics of every unit equal an oracle over the rows the source
/// reads for that unit: row counts, canonical min and max at the dataset
/// type, null counts. Rows are read once each across units.
#[test]
fn unit_statistics_equal_an_oracle_over_the_rows() {
    let (_d, dir) = tmp();
    for (engine, root) in engines(&dir) {
        for extra in [
            "",
            "&range=id&units=7",
            "&range=ts&units=5",
            "&range=name&units=3",
        ] {
            let src = SqlSource::connect(&root(extra), SqlOptions::default()).unwrap();
            let schema = src.schema(src.root()).unwrap();
            let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
            let mut ids = Vec::new();
            let files = src.discover(src.root()).unwrap();
            if !extra.is_empty() {
                assert!(files.len() > 2, "{engine}{extra}: {}", files.len());
            }
            for fp in &files {
                assert!(
                    fp.path.starts_with(&format!("sql://{engine}/main.t/")),
                    "{}",
                    fp.path
                );
                let rgs = src.row_groups(fp).unwrap();
                assert_eq!(rgs.len(), 1);
                let rg = &rgs[0];
                assert_eq!(rg.rows, fp.size);
                assert_eq!(
                    fp.tail_blake3,
                    bellwether_types::Hash::of(fp.etag.as_ref().unwrap().as_bytes())
                );
                let b = src.read_columns(rg, &cols).unwrap();
                assert_eq!(b.num_rows() as u64, rg.rows, "{engine}{extra} {}", fp.path);
                for (i, f) in schema.fields().iter().enumerate() {
                    let a = b.column(i);
                    let mut lo: Option<Vec<u8>> = None;
                    let mut hi: Option<Vec<u8>> = None;
                    for r in 0..a.len() {
                        if let Some(e) = encode_as(a.as_ref(), r, f.data_type()).unwrap() {
                            if lo.as_ref().is_none_or(|l| e < *l) {
                                lo = Some(e.clone());
                            }
                            if hi.as_ref().is_none_or(|h| e > *h) {
                                hi = Some(e);
                            }
                        }
                    }
                    let (lo, hi) = normalize_float_bounds(lo, hi);
                    let st = &rg.stats[i];
                    assert_eq!(st.column, *f.name());
                    assert_eq!(
                        st.null_count,
                        Some(a.null_count() as u64),
                        "{engine} {}",
                        f.name()
                    );
                    assert_eq!(
                        (&st.min, &st.max),
                        (&lo, &hi),
                        "{engine}{extra} {} {}",
                        fp.path,
                        f.name()
                    );
                }
                ids.extend(
                    b.column(0)
                        .as_primitive::<Int64Type>()
                        .values()
                        .iter()
                        .copied(),
                );
            }
            ids.sort_unstable();
            assert_eq!(
                ids,
                (1..=ROWS).collect::<Vec<_>>(),
                "{engine}{extra}: every row once"
            );
            let run = src.finish().unwrap();
            assert_eq!(run.rows, ROWS as u64);
            assert!(run.snapshot.is_some(), "{engine}: a snapshot is held");
        }
    }
}

/// The two engines hold the same rows, so they report the same units'
/// statistics: two independent readers agree.
#[test]
fn sqlite_and_duckdb_report_the_same_statistics() {
    let (_d, dir) = tmp();
    let e = engines(&dir);
    if e.len() < 2 {
        return;
    }
    let stats = |root: &str| {
        let src = SqlSource::connect(root, SqlOptions::default()).unwrap();
        src.discover(src.root())
            .unwrap()
            .iter()
            .map(|fp| {
                let rg = src.row_groups(fp).unwrap().remove(0);
                (
                    fp.path.split_once("/main.t/").unwrap().1.to_string(),
                    rg.rows,
                    rg.stats,
                )
            })
            .collect::<Vec<_>>()
    };
    for extra in ["", "&range=id&units=4"] {
        assert_eq!(stats(&(e[0].1)(extra)), stats(&(e[1].1)(extra)), "{extra}");
    }
}

#[test]
fn reads_are_projected_and_nulls_survive() {
    let (_d, dir) = tmp();
    for (engine, root) in engines(&dir) {
        let src = SqlSource::connect(&root(""), SqlOptions::default()).unwrap();
        let b = all_rows(&src, &["amount", "id"]).remove(0);
        assert_eq!(b.schema().field(0).name(), "amount");
        assert_eq!(b.num_columns(), 2);
        let mut by_id: Vec<(i64, Option<i128>)> = (0..b.num_rows())
            .map(|r| {
                let a = b.column(0).as_primitive::<Decimal128Type>();
                (
                    b.column(1).as_primitive::<Int64Type>().value(r),
                    (!a.is_null(r)).then(|| a.value(r)),
                )
            })
            .collect();
        by_id.sort_unstable();
        for (id, a) in by_id {
            assert_eq!(a, amount(id).map(i128::from), "{engine} id {id}");
        }
        let empty = src
            .read_columns(
                &src.row_groups(&src.discover(src.root()).unwrap()[0])
                    .unwrap()[0],
                &[],
            )
            .unwrap();
        assert_eq!(empty.num_rows() as i64, ROWS);
        let missing = src.read_columns(
            &src.row_groups(&src.discover(src.root()).unwrap()[0])
                .unwrap()[0],
            &["nope".to_string()],
        );
        assert!(matches!(missing, Err(Error::KeySpec(_))), "{engine}");
    }
}

/// One dataset scale over a table declared at another: scale 4 holds every
/// scale-2 value exactly; scale 1 does not hold 1.25, which is refused,
/// never rounded. Timestamps declared in milliseconds read as microseconds;
/// read as seconds, a fractional second is refused.
#[test]
fn two_scales_and_two_units_under_one_dataset_type() {
    let (_d, dir) = tmp();
    for (engine, root) in engines(&dir) {
        let wide = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Decimal128(12, 4), true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));
        let src = SqlSource::connect(
            &root(""),
            SqlOptions {
                schema: Some(wide),
                ..SqlOptions::default()
            },
        )
        .unwrap();
        let b = all_rows(&src, &["id", "amount", "ts"]).remove(0);
        let ids = b.column(0).as_primitive::<Int64Type>();
        let amt = b.column(1).as_primitive::<Decimal128Type>();
        let ts = b
            .column(2)
            .as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        for r in 0..b.num_rows() {
            let i = ids.value(r);
            assert_eq!(
                (!amt.is_null(r)).then(|| amt.value(r)),
                amount(i).map(|a| i128::from(a) * 100),
                "{engine}"
            );
            assert_eq!(ts.value(r), (BASE_MS + i * 1000) * 1000, "{engine}");
        }
        let rg = src
            .row_groups(&src.discover(src.root()).unwrap()[0])
            .unwrap()
            .remove(0);
        let min = rg.stats[2].min.clone().unwrap();
        let expect = encode_as(
            &arrow::array::Decimal128Array::from(vec![1_2500_i128])
                .with_precision_and_scale(12, 4)
                .unwrap(),
            0,
            &DataType::Decimal128(12, 4),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            min, expect,
            "{engine}: the minimum is encoded at the dataset scale"
        );

        for (field, refused) in [
            (
                Field::new("amount", DataType::Decimal128(10, 1), true),
                "fractional digits",
            ),
            (
                Field::new("ts2", DataType::Timestamp(TimeUnit::Second, None), true),
                "finer",
            ),
        ] {
            let src = SqlSource::connect(
                &root(""),
                SqlOptions {
                    schema: Some(Arc::new(Schema::new(vec![field]))),
                    ..SqlOptions::default()
                },
            )
            .unwrap();
            let e = src.discover(src.root()).unwrap_err().to_string();
            assert!(e.contains(refused), "{engine}: {e}");
        }
    }
}

/// With a snapshot held, a write that commits mid-run is not seen: every
/// read sees the table as discovered, and finishing succeeds.
#[test]
fn a_snapshot_hides_a_write_that_lands_mid_run() {
    let (_d, dir) = tmp();
    for (engine, root) in engines(&dir) {
        let db = dir.join(format!("t.{engine}"));
        // Keep the writable instance alive before opening the source, so
        // both connections share one DuckDB database. A read-only instance
        // cannot be upgraded by opening another read-write instance of the
        // same file (Windows correctly refuses the second file lock).
        let mut writer = (engine == "duckdb")
            .then(|| bellwether_source_sql::open_driver(&duck_url(&db)).unwrap());
        let src = SqlSource::connect(&root(""), SqlOptions::default()).unwrap();
        let fps = src.discover(src.root()).unwrap();
        let rg = src.row_groups(&fps[0]).unwrap().remove(0);
        // Commit a change through another connection.
        if let Some(driver) = writer.as_mut() {
            driver
                .execute("UPDATE t SET amount = 0 WHERE id = 5")
                .unwrap();
        }
        if engine == "sqlite" {
            // A SQLite reader's transaction holds a shared lock until it
            // reads; it has read, so a writer in rollback-journal mode
            // waits. The snapshot is the lock: nothing can land.
            let c = rusqlite::Connection::open(dir.join("t.sqlite")).unwrap();
            c.busy_timeout(std::time::Duration::from_millis(50))
                .unwrap();
            assert!(c
                .execute("UPDATE t SET amount = 0 WHERE id = 5", [])
                .is_err());
        }
        let b = src
            .read_columns(&rg, &["id".into(), "amount".into()])
            .unwrap();
        let ids = b.column(0).as_primitive::<Int64Type>();
        let r = (0..b.num_rows()).find(|r| ids.value(*r) == 5).unwrap();
        assert_eq!(
            b.column(1).as_primitive::<Decimal128Type>().value(r),
            525,
            "{engine}"
        );
        let run: TableRun = src.finish().unwrap();
        assert!(!run.rechecked);
        drop(src);
        if engine != "sqlite" {
            // A new run sees the write.
            let src = SqlSource::connect(&root(""), SqlOptions::default()).unwrap();
            let fp2 = src.discover(src.root()).unwrap();
            assert_ne!(fp2[0].etag, fps[0].etag, "{engine}: the identity changed");
        }
    }
}

/// The identity is a checksum of the unit's statistics by default and a
/// version column's maximum when one is named; both change with the data.
#[test]
fn identities_follow_the_data() {
    let (_d, dir) = tmp();
    for (engine, root) in engines(&dir) {
        let etag = |extra: &str| {
            let src = SqlSource::connect(&root(extra), SqlOptions::default()).unwrap();
            src.discover(src.root()).unwrap()[0].etag.clone().unwrap()
        };
        let stats = etag("");
        assert!(
            stats.starts_with(&format!("v1;rows={ROWS};stats=")),
            "{stats}"
        );
        let version = etag("&version=id");
        assert!(version.contains(";version="), "{version}");
        assert_eq!(etag(""), stats, "{engine}: stable over unchanged data");
        let db = dir.join(format!("t.{engine}"));
        let insert = "INSERT INTO t (id) VALUES (1000)";
        if engine == "sqlite" {
            sqlite_exec(&db, &[insert]);
        } else {
            duck_exec(&db, &[insert]);
        }
        assert_ne!(etag(""), stats, "{engine}");
        assert_ne!(etag("&version=id"), version, "{engine}");
    }
}

#[test]
fn what_cannot_be_read_is_refused_naming_the_fix() {
    let (_d, dir) = tmp();
    let db = dir.join("bad.sqlite");
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE untyped (a)",
            "CREATE TABLE unscaled (a NUMERIC)",
            "CREATE TABLE ints (t TIMESTAMP)",
            "INSERT INTO ints VALUES (1700000000)",
            "CREATE TABLE text_in_int (a INTEGER)",
            "INSERT INTO text_in_int VALUES ('seven')",
        ],
    );
    for (table, says) in [
        ("untyped", "declares no type"),
        ("unscaled", "precision and scale"),
        ("ints", "TIMESTAMP(0)"),
        ("text_in_int", "not an integer"),
        ("absent", "no table main.absent"),
    ] {
        let e = SqlSource::connect(&sqlite_root(&db, table, ""), SqlOptions::default())
            .and_then(|s| s.discover(s.root()).map(|_| ()))
            .unwrap_err()
            .to_string();
        assert!(e.contains(says), "{table}: {e}");
    }
    let e = SqlSource::connect(
        &sqlite_root(&dir.join("absent.sqlite"), "t", ""),
        SqlOptions::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("no database file"), "{e}");
}

/// A unit larger than the cap is refused at discovery, naming key ranges.
#[test]
fn a_unit_over_the_row_cap_is_refused() {
    let (_d, dir) = tmp();
    for (engine, root) in engines(&dir) {
        let opts = SqlOptions {
            max_unit_rows: 50,
            ..SqlOptions::default()
        };
        let src = SqlSource::connect(&root(""), opts.clone()).unwrap();
        let e = src.discover(src.root()).unwrap_err().to_string();
        assert!(e.contains("range=<column>&units=<n>"), "{engine}: {e}");
        let src = SqlSource::connect(&root("&range=id&units=8"), opts).unwrap();
        assert!(src.discover(src.root()).is_ok(), "{engine}");
    }
}

#[test]
fn several_tables_of_one_shape_are_one_dataset() {
    let (_d, dir) = tmp();
    let db = dir.join("m.sqlite");
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE jan (id INTEGER, v TEXT)",
            "CREATE TABLE feb (id INTEGER, v TEXT)",
            "CREATE TABLE odd (id INTEGER, v REAL)",
            "INSERT INTO jan VALUES (1, 'a'), (2, 'b')",
            "INSERT INTO feb VALUES (3, 'c')",
        ],
    );
    let root = format!(
        "sql://sqlite/main.jan,main.feb?db={}",
        q(&db.to_string_lossy())
    );
    let src = SqlSource::connect(&root, SqlOptions::default()).unwrap();
    let files = src.discover(src.root()).unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files.iter().map(|f| f.size).sum::<u64>(), 3);
    let root = format!(
        "sql://sqlite/main.jan,main.odd?db={}",
        q(&db.to_string_lossy())
    );
    let src = SqlSource::connect(&root, SqlOptions::default()).unwrap();
    let e = src.discover(src.root()).unwrap_err().to_string();
    assert!(e.contains("same columns and types"), "{e}");
}

#[test]
fn the_index_statement_names_the_table() {
    let (_d, dir) = tmp();
    for (engine, root) in engines(&dir) {
        let src = SqlSource::connect(&root(""), SqlOptions::default()).unwrap();
        let s = src.create_index_statements(&["id".into(), "ts".into()]);
        assert_eq!(
            s,
            vec!["CREATE INDEX \"bw_t_id_ts\" ON \"main\".\"t\" (\"id\", \"ts\")".to_string()],
            "{engine}"
        );
    }
}
