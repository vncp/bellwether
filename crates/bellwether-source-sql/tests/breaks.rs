//! The snapshot discipline's load-bearing checks, each named for the
//! deliberate break it catches (README, "Deliberate-break checks"):
//!
//! - skip the identity re-read once: `a_write_mid_run_without_a_snapshot_is_refused`;
//! - trust a partial unit once: `a_partial_unit_is_refused` and
//!   `units_that_do_not_partition_the_table_are_refused`.
#![allow(clippy::unwrap_used, clippy::doc_markdown)]

mod common;

use std::path::Path;

use arrow::array::RecordBatch;
use bellwether_source_sql::{open_driver, SqlOptions, SqlSource, SqlUrl};
use bellwether_types::{Error, Source};
use common::*;

fn sqlite_db(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("b.sqlite");
    let mut rows = Vec::new();
    for i in 1..=100 {
        rows.push(format!("({i}, {}.50)", i * 3));
    }
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE t (id INTEGER NOT NULL, amount DECIMAL(10,2))",
            &format!("INSERT INTO t VALUES {}", rows.join(", ")),
        ],
    );
    db
}

fn duck_db(dir: &Path) -> Option<std::path::PathBuf> {
    duckdb_driver()?;
    let db = dir.join("b.duckdb");
    duck_exec(
        &db,
        &[
            "CREATE TABLE t (id BIGINT NOT NULL, amount DECIMAL(10,2))",
            "INSERT INTO t SELECT i, i * 3 + 0.5 FROM range(1, 101) r(i)",
        ],
    );
    Some(db)
}

/// `(engine, root builder, writer)` for each engine available.
#[allow(clippy::type_complexity)]
fn engines(dir: &Path) -> Vec<(&'static str, Box<dyn Fn(&str) -> String>, Box<dyn Fn(&str)>)> {
    let mut out: Vec<(&'static str, Box<dyn Fn(&str) -> String>, Box<dyn Fn(&str)>)> = Vec::new();
    let s = sqlite_db(dir);
    let s2 = s.clone();
    out.push((
        "sqlite",
        Box::new(move |extra| sqlite_root(&s, "t", extra)),
        Box::new(move |sql| sqlite_exec(&s2, &[sql])),
    ));
    if let Some(d) = duck_db(dir) {
        let d2 = d.clone();
        out.push((
            "duckdb",
            Box::new(move |extra| duck_root(&d, "t", extra)),
            Box::new(move |sql| duck_exec(&d2, &[sql])),
        ));
    }
    out
}

fn read_all(src: &SqlSource) -> bellwether_types::Result<u64> {
    let mut rows = 0;
    for fp in src.discover(src.root())? {
        for rg in src.row_groups(&fp)? {
            rows += src
                .read_columns(&rg, &["id".into(), "amount".into()])?
                .num_rows() as u64;
        }
    }
    Ok(rows)
}

/// Without a snapshot, a write that lands after discovery and before the
/// run ends is refused when the run ends: the identities are re-read.
/// Breaking the re-read (skipping it once) makes this test fail.
#[test]
fn a_write_mid_run_without_a_snapshot_is_refused() {
    let (_d, dir) = tmp();
    for (engine, root, write) in engines(&dir) {
        for units in ["", "&range=id&units=4"] {
            // An update that keeps the row count.
            let src = SqlSource::connect(
                &root(&format!("&snapshot=reread{units}")),
                SqlOptions::default(),
            )
            .unwrap();
            assert_eq!(src.snapshot().unwrap(), None);
            let fps = src.discover(src.root()).unwrap();
            write("UPDATE t SET amount = 99999.25 WHERE id = 7");
            assert_eq!(
                read_all(&src).unwrap(),
                100,
                "{engine}: the reads alone see nothing amiss"
            );
            let e = src.finish().unwrap_err();
            let Error::SourceChanged {
                units: changed,
                message,
                ..
            } = &e
            else {
                panic!("{engine}: {e}");
            };
            assert_eq!(changed.len(), 1, "{engine}{units}: {changed:?}");
            assert!(fps.iter().any(|f| f.path == changed[0]));
            assert!(message.contains("run it again"), "{message}");
            write("UPDATE t SET amount = 21.50 WHERE id = 7");
            // An unchanged table finishes, and says it re-read.
            let src = SqlSource::connect(
                &root(&format!("&snapshot=reread{units}")),
                SqlOptions::default(),
            )
            .unwrap();
            read_all(&src).unwrap();
            let run = src.finish().unwrap();
            assert!(run.rechecked && run.snapshot.is_none(), "{engine}");
        }
    }
}

/// Without a snapshot, a row inserted mid-run makes the unit read more
/// rows than its identity says: refused at the read.
#[test]
fn an_insert_mid_read_without_a_snapshot_is_refused_at_the_read() {
    let (_d, dir) = tmp();
    for (engine, root, write) in engines(&dir) {
        let src = SqlSource::connect(&root("&snapshot=reread"), SqlOptions::default()).unwrap();
        src.discover(src.root()).unwrap();
        write("INSERT INTO t VALUES (1000, 1.00)");
        let e = read_all(&src).unwrap_err();
        assert!(matches!(e, Error::SourceChanged { .. }), "{engine}: {e}");
        write("DELETE FROM t WHERE id = 1000");
    }
}

/// A driver that returns fewer rows than the unit holds: the unit is not
/// read as complete, with or without a snapshot. Trusting the partial
/// unit once makes this test fail.
#[test]
fn a_partial_unit_is_refused() {
    let (_d, dir) = tmp();
    for (engine, root, _) in engines(&dir) {
        for snapshot in ["", "&snapshot=reread"] {
            let url = SqlUrl::parse(&root(snapshot)).unwrap();
            let mut w = Wrapped::new(open_driver(&url).unwrap());
            w.hook = Box::new(|sql, b: RecordBatch| {
                let reading = sql.starts_with("SELECT \"id\", \"amount\" FROM");
                Ok(Some(if reading && b.num_rows() > 0 {
                    b.slice(0, b.num_rows() - 1)
                } else {
                    b
                }))
            });
            let src = SqlSource::with_driver(url, Box::new(w), SqlOptions::default()).unwrap();
            let e = read_all(&src).unwrap_err().to_string();
            assert!(e.contains("partial result"), "{engine}{snapshot}: {e}");
        }
    }
}

/// Key-range units must hold every row of the table exactly once: a cut
/// whose units do not sum to the table's count is refused.
#[test]
fn units_that_do_not_partition_the_table_are_refused() {
    let (_d, dir) = tmp();
    for (engine, root, _) in engines(&dir) {
        let url = SqlUrl::parse(&root("&range=id&units=4")).unwrap();
        let mut w = Wrapped::new(open_driver(&url).unwrap());
        // The unit `id < first boundary` loses one row in its count.
        w.hook = Box::new(|sql, b: RecordBatch| {
            if sql.contains("WHERE \"id\" < ") && sql.contains("COUNT(*)") {
                let mut cols = b.columns().to_vec();
                let n = arrow::compute::cast(&cols[0], &arrow::datatypes::DataType::Int64).unwrap();
                let n = n
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                cols[0] = std::sync::Arc::new(arrow::array::Int64Array::from(vec![n.value(0) - 1]));
                let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(
                    b.schema()
                        .fields()
                        .iter()
                        .zip(&cols)
                        .map(|(f, c)| {
                            arrow::datatypes::Field::new(f.name(), c.data_type().clone(), true)
                        })
                        .collect::<Vec<_>>(),
                ));
                return Ok(Some(RecordBatch::try_new(schema, cols).unwrap()));
            }
            Ok(Some(b))
        });
        let src = SqlSource::with_driver(url, Box::new(w), SqlOptions::default()).unwrap();
        let e = src.discover(src.root()).unwrap_err().to_string();
        assert!(e.contains("do not partition the table"), "{engine}: {e}");
    }
}
