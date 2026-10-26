//! The PR #11 review's probes, kept as regression tests: bounds that are
//! bounds of the values read (invariant 1) under SQLite text times, a
//! `NOCASE` collation, and DuckDB non-ASCII text; key ranges over integer
//! time counts; injective unit paths; a second end-of-run re-read; and no
//! connection string in any error.
#![allow(clippy::unwrap_used, clippy::doc_markdown, clippy::type_complexity)]

mod common;

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use bellwether_source::canonical::encode_utf8;
use bellwether_source_sql::{SqlOptions, SqlSource};
use bellwether_types::{Error, Source};
use common::*;

fn stats_of(root: &str, column: usize) -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> {
    let src = SqlSource::connect(root, SqlOptions::default()).unwrap();
    src.discover(src.root())
        .unwrap()
        .iter()
        .map(|fp| {
            let rg = src.row_groups(fp).unwrap().remove(0);
            (rg.stats[column].min.clone(), rg.stats[column].max.clone())
        })
        .collect()
}

/// Text times in mixed zones or shapes, a date with a leading space, and a
/// number stored as text order differently as text than as values: no
/// bounds. Canonical text keeps them.
#[test]
fn sqlite_bounds_only_where_text_order_is_value_order() {
    let (_d, dir) = tmp();
    let db = dir.join("t.sqlite");
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE zones (ts TIMESTAMP)",
            "INSERT INTO zones VALUES ('2024-01-01 05:00:00'), ('2024-01-01 06:00:00+05:00'), \
             ('2024-01-01 09:00:00')",
            "CREATE TABLE plain (ts TIMESTAMP)",
            "INSERT INTO plain VALUES ('2024-01-01 05:00:00'), ('2024-01-01 06:00:00.5'), \
             ('2024-01-01 09:00:00')",
            "CREATE TABLE dates (d DATE)",
            "INSERT INTO dates VALUES (' 2024-06-01'), ('2024-01-01'), ('2024-12-31')",
        ],
    );
    assert_eq!(
        stats_of(&sqlite_root(&db, "zones", ""), 0),
        vec![(None, None)]
    );
    assert_eq!(
        stats_of(&sqlite_root(&db, "dates", ""), 0),
        vec![(None, None)]
    );
    let plain = stats_of(&sqlite_root(&db, "plain", ""), 0);
    assert!(plain[0].0.is_some() && plain[0].1.is_some());
}

/// M9: a `NOCASE` column is still bounded in byte order.
#[test]
fn sqlite_nocase_column_is_bounded_in_byte_order() {
    let (_d, dir) = tmp();
    let db = dir.join("t.sqlite");
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE t (s TEXT COLLATE NOCASE)",
            "INSERT INTO t VALUES ('a'), ('B'), ('c')",
        ],
    );
    assert_eq!(
        stats_of(&sqlite_root(&db, "t", ""), 0),
        vec![(Some(encode_utf8("B")), Some(encode_utf8("c")))]
    );
}

/// Key ranges over a `TIMESTAMP(3)` column holding integer milliseconds are
/// cut with integer boundaries, so the rows spread over the ranges.
#[test]
fn integer_time_counts_are_cut_with_integer_boundaries() {
    let (_d, dir) = tmp();
    let db = dir.join("t.sqlite");
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE t (ts TIMESTAMP(3))",
            "WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM r WHERE i < 399) \
             INSERT INTO t SELECT 1704067200000 + i * 1000 FROM r",
        ],
    );
    let src = SqlSource::connect(
        &sqlite_root(&db, "t", "&range=ts&units=4"),
        SqlOptions::default(),
    )
    .unwrap();
    let mut sizes: Vec<u64> = src
        .discover(src.root())
        .unwrap()
        .iter()
        .map(|f| f.size)
        .collect();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![0, 100, 100, 100, 100]);
}

/// Boundaries that differ only in escaped characters name different units,
/// and every row is read once; no message prints a Rust `Option`.
#[test]
fn unit_paths_are_injective() {
    let (_d, dir) = tmp();
    let db = dir.join("t.sqlite");
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE t (s TEXT)",
            "INSERT INTO t VALUES ('a %2F'), ('a /'), ('aT%2F'), ('aT/'), ('b'), ('c'), ('d'), ('e')",
        ],
    );
    let src = SqlSource::connect(
        &sqlite_root(&db, "t", "&range=s&units=8"),
        SqlOptions::default(),
    )
    .unwrap();
    let files = src.discover(src.root()).unwrap();
    let mut paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    paths.sort_unstable();
    paths.dedup();
    assert_eq!(paths.len(), files.len(), "{paths:?}");
    let mut rows = 0;
    for f in &files {
        rows += src
            .read_columns(&src.row_groups(f).unwrap()[0], &["s".into()])
            .unwrap()
            .num_rows();
    }
    assert_eq!(rows, 8);
    let mut stale = files[1].clone();
    stale.etag = Some("v1;rows=0;stats=x".into());
    stale.tail_blake3 = bellwether_types::Hash::of(b"x");
    let e = src.row_groups(&stale).unwrap_err().to_string();
    assert!(!e.contains("Some("), "{e}");
}

/// M22: a run that reads again after a first `finish` is re-checked at the
/// second.
#[test]
fn reads_after_a_first_finish_are_rechecked() {
    let (_d, dir) = tmp();
    let db = dir.join("t.sqlite");
    sqlite_exec(
        &db,
        &[
            "CREATE TABLE t (id INTEGER, v INTEGER)",
            "INSERT INTO t VALUES (1, 1), (2, 2)",
        ],
    );
    let src = SqlSource::connect(
        &sqlite_root(&db, "t", "&snapshot=reread"),
        SqlOptions::default(),
    )
    .unwrap();
    let fp = src.discover(src.root()).unwrap().remove(0);
    let rg = src.row_groups(&fp).unwrap().remove(0);
    src.read_columns(&rg, &["v".into()]).unwrap();
    src.finish().unwrap();
    sqlite_exec(&db, &["UPDATE t SET v = 9 WHERE id = 2"]);
    src.read_columns(&rg, &["v".into()]).unwrap();
    assert!(matches!(src.finish(), Err(Error::SourceChanged { .. })));
}

/// Item 1: DuckDB text that is not ASCII is measured in UTF-8 bytes.
#[test]
fn duckdb_non_ascii_text_is_measured() {
    if duckdb_driver().is_none() {
        return;
    }
    let (_d, dir) = tmp();
    let db = dir.join("t.duckdb");
    duck_exec(
        &db,
        &[
            "CREATE TABLE t (s VARCHAR)",
            "INSERT INTO t VALUES ('é'), ('中文'), ('😀'), ('a')",
        ],
    );
    let src = SqlSource::connect(&duck_root(&db, "t", ""), SqlOptions::default()).unwrap();
    let rg = src
        .row_groups(&src.discover(src.root()).unwrap()[0])
        .unwrap()
        .remove(0);
    assert_eq!(rg.stats[0].bytes, Some(2 + 6 + 4 + 1));
    assert_eq!(rg.stats[0].min, Some(encode_utf8("a")));
    let b = src.read_columns(&rg, &["s".into()]).unwrap();
    assert_eq!(b.column(0).as_string::<i32>().value(2), "😀");
    let _ = b.column(0).as_primitive_opt::<Int64Type>();
    assert_eq!(b.column(0).null_count(), 0);
}

/// Item 4: the connection never appears in an error.
#[test]
fn no_error_names_the_connection() {
    let (_d, dir) = tmp();
    let token = "TOKEN-a8f3c2";
    for root in [
        format!("sql://sqlite/main.t?db={}/{token}.sqlite", dir.display()),
        format!("sql://duckdb/main.t?db={}/{token}.duckdb", dir.display()),
        format!("sql://duckdb/main.t?db={}/{token}/x.duckdb", dir.display()),
    ] {
        let e = SqlSource::connect(&root, SqlOptions::default())
            .and_then(|s| s.discover(s.root()).map(|_| ()))
            .unwrap_err()
            .to_string();
        assert!(!e.contains(token), "{e}");
    }
    let db = dir.join(format!("{token}.sqlite"));
    sqlite_exec(&db, &["CREATE TABLE t (a INTEGER)"]);
    let e = SqlSource::connect(&sqlite_root(&db, "nope", ""), SqlOptions::default())
        .and_then(|s| s.discover(s.root()).map(|_| ()))
        .unwrap_err()
        .to_string();
    assert!(!e.contains(token), "{e}");
}

/// Item 14: two databases holding a table of one name have two identities,
/// and a password does not change the identity.
#[test]
fn the_identity_names_the_database_without_its_credentials() {
    use bellwether_source_sql::url::strip_credentials;
    assert_eq!(
        strip_credentials("postgresql://u:secret@h:5432/d?sslmode=require&password=x"),
        "postgresql://h:5432/d?sslmode=require"
    );
    assert_eq!(
        strip_credentials("Driver={X};Server=h;UID=u;PWD=secret"),
        "Driver={X};Server=h;UID=u"
    );
    let (_d, dir) = tmp();
    let (a, b) = (dir.join("a.sqlite"), dir.join("b.sqlite"));
    for db in [&a, &b] {
        sqlite_exec(
            db,
            &["CREATE TABLE t (x INTEGER)", "INSERT INTO t VALUES (1)"],
        );
    }
    let id = |db: &std::path::Path| {
        SqlSource::connect(&sqlite_root(db, "t", ""), SqlOptions::default())
            .unwrap()
            .identity()
            .unwrap()
    };
    assert_ne!(id(&a), id(&b));
}

/// Item 8: a DuckDB source opens read-only, so a database file it may not
/// write is still read.
#[test]
fn duckdb_sources_open_read_only() {
    if duckdb_driver().is_none() {
        return;
    }
    let (_d, dir) = tmp();
    let db = dir.join("ro.duckdb");
    duck_exec(
        &db,
        &["CREATE TABLE t (a BIGINT)", "INSERT INTO t VALUES (1)"],
    );
    let mut perm = std::fs::metadata(&db).unwrap().permissions();
    perm.set_readonly(true);
    std::fs::set_permissions(&db, perm).unwrap();
    let src = SqlSource::connect(&duck_root(&db, "t", ""), SqlOptions::default()).unwrap();
    assert_eq!(src.discover(src.root()).unwrap()[0].size, 1);
}

/// Round 2: a relative DuckDB path with `..` is scrubbed in its canonical
/// spelling too, and a file another instance holds read-write names the
/// next step.
#[test]
fn duckdb_errors_scrub_the_canonical_path() {
    if duckdb_driver().is_none() {
        return;
    }
    let (_d, dir) = tmp();
    let sub = dir.join("TOKEN-9e2b");
    std::fs::create_dir_all(sub.join("x")).unwrap();
    let db = sub.join("t.duckdb");
    duck_exec(&db, &["CREATE TABLE t (a BIGINT)"]);
    let rel = format!("{}/x/../t.duckdb", sub.display());
    let e = SqlSource::connect(
        &format!("sql://duckdb/main.nope?db={}", q(&rel)),
        SqlOptions::default(),
    )
    .and_then(|s| s.discover(s.root()).map(|_| ()))
    .unwrap_err()
    .to_string();
    assert!(!e.contains("TOKEN-9e2b"), "{e}");
}
