//! Postgres through a local server (`BELLWETHER_PG_URL`, with the ADBC
//! Postgres driver found by name or at `BELLWETHER_ADBC_POSTGRESQL_DRIVER`)
//! and SQL Server through ODBC (`BELLWETHER_MSSQL_URL`, an ODBC connection
//! string, with the `odbc` feature). Each test says why it skipped when its
//! server is not configured.
#![allow(clippy::unwrap_used, clippy::doc_markdown)]

mod common;

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use bellwether_source::canonical::{encode_as, normalize_float_bounds};
use bellwether_source_sql::{open_driver, Driver, SqlOptions, SqlSource, SqlUrl};
use bellwether_types::{Error, Source};

fn env(var: &str, what: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("SKIPPED: {what}: set {var} to run it");
            None
        }
    }
}

fn pg_driver_param() -> String {
    std::env::var("BELLWETHER_ADBC_POSTGRESQL_DRIVER")
        .map(|p| format!("&driver={}", common::q(&p)))
        .unwrap_or_default()
}

fn pg_root(schema: &str, table: &str, extra: &str) -> String {
    format!(
        "sql://postgres/{schema}.{table}?db=env:BELLWETHER_PG_URL{}{extra}",
        pg_driver_param()
    )
}

fn pg_exec(statements: &[&str]) {
    let url = SqlUrl::parse(&pg_root("public", "x", "")).unwrap();
    let mut d: Box<dyn Driver> = open_driver(&url).unwrap();
    for s in statements {
        d.execute(s).unwrap_or_else(|e| panic!("{s}: {e}"));
    }
}

/// Statistics equal an oracle over the rows read, strings bound in byte
/// order (`'B' < 'a'`) whatever the database collation; declared
/// partitions are the units; numeric(10,2) and both timestamp kinds read.
#[test]
fn postgres_units_statistics_and_snapshot() {
    if env("BELLWETHER_PG_URL", "the Postgres tests").is_none() {
        return;
    }
    let s = format!("bw_test_{}", std::process::id());
    pg_exec(&[
        &format!("DROP SCHEMA IF EXISTS {s} CASCADE"),
        &format!("CREATE SCHEMA {s}"),
        &format!(
            "CREATE TABLE {s}.t (id bigint NOT NULL, name text, amount numeric(10,2), ts \
             timestamp, tz timestamptz, flag boolean) PARTITION BY RANGE (id)"
        ),
        &format!("CREATE TABLE {s}.t_lo PARTITION OF {s}.t FOR VALUES FROM (0) TO (50)"),
        &format!("CREATE TABLE {s}.t_hi PARTITION OF {s}.t FOR VALUES FROM (50) TO (1000)"),
        &format!(
            "INSERT INTO {s}.t SELECT i, CASE WHEN i % 11 = 0 THEN NULL WHEN i % 2 = 0 THEN 'B' \
             || i ELSE 'a' || i END, CASE WHEN i % 13 = 0 THEN NULL ELSE i * 1.25 END, \
             timestamp '2024-01-01' + i * interval '1 second', timestamptz '2024-01-01 \
             00:00:00+00' + i * interval '1 millisecond', i % 2 = 0 FROM generate_series(1, 99) i"
        ),
        &format!("CREATE TABLE {s}.plain AS SELECT * FROM {s}.t"),
        // Review items 2, 3, M11: bytea and character(n) get no bounds; an
        // ICU-collated column is still bounded in byte order.
        &format!("CREATE TABLE {s}.odd (b bytea, c character(4), u text COLLATE \"und-x-icu\")"),
        &format!("INSERT INTO {s}.odd VALUES ('\\x01', 'ab', 'a'), ('\\x02', E'ab\\t', 'B')"),
    ]);
    let odd = SqlSource::connect(&pg_root(&s, "odd", ""), SqlOptions::default()).unwrap();
    let rg = odd
        .row_groups(&odd.discover(odd.root()).unwrap()[0])
        .unwrap()
        .remove(0);
    assert_eq!((&rg.stats[0].min, &rg.stats[1].min), (&None, &None));
    assert_eq!(
        rg.stats[2].min,
        Some(bellwether_source::canonical::encode_utf8("B"))
    );
    drop(odd);
    let src = SqlSource::connect(&pg_root(&s, "t", ""), SqlOptions::default()).unwrap();
    let schema = src.schema(src.root()).unwrap();
    let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let files = src.discover(src.root()).unwrap();
    assert_eq!(files.len(), 2, "one unit per declared partition");
    assert!(files[0].path.contains("/partition="), "{}", files[0].path);
    let mut ids = Vec::new();
    for fp in &files {
        let rg = src.row_groups(fp).unwrap().remove(0);
        let b = src.read_columns(&rg, &cols).unwrap();
        for (i, f) in schema.fields().iter().enumerate() {
            let a = b.column(i);
            let enc: Vec<Vec<u8>> = (0..a.len())
                .filter_map(|r| encode_as(a.as_ref(), r, f.data_type()).unwrap())
                .collect();
            let (lo, hi) =
                normalize_float_bounds(enc.iter().min().cloned(), enc.iter().max().cloned());
            assert_eq!(
                (&rg.stats[i].min, &rg.stats[i].max),
                (&lo, &hi),
                "{}",
                f.name()
            );
            assert_eq!(rg.stats[i].null_count, Some(a.null_count() as u64));
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
    assert_eq!(ids, (1..=99).collect::<Vec<_>>());
    // REPEATABLE READ: a write committed now is not seen by this run.
    pg_exec(&[&format!("UPDATE {s}.t SET amount = 0 WHERE id = 3")]);
    let run = src.finish().unwrap();
    assert!(run.snapshot.unwrap().contains("REPEATABLE READ"));
    drop(src);

    // Without a snapshot: a write is refused at the end; a partition
    // added mid-run is a unit that appeared.
    let src =
        SqlSource::connect(&pg_root(&s, "t", "&snapshot=reread"), SqlOptions::default()).unwrap();
    src.discover(src.root()).unwrap();
    pg_exec(&[&format!(
        "CREATE TABLE {s}.t_top PARTITION OF {s}.t FOR VALUES FROM (1000) TO (2000)"
    )]);
    let e = src.finish().unwrap_err();
    assert!(
        matches!(&e, Error::SourceChanged { units, .. } if units.iter().any(|u| u.ends_with("t_top"))),
        "{e}"
    );
    // Key ranges on a plain table.
    let src = SqlSource::connect(
        &pg_root(&s, "plain", "&range=name&units=4"),
        SqlOptions::default(),
    )
    .unwrap();
    let n: u64 = src
        .discover(src.root())
        .unwrap()
        .iter()
        .map(|f| f.size)
        .sum();
    assert_eq!(n, 99);
    drop(src);
    pg_exec(&[&format!("DROP SCHEMA {s} CASCADE")]);
}

#[cfg(feature = "odbc")]
#[test]
fn sql_server_through_odbc() {
    let Some(_) = env("BELLWETHER_MSSQL_URL", "the SQL Server tests") else {
        return;
    };
    let url = SqlUrl::parse("sql://mssql/dbo.bw_test?db=env:BELLWETHER_MSSQL_URL").unwrap();
    let mut d = open_driver(&url).unwrap();
    for s in [
        "IF OBJECT_ID('dbo.bw_test') IS NOT NULL DROP TABLE dbo.bw_test",
        "CREATE TABLE dbo.bw_test (id BIGINT NOT NULL, amount DECIMAL(10,2), ts DATETIME2(3))",
        "INSERT INTO dbo.bw_test VALUES (1, 1.25, '2024-01-01T00:00:00.001'), (2, NULL, NULL)",
    ] {
        d.execute(s).unwrap();
    }
    let src = SqlSource::connect(
        "sql://mssql/dbo.bw_test?db=env:BELLWETHER_MSSQL_URL",
        SqlOptions::default(),
    )
    .unwrap();
    let fps = src.discover(src.root()).unwrap();
    assert_eq!(fps[0].size, 2);
    let rg = src.row_groups(&fps[0]).unwrap().remove(0);
    let b = src
        .read_columns(&rg, &["id".into(), "amount".into(), "ts".into()])
        .unwrap();
    assert_eq!(b.num_rows(), 2);
    src.finish().unwrap();
    d.execute("DROP TABLE dbo.bw_test").unwrap();
}

#[cfg(not(feature = "odbc"))]
#[test]
fn sql_server_through_odbc() {
    eprintln!(
        "SKIPPED: the SQL Server tests: build with --features bellwether-source-sql/odbc and set \
         BELLWETHER_MSSQL_URL to run them"
    );
}
