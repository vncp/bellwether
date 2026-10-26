//! The reference adapter against a ClickHouse server: submit runs read-only
//! and the query log yields its statistics by the tag; export writes whole
//! Parquet and is refused (`ExportDenied`) without the permission; the
//! identity follows inserts and mutations and ignores merges; units read
//! back exactly at their declared types.
//!
//! Runs when `BELLWETHER_CLICKHOUSE_URL` names a server
//! (`clickhouse://127.0.0.1:8123/default`, the user needing CREATE DATABASE
//! and access management); prints `SKIPPED:` and passes otherwise, except
//! in CI (`CI` set), where the workflow starts one and the tests must run.
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, StringArray, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, TimeUnit};
use bellwether_source_engine::{connect, identity, EngineUri, ResultHandle, Tag};
use bellwether_types::Error;

struct Server {
    http: String,
    root: String,
    db: String,
}

impl Server {
    /// A fresh database on the configured server, dropped on drop.
    fn new() -> Option<Self> {
        let Ok(url) = std::env::var("BELLWETHER_CLICKHOUSE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "CI must run the ClickHouse adapter tests: set BELLWETHER_CLICKHOUSE_URL"
            );
            eprintln!(
                "SKIPPED: BELLWETHER_CLICKHOUSE_URL is not set (for example \
                 clickhouse://127.0.0.1:8123/default); the ClickHouse adapter tests need a server"
            );
            return None;
        };
        let u = EngineUri::parse(&url).unwrap();
        let db = format!("bw_{}", Tag::run());
        let s = Self {
            http: format!("http://{}/", u.host),
            root: format!("clickhouse://{}/{db}", u.host),
            db,
        };
        s.sql(&format!("CREATE DATABASE {}", s.db));
        Some(s)
    }

    fn sql(&self, q: &str) -> String {
        let r = reqwest::blocking::Client::new()
            .post(&self.http)
            .body(q.to_string())
            .send()
            .unwrap();
        let ok = r.status().is_success();
        let text = r.text().unwrap();
        assert!(ok, "{q}: {text}");
        text
    }

    fn table(&self, t: &str) -> String {
        format!("{}/{t}", self.root)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = reqwest::blocking::Client::new()
            .post(&self.http)
            .body(format!("DROP DATABASE IF EXISTS {}", self.db))
            .send();
    }
}

fn orders(s: &Server) {
    s.sql(&format!(
        "CREATE TABLE {}.orders (id UInt64, batch UInt32, amount Decimal(18, 4), ts DateTime64(3), \
         note Nullable(String)) ENGINE = MergeTree PARTITION BY intDiv(id, 1000) ORDER BY id",
        s.db
    ));
    s.sql(&format!(
        "INSERT INTO {}.orders SELECT number, intDiv(number, 500), toDecimal64(number, 4) / 7, \
         toDateTime64('2024-01-01 00:00:00', 3, 'UTC') + number, \
         if(number % 5 = 0, NULL, toString(number)) FROM numbers(3000)",
        s.db
    ));
}

#[test]
fn submit_is_read_only_and_the_log_holds_its_statistics_by_tag() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let e = connect(&s.table("orders"), None).unwrap();
    assert_eq!(e.model(), "clickhouse.query_log");
    let run = Tag::run();
    let t = Tag::new(&run, "p0-before").unwrap();
    let q = format!("SELECT id, amount FROM {}.orders WHERE id < 1500", s.db);
    let h = e.submit(&q, &t).unwrap();
    assert_eq!(h.query, q);
    let st = e.stats(&t).unwrap();
    assert_eq!(st.tag, t.as_str());
    assert_eq!(st.result_rows, 1500);
    assert!(st.read_rows >= 1500 && st.read_rows <= 3000, "{st:?}");
    assert!(st.read_bytes > 0 && st.peak_memory_bytes > 0, "{st:?}");
    assert!(st.units_read.unwrap() >= 2, "{st:?}");
    // A write is refused by the engine's own read-only setting, and the
    // table is unchanged.
    let w = Tag::new(&run, "p1-before").unwrap();
    let err = e
        .submit(
            &format!("INSERT INTO {}.orders (id) VALUES (99999)", s.db),
            &w,
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("readonly") || err.contains("READONLY"),
        "{err}"
    );
    assert_eq!(
        s.sql(&format!("SELECT count() FROM {}.orders", s.db))
            .trim(),
        "3000"
    );
    // A tag never submitted has no log entry; a failed query is refused.
    let bad = Tag::new(&run, "p2-before").unwrap();
    assert!(e
        .submit("SELECT no_such_column FROM system.one", &bad)
        .is_err());
    let missing = e.stats(&bad).unwrap_err().to_string();
    assert!(
        missing.contains("failed") || missing.contains("no finished entry"),
        "{missing}"
    );
}

#[test]
fn export_writes_whole_parquet_and_is_refused_without_the_permission() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let e = connect(&s.table("orders"), None).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let run = Tag::run();
    let q = format!(
        "SELECT id, amount FROM {}.orders WHERE id % 10 = 3 ORDER BY id",
        s.db
    );
    let h = e.submit(&q, &Tag::new(&run, "p0-after").unwrap()).unwrap();
    let out = dir.path().join("p0").join("after");
    let x = e.export(&h, &out).unwrap();
    assert_eq!(x.files.len(), 1);
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&x.files[0]).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, 300);
    // A fresh folder only.
    let err = e.export(&h, &out).unwrap_err().to_string();
    assert!(err.contains("not empty"), "{err}");
    // A scratch folder that cannot be written: export_denied naming it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let ro = dir.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let err = e.export(&h, &ro.join("x")).unwrap_err();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o700)).unwrap();
        match err {
            Error::ExportDenied { permission, .. } => {
                assert!(permission.contains("write access"), "{permission}");
            }
            other => panic!("{other}"),
        }
    }
    // A user the engine refuses for the queried table: export_denied
    // naming the grant.
    let user = format!("{}_reader", s.db);
    s.sql(&format!("CREATE USER {user} IDENTIFIED WITH no_password"));
    s.sql(&format!("GRANT SELECT ON system.* TO {user}"));
    let denied = connect(
        &format!(
            "clickhouse://{user}@{}",
            s.root.trim_start_matches("clickhouse://")
        ),
        None,
    )
    .unwrap();
    let handle = ResultHandle {
        tag: Tag::new(&run, "p1-after").unwrap(),
        query: q.clone(),
    };
    let err = denied
        .export(&handle, &dir.path().join("denied"))
        .unwrap_err();
    s.sql(&format!("DROP USER {user}"));
    match err {
        Error::ExportDenied {
            permission,
            message,
            ..
        } => {
            assert!(permission.contains("SELECT"), "{permission}");
            assert!(message.contains("grant"), "{message}");
        }
        other => panic!("{other}"),
    }
    assert!(
        !dir.path().join("denied").exists(),
        "a refused export leaves nothing"
    );
}

#[test]
fn the_identity_follows_inserts_and_mutations_and_ignores_merges() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    s.sql(&format!(
        "INSERT INTO {}.orders SELECT number, 0, 0, now64(3), NULL FROM numbers(10)",
        s.db
    ));
    let id = |s: &Server| identity(&*connect(&s.table("orders"), None).unwrap()).unwrap();
    let a = id(&s);
    assert_eq!(a.units, 3);
    assert_eq!(a.rows, 3010);
    s.sql(&format!("OPTIMIZE TABLE {}.orders FINAL", s.db));
    assert_eq!(id(&s), a, "a merge changes no identity");
    s.sql(&format!("INSERT INTO {}.orders (id) VALUES (5)", s.db));
    let b = id(&s);
    assert_ne!(b, a);
    s.sql(&format!(
        "ALTER TABLE {}.orders UPDATE batch = 7 WHERE id = 5 SETTINGS mutations_sync = 1",
        s.db
    ));
    assert_ne!(id(&s), b, "a mutation changes the identity");
    // A table the user cannot see, or that is not MergeTree, is named.
    let err = connect(&s.table("nope"), None)
        .and_then(|e| identity(&*e))
        .unwrap_err()
        .to_string();
    assert!(err.contains("no table \"nope\""), "{err}");
    s.sql(&format!(
        "CREATE TABLE {}.mem (x UInt8) ENGINE = Memory",
        s.db
    ));
    let err = connect(&s.table("mem"), None)
        .and_then(|e| identity(&*e))
        .unwrap_err()
        .to_string();
    assert!(err.contains("Memory table"), "{err}");
}

#[test]
fn units_read_back_exactly_at_their_declared_types() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let e = connect(&s.table("orders"), None).unwrap();
    let schema = e.schema(e.root()).unwrap();
    assert_eq!(
        schema.field_with_name("id").unwrap().data_type(),
        &DataType::UInt64
    );
    assert_eq!(
        schema.field_with_name("amount").unwrap().data_type(),
        &DataType::Decimal128(18, 4)
    );
    assert_eq!(
        schema.field_with_name("ts").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    assert!(schema.field_with_name("note").unwrap().is_nullable());
    let units = e.discover(e.root()).unwrap();
    assert_eq!(units.len(), 3);
    let mut total = 0;
    for u in &units {
        let rg = &e.row_groups(u).unwrap()[0];
        assert_eq!(rg.rows, u.size);
        let cols: Vec<String> = ["id", "amount", "ts", "note"].map(String::from).to_vec();
        let b = e.read_columns(rg, &cols).unwrap();
        assert_eq!(b.num_rows() as u64, rg.rows);
        let id = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let amount = b
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let ts = b
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let note = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            let n = id.value(i);
            // toDecimal64(n, 4) / 7 at scale 4, truncated as ClickHouse does.
            assert_eq!(amount.value(i), i128::from(n) * 10_000 / 7, "id {n}");
            assert_eq!(
                ts.value(i),
                1_704_067_200_000 + i64::try_from(n).unwrap() * 1000
            );
            assert_eq!(note.is_null(i), n % 5 == 0);
        }
        total += b.num_rows();
        // No columns: the unit's row count.
        assert_eq!(e.read_columns(rg, &[]).unwrap().num_rows() as u64, rg.rows);
    }
    assert_eq!(total, 3000);
    let _ = Arc::new(0);
}
