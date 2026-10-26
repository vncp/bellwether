//! The reference adapter against a ClickHouse server: submit runs read-only
//! and the query log yields its statistics for that run; export is itself
//! the measured run (one log entry, its result rows the rows exported),
//! writes whole Parquet, and is refused (`ExportDenied`) without the
//! permission; a run served from a cache, or turning one on, is refused
//! (`cache_served`), while the user's own cache settings never reach a
//! bellwether run; a log entry another client wrote under the tag is
//! refused (`ambiguous_log_entry`, `not_the_query_submitted`); the
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
use bellwether_types::{Error, QueryStats};

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

/// The reason of a measurement refusal, or the statistics.
fn reason(r: bellwether_types::Result<QueryStats>) -> std::result::Result<QueryStats, String> {
    match r {
        Ok(s) => Ok(s),
        Err(Error::MeasurementRefused { reason, .. }) => Err(reason.to_string()),
        Err(e) => panic!("{e}"),
    }
}

/// A handle for a run of `query` under `tag` that bellwether did not make.
fn handle(tag: &Tag, query: &str) -> ResultHandle {
    ResultHandle {
        tag: tag.clone(),
        query: query.into(),
        logged: query.into(),
        exported: None,
    }
}

fn parquet_rows(path: &std::path::Path) -> usize {
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(path).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap()
    .map(|b| b.unwrap().num_rows())
    .sum()
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
    assert_eq!(
        (h.query.as_str(), h.logged.as_str()),
        (q.as_str(), q.as_str())
    );
    let st = e.stats(&h).unwrap();
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
    // A failed query is refused, and so is reading its cost.
    let bad = Tag::new(&run, "p2-before").unwrap();
    let failing = "SELECT no_such_column FROM system.one";
    assert!(e.submit(failing, &bad).is_err());
    let missing = e.stats(&handle(&bad, failing)).unwrap_err().to_string();
    assert!(
        missing.contains("failed") || missing.contains("no finished entry"),
        "{missing}"
    );
}

#[test]
fn export_is_the_measured_run_and_is_refused_without_the_permission() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let e = connect(&s.table("orders"), None).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let run = Tag::run();
    let q = format!(
        "SELECT id, amount FROM {}.orders WHERE id % 10 = 3 ORDER BY id",
        s.db
    );
    let tag = Tag::new(&run, "p0-after").unwrap();
    let out = dir.path().join("p0").join("after");
    let h = e.export(&q, &tag, &out).unwrap();
    let x = h.exported.clone().unwrap();
    assert_eq!(x.files.len(), 1);
    assert_eq!(parquet_rows(&x.files[0]), 300);
    // The export is the run measured: one entry under its tag, whose
    // result rows are the rows exported.
    let st = e.stats(&h).unwrap();
    assert_eq!((st.tag.as_str(), st.result_rows), (tag.as_str(), 300));
    s.sql("SYSTEM FLUSH LOGS");
    let logged = s.sql(&format!(
        "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND \
         startsWith(query_id, '{}')",
        tag.as_str()
    ));
    assert_eq!(logged.trim(), "1", "one run, under the tag itself");
    // A fresh folder only.
    let err = e.export(&q, &tag, &out).unwrap_err().to_string();
    assert!(err.contains("not empty"), "{err}");
    // A scratch folder that cannot be written: export_denied naming it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let ro = dir.path().join("ro");
        std::fs::create_dir(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o500)).unwrap();
        let err = e
            .export(&q, &Tag::new(&run, "p1-after").unwrap(), &ro.join("x"))
            .unwrap_err();
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
    let err = denied
        .export(
            &q,
            &Tag::new(&run, "p2-after").unwrap(),
            &dir.path().join("denied"),
        )
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

/// The review's probes: a `SETTINGS` clause in the agent's text turns the
/// query cache on (round 1), and also turns the logging of profile events
/// and settings off, so the cache counters read 0 (round 2); or it turns
/// the query condition cache on. Every run is `readonly=1`, so the engine
/// refuses the text; and a log entry that is not fully logged (as a run
/// with those settings leaves it) is refused even when another route lets
/// it through.
#[test]
fn a_query_text_that_changes_a_setting_or_an_entry_not_fully_logged_is_refused() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let e = connect(&s.table("orders"), None).unwrap();
    let run = Tag::run();
    let base = format!(
        "SELECT count(), sum(amount) FROM {}.orders WHERE id % 11 = 3",
        s.db
    );
    let hidden = format!(
        "{base} SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, \
         query_cache_ttl = 600, log_profile_events = 0, log_query_settings = 0"
    );
    let condition = format!("{base} SETTINGS use_query_condition_cache = 1");
    let cached = format!(
        "{base} SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, \
         query_cache_ttl = 600"
    );
    for (i, q) in [&hidden, &condition, &cached].into_iter().enumerate() {
        for run_kind in ["submit", "export"] {
            let t = Tag::new(&run, &format!("s{i}-{run_kind}")).unwrap();
            let dir = tempfile::tempdir().unwrap();
            let err = if run_kind == "submit" {
                e.submit(q, &t).unwrap_err()
            } else {
                e.export(q, &t, &dir.path().join("x")).unwrap_err()
            }
            .to_string();
            assert!(err.contains("read-only mode"), "{q}: {err}");
            assert!(err.contains("propose the rewrite again"), "{err}");
        }
    }
    // The same text run by another route (the old readonly=2), twice, so
    // the second run is served from the cache with nothing logged: its
    // entry is refused as not fully logged.
    for i in 0..2 {
        let t = Tag::new(&run, &format!("r{i}")).unwrap();
        let body = reqwest::blocking::Client::new()
            .post(format!(
                "{}?query_id={t}&readonly=2&use_query_cache=0&default_format=Null",
                s.http
            ))
            .body(hidden.clone())
            .send()
            .unwrap()
            .text()
            .unwrap();
        assert!(!body.contains("Exception"), "{body}");
        assert_eq!(
            reason(e.stats(&handle(&t, &hidden))).unwrap_err(),
            "statistics_missing",
            "run {i}"
        );
    }
}

/// A user whose profile sets readonly (a common read-only setup): no
/// request may set its own read-only mode, so the adapter refuses at
/// connect, naming the user's profile, never a rewrite.
#[test]
fn a_user_whose_profile_sets_readonly_is_named_at_connect() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let user = format!("{}_ro", s.db);
    s.sql(&format!(
        "CREATE USER {user} IDENTIFIED WITH no_password SETTINGS readonly = 1"
    ));
    s.sql(&format!("GRANT SELECT ON {}.* TO {user}", s.db));
    s.sql(&format!("GRANT SELECT ON system.* TO {user}"));
    let err = connect(
        &format!(
            "clickhouse://{user}@{}",
            s.root.trim_start_matches("clickhouse://")
        ),
        None,
    )
    .map(|_| ())
    .unwrap_err()
    .to_string();
    s.sql(&format!("DROP USER {user}"));
    assert!(err.contains("profile"), "{err}");
    assert!(err.contains("no readonly in its profile"), "{err}");
    assert!(!err.contains("rewrite"), "{err}");
}

/// A server or user that does not log profile events: every entry is
/// refused as not fully logged, never read as "no cache, nothing read".
#[test]
fn a_user_whose_queries_are_not_fully_logged_is_refused() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let user = format!("{}_unlogged", s.db);
    s.sql(&format!(
        "CREATE USER {user} IDENTIFIED WITH no_password SETTINGS log_profile_events = 0"
    ));
    s.sql(&format!("GRANT SELECT ON {}.* TO {user}", s.db));
    s.sql(&format!("GRANT SELECT ON system.* TO {user}"));
    s.sql(&format!("GRANT SYSTEM FLUSH LOGS ON *.* TO {user}"));
    let e = connect(
        &format!(
            "clickhouse://{user}@{}",
            s.root.trim_start_matches("clickhouse://")
        ),
        None,
    )
    .unwrap();
    let q = format!("SELECT count() FROM {}.orders", s.db);
    let h = e
        .submit(&q, &Tag::new(&Tag::run(), "p0-before").unwrap())
        .unwrap();
    let refused = reason(e.stats(&h));
    s.sql(&format!("DROP USER {user}"));
    assert_eq!(refused.unwrap_err(), "statistics_missing");
}

/// The user's own settings turn the query cache on (and the query
/// condition cache is on by default): every bellwether run turns both off,
/// so the same query measures the same bytes twice and is never refused.
#[test]
fn a_user_profile_s_caches_never_reach_a_measured_run() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let user = format!("{}_cached", s.db);
    s.sql(&format!(
        "CREATE USER {user} IDENTIFIED WITH no_password SETTINGS use_query_cache = 1, \
         query_cache_min_query_runs = 0, query_cache_ttl = 600"
    ));
    s.sql(&format!("GRANT SELECT ON {}.* TO {user}", s.db));
    s.sql(&format!("GRANT SELECT ON system.* TO {user}"));
    s.sql(&format!("GRANT SYSTEM FLUSH LOGS ON *.* TO {user}"));
    let e = connect(
        &format!(
            "clickhouse://{user}@{}",
            s.root.trim_start_matches("clickhouse://")
        ),
        None,
    )
    .unwrap();
    let run = Tag::run();
    let q = format!(
        "SELECT id, amount FROM {}.orders WHERE amount > 100 AND id % 3 = 1",
        s.db
    );
    let measured: Vec<QueryStats> = (0..2)
        .map(|i| {
            let h = e
                .submit(&q, &Tag::new(&run, &format!("u{i}")).unwrap())
                .unwrap();
            reason(e.stats(&h)).unwrap()
        })
        .collect();
    s.sql(&format!("DROP USER {user}"));
    assert_eq!(measured[0].read_bytes, measured[1].read_bytes);
    assert_eq!(measured[0].read_rows, measured[1].read_rows);
    assert!(measured[0].read_rows >= 3000, "{:?}", measured[0]);
}

/// The reviewer's probe: another client reuses bellwether's query id
/// after its run. Two finished entries under the tag: refused. An entry
/// with another text or user than the run's: refused.
#[test]
fn a_log_entry_another_client_wrote_under_the_tag_is_refused() {
    let Some(s) = Server::new() else { return };
    orders(&s);
    let e = connect(&s.table("orders"), None).unwrap();
    let run = Tag::run();
    let t = Tag::new(&run, "p0-before").unwrap();
    let q = format!("SELECT count(), sum(amount) FROM {}.orders", s.db);
    let h = e.submit(&q, &t).unwrap();
    let other = |tag: &Tag, text: &str| {
        reqwest::blocking::Client::new()
            .post(format!("{}?query_id={}&default_format=Null", s.http, tag))
            .body(text.to_string())
            .send()
            .unwrap()
            .text()
            .unwrap()
    };
    other(&t, "SELECT 1");
    assert_eq!(reason(e.stats(&h)).unwrap_err(), "ambiguous_log_entry");
    // One entry, not the run's text.
    let u = Tag::new(&run, "p1-before").unwrap();
    other(&u, "SELECT 1");
    assert_eq!(
        reason(e.stats(&handle(&u, &q))).unwrap_err(),
        "not_the_query_submitted"
    );
    // One entry with the run's text, logged for another user.
    let user = format!("{}_other", s.db);
    s.sql(&format!("CREATE USER {user} IDENTIFIED WITH no_password"));
    s.sql(&format!("GRANT SELECT ON {}.* TO {user}", s.db));
    let v = Tag::new(&run, "p2-before").unwrap();
    reqwest::blocking::Client::new()
        .post(format!("{}?query_id={}&default_format=Null", s.http, v))
        .header("X-ClickHouse-User", &user)
        .body(q.clone())
        .send()
        .unwrap()
        .text()
        .unwrap();
    let refused = reason(e.stats(&handle(&v, &q)));
    s.sql(&format!("DROP USER {user}"));
    assert_eq!(refused.unwrap_err(), "not_the_query_submitted");
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
