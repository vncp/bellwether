//! Tables as sources through the facade (ADR 027): reconcile a table
//! against the Parquet files of the same rows (two decimal scales and two
//! time units under one dataset type), a planted difference found; `why`
//! over a table reports measured costs and records no Replay; `fix` over a
//! table verifies, writes the certificate, and refuses to apply naming the
//! statement a human runs, with no Receipt. SQLite always; DuckDB when
//! libduckdb is found (required in CI).
//!
//! Deliberate break: mark a measured cost exact once, and
//! `why_over_a_table_never_reports_or_stores_a_measured_cost_as_exact` fails.
#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bellwether::types::{Error, ProposalBody, Record, RewriteShape};
use bellwether::{
    Engine, EngineOptions, FixOutcome, FixRequest, KeyFile, ProposeResult, ReconcileRequest,
    Refusal, RewriteOutputs, Score, WhyRequest, MEASURED_AGGREGATES,
};
use bellwether_source_sql::{open_driver, EngineKind, SnapshotPolicy, SqlUrl, TableName};
use parquet::arrow::ArrowWriter;

const N: i64 = 120;
const BASE_US: i64 = 1_704_067_200_000_000;

fn amount_cents(i: i64) -> Option<i64> {
    (i % 9 != 0).then(|| i * 100 + 25 * (i % 4))
}

fn name(i: i64) -> Option<String> {
    (i % 7 != 0).then(|| format!("z{}", i % 5))
}

/// The rows as Parquet, two files, at scale 4 and in microseconds.
fn files(dir: &Path) -> PathBuf {
    let out = dir.join("files");
    std::fs::create_dir_all(&out).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(12, 4), true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("name", DataType::Utf8, true),
    ]));
    for (f, range) in [(0, 1..=N / 2), (1, N / 2 + 1..=N)] {
        let ids: Vec<i64> = range.collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
                Arc::new(
                    Decimal128Array::from(
                        ids.iter()
                            .map(|i| amount_cents(*i).map(|c| i128::from(c) * 100))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(12, 4)
                    .unwrap(),
                ),
                Arc::new(TimestampMicrosecondArray::from(
                    ids.iter()
                        .map(|i| Some(BASE_US + i * 1_000_000))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| name(*i)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(
            File::create(out.join(format!("part-{f}.parquet"))).unwrap(),
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    out
}

fn sqlite(dir: &Path) -> String {
    let db = dir.join("t.sqlite");
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute_batch(
        "CREATE TABLE trips (id BIGINT NOT NULL, amount DECIMAL(10,2), ts TIMESTAMP(3), name TEXT)",
    )
    .unwrap();
    for i in 1..=N {
        c.execute(
            "INSERT INTO trips VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                i,
                amount_cents(i).map(|a| format!("{}.{:02}", a / 100, a % 100)),
                BASE_US / 1000 + i * 1000,
                name(i)
            ],
        )
        .unwrap();
    }
    format!(
        "sql://sqlite/main.trips?db={}&range=id&units=3",
        db.display()
    )
}

fn duckdb_driver() -> Option<String> {
    if let Ok(p) = std::env::var("BELLWETHER_DUCKDB_DRIVER") {
        return Some(p);
    }
    let found = [
        "/opt/homebrew/lib/libduckdb.dylib",
        "/usr/local/lib/libduckdb.so",
        "/usr/lib/libduckdb.so",
    ]
    .into_iter()
    .find(|p| Path::new(p).is_file())
    .map(str::to_string);
    if found.is_none() {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must run the DuckDB tests"
        );
        eprintln!("SKIPPED: DuckDB: set BELLWETHER_DUCKDB_DRIVER to libduckdb to run it");
    }
    found
}

fn duckdb(dir: &Path) -> Option<String> {
    let driver = duckdb_driver()?;
    let db = dir.join("t.duckdb");
    let url = SqlUrl {
        engine: EngineKind::Duckdb,
        tables: vec![TableName {
            schema: "main".into(),
            table: "trips".into(),
        }],
        db: db.to_string_lossy().into_owned(),
        driver: Some(driver),
        range: None,
        version: None,
        snapshot: SnapshotPolicy::Transaction,
    };
    let mut d = open_driver(&url).unwrap();
    d.execute(
        "CREATE TABLE trips (id BIGINT NOT NULL, amount DECIMAL(10,2), ts TIMESTAMP_MS, name VARCHAR)",
    )
    .unwrap();
    d.execute(&format!(
        "INSERT INTO trips SELECT i, CASE WHEN i % 9 = 0 THEN NULL ELSE CAST((i * 100 + 25 * (i \
         % 4)) / 100.0 AS DECIMAL(10,2)) END, CAST(make_timestamp({BASE_US} + i * 1000000) AS \
         TIMESTAMP_MS), CASE WHEN i % 7 = 0 THEN NULL ELSE 'z' || (i % 5) END FROM range(1, {}) \
         r(i)",
        N + 1
    ))
    .unwrap();
    Some(format!(
        "sql://duckdb/main.trips?db={}&range=id&units=3",
        db.display()
    ))
}

fn roots(dir: &Path) -> Vec<(&'static str, String)> {
    let mut out = vec![("sqlite", sqlite(dir))];
    if let Some(d) = duckdb(dir) {
        out.push(("duckdb", d));
    }
    out
}

fn open(root: &str, index: &Path) -> Engine {
    Engine::open(
        root,
        EngineOptions {
            index: Some(index.to_string_lossy().into_owned()),
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

fn ledger_kinds(index: &Path) -> Vec<String> {
    let mut kinds: Vec<String> = std::fs::read_dir(index.join("ledger"))
        .map(|d| {
            d.map(|e| {
                let v: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(e.unwrap().path()).unwrap()).unwrap();
                v["record"].as_str().unwrap().to_string()
            })
            .collect()
        })
        .unwrap_or_default();
    kinds.sort();
    kinds
}

#[test]
fn a_table_reconciles_against_its_files_and_a_planted_difference_is_found() {
    let d = tempfile::tempdir().unwrap();
    let folder = files(d.path());
    for (engine, root) in roots(d.path()) {
        let index = d.path().join(format!("idx-{engine}"));
        let e = open(&root, &index);
        assert_eq!(e.root(), "sql://".to_string() + engine + "/main.trips");
        let req = ReconcileRequest {
            key: vec!["id".into()],
            totals: vec!["amount".into()],
            ..ReconcileRequest::default()
        };
        let r = e.reconcile(folder.to_str().unwrap(), &req).unwrap();
        assert!(r.outcome.pass, "{engine}: {:?}", r.outcome.counts);
        assert_eq!(r.outcome.counts.keys_compared, N as u64);
        assert_eq!(r.tables.len(), 1);
        assert_eq!(r.tables[0].rows, N as u64);
        assert!(r.tables[0].snapshot.is_some());
        let Record::Certificate(c) = &r.certificate.record else {
            panic!()
        };
        // Both catalog digests and the table's identity.
        assert!(
            c.provenance.inputs.contains(&r.tables[0].identity),
            "{engine}"
        );
        assert!(
            c.provenance.inputs.len() >= 3,
            "{engine}: {:?}",
            c.provenance.inputs
        );
        let json = serde_json::to_string(&r).unwrap();
        assert!(
            !json.contains("db="),
            "{engine}: the connection is never recorded"
        );

        // The table on the right of a folder.
        let f = Engine::open(
            folder.to_str().unwrap(),
            EngineOptions {
                index: Some(
                    d.path()
                        .join(format!("fidx-{engine}"))
                        .to_string_lossy()
                        .into_owned(),
                ),
                ..EngineOptions::default()
            },
        )
        .unwrap();
        let r = f.reconcile(&root, &req).unwrap();
        assert!(r.outcome.pass, "{engine}: folder vs table");

        // A planted difference in the table.
        let fix = "UPDATE trips SET amount = 1.00 WHERE id = 17";
        if engine == "sqlite" {
            let db = root.split("db=").nth(1).unwrap().split('&').next().unwrap();
            rusqlite::Connection::open(db)
                .unwrap()
                .execute_batch(fix)
                .unwrap();
        } else {
            let url = SqlUrl::parse(&root).unwrap();
            open_driver(&url).unwrap().execute(fix).unwrap();
        }
        let r = open(&root, &index)
            .reconcile(folder.to_str().unwrap(), &req)
            .unwrap();
        assert!(!r.outcome.pass);
        assert_eq!(r.outcome.counts.mismatched, 1, "{engine}");
        assert_eq!(r.outcome.counts.totals_differ, 1, "{engine}");
    }
}

#[test]
fn a_scale_the_files_cannot_hold_is_refused_not_rounded() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("s.sqlite");
    let c = rusqlite::Connection::open(&db).unwrap();
    c.execute_batch("CREATE TABLE t (id BIGINT NOT NULL, amount DECIMAL(10,3)); INSERT INTO t VALUES (1, 1.125)")
        .unwrap();
    let folder = d.path().join("f");
    std::fs::create_dir_all(&folder).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(10, 2), true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![113])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(
        File::create(folder.join("a.parquet")).unwrap(),
        schema,
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let e = open(
        &format!("sql://sqlite/main.t?db={}", db.display()),
        &d.path().join("i"),
    );
    let err = e
        .reconcile(
            folder.to_str().unwrap(),
            &ReconcileRequest {
                key: vec!["id".into()],
                ..ReconcileRequest::default()
            },
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("not rounded"), "{err}");
    assert!(!d.path().join("i/ledger").exists() || ledger_kinds(&d.path().join("i")).is_empty());
}

/// The deliberate break "mark a measured cost exact once" fails here.
#[test]
fn why_over_a_table_never_reports_or_stores_a_measured_cost_as_exact() {
    let d = tempfile::tempdir().unwrap();
    let keys = d.path().join("keys.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3, 4, 5])) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(&keys).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    for (engine, root) in roots(d.path()) {
        let index = d.path().join(format!("w-{engine}"));
        let e = open(&root, &index);
        let r = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(&keys)),
                ..WhyRequest::default()
            })
            .unwrap();
        assert!(!r.exact, "{engine}");
        assert_eq!(r.model.as_deref(), Some(MEASURED_AGGREGATES));
        assert_eq!(r.tables.len(), 1);
        assert_ne!(r.candidates.len(), 0, "a candidate is priced");
        for c in &r.candidates {
            let Record::Replay(rp) = &c.replay.record else {
                panic!()
            };
            assert!(
                rp.current.iter().chain(&rp.proposed).all(|c| !c.exact),
                "{engine}"
            );
            assert_eq!(rp.model.as_deref(), Some(MEASURED_AGGREGATES));
            assert_eq!(c.replay.record.hash().unwrap(), c.replay.hash);
        }
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["exact"], serde_json::json!(false));
        let kinds = ledger_kinds(&index);
        assert!(
            !kinds.contains(&"replay".to_string()),
            "{engine}: {kinds:?}"
        );
        assert!(kinds.contains(&"evidence".to_string()));
        // The fold over the folder stays exact and silent about it.
        let folder = files(&d.path().join(engine));
        let f = Engine::open(folder.to_str().unwrap(), EngineOptions::default()).unwrap();
        let fr = f.why(&WhyRequest::default()).unwrap();
        assert!(fr.exact && fr.model.is_none());
        assert!(serde_json::to_value(&fr).unwrap().get("exact").is_none());
    }
}

#[test]
fn fix_over_a_table_is_attest_only_and_writes_no_receipt() {
    let d = tempfile::tempdir().unwrap();
    let keys = d.path().join("keys.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![10, 11])) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(&keys).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    for (engine, root) in roots(d.path()) {
        let index = d.path().join(format!("f-{engine}"));
        let e = open(&root, &index);
        let why = e
            .why(&WhyRequest {
                keys: Some(KeyFile::new(&keys)),
                ..WhyRequest::default()
            })
            .unwrap();
        let rec = why.recommended.clone().unwrap();
        let workload = why.workload.as_ref().unwrap().hash;
        let r = e
            .fix(&FixRequest {
                proposal: Some(rec.proposal),
                workload: Some(workload),
                ..FixRequest::default()
            })
            .unwrap();
        assert!(matches!(r.score, Some(Score::Measured { .. })), "{engine}");
        let cert = r.certificate.as_ref().unwrap().hash;
        let FixOutcome::Refused {
            refusal:
                Refusal::AttestOnly {
                    certificate,
                    target,
                    run,
                    ..
                },
        } = &r.outcome
        else {
            panic!("{engine}: {:?}", r.outcome);
        };
        assert_eq!(*certificate, cert);
        assert_eq!(target, &format!("sql://{engine}/main.trips"));
        assert_eq!(
            run,
            &vec!["CREATE INDEX \"bw_trips_id\" ON \"main\".\"trips\" (\"id\")".to_string()]
        );
        // The apply tier and its dry run refuse the same way.
        for out in [
            e.fix(&FixRequest {
                certificate: Some(cert),
                ..FixRequest::default()
            })
            .unwrap()
            .outcome,
            e.apply_dry_run(&cert).unwrap().outcome,
        ] {
            assert!(
                matches!(
                    out,
                    FixOutcome::Refused {
                        refusal: Refusal::AttestOnly { .. }
                    }
                ),
                "{engine}: {out:?}"
            );
        }
        let kinds = ledger_kinds(&index);
        assert!(
            !kinds.iter().any(|k| k == "receipt" || k == "replay"),
            "{engine}: {kinds:?}"
        );
        assert!(kinds.contains(&"certificate".to_string()));
    }
}

#[test]
fn folder_only_operations_refuse_a_table() {
    let d = tempfile::tempdir().unwrap();
    let root = sqlite(d.path());
    let e = open(&root, &d.path().join("i"));
    let refusal = |r: Result<(), Error>| match r.unwrap_err() {
        Error::NotAFolder { operation, .. } => operation,
        other => panic!("{other}"),
    };
    assert_eq!(
        refusal(
            e.index(&[bellwether::spec::parse("exact:id").unwrap()])
                .map(|_| ())
        ),
        "index"
    );
    assert_eq!(
        refusal(
            e.locate(&KeyFile::new(d.path().join("k.parquet")))
                .map(|_| ())
        ),
        "locate"
    );
    let e = Engine::open(&root, EngineOptions::default()).unwrap_err();
    assert!(
        matches!(e, Error::InvalidArgument { field: "index", .. }),
        "{e}"
    );
    let e = Engine::open(
        &root,
        EngineOptions {
            index: Some("x".into()),
            at_manifest: Some(1),
            ..EngineOptions::default()
        },
    )
    .unwrap_err();
    assert!(matches!(e, Error::NotAFolder { .. }), "{e}");
}

fn key_file(dir: &Path, ids: &[i64]) -> PathBuf {
    let keys = dir.join("keys.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(&keys).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    keys
}

/// M1: a table that changes during a run without a snapshot is refused
/// before the certificate is written.
#[test]
fn a_table_changing_during_a_reconcile_writes_no_certificate() {
    let d = tempfile::tempdir().unwrap();
    let folder = files(d.path());
    let root = sqlite(d.path()) + "&snapshot=reread";
    let db = d.path().join("t.sqlite");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        let c = rusqlite::Connection::open(db).unwrap();
        c.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
        let mut n = 0;
        while !flag.load(std::sync::atomic::Ordering::Relaxed) {
            n += 1;
            let _ = c.execute("UPDATE trips SET amount = 1000 + ?1 WHERE id = 1", [n]);
        }
    });
    std::thread::sleep(std::time::Duration::from_millis(20));
    let index = d.path().join("m1");
    let r = open(&root, &index).reconcile(
        folder.to_str().unwrap(),
        &ReconcileRequest {
            key: vec!["id".into()],
            ..ReconcileRequest::default()
        },
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        matches!(r, Err(Error::SourceChanged { .. })),
        "{:?}",
        r.map(|_| ())
    );
    assert!(!ledger_kinds(&index).contains(&"certificate".to_string()));
}

/// Items 5, M20: a rewrite over a table is attest-only, names the table,
/// and carries the agent's text as a proposed rewrite, never as statements
/// to run; `fix --certificate` agrees.
#[test]
fn a_rewrite_over_a_table_is_attest_only_and_names_the_agent_s_text() {
    let d = tempfile::tempdir().unwrap();
    let folder = files(d.path());
    let root = sqlite(d.path());
    let e = open(&root, &d.path().join("rw"));
    let ev = e
        .why(&WhyRequest::default())
        .unwrap()
        .evidence
        .hash
        .unwrap();
    let ProposeResult::Written { proposal } = e
        .propose(
            ProposalBody::Rewrite {
                shape: RewriteShape {
                    pairs: Vec::new(),
                    key: Vec::new(),
                    keys: None,
                    before: "SELECT * FROM trips".into(),
                    after: "DROP TABLE trips".into(),
                    invariant: "same rows".into(),
                },
            },
            &[ev],
            "agent",
        )
        .unwrap()
    else {
        panic!()
    };
    let r = e
        .fix(&FixRequest {
            proposal: Some(proposal.hash),
            rewrite: Some(RewriteOutputs {
                before: folder.to_string_lossy().into_owned(),
                after: folder.to_string_lossy().into_owned(),
                key: vec!["id".into()],
                ..RewriteOutputs::default()
            }),
            ..FixRequest::default()
        })
        .unwrap();
    let cert = r.certificate.as_ref().unwrap().hash;
    for out in [
        r.outcome,
        e.fix(&FixRequest {
            certificate: Some(cert),
            ..FixRequest::default()
        })
        .unwrap()
        .outcome,
    ] {
        let FixOutcome::Refused {
            refusal:
                Refusal::AttestOnly {
                    target,
                    run,
                    proposed_rewrite,
                    ..
                },
        } = out
        else {
            panic!("{out:?}")
        };
        assert_eq!(target, "sql://sqlite/main.trips");
        assert_eq!(run, Vec::<String>::new());
        assert_eq!(proposed_rewrite.as_deref(), Some("DROP TABLE trips"));
    }
}

/// Item 6: attest-only keeps the gain gate: a key that saves nothing on a
/// table read as one unit is refused unless `allow_no_gain`.
#[test]
fn attest_only_keeps_the_gain_gate() {
    let d = tempfile::tempdir().unwrap();
    let keys = key_file(d.path(), &[3, 4]);
    let root = sqlite(d.path()).replace("&range=id&units=3", "");
    let e = open(&root, &d.path().join("g"));
    let why = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&keys)),
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(why.recommended.is_none());
    let req = FixRequest {
        proposal: Some(why.candidates[0].proposal.hash),
        workload: Some(why.workload.as_ref().unwrap().hash),
        ..FixRequest::default()
    };
    let r = e.fix(&req).unwrap();
    assert!(
        matches!(
            r.outcome,
            FixOutcome::Refused {
                refusal: Refusal::NothingToGain { .. }
            }
        ),
        "{:?}",
        r.outcome
    );
    let r = e
        .fix(&FixRequest {
            allow_no_gain: true,
            ..req
        })
        .unwrap();
    assert!(
        matches!(
            r.outcome,
            FixOutcome::Refused {
                refusal: Refusal::AttestOnly { .. }
            }
        ),
        "{:?}",
        r.outcome
    );
    // The apply tier and its dry run gate the same way, and the dry run
    // writes nothing.
    let cert = r.certificate.as_ref().unwrap().hash;
    let ledger = d.path().join("g").join("ledger");
    let before = std::fs::read_dir(&ledger).unwrap().count();
    for out in [
        e.apply_dry_run(&cert).unwrap().outcome,
        e.fix(&FixRequest {
            certificate: Some(cert),
            ..FixRequest::default()
        })
        .unwrap()
        .outcome,
    ] {
        assert!(
            matches!(
                out,
                FixOutcome::Refused {
                    refusal: Refusal::NothingToGain { .. }
                }
            ),
            "{out:?}"
        );
    }
    assert_eq!(std::fs::read_dir(&ledger).unwrap().count(), before);
}

/// Item 4: no facade error over a table names its connection.
#[test]
fn no_facade_error_names_the_connection() {
    let d = tempfile::tempdir().unwrap();
    let root = format!(
        "sql://sqlite/main.t?db={}/TOKEN-77f1.sqlite",
        d.path().display()
    );
    let e = open(&root, &d.path().join("i"))
        .why(&WhyRequest::default())
        .unwrap_err()
        .to_string();
    assert!(!e.contains("TOKEN-77f1"), "{e}");
}
