//! The conformance vectors of `docs/format.md`, "Query engine sources":
//! tags, prediction ratios, an engine identity, and the canonical bytes and
//! hashes of the ADR 033 record fields. Also the contract's promise that the
//! fields are additive: a record of each kind as written before them reads
//! and writes back to the same bytes, so its hash is unchanged.
//!
//! Deliberate break (made once in a scratch copy; the named test failed):
//! drop `skip_serializing_if` from `Receipt::attest_only`
//! (`records_written_before_adr_033_keep_their_bytes_and_hashes`).

#![allow(clippy::unwrap_used)]

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use bellwether_source_engine::{identity, ratio, MeasuredEngine, ResultHandle, Tag};
use bellwether_types::{
    FileFingerprint, Hash, PairPrediction, Prediction, QueryStats, Receipt, Record, Result,
    RowGroupMeta, RunCost, Source,
};

#[test]
fn a_tag_is_bw_run_role() {
    let t = Tag::new("0123456789abcdef", "p0-before").unwrap();
    assert_eq!(t.as_str(), "bw-0123456789abcdef-p0-before");
    assert_eq!(
        Tag::new("0123456789abcdef", "p12-after").unwrap().as_str(),
        "bw-0123456789abcdef-p12-after"
    );
    assert_eq!(
        Tag::new("0123456789abcdef", "keys").unwrap().as_str(),
        "bw-0123456789abcdef-keys"
    );
}

#[test]
fn ratios_have_three_places_rounded_half_to_even() {
    for (num, den, want) in [
        (1600, 2, Some("800.000")),
        (1, 3, Some("0.333")),
        (2, 3, Some("0.667")),
        (1, 2000, Some("0.000")),
        (3, 2000, Some("0.002")),
        (5, 2000, Some("0.002")),
        (7, 2000, Some("0.004")),
        (400, 1600, Some("0.250")),
        (9, 0, None),
    ] {
        assert_eq!(ratio(num, den).as_deref(), want, "{num}/{den}");
    }
}

/// An engine that lists two units and does nothing else.
struct Listing;

const ROOT: &str = "clickhouse://h:8123/db/orders";

impl MeasuredEngine for Listing {
    fn root(&self) -> &str {
        ROOT
    }
    fn model(&self) -> &'static str {
        "clickhouse.query_log"
    }
    fn submit(&self, _q: &str, _t: &Tag) -> Result<ResultHandle> {
        unreachable!()
    }
    fn export(&self, _q: &str, _t: &Tag, _d: &std::path::Path) -> Result<ResultHandle> {
        unreachable!()
    }
    fn stats(&self, _r: &ResultHandle) -> Result<QueryStats> {
        unreachable!()
    }
}

impl Source for Listing {
    fn discover(&self, _root: &str) -> Result<Vec<FileFingerprint>> {
        // Listed out of order, and the identity strings sort the other way
        // round from the paths: the identity sorts by path.
        Ok([
            ("1", "v1;rows=1000;blocks=2-4;mutation=0", 1000),
            ("0", "v1;rows=500;blocks=1-1;mutation=0", 500),
        ]
        .into_iter()
        .map(|(p, etag, rows)| FileFingerprint {
            path: format!("{ROOT}/{p}"),
            size: rows,
            etag: Some(etag.into()),
            tail_blake3: Hash::of(etag.as_bytes()),
        })
        .collect())
    }
    fn row_groups(&self, _f: &FileFingerprint) -> Result<Vec<RowGroupMeta>> {
        Ok(Vec::new())
    }
    fn read_columns(&self, _rg: &RowGroupMeta, _c: &[String]) -> Result<RecordBatch> {
        unreachable!()
    }
    fn schema(&self, _root: &str) -> Result<SchemaRef> {
        Ok(std::sync::Arc::new(Schema::empty()))
    }
}

#[test]
fn an_engine_identity_hashes_the_root_and_the_sorted_unit_list() {
    let id = identity(&Listing).unwrap();
    let canonical = r#"["clickhouse://h:8123/db/orders",[["clickhouse://h:8123/db/orders/0","v1;rows=500;blocks=1-1;mutation=0"],["clickhouse://h:8123/db/orders/1","v1;rows=1000;blocks=2-4;mutation=0"]]]"#;
    assert_eq!(id.hash, Hash::of(canonical.as_bytes()));
    assert_eq!(id.hash.to_hex(), IDENTITY);
    assert_eq!((id.units, id.rows), (2, 1500));
    // A unit's tail hash is BLAKE3 of its identity string.
    assert_eq!(
        Hash::of(b"v1;rows=1000;blocks=1-3;mutation=0").to_hex(),
        UNIT_TAIL
    );
}

const IDENTITY: &str = "2dfe8d56adbc1ecb0e1c9f5ebd4bcc91de6dedde8eeafe233aef35fc6c0aa8b1";
const UNIT_TAIL: &str = "97ddb63656c5d7e616b9f7d953d66097a154551af091837730c0ef76563f58c7";

fn h(c: char) -> Hash {
    serde_json::from_value(serde_json::json!(c.to_string().repeat(64))).unwrap()
}

/// Two pairs over one key sample: each pair's reads against the twin's
/// prediction on their own, never summed.
fn prediction() -> Prediction {
    Prediction {
        evidence: h('e'),
        predicted_rows: 2,
        predicted_row_groups: 1,
        pairs: vec![
            PairPrediction {
                before_read_rows: 1600,
                after_read_rows: 400,
                before_read_bytes: 64_000,
                after_read_bytes: 16_000,
                before_ratio: ratio(1600, 2),
                after_ratio: ratio(400, 2),
            },
            PairPrediction {
                before_read_rows: 5,
                after_read_rows: 3,
                before_read_bytes: 200,
                after_read_bytes: 100,
                before_ratio: ratio(5, 2),
                after_ratio: ratio(3, 2),
            },
        ],
    }
}

#[test]
fn the_adr_033_fields_have_pinned_bytes_and_hashes() {
    let receipt = Record::Receipt(Receipt {
        record_version: 2,
        evidence: vec![h('e')],
        proposal: h('f'),
        replay: h('a'),
        certificate: h('c'),
        manifest_before: "0".into(),
        manifest_after: "0".into(),
        created_at: "2026-10-26T00:00:00.000Z".into(),
        attest_only: true,
        prediction: Some(prediction()),
    });
    let e = "e".repeat(64);
    let f = "f".repeat(64);
    let a = "a".repeat(64);
    let c = "c".repeat(64);
    let want = format!(
        r#"{{"record":"receipt","record_version":2,"evidence":["{e}"],"proposal":"{f}","replay":"{a}","certificate":"{c}","manifest_before":"0","manifest_after":"0","created_at":"2026-10-26T00:00:00.000Z","attest_only":true,"prediction":{{"evidence":"{e}","predicted_rows":2,"predicted_row_groups":1,"pairs":[{{"before_read_rows":1600,"after_read_rows":400,"before_read_bytes":64000,"after_read_bytes":16000,"before_ratio":"800.000","after_ratio":"200.000"}},{{"before_read_rows":5,"after_read_rows":3,"before_read_bytes":200,"after_read_bytes":100,"before_ratio":"2.500","after_ratio":"1.500"}}]}}}}"#
    );
    assert_eq!(serde_json::to_string(&receipt).unwrap(), want);
    assert_eq!(receipt.hash().unwrap().to_hex(), RECEIPT);

    let cost = RunCost {
        run_id: "bw-0123456789abcdef-p0-after".into(),
        keys: 2,
        files: 1,
        row_groups: 1,
        bytes: 16_000,
        exact: false,
        measured: Some(QueryStats {
            tag: "bw-0123456789abcdef-p0-after".into(),
            read_rows: 400,
            read_bytes: 16_000,
            peak_memory_bytes: 1_048_576,
            cpu_ns: Some(1_500_000),
            duration_ns: 2_000_000,
            result_rows: 2,
            units_read: Some(1),
            units_total: Some(4),
        }),
    };
    let want = r#"{"run_id":"bw-0123456789abcdef-p0-after","keys":2,"files":1,"row_groups":1,"bytes":16000,"exact":false,"measured":{"tag":"bw-0123456789abcdef-p0-after","read_rows":400,"read_bytes":16000,"peak_memory_bytes":1048576,"cpu_ns":1500000,"duration_ns":2000000,"result_rows":2,"units_read":1,"units_total":4}}"#;
    assert_eq!(serde_json::to_string(&cost).unwrap(), want);
    assert_eq!(Hash::of_record(&cost).unwrap().to_hex(), RUN_COST);
}

const RECEIPT: &str = "a75c47726b81d82a0e968c4aba143fc7feed9b5a32bfbc690e2dcd3379d81ac1";
const RUN_COST: &str = "39c0305370655bd56bc6602de676ecfb4bb4e9868858774e37a31944db9d0537";

/// Records of every kind whose type gained a field, as written before ADR
/// 033 (`serde_json` of the types at `origin/main`), byte for byte.
const BEFORE: [&str; 4] = [
    r#"{"record":"evidence","record_version":2,"provenance":{"inputs":["1111111111111111111111111111111111111111111111111111111111111111"],"oldest_input_age":0,"manifest_version":null,"created_at":"2026-10-26T00:00:00.000Z"},"body":{"kind":"profile","root":"/lake","fanout":[],"lints":[],"workload_keys":null}}"#,
    r#"{"record":"proposal","record_version":2,"evidence":["1111111111111111111111111111111111111111111111111111111111111111"],"proposed_by":"agent","created_at":"2026-10-26T00:00:00.000Z","body":{"kind":"rewrite","shape":{"before":"b","after":"a","invariant":"i"}}}"#,
    r#"{"record":"replay","record_version":2,"proposal":"2222222222222222222222222222222222222222222222222222222222222222","manifest_version":"3","workload":"3333333333333333333333333333333333333333333333333333333333333333","current":[{"run_id":"r","keys":1,"files":1,"row_groups":2,"bytes":100,"exact":true}],"proposed":[{"run_id":"r","keys":1,"files":1,"row_groups":1,"bytes":50,"exact":true}],"created_at":"2026-10-26T00:00:00.000Z"}"#,
    r#"{"record":"receipt","record_version":2,"evidence":["1111111111111111111111111111111111111111111111111111111111111111"],"proposal":"2222222222222222222222222222222222222222222222222222222222222222","replay":"4444444444444444444444444444444444444444444444444444444444444444","certificate":"5555555555555555555555555555555555555555555555555555555555555555","manifest_before":"3","manifest_after":"4","created_at":"2026-10-26T00:00:00.000Z"}"#,
];

#[test]
fn records_written_before_adr_033_keep_their_bytes_and_hashes() {
    for old in BEFORE {
        let r: Record = serde_json::from_str(old).unwrap();
        assert_eq!(serde_json::to_string(&r).unwrap(), old);
        assert_eq!(r.hash().unwrap(), Hash::of(old.as_bytes()));
    }
}
