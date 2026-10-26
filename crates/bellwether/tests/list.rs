//! List keys (B4) through the facade: `locate --any/--all`, `why --mode
//! contains-any|contains-all` with `list:<column>` as the floor candidate
//! and its `list` column (distinct elements), the `R-list-1` detector, and
//! the loop why -> fix -> receipt -> locate, where the plan `locate` reads
//! after the fix is the one `why` predicted. Every truth comes from an
//! oracle that decodes the files itself.

#![allow(clippy::unwrap_used, clippy::too_many_lines, missing_docs)]

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether::types::{Error, EvidenceBody, KeySpec, Record};
use bellwether::{
    CandidateOrigin, Engine, EngineOptions, FixOutcome, FixRequest, KeyFile, LookupKind,
    LookupMode, PlanResult, WhyRequest,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const CLOCK: &str = "2026-10-26T12:00:00.000Z";

/// Four files of three row groups of 20 rows. Tags: `hot` in every row
/// group, `f<f>-<k>` clustered by file, `rare` in one row group, repeats
/// within a list, null and empty lists.
fn write_corpus(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let tags_type = ListBuilder::new(StringBuilder::new())
        .finish()
        .data_type()
        .clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", tags_type, true),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1 << 20))
        .build();
    let mut id = 0i64;
    for f in 0..4 {
        let file = File::create(dir.join(format!("part-{f}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props.clone())).unwrap();
        for rg in 0..3 {
            let mut ids = Vec::new();
            let mut tags = ListBuilder::new(StringBuilder::new());
            for r in 0..20i64 {
                ids.push(id);
                id += 1;
                match r % 10 {
                    0 => tags.append(false),
                    1 => tags.append(true),
                    _ => {
                        tags.values().append_value("hot");
                        let k = (r + rg) % 4;
                        tags.values().append_value(format!("f{f}-{k}"));
                        tags.values().append_value(format!("f{f}-{k}"));
                        if f == 2 && rg == 1 && r == 5 {
                            tags.values().append_value("rare");
                        }
                        tags.append(true);
                    }
                }
            }
            let columns: Vec<ArrayRef> =
                vec![Arc::new(Int64Array::from(ids)), Arc::new(tags.finish())];
            w.write(&RecordBatch::try_new(Arc::clone(&schema), columns).unwrap())
                .unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
}

/// (file name, row group, the row's tags; `None` for a null list), read
/// with the parquet crate.
fn decode(dir: &Path) -> Vec<(String, usize, Option<Vec<String>>)> {
    let mut out = Vec::new();
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    files.sort();
    for p in files {
        let name = p.file_name().unwrap().to_str().unwrap().to_string();
        let n = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        for rg in 0..n {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&p).unwrap())
                .unwrap()
                .with_row_groups(vec![rg])
                .build()
                .unwrap();
            for b in reader {
                let b = b.unwrap();
                let lists = b.column_by_name("tags").unwrap().as_list::<i32>();
                for i in 0..b.num_rows() {
                    let row = (!lists.is_null(i)).then(|| {
                        let v = lists.value(i);
                        let s = v.as_string::<i32>();
                        (0..s.len()).map(|j| s.value(j).to_string()).collect()
                    });
                    out.push((name.clone(), rg, row));
                }
            }
        }
    }
    out
}

fn truth(dir: &Path, values: &[&str], all: bool) -> BTreeSet<(String, usize)> {
    decode(dir)
        .into_iter()
        .filter(|(_, _, row)| {
            row.as_ref().is_some_and(|tags| {
                let has = |v: &&str| tags.iter().any(|t| t == v);
                if all {
                    values.iter().all(has)
                } else {
                    values.iter().any(has)
                }
            })
        })
        .map(|(f, rg, _)| (f, rg))
        .collect()
}

fn planned(r: &PlanResult) -> BTreeSet<(String, usize)> {
    let Record::Evidence(e) = &r.evidence.record else {
        panic!("{:?}", r.evidence)
    };
    let EvidenceBody::Plan { plan } = &e.body else {
        panic!("{e:?}")
    };
    plan.entries
        .iter()
        .flat_map(|en| {
            let name = Path::new(&en.file.path)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            en.row_groups
                .iter()
                .map(move |rg| (name.clone(), rg as usize))
        })
        .collect()
}

fn write_values(path: &Path, values: &[Option<&str>]) {
    let schema = Arc::new(Schema::new(vec![Field::new("tags", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(values.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn engine(data: &Path) -> Engine {
    Engine::open(
        data.to_str().unwrap(),
        EngineOptions {
            clock: Some(CLOCK.into()),
            ..EngineOptions::default()
        },
    )
    .unwrap()
}

fn list_tags() -> KeySpec {
    KeySpec::List {
        column: "tags".into(),
    }
}

fn distinct_tags(dir: &Path) -> usize {
    decode(dir)
        .into_iter()
        .flat_map(|(_, _, row)| row.unwrap_or_default())
        .collect::<BTreeSet<String>>()
        .len()
}

#[test]
fn why_fix_receipt_locate_on_a_list_column() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    write_corpus(&data);
    let values = dir.path().join("values.parquet");
    write_values(&values, &[Some("rare"), Some("f1-2")]);
    let e = engine(&data);
    // An index without the list key: a membership lookup is refused with
    // the key to declare; a plain locate by the list column says --any.
    e.index(&[KeySpec::Exact {
        columns: vec!["id".into()],
        counts: false,
        bounds: None,
    }])
    .unwrap();
    match e
        .locate_contains(&KeyFile::new(&values), false)
        .unwrap_err()
    {
        Error::NotIndexed { needs, .. } => assert_eq!(needs.as_deref(), Some("list:tags")),
        other => panic!("{other:?}"),
    }
    let plain = e.locate(&KeyFile::new(&values)).unwrap_err().to_string();
    assert!(
        plain.contains("--any") && plain.contains("--all"),
        "{plain}"
    );

    // why --mode contains-any: the floor is list:tags, its column is the
    // distinct elements, and R-list-1 fires.
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&values)),
            mode: Some(LookupMode::ContainsAny),
            ..WhyRequest::default()
        })
        .unwrap();
    let info = w.workload.as_ref().unwrap();
    assert_eq!(info.query_kind, "contains_any");
    assert_eq!(
        info.lookup,
        Some(LookupKind::ContainsAny {
            column: "tags".into()
        })
    );
    let floor = &w.candidates[0];
    assert_eq!(floor.spec, list_tags());
    assert_eq!(floor.origin, CandidateOrigin::Workload);
    assert!(
        w.candidates
            .iter()
            .all(|c| matches!(c.spec, KeySpec::List { .. })),
        "{:?}",
        w.candidates
            .iter()
            .map(|c| c.spec.name())
            .collect::<Vec<_>>()
    );
    let column = floor.kind_column.as_ref().unwrap();
    assert_eq!(column.kind, "list");
    assert_eq!(column.value, distinct_tags(&data).to_string());
    assert!(column.exactness.is_exact());
    let lint = w
        .profile
        .lints
        .iter()
        .find(|l| l.rule == "R-list-1")
        .expect("R-list-1 fires without a list key");
    assert_eq!(lint.columns, ["tags"]);
    assert!(lint.message.contains("list:tags"), "{}", lint.message);
    let rec = w.recommended.as_ref().unwrap();
    assert_eq!(rec.spec, list_tags());
    let current = w.current.unwrap();
    assert_eq!(current.row_groups, 12, "no list key: every row group");
    let predicted = floor.proposed;
    let want = truth(&data, &["rare", "f1-2"], false);
    assert_eq!(predicted.row_groups, want.len() as u64);

    // fix: score, verify (rows whose tags hold a value, keyed by every
    // hashable column), apply, attest.
    let fixed = e
        .fix(&FixRequest {
            proposal: Some(floor.proposal.hash),
            workload: Some(info.hash),
            ..FixRequest::default()
        })
        .unwrap();
    let FixOutcome::Applied {
        receipt, realized, ..
    } = &fixed.outcome
    else {
        panic!("{:?}", fixed.outcome)
    };
    assert!(matches!(receipt.record, Record::Receipt(_)));
    let Some(Record::Replay(realized)) = realized.as_ref().map(|r| &r.record) else {
        panic!("{realized:?}")
    };
    let realized_rgs: u64 = realized.proposed.iter().map(|c| c.row_groups).sum();
    assert_eq!(realized_rgs, predicted.row_groups);
    let Some(Record::Certificate(cert)) = fixed.certificate.as_ref().map(|c| &c.record) else {
        panic!("{:?}", fixed.certificate)
    };
    assert!(cert.pass, "{cert:?}");

    // locate --any after the fix: the plan why predicted, a superset of
    // the truth.
    let any = e.locate_contains(&KeyFile::new(&values), false).unwrap();
    assert_eq!(
        any.lookup,
        LookupKind::ContainsAny {
            column: "tags".into()
        }
    );
    assert_eq!(any.specs, vec![list_tags()]);
    assert_eq!(
        (any.planned.row_groups, any.planned.bytes),
        (predicted.row_groups, predicted.bytes)
    );
    assert_eq!(planned(&any), want);

    // why again: the list key is current, nothing to recommend, no lint.
    let again = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&values)),
            mode: Some(LookupMode::ContainsAny),
            ..WhyRequest::default()
        })
        .unwrap();
    assert!(again.candidates.is_empty(), "{:?}", again.candidates);
    assert!(again.profile.lints.iter().all(|l| l.rule != "R-list-1"));
    assert_eq!(again.current.unwrap(), predicted);

    // contains_all: priced by why, planned by locate --all, the same.
    let both = dir.path().join("both.parquet");
    write_values(&both, &[Some("hot"), Some("f3-1")]);
    let w_all = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&both)),
            mode: Some(LookupMode::ContainsAll),
            ..WhyRequest::default()
        })
        .unwrap();
    let all = e.locate_contains(&KeyFile::new(&both), true).unwrap();
    assert_eq!(w_all.current.unwrap(), all.planned);
    let want_all = truth(&data, &["hot", "f3-1"], true);
    assert!(want_all.is_subset(&planned(&all)));
    assert!(all.planned.row_groups < 12, "{:?}", all.planned);
    // A null value under --all: no row could match; refused, saying so.
    let with_null = dir.path().join("null.parquet");
    write_values(&with_null, &[Some("hot"), None]);
    let e_null = e
        .locate_contains(&KeyFile::new(&with_null), true)
        .unwrap_err();
    assert!(e_null.to_string().contains("--any"), "{e_null}");
    // ...which --any drops and counts.
    let any_null = e.locate_contains(&KeyFile::new(&with_null), false).unwrap();
    assert_eq!((any_null.keys, any_null.null_tuples), (1, 1));
}

/// `fix` on a `contains_all` workload certifies the rows holding every
/// value (a row holding only one is never asked for), and a value no row
/// holds is refused as nothing to compare, never a receipt over zero rows.
#[test]
fn fix_verifies_contains_all_and_refuses_values_no_row_holds() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    write_corpus(&data);
    let e = engine(&data);
    let both = dir.path().join("both.parquet");
    write_values(&both, &[Some("hot"), Some("f3-1")]);
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&both)),
            mode: Some(LookupMode::ContainsAll),
            ..WhyRequest::default()
        })
        .unwrap();
    let floor = &w.candidates[0];
    assert_eq!(floor.spec, list_tags());
    let fixed = e
        .fix(&FixRequest {
            proposal: Some(floor.proposal.hash),
            workload: Some(w.workload.as_ref().unwrap().hash),
            ..FixRequest::default()
        })
        .unwrap();
    assert!(
        matches!(fixed.outcome, FixOutcome::Applied { .. }),
        "{:?}",
        fixed.outcome
    );
    let Some(Record::Certificate(cert)) = fixed.certificate.as_ref().map(|c| &c.record) else {
        panic!("{:?}", fixed.certificate)
    };
    let rows = decode(&data)
        .into_iter()
        .filter(|(_, _, r)| {
            r.as_ref()
                .is_some_and(|t| t.iter().any(|x| x == "hot") && t.iter().any(|x| x == "f3-1"))
        })
        .count() as u64;
    assert!(cert.pass && rows > 0);
    match &cert.coverage {
        bellwether::types::Coverage::Workload { keys, .. } => assert_eq!(*keys, rows, "{cert:?}"),
        other => panic!("{other:?}"),
    }

    let dir2 = tempfile::tempdir().unwrap();
    let data2 = dir2.path().join("data");
    write_corpus(&data2);
    let e2 = engine(&data2);
    let absent = dir2.path().join("absent.parquet");
    write_values(&absent, &[Some("absent")]);
    let w = e2
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&absent)),
            mode: Some(LookupMode::ContainsAny),
            ..WhyRequest::default()
        })
        .unwrap();
    let err = e2
        .fix(&FixRequest {
            proposal: Some(w.candidates[0].proposal.hash),
            workload: Some(w.workload.as_ref().unwrap().hash),
            ..FixRequest::default()
        })
        .unwrap_err()
        .to_string();
    assert!(err.contains("nothing to verify"), "{err}");
}

/// A value in every row group: the culprit is the layout, stated from the
/// queried values' own row groups, both before the list key exists and
/// after it is built (the key serves the runs, so the floor is priced).
#[test]
fn the_layout_culprit_names_where_the_values_sit_before_and_after_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    write_corpus(&data);
    let e = engine(&data);
    let hot = dir.path().join("hot.parquet");
    write_values(&hot, &[Some("hot")]);
    let req = WhyRequest {
        keys: Some(KeyFile::new(&hot)),
        mode: Some(LookupMode::ContainsAny),
        ..WhyRequest::default()
    };
    let culprit = |w: &bellwether::WhyReport| {
        let c = w.culprit.clone().unwrap();
        let bellwether::CulpritReason::ListLayout {
            least_row_groups,
            row_groups,
            values,
        } = c.reason
        else {
            panic!("{c:?}")
        };
        assert_eq!(c.columns, ["tags"]);
        (least_row_groups, row_groups, values)
    };
    let before = culprit(&e.why(&req).unwrap());
    assert_eq!((before.0, before.1), (12, 12));
    assert_eq!(before.2.distinct, 1);
    assert_eq!(before.2.row_groups_per_value, [12.0; 3]);
    assert_eq!(before.2.files_per_value, [4.0; 3]);
    e.index(&[list_tags()]).unwrap();
    let after = culprit(&e.why(&req).unwrap());
    assert_eq!(after, before);
}

#[test]
fn why_membership_modes_refuse_what_they_cannot_price() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    write_corpus(&data);
    let values = dir.path().join("values.parquet");
    write_values(&values, &[Some("rare")]);
    let e = engine(&data);
    // a membership mode with --latest-by
    let r = WhyRequest {
        keys: Some(KeyFile::new(&values)),
        mode: Some(LookupMode::ContainsAll),
        latest_by: Some("id".into()),
        ..WhyRequest::default()
    };
    let msg = r.lookup_mode().unwrap_err();
    assert!(msg.contains("--mode contains-all"), "{msg}");
    // without values
    let err = e
        .why(&WhyRequest {
            mode: Some(LookupMode::ContainsAny),
            ..WhyRequest::default()
        })
        .unwrap_err()
        .to_string();
    assert!(err.contains("--from"), "{err}");
    // a recorded membership workload priced as locate is refused; as
    // recorded it is priced
    let w = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&values)),
            mode: Some(LookupMode::ContainsAny),
            ..WhyRequest::default()
        })
        .unwrap();
    let h = w.workload.as_ref().unwrap().hash;
    let err = e
        .why(&WhyRequest {
            workload: Some(h),
            mode: Some(LookupMode::Locate),
            ..WhyRequest::default()
        })
        .unwrap_err()
        .to_string();
    assert!(err.contains("drop --mode"), "{err}");
    let err = e
        .why(&WhyRequest {
            workload: Some(h),
            mode: Some(LookupMode::ContainsAll),
            ..WhyRequest::default()
        })
        .unwrap_err()
        .to_string();
    assert!(err.contains("contains_any"), "{err}");
    let again = e
        .why(&WhyRequest {
            workload: Some(h),
            ..WhyRequest::default()
        })
        .unwrap();
    assert_eq!(again.candidates[0].spec, list_tags());
    assert_eq!(again.candidates[0].proposed, w.candidates[0].proposed);
    assert!(again.profile.lints.iter().any(|l| l.rule == "R-list-1"));
    // a flat column priced as membership
    let ids = dir.path().join("ids.parquet");
    {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let mut wr = ArrowWriter::try_new(File::create(&ids).unwrap(), schema, None).unwrap();
        wr.write(&batch).unwrap();
        wr.close().unwrap();
    }
    let err = e
        .why(&WhyRequest {
            keys: Some(KeyFile::new(&ids)),
            mode: Some(LookupMode::ContainsAny),
            ..WhyRequest::default()
        })
        .unwrap_err()
        .to_string();
    assert!(err.contains("not a list column"), "{err}");
}
