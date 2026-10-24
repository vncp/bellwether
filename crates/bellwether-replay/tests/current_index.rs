//! Replay of the current index, proposals, the ledger write, layout models,
//! reports, and the attribution hooks.

#![allow(clippy::unwrap_used, missing_docs, clippy::too_many_lines)]

mod common;

use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_ledger::{Ledger, RecordKind};
use bellwether_replay::{
    report, write_replay, Alternative, CostDelta, KeyInput, MemoryKeySets, PlanSource, ReplayError,
    Replayer, Scored, WorkloadRecorder,
};
use bellwether_source::ParquetSource;
use bellwether_store::{ObjectStoreBackend, Snapshot};
use bellwether_types::format::RECORD_VERSION;
use bellwether_types::{Error, Hash, KeySpec, Proposal, ProposalBody, Record, RunCost};
use common::{BuildOptions, CorpusSpec};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const AT: &str = "2026-10-24T00:00:00Z";

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

fn specs() -> Vec<KeySpec> {
    vec![
        KeySpec::Exact {
            columns: cols(&["entity_id"]),
        },
        KeySpec::Bloom {
            columns: cols(&["batch_id"]),
            fpr: 0.05,
        },
        KeySpec::Range {
            column: "version_ts".into(),
        },
        KeySpec::Temporal {
            columns: cols(&["status", "entity_id"]),
            ts_column: "version_ts".into(),
        },
    ]
}

fn indexed(opts: BuildOptions) -> (common::Corpus, Arc<ObjectStoreBackend>) {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let store = Arc::new(ObjectStoreBackend::local(corpus.dir.path().join("store")).unwrap());
    common::build_index(&store, "idx", &corpus, &specs(), opts);
    (corpus, store)
}

fn proposal(ledger: &Ledger<Arc<ObjectStoreBackend>>, body: ProposalBody) -> Hash {
    ledger
        .put(&Record::Proposal(Proposal {
            record_version: RECORD_VERSION,
            evidence: vec![],
            proposed_by: "test".into(),
            created_at: AT.into(),
            body,
        }))
        .unwrap()
}

#[test]
fn committed_segments_cost_exactly_what_transient_ones_do() {
    let (corpus, store) = indexed(BuildOptions::default());
    let source = corpus.source();
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let committed = Replayer::for_snapshot(&source, &snapshot).unwrap();
    assert_eq!(committed.manifest_version(), "1");
    assert_eq!(committed.current_specs(), specs().as_slice());
    let transient = Replayer::unindexed(&source, &corpus.data).unwrap();
    let runs = common::entity_runs(&corpus, 16, 4);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    // Each spec alone, and the whole configuration: entity runs are served
    // by the exact spec (and by the temporal one only through a prefix it
    // cannot take, so not at all).
    for spec in specs() {
        let a = committed
            .replay_specs(std::slice::from_ref(&spec), &workload, &keys)
            .unwrap();
        let b = transient
            .replay_specs(std::slice::from_ref(&spec), &workload, &keys)
            .unwrap();
        assert_eq!(a, b, "{spec:?}");
    }
    let current = committed.replay_current(&workload, &keys).unwrap();
    let exact_only = transient
        .replay_specs(&specs()[..1], &workload, &keys)
        .unwrap();
    assert_eq!(current, exact_only);
    // A run no spec serves reads every row group.
    let raw = transient.replay_specs(&[], &workload, &keys).unwrap();
    let range_only = committed
        .replay_specs(&specs()[2..3], &workload, &keys)
        .unwrap();
    assert_eq!(
        range_only, raw,
        "range on version_ts cannot serve entity runs"
    );
}

/// An incomplete spec (no completeness recorded) or a coverage gap
/// (completeness recorded, a segment missing) refuses only the plans that
/// list it, in the build crate's planner and in this crate's alike: both
/// construct, neither lists it among the specs it plans with, both refuse
/// the same plan with `Error::KeySpec`, and the complete specs still plan,
/// identically. The replayer's current configuration leaves it out, and a
/// configuration naming it costs it as transient segments.
#[test]
fn an_incomplete_spec_or_a_coverage_gap_refuses_only_the_plans_that_list_it() {
    for (opts, why) in [
        (
            BuildOptions {
                complete: false,
                ..BuildOptions::default()
            },
            "not complete",
        ),
        (
            BuildOptions {
                leave_gap: true,
                ..BuildOptions::default()
            },
            "",
        ),
    ] {
        let (corpus, store) = indexed(opts);
        let source = corpus.source();
        let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
        let all = specs();
        let (complete, bad) = (&all[..3], &all[3]);
        let replayer = Replayer::for_snapshot(&source, &snapshot).unwrap();
        assert_eq!(replayer.current_specs(), complete);
        let wp4 = bellwether_build::Planner::from_snapshot(snapshot.clone()).unwrap();
        let ours = bellwether_replay::SegmentPlanner::from_snapshot(
            &snapshot,
            Arc::clone(replayer.catalog()),
        )
        .unwrap();
        assert_eq!(PlanSource::specs(&wp4), complete.to_vec());
        assert_eq!(PlanSource::specs(&ours), complete.to_vec());
        let runs = common::entity_runs(&corpus, 6, 9);
        let (keys, workload) = common::record(&["entity_id"], &runs);
        let rk = workload.run_keys(&keys, &workload.runs()[0]).unwrap();
        let listing = vec![all[0].clone(), bad.clone()];
        for (planner, name) in [(&wp4 as &dyn PlanSource, "build"), (&ours, "replay")] {
            let err = planner.plan(&listing, &rk).unwrap_err();
            assert!(
                matches!(&err, Error::KeySpec(m) if m.contains(why)),
                "{name}: {err}"
            );
        }
        assert_eq!(
            PlanSource::plan(&wp4, &all[..1], &rk).unwrap(),
            ours.plan(&all[..1], &rk).unwrap()
        );
        // Named in a configuration, the incomplete spec is built
        // transiently and costed like any hypothetical spec.
        let hypothetical = replayer
            .replay_specs(std::slice::from_ref(bad), &workload, &keys)
            .unwrap();
        assert_eq!(hypothetical.len(), workload.runs().len());
        assert_eq!(replayer.cache_stats().unwrap().builds, 1);
    }
}

#[test]
fn a_rewritten_file_is_refused_not_replayed() {
    let (corpus, store) = indexed(BuildOptions::default());
    let victim = corpus.files()[2].clone();
    let mut bytes = std::fs::read(&victim).unwrap();
    let n = bytes.len();
    bytes[n - 10] ^= 0xFF;
    std::fs::write(&victim, bytes).unwrap();
    let source = corpus.source();
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let err = Replayer::for_snapshot(&source, &snapshot).unwrap_err();
    assert!(
        matches!(&err, Error::Source(m) if m.contains("drift")),
        "{err}"
    );
}

/// Replaying the current index makes no `read_columns` call: plans come
/// from segments, sizes from footers (the build crate's planner reads them
/// through its own source to verify fingerprints and sum bytes, which this
/// counting source does not see). A candidate reads only its key columns,
/// one row group at a time in id order, once.
#[test]
fn replaying_the_current_index_reads_no_columns_and_a_candidate_reads_only_its_keys() {
    let (corpus, store) = indexed(BuildOptions::default());
    let source = common::Counting::new(corpus.source());
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snapshot).unwrap();
    let runs = common::entity_runs(&corpus, 8, 6);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    source.clear();
    replayer.replay_current(&workload, &keys).unwrap();
    assert!(source.reads().is_empty(), "current replay read columns");

    let candidate = KeySpec::Temporal {
        columns: cols(&["entity_id"]),
        ts_column: "version_ts".into(),
    };
    replayer
        .replay_specs(std::slice::from_ref(&candidate), &workload, &keys)
        .unwrap();
    let reads = source.reads();
    assert_eq!(
        reads.len(),
        replayer.catalog().len(),
        "one read per row group"
    );
    for (i, (path, rg, columns)) in reads.iter().enumerate() {
        let want = &replayer.catalog().row_groups()[i].meta;
        assert_eq!(
            (path, *rg),
            (&want.file.path, want.index_in_file),
            "id order"
        );
        assert_eq!(
            columns,
            &cols(&["entity_id", "version_ts"]),
            "key columns only"
        );
    }
    source.clear();
    replayer
        .replay_specs(std::slice::from_ref(&candidate), &workload, &keys)
        .unwrap();
    assert!(
        source.reads().is_empty(),
        "a repeated what-if is a cache hit"
    );
    let stats = replayer.cache_stats().unwrap();
    assert_eq!((stats.builds, stats.hits), (1, 1));
}

#[test]
fn a_key_proposal_is_scored_and_written_through_the_ledger() {
    let (corpus, store) = indexed(BuildOptions::default());
    let source = corpus.source();
    let snapshot = Snapshot::open(Arc::clone(&store), "idx").unwrap();
    let replayer = Replayer::for_snapshot(&source, &snapshot).unwrap();
    let runs = common::entity_runs(&corpus, 10, 8);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let ledger = Ledger::new(Arc::clone(&store));

    // A bloom key on the run column is added next to the exact one (their
    // names differ), so the per-key intersection is the exact answer.
    let bloom = KeySpec::Bloom {
        columns: cols(&["entity_id"]),
        fpr: 0.3,
    };
    let p = proposal(
        &ledger,
        ProposalBody::Key {
            spec: bloom.clone(),
        },
    );
    let (hash, scored) = replayer.record(&ledger, &p, &workload, &keys, AT).unwrap();
    let Record::Replay(written) = ledger.get(&hash).unwrap() else {
        panic!("not a replay");
    };
    assert_eq!(written, scored.replay);
    assert_eq!(written.proposal, p);
    assert_eq!(written.manifest_version, "1");
    assert_eq!(written.workload, workload.hash());
    assert_eq!(written.created_at, AT);
    assert_eq!(written.record_version, 2);
    assert_eq!(written.inputs, vec![replayer.catalog().digest()]);
    assert_eq!(written.model, None, "a key proposal names no model");
    assert_eq!(
        written.current,
        replayer.replay_current(&workload, &keys).unwrap()
    );
    let mut with_bloom = specs();
    with_bloom.push(bloom.clone());
    assert_eq!(
        written.proposed,
        replayer
            .replay_specs(&with_bloom, &workload, &keys)
            .unwrap()
    );
    assert_eq!(written.proposed, written.current);
    // Alone, the bloom key reads at least what the exact key does.
    let alone = replayer
        .replay_specs(std::slice::from_ref(&bloom), &workload, &keys)
        .unwrap();
    for (c, b) in written.current.iter().zip(&alone) {
        assert!(b.row_groups >= c.row_groups, "{}", c.run_id);
    }
    // A proposal replaces a current spec of the same name: a bloom key on
    // batch_id at another false-positive rate.
    let tighter = KeySpec::Bloom {
        columns: cols(&["batch_id"]),
        fpr: 0.001,
    };
    let replaced = bellwether_replay::proposed_specs(&specs(), &tighter);
    assert_eq!(replaced.len(), specs().len());
    assert!(replaced.contains(&tighter) && !replaced.contains(&specs()[1]));

    // A proposal that does not resolve, or a hash that is not a proposal.
    let missing = Hash::of(b"no such proposal");
    assert!(matches!(
        replayer.record(&ledger, &missing, &workload, &keys, AT),
        Err(ReplayError::Engine(Error::NotFound(_)))
    ));
    assert!(matches!(
        replayer.record(&ledger, &hash, &workload, &keys, AT),
        Err(ReplayError::NotAProposal(h)) if h == hash
    ));
    // created_at must be RFC 3339.
    assert!(replayer
        .score(
            &p,
            &ProposalBody::Key {
                spec: specs()[0].clone()
            },
            &workload,
            &keys,
            "yesterday"
        )
        .is_err());
}

#[test]
fn irrelevant_specs_and_rewrites_are_refused() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let source = corpus.source();
    let replayer = Replayer::unindexed(&source, &corpus.data).unwrap();
    let runs = common::entity_runs(&corpus, 4, 1);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let p = Hash::of(b"p");
    let err = replayer
        .score(
            &p,
            &ProposalBody::Key {
                spec: KeySpec::Exact {
                    columns: cols(&["status", "entity_id"]),
                },
            },
            &workload,
            &keys,
            AT,
        )
        .unwrap_err();
    assert!(
        matches!(&err, ReplayError::SpecServesNoRun { spec, .. } if spec == "exact:status,entity_id"),
        "{err}"
    );
    assert!(matches!(Error::from(err), Error::KeySpec(_)));
    let err = replayer
        .score(
            &p,
            &ProposalBody::Rewrite {
                shape: bellwether_types::RewriteShape {
                    before: "a".into(),
                    after: "b".into(),
                    invariant: "c".into(),
                },
            },
            &workload,
            &keys,
            AT,
        )
        .unwrap_err();
    assert!(matches!(err, ReplayError::RewriteNotScorable), "{err}");
}

/// Every refusal through `record` reports its own variant and writes no
/// Replay. `write_replay` resolves the proposal itself and refuses a
/// doctored `Scored`: a layout's costs relabelled exact with the model
/// dropped, a renamed model, a model on a key replay, one proposal's costs
/// under another proposal's hash, an inexact key cost, an inexact current
/// cost, or no inputs. The honest key and layout replays are written.
#[test]
fn refusals_are_typed_and_write_nothing() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let source = corpus.source();
    let replayer = Replayer::unindexed(&source, &corpus.data).unwrap();
    let runs = common::entity_runs(&corpus, 6, 2);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let replays = || ledger.list(RecordKind::Replay).unwrap().count();
    let mismatch = |s: &Scored| match write_replay(&ledger, s) {
        Err(ReplayError::ProposalMismatch { proposal, detail }) => {
            assert_eq!(proposal, s.replay.proposal);
            detail
        }
        other => panic!("expected a ProposalMismatch, got {other:?}"),
    };

    let rewrite = proposal(
        &ledger,
        ProposalBody::Rewrite {
            shape: bellwether_types::RewriteShape {
                before: "a".into(),
                after: "b".into(),
                invariant: "c".into(),
            },
        },
    );
    let e = replayer
        .record(&ledger, &rewrite, &workload, &keys, AT)
        .unwrap_err();
    assert!(matches!(e, ReplayError::RewriteNotScorable), "{e}");
    assert!(matches!(Error::from(e), Error::LedgerRule(_)));

    let irrelevant = proposal(
        &ledger,
        ProposalBody::Key {
            spec: KeySpec::Exact {
                columns: cols(&["payload"]),
            },
        },
    );
    let e = replayer
        .record(&ledger, &irrelevant, &workload, &keys, AT)
        .unwrap_err();
    assert!(matches!(e, ReplayError::SpecServesNoRun { .. }), "{e}");
    assert_eq!(replays(), 0, "no refusal writes a Replay");

    // Layout: a layout's costs relabelled exact with the model dropped,
    // a renamed model, or a model clustering by other columns.
    let layout_body = ProposalBody::Layout {
        cluster_by: cols(&["entity_id"]),
    };
    let layout = proposal(&ledger, layout_body.clone());
    let honest_layout = replayer
        .score(&layout, &layout_body, &workload, &keys, AT)
        .unwrap();
    let mut doctored = honest_layout.clone();
    doctored.layout = None;
    doctored.replay.model = None;
    for c in &mut doctored.replay.proposed {
        c.exact = true;
    }
    assert!(mismatch(&doctored).contains("layout model"));
    let mut doctored = honest_layout.clone();
    doctored.replay.model = Some("a better model".into());
    assert!(mismatch(&doctored).contains("must name the layout model"));
    let mut doctored = honest_layout.clone();
    doctored.layout.as_mut().unwrap().model.cluster_by = cols(&["status"]);
    assert!(mismatch(&doctored).contains("other columns"));

    // Key: proposal A's hash with body B's costs; an inexact proposed cost;
    // a model named; an inexact current cost; no inputs.
    let range_spec = KeySpec::Range {
        column: "entity_id".into(),
    };
    let range_body = ProposalBody::Key {
        spec: range_spec.clone(),
    };
    let a = proposal(&ledger, range_body.clone());
    let body_b = ProposalBody::Key {
        spec: KeySpec::Exact {
            columns: cols(&["entity_id"]),
        },
    };
    let scored = replayer.score(&a, &body_b, &workload, &keys, AT).unwrap();
    assert!(mismatch(&scored).contains("another proposal body"));
    let honest = replayer
        .score(&a, &range_body, &workload, &keys, AT)
        .unwrap();
    let mut doctored = honest.clone();
    doctored.replay.proposed[0].exact = false;
    assert!(mismatch(&doctored).contains("exact and come from no model"));
    let mut doctored = honest.clone();
    doctored.replay.model = Some("layout model v1".into());
    assert!(mismatch(&doctored).contains("exact and come from no model"));
    let mut doctored = honest.clone();
    doctored.replay.current[0].exact = false;
    assert!(mismatch(&doctored).contains("current costs"));
    let mut doctored = honest.clone();
    doctored.replay.inputs.clear();
    assert!(mismatch(&doctored).contains("names no inputs"));
    assert_eq!(replays(), 0, "no refusal writes a Replay");

    // The honest ones are written, the layout replay naming its model.
    write_replay(&ledger, &honest).unwrap();
    let h = write_replay(&ledger, &honest_layout).unwrap();
    assert_eq!(replays(), 2);
    let Record::Replay(r) = ledger.get(&h).unwrap() else {
        panic!("not a replay");
    };
    assert_eq!(
        r.model.as_deref(),
        Some(honest_layout.layout.as_ref().unwrap().description.as_str())
    );
    assert!(r.proposed.iter().all(|c| !c.exact));
    assert!(r.current.iter().all(|c| c.exact));
}

/// One file, two row groups of four rows, entity ids chosen so the layout
/// model's answer can be worked by hand.
fn tiny_corpus() -> (tempfile::TempDir, String, Arc<Schema>) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::UInt64, true),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![5, 1, 5, 2, 1, 9, 5, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a"; 8])),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut w = ArrowWriter::try_new(
        File::create(data.join("part-0.parquet")).unwrap(),
        Arc::clone(&schema),
        Some(props),
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let data = data.to_str().unwrap().to_string();
    (dir, data, schema)
}

#[test]
fn a_layout_proposal_is_costed_and_written_under_its_named_model() {
    let (_dir, data, schema) = tiny_corpus();
    let source = ParquetSource::new().with_schema(Arc::clone(&schema));
    let replayer = Replayer::unindexed(&source, &data).unwrap();
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, Arc::clone(&schema));
    for (id, ks) in [
        ("a", vec![5u64]),
        ("b", vec![1, 9]),
        ("c", vec![2]),
        ("d", vec![7]),
    ] {
        let batch = RecordBatch::try_from_iter(vec![(
            "entity_id",
            Arc::new(UInt64Array::from(ks)) as ArrayRef,
        )])
        .unwrap();
        rec.record(id, KeyInput::Batch(&batch)).unwrap();
    }
    let workload = rec.finish().unwrap();
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let ledger = Ledger::new(Arc::clone(&store));
    let body = ProposalBody::Layout {
        cluster_by: cols(&["entity_id"]),
    };
    let p = proposal(&ledger, body.clone());
    let scored = replayer.score(&p, &body, &workload, &keys, AT).unwrap();
    let layout = scored.layout.clone().unwrap();
    assert_eq!(layout.model.rows_per_row_group, 4);
    assert_eq!(layout.model.row_groups_per_file, 2);
    assert!(
        layout.description.starts_with("layout model v1"),
        "{}",
        layout.description
    );
    assert!(layout.description.contains("assumed, not measured"));
    // Sorted ids [1 1 2 2 | 5 5 5 9]: row group 0 holds 1 and 2, row group 1
    // holds 5 and 9. Each row group's bytes are half the total.
    let total = replayer.catalog().bytes_total();
    let half = total / 2;
    let want = |run: &str, rgs: u64, bytes: u64, keys: u64| RunCost {
        run_id: run.into(),
        keys,
        files: u64::from(rgs > 0),
        row_groups: rgs,
        bytes,
        exact: false,
    };
    assert_eq!(
        scored.replay.proposed,
        vec![
            want("a", 1, total - half, 1),
            want("b", 2, total, 2),
            want("c", 1, half, 1),
            want("d", 0, 0, 1),
        ]
    );
    // Currently unindexed: every run reads everything, exactly.
    assert!(scored
        .replay
        .current
        .iter()
        .all(|c| c.exact && c.row_groups == 2 && c.bytes == total));

    // Written through the ledger, naming the model (contract v2.1).
    assert_eq!(
        scored.replay.model.as_deref(),
        Some(layout.description.as_str())
    );
    assert_eq!(scored.replay.inputs, vec![replayer.catalog().digest()]);
    let (h, again) = replayer.record(&ledger, &p, &workload, &keys, AT).unwrap();
    assert_eq!(again, scored);
    let Record::Replay(written) = ledger.get(&h).unwrap() else {
        panic!("not a replay");
    };
    assert_eq!(written, scored.replay);
    assert_eq!(ledger.list(RecordKind::Replay).unwrap().count(), 1);
}

#[test]
fn reports_group_runs_by_template_with_nearest_rank_quantiles() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let source = corpus.source();
    let replayer = Replayer::unindexed(&source, &corpus.data).unwrap();
    let runs = common::entity_runs(&corpus, 9, 12);
    let keys = MemoryKeySets::new();
    let mut rec = WorkloadRecorder::new(&keys, Arc::clone(&corpus.schema));
    for (i, (id, tuples)) in runs.iter().enumerate() {
        let template = if i % 3 == 0 { "nightly" } else { "hourly" };
        rec.record_with_template(
            id,
            KeyInput::Batch(&common::key_batch(&["entity_id"], tuples)),
            Some(template),
        )
        .unwrap();
    }
    let workload = rec.finish().unwrap();
    let scored = replayer
        .score(
            &Hash::of(b"p"),
            &ProposalBody::Key {
                spec: KeySpec::Exact {
                    columns: cols(&["entity_id"]),
                },
            },
            &workload,
            &keys,
            AT,
        )
        .unwrap();
    let r = report(&scored.replay, &workload).unwrap();
    assert_eq!(r.runs.len(), 9);
    let names: Vec<&str> = r.templates.iter().map(|t| t.template.as_str()).collect();
    assert_eq!(names, ["hourly", "nightly"]);
    for t in &r.templates {
        let rows: Vec<&bellwether_replay::RunRow> =
            r.runs.iter().filter(|x| x.template == t.template).collect();
        assert_eq!(t.runs, rows.len() as u64);
        let mut bytes: Vec<u64> = rows.iter().map(|x| x.proposed.bytes).collect();
        bytes.sort_unstable();
        // Nearest rank in integers: index ceil(pct * n / 100) - 1.
        let rank = |pct: usize| bytes[(pct * bytes.len()).div_ceil(100).max(1) - 1];
        assert_eq!(
            t.proposed.bytes,
            [rank(50), rank(95), *bytes.last().unwrap()]
        );
        assert!(t.proposed_exact);
    }
    // A report of another workload is refused.
    let (_, other) = common::record(&["entity_id"], &runs[..2]);
    assert!(report(&scored.replay, &other).is_err());
}

#[test]
fn attribution_hooks_return_exact_counterfactual_deltas() {
    let corpus = common::write_corpus(&CorpusSpec::default());
    let source = corpus.source();
    let replayer = Replayer::unindexed(&source, &corpus.data).unwrap();
    let runs = common::entity_runs(&corpus, 8, 21);
    let (keys, workload) = common::record(&["entity_id"], &runs);
    let range = KeySpec::Range {
        column: "entity_id".into(),
    };
    let exact = KeySpec::Exact {
        columns: cols(&["entity_id"]),
    };
    let nodes = vec![range.clone(), exact.clone()];
    let c_both = replayer.replay_specs(&nodes, &workload, &keys).unwrap();
    let c_range = replayer
        .replay_specs(std::slice::from_ref(&range), &workload, &keys)
        .unwrap();
    let c_exact = replayer
        .replay_specs(std::slice::from_ref(&exact), &workload, &keys)
        .unwrap();
    let c_none = replayer.replay_specs(&[], &workload, &keys).unwrap();

    let d = replayer
        .replace_node(&nodes, 1, &Alternative::Ablate, &workload, &keys)
        .unwrap();
    for (i, x) in d.iter().enumerate() {
        assert_eq!(x.base, c_both[i]);
        assert_eq!(x.counterfactual, c_range[i]);
        assert_eq!(x.delta, CostDelta::between(&c_both[i], &c_range[i]));
        assert!(
            x.delta.row_groups <= 0,
            "removing the exact key never helps"
        );
    }
    let bloom = KeySpec::Bloom {
        columns: cols(&["entity_id"]),
        fpr: 0.1,
    };
    let d = replayer
        .replace_node(
            &nodes,
            0,
            &Alternative::Replace(bloom.clone()),
            &workload,
            &keys,
        )
        .unwrap();
    let c_bloom_exact = replayer
        .replay_specs(&[bloom, exact.clone()], &workload, &keys)
        .unwrap();
    assert!(d
        .iter()
        .zip(&c_bloom_exact)
        .all(|(x, c)| &x.counterfactual == c));

    // I_uv = c(a_uv) − c(a_u) − c(a_v) + c(a), with both nodes ablated.
    let inter = replayer
        .interaction(
            &nodes,
            (0, &Alternative::Ablate),
            (1, &Alternative::Ablate),
            &workload,
            &keys,
        )
        .unwrap();
    for (i, x) in inter.iter().enumerate() {
        let want = CostDelta {
            files: i128::from(c_none[i].files)
                - i128::from(c_exact[i].files)
                - i128::from(c_range[i].files)
                + i128::from(c_both[i].files),
            row_groups: i128::from(c_none[i].row_groups)
                - i128::from(c_exact[i].row_groups)
                - i128::from(c_range[i].row_groups)
                + i128::from(c_both[i].row_groups),
            bytes: i128::from(c_none[i].bytes)
                - i128::from(c_exact[i].bytes)
                - i128::from(c_range[i].bytes)
                + i128::from(c_both[i].bytes),
        };
        assert_eq!(x.interaction, want, "{}", x.run_id);
    }
    assert!(replayer
        .interaction(
            &nodes,
            (0, &Alternative::Ablate),
            (0, &Alternative::Ablate),
            &workload,
            &keys
        )
        .is_err());
    let ids: BTreeSet<&str> = d.iter().map(|x| x.run_id.as_str()).collect();
    assert_eq!(ids.len(), workload.runs().len());
}
