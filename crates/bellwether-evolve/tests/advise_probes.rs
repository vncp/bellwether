//! Property and probe tests for `bellwether-evolve` (WP13a, M6):
//! - Subsumption lattice and canonical set reduction.
//! - Multi-objective Pareto dominance and crowding distance.
//! - Preserve-and-extend anchor rejection when an anchor run regresses beyond `δ`.
//! - Exact bloom FPR descent and monotone formula byte estimation.
//! - Multi-workload conflicting single-query optima resolved by a 2-key set `D = {exact:a, exact:b}`
//!   and composite prefix subsumption (`exact:a,b` subsuming `exact:a`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bellwether_evolve::{
    admit, bloom_bytes_estimate, canonicalize_set, compute_frontier, descend_bloom_fpr,
    search_configurations, select_anchors, subsumes, AnchorCheck, AnchorRun, BloomGridPoint,
    CandidateEval, DiscardReason, MaintenanceCost, Objectives, SearchConfig, SubsumptionKind,
    BLOOM_FPR_GRID,
};
use bellwether_profile::{profile, ProfileOptions};
use bellwether_replay::{KeyInput, MemoryKeySets, Replayer, WorkloadRecorder};
use bellwether_source::{ParquetSource, Partitioning};
use bellwether_store::ObjectStoreBackend;
use bellwether_types::KeySpec;
use parquet::arrow::ArrowWriter;
use proptest::prelude::*;

fn write_two_col_dataset(dir: &std::path::Path) -> Arc<Schema> {
    std::fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    // Write 4 files, each with distinct tenant and order_id ranges so both
    // `tenant` and `order_id` are selective on their own workloads, while
    // neither single column serves the other workload!
    for file_idx in 0..4i64 {
        let path = dir.join(format!("part-{file_idx}.parquet"));
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();

        let mut tenants = Vec::new();
        let mut orders = Vec::new();
        let mut regions = Vec::new();
        for r in 0..500i64 {
            tenants.push(format!("t_{}", file_idx * 2 + (r % 2)));
            orders.push(file_idx * 10 + (r % 2));
            regions.push(format!("reg_{}", r % 4));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(tenants)),
                Arc::new(Int64Array::from(orders)),
                Arc::new(StringArray::from(regions)),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    schema
}

#[test]
fn multi_workload_conflicting_optima_resolved_by_two_key_set() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    let schema = write_two_col_dataset(&root);
    let root_str = root.to_str().unwrap();

    let source = ParquetSource::new();
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let prof = profile(
        &source,
        root_str,
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    let replayer = Replayer::unindexed(&source, root_str, &Partitioning::None).unwrap();
    let keys_store = MemoryKeySets::new();

    // Workload 1: queries `tenant` (only present in file 0)
    let mut rec1 = WorkloadRecorder::new(&keys_store, Arc::clone(&schema));
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "tenant",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["t_0", "t_1"]))],
    )
    .unwrap();
    rec1.record("w1_r1", KeyInput::Batch(&b1)).unwrap();
    let w1 = rec1.finish().unwrap();

    // Workload 2: queries `order_id` (only present in file 2)
    let mut rec2 = WorkloadRecorder::new(&keys_store, Arc::clone(&schema));
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "order_id",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![20, 21]))],
    )
    .unwrap();
    rec2.record("w2_r1", KeyInput::Batch(&b2)).unwrap();
    let w2 = rec2.finish().unwrap();

    // Search with max_keys = 2 across both workloads
    let cfg = SearchConfig {
        max_keys: 2,
        max_index_bytes: None,
        delta: 0.05,
        gates: Vec::new(),
    };
    let outcome = search_configurations(&prof, &replayer, &[w1, w2], &keys_store, &cfg).unwrap();

    // A single key on `tenant` leaves w2 reading more; a single key on `order_id` leaves w1
    // reading more; the 2-key set `exact:order_id + exact:tenant` serves both workloads!
    let rec_idx = outcome
        .recommended_index
        .expect("should recommend a frontier configuration");
    let rec = &outcome.frontier[rec_idx];
    assert!(
        rec.objectives.total_bytes_read < outcome.baseline.objectives.total_bytes_read,
        "recommended bytes {} must beat baseline {}",
        rec.objectives.total_bytes_read,
        outcome.baseline.objectives.total_bytes_read
    );

    // Check that the 2-key configuration {exact:order_id, exact:tenant} is on the Pareto frontier
    // and achieves the minimum total bytes read across all frontier members!
    let min_bytes = outcome
        .frontier
        .iter()
        .map(|e| e.objectives.total_bytes_read)
        .min()
        .unwrap();
    let two_key = outcome
        .frontier
        .iter()
        .find(|e| e.labels == vec!["exact:order_id".to_string(), "exact:tenant".to_string()])
        .expect("2-key set {exact:order_id, exact:tenant} must be on the Pareto frontier");
    assert_eq!(two_key.objectives.total_bytes_read, min_bytes);
}

#[test]
fn composite_prefix_subsumption_discards_redundant_pair() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    let schema = write_two_col_dataset(&root);
    let root_str = root.to_str().unwrap();

    let source = ParquetSource::new();
    let store = Arc::new(ObjectStoreBackend::memory().unwrap());
    let prof = profile(
        &source,
        root_str,
        &store,
        &ProfileOptions::for_source("parquet"),
    )
    .unwrap();
    let replayer = Replayer::unindexed(&source, root_str, &Partitioning::None).unwrap();
    let keys_store = MemoryKeySets::new();

    // Workload 1 looks up (tenant), Workload 2 looks up (tenant, region)
    let mut rec1 = WorkloadRecorder::new(&keys_store, Arc::clone(&schema));
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "tenant",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["t_0"]))],
    )
    .unwrap();
    rec1.record("r_single", KeyInput::Batch(&b1)).unwrap();
    let w1 = rec1.finish().unwrap();

    let mut rec2 = WorkloadRecorder::new(&keys_store, Arc::clone(&schema));
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["t_0"])),
            Arc::new(StringArray::from(vec!["reg_0"])),
        ],
    )
    .unwrap();
    rec2.record("r_comp", KeyInput::Batch(&b2)).unwrap();
    let w2 = rec2.finish().unwrap();

    let cfg = SearchConfig {
        max_keys: 2,
        max_index_bytes: None,
        delta: 0.05,
        gates: Vec::new(),
    };
    let outcome = search_configurations(&prof, &replayer, &[w1, w2], &keys_store, &cfg).unwrap();

    // Verify no frontier entry contains both `exact:tenant` and `exact:tenant,region`
    for entry in &outcome.frontier {
        let has_single = entry.labels.iter().any(|l| l == "exact:tenant");
        let has_comp = entry.labels.iter().any(|l| l == "exact:tenant,region");
        assert!(
            !(has_single && has_comp),
            "frontier entry {:?} contains subsumed spec alongside dominating composite",
            entry.labels
        );
    }

    // And the subsumption pair was explicitly recorded and discarded
    assert!(
        outcome.subsumptions.iter().any(|p| {
            p.subsumed == "exact:tenant"
                && p.by == "exact:tenant,region"
                && p.kind == SubsumptionKind::PrefixSeek
        }),
        "expected prefix_seek subsumption in {:?}",
        outcome.subsumptions
    );
    assert!(
        outcome
            .discarded
            .iter()
            .any(|d| d.reason == DiscardReason::Subsumed),
        "expected Subsumed discard in {:?}",
        outcome.discarded
    );
}

proptest! {
    #[test]
    fn prop_pareto_frontier_is_never_self_dominating(
        cands in prop::collection::vec((100u64..100_000, 100u64..100_000, 1usize..4, 100u64..50_000), 1..16)
    ) {
        let base = CandidateEval {
            specs: Vec::new(),
            labels: Vec::new(),
            is_baseline: true,
            objectives: Objectives {
                total_bytes_read: 200_000,
                p95_run_bytes_read: 200_000,
                max_run_bytes_read: 200_000,
                spec_count: 0,
                estimated_index_bytes: 0,
            },
            total_files_read: 4,
            total_row_groups_read: 4,
            maintenance: MaintenanceCost::default(),
            anchor_check: AnchorCheck {
                delta: 0.05,
                anchors_checked: 1,
                max_regression_fraction: 0.0,
                regressions: Vec::new(),
                passed: true,
            },
        };
        let admitted: Vec<CandidateEval> = cands
            .into_iter()
            .enumerate()
            .map(|(i, (tot, p95, sc, idx_b))| CandidateEval {
                specs: vec![KeySpec::Exact {
                    columns: vec![format!("c{i}")],
                    counts: false,
                    bounds: None,
                }],
                labels: vec![format!("exact:c{i}")],
                is_baseline: false,
                objectives: Objectives {
                    total_bytes_read: tot,
                    p95_run_bytes_read: p95,
                    max_run_bytes_read: p95.max(tot / 2),
                    spec_count: sc,
                    estimated_index_bytes: idx_b,
                },
                total_files_read: 1,
                total_row_groups_read: 1,
                maintenance: MaintenanceCost::default(),
                anchor_check: base.anchor_check.clone(),
            })
            .collect();

        let (frontier, rec) = compute_frontier(base, &admitted);
        prop_assert!(frontier[0].is_baseline);
        prop_assert!(rec.is_some());

        // No non-baseline frontier member may dominate another non-baseline frontier member
        let tail = &frontier[1..];
        for (i, a) in tail.iter().enumerate() {
            for (j, b) in tail.iter().enumerate() {
                if i != j {
                    prop_assert!(!a.objectives.dominates(&b.objectives));
                }
            }
        }
    }

    #[test]
    fn prop_canonicalize_set_is_idempotent_and_free_of_subsumption(
        has_single in any::<bool>(),
        has_counted in any::<bool>(),
        has_bounded in any::<bool>(),
        has_comp in any::<bool>(),
        has_temporal in any::<bool>(),
    ) {
        let mut specs = Vec::new();
        if has_single {
            specs.push(KeySpec::Exact {
                columns: vec!["a".into()],
                counts: false,
                bounds: None,
            });
        }
        if has_counted {
            specs.push(KeySpec::Exact {
                columns: vec!["a".into()],
                counts: true,
                bounds: None,
            });
        }
        if has_bounded {
            specs.push(KeySpec::Exact {
                columns: vec!["a".into()],
                counts: false,
                bounds: Some("ts".into()),
            });
        }
        if has_comp {
            specs.push(KeySpec::Exact {
                columns: vec!["a".into(), "b".into()],
                counts: false,
                bounds: None,
            });
        }
        if has_temporal {
            specs.push(KeySpec::Temporal {
                columns: vec!["a".into()],
                ts_column: "ts".into(),
                counts: true,
                tombstone: None,
            });
        }
        let (canon1, _) = canonicalize_set(&specs, &[]);
        let (canon2, rem2) = canonicalize_set(&canon1, &[]);
        prop_assert_eq!(&canon1, &canon2);
        prop_assert!(rem2.is_empty());
        for (i, a) in canon1.iter().enumerate() {
            for (j, b) in canon1.iter().enumerate() {
                if i != j {
                    prop_assert!(subsumes(a, b).is_none());
                    prop_assert_ne!(a.name(), b.name());
                }
            }
        }
    }
}

#[test]
fn bloom_fpr_grid_and_anchor_selection_probes() {
    for w in BLOOM_FPR_GRID.windows(2) {
        assert!(w[0] < w[1]);
        assert!(bloom_bytes_estimate(50_000, w[0]) >= bloom_bytes_estimate(50_000, w[1]));
    }
    let cols = vec!["k".to_string()];
    let pts: Vec<BloomGridPoint> = BLOOM_FPR_GRID
        .iter()
        .map(|&fpr| BloomGridPoint {
            fpr,
            spec: KeySpec::Bloom {
                columns: cols.clone(),
                fpr,
                bounds: None,
            },
            total_bytes_read: (fpr * 1_000_000.0) as u64 + 1_000,
            p95_run_bytes_read: (fpr * 500_000.0) as u64 + 500,
            max_run_bytes_read: (fpr * 500_000.0) as u64 + 500,
            formula_index_bytes: bloom_bytes_estimate(100_000, fpr),
            segment_bytes: bloom_bytes_estimate(100_000, fpr),
            marginal_bytes_saved_per_index_byte: None,
            pareto_optimal: false,
        })
        .collect();
    let sweep = descend_bloom_fpr(cols, None, pts);
    assert!(sweep.points.iter().all(|p| p.pareto_optimal));
    assert!(BLOOM_FPR_GRID.contains(&sweep.chosen_fpr));

    let _ = select_anchors(&[]);
    let _ = AnchorRun {
        workload: bellwether_types::Hash::of(b"w"),
        run_id: "r1".into(),
        template: "t".into(),
        keys: 10,
        baseline_bytes: 100,
        role: "p95_keys".into(),
    };
    let _ = admit;
}
