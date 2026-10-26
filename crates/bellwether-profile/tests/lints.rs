//! Lints that depend on a recorded workload or a manifest, and the
//! confidence rubric of `docs/rule-catalog.md`.

#![allow(clippy::unwrap_used, clippy::assert_is_empty)]

mod common;

use std::path::Path;

use bellwether_profile::{lints, profile, ProfileOptions, ProfileReport};
use bellwether_source::ParquetSource;
use bellwether_store::{commit, StagedSegments};
use bellwether_types::{KeySpec, Lint};
use common::{path_str, temp_store, write_bench_workload, write_bench_workload_queries, Layout};

fn rules<'a>(report: &'a ProfileReport, rule: &str) -> Vec<&'a bellwether_types::Lint> {
    report.lints.iter().filter(|l| l.rule == rule).collect()
}

fn index(store: &bellwether_store::ObjectStoreBackend, root: &str, specs: &[KeySpec]) {
    commit(store, "", &StagedSegments::new(), |m| {
        let mut m = m.clone();
        m.source.root = root.to_string();
        m.schema.key_specs = specs.to_vec();
        Ok(m)
    })
    .unwrap();
}

fn specs() -> Vec<KeySpec> {
    vec![
        KeySpec::Exact {
            columns: vec!["entity_id".into()],
            counts: false,
            bounds: None,
        },
        KeySpec::Exact {
            columns: vec!["status".into(), "entity_id".into()],
            counts: false,
            bounds: None,
        },
        KeySpec::Range {
            column: "version_ts".into(),
        },
    ]
}

fn run(
    data: &Path,
    store: &std::sync::Arc<bellwether_store::ObjectStoreBackend>,
    workload: Option<&Path>,
) -> ProfileReport {
    profile(
        &ParquetSource::new(),
        &path_str(data),
        store,
        &ProfileOptions {
            workload: workload.map(Path::to_path_buf),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap()
}

#[test]
fn declared_but_unused_key_needs_a_manifest_and_a_workload() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let wl = tmp.path().join("wl");
    write_bench_workload(&wl, &[vec![1, 2, 3], vec![10, 20]]);

    // No manifest: never raised, even with a workload.
    let (_a, bare) = temp_store();
    assert_eq!(
        rules(
            &run(&data, &bare, Some(&wl)),
            lints::DECLARED_BUT_UNUSED_KEY
        ),
        Vec::<&Lint>::new()
    );

    // A manifest but no workload: never raised.
    let (_b, store) = temp_store();
    index(&store, &path_str(&data), &specs());
    assert_eq!(
        rules(&run(&data, &store, None), lints::DECLARED_BUT_UNUSED_KEY),
        Vec::<&Lint>::new()
    );

    // Both: the specs whose leading column no run supplies are flagged; the
    // workload looks up by entity_id only.
    let report = run(&data, &store, Some(&wl));
    let found = rules(&report, lints::DECLARED_BUT_UNUSED_KEY);
    let columns: Vec<&Vec<String>> = found.iter().map(|l| &l.columns).collect();
    assert_eq!(
        columns,
        [
            &vec!["status".to_string(), "entity_id".to_string()],
            &vec!["version_ts".to_string()]
        ]
    );
    for l in found {
        assert!((l.confidence - lints::MEDIUM).abs() < 1e-9);
        assert!(l.site.is_none());
        assert!(l.message.contains("no recorded run"), "{}", l.message);
    }
}

#[test]
fn confidence_follows_the_rubric() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::planted().write(&tmp.path().join("data"));
    let (_s, store) = temp_store();

    // Without a workload: medium.
    let bare = run(&data, &store, None);
    let b = rules(&bare, lints::BATCH_STAMP_KEY)[0];
    assert!((b.confidence - lints::MEDIUM).abs() < 1e-9);
    let l = rules(&bare, lints::LOW_SELECTIVITY_SINGLE_COLUMN)[0];
    assert!((l.confidence - lints::MEDIUM).abs() < 1e-9);
    let t = rules(&bare, lints::SCATTERED_TIME_COLUMN)[0];
    assert!((t.confidence - lints::MEDIUM).abs() < 1e-9);

    // With a workload over entity_id, unsampled: the batch stamp is high;
    // low selectivity is medium, the workload supplies entity_id but not
    // status, the leading column of (status, entity_id).
    let wl = tmp.path().join("wl");
    write_bench_workload(&wl, &[vec![1, 2, 3]]);
    let with = run(&data, &store, Some(&wl));
    let b = rules(&with, lints::BATCH_STAMP_KEY)[0];
    assert!((b.confidence - lints::HIGH).abs() < 1e-9, "{b:?}");
    let l = rules(&with, lints::LOW_SELECTIVITY_SINGLE_COLUMN)[0];
    assert!((l.confidence - lints::MEDIUM).abs() < 1e-9, "{l:?}");

    // Sampled measurements make both low.
    let sampled = profile(
        &ParquetSource::new(),
        &path_str(&data),
        &store,
        &ProfileOptions {
            workload: Some(wl),
            max_tracked_per_key: Some(100),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap();
    for rule in [lints::BATCH_STAMP_KEY, lints::LOW_SELECTIVITY_SINGLE_COLUMN] {
        let found = rules(&sampled, rule);
        assert_eq!(found.len(), 1, "{rule} still fires on a sample");
        assert!(
            (found[0].confidence - lints::LOW).abs() < 1e-9,
            "{:?}",
            found[0]
        );
    }
}

#[test]
fn every_lint_message_is_one_sentence() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::planted().write(&tmp.path().join("data"));
    let (_s, store) = temp_store();
    let report = run(&data, &store, None);
    assert_eq!(report.lints.len(), 3);
    for l in &report.lints {
        let body = l.message.trim_end_matches('.');
        assert!(l.message.ends_with('.'), "{}", l.message);
        assert!(
            !body.contains(". "),
            "more than one sentence: {}",
            l.message
        );
        assert!((0.0..=1.0).contains(&l.confidence));
    }
}

fn counted_specs() -> Vec<KeySpec> {
    vec![KeySpec::Exact {
        columns: vec!["entity_id".into()],
        counts: true,
        bounds: None,
    }]
}

/// `R-counts-1` fires on a count run that a later run over the same keys
/// follows, with no counting spec on the workload's key columns, and names
/// the spec that answers it.
#[test]
fn count_before_scan_fires_on_a_count_pass_ahead_of_its_query() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let wl = tmp.path().join("wl");
    write_bench_workload_queries(
        &wl,
        &[
            (Some("count"), vec![1, 2, 3]),
            (Some("locate"), vec![1, 2, 3]),
            (Some("count"), vec![10, 20]),
            (None, vec![20, 10]),
        ],
    );
    // Unindexed, and indexed with a non-counting exact spec on the column:
    // both fire, once, naming the two count passes.
    let (_a, bare) = temp_store();
    let (_b, store) = temp_store();
    index(&store, &path_str(&data), &specs());
    for s in [&bare, &store] {
        let report = run(&data, s, Some(&wl));
        assert_eq!(report.workload.as_ref().unwrap().counts_before_scan, 2);
        let found = rules(&report, lints::COUNT_BEFORE_SCAN);
        assert_eq!(found.len(), 1, "{:?}", report.lints);
        let l = found[0];
        assert_eq!(l.columns, ["entity_id"]);
        assert!((l.confidence - lints::HIGH).abs() < 1e-9);
        assert!(
            l.message.starts_with("2 recorded count runs each precede"),
            "{}",
            l.message
        );
        assert!(
            l.message.contains("exact:entity_id,counts"),
            "{}",
            l.message
        );
        assert!(l.message.ends_with('.') && !l.message.contains(". "));
    }
}

/// `R-counts-1` stays quiet when a counting spec answers the counts, when no
/// run counts, when a count is never followed by its query (or only
/// preceded by it), and without a workload.
#[test]
fn count_before_scan_does_not_fire_without_a_gated_count_or_with_counts() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let gated = tmp.path().join("gated");
    write_bench_workload_queries(
        &gated,
        &[(Some("count"), vec![1, 2]), (Some("locate"), vec![1, 2])],
    );
    let (_c, counted) = temp_store();
    index(&counted, &path_str(&data), &counted_specs());
    assert!(rules(
        &run(&data, &counted, Some(&gated)),
        lints::COUNT_BEFORE_SCAN
    )
    .is_empty());

    let (_s, store) = temp_store();
    let none = |name: &str, runs: &[(Option<&str>, Vec<u64>)]| {
        let wl = tmp.path().join(name);
        write_bench_workload_queries(&wl, runs);
        let report = run(&data, &store, Some(&wl));
        assert_eq!(
            report.workload.as_ref().unwrap().counts_before_scan,
            0,
            "{name}"
        );
        assert!(
            rules(&report, lints::COUNT_BEFORE_SCAN).is_empty(),
            "{name}"
        );
    };
    none("locate-only", &[(None, vec![1, 2]), (None, vec![1, 2])]);
    none(
        "other-keys",
        &[(Some("count"), vec![1, 2]), (Some("locate"), vec![1, 3])],
    );
    none(
        "count-after",
        &[(Some("locate"), vec![1, 2]), (Some("count"), vec![1, 2])],
    );
    none(
        "count-twice",
        &[(Some("count"), vec![1, 2]), (Some("count"), vec![1, 2])],
    );
    assert!(rules(&run(&data, &store, None), lints::COUNT_BEFORE_SCAN).is_empty());
}

/// `R-counts-1` is silenced by any spec the planner counts the workload's
/// key with: one storing counts whose key is exactly its columns, exact or
/// temporal. A spec without counts, or storing counts on other columns,
/// leaves it firing.
#[test]
fn count_before_scan_is_silenced_by_every_spec_the_planner_counts_with() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let gated = tmp.path().join("gated");
    write_bench_workload_queries(
        &gated,
        &[(Some("count"), vec![1, 2]), (Some("locate"), vec![1, 2])],
    );
    let temporal = |counts| KeySpec::Temporal {
        columns: vec!["entity_id".into()],
        ts_column: "version_ts".into(),
        counts,
    };
    let exact_on = |columns: &[&str], counts| KeySpec::Exact {
        columns: columns.iter().map(|c| (*c).to_string()).collect(),
        counts,
        bounds: None,
    };
    for (specs, fires) in [
        (vec![temporal(true)], false),
        (vec![exact_on(&["entity_id"], true)], false),
        (vec![temporal(false)], true),
        (vec![exact_on(&["status", "entity_id"], true)], true),
        (vec![exact_on(&["entity_id"], false), temporal(true)], false),
    ] {
        let (_d, store) = temp_store();
        index(&store, &path_str(&data), &specs);
        let found = rules(&run(&data, &store, Some(&gated)), lints::COUNT_BEFORE_SCAN).len();
        let names: Vec<String> = specs.iter().map(KeySpec::identity).collect();
        assert_eq!(found, usize::from(fires), "{names:?}");
    }
}

/// A workload summary cached by a binary whose summary had no
/// `counts_before_scan` (format 1) is never served: the format is part of
/// the cache key, so `R-counts-1` sees the count passes.
#[test]
fn a_workload_summary_cached_in_the_previous_format_is_not_reused() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let gated = tmp.path().join("gated");
    write_bench_workload_queries(
        &gated,
        &[(Some("count"), vec![1, 2]), (Some("locate"), vec![1, 2])],
    );
    let (_d, store) = temp_store();
    // The key and the summary a format 1 binary wrote for this workload:
    // the same key inputs, serialized in the same order, with format 1.
    let key = serde_json::to_string(&bellwether_profile::workload::cache_key(&gated).unwrap())
        .unwrap()
        .replace(
            bellwether_profile::workload::WORKLOAD_CACHE_FORMAT,
            "bellwether-profile/workload-summary/1",
        );
    let old_key = bellwether_types::Hash::of(key.as_bytes());
    let summary = bellwether_profile::workload::summarize(&gated).unwrap();
    assert_eq!(summary.counts_before_scan, 1);
    let mut old = serde_json::to_value(&summary).unwrap();
    old.as_object_mut().unwrap().remove("counts_before_scan");
    bellwether_store::Cache::new(std::sync::Arc::clone(&store), "")
        .put_cached(
            &old_key,
            bytes::Bytes::from(serde_json::to_vec(&old).unwrap()),
        )
        .unwrap();
    let report = run(&data, &store, Some(&gated));
    assert_eq!(report.workload.as_ref().unwrap().counts_before_scan, 1);
    assert_eq!(rules(&report, lints::COUNT_BEFORE_SCAN).len(), 1);
}

/// A run whose `query` is neither `locate` nor `count` is refused, naming
/// both.
#[test]
fn an_unknown_run_query_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let data = Layout::flat().write(&tmp.path().join("data"));
    let wl = tmp.path().join("wl");
    write_bench_workload_queries(&wl, &[(Some("latest"), vec![1])]);
    let (_s, store) = temp_store();
    let err = profile(
        &ParquetSource::new(),
        &path_str(&data),
        &store,
        &ProfileOptions {
            workload: Some(wl),
            ..ProfileOptions::for_source("parquet")
        },
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("\"latest\"") && err.contains("\"count\""),
        "{err}"
    );
}
