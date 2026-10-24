//! Lints that depend on a recorded workload or a manifest, and the
//! confidence rubric of `docs/rule-catalog.md`.

#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;

use bellwether_profile::{lints, profile, ProfileOptions, ProfileReport};
use bellwether_source::ParquetSource;
use bellwether_store::{commit, StagedSegments};
use bellwether_types::{KeySpec, Lint};
use common::{path_str, temp_store, write_bench_workload, Layout};

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
        },
        KeySpec::Exact {
            columns: vec!["status".into(), "entity_id".into()],
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
