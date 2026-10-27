//! DataFusion (WP16 items 2, 5, 7): every lookup shape run in DataFusion
//! returns exactly the reference evaluator's rows, and, with DataFusion's
//! own pruning off, the bytes it fetched at the storage boundary
//! (`CountingStore`) are exactly the plan's: data bytes equal to the
//! planned bytes, the row groups touched equal to the planned ones, the
//! footer counted apart, nothing else read.

#![cfg(feature = "datafusion")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bellwether_adapters::datafusion::engine::object_store::local::LocalFileSystem;
use bellwether_adapters::datafusion::engine::prelude::SessionContext;
use bellwether_adapters::datafusion::{self, CountingStore};
use bellwether_adapters::measure::{classify, FileSpans, Realized};
use bellwether_adapters::{EmitOptions, ResidualMode};
use futures::TryStreamExt;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn opts(c: &common::Corpus) -> EmitOptions {
    EmitOptions {
        row_offsets: Some(common::row_offsets(c)),
        partitions: Some(common::partitions(c)),
        footer_hint: true,
        ..EmitOptions::default()
    }
}

/// Runs `plan` in a fresh session reading through a counting store.
fn run(
    rt: &tokio::runtime::Runtime,
    plan: &bellwether_types::ScanPlan,
    o: &EmitOptions,
    spans: &BTreeMap<String, FileSpans>,
) -> (Vec<arrow::array::RecordBatch>, Realized) {
    let cfg = datafusion::scan_config(plan, &common::dataset(), o).unwrap();
    let ctx = SessionContext::new();
    let store = Arc::new(CountingStore::new(Arc::new(LocalFileSystem::new())));
    datafusion::register(&ctx, &cfg, store.clone()).unwrap();
    let batches = rt.block_on(async {
        datafusion::execute(&cfg, &ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    });
    (batches, classify(&store.fetches(), spans))
}

fn spans(c: &common::Corpus) -> BTreeMap<String, FileSpans> {
    c.files
        .iter()
        .map(|f| (f.path.clone(), FileSpans::of_local(&f.path).unwrap()))
        .collect()
}

fn planned(plan: &bellwether_types::ScanPlan) -> BTreeSet<(String, u32)> {
    plan.entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect()
}

#[test]
fn every_lookup_shape_returns_the_reference_rows_and_reads_exactly_the_plan() {
    let rt = rt();
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let spans = spans(&c);
    let columns = common::all_columns();
    let mut table = Vec::new();
    for case in common::cases(&c) {
        let (batches, realized) = run(&rt, &case.plan, &opts(&c), &spans);
        assert_eq!(
            common::rendered(&batches, &columns),
            common::expected(&c, &case.plan, &columns),
            "{}",
            case.name
        );
        // The headline: data bytes are the plan's, at the storage boundary.
        assert_eq!(realized.data(), case.plan.bytes, "{}: {realized:#?}", case.name);
        assert_eq!(realized.touched(), planned(&case.plan), "{}", case.name);
        assert_eq!(realized.other(), 0, "{}: page index or bloom bytes read", case.name);
        for e in &case.plan.entries {
            let fb = &realized.files[&e.file.path];
            assert_eq!(fb.data, e.bytes, "{} {}", case.name, e.file.path);
            // One exact footer read per file: the size hint is the footer.
            assert_eq!(fb.footer, spans[&e.file.path].footer_len, "{} {}", case.name, e.file.path);
        }
        assert!(
            realized.files.keys().all(|p| case.plan.entries.iter().any(|e| &e.file.path == p)),
            "{}: an unplanned file was read",
            case.name
        );
        table.push(format!(
            "{:<52} planned {:>7}  data {:>7}  footer {:>6}  other {:>3}  rg {:>2}",
            case.name,
            case.plan.bytes,
            realized.data(),
            realized.footer(),
            realized.other(),
            realized.touched().len()
        ));
    }
    eprintln!("DataFusion through CountingStore:\n{}", table.join("\n"));
}

#[test]
fn engine_pruning_reads_at_most_the_plan() {
    let rt = rt();
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let spans = spans(&c);
    let columns = common::all_columns();
    for case in common::cases(&c) {
        let o = EmitOptions {
            engine_pruning: true,
            ..opts(&c)
        };
        let (batches, realized) = run(&rt, &case.plan, &o, &spans);
        assert_eq!(
            common::rendered(&batches, &columns),
            common::expected(&c, &case.plan, &columns),
            "{}",
            case.name
        );
        assert!(realized.data() <= case.plan.bytes, "{}", case.name);
        assert!(realized.touched().is_subset(&planned(&case.plan)), "{}", case.name);
    }
}

#[test]
fn without_the_footer_hint_the_footer_is_counted_apart_and_data_is_unchanged() {
    let rt = rt();
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let spans = spans(&c);
    let case = common::cases(&c).into_iter().next().unwrap();
    let o = EmitOptions {
        footer_hint: false,
        ..opts(&c)
    };
    let (_, realized) = run(&rt, &case.plan, &o, &spans);
    assert_eq!(realized.data(), case.plan.bytes);
    // Footer bytes are footer bytes, never counted as data.
    assert!(realized.footer() > 0);
    // Comparing totals including footers would claim more than the plan.
    let total: u64 = realized.files.values().map(|f| f.fetched).sum();
    assert!(total > case.plan.bytes, "{total}");
}

#[test]
fn an_ordered_plan_returns_rows_in_its_read_order() {
    let rt = rt();
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let spans = spans(&c);
    for interleave in [false, true] {
        let mut plan = common::latest(&c, "id", "ts", &common::ids(&[1, 2, 3, 4, 5, 6]), None, false);
        let pairs: Vec<(u32, u32)> = plan
            .entries
            .iter()
            .enumerate()
            .flat_map(|(i, e)| e.row_groups.iter().map(move |rg| (u32::try_from(i).unwrap(), rg)))
            .collect();
        let read: Vec<(u32, u32)> = if interleave {
            let mut o: Vec<(u32, u32)> = pairs.iter().rev().copied().collect();
            o.rotate_left(1);
            plan.order = Some(o.clone());
            o
        } else {
            pairs
        };
        let o = EmitOptions {
            ordered: true,
            ..opts(&c)
        };
        let (batches, realized) = run(&rt, &plan, &o, &spans);
        assert_eq!(realized.data(), plan.bytes);
        // The returned rows, in order, are the reference's rows sorted by
        // read position then row number.
        let kept = common::apply(plan.residual.as_ref().unwrap(), common::planned_rows(&c, &plan));
        let pos = |r: &common::Row| {
            let path = &c.files[r.file].path;
            let e = plan.entries.iter().position(|e| &e.file.path == path).unwrap();
            read.iter()
                .position(|&(pe, rg)| pe as usize == e && rg == r.rg)
                .unwrap()
        };
        let mut want: Vec<&common::Row> = kept.iter().collect();
        want.sort_by_key(|r| (pos(r), r.row));
        let columns = common::all_columns();
        let want: Vec<Vec<String>> = want.iter().map(|r| r.render(&columns)).collect();
        let mut got = Vec::new();
        for b in &batches {
            got.extend(common::rendered(std::slice::from_ref(b), &columns));
        }
        // `rendered` sorts within a batch; compare per row instead.
        let got_unsorted: Vec<Vec<String>> = batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).map(move |i| common::rendered(&[b.slice(i, 1)], &columns).remove(0)))
            .collect();
        assert_eq!(got_unsorted, want, "interleave {interleave}");
    }
}

#[test]
fn a_keys_file_semi_join_returns_the_same_rows_for_100k_keys() {
    let rt = rt();
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let spans = spans(&c);
    let keys: Vec<i64> = (-50_000..50_000).collect();
    let kf = tmp.path().join("keys.parquet");
    common::keys_file(&kf, &keys);
    let plan = common::locate(&c, &["id"], &common::ids(&keys));
    let columns = common::all_columns();
    let inline = run(&rt, &plan, &opts(&c), &spans);
    let o = EmitOptions {
        keys_file: Some(kf.clone()),
        ..opts(&c)
    };
    let (batches, realized) = run(&rt, &plan, &o, &spans);
    assert_eq!(common::rendered(&batches, &columns), common::expected(&c, &plan, &columns));
    assert_eq!(common::rendered(&inline.0, &columns), common::expected(&c, &plan, &columns));
    assert_eq!(realized.data(), plan.bytes);
    // A keys file that is not the plan's key set is refused.
    common::keys_file(&kf, &keys[1..]);
    assert!(matches!(
        datafusion::scan_config(&plan, &common::dataset(), &o),
        Err(bellwether_adapters::AdapterError::KeysFile { .. })
    ));
}

#[test]
fn residual_modes_are_labeled_and_supersets() {
    let rt = rt();
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let spans = spans(&c);
    let plan = common::latest(&c, "id", "ts", &common::ids(&[1, 2, 3]), None, true);
    let columns = common::all_columns();
    let full = common::expected(&c, &plan, &columns);
    for mode in [ResidualMode::KeysOnly, ResidualMode::None] {
        let o = EmitOptions {
            residual: mode,
            ..opts(&c)
        };
        let cfg = datafusion::scan_config(&plan, &common::dataset(), &o).unwrap();
        assert!(cfg.to_json().unwrap().contains(mode.as_str()));
        let (b, _) = run(&rt, &plan, &o, &spans);
        let got = common::rendered(&b, &columns);
        assert!(full.iter().all(|r| got.contains(r)), "{mode:?}");
        assert!(got.len() > full.len(), "{mode:?}");
    }
}
