//! Review probe for PR #21: one full reconcile of a bench corpus against
//! itself (key `entity_id`, newest by `version_ts`, `amount` totals by
//! `status`), with the real heap measured by a counting allocator. Ignored;
//! `R21_CORPUS=<dir>` names a corpus written by `bellwether-bench generate`.

#![allow(
    clippy::unwrap_used,
    clippy::pedantic,
    clippy::cloned_ref_to_slice_refs,
    missing_docs
)]

use std::alloc::System;
use std::time::Instant;

use bellwether_source::ParquetSource;
use bellwether_verify::{Options, ReconcileSpec, Reconciler, Side};
use cap::Cap;

#[global_allocator]
static ALLOC: Cap<System> = Cap::new(System, usize::MAX);

#[test]
#[ignore = "needs R21_CORPUS"]
fn scale_full_reconcile_against_itself() {
    let dir = std::env::var("R21_CORPUS").unwrap();
    let data = format!("{dir}/data");
    let budget: u64 = std::env::var("R21_BUDGET")
        .ok()
        .and_then(|b| b.parse().ok())
        .unwrap_or(bellwether_verify::DEFAULT_MEMORY_BUDGET);
    let (ls, rs) = (ParquetSource::new(), ParquetSource::new());
    let spec = ReconcileSpec::new(["entity_id"])
        .latest_by("version_ts")
        .totals(["amount"])
        .group_by(["status"]);
    let reconciler = Reconciler::new(
        spec,
        Options {
            memory_budget: budget,
            ..Options::default()
        },
    )
    .unwrap();
    let before = ALLOC.allocated() as u64;
    let t = Instant::now();
    let out = reconciler
        .run(&Side::new(&ls, &data), &Side::new(&rs, &data))
        .unwrap();
    let secs = t.elapsed().as_secs_f64();
    let peak = ALLOC.max_allocated() as u64;
    let rows = out.sides[0].rows;
    #[allow(clippy::cast_precision_loss)]
    let rate = rows as f64 * f64::from(out.memory.passes) / secs;
    eprintln!(
        "R21SCALE rows/side {rows}; keys {}; pass {}; passes {}; restarts {}; budget {budget}; \
         meter peak {}; peak batch {}; heap growth {}; wall {secs:.1}s; \
         rows/s/side (one pass, as read) {:.0}; rows/s/side (end to end) {:.0}",
        out.counts.keys_compared,
        out.pass,
        out.memory.passes,
        out.memory.restarts,
        out.memory.peak_state_bytes,
        out.memory.peak_batch_bytes,
        peak.saturating_sub(before),
        rate,
        {
            #[allow(clippy::cast_precision_loss)]
            let r = rows as f64 / secs;
            r
        },
    );
    assert!(
        out.pass,
        "a corpus against itself must pass: {:?}",
        out.counts
    );
    assert!(out.memory.peak_state_bytes <= budget);
}
