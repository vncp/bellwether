//! Review probe for PR #21: real heap of a multi-pass reconcile with
//! `latest_by`, totals, and `group_by`, measured with a counting allocator.

#![allow(
    clippy::unwrap_used,
    clippy::pedantic,
    clippy::cloned_ref_to_slice_refs,
    missing_docs
)]

mod review21_mem;

use std::alloc::System;

use bellwether_verify::ReconcileSpec;
use cap::Cap;
use review21_mem as mem;

#[global_allocator]
static ALLOC: Cap<System> = Cap::new(System, usize::MAX);

#[test]
fn measure_latest_totals() {
    let (l, r) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    mem::write(l.path(), 300_000, 2, false, None);
    mem::write(r.path(), 300_000, 2, false, Some(12_345));
    let budget = 32 * mem::MIB;
    let spec = ReconcileSpec::new(["k"])
        .latest_by("ts")
        .totals(["amt"])
        .group_by(["grp"]);
    let max_before = ALLOC.max_allocated() as u64;
    let before = ALLOC.allocated() as u64;
    let out = mem::run(l.path(), r.path(), spec, budget);
    let peak = ALLOC.max_allocated() as u64;
    mem::report("latest_totals", budget, before, max_before, peak, &out);
}
