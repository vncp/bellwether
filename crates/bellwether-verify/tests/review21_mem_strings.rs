//! Review probe for PR #21: real heap of a multi-pass reconcile keyed by
//! 63-byte strings (the pass estimate assumes 32), so the run restarts.

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
fn measure_strings() {
    let (l, r) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    mem::write(l.path(), 300_000, 1, true, None);
    mem::write(r.path(), 300_000, 1, true, Some(12_345));
    let budget = 24 * mem::MIB;
    let max_before = ALLOC.max_allocated() as u64;
    let before = ALLOC.allocated() as u64;
    let out = mem::run(l.path(), r.path(), ReconcileSpec::new(["k"]), budget);
    let peak = ALLOC.max_allocated() as u64;
    mem::report("strings", budget, before, max_before, peak, &out);
}
