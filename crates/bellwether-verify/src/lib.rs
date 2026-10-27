//! Reconciliation is the verifier (ADR 004).
//!
//! [`Reconciler`] compares two [`Side`]s, each a `bellwether_types::Source`
//! over a root, by key:
//!
//! - **Key.** One or more columns, encoded canonically (ADR 016) at each
//!   side's dataset types. A null element is the `0x00` marker, so rows with
//!   null keys are compared rather than dropped.
//! - **Version.** With `latest_by`, the version compared per key is the
//!   newest by that column; a null never wins (ADR 014), and a key with no
//!   non-null version on a side fails to compare. Without it, the multiset
//!   of row hashes per key must match.
//! - **Row hash.** xxh3-128 over each hashed column's name length, name,
//!   and canonical value, in the shared schema order ([`row_hash`]).
//!   `ignored_columns` are left out and recorded on the certificate.
//! - **Totals.** Decimal columns summed exactly at the dataset scale, per
//!   group, as scaled integers ([`ExactTotal`]); overflow is an error. A
//!   float column is totalled only when quantized at an explicit scale
//!   ([`TotalColumn::quantize`], `--totals volume:dec(4)`): each value's
//!   exact binary value rounded half to even at that scale ([`quantize`]),
//!   then summed exactly; a NaN or infinite value is an error naming its
//!   key.
//! - **Coverage.** Every key, the keys of a recorded workload, or a
//!   deterministic sample. Never a static proof.
//!
//! The comparison runs in `P` passes over hash-partitioned key space so its
//! state fits a stated budget ([`Options::memory_budget`]; README, "Memory
//! bound"). [`Reconciler::certify`] builds the [`bellwether_types::Certificate`],
//! and [`record`] writes it through the ledger. This crate never writes a
//! Receipt.
//!
//! An indexed side ([`SidePlanner`], implemented over the build crate's
//! plan builder by [`IndexPlanner`]) anchors the certificate to its
//! manifest, and workload coverage then reads only the row groups its plan
//! selects.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

mod agg_check;
mod certificate;
mod error;
mod index;
mod outcome;
mod planner;
mod reconcile;
mod rowhash;
mod schema;
mod spec;
mod state;
mod totals;

pub use agg_check::check_agg_cells;
pub use certificate::{record, AgedInput, CertificateContext, Certified, ManifestAnchor};
pub use error::{Result, VerifyError};
pub use index::IndexPlanner;
pub use outcome::{
    AggNotUsed, Counts, GroupRepr, GroupTotal, KeyRepr, MemoryReport, Mismatch, Reconciliation,
    SidePath, SideReport, Sides, VersionDigest,
};
pub use planner::{AggTotals, ManifestIdentity, SidePlanner};
pub use reconcile::{Reconciler, Side, PARTITION_SEED, READ_BATCH_ROWS};
pub use rowhash::{push_hashed_column, row_hash};
pub use spec::{
    CoverageRequest, Options, ReconcileSpec, TotalColumn, WorkloadKeys, DEFAULT_MAX_EXAMPLES,
    DEFAULT_MEMORY_BUDGET, MAX_PASSES, MAX_QUANTIZE_SCALE,
};
pub use totals::{quantize, ExactTotal, QuantizeError, TotalValue};
