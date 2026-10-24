//! Evidence emission (invariant 6: every Evidence carries provenance).
//!
//! The only way this crate builds an [`Evidence`] is [`evidence`], and its
//! provenance comes from the report's inputs: the cache artifacts the
//! numbers were computed from (always the footer statistics and the fanout
//! table), the workload summary and the workload hash when a workload was
//! summarized, and the manifest's content hash when the folder is indexed.
//! `oldest_input_age` is the largest age among them and `created_at` is RFC
//! 3339 UTC. A report with no inputs is refused here, before it reaches the
//! ledger, and the ledger refuses one again at write time
//! (`bellwether-ledger`, rule 4).

use std::ops::Deref;
use std::time::Duration;

use bellwether_ledger::Ledger;
use bellwether_store::now_rfc3339;
use bellwether_types::{
    format, Error, Evidence, EvidenceBody, Hash, Provenance, Record, Result, Store,
};
use serde::{Deserialize, Serialize};

use crate::ProfileReport;

/// What a provenance input is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputKind {
    /// Footer statistics of every file (cache artifact).
    FooterStats,
    /// Fanout and time-scatter tables (cache artifact).
    FanoutTable,
    /// Verified workload summary (cache artifact).
    WorkloadSummary,
    /// The recorded workload's own hash (bench definition).
    Workload,
    /// The manifest's content hash.
    Manifest,
}

/// One provenance input with its age.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRef {
    /// What it is.
    pub kind: InputKind,
    /// The hash cited in `Provenance::inputs`.
    pub hash: Hash,
    /// Age when the report was made.
    #[serde(with = "duration_nanos")]
    pub age: Duration,
    /// Whether a cache artifact was reused rather than computed.
    pub cache_hit: bool,
}

/// Builds the `profile` Evidence for `report`. Refuses a report with no
/// provenance inputs (invariant 6).
pub fn evidence(report: &ProfileReport) -> Result<Evidence> {
    if report.inputs.is_empty() {
        return Err(Error::LedgerRule(
            "profile evidence would carry no provenance inputs (invariant 6)".into(),
        ));
    }
    let oldest_input_age = report
        .inputs
        .iter()
        .map(|i| i.age)
        .max()
        .unwrap_or_default();
    Ok(Evidence {
        record_version: format::RECORD_VERSION,
        provenance: Provenance {
            inputs: report.inputs.iter().map(|i| i.hash).collect(),
            oldest_input_age,
            manifest_version: report.manifest.as_ref().map(|m| m.sequence.to_string()),
            created_at: now_rfc3339(),
        },
        body: EvidenceBody::Profile {
            root: report.root.clone(),
            fanout: report.fanout.iter().map(|e| e.fanout.clone()).collect(),
            lints: report.lints.clone(),
            workload_keys: report.workload.as_ref().map(|w| w.sizes),
        },
    })
}

/// Builds the Evidence for `report` and writes it through the ledger.
/// Returns the record and its hash.
pub fn record<S>(ledger: &Ledger<S>, report: &ProfileReport) -> Result<(Evidence, Hash)>
where
    S: Deref,
    S::Target: Store,
{
    let e = evidence(report)?;
    let hash = ledger.put(&Record::Evidence(e.clone()))?;
    Ok((e, hash))
}

mod duration_nanos {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        u64::try_from(d.as_nanos()).unwrap_or(u64::MAX).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_nanos(u64::deserialize(d)?))
    }
}
