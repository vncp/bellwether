//! Read-only views over the ledger: the newest profile findings per root,
//! every receipt with its chain status, and a scorecard per rule id. These
//! back the MCP server's resources and `bellwether ledger` (ADR 010). They
//! read records only, write nothing, and never create the index location:
//! an index that does not exist yet holds no records.
//!
//! A record this build cannot read (tampered bytes, a newer version, an
//! unknown kind, malformed JSON) is listed in `unreadable` and skipped,
//! never fatal; a store failure is an error.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bellwether_ledger::{parse_record_path, Ledger, LEDGER_PREFIX};
use bellwether_types::Store as _;
use bellwether_types::{
    Certificate, Error, Evidence, EvidenceBody, Hash, KeySpec, Proposal, ProposalBody, Receipt,
    Record, Result,
};
use serde::{Deserialize, Serialize};

use crate::engine::{guard, Engine};
use crate::results::{ChainLink, Written};

/// The rule id a proposal is counted under when no lint in the evidence it
/// cites names one of its columns.
pub const UNATTRIBUTED: &str = "unattributed";

/// The newest profile Evidence for one data root.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    /// The data root the Evidence profiles.
    pub root: String,
    /// The Evidence (its lints are in its body), exactly as stored.
    pub evidence: Written,
    /// Receipts citing this Evidence, sorted by hash.
    pub receipts: Vec<Hash>,
}

/// What [`Engine::findings`] found.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FindingsResult {
    /// One per root, sorted by root: the profile Evidence with the latest
    /// `created_at` (ties: the smaller hash).
    pub findings: Vec<Finding>,
    /// Records this build could not read, skipped.
    pub unreadable: Vec<Hash>,
}

/// One receipt and the state of its chain.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReceiptEntry {
    /// The Receipt, exactly as stored.
    pub receipt: Written,
    /// Whether every link resolved to its expected kind (the ledger's
    /// `explain`).
    pub intact: bool,
    /// The chain: evidence, proposal, replay, certificate, receipt.
    pub chain: Vec<ChainLink>,
    /// With trusted keys: whether the receipt and its certificate are both
    /// signed by a trusted key within its window ([`crate::ReceiptResult`]).
    pub signed: Option<bool>,
}

/// What [`Engine::receipts`] found.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReceiptsResult {
    /// Every receipt, newest `created_at` first (ties: the smaller hash).
    pub receipts: Vec<ReceiptEntry>,
    /// Records this build could not read, skipped.
    pub unreadable: Vec<Hash>,
}

/// Counts for one rule id.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleScore {
    /// The rule id (`docs/rule-catalog.md`), or [`UNATTRIBUTED`].
    pub rule: String,
    /// Proposals attributed to the rule.
    pub proposals: u64,
    /// Certificates that passed, over those proposals.
    pub certificates_passed: u64,
    /// Certificates that failed, over those proposals.
    pub certificates_failed: u64,
    /// Receipts, over those proposals.
    pub receipts: u64,
}

/// What [`Engine::scorecard`] found.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScorecardResult {
    /// One row per rule id, sorted by rule id.
    ///
    /// Attribution: a proposal counts under every rule id raised by a lint,
    /// in the profile Evidence it cites, whose columns share a column with
    /// the proposal (a key's columns and timestamp column, a layout's
    /// clustering columns); a rewrite names no columns and counts under
    /// every rule id its cited Evidence raises. A proposal matching none
    /// counts under [`UNATTRIBUTED`]. Certificates and receipts count under
    /// the rule ids of the proposal they verified or attest; a standalone
    /// reconcile's certificate verified no proposal and is not counted.
    pub rules: Vec<RuleScore>,
    /// Records this build could not read, skipped.
    pub unreadable: Vec<Hash>,
}

/// Every readable record, by kind.
#[derive(Default)]
struct Records {
    evidence: Vec<(Hash, Evidence)>,
    proposals: Vec<(Hash, Proposal)>,
    certificates: Vec<(Hash, Certificate)>,
    receipts: Vec<(Hash, Receipt)>,
    unreadable: Vec<Hash>,
}

impl Engine {
    /// The newest profile Evidence per data root, with the receipts that
    /// cite it. Writes nothing.
    pub fn findings(&self) -> Result<FindingsResult> {
        guard(|| {
            let all = self.records()?;
            let mut newest: BTreeMap<String, (Hash, Evidence)> = BTreeMap::new();
            for (h, e) in all.evidence {
                let EvidenceBody::Profile { root, .. } = &e.body else {
                    continue;
                };
                let newer = newest.get(root).is_none_or(|(oh, o)| {
                    (e.provenance.created_at.as_str(), std::cmp::Reverse(h))
                        > (o.provenance.created_at.as_str(), std::cmp::Reverse(*oh))
                });
                if newer {
                    newest.insert(root.clone(), (h, e));
                }
            }
            let findings = newest
                .into_iter()
                .map(|(root, (hash, e))| {
                    let mut receipts: Vec<Hash> = all
                        .receipts
                        .iter()
                        .filter(|(_, r)| r.evidence.contains(&hash))
                        .map(|(h, _)| *h)
                        .collect();
                    receipts.sort();
                    Finding {
                        root,
                        evidence: Written {
                            hash,
                            record: Record::Evidence(e),
                        },
                        receipts,
                    }
                })
                .collect();
            Ok(FindingsResult {
                findings,
                unreadable: all.unreadable,
            })
        })
    }

    /// Every receipt with its chain status (the ledger's `explain`). Writes
    /// nothing.
    pub fn receipts(&self) -> Result<ReceiptsResult> {
        guard(|| {
            let mut all = self.records()?;
            all.receipts.sort_by(|a, b| {
                b.1.created_at
                    .cmp(&a.1.created_at)
                    .then_with(|| a.0.cmp(&b.0))
            });
            let mut receipts = Vec::with_capacity(all.receipts.len());
            for (hash, receipt) in all.receipts {
                let shown = self.receipt(&hash)?;
                receipts.push(ReceiptEntry {
                    receipt: Written {
                        hash,
                        record: Record::Receipt(receipt),
                    },
                    intact: shown.intact,
                    chain: shown.chain,
                    signed: shown.signed,
                });
            }
            Ok(ReceiptsResult {
                receipts,
                unreadable: all.unreadable,
            })
        })
    }

    /// Per rule id: proposals, certificates passed and failed, receipts
    /// ([`ScorecardResult::rules`] says how a proposal is attributed).
    /// Writes nothing.
    pub fn scorecard(&self) -> Result<ScorecardResult> {
        guard(|| {
            let all = self.records()?;
            let evidence: BTreeMap<Hash, &Evidence> =
                all.evidence.iter().map(|(h, e)| (*h, e)).collect();
            let rules_of: BTreeMap<Hash, BTreeSet<String>> = all
                .proposals
                .iter()
                .map(|(h, p)| (*h, attribute(p, &evidence)))
                .collect();
            let mut rows: BTreeMap<String, RuleScore> = BTreeMap::new();
            let mut bump = |proposal: &Hash, f: &dyn Fn(&mut RuleScore)| {
                if let Some(rules) = rules_of.get(proposal) {
                    for r in rules {
                        let row = rows.entry(r.clone()).or_insert_with(|| RuleScore {
                            rule: r.clone(),
                            ..RuleScore::default()
                        });
                        f(row);
                    }
                }
            };
            for (h, _) in &all.proposals {
                bump(h, &|r| r.proposals += 1);
            }
            for (_, c) in &all.certificates {
                if let Some(p) = &c.proposal {
                    if c.pass {
                        bump(p, &|r| r.certificates_passed += 1);
                    } else {
                        bump(p, &|r| r.certificates_failed += 1);
                    }
                }
            }
            for (_, r) in &all.receipts {
                bump(&r.proposal, &|row| row.receipts += 1);
            }
            Ok(ScorecardResult {
                rules: rows.into_values().collect(),
                unreadable: all.unreadable,
            })
        })
    }

    /// Reads every record in the ledger, sorted by hash. An index location
    /// that does not exist holds none, and is not created.
    fn records(&self) -> Result<Records> {
        let mut out = Records::default();
        let Some(store) = self.existing_store()? else {
            return Ok(out);
        };
        let ledger = Ledger::new(Arc::clone(store));
        let mut hashes: Vec<Hash> = store
            .list(LEDGER_PREFIX)?
            .iter()
            .filter_map(|p| parse_record_path(p))
            .collect();
        hashes.sort();
        for h in hashes {
            match ledger.get(&h) {
                Ok(Record::Evidence(e)) => out.evidence.push((h, e)),
                Ok(Record::Proposal(p)) => out.proposals.push((h, p)),
                Ok(Record::Certificate(c)) => out.certificates.push((h, c)),
                Ok(Record::Receipt(r)) => out.receipts.push((h, r)),
                Ok(Record::Replay(_)) => {}
                Err(Error::Store(m)) => return Err(Error::Store(m)),
                Err(_) => out.unreadable.push(h),
            }
        }
        Ok(out)
    }
}

/// The rule ids a proposal is counted under ([`ScorecardResult::rules`]).
fn attribute(p: &Proposal, evidence: &BTreeMap<Hash, &Evidence>) -> BTreeSet<String> {
    let columns: Option<BTreeSet<&String>> = match &p.body {
        ProposalBody::Key { spec } => {
            let mut c: BTreeSet<&String> = spec.columns().iter().collect();
            if let KeySpec::Temporal { ts_column, .. } = spec {
                c.insert(ts_column);
            }
            Some(c)
        }
        ProposalBody::Layout { cluster_by } => Some(cluster_by.iter().collect()),
        ProposalBody::Rewrite { .. } => None,
    };
    let mut rules = BTreeSet::new();
    for h in &p.evidence {
        let Some(EvidenceBody::Profile { lints, .. }) = evidence.get(h).map(|e| &e.body) else {
            continue;
        };
        for lint in lints {
            let names_a_column = columns
                .as_ref()
                .is_none_or(|c| lint.columns.iter().any(|x| c.contains(x)));
            if names_a_column {
                rules.insert(lint.rule.clone());
            }
        }
    }
    if rules.is_empty() {
        rules.insert(UNATTRIBUTED.to_string());
    }
    rules
}
