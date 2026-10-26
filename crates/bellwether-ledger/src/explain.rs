//! `explain(receipt)`: walk a receipt's chain back to its evidence.

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Deref;

use bellwether_types::{
    Certificate, Coverage, Error, Evidence, EvidenceBody, Hash, Proposal, ProposalBody, Receipt,
    Record, Replay, Result, RunCost, Store,
};
use serde::Serialize;

use crate::{printable, Ledger, RecordKind, SignatureStatus, TrustedKeys, Verdict};

/// Hex characters of the hash shown per line in the text rendering.
const HASH_PREFIX: usize = 12;

/// Whether a link resolved to the record the chain expects. Anything but
/// `Ok` is flagged in the rendering; `explain` never fails on a bad link.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LinkStatus {
    /// Resolved to a record of the expected kind.
    Ok,
    /// Not in the ledger. Write-time rules allow cited evidence to be absent.
    Missing,
    /// Resolved to a record of another kind.
    WrongKind {
        /// The kind actually stored.
        found: RecordKind,
    },
    /// Present but not readable by this build: a newer record version, an
    /// unknown kind, content that does not match its name, or a malformed
    /// record.
    Unreadable {
        /// Why.
        reason: String,
    },
}

/// One link in a receipt's chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Link {
    /// The record's hash.
    pub hash: Hash,
    /// The kind the chain expects at this link.
    pub kind: RecordKind,
    /// RFC 3339 creation time, from the record or its provenance.
    pub created_at: Option<String>,
    /// Whether the link resolved as expected.
    pub status: LinkStatus,
    /// Key facts, as ordered `(name, value)` pairs.
    pub facts: Vec<(String, String)>,
    /// For the receipt and its certificate, when they resolved: every
    /// signature object beside the record, verified on its own (ADR 029).
    /// `None` for other links and flagged ones.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signatures: Option<Vec<SignatureStatus>>,
    /// For the receipt and its certificate, when explained against a trust
    /// set ([`crate::Ledger::explain_verified`]): the signatures folded
    /// against it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verdict: Option<Verdict>,
}

impl Link {
    fn new(hash: Hash, kind: RecordKind, created_at: &str, facts: Vec<(&str, String)>) -> Self {
        Self {
            hash,
            kind,
            // Every value comes from a stored record, which a store writer
            // controls: escaped before it can reach a terminal.
            created_at: Some(printable(created_at)),
            status: LinkStatus::Ok,
            facts: facts
                .into_iter()
                .map(|(k, v)| (k.to_owned(), printable(&v)))
                .collect(),
            signatures: None,
            verdict: None,
        }
    }

    fn flagged(hash: Hash, kind: RecordKind, status: LinkStatus) -> Self {
        Self {
            hash,
            kind,
            created_at: None,
            status,
            facts: Vec::new(),
            signatures: None,
            verdict: None,
        }
    }

    /// Attaches the record's signatures, and its verdict when a trust set
    /// is given.
    fn signed<S>(mut self, ledger: &Ledger<S>, trusted: Option<&TrustedKeys>) -> Result<Self>
    where
        S: Deref,
        S::Target: Store,
    {
        let statuses = ledger.signatures(&self.hash)?;
        self.verdict = trusted.map(|t| Verdict::of(&statuses, t));
        self.signatures = Some(statuses);
        Ok(self)
    }
}

/// A receipt's chain in walk order: evidence, proposal, replay, certificate,
/// and finally the receipt itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Chain {
    /// The receipt explained.
    pub receipt: Hash,
    /// Links in walk order; the last is the receipt.
    pub links: Vec<Link>,
}

impl Chain {
    /// Plain-text rendering for a terminal: one line per link with hash
    /// prefix, kind, `created_at`, and key facts.
    #[must_use]
    pub fn render(&self) -> String {
        self.to_string()
    }

    /// Whether every link is present, readable, and of the expected kind.
    ///
    /// This checks link presence and kind only. It does not re-run any
    /// write-time rule: not whether the certificate passed, verified this
    /// proposal, or was computed against `manifest_before`, nor whether the
    /// replay matches. A gate must not use it as re-verification of a
    /// receipt. The receipt's guarantees come from [`Ledger::put_receipt`]
    /// having accepted it (invariant 2).
    ///
    /// [`Ledger::put_receipt`]: crate::Ledger::put_receipt
    #[must_use]
    pub fn is_intact(&self) -> bool {
        self.links.iter().all(|l| l.status == LinkStatus::Ok)
    }
}

impl fmt::Display for Chain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for link in &self.links {
            let hex = link.hash.to_hex();
            let created = link.created_at.as_deref().unwrap_or("-");
            write!(
                f,
                "{}  {:<11}  {:<20}",
                &hex[..HASH_PREFIX],
                link.kind.as_str(),
                created
            )?;
            match &link.status {
                LinkStatus::Ok => {}
                LinkStatus::Missing => f.write_str("  MISSING (not in ledger)")?,
                LinkStatus::WrongKind { found } => {
                    write!(f, "  WRONG KIND (is {})", found.with_article())?;
                }
                LinkStatus::Unreadable { reason } => write!(f, "  UNREADABLE ({reason})")?,
            }
            for (k, v) in &link.facts {
                write!(f, "  {k}={v}")?;
            }
            if let Some(v) = &link.verdict {
                write!(f, "  sig={}", v.token())?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

/// Fetches a link. `Ok(Err(status))` is a flagged link; only store failures
/// are errors.
fn fetch<S>(ledger: &Ledger<S>, hash: &Hash) -> Result<std::result::Result<Record, LinkStatus>>
where
    S: Deref,
    S::Target: Store,
{
    match ledger.get(hash) {
        Ok(r) => Ok(Ok(r)),
        Err(Error::NotFound(_)) => Ok(Err(LinkStatus::Missing)),
        Err(
            e @ (Error::UnsupportedVersion { .. }
            | Error::UnknownKind { .. }
            | Error::LedgerRule(_)
            | Error::Serde(_)
            | Error::Other(_)),
        ) => Ok(Err(LinkStatus::Unreadable {
            reason: printable(&e.to_string()),
        })),
        Err(e) => Err(e),
    }
}

/// Resolves one link to its expected kind, or the flag explaining why not.
macro_rules! resolve_link {
    ($ledger:expr, $hash:expr, $variant:ident) => {
        match fetch($ledger, $hash)? {
            Ok(Record::$variant(x)) => Ok(x),
            Ok(other) => Err(Link::flagged(
                *$hash,
                RecordKind::$variant,
                LinkStatus::WrongKind {
                    found: RecordKind::of(&other),
                },
            )),
            Err(status) => Err(Link::flagged(*$hash, RecordKind::$variant, status)),
        }
    };
}

pub(crate) fn explain<S>(
    ledger: &Ledger<S>,
    receipt_hash: &Hash,
    trusted: Option<&TrustedKeys>,
) -> Result<Chain>
where
    S: Deref,
    S::Target: Store,
{
    let receipt = match ledger.get(receipt_hash)? {
        Record::Receipt(r) => r,
        other => {
            return Err(Error::Other(format!(
                "{receipt_hash} is {}, not a receipt",
                RecordKind::of(&other).with_article()
            )))
        }
    };
    let proposal = resolve_link!(ledger, &receipt.proposal, Proposal);
    let replay = resolve_link!(ledger, &receipt.replay, Replay);
    let cert = resolve_link!(ledger, &receipt.certificate, Certificate);

    // Evidence cited by the receipt, then any further evidence the proposal
    // cites, each once, in citation order.
    let proposal_evidence = proposal.as_ref().map_or(&[][..], |p| &p.evidence[..]);
    let mut seen = BTreeSet::new();
    let mut links = Vec::new();
    for h in receipt.evidence.iter().chain(proposal_evidence) {
        if !seen.insert(*h) {
            continue;
        }
        links.push(match resolve_link!(ledger, h, Evidence) {
            Ok(e) => evidence_link(*h, &e),
            Err(flagged) => flagged,
        });
    }
    links.push(match proposal {
        Ok(p) => proposal_link(receipt.proposal, &p),
        Err(flagged) => flagged,
    });
    links.push(match replay {
        Ok(r) => replay_link(receipt.replay, &r),
        Err(flagged) => flagged,
    });
    links.push(match cert {
        Ok(c) => certificate_link(receipt.certificate, &c).signed(ledger, trusted)?,
        Err(flagged) => flagged,
    });
    links.push(receipt_link(*receipt_hash, &receipt).signed(ledger, trusted)?);
    Ok(Chain {
        receipt: *receipt_hash,
        links,
    })
}

fn evidence_link(hash: Hash, e: &Evidence) -> Link {
    let p = &e.provenance;
    let mut facts = match &e.body {
        EvidenceBody::Profile {
            root,
            fanout,
            lints,
            twin,
            ..
        } => {
            let mut f = vec![
                ("body", "profile".to_owned()),
                ("root", root.clone()),
                ("fanout", fanout.len().to_string()),
                ("lints", lints.len().to_string()),
            ];
            if let Some(t) = twin {
                f.push(("twin_of", t.engine.clone()));
                f.push(("engine_rows", t.engine_rows.to_string()));
                f.push(("lake_rows", t.lake_rows.to_string()));
                f.push(("predicted_rows", t.predicted_rows.to_string()));
            }
            f
        }
        EvidenceBody::Plan { plan } => vec![
            ("body", "plan".to_owned()),
            ("bytes", plan.bytes.to_string()),
            (
                "row_groups",
                format!("{}/{}", plan.row_groups_selected, plan.row_groups_total),
            ),
        ],
    };
    facts.push(("inputs", p.inputs.len().to_string()));
    facts.push(("age", format!("{}s", p.oldest_input_age.as_secs())));
    facts.push((
        "manifest",
        p.manifest_version.clone().unwrap_or_else(|| "-".to_owned()),
    ));
    Link::new(hash, RecordKind::Evidence, &p.created_at, facts)
}

fn proposal_link(hash: Hash, p: &Proposal) -> Link {
    let body = match &p.body {
        ProposalBody::Key { spec } => ("key", spec.name()),
        ProposalBody::Layout { cluster_by } => ("layout", cluster_by.join(",")),
        ProposalBody::Rewrite { shape } if !shape.pairs.is_empty() => (
            "rewrite",
            format!(
                "{} engine query pair(s), compared by {}",
                shape.pairs.len(),
                shape.key.join(",")
            ),
        ),
        ProposalBody::Rewrite { shape } => ("rewrite", format!("{:?}", shape.after)),
    };
    Link::new(
        hash,
        RecordKind::Proposal,
        &p.created_at,
        vec![body, ("by", p.proposed_by.clone())],
    )
}

fn total_bytes(runs: &[RunCost]) -> u64 {
    runs.iter().fold(0u64, |acc, r| acc.saturating_add(r.bytes))
}

fn replay_link(hash: Hash, r: &Replay) -> Link {
    let mut facts = vec![
        ("runs", r.current.len().to_string()),
        ("bytes_current", total_bytes(&r.current).to_string()),
        ("bytes_proposed", total_bytes(&r.proposed).to_string()),
        ("manifest", r.manifest_version.clone()),
    ];
    if let Some(m) = &r.model {
        // A modelled number must never read like a measured one.
        facts.push(("model", m.clone()));
        if r.current
            .iter()
            .chain(&r.proposed)
            .any(|c| c.measured.is_some())
        {
            facts.push(("costs", "measured, not exact".to_owned()));
        }
    }
    if let Some(p) = &r.prediction {
        prediction_facts(&mut facts, p);
    }
    Link::new(hash, RecordKind::Replay, &r.created_at, facts)
}

/// The prediction error, as facts: the twin's rows beside the engine's,
/// each pair's value in the proposal's order.
fn prediction_facts(facts: &mut Vec<(&'static str, String)>, p: &bellwether_types::Prediction) {
    let each = |f: &dyn Fn(&bellwether_types::PairPrediction) -> String| {
        p.pairs.iter().map(f).collect::<Vec<_>>().join(", ")
    };
    facts.push(("predicted_rows", p.predicted_rows.to_string()));
    facts.push((
        "read_rows_before",
        each(&|q| q.before_read_rows.to_string()),
    ));
    facts.push(("read_rows_after", each(&|q| q.after_read_rows.to_string())));
    facts.push((
        "ratio_after",
        each(&|q| {
            q.after_ratio
                .clone()
                .unwrap_or_else(|| "undefined".to_owned())
        }),
    ));
}

fn certificate_link(hash: Hash, c: &Certificate) -> Link {
    let coverage = match &c.coverage {
        Coverage::Full => "full".to_owned(),
        Coverage::Workload { keys, .. } => format!("workload({keys} keys)"),
        Coverage::Sampled { keys, fraction } => format!("sampled({keys} keys, {fraction})"),
        Coverage::StaticProof { identity } => format!("static-proof({identity})"),
    };
    Link::new(
        hash,
        RecordKind::Certificate,
        &c.provenance.created_at,
        vec![
            ("coverage", coverage),
            ("pass", c.pass.to_string()),
            ("mismatched", c.body.mismatched.to_string()),
        ],
    )
}

fn receipt_link(hash: Hash, r: &Receipt) -> Link {
    let mut facts = vec![(
        "manifest",
        format!("{}->{}", r.manifest_before, r.manifest_after),
    )];
    if r.attest_only {
        facts.push(("apply", "attest-only".to_owned()));
    }
    if let Some(p) = &r.prediction {
        prediction_facts(&mut facts, p);
    }
    Link::new(hash, RecordKind::Receipt, &r.created_at, facts)
}
