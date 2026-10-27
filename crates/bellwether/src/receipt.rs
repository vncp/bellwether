//! `receipt`: the ledger's `explain` chain behind a receipt, with its
//! MISSING, WRONG KIND, and UNREADABLE flags, every record that resolved,
//! and the signatures beside the receipt and its certificate (ADR 029),
//! folded into verdicts when the engine has trusted keys.

use bellwether_ledger::{
    printable, LinkStatus as LedgerStatus, RecordKind, SignatureIndex, Verdict,
};
use bellwether_types::{Error, EvidenceBody, Hash, HashKind, Record, Result};

use crate::engine::{guard, Engine};
use crate::results::{ChainLink, LinkStatus, NotSigned, ReceiptResult, Written};

impl Engine {
    /// Walks back from the receipt `hash` through its certificate, replay,
    /// proposal, and evidence. A bad link is flagged, never an error; only a
    /// store failure or a hash that is not a receipt fails. Writes nothing.
    pub fn receipt(&self, hash: &Hash) -> Result<ReceiptResult> {
        guard(|| {
            let index = self.ledger()?.signature_index()?;
            self.receipt_in(hash, &index)
        })
    }

    /// [`Engine::receipt`] with the signatures of `index`, one listing of
    /// the ledger shared by every receipt a command shows.
    pub(crate) fn receipt_in(&self, hash: &Hash, index: &SignatureIndex) -> Result<ReceiptResult> {
        let ledger = self.ledger()?;
        let chain = ledger
            .explain_in(hash, self.options().trusted_keys.as_ref(), index)
            .map_err(|e| match e {
                Error::NotFound(h) if h == *hash => Error::RecordNotFound {
                    kind: HashKind::Receipt,
                    hash: h,
                    place: format!("the ledger at {}", self.index_location()),
                },
                other => other,
            })?;
        let mut links = Vec::with_capacity(chain.links.len());
        let mut records = Vec::new();
        for l in &chain.links {
            let status = match &l.status {
                LedgerStatus::Ok => LinkStatus::Ok,
                LedgerStatus::Missing => LinkStatus::Missing,
                LedgerStatus::WrongKind { found } => LinkStatus::WrongKind { found: *found },
                LedgerStatus::Unreadable { reason } => LinkStatus::Unreadable {
                    reason: reason.clone(),
                },
            };
            if status == LinkStatus::Ok {
                records.push(self.get(&l.hash)?);
            }
            links.push(ChainLink {
                hash: l.hash,
                kind: l.kind,
                created_at: l.created_at.clone(),
                status,
                facts: l.facts.clone(),
                signatures: l.signatures.clone(),
                verdict: l.verdict.clone(),
            });
        }
        let roots = evidence_roots(&records, self.root());
        let not_signed = self
            .options()
            .trusted_keys
            .as_ref()
            .map(|_| not_signed(&links, &roots, self.root()));
        Ok(ReceiptResult {
            receipt: *hash,
            intact: chain.is_intact(),
            chain: links,
            records,
            roots,
            signed: not_signed.as_ref().map(Option::is_none),
            not_signed: not_signed.flatten(),
        })
    }
}

/// Why the chain is not signed, or `None` when it is: every link intact,
/// the receipt and its certificate each `signed`, and the evidence about
/// `root` only.
pub(crate) fn not_signed(links: &[ChainLink], roots: &[String], root: &str) -> Option<NotSigned> {
    if let Some(l) = links.iter().find(|l| l.status != LinkStatus::Ok) {
        return Some(NotSigned::LinksFlagged {
            link: l.hash,
            link_kind: l.kind,
            status: l.status.clone(),
        });
    }
    let both = [RecordKind::Receipt, RecordKind::Certificate]
        .iter()
        .all(|k| {
            links
                .iter()
                .any(|l| l.kind == *k && l.verdict.as_ref().is_some_and(Verdict::is_signed))
        });
    if !both {
        return Some(NotSigned::Verdicts);
    }
    if roots.is_empty() {
        return Some(NotSigned::NoRoot);
    }
    if roots.iter().any(|r| r != root) {
        return Some(NotSigned::OtherRoot {
            roots: roots.to_vec(),
            expected: root.to_string(),
        });
    }
    None
}

/// The data roots the chain's evidence is about: each profile's root, and
/// for a plan, `root` when every file lies under it, else the folder of
/// each file that does not. Sorted, each once.
pub(crate) fn evidence_roots(records: &[Written], root: &str) -> Vec<String> {
    let prefix = format!("{}/", root.trim_end_matches('/'));
    let mut out = std::collections::BTreeSet::new();
    for w in records {
        let Record::Evidence(e) = &w.record else {
            continue;
        };
        match &e.body {
            EvidenceBody::Profile { root: r, .. } => {
                out.insert(r.clone());
            }
            EvidenceBody::Plan { plan } => {
                for entry in &plan.entries {
                    if entry.file.path.starts_with(&prefix) {
                        out.insert(root.to_string());
                    } else {
                        let folder = std::path::Path::new(&entry.file.path)
                            .parent()
                            .map_or_else(String::new, |p| p.display().to_string());
                        out.insert(folder);
                    }
                }
            }
        }
    }
    out.into_iter().collect()
}

/// One sentence on a chain's signatures against trusted keys: signed, or
/// the first reason it is not (a flagged link named, each record's
/// verdict, or the data root the evidence is about). Without trusted keys,
/// what is there. Store text in it is escaped.
#[must_use]
pub fn signature_summary(r: &ReceiptResult) -> String {
    let verdict = |k: RecordKind| {
        r.chain
            .iter()
            .find(|l| l.kind == k)
            .and_then(|l| l.verdict.as_ref())
            .map_or_else(|| "not verified".to_string(), ToString::to_string)
    };
    let both = || {
        format!(
            "receipt {}; certificate {}",
            verdict(RecordKind::Receipt),
            verdict(RecordKind::Certificate)
        )
    };
    match (&r.signed, &r.not_signed) {
        (None, _) => "not checked: no trusted keys given".to_string(),
        (Some(true), _) => format!("signed: {}", both()),
        (
            Some(false),
            Some(NotSigned::LinksFlagged {
                link,
                link_kind,
                status,
            }),
        ) => {
            let how = match status {
                LinkStatus::Ok => "ok".to_string(),
                LinkStatus::Missing => "MISSING from the ledger".to_string(),
                LinkStatus::WrongKind { found } => {
                    format!("WRONG KIND (is {})", found.with_article())
                }
                LinkStatus::Unreadable { reason } => format!("UNREADABLE ({})", printable(reason)),
            };
            format!(
                "not signed: the chain's {link_kind} {} is {how}, so the chain does not stand \
                 whatever its signatures say",
                &link.to_hex()[..12]
            )
        }
        (Some(false), Some(NotSigned::OtherRoot { roots, expected })) => {
            let roots: Vec<String> = roots.iter().map(|r| printable(r)).collect();
            format!(
                "signed, but for {}: its evidence is about another data root than {expected} \
                 (a chain copied from another index), so it does not vouch for this one",
                roots.join(", ")
            )
        }
        (Some(false), Some(NotSigned::NoRoot)) => {
            "signed, but no evidence in the chain names a data root, so it cannot be tied to \
             this one"
                .to_string()
        }
        (Some(false), _) => format!("not signed: {}", both()),
    }
}
