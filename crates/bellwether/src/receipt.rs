//! `receipt`: the ledger's `explain` chain behind a receipt, with its
//! MISSING, WRONG KIND, and UNREADABLE flags, every record that resolved,
//! and the signatures beside the receipt and its certificate (ADR 029),
//! folded into verdicts when the engine has trusted keys.

use bellwether_ledger::{LinkStatus as LedgerStatus, RecordKind, Verdict};
use bellwether_types::{Error, Hash, HashKind, Result};

use crate::engine::{guard, Engine};
use crate::results::{ChainLink, LinkStatus, ReceiptResult};

impl Engine {
    /// Walks back from the receipt `hash` through its certificate, replay,
    /// proposal, and evidence. A bad link is flagged, never an error; only a
    /// store failure or a hash that is not a receipt fails. Writes nothing.
    pub fn receipt(&self, hash: &Hash) -> Result<ReceiptResult> {
        guard(|| self.receipt_inner(hash))
    }

    fn receipt_inner(&self, hash: &Hash) -> Result<ReceiptResult> {
        let ledger = self.ledger()?;
        let chain = match &self.options().trusted_keys {
            Some(t) => ledger.explain_verified(hash, t),
            None => ledger.explain(hash),
        }
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
        let signed = self
            .options()
            .trusted_keys
            .as_ref()
            .map(|_| chain_signed(&links));
        Ok(ReceiptResult {
            receipt: *hash,
            intact: chain.is_intact(),
            chain: links,
            records,
            signed,
        })
    }
}

/// Whether the receipt and its certificate both carry a `signed` verdict.
/// A flagged certificate link has none, so it is not signed.
pub(crate) fn chain_signed(links: &[ChainLink]) -> bool {
    [RecordKind::Receipt, RecordKind::Certificate]
        .iter()
        .all(|k| {
            links
                .iter()
                .any(|l| l.kind == *k && l.verdict.as_ref().is_some_and(Verdict::is_signed))
        })
}
