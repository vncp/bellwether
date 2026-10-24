//! `receipt`: the ledger's `explain` chain behind a receipt, with its
//! MISSING, WRONG KIND, and UNREADABLE flags, and every record that
//! resolved.

use bellwether_ledger::LinkStatus as LedgerStatus;
use bellwether_types::{Error, Hash, Result};

use crate::engine::{guard, Engine};
use crate::results::{ChainLink, LinkStatus, ReceiptResult};

impl Engine {
    /// Walks back from the receipt `hash` through its certificate, replay,
    /// proposal, and evidence. A bad link is flagged, never an error; only a
    /// store failure or a hash that is not a receipt fails.
    pub fn receipt(&self, hash: &Hash) -> Result<ReceiptResult> {
        guard(|| self.receipt_inner(hash))
    }

    fn receipt_inner(&self, hash: &Hash) -> Result<ReceiptResult> {
        let chain = self.ledger()?.explain(hash).map_err(|e| match e {
            Error::NotFound(h) => Error::Other(format!(
                "receipt {h} is not in the ledger at {}; pass a hash `bellwether fix` printed",
                self.index_location()
            )),
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
            });
        }
        Ok(ReceiptResult {
            receipt: *hash,
            intact: chain.is_intact(),
            chain: links,
            records,
        })
    }
}
