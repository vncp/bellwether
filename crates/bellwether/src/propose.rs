//! `propose`: write a Proposal citing evidence. There is no score field in
//! a Proposal and the facade adds none, anywhere (ADR 009).

use bellwether_ledger::RecordKind;
use bellwether_types::{
    format, Error, Hash, KeySpec, Proposal, ProposalBody, Record, Result, RewriteShape,
};

use crate::engine::{guard, Engine};
use crate::results::{ProposeRefusal, ProposeResult};
use crate::why::RESERVED_PROPOSERS;

impl Engine {
    /// Writes a Proposal (Key, Layout, or Rewrite) by `proposed_by`, citing
    /// `evidence`. Every cited hash must resolve to an Evidence record in
    /// this ledger, and at least one must be cited: nothing crosses toward
    /// apply without a record of why. A key spec is validated (core's rules)
    /// and its columns, like a layout's, must exist in the dataset; a
    /// rewrite's three descriptions must be non-empty. A rewrite is data,
    /// never code the engine runs (threat model).
    pub fn propose(
        &self,
        body: ProposalBody,
        evidence: &[Hash],
        proposed_by: &str,
    ) -> Result<ProposeResult> {
        guard(|| self.propose_inner(body, evidence, proposed_by))
    }

    fn propose_inner(
        &self,
        body: ProposalBody,
        evidence: &[Hash],
        proposed_by: &str,
    ) -> Result<ProposeResult> {
        if proposed_by.trim().is_empty() {
            return Err(Error::Other(
                "a proposal needs a proposer; pass --by <agent or user name>".into(),
            ));
        }
        let name = proposed_by.trim();
        if RESERVED_PROPOSERS
            .iter()
            .any(|r| name.eq_ignore_ascii_case(r))
        {
            // A trust-boundary refusal, typed: nothing is written.
            return Ok(ProposeResult::Refused {
                refusal: ProposeRefusal::ReservedProposer {
                    name: name.to_string(),
                },
            });
        }
        if evidence.is_empty() {
            return Err(Error::Other(
                "a proposal must cite at least one Evidence hash; run `bellwether why` and cite \
                 the evidence hash it prints with --evidence"
                    .into(),
            ));
        }
        let ledger = self.ledger()?;
        for h in evidence {
            match ledger.kind(h) {
                Ok(RecordKind::Evidence) => {}
                Ok(other) => {
                    return Err(Error::LedgerRule(format!(
                        "{h} is {}, not an evidence record; cite the evidence hash `bellwether \
                         why` printed",
                        other.with_article()
                    )))
                }
                Err(Error::NotFound(_)) => {
                    return Err(Error::Other(format!(
                        "evidence {h} is not in the ledger at {}; cite a hash `bellwether why` \
                         printed for this index",
                        self.index_location()
                    )))
                }
                Err(e) => return Err(e),
            }
        }
        self.check_body(&body)?;
        let proposal = Proposal {
            record_version: format::RECORD_VERSION,
            evidence: evidence.to_vec(),
            proposed_by: proposed_by.to_string(),
            created_at: self.now(),
            body,
        };
        Ok(ProposeResult::Written {
            proposal: Box::new(self.put(Record::Proposal(proposal))?),
        })
    }

    fn check_body(&self, body: &ProposalBody) -> Result<()> {
        match body {
            ProposalBody::Key { spec } => {
                crate::spec::check(spec)?;
                let mut cols: Vec<&String> = spec.columns().iter().collect();
                if let KeySpec::Temporal { ts_column, .. } = spec {
                    cols.push(ts_column);
                }
                self.check_columns(&cols)
            }
            ProposalBody::Layout { cluster_by } => {
                if cluster_by.is_empty() {
                    return Err(Error::KeySpec(
                        "a layout proposal clusters by at least one column".into(),
                    ));
                }
                self.check_columns(&cluster_by.iter().collect::<Vec<_>>())
            }
            ProposalBody::Rewrite { shape } => check_shape(shape),
        }
    }

    fn check_columns(&self, cols: &[&String]) -> Result<()> {
        let pinned = self.pin()?;
        let source = self.source(pinned.as_ref())?;
        let schema = source.schema(self.root())?;
        for c in cols {
            if schema.field_with_name(c).is_err() {
                return Err(Error::KeySpec(format!(
                    "column {c:?} is not in the dataset under {}; propose over its columns ({})",
                    self.root(),
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
        Ok(())
    }
}

fn check_shape(shape: &RewriteShape) -> Result<()> {
    for (name, v) in [
        ("before", &shape.before),
        ("after", &shape.after),
        ("invariant", &shape.invariant),
    ] {
        if v.trim().is_empty() {
            return Err(Error::Other(format!(
                "a rewrite proposal needs a non-empty {name}; describe the current shape, the \
                 target shape, and the invariant the rewrite preserves"
            )));
        }
    }
    Ok(())
}
