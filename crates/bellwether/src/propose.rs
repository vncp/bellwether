//! `propose`: write a Proposal citing evidence. There is no score field in
//! a Proposal and the facade adds none, anywhere (ADR 009).

use bellwether_ledger::RecordKind;
use bellwether_types::{
    format, Error, Hash, HashKind, KeySpec, Proposal, ProposalBody, Record, Result, RewriteShape,
};

use crate::engine::{guard, Engine};
use crate::results::{ProposeRefusal, ProposeResult, Written};
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
        guard(|| self.propose_inner(body, evidence, proposed_by, true))
    }

    /// Runs every check [`Engine::propose`] runs and returns the Proposal it
    /// would write, with the hash it would have
    /// ([`ProposeResult::WouldWrite`]), or the same refusal or error, and
    /// writes nothing anywhere: it never creates the index location, and it
    /// reads the ledger only if it exists. `created_at` is this engine's
    /// clock now, so a later real write has the same hash only under a fixed
    /// clock.
    pub fn propose_dry_run(
        &self,
        body: ProposalBody,
        evidence: &[Hash],
        proposed_by: &str,
    ) -> Result<ProposeResult> {
        guard(|| self.propose_inner(body, evidence, proposed_by, false))
    }

    fn propose_inner(
        &self,
        body: ProposalBody,
        evidence: &[Hash],
        proposed_by: &str,
        write: bool,
    ) -> Result<ProposeResult> {
        if proposed_by.trim().is_empty() {
            return Err(Error::InvalidArgument {
                field: "by",
                reason: "a proposal needs a proposer: an agent or user name".into(),
            });
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
            return Err(Error::InvalidArgument {
                field: "evidence",
                reason: "a proposal must cite at least one Evidence hash, such as the profile \
                         Evidence of this data folder"
                    .into(),
            });
        }
        let not_found = |h: &Hash| Error::RecordNotFound {
            kind: HashKind::Evidence,
            hash: *h,
            place: format!("the ledger at {}", self.index_location()),
        };
        // A dry run never creates the index location: with no ledger yet,
        // no evidence can resolve.
        let ledger = if write {
            self.ledger()?
        } else {
            match self.existing_store()? {
                Some(store) => bellwether_ledger::Ledger::new(std::sync::Arc::clone(store)),
                None => return Err(not_found(&evidence[0])),
            }
        };
        for h in evidence {
            match ledger.kind(h) {
                Ok(RecordKind::Evidence) => {}
                Ok(other) => {
                    return Err(Error::WrongKind {
                        hash: *h,
                        expected: HashKind::Evidence,
                        found: other.with_article().into(),
                    })
                }
                Err(Error::NotFound(_)) => return Err(not_found(h)),
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
        let record = Record::Proposal(proposal);
        if !write {
            // The hash the ledger would name it by: BLAKE3 of its canonical
            // bytes, exactly as `Ledger::put` computes it.
            return Ok(ProposeResult::WouldWrite {
                proposal: Box::new(Written {
                    hash: Hash::of_record(&record)?,
                    record,
                }),
            });
        }
        Ok(ProposeResult::Written {
            proposal: Box::new(self.put(record)?),
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
                    return Err(Error::InvalidArgument {
                        field: "cluster_by",
                        reason: "a layout proposal clusters by at least one column".into(),
                    });
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
            return Err(Error::InvalidArgument {
                field: "shape",
                reason: format!(
                    "a rewrite proposal needs a non-empty {name}; describe the current shape, \
                     the target shape, and the invariant the rewrite preserves"
                ),
            });
        }
    }
    Ok(())
}
