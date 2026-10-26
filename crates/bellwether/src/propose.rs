//! `propose`: write a Proposal citing evidence. There is no score field in
//! a Proposal and the facade adds none, anywhere (ADR 009).

use std::fmt::Write as _;

use bellwether_ledger::RecordKind;
use bellwether_source::Partitioning;
use bellwether_types::{
    format, Error, Hash, HashKind, KeySpec, Proposal, ProposalBody, Record, Result, RewriteShape,
};

use crate::engine::{guard, Engine, SourceFormat};
use crate::results::{ProposeRefusal, ProposeResult, Written};
use crate::spec::Declaration;
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

    /// [`Engine::propose`] of a key, given as the `index --key` argument
    /// [`crate::spec::parse_declaration`] reads (`propose --key`, MCP
    /// `propose.key`): a key spec is proposed as `ProposalBody::Key`. A
    /// declaration that builds a sidecar (`temporal2:id@valid,known`, or
    /// `exact:symbol@ts`, whose per-key time bounds ride beside the key) is
    /// refused, typed ([`ProposeRefusal::SidecarNotProposable`]), before
    /// anything is read or written: a Proposal holds a key spec only, and
    /// replay prices key specs only, so it has no path to a receipt; the
    /// refusal names the `index` command that builds it.
    pub fn propose_key(
        &self,
        declared: &Declaration,
        evidence: &[Hash],
        proposed_by: &str,
    ) -> Result<ProposeResult> {
        guard(|| self.propose_declared(declared, evidence, proposed_by, true))
    }

    /// [`Engine::propose_key`] as [`Engine::propose_dry_run`]: the same
    /// refusal, or the Proposal it would write; writes nothing.
    pub fn propose_key_dry_run(
        &self,
        declared: &Declaration,
        evidence: &[Hash],
        proposed_by: &str,
    ) -> Result<ProposeResult> {
        guard(|| self.propose_declared(declared, evidence, proposed_by, false))
    }

    fn propose_declared(
        &self,
        declared: &Declaration,
        evidence: &[Hash],
        proposed_by: &str,
        write: bool,
    ) -> Result<ProposeResult> {
        match (&declared.key, &declared.sidecar) {
            (Some(spec), None) => self.propose_inner(
                ProposalBody::Key { spec: spec.clone() },
                evidence,
                proposed_by,
                write,
            ),
            (key, Some(sidecar)) => {
                let declaration = crate::spec::declaration_label(declared);
                Ok(ProposeResult::Refused {
                    refusal: ProposeRefusal::SidecarNotProposable {
                        command: self.index_command(&declaration),
                        declaration,
                        sidecar: crate::spec::sidecar_label(sidecar),
                        key: key.as_ref().map(crate::spec::label),
                        root: self.root().to_string(),
                    },
                })
            }
            (None, None) => Err(Error::KeySpec(
                "the declaration names no key spec; pass one such as exact:<column>".into(),
            )),
        }
    }

    /// `bellwether index <root> --key <declaration>`, with the source flags
    /// this engine was opened with, so a folder not yet indexed is built as
    /// it is read here (an existing index keeps its recorded scheme and
    /// accepts the same flags). Reads nothing.
    fn index_command(&self, declaration: &str) -> String {
        let mut cmd = format!("bellwether index {}", self.root());
        match self.format() {
            SourceFormat::Parquet => {}
            SourceFormat::Csv { .. } => cmd.push_str(" --format csv"),
            SourceFormat::JsonLines => cmd.push_str(" --format jsonl"),
        }
        match &self.options().partitioning {
            None | Some(Partitioning::None) => {}
            Some(Partitioning::Hive) => cmd.push_str(" --partitioning hive"),
            Some(Partitioning::Template(t)) => {
                let _ = write!(cmd, " --path-template '{}'", t.pattern());
            }
        }
        let _ = write!(cmd, " --key {declaration}");
        cmd
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
                let schema = self.check_columns(&cols)?;
                // A key column needs a canonical encoding: refuse a nested
                // one now, with the sentence `index` gives, not at `fix`.
                for c in spec.columns() {
                    let dt = schema
                        .field_with_name(c)
                        .map_err(|e| Error::KeySpec(e.to_string()))?
                        .data_type();
                    if bellwether_core::encoding::check_supported(dt).is_err() {
                        return Err(Error::KeySpec(format!(
                            "{}: key column {c:?}: {}",
                            spec.name(),
                            bellwether_core::encoding::no_encoding(dt)
                        )));
                    }
                }
                Ok(())
            }
            ProposalBody::Layout { cluster_by } => {
                if cluster_by.is_empty() {
                    return Err(Error::InvalidArgument {
                        field: "cluster_by",
                        reason: "a layout proposal clusters by at least one column".into(),
                    });
                }
                self.check_columns(&cluster_by.iter().collect::<Vec<_>>())
                    .map(|_| ())
            }
            ProposalBody::Rewrite { shape } => check_shape(shape),
        }
    }

    /// Every column in `cols` is in the dataset, or [`Error::UnknownColumn`]
    /// naming the first that is not and the columns there are. Returns the
    /// dataset schema.
    pub(crate) fn check_columns(&self, cols: &[&String]) -> Result<arrow::datatypes::SchemaRef> {
        let pinned = self.pin()?;
        let source = self.source(pinned.as_ref())?;
        let schema = source.schema(self.root())?;
        for c in cols {
            if schema.field_with_name(c).is_err() {
                // A field path into a nested column gets the reason and the
                // next step (flatten first), not the column list.
                let why = bellwether_core::encoding::not_in_schema(&schema, c);
                if why.contains("nested") {
                    return Err(Error::KeySpec(why));
                }
                return Err(Error::UnknownColumn {
                    column: (*c).clone(),
                    root: self.root().to_string(),
                    columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
                });
            }
        }
        Ok(schema)
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
