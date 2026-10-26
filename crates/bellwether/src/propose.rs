//! `propose`: write a Proposal citing evidence. There is no score field in
//! a Proposal and the facade adds none, anywhere (ADR 009).

use std::fmt::Write as _;

use bellwether_ledger::RecordKind;
use bellwether_source::Partitioning;
use bellwether_types::{
    format, Error, Evidence, EvidenceBody, Hash, HashKind, KeySpec, Proposal, ProposalBody,
    QueryPair, Record, Result, RewriteShape,
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
    /// refused, typed ([`ProposeRefusal::SidecarNotProposable`]), and
    /// nothing is written: a Proposal holds a key spec only, and replay
    /// prices key specs only, so it has no path to a receipt. The refusal
    /// comes after every check a proposal gets (proposer, evidence, and the
    /// key and time columns against the dataset), so both next steps it
    /// names work: the `index` command that builds the sidecar, and, beside
    /// a key, proposing the key alone.
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
        if let Some(sketch) = &declared.sketch {
            // A sketch is approximate and never planned with: no replay can
            // price it, so it has no path to a receipt.
            if let Some(refused) = self.check_proposer_and_evidence(evidence, proposed_by, false)? {
                return Ok(refused);
            }
            let schema = self.check_columns(&sketch.items.iter().flatten().collect::<Vec<_>>())?;
            bellwether_core::sketch::item_types(sketch, &schema)?;
            let declaration = crate::spec::declaration_label(declared);
            return Ok(ProposeResult::Refused {
                refusal: ProposeRefusal::SidecarNotProposable {
                    command: self.index_command(&declaration),
                    declaration,
                    sidecar: sketch.name(),
                    key: None,
                    root: self.root().to_string(),
                },
            });
        }
        let sidecar = match (&declared.key, &declared.sidecar) {
            (Some(spec), None) => {
                return self.propose_inner(
                    ProposalBody::Key {
                        spec: spec.clone(),
                        sidecars: Vec::new(),
                    },
                    evidence,
                    proposed_by,
                    write,
                )
            }
            (_, Some(sidecar)) => sidecar,
            (None, None) => {
                return Err(Error::KeySpec(
                    "the declaration names no key spec; pass one such as exact:<column>".into(),
                ))
            }
        };
        // Every check a proposal gets, without creating the index location:
        // the refusal writes nothing.
        if let Some(refused) = self.check_proposer_and_evidence(evidence, proposed_by, false)? {
            return Ok(refused);
        }
        if let Some(spec) = &declared.key {
            self.check_body(&ProposalBody::Key {
                spec: spec.clone(),
                sidecars: Vec::new(),
            })?;
        }
        let schema = self.check_columns(
            &sidecar
                .columns
                .iter()
                .chain(&sidecar.time_columns)
                .collect::<Vec<_>>(),
        )?;
        // The checks the build applies to a sidecar's columns: encodable
        // keys, and time columns of a type a sidecar orders by.
        sidecar
            .key_types(&schema)
            .and_then(|_| sidecar.time_types(&schema))
            .map_err(|e| match e {
                Error::KeySpec(m) => Error::KeySpec(format!("{}: {m}", sidecar.name())),
                other => other,
            })?;
        let declaration = crate::spec::declaration_label(declared);
        Ok(ProposeResult::Refused {
            refusal: ProposeRefusal::SidecarNotProposable {
                command: self.index_command(&declaration),
                declaration,
                sidecar: crate::spec::sidecar_label(sidecar),
                key: declared.key.as_ref().map(crate::spec::label),
                root: self.root().to_string(),
            },
        })
    }

    /// `bellwether index <root> <source flags> --key <declaration>`, every
    /// argument POSIX shell-quoted, the source flags those the engine was
    /// opened with ([`crate::EngineOptions::source_flags`]), so following it
    /// builds the folder as it was read here and where its lookups look.
    /// `None` when a library caller opened the engine with source options
    /// and no flags spell them. Reads nothing.
    fn index_command(&self, declaration: &str) -> Option<String> {
        let o = self.options();
        let flags: &[String] = match &o.source_flags {
            Some(flags) => flags,
            None if o.index.is_none()
                && o.format == SourceFormat::Parquet
                && o.chunk_size.is_none()
                && o.schema.is_none()
                && o.fingerprint_policy.is_none()
                && matches!(o.partitioning, None | Some(Partitioning::None)) =>
            {
                &[]
            }
            None => return None,
        };
        let mut cmd = format!("bellwether index {}", crate::spec::shell_quote(self.root()));
        for f in flags {
            let _ = write!(cmd, " {}", crate::spec::shell_quote(f));
        }
        let _ = write!(cmd, " --key {}", crate::spec::shell_quote(declaration));
        Some(cmd)
    }

    /// The proposer and evidence checks every proposal gets: a typed
    /// refusal for a reserved proposer name, or an error for an empty name,
    /// no evidence, or a hash that is not Evidence in this ledger. `create`
    /// lets the check create the index location (a real write); otherwise
    /// a folder with no ledger resolves no evidence.
    fn check_proposer_and_evidence(
        &self,
        evidence: &[Hash],
        proposed_by: &str,
        create: bool,
    ) -> Result<Option<ProposeResult>> {
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
            return Ok(Some(ProposeResult::Refused {
                refusal: ProposeRefusal::ReservedProposer {
                    name: name.to_string(),
                },
            }));
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
        let ledger = if create {
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
        Ok(None)
    }

    fn propose_inner(
        &self,
        body: ProposalBody,
        evidence: &[Hash],
        proposed_by: &str,
        write: bool,
    ) -> Result<ProposeResult> {
        if let Some(refused) = self.check_proposer_and_evidence(evidence, proposed_by, write)? {
            return Ok(refused);
        }
        let mut body = body;
        if let ProposalBody::Rewrite { shape } = &mut body {
            if !shape.pairs.is_empty() {
                // The evidence resolved above, so the ledger is there; a dry
                // run reads it without creating anything.
                let store = self
                    .existing_store()?
                    .ok_or_else(|| Error::RecordNotFound {
                        kind: HashKind::Evidence,
                        hash: evidence[0],
                        place: format!("the ledger at {}", self.index_location()),
                    })?;
                let ledger = bellwether_ledger::Ledger::new(std::sync::Arc::clone(store));
                self.complete_engine_rewrite(&ledger, evidence, shape)?;
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
            ProposalBody::Key { spec, .. } => {
                crate::spec::check(spec)?;
                // A spec the index does not declare yet is a new
                // declaration: it must name its columns uniquely.
                if !self
                    .manifest()?
                    .is_some_and(|m| m.schema.key_specs.contains(spec))
                {
                    spec.check_names()?;
                }
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
                    if matches!(spec, KeySpec::Text { .. }) {
                        bellwether_core::text::check_column_type(c, dt)?;
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

    /// A rewrite of engine queries (ADR 033) cites the twin diagnosis it
    /// rests on, and that diagnosis must have run on a key sample; its key
    /// columns default to the sample's, and its key sample to the twin's
    /// recorded workload. A key sample given must be the twin's.
    fn complete_engine_rewrite<S>(
        &self,
        ledger: &bellwether_ledger::Ledger<S>,
        evidence: &[Hash],
        shape: &mut RewriteShape,
    ) -> Result<()>
    where
        S: std::ops::Deref,
        S::Target: bellwether_types::Store,
    {
        let twin = evidence.iter().find_map(|h| match ledger.get(h) {
            Ok(Record::Evidence(Evidence {
                body: EvidenceBody::Profile { twin: Some(t), .. },
                ..
            })) => Some(t),
            _ => None,
        });
        let Some(twin) = twin else {
            return Err(Error::InvalidArgument {
                field: "evidence",
                reason: "a rewrite of engine queries cites the twin diagnosis it rests on: run \
                         `why` with the engine named (--twin-of) and cite the evidence it prints"
                    .into(),
            });
        };
        if shape.keys.is_none() {
            shape.keys = twin.workload;
        }
        crate::engine_fix::key_sample(shape, &twin)?;
        if shape.key.is_empty() {
            // Read from an existing index only: a dry run creates nothing.
            if let (Some(w), Some(store)) = (twin.workload, self.existing_store()?) {
                let cache = bellwether_store::Cache::new(std::sync::Arc::clone(store), "");
                if let Ok(wl) = bellwether_replay::Workload::load(&cache, &w) {
                    if let Some(r) = wl.runs().first() {
                        shape.key.clone_from(&r.columns);
                    }
                }
            }
        }
        crate::engine_fix::check_pairs(shape)
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

/// A rewrite of engine queries as the CLI writes it (ADR 033): the pairs
/// as given, the key columns (empty: the key sample's), and the invariant
/// (default: each pair returns the same rows by its key columns). The
/// descriptions say what the pairs are; the pairs are the proposal.
#[must_use]
pub fn engine_rewrite(
    pairs: Vec<QueryPair>,
    key: Vec<String>,
    invariant: Option<String>,
) -> RewriteShape {
    let n = pairs.len();
    RewriteShape {
        before: format!(
            "{n} quer{} as {} run in the engine today",
            if n == 1 { "y" } else { "ies" },
            if n == 1 { "it" } else { "they" }
        ),
        after: format!("the rewritten quer{}", if n == 1 { "y" } else { "ies" }),
        invariant: invariant.unwrap_or_else(|| {
            "each pair returns the same rows, compared by the proposal's key columns".to_string()
        }),
        pairs,
        key,
        keys: None,
        before_plan: None,
        after_plan: None,
    }
}
