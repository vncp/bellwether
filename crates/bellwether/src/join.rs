//! Join keys (B3): a semi-join's two datasets, addressed and planned.
//!
//! **Addressing.** The data root is the left dataset; a join key
//! `join:<left>.<cols>=<right>.<cols>` names it by its folder name and the
//! right dataset by the name of a folder **beside** it (the root's parent
//! joined with the name, symlinks resolved as the data root's are). The
//! right dataset is read through its own index at its default location
//! (`<right>/.bellwether`), in the left's file format; `--index` names the
//! left's index only. The same rule holds on the command line and over MCP,
//! where the right side is fixed by the server's operator (`--join`), never
//! by a tool argument.
//!
//! **Freshness.** The pair index is stored in the left index with the
//! right manifest's sequence pinned (ADR 020). It is stale when the left's
//! row groups change or the right commits again; every join lookup then
//! refuses with [`Error::StaleJoin`] naming the `bellwether index` command
//! that rebuilds it, and [`Engine::join_verify`] reports each key's state.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use bellwether_build::{transient_pair, Builder, JoinRefresh, JoinState};
use bellwether_core::SegmentReader;
use bellwether_replay::{PairInput, Replayer, RunJoin, Workload};
use bellwether_store::{Cache, ObjectStoreBackend};
use bellwether_types::{
    format, Error, Evidence, EvidenceBody, Exactness, Gate, KeySpec, KindColumn, Provenance,
    Record, Result, ScanPlan, Source,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::engine::{guard, root_hint, schema_hint, Engine, EngineOptions, Pinned};
use crate::keys::KeyFile;
use crate::results::{Cost, Written};
use crate::spec::shell_quote;
use crate::why::{LookupMode, WhyRequest};

/// Whether a join key can be planned with, as [`JoinKeyState`] reports it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JoinFreshness {
    /// Built over the left's live row groups as they are and pinned to the
    /// right dataset's current manifest.
    Fresh {
        /// The left sequence at which it became complete.
        complete_at: u64,
        /// The right sequence it is pinned to (the current one).
        right_sequence: u64,
    },
    /// Declared, with no pair index built yet.
    NotBuilt,
    /// The left's row groups changed since it was built (a file added,
    /// changed, or removed).
    StaleLeft {
        /// The left sequence its exact keys were read at.
        built_from: u64,
        /// The left's current sequence.
        current: u64,
    },
    /// The right dataset committed again since it was built.
    StaleRight {
        /// The right sequence it is pinned to.
        pinned: u64,
        /// The right's current sequence.
        current: u64,
    },
    /// The right dataset's index was deleted and rebuilt (or its manifest
    /// replaced) since it was built: its current manifest is another one,
    /// at a sequence no later than the pinned one.
    StaleRightRebuilt {
        /// The right sequence it is pinned to.
        pinned: u64,
        /// The right's current sequence (another manifest).
        current: u64,
    },
    /// The right dataset's files changed since its index was built (files
    /// its index does not cover), so its index, and then the join key, must
    /// be rebuilt before a semi-join can use it.
    StaleRightFiles {
        /// The right manifest sequence that no longer describes its files.
        right_sequence: u64,
        /// Files its index does not track.
        new: u64,
        /// Tracked files whose fingerprint changed.
        changed: u64,
        /// Tracked files no longer found.
        removed: u64,
    },
    /// `index` committed the left dataset's build, and could not rebuild
    /// this join key; `message` says why and what to run.
    NotRebuilt,
}

impl From<JoinState> for JoinFreshness {
    fn from(s: JoinState) -> Self {
        match s {
            JoinState::Fresh {
                complete_at,
                right_sequence,
            } => Self::Fresh {
                complete_at,
                right_sequence,
            },
            JoinState::NotBuilt => Self::NotBuilt,
            JoinState::LeftChanged {
                built_from,
                current,
            } => Self::StaleLeft {
                built_from,
                current,
            },
            JoinState::RightAdvanced { pinned, current } => Self::StaleRight { pinned, current },
            JoinState::RightRebuilt { pinned, current } => {
                Self::StaleRightRebuilt { pinned, current }
            }
        }
    }
}

/// Which right datasets a join key (B3) may name
/// ([`EngineOptions::join_scope`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum JoinScope {
    /// Any folder beside the data root: the command line, where the human
    /// names it.
    #[default]
    Any,
    /// Only this right side, `<right>.<columns>` (the MCP server's
    /// operator-given `--join`).
    Only(String),
    /// None: an MCP server started without `--join`.
    Unserved,
}

/// One declared join key and its freshness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinKeyState {
    /// The join key.
    pub spec: KeySpec,
    /// The right dataset's canonical folder.
    pub right_root: String,
    /// Its state.
    pub state: JoinFreshness,
    /// The state in words.
    pub message: String,
    /// The command that makes it fresh, when it is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// Whether this call rebuilt its pair index (`index` only).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub refreshed: bool,
}

/// What [`Engine::join_verify`] found: every declared join key (or the one
/// named) and whether each can be planned with.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinVerifyResult {
    /// The left dataset's canonical root.
    pub root: String,
    /// The left manifest sequence.
    pub sequence: u64,
    /// The join keys checked.
    pub joins: Vec<JoinKeyState>,
    /// Whether every one is fresh.
    pub pass: bool,
}

/// One side of a semi-join plan ([`SemiJoinResult`]).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SemiJoinStep {
    /// The dataset's canonical root.
    pub root: String,
    /// Its manifest sequence planned against.
    pub sequence: u64,
    /// Its key columns.
    pub columns: Vec<String>,
    /// Live files the step chose from.
    pub files_total: u64,
    /// Live row groups the step chose from.
    pub row_groups_total: u64,
    /// What the step reads: the row groups holding a key both datasets
    /// hold, with an `In` residual over those keys.
    pub planned: Cost,
    /// The rows of those keys in the step's dataset, exact, when the join
    /// key stores counts. Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// The step's `plan` Evidence (in the left dataset's ledger).
    pub evidence: Written,
}

/// What [`Engine::semijoin`] planned: `semijoin(keys)` through a fresh join
/// key, a two-step plan, the left dataset then the right.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SemiJoinResult {
    /// The join key planned with.
    pub spec: KeySpec,
    /// Distinct non-null keys asked.
    pub keys: u64,
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
    /// Keys both datasets hold: the only ones a semi-join keeps.
    pub matched: u64,
    /// BLAKE3 of the canonical key set (in the cache, cited by both
    /// Evidences).
    pub key_set: bellwether_types::Hash,
    /// Step 1: the left dataset.
    pub left: SemiJoinStep,
    /// Step 2: the right dataset.
    pub right: SemiJoinStep,
}

/// What [`Engine::join_overlap`] read from a fresh join key's pair index:
/// exact, no data file opened.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinOverlapResult {
    /// The join key.
    pub spec: KeySpec,
    /// The left dataset's canonical root and manifest sequence.
    pub root: String,
    /// The left manifest sequence.
    pub sequence: u64,
    /// The right dataset's canonical root.
    pub right_root: String,
    /// The right manifest sequence the key is pinned to.
    pub right_sequence: u64,
    /// Keys on both sides, `|L ∩ R|`.
    pub both: u64,
    /// Keys on the left only, `|L \ R|` (a tuple holding a null equals
    /// nothing, so it is one-sided).
    pub left_only: u64,
    /// Keys on the right only, `|R \ L|`.
    pub right_only: u64,
    /// Most right row groups one shared key is in.
    pub max_right_row_groups: u64,
    /// Right row groups summed over shared keys; over `both`, the mean.
    pub sum_right_row_groups: u64,
    /// Most left row groups one shared key is in.
    pub max_left_row_groups: u64,
    /// Left row groups summed over shared keys.
    pub sum_left_row_groups: u64,
    /// With counts: the rows `left ⋉ right` and `right ⋉ left` keep, and the
    /// rows of `left ⋈ right`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<JoinRows>,
}

/// Exact row counts of a counted join key's shared keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRows {
    /// Left rows whose key the right holds.
    pub left: u64,
    /// Right rows whose key the left holds.
    pub right: u64,
    /// Rows of the inner join on the key.
    pub join: u64,
}

/// The right dataset's folder for a join key on `root`: `right` beside it.
pub(crate) fn right_root_of(root: &str, right: &str) -> Result<String> {
    let parent = Path::new(root).parent().ok_or_else(|| {
        Error::Source(format!(
            "the data root {root} has no parent folder, so no dataset {right} can sit beside it; \
             move the datasets into one folder"
        ))
    })?;
    Ok(parent.join(right).to_string_lossy().into_owned())
}

impl Engine {
    /// Refuses the join key `spec` (B3) when this engine may not read its
    /// right dataset ([`EngineOptions::join_scope`]): before anything is
    /// read, with a refusal that names only the right side served, so it
    /// is the same whether a folder of the name asked exists, is indexed,
    /// or not. Any other key passes.
    pub(crate) fn check_join_scope(&self, spec: &KeySpec) -> Result<()> {
        match spec {
            KeySpec::Join {
                right,
                right_columns,
                ..
            } => self.check_right_side(right, right_columns),
            _ => Ok(()),
        }
    }

    /// [`Engine::check_join_scope`] for a right side, `right.right_columns`.
    pub(crate) fn check_right_side(&self, right: &str, right_columns: &[String]) -> Result<()> {
        let served = match &self.options().join_scope {
            JoinScope::Any => return Ok(()),
            JoinScope::Only(side) => {
                if crate::spec::parse_join_side(side)
                    .is_ok_and(|(r, c)| r == right && c.as_slice() == right_columns)
                {
                    return Ok(());
                }
                Some(side.clone())
            }
            JoinScope::Unserved => None,
        };
        let message = match &served {
            Some(side) => format!(
                "this server semi-joins only with the right side it was started with (--join \
                 {side}); a join key naming another right dataset or other columns is refused \
                 before any folder is read. Use the join key on {side}, or ask the human to \
                 restart the server with --join <right>.<columns> naming the dataset to join \
                 with"
            ),
            None => "this server was started without --join, so it reads no right dataset and \
                     plans no semi-join; a join key is refused before any folder is read. Ask \
                     the human to restart the server with --join <right>.<columns> naming the \
                     dataset to join with"
                .to_string(),
        };
        Err(Error::JoinNotServed { served, message })
    }

    /// The right dataset of the join key `spec` on this (the left)
    /// dataset: the folder of that name beside the data root (a symbolic
    /// link there must resolve to a folder beside it too), opened through
    /// its own index at its default location, in this engine's file format
    /// and clock. Its index must exist; an unindexed right is refused
    /// naming the exact key the join key reads there.
    pub(crate) fn right_engine(&self, spec: &KeySpec) -> Result<Engine> {
        let KeySpec::Join {
            right,
            right_columns,
            counts,
            ..
        } = spec
        else {
            return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
        };
        self.require_folder("a join key")?;
        let path = right_root_of(self.root(), right)?;
        if !Path::new(&path).is_dir() {
            return Err(Error::InvalidArgument {
                field: "join",
                reason: format!(
                    "the right dataset {right} is the folder {path} beside the data root, and \
                     there is no such folder; a join key names its right dataset by a folder \
                     beside the left one"
                ),
            });
        }
        let parent = Path::new(&path).parent().map(std::fs::canonicalize);
        let resolved = std::fs::canonicalize(&path);
        if let (Some(Ok(parent)), Ok(resolved)) = (parent, resolved) {
            if resolved.parent() != Some(parent.as_path()) {
                return Err(Error::InvalidArgument {
                    field: "join",
                    reason: format!(
                        "the right dataset {right} is {path}, a link to {}, which is not beside \
                         the data root (in {}); a join key reads only a dataset in the folder \
                         that holds the left one: move or copy {right} there",
                        resolved.display(),
                        parent.display()
                    ),
                });
            }
        }
        let options = EngineOptions {
            format: self.options().format,
            clock: self.options().clock.clone(),
            threads: self.options().threads,
            ..EngineOptions::default()
        };
        let e = Engine::open(&path, options)?;
        if e.manifest()?.is_none() {
            return Err(Error::NotIndexed {
                root: e.root().to_string(),
                operation: "a join key",
                needs: Some(bellwether_core::join::side_spec(right_columns, *counts).identity()),
            });
        }
        Ok(e)
    }

    /// The `bellwether index` command that rebuilds this dataset's join
    /// keys.
    pub(crate) fn refresh_command(&self) -> String {
        let mut words = vec![
            "bellwether".to_string(),
            "index".into(),
            shell_quote(self.root()),
        ];
        if let Some(flags) = &self.options().source_flags {
            words.extend(flags.iter().map(|f| shell_quote(f)));
        }
        words.join(" ")
    }

    /// The state of the declared join key `spec` against its right dataset
    /// `right`, whose current manifest `rpin` pins.
    pub(crate) fn join_key_state(
        &self,
        pinned: &Pinned,
        spec: &KeySpec,
        right: &Engine,
        rpin: &Pinned,
    ) -> Result<JoinKeyState> {
        let state = pinned.planner.join_state(spec, &rpin.planner)?;
        let message = format!("{}: {}", spec.identity(), state.describe());
        let next = (!matches!(state, JoinState::Fresh { .. })).then(|| self.refresh_command());
        Ok(JoinKeyState {
            spec: spec.clone(),
            right_root: right.root().to_string(),
            state: state.into(),
            message,
            next,
            refreshed: false,
        })
    }

    /// The typed refusal for a join key that is not fresh.
    pub(crate) fn stale_join(&self, s: &JoinKeyState) -> Error {
        let (side, pinned, current) = match &s.state {
            // Never called on a fresh key; a key never built (or not
            // rebuilt) is behind on the left.
            JoinFreshness::Fresh { .. } | JoinFreshness::NotBuilt | JoinFreshness::NotRebuilt => {
                ("left", 0, 0)
            }
            JoinFreshness::StaleLeft {
                built_from,
                current,
            } => ("left", *built_from, *current),
            JoinFreshness::StaleRight { pinned, current }
            | JoinFreshness::StaleRightRebuilt { pinned, current } => ("right", *pinned, *current),
            JoinFreshness::StaleRightFiles { right_sequence, .. } => {
                ("right", *right_sequence, *right_sequence)
            }
        };
        let rebuild = s.next.clone().unwrap_or_else(|| self.refresh_command());
        Error::StaleJoin {
            spec: s.spec.name(),
            side,
            pinned,
            current,
            message: format!(
                "{}, so it cannot be planned with: a semi-join planned from it could miss rows on \
                 the side that moved; rebuild it with `{rebuild}`",
                s.message
            ),
        }
    }

    /// Rebuilds every join key `given` declares or this index declares
    /// (B3) that is not fresh, each from both datasets' complete exact keys
    /// (a missing one is refused naming the `bellwether index` command that
    /// declares it), and reports each key's state. Called by `index` after
    /// the build, whose commit `left_committed` says: once anything was
    /// committed, a key that cannot be rebuilt is reported
    /// ([`JoinFreshness::NotRebuilt`], with why and what to run) beside the
    /// commit rather than refused, so the commit is never hidden; before,
    /// the first refusal is returned.
    pub(crate) fn refresh_joins(
        &self,
        given: &[KeySpec],
        left_committed: bool,
    ) -> Result<Vec<JoinKeyState>> {
        let declared: Vec<KeySpec> = self
            .pin()?
            .map(|p| {
                p.planner
                    .declared_specs()
                    .iter()
                    .filter(|s| matches!(s, KeySpec::Join { .. }))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let mut joins = declared.clone();
        for g in given.iter().filter(|s| matches!(s, KeySpec::Join { .. })) {
            joins.retain(|j| j.name() != g.name());
            joins.push(g.clone());
        }
        let builder = Builder::new(Arc::clone(self.store()?), "");
        let caps = self.build_options().pair_caps();
        let mut committed = left_committed;
        let mut out = Vec::with_capacity(joins.len());
        for spec in joins {
            let KeySpec::Join { right, .. } = &spec else {
                continue;
            };
            if let Err(e) = self.check_join_scope(&spec) {
                if committed {
                    let right_root = right_root_of(self.root(), right)?;
                    out.push(JoinKeyState {
                        next: None,
                        right_root,
                        message: format!("{}: not rebuilt: {e}", spec.identity()),
                        spec,
                        state: JoinFreshness::NotRebuilt,
                        refreshed: false,
                    });
                    continue;
                }
                return Err(e);
            }
            let refreshed = (|| {
                let right = self.right_engine(&spec)?;
                let rpin = right.pin_required("a join key")?;
                right.refuse_stale(&rpin, "a join key")?;
                let done = builder
                    .refresh_join(&spec, right.store()?, "", Some(rpin.hash), caps)
                    .map_err(|e| self.side_hint(&right, e))?;
                if let JoinRefresh::RightChanged { found, .. } = done {
                    return Err(Error::VersionConflict {
                        expected: format!(
                            "the manifest of {} read at sequence {}",
                            right.root(),
                            rpin.sequence()
                        ),
                        found: format!(
                            "another manifest at sequence {} committed meanwhile; run the index \
                             again",
                            found.sequence
                        ),
                    });
                }
                let pinned = self.pin_required("index")?;
                let mut state = self.join_key_state(&pinned, &spec, &right, &rpin)?;
                state.refreshed = matches!(done, JoinRefresh::Built { .. });
                Ok(state)
            })();
            match refreshed {
                Ok(state) => {
                    committed |= state.refreshed;
                    out.push(state);
                }
                Err(e) if committed => {
                    let right_root = right_root_of(self.root(), right)?;
                    // A key this call declares is not in the manifest yet:
                    // the command that rebuilds it must name it again.
                    let mut refresh = self.refresh_command();
                    if !declared.contains(&spec) {
                        refresh = format!("{refresh} --key {}", shell_quote(&spec.identity()));
                    }
                    out.push(JoinKeyState {
                        next: Some(self.next_after(&e, &right_root, &refresh)),
                        right_root,
                        message: format!("{}: not rebuilt: {e}", spec.identity()),
                        spec,
                        state: JoinFreshness::NotRebuilt,
                        refreshed: false,
                    });
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// What to run when a join key could not be rebuilt for `e`, its right
    /// dataset at `right_root`: a cause on the right is fixed there first
    /// (its exact key declared, or its files indexed), then `refresh` (this
    /// dataset's `bellwether index`, naming the key when it is not declared
    /// yet) rebuilds the key; any other cause, `refresh` alone.
    fn next_after(&self, e: &Error, right_root: &str, refresh: &str) -> String {
        let right_index = format!("bellwether index {}", shell_quote(right_root));
        match e {
            Error::NotIndexed {
                root,
                needs: Some(needs),
                ..
            } if root != self.root() => format!(
                "bellwether index {} --key {} && {refresh}",
                shell_quote(root),
                shell_quote(needs)
            ),
            // This dataset's own exact key: the same `index` declares it
            // and then rebuilds the join key.
            Error::NotIndexed {
                needs: Some(needs), ..
            } => format!("{refresh} --key {}", shell_quote(needs)),
            Error::StaleIndex { .. } => format!("{right_index} && {refresh}"),
            _ => refresh.to_string(),
        }
    }

    /// A missing exact key on one side of a join key, named with the full
    /// root of that side so the `bellwether index` step can be followed.
    fn side_hint(&self, right: &Engine, e: Error) -> Error {
        match e {
            Error::NotIndexed {
                root,
                operation,
                needs: Some(needs),
            } => {
                let root = if root == folder_name(right.root()) {
                    right.root().to_string()
                } else {
                    self.root().to_string()
                };
                let spec = needs
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .to_string();
                Error::NotIndexed {
                    root,
                    operation,
                    needs: Some(spec),
                }
            }
            other => other,
        }
    }

    /// The declared join key `join` names: its whole spec
    /// (`join:orders.order_id=payables.order_id`), or its right side
    /// (`payables.order_id`), with `left_columns` (a key file's) choosing
    /// among keys of that right side.
    fn resolve_join(
        &self,
        pinned: &Pinned,
        join: &str,
        left_columns: Option<&[String]>,
    ) -> Result<KeySpec> {
        let all = pinned.planner.declared_specs();
        let declared: Vec<&KeySpec> = all
            .iter()
            .filter(|s| matches!(s, KeySpec::Join { .. }))
            .collect();
        let folder = folder_name(self.root());
        if join.trim_start().starts_with("join:") {
            let spec = crate::spec::parse(join)?;
            if let (Some(c), KeySpec::Join { columns, .. }) = (left_columns, &spec) {
                if c != columns.as_slice() {
                    return Err(Error::InvalidArgument {
                        field: "keys",
                        reason: format!(
                            "the key file's columns ({}) are not {}'s left columns ({}); give a \
                             key file of those columns",
                            c.join(", "),
                            spec.name(),
                            columns.join(", ")
                        ),
                    });
                }
            }
            if declared.iter().any(|d| **d == spec) {
                return Ok(spec);
            }
            return Err(Error::NotIndexed {
                root: self.root().to_string(),
                operation: "a semi-join",
                needs: Some(crate::spec::label(&spec)),
            });
        }
        let (right, right_columns) = crate::spec::parse_join_side(join)?;
        let matching: Vec<&KeySpec> = declared
            .iter()
            .copied()
            .filter(|s| {
                matches!(s, KeySpec::Join { right: r, right_columns: rc, columns, .. }
                    if *r == right && *rc == right_columns
                        && left_columns.is_none_or(|c| c == columns.as_slice()))
            })
            .collect();
        match matching.as_slice() {
            [one] => Ok((*one).clone()),
            [] => {
                let left = left_columns.map_or_else(|| "<column>".to_string(), |c| c.join(","));
                Err(Error::NotIndexed {
                    root: self.root().to_string(),
                    operation: "a semi-join",
                    needs: Some(format!(
                        "join:{folder}.{left}={right}.{}",
                        right_columns.join(",")
                    )),
                })
            }
            many => Err(Error::InvalidArgument {
                field: "join",
                reason: format!(
                    "several join keys pair {right}.{}: {}; name one whole (--join \
                     join:{folder}.<columns>={right}.{})",
                    right_columns.join(","),
                    many.iter().map(|s| s.name()).collect::<Vec<_>>().join(", "),
                    right_columns.join(",")
                ),
            }),
        }
    }

    /// The join key `join` names, fresh, with both datasets pinned: or the
    /// refusal (`stale_join`, `stale_index` on either side, `not_indexed`).
    fn fresh_join(
        &self,
        verb: &'static str,
        join: &str,
        left_columns: Option<&[String]>,
    ) -> Result<(Pinned, KeySpec, Engine, Pinned)> {
        self.require_folder(verb)?;
        let pinned = self.pin_required(verb)?;
        self.refuse_stale(&pinned, verb)?;
        let spec = self.resolve_join(&pinned, join, left_columns)?;
        let right = self.right_engine(&spec)?;
        let rpin = right.pin_required(verb)?;
        right.refuse_stale(&rpin, verb)?;
        let state = self.join_key_state(&pinned, &spec, &right, &rpin)?;
        if !matches!(state.state, JoinFreshness::Fresh { .. }) {
            return Err(self.stale_join(&state));
        }
        Ok((pinned, spec, right, rpin))
    }

    /// `semijoin(keys)` (B3): the rows of both datasets whose key, one of
    /// `keys` (a key file on the join key's left columns), the other dataset
    /// also holds; `in (subquery)`, `exists`, a semi-join. `join` names the
    /// declared join key: whole (`join:orders.order_id=payables.order_id`)
    /// or by its right side (`payables.order_id`). Planned through its pair
    /// index alone as a two-step plan, the left dataset then the right,
    /// each step the row groups holding a key both hold with an `In`
    /// residual over those keys (invariant 1 per side); with counts, each
    /// step's rows are exact. A stale key is refused with
    /// [`Error::StaleJoin`], naming the command that rebuilds it. Writes one
    /// `plan` Evidence per step and the key set to the cache.
    pub fn semijoin(&self, join: &str, keys: &KeyFile) -> Result<SemiJoinResult> {
        guard(|| self.semijoin_inner(join, keys))
            .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    fn semijoin_inner(&self, join: &str, keys: &KeyFile) -> Result<SemiJoinResult> {
        let verb = "semijoin";
        let columns = keys.column_names()?;
        let (pinned, spec, right, rpin) = self.fresh_join(verb, join, Some(&columns))?;
        let (run_keys, null_tuples) = keys.run_keys(pinned.planner.schema())?;
        let tuples: Vec<Vec<u8>> = run_keys.tuples().map(<[u8]>::to_vec).collect();
        let payload = run_keys.to_payload()?;
        let key_set = bellwether_types::Hash::of(&payload);
        self.cache()?
            .put_cached_once(&key_set, Bytes::from(payload))?;
        let plan = pinned
            .planner
            .plan_semijoin(&spec, &tuples, &rpin.planner)?;
        let KeySpec::Join { right_columns, .. } = &spec else {
            return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
        };
        let left = self.step(&pinned, self.root(), &columns, key_set, plan.left)?;
        let right = self.step(&rpin, right.root(), right_columns, key_set, plan.right)?;
        Ok(SemiJoinResult {
            spec,
            keys: plan.keys,
            null_tuples,
            matched: plan.matched,
            key_set,
            left,
            right,
        })
    }

    fn step(
        &self,
        pinned: &Pinned,
        root: &str,
        columns: &[String],
        key_set: bellwether_types::Hash,
        plan: ScanPlan,
    ) -> Result<SemiJoinStep> {
        let planned = Cost {
            files: plan.entries.len() as u64,
            row_groups: plan.row_groups_selected,
            bytes: plan.bytes,
        };
        let (row_groups_total, rows) = (plan.row_groups_total, plan.rows.as_ref().map(|q| q.value));
        let evidence = self.put(Record::Evidence(Evidence {
            record_version: format::RECORD_VERSION,
            provenance: Provenance {
                inputs: vec![pinned.hash, key_set],
                oldest_input_age: pinned.age,
                manifest_version: Some(pinned.sequence().to_string()),
                created_at: self.now(),
            },
            body: EvidenceBody::Plan { plan },
        }))?;
        Ok(SemiJoinStep {
            root: root.to_string(),
            sequence: pinned.sequence(),
            columns: columns.to_vec(),
            files_total: pinned.manifest().rowgroup_space.files.len() as u64,
            row_groups_total,
            planned,
            rows,
            evidence,
        })
    }

    /// `overlap_stats()` of the declared join key `join` (named as
    /// [`Engine::semijoin`] takes it; by its right side, the one key of that
    /// right side): `|L ∩ R|`, `|L \ R|`, `|R \ L|`, and the row groups (with
    /// counts, the rows) per shared key on each side, exact, from the pair
    /// index alone. Refused with [`Error::StaleJoin`] when it is stale.
    /// Writes nothing.
    pub fn join_overlap(&self, join: &str) -> Result<JoinOverlapResult> {
        guard(|| {
            let (pinned, spec, right, rpin) = self.fresh_join("overlap", join, None)?;
            let s = pinned.planner.join_overlap(&spec, &rpin.planner)?;
            Ok(JoinOverlapResult {
                spec,
                root: self.root().to_string(),
                sequence: pinned.sequence(),
                right_root: right.root().to_string(),
                right_sequence: rpin.sequence(),
                both: s.both,
                left_only: s.left_only,
                right_only: s.right_only,
                max_right_row_groups: s.max_right_row_groups,
                sum_right_row_groups: s.sum_right_row_groups,
                max_left_row_groups: s.max_left_row_groups,
                sum_left_row_groups: s.sum_left_row_groups,
                rows: s.rows.map(|r| JoinRows {
                    left: r.left,
                    right: r.right,
                    join: r.join,
                }),
            })
        })
        .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// Whether each declared join key (or only the one `join` names) can be
    /// planned with: fresh, or stale on the left (its row groups changed) or
    /// on the right (it committed again), each with the command that
    /// rebuilds it. `pass` is whether every one is fresh. Writes nothing.
    pub fn join_verify(&self, join: Option<&str>) -> Result<JoinVerifyResult> {
        guard(|| {
            self.require_folder("join verify")?;
            let pinned = self.pin_required("join verify")?;
            let specs: Vec<KeySpec> = match join {
                Some(j) => vec![self.resolve_join(&pinned, j, None)?],
                None => pinned
                    .planner
                    .declared_specs()
                    .iter()
                    .filter(|s| matches!(s, KeySpec::Join { .. }))
                    .cloned()
                    .collect(),
            };
            if specs.is_empty() {
                return Err(Error::NotIndexed {
                    root: self.root().to_string(),
                    operation: "join verify",
                    needs: Some(format!(
                        "join:{}.<columns>=<right>.<columns>",
                        folder_name(self.root())
                    )),
                });
            }
            let mut joins = Vec::with_capacity(specs.len());
            for spec in &specs {
                if !matches!(spec, KeySpec::Join { .. }) {
                    continue;
                }
                self.check_join_scope(spec)?;
                let right = self.right_engine(spec)?;
                let rpin = right.pin_required("join verify")?;
                let mut state = self.join_key_state(&pinned, spec, &right, &rpin)?;
                // Fresh against the right manifest, and the right's files
                // moved on since its index was built: the next semi-join
                // would be refused, so the key is not fresh.
                if matches!(state.state, JoinFreshness::Fresh { .. }) {
                    if let Err(Error::StaleIndex {
                        untracked,
                        changed,
                        missing,
                        message,
                    }) = right.refuse_stale(&rpin, "a semi-join with it")
                    {
                        state.state = JoinFreshness::StaleRightFiles {
                            right_sequence: rpin.sequence(),
                            new: untracked.len() as u64,
                            changed: changed.len() as u64,
                            removed: missing.len() as u64,
                        };
                        state.message = format!(
                            "{}: stale on the right: {message}; then `{}` rebuilds the join key",
                            spec.identity(),
                            self.refresh_command()
                        );
                        state.next = Some(format!(
                            "{} && {}",
                            right.refresh_command(),
                            self.refresh_command()
                        ));
                    }
                }
                joins.push(state);
            }
            Ok(JoinVerifyResult {
                root: self.root().to_string(),
                sequence: pinned.sequence(),
                pass: joins
                    .iter()
                    .all(|j| matches!(j.state, JoinFreshness::Fresh { .. })),
                joins,
            })
        })
    }
}

pub(crate) fn folder_name(root: &str) -> String {
    Path::new(root)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The join key on a semi-join run's pair (B3): the data root's folder and
/// the run's columns, the run's right side; uncounted.
pub(crate) fn join_key(root: &str, columns: &[String], j: &RunJoin) -> KeySpec {
    KeySpec::Join {
        dataset: folder_name(root),
        columns: columns.to_vec(),
        right: j.right.clone(),
        right_columns: j.right_columns.clone(),
        counts: false,
    }
}

/// What `why` prices a semi-join with (B3): the right dataset, pinned, its
/// source, and the transient pair index (its segments) of each join key it
/// prices.
pub(crate) struct WhyPair {
    right: Engine,
    rpin: Pinned,
    source: Box<dyn Source>,
    pairs: BTreeMap<String, Vec<Bytes>>,
}

impl WhyPair {
    /// The right dataset's engine.
    pub(crate) fn right(&self) -> &Engine {
        &self.right
    }

    /// The right dataset's pinned manifest.
    pub(crate) fn rpin(&self) -> &Pinned {
        &self.rpin
    }

    /// The right dataset's source.
    pub(crate) fn source(&self) -> &dyn Source {
        &*self.source
    }

    /// The right side to attach to the left replayer: the right dataset's
    /// own replayer (its current keys; over Parquet, an unserved step is
    /// priced by its files' statistics) and the pair indexes.
    pub(crate) fn input(&self) -> Result<PairInput<'_>> {
        Ok(PairInput {
            right: folder_name(self.right.root()),
            replayer: crate::fix::replayer(&self.right, &*self.source, Some(&self.rpin))?,
            pairs: self.pairs.clone(),
        })
    }
}

impl Engine {
    /// The right side `why --mode semijoin` prices with (B3), `None` for
    /// another mode: the right dataset named by `--join`, and the transient
    /// pair index of the candidate join key on the key columns (the key
    /// file's, or the recorded workload's) and of each declared join key on
    /// the same pair, built from both datasets' complete exact keys; a
    /// missing exact key is refused naming it, and a declared join key
    /// that is stale is refused ([`Error::StaleJoin`]), since the current
    /// configuration could not plan with it.
    pub(crate) fn why_pair(
        &self,
        req: &WhyRequest,
        pinned: Option<&Pinned>,
    ) -> Result<Option<WhyPair>> {
        if req.mode != Some(LookupMode::SemiJoin) {
            return Ok(None);
        }
        let side = req.join.as_deref().ok_or_else(|| Error::InvalidArgument {
            field: "join",
            reason: "a semi-join is priced on both datasets; name the right one with its \
                     columns, <right>.<column>"
                .into(),
        })?;
        let (right, right_columns) = crate::spec::parse_join_side(side)?;
        let columns = match (&req.keys, &req.workload) {
            (Some(k), _) => k.column_names()?,
            (None, Some(h)) => {
                let w = crate::fix::load_workload(self, &self.cache()?, h)?;
                w.runs()
                    .first()
                    .map(|r| r.columns.clone())
                    .unwrap_or_default()
            }
            (None, None) => Vec::new(),
        };
        let run = RunJoin {
            right: right.clone(),
            right_columns: right_columns.clone(),
        };
        let spec = join_key(self.root(), &columns, &run);
        crate::spec::check(&spec).map_err(|e| Error::InvalidArgument {
            field: "join",
            reason: format!(
                "{}; the right side names one column per key column, in the key columns' order \
                 ({right}.<column>,<column>...)",
                match &e {
                    Error::KeySpec(m) => m.clone(),
                    other => other.to_string(),
                }
            ),
        })?;
        self.pair_for(pinned, &spec).map(Some)
    }

    /// The right dataset of the join key `spec` and the transient pair
    /// index of `spec` and of every declared join key on its pair, each
    /// built from both datasets' complete exact keys (a missing one is
    /// refused naming the `bellwether index` command that declares it). A
    /// declared join key on the pair that is not fresh is refused
    /// ([`Error::StaleJoin`]): the current configuration cannot plan with
    /// it, so no price of it would be the plan.
    pub(crate) fn pair_for(&self, pinned: Option<&Pinned>, spec: &KeySpec) -> Result<WhyPair> {
        let KeySpec::Join {
            columns,
            right,
            right_columns,
            ..
        } = spec
        else {
            return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
        };
        let Some(pinned) = pinned else {
            return Err(Error::NotIndexed {
                root: self.root().to_string(),
                operation: "a join key's price",
                needs: Some(bellwether_core::join::side_spec(columns, false).identity()),
            });
        };
        self.check_join_scope(spec)?;
        let r = self.right_engine(spec)?;
        let rpin = r.pin_required("a join key's price")?;
        r.refuse_stale(&rpin, "a join key's price")?;
        let mut priced = vec![spec.clone()];
        for d in &pinned.planner.declared_specs() {
            if let KeySpec::Join {
                columns: c,
                right: rr,
                right_columns: rc,
                ..
            } = d
            {
                if c == columns && rr == right && rc == right_columns {
                    let state = self.join_key_state(pinned, d, &r, &rpin)?;
                    if !matches!(state.state, JoinFreshness::Fresh { .. }) {
                        return Err(self.stale_join(&state));
                    }
                    if !priced.contains(d) {
                        priced.push(d.clone());
                    }
                }
            }
        }
        // Split as the index build splits it, and held within a transient
        // index's cap (ADR 018).
        let caps = self.build_options().pair_caps();
        let max_bytes = bellwether_replay::TransientLimits::default().max_index_bytes;
        let mut pairs = BTreeMap::new();
        for s in priced {
            let built = transient_pair(&pinned.snapshot, &rpin.snapshot, &s, caps, max_bytes)
                .map_err(|e| self.side_hint(&r, e))?;
            pairs.insert(
                s.identity(),
                built.segments.into_iter().map(Bytes::from).collect(),
            );
        }
        let source = r.source(Some(&rpin))?;
        Ok(WhyPair {
            right: r,
            rpin,
            source,
            pairs,
        })
    }
}

/// A join key's `why` column (B3): of the keys the workload asks (over
/// every run, distinct), how many both datasets hold, and the right row
/// groups per key of the whole pair (the most per shared key; the mean per
/// shared key and per left key, a left-only key counting 0), exact, from
/// the transient pair index the candidate was priced with; with gates, the
/// shared keyset against each.
pub(crate) fn join_column(
    replayer: &Replayer<'_>,
    spec: &KeySpec,
    workload: &Workload,
    cache: &Cache<ObjectStoreBackend>,
    gates: &[Gate],
) -> Result<KindColumn> {
    use bellwether_profile::render::thousands;
    let segments = replayer
        .pair_index(spec)
        .ok_or_else(|| Error::Other(format!("no pair index was built for {}", spec.name())))?;
    let readers = segments
        .iter()
        .map(|b| SegmentReader::open(b.clone()))
        .collect::<Result<Vec<_>>>()?;
    let stats = bellwether_core::join::overlap_stats(&readers)?;
    let mut asked: BTreeSet<Vec<u8>> = BTreeSet::new();
    for run in workload.runs() {
        let rk = workload.run_keys(cache, run)?;
        let all: Vec<usize> = (0..rk.columns().len()).collect();
        asked.extend(rk.all_probes(&all));
    }
    let probes: Vec<Vec<u8>> = asked.into_iter().collect();
    let matched = bellwether_core::join::semijoin(&readers, &probes)?.len() as u64;
    #[allow(clippy::cast_precision_loss)]
    let mean = |keys: u64| {
        if keys == 0 {
            0.0
        } else {
            stats.sum_right_row_groups as f64 / keys as f64
        }
    };
    // The cell is short (the asked keys both hold); what it means and the
    // pair's shape are the label, which a report prints once, under the
    // table.
    let mut label = format!(
        "asked keys both datasets hold, of those asked; over the pair {} keys are shared, each \
         in at most {} right row groups (mean {:.2} per shared key, {:.2} per left key, a \
         left-only key counting 0)",
        thousands(stats.both),
        thousands(stats.max_right_row_groups),
        mean(stats.both),
        mean(stats.both + stats.left_only)
    );
    for g in gates {
        let fits = if matched <= g.limit { "within" } else { "over" };
        let _ = write!(
            label,
            "; {} shared keys {fits} {} ({})",
            thousands(matched),
            g.name,
            thousands(g.limit)
        );
    }
    Ok(KindColumn {
        kind: "join".into(),
        label,
        value: format!(
            "{} of {}",
            thousands(matched),
            thousands(probes.len() as u64)
        ),
        exactness: Exactness::Exact,
    })
}
