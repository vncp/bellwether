//! `why`: the dataset from footer statistics and its profile (WP5: fanout
//! per column and composite, the statistics-only lints, a `profile`
//! Evidence), then, when a workload is available, a replay of it (WP6) under
//! the current configuration and under each candidate key, one engine
//! Proposal citing the Evidence and one Replay record per candidate.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use bellwether_core::encoding::check_ts_type;
use bellwether_profile::lints::MembershipUse;
use bellwether_profile::{ProfileOptions, ProfileReport};
use bellwether_replay::{
    proposed_specs, record_bench_workload, servable, serves_query, Baseline, KeyInput, Lookup,
    RecordedRun, Replayer, RunJoin, RunQuery, RunWindow, Workload, WorkloadRecorder,
    QUERY_KIND_LOCATE,
};
use bellwether_store::{Cache, ObjectStoreBackend, StoreExt};
use bellwether_types::{
    format, Error, Evidence, EvidenceBody, Fanout, Gate, Hash, KeySpec, Lint, Proposal,
    ProposalBody, Record, Result, Store, TextOp, TwinOf,
};
use serde::{Deserialize, Serialize};

use bellwether_source_sql::{TableRun, MEASURED_AGGREGATES};

use crate::engine::{folder_changes, guard, root_hint, schema_hint, Engine, Pinned, SnapshotFiles};
use crate::index::{instant_in_unit, no_time_column};
use crate::keys::KeyFile;
use crate::lookback::Lookback;
use crate::results::{total, Cost, LookbackWindow, LookupKind, Written};
use crate::sketch::{CountingSource, ItemCardinality};
use crate::spec::label;
use crate::text::TextQuery;
use crate::twin::{self, TwinCandidate, TwinReport};

/// Who the engine's own candidate proposals are by.
pub const ENGINE_PROPOSER: &str = "engine";

/// Proposer names only the engine writes. `propose` refuses them (compared
/// trimmed and ignoring ASCII case), so a caller cannot pass a proposal off
/// as one of the engine's own candidates.
pub const RESERVED_PROPOSERS: &[&str] = &[ENGINE_PROPOSER];

/// Default cap on the profile's candidates `why` replays beyond the
/// workload's own key columns.
pub const DEFAULT_MAX_CANDIDATES: usize = 8;

/// A time window the runs `why` records look their keys up in
/// (`--window FROM..TO`): both ends inclusive, each an RFC 3339 instant, a
/// date, or an integer in the time column's unit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindow {
    /// The start as given.
    pub from: String,
    /// The end as given.
    pub to: String,
}

/// The share of the row groups a workload's runs span that every priced
/// configuration, the floor included, must read for `why` to name the
/// layout as the culprit ([`CulpritReason::Layout`]). The same threshold as
/// the profile's `no_discriminating_column` lint.
pub const LAYOUT_BOUND_FRACTION: f64 = bellwether_profile::lints::NO_DISCRIMINATING_MIN_FRACTION;

/// Which lookup `why` prices a workload's keys under (`--mode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LookupMode {
    /// Every row holding a key (`bellwether locate`).
    Locate,
    /// The newest version of each key (`bellwether latest`).
    Latest,
    /// The version of each key current at an instant (`bellwether asof`).
    Asof,
    /// The rows whose string column contains one of the values, case-folded
    /// (B5: `contains`, `like '%x%'`; `bellwether locate --contains`).
    Contains,
    /// The rows whose string column holds one of the values as a word,
    /// case-folded (B5: `has`; `bellwether locate --has`).
    HasToken,
    /// The rows whose string column holds a word starting with one of the
    /// values, case-folded (B5: prefix match; `bellwether locate --prefix`).
    HasPrefix,
    /// The rows whose list column holds any of the values (`bellwether
    /// locate --any`: `array_contains`, `has_any`, `exists(unnest)`).
    ContainsAny,
    /// The rows whose list column holds every one of the values
    /// (`bellwether locate --all`).
    ContainsAll,
    /// The rows of the data root (the left dataset) and of the right
    /// dataset named by [`WhyRequest::join`] whose key the other also holds
    /// (B3: a semi-join, `in (subquery)`, `exists`; `bellwether join
    /// semijoin`), priced as two steps.
    #[serde(rename = "semijoin")]
    SemiJoin,
}

impl LookupMode {
    /// The CLI spelling: `locate`, `latest`, `asof`, `contains`,
    /// `has-token`, `has-prefix`, `contains-any`, `contains-all`, or
    /// `semijoin`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Locate => "locate",
            Self::Latest => "latest",
            Self::Asof => "asof",
            Self::Contains => "contains",
            Self::HasToken => "has-token",
            Self::HasPrefix => "has-prefix",
            Self::ContainsAny => "contains-any",
            Self::ContainsAll => "contains-all",
            Self::SemiJoin => "semijoin",
        }
    }

    /// The text match a text mode (`contains`, `has-token`, `has-prefix`)
    /// prices, `None` for the others.
    #[must_use]
    pub fn text_op(self) -> Option<TextOp> {
        match self {
            Self::Contains => Some(TextOp::Contains),
            Self::HasToken => Some(TextOp::HasToken),
            Self::HasPrefix => Some(TextOp::HasPrefix),
            Self::Locate
            | Self::Latest
            | Self::Asof
            | Self::ContainsAny
            | Self::ContainsAll
            | Self::SemiJoin => None,
        }
    }

    /// Whether this prices list membership (`contains-any`,
    /// `contains-all`).
    #[must_use]
    pub fn is_membership(self) -> bool {
        matches!(self, Self::ContainsAny | Self::ContainsAll)
    }

    /// The run query a text or membership mode records (its values are
    /// words, substrings, prefixes, or list elements), `None` for the
    /// others.
    fn value_query(self) -> Option<RunQuery> {
        self.text_op()
            .map(RunQuery::Text)
            .or_else(|| self.membership_query())
    }

    /// The run query a membership mode records, `None` for the others.
    fn membership_query(self) -> Option<RunQuery> {
        match self {
            Self::ContainsAny => Some(RunQuery::ContainsAny),
            Self::ContainsAll => Some(RunQuery::ContainsAll),
            _ => None,
        }
    }
}

/// What `why` looks at. With no workload, `why` reports the dataset and its
/// profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhyRequest {
    /// The keys one run looked up, as a key file. Recorded as a one-run
    /// workload (run id `keys`).
    pub keys: Option<KeyFile>,
    /// With `keys`: further key files, each recorded as one more run of the
    /// same lookup (run ids `keys-2`, `keys-3`, ...): several runs of one
    /// query, so a detector that reads repetition (`R-join-1`) sees it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub more_keys: Vec<KeyFile>,
    /// A recorded workload in the benchmark's format (a directory holding
    /// `workload.json`, or the file itself).
    pub bench_workload: Option<PathBuf>,
    /// A workload recorded earlier, by its hash (it must be in the cache).
    pub workload: Option<Hash>,
    /// The lookup the workload's keys are priced under. `None` infers it:
    /// `asof` when `at` is given, else `latest` when `latest_by` is given,
    /// else `locate` (a recorded workload of text runs is priced as
    /// recorded). `latest` needs `latest_by`; `asof` needs `latest_by` and
    /// `at`; `locate` takes neither, nor `lookback`; `contains`,
    /// `has_token`, and `has_prefix` (B5) price a key file of one string
    /// column's words, substrings, or prefixes as one text run, with the
    /// text keys on the column as the floor candidates, and take none of
    /// them;
    /// `contains_any` and `contains_all` (B4) price a key file of one list
    /// column's element values as membership lookups, with `list:<column>`
    /// as the floor candidate, and take none of them.
    #[serde(default)]
    pub mode: Option<LookupMode>,
    /// Timestamp (or date, time, or integer version) column for temporal
    /// pricing (`latest`, or `asof` when `at` is also given). Records the
    /// key file run with that query kind, adds a temporal floor candidate
    /// `temporal:<run columns>@<latest_by>`, and prices candidates under
    /// both the temporal lookup and point (`locate`) lookups, choosing the
    /// recommendation by the requested temporal lookup.
    #[serde(default)]
    pub latest_by: Option<String>,
    /// Instant for `asof` pricing (with `latest_by`): RFC 3339, a date
    /// (`YYYY-MM-DD`), or an integer in `latest_by`'s unit.
    #[serde(default)]
    pub at: Option<String>,
    /// A lookback window on a `latest` or `asof` lookup: every version of
    /// each key in `[end − lookback, end]`, `end` being `at` (asof) or the
    /// key's newest version (latest). A whole number with a unit, `s`, `m`,
    /// `h`, `d`, or `w` (calendar seconds, minutes, hours, days, weeks;
    /// trading-day calendars are out of scope), converted exactly to
    /// `latest_by`'s unit; a bare integer in that unit for an integer
    /// column.
    #[serde(default)]
    pub lookback: Option<String>,
    /// Record the runs of `keys` or `bench_workload` as window runs: each
    /// looks its keys up in this window, and every cost is the window's
    /// plan (pruned by the time column as `locate --window` is). Prices
    /// `locate` lookups only; a lookback on `latest` or `asof` is
    /// `lookback`.
    pub window: Option<TimeWindow>,
    /// The window's time column; `None` takes the one a window lookup
    /// would ([`crate::Engine::locate_window`]). With `at` and `lookback`
    /// instead of `window`, it is the column `asof --at --lookback --ts`
    /// plans on: the run is recorded as the window `[at − lookback, at]`
    /// on it, measured in its unit, so it must be `latest_by` too
    /// ([`WhyRequest::asof_window_column`]).
    pub ts_column: Option<String>,
    /// Candidate key columns for the profile; `None` selects every id-like
    /// column.
    pub columns: Option<Vec<String>>,
    /// Most profile candidates replayed beyond the workload's own key
    /// columns.
    pub max_candidates: usize,
    /// With a twin (`twin_of`): the gates each candidate's keyset is
    /// compared against (`--gate <name>:<n>`, ADR 033).
    #[serde(default)]
    pub gates: Vec<Gate>,
    /// Diagnose from the index's manifest and its complete `sketch`
    /// sidecars alone: no data file is opened, footers included, and the
    /// report's `sketch` column and the `R-sketch-1` lint carry the
    /// diagnosis (approximate). Needs a complete sketch and a folder whose
    /// files the index still describes; refuses a workload (its keys are
    /// priced by reading key columns) and `columns`.
    #[serde(default)]
    pub sketches_only: bool,
    /// Text lookups (B5), recorded as the workload: one text run each
    /// (`text-1`, `text-2`, ...), priced like any workload under the
    /// current configuration and each candidate text key. Instead of a key
    /// file or a workload, and with no `mode`.
    #[serde(default)]
    pub text: Vec<TextQuery>,
    /// With `mode` `semijoin` (B3): the right dataset and its join columns,
    /// `<right>.<column>[,<column>...]` (`payables.order_id`), pairwise with
    /// the key file's columns. The right dataset is the folder of that name
    /// beside the data root, read through its own index; the candidate is
    /// the join key on the pair, priced from both datasets' complete exact
    /// keys (a missing one is refused, never guessed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<String>,
}

impl Default for WhyRequest {
    fn default() -> Self {
        Self {
            keys: None,
            more_keys: Vec::new(),
            bench_workload: None,
            workload: None,
            mode: None,
            latest_by: None,
            at: None,
            lookback: None,
            window: None,
            ts_column: None,
            columns: None,
            max_candidates: DEFAULT_MAX_CANDIDATES,
            gates: Vec::new(),
            sketches_only: false,
            text: Vec::new(),
            join: None,
        }
    }
}

impl WhyRequest {
    /// The time column named for an `asof` lookback (`ts_column` with `at`
    /// and `lookback` and no `window`): the run is priced as the window
    /// `asof --at --lookback --ts` plans, recorded as a window run.
    #[must_use]
    pub fn asof_window_column(&self) -> Option<&str> {
        match (&self.window, &self.at, &self.lookback, &self.ts_column) {
            (None, Some(_), Some(_), Some(c)) => Some(c),
            _ => None,
        }
    }

    /// The lookup this request prices: `mode`, or the one `at` and
    /// `latest_by` imply. An inconsistent combination (`latest` or `asof`
    /// without `latest_by`, `asof` without `at`, `latest` with `at`,
    /// `locate` with `latest_by`, `at`, or `lookback`, or a `lookback` that
    /// is not a whole number with an optional unit) is refused with one
    /// sentence saying what to pass instead. Needs no schema, so a caller
    /// can tell a usage error from an engine failure before any work.
    pub fn lookup_mode(&self) -> std::result::Result<LookupMode, String> {
        if let Some(l) = &self.lookback {
            Lookback::parse(l).map_err(|e| e.to_string())?;
        }
        let inferred = if self.at.is_some() {
            LookupMode::Asof
        } else if self.latest_by.is_some() {
            LookupMode::Latest
        } else {
            LookupMode::Locate
        };
        if self.join.is_some() && self.mode != Some(LookupMode::SemiJoin) {
            return Err(
                "--join names the right dataset of a semi-join; pass --mode semijoin with it"
                    .into(),
            );
        }
        let Some(mode) = self.mode else {
            return match inferred {
                LookupMode::Asof if self.latest_by.is_none() => Err(
                    "--at requires --latest-by <timestamp column>; pass --latest-by <column> --at \
                     <instant>"
                        .into(),
                ),
                LookupMode::Locate if self.lookback.is_some() => Err(
                    "--lookback windows a latest or asof lookup; pass --latest-by <timestamp \
                     column> (and --at <instant> for asof)"
                        .into(),
                ),
                m => Ok(m),
            };
        };
        match mode {
            LookupMode::SemiJoin if self.join.is_none() => Err(
                "--mode semijoin prices the keys on both datasets of a semi-join; name the right \
                 dataset and its columns with --join <right>.<column> (a folder beside the data \
                 root)"
                    .into(),
            ),
            LookupMode::SemiJoin
                if self.latest_by.is_some()
                    || self.at.is_some()
                    || self.lookback.is_some()
                    || self.window.is_some() =>
            {
                Err(
                    "--mode semijoin prices the rows each dataset keeps for the keys and takes no \
                     --latest-by, --at, --lookback, or --window; drop them"
                        .into(),
                )
            }
            LookupMode::Contains | LookupMode::HasToken | LookupMode::HasPrefix
            | LookupMode::ContainsAny
            | LookupMode::ContainsAll
                if self.latest_by.is_some()
                    || self.at.is_some()
                    || self.lookback.is_some()
                    || self.window.is_some() =>
            {
                Err(format!(
                    "--mode {} prices text lookups or list membership and takes no --latest-by, --at, --lookback, or \
                     --window; drop them, or pass --mode latest or --mode asof to price versions",
                    mode.as_str()
                ))
            }
            LookupMode::Locate
                if self.latest_by.is_some() || self.at.is_some() || self.lookback.is_some() =>
            {
                Err(
                    "--mode locate prices point lookups and takes no --latest-by, --at, or \
                     --lookback; pass --mode latest or --mode asof to price versions"
                        .into(),
                )
            }
            LookupMode::Latest | LookupMode::Asof if self.latest_by.is_none() => Err(format!(
                "--mode {} needs the timestamp column that orders versions; pass --latest-by \
                 <column>",
                mode.as_str()
            )),
            LookupMode::Latest if self.at.is_some() => Err(
                "--at prices the version current at an instant; pass --mode asof (or drop \
                 --mode)"
                    .into(),
            ),
            LookupMode::Asof if self.at.is_none() => Err(
                "--mode asof prices the version current at an instant; pass --at <instant>".into(),
            ),
            m => Ok(m),
        }
    }
}

/// The `profile` Evidence `why` computed. It is written to the ledger when
/// the index location exists or can be created; otherwise it is returned
/// unrecorded, with how to record it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileEvidence {
    /// Whether the Evidence is in the ledger.
    pub recorded: bool,
    /// Its hash in the ledger, when recorded.
    pub hash: Option<Hash>,
    /// The Evidence, exactly as written when recorded.
    pub evidence: Evidence,
    /// When not recorded: one line saying how to record it.
    pub note: Option<String>,
}

/// The recorded workload `why` replayed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadInfo {
    /// The workload hash; `fix` takes it.
    pub hash: Hash,
    /// Recorded runs.
    pub runs: u64,
    /// Query kind the workload's runs were priced under (`locate`, `latest`,
    /// or `asof`).
    pub query_kind: String,
    /// When `why` priced a temporal lookup (`--latest-by` or a recorded
    /// `latest`/`asof` workload): [`LookupKind::Latest`] or
    /// [`LookupKind::Asof`]. Absent (`None`, omitted in JSON) for point-only
    /// (`locate`) workloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookup: Option<LookupKind>,
    /// The lookback window the runs were priced with (`--lookback`, or a
    /// recorded window workload). Absent (`None`, omitted in JSON) for
    /// lookups of one version per key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<LookbackWindow>,
    /// The distinct key-column sets the runs look up by.
    pub columns: Vec<Vec<String>>,
    /// Distinct keys per run at nearest-rank p50, p95, max.
    pub keys_p50_p95_max: [u64; 3],
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
    /// The distinct windows of its window runs, in the time column's unit
    /// (empty when every run is a plain key lookup).
    pub windows: Vec<RunWindow>,
}

/// The whole dataset, from footer statistics: the replay crate's catalog
/// plus, in an indexed folder, any file the index does not track yet.
/// Under [`WhyReport::from_sketches`] no footer is read: files and live row
/// groups are the manifest's, rows the sketch's exact row counts, and
/// `bytes` the file sizes the manifest recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dataset {
    /// Files.
    pub files: u64,
    /// Live row groups.
    pub row_groups: u64,
    /// Rows.
    pub rows: u64,
    /// Compressed bytes of every live row group (file sizes under
    /// `from_sketches`).
    pub bytes: u64,
}

/// Where a candidate came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateOrigin {
    /// An exact key on the workload's own key columns: it selects exactly
    /// the row groups that hold a queried key, the least any row-group
    /// index can read for these runs (the floor).
    Workload,
    /// A temporal key on a window run's key columns and its window's time
    /// column: its version chains keep only the row groups holding a
    /// version of a key inside the window.
    Window,
    /// A column or composite the profile measured that serves the workload.
    Profile,
}

/// One candidate key, replayed over the workload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    /// The key.
    pub spec: KeySpec,
    /// Where it came from.
    pub origin: CandidateOrigin,
    /// Summed over runs under the workload's recorded query kind (`locate`
    /// by default, or `latest`/`asof` when `--latest-by` is given): what the
    /// workload would read under the current configuration with this key
    /// added.
    pub proposed: Cost,
    /// When the workload is temporal (`latest` or `asof`): what the same
    /// key configuration would read for point (`locate`) lookups over the
    /// same runs. Absent (`None`, omitted in JSON) when `proposed` is
    /// already `locate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locate: Option<Cost>,
    /// The engine's Proposal for it, citing the profile Evidence.
    pub proposal: Written,
    /// The Replay scoring that Proposal (`inputs`: the catalog digest; a
    /// key replay is exact, so no `model`; contract v2.1).
    pub replay: Written,
    /// This candidate's index-kind column (`bellwether_types::KindColumn`),
    /// when its kind contributes one; absent (omitted in JSON) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind_column: Option<bellwether_types::KindColumn>,
}

/// The candidate that reads the least under the requested lookup kind
/// (`locate` by default, or `latest`/`asof` when `--latest-by` is given).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Recommendation {
    /// The key.
    pub spec: KeySpec,
    /// Its Proposal; pass it to `fix`.
    pub proposal: Hash,
    /// Its Replay.
    pub replay: Hash,
    /// Which lookup kind (`locate`, `latest`, or `asof`) `current` and
    /// `proposed` are priced for and by which this recommendation was chosen.
    pub lookup: LookupKind,
    /// The lookback window of a `latest` or `asof` lookup, when priced with
    /// `--lookback`. Absent (`None`, omitted in JSON) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<LookbackWindow>,
    /// Summed over runs, under the current configuration.
    pub current: Cost,
    /// Summed over runs, with this key.
    pub proposed: Cost,
}

/// Why columns were named the culprit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CulpritReason {
    /// The profile raised a lint on a column the workload or the current
    /// index relies on.
    Lint {
        /// Rule id, for example `batch_stamp_key`.
        rule: String,
        /// Confidence in `[0, 1]` per the rule catalog.
        confidence: f64,
        /// The lint's one-sentence message.
        message: String,
    },
    /// No current key spec serves the workload's key columns, so every run
    /// reads every row group.
    NoIndexServes,
    /// Every priced configuration, the floor included (an exact key on the
    /// workload's own columns), reads at least
    /// [`LAYOUT_BOUND_FRACTION`] of the row groups the runs span: each
    /// value is spread over nearly every row group, so the layout, not a
    /// missing index, is the cause, and no index helps.
    Layout {
        /// The fewest row groups any priced configuration reads, summed
        /// over runs.
        least_row_groups: u64,
        /// The row groups the runs span: runs times the indexed row
        /// groups.
        row_groups: u64,
        /// The measured single column with the lowest median row groups
        /// per value (ties: lower p95), and its fanout; `None` when the
        /// profile measured no single column.
        lowest_fanout: Option<Fanout>,
    },
    /// As [`CulpritReason::Layout`], for a workload of list-membership runs
    /// (B4): every priced configuration, the list key included, reads at
    /// least [`LAYOUT_BOUND_FRACTION`] of the row groups the runs span, and
    /// `values` says why: the row groups (and files) holding each queried
    /// value present in the data, exact, from the list key.
    ListLayout {
        /// The fewest row groups any priced configuration reads, summed
        /// over runs.
        least_row_groups: u64,
        /// The row groups the runs span.
        row_groups: u64,
        /// Per queried value present in the data (`distinct` of them, over
        /// every run): files and row groups holding it, p50 / p95 / max.
        values: Fanout,
    },
    /// A current key spec serves the workload; its columns bound what it
    /// prunes.
    ServingSpec {
        /// The serving spec.
        spec: KeySpec,
    },
    /// The workload's text runs (B5), per match they ask for on the
    /// culprit's column: the complete text keys that serve it, or none (its
    /// runs then read every row group). A text lookup has no value fanout:
    /// what it reads depends on the words or substrings, not on one value.
    Text {
        /// One entry per match the runs ask for, in `contains`,
        /// `has_token`, `has_prefix` order.
        matches: Vec<TextMatch>,
    },
}

/// One text match of a workload's runs on one column, and what serves it
/// ([`CulpritReason::Text`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextMatch {
    /// The match.
    pub operator: TextOp,
    /// Runs asking for it.
    pub runs: u64,
    /// The complete text keys on the column that serve it; empty when none
    /// does.
    pub served_by: Vec<KeySpec>,
}

/// The columns behind the read, with their measured fanout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Culprit {
    /// Columns.
    pub columns: Vec<String>,
    /// Their fanout, when the profile measured this tuple.
    pub fanout: Option<Fanout>,
    /// Why.
    pub reason: CulpritReason,
}

/// What [`Engine::why`] found.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WhyReport {
    /// The canonical data root.
    pub root: String,
    /// The manifest sequence, `0` for a folder never indexed.
    pub sequence: u64,
    /// The current configuration: declared specs complete at `sequence`.
    pub current_specs: Vec<KeySpec>,
    /// The workload replayed, if any.
    pub workload: Option<WorkloadInfo>,
    /// The `profile` Evidence.
    pub evidence: ProfileEvidence,
    /// Everything the profile measured, including what the Evidence's types
    /// cannot carry (sampling detail, ranges, time scatter, memory).
    pub profile: ProfileReport,
    /// The dataset, untracked files included.
    pub dataset: Dataset,
    /// Files under the root that the index does not track yet (they landed
    /// after the last build). They are counted in `dataset`; the costs below
    /// cover the files the index tracks, so run `bellwether index` first.
    pub untracked: Vec<String>,
    /// Summed over runs under the workload's query kind, under the current
    /// configuration (exact). Present with a workload. Over Parquet files,
    /// a run no current spec serves is priced as the files' own min/max
    /// statistics (and embedded bloom filters) read it, as the Replays say
    /// in `unserved`: what a reader without an index skips to.
    pub current: Option<Cost>,
    /// When the workload is temporal (`latest` or `asof`): summed over runs
    /// for point (`locate`) lookups under the current configuration. Absent
    /// (`None`, omitted in JSON) when `current` is already `locate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_locate: Option<Cost>,
    /// With a workload, when `current` prices the runs by the files'
    /// min/max statistics (no key spec is current, over Parquet files):
    /// summed over runs, what a reader that skips nothing reads, every live
    /// row group (replay's `Baseline::RawScan`). Context only; nothing is
    /// measured against it. Absent (`None`, omitted in JSON) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_scan: Option<Cost>,
    /// With a workload over Parquet files whose footers carry min/max
    /// statistics on some key column of the runs: `"parquet_stats"`, the
    /// pricing of runs no current key serves (the Replays' `unserved`), so
    /// `current` is what a reader skips to by those statistics. Absent
    /// (`None`, omitted in JSON) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unserved: Option<String>,
    /// With a workload over Parquet files whose footers carry no min/max
    /// statistics on any key column of the runs: nothing but an index can
    /// skip a row group for them. Omitted in JSON when false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_statistics: bool,
    /// Candidates replayed: the floor key(s) on each run's key columns, then
    /// the profile's measured candidates that serve a run in the profile's
    /// rank order, minus specs already current.
    pub candidates: Vec<Candidate>,
    /// Candidate keys the profile measured that serve no run of the
    /// workload, so they cannot be replayed against it.
    pub not_serving: Vec<KeySpec>,
    /// The candidate reading the fewest bytes, if any reads fewer than the
    /// current configuration.
    pub recommended: Option<Recommendation>,
    /// The culprit, with a workload.
    pub culprit: Option<Culprit>,
    /// Whether every byte count above is a sum of footer statistics
    /// (`true`, omitted in JSON), or measured (`false`): over a table (ADR
    /// 027) the bytes are the logical bytes the engine's aggregates report,
    /// so the dataset, every cost, and every candidate's Replay are
    /// measured, `model` names how, and no Replay is recorded (the ledger
    /// keeps only exact current costs, ADR 005). Counts of units and rows
    /// are exact either way.
    #[serde(default = "exact_default", skip_serializing_if = "is_exact")]
    pub exact: bool,
    /// The cost model when `exact` is false: `measured: engine plan
    /// statistics`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Each table read (ADR 027): its units, rows, and the snapshot held or
    /// the identity re-read that stood in for one. Omitted for a folder.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<TableRun>,
    /// With `twin_of` (ADR 033): the diagnosis is on the lake twin of a
    /// query engine; its header line, the engine's identity beside the
    /// lake's, and per candidate the fanout, files per key, and keyset
    /// against each gate. Omitted otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub twin: Option<TwinReport>,
    /// The `sketch` column: for each item of the index's complete `sketch`
    /// sidecars, its approximate distinct count with its bound, fanout
    /// (`rows_per_value`), and files per value, read from the sketch's
    /// segments (no data file). Labelled approximate; `exact_distinct` is
    /// set where a complete `exact` key spec on the same columns holds the
    /// exact count. Empty without a complete sketch. No plan reads it.
    #[serde(default)]
    pub sketch: Vec<ItemCardinality>,
    /// What this report read for its diagnosis: data pages, footers, and
    /// exact-index segments.
    #[serde(default)]
    pub reads: DiagnosisReads,
    /// Whether the diagnosis came from the manifest and sketches alone
    /// ([`WhyRequest::sketches_only`]): no data file was opened, footers
    /// included, so the profile measured nothing (its fanout table, ranges,
    /// and footer lints are empty) and `dataset` is the manifest's.
    #[serde(default)]
    pub from_sketches: bool,
    /// Why the `sketch` column is empty although the index declares a
    /// sketch (a missing or damaged sketch segment), with the command that
    /// rebuilds it. Absent (omitted in JSON) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sketch_unavailable: Option<String>,
}

fn exact_default() -> bool {
    true
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_exact(e: &bool) -> bool {
    *e
}

/// What a `why` read: row groups read through the source (the profile's
/// key-read pass, a key file's replay) and the distinct files they belong
/// to; the files whose footer statistics were read from the source; and
/// the exact-index segments scanned for exact distinct counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosisReads {
    /// Files whose data pages were read.
    pub files: u64,
    /// Row groups read.
    pub row_groups: u64,
    /// Files whose footer (row-group statistics) was read from the source,
    /// each at most once: the replay catalog's pass over the dataset and
    /// any footer the profile's cache did not hold. 0 under
    /// [`WhyReport::from_sketches`].
    #[serde(default)]
    pub footers: u64,
    /// Segments of complete `exact` key specs read to give the `sketch`
    /// column exact distinct counts: each such count scans every key of the
    /// spec's index. Index segments, not data.
    #[serde(default)]
    pub exact_index_segments: u64,
}

/// What `why` takes from the index's sketches.
#[derive(Default)]
struct SketchInputs {
    /// The `sketch` column.
    column: Vec<ItemCardinality>,
    /// The `R-sketch-1` lints.
    lints: Vec<bellwether_types::Lint>,
    /// Why a declared sketch could not be read, with how to rebuild it.
    unavailable: Option<String>,
    /// Exact-index segments read for exact distinct counts.
    exact_index_segments: u64,
}

/// What `why`'s profile measures from.
#[derive(Clone, Copy)]
enum Measure<'a> {
    /// The source: footers, then the key-read pass.
    Source(&'a dyn bellwether_types::Source),
    /// Nothing: totals the manifest and the sketches already hold.
    Manifest(bellwether_profile::DatasetTotals),
}

impl Engine {
    /// Explains what the data's layout costs a lookup.
    ///
    /// 1. **Footer statistics**: files, live row groups, rows, and bytes,
    ///    through the replay crate's catalog (the index's manifest when one
    ///    exists, else discovery). Works on a folder with no index and no
    ///    workload.
    /// 2. **Profile** (WP5), always: fanout per candidate column and
    ///    composite, range statistics, and the statistics-only lints, as a
    ///    `profile` Evidence (invariant 6). It is written to the ledger when
    ///    the index location exists or can be created; otherwise (a
    ///    read-only folder and no `--index`) the profile is computed in
    ///    memory and the Evidence is returned with `recorded: false` and one
    ///    line naming `--index`. A bench workload's key columns are the
    ///    profile's supplied columns.
    /// 3. **Replay** (WP6), when a workload is given (a key file, a bench
    ///    workload directory, or a recorded workload hash): every run under
    ///    the current configuration and under each candidate exact key,
    ///    each candidate written as an engine Proposal citing the Evidence
    ///    and scored by a Replay record computed immediately before it is
    ///    written. Candidates are the exact key on each run's own key
    ///    columns (the floor); for the columns of `count` runs, the exact
    ///    key on them storing counts (`exact:<cols>,counts`, which answers a
    ///    count from the index with no data file read), unless a current
    ///    spec already counts them; then the profile's measured candidates
    ///    that serve a run, at most `max_candidates`, minus specs already
    ///    current. A workload needs a writable index location: Proposals
    ///    cite a recorded Evidence.
    ///
    /// The **culprit** is, in order: with a workload, the layout, when
    /// every priced configuration, the floor included, reads at least
    /// [`LAYOUT_BOUND_FRACTION`] of the row groups the runs span (it names
    /// the lowest-fanout measured column); else the highest-confidence
    /// `batch_stamp_key` or `low_selectivity_single_column` lint on a column
    /// the workload or a current spec uses; else, with a workload, its key
    /// columns when no current spec serves them, or the serving current
    /// spec's columns; each with its measured fanout.
    ///
    /// **Recommendation rule.** The `recommended` key is chosen by the
    /// workload's requested lookup kind: `locate` by default, or `latest` /
    /// `asof` when `--latest-by` is given (point-lookup costs are still
    /// reported in [`WhyReport::current_locate`] and [`Candidate::locate`]).
    /// Among all candidates it selects the one reading the fewest bytes over
    /// all runs under that lookup kind (ties broken by fewer key columns,
    /// then candidate order), and recommends it only if it reads strictly
    /// fewer bytes than the current configuration under that lookup kind,
    /// as priced by the candidate's own Replay (over Parquet files, a run no
    /// spec serves read as the files' min/max statistics read it), so the
    /// number is the one `fix` gates on and a receipt cites.
    pub fn why(&self, request: &WhyRequest) -> Result<WhyReport> {
        guard(|| self.why_inner(request)).map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    #[allow(clippy::too_many_lines)]
    fn why_inner(&self, req: &WhyRequest) -> Result<WhyReport> {
        let has_workload = self.check_why_request(req)?;
        let pinned = self.pin()?;
        if req.sketches_only {
            return self.why_from_sketches(req, pinned.as_ref());
        }
        let SketchInputs {
            column: sketch,
            lints: sketch_lints,
            unavailable: sketch_unavailable,
            exact_index_segments,
        } = self.sketch_inputs(false, pinned.as_ref())?;
        let folder = self.source(pinned.as_ref())?;
        // Pinned to an earlier manifest: the dataset is exactly the files
        // that manifest recorded, for the profile as for the replay.
        let source: Box<dyn bellwether_types::Source> = match (&pinned, self.options().at_manifest)
        {
            (Some(p), Some(_)) => Box::new(SnapshotFiles::new(folder, p.manifest())),
            _ => folder,
        };
        let counting = CountingSource::new(&*source);
        let source: &dyn bellwether_types::Source = &counting;
        let sequence = pinned.as_ref().map_or(0, Pinned::sequence);
        let current_specs = pinned.as_ref().map(|p| p.specs().0).unwrap_or_default();
        // A semi-join (B3) is priced on both datasets: the right one's
        // replayer and the candidate join key's transient pair index.
        let why_pair = self.why_pair(req, pinned.as_ref())?;
        let replayer = crate::fix::replayer(self, source, pinned.as_ref())?;
        let replayer = match &why_pair {
            Some(p) => replayer.with_pair(p.input()?),
            None => replayer,
        };
        let resolved_temporal = resolve_temporal(req, replayer.schema())?;
        let catalog = replayer.catalog();
        let indexed_row_groups = catalog.len() as u64;
        let (dataset, untracked) = self.dataset(catalog, pinned.as_ref(), source)?;
        // The lake twin of a query engine (ADR 033): the engine's identity
        // now, and, with a key sample, the twin pass, before the profile
        // Evidence is written so the Evidence records both.
        let mut twin_of: Option<TwinOf> = if self.options().twin_of.is_some() {
            let engine = self.twin_adapter(None)?;
            let identity = Engine::twin_identity(&*engine)?;
            Some(twin::twin_of(
                &*engine,
                &identity,
                sequence,
                dataset.rows,
                &req.gates,
            ))
        } else {
            None
        };
        if twin_of.is_none() && !req.gates.is_empty() {
            return Err(Error::InvalidArgument {
                field: "gates",
                reason: "a gate compares the key sample with a query engine's limit; name the \
                         engine the folder copies with --twin-of"
                    .into(),
            });
        }
        let mut recorded = None;
        let mut twin_pass = None;
        let mut twin_lints = Vec::new();
        let mut resolved_temporal = resolved_temporal;
        // The workload is recorded before the profile, so the detectors
        // that read its runs (the twin pass, `R-text-1`) land in the profile
        // Evidence.
        if has_workload {
            let cache = self.cache()?;
            let (w, temporal) = self.record_workload(
                req,
                pinned.as_ref(),
                &replayer,
                &cache,
                resolved_temporal.take(),
            )?;
            if let Some(of) = &mut twin_of {
                let p = twin::pass(source, replayer.catalog(), &w, &cache, &req.gates)?;
                twin::predict(of, &p, w.hash());
                twin_lints = twin::lints(
                    &p,
                    &req.gates,
                    temporal.is_some(),
                    of.engine_rows == of.lake_rows,
                );
                twin_pass = Some(p);
            }
            recorded = Some((w, temporal));
        }
        // The text detector (B5) reads the workload's runs, which the
        // profile does not; its lints ride in the same Evidence, which then
        // cites the workload they were read from.
        let text_lints = match &recorded {
            Some((w, _)) => {
                bellwether_profile::lints::text_lookups(&crate::text::text_uses(w)?, &current_specs)
            }
            None => Vec::new(),
        };
        let text_input = match &recorded {
            Some((w, _)) if !text_lints.is_empty() => Some(w.hash()),
            _ => None,
        };
        // The list-membership detector (B4) likewise, citing the key set or
        // workload it read.
        let membership = self.membership_lints(req, &replayer, &current_specs)?;
        // The repeated semi-join detector (B3) likewise, citing the
        // workload it read.
        let join_lints = match &recorded {
            Some((w, _)) => bellwether_profile::lints::repeated_semijoins(
                &semijoin_uses(w, &self.cache()?)?,
                &crate::join::folder_name(self.root()),
                &current_specs,
            ),
            None => Vec::new(),
        };
        let join_input = match &recorded {
            Some((w, _)) if !join_lints.is_empty() && text_input.is_none() => Some(w.hash()),
            _ => None,
        };
        let mut extra_lints = sketch_lints;
        extra_lints.extend(text_lints);
        extra_lints.extend(membership.lints);
        extra_lints.extend(join_lints);
        let mut cites: Vec<Hash> = text_input.into_iter().chain(join_input).collect();
        cites.extend(membership.inputs);
        let (profile, evidence) = self.profile(
            req,
            Measure::Source(source),
            pinned.as_ref(),
            has_workload,
            twin_of.clone(),
            &twin_lints,
            extra_lints,
            cites,
        )?;
        let mut report = WhyReport {
            root: self.root().to_string(),
            sequence,
            current_specs: current_specs.clone(),
            workload: None,
            evidence,
            profile,
            dataset,
            untracked,
            current: None,
            current_locate: None,
            full_scan: None,
            unserved: None,
            no_statistics: false,
            candidates: Vec::new(),
            not_serving: Vec::new(),
            recommended: None,
            culprit: None,
            exact: true,
            model: None,
            tables: Vec::new(),
            twin: None,
            sketch,
            reads: DiagnosisReads::default(),
            from_sketches: false,
            sketch_unavailable,
        };
        let workload = if has_workload {
            let cache = self.cache()?;
            let (workload, temporal) = match recorded {
                Some(r) => r,
                None => self.record_workload(
                    req,
                    pinned.as_ref(),
                    &replayer,
                    &cache,
                    resolved_temporal,
                )?,
            };
            self.replay_candidates(
                &mut report,
                &replayer,
                &workload,
                &cache,
                req,
                temporal.as_ref(),
            )?;
            let spread = queried_spread(&replayer, &workload, &cache)?;
            Some((workload, spread))
        } else {
            None
        };
        let (workload, spread) = workload.map_or((None, None), |(w, s)| (Some(w), s));
        let text_runs = match &workload {
            Some(w) => is_text_workload(w)?,
            None => false,
        };
        report.culprit = match workload.as_ref() {
            Some(w) if text_runs => text_culprit(&report.current_specs, w)?,
            _ => workload
                .as_ref()
                .and_then(|w| layout_culprit(&report, w, indexed_row_groups, spread))
                .or_else(|| culprit(&report.current_specs, &report.profile, workload.as_ref())),
        };
        if self.is_table() {
            report.exact = false;
            report.model = Some(MEASURED_AGGREGATES.to_string());
            report.tables = self.finish_tables()?;
        }
        if let Some(of) = twin_of {
            report.twin = Some(TwinReport {
                note: twin::note(&of),
                key: twin_pass.as_ref().and_then(|p| p.sets.get(&p.key).cloned()),
                candidates: report
                    .candidates
                    .iter()
                    .map(|c| TwinCandidate {
                        spec: c.spec.clone(),
                        twin: twin_pass
                            .as_ref()
                            .and_then(|p| twin::measured(p, c.spec.columns())),
                        kind_column: c.kind_column.clone(),
                    })
                    .collect(),
                lints: twin_lints,
                row_groups_read: twin_pass.as_ref().map_or(0, |p| p.row_groups_read),
                rows_read: twin_pass.as_ref().map_or(0, |p| p.rows_read),
                of,
            });
        }
        let read = counting.reads();
        report.reads = DiagnosisReads {
            files: read.files,
            row_groups: read.row_groups,
            footers: read.footers,
            exact_index_segments,
        };
        Ok(report)
    }

    /// The whole dataset from the replay catalog's footer statistics, plus,
    /// in an indexed folder not pinned to an earlier manifest, every file
    /// the index does not track yet (read through `source`, so counted),
    /// with those files' paths.
    fn dataset(
        &self,
        catalog: &bellwether_replay::Catalog,
        pinned: Option<&Pinned>,
        source: &dyn bellwether_types::Source,
    ) -> Result<(Dataset, Vec<String>)> {
        let mut dataset = Dataset {
            files: catalog.files().len() as u64,
            row_groups: catalog.len() as u64,
            rows: catalog.rows_total(),
            bytes: catalog.bytes_total(),
        };
        let mut untracked = Vec::new();
        if let Some(p) = pinned.filter(|_| self.options().at_manifest.is_none()) {
            for fp in folder_changes(self.root(), p, self.format(), &self.excluded())?.untracked {
                for rg in source.row_groups(&fp)? {
                    dataset.row_groups += 1;
                    dataset.rows += rg.rows;
                    dataset.bytes += rg.bytes;
                }
                dataset.files += 1;
                untracked.push(fp.path);
            }
        }
        Ok((dataset, untracked))
    }

    /// `why --sketches-only`: the diagnosis from the index's manifest and
    /// its sketch segments alone. No data file is opened, footers
    /// included: the dataset's files and live row groups are the
    /// manifest's, its rows the sketch's exact row counts, its bytes the
    /// file sizes the manifest recorded; the profile measures nothing (no
    /// fanout, ranges, or footer lints) and its lints are the sketch
    /// detector's. Refused on a table, without an index or a readable
    /// complete sketch (naming a declaration over the dataset's id-like
    /// columns), and over files that changed since the index was built (the
    /// sketch would describe other files).
    fn why_from_sketches(&self, req: &WhyRequest, pinned: Option<&Pinned>) -> Result<WhyReport> {
        const VERB: &str = "why --sketches-only";
        self.require_folder(VERB)?;
        let Some(p) = pinned else {
            return Err(Error::NotIndexed {
                root: self.root().to_string(),
                operation: VERB,
                needs: Some(self.sketch_declaration(None)?),
            });
        };
        self.refuse_stale(p, VERB)?;
        let SketchInputs {
            column,
            lints,
            exact_index_segments,
            ..
        } = self.sketch_inputs(true, Some(p))?;
        let files = &p.manifest().rowgroup_space.files;
        let totals = bellwether_profile::DatasetTotals {
            files: files.len() as u64,
            row_groups: files.iter().map(|f| f.rowgroup_ids.len() as u64).sum(),
            // Every item of a complete sketch saw every row: sketched rows
            // plus those whose leading column is null.
            rows: column
                .first()
                .map_or(0, |i| i.cardinality.rows + i.cardinality.nulls),
        };
        let dataset = Dataset {
            files: totals.files,
            row_groups: totals.row_groups,
            rows: totals.rows,
            bytes: files.iter().map(|f| f.fingerprint.size).sum(),
        };
        let (profile, evidence) = self.profile(
            req,
            Measure::Manifest(totals),
            Some(p),
            false,
            None,
            &[],
            lints,
            Vec::new(),
        )?;
        let current_specs = p.specs().0;
        let culprit = culprit(&current_specs, &profile, None);
        Ok(WhyReport {
            root: self.root().to_string(),
            sequence: p.sequence(),
            current_specs,
            workload: None,
            evidence,
            profile,
            dataset,
            untracked: Vec::new(),
            current: None,
            current_locate: None,
            full_scan: None,
            unserved: None,
            no_statistics: false,
            candidates: Vec::new(),
            not_serving: Vec::new(),
            recommended: None,
            culprit,
            exact: true,
            model: None,
            tables: Vec::new(),
            twin: None,
            sketch: column,
            reads: DiagnosisReads {
                exact_index_segments,
                ..DiagnosisReads::default()
            },
            from_sketches: true,
            sketch_unavailable: None,
        })
    }

    /// The `sketch` column and the `R-sketch-1` lints from the index's
    /// complete sketches (empty without an index), and, when a declared
    /// sketch cannot be read (a missing or damaged segment), why not: the
    /// planner never needs a sketch, so `why` goes on without the column
    /// and says how to rebuild it. With `required` (`--sketches-only`) an
    /// unreadable sketch is an error, and no complete sketch is refused,
    /// naming a declaration over the dataset's id-like columns.
    fn sketch_inputs(&self, required: bool, pinned: Option<&Pinned>) -> Result<SketchInputs> {
        let Some(p) = pinned else {
            return Ok(SketchInputs::default());
        };
        let read = crate::sketch::fold_complete_sketches(p).and_then(|sketches| {
            if required && sketches.is_empty() {
                return Err(Error::NotIndexed {
                    root: self.root().to_string(),
                    operation: "why --sketches-only",
                    needs: Some(self.sketch_declaration(Some(p))?),
                });
            }
            sketch_view(p, &sketches)
        });
        match read {
            Ok(inputs) => Ok(inputs),
            Err(e @ Error::NotIndexed { .. }) if required => Err(e),
            Err(e) => {
                let why_not = self.sketch_unavailable(p, &e);
                if required {
                    return Err(Error::Other(why_not));
                }
                Ok(SketchInputs {
                    unavailable: Some(why_not),
                    ..SketchInputs::default()
                })
            }
        }
    }
}

/// The `sketch` kind's funnel column for a candidate on `columns`: the
/// fanout (rows per value) of the sketched item on exactly those columns,
/// `≈` and labelled approximate (`Measured` by the sketch's estimator), or
/// exact where a complete exact key spec holds the distinct count. `None`
/// when no complete sketch holds that item.
fn sketch_column(
    sketch: &[ItemCardinality],
    columns: &[String],
) -> Option<bellwether_types::KindColumn> {
    let item = sketch.iter().find(|i| i.cardinality.columns == columns)?;
    let c = &item.cardinality;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let fanout = c.rows_per_value.round() as u64;
    let (value, exactness) = if c.rows_per_value_exact {
        (fanout.to_string(), bellwether_types::Exactness::Exact)
    } else {
        (
            format!("≈{fanout}"),
            bellwether_types::Exactness::Measured {
                model: "sketch.hll".into(),
            },
        )
    };
    Some(bellwether_types::KindColumn {
        kind: "sketch".into(),
        label: if c.rows_per_value_exact {
            "rows per value (exact distinct count)".into()
        } else {
            format!(
                "rows per value, approximate (±{:.1}% distinct)",
                300.0 * c.relative_standard_error
            )
        },
        value,
        exactness,
    })
}

/// The `sketch` column and the `R-sketch-1` lints from the folded complete
/// sketches: one entry per distinct item (the finest sketch holding it),
/// and the low-cardinality-key detector over each sketch's single-column
/// items. Reads sketch and exact-key segments, no data: an item's exact
/// distinct count, where a complete exact spec holds it, scans that spec's
/// segments once, and the segments read are counted.
fn sketch_view(pinned: &Pinned, folded: &[crate::sketch::Folded]) -> Result<SketchInputs> {
    let schema = pinned.planner.schema();
    let mut column = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut lints: Vec<bellwether_types::Lint> = Vec::new();
    let mut exact_of: std::collections::BTreeMap<&[String], Option<u64>> =
        std::collections::BTreeMap::new();
    let mut exact_index_segments = 0;
    for f in folded {
        let mut singles = Vec::new();
        for (i, item) in f.spec.items.iter().enumerate() {
            let label = bellwether_types::SketchSpec::item_label(item);
            let exact = if let Some(e) = exact_of.get(item.as_slice()) {
                *e
            } else {
                let (e, segments) = crate::sketch::exact_distinct(pinned, item)?;
                exact_index_segments += segments;
                exact_of.insert(item.as_slice(), e);
                e
            };
            let card = f.totals.cardinality(i, exact)?;
            if item.len() == 1 {
                singles.push((i, card.clone()));
            }
            if seen.insert(label.clone()) {
                if let Some((best, bi)) = crate::sketch::best_sketch(folded, &label) {
                    let card = if std::ptr::eq(best, f) {
                        card
                    } else {
                        best.totals.cardinality(bi, exact)?
                    };
                    column.push(ItemCardinality {
                        source: crate::sketch::SketchSource::Index,
                        sketch: Some(best.spec.label()),
                        cardinality: card,
                    });
                }
            }
        }
        let cols: Vec<bellwether_profile::lints::SketchedColumn> = singles
            .iter()
            .map(|(i, c)| {
                Ok(bellwether_profile::lints::SketchedColumn {
                    column: c.item.clone(),
                    data_type: schema
                        .field_with_name(&c.item)
                        .map_err(|e| Error::KeySpec(e.to_string()))?
                        .data_type()
                        .clone(),
                    rows: c.rows,
                    distinct: c.exact_distinct.unwrap_or(c.distinct),
                    distinct_exact: c.exact_distinct.is_some(),
                    files_present: f.totals.copresent(*i, *i),
                    copresent: singles
                        .iter()
                        .filter(|(j, _)| j != i)
                        .map(|(j, o)| (o.item.clone(), f.totals.copresent(*i, *j)))
                        .collect(),
                })
            })
            .collect::<Result<_>>()?;
        for l in bellwether_profile::lints::low_cardinality_key(&cols) {
            if !lints.iter().any(|x| x.columns.first() == l.columns.first()) {
                lints.push(l);
            }
        }
    }
    Ok(SketchInputs {
        column,
        lints,
        unavailable: None,
        exact_index_segments,
    })
}

impl Engine {
    /// Refuses text lookups (`req.text`) given with a lookup mode, a
    /// timestamp column, an instant, a lookback, or a window, on a column the
    /// dataset lacks, or with an argument their match refuses; before any
    /// other check, so the refusal states this cause.
    fn check_text_request(&self, req: &WhyRequest) -> Result<()> {
        if !req.text.is_empty() {
            if req.mode.is_some()
                || req.latest_by.is_some()
                || req.at.is_some()
                || req.lookback.is_some()
                || req.window.is_some()
            {
                return Err(Error::InvalidArgument {
                    field: "text",
                    reason: "text lookups say their own match and take no lookup mode, \
                         timestamp column, instant, lookback, or window; drop those"
                        .into(),
                });
            }
            self.check_columns(&req.text.iter().map(|q| &q.column).collect::<Vec<_>>())?;
            for q in &req.text {
                bellwether_core::text::check_arg(q.op, &q.value)?;
            }
        }
        Ok(())
    }

    /// The `R-list-1` lints (B4) for the request's membership runs: a key
    /// file priced with `--mode contains-any|contains-all`, or a recorded
    /// workload's membership runs, on a list column no complete list key in
    /// `current` serves. The key set (stored in the cache) or the workload
    /// is the lints' input.
    fn membership_lints(
        &self,
        req: &WhyRequest,
        replayer: &Replayer<'_>,
        current: &[KeySpec],
    ) -> Result<MembershipLints> {
        let mut uses: BTreeMap<String, MembershipUse> = BTreeMap::new();
        let mut inputs = Vec::new();
        let mut add = |column: &str, all: bool, values: u64| {
            let u = uses
                .entry(column.to_string())
                .or_insert_with(|| MembershipUse {
                    column: column.to_string(),
                    any_runs: 0,
                    all_runs: 0,
                    values_max: 0,
                });
            if all {
                u.all_runs += 1;
            } else {
                u.any_runs += 1;
            }
            u.values_max = u.values_max.max(values);
        };
        match (
            req.mode.filter(|m| m.is_membership()),
            &req.keys,
            &req.workload,
        ) {
            (Some(mode), Some(keys), _) => {
                let (run_keys, _) = keys.run_keys(replayer.schema())?;
                let is_list = |c: &String| {
                    replayer
                        .schema()
                        .field_with_name(c)
                        .is_ok_and(|f| bellwether_core::list::element_type(f.data_type()).is_some())
                };
                // Anything but one list column is refused when the run is
                // recorded, with the reason; it raises no lint here.
                if let [column] = run_keys
                    .columns()
                    .iter()
                    .filter(|c| is_list(c))
                    .collect::<Vec<_>>()[..]
                {
                    let payload = run_keys.to_payload()?;
                    let hash = Hash::of(&payload);
                    if let Some(cache) = self.writable_store().and_then(|_| self.cache().ok()) {
                        cache.put_cached(&hash, bytes::Bytes::from(payload))?;
                    }
                    inputs.push(hash);
                    add(
                        column,
                        mode == LookupMode::ContainsAll,
                        run_keys.len() as u64,
                    );
                }
            }
            (_, None, Some(h)) => {
                if let Some(w) = self.cache().ok().and_then(|c| Workload::load(&c, h).ok()) {
                    for r in w.runs() {
                        if let (Ok(q), [column]) = (r.query(), r.columns.as_slice()) {
                            if q.is_membership() {
                                add(column, q == RunQuery::ContainsAll, r.keys);
                            }
                        }
                    }
                    if !uses.is_empty() {
                        inputs.push(*h);
                    }
                }
            }
            _ => {}
        }
        let uses: Vec<MembershipUse> = uses.into_values().collect();
        Ok(MembershipLints {
            lints: bellwether_profile::lints::list_membership(&uses, current),
            inputs,
        })
    }

    /// Refuses a `why` request whose flags contradict each other, before any
    /// work. Returns whether it carries a workload.
    #[allow(clippy::too_many_lines)]
    fn check_why_request(&self, req: &WhyRequest) -> Result<bool> {
        let given = [
            req.keys.is_some(),
            req.bench_workload.is_some(),
            req.workload.is_some(),
            !req.text.is_empty(),
        ];
        if !req.more_keys.is_empty() && req.keys.is_none() {
            return Err(Error::InvalidArgument {
                field: "more_keys",
                reason: "further key files are more runs of the first; give the first as the key \
                         file (--from) too"
                    .into(),
            });
        }
        if given.iter().filter(|g| **g).count() > 1 {
            return Err(Error::InvalidArgument {
                field: "workload",
                reason: "give one workload: a key file, a bench workload directory, a recorded \
                         workload hash, or text lookups, not several"
                    .into(),
            });
        }
        if let Some(cols) = &req.columns {
            self.check_columns(&cols.iter().collect::<Vec<_>>())?;
        }
        if req.sketches_only && given.iter().any(|g| *g) {
            return Err(Error::InvalidArgument {
                field: "sketches_only",
                reason: "a workload's keys are priced by reading their key columns, which a \
                         diagnosis from sketches alone does not do; drop the sketches-only option \
                         to replay them, or drop the workload"
                    .into(),
            });
        }
        if req.sketches_only && req.columns.is_some() {
            return Err(Error::InvalidArgument {
                field: "sketches_only",
                reason: "columns choose what the profile reads and measures; a diagnosis from \
                         sketches alone reads no column, so drop one of the two"
                    .into(),
            });
        }
        self.check_text_request(req)?;
        let mode = req.lookup_mode().map_err(Error::KeySpec)?;
        check_values_given(req, mode)?;
        if mode == LookupMode::SemiJoin && !(given[0] || given[2]) {
            return Err(Error::KeySpec(
                "--mode semijoin prices a key file on the left dataset's join columns, recorded \
                 as one semi-join run; pass --from <keys.parquet> (or --workload <hash> of \
                 semi-join runs)"
                    .into(),
            ));
        }
        if req.window.is_some() && !(given[0] || given[1]) {
            return Err(Error::Other(
                "a window (--window) applies to the runs why records: give a key file (--from) or \
                 a bench workload (--bench-workload); a recorded workload (--workload) keeps the \
                 windows it was recorded with"
                    .into(),
            ));
        }
        if req.window.is_some() && mode != LookupMode::Locate {
            return Err(Error::KeySpec(
                "--window prices point lookups in a time window; a latest or asof lookup takes a \
                 lookback instead: pass --latest-by <column> --lookback <length> (and --at \
                 <instant> for asof) without --window"
                    .into(),
            ));
        }
        if let Some(ts) = req.asof_window_column() {
            if req.keys.is_none() {
                return Err(Error::Other(
                    "--ts with --at and --lookback records the key file's run as that window: \
                     give a key file (--from); a recorded workload (--workload) is priced as it \
                     was recorded"
                        .into(),
                ));
            }
            if req.latest_by.as_deref().map(str::trim) != Some(ts.trim()) {
                return Err(Error::InvalidArgument {
                    field: "latest_by",
                    reason: format!(
                        "--ts {ts} plans the window on {ts}, so --at and --lookback are measured \
                         in its unit, as `bellwether asof --ts {ts}` measures them; pass \
                         --latest-by {ts} --ts {ts}"
                    ),
                });
            }
        } else if let (None, Some(ts)) = (&req.window, &req.ts_column) {
            return Err(Error::Other(format!(
                "--ts names a window's time column; give the window too (--window), or price \
                 `asof --at --lookback --ts {ts}` with --latest-by {ts} --at <instant> \
                 --lookback <length>"
            )));
        }
        if matches!(mode, LookupMode::Latest | LookupMode::Asof)
            && req.keys.is_none()
            && req.workload.is_none()
        {
            return Err(Error::KeySpec(
                "--latest-by prices a workload's keys under latest or asof lookups; pass --from \
                 <keys.parquet> (or --workload <hash>)"
                    .into(),
            ));
        }
        Ok(given.iter().any(|g| *g))
    }

    /// Profiles the data root and builds its Evidence, recording it when the
    /// index location is writable. With a workload, an unwritable location
    /// is refused (the store's one-line error naming `--index`), since the
    /// Proposals must cite a recorded Evidence. Lints the profile does not
    /// compute (`extra_lints`: the sketch detector's, and `R-text-1`, which
    /// reads the workload's runs) join the same Evidence, which then also
    /// cites `cites` (the workload `R-text-1` read, the key set or workload
    /// `R-list-1` read). [`Measure::Manifest`]
    /// profiles from totals the caller already holds and reads no file.
    #[allow(clippy::too_many_arguments)]
    fn profile(
        &self,
        req: &WhyRequest,
        measure: Measure<'_>,
        pinned: Option<&Pinned>,
        has_workload: bool,
        twin: Option<TwinOf>,
        twin_lints: &[Lint],
        extra_lints: Vec<bellwether_types::Lint>,
        cites: Vec<Hash>,
    ) -> Result<(ProfileReport, ProfileEvidence)> {
        let writable = if has_workload {
            Some(Arc::clone(self.store()?))
        } else {
            self.writable_store().map(Arc::clone)
        };
        let store = match (
            &writable,
            pinned.filter(|_| self.options().at_manifest.is_some()),
        ) {
            // Pinned to an earlier manifest: the profile reads that
            // manifest, in memory, so its lints see the specs it declared.
            (_, Some(p)) => {
                let memory = Arc::new(ObjectStoreBackend::memory()?);
                memory.put_if_absent("manifest.json", p.bytes.clone())?;
                memory
            }
            (Some(s), None) => Arc::clone(s),
            (None, None) => {
                // Profile in memory, with the index's manifest when it can
                // be read, so a read-only index still informs the lints.
                let memory = Arc::new(ObjectStoreBackend::memory()?);
                if let Some(existing) = self.existing_store().ok().flatten() {
                    if let Some((bytes, _)) = existing.get_opt("manifest.json")? {
                        memory.put_if_absent("manifest.json", bytes)?;
                    }
                }
                memory
            }
        };
        let mut options = ProfileOptions::for_source(&self.source_config(pinned)?);
        options.columns.clone_from(&req.columns);
        // A fixed clock asks for reproducible output: recompute the cached
        // artifacts rather than reuse them, so no input carries a wall-clock
        // age or a cache hit, and measure the manifest's age at that clock.
        options.reuse_cache = self.options().clock.is_none();
        if self.options().clock.is_some() {
            options.now_millis = Some(self.now_nanos()?.div_euclid(1_000_000));
        }
        options.workload = req.bench_workload.as_ref().map(|p| {
            if p.is_dir() {
                p.clone()
            } else {
                p.parent().map(PathBuf::from).unwrap_or_default()
            }
        });
        // A key file or a recorded workload says which columns lookups
        // supply, as a bench workload does (contract v3 backlog #35): the
        // lints then read them instead of assuming an order. A text run's
        // column is searched by its words, never looked up by value, so it
        // supplies no key column.
        let text_mode = req.mode.and_then(LookupMode::text_op).is_some();
        if let (Some(keys), false) = (&req.keys, text_mode) {
            options.supplied_columns = keys.column_names()?;
        } else if let Some(h) = &req.workload {
            // An unreadable workload is refused by `record_workload`, with
            // its own message; here it only informs no column.
            if let Some(w) = self.cache().ok().and_then(|c| Workload::load(&c, h).ok()) {
                for r in w.runs() {
                    if r.query().is_ok_and(|q| q.text_op().is_some()) {
                        continue;
                    }
                    for c in &r.columns {
                        if !options.supplied_columns.contains(c) {
                            options.supplied_columns.push(c.clone());
                        }
                    }
                }
            }
        }
        let mut profile = match measure {
            Measure::Source(source) => {
                bellwether_profile::profile(source, self.root(), &store, &options)?
            }
            Measure::Manifest(totals) => {
                bellwether_profile::manifest_profile(self.root(), &store, &options, totals)?
            }
        };
        // The sketch detector's lints and `R-text-1` join the profile's
        // before the Evidence is built, so the Evidence records them; the
        // sketches are cited through the manifest, which names their
        // segments by content, and the text lints through the workload.
        profile.lints.extend(extra_lints);
        let mut evidence = bellwether_profile::evidence(&profile)?;
        for h in cites {
            if !evidence.provenance.inputs.contains(&h) {
                evidence.provenance.inputs.push(h);
            }
        }
        if let EvidenceBody::Profile {
            twin: slot, lints, ..
        } = &mut evidence.body
        {
            *slot = twin;
            lints.extend_from_slice(twin_lints);
        }
        // Every record this engine writes carries the engine's clock.
        evidence.provenance.created_at = self.now();
        let out = if writable.is_some() {
            // A run over a table ends before anything is recorded.
            self.finish_tables()?;
            let hash = self.ledger()?.put(&Record::Evidence(evidence.clone()))?;
            ProfileEvidence {
                recorded: true,
                hash: Some(hash),
                evidence,
                note: None,
            }
        } else {
            ProfileEvidence {
                recorded: false,
                hash: None,
                evidence,
                note: Some(format!(
                    "not recorded: the index location {} is not writable; pass a writable \
                     location with --index to record it",
                    self.index_location()
                )),
            }
        };
        Ok((profile, out))
    }

    #[allow(clippy::too_many_lines)]
    fn record_workload(
        &self,
        req: &WhyRequest,
        pinned: Option<&Pinned>,
        replayer: &Replayer<'_>,
        cache: &Cache<ObjectStoreBackend>,
        resolved: Option<ResolvedTemporal>,
    ) -> Result<(Workload, Option<ResolvedTemporal>)> {
        if let Some(h) = &req.workload {
            let w = crate::fix::load_workload(self, cache, h)?;
            let temporal = match resolved {
                Some(t) => {
                    // The runs are priced as recorded: a `--latest-by` that
                    // asks for another lookup would label their costs as
                    // the requested lookup's.
                    for r in w.runs() {
                        let q = r.query()?;
                        if q != t.query {
                            return Err(Error::KeySpec(format!(
                                "workload {h} records its runs as {} lookups, and --mode, \
                                 --latest-by, --at, and --lookback ask for {} lookups; record \
                                 the keys again with `bellwether why --from <keys.parquet> \
                                 --latest-by {}` and the same options",
                                describe(&q),
                                describe(&t.query),
                                t.ts_column
                            )));
                        }
                    }
                    Some(t)
                }
                None if req.mode.and_then(LookupMode::value_query).is_some() => {
                    check_value_runs(h, &w, req.mode)?;
                    None
                }
                None if req.mode == Some(LookupMode::SemiJoin) => {
                    let want = semijoin_query(req)?;
                    if let Some(r) = w.runs().iter().find(|r| r.query().ok() != want) {
                        return Err(Error::KeySpec(format!(
                            "workload {h} records run {:?} as a {} lookup, and --mode semijoin \
                             --join {} asks for semi-joins with that right side; drop --mode and \
                             --join to price it as recorded, or record the keys again with \
                             `bellwether why --from <keys.parquet> --mode semijoin --join {}`",
                            r.run_id,
                            describe(&r.query()?),
                            req.join.as_deref().unwrap_or_default(),
                            req.join.as_deref().unwrap_or_default()
                        )));
                    }
                    None
                }
                None if w
                    .runs()
                    .iter()
                    .any(|r| r.query().is_ok_and(|q| q.join().is_some())) =>
                {
                    return Err(Error::KeySpec(format!(
                        "workload {h} records semi-join runs, which are priced on both datasets; \
                         pass --mode semijoin --join <right>.<columns> with the right side they \
                         record"
                    )));
                }
                None if req.mode == Some(LookupMode::Locate) => {
                    if let Some(r) = w
                        .runs()
                        .iter()
                        .find(|r| r.query().ok() != Some(RunQuery::Locate))
                    {
                        return Err(Error::KeySpec(format!(
                            "workload {h} records its runs as {} lookups, and --mode locate asks \
                             for point lookups; drop --mode to price them as recorded, or record \
                             the keys again with `bellwether why --from <keys.parquet>`",
                            describe(&r.query()?)
                        )));
                    }
                    None
                }
                None => infer_workload_temporal(&w, replayer.current_specs())?,
            };
            return Ok((w, temporal));
        }
        let recorder = WorkloadRecorder::new(cache, std::sync::Arc::clone(replayer.schema()));
        if let Some(first) = &req.keys {
            let files: Vec<&KeyFile> = std::iter::once(first).chain(&req.more_keys).collect();
            let batches = files.iter().map(|k| k.read()).collect::<Result<Vec<_>>>()?;
            if let Some(t) = &resolved {
                for batch in &batches {
                    for f in batch.schema().fields() {
                        if f.name() == &t.ts_column {
                            return Err(Error::KeySpec(format!(
                                "--latest-by column {:?} is also a key column of the lookup; \
                                 give a separate timestamp column or select key columns with --key",
                                t.ts_column
                            )));
                        }
                    }
                }
            }
            let columns: Vec<String> = batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let mut recorder = self.windowed(recorder, req, pinned, replayer, &columns)?;
            let query = match (
                req.mode.and_then(LookupMode::value_query),
                semijoin_query(req)?,
            ) {
                (Some(q), _) | (None, Some(q)) => q,
                (None, None) => resolved
                    .as_ref()
                    .map_or(RunQuery::Locate, |t| t.query.clone()),
            };
            for (i, batch) in batches.iter().enumerate() {
                let id = if i == 0 {
                    "keys".to_string()
                } else {
                    format!("keys-{}", i + 1)
                };
                recorder.record_query(&id, KeyInput::Batch(batch), query.clone(), None)?;
            }
            return Ok((recorder.finish()?, resolved));
        }
        if !req.text.is_empty() {
            let mut recorder = recorder;
            crate::text::record(&mut recorder, replayer.schema(), &req.text)?;
            return Ok((recorder.finish()?, resolved));
        }
        let mut recorder = self.windowed(recorder, req, pinned, replayer, &[])?;
        if let Some(dir) = &req.bench_workload {
            let file = if dir.is_dir() {
                dir.join("workload.json")
            } else {
                dir.clone()
            };
            record_bench_workload(&mut recorder, &file).map_err(Error::from)?;
        }
        Ok((recorder.finish()?, resolved))
    }

    /// `recorder`, recording window runs when the request gives a window:
    /// its time column (`--ts`, else the one a window lookup on `columns`
    /// would take) and both bounds in that column's unit. With `--ts`,
    /// `--at`, and `--lookback` instead, the window is the one `asof --at
    /// --lookback --ts` plans: `[max(at − lookback, floor), at]` on `--ts`.
    fn windowed<'r>(
        &self,
        recorder: WorkloadRecorder<'r>,
        req: &WhyRequest,
        pinned: Option<&Pinned>,
        replayer: &Replayer<'_>,
        columns: &[String],
    ) -> Result<WorkloadRecorder<'r>> {
        if let (Some(c), Some(at), Some(lookback)) =
            (req.asof_window_column(), &req.at, &req.lookback)
        {
            let ty = replayer
                .schema()
                .field_with_name(c)
                .map_err(|_| {
                    Error::KeySpec(format!(
                        "the window's time column {c:?} is not in the dataset; name one with --ts"
                    ))
                })?
                .data_type()
                .clone();
            let hi = instant_in_unit("at", at, c, &ty)?;
            let n = Lookback::parse(lookback)?.in_unit(c, &ty)?;
            let lo = hi
                .saturating_sub(n)
                .max(bellwether_core::encoding::ts_min(&ty)?);
            return recorder.in_window(RunWindow {
                ts_column: c.to_string(),
                lo,
                hi,
            });
        }
        let Some(window) = &req.window else {
            return Ok(recorder);
        };
        let schema = replayer.schema();
        let ts_column = if let Some(c) = &req.ts_column {
            c.clone()
        } else if let Some(p) = pinned {
            p.planner
                .window_column(&p.specs().0, columns)?
                .ok_or_else(|| no_time_column(columns))?
        } else {
            let first = replayer.catalog().files().first().map(|f| f.path.as_str());
            let partition =
                bellwether_build::partition_columns(&self.partitioning(None)?, self.root(), first)?;
            bellwether_build::default_window_column(schema, &[], Vec::new(), partition, columns)?
                .ok_or_else(|| no_time_column(columns))?
        };
        let ty = schema
            .field_with_name(&ts_column)
            .map_err(|_| {
                Error::KeySpec(format!(
                    "the window's time column {ts_column:?} is not in the dataset; name one with \
                     --ts"
                ))
            })?
            .data_type()
            .clone();
        let TimeWindow { from, to } = window;
        let lo = instant_in_unit("window", from, &ts_column, &ty)?;
        let hi = instant_in_unit("window", to, &ts_column, &ty)?;
        if lo > hi {
            return Err(Error::Other(format!(
                "the window starts at {from} ({ts_column} = {lo}) after it ends at {to} ({hi}); \
                 give the start first"
            )));
        }
        recorder.in_window(RunWindow { ts_column, lo, hi })
    }

    // One linear pass: choose the candidates, then write and score each.
    #[allow(clippy::too_many_lines)]
    fn replay_candidates(
        &self,
        report: &mut WhyReport,
        replayer: &Replayer<'_>,
        workload: &Workload,
        cache: &Cache<ObjectStoreBackend>,
        req: &WhyRequest,
        temporal: Option<&ResolvedTemporal>,
    ) -> Result<()> {
        let evidence = report.evidence.hash.ok_or_else(|| {
            Error::Store(format!(
                "the profile Evidence could not be recorded at {}; pass a writable location \
                 with --index",
                self.index_location()
            ))
        })?;
        let run_columns: Vec<Vec<String>> = {
            let set: BTreeSet<Vec<String>> =
                workload.runs().iter().map(|r| r.columns.clone()).collect();
            set.into_iter().collect()
        };
        // A workload of text runs (B5) is priced on text keys alone, and one
        // of membership runs (B4) on list keys alone; one mixing either with
        // key lookups would price each candidate on runs it cannot serve.
        let queries: Vec<RunQuery> = workload
            .runs()
            .iter()
            .map(RecordedRun::query)
            .collect::<Result<_>>()?;
        let text_runs = queries.iter().filter(|q| q.text_op().is_some()).count();
        if text_runs > 0 && text_runs < queries.len() {
            return Err(Error::KeySpec(format!(
                "workload {} mixes {text_runs} text runs (contains, has_token, has_prefix) with {} \
                 key lookups; a text key serves only the former, so price them as two \
                 workloads (record the text lookups with `bellwether why --contains, --has, or \
                 --prefix COL=TEXT`, the keys with `bellwether why --from <keys.parquet>`)",
                workload.hash(),
                queries.len() - text_runs
            )));
        }
        let text = text_runs > 0;
        let membership = queries.iter().filter(|q| q.is_membership()).count();
        if membership > 0 && membership < queries.len() {
            return Err(Error::KeySpec(format!(
                "workload {} mixes {membership} list-membership runs with {} key lookups; a list \
                 key serves only the former, so price them as two workloads (record each with \
                 `bellwether why --from <keys.parquet>`, adding --mode contains-any or \
                 contains-all for the list values)",
                workload.hash(),
                queries.len() - membership
            )));
        }
        let membership = membership > 0;
        // A semi-join workload (B3) is priced on the join key of its pair
        // alone: the right side its runs record (one right side, since the
        // pair attached is one right dataset).
        let joined: BTreeSet<&RunJoin> = queries.iter().filter_map(RunQuery::join).collect();
        if !joined.is_empty() && (joined.len() > 1 || queries.iter().any(|q| q.join().is_none())) {
            return Err(Error::KeySpec(format!(
                "workload {} mixes semi-join runs of {} right side(s) with {} other lookups; a \
                 join key serves the semi-joins of one pair, so record each as its own workload",
                workload.hash(),
                joined.len(),
                queries.iter().filter(|q| q.join().is_none()).count()
            )));
        }
        let semijoin = joined.into_iter().next().cloned();
        let mut windows: Vec<RunWindow> = Vec::new();
        for w in workload.runs().iter().filter_map(|r| r.window.as_ref()) {
            if !windows.contains(w) {
                windows.push(w.clone());
            }
        }
        if let Some(t) = &temporal {
            for cols in &run_columns {
                if cols.contains(&t.ts_column) {
                    return Err(Error::KeySpec(format!(
                        "--latest-by column {:?} is also a key column of the workload; give a \
                         separate timestamp column",
                        t.ts_column
                    )));
                }
                check_planned_column(replayer, cols, t)?;
            }
        }
        let text_kind = text.then(|| {
            let columns: BTreeSet<String> = run_columns.iter().flatten().cloned().collect();
            let operators: BTreeSet<TextOp> =
                queries.iter().filter_map(RunQuery::text_op).collect();
            LookupKind::TextRuns {
                columns: columns.into_iter().collect(),
                operators: operators.into_iter().collect(),
            }
        });
        let membership_kind = match (membership, queries.first(), run_columns.first()) {
            (true, Some(q), Some(c)) => Some(match q {
                RunQuery::ContainsAll => LookupKind::ContainsAll {
                    column: c.join(","),
                },
                _ => LookupKind::ContainsAny {
                    column: c.join(","),
                },
            }),
            _ => None,
        };
        let semijoin_kind = semijoin.as_ref().map(|j| LookupKind::SemiJoin {
            right: j.right.clone(),
            right_columns: j.right_columns.clone(),
        });
        let run_kind = text_kind.or(membership_kind).or(semijoin_kind);
        let query_kind = if text {
            let kinds: BTreeSet<&str> = queries.iter().map(RunQuery::kind_str).collect();
            kinds.into_iter().collect::<Vec<_>>().join(",")
        } else {
            match (&run_kind, queries.first()) {
                (Some(_), Some(q)) => q.kind_str(),
                _ => temporal
                    .as_ref()
                    .map_or(QUERY_KIND_LOCATE, |t| t.query.kind_str()),
            }
            .to_string()
        };
        let lookup_kind = match &run_kind {
            Some(k) => k.clone(),
            None => temporal
                .as_ref()
                .map_or(LookupKind::Locate, |t| t.lookup.clone()),
        };
        let lookback = temporal.as_ref().and_then(|t| t.lookback.clone());
        report.workload = Some(WorkloadInfo {
            hash: workload.hash(),
            runs: workload.runs().len() as u64,
            query_kind,
            lookup: run_kind
                .clone()
                .or_else(|| temporal.as_ref().map(|t| t.lookup.clone())),
            lookback: lookback.clone(),
            columns: run_columns.clone(),
            keys_p50_p95_max: workload.keys_p50_p95_max(),
            null_tuples: workload.runs().iter().map(|r| r.null_tuples).sum(),
            windows,
        });
        // Candidates are told apart by their full spec, time column included,
        // so `temporal:symbol@day` is offered beside a current
        // `temporal:symbol@ts`.
        let current_names: BTreeSet<String> = report.current_specs.iter().map(label).collect();
        // How a measured key serves the workload: by point lookup on some
        // run, only by prefix of a run's columns, or not at all. A key that
        // serves only by prefix is a composite led by a run's whole key, so
        // it selects at least the row groups the exact key on that run's
        // columns (the floor, a candidate or current) selects: it can never
        // read less, and is not replayed.
        let point = |spec: &KeySpec| {
            run_columns
                .iter()
                .any(|c| matches!(servable(spec, c), Some(Lookup::Point { .. })))
        };
        let serves = |spec: &KeySpec| run_columns.iter().any(|c| servable(spec, c).is_some());
        let mut chosen: Vec<(KeySpec, CandidateOrigin)> = Vec::new();
        let mut names = BTreeSet::new();
        if let Some(t) = &temporal {
            for c in &run_columns {
                let spec = KeySpec::Temporal {
                    columns: c.clone(),
                    ts_column: t.ts_column.clone(),
                    counts: false,
                };
                if !current_names.contains(&label(&spec)) && names.insert(label(&spec)) {
                    chosen.push((spec, CandidateOrigin::Workload));
                }
            }
        }
        // A text workload's floor is the text keys on its columns (a words
        // dictionary for whole words and prefixes, a trigram key for every
        // match); an exact key looks whole values up and serves none of it.
        // A run keyed on an expression gets an `expr` key over it (an exact
        // segment on a virtual column the engine names); a run on dataset
        // columns gets the exact key on them.
        let floor: Vec<KeySpec> = if text {
            crate::text::floor_keys(workload)?
        } else if let Some(j) = &semijoin {
            // A semi-join run's floor is the join key on its pair (B3): an
            // exact key on either side locates every asked key, held by the
            // other side or not.
            run_columns
                .iter()
                .map(|c| crate::join::join_key(self.root(), c, j))
                .collect()
        } else if membership {
            // A membership run's floor is the list key on its one column
            // (B4); an exact key cannot look a list's elements up.
            run_columns
                .iter()
                .filter_map(|c| match c.as_slice() {
                    [column] => Some(KeySpec::List {
                        column: column.clone(),
                    }),
                    _ => None,
                })
                .collect()
        } else {
            run_columns
                .iter()
                .map(|c| -> Result<KeySpec> {
                    Ok(match c.as_slice() {
                        [e] if bellwether_core::expr::is_expression(e, replayer.schema()) => {
                            let parsed = bellwether_core::expr::Expr::parse(e)?;
                            let name =
                                bellwether_core::expr::suggested_name(&parsed, replayer.schema());
                            bellwether_core::expr::spec(
                                &name,
                                e,
                                bellwether_core::expr::Over::Exact,
                            )?
                        }
                        _ => KeySpec::Exact {
                            columns: c.clone(),
                            counts: false,
                            bounds: None,
                        },
                    })
                })
                .collect::<Result<_>>()?
        };
        for spec in floor {
            // An exact `expr` key the index already declares over the same
            // (normalized) expression is the same key under another name.
            let declared = matches!(&spec, KeySpec::Expr { expr, .. } if report
                .current_specs
                .iter()
                .any(|s| matches!(s, KeySpec::Expr { expr: e, over, .. }
                    if e == expr && matches!(over.as_ref(), KeySpec::Exact { .. }))));
            if !declared && !current_names.contains(&label(&spec)) && names.insert(label(&spec)) {
                chosen.push((spec, CandidateOrigin::Workload));
            }
        }
        // A count run: the exact key on its columns storing counts answers
        // the count from the index with no data file read, so it is priced
        // whenever no current spec counts those columns already (one that
        // stores counts on exactly the run's columns, in any order).
        if temporal.is_none() {
            let counted: BTreeSet<Vec<String>> = workload
                .runs()
                .iter()
                .filter(|r| matches!(r.query(), Ok(RunQuery::Count)))
                .map(|r| r.columns.clone())
                .collect();
            for c in counted {
                let answered = report.current_specs.iter().any(|s| {
                    s.counts()
                        && s.columns().len() == c.len()
                        && s.columns().iter().all(|x| c.contains(x))
                });
                let spec = KeySpec::Exact {
                    columns: c,
                    counts: true,
                    bounds: None,
                };
                if !answered && !current_names.contains(&label(&spec)) && names.insert(label(&spec))
                {
                    chosen.push((spec, CandidateOrigin::Workload));
                }
            }
        }
        // A window run: the temporal key on its key columns and the
        // window's time column, whose version chains prune by time per key.
        let windowed: BTreeSet<(Vec<String>, String)> = workload
            .runs()
            .iter()
            .filter_map(|r| {
                r.window
                    .as_ref()
                    .map(|w| (r.columns.clone(), w.ts_column.clone()))
            })
            .collect();
        for (columns, ts_column) in windowed {
            let orderable = replayer
                .schema()
                .field_with_name(&ts_column)
                .is_ok_and(|f| bellwether_core::encoding::check_ts_type(f.data_type()).is_ok());
            if !orderable || columns.contains(&ts_column) {
                continue;
            }
            let spec = KeySpec::Temporal {
                columns,
                ts_column,
                counts: false,
            };
            if !current_names.contains(&label(&spec)) && names.insert(label(&spec)) {
                chosen.push((spec, CandidateOrigin::Window));
            }
        }
        let mut from_profile = 0usize;
        for entry in &report.profile.fanout {
            let cols = &entry.fanout.columns;
            let exact_spec = KeySpec::Exact {
                columns: cols.clone(),
                counts: false,
                bounds: None,
            };
            if text || membership || semijoin.is_some() || !serves(&exact_spec) {
                if !current_names.contains(&label(&exact_spec))
                    && !names.contains(&label(&exact_spec))
                {
                    report.not_serving.push(exact_spec);
                }
                continue;
            }
            if !point(&exact_spec) {
                continue;
            }
            if let Some(t) = &temporal {
                if !cols.contains(&t.ts_column) {
                    let tspec = KeySpec::Temporal {
                        columns: cols.clone(),
                        ts_column: t.ts_column.clone(),
                        counts: false,
                    };
                    if !current_names.contains(&label(&tspec))
                        && !names.contains(&label(&tspec))
                        && from_profile < req.max_candidates
                    {
                        names.insert(label(&tspec));
                        chosen.push((tspec, CandidateOrigin::Profile));
                        from_profile += 1;
                    }
                }
            }
            if !current_names.contains(&label(&exact_spec))
                && !names.contains(&label(&exact_spec))
                && from_profile < req.max_candidates
            {
                names.insert(label(&exact_spec));
                chosen.push((exact_spec, CandidateOrigin::Profile));
                from_profile += 1;
            }
        }
        let ledger = self.ledger()?;
        for (spec, origin) in chosen {
            let proposal = self.put(Record::Proposal(Proposal {
                record_version: format::RECORD_VERSION,
                evidence: vec![evidence],
                proposed_by: ENGINE_PROPOSER.to_string(),
                created_at: self.now(),
                body: ProposalBody::Key {
                    spec: spec.clone(),
                    sidecars: Vec::new(),
                },
            }))?;
            let (replay_hash, scored) = if self.is_table() {
                // Over a table every cost is measured: scored, marked so,
                // and not recorded (ADR 027; the ledger keeps exact current
                // costs only, ADR 005).
                let tw;
                let w = if temporal.is_some()
                    && workload
                        .runs()
                        .iter()
                        .all(|r| matches!(r.query(), Ok(RunQuery::Locate)))
                {
                    tw = with_query(
                        workload,
                        &temporal.map(|t| t.query.clone()).unwrap_or_default(),
                    )?;
                    &tw
                } else {
                    workload
                };
                crate::fix::measured_score(self, replayer, &proposal.hash, w, cache)?
            } else if temporal.is_some()
                && workload
                    .runs()
                    .iter()
                    .all(|r| matches!(r.query(), Ok(RunQuery::Locate)))
            {
                // A recorded locate workload replayed with --latest-by: score
                // against a synthetic temporal view of the same runs, or
                // score directly when the workload already records the query.
                let tw = with_query(
                    workload,
                    &temporal.map(|t| t.query.clone()).unwrap_or_default(),
                )?;
                replayer
                    .record(&ledger, &proposal.hash, &tw, cache, &self.now())
                    .map_err(Error::from)?
            } else {
                replayer
                    .record(&ledger, &proposal.hash, workload, cache, &self.now())
                    .map_err(Error::from)?
            };
            let locate = if temporal.is_some() {
                let specs = proposed_specs(replayer.current_specs(), &spec);
                Some(total(&replayer.replay_specs_as(
                    &specs,
                    workload,
                    cache,
                    &RunQuery::Locate,
                )?))
            } else {
                None
            };
            // The kind column: a text key's (distinct tokens); a list key's
            // (distinct elements); an `expr`
            // candidate's (null-expression rows and distinct derived values);
            // else the sketch's rows per value where a sketch holds the
            // candidate's columns; otherwise the `counts` column: for an exact
            // candidate on exactly the workload's key columns, the rows its
            // keys hold, from a replay with that key storing counts (the
            // counts replay computes for `exact:<cols>,counts`; exact).
            let kind_column = match &spec {
                KeySpec::Text { .. } => Some(crate::text::kind_column(
                    replayer,
                    &spec,
                    total(&scored.replay.proposed).bytes < total(&scored.replay.current).bytes,
                )?),
                // a list key's column: its distinct elements (B4)
                KeySpec::List { .. } => Some(list_column(replayer, &spec)?),
                // a join key's column: the asked keys both datasets hold and
                // the right row groups per shared key (B3)
                KeySpec::Join { .. } => Some(crate::join::join_column(
                    replayer, &spec, workload, cache, &req.gates,
                )?),
                _ => match expr_column(replayer, &spec)? {
                    Some(c) => Some(c),
                    None => match sketch_column(&report.sketch, spec.columns()) {
                        Some(c) => Some(c),
                        None => match &spec {
                            KeySpec::Exact {
                                columns,
                                bounds: None,
                                ..
                            } if temporal.is_none()
                                && workload.runs().iter().all(|r| {
                                    &r.columns == columns
                                        && matches!(r.query(), Ok(RunQuery::Locate))
                                }) =>
                            {
                                let counted = KeySpec::Exact {
                                    columns: columns.clone(),
                                    counts: true,
                                    bounds: None,
                                };
                                let specs = proposed_specs(replayer.current_specs(), &counted);
                                let costs = replayer.replay_specs(&specs, workload, cache)?;
                                costs
                                    .iter()
                                    .map(|c| c.rows.as_ref().map(|q| q.value))
                                    .sum::<Option<u64>>()
                                    .map(|rows| bellwether_types::KindColumn {
                                        kind: "counts".into(),
                                        label: format!(
                                        "rows holding the keys over every run, as {} counts them",
                                        counted.identity()
                                    ),
                                        value: rows.to_string(),
                                        exactness: bellwether_types::Exactness::Exact,
                                    })
                            }
                            _ => None,
                        },
                    },
                },
            };
            report.candidates.push(Candidate {
                spec,
                origin,
                proposed: total(&scored.replay.proposed),
                locate,
                proposal,
                replay: Written {
                    hash: replay_hash,
                    record: Record::Replay(scored.replay),
                },
                kind_column,
            });
        }
        let current = match report.candidates.first() {
            Some(Candidate {
                replay:
                    Written {
                        record: Record::Replay(r),
                        ..
                    },
                ..
            }) => total(&r.current),
            _ => match &temporal {
                Some(t) => total(&replayer.replay_specs_as(
                    replayer.current_specs(),
                    workload,
                    cache,
                    &t.query,
                )?),
                None => total(&replayer.replay_current(workload, cache)?),
            },
        };
        report.current = Some(current);
        if temporal.is_some() {
            report.current_locate = Some(total(&replayer.replay_specs_as(
                replayer.current_specs(),
                workload,
                cache,
                &RunQuery::Locate,
            )?));
        }
        // Over Parquet files a run no current key serves is priced as the
        // files' own statistics read it; say so only when some run is so
        // priced (as the runs are priced: a locate run under --latest-by as
        // the temporal lookup) and the files carry statistics on its
        // columns, else name their absence. Without an index, the full scan
        // is context beside it.
        let unserved_runs: Vec<&bellwether_replay::RecordedRun> = workload
            .runs()
            .iter()
            .filter(|r| {
                let query = match (temporal, r.query()) {
                    (Some(t), Ok(RunQuery::Locate)) => t.query.clone(),
                    // A semi-join (B3) is priced only with an exact key on
                    // each side (the pair index needs both), so each step
                    // is served without a join key too.
                    (_, Ok(RunQuery::SemiJoin(_))) => return false,
                    (_, Ok(q)) => q,
                    (_, Err(_)) => return true,
                };
                !replayer
                    .current_specs()
                    .iter()
                    .any(|s| serves_query(s, &r.columns, &query))
            })
            .collect();
        // A run on an expression looks up derived values no file statistics
        // hold: the unserved baseline reads every row group for it (correct,
        // and nothing to say about statistics), so only runs on dataset
        // columns decide the note.
        let unserved_runs: Vec<_> = unserved_runs
            .into_iter()
            .filter(|r| !is_expression_run(replayer.schema(), &r.columns))
            .collect();
        if replayer.unserved() == Baseline::ParquetStats && !unserved_runs.is_empty() {
            let columns: BTreeSet<&str> = unserved_runs
                .iter()
                .flat_map(|r| r.columns.iter().map(String::as_str))
                .collect();
            let carries = |column: &str| {
                replayer.catalog().row_groups().iter().any(|rg| {
                    rg.meta
                        .stats
                        .iter()
                        .any(|s| s.column == column && s.min.is_some() && s.max.is_some())
                })
            };
            if columns.iter().any(|c| carries(c)) {
                report.unserved = Some(Baseline::ParquetStats.name().to_string());
                if replayer.current_specs().is_empty() {
                    report.full_scan = Some(total(
                        &replayer
                            .replay_baseline(Baseline::RawScan, workload, cache)
                            .map_err(Error::from)?,
                    ));
                }
            } else {
                report.no_statistics = true;
            }
        }
        report.recommended = report
            .candidates
            .iter()
            .enumerate()
            .min_by_key(|(i, c)| (c.proposed.bytes, c.spec.columns().len(), *i))
            .filter(|(_, c)| c.proposed.bytes < current.bytes)
            .map(|(_, c)| Recommendation {
                spec: c.spec.clone(),
                proposal: c.proposal.hash,
                replay: c.replay.hash,
                lookup: lookup_kind,
                lookback,
                current,
                proposed: c.proposed,
            });
        Ok(())
    }
}

/// The `expr` column of a candidate `expr` key (`KindColumn`): the rows
/// whose expression is null, which the key never indexes (ADR 014), and the
/// distinct derived values, counted from the transient index the replay
/// priced it with. `None` for a candidate of another kind.
fn expr_column(
    replayer: &Replayer<'_>,
    spec: &KeySpec,
) -> Result<Option<bellwether_types::KindColumn>> {
    use bellwether_profile::render::thousands;
    if !matches!(spec, KeySpec::Expr { .. }) {
        return Ok(None);
    }
    let index = replayer.transient(spec)?;
    let nulls = index.stats().null_rows;
    let Some((low, high)) = index.distinct_keys() else {
        return Ok(None);
    };
    let (distinct, exactness) = if low == high {
        (thousands(low), bellwether_types::Exactness::Exact)
    } else {
        (
            format!("{}..{}", thousands(low), thousands(high)),
            bellwether_types::Exactness::Bounded { low, high },
        )
    };
    Ok(Some(bellwether_types::KindColumn {
        kind: "expr".into(),
        label: "rows whose expression is null / distinct derived values".into(),
        value: format!("{} null / {distinct} distinct", thousands(nulls)),
        exactness,
    }))
}

/// Whether a run is keyed on one expression over dataset columns (an `expr`
/// key's lookup) rather than on dataset columns.
fn is_expression_run(schema: &arrow::datatypes::Schema, columns: &[String]) -> bool {
    matches!(columns, [c] if bellwether_core::expr::is_expression(c, schema))
}

/// How the values a workload of membership runs (B4) asks for are spread
/// over the data: per distinct queried value present in it (both encodings
/// of a float zero are one value), the row groups and files holding it,
/// read from the list key on the runs' column (its transient segments, the
/// ones the candidate was priced with). Exact. `None` for any other
/// workload.
fn queried_spread(
    replayer: &Replayer<'_>,
    workload: &Workload,
    cache: &Cache<ObjectStoreBackend>,
) -> Result<Option<Fanout>> {
    let Some(first) = workload.runs().first() else {
        return Ok(None);
    };
    let ([column], true) = (first.columns.as_slice(), first.query()?.is_membership()) else {
        return Ok(None);
    };
    let mut groups: BTreeSet<Vec<Vec<u8>>> = BTreeSet::new();
    for run in workload.runs() {
        let rk = workload.run_keys(cache, run)?;
        groups.extend((0..rk.len()).map(|i| rk.probes(i, &[0])));
    }
    let probes: Vec<&Vec<u8>> = groups.iter().flatten().collect();
    let index = replayer.transient(&KeySpec::List {
        column: column.clone(),
    })?;
    let per = bellwether_core::list::element_row_groups(index.segments().readers(), &probes)?;
    let catalog = replayer.catalog();
    let file_of: BTreeMap<u32, usize> = catalog
        .row_groups()
        .iter()
        .map(|rg| (rg.id.0, rg.file))
        .collect();
    let (mut rgs, mut files): (Vec<u32>, Vec<u32>) = (Vec::new(), Vec::new());
    for g in &groups {
        let held = g
            .iter()
            .filter_map(|v| per.get(v))
            .fold(roaring::RoaringBitmap::new(), |u, b| u | b);
        if held.is_empty() {
            continue;
        }
        let in_files: BTreeSet<usize> = held
            .iter()
            .filter_map(|id| file_of.get(&id).copied())
            .collect();
        rgs.push(u32::try_from(held.len()).unwrap_or(u32::MAX));
        files.push(u32::try_from(in_files.len()).unwrap_or(u32::MAX));
    }
    rgs.sort_unstable();
    files.sort_unstable();
    Ok(Some(Fanout {
        columns: vec![column.clone()],
        distinct: rgs.len() as u64,
        sampled: false,
        files_per_value: bellwether_profile::quantile::p50_p95_max(&files),
        row_groups_per_value: bellwether_profile::quantile::p50_p95_max(&rgs),
        files_total: catalog.files().len() as u64,
        row_groups_total: catalog.len() as u64,
        sample_size: None,
        quantile_error: None,
    }))
}

/// The semi-join runs of `w` per column pair (B3), for `R-join-1`.
fn semijoin_uses(
    w: &Workload,
    cache: &Cache<ObjectStoreBackend>,
) -> Result<Vec<bellwether_profile::lints::SemiJoinUse>> {
    let mut by: BTreeMap<(Vec<String>, RunJoin), (u64, u64)> = BTreeMap::new();
    for r in w.runs() {
        if let Ok(RunQuery::SemiJoin(j)) = r.query() {
            let keys = w.run_keys(cache, r)?.len() as u64;
            let e = by.entry((r.columns.clone(), j)).or_default();
            e.0 += 1;
            e.1 = e.1.max(keys);
        }
    }
    Ok(by
        .into_iter()
        .map(
            |((columns, j), (runs, keys_max))| bellwether_profile::lints::SemiJoinUse {
                columns,
                right: j.right,
                right_columns: j.right_columns,
                runs,
                keys_max,
            },
        )
        .collect())
}

/// The semi-join run a `semijoin` request records (B3): its right side
/// from [`WhyRequest::join`]. `None` for another mode.
fn semijoin_query(req: &WhyRequest) -> Result<Option<RunQuery>> {
    match (req.mode, &req.join) {
        (Some(LookupMode::SemiJoin), Some(j)) => {
            let (right, right_columns) = crate::spec::parse_join_side(j)?;
            Ok(Some(RunQuery::SemiJoin(RunJoin {
                right,
                right_columns,
            })))
        }
        _ => Ok(None),
    }
}

/// Refuses a text (B5) or membership (B4) `mode` without the values it
/// prices: a key file of one string or list column's values, or a recorded
/// workload.
fn check_values_given(req: &WhyRequest, mode: LookupMode) -> Result<()> {
    if mode.value_query().is_none() || req.keys.is_some() || req.workload.is_some() {
        return Ok(());
    }
    let (kind, also) = if mode.is_membership() {
        ("list", "")
    } else {
        (
            "string",
            ", or give the lookups themselves with --contains, --has, or --prefix COL=TEXT",
        )
    };
    Err(Error::KeySpec(format!(
        "--mode {} prices a key file of one {kind} column's values (named after the column); \
         pass --from <values.parquet> (or --workload <hash>){also}",
        mode.as_str()
    )))
}

/// Refuses a recorded workload `w` priced under a text (B5) or membership
/// (B4) `mode` when a run records another lookup: its costs would be
/// labelled as the mode's.
fn check_value_runs(h: &Hash, w: &Workload, mode: Option<LookupMode>) -> Result<()> {
    let want = mode.and_then(LookupMode::value_query);
    let Some(r) = w.runs().iter().find(|r| r.query().ok() != want) else {
        return Ok(());
    };
    let name = mode.map_or("", LookupMode::as_str);
    // The mode's own run kind (`contains_all`, `has_token`), so a refusal
    // between two modes of one family says which.
    let asks = want.as_ref().map_or("", RunQuery::kind_str);
    Err(Error::KeySpec(format!(
        "workload {h} records its runs as {} lookups, and --mode {name} asks for {asks} \
         lookups; drop --mode to price them as recorded, or record the values again with \
         `bellwether why --from <values.parquet> --mode {name}`",
        describe(&r.query()?),
    )))
}

/// The list-membership detector's lints (B4) and the inputs they cite.
#[derive(Default)]
struct MembershipLints {
    lints: Vec<bellwether_types::Lint>,
    inputs: Vec<Hash>,
}

/// A list key's `why` column (B4): the distinct element values of its
/// column, exact, counted over the transient index the candidate was priced
/// with (an element in several segments counts once).
fn list_column(replayer: &Replayer<'_>, spec: &KeySpec) -> Result<bellwether_types::KindColumn> {
    let index = replayer.transient(spec)?;
    let distinct = bellwether_core::list::distinct_elements(index.segments().readers())?;
    Ok(bellwether_types::KindColumn {
        kind: "list".into(),
        label: "distinct elements".into(),
        value: distinct.to_string(),
        exactness: bellwether_types::Exactness::Exact,
    })
}

/// A run's lookup in words, for refusals: `locate`, `latest`, `asof at 30`,
/// either with `, lookback 3600` (in the timestamp column's unit), or
/// `window 10..=20 of ts`.
fn describe(q: &RunQuery) -> String {
    let base = match (q.asof_ts(), q.window()) {
        (Some(ts), _) => format!("asof at {ts}"),
        (None, Some(w)) => format!("window {}..={} of {}", w.lo, w.hi, w.ts_column),
        (None, None) => q.kind_str().to_string(),
    };
    match q.lookback() {
        Some(l) => format!("{base}, lookback {l}"),
        None => base,
    }
}

/// The lint rules that name a column a lookup should not rely on, in rule
/// catalog order.
const CULPRIT_RULES: [&str; 2] = ["batch_stamp_key", "low_selectivity_single_column"];

/// The profile's fanout of exactly `columns`, in that order or another.
fn fanout_of(profile: &ProfileReport, columns: &[String]) -> Option<Fanout> {
    let exact = profile
        .fanout
        .iter()
        .find(|e| e.fanout.columns == columns)
        .map(|e| e.fanout.clone());
    exact.or_else(|| {
        let want: BTreeSet<&String> = columns.iter().collect();
        profile
            .fanout
            .iter()
            .find(|e| e.fanout.columns.iter().collect::<BTreeSet<_>>() == want)
            .map(|e| e.fanout.clone())
    })
}

/// Whether every run of `w` is a text run (a mixed workload is refused
/// before any culprit is named).
fn is_text_workload(w: &Workload) -> Result<bool> {
    for r in w.runs() {
        if r.query()?.text_op().is_none() {
            return Ok(false);
        }
    }
    Ok(!w.runs().is_empty())
}

/// The culprit of a text workload: per column its runs search (the first
/// one; the report names one culprit), each match they ask for and the
/// complete text keys that serve it ([`CulpritReason::Text`]).
fn text_culprit(current: &[KeySpec], w: &Workload) -> Result<Option<Culprit>> {
    let Some(u) = crate::text::text_uses(w)?.into_iter().next() else {
        return Ok(None);
    };
    let matches = [TextOp::Contains, TextOp::HasToken, TextOp::HasPrefix]
        .into_iter()
        .filter(|op| u.runs(*op) > 0)
        .map(|op| TextMatch {
            operator: op,
            runs: u.runs(op),
            served_by: current
                .iter()
                .filter(|s| bellwether_core::text::serves(s, &u.column, op))
                .cloned()
                .collect(),
        })
        .collect();
    Ok(Some(Culprit {
        columns: vec![u.column],
        fanout: None,
        reason: CulpritReason::Text { matches },
    }))
}

/// The layout culprit documented on [`Engine::why`]: `None` for text
/// lookups, and unless the
/// floor was priced (a candidate exact key on a run's own columns, or a
/// current spec serving the first run) and every priced configuration reads
/// at least [`LAYOUT_BOUND_FRACTION`] of the `runs × indexed_row_groups` row
/// groups the runs span. A dataset of one row group never names the layout.
fn layout_culprit(
    r: &WhyReport,
    w: &Workload,
    indexed_row_groups: u64,
    spread: Option<Fanout>,
) -> Option<Culprit> {
    // A text lookup (B5) that reads nearly every row group under every
    // text key is a term in nearly every row group, which the text key's
    // `why` column says; no clustering of the files changes that.
    if matches!(
        r.workload.as_ref().and_then(|i| i.lookup.as_ref()),
        Some(LookupKind::TextRuns { .. })
    ) {
        return None;
    }
    let current = r.current?;
    let first = w.runs().first()?;
    let columns = first.columns.clone();
    let query = first.query().ok()?;
    // The floor is priced when it is a candidate or a current spec serves
    // the runs: for membership runs (B4), the list key on their column.
    let serves = |s: &KeySpec| {
        if query.is_membership() {
            serves_query(s, &columns, &query)
        } else {
            servable(s, &columns).is_some()
        }
    };
    let floor_priced = r
        .candidates
        .iter()
        .any(|c| c.origin == CandidateOrigin::Workload)
        || r.current_specs.iter().any(serves);
    let row_groups = indexed_row_groups * w.runs().len() as u64;
    // One row group has nothing to prune: no layout could do better.
    if !floor_priced || indexed_row_groups < 2 {
        return None;
    }
    let least_row_groups = r
        .candidates
        .iter()
        .map(|c| c.proposed.row_groups)
        .chain([current.row_groups])
        .min()?;
    #[allow(clippy::cast_precision_loss)]
    if (least_row_groups as f64) < LAYOUT_BOUND_FRACTION * row_groups as f64 {
        return None;
    }
    // Membership runs: the cause is where the queried values sit, measured
    // from the list key itself, not any column's fanout.
    if let Some(values) = spread {
        return Some(Culprit {
            columns,
            fanout: None,
            reason: CulpritReason::ListLayout {
                least_row_groups,
                row_groups,
                values,
            },
        });
    }
    // A run keyed on an expression looks up derived values, which no
    // profiled column holds: no column's fanout explains its layout.
    let expression = matches!(
        columns.as_slice(),
        [c] if bellwether_core::expr::Expr::parse(c).is_ok_and(|e| e.is_call())
    );
    let lowest_fanout = r
        .profile
        .fanout
        .iter()
        .filter(|_| !expression)
        .map(|e| &e.fanout)
        .filter(|f| f.columns.len() == 1)
        .min_by(|a, b| {
            a.row_groups_per_value[0]
                .total_cmp(&b.row_groups_per_value[0])
                .then(a.row_groups_per_value[1].total_cmp(&b.row_groups_per_value[1]))
        })
        .cloned();
    Some(Culprit {
        fanout: fanout_of(&r.profile, &columns),
        columns,
        reason: CulpritReason::Layout {
            least_row_groups,
            row_groups,
            lowest_fanout,
        },
    })
}

/// The culprit rule documented on [`Engine::why`].
fn culprit(
    current_specs: &[KeySpec],
    profile: &ProfileReport,
    workload: Option<&Workload>,
) -> Option<Culprit> {
    let mut used: BTreeSet<&String> = current_specs
        .iter()
        .flat_map(|s| s.columns().iter())
        .collect();
    if let Some(w) = workload {
        used.extend(w.runs().iter().flat_map(|r| r.columns.iter()));
    }
    let lint = profile
        .lints
        .iter()
        .filter_map(|l| {
            let rank = CULPRIT_RULES.iter().position(|r| *r == l.rule)?;
            l.columns
                .iter()
                .any(|c| used.contains(c))
                .then_some((rank, l))
        })
        .min_by(|(ra, a), (rb, b)| b.confidence.total_cmp(&a.confidence).then(ra.cmp(rb)));
    if let Some((_, l)) = lint {
        return Some(Culprit {
            columns: l.columns.clone(),
            fanout: fanout_of(profile, &l.columns),
            reason: CulpritReason::Lint {
                rule: l.rule.clone(),
                confidence: l.confidence,
                message: l.message.clone(),
            },
        });
    }
    let columns = workload?.runs().first()?.columns.clone();
    let query = workload
        .and_then(|w| w.runs().first())
        .and_then(|r| r.query().ok())
        .unwrap_or(RunQuery::Locate);
    let serving = current_specs
        .iter()
        .find(|s| serves_query(s, &columns, &query));
    Some(match serving {
        None => Culprit {
            fanout: fanout_of(profile, &columns),
            columns,
            reason: CulpritReason::NoIndexServes,
        },
        Some(spec) => Culprit {
            columns: spec.columns().to_vec(),
            fanout: fanout_of(profile, spec.columns()),
            reason: CulpritReason::ServingSpec { spec: spec.clone() },
        },
    })
}

#[derive(Clone, Debug)]
struct ResolvedTemporal {
    ts_column: String,
    query: RunQuery,
    lookup: LookupKind,
    lookback: Option<LookbackWindow>,
}

/// Refuses `--latest-by` when the current configuration plans `t`'s lookups
/// on runs keyed on `cols` by another time column (a serving temporal key's,
/// or for a lookback the per-key bounds' or window's): `at` and the lookback
/// are measured in `--latest-by`'s unit, so the prices would not be what the
/// lookup verb reads. The typed invalid argument `latest_by`, naming both
/// columns and the flag to pass.
fn check_planned_column(
    replayer: &Replayer<'_>,
    cols: &[String],
    t: &ResolvedTemporal,
) -> Result<()> {
    let current = replayer.current_specs();
    let Some(planned) = replayer
        .planned_time_column(current, cols, &t.query)?
        .filter(|c| *c != t.ts_column)
    else {
        return Ok(());
    };
    let temporal = current.iter().find(|s| {
        matches!(s, KeySpec::Temporal { ts_column, .. } if *ts_column == planned)
            && matches!(servable(s, cols), Some(Lookup::Point { .. }))
    });
    let source = match (temporal, &t.query) {
        (Some(s), _) => format!("its temporal key {}", label(s)),
        (None, RunQuery::Lookback { asof: None, .. }) => "its per-key bounds".to_string(),
        (None, _) => "the time column a window on these keys takes".to_string(),
    };
    let verb = t.query.kind_str();
    Err(Error::InvalidArgument {
        field: "latest_by",
        reason: format!(
            "the index plans {verb} lookups on ({}) by {planned} ({source}), not by {}, so \
             prices measured on {} would not be what `bellwether {verb}` reads; pass \
             --latest-by {planned}",
            cols.join(", "),
            t.ts_column,
            t.ts_column
        ),
    })
}

/// Validates `req.latest_by` (with `req.at` and `req.lookback`) against the
/// dataset schema before profiling or recording: the column must exist in
/// the dataset and have a type that can order versions (`check_ts_type`:
/// timestamp, date, time, or integer), and `at` and `lookback` must convert
/// exactly to that column's unit. `None` for a `locate` request.
fn resolve_temporal(req: &WhyRequest, schema: &Schema) -> Result<Option<ResolvedTemporal>> {
    if !matches!(
        req.lookup_mode().map_err(Error::KeySpec)?,
        LookupMode::Latest | LookupMode::Asof
    ) {
        return Ok(None);
    }
    let Some(ts_column) = &req.latest_by else {
        return Ok(None);
    };
    let ts_column = ts_column.trim();
    if ts_column.is_empty() {
        return Err(Error::KeySpec(
            "--latest-by needs a timestamp column name; pass --latest-by <column>".into(),
        ));
    }
    let field = schema.field_with_name(ts_column).map_err(|_| {
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        Error::KeySpec(format!(
            "--latest-by column {ts_column:?} is not in the dataset schema (columns: {}); pass an \
             existing timestamp, date, time, or integer column with --latest-by",
            names.join(", ")
        ))
    })?;
    let ts_type = field.data_type();
    check_ts_type(ts_type).map_err(|_| {
        Error::KeySpec(format!(
            "--latest-by column {ts_column:?} has type {ts_type}, which cannot order versions; \
             pass a timestamp, date, time, or integer column with --latest-by"
        ))
    })?;
    let (query, lookup) = match &req.at {
        None => (RunQuery::Latest, LookupKind::Latest),
        Some(at) => {
            let ts = instant_in_unit("at", at, ts_column, ts_type)?;
            (
                RunQuery::Asof { ts },
                LookupKind::Asof {
                    at: at.clone(),
                    ts,
                    ts_column: ts_column.to_string(),
                },
            )
        }
    };
    let lookback = match &req.lookback {
        None => None,
        Some(given) => {
            let lb = Lookback::parse(given)?;
            Some(LookbackWindow {
                given: lb.as_given().to_string(),
                length: lb.in_unit(ts_column, ts_type)?,
            })
        }
    };
    let query = match &lookback {
        None => query,
        Some(l) => RunQuery::Lookback {
            asof: query.asof_ts(),
            lookback: l.length,
        },
    };
    // With --ts, the run is the window `asof --at --lookback --ts` plans,
    // recorded as a window run (`Engine::windowed`), not a temporal lookup.
    if req.asof_window_column().is_some() {
        return Ok(None);
    }
    Ok(Some(ResolvedTemporal {
        ts_column: ts_column.to_string(),
        query,
        lookup,
        lookback,
    }))
}

/// When `--workload <hash>` loads a workload whose runs already record
/// `latest` or `asof`, derive the temporal lookup metadata from the workload
/// and any current temporal spec.
fn infer_workload_temporal(
    workload: &Workload,
    current_specs: &[KeySpec],
) -> Result<Option<ResolvedTemporal>> {
    let Some(first) = workload.runs().first() else {
        return Ok(None);
    };
    let query = first.query()?;
    // A window run reads every row of its keys in a fixed window, and a
    // count run counts every row of its keys: no version is chosen, so
    // there is no temporal lookup to infer. A text run (B5) chooses no
    // version either, nor does a membership run (B4).
    if matches!(
        query,
        RunQuery::Locate
            | RunQuery::Window(_)
            | RunQuery::Count
            | RunQuery::Text(_)
            | RunQuery::ContainsAny
            | RunQuery::ContainsAll
    ) {
        return Ok(None);
    }
    let ts_column = current_specs
        .iter()
        .find_map(|s| match s {
            KeySpec::Temporal { ts_column, .. } => Some(ts_column.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            Error::KeySpec(format!(
                "workload {} records {} runs; pass --latest-by <timestamp column> so `why` knows \
                 which timestamp column to build temporal candidates on",
                workload.hash(),
                first.query_kind
            ))
        })?;
    let lookup = match query.asof_ts() {
        None => LookupKind::Latest,
        Some(ts) => LookupKind::Asof {
            at: ts.to_string(),
            ts,
            ts_column: ts_column.clone(),
        },
    };
    let lookback = query.lookback().map(|length| LookbackWindow {
        given: length.to_string(),
        length,
    });
    Ok(Some(ResolvedTemporal {
        ts_column,
        query,
        lookup,
        lookback,
    }))
}

fn with_query(workload: &Workload, query: &RunQuery) -> Result<Workload> {
    let mut m = workload.manifest().clone();
    for r in &mut m.runs {
        r.query_kind = query.kind_str().to_string();
        r.asof = query.asof_ts();
        r.lookback = query.lookback();
    }
    Workload::new(m)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bellwether_types::Lint;

    fn lint(rule: &str, column: &str, confidence: f64) -> Lint {
        Lint {
            rule: rule.into(),
            columns: vec![column.into()],
            message: format!("{rule} on {column}"),
            confidence,
            site: None,
            fix: None,
        }
    }

    fn profile(lints: Vec<Lint>) -> ProfileReport {
        ProfileReport {
            root: "/data".into(),
            files_total: 1,
            row_groups_total: 1,
            rows_total: 1,
            key_columns: Vec::new(),
            skipped: Vec::new(),
            time_columns: Vec::new(),
            fanout: Vec::new(),
            ranges: Vec::new(),
            time_scatter: Vec::new(),
            lints,
            workload: None,
            manifest: None,
            inputs: Vec::new(),
            memory: bellwether_profile::MemoryReport::default(),
        }
    }

    fn exact(c: &str) -> KeySpec {
        KeySpec::Exact {
            columns: vec![c.into()],
            counts: false,
            bounds: None,
        }
    }

    fn named(c: &Culprit) -> (String, String) {
        let CulpritReason::Lint { rule, .. } = &c.reason else {
            panic!("{c:?}")
        };
        (rule.clone(), c.columns[0].clone())
    }

    /// The highest-confidence culprit lint wins; a lint on a column nothing
    /// uses, or of another rule, never does.
    #[test]
    fn the_highest_confidence_culprit_lint_on_a_used_column_wins() {
        let used = [exact("stamp"), exact("tenant")];
        let p = profile(vec![
            lint("batch_stamp_key", "stamp", 0.6),
            lint("low_selectivity_single_column", "tenant", 0.9),
            lint("batch_stamp_key", "unused", 1.0),
            lint("scattered_time_column", "tenant", 1.0),
        ]);
        let c = culprit(&used, &p, None).unwrap();
        assert_eq!(
            named(&c),
            ("low_selectivity_single_column".into(), "tenant".into())
        );
        let p = profile(vec![
            lint("batch_stamp_key", "stamp", 0.9),
            lint("low_selectivity_single_column", "tenant", 0.6),
        ]);
        assert_eq!(
            named(&culprit(&used, &p, None).unwrap()),
            ("batch_stamp_key".into(), "stamp".into())
        );
    }

    /// At equal confidence the rule catalog order decides: `batch_stamp_key`
    /// before `low_selectivity_single_column`, whichever is listed first.
    #[test]
    fn equal_confidence_culprit_lints_follow_the_rule_catalog_order() {
        let used = [exact("stamp"), exact("tenant")];
        for lints in [
            vec![
                lint("low_selectivity_single_column", "tenant", 0.8),
                lint("batch_stamp_key", "stamp", 0.8),
            ],
            vec![
                lint("batch_stamp_key", "stamp", 0.8),
                lint("low_selectivity_single_column", "tenant", 0.8),
            ],
        ] {
            let c = culprit(&used, &profile(lints), None).unwrap();
            assert_eq!(named(&c), ("batch_stamp_key".into(), "stamp".into()));
        }
    }
}
