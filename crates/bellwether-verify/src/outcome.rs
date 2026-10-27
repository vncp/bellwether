//! The typed result of a reconciliation. Everything here is data: no
//! formatting beyond exact renderings of keys and decimals, so the facade
//! can serialize it for `--json` or render it for a terminal.

use bellwether_types::{AggServed, Coverage, Hash, Predicate};
use serde::{Deserialize, Serialize};

use crate::totals::ExactTotal;

/// A key tuple: its canonical bytes and a rendering of each element.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRepr {
    /// Lowercase hex of the canonical tuple (ADR 016). This is the identity.
    pub canonical: String,
    /// Each element as Arrow displays it at the dataset type; `None` for a
    /// null element.
    pub values: Vec<Option<String>>,
}

/// A group of the totals: its columns, canonical bytes, and rendering.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRepr {
    /// Lowercase hex of the canonical group tuple (empty without group-by).
    pub canonical: String,
    /// `(column, rendered value)` per group-by column, in spec order.
    pub values: Vec<(String, Option<String>)>,
}

impl GroupRepr {
    /// The certificate rendering of a `(column, group)` pair: canonical JSON
    /// `{"column":<total column>,"group":[[<group column>,<value or null>],...]}`,
    /// with the group-by columns in spec order. Unambiguous for any column
    /// name or value.
    #[must_use]
    pub fn label(&self, column: &str) -> String {
        let group: Vec<serde_json::Value> = self
            .values
            .iter()
            .map(|(c, v)| serde_json::json!([c, v]))
            .collect();
        serde_json::json!({ "column": column, "group": group }).to_string()
    }
}

/// The version of one key on one side that the comparison used.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionDigest {
    /// Rows of this key on this side in scope, whatever their version.
    pub rows: u64,
    /// The newest `latest_by` value (in the column's dataset unit), when a
    /// version column is set and some row has a non-null value.
    pub newest: Option<i64>,
    /// Rows in the compared version: every row without `latest_by`, the
    /// rows at the newest value with it.
    pub compared_rows: u64,
    /// Lowercase hex of the wrapping sum of the compared rows' 128-bit row
    /// hashes: the row hash itself when one row is compared.
    pub hash: String,
}

/// Which sides a condition holds on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sides {
    /// The left side only.
    Left,
    /// The right side only.
    Right,
    /// Both sides.
    Both,
}

/// One difference between the two sides.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Mismatch {
    /// The key has rows on the right and none on the left.
    MissingOnLeft {
        /// The key.
        key: KeyRepr,
        /// The right side's version.
        right: VersionDigest,
    },
    /// The key has rows on the left and none on the right.
    MissingOnRight {
        /// The key.
        key: KeyRepr,
        /// The left side's version.
        left: VersionDigest,
    },
    /// The key is on both sides and the compared versions differ.
    RowHashDiffers {
        /// The key.
        key: KeyRepr,
        /// The left side's version.
        left: VersionDigest,
        /// The right side's version.
        right: VersionDigest,
    },
    /// The key failed to compare: with `latest_by`, every row of the key on
    /// `sides` has a null version value, and a null never wins (ADR 014),
    /// so that side has no version to compare. Counted in `mismatched`.
    NoComparableVersion {
        /// The key.
        key: KeyRepr,
        /// Where no version exists.
        sides: Sides,
        /// The left side's rows (zero if the key is absent there).
        left: VersionDigest,
        /// The right side's rows.
        right: VersionDigest,
    },
    /// A requested total differs for one group.
    TotalDiffers {
        /// The group.
        group: GroupRepr,
        /// The total column.
        column: String,
        /// The left side's exact total.
        left: ExactTotal,
        /// The right side's exact total.
        right: ExactTotal,
    },
}

/// Exact counts. `keys_compared` is the sum of `matched`, `missing_left`,
/// `missing_right`, `mismatched`, and `absent_both`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    /// Distinct keys compared (see [`Reconciliation::coverage`]).
    pub keys_compared: u64,
    /// Keys whose compared versions are equal.
    pub matched: u64,
    /// Keys with rows on the right only (`CertBody::missing_left`).
    pub missing_left: u64,
    /// Keys with rows on the left only (`CertBody::missing_right`).
    pub missing_right: u64,
    /// Keys whose compared versions differ, plus keys that failed to
    /// compare (counted again in `no_comparable_version`). The certificate
    /// counts them apart: its `mismatched` is this less
    /// `no_comparable_version`, and its `uncomparable` is
    /// `no_comparable_version`.
    pub mismatched: u64,
    /// Of `mismatched`, the keys that failed to compare: no comparable
    /// version on some side (the certificate's `uncomparable`).
    pub no_comparable_version: u64,
    /// Workload keys found on neither side; both sides agree they are
    /// absent.
    pub absent_both: u64,
    /// `(group, column)` totals that differ.
    pub totals_differ: u64,
}

/// One group's exact totals for one column on both sides.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupTotal {
    /// The group.
    pub group: GroupRepr,
    /// The total column.
    pub column: String,
    /// Compared rows in this group on the left.
    pub left_rows: u64,
    /// Compared rows in this group on the right.
    pub right_rows: u64,
    /// The left side's exact total (zero when it has no rows here).
    pub left: ExactTotal,
    /// The right side's exact total.
    pub right: ExactTotal,
}

/// How a side's rows were found.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SidePath {
    /// Every row group of every discovered file was read.
    Scan,
    /// Only the row groups an index's plan selected were read (workload
    /// coverage on an indexed side).
    Planned {
        /// The manifest sequence the plan was computed against.
        manifest_sequence: u64,
        /// The hash of the `plan` Evidence recording the plan, which the
        /// certificate cites as a provenance input.
        plan_evidence: Hash,
        /// Row groups the plan selected.
        row_groups_selected: u64,
        /// Live row groups the plan considered.
        row_groups_total: u64,
    },
    /// No row group was read: an index's `agg` key (B7) holds every row's
    /// key and total, so its per-key rows and exact sums stand in for the
    /// rows (every compared column is a key column, and the one total is
    /// the agg key's sum).
    Agg {
        /// The agg key's identity.
        spec: String,
    },
}

/// What was read from one side. Independent of the pass count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideReport {
    /// The root the source was asked about.
    pub root: String,
    /// Files discovered.
    pub files: u64,
    /// The side's catalog digest (`docs/format.md`, "Catalog digest"): the
    /// manifest's when the side is indexed, else the folder's at manifest
    /// version `"0"`. A provenance input.
    pub catalog: Hash,
    /// The manifest's content hash and sequence, when the side is indexed.
    pub manifest: Option<(Hash, u64)>,
    /// How rows were found.
    pub path: SidePath,
    /// Row groups in the read set.
    pub row_groups: u64,
    /// Rows in the read set, from the sources' statistics.
    pub rows: u64,
    /// Rows read whose leading key column is null (counted once, whatever
    /// the pass count; 0 on a side answered from an agg key, which holds
    /// none). No agg key (B7) holds such a row (ADR 014), so a side with
    /// one is always read. Omitted when 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub null_key_rows: u64,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's skip_serializing_if signature
fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// A side's `agg` key (B7) that did not stand in for its rows, and why:
/// the side was read instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggNotUsed {
    /// `left` or `right`.
    pub side: String,
    /// The agg key's identity.
    pub spec: String,
    /// Each reason the agg key could not answer this reconcile.
    pub causes: Vec<String>,
    /// What makes it answer the next one, when anything does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

/// How the memory bound was kept (ADR 018). Depends on the budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryReport {
    /// The budget for comparison state, in bytes.
    pub budget_bytes: u64,
    /// Passes over hash-partitioned key space in the run that completed.
    pub passes: u32,
    /// Times a pass outgrew the budget and the run restarted with twice
    /// the passes.
    pub restarts: u32,
    /// Peak accounted comparison state, in bytes; never above the budget.
    pub peak_state_bytes: u64,
    /// Largest decoded row group of projected columns, in bytes (Arrow's
    /// `get_array_memory_size`); outside the budget, one at a time.
    pub peak_batch_bytes: u64,
    /// Rows read per side over every pass of every attempt.
    pub rows_read: [u64; 2],
}

/// The result of a reconciliation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reconciliation {
    /// True only when every compared key matched, every requested total
    /// matched, and no key failed to compare.
    pub pass: bool,
    /// What was actually compared. A `Sampled` fraction is the realized
    /// fraction of distinct keys, not the requested one.
    pub coverage: Coverage,
    /// Exact counts.
    pub counts: Counts,
    /// The first [`crate::Options::max_examples`] per-key differences in
    /// canonical key order, then every total that differs.
    pub mismatches: Vec<Mismatch>,
    /// Every group's totals for every total column, both sides.
    pub totals: Vec<GroupTotal>,
    /// Key columns.
    pub key_columns: Vec<String>,
    /// The version column, if any.
    pub latest_by: Option<String>,
    /// The row filter, when only some rows of the covered keys were
    /// compared ([`crate::ReconcileSpec::row_filter`]). Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<Vec<Predicate>>,
    /// Hashed columns, in hash order.
    pub hashed_columns: Vec<String>,
    /// Columns left out of the row hash, sorted.
    pub ignored_columns: Vec<String>,
    /// Left side, then right side.
    pub sides: [SideReport; 2],
    /// The memory bound.
    pub memory: MemoryReport,
    /// The sides whose per-key rows and totals an index's `agg` keys
    /// answered instead of a scan (B7); empty when every side was read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agg: Vec<AggServed>,
    /// The sides that hold a complete agg key on the key and a total but
    /// were read anyway, with the reasons and the next step (B7); empty
    /// when no such side exists.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agg_not_used: Vec<AggNotUsed>,
}
