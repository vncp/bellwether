//! `index` (the build crate's builder) and `locate`, `latest`, `asof` (its
//! plan builder), each lookup writing an Evidence of kind `plan`.

use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use bellwether_build::{Builder, LiveOutcome, Lookup, PlanOrder, TimedSource};
use bellwether_core::encoding;
use bellwether_core::text;
use bellwether_store::parse_rfc3339;
use bellwether_types::{
    format, Error, Evidence, EvidenceBody, KeySpec, Predicate, Provenance, Record, Result,
    ScanPlan, TextOp, Tokenizer, Tombstone,
};
use bytes::Bytes;

use crate::engine::{folder_changes, guard, root_hint, schema_hint, Engine, FolderChanges, Pinned};
use crate::keys::KeyFile;
use crate::lookback::Lookback;
use crate::results::{
    Cost, CountResult, IndexResult, LivePlan, LookupKind, PlanResult, SidecarFolded, SidecarState,
    SpecState, TextRebuilt, TombstoneDropped, TombstoneRows, TombstoneUnused,
};
use crate::spec::Declaration;
use crate::text::TextQuery;

impl Engine {
    /// Builds or updates the index so it declares `specs` (in addition to
    /// those it already declares), over every file under the data root, in
    /// one manifest commit (build crate, ADR 012, 013). Returns the committed
    /// sequence, the segment counts, and every declared spec's completeness.
    pub fn index(&self, specs: &[KeySpec]) -> Result<IndexResult> {
        let declared: Vec<Declaration> = specs
            .iter()
            .map(|s| Declaration {
                key: Some(s.clone()),
                sketch: None,
            })
            .collect();
        self.index_declared(&declared)
    }

    /// [`Engine::index`] for `index --key` arguments as
    /// [`crate::spec::parse_declaration`] reads them: key specs (per-key
    /// time bounds `exact:symbol@ts` and bitemporal versions
    /// `temporal2:id@valid,known` among them) and the approximate
    /// declarations it reads beside them.
    pub fn index_declared(&self, declared: &[Declaration]) -> Result<IndexResult> {
        guard(|| self.index_inner(declared)).map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    #[allow(clippy::too_many_lines)]
    fn index_inner(&self, declared: &[Declaration]) -> Result<IndexResult> {
        self.require_folder("index")?;
        self.require_current("index")?;
        if declared.is_empty() && self.manifest()?.is_none() {
            return Err(Error::KeySpec(
                "a new index needs at least one key spec; pass --key exact:<column> (or \
                 temporal:<column>@<ts>)"
                    .into(),
            ));
        }
        let specs: Vec<KeySpec> = declared.iter().filter_map(|d| d.key.clone()).collect();
        for s in &specs {
            crate::spec::check(s)?;
        }
        // A join key (B3) is built whole from both datasets' exact keys,
        // after the build, in its own commit (`refresh_joins`).
        let (joins, specs): (Vec<KeySpec>, Vec<KeySpec>) = specs
            .into_iter()
            .partition(|s| matches!(s, KeySpec::Join { .. }));
        if !joins.is_empty() && specs.is_empty() && self.manifest()?.is_none() {
            return Err(Error::KeySpec(format!(
                "{} is built from the exact keys on its columns in both datasets, and {} is not \
                 indexed yet; declare the exact key on its left columns too (--key exact:{})",
                joins[0].name(),
                self.root(),
                joins[0].columns().join(",")
            )));
        }
        let mut options = self.build_options();
        options.sketches = declared.iter().filter_map(|d| d.sketch.clone()).collect();
        let builder = Builder::new(Arc::clone(self.store()?), "");
        let sketches_rebuilt = self.reset_unreadable_sketches(&builder, &options.sketches)?;
        let report = builder.build(self.root(), &specs, &options)?;
        // A join key that cannot be rebuilt once the build committed is
        // reported beside that commit (`JoinFreshness::NotRebuilt`), never
        // in place of it.
        let joins = self.refresh_joins(&joins, report.sequence.is_some())?;
        let pinned = self.pin_required("index")?;
        let m = pinned.manifest();
        Ok(IndexResult {
            root: self.root().to_string(),
            committed: joins
                .iter()
                .any(|j| j.refreshed)
                .then(|| pinned.sequence())
                .or(report.sequence),
            sequence: pinned.sequence(),
            manifest: pinned.hash,
            segments: m.segments.len() as u64,
            segments_written: report.segments_written,
            specs: pinned
                .planner
                .declared_specs()
                .iter()
                .map(|s| SpecState {
                    spec: s.clone(),
                    complete_at: pinned.planner.completeness(s),
                })
                .collect(),
            sidecars: pinned
                .planner
                .declared_sidecars()
                .iter()
                .map(|d| SidecarState {
                    sidecar: d.name(),
                    complete_at: pinned.planner.sidecar_completeness(d),
                })
                .chain(
                    bellwether_build::declared_sketches(m)
                        .iter()
                        .map(|s| SidecarState {
                            sidecar: s.name(),
                            complete_at: bellwether_build::sketch_completeness(m, s),
                        }),
                )
                .collect(),
            unknown_segment_kinds: m.unknown_segment_kinds(),
            sketches_rebuilt,
            specs_repaired: report.specs_repaired,
            files_new: report.files_new,
            files_changed: report.files_changed,
            files_removed: report.files_removed,
            files_unchanged: report.files_unchanged,
            row_groups_read: report.row_groups_read,
            row_groups_assigned: report.row_groups_assigned,
            row_groups_retired: report.row_groups_retired,
            files_filled: report.files_filled,
            upgraded_from: report.upgraded_from,
            sidecars_folded: report
                .sidecars_folded
                .into_iter()
                .map(|(sidecar, into)| SidecarFolded { sidecar, into })
                .collect(),
            kept_legacy: report.sidecars_kept,
            fold_pending: report
                .sidecars_fold_pending
                .into_iter()
                .map(|(sidecar, into)| SidecarFolded { sidecar, into })
                .collect(),
            text_rebuilt: report
                .text_tables_replaced
                .into_iter()
                .map(|t| TextRebuilt {
                    spec: t.spec,
                    case: [t.recorded.case, t.current.case],
                    words: [t.recorded.words, t.current.words],
                })
                .collect(),
            joins,
            tombstones_unused: bellwether_build::unused_tombstones(m)
                .into_iter()
                .map(|u| TombstoneUnused {
                    sidecar: u.sidecar,
                    reason: u.reason,
                })
                .collect(),
            tombstones_dropped: report
                .tombstones_dropped
                .into_iter()
                .map(|d| TombstoneDropped {
                    sidecar: d.sidecar,
                    tombstone: d.tombstone,
                    now: d.now,
                })
                .collect(),
            tombstone_rows: report
                .tombstone_rows
                .into_iter()
                .map(|t| TombstoneRows::of(t.sidecar, t.tombstone, t.marks))
                .collect(),
        })
    }

    /// Plans a lookup of the keys in `keys` with every declared key spec
    /// complete at the current manifest sequence, and writes the plan as an
    /// Evidence of kind `plan`. The plan includes every row group holding a
    /// queried key (invariant 1) and carries an `In` residual (ADR 011).
    pub fn locate(&self, keys: &KeyFile) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Locate, None))
            .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// Plans the newest version of each key, through the one temporal spec
    /// that serves the key columns. The residual carries `NotOlderThan` and
    /// `LatestPerKey` (invariant 5).
    pub fn latest(&self, keys: &KeyFile) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Latest { live: false }, None))
            .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// [`Engine::latest`] keeping only live rows (B8): `arg_max(ts, *) by
    /// key` then `where not retracted`, through the serving temporal key's
    /// declared tombstone (`temporal:id@ts,tombstone=void`). A key whose
    /// newest row is a tombstone, or whose only rows are, answers no row; a
    /// key written again after its tombstone answers the later live row.
    /// The residual ends with [`Predicate::Live`]; the complete tombstone
    /// sidecar prunes the row groups holding no live row at each key's
    /// newest time, and [`PlanResult::live`] says what it pruned, or, when
    /// the sidecar is not complete, that nothing was pruned by tombstones
    /// and which command completes it. A serving key with no tombstone is
    /// [`Error::NotIndexed`] naming the spec to declare.
    pub fn latest_live(&self, keys: &KeyFile) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Latest { live: true }, None))
            .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// Plans the version of each key current at `at`: an RFC 3339 instant
    /// (or a date, `YYYY-MM-DD`, for a date column), or an integer already in
    /// the timestamp column's unit. The conversion to that unit must be
    /// exact. The residual carries `NotOlderThan` and `LatestPerKey`
    /// (invariant 5).
    pub fn asof(&self, keys: &KeyFile, at: &str) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Asof { at, live: false }, None))
            .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// [`Engine::asof`] keeping only live rows (B8), as
    /// [`Engine::latest_live`]: per key, the rows at its newest time at or
    /// before `at`, of which the ones that are not tombstones. Equal to
    /// `asof` followed by the tombstone filter, row for row; the mechanical
    /// guard against reading a retracted value in a backtest.
    pub fn asof_live(&self, keys: &KeyFile, at: &str) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Asof { at, live: true }, None))
            .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// Plans the rows of the keys in `keys` whose time column lies in
    /// `[from, to]` (each an RFC 3339 instant, a `YYYY-MM-DD` date for a
    /// date column, or an integer in the column's unit; converted exactly).
    /// The time column is `ts_column`, or else the one time column that a
    /// complete per-key bounds sidecar (`exact:<keys>@<ts>`) or a range spec
    /// on a timestamp column names. Every bounds sidecar on that column
    /// prunes per key: a row group stays only when the key's own times
    /// there reach into the window. The residual is `In` and `Between`.
    pub fn locate_window(
        &self,
        keys: &KeyFile,
        from: &str,
        to: &str,
        ts_column: Option<&str>,
    ) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Window {
                    from,
                    to,
                    ts_column,
                },
                None,
            )
        })
        .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// The bitemporal as-of: per key, the newest row by valid time among
    /// rows known at or before `known` and valid at or before `valid`, ties
    /// on valid time going to the later known time, through the one
    /// complete bitemporal key serving the key columns (or the pre-M5
    /// `temporal2` sidecar), or, with `clocks` (`(valid column, known
    /// column)`), through the one on those clocks; several serving and no
    /// `clocks` is refused, naming each with the clocks that pick it.
    /// Either bound may be omitted (unbounded); `latest --known K` is
    /// `valid = None`. A null in either clock never wins (ADR 014). The
    /// residual carries both bounds, `NotOlderThan` on each clock, then
    /// `LatestPerKey`. With `live` (B8), only live rows of the winning
    /// version answer, through the bitemporal key's declared tombstone
    /// (`temporal2:id@valid,known,tombstone=void`), as
    /// [`Engine::latest_live`] describes.
    pub fn asof_bitemporal(
        &self,
        keys: &KeyFile,
        valid: Option<&str>,
        known: Option<&str>,
        clocks: Option<(&str, &str)>,
        live: bool,
    ) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Bitemporal {
                    valid,
                    known,
                    clocks,
                    live,
                },
                None,
            )
        })
        .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// Plans the rows of the keys in `keys` in the lookback window ending at
    /// `at`: every row whose time column lies in `[at - lookback, at]`,
    /// both inclusive. `at` is read as [`Engine::asof`] reads it; `lookback`
    /// is a whole number and a calendar unit, `s`, `m`, `h`, `d` (24 hours
    /// of UTC), or `w` (7 days), or a bare number for an integer time
    /// column ([`crate::Lookback`]; trading-day calendars are out of scope).
    /// The time column is `ts_column`, else the timestamp of the one
    /// temporal spec serving the key columns, else the one a window would
    /// take ([`Engine::locate_window`]). The plan is the window's: every row
    /// group holding a key with a row in the window, pruned by per-key time
    /// bounds, a temporal spec's version chains, a range key on the time
    /// column, and partition or path-template values, each when there is
    /// one. The residual is `In` and `Between`.
    pub fn asof_lookback(
        &self,
        keys: &KeyFile,
        at: &str,
        lookback: &str,
        ts_column: Option<&str>,
    ) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Lookback {
                    at: Some(at),
                    lookback,
                    ts_column,
                },
                None,
            )
        })
        .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// Plans every version of each key in `keys` in its own lookback window
    /// `[newest − lookback, newest]`, `newest` being the key's newest
    /// non-null time over live row groups (invariant 5): so a key whose
    /// history ended earlier still has its last `lookback` of versions. The
    /// versions come from the one temporal spec serving the key columns,
    /// else from the one complete per-key bounds sidecar on them
    /// (`exact:<cols>@<ts>`, each key's own time range per row group); with
    /// neither the lookup is [`Error::NotIndexed`]. `lookback` is read as
    /// [`Engine::asof_lookback`] reads it. A key with no non-null time has
    /// no window (ADR 014). The residual is `In` and `NotOlderThan`.
    pub fn latest_lookback(&self, keys: &KeyFile, lookback: &str) -> Result<PlanResult> {
        guard(|| {
            self.plan_lookup(
                keys,
                Mode::Lookback {
                    at: None,
                    lookback,
                    ts_column: None,
                },
                None,
            )
        })
        .map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    /// Plans the rows whose string column matches a text lookup (B5):
    /// `has_token`, `contains`, or `has_prefix`, both sides case-folded,
    /// through the complete text keys on the column that answer it (their
    /// row groups intersected), and writes the plan as an Evidence of kind
    /// `plan`. The residual is `Text`, the match itself; a plan through a
    /// trigram or bloom text key is a candidate set (a planned row group
    /// may hold no match), which [`LookupKind::Text`] says, so the residual
    /// is mandatory. No complete text key answering the lookup is
    /// [`Error::NotIndexed`], naming the key that would.
    pub fn locate_text(&self, query: &TextQuery) -> Result<PlanResult> {
        guard(|| self.plan_text_lookup(query)).map_err(|e| root_hint(self.root(), schema_hint(e)))
    }

    fn plan_text_lookup(&self, query: &TextQuery) -> Result<PlanResult> {
        let verb = "locate";
        let pinned = self.pin_required(verb)?;
        self.refuse_stale(&pinned, verb)?;
        self.check_columns(&[&query.column])?;
        let dt = pinned
            .planner
            .column_types(std::slice::from_ref(&query.column))?
            .remove(0)
            .1;
        text::check_column_type(&query.column, &dt)?;
        let (specs, excluded) = pinned.specs();
        let shape = text::Shape::of(&specs, &query.column, query.op, &query.value)?;
        if shape.serving.is_empty() {
            let pending: Vec<KeySpec> = excluded
                .iter()
                .filter(|s| text::probe(s, &query.column, query.op, &shape.folded).is_some())
                .cloned()
                .collect();
            return Err(if pending.is_empty() {
                Error::NotIndexed {
                    root: self.root().to_string(),
                    operation: verb,
                    needs: Some(crate::spec::label(&text_key_for(&query.column, query.op))),
                }
            } else {
                not_complete_yet(self.root(), verb, &pending)
            });
        }
        let payload = serde_json::to_vec(query)?;
        let key_set = bellwether_types::Hash::of(&payload);
        self.cache()?.put_cached(&key_set, Bytes::from(payload))?;
        let lookup = Lookup::Text {
            column: query.column.clone(),
            op: query.op,
            values: vec![query.value.clone()],
        };
        let (plan, order) = pinned.planner.plan_ordered(&specs, &lookup)?;
        let files_total = pinned.manifest().rowgroup_space.files.len() as u64;
        let row_groups_total = plan.row_groups_total;
        let planned = Cost {
            files: plan.entries.len() as u64,
            row_groups: plan.row_groups_selected,
            bytes: plan.bytes,
        };
        let (ordered_by, unordered_reason) = match order {
            PlanOrder::Ordered { column } => (Some(column), None),
            PlanOrder::Unspecified { reason } => (None, Some(reason)),
        };
        let serving: Vec<KeySpec> = shape.serving.iter().map(|(s, _)| s.clone()).collect();
        let (exact_row_groups, unpruned) = (shape.exact(), !shape.prunes());
        let evidence = self.put(Record::Evidence(plan_evidence(
            &pinned,
            key_set,
            plan,
            self.now(),
        )))?;
        Ok(PlanResult {
            lookup: LookupKind::Text {
                operator: query.op,
                value: query.value.clone(),
                folded: shape.folded,
                column: query.column.clone(),
                serving,
                exact_row_groups,
                unpruned: unpruned.then(|| {
                    format!(
                        "the argument is shorter than {} bytes once case-folded, so no trigram \
                         prunes; every row group is planned",
                        text::TRIGRAM
                    )
                }),
            },
            columns: vec![query.column.clone()],
            keys: 1,
            null_tuples: 0,
            key_set,
            sequence: pinned.sequence(),
            specs,
            excluded_incomplete: excluded,
            sidecars: Vec::new(),
            path_values: None,
            files_total,
            row_groups_total,
            planned,
            ordered_by,
            unordered_reason,
            rows: None,
            projected_bytes: None,
            column_bytes_from_footers: 0,
            evidence,
            live: None,
        })
    }

    /// [`Engine::locate`], or [`Engine::locate_window`] when `window` is
    /// `(from, to, ts_column)`, for a consumer that reads only `columns`:
    /// each plan entry also carries the compressed bytes of each of those
    /// columns over its row groups, and the result their sum
    /// (`projected_bytes`), from the manifest's per-column bytes (storage
    /// version 2; no footer is read). A storage version 1 manifest records
    /// none, so they are read from the planned files' footers and the
    /// result says how many (`column_bytes_from_footers`). A text source's
    /// chunks have no per-column bytes, so its entries count their whole
    /// chunks. `columns` must be distinct dataset columns (the invalid
    /// argument `columns` otherwise).
    pub fn locate_columns(
        &self,
        keys: &KeyFile,
        window: Option<(&str, &str, Option<&str>)>,
        columns: &[String],
    ) -> Result<PlanResult> {
        let mode = match window {
            Some((from, to, ts_column)) => Mode::Window {
                from,
                to,
                ts_column,
            },
            None => Mode::Locate,
        };
        guard(|| self.plan_lookup(keys, mode, Some(columns))).map_err(schema_hint)
    }

    /// `count(*) where (columns) in keys` over the keys in `keys`, exact,
    /// from the per-key counts of the complete spec with counts on exactly
    /// the key file's columns (`exact:<cols>,counts`, or a temporal spec
    /// with counts): rows, row groups, and files holding the keys. Reads the
    /// manifest and the index segments; no data file is opened, but the
    /// folder is checked for files changed since the build, as for every
    /// lookup. Writes nothing. Without such a spec the count is refused
    /// with the `index --key` that adds one.
    pub fn count(&self, keys: &KeyFile) -> Result<CountResult> {
        guard(|| self.count_inner(keys)).map_err(schema_hint)
    }

    fn count_inner(&self, keys: &KeyFile) -> Result<CountResult> {
        let pinned = self.pin_required("count")?;
        self.refuse_stale(&pinned, "count")?;
        let (specs, _) = pinned.specs();
        let (run_keys, null_tuples) = keys.run_keys(pinned.planner.schema())?;
        let columns = run_keys.columns().to_vec();
        let tuples: Vec<Vec<u8>> = run_keys.tuples().map(<[u8]>::to_vec).collect();
        let key_set = bellwether_types::Hash::of(&run_keys.to_payload()?);
        let counted = pinned
            .planner
            .count(&specs, &columns, &tuples)?
            .ok_or_else(|| no_counts(self.root(), &columns, pinned.planner.schema()))?;
        Ok(CountResult {
            columns,
            keys: counted.keys,
            null_tuples,
            key_set,
            sequence: pinned.sequence(),
            counted_by: counted.counted_by.identity(),
            rows: counted.rows,
            row_groups: counted.row_groups,
            row_groups_total: counted.row_groups_total,
            files: counted.files,
            files_total: pinned.manifest().rowgroup_space.files.len() as u64,
        })
    }

    /// Plans the rows whose list column holds at least one (`all` false:
    /// `array_contains`, `has_any`) or every one (`all` true) of the values
    /// in `keys`, a key file of one column named after a list (or map)
    /// column of the dataset, its values of the list's element type. The
    /// plan goes through the one complete list key on that column
    /// (`list:<column>`); without one the lookup is refused with the
    /// `bellwether index` command that declares it. A null value matches
    /// nothing (ADR 014): `any` drops and counts it; `all` is refused, since
    /// no row could hold every value. The residual is
    /// `ContainsAny` or `ContainsAll`, which the reader applies per row.
    pub fn locate_contains(&self, keys: &KeyFile, all: bool) -> Result<PlanResult> {
        guard(|| self.plan_lookup(keys, Mode::Contains { all }, None)).map_err(schema_hint)
    }

    /// Invariant 1 over the folder, not only the index: a file that landed
    /// or changed after the last build would be silently left out of the
    /// plan, so it is refused. A plan pinned to an earlier manifest
    /// (`--at-manifest`) answers over the files that manifest recorded, so
    /// files that arrived since are out of scope by design; a recorded file
    /// that changed or vanished is still refused.
    pub(crate) fn refuse_stale(&self, pinned: &Pinned, verb: &str) -> Result<()> {
        let mut changes = folder_changes(self.root(), pinned, self.format(), &self.excluded())?;
        if self.options().at_manifest.is_some() {
            changes.untracked.clear();
        }
        if changes.is_empty() {
            Ok(())
        } else {
            Err(stale_index(self.root(), verb, &changes))
        }
    }

    /// The pinned manifest for a lookup; on a folder never indexed, a
    /// membership lookup names the key to declare, the list key on the key
    /// file's one column.
    fn pin_for_lookup(
        &self,
        verb: &'static str,
        keys: &KeyFile,
        mode: &Mode<'_>,
    ) -> Result<Pinned> {
        match self.pin_required(verb) {
            Err(Error::NotIndexed {
                root,
                operation,
                needs: None,
            }) if matches!(mode, Mode::Contains { .. }) => {
                let needs = match keys.column_names()?.as_slice() {
                    [c] => Some(format!("list:{c}")),
                    _ => None,
                };
                Err(Error::NotIndexed {
                    root,
                    operation,
                    needs,
                })
            }
            other => other,
        }
    }

    #[allow(clippy::too_many_lines)] // one arm per lookup mode
    fn plan_lookup(
        &self,
        keys: &KeyFile,
        mode: Mode<'_>,
        projection: Option<&[String]>,
    ) -> Result<PlanResult> {
        let verb = match mode {
            Mode::Locate | Mode::Window { .. } | Mode::Contains { .. } => "locate",
            Mode::Latest { .. }
            | Mode::Bitemporal { valid: None, .. }
            | Mode::Lookback { at: None, .. } => "latest",
            Mode::Asof { .. } | Mode::Bitemporal { .. } | Mode::Lookback { .. } => "asof",
        };
        self.require_folder(verb)?;
        let pinned = self.pin_for_lookup(verb, keys, &mode)?;
        self.refuse_stale(&pinned, verb)?;
        let (specs, excluded) = pinned.specs();
        if let Some(columns) = projection {
            check_projection(pinned.planner.schema(), columns)?;
        }
        let (run_keys, null_tuples) = keys.run_keys(pinned.planner.schema())?;
        refuse_null_in_all(&mode, null_tuples)?;
        let columns = run_keys.columns().to_vec();
        refuse_expression_lookup(&columns, &mode, verb, self.root(), pinned.planner.schema())?;
        let tuples: Vec<Vec<u8>> = run_keys.tuples().map(<[u8]>::to_vec).collect();
        let payload = run_keys.to_payload()?;
        let key_set = bellwether_types::Hash::of(&payload);
        self.cache()?.put_cached(&key_set, Bytes::from(payload))?;
        let (lookup, kind) = self.lookup_of(&pinned, &specs, &columns, tuples, mode, verb)?;
        // A membership lookup plans with the list key on its column alone:
        // no other kind answers it.
        let specs = match &kind {
            LookupKind::ContainsAny { column } | LookupKind::ContainsAll { column } => {
                list_key(self.root(), &specs, &excluded, column)?
            }
            _ => specs,
        };
        let not_complete = |e: Error| {
            if specs.is_empty() && !excluded.is_empty() {
                not_complete_yet(self.root(), verb, &excluded)
            } else {
                e
            }
        };
        let (mut plan, order, live) = match live_tombstone(&lookup) {
            None => {
                let (plan, order) = pinned
                    .planner
                    .plan_ordered(&specs, &lookup)
                    .map_err(not_complete)?;
                (plan, order, None)
            }
            Some(tombstone) => {
                let (plan, order, outcome) = pinned
                    .planner
                    .plan_ordered_live(&specs, &lookup)
                    .map_err(not_complete)?;
                let live = live_plan(self.root(), &pinned, tombstone, outcome);
                (plan, order, Some(live))
            }
        };
        if matches!(
            kind,
            LookupKind::Latest | LookupKind::Asof { .. } | LookupKind::Bitemporal { .. }
        ) {
            check_temporal_residual(&plan, live.is_some())?;
        }
        let column_bytes_from_footers = match projection {
            Some(columns) => pinned.planner.project(&mut plan, columns)?,
            None => 0,
        };
        let rows = plan.rows.as_ref().map(|q| q.value);
        let projected_bytes = plan.projected_bytes;
        // The sidecars of an index written before M5 a plan read; per-key
        // bounds and bitemporal versions held by a key spec are among
        // `specs`.
        let sidecars: Vec<String> = pinned
            .planner
            .sidecars_for(&specs, &lookup)
            .iter()
            .filter(|(_, src)| matches!(src, TimedSource::Legacy))
            .map(|(d, _)| d.name())
            .collect();
        let path_values = pinned.planner.path_column_for(&lookup)?;
        let files_total = pinned.manifest().rowgroup_space.files.len() as u64;
        let planned_total = plan.row_groups_total;
        let planned = Cost {
            files: plan.entries.len() as u64,
            row_groups: plan.row_groups_selected,
            bytes: plan.bytes,
        };
        let (ordered_by, unordered_reason) = match order {
            PlanOrder::Ordered { column } => (Some(column), None),
            PlanOrder::Unspecified { reason } => (None, Some(reason)),
        };
        let evidence = self.put(Record::Evidence(plan_evidence(
            &pinned,
            key_set,
            plan,
            self.now(),
        )))?;
        Ok(PlanResult {
            lookup: kind,
            columns,
            keys: run_keys.len() as u64,
            null_tuples,
            key_set,
            sequence: pinned.sequence(),
            specs,
            excluded_incomplete: excluded,
            sidecars,
            path_values,
            files_total,
            row_groups_total: planned_total,
            planned,
            ordered_by,
            unordered_reason,
            rows,
            projected_bytes,
            column_bytes_from_footers,
            evidence,
            live,
        })
    }

    /// The planner's lookup for `mode` over the key `tuples` on `columns`,
    /// and how the result names it.
    fn lookup_of(
        &self,
        pinned: &Pinned,
        specs: &[KeySpec],
        columns: &[String],
        tuples: Vec<Vec<u8>>,
        mode: Mode<'_>,
        verb: &'static str,
    ) -> Result<(Lookup, LookupKind)> {
        let schema = pinned.planner.schema();
        let list_column = columns.iter().find(|c| {
            schema
                .field_with_name(c)
                .is_ok_and(|f| bellwether_core::list::element_type(f.data_type()).is_some())
        });
        if let (Some(c), false) = (list_column, matches!(mode, Mode::Contains { .. })) {
            return Err(Error::InvalidArgument {
                field: "keys",
                reason: format!(
                    "{c:?} is a list column, whose rows hold many values, so a {verb} by its \
                     value is not a lookup the index answers; look its elements up with \
                     `bellwether locate {} --keys <file> --any` (rows holding any of the \
                     values) or `--all` (rows holding every one)",
                    self.root()
                ),
            });
        }
        Ok(match mode {
            Mode::Contains { all } => {
                membership_lookup(self.root(), columns, tuples, list_column.is_some(), all)?
            }
            Mode::Locate => (
                Lookup::Keys {
                    columns: columns.to_vec(),
                    keys: tuples,
                },
                LookupKind::Locate,
            ),
            Mode::Latest { live } => {
                let serving = serving_temporal_spec(self.root(), verb, specs, columns)?;
                let live = live
                    .then(|| declared_tombstone(self.root(), verb, pinned, specs, serving))
                    .transpose()?;
                (
                    Lookup::Latest {
                        columns: columns.to_vec(),
                        keys: tuples,
                        live,
                    },
                    LookupKind::Latest,
                )
            }
            Mode::Asof { at, live } => {
                asof_lookup(self.root(), verb, pinned, specs, columns, tuples, at, live)?
            }
            Mode::Window {
                from,
                to,
                ts_column,
            } => window_lookup(pinned, specs, columns, tuples, from, to, ts_column)?,
            Mode::Lookback {
                at,
                lookback,
                ts_column,
            } => lookback_lookup(
                self.root(),
                verb,
                pinned,
                specs,
                columns,
                tuples,
                at,
                lookback,
                ts_column,
            )?,
            Mode::Bitemporal {
                valid,
                known,
                clocks,
                live,
            } => self.bitemporal_lookup(
                pinned,
                specs,
                columns,
                tuples,
                (valid, known),
                clocks,
                live,
            )?,
        })
    }

    /// The bitemporal lookup on `columns` through the one complete
    /// bitemporal key serving them (or the `temporal2` sidecar of an index
    /// written before M5), on `clocks` when given, with both bounds
    /// converted to their columns' units exactly.
    #[allow(clippy::too_many_arguments)]
    fn bitemporal_lookup(
        &self,
        pinned: &Pinned,
        specs: &[KeySpec],
        columns: &[String],
        tuples: Vec<Vec<u8>>,
        (valid, known): (Option<&str>, Option<&str>),
        clocks: Option<(&str, &str)>,
        live: bool,
    ) -> Result<(Lookup, LookupKind)> {
        let probe = Lookup::Bitemporal {
            columns: columns.to_vec(),
            clocks: clocks.map(|(v, k)| (v.to_string(), k.to_string())),
            keys: Vec::new(),
            valid: None,
            known: None,
            live: None,
        };
        let def = match pinned.planner.sidecars_for(specs, &probe).as_slice() {
            [(one, _)] => one.clone(),
            [] => {
                let (v, k) = clocks.unwrap_or(("<valid>", "<known>"));
                let picked = clocks.map_or(String::new(), |(v, k)| {
                    format!(" on the clocks {v} (valid) and {k} (known)")
                });
                return Err(Error::KeySpec(format!(
                    "no complete bitemporal key{picked} serves a lookup on ({}); declare one \
                     with `bellwether index {} --key temporal2:{}@{v},{k}`",
                    columns.join(", "),
                    self.root(),
                    columns.join(",")
                )));
            }
            many => {
                let defs: Vec<_> = many.iter().map(|(d, _)| d.clone()).collect();
                return Err(bellwether_build::several_bitemporal_keys(columns, &defs));
            }
        };
        let (valid_column, known_column) =
            (def.time_columns[0].clone(), def.time_columns[1].clone());
        let in_unit =
            |field: &'static str, at: Option<&str>, column: &str| -> Result<Option<i64>> {
                at.map(|a| {
                    let t = pinned
                        .planner
                        .column_types(&[column.to_string()])?
                        .remove(0)
                        .1;
                    instant_in_unit(field, a, column, &t)
                })
                .transpose()
            };
        let valid_ts = in_unit("valid", valid, &valid_column)?;
        let known_ts = in_unit("known", known, &known_column)?;
        let live = if live {
            let serving = KeySpec::Bitemporal {
                columns: def.columns.clone(),
                valid_column: valid_column.clone(),
                known_column: known_column.clone(),
                tombstone: None,
            };
            let verb = if valid.is_none() { "latest" } else { "asof" };
            Some(declared_tombstone(
                self.root(),
                verb,
                pinned,
                specs,
                &serving,
            )?)
        } else {
            None
        };
        Ok((
            Lookup::Bitemporal {
                columns: columns.to_vec(),
                clocks: Some((valid_column.clone(), known_column.clone())),
                keys: tuples,
                valid: valid_ts,
                known: known_ts,
                live,
            },
            LookupKind::Bitemporal {
                valid: valid.map(str::to_string),
                known: known.map(str::to_string),
                valid_ts,
                known_ts,
                valid_column,
                known_column,
            },
        ))
    }
}

#[derive(Clone, Copy)]
enum Mode<'a> {
    Locate,
    /// `live` (B8): only live rows, by the serving key's tombstone.
    Latest {
        live: bool,
    },
    Asof {
        at: &'a str,
        live: bool,
    },
    Window {
        from: &'a str,
        to: &'a str,
        ts_column: Option<&'a str>,
    },
    Lookback {
        at: Option<&'a str>,
        lookback: &'a str,
        ts_column: Option<&'a str>,
    },
    Bitemporal {
        valid: Option<&'a str>,
        known: Option<&'a str>,
        /// `(valid column, known column)` of the bitemporal key to plan
        /// through, when the caller names it.
        clocks: Option<(&'a str, &'a str)>,
        live: bool,
    },
    /// A membership lookup on a list column (B4): any of the values, or
    /// every one (`all`).
    Contains {
        all: bool,
    },
}

/// The one complete list key on `column` among `specs`, as the spec list a
/// membership lookup plans with; none is [`Error::NotIndexed`] naming the
/// key it needs (or the incomplete one in `excluded`).
fn list_key(
    root: &str,
    specs: &[KeySpec],
    excluded: &[KeySpec],
    column: &str,
) -> Result<Vec<KeySpec>> {
    let want = KeySpec::List {
        column: column.to_string(),
    };
    if specs.contains(&want) {
        return Ok(vec![want]);
    }
    if excluded.contains(&want) {
        return Err(not_complete_yet(
            root,
            "locate",
            std::slice::from_ref(&want),
        ));
    }
    Err(Error::NotIndexed {
        root: root.to_string(),
        operation: "locate",
        needs: Some(want.name()),
    })
}

/// `locate --all` with a null value: no list holds a null (ADR 014), so no
/// row holds every value; refused rather than dropping the null, which would
/// widen the lookup to the remaining values.
fn refuse_null_in_all(mode: &Mode<'_>, null_tuples: u64) -> Result<()> {
    if matches!(mode, Mode::Contains { all: true }) && null_tuples > 0 {
        return Err(Error::InvalidArgument {
            field: "keys",
            reason: format!(
                "the key file holds {null_tuples} null value(s), and no list holds a null (ADR \
                 014), so no row holds every value; remove the null values for --all, or use \
                 --any, which skips them"
            ),
        });
    }
    Ok(())
}

/// The membership lookup (`locate --any`, or `--all` when `all`) over the
/// key file's one column, which must be a list column (`is_list`).
fn membership_lookup(
    root: &str,
    columns: &[String],
    tuples: Vec<Vec<u8>>,
    is_list: bool,
    all: bool,
) -> Result<(Lookup, LookupKind)> {
    let [column] = columns else {
        return Err(Error::InvalidArgument {
            field: "keys",
            reason: format!(
                "a membership lookup reads one list column, and the key file has \
                 {} ({}); select it with --key <column>",
                columns.len(),
                columns.join(", ")
            ),
        });
    };
    if !is_list {
        return Err(Error::InvalidArgument {
            field: "keys",
            reason: format!(
                "{column:?} is not a list column (List, LargeList, or Map), so \
                 --any and --all do not apply; look its values up with `bellwether \
                 locate {root} --keys <file>`"
            ),
        });
    }
    let column = column.clone();
    Ok(if all {
        (
            Lookup::ContainsAll {
                column: column.clone(),
                values: tuples,
            },
            LookupKind::ContainsAll { column },
        )
    } else {
        (
            Lookup::ContainsAny {
                column: column.clone(),
                values: tuples,
            },
            LookupKind::ContainsAny { column },
        )
    })
}

/// The as-of lookup through the one temporal spec serving `columns`, its
/// instant converted to the timestamp column's unit exactly; with `live`
/// (B8), through that spec's declared tombstone.
#[allow(clippy::too_many_arguments)]
fn asof_lookup(
    root: &str,
    verb: &'static str,
    pinned: &Pinned,
    specs: &[KeySpec],
    columns: &[String],
    tuples: Vec<Vec<u8>>,
    at: &str,
    live: bool,
) -> Result<(Lookup, LookupKind)> {
    let serving = serving_temporal_spec(root, verb, specs, columns)?;
    let live = live
        .then(|| declared_tombstone(root, verb, pinned, specs, serving))
        .transpose()?;
    let ts_column = temporal_ts(serving).to_string();
    let ts_type = pinned
        .planner
        .column_types(std::slice::from_ref(&ts_column))?
        .remove(0)
        .1;
    let ts = instant_in_unit("at", at, &ts_column, &ts_type)?;
    Ok((
        Lookup::AsOf {
            columns: columns.to_vec(),
            keys: tuples,
            ts,
            live,
        },
        LookupKind::Asof {
            at: at.to_string(),
            ts,
            ts_column,
        },
    ))
}

/// The window lookup on `columns`: the time column named or found
/// ([`window_column`]), and both ends converted to its unit exactly.
fn window_lookup(
    pinned: &Pinned,
    specs: &[KeySpec],
    columns: &[String],
    tuples: Vec<Vec<u8>>,
    from: &str,
    to: &str,
    ts_column: Option<&str>,
) -> Result<(Lookup, LookupKind)> {
    let ts_column = match ts_column {
        Some(c) => c.to_string(),
        None => window_column(pinned, specs, columns)?,
    };
    let ts_type = pinned
        .planner
        .column_types(std::slice::from_ref(&ts_column))?
        .remove(0)
        .1;
    let lo = instant_in_unit("window", from, &ts_column, &ts_type)?;
    let hi = instant_in_unit("window", to, &ts_column, &ts_type)?;
    if lo > hi {
        return Err(Error::Other(format!(
            "the window starts at {from} ({ts_column} = {lo}) after it ends at {to} ({hi}); \
             pass --from before --to"
        )));
    }
    Ok((
        Lookup::Window {
            columns: columns.to_vec(),
            keys: tuples,
            ts_column: ts_column.clone(),
            lo,
            hi,
        },
        LookupKind::Window {
            from: from.to_string(),
            to: to.to_string(),
            lo,
            hi,
            ts_column,
        },
    ))
}

/// The time column of a window lookup on `columns` when none is named
/// (`bellwether_build::default_window_column` over the pinned index), or
/// [`no_time_column`].
fn window_column(pinned: &Pinned, specs: &[KeySpec], columns: &[String]) -> Result<String> {
    pinned
        .planner
        .window_column(specs, columns)?
        .ok_or_else(|| no_time_column(columns))
}

/// The refusal of a count on `columns` with no complete spec that counts
/// them: typed, naming the spec that would (each surface adds the command
/// that declares it).
/// An expression key column is answered by an `expr` key, which serves
/// point lookups only: refuse every other lookup before any temporal or
/// window rule would name a key that cannot exist.
fn refuse_expression_lookup(
    columns: &[String],
    mode: &Mode<'_>,
    verb: &str,
    root: &str,
    schema: &arrow::datatypes::Schema,
) -> Result<()> {
    if let ([c], false) = (columns, matches!(mode, Mode::Locate)) {
        if bellwether_core::expr::is_expression(c, schema) {
            return Err(Error::KeySpec(format!(
                "the key column {c} is an expression; an expr key answers point lookups \
                     only, so {} on an expression is not supported; find the \
                     rows holding these values with `bellwether locate {} --keys <key file>`, \
                     or key the lookup on dataset columns",
                if matches!(mode, Mode::Window { .. }) {
                    "a window (--from/--to)".to_string()
                } else {
                    verb.to_string()
                },
                crate::spec::shell_quote(root)
            )));
        }
    }
    Ok(())
}

fn no_counts(root: &str, columns: &[String], schema: &arrow::datatypes::Schema) -> Error {
    let needs = match columns {
        // A count on an expression is answered by an expr key over it that
        // stores counts.
        [c] if bellwether_core::expr::is_expression(c, schema) => {
            let e = bellwether_core::expr::Expr::parse(c).map(|e| {
                let name = bellwether_core::expr::suggested_name(&e, schema);
                format!("expr:{name}={e}:exact,counts")
            });
            e.unwrap_or_else(|_| format!("expr:<name>={c}:exact,counts"))
        }
        _ => format!("exact:{},counts", columns.join(",")),
    };
    Error::NotIndexed {
        root: root.to_string(),
        operation: "count",
        needs: Some(needs),
    }
}

/// Refuses a projection that is not a list of distinct dataset columns:
/// the typed invalid argument `columns`, naming the dataset's columns, so
/// each surface reports it as its own flag or field.
fn check_projection(schema: &arrow::datatypes::Schema, columns: &[String]) -> Result<()> {
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let invalid = |reason: String| Error::InvalidArgument {
        field: "columns",
        reason,
    };
    if columns.is_empty() {
        return Err(invalid(format!(
            "a projection names no column; name one or more of {}",
            names.join(", ")
        )));
    }
    for (i, c) in columns.iter().enumerate() {
        if c.is_empty() {
            return Err(invalid(format!(
                "a projection column name is empty; name one or more of {}",
                names.join(", ")
            )));
        }
        if !names.contains(&c.as_str()) {
            return Err(invalid(format!(
                "the projection column \"{c}\" is not in the dataset; its columns are {}",
                names.join(", ")
            )));
        }
        if columns[..i].contains(c) {
            return Err(invalid(format!(
                "the projection lists the column \"{c}\" twice; list each column once"
            )));
        }
    }
    Ok(())
}

/// The refusal of a window lookup on `columns` with no time column named
/// and none to take: it lists what gives one.
pub(crate) fn no_time_column(columns: &[String]) -> Error {
    Error::KeySpec(format!(
        "a window lookup on ({}) needs a time column: declare per-key bounds with `--key \
         exact:{}@<ts>`, a range key `--key range:<ts>`, or a temporal key `--key \
         temporal:{}@<ts>`, take the time from file paths (--partitioning hive or \
         --path-template with a date placeholder), or name the column with --ts",
        columns.join(", "),
        columns.join(","),
        columns.join(",")
    ))
}

/// The lookback lookup on `columns` (see [`Engine::asof_lookback`] and
/// [`Engine::latest_lookback`]): with `at`, the window `[at - lookback, at]`
/// on the time column; without, each key's own window
/// ([`Lookup::Lookback`]).
#[allow(clippy::too_many_arguments)]
fn lookback_lookup(
    root: &str,
    verb: &'static str,
    pinned: &Pinned,
    specs: &[KeySpec],
    columns: &[String],
    tuples: Vec<Vec<u8>>,
    at: Option<&str>,
    lookback: &str,
    ts_column: Option<&str>,
) -> Result<(Lookup, LookupKind)> {
    let lb = Lookback::parse(lookback)?;
    let column_type = |c: &String| -> Result<DataType> {
        Ok(pinned
            .planner
            .column_types(std::slice::from_ref(c))?
            .remove(0)
            .1)
    };
    let Some(at) = at else {
        // `latest`: the temporal key's timestamp, else the per-key bounds'
        // (the planner reads each key's versions from the same one).
        let ts_column = pinned
            .planner
            .lookback_column(specs, columns)?
            .ok_or_else(|| Error::NotIndexed {
                root: root.to_string(),
                operation: verb,
                needs: Some(format!("temporal:{}@<timestamp column>", columns.join(","))),
            })?;
        let n = lb.in_unit(&ts_column, &column_type(&ts_column)?)?;
        return Ok((
            Lookup::Lookback {
                columns: columns.to_vec(),
                keys: tuples,
                lookback: n,
            },
            LookupKind::Lookback {
                at: None,
                lookback: lb.as_given().to_string(),
                lookback_seconds: lb.seconds(),
                end: None,
                lo: None,
                ts_column,
            },
        ));
    };
    let ts_column = if let Some(c) = ts_column {
        c.to_string()
    } else {
        let temporal: Vec<&String> = specs
            .iter()
            .filter_map(|s| match s {
                KeySpec::Temporal {
                    columns: c,
                    ts_column,
                    ..
                } if c.iter().all(|x| columns.contains(x)) => Some(ts_column),
                _ => None,
            })
            .collect();
        match temporal.as_slice() {
            [one] => (*one).clone(),
            [] => window_column(pinned, specs, columns)?,
            _ => serving_temporal(root, verb, specs, columns)?,
        }
    };
    let ts_type = column_type(&ts_column)?;
    let n = lb.in_unit(&ts_column, &ts_type)?;
    let end = instant_in_unit("at", at, &ts_column, &ts_type)?;
    let lo = end.saturating_sub(n).max(encoding::ts_min(&ts_type)?);
    Ok((
        Lookup::Window {
            columns: columns.to_vec(),
            keys: tuples,
            ts_column: ts_column.clone(),
            lo,
            hi: end,
        },
        LookupKind::Lookback {
            at: Some(at.to_string()),
            lookback: lb.as_given().to_string(),
            lookback_seconds: lb.seconds(),
            end: Some(end),
            lo: Some(lo),
            ts_column,
        },
    ))
}

/// The `plan` Evidence: provenance cites the manifest's content hash and the
/// key set's hash, aged by the manifest's commit (invariant 6).
fn plan_evidence(
    pinned: &Pinned,
    key_set: bellwether_types::Hash,
    plan: ScanPlan,
    created_at: String,
) -> Evidence {
    Evidence {
        record_version: format::RECORD_VERSION,
        provenance: Provenance {
            inputs: vec![pinned.hash, key_set],
            oldest_input_age: pinned.age,
            manifest_version: Some(pinned.sequence().to_string()),
            created_at,
        },
        body: EvidenceBody::Plan { plan },
    }
}

/// The text key that answers `op` on `column`: a words dictionary for
/// whole words and word prefixes, trigrams for substrings.
pub(crate) fn text_key_for(column: &str, op: TextOp) -> KeySpec {
    KeySpec::Text {
        column: column.to_string(),
        tokenizer: match op {
            TextOp::HasToken | TextOp::HasPrefix => Tokenizer::Words,
            TextOp::Contains => Tokenizer::Trigram,
        },
        bloom: None,
    }
}

/// The refusal for a folder that no longer matches its index: it names the
/// files and `bellwether index`.
pub(crate) fn stale_index(root: &str, verb: &str, c: &FolderChanges) -> Error {
    let name = |p: &str| {
        std::path::Path::new(p)
            .file_name()
            .map_or_else(|| p.to_string(), |n| n.to_string_lossy().into_owned())
    };
    // One source for the lists the message names and the typed fields carry.
    let (untracked, changed, missing) = c.paths();
    let mut parts = Vec::new();
    let list = |paths: &[String]| {
        let shown: Vec<String> = paths.iter().take(5).map(|p| name(p)).collect();
        let more = paths.len().saturating_sub(5);
        if more > 0 {
            format!("{} and {more} more", shown.join(", "))
        } else {
            shown.join(", ")
        }
    };
    let files = |n: usize| if n == 1 { "file" } else { "files" };
    if !untracked.is_empty() {
        parts.push(format!(
            "{} new {} the index does not cover ({})",
            untracked.len(),
            files(untracked.len()),
            list(&untracked)
        ));
    }
    if !changed.is_empty() {
        parts.push(format!(
            "{} changed {} ({})",
            changed.len(),
            files(changed.len()),
            list(&changed)
        ));
    }
    if !missing.is_empty() {
        parts.push(format!(
            "{} removed {} ({})",
            missing.len(),
            files(missing.len()),
            list(&missing)
        ));
    }
    let message = format!(
        "{root} has {} since its index was built, so {verb} could miss rows (invariant 1); run \
         `bellwether index {root}` first",
        parts.join(" and ")
    );
    Error::StaleIndex {
        untracked,
        changed,
        missing,
        message,
    }
}

/// The lookup's refusal when key specs are declared but none is complete
/// yet: there is nothing to plan with until the index finishes them.
fn not_complete_yet(root: &str, operation: &'static str, excluded: &[KeySpec]) -> Error {
    Error::NotIndexed {
        root: root.to_string(),
        operation,
        needs: Some(
            excluded
                .iter()
                .map(KeySpec::name)
                .collect::<Vec<_>>()
                .join(", "),
        ),
    }
}

/// The timestamp column of the one complete temporal spec whose columns are
/// all key columns. None is [`Error::NotIndexed`]; several is an
/// ambiguous `key_columns`.
fn serving_temporal(
    root: &str,
    operation: &'static str,
    specs: &[KeySpec],
    columns: &[String],
) -> Result<String> {
    serving_temporal_spec(root, operation, specs, columns).map(|s| temporal_ts(s).to_string())
}

/// The one complete temporal spec whose columns are all key columns, as
/// [`serving_temporal`] finds it.
fn serving_temporal_spec<'a>(
    root: &str,
    operation: &'static str,
    specs: &'a [KeySpec],
    columns: &[String],
) -> Result<&'a KeySpec> {
    let mut found = specs.iter().filter(|s| match s {
        KeySpec::Temporal { columns: c, .. } => c.iter().all(|x| columns.contains(x)),
        _ => false,
    });
    match (found.next(), found.next()) {
        (Some(s), None) => Ok(s),
        (None, _) => Err(Error::NotIndexed {
            root: root.to_string(),
            operation,
            needs: Some(format!("temporal:{}@<timestamp column>", columns.join(","))),
        }),
        (Some(_), Some(_)) => Err(Error::InvalidArgument {
            field: "key_columns",
            reason: format!(
                "more than one temporal key spec serves a lookup on {columns:?}; name the key \
                 columns of exactly one"
            ),
        }),
    }
}

/// The timestamp column of a temporal spec (empty for any other).
fn temporal_ts(spec: &KeySpec) -> &str {
    match spec {
        KeySpec::Temporal { ts_column, .. } => ts_column,
        _ => "",
    }
}

/// The tombstone a `live` lookup (B8) through `serving` (the temporal or
/// bitemporal key that plans it, as usable at the pinned sequence) keeps
/// live rows by: the one the index declares on that key. A key whose
/// tombstone sidecar is not complete plans without it (its own segments
/// are complete), and the tombstone still comes from the declaration, so
/// the residual drops tombstone rows and [`live_plan`] says nothing was
/// pruned by them. A key declared without a tombstone is
/// [`Error::NotIndexed`], naming the spec to declare; when the index holds
/// a tombstone declaration on the key's columns this build does not use
/// (`bellwether_build::unused_tombstones`), the refusal names it, says
/// why, and gives the `index` command that declares one it uses.
fn declared_tombstone(
    root: &str,
    verb: &'static str,
    pinned: &Pinned,
    specs: &[KeySpec],
    serving: &KeySpec,
) -> Result<Tombstone> {
    let plain = serving.without_tombstone();
    let declared = specs
        .iter()
        .chain(pinned.planner.declared_specs().iter())
        .find_map(|s| {
            (s.without_tombstone() == plain)
                .then(|| s.tombstone().cloned())
                .flatten()
        });
    if let Some(t) = declared {
        return Ok(t);
    }
    let operation = if verb == "latest" {
        "a live latest"
    } else {
        "a live asof"
    };
    let unused = bellwether_build::unused_tombstones(pinned.manifest());
    let Some(u) = unused.iter().find(|u| u.columns == plain.columns()) else {
        return Err(Error::NotIndexed {
            root: root.to_string(),
            operation,
            needs: Some(format!("{},tombstone=<column>", plain.identity())),
        });
    };
    let spelling = u
        .tombstone
        .as_ref()
        .map_or_else(|| "tombstone=<column>".to_string(), Tombstone::spelling);
    let replaces = u
        .tombstone
        .as_ref()
        .and_then(|t| plain.with_tombstone(t.clone()))
        .is_some_and(|s| bellwether_build::tombstone_name(&s) == u.sidecar);
    Err(Error::KeySpec(format!(
        "the index declares the tombstone {}, which this build does not use: {}; so {operation} \
         through {} has no tombstone to keep live rows by, and nothing was planned. Declare a \
         tombstone on {}: `bellwether index {} --key '{},{spelling}'`{}",
        u.sidecar,
        u.reason,
        plain.identity(),
        plain.identity(),
        crate::spec::shell_quote(root),
        plain.identity(),
        if replaces {
            ", which replaces that declaration"
        } else {
            ""
        }
    )))
}

/// The tombstone of a `live` lookup (B8), if it is one.
fn live_tombstone(lookup: &Lookup) -> Option<Tombstone> {
    match lookup {
        Lookup::Latest { live, .. }
        | Lookup::AsOf { live, .. }
        | Lookup::Bitemporal { live, .. } => live.clone(),
        _ => None,
    }
}

/// What a `live` lookup's tombstones did, for [`PlanResult::live`]: the
/// sidecar that pruned, or why none did and the command that fixes it.
fn live_plan(root: &str, pinned: &Pinned, tombstone: Tombstone, o: LiveOutcome) -> LivePlan {
    let unpruned = o.pruned_by.is_none().then(|| {
        format!(
            "the key's tombstone sidecar is not complete at manifest sequence {}: row groups \
             were added after it was built (by a binary without tombstones, or an index run \
             that has not finished it), so tombstones pruned nothing and the plan reads every \
             row group holding a key's newest version; the residual still drops every \
             tombstone row, so the rows are exact. Run `bellwether index {}` to complete it",
            pinned.sequence(),
            crate::spec::shell_quote(root)
        )
    });
    LivePlan {
        filter: tombstone.live_sql(
            &pinned
                .planner
                .schema()
                .field_with_name(&tombstone.column)
                .map_or(DataType::Null, |f| f.data_type().clone()),
        ),
        tombstone,
        pruned_by: o.pruned_by,
        unpruned,
        keys_with_version: o.keys_with_version,
        keys_without_row_groups: o.keys_without_row_groups,
        key_row_group_pairs_pruned: o.key_row_group_pairs_pruned,
    }
}

/// Invariant 5 at the boundary: a temporal plan's residual must apply
/// `NotOlderThan` before `LatestPerKey`, or a superseded row could be
/// returned; a `live` plan's (B8) must apply `Live` after `LatestPerKey`,
/// or an older live row could stand in for a newest tombstone. A plan
/// without them in that order is refused, never passed on.
fn check_temporal_residual(plan: &ScanPlan, live: bool) -> Result<()> {
    let ok = match &plan.residual {
        Some(Predicate::And { all }) => {
            let not_older = all
                .iter()
                .position(|p| matches!(p, Predicate::NotOlderThan { .. }));
            let latest = all
                .iter()
                .position(|p| matches!(p, Predicate::LatestPerKey { .. }));
            let tombstones = all.iter().position(|p| matches!(p, Predicate::Live { .. }));
            matches!((not_older, latest), (Some(a), Some(b)) if a < b)
                && match (live, latest, tombstones) {
                    (false, _, None) => true,
                    (true, Some(b), Some(c)) => b < c,
                    _ => false,
                }
        }
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(Error::Other(format!(
            "the planner returned a temporal plan without NotOlderThan then LatestPerKey (then \
             Live, for a live lookup) in its residual (invariant 5); this is a bug, please \
             report it at {}",
            crate::engine::ISSUE_TRACKER
        )))
    }
}

/// `at` in `ts_type`'s unit, exactly: an integer is taken as already in the
/// unit; an RFC 3339 instant (or a `YYYY-MM-DD` date) is converted, and a
/// remainder below the unit is an error, never rounded. `field` names the
/// request field the instant came from, for [`Error::InvalidArgument`].
pub(crate) fn instant_in_unit(
    field: &'static str,
    at: &str,
    ts_column: &str,
    ts_type: &DataType,
) -> Result<i64> {
    let at = at.trim();
    if let Ok(n) = at.parse::<i64>() {
        return Ok(n);
    }
    let integer_column = matches!(
        ts_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    );
    if integer_column {
        return Err(Error::InvalidArgument {
            field,
            reason: format!(
                "{ts_column} is an integer column ({ts_type}), so the instant must be an integer \
                 in its unit, not {at:?}"
            ),
        });
    }
    let instant = if at.len() == 10 && at.as_bytes().get(4) == Some(&b'-') {
        format!("{at}T00:00:00Z")
    } else {
        at.to_string()
    };
    let parsed = parse_rfc3339(&instant).map_err(|_| Error::InvalidArgument {
        field,
        reason: format!(
            "the instant {at:?} is neither an RFC 3339 instant (2026-03-03T00:00:00Z), a date \
             (2026-03-03), nor an integer in {ts_column}'s unit"
        ),
    })?;
    let nanos = i128::from(parsed.timestamp()) * 1_000_000_000
        + i128::from(parsed.timestamp_subsec_nanos());
    let per_unit: i128 = match ts_type {
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Date64 => 1_000_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
        DataType::Date32 => 86_400 * 1_000_000_000,
        other => {
            return Err(Error::KeySpec(format!(
                "{ts_column} has type {other}, which a time lookup cannot order by; use a \
                 timestamp, date, or integer column"
            )))
        }
    };
    if nanos % per_unit != 0 {
        return Err(Error::InvalidArgument {
            field,
            reason: format!(
                "the instant {at:?} is finer than {ts_column}'s unit ({ts_type}); give an \
                 instant that is a whole number of that unit"
            ),
        });
    }
    i64::try_from(nanos / per_unit).map_err(|_| Error::InvalidArgument {
        field,
        reason: format!("the instant {at:?} is out of range for {ts_column} ({ts_type})"),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_temporal_residual_without_not_older_than_before_latest_is_refused() {
        let c = vec!["id".to_string()];
        let not_older = Predicate::NotOlderThan {
            columns: c.clone(),
            ts_column: "ts".into(),
            bounds: vec![],
        };
        let latest = Predicate::LatestPerKey {
            columns: c.clone(),
            ts_column: "ts".into(),
        };
        let plan = |residual: Option<Predicate>| ScanPlan {
            manifest_version: "1".into(),
            entries: vec![],
            residual,
            bytes: 0,
            row_groups_total: 0,
            row_groups_selected: 0,
            projected_bytes: None,
            order: None,
            rows: None,
        };
        let and = |all: Vec<Predicate>| Some(Predicate::And { all });
        let live = Predicate::Live {
            column: "void".into(),
            tombstone: vec![0x01, 0x01],
        };
        assert!(check_temporal_residual(
            &plan(and(vec![not_older.clone(), latest.clone()])),
            false
        )
        .is_ok());
        // A live lookup (B8): `Live` after `LatestPerKey`, never before or
        // missing; a plain lookup carries none.
        assert!(check_temporal_residual(
            &plan(and(vec![not_older.clone(), latest.clone(), live.clone()])),
            true
        )
        .is_ok());
        for (bad, is_live) in [
            (
                and(vec![not_older.clone(), live.clone(), latest.clone()]),
                true,
            ),
            (and(vec![not_older.clone(), latest.clone()]), true),
            (
                and(vec![not_older.clone(), latest.clone(), live.clone()]),
                false,
            ),
        ] {
            assert!(check_temporal_residual(&plan(bad), is_live).is_err());
        }
        for bad in [
            and(vec![latest.clone(), not_older.clone()]),
            and(vec![latest.clone()]),
            and(vec![not_older]),
            Some(latest),
            None,
        ] {
            assert!(check_temporal_residual(&plan(bad), false).is_err());
        }
    }

    #[test]
    fn instants_convert_exactly_or_are_refused() {
        let us = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            instant_in_unit("at", "1970-01-01T00:00:01Z", "ts", &us).unwrap(),
            1_000_000
        );
        assert_eq!(instant_in_unit("at", "42", "ts", &us).unwrap(), 42);
        let s = DataType::Timestamp(TimeUnit::Second, None);
        assert!(instant_in_unit("at", "1970-01-01T00:00:01.5Z", "ts", &s).is_err());
        assert_eq!(
            instant_in_unit("at", "1970-01-03", "d", &DataType::Date32).unwrap(),
            2
        );
        assert!(instant_in_unit("at", "1970-01-03T01:00:00Z", "d", &DataType::Date32).is_err());
        assert!(instant_in_unit("at", "yesterday", "ts", &us).is_err());
        assert!(instant_in_unit("at", "2026-01-01T00:00:00Z", "v", &DataType::Int64).is_err());
        assert!(instant_in_unit("at", "2026-01-01T00:00:00Z", "v", &DataType::Utf8).is_err());
    }
}
