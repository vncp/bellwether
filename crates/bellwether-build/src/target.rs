//! What a build indexes: key specs, the `bounds` segments of a bloom with
//! per-key bounds, sketches, and the sidecars of an index written before M5.
//!
//! Per-key time bounds and bitemporal versions are key specs (storage
//! version 2, M5): `exact:symbol@ts` keeps each key's `[min, max]` per row
//! group in its own value records, `temporal2:id@valid,known` is a
//! `bitemporal` segment. A bloom has no per-key record, so
//! `bloom:symbol@0.01@ts` keeps its bounds in a second kind of segment of
//! its own: the `keyed` layout under the bloom spec, each value record
//! `bitmap | bounds` (core's `SegmentWriter::bloom_bounds_for_dataset`),
//! listed with `kind: "bounds"` under the bloom's identity
//! ([`Target::BloomBounds`]); the bloom is complete when its filters and
//! its bounds both cover every live row group.
//!
//! An index written before M5 declares bounds and bitemporal versions as
//! **sidecars** in `schema.sidecars` (`bounds:symbol@ts` beside the key it
//! rode with, `temporal2:id@valid,known`), their segments listed with
//! `kind` set and `key_spec` set to the sidecar's name. They are read as
//! they were ([`Target::Sidecar`], [`TimedSource::Legacy`]), and the next
//! build with work folds them into key specs ([`fold_legacy`]) and rebuilds
//! those specs.
//!
//! A `sketch` (`bellwether_types::SketchSpec`, `bellwether_core::sketch`) is
//! declared and stored the same way (`schema.sidecars[]` with kind
//! `sketch`, its parameters in `params`), but it is approximate and **no
//! planner path reads it**: [`declared_sidecars`], the only sidecars the
//! planner sees, never yields one. A build indexes it like any target.
//!
//! The reader rule for kinds this binary does not know: a segment whose
//! `kind` is unknown is never planned with, merged, or counted toward
//! coverage; a build keeps it unchanged, and so does a declaration of an
//! unknown kind; both are reported (`Manifest::unknown_segment_kinds`).
//!
//! A `tombstone` declaration (B8) this binary cannot use is held to the
//! same rule ([`unused_tombstones`]): one whose `params` it cannot read, or
//! whose temporal or bitemporal key the index no longer declares on its
//! key and time columns (a binary without tombstones redefined the key),
//! is kept unchanged with its segments, listed by name, and never planned
//! with. Only `Builder::remove_spec` on a spec with a tombstone under that
//! name removes it, and a build that declares a tombstone under its name
//! replaces it and says so (`BuildReport::tombstones_dropped`).

use std::collections::BTreeMap;

use bellwether_core::sidecar::{SidecarDef, SidecarKind};
use bellwether_core::tombstone::TOMBSTONE_KIND;
use bellwether_store::{Extra, Manifest, SegmentRef, SidecarDecl, KNOWN_SEGMENT_KINDS};
use bellwether_types::{KeySpec, SketchSpec, Tombstone, SKETCH_DEFAULT_PRECISION};

/// A key spec or a sidecar: one thing a build indexes, with its own
/// segments and completeness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    /// A key spec (`segments[].kind` absent). Never one with a tombstone:
    /// the spec without it ([`targets_of`]).
    Key(KeySpec),
    /// The `bounds` segments of a bloom spec with per-key bounds
    /// (`segments[].kind` = `bounds`, `key_spec` = the bloom's identity).
    BloomBounds(KeySpec),
    /// A sidecar declared in `schema.sidecars` by an index written before
    /// M5 (`segments[].kind` = its kind).
    Sidecar(SidecarDef),
    /// A sketch (`segments[].kind` = `sketch`). Never planned with.
    Sketch(SketchSpec),
    /// The tombstone sidecar (B8) of a temporal or bitemporal spec with a
    /// tombstone, the spec held whole (`segments[].kind` = `tombstone`,
    /// `key_spec` = [`tombstone_name`]).
    Tombstone(KeySpec),
}

impl Target {
    /// The key its segments and completeness are recorded under: a key
    /// spec's [`KeySpec::identity`] (storage version 2), a sidecar's name.
    pub(crate) fn name(&self) -> String {
        match self {
            Self::Key(k) | Self::BloomBounds(k) => k.identity(),
            Self::Sidecar(d) => d.name(),
            Self::Sketch(s) => s.name(),
            Self::Tombstone(k) => tombstone_name(k),
        }
    }

    /// The slot it occupies in the declaration: a key spec's
    /// [`KeySpec::name`] (one spec per name; another spec of the same name
    /// is a redefinition), a sidecar's name.
    pub(crate) fn slot(&self) -> String {
        match self {
            Self::Key(k) => k.name(),
            Self::BloomBounds(k) => format!("bounds:{}", k.name()),
            Self::Sidecar(d) => d.name(),
            Self::Sketch(s) => s.name(),
            Self::Tombstone(k) => format!("{TOMBSTONE_KIND}:{}", k.name()),
        }
    }

    /// `segments[].kind` for its segments.
    pub(crate) fn kind(&self) -> Option<&'static str> {
        match self {
            Self::Key(_) => None,
            Self::BloomBounds(_) => Some(SidecarKind::Bounds.as_str()),
            Self::Sidecar(d) => Some(d.kind.as_str()),
            Self::Sketch(_) => Some(SKETCH_KIND),
            Self::Tombstone(_) => Some(TOMBSTONE_KIND),
        }
    }

    /// Every column whose type a segment records: key columns, then the
    /// temporal timestamp or the sidecar's time columns; for an `expr` key,
    /// the dataset columns its expression reads (the virtual column's type
    /// follows from theirs); for an `agg` key, its key columns and its
    /// value column; for a tombstone, also the tombstone column.
    pub(crate) fn typed_columns(&self) -> Vec<String> {
        match self {
            Self::Key(KeySpec::Temporal {
                columns, ts_column, ..
            }) => {
                let mut c = columns.clone();
                c.push(ts_column.clone());
                c
            }
            Self::Key(k @ (KeySpec::Expr { .. } | KeySpec::Agg { .. })) => {
                bellwether_core::expr::input_columns(k).unwrap_or_default()
            }
            Self::Key(k) => {
                let mut c = k.columns().to_vec();
                c.extend(bellwether_core::segment::time_columns(k));
                c
            }
            Self::BloomBounds(k) => bloom_bounds_def(k)
                .map(|d| d.columns.into_iter().chain(d.time_columns).collect())
                .unwrap_or_default(),
            Self::Sidecar(d) => d.columns.iter().chain(&d.time_columns).cloned().collect(),
            Self::Sketch(s) => sketch_columns(s),
            Self::Tombstone(k) => {
                let mut c = Self::Key(k.without_tombstone()).typed_columns();
                c.extend(k.tombstone().map(|t| t.column.clone()));
                c
            }
        }
    }
}

/// The per-key bounds a bloom spec with per-key bounds keeps beside its
/// filters, as the definition they serve: `bounds:<columns>@<ts>`. `None`
/// for any other spec.
pub(crate) fn bloom_bounds_def(spec: &KeySpec) -> Option<SidecarDef> {
    match spec {
        KeySpec::Bloom {
            columns,
            bounds: Some(ts),
            ..
        } => Some(SidecarDef {
            kind: SidecarKind::Bounds,
            columns: columns.clone(),
            time_columns: vec![ts.clone()],
        }),
        _ => None,
    }
}

/// The targets a key spec's segments are: the spec (without its tombstone),
/// for a bloom with per-key bounds its `bounds` segments, and for a spec
/// with a tombstone its tombstone sidecar.
pub(crate) fn targets_of(spec: &KeySpec) -> Vec<Target> {
    let mut out = vec![Target::Key(spec.without_tombstone())];
    if bloom_bounds_def(spec).is_some() {
        out.push(Target::BloomBounds(spec.clone()));
    }
    if spec.tombstone().is_some() {
        out.push(Target::Tombstone(spec.clone()));
    }
    out
}

/// The time columns of a temporal (`[ts]`) or bitemporal (`[valid,
/// known]`) spec; empty for any other.
fn clock_columns(spec: &KeySpec) -> Vec<String> {
    match spec {
        KeySpec::Temporal { ts_column, .. } => vec![ts_column.clone()],
        KeySpec::Bitemporal {
            valid_column,
            known_column,
            ..
        } => vec![valid_column.clone(), known_column.clone()],
        _ => Vec::new(),
    }
}

/// The name a tombstone sidecar is declared, listed, and complete under:
/// `tombstone:<key columns>@<time columns>` (`tombstone:id@ts`,
/// `tombstone:id@valid,known`), the shape of every sidecar's name
/// (`SidecarDecl::name`). One per key spec: a key's slot holds one temporal
/// and one bitemporal spec, whose names differ by their time columns.
#[must_use]
pub fn tombstone_name(spec: &KeySpec) -> String {
    tombstone_decl(spec).name()
}

/// The manifest declaration of `spec`'s tombstone sidecar: the key
/// columns, the time columns, and `params` `column` and, for the value
/// form, `value`.
pub(crate) fn tombstone_decl(spec: &KeySpec) -> SidecarDecl {
    let mut params = BTreeMap::new();
    if let Some(t) = spec.tombstone() {
        params.insert("column".to_string(), t.column.clone());
        if let Some(v) = &t.value {
            params.insert("value".to_string(), v.clone());
        }
    }
    SidecarDecl {
        kind: TOMBSTONE_KIND.to_string(),
        columns: spec.columns().to_vec(),
        time_columns: clock_columns(spec),
        params,
        extra: Extra::new(),
    }
}

/// The tombstone a declaration names, when it is a well-formed tombstone
/// declaration (`params` exactly `column` and an optional `value`).
fn tombstone_param(decl: &SidecarDecl) -> Option<Tombstone> {
    if decl.kind != TOMBSTONE_KIND || decl.params.keys().any(|k| k != "column" && k != "value") {
        return None;
    }
    Some(Tombstone {
        column: decl.params.get("column")?.clone(),
        value: decl.params.get("value").cloned(),
    })
}

/// The spec with a tombstone a declaration names: the temporal or
/// bitemporal spec of `specs` on its key and time columns, with the
/// tombstone. `None` for a declaration of another kind, a malformed one,
/// or one whose spec `specs` does not hold.
pub(crate) fn tombstone_of(decl: &SidecarDecl, specs: &[KeySpec]) -> Option<KeySpec> {
    let t = tombstone_param(decl)?;
    specs
        .iter()
        .find(|s| {
            matches!(s, KeySpec::Temporal { .. } | KeySpec::Bitemporal { .. })
                && s.tombstone().is_none()
                && s.columns() == decl.columns.as_slice()
                && clock_columns(s) == decl.time_columns
        })
        .and_then(|s| s.with_tombstone(t))
}

/// A `tombstone` declaration (B8) the index holds that this binary does
/// not use: kept unchanged with its segments, never planned with
/// ([`unused_tombstones`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnusedTombstone {
    /// Its name (`tombstone:id@ts`).
    pub sidecar: String,
    /// Why this binary does not use it.
    pub reason: String,
    /// Its key columns.
    pub columns: Vec<String>,
    /// The tombstone its `params` name (`column`, and `value` when given),
    /// when it names a column; read even when other params are not.
    pub tombstone: Option<Tombstone>,
}

/// The specs `m` declares with the tombstone each one's declaration names,
/// each with the index of that declaration in `schema.sidecars`: the first
/// usable declaration per spec.
fn usable_tombstones(m: &Manifest) -> Vec<(usize, KeySpec)> {
    m.schema
        .key_specs
        .iter()
        .filter_map(|s| {
            m.schema
                .sidecars
                .iter()
                .enumerate()
                .find_map(|(i, d)| tombstone_of(d, std::slice::from_ref(s)).map(|t| (i, t)))
        })
        .collect()
}

/// The key specs `m` declares, each temporal or bitemporal spec with the
/// tombstone its `tombstone` sidecar declares (B8): the specs as declared,
/// where `schema.key_specs` holds each without its tombstone. A
/// declaration this binary cannot use ([`unused_tombstones`]) adds none.
#[must_use]
pub fn declared_key_specs(m: &Manifest) -> Vec<KeySpec> {
    let usable = usable_tombstones(m);
    m.schema
        .key_specs
        .iter()
        .map(|s| {
            usable
                .iter()
                .find(|(_, t)| t.without_tombstone() == *s)
                .map_or_else(|| s.clone(), |(_, t)| t.clone())
        })
        .collect()
}

/// Every `tombstone` declaration of `m` this binary does not use, in
/// declaration order, with why: its `params` are not the ones it reads
/// (`column` and an optional `value`), or the index declares no temporal
/// or bitemporal key on its key and time columns (a binary without
/// tombstones redefined or removed the key), or another declaration under
/// its name is the one used. A build keeps each unchanged with its
/// segments; no planner reads it.
#[must_use]
pub fn unused_tombstones(m: &Manifest) -> Vec<UnusedTombstone> {
    let used: Vec<usize> = usable_tombstones(m).into_iter().map(|(i, _)| i).collect();
    m.schema
        .sidecars
        .iter()
        .enumerate()
        .filter(|(i, d)| d.kind == TOMBSTONE_KIND && !used.contains(i))
        .map(|(_, d)| UnusedTombstone {
            sidecar: d.name(),
            reason: unused_reason(d, &m.schema.key_specs),
            columns: d.columns.clone(),
            tombstone: d.params.get("column").map(|c| Tombstone {
                column: c.clone(),
                value: d.params.get("value").cloned(),
            }),
        })
        .collect()
}

/// Why a `tombstone` declaration is not used ([`unused_tombstones`]).
fn unused_reason(d: &SidecarDecl, specs: &[KeySpec]) -> String {
    let unread: Vec<&str> = d
        .params
        .keys()
        .map(String::as_str)
        .filter(|k| *k != "column" && *k != "value")
        .collect();
    if !unread.is_empty() {
        return format!(
            "it has params this build does not read ({}; this build reads column and value), \
             written by a newer binary",
            unread.join(", ")
        );
    }
    let Some(column) = d.params.get("column") else {
        return "it names no tombstone column (params.column)".into();
    };
    let mut t = format!("tombstone={column}");
    if let Some(v) = d.params.get("value") {
        t.push('=');
        t.push_str(v);
    }
    let on_columns: Vec<&KeySpec> = specs
        .iter()
        .filter(|s| {
            matches!(s, KeySpec::Temporal { .. } | KeySpec::Bitemporal { .. })
                && s.columns() == d.columns.as_slice()
        })
        .collect();
    if let Some(s) = on_columns
        .iter()
        .find(|s| clock_columns(s) == d.time_columns)
    {
        // The declaration reads, and the key is declared: only a first
        // declaration under the same name takes its place.
        return format!(
            "another tombstone declaration under that name is the one {} uses",
            s.identity()
        );
    }
    let was = format!("@{}", d.time_columns.join(","));
    match on_columns.as_slice() {
        [] => format!(
            "it declares {t} for a key on ({}) with time columns {was}, and the index declares \
             no temporal or temporal2 key on ({})",
            d.columns.join(", "),
            d.columns.join(", ")
        ),
        now => format!(
            "it declares {t} for a key on ({}) with time columns {was}, and the index now \
             declares {} on those key columns",
            d.columns.join(", "),
            now.iter()
                .map(|s| s.identity())
                .collect::<Vec<_>>()
                .join(" and ")
        ),
    }
}

/// The names of `m`'s unused tombstone declarations ([`unused_tombstones`])
/// no used declaration shares: the ones whose segments a build keeps
/// unchanged.
pub(crate) fn kept_tombstone_names(m: &Manifest) -> std::collections::BTreeSet<String> {
    let used: std::collections::BTreeSet<String> = usable_tombstones(m)
        .iter()
        .map(|(_, t)| tombstone_name(t))
        .collect();
    unused_tombstones(m)
        .into_iter()
        .map(|u| u.sidecar)
        .filter(|n| !used.contains(n))
        .collect()
}

/// Whether `seg` is a segment of an unused tombstone declaration a build
/// keeps unchanged (`kept`, [`kept_tombstone_names`]).
pub(crate) fn kept_tombstone_segment(
    seg: &SegmentRef,
    kept: &std::collections::BTreeSet<String>,
) -> bool {
    seg.kind.as_deref() == Some(TOMBSTONE_KIND) && kept.contains(&seg.key_spec)
}

/// A tombstone (B8) a build stopped declaring, which it says
/// (`BuildReport::tombstones_dropped`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TombstoneDropped {
    /// The declaration's name (`tombstone:id@ts`).
    pub sidecar: String,
    /// What it declared (`tombstone=void`), or, for a declaration this
    /// binary could not read, why ([`UnusedTombstone::reason`]).
    pub tombstone: String,
    /// The spec the build declares in its place: under the key's name (a
    /// redefinition, `temporal:id@ts2`), or with a tombstone under the
    /// same declaration name (`temporal:id@ts,tombstone=status=VOID`);
    /// `None` when the key was removed (`remove_unlisted`).
    pub now: Option<String>,
}

/// The `tombstone` declarations a build over `base` declaring `declared`
/// commits, and the ones it drops:
///
/// - per declared spec with a tombstone, the base's declaration of exactly
///   that tombstone on that key, unchanged (its unknown fields kept, ADR
///   017), or else a new one ([`tombstone_decl`]);
/// - every base declaration this binary does not use
///   ([`unused_tombstones`]) under a name no declared spec takes, unchanged;
///
/// Dropped: a base tombstone whose spec is redefined or removed, or
/// declared with another tombstone, and an unused declaration whose name a
/// declared tombstone takes.
pub(crate) fn tombstone_decls(
    base: Option<&Manifest>,
    declared: &[KeySpec],
) -> (Vec<SidecarDecl>, Vec<TombstoneDropped>) {
    let base_decls: &[SidecarDecl] = base.map_or(&[], |m| m.schema.sidecars.as_slice());
    let mut decls: Vec<SidecarDecl> = Vec::new();
    for spec in declared.iter().filter(|s| s.tombstone().is_some()) {
        let plain = spec.without_tombstone();
        let kept = base_decls
            .iter()
            .find(|d| tombstone_of(d, std::slice::from_ref(&plain)).as_ref() == Some(spec));
        decls.push(kept.cloned().unwrap_or_else(|| tombstone_decl(spec)));
    }
    let taken: Vec<String> = decls.iter().map(SidecarDecl::name).collect();
    let mut dropped = Vec::new();
    if let Some(m) = base {
        for (_, was) in usable_tombstones(m) {
            if declared.contains(&was) {
                continue;
            }
            let name = tombstone_name(&was);
            let now = declared
                .iter()
                .find(|s| s.name() == was.name())
                .or_else(|| {
                    declared
                        .iter()
                        .find(|s| s.tombstone().is_some() && tombstone_name(s) == name)
                })
                .map(KeySpec::identity);
            dropped.push(TombstoneDropped {
                sidecar: name,
                tombstone: was.tombstone().map(Tombstone::spelling).unwrap_or_default(),
                now,
            });
        }
        let unused = unused_tombstones(m);
        let kept = kept_tombstone_names(m);
        for d in base_decls.iter().filter(|d| d.kind == TOMBSTONE_KIND) {
            let name = d.name();
            if !kept.contains(&name) || decls.contains(d) {
                continue;
            }
            if taken.contains(&name) {
                dropped.push(TombstoneDropped {
                    tombstone: unused
                        .iter()
                        .find(|u| u.sidecar == name)
                        .map(|u| u.reason.clone())
                        .unwrap_or_default(),
                    now: declared
                        .iter()
                        .find(|s| s.tombstone().is_some() && tombstone_name(s) == name)
                        .map(KeySpec::identity),
                    sidecar: name,
                });
            } else {
                decls.push(d.clone());
            }
        }
    }
    (decls, dropped)
}

/// Where a planner reads per-key time bounds or bitemporal versions
/// described by a [`SidecarDef`] (listed by `timed_sources`).
#[derive(Clone, Debug, PartialEq)]
pub enum TimedSource {
    /// A sidecar of an index written before M5, declared in
    /// `schema.sidecars`: its segments of its kind under its name.
    Legacy,
    /// An exact spec with per-key bounds, or a bitemporal spec: the spec's
    /// own segments.
    Spec(KeySpec),
    /// A bloom spec with per-key bounds: its `bounds` segments, listed
    /// under the bloom's identity.
    BloomBounds(KeySpec),
}

/// Every source of per-key time bounds and bitemporal versions `m`
/// declares, each with the [`SidecarDef`] it serves as: exact specs with
/// bounds and bitemporal specs first, then blooms with bounds, then the
/// sidecars of an index written before M5. Where two serve as the same
/// definition (an exact and a bloom with bounds on one key and time
/// column), only the first is listed: they hold the same bounds.
pub(crate) fn timed_sources(m: &Manifest) -> Vec<(SidecarDef, TimedSource)> {
    let mut out: Vec<(SidecarDef, TimedSource)> = Vec::new();
    let mut push = |def: SidecarDef, src: TimedSource| {
        if !out.iter().any(|(d, _)| *d == def) {
            out.push((def, src));
        }
    };
    for spec in &m.schema.key_specs {
        let def = match spec {
            KeySpec::Exact {
                columns,
                bounds: Some(ts),
                ..
            } => SidecarDef {
                kind: SidecarKind::Bounds,
                columns: columns.clone(),
                time_columns: vec![ts.clone()],
            },
            KeySpec::Bitemporal {
                columns,
                valid_column,
                known_column,
                ..
            } => SidecarDef {
                kind: SidecarKind::Bitemporal,
                columns: columns.clone(),
                time_columns: vec![valid_column.clone(), known_column.clone()],
            },
            _ => continue,
        };
        push(def, TimedSource::Spec(spec.clone()));
    }
    for spec in &m.schema.key_specs {
        if let Some(def) = bloom_bounds_def(spec) {
            push(def, TimedSource::BloomBounds(spec.clone()));
        }
    }
    for def in declared_sidecars(m) {
        push(def, TimedSource::Legacy);
    }
    out
}

/// What folding the sidecars of an index written before M5 does
/// ([`fold_legacy`]).
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Fold {
    /// The key specs after the fold.
    pub(crate) specs: Vec<KeySpec>,
    /// Per sidecar that folded, its name and the identity of the key spec
    /// that now holds what it held.
    pub(crate) folded: Vec<(String, String)>,
    /// The sidecars no key spec can take; they stay declared as sidecars,
    /// keep their segments, are built like any target, and still plan.
    pub(crate) kept: Vec<SidecarDef>,
}

/// Folds the sidecars of an index written before M5 into key specs, as the
/// first build with work over it does:
///
/// - `temporal2:<key>@<valid>,<known>` becomes the bitemporal key spec of
///   that identity, or folds into one already declared;
/// - `bounds:<key>@<ts>` folds into a spec on exactly those key columns
///   that already keeps bounds on `ts`, else becomes the per-key bounds of
///   the exact spec on them without bounds, else of the bloom spec on them
///   without bounds, else a new exact spec (`exact:<key>@<ts>`) when no
///   exact spec is on those columns.
///
/// A sidecar that none of these fits (its key spec's slot is held by a spec
/// on another time column, such as `temporal2:sym@ts,known` beside
/// `temporal2:sym@valid,known`, or `bounds:sym@valid` beside
/// `exact:sym@ts`) is never discarded: it is kept as a sidecar
/// ([`Fold::kept`]).
pub(crate) fn fold_legacy(specs: &[KeySpec], sidecars: &[SidecarDef]) -> Fold {
    let mut out = Fold {
        specs: specs.to_vec(),
        ..Fold::default()
    };
    for def in sidecars {
        let into = match def.kind {
            SidecarKind::Bitemporal => match def.time_columns.as_slice() {
                [valid, known] => {
                    let spec = KeySpec::Bitemporal {
                        columns: def.columns.clone(),
                        valid_column: valid.clone(),
                        known_column: known.clone(),
                        tombstone: None,
                    };
                    match out.specs.iter().find(|s| s.name() == spec.name()) {
                        Some(s) if s.without_tombstone() == spec => Some(spec.identity()),
                        Some(_) => None,
                        None => {
                            out.specs.push(spec.clone());
                            Some(spec.identity())
                        }
                    }
                }
                _ => None,
            },
            SidecarKind::Bounds => match def.time_columns.as_slice() {
                [ts] => fold_bounds(&mut out.specs, &def.columns, ts),
                _ => None,
            },
        };
        match into {
            Some(into) => out.folded.push((def.name(), into)),
            None => out.kept.push(def.clone()),
        }
    }
    out
}

/// Folds a `bounds:<columns>@<ts>` sidecar into `specs` ([`fold_legacy`]):
/// the identity of the spec that now keeps those bounds, or `None` when no
/// spec can.
fn fold_bounds(specs: &mut Vec<KeySpec>, columns: &[String], ts: &str) -> Option<String> {
    let on = |s: &KeySpec| s.columns() == columns;
    let bounds_of = |s: &KeySpec| match s {
        KeySpec::Exact { bounds, .. } | KeySpec::Bloom { bounds, .. } => Some(bounds.clone()),
        _ => None,
    };
    // A spec that already keeps these bounds holds what the sidecar held.
    if let Some(s) = specs
        .iter()
        .find(|s| on(s) && bounds_of(s) == Some(Some(ts.to_string())))
    {
        return Some(s.identity());
    }
    let slot = specs
        .iter()
        .position(|s| on(s) && matches!(s, KeySpec::Exact { bounds: None, .. }))
        .or_else(|| {
            specs
                .iter()
                .position(|s| on(s) && matches!(s, KeySpec::Bloom { bounds: None, .. }))
        });
    if let Some(i) = slot {
        if let KeySpec::Exact { bounds, .. } | KeySpec::Bloom { bounds, .. } = &mut specs[i] {
            *bounds = Some(ts.to_string());
        }
        return Some(specs[i].identity());
    }
    let spec = KeySpec::Exact {
        columns: columns.to_vec(),
        counts: false,
        bounds: Some(ts.to_string()),
    };
    if specs.iter().any(|s| s.name() == spec.name()) {
        // The exact spec on these columns keeps bounds on another column.
        return None;
    }
    specs.push(spec.clone());
    Some(spec.identity())
}

/// `segments[].kind` and `schema.sidecars[].kind` of a sketch.
pub(crate) const SKETCH_KIND: &str = "sketch";

/// Every column a sketch reads, each once, in item order.
pub(crate) fn sketch_columns(spec: &SketchSpec) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in spec.items.iter().flatten() {
        if !out.contains(c) {
            out.push(c.clone());
        }
    }
    out
}

/// The manifest declaration of a sidecar.
pub(crate) fn decl_of(def: &SidecarDef) -> SidecarDecl {
    SidecarDecl {
        kind: def.kind.as_str().to_string(),
        columns: def.columns.clone(),
        time_columns: def.time_columns.clone(),
        params: BTreeMap::new(),
        extra: Extra::new(),
    }
}

/// The manifest declaration of a sketch: its items as `columns` (a tuple
/// joined by `+`), no time column, and `params` `p` and, when kept,
/// `minhash`.
pub(crate) fn sketch_decl(spec: &SketchSpec) -> SidecarDecl {
    let mut params = BTreeMap::from([("p".to_string(), spec.precision.to_string())]);
    if let Some(k) = spec.minhash {
        params.insert("minhash".to_string(), k.to_string());
    }
    SidecarDecl {
        kind: SKETCH_KIND.to_string(),
        columns: spec
            .items
            .iter()
            .map(|i| SketchSpec::item_label(i))
            .collect(),
        time_columns: Vec::new(),
        params,
        extra: Extra::new(),
    }
}

/// The sketch a declaration names, or `None` when it is not a sketch or is
/// malformed (kept unchanged, like an unknown kind, and never used).
pub(crate) fn sketch_of(decl: &SidecarDecl) -> Option<SketchSpec> {
    if decl.kind != SKETCH_KIND || !decl.time_columns.is_empty() {
        return None;
    }
    let precision = match decl.params.get("p") {
        Some(p) => p.parse().ok()?,
        None => SKETCH_DEFAULT_PRECISION,
    };
    let minhash = match decl.params.get("minhash") {
        Some(k) => Some(k.parse().ok()?),
        None => None,
    };
    if decl.params.keys().any(|k| k != "p" && k != "minhash") {
        return None;
    }
    let spec = SketchSpec {
        items: decl
            .columns
            .iter()
            .map(|i| i.split('+').map(str::to_string).collect())
            .collect(),
        precision,
        minhash,
    };
    bellwether_core::sketch::validate(&spec).ok()?;
    Some(spec)
}

/// The sketches a manifest declares. No planner path calls this.
#[must_use]
pub fn declared_sketches(m: &Manifest) -> Vec<SketchSpec> {
    m.schema.sidecars.iter().filter_map(sketch_of).collect()
}

/// Whether a build writes `decl` again itself (a known sidecar, a sketch,
/// or a tombstone, B8); others are kept unchanged. Tombstone declarations
/// are written by [`tombstone_decls`], which keeps the base's unchanged
/// when their tombstone is, and keeps the ones this binary does not use.
pub(crate) fn known_decl(decl: &SidecarDecl) -> bool {
    def_of(decl).is_some() || sketch_of(decl).is_some() || decl.kind == TOMBSTONE_KIND
}

/// The sidecar a declaration names, or `None` for a kind this binary does
/// not know.
pub(crate) fn def_of(decl: &SidecarDecl) -> Option<SidecarDef> {
    Some(SidecarDef {
        kind: SidecarKind::parse(&decl.kind)?,
        columns: decl.columns.clone(),
        time_columns: decl.time_columns.clone(),
    })
}

/// The sidecars a manifest declares that this binary knows.
pub(crate) fn declared_sidecars(m: &Manifest) -> Vec<SidecarDef> {
    m.schema.sidecars.iter().filter_map(def_of).collect()
}

/// Every key spec (with a bloom's bounds sidecar), known sidecar, and
/// sketch `m` declares, key specs first: what a build indexes row group by
/// row group. A join key (B3) is not one: it is built whole from both
/// datasets' exact keys (`Builder::refresh_join`), and a build only keeps
/// its segment and its completeness while the left's row groups stay
/// covered ([`declared_joins`]).
pub(crate) fn declared_targets(m: &Manifest) -> Vec<Target> {
    declared_key_specs(m)
        .iter()
        .filter(|s| !matches!(s, KeySpec::Join { .. }))
        .flat_map(targets_of)
        .chain(declared_sidecars(m).into_iter().map(Target::Sidecar))
        .chain(declared_sketches(m).into_iter().map(Target::Sketch))
        .collect()
}

/// The join keys (B3) `specs` declares.
pub(crate) fn declared_joins(specs: &[KeySpec]) -> Vec<KeySpec> {
    specs
        .iter()
        .filter(|s| matches!(s, KeySpec::Join { .. }))
        .cloned()
        .collect()
}

/// Whether `seg` has a kind this binary does not know (kept, never used).
pub(crate) fn unknown_kind(seg: &SegmentRef) -> bool {
    seg.kind
        .as_deref()
        .is_some_and(|k| !KNOWN_SEGMENT_KINDS.contains(&k))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(kind: SidecarKind, time: &[&str]) -> SidecarDef {
        SidecarDef {
            kind,
            columns: vec!["sym".into()],
            time_columns: time.iter().map(|t| (*t).to_string()).collect(),
        }
    }

    /// The PR #30 review's four-sidecar index (`exact:sym` beside
    /// `bounds:sym@ts`, `bounds:sym@valid`, `temporal2:sym@valid,known` and
    /// `temporal2:sym@ts,known`): two fold, and the two whose key spec slot
    /// is taken by a spec on another time column are kept, never discarded
    /// and never reported as folded.
    #[test]
    fn a_sidecar_no_spec_can_take_is_kept_and_only_what_folded_is_reported() {
        let exact = KeySpec::Exact {
            columns: vec!["sym".into()],
            counts: false,
            bounds: None,
        };
        let sidecars = [
            def(SidecarKind::Bounds, &["ts"]),
            def(SidecarKind::Bounds, &["valid"]),
            def(SidecarKind::Bitemporal, &["valid", "known"]),
            def(SidecarKind::Bitemporal, &["ts", "known"]),
        ];
        let fold = fold_legacy(&[exact], &sidecars);
        assert_eq!(
            fold.folded,
            vec![
                ("bounds:sym@ts".to_string(), "exact:sym@ts".to_string()),
                (
                    "temporal2:sym@valid,known".to_string(),
                    "temporal2:sym@valid,known".to_string()
                ),
            ]
        );
        assert_eq!(fold.kept, vec![sidecars[1].clone(), sidecars[3].clone()]);
        let ids: Vec<String> = fold.specs.iter().map(KeySpec::identity).collect();
        assert_eq!(ids, ["exact:sym@ts", "temporal2:sym@valid,known"]);
        // Folding again over what was kept changes nothing and keeps both.
        let again = fold_legacy(&fold.specs, &fold.kept);
        assert_eq!(again.folded, Vec::new());
        assert_eq!(again.kept, fold.kept);
        assert_eq!(again.specs, fold.specs);
    }

    /// A bounds sidecar a spec already keeps (same columns and time column)
    /// folds into it; with only a bloom on another time column, a new exact
    /// spec takes it.
    #[test]
    fn bounds_fold_into_the_spec_that_keeps_them_or_a_new_exact_spec() {
        let exact_ts = KeySpec::Exact {
            columns: vec!["sym".into()],
            counts: true,
            bounds: Some("ts".into()),
        };
        let f = fold_legacy(
            std::slice::from_ref(&exact_ts),
            &[def(SidecarKind::Bounds, &["ts"])],
        );
        assert_eq!(f.folded[0].1, exact_ts.identity());
        assert_eq!(f.specs, vec![exact_ts]);
        let bloom_valid = KeySpec::Bloom {
            columns: vec!["sym".into()],
            fpr: 0.01,
            bounds: Some("valid".into()),
        };
        let f = fold_legacy(&[bloom_valid], &[def(SidecarKind::Bounds, &["ts"])]);
        assert_eq!(f.folded[0].1, "exact:sym@ts");
        assert_eq!(f.kept, Vec::new());
    }
}
