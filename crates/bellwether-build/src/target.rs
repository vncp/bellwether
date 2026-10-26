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

use std::collections::BTreeMap;

use bellwether_core::sidecar::{SidecarDef, SidecarKind};
use bellwether_store::{Extra, Manifest, SegmentRef, SidecarDecl, KNOWN_SEGMENT_KINDS};
use bellwether_types::{KeySpec, SketchSpec, SKETCH_DEFAULT_PRECISION};

/// A key spec or a sidecar: one thing a build indexes, with its own
/// segments and completeness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    /// A key spec (`segments[].kind` absent).
    Key(KeySpec),
    /// The `bounds` segments of a bloom spec with per-key bounds
    /// (`segments[].kind` = `bounds`, `key_spec` = the bloom's identity).
    BloomBounds(KeySpec),
    /// A sidecar declared in `schema.sidecars` by an index written before
    /// M5 (`segments[].kind` = its kind).
    Sidecar(SidecarDef),
    /// A sketch (`segments[].kind` = `sketch`). Never planned with.
    Sketch(SketchSpec),
}

impl Target {
    /// The key its segments and completeness are recorded under: a key
    /// spec's [`KeySpec::identity`] (storage version 2), a sidecar's name.
    pub(crate) fn name(&self) -> String {
        match self {
            Self::Key(k) | Self::BloomBounds(k) => k.identity(),
            Self::Sidecar(d) => d.name(),
            Self::Sketch(s) => s.name(),
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
        }
    }

    /// `segments[].kind` for its segments.
    pub(crate) fn kind(&self) -> Option<&'static str> {
        match self {
            Self::Key(_) => None,
            Self::BloomBounds(_) => Some(SidecarKind::Bounds.as_str()),
            Self::Sidecar(d) => Some(d.kind.as_str()),
            Self::Sketch(_) => Some(SKETCH_KIND),
        }
    }

    /// Every column whose type a segment records: key columns, then the
    /// temporal timestamp or the sidecar's time columns; for an `expr` key,
    /// the dataset columns its expression reads (the virtual column's type
    /// follows from theirs).
    pub(crate) fn typed_columns(&self) -> Vec<String> {
        match self {
            Self::Key(KeySpec::Temporal {
                columns, ts_column, ..
            }) => {
                let mut c = columns.clone();
                c.push(ts_column.clone());
                c
            }
            Self::Key(k @ KeySpec::Expr { .. }) => {
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

/// The targets a key spec's segments are: the spec, and, for a bloom with
/// per-key bounds, its `bounds` segments.
pub(crate) fn targets_of(spec: &KeySpec) -> Vec<Target> {
    let mut out = vec![Target::Key(spec.clone())];
    if bloom_bounds_def(spec).is_some() {
        out.push(Target::BloomBounds(spec.clone()));
    }
    out
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
                    };
                    match out.specs.iter().find(|s| s.name() == spec.name()) {
                        Some(s) if *s == spec => Some(spec.identity()),
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

/// Whether this binary uses `decl` (a known sidecar or a sketch); others are
/// kept unchanged.
pub(crate) fn known_decl(decl: &SidecarDecl) -> bool {
    def_of(decl).is_some() || sketch_of(decl).is_some()
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
    m.schema
        .key_specs
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
