//! What a build indexes: key specs, and sidecars beside them (WP21).
//!
//! A **sidecar** is an additive, storage-version-1 segment kind
//! (`bellwether_core::sidecar`): `bounds` (per key, per row group, the min
//! and max of a time column over that key's rows) and `temporal2` (per key,
//! the sorted `(valid, known, row group)` triples of a bitemporal key). The
//! manifest declares them in `schema.sidecars` and lists their segments in
//! `segments[]` with `kind` set and `key_spec` set to the sidecar's name
//! (`bounds:symbol@ts`, `temporal2:id@valid,known`), which never equals a
//! key spec's name. A sidecar is complete, like a key spec, when its
//! segments cover every live row group; completeness is recorded in
//! `key_spec_completeness` under its name.
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
    /// A sidecar (`segments[].kind` = its kind).
    Sidecar(SidecarDef),
    /// A sketch (`segments[].kind` = `sketch`). Never planned with.
    Sketch(SketchSpec),
}

impl Target {
    /// The key its segments and completeness are recorded under: a key
    /// spec's [`KeySpec::identity`] (storage version 2), a sidecar's name.
    pub(crate) fn name(&self) -> String {
        match self {
            Self::Key(k) => k.identity(),
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
            Self::Sidecar(d) => d.name(),
            Self::Sketch(s) => s.name(),
        }
    }

    /// `segments[].kind` for its segments.
    pub(crate) fn kind(&self) -> Option<&'static str> {
        match self {
            Self::Key(_) => None,
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
            Self::Key(k) => k.columns().to_vec(),
            Self::Sidecar(d) => d.columns.iter().chain(&d.time_columns).cloned().collect(),
            Self::Sketch(s) => sketch_columns(s),
        }
    }
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

/// Every key spec, known sidecar, and sketch `m` declares, key specs first:
/// what a build indexes row group by row group. A join key (B3) is not
/// one: it is built whole from both datasets' exact keys
/// (`Builder::refresh_join`), and a build only keeps its segment and its
/// completeness while the left's row groups stay covered ([`declared_joins`]).
pub(crate) fn declared_targets(m: &Manifest) -> Vec<Target> {
    m.schema
        .key_specs
        .iter()
        .filter(|s| !matches!(s, KeySpec::Join { .. }))
        .cloned()
        .map(Target::Key)
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
