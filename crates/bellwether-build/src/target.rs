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
//! The reader rule for kinds this binary does not know: a segment whose
//! `kind` is unknown is never planned with, merged, or counted toward
//! coverage; a build keeps it unchanged, and so does a declaration of an
//! unknown kind; both are reported (`Manifest::unknown_segment_kinds`).

use bellwether_core::sidecar::{SidecarDef, SidecarKind};
use bellwether_store::{Extra, Manifest, SegmentRef, SidecarDecl, KNOWN_SEGMENT_KINDS};
use bellwether_types::KeySpec;

/// A key spec or a sidecar: one thing a build indexes, with its own
/// segments and completeness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    /// A key spec (`segments[].kind` absent).
    Key(KeySpec),
    /// A sidecar (`segments[].kind` = its kind).
    Sidecar(SidecarDef),
}

impl Target {
    /// The name its segments and completeness are recorded under.
    pub(crate) fn name(&self) -> String {
        match self {
            Self::Key(k) => k.name(),
            Self::Sidecar(d) => d.name(),
        }
    }

    /// `segments[].kind` for its segments.
    pub(crate) fn kind(&self) -> Option<&'static str> {
        match self {
            Self::Key(_) => None,
            Self::Sidecar(d) => Some(d.kind.as_str()),
        }
    }

    /// Every column whose type a segment records: key columns, then the
    /// temporal timestamp or the sidecar's time columns.
    pub(crate) fn typed_columns(&self) -> Vec<String> {
        match self {
            Self::Key(KeySpec::Temporal {
                columns, ts_column, ..
            }) => {
                let mut c = columns.clone();
                c.push(ts_column.clone());
                c
            }
            Self::Key(k) => k.columns().to_vec(),
            Self::Sidecar(d) => d.columns.iter().chain(&d.time_columns).cloned().collect(),
        }
    }
}

/// The manifest declaration of a sidecar.
pub(crate) fn decl_of(def: &SidecarDef) -> SidecarDecl {
    SidecarDecl {
        kind: def.kind.as_str().to_string(),
        columns: def.columns.clone(),
        time_columns: def.time_columns.clone(),
        extra: Extra::new(),
    }
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

/// Every key spec and known sidecar `m` declares, key specs first.
pub(crate) fn declared_targets(m: &Manifest) -> Vec<Target> {
    m.schema
        .key_specs
        .iter()
        .cloned()
        .map(Target::Key)
        .chain(declared_sidecars(m).into_iter().map(Target::Sidecar))
        .collect()
}

/// Whether `seg` has a kind this binary does not know (kept, never used).
pub(crate) fn unknown_kind(seg: &SegmentRef) -> bool {
    seg.kind
        .as_deref()
        .is_some_and(|k| !KNOWN_SEGMENT_KINDS.contains(&k))
}
