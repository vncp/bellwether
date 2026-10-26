//! A CSV index whose manifest spells its dialect as storage version 1 did,
//! inside `source.kind` (`csv;delimiter=59;header=false`): a build with
//! nothing to do commits nothing and leaves the manifest's bytes as they
//! are, because the dialect it would record is the same one respelled
//! (`source.dialect`, M5); the first build with work respells it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names
)]

mod common;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use bellwether_build::{BuildOptions, SourceKind};
use bellwether_store::{commit, StagedSegments};
use bellwether_types::KeySpec;
use common::*;

#[test]
fn a_v1_dialect_spelling_is_no_work_and_the_next_build_with_work_respells_it() {
    let s = Scratch::new();
    std::fs::write(s.path("a.csv"), "1;x\n2;y\n3;z\n").unwrap();
    let store = memory_store();
    let b = builder(&store);
    let kind = SourceKind::Csv {
        delimiter: b';',
        has_header: false,
    };
    let opts = BuildOptions {
        schema: Some(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))),
        ..BuildOptions::new(kind)
    };
    let specs = [KeySpec::Exact {
        columns: vec!["id".into()],
        counts: false,
        bounds: None,
    }];
    build_ok(&b, &s.root_str(), &specs, &opts);
    // The manifest as a storage version 1 binary spelled the dialect.
    commit(&*store, "idx", &StagedSegments::new(), |cur| {
        let mut next = cur.clone();
        assert!(next.source.dialect.is_some());
        next.source.kind = "csv;delimiter=59;header=false".into();
        next.source.dialect = None;
        Ok(next)
    })
    .unwrap();
    let before = manifest_bytes(&store);
    let r = b.build(&s.root_str(), &specs, &opts).unwrap();
    assert_eq!(r.sequence, None, "nothing to do: {r:?}");
    assert_eq!(manifest_bytes(&store), before);

    // A new file is work: its commit spells the dialect in `source.dialect`.
    std::fs::write(s.path("b.csv"), "4;w\n").unwrap();
    build_ok(&b, &s.root_str(), &specs, &opts);
    let m = b.manifest().unwrap().unwrap();
    assert_eq!(m.source.kind, "csv");
    let d = m.source.dialect.unwrap();
    assert_eq!((d.delimiter, d.header, d.quote), (b';', false, b'"'));
}
