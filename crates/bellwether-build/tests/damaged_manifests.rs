//! Manifests damaged or edited by other writers: a lowered `next_id` is
//! refused before any id is assigned, and a dropped segment is caught by
//! re-deriving coverage by subset.
//!
//! Added as regression tests from the PR 19 review.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::float_cmp,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_arguments,
    clippy::assert_is_empty,
    clippy::redundant_closure_for_method_calls,
    clippy::format_push_string,
    clippy::doc_markdown
)]

mod common;

/// A damaged manifest whose `next_id` was lowered below retired ids: the
/// builder validates the saved row-group space before assigning ids, so it
/// never hands out a retired id again (ADR 013).
mod lowered_next_id {
    use crate::common;

    use std::sync::Arc;

    use bellwether_build::{BuildOptions, Builder, Planner, SourceKind};
    use bellwether_store::{commit, StagedSegments};
    use bellwether_types::KeySpec;
    use common::{memory_store, simple_schema, write_numbered, Scratch};

    #[test]
    fn the_builder_refuses_a_space_whose_next_id_is_below_retired_ids() {
        let s = Scratch::new();
        write_numbered(&s.path("a.parquet"), 0, 8, 4);
        write_numbered(&s.path("b.parquet"), 100, 8, 4);
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        let spec = KeySpec::Exact {
            columns: vec!["id".into()],
        };
        let o = BuildOptions {
            schema: Some(simple_schema()),
            ..BuildOptions::new(SourceKind::Parquet)
        };
        b.build(&s.root_str(), std::slice::from_ref(&spec), &o)
            .unwrap();
        // b vanishes: ids 2, 3 retire; next_id stays 4
        std::fs::remove_file(s.path("b.parquet")).unwrap();
        b.build(&s.root_str(), std::slice::from_ref(&spec), &o)
            .unwrap();
        // a damaged writer lowers next_id to 2, below the retired ids
        commit(&*store, "idx", &StagedSegments::new(), |cur| {
            let mut next = cur.clone();
            next.rowgroup_space.next_id = 2;
            Ok(next)
        })
        .unwrap();
        write_numbered(&s.path("c.parquet"), 200, 8, 4);
        let r = b.build(&s.root_str(), std::slice::from_ref(&spec), &o);
        let m = b.manifest().unwrap().unwrap();
        let c_ids: Vec<u32> = m
            .rowgroup_space
            .files
            .iter()
            .filter(|f| f.fingerprint.path.ends_with("c.parquet"))
            .flat_map(|f| f.rowgroup_ids.iter().map(|i| i.0))
            .collect();
        let planner = Planner::open(Arc::clone(&store), "idx");
        eprintln!(
            "build -> {:?}; c got ids {c_ids:?}; retired {:?}; planner open -> {:?}",
            r.as_ref().map(|r| r.sequence),
            m.rowgroup_space
                .tombstones
                .iter()
                .flat_map(|t| t.rowgroup_ids.iter().map(|i| i.0))
                .collect::<Vec<_>>(),
            planner.as_ref().err().map(ToString::to_string)
        );
        assert!(
            r.is_err(),
            "the builder reused retired ids {c_ids:?} and committed"
        );
    }
}

/// Readers re-derive completeness (ADR 012) by subset, not count: after a
/// rewrite, retired ids that old segments still cover make the covered count
/// look total when a foreign edit drops the segment over the fresh ids.
mod dropped_segment_after_rewrite {
    use crate::common;

    use std::sync::Arc;

    use bellwether_build::{completeness, BuildOptions, Builder, Lookup, Planner, SourceKind};
    use bellwether_store::{commit, StagedSegments};
    use bellwether_types::KeySpec;
    use common::{cols, enc_i64, memory_store, simple_schema, write_numbered, Scratch};

    #[test]
    fn a_dropped_segment_after_a_rewrite_makes_the_spec_incomplete() {
        let s = Scratch::new();
        write_numbered(&s.path("a.parquet"), 0, 8, 4);
        write_numbered(&s.path("b.parquet"), 100, 8, 4);
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        let spec = KeySpec::Exact {
            columns: vec!["id".into()],
        };
        let o = BuildOptions {
            schema: Some(simple_schema()),
            ..BuildOptions::new(SourceKind::Parquet)
        };
        b.build(&s.root_str(), std::slice::from_ref(&spec), &o)
            .unwrap();
        // rewrite b with the same row-group count: ids 2,3 retire, 4,5 fresh
        write_numbered(&s.path("b.parquet"), 200, 8, 4);
        b.build(&s.root_str(), std::slice::from_ref(&spec), &o)
            .unwrap();
        let m = b.manifest().unwrap().unwrap();
        let fresh = m
            .segments
            .iter()
            .find(|x| x.rowgroup_range.start.0 >= 4)
            .unwrap()
            .clone();
        // a foreign writer drops the segment over the fresh ids, keeping the
        // completeness entry; the old segment still covers retired ids 2,3
        commit(&*store, "idx", &StagedSegments::new(), |cur| {
            let mut next = cur.clone();
            next.segments.retain(|x| x.id != fresh.id);
            Ok(next)
        })
        .unwrap();
        let m = b.manifest().unwrap().unwrap();
        assert!(m.key_spec_completeness.contains_key("exact:id"));
        assert_eq!(completeness(&m, &spec), None, "coverage must be re-derived");
        let p = Planner::open(Arc::clone(&store), "idx").unwrap();
        let r = p.plan(
            std::slice::from_ref(&spec),
            &Lookup::Keys {
                columns: cols(&["id"]),
                keys: vec![enc_i64(201)],
            },
        );
        assert!(r.is_err(), "a partial plan: {r:?}");
    }
}
