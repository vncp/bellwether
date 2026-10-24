//! The commit protocol's decisions under concurrency: restart on any change
//! to the row-group space or the schema, and refuse to commit a declared spec
//! left incomplete by a rebase.
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

/// The commit's restart decision: a concurrent commit that changes the
/// row-group space, even one that only retires ids, restarts the build with
/// fresh ids; no committed segment references an id the winner retired.
mod winner_changes_the_row_group_space {
    use crate::common;

    use std::collections::BTreeSet;
    use std::sync::Arc;

    use bellwether_build::{completeness, BuildOptions, Builder, Planner, SourceKind};
    use bellwether_core::SegmentReader;
    use bellwether_store::{segment_path, Manifest, ObjectStoreBackend};
    use bellwether_types::{KeySpec, Store};
    use common::{
        cols, enc_i64, enc_str, memory_store, simple_schema, write_numbered, HookStore, Scratch,
    };
    use roaring::RoaringBitmap;

    fn exact(c: &[&str]) -> KeySpec {
        KeySpec::Exact {
            columns: c.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    fn bloom(c: &str) -> KeySpec {
        KeySpec::Bloom {
            columns: vec![c.into()],
            fpr: 0.01,
        }
    }

    fn opts() -> BuildOptions {
        BuildOptions {
            schema: Some(simple_schema()),
            ..BuildOptions::new(SourceKind::Parquet)
        }
    }

    fn live(m: &Manifest) -> RoaringBitmap {
        m.rowgroup_space
            .files
            .iter()
            .flat_map(|f| f.rowgroup_ids.iter().map(|i| i.0))
            .collect()
    }

    fn retired(m: &Manifest) -> RoaringBitmap {
        m.rowgroup_space
            .tombstones
            .iter()
            .flat_map(|t| t.rowgroup_ids.iter().map(|i| i.0))
            .collect()
    }

    /// Ids a segment's bitmaps reference, probed with every key the corpus can
    /// hold (ids 0..1000, groups g0..g3).
    fn referenced(store: &ObjectStoreBackend, id: &bellwether_types::Hash) -> RoaringBitmap {
        let bytes = store.get(&segment_path("idx", id)).unwrap().0;
        let r = SegmentReader::open(bytes).unwrap();
        let keys: Vec<Vec<u8>> = match r.key_spec() {
            KeySpec::Bloom { .. } => (0..4).map(|g| enc_str(&format!("g{g}"))).collect(),
            _ => (0..1000).map(enc_i64).collect(),
        };
        r.lookup(&keys).unwrap()
    }

    #[test]
    fn a_winner_that_retires_ids_forces_a_restart_with_fresh_ids() {
        let s = Scratch::new();
        write_numbered(&s.path("m.parquet"), 0, 8, 4);
        let inner = memory_store();
        build_base(&inner, &s);
        let base = Builder::new(Arc::clone(&inner), "idx")
            .manifest()
            .unwrap()
            .unwrap();
        assert_eq!(live(&base).iter().collect::<Vec<_>>(), vec![0, 1]);

        // X will index the new file x.parquet and a new spec. Between X's
        // planning and its CAS, Y rewrites m.parquet (retiring ids 0, 1) and
        // indexes it and x: Y gives m's fresh row groups the ids X gave x.
        write_numbered(&s.path("x.parquet"), 500, 8, 4);
        let (root, y_store, m_path) = (s.root_str(), Arc::clone(&inner), s.path("m.parquet"));
        let hook = Box::new(move || {
            write_numbered(&m_path, 50, 12, 4);
            let r = Builder::new(y_store, "idx")
                .build(&root, &[exact(&["id"])], &opts())
                .unwrap();
            assert_eq!(r.row_groups_retired, 2);
        });
        let x = Builder::new(Arc::new(HookStore::new(Arc::clone(&inner), hook)), "idx");
        let r = x
            .build(&s.root_str(), &[exact(&["id"]), bloom("grp")], &opts())
            .unwrap();
        assert_eq!(r.restarts, 1, "{r:?}");

        let m = Builder::new(Arc::clone(&inner), "idx")
            .manifest()
            .unwrap()
            .unwrap();
        let (lv, rt) = (live(&m), retired(&m));
        assert!(lv.is_disjoint(&rt));
        assert_eq!(rt.iter().collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(lv.len() + rt.len(), u64::from(m.rowgroup_space.next_id));
        // X's committed segments reference only ids live after the winner
        for seg in &r.segments {
            let refs = referenced(&inner, &seg.id);
            assert!(
                refs.is_subset(&lv),
                "segment {} of the restarted build references {:?}, which the winner retired or never assigned",
                seg.key_spec,
                (&refs - &lv).iter().collect::<Vec<_>>()
            );
        }
        assert!(completeness(&m, &bloom("grp")).is_some());
        // and plans are right for keys of both files
        let rows: Vec<common::Row> = ["m.parquet", "x.parquet"]
            .iter()
            .flat_map(|f| common::parquet_rows(&s.path(f), &simple_schema()))
            .collect();
        let p = Planner::open(Arc::clone(&inner), "idx").unwrap();
        let keys: Vec<Vec<u8>> = (0..600).map(enc_i64).collect();
        common::check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &keys);
        let tuples: Vec<Vec<u8>> = [(51, "g1"), (503, "g3"), (507, "g3")]
            .iter()
            .map(|&(k, g)| [enc_i64(k), enc_str(g)].concat())
            .collect();
        common::check_keys(
            &p,
            &rows,
            &[exact(&["id"]), bloom("grp")],
            &cols(&["id", "grp"]),
            &tuples,
        );
    }

    /// The winner only retires ids (a file vanished) and assigns none, so
    /// `next_id` is unchanged: the build must still restart rather than write
    /// its stale row-group space over the winner's.
    #[test]
    fn a_winner_that_only_retires_ids_forces_a_restart() {
        let s = Scratch::new();
        write_numbered(&s.path("a.parquet"), 0, 8, 4);
        write_numbered(&s.path("c.parquet"), 200, 8, 4);
        let inner = memory_store();
        build_base(&inner, &s);
        let (root, y_store, c_path) = (s.root_str(), Arc::clone(&inner), s.path("c.parquet"));
        let hook = Box::new(move || {
            std::fs::remove_file(&c_path).unwrap();
            let r = Builder::new(y_store, "idx")
                .build(&root, &[exact(&["id"])], &opts())
                .unwrap();
            assert_eq!((r.files_removed, r.row_groups_assigned), (1, 0));
        });
        let x = Builder::new(Arc::new(HookStore::new(Arc::clone(&inner), hook)), "idx");
        let r = x
            .build(&s.root_str(), &[exact(&["id"]), bloom("grp")], &opts())
            .unwrap();
        let m = Builder::new(Arc::clone(&inner), "idx")
            .manifest()
            .unwrap()
            .unwrap();
        let tracked: BTreeSet<String> = m
            .rowgroup_space
            .files
            .iter()
            .map(|f| f.fingerprint.path.clone())
            .collect();
        assert!(
            !tracked.contains(&s.path_str("c.parquet")),
            "the vanished file is tracked again (restarts {}), the winner's retirement was overwritten",
            r.restarts
        );
        assert_eq!(retired(&m).len(), 2);
        let p = Planner::open(Arc::clone(&inner), "idx").unwrap();
        let rows = common::parquet_rows(&s.path("a.parquet"), &simple_schema());
        let keys: Vec<Vec<u8>> = (0..300).map(enc_i64).collect();
        common::check_keys(&p, &rows, &[exact(&["id"])], &cols(&["id"]), &keys);
        common::check_keys(
            &p,
            &rows,
            &[bloom("grp")],
            &cols(&["grp"]),
            &[enc_str("g1")],
        );
    }

    fn build_base(inner: &Arc<ObjectStoreBackend>, s: &Scratch) {
        Builder::new(Arc::clone(inner), "idx")
            .build(&s.root_str(), &[exact(&["id"])], &opts())
            .unwrap();
    }
}

/// The restart decision on a concurrent schema change: the winner changes a
/// key column's dataset type and rebuilds that spec without touching the
/// row-group space; the loser restarts rather than publish its stale schema
/// over the winner's segments.
mod winner_changes_the_schema {
    use crate::common;

    use std::sync::Arc;

    use arrow::array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use bellwether_build::{completeness, BuildOptions, Builder, Lookup, Planner, SourceKind};
    use bellwether_core::encoding::Value;
    use bellwether_types::KeySpec;
    use common::{cols, memory_store, HookStore, Scratch};

    fn dataset(scale: i8) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("amt", DataType::Decimal128(20, scale), true),
            Field::new("grp", DataType::Utf8, true),
        ]))
    }

    fn exact(c: &str) -> KeySpec {
        KeySpec::Exact {
            columns: vec![c.into()],
        }
    }

    #[test]
    fn a_winner_that_changes_a_key_type_forces_a_restart() {
        let s = Scratch::new();
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "amt",
                Arc::new(
                    Decimal128Array::from(vec![150, 225, 999])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            (
                "grp",
                Arc::new(StringArray::from(vec!["a", "b", "a"])) as ArrayRef,
            ),
        ])
        .unwrap();
        common::write_parquet(&s.path("a.parquet"), &batch, 1);
        let inner = memory_store();
        let base = BuildOptions {
            schema: Some(dataset(2)),
            ..BuildOptions::new(SourceKind::Parquet)
        };
        Builder::new(Arc::clone(&inner), "idx")
            .build(&s.root_str(), &[exact("id"), exact("amt")], &base)
            .unwrap();
        // X adds a spec and keeps the recorded schema. Between X's planning
        // and its CAS, Y moves `amt` to scale 3, which rebuilds exact:amt; the
        // row-group space does not change.
        let (root, y_store) = (s.root_str(), Arc::clone(&inner));
        let hook = Box::new(move || {
            let y = BuildOptions {
                schema: Some(dataset(3)),
                ..BuildOptions::new(SourceKind::Parquet)
            };
            let r = Builder::new(y_store, "idx")
                .build(&root, &[exact("id"), exact("amt")], &y)
                .unwrap();
            assert_eq!(r.specs_rebuilt, vec!["exact:amt".to_string()]);
            assert_eq!(r.row_groups_assigned, 0);
        });
        let x = Builder::new(Arc::new(HookStore::new(Arc::clone(&inner), hook)), "idx");
        let keep = BuildOptions::new(SourceKind::Parquet);
        x.build(
            &s.root_str(),
            &[exact("id"), exact("amt"), exact("grp")],
            &keep,
        )
        .unwrap();
        let m = Builder::new(Arc::clone(&inner), "idx")
            .manifest()
            .unwrap()
            .unwrap();
        for spec in [exact("id"), exact("amt"), exact("grp")] {
            assert!(completeness(&m, &spec).is_some(), "{}", spec.name());
        }
        // every committed exact:amt segment opens under the committed schema,
        // and a lookup of 1.50 finds its row group
        let p = Planner::open(Arc::clone(&inner), "idx").unwrap();
        let key = p
            .encode_tuple(
                &cols(&["amt"]),
                &[Value::Decimal128 {
                    value: 150,
                    scale: 2,
                }],
            )
            .unwrap();
        let plan = p.plan(
            &[exact("amt")],
            &Lookup::Keys {
                columns: cols(&["amt"]),
                keys: vec![key],
            },
        );
        assert!(
            plan.as_ref().is_ok_and(|p| p.row_groups_selected == 1),
            "schema {:?}, plan {plan:?}",
            m.schema
                .columns
                .iter()
                .map(|c| c.data_type.clone())
                .collect::<Vec<_>>()
        );
    }
}

/// A build that would leave a declared spec incomplete commits nothing: a
/// foreign writer drops a kept spec's segment between the build's planning
/// and its CAS; the row-group space is unchanged, so the build rebases, and
/// then refuses.
mod incomplete_after_rebase {
    use crate::common;

    use std::sync::Arc;

    use bellwether_build::{completeness, BuildOptions, Builder, SourceKind};
    use bellwether_store::{commit, StagedSegments};
    use bellwether_types::KeySpec;
    use common::{memory_store, simple_schema, write_numbered, HookStore, Scratch};

    #[test]
    fn a_rebased_build_that_would_leave_a_spec_incomplete_commits_nothing() {
        let s = Scratch::new();
        write_numbered(&s.path("a.parquet"), 0, 8, 4);
        write_numbered(&s.path("b.parquet"), 100, 8, 4);
        let inner = memory_store();
        let spec = KeySpec::Exact {
            columns: vec!["id".into()],
        };
        let o = BuildOptions {
            schema: Some(simple_schema()),
            max_segment_row_groups: 2,
            ..BuildOptions::new(SourceKind::Parquet)
        };
        Builder::new(Arc::clone(&inner), "idx")
            .build(&s.root_str(), std::slice::from_ref(&spec), &o)
            .unwrap();
        let before = Builder::new(Arc::clone(&inner), "idx")
            .manifest()
            .unwrap()
            .unwrap();
        let victim = before.segments[0].id;
        write_numbered(&s.path("c.parquet"), 200, 8, 4);
        let f_store = Arc::clone(&inner);
        let hook = Box::new(move || {
            commit(&*f_store, "idx", &StagedSegments::new(), |cur| {
                let mut next = cur.clone();
                next.segments.retain(|x| x.id != victim);
                Ok(next)
            })
            .unwrap();
        });
        let x = Builder::new(Arc::new(HookStore::new(Arc::clone(&inner), hook)), "idx");
        let r = x.build(&s.root_str(), std::slice::from_ref(&spec), &o);
        let after = Builder::new(Arc::clone(&inner), "idx")
            .manifest()
            .unwrap()
            .unwrap();
        assert!(
            r.is_err(),
            "the build committed sequence {:?} although exact:id is incomplete (completeness {:?})",
            r.as_ref().ok().and_then(|r| r.sequence),
            completeness(&after, &spec)
        );
        assert_eq!(
            after.sequence,
            before.sequence + 1,
            "only the foreign edit landed"
        );
    }
}
