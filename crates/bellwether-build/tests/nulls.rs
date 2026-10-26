//! Nulls: all-null row groups are covered (ADR 012), a probe containing a
//! null matches nothing (ADR 014), and key-less segments over different row
//! groups never share an id.
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

/// ADR 012 coverage of all-null row groups for every spec kind, ADR 014
/// (a probe containing a null matches nothing), and key-less segments of one
/// spec that must not share an id.
mod coverage_and_null_probes {
    use crate::common;

    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use bellwether_build::{completeness, BuildOptions, Builder, Lookup, Planner, SourceKind};
    use bellwether_types::{KeySpec, Predicate};
    use common::{apply_residual, cols, memory_store, parquet_rows, Scratch};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]))
    }

    fn write(
        path: &std::path::Path,
        rows: &[(Option<i64>, Option<&str>, Option<i64>)],
        per_rg: usize,
    ) {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        common::write_parquet(path, &batch, per_rg);
    }

    fn opts() -> BuildOptions {
        BuildOptions {
            schema: Some(schema()),
            ..BuildOptions::new(SourceKind::Parquet)
        }
    }

    fn exact(c: &[&str]) -> KeySpec {
        KeySpec::Exact {
            columns: c.iter().map(|s| (*s).to_string()).collect(),
            counts: false,
            bounds: None,
        }
    }

    /// Ambiguity 3: a row group whose key column is entirely null is covered,
    /// so the spec is complete; every kind of spec.
    #[test]
    fn an_all_null_row_group_is_covered_for_every_kind() {
        let s = Scratch::new();
        write(
            &s.path("f.parquet"),
            &[
                (Some(1), Some("x"), Some(1)),
                (Some(2), Some("y"), Some(2)),
                (None, None, None),
                (None, None, None),
            ],
            2,
        );
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        let specs = [
            exact(&["a"]),
            KeySpec::Bloom {
                columns: vec!["a".into()],
                fpr: 0.01,
                bounds: None,
            },
            KeySpec::Range { column: "a".into() },
            KeySpec::Temporal {
                columns: vec!["a".into()],
                ts_column: "c".into(),
                counts: false,
            },
        ];
        let r = b.build(&s.root_str(), &specs, &opts()).unwrap();
        let m = b.manifest().unwrap().unwrap();
        for spec in &specs {
            assert_eq!(completeness(&m, spec), r.sequence, "{}", spec.name());
        }
    }

    /// ADR 014: a probe whose tuple contains a null matches nothing. Checked
    /// for a single-column null probe, a composite tuple with a trailing null
    /// (exact projection), and a prefix probe with a trailing null.
    #[test]
    fn a_probe_containing_a_null_matches_nothing() {
        let s = Scratch::new();
        let path = s.path("f.parquet");
        write(
            &path,
            &[
                (Some(1), None, Some(10)),
                (Some(1), Some("x"), Some(11)),
                (None, None, None),
                (Some(2), None, None),
            ],
            1,
        );
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        let abc = exact(&["a", "b", "c"]);
        let specs = [exact(&["a"]), exact(&["a", "b"]), abc.clone()];
        b.build(&s.root_str(), &specs, &opts()).unwrap();
        let rows = parquet_rows(&path, &schema());
        let p = Planner::open(Arc::clone(&store), "idx").unwrap();
        let null = vec![0u8];
        let one = p
            .encode_tuple(&cols(&["a"]), &[bellwether_core::encoding::Value::Int(1)])
            .unwrap();
        let one_null = [one.clone(), null.clone()].concat();

        let mut report = Vec::new();
        // (spec, lookup columns, probe)
        let cases: Vec<(KeySpec, Vec<String>, Vec<u8>, &str)> = vec![
            (exact(&["a"]), cols(&["a"]), null.clone(), "single null"),
            (
                exact(&["a", "b"]),
                cols(&["a", "b"]),
                one_null.clone(),
                "(1, NULL) exact",
            ),
            (
                abc.clone(),
                cols(&["a", "b"]),
                one_null.clone(),
                "(1, NULL) prefix of (a,b,c)",
            ),
        ];
        for (spec, columns, probe, what) in cases {
            let plan = p
                .plan(
                    std::slice::from_ref(&spec),
                    &Lookup::Keys {
                        columns: columns.clone(),
                        keys: vec![probe.clone()],
                    },
                )
                .unwrap();
            let in_lists_it = matches!(&plan.residual, Some(Predicate::In { values, .. }) if values.contains(&probe));
            // A consumer that encodes row tuples as core does (a trailing null
            // becomes the 0x00 marker) and compares bytes:
            let rgs: std::collections::BTreeSet<u32> = plan
                .entries
                .iter()
                .flat_map(|e| e.row_groups.iter())
                .collect();
            let byte_matches = rows
                .iter()
                .filter(|r| rgs.contains(&r.rg))
                .filter(|r| {
                    let mut t = Vec::new();
                    for (i, c) in columns.iter().enumerate() {
                        match r.get(c) {
                            Some(v) => t.extend_from_slice(v),
                            None if i == 0 => return false,
                            None => t.push(0),
                        }
                    }
                    t == probe
                })
                .count();
            // and the SQL reference evaluator
            let sql = apply_residual(
                plan.residual.as_ref().unwrap(),
                rows.iter()
                    .filter(|r| rgs.contains(&r.rg))
                    .cloned()
                    .collect(),
            )
            .len();
            report.push(format!(
                "{what}: planned row groups {rgs:?}, residual In lists the null probe: {in_lists_it}, \
                 rows a byte-comparing consumer returns: {byte_matches}, SQL evaluator: {sql}"
            ));
        }
        for line in &report {
            eprintln!("{line}");
        }
        let bad: Vec<&String> = report
            .iter()
            .filter(|l| !l.contains("row groups {}") || l.contains("In lists the null probe: true"))
            .collect();
        assert!(
            bad.is_empty(),
            "ADR 014: a probe containing a null must match nothing:\n{}",
            bad.iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    /// Two key-less segments of one spec (every row group they processed has
    /// a null leading key) over different row groups must stay two entries:
    /// a segment records the row groups its writer processed, so their bytes,
    /// and ids, differ. Case 1: an incremental build whose new file has an
    /// all-null key column, after an earlier file did.
    #[test]
    fn a_second_all_null_file_can_still_be_indexed_incrementally() {
        let s = Scratch::new();
        write(&s.path("day1.parquet"), &[(None, Some("x"), Some(1))], 4);
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        let specs = [exact(&["a"]), exact(&["b"])];
        b.build(&s.root_str(), &specs, &opts()).unwrap();
        write(&s.path("day2.parquet"), &[(None, Some("y"), Some(2))], 4);
        let r = b.build(&s.root_str(), &specs, &opts());
        assert!(
            r.is_ok(),
            "the second all-null file cannot be indexed: {:?}",
            r.err()
        );
        assert_eq!(
            r.unwrap().sequence,
            Some(2),
            "the second file was committed"
        );
        let m = b.manifest().unwrap().unwrap();
        // ADR 020: completeness is the sequence at which the spec became
        // complete; the incremental build kept it complete
        assert_eq!(completeness(&m, &specs[0]), Some(1));
        // both key-less segments of exact:a are live, each over its own row
        // group: neither was dropped as a duplicate id
        let mut ranges: Vec<(u32, u32)> = m
            .segments
            .iter()
            .filter(|x| x.key_spec == "exact:a")
            .map(|x| (x.rowgroup_range.start.0, x.rowgroup_range.end.0))
            .collect();
        ranges.sort_unstable();
        assert_eq!(ranges, vec![(0, 1), (1, 2)]);
    }

    /// Case 2: one build in which two work items each see only null keys for
    /// a spec (forced here by `max_segment_row_groups = 1`).
    #[test]
    fn two_all_null_items_in_one_build() {
        let s = Scratch::new();
        write(&s.path("a.parquet"), &[(None, Some("x"), Some(1))], 4);
        write(&s.path("b.parquet"), &[(Some(5), Some("y"), Some(2))], 4);
        write(&s.path("c.parquet"), &[(None, Some("z"), Some(3))], 4);
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        let o = BuildOptions {
            max_segment_row_groups: 1,
            ..opts()
        };
        let r = b.build(&s.root_str(), &[exact(&["a"])], &o);
        assert!(r.is_ok(), "{:?}", r.err());
    }

    /// Case 3: the segment cap flushes a writer that has seen only an all-null
    /// row group, twice in one file.
    #[test]
    fn cap_flushes_after_all_null_row_groups() {
        let s = Scratch::new();
        write(
            &s.path("a.parquet"),
            &[
                (None, Some("x"), Some(1)),
                (None, Some("x"), Some(1)),
                (Some(1), Some("y"), Some(2)),
                (Some(2), Some("y"), Some(2)),
                (None, Some("x"), Some(1)),
                (None, Some("x"), Some(1)),
                (Some(3), Some("y"), Some(2)),
                (Some(4), Some("y"), Some(2)),
            ],
            2,
        );
        let store = memory_store();
        let b = Builder::new(Arc::clone(&store), "idx");
        let o = BuildOptions {
            max_segment_keys: 1,
            ..opts()
        };
        let r = b.build(&s.root_str(), &[exact(&["a"])], &o);
        assert!(r.is_ok(), "{:?}", r.err());
    }
}

/// A key-less segment over new row groups never collides with an earlier
/// one, for every spec kind; later builds, and later keyed files, index.
mod keyless_segments {
    use crate::common;

    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use bellwether_build::{BuildOptions, Builder, SourceKind};
    use bellwether_types::KeySpec;
    use common::{memory_store, Scratch};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("g", DataType::Utf8, true),
            Field::new("t", DataType::Int64, true),
        ]))
    }

    fn write(path: &std::path::Path, k: Option<i64>, g: &str) {
        let b = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![k])),
                Arc::new(StringArray::from(vec![g])),
                Arc::new(Int64Array::from(vec![Some(1)])),
            ],
        )
        .unwrap();
        common::write_parquet(path, &b, 8);
    }

    #[test]
    fn every_kind_indexes_a_second_keyless_file_and_retries_succeed() {
        let kinds = [
            KeySpec::Exact {
                columns: vec!["k".into()],
                counts: false,
                bounds: None,
            },
            KeySpec::Bloom {
                columns: vec!["k".into()],
                fpr: 0.01,
                bounds: None,
            },
            KeySpec::Range { column: "k".into() },
            KeySpec::Temporal {
                columns: vec!["k".into()],
                ts_column: "t".into(),
                counts: false,
            },
        ];
        let mut report = Vec::new();
        for spec in kinds {
            let s = Scratch::new();
            write(&s.path("d1.parquet"), None, "x");
            let store = memory_store();
            let b = Builder::new(Arc::clone(&store), "idx");
            let o = BuildOptions {
                schema: Some(schema()),
                ..BuildOptions::new(SourceKind::Parquet)
            };
            b.build(&s.root_str(), std::slice::from_ref(&spec), &o)
                .unwrap();
            write(&s.path("d2.parquet"), None, "y");
            let tries: Vec<bool> = (0..3)
                .map(|_| {
                    b.build(&s.root_str(), std::slice::from_ref(&spec), &o)
                        .is_err()
                })
                .collect();
            // and a later file with keys builds too
            write(&s.path("d3.parquet"), Some(5), "z");
            let after = b.build(&s.root_str(), std::slice::from_ref(&spec), &o);
            report.push(format!(
                "{}: retries fail {tries:?}; after a keyed file arrives: {:?}",
                spec.name(),
                after.as_ref().err().map(ToString::to_string)
            ));
        }
        for l in &report {
            eprintln!("{l}");
        }
        assert!(
            report
                .iter()
                .all(|l| !l.contains("true") && l.ends_with("arrives: None")),
            "{report:#?}"
        );
    }
}

/// Empty and null lookups, decided as replay decides them: no key plans no
/// row group with an empty `In`, never a full scan, with or without specs;
/// a probe containing a null is dropped before lookup and from the
/// residual; a range with a null bound selects nothing; `encode_keys`
/// skips a row with a null in any lookup column, not only the first.
mod empty_and_null_lookups {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use bellwether_build::Lookup;
    use bellwether_types::{Predicate, ScanPlan};

    use crate::common::{
        builder, cols, enc_i64, enc_str, exact, memory_store, parquet_opts, planner, range,
        simple_schema, write_numbered, Scratch,
    };

    fn empty_in(plan: &ScanPlan) -> bool {
        matches!(&plan.residual, Some(Predicate::In { values, .. }) if values.is_empty())
    }

    #[test]
    fn no_key_and_null_probes_select_nothing() {
        let s = Scratch::new();
        write_numbered(&s.path("a.parquet"), 0, 8, 4);
        let store = memory_store();
        let specs = [exact(&["id"]), exact(&["id", "grp"]), range("id")];
        builder(&store)
            .build(&s.root_str(), &specs, &parquet_opts(&simple_schema()))
            .unwrap();
        let p = planner(&store);
        let none = Lookup::Keys {
            columns: cols(&["id"]),
            keys: vec![],
        };
        // no key: nothing, with a spec and without one (never a raw scan)
        for specs in [&[exact(&["id"])][..], &[][..]] {
            let plan = p.plan(specs, &none).unwrap();
            assert_eq!(plan.row_groups_selected, 0, "{specs:?}");
            assert!(plan.entries.is_empty() && empty_in(&plan));
            assert_eq!(plan.row_groups_total, 2);
        }
        // null-containing probes, point, projection, prefix and raw: dropped
        let id_null = [enc_i64(1), vec![0u8]].concat();
        let cases = [
            (vec![exact(&["id"])], cols(&["id"]), vec![0u8]),
            (
                vec![exact(&["id", "grp"])],
                cols(&["id", "grp"]),
                id_null.clone(),
            ),
            (vec![exact(&["id"])], cols(&["id", "grp"]), id_null.clone()),
            (vec![], cols(&["id", "grp"]), id_null.clone()),
        ];
        for (specs, columns, key) in cases {
            let lookup = Lookup::Keys {
                columns,
                keys: vec![key],
            };
            let plan = p.plan(&specs, &lookup).unwrap();
            assert_eq!(plan.row_groups_selected, 0, "{specs:?}");
            assert!(empty_in(&plan), "{:?}", plan.residual);
        }
        // a null probe next to a real one: only the real one is planned
        let real = [enc_i64(5), enc_str("g1")].concat();
        let both = Lookup::Keys {
            columns: cols(&["id", "grp"]),
            keys: vec![id_null, real.clone()],
        };
        let plan = p.plan(&[exact(&["id", "grp"])], &both).unwrap();
        assert_eq!(plan.row_groups_selected, 1);
        let Some(Predicate::In { values, .. }) = &plan.residual else {
            panic!("{:?}", plan.residual)
        };
        assert_eq!(values, &vec![real]);
        // a range with a null bound: nothing
        for (lo, hi) in [(vec![0u8], enc_i64(5)), (enc_i64(0), vec![0u8])] {
            let lookup = Lookup::Between {
                column: "id".into(),
                lo,
                hi,
            };
            for specs in [&[range("id")][..], &[][..]] {
                let plan = p.plan(specs, &lookup).unwrap();
                assert_eq!(plan.row_groups_selected, 0);
                assert!(empty_in(&plan));
            }
        }
    }

    #[test]
    fn encode_keys_skips_a_null_in_any_lookup_column() {
        let s = Scratch::new();
        write_numbered(&s.path("a.parquet"), 0, 4, 4);
        let store = memory_store();
        builder(&store)
            .build(
                &s.root_str(),
                &[exact(&["id", "grp"])],
                &parquet_opts(&simple_schema()),
            )
            .unwrap();
        let p = planner(&store);
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)])) as ArrayRef,
            ),
            (
                "grp",
                Arc::new(StringArray::from(vec![
                    Some("g1"),
                    Some("g2"),
                    None,
                    Some("g0"),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let keys = p.encode_keys(&cols(&["id", "grp"]), &batch).unwrap();
        assert_eq!(
            keys,
            vec![
                [enc_i64(1), enc_str("g1")].concat(),
                [enc_i64(4), enc_str("g0")].concat()
            ],
            "the null-led row and the trailing-null row are both skipped"
        );
        // one column keeps its three non-null rows
        assert_eq!(p.encode_keys(&cols(&["id"]), &batch).unwrap().len(), 3);
    }
}
