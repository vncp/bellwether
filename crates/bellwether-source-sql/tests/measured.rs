//! The measured scorers on SQLite and DuckDB: never exact, the model
//! named, the noise stated, one statement only.
#![allow(clippy::unwrap_used, clippy::doc_markdown)]

mod common;

use bellwether_source_sql::{
    ExplainScorer, MeasuredScorer, TimingScorer, MEASURED_PLAN, MEASURED_WALL,
};
use common::*;

#[test]
fn timing_scores_a_rewrite_with_its_noise() {
    let (_d, dir) = tmp();
    let s = dir.join("m.sqlite");
    sqlite_exec(
        &s,
        &[
            "CREATE TABLE t (id INTEGER, v TEXT)",
            "WITH RECURSIVE r(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < 5000) \
             INSERT INTO t SELECT i, 'v' || i FROM r",
        ],
    );
    let mut roots = vec![sqlite_root(&s, "t", "")];
    if duckdb_driver().is_some() {
        let d = dir.join("m.duckdb");
        duck_exec(
            &d,
            &["CREATE TABLE t AS SELECT i AS id, 'v' || i AS v FROM range(5000) r(i)"],
        );
        roots.push(duck_root(&d, "t", ""));
    }
    for root in roots {
        let mut scorer = TimingScorer::connect(&root, 5).unwrap();
        let s = scorer
            .score("SELECT * FROM t", "SELECT id FROM t WHERE id = 7")
            .unwrap();
        assert!(!s.exact);
        assert_eq!(s.model, MEASURED_WALL);
        assert_eq!(s.before.samples.len(), 5);
        assert_eq!(s.noise, s.before.spread + s.after.spread);
        assert!(scorer.score("SELECT 1; SELECT 2", "SELECT 1").is_err());
    }
}

#[test]
fn explain_analyze_reads_the_engine_s_counters() {
    let (_d, dir) = tmp();
    let s = dir.join("m.sqlite");
    sqlite_exec(&s, &["CREATE TABLE t (id INTEGER)"]);
    assert!(ExplainScorer::connect(&sqlite_root(&s, "t", ""), 1)
        .err()
        .unwrap()
        .to_string()
        .contains("TimingScorer"));
    if duckdb_driver().is_none() {
        return;
    }
    let d = dir.join("m.duckdb");
    duck_exec(
        &d,
        &["CREATE TABLE t AS SELECT i AS id FROM range(100000) r(i)"],
    );
    let mut scorer = ExplainScorer::connect(&duck_root(&d, "t", ""), 2).unwrap();
    let s = scorer
        .score(
            "SELECT sum(id) FROM t",
            "SELECT sum(id) FROM t WHERE id < 10",
        )
        .unwrap();
    assert!(!s.exact);
    assert_eq!(s.model, MEASURED_PLAN);
    assert_eq!(s.before.unit, "rows scanned");
    assert!(s.before.median >= 100_000, "{s:?}");
}
