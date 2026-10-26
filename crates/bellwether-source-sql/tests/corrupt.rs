//! The corrupt-response sweep over the driver boundary: every response of
//! an honest run (catalog listing, boundary scan, aggregates, counts,
//! reads) is corrupted in turn, one way at a time, and the whole run
//! (discover, statistics, reads, finish) must never panic. Most
//! corruptions must be refused; the few that stay well-formed (a value
//! nulled where nulls are legal) may produce an answer, never a panic.
#![allow(clippy::unwrap_used, clippy::doc_markdown)]

mod common;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use bellwether_source_sql::{open_driver, SqlOptions, SqlSource, SqlUrl};
use bellwether_types::{Error, Result, Source};
use common::*;

#[derive(Clone, Copy, Debug)]
enum Corruption {
    DropColumn,
    ExtraColumn,
    Garbage,
    Negate,
    AllNull,
    Duplicate,
    Empty,
    Halve,
    Fail,
    ToFloat,
}

const ALL: [Corruption; 10] = [
    Corruption::DropColumn,
    Corruption::ExtraColumn,
    Corruption::Garbage,
    Corruption::Negate,
    Corruption::AllNull,
    Corruption::Duplicate,
    Corruption::Empty,
    Corruption::Halve,
    Corruption::Fail,
    Corruption::ToFloat,
];

fn rebuild(cols: Vec<ArrayRef>, rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .enumerate()
            .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new_with_options(
        schema,
        cols,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap()
}

fn corrupt(c: Corruption, b: RecordBatch) -> Result<Option<RecordBatch>> {
    let n = b.num_rows();
    let mut cols = b.columns().to_vec();
    Ok(Some(match c {
        Corruption::DropColumn => {
            cols.pop();
            rebuild(cols, n)
        }
        Corruption::ExtraColumn => {
            if let Some(first) = cols.first().cloned() {
                cols.push(first);
            }
            rebuild(cols, n)
        }
        Corruption::Garbage => rebuild(
            cols.iter()
                .map(|_| Arc::new(StringArray::from(vec!["\u{1F4A5}garbage"; n])) as ArrayRef)
                .collect(),
            n,
        ),
        Corruption::Negate => rebuild(
            cols.iter()
                .map(|c| match c.as_any().downcast_ref::<Int64Array>() {
                    Some(a) => Arc::new(Int64Array::from_iter(a.iter().map(|v| v.map(|v| -v - 1))))
                        as ArrayRef,
                    None => Arc::clone(c),
                })
                .collect(),
            n,
        ),
        Corruption::AllNull => rebuild(
            cols.iter()
                .map(|c| new_null_array(c.data_type(), n))
                .collect(),
            n,
        ),
        Corruption::Duplicate => concat_batches(&b.schema(), &[b.clone(), b]).unwrap(),
        Corruption::Empty => return Ok(None),
        Corruption::Halve => b.slice(0, n / 2),
        Corruption::Fail => return Err(Error::Source("the connection dropped".into())),
        Corruption::ToFloat => rebuild(
            cols.iter()
                .map(|c| {
                    if c.data_type().is_integer()
                        || matches!(c.data_type(), DataType::Decimal128(..))
                    {
                        arrow::compute::cast(c, &DataType::Float64).unwrap()
                    } else {
                        Arc::clone(c)
                    }
                })
                .collect(),
            n,
        ),
    }))
}

fn run(src: &SqlSource) -> Result<u64> {
    let schema = src.schema(src.root())?;
    let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut rows = 0;
    for fp in src.discover(src.root())? {
        for rg in src.row_groups(&fp)? {
            rows += src.read_columns(&rg, &cols)?.num_rows() as u64;
        }
    }
    src.finish()?;
    Ok(rows)
}

fn sweep(engine: &str, root: &str) {
    let url = SqlUrl::parse(root).unwrap();
    // The honest run: how many queries it makes.
    let honest = Wrapped::new(open_driver(&url).unwrap());
    let log = Arc::clone(&honest.log);
    let src = SqlSource::with_driver(url.clone(), Box::new(honest), SqlOptions::default()).unwrap();
    let want = run(&src).unwrap();
    drop(src);
    let queries = log
        .lock()
        .unwrap()
        .iter()
        .filter(|s| s.starts_with("SELECT"))
        .count();
    assert!(queries >= 5, "{engine}: {queries}");
    let (mut refused, mut answered) = (0, 0);
    for k in 0..queries {
        for c in ALL {
            let mut w = Wrapped::new(open_driver(&url).unwrap());
            let mut seen = 0usize;
            let mut armed = true;
            let mut last = String::new();
            w.hook = Box::new(move |sql, b| {
                if sql != last {
                    seen += 1;
                    last = sql.to_string();
                }
                if armed && seen == k + 1 {
                    armed = false;
                    return corrupt(c, b);
                }
                Ok(Some(b))
            });
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let src = SqlSource::with_driver(url.clone(), Box::new(w), SqlOptions::default())?;
                run(&src)
            }));
            match outcome {
                Err(panic) => panic!("{engine}: query {k}, {c:?}: panicked: {panic:?}"),
                Ok(Err(_)) => refused += 1,
                Ok(Ok(rows)) => {
                    answered += 1;
                    // An answer is never a partial one.
                    assert_eq!(rows, want, "{engine}: query {k}, {c:?}");
                }
            }
        }
    }
    assert!(
        refused > answered * 3,
        "{engine}: {refused} refused, {answered} answered"
    );
}

#[test]
fn corrupt_responses_never_panic() {
    let (_d, dir) = tmp();
    let s = dir.join("c.sqlite");
    sqlite_exec(
        &s,
        &[
            "CREATE TABLE t (id INTEGER NOT NULL, name TEXT, amount DECIMAL(10,2), ts TIMESTAMP(6))",
            "INSERT INTO t VALUES (1, 'a', 1.25, 1700000000000000), (2, NULL, NULL, NULL), \
             (3, 'c', 3.50, 1700000001000000), (4, 'd', 4.75, 1700000002000000)",
        ],
    );
    sweep("sqlite", &sqlite_root(&s, "t", "&range=id&units=2"));
    if duckdb_driver().is_some() {
        let d = dir.join("c.duckdb");
        duck_exec(
            &d,
            &[
                "CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR, amount DECIMAL(10,2), ts TIMESTAMP)",
                "INSERT INTO t VALUES (1, 'a', 1.25, TIMESTAMP '2023-11-14 22:13:20'), (2, NULL, \
                 NULL, NULL), (3, 'c', 3.50, TIMESTAMP '2023-11-14 22:13:21'), (4, 'd', 4.75, \
                 TIMESTAMP '2023-11-14 22:13:22')",
            ],
        );
        sweep(
            "duckdb",
            &duck_root(&d, "t", "&range=id&units=2&snapshot=reread"),
        );
    }
}
