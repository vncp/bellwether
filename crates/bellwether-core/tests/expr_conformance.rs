//! Pinned conformance vectors of the `expr` language, one table per
//! function (`docs/format.md`, "Expression keys"), and the normalized form.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use bellwether_core::expr::{self, eval_closed, normalize, Expr, VECTORS};

#[test]
fn every_vector_reproduces() {
    let mut bad = Vec::new();
    for (f, e, want) in VECTORS {
        let got = eval_closed(e).unwrap().unwrap_or_else(|| "null".into());
        if got != want.replace(' ', "") {
            bad.push(format!("{f}: {e} => {got} (pinned {want})"));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[test]
fn every_function_has_vectors() {
    for f in expr::FUNCTIONS {
        assert!(
            VECTORS.iter().filter(|(g, _, _)| *g == f).count() >= 2,
            "{f} needs at least two vectors"
        );
    }
    for (f, e, _) in VECTORS {
        assert!(
            Expr::parse(e).unwrap().functions().contains(f),
            "{e} is listed under {f} and must call it"
        );
    }
}

#[test]
fn normalize_is_idempotent_on_every_vector() {
    for (_, e, _) in VECTORS {
        let n = normalize(e).unwrap();
        assert_eq!(normalize(&n).unwrap(), n);
    }
}

/// `docs/format.md` ("Expression keys", conformance vectors) lists exactly
/// the code's table, row for row, so neither drifts alone.
#[test]
fn the_format_doc_lists_exactly_these_vectors() {
    let doc = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/format.md"))
        .unwrap();
    let section = doc
        .split("| Function | Closed expression | Value |")
        .nth(1)
        .expect("format.md has the expr vector table");
    let rows: Vec<(String, String, String)> = section
        .lines()
        .skip(2)
        .take_while(|l| l.starts_with("| `"))
        .map(|l| {
            let cells: Vec<&str> = l
                .trim_matches('|')
                .split(" | ")
                .map(|c| c.trim().trim_matches('`'))
                .collect();
            assert_eq!(cells.len(), 3, "{l}");
            (cells[0].into(), cells[1].into(), cells[2].into())
        })
        .collect();
    let code: Vec<(String, String, String)> = VECTORS
        .iter()
        .map(|(f, e, v)| ((*f).into(), (*e).into(), (*v).into()))
        .collect();
    assert_eq!(rows, code);
}
