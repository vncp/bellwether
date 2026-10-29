//! The HTTP range server the DuckDB and Spark byte measurements stand on:
//! files are served at URL routes relative to the served root, every
//! ranged `GET` is served exactly and recorded with its bytes against the
//! local file, a `HEAD` is not, nothing outside the served root is
//! reachable, and the classification splits fetched bytes into footer,
//! data, and other bytes against the file's own layout.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::doc_markdown)]

mod common;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;

use bellwether_adapters::measure::{classify, FileSpans};
use common::range_server::{parse_range, RangeServer};

fn request(base: &str, raw: &str) -> Vec<u8> {
    let mut s = TcpStream::connect(base.trim_start_matches("http://")).unwrap();
    s.write_all(raw.as_bytes()).unwrap();
    s.shutdown(std::net::Shutdown::Write).unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).unwrap();
    out
}

fn body(resp: &[u8]) -> &[u8] {
    let at = resp.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    &resp[at + 4..]
}

#[test]
fn ranges_parse_as_http_defines_them() {
    assert_eq!(parse_range("bytes=0-9", 100), Some((0, 10)));
    assert_eq!(parse_range("bytes=90-", 100), Some((90, 100)));
    assert_eq!(parse_range("bytes=-8", 100), Some((92, 100)));
    assert_eq!(parse_range("bytes=95-200", 100), Some((95, 100)));
    assert_eq!(parse_range("bytes=5-4", 100), None);
    assert_eq!(parse_range("bytes=0-1,5-6", 100), None);
    assert_eq!(parse_range("items=0-1", 100), None);
}

#[test]
fn served_ranges_are_exact_and_recorded() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("served");
    let c = common::corpus(&root.join("local lake"));
    let server = RangeServer::start(&root);
    let f = &c.files[0];
    // One percent-encoded segment per component below the served root.
    let route = server.url_path(std::path::Path::new(&f.path));
    assert!(
        route.starts_with("/local%20lake/") && !route.contains('\\'),
        "{route}"
    );
    let bytes = std::fs::read(&f.path).unwrap();
    let r = request(
        &server.base,
        &format!("GET {route} HTTP/1.1\r\nHost: x\r\nRange: bytes=4-19\r\n\r\n"),
    );
    assert!(r.starts_with(b"HTTP/1.1 206"));
    assert_eq!(body(&r), &bytes[4..20]);
    let r = request(
        &server.base,
        &format!("HEAD {route} HTTP/1.1\r\nHost: x\r\n\r\n"),
    );
    assert_eq!(body(&r), b"");
    let r = request(
        &server.base,
        &format!("GET {route} HTTP/1.1\r\nHost: x\r\nRange: bytes=-8\r\n\r\n"),
    );
    assert_eq!(body(&r), &bytes[bytes.len() - 8..]);
    let fetches = server.fetches();
    assert_eq!(fetches.len(), 2, "{fetches:?}");
    assert_eq!(fetches[0].path, f.path);
    assert_eq!(fetches[0].range, 4..20);
    assert_eq!(fetches[1].path, f.path);
    assert_eq!(fetches[1].range.end, f.size);
    // Nothing outside the root: not an absolute path, not a file reached
    // by climbing out of the root (raw or percent-encoded).
    let outside = tmp.path().join("outside.parquet");
    std::fs::write(&outside, &bytes).unwrap();
    for target in [
        "/etc/hosts",
        "/../outside.parquet",
        "/%2E%2E/outside.parquet",
        "/..%2Foutside.parquet",
        "/..%5Coutside.parquet",
    ] {
        let r = request(
            &server.base,
            &format!("GET {target} HTTP/1.1\r\nHost: x\r\n\r\n"),
        );
        assert!(r.starts_with(b"HTTP/1.1 404"), "{target}");
    }
    assert_eq!(server.fetches().len(), 2);
}

#[test]
fn classification_splits_footer_data_and_other_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(tmp.path());
    let f = &c.files[0];
    let spans = FileSpans::of_local(&f.path).unwrap();
    let layouts: BTreeMap<String, FileSpans> = [(f.path.clone(), spans.clone())].into();
    // Row group 1 exactly, the footer exactly, and the 4-byte magic.
    let rg1: Vec<_> = spans
        .chunks
        .iter()
        .filter(|(rg, _)| *rg == 1)
        .map(|(_, s)| s.clone())
        .collect();
    let mut fetches: Vec<bellwether_adapters::measure::Fetch> = rg1
        .iter()
        .map(|r| bellwether_adapters::measure::Fetch {
            path: f.path.clone(),
            range: r.clone(),
        })
        .collect();
    fetches.push(bellwether_adapters::measure::Fetch {
        path: f.path.clone(),
        range: spans.size - spans.footer_len..spans.size,
    });
    fetches.push(bellwether_adapters::measure::Fetch {
        path: f.path.clone(),
        range: 0..4,
    });
    let r = classify(&fetches, &layouts);
    let fb = &r.files[&f.path];
    assert_eq!(fb.data, f.rg_bytes[1]);
    assert_eq!(fb.data, spans.row_group_bytes(1));
    assert_eq!(fb.footer, spans.footer_len);
    assert_eq!(fb.other, 4);
    assert_eq!(fb.row_groups_touched, [1u32].into());
}
