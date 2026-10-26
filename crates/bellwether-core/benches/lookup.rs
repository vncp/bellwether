//! Single-key lookup latency versus segment size (exact key spec, in-memory
//! segment bytes). Present and absent keys are measured separately: an
//! absent key inside a block's range is normally stopped by the block bloom.
#![allow(missing_docs, clippy::unwrap_used)]

use bellwether_core::encoding::{encode_tuple, Value};
use bellwether_core::{SegmentReader, SegmentWriter, WriterOptions};
use bellwether_types::{KeySpec, RowGroupId};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn key(i: i64) -> Vec<u8> {
    encode_tuple(&[Value::Int(i)]).unwrap()
}

fn segment(n: u32) -> SegmentReader<Vec<u8>> {
    let spec = KeySpec::Exact {
        columns: vec!["id".into()],
        counts: false,
        bounds: None,
    };
    let mut w = SegmentWriter::new(spec, WriterOptions::default()).unwrap();
    for i in 0..n {
        // even ids only, so odd ids are absent but inside block ranges
        let k = key(i64::from(i) * 2);
        w.insert(&k, RowGroupId(i % 4096)).unwrap();
        w.insert(&k, RowGroupId((i * 7) % 4096)).unwrap();
    }
    SegmentReader::open(w.finish().unwrap().bytes).unwrap()
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("single_key_lookup");
    for n in [10_000u32, 100_000, 1_000_000] {
        let seg = segment(n);
        g.throughput(Throughput::Elements(1));
        let mut i: i64 = 0;
        g.bench_with_input(BenchmarkId::new("present", n), &seg, |b, seg| {
            b.iter(|| {
                i = (i + 7919) % i64::from(n);
                seg.lookup(&[key(i * 2)]).unwrap()
            });
        });
        let mut j: i64 = 0;
        g.bench_with_input(BenchmarkId::new("absent", n), &seg, |b, seg| {
            b.iter(|| {
                j = (j + 7919) % i64::from(n);
                seg.lookup(&[key(j * 2 + 1)]).unwrap()
            });
        });
    }
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
