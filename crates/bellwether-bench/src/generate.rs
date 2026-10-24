//! `generate`: writes the synthetic versioned ledger.
//!
//! Streaming (ADR 018): the generator builds exactly one row group as an
//! Arrow `RecordBatch`, hands it to the Parquet writer, flushes the row group
//! to disk, and drops it before building the next. Two measurements guard
//! that, each against a bound stated from the parameters before the run:
//!
//! - **Process RSS growth** over the whole generation, from [`crate::mem`]
//!   (exact peak on Linux, sampled elsewhere), against
//!   [`rss_growth_bound_bytes`]. This is the guard that can see retention: a
//!   generator that keeps row groups alive grows by their size, whatever its
//!   own bookkeeping says.
//! - **Buffered bytes** the generator knows it holds (the batch plus the
//!   writer's buffers), against [`memory_bound_bytes`].
//!
//! The RNG is ChaCha12 keyed per row group from the seed
//! ([`crate::stats::chacha`]), so any row group can be regenerated alone.

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, Decimal128Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use rand::Rng;
use rand_chacha::ChaCha12Rng;
use serde::{Deserialize, Serialize};

use crate::corpus::{
    batch_id_for, file_rel_path, CorpusManifest, DayLayout, GenParams, Permutation, COLUMNS,
    CORPUS_FORMAT, DAY_US,
};
use crate::mem::{MemProbe, MemReport};
use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// Fixed headroom added to the buffered-bytes bound for writer metadata,
/// dictionaries, and page indexes.
pub const MEMORY_BOUND_HEADROOM_BYTES: u64 = 8 * 1024 * 1024;
/// Multiple of the estimated row-group size the generator may hold in its
/// buffers: the Arrow batch, the writer's encoded copy, and dictionary slack.
pub const MEMORY_BOUND_ROW_GROUP_MULTIPLE: u64 = 3;
/// Fixed allowance in the RSS growth bound: allocator arenas, the writer's
/// page buffers and dictionaries, and the probe itself.
pub const RSS_GROWTH_HEADROOM_BYTES: u64 = 16 * 1024 * 1024;
/// Row groups of RSS growth the generator may add beyond the headroom.
pub const RSS_GROWTH_ROW_GROUP_MULTIPLE: u64 = 6;

/// The buffered-bytes bound, a function of the parameters alone:
/// `3 * estimated_row_group_bytes + 8 MiB`. It does not grow with files,
/// days, or total rows.
#[must_use]
pub fn memory_bound_bytes(params: &GenParams) -> u64 {
    MEMORY_BOUND_ROW_GROUP_MULTIPLE * params.estimated_row_group_bytes()
        + MEMORY_BOUND_HEADROOM_BYTES
}

/// The RSS growth bound, a function of the parameters alone:
/// `6 * estimated_row_group_bytes + 16 MiB`. A generator that retained its
/// output would grow by the whole corpus, so a corpus several times larger
/// than this bound makes retention visible.
#[must_use]
pub fn rss_growth_bound_bytes(params: &GenParams) -> u64 {
    RSS_GROWTH_ROW_GROUP_MULTIPLE * params.estimated_row_group_bytes() + RSS_GROWTH_HEADROOM_BYTES
}

/// `generator-stats.json`: what one generator run measured. Kept out of
/// `corpus.json` because measurements are not deterministic.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GeneratorStats {
    /// Largest `batch.get_array_memory_size() + writer.memory_size()` (plus
    /// anything retained by a probe run) seen after writing a row group and
    /// before flushing it.
    pub peak_buffered_bytes: u64,
    /// [`memory_bound_bytes`] for these parameters.
    pub memory_bound_bytes: u64,
    /// Process RSS over the generation; `None` where no probe exists.
    pub rss: Option<MemReport>,
    /// [`rss_growth_bound_bytes`] for these parameters.
    pub rss_growth_bound_bytes: u64,
    /// Row groups written.
    pub row_groups_written: u64,
    /// Rows written.
    pub rows_written: u64,
    /// Wall-clock milliseconds.
    pub elapsed_ms: f64,
}

/// Knobs that exist only to test the guards.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GenOptions {
    /// Keep every row group's batch alive until generation ends: a
    /// deliberately non-streaming generator. Tests use it to prove the RSS
    /// guard detects retention. Never set it for a real corpus.
    pub retain_row_groups: bool,
}

/// The Arrow schema of every generated file.
#[must_use]
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COLUMNS[0], DataType::UInt64, false),
        Field::new(COLUMNS[1], DataType::Int64, false),
        Field::new(
            COLUMNS[2],
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(COLUMNS[3], DataType::Utf8, false),
        Field::new(COLUMNS[4], DataType::Decimal128(18, 2), false),
        Field::new(COLUMNS[5], DataType::Utf8, false),
    ]))
}

/// The `status` values and their weights in percent. Five values that each
/// appear in nearly every row group: the planted
/// `low_selectivity_single_column` pattern.
pub const STATUSES: [(&str, u32); 5] = [
    ("posted", 60),
    ("settled", 25),
    ("pending", 10),
    ("reversed", 4),
    ("void", 1),
];
const PAYLOAD_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

fn pick_status(rng: &mut ChaCha12Rng) -> &'static str {
    let mut roll = rng.gen_range(0..100u32);
    for (name, weight) in STATUSES {
        if roll < weight {
            return name;
        }
        roll -= weight;
    }
    STATUSES[0].0
}

/// Builds row group `rg` of file `file_in_day` of `day` as one batch.
fn build_row_group(
    params: &GenParams,
    day: &DayLayout,
    perm: &Permutation,
    file_in_day: u32,
    rg: u32,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let rows = params.rows_per_row_group as usize;
    let files = u64::from(day.files);
    let mut rng = chacha(&[
        params.seed,
        u64::from(day.day),
        u64::from(file_in_day),
        u64::from(rg),
    ]);
    let slot = (DAY_US / day.rows.max(1) as i64).max(1);
    let start = day.start_us();
    let late_back_max = i64::from(day.day.min(3));

    let mut entity = UInt64Builder::with_capacity(rows);
    let mut batch = Int64Builder::with_capacity(rows);
    let mut ts = TimestampMicrosecondBuilder::with_capacity(rows).with_timezone("UTC");
    let mut status = StringBuilder::with_capacity(rows, rows * 8);
    let mut amount = Decimal128Builder::with_capacity(rows).with_precision_and_scale(18, 2)?;
    let mut payload = StringBuilder::with_capacity(rows, rows * params.payload_bytes as usize);
    let mut text = String::with_capacity(params.payload_bytes as usize);

    for i in 0..rows as u64 {
        let pos = u64::from(rg) * u64::from(params.rows_per_row_group) + i;
        let j = u64::from(file_in_day) + files * pos;
        let e = perm.apply(j % day.active);
        let write_us = start
            + (i128::from(j) * i128::from(DAY_US) / i128::from(day.rows)) as i64
            + rng.gen_range(0..slot);
        let version_us = if late_back_max > 0 && rng.gen_bool(params.late_fraction) {
            write_us - rng.gen_range(1..=late_back_max) * DAY_US
        } else {
            write_us
        };
        entity.append_value(e);
        batch.append_value(batch_id_for(write_us));
        ts.append_value(version_us);
        status.append_value(pick_status(&mut rng));
        amount.append_value(i128::from(rng.gen_range(-5_000_000i64..=50_000_000)));
        text.clear();
        let mut bits = 0u64;
        for k in 0..params.payload_bytes {
            if k % 12 == 0 {
                bits = rng.gen();
            }
            text.push(char::from(PAYLOAD_ALPHABET[(bits & 31) as usize]));
            bits >>= 5;
        }
        payload.append_value(&text);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(entity.finish()),
        Arc::new(batch.finish()),
        Arc::new(ts.finish()),
        Arc::new(status.finish()),
        Arc::new(amount.finish()),
        Arc::new(payload.finish()),
    ];
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// False-positive rate of the `entity_id` bloom filter in every row group.
pub const ENTITY_BLOOM_FPP: f64 = 0.01;

fn writer_properties(params: &GenParams) -> WriterProperties {
    let entity = ColumnPath::from(COLUMNS[0]);
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        // Parquet-native split-block bloom filter on the lookup key, sized
        // for one row group's distinct values (ADR 019 baseline).
        .set_column_bloom_filter_enabled(entity.clone(), true)
        .set_column_bloom_filter_max_ndv(entity.clone(), u64::from(params.rows_per_row_group))
        .set_column_bloom_filter_fpp(entity, ENTITY_BLOOM_FPP)
        // Row groups are cut explicitly with `flush`, one per batch.
        .set_max_row_group_row_count(Some(params.rows_per_row_group as usize + 1))
        .set_created_by(format!("bellwether-bench {}", env!("CARGO_PKG_VERSION")))
        .build()
}

/// Writes the corpus to `out`: `out/data/day=YYYY-MM-DD/part-NNNNN.parquet`,
/// `out/corpus.json`, and `out/generator-stats.json`.
///
/// Refuses to write into a non-empty directory that is not a previous corpus;
/// replaces the `data/` tree of a previous corpus.
pub fn generate(params: &GenParams, out: &Path) -> Result<GeneratorStats> {
    generate_with(params, out, GenOptions::default())
}

/// [`generate`] with test-only knobs.
pub fn generate_with(
    params: &GenParams,
    out: &Path,
    options: GenOptions,
) -> Result<GeneratorStats> {
    params.validate()?;
    let started = Instant::now();
    prepare_out_dir(out)?;

    let schema = schema();
    let props = writer_properties(params);
    let layout = params.layout();
    let mut files = Vec::with_capacity(params.files as usize);
    let mut peak = 0u64;
    let mut row_groups_written = 0u64;
    let mut rows_written = 0u64;
    let mut retained: Vec<RecordBatch> = Vec::new();
    let mut retained_bytes = 0u64;

    let mut probe = MemProbe::start();
    for day in &layout {
        let perm = day.permutation(params);
        for f in 0..day.files {
            let rel = file_rel_path(&day.date, day.first_file + f);
            let path = out.join(&rel);
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir).map_err(io_err(dir))?;
            }
            let sink = BufWriter::new(File::create(&path).map_err(io_err(&path))?);
            let mut writer = ArrowWriter::try_new(sink, schema.clone(), Some(props.clone()))?;
            for rg in 0..params.row_groups_per_file {
                let batch = build_row_group(params, day, &perm, f, rg, &schema)?;
                writer.write(&batch)?;
                let held = batch.get_array_memory_size() as u64
                    + writer.memory_size() as u64
                    + retained_bytes;
                peak = peak.max(held);
                writer.flush()?;
                row_groups_written += 1;
                rows_written += batch.num_rows() as u64;
                if options.retain_row_groups {
                    retained_bytes += batch.get_array_memory_size() as u64;
                    retained.push(batch);
                }
                if let Some(p) = probe.as_mut() {
                    p.sample();
                }
            }
            writer.close()?;
            files.push(rel);
            if let Some(p) = probe.as_mut() {
                p.force_sample();
            }
        }
    }
    // Finish while anything retained is still alive.
    let rss = probe.map(MemProbe::finish);
    drop(retained);

    let manifest = CorpusManifest {
        format: CORPUS_FORMAT.to_string(),
        params: params.clone(),
        days: layout,
        files,
        rows: rows_written,
        row_groups: row_groups_written,
    };
    write_json(&out.join("corpus.json"), &manifest)?;

    let stats = GeneratorStats {
        peak_buffered_bytes: peak,
        memory_bound_bytes: memory_bound_bytes(params),
        rss,
        rss_growth_bound_bytes: rss_growth_bound_bytes(params),
        row_groups_written,
        rows_written,
        elapsed_ms: started.elapsed().as_secs_f64() * 1e3,
    };
    write_json(&out.join("generator-stats.json"), &stats)?;
    Ok(stats)
}

fn prepare_out_dir(out: &Path) -> Result<()> {
    if out.exists() {
        let is_corpus = out.join("corpus.json").exists();
        let mut entries = std::fs::read_dir(out).map_err(io_err(out))?;
        if !is_corpus && entries.next().is_some() {
            return Err(BenchError::Invalid(format!(
                "{} is not empty and is not a corpus; refusing to write into it",
                out.display()
            )));
        }
        let data = out.join("data");
        if is_corpus && data.exists() {
            std::fs::remove_dir_all(&data).map_err(io_err(&data))?;
        }
    }
    std::fs::create_dir_all(out).map_err(io_err(out))
}
