//! `market`: a synthetic time-series corpus (symbols × days × intraday rows,
//! with restatements and late prints, Hive-style `day=YYYY-MM-DD/`
//! directories) and a workload of symbol-set × window lookups (WP21).
//!
//! Layout under `<out>/`:
//! - `data/day=YYYY-MM-DD/part-NNNNN.parquet`: intraday bars with schema
//!   `symbol: Utf8`, `valid_ts: Timestamp(µs, UTC)`, `known_ts: Timestamp(µs, UTC)`,
//!   `price: Float64`, `volume: Int64`, `venue: Utf8`.
//! - `market-corpus.json`: deterministic manifest of the corpus parameters.
//! - `workload/keys-NNNN.parquet` and `market-workload.json`: symbol-set ×
//!   window lookups (`window_lo`, `window_hi`, `valid_at`, `known_at`, and
//!   symbol keys).

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_types::Hash;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::corpus::{date_string, day_start_us, DAY_US};
use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// Format tag written into `market-corpus.json`.
pub const MARKET_CORPUS_FORMAT: &str = "bellwether-bench-market-corpus/1";
/// Format tag written into `market-workload.json`.
pub const MARKET_WORKLOAD_FORMAT: &str = "bellwether-bench-market-workload/1";

const VENUES: [&str; 4] = ["XNAS", "XNYS", "ARCX", "BATS"];

/// Parameters of the `market` benchmark corpus.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketParams {
    /// RNG seed.
    pub seed: u64,
    /// Number of trading days (`day=YYYY-MM-DD/` directories).
    pub days: u32,
    /// Files per day.
    pub files_per_day: u32,
    /// Row groups per file.
    pub row_groups_per_file: u32,
    /// Intraday rows per row group.
    pub rows_per_row_group: u32,
    /// Distinct ticker symbols (`SYM0000` .. `SYM{symbols-1:04}`).
    pub symbols: u32,
    /// Fraction of rows that are restatements or late prints (`known_ts > valid_ts`).
    pub restatement_fraction: f64,
}

impl Default for MarketParams {
    fn default() -> Self {
        Self {
            seed: 42,
            days: 8,
            files_per_day: 2,
            row_groups_per_file: 4,
            rows_per_row_group: 500,
            symbols: 200,
            restatement_fraction: 0.05,
        }
    }
}

impl MarketParams {
    /// Validates parameters.
    pub fn validate(&self) -> Result<()> {
        if self.days == 0
            || self.files_per_day == 0
            || self.row_groups_per_file == 0
            || self.rows_per_row_group == 0
            || self.symbols == 0
        {
            return Err(BenchError::Invalid(
                "market corpus counts (days, files_per_day, row_groups_per_file, rows_per_row_group, symbols) must be positive"
                    .into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.restatement_fraction) {
            return Err(BenchError::Invalid(
                "restatement_fraction must be in 0.0..=1.0".into(),
            ));
        }
        Ok(())
    }

    /// Symbol name for index `s`.
    #[must_use]
    pub fn symbol_name(s: u32) -> String {
        format!("SYM{s:04}")
    }
}

/// Written to `market-corpus.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketManifest {
    /// [`MARKET_CORPUS_FORMAT`].
    pub format: String,
    /// BLAKE3 of the parameters JSON.
    pub corpus_id: Hash,
    /// Parameters.
    pub params: MarketParams,
    /// Total files written.
    pub files_total: u32,
    /// Total row groups written.
    pub row_groups_total: u32,
    /// Total rows written.
    pub rows_total: u64,
}

/// One symbol-set × window lookup in the market workload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketRun {
    /// Run identifier (`run-0000`).
    pub run_id: String,
    /// Relative path to the Parquet key file (`workload/run-0000.parquet`).
    pub keys_file: String,
    /// Number of distinct symbols in the key file.
    pub symbols: u32,
    /// Inclusive window start (`valid_ts`, microseconds since epoch).
    pub window_lo: i64,
    /// Inclusive window end (`valid_ts`, microseconds since epoch).
    pub window_hi: i64,
    /// Point-in-time valid timestamp for `asof`.
    pub valid_at: i64,
    /// Point-in-time known timestamp for bitemporal `asof --valid --known`.
    pub known_at: i64,
}

/// Written to `market-workload.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketWorkload {
    /// [`MARKET_WORKLOAD_FORMAT`].
    pub format: String,
    /// Runs.
    pub runs: Vec<MarketRun>,
}

/// Arrow schema of every generated market Parquet file.
#[must_use]
pub fn market_schema() -> SchemaRef {
    let ts_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("valid_ts", ts_type.clone(), false),
        Field::new("known_ts", ts_type, false),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Int64, false),
        Field::new("venue", DataType::Utf8, false),
    ]))
}

/// Generates the `market` corpus under `out/data/day=YYYY-MM-DD/part-NNNNN.parquet`,
/// `out/market-corpus.json`, and a default symbol-set × window workload under
/// `out/workload/` and `out/market-workload.json`.
pub fn generate_market(params: &MarketParams, out: &Path) -> Result<MarketManifest> {
    params.validate()?;
    let data_dir = out.join("data");
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir).map_err(io_err(&data_dir))?;
    }
    std::fs::create_dir_all(&data_dir).map_err(io_err(&data_dir))?;

    let schema = market_schema();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(params.rows_per_row_group as usize))
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_encoding(Encoding::PLAIN)
        .build();

    let mut global_file = 0u32;
    let total_rgs_per_day = params.files_per_day * params.row_groups_per_file;
    // Partition symbols across row groups of a day so each row group holds a
    // subset of symbols and a slice of the day's session, plus some symbols
    // whose rows span specific intraday windows—making per-key time bounds
    // (`exact:symbol@valid_ts`), ordered plans (`range:valid_ts`), Hive
    // `day=` pruning, and bitemporal `temporal2:symbol@valid_ts,known_ts` all
    // discriminate cleanly.
    for day in 0..params.days {
        let date = date_string(day);
        let day_dir = data_dir.join(format!("day={date}"));
        std::fs::create_dir_all(&day_dir).map_err(io_err(&day_dir))?;
        let day_base_us = day_start_us(day);

        for f_in_day in 0..params.files_per_day {
            let file_path = day_dir.join(format!("part-{global_file:05}.parquet"));
            let file = File::create(&file_path).map_err(io_err(&file_path))?;
            let mut writer =
                ArrowWriter::try_new(file, Arc::clone(&schema), Some(props.clone()))?;

            for rg in 0..params.row_groups_per_file {
                let rg_in_day = f_in_day * params.row_groups_per_file + rg;
                let batch = build_market_row_group(
                    params,
                    day,
                    day_base_us,
                    rg_in_day,
                    total_rgs_per_day,
                    &schema,
                )?;
                writer.write(&batch)?;
                writer.flush()?;
            }
            writer.close()?;
            global_file += 1;
        }
    }

    let params_bytes = serde_json::to_vec(params)?;
    let manifest = MarketManifest {
        format: MARKET_CORPUS_FORMAT.to_string(),
        corpus_id: Hash::of(&params_bytes),
        params: params.clone(),
        files_total: global_file,
        row_groups_total: global_file * params.row_groups_per_file,
        rows_total: u64::from(global_file)
            * u64::from(params.row_groups_per_file)
            * u64::from(params.rows_per_row_group),
    };
    write_json(&out.join("market-corpus.json"), &manifest)?;
    generate_market_workload(out, params, 16)?;
    Ok(manifest)
}

fn build_market_row_group(
    params: &MarketParams,
    day: u32,
    day_base_us: i64,
    rg_in_day: u32,
    total_rgs_per_day: u32,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut rng = chacha(&[
        params.seed,
        u64::from(day),
        u64::from(rg_in_day),
        0x6d61_726b,
    ]);
    let n = params.rows_per_row_group as usize;
    let mut symbols = Vec::with_capacity(n);
    let mut valid_ts = Vec::with_capacity(n);
    let mut known_ts = Vec::with_capacity(n);
    let mut prices = Vec::with_capacity(n);
    let mut volumes = Vec::with_capacity(n);
    let mut venues = Vec::with_capacity(n);

    // Each row group covers a wide global `valid_ts` span (due to a few
    // market-wide index prints), while each symbol's own active trading window
    // inside the row group is localized by `sym_idx % total_rgs_per_day`.
    let slice_us = DAY_US / i64::from(total_rgs_per_day.max(1));
    let rg_start_us = day_base_us + i64::from(rg_in_day) * slice_us;
    let restate_threshold = (params.restatement_fraction * 10_000.0) as u64;

    // Group symbols into cohorts so each row group holds ~1/2 of the symbols,
    // and for symbols in cohort `rg_in_day`, their timestamps are in `rg_start_us..rg_start_us + slice_us`,
    // whereas one background symbol (`SYM0000`) spans the whole day in every row group
    // (widening the row group's global `valid_ts` range so `bounds` beats `range`).
    let cohort_size = (params.symbols / total_rgs_per_day.max(1)).max(1);
    let cohort_start = (rg_in_day * cohort_size) % params.symbols;

    for i in 0..n {
        let r = rand_u64(&mut rng);
        let (sym_id, v_ts) = if i == 0 {
            // Anchor row at start of day so the row group's global min `valid_ts` is early
            (0, day_base_us + (i64::from(rg_in_day) * 1_000_000))
        } else if i + 1 == n {
            // Anchor row near end of day so the row group's global max `valid_ts` is late
            (0, day_base_us + DAY_US - 1_000_000 - i64::from(rg_in_day) * 1_000)
        } else if i % 4 == 0 {
            // Secondary cohort symbol present in this row group but only at early timestamps
            // (so `exact:symbol` alone matches this row group, while `exact:symbol@valid_ts`
            // prunes it for later windows!).
            let prev_cohort = ((rg_in_day + 1) % total_rgs_per_day) * cohort_size;
            let sym = (prev_cohort + (i as u32 % cohort_size)).min(params.symbols - 1);
            let early_ts = day_base_us + (i as i64 * 1_000);
            (sym, early_ts)
        } else {
            let sym = (cohort_start + (i as u32 % cohort_size)).min(params.symbols - 1);
            let offset = (i as i64 * slice_us) / (n as i64).max(1);
            (sym, rg_start_us + offset)
        };

        // Known time: normally valid_ts + 500 us; for restatements/late prints,
        // delayed by 1 to 6 hours (or next day).
        let is_restatement = (r % 10_000) < restate_threshold;
        let k_ts = if is_restatement {
            v_ts + 3_600_000_000 * (1 + (r % 6) as i64)
        } else {
            v_ts + 500
        };

        symbols.push(MarketParams::symbol_name(sym_id));
        valid_ts.push(v_ts);
        known_ts.push(k_ts);
        let base_price = 50.0 + f64::from(sym_id) * 1.25 + ((r % 1000) as f64) * 0.01;
        prices.push(if is_restatement {
            base_price + 0.50
        } else {
            base_price
        });
        volumes.push(100 + (r % 5_000) as i64);
        venues.push(VENUES[(r as usize) % VENUES.len()]);
    }

    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(symbols)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(valid_ts).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(known_ts).with_timezone("UTC")),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Int64Array::from(volumes)),
            Arc::new(StringArray::from(venues)),
        ],
    )?)
}

/// Generates a workload of symbol-set × window lookups under `corpus_root/workload/`.
pub fn generate_market_workload(
    corpus_root: &Path,
    params: &MarketParams,
    num_runs: u32,
) -> Result<MarketWorkload> {
    let wdir = corpus_root.join("workload");
    if wdir.exists() {
        std::fs::remove_dir_all(&wdir).map_err(io_err(&wdir))?;
    }
    std::fs::create_dir_all(&wdir).map_err(io_err(&wdir))?;

    let key_schema = Arc::new(Schema::new(vec![Field::new("symbol", DataType::Utf8, false)]));
    let total_rgs_per_day = (params.files_per_day * params.row_groups_per_file).max(1);
    let cohort_size = (params.symbols / total_rgs_per_day).max(1);
    let slice_us = DAY_US / i64::from(total_rgs_per_day);

    let mut runs = Vec::with_capacity(num_runs as usize);
    for r in 0..num_runs {
        let day = r % params.days;
        let rg_slot = ((r / params.days) % total_rgs_per_day).max(1);
        let cohort_start = (rg_slot * cohort_size) % params.symbols;
        let k_count = cohort_size.clamp(1, 5);
        let syms: Vec<String> = (0..k_count)
            .map(|i| MarketParams::symbol_name((cohort_start + i).min(params.symbols - 1)))
            .collect();

        let rel = format!("workload/run-{r:04}.parquet");
        let path = corpus_root.join(&rel);
        let batch = RecordBatch::try_new(
            Arc::clone(&key_schema),
            vec![Arc::new(StringArray::from(syms)) as ArrayRef],
        )?;
        let file = File::create(&path).map_err(io_err(&path))?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&key_schema), None)?;
        writer.write(&batch)?;
        writer.close()?;

        let day_base = day_start_us(day);
        let window_lo = day_base + i64::from(rg_slot.max(1)) * slice_us;
        let window_hi = window_lo + slice_us - 1;
        let valid_at = window_hi;
        let known_at = valid_at + 1_000_000;

        runs.push(MarketRun {
            run_id: format!("run-{r:04}"),
            keys_file: rel,
            symbols: k_count,
            window_lo,
            window_hi,
            valid_at,
            known_at,
        });
    }

    let wl = MarketWorkload {
        format: MARKET_WORKLOAD_FORMAT.to_string(),
        runs,
    };
    write_json(&corpus_root.join("market-workload.json"), &wl)?;
    Ok(wl)
}

fn rand_u64(rng: &mut rand_chacha::ChaCha12Rng) -> u64 {
    use rand::RngCore;
    rng.next_u64()
}
