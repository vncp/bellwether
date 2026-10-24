//! `report`: renders `results.json` as one self-contained HTML file.
//!
//! No scripts and no external resources: the charts are inline SVG drawn to
//! scale from the data, hover detail is native SVG `<title>` tooltips, and
//! every chart has a table view in a `<details>` element. Light and dark
//! themes come from `prefers-color-scheme`.
//!
//! Charts:
//! 1. Pruning funnel per run: row groups total, after footer statistics,
//!    after the index (marked pending until it exists), on a log axis, with
//!    the exact minimum (oracle) as a tick.
//! 2. Latency versus corpus size: p50 latency per strategy against corpus
//!    bytes, log-log.
//! 3. Fanout histograms from the ground truth: values per power-of-two bucket
//!    of row groups touched, one small multiple per planted column.

use std::fmt::Write as _;
use std::path::Path;

use crate::results::{CorpusResult, Results, DATAFUSION, DUCKDB, INDEX, PARQUET_STATS, RAW_SCAN};
use crate::{io_err, read_json, Result};

/// Fixed categorical slot per strategy, so a strategy keeps its color in
/// every chart regardless of which strategies are present.
fn slot(strategy: &str) -> usize {
    match strategy {
        RAW_SCAN => 1,
        PARQUET_STATS => 2,
        DUCKDB => 3,
        INDEX => 4,
        DATAFUSION => 5,
        _ => 7,
    }
}

const STYLE: &str = r#"
:root {
  color-scheme: light;
  --page: #f9f9f7; --surface-1: #fcfcfb;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #898781;
  --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
  --series-1: #2a78d6; --series-2: #eb6834; --series-3: #1baf7a; --series-4: #eda100;
  --series-5: #e87ba4; --series-7: #4a3aa7;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --page: #0d0d0d; --surface-1: #1a1a19;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
    --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70; --series-4: #c98500;
    --series-5: #d55181; --series-7: #9085e9;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --page: #0d0d0d; --surface-1: #1a1a19;
  --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #898781;
  --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
  --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70; --series-4: #c98500;
  --series-5: #d55181; --series-7: #9085e9;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--page); color: var(--text-primary);
  font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 1040px; margin: 0 auto; padding: 24px 16px 64px; }
h1 { font-size: 22px; margin: 0 0 4px; }
h2 { font-size: 17px; margin: 32px 0 4px; }
h3 { font-size: 14px; margin: 16px 0 4px; color: var(--text-secondary); }
p.sub { color: var(--text-secondary); margin: 0 0 12px; }
section.card { background: var(--surface-1); border: 1px solid var(--border);
  border-radius: 8px; padding: 16px; margin: 12px 0; overflow-x: auto; }
svg { display: block; max-width: 100%; height: auto; }
svg text { fill: var(--text-muted); font-size: 11px; font-variant-numeric: tabular-nums; }
svg text.ink { fill: var(--text-secondary); }
.grid { stroke: var(--grid); stroke-width: 1; }
.axis { stroke: var(--axis); stroke-width: 1; }
.oracle { stroke: var(--text-primary); stroke-width: 2; }
.pending { fill: none; stroke: var(--axis); stroke-width: 1; }
.legend { display: flex; flex-wrap: wrap; gap: 4px 16px; margin: 4px 0 8px;
  color: var(--text-secondary); font-size: 12px; }
.legend span { display: inline-flex; align-items: center; gap: 6px; }
.sw { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
.tick { width: 2px; height: 12px; background: var(--text-primary); display: inline-block; }
table { border-collapse: collapse; font-size: 12px; font-variant-numeric: tabular-nums; }
th, td { text-align: right; padding: 3px 10px; border-bottom: 1px solid var(--grid); }
th:first-child, td:first-child { text-align: left; }
table.text th, table.text td { text-align: left; vertical-align: top; }
th { color: var(--text-secondary); font-weight: 600; }
details { margin-top: 8px; color: var(--text-secondary); }
summary { cursor: pointer; }
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }
.kpi .label { color: var(--text-secondary); font-size: 12px; }
.kpi .value { font-size: 20px; font-weight: 600; }
"#;

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn compact(v: f64) -> String {
    let a = v.abs();
    if a >= 1e9 {
        format!("{:.1}G", v / 1e9)
    } else if a >= 1e6 {
        format!("{:.1}M", v / 1e6)
    } else if a >= 1e4 {
        format!("{:.0}K", v / 1e3)
    } else if a >= 1e3 {
        format!("{:.1}K", v / 1e3)
    } else if a >= 10.0 || a == 0.0 || (v.fract() == 0.0) {
        format!("{v:.0}")
    } else if a >= 1.0 {
        format!("{v:.1}")
    } else {
        format!("{v:.2}")
    }
}

fn bytes_label(b: f64) -> String {
    if b >= 1e9 {
        format!("{:.1} GB", b / 1e9)
    } else if b >= 1e6 {
        format!("{:.1} MB", b / 1e6)
    } else if b >= 1e3 {
        format!("{:.0} KB", b / 1e3)
    } else {
        format!("{b:.0} B")
    }
}

/// A log10 axis covering `[lo, hi]`, snapped outward to powers of ten.
struct LogAxis {
    lo: f64,
    hi: f64,
    px0: f64,
    px1: f64,
}

impl LogAxis {
    fn new(lo: f64, hi: f64, px0: f64, px1: f64) -> Self {
        let lo = 10f64.powf(lo.max(1e-3).log10().floor());
        let mut hi = 10f64.powf(hi.max(lo * 10.0).log10().ceil());
        if hi <= lo {
            hi = lo * 10.0;
        }
        Self { lo, hi, px0, px1 }
    }

    /// A log axis from a power of ten up to exactly `hi` (for counts with a
    /// known total), so the longest bar ends at the plot edge.
    fn to_exact(lo: f64, hi: f64, px0: f64, px1: f64) -> Self {
        let lo = 10f64.powf(lo.max(1e-3).log10().floor());
        let hi = if hi > lo { hi } else { lo * 10.0 };
        Self { lo, hi, px0, px1 }
    }

    /// Powers of ten in range, plus `hi` itself when it is not one; a power
    /// of ten too close to `hi` to label legibly is dropped.
    fn ticks_with_end(&self) -> Vec<f64> {
        let mut out: Vec<f64> = self
            .ticks()
            .into_iter()
            .filter(|&t| t <= self.hi * 1.0001)
            .collect();
        let ends_on_hi = matches!(out.last(), Some(&t) if (t - self.hi).abs() <= 1e-9 * self.hi);
        if !ends_on_hi {
            out.retain(|&t| self.at(self.hi) - self.at(t) >= 36.0);
            out.push(self.hi);
        }
        out
    }

    fn at(&self, v: f64) -> f64 {
        let v = v.clamp(self.lo, self.hi);
        let t = (v.log10() - self.lo.log10()) / (self.hi.log10() - self.lo.log10());
        self.px0 + t * (self.px1 - self.px0)
    }

    fn ticks(&self) -> Vec<f64> {
        let mut out = Vec::new();
        let mut v = self.lo;
        while v <= self.hi * 1.0001 {
            out.push(v);
            v *= 10.0;
        }
        out
    }
}

fn legend(items: &[(String, String)]) -> String {
    let mut s = String::from("<div class=\"legend\">");
    for (swatch, label) in items {
        let _ = write!(s, "<span>{swatch}{}</span>", esc(label));
    }
    s.push_str("</div>");
    s
}

fn swatch(slot: usize) -> String {
    format!("<i class=\"sw\" style=\"background:var(--series-{slot})\"></i>")
}

/// Pruning funnel per run for one corpus: one row per run (sorted by keys),
/// three bars on a shared log axis, oracle as an ink tick.
fn funnel_chart(c: &CorpusResult) -> String {
    let mut runs: Vec<_> = c.runs.iter().collect();
    runs.sort_by_key(|r| (r.keys, r.run_id.clone()));
    let label_w = 150.0;
    let width = 960.0;
    let right = 24.0;
    let bar = 6.0;
    let gap = 2.0;
    let row_h = 3.0 * bar + 2.0 * gap + 10.0;
    let top = 24.0;
    let height = top + row_h * runs.len() as f64 + 8.0;
    let total = c.row_groups_total.max(1) as f64;
    let axis = LogAxis::to_exact(1.0, total, label_w, width - right);
    let has_index = runs.iter().any(|r| r.funnel.after_index.is_some());

    let mut s = String::new();
    let _ = write!(
        s,
        "<svg viewBox=\"0 0 {width} {height}\" width=\"{width}\" role=\"img\" \
         aria-label=\"Row groups per run at each pruning stage, log scale\">"
    );
    for t in axis.ticks_with_end() {
        let x = axis.at(t);
        let _ = write!(
            s,
            "<line class=\"grid\" x1=\"{x:.1}\" x2=\"{x:.1}\" y1=\"{:.1}\" y2=\"{:.1}\"/>\
             <text x=\"{x:.1}\" y=\"14\" text-anchor=\"middle\">{}</text>",
            top - 4.0,
            height - 4.0,
            compact(t)
        );
    }
    let x0 = axis.at(axis.lo);
    for (i, run) in runs.iter().enumerate() {
        let y = top + row_h * i as f64;
        let _ = write!(
            s,
            "<text class=\"ink\" x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\">{} · {} keys</text>",
            label_w - 8.0,
            y + bar * 1.5 + gap + 4.0,
            esc(&run.run_id),
            compact(run.keys as f64)
        );
        let stages: [(&str, usize, Option<u64>); 3] = [
            (
                "row groups total",
                slot(RAW_SCAN),
                Some(run.funnel.row_groups_total),
            ),
            (
                "after statistics",
                slot(PARQUET_STATS),
                run.funnel.after_stats,
            ),
            ("after index", slot(INDEX), run.funnel.after_index),
        ];
        for (k, (name, sl, value)) in stages.iter().enumerate() {
            let by = y + k as f64 * (bar + gap);
            match value {
                Some(v) => {
                    let x1 = axis.at((*v).max(1) as f64);
                    let w = (x1 - x0).max(2.0);
                    let _ = write!(
                        s,
                        "<path d=\"M{x0:.1},{by:.1} h{:.1} a2,2 0 0 1 2,2 v{:.1} a2,2 0 0 1 -2,2 \
                         h{:.1} z\" fill=\"var(--series-{sl})\"><title>{}: {name} {} of {} row \
                         groups</title></path>",
                        w - 2.0,
                        bar - 4.0,
                        -(w - 2.0),
                        esc(&run.run_id),
                        v,
                        run.funnel.row_groups_total
                    );
                }
                None if k == 2 => {
                    let _ = write!(
                        s,
                        "<text x=\"{:.1}\" y=\"{:.1}\">pending (index not built)</text>",
                        x0 + 4.0,
                        by + bar
                    );
                }
                None => {}
            }
        }
        let ox = axis.at(run.funnel.oracle.max(1) as f64);
        let _ = write!(
            s,
            "<line class=\"oracle\" x1=\"{ox:.1}\" x2=\"{ox:.1}\" y1=\"{:.1}\" y2=\"{:.1}\">\
             <title>{}: exact minimum {} row groups</title></line>",
            y - 1.0,
            y + 3.0 * bar + 2.0 * gap + 1.0,
            esc(&run.run_id),
            run.funnel.oracle
        );
    }
    s.push_str("</svg>");

    let mut items = vec![
        (
            swatch(slot(RAW_SCAN)),
            "row groups total (raw scan)".to_string(),
        ),
        (
            swatch(slot(PARQUET_STATS)),
            "after footer min/max (Parquet statistics)".to_string(),
        ),
    ];
    items.push((
        swatch(slot(INDEX)),
        if has_index {
            "after Bellwether index".to_string()
        } else {
            "after Bellwether index (pending)".to_string()
        },
    ));
    items.push((
        "<i class=\"tick\"></i>".to_string(),
        "exact minimum (oracle)".to_string(),
    ));

    let mut table = String::from(
        "<details><summary>Table view</summary><table><tr><th>run</th><th>keys</th>\
         <th>total</th><th>after stats</th><th>after index</th><th>oracle</th></tr>",
    );
    for r in &runs {
        let opt = |v: Option<u64>| v.map_or_else(|| "–".to_string(), |v| v.to_string());
        let _ = write!(
            table,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            esc(&r.run_id),
            r.keys,
            r.funnel.row_groups_total,
            opt(r.funnel.after_stats),
            opt(r.funnel.after_index),
            r.funnel.oracle
        );
    }
    table.push_str("</table></details>");
    format!("{}{s}{table}", legend(&items))
}

/// p50 latency per strategy against corpus bytes, log-log.
fn latency_chart(results: &Results) -> String {
    let mut corpora: Vec<&CorpusResult> = results.corpora.iter().collect();
    corpora.sort_by_key(|c| c.bytes_total);
    let mut names: Vec<String> = Vec::new();
    for c in &corpora {
        for s in &c.summary {
            if !names.contains(&s.strategy) {
                names.push(s.strategy.clone());
            }
        }
    }
    names.sort_by_key(|n| slot(n));
    let series: Vec<(String, Vec<(f64, f64)>)> = names
        .iter()
        .map(|n| {
            let pts = corpora
                .iter()
                .filter_map(|c| {
                    let s = c.summary.iter().find(|s| &s.strategy == n)?;
                    (s.latency_ms[0] > 0.0).then_some((c.bytes_total as f64, s.latency_ms[0]))
                })
                .collect();
            (n.clone(), pts)
        })
        .collect();
    let all: Vec<(f64, f64)> = series.iter().flat_map(|(_, p)| p.iter().copied()).collect();
    if all.is_empty() {
        return "<p class=\"sub\">No latency data.</p>".into();
    }
    let (width, height) = (960.0, 360.0);
    let (left, right, top, bottom) = (64.0, 120.0, 16.0, 40.0);
    let xmin = all.iter().map(|p| p.0).fold(f64::MAX, f64::min);
    let xmax = all.iter().map(|p| p.0).fold(0.0, f64::max);
    let ymin = all.iter().map(|p| p.1).fold(f64::MAX, f64::min);
    let ymax = all.iter().map(|p| p.1).fold(0.0, f64::max);
    let xa = LogAxis::new(xmin, xmax, left, width - right);
    let ya = LogAxis::new(ymin, ymax, height - bottom, top);

    let mut s = String::new();
    let _ = write!(
        s,
        "<svg viewBox=\"0 0 {width} {height}\" width=\"{width}\" role=\"img\" \
         aria-label=\"Median latency per strategy versus corpus size, log-log\">"
    );
    for t in ya.ticks() {
        let y = ya.at(t);
        let _ = write!(
            s,
            "<line class=\"grid\" x1=\"{left}\" x2=\"{:.1}\" y1=\"{y:.1}\" y2=\"{y:.1}\"/>\
             <text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\">{} ms</text>",
            width - right,
            left - 6.0,
            y + 4.0,
            compact(t)
        );
    }
    for t in xa.ticks() {
        let x = xa.at(t);
        let _ = write!(
            s,
            "<line class=\"grid\" x1=\"{x:.1}\" x2=\"{x:.1}\" y1=\"{top}\" y2=\"{:.1}\"/>\
             <text x=\"{x:.1}\" y=\"{:.1}\" text-anchor=\"middle\">{}</text>",
            height - bottom,
            height - bottom + 16.0,
            bytes_label(t)
        );
    }
    let _ = write!(
        s,
        "<line class=\"axis\" x1=\"{left}\" x2=\"{:.1}\" y1=\"{:.1}\" y2=\"{:.1}\"/>\
         <text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"middle\">corpus size (compressed row-group bytes)</text>",
        width - right,
        height - bottom,
        height - bottom,
        (left + width - right) / 2.0,
        height - 6.0
    );
    // Draw in reverse slot order so the first series sits on top where lines
    // coincide, matching the end label it keeps.
    for (name, pts) in series.iter().rev() {
        if pts.is_empty() {
            continue;
        }
        let sl = slot(name);
        if pts.len() > 1 {
            let d: Vec<String> = pts
                .iter()
                .enumerate()
                .map(|(i, &(x, y))| {
                    format!(
                        "{}{:.1},{:.1}",
                        if i == 0 { "M" } else { "L" },
                        xa.at(x),
                        ya.at(y)
                    )
                })
                .collect();
            let _ = write!(
                s,
                "<path d=\"{}\" fill=\"none\" stroke=\"var(--series-{sl})\" stroke-width=\"2\" \
                 stroke-linejoin=\"round\" stroke-linecap=\"round\"/>",
                d.join(" ")
            );
        }
        for &(x, y) in pts {
            let _ = write!(
                s,
                "<circle cx=\"{:.1}\" cy=\"{:.1}\" r=\"4\" fill=\"var(--series-{sl})\" \
                 stroke=\"var(--surface-1)\" stroke-width=\"2\"><title>{}: {:.2} ms p50 at {}\
                 </title></circle>",
                xa.at(x),
                ya.at(y),
                esc(name),
                y,
                bytes_label(x)
            );
        }
    }
    // End labels: skip any that would collide with one already placed; the
    // legend and tooltips carry those series.
    let mut placed: Vec<(f64, f64)> = Vec::new();
    for (name, pts) in &series {
        if let Some(&(x, y)) = pts.last() {
            let (lx, ly) = (xa.at(x) + 8.0, ya.at(y) + 4.0);
            if placed
                .iter()
                .any(|&(px, py)| (px - lx).abs() < 100.0 && (py - ly).abs() < 13.0)
            {
                continue;
            }
            placed.push((lx, ly));
            let _ = write!(
                s,
                "<text class=\"ink\" x=\"{lx:.1}\" y=\"{ly:.1}\">{}</text>",
                esc(name)
            );
        }
    }
    s.push_str("</svg>");

    let items: Vec<(String, String)> = series
        .iter()
        .filter(|(_, p)| !p.is_empty())
        .map(|(n, _)| (swatch(slot(n)), n.clone()))
        .collect();
    let mut table = String::from(
        "<details><summary>Table view</summary><table><tr><th>corpus</th><th>bytes</th>\
         <th>strategy</th><th>p50 ms</th><th>p95 ms</th><th>max ms</th></tr>",
    );
    for c in &corpora {
        for sm in &c.summary {
            let _ = write!(
                table,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.2}</td><td>{:.2}</td><td>{:.2}</td></tr>",
                esc(&c.path),
                c.bytes_total,
                esc(&sm.strategy),
                sm.latency_ms[0],
                sm.latency_ms[1],
                sm.latency_ms[2]
            );
        }
    }
    table.push_str("</table></details>");
    let note = if corpora.len() < 2 {
        "<p class=\"sub\">One corpus in this results file: run the harness over several \
         corpus sizes to draw lines.</p>"
    } else {
        ""
    };
    format!("{}{note}{s}{table}", legend(&items))
}

/// Small multiples: values per power-of-two bucket of row groups touched.
fn fanout_charts(c: &CorpusResult) -> String {
    let Some(truth) = &c.ground_truth else {
        return "<p class=\"sub\">No expected.json for this corpus; run <code>ground-truth</code> \
                first.</p>"
            .into();
    };
    let mut out = String::new();
    for ap in truth
        .anti_patterns
        .iter()
        .filter(|a| !a.row_groups_histogram.is_empty())
    {
        let buckets = &ap.row_groups_histogram;
        let (width, height) = (460.0, 220.0);
        let (left, right, top, bottom) = (56.0, 12.0, 12.0, 40.0);
        let maxv = buckets.iter().map(|b| b.values).max().unwrap_or(1).max(1) as f64;
        // Clean linear ticks.
        let step = {
            let raw = maxv / 4.0;
            let mag = 10f64.powf(raw.log10().floor());
            let n = raw / mag;
            mag * if n <= 1.0 {
                1.0
            } else if n <= 2.0 {
                2.0
            } else if n <= 5.0 {
                5.0
            } else {
                10.0
            }
        };
        let ymax = (maxv / step).ceil() * step;
        let plot_h = height - top - bottom;
        let band = (width - left - right) / buckets.len() as f64;
        let bw = (band - 2.0).clamp(2.0, 24.0);
        let mut s = String::new();
        let _ = write!(
            s,
            "<svg viewBox=\"0 0 {width} {height}\" width=\"{width}\" role=\"img\" \
             aria-label=\"Fanout histogram of {}\">",
            esc(&ap.columns.join(", "))
        );
        let mut t = 0.0;
        while t <= ymax + 1e-9 {
            let y = top + plot_h * (1.0 - t / ymax);
            let _ = write!(
                s,
                "<line class=\"grid\" x1=\"{left}\" x2=\"{:.1}\" y1=\"{y:.1}\" y2=\"{y:.1}\"/>\
                 <text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\">{}</text>",
                width - right,
                left - 6.0,
                y + 4.0,
                compact(t)
            );
            t += step;
        }
        let base = top + plot_h;
        for (i, b) in buckets.iter().enumerate() {
            let cx = left + band * (i as f64 + 0.5);
            let h = plot_h * b.values as f64 / ymax;
            let label = if b.lo == b.hi {
                b.lo.to_string()
            } else {
                format!("{}–{}", compact(b.lo as f64), compact(b.hi as f64))
            };
            if h > 0.0 {
                let r = 2.0f64.min(h / 2.0).min(bw / 2.0);
                let _ = write!(
                    s,
                    "<path d=\"M{:.1},{base:.1} v{:.1} a{r:.1},{r:.1} 0 0 1 {r:.1},{:.1} h{:.1} \
                     a{r:.1},{r:.1} 0 0 1 {r:.1},{r:.1} v{:.1} z\" fill=\"var(--series-1)\">\
                     <title>{} row groups: {} values</title></path>",
                    cx - bw / 2.0,
                    -(h - r),
                    -r,
                    bw - 2.0 * r,
                    h - r,
                    label,
                    b.values
                );
            }
            // Label every k-th bucket so labels (up to ~48 px) never collide.
            let every = (48.0 / band).ceil().max(1.0) as usize;
            if i % every == 0 {
                let _ = write!(
                    s,
                    "<text x=\"{cx:.1}\" y=\"{:.1}\" text-anchor=\"middle\">{label}</text>",
                    base + 14.0
                );
            }
        }
        let _ = write!(
            s,
            "<line class=\"axis\" x1=\"{left}\" x2=\"{:.1}\" y1=\"{base:.1}\" y2=\"{base:.1}\"/>\
             <text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"middle\">row groups touched per value</text>",
            width - right,
            (left + width - right) / 2.0,
            height - 6.0
        );
        s.push_str("</svg>");
        let mut table = String::from(
            "<details><summary>Table view</summary><table><tr><th>row groups</th><th>values</th></tr>",
        );
        for b in buckets {
            let _ = write!(
                table,
                "<tr><td>{}–{}</td><td>{}</td></tr>",
                b.lo, b.hi, b.values
            );
        }
        table.push_str("</table></details>");
        let fan = ap.fanout.as_ref().map_or_else(String::new, |f| {
            format!(
                "{} distinct · row groups per value p50 {} / p95 {} / max {}",
                f.distinct,
                compact(f.row_groups_per_value[0]),
                compact(f.row_groups_per_value[1]),
                compact(f.row_groups_per_value[2])
            )
        });
        let _ = write!(
            out,
            "<div><h3>{} <span style=\"font-weight:400\">({})</span></h3><p class=\"sub\">{}</p>{s}{table}</div>",
            esc(&ap.columns.join(", ")),
            esc(&ap.rule),
            esc(&fan)
        );
    }
    out
}

/// The ground truth as a table: every catalog rule, whether this corpus
/// plants it (measured), the criterion, and the expert fix.
fn patterns_table(c: &CorpusResult) -> String {
    let Some(truth) = &c.ground_truth else {
        return String::new();
    };
    let mut s = String::from(
        "<table class=\"text\"><tr><th>rule</th><th>columns</th><th>status</th><th>criterion</th>\
         <th>expert fix</th></tr>",
    );
    for ap in &truth.anti_patterns {
        let status = match (ap.deferred.is_some(), ap.planted) {
            (true, true) => "deferred (measured: planted)",
            (true, false) => "deferred (measured: not planted)",
            (false, true) => "planted",
            (false, false) => "not planted",
        };
        let fix = ap
            .fix
            .as_ref()
            .map_or_else(String::new, |f| format!("{} → {}", f.before, f.after));
        let _ = write!(
            s,
            "<tr><td>{}</td><td>{}</td><td>{status}</td><td>{}</td><td>{}</td></tr>",
            esc(&ap.rule),
            esc(&ap.columns.join(", ")),
            esc(ap.deferred.as_deref().unwrap_or(&ap.criterion)),
            esc(&fix)
        );
    }
    s.push_str("</table>");
    s
}

fn kpis(c: &CorpusResult) -> String {
    let tile = |label: &str, value: String| {
        format!(
            "<div class=\"kpi\"><div class=\"label\">{}</div><div class=\"value\">{}</div></div>",
            esc(label),
            esc(&value)
        )
    };
    let mut s = String::from("<div class=\"kpis\">");
    s.push_str(&tile("Files", compact(c.files_total as f64)));
    s.push_str(&tile("Row groups", compact(c.row_groups_total as f64)));
    s.push_str(&tile("Rows", compact(c.rows_total as f64)));
    s.push_str(&tile("Compressed bytes", bytes_label(c.bytes_total as f64)));
    s.push_str(&tile("Recorded runs", c.workload.runs.to_string()));
    if let Some(raw) = c.summary.iter().find(|x| x.strategy == RAW_SCAN) {
        if let (Some(read), Some(planned)) = (raw.bytes_read_total, raw.bytes_planned_total) {
            s.push_str(&tile(
                "Raw scan bytes read / planned",
                format!("{:.4}", read as f64 / planned.max(1) as f64),
            ));
        }
    }
    s.push_str("</div>");
    s
}

/// Renders the report HTML for `results`.
#[must_use]
pub fn render(results: &Results) -> String {
    let mut body = String::new();
    let _ = write!(
        body,
        "<h1>Bellwether benchmark</h1><p class=\"sub\">{} · {} corpora · design fixed by ADR 019. \
         Charts are drawn to scale from results.json; hover a mark for its value.</p>",
        esc(&results.generated_by),
        results.corpora.len()
    );
    let _ = write!(
        body,
        "<h2>Latency versus corpus size</h2><p class=\"sub\">Median end-to-end latency per \
         recorded run (plan plus scan of the planned row groups), log-log.</p>\
         <section class=\"card\">{}</section>",
        latency_chart(results)
    );
    for c in &results.corpora {
        let _ = write!(
            body,
            "<h2>Corpus {}</h2><p class=\"sub\">corpus id {}…, workload {}…</p>\
             <section class=\"card\">{}</section>\
             <h3>Pruning funnel per run</h3><p class=\"sub\">Row groups read at each stage, log \
             scale; runs sorted by key-set size.</p><section class=\"card\">{}</section>\
             <h3>Planted anti-patterns (ground truth)</h3><section class=\"card\">{}</section>\
             <h3>Fanout of the planted columns</h3>\
             <section class=\"card\" style=\"display:flex;flex-wrap:wrap;gap:16px\">{}</section>",
            esc(&c.path),
            &c.corpus_id.to_hex()[..12],
            &c.workload.hash.to_hex()[..12],
            kpis(c),
            funnel_chart(c),
            patterns_table(c),
            fanout_charts(c)
        );
    }
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>Bellwether benchmark</title><style>{STYLE}</style></head>\
         <body><main>{body}</main></body></html>\n"
    )
}

/// Reads `results` and writes the HTML report to `out`.
pub fn write_report(results: &Path, out: &Path) -> Result<()> {
    let r: Results = read_json(results)?;
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).map_err(io_err(parent))?;
    }
    std::fs::write(out, render(&r)).map_err(io_err(out))
}
