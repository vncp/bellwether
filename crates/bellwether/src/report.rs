//! `report`: one self-contained HTML file (inline SVG and CSS, no scripts,
//! no external resources) rendered from ledger records only: the funnel
//! behind each receipt, the fanout table of each profile Evidence, and
//! realized against predicted bytes where a receipt has both.
//!
//! "Predicted" is the bytes the receipt's Replay computed for the proposal.
//! "Realized" is the current-configuration bytes of the Replay of the same
//! proposal at the receipt's `manifest_after` (`fix` writes it right after a
//! successful apply). Replay is exact (ADR 005), so the two should be equal;
//! a difference is shown, never hidden.
//!
//! A record that cannot be read (tampered bytes, a newer version, an unknown
//! kind, malformed JSON) is skipped and listed, never fatal.

// Floats here are chart coordinates only; every number shown as text is
// printed from its exact integer.
#![allow(clippy::cast_precision_loss)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use bellwether_ledger::{parse_record_path, LEDGER_PREFIX};
use bellwether_types::Store as _;
use bellwether_types::{
    Certificate, Coverage, Error, Evidence, EvidenceBody, Hash, Proposal, ProposalBody, Receipt,
    Record, Replay, Result,
};

use crate::engine::{guard, Engine};
use crate::results::{total, Cost, ReceiptResult, ReportResult};
use crate::spec::label;

/// Profile Evidence rendered, newest first; older ones are counted.
const MAX_EVIDENCE: usize = 50;

struct ReceiptView {
    hash: Hash,
    receipt: Receipt,
    proposal: Option<Proposal>,
    replay: Option<Replay>,
    certificate: Option<Certificate>,
    realized: Option<(Hash, Cost)>,
    /// With trusted keys: the chain as `receipt` shows it, whose `signed`
    /// is the one rule every surface applies.
    signature: Option<ReceiptResult>,
}

impl ReceiptView {
    fn signed(&self) -> bool {
        self.signature.as_ref().and_then(|r| r.signed) == Some(true)
    }
}

impl Engine {
    /// Renders the ledger to one HTML file at `out` (a file path, or an
    /// existing directory, which gets `report.html`). Reads ledger records
    /// only; records this build cannot read are listed and skipped.
    pub fn report(&self, out: &Path) -> Result<ReportResult> {
        guard(|| self.report_inner(out))
    }

    #[allow(clippy::too_many_lines)]
    fn report_inner(&self, out: &Path) -> Result<ReportResult> {
        let ledger = self.ledger()?;
        let mut unreadable: Vec<Hash> = Vec::new();
        let mut replays: Vec<(Hash, Replay)> = Vec::new();
        let mut receipt_records: Vec<(Hash, Receipt)> = Vec::new();
        let mut evidence: Vec<(Hash, Evidence)> = Vec::new();
        let mut hashes: Vec<Hash> = self
            .store()?
            .list(LEDGER_PREFIX)?
            .iter()
            .filter_map(|p| parse_record_path(p))
            .collect();
        hashes.sort();
        for h in hashes {
            match ledger.get(&h) {
                Ok(Record::Replay(r)) => replays.push((h, r)),
                Ok(Record::Receipt(r)) => receipt_records.push((h, r)),
                Ok(Record::Evidence(e)) if matches!(e.body, EvidenceBody::Profile { .. }) => {
                    evidence.push((h, e));
                }
                Ok(_) => {}
                Err(Error::Store(m)) => return Err(Error::Store(m)),
                Err(_) => unreadable.push(h),
            }
        }
        let mut receipts = Vec::new();
        for (h, receipt) in receipt_records {
            let proposal = match ledger.get(&receipt.proposal) {
                Ok(Record::Proposal(p)) => Some(p),
                _ => None,
            };
            let replay = match ledger.get(&receipt.replay) {
                Ok(Record::Replay(r)) => Some(r),
                _ => None,
            };
            let certificate = match ledger.get(&receipt.certificate) {
                Ok(Record::Certificate(c)) => Some(c),
                _ => None,
            };
            // An attest-only receipt applied nothing: there is no realized
            // cost after it, only the measurement and the prediction error.
            let realized = replay
                .as_ref()
                .filter(|_| !receipt.attest_only)
                .and_then(|p| {
                    replays
                        .iter()
                        .filter(|(_, r)| {
                            r.proposal == receipt.proposal
                                && r.workload == p.workload
                                && r.manifest_version == receipt.manifest_after
                        })
                        .max_by(|a, b| a.1.created_at.cmp(&b.1.created_at).then(a.0.cmp(&b.0)))
                        .map(|(h, r)| (*h, total(&r.current)))
                });
            let signature = self.verdicts(&h)?;
            receipts.push(ReceiptView {
                hash: h,
                receipt,
                proposal,
                replay,
                certificate,
                realized,
                signature,
            });
        }
        receipts.sort_by(|a, b| {
            b.receipt
                .created_at
                .cmp(&a.receipt.created_at)
                .then(a.hash.cmp(&b.hash))
        });
        evidence.sort_by(|a, b| {
            b.1.provenance
                .created_at
                .cmp(&a.1.provenance.created_at)
                .then(a.0.cmp(&b.0))
        });
        let omitted = evidence.len().saturating_sub(MAX_EVIDENCE);
        evidence.truncate(MAX_EVIDENCE);

        let html = render(self, &receipts, &evidence, omitted, &unreadable);
        let path = target(out);
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::Other(format!(
                    "cannot create {} ({e}); pass a writable output path",
                    parent.display()
                ))
            })?;
        }
        std::fs::write(&path, html.as_bytes()).map_err(|e| {
            Error::Other(format!(
                "cannot write {} ({e}); pass a writable output path",
                path.display()
            ))
        })?;
        Ok(ReportResult {
            path: path.display().to_string(),
            bytes: html.len() as u64,
            blake3: Hash::of(html.as_bytes()),
            receipts: receipts.iter().map(|r| r.hash).collect(),
            evidence: evidence.iter().map(|(h, _)| *h).collect(),
            realized: receipts.iter().filter(|r| r.realized.is_some()).count() as u64,
            unreadable,
            signed: self.signed_count(&receipts),
        })
    }

    /// With trusted keys: how many receipts are signed (receipt and
    /// certificate).
    fn signed_count(&self, receipts: &[ReceiptView]) -> Option<u64> {
        self.options()
            .trusted_keys
            .as_ref()
            .map(|_| receipts.iter().filter(|r| r.signed()).count() as u64)
    }

    /// With trusted keys: the chain behind `hash` as `receipt` shows it.
    fn verdicts(&self, hash: &Hash) -> Result<Option<ReceiptResult>> {
        if self.options().trusted_keys.is_none() {
            return Ok(None);
        }
        self.receipt(hash).map(Some)
    }
}

fn target(out: &Path) -> PathBuf {
    if out.is_dir() {
        out.join("report.html")
    } else {
        out.to_path_buf()
    }
}

fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => o.push_str("&amp;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&#39;"),
            c => o.push(c),
        }
    }
    o
}

fn short(h: &Hash) -> String {
    h.to_hex()[..12].to_string()
}

/// `n` with its noun in the singular or the plural, `n` grouped by
/// thousands.
fn count(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", num(n), if n == 1 { one } else { many })
}

fn num(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

const STYLE: &str = "
:root{--bg:#ffffff;--fg:#1d2330;--muted:#5b6475;--rule:#d9dde5;--now:#b54a3c;--fix:#2f6f9f;--ok:#2e7d4f;--bad:#b54a3c}
@media (prefers-color-scheme: dark){:root{--bg:#14171d;--fg:#e4e7ee;--muted:#9aa3b5;--rule:#2c323d;--now:#e07a6a;--fix:#6fb0e0;--ok:#6cc08e;--bad:#e07a6a}}
body{background:var(--bg);color:var(--fg);font:15px/1.45 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0 auto;max-width:1040px;padding:24px 16px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:18px;margin:32px 0 8px;border-bottom:1px solid var(--rule);padding-bottom:4px}h3{font-size:15px;margin:20px 0 6px}
p.muted,span.muted{color:var(--muted)}code{font:13px ui-monospace,SFMono-Regular,Menlo,monospace}
table{border-collapse:collapse;width:100%;margin:6px 0 12px;font-size:13px}th,td{border-bottom:1px solid var(--rule);padding:4px 8px;text-align:right;white-space:nowrap}
th:first-child,td:first-child{text-align:left}.scroll{overflow-x:auto}svg text{fill:var(--fg);font:12px sans-serif}
.pass{color:var(--ok)}.fail{color:var(--bad)}.measured,.attest{font-weight:600;border:1px solid var(--muted);border-radius:3px;padding:0 4px}
";

fn render(
    engine: &Engine,
    receipts: &[ReceiptView],
    evidence: &[(Hash, Evidence)],
    omitted: usize,
    unreadable: &[Hash],
) -> String {
    let mut h = String::new();
    let _ = write!(
        h,
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>Bellwether report</title><style>{STYLE}</style></head><body>\
         <h1>Bellwether report</h1><p class=\"muted\">Data root <code>{}</code>, ledger at \
         <code>{}</code>. Every number below is read from ledger records; costs are exact sums \
         of row-group statistics (ADR 005).</p>",
        esc(engine.root()),
        esc(engine.index_location())
    );
    h.push_str("<h2>Receipts</h2>");
    if receipts.is_empty() {
        h.push_str("<p class=\"muted\">No receipt in the ledger yet. <code>bellwether fix</code> writes one.</p>");
    }
    for r in receipts {
        render_receipt(&mut h, r);
    }
    render_realized(&mut h, receipts);
    render_predictions(&mut h, receipts);
    h.push_str("<h2>Fanout per profile evidence</h2>");
    if evidence.is_empty() {
        h.push_str("<p class=\"muted\">No profile evidence yet. <code>bellwether why</code> writes one.</p>");
    }
    for (hash, e) in evidence {
        render_evidence(&mut h, hash, e);
    }
    if omitted > 0 {
        let _ = write!(
            h,
            "<p class=\"muted\">{omitted} older profile evidence records are not shown.</p>"
        );
    }
    if !unreadable.is_empty() {
        h.push_str("<h2>Unreadable records</h2><p class=\"muted\">A newer record version or an unknown kind; skipped.</p><ul>");
        for u in unreadable {
            let _ = write!(h, "<li><code>{u}</code></li>");
        }
        h.push_str("</ul>");
    }
    h.push_str("</body></html>\n");
    h
}

fn proposal_label(p: Option<&Proposal>) -> String {
    match p.map(|p| &p.body) {
        Some(ProposalBody::Key { spec, .. }) => format!("key {}", label(spec)),
        Some(ProposalBody::Layout { cluster_by }) => format!("layout by {}", cluster_by.join(", ")),
        Some(ProposalBody::Rewrite { shape }) if !shape.pairs.is_empty() => format!(
            "rewrite of {} engine query pair(s), compared by ({})",
            shape.pairs.len(),
            shape.key.join(", ")
        ),
        Some(ProposalBody::Rewrite { shape }) => format!("rewrite to {}", shape.after),
        None => "proposal missing from the ledger".to_string(),
    }
}

#[allow(clippy::too_many_lines)]
fn render_receipt(h: &mut String, r: &ReceiptView) {
    let _ = write!(
        h,
        "<h3>{} <code>{}</code></h3><p class=\"muted\">manifest {} &rarr; {}, {} by {}</p>",
        esc(&proposal_label(r.proposal.as_ref())),
        short(&r.hash),
        esc(&r.receipt.manifest_before),
        esc(&r.receipt.manifest_after),
        esc(&r.receipt.created_at),
        esc(r.proposal.as_ref().map_or("?", |p| p.proposed_by.as_str()))
    );
    if let Some(c) = &r.certificate {
        let coverage = match &c.coverage {
            Coverage::Full => "full".to_string(),
            Coverage::Workload { keys, .. } => format!("workload, {}", count(*keys, "key", "keys")),
            Coverage::Sampled { keys, fraction } => {
                format!("sampled, {} ({fraction})", count(*keys, "key", "keys"))
            }
            Coverage::StaticProof { identity } => format!("static proof {identity}"),
        };
        let _ = write!(
            h,
            "<p>Certificate <code>{}</code>: <span class=\"{}\">{}</span>, coverage {}, {} mismatched, {} missing left, {} missing right.</p>",
            short(&r.receipt.certificate),
            if c.pass { "pass" } else { "fail" },
            if c.pass { "pass" } else { "fail" },
            esc(&coverage),
            num(c.body.mismatched),
            num(c.body.missing_left),
            num(c.body.missing_right)
        );
    }
    if let Some(shown) = &r.signature {
        let class = if r.signed() { "pass" } else { "fail" };
        let _ = write!(
            h,
            "<p>Signatures: <span class=\"{class}\">{}</span>.</p>",
            esc(&crate::receipt::signature_summary(shown))
        );
    }
    let Some(replay) = &r.replay else {
        h.push_str("<p class=\"fail\">The receipt's replay is missing from the ledger.</p>");
        return;
    };
    let now = total(&replay.current);
    let fix = total(&replay.proposed);
    let measured = replay
        .current
        .iter()
        .chain(&replay.proposed)
        .any(|c| !c.exact);
    if r.receipt.attest_only {
        h.push_str(
            "<p><span class=\"attest\">attest-only</span>: verified, nothing applied; a human \
             or a CI gate deploys it.</p>",
        );
    }
    let _ = write!(
        h,
        "<p class=\"muted\">Replay <code>{}</code> over workload <code>{}</code>, {}.</p>",
        short(&r.receipt.replay),
        short(&replay.workload),
        count(replay.current.len() as u64, "run", "runs")
    );
    if measured {
        let _ = write!(
            h,
            "<p><span class=\"measured\">measured, not exact</span>: every cost below is \
             what <code>{}</code> recorded for the queries the engine submitted; units are the \
             engine's storage units and bytes its bytes read.</p>",
            esc(replay.model.as_deref().unwrap_or("an unnamed model"))
        );
    }
    funnel_svg(h, now, fix);
    let (b, a) = if measured {
        ("before (measured)", "after (measured)")
    } else {
        ("before", "after (predicted)")
    };
    let _ = write!(
        h,
        "<div class=\"scroll\"><table><tr><th>summed over runs</th><th>files</th><th>row groups</th><th>bytes</th></tr>\
         <tr><td>{b}</td><td>{}</td><td>{}</td><td>{}</td></tr>\
         <tr><td>{a}</td><td>{}</td><td>{}</td><td>{}</td></tr></table></div>",
        num(now.files),
        num(now.row_groups),
        num(now.bytes),
        num(fix.files),
        num(fix.row_groups),
        num(fix.bytes)
    );
    if let Some(p) = &r.receipt.prediction {
        let ratio = |x: &Option<String>| x.clone().unwrap_or_else(|| "undefined".into());
        let _ = write!(
            h,
            "<div class=\"scroll\"><table><tr><th>prediction error</th><th>rows</th><th>bytes read</th><th>ratio to the twin's rows</th></tr>\
             <tr><td>twin predicted (exact, lake)</td><td>{}</td><td>-</td><td>1.000</td></tr>",
            num(p.predicted_rows)
        );
        for (i, q) in p.pairs.iter().enumerate() {
            let _ = write!(
                h,
                "<tr><td>p{i} engine before (measured)</td><td>{}</td><td>{}</td><td>{}</td></tr>\
                 <tr><td>p{i} engine after (measured)</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                num(q.before_read_rows),
                num(q.before_read_bytes),
                esc(&ratio(&q.before_ratio)),
                num(q.after_read_rows),
                num(q.after_read_bytes),
                esc(&ratio(&q.after_ratio))
            );
        }
        h.push_str("</table></div>");
    }
}

/// Before and after bars per measure, each scaled to its "before" value.
fn funnel_svg(h: &mut String, now: Cost, fix: Cost) {
    let rows = [
        ("files", now.files, fix.files),
        ("row groups", now.row_groups, fix.row_groups),
        ("bytes", now.bytes, fix.bytes),
    ];
    let (w, left, bar) = (640.0_f64, 110.0_f64, 12.0_f64);
    let height = 20.0 + rows.len() as f64 * 40.0;
    let _ = write!(
        h,
        "<svg role=\"img\" aria-label=\"funnel\" viewBox=\"0 0 {w} {height}\" width=\"100%\" style=\"max-width:{w}px\">"
    );
    for (i, (name, before, after)) in rows.iter().enumerate() {
        let y = 10.0 + i as f64 * 40.0;
        let scale = if *before == 0 {
            0.0
        } else {
            (w - left - 90.0) / *before as f64
        };
        let wb = *before as f64 * scale;
        let wa = (*after as f64 * scale).min(w - left - 90.0);
        let pct = if *before == 0 {
            "-".to_string()
        } else {
            format!("{:.2}%", *after as f64 * 100.0 / *before as f64)
        };
        let _ = write!(
            h,
            "<text x=\"0\" y=\"{}\">{name}</text>\
             <rect x=\"{left}\" y=\"{y}\" width=\"{wb:.1}\" height=\"{bar}\" fill=\"var(--now)\"><title>before: {}</title></rect>\
             <rect x=\"{left}\" y=\"{}\" width=\"{wa:.1}\" height=\"{bar}\" fill=\"var(--fix)\"><title>after: {}</title></rect>\
             <text x=\"{}\" y=\"{}\">{pct}</text>",
            y + 16.0,
            num(*before),
            y + bar + 2.0,
            num(*after),
            left + wb.max(wa) + 6.0,
            y + 16.0
        );
    }
    h.push_str("</svg>");
}

fn render_realized(h: &mut String, receipts: &[ReceiptView]) {
    let both: Vec<(&ReceiptView, u64, Hash, u64)> = receipts
        .iter()
        .filter_map(|r| {
            let predicted = total(&r.replay.as_ref()?.proposed).bytes;
            let (rh, realized) = r.realized?;
            Some((r, predicted, rh, realized.bytes))
        })
        .collect();
    h.push_str("<h2>Realized against predicted bytes</h2>");
    if both.is_empty() {
        h.push_str("<p class=\"muted\">No receipt has a later replay of its workload at its manifest_after yet. Run <code>bellwether why</code> with the same keys after <code>fix</code>.</p>");
        return;
    }
    let max = both
        .iter()
        .map(|(_, p, _, r)| (*p).max(*r))
        .max()
        .unwrap_or(1)
        .max(1) as f64;
    let side = 300.0_f64;
    let _ = write!(
        h,
        "<svg role=\"img\" aria-label=\"realized against predicted\" viewBox=\"0 0 {} {}\" width=\"100%\" style=\"max-width:{}px\">\
         <line x1=\"40\" y1=\"{}\" x2=\"{}\" y2=\"10\" stroke=\"var(--muted)\" stroke-dasharray=\"4 4\"/>\
         <text x=\"40\" y=\"{}\">predicted &rarr;</text><text x=\"0\" y=\"10\">realized</text>",
        side + 60.0,
        side + 40.0,
        side + 60.0,
        side + 10.0,
        side + 40.0,
        side + 34.0
    );
    for (r, p, _, z) in &both {
        let x = 40.0 + *p as f64 / max * side;
        let y = 10.0 + side - *z as f64 / max * side;
        let _ = write!(
            h,
            "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"5\" fill=\"var(--fix)\"><title>{}: predicted {}, realized {}</title></circle>",
            short(&r.hash),
            num(*p),
            num(*z)
        );
    }
    h.push_str("</svg><div class=\"scroll\"><table><tr><th>receipt</th><th>predicted bytes</th><th>realized bytes</th><th>difference</th><th>realized by replay</th></tr>");
    for (r, p, rh, z) in &both {
        let diff = i128::from(*z) - i128::from(*p);
        let _ = write!(
            h,
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td class=\"{}\">{diff}</td><td><code>{}</code></td></tr>",
            short(&r.hash),
            num(*p),
            num(*z),
            if diff == 0 { "pass" } else { "fail" },
            short(rh)
        );
    }
    h.push_str("</table></div>");
}

/// The twin's prediction against each engine's measurement, over every
/// attest-only receipt that carries one (ADR 033): whether `why` on the
/// twin can be trusted for that engine, or the engine must be measured.
fn render_predictions(h: &mut String, receipts: &[ReceiptView]) {
    let with: Vec<(&ReceiptView, &bellwether_types::Prediction)> = receipts
        .iter()
        .filter_map(|r| r.receipt.prediction.as_ref().map(|p| (r, p)))
        .collect();
    if with.is_empty() {
        return;
    }
    h.push_str(
        "<h2>Twin prediction against the engine (measured, not exact)</h2><div class=\"scroll\"><table>\
         <tr><th>receipt</th><th>pair</th><th>engine model</th><th>twin predicted rows</th><th>row groups</th>\
         <th>engine read before</th><th>ratio</th><th>engine read after</th><th>ratio</th></tr>",
    );
    for (r, p) in with {
        let ratio = |x: &Option<String>| x.clone().unwrap_or_else(|| "undefined".into());
        for (i, q) in p.pairs.iter().enumerate() {
            let _ = write!(
                h,
                "<tr><td><code>{}</code></td><td>p{i}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                short(&r.hash),
                esc(r.replay.as_ref().and_then(|x| x.model.as_deref()).unwrap_or("-")),
                num(p.predicted_rows),
                num(p.predicted_row_groups),
                num(q.before_read_rows),
                esc(&ratio(&q.before_ratio)),
                num(q.after_read_rows),
                esc(&ratio(&q.after_ratio))
            );
        }
    }
    h.push_str("</table></div>");
}

fn render_evidence(h: &mut String, hash: &Hash, e: &Evidence) {
    let EvidenceBody::Profile {
        root,
        fanout,
        lints,
        workload_keys,
        twin,
    } = &e.body
    else {
        return;
    };
    let _ = write!(
        h,
        "<h3><code>{}</code> {}</h3><p class=\"muted\">{}, manifest {}, {}{}</p>",
        short(hash),
        esc(root),
        esc(&e.provenance.created_at),
        esc(e
            .provenance
            .manifest_version
            .as_deref()
            .unwrap_or("none (not indexed)")),
        count(lints.len() as u64, "lint", "lints"),
        workload_keys.map_or_else(String::new, |k| format!(
            ", workload keys p50/p95/max {}/{}/{}",
            num(k[0]),
            num(k[1]),
            num(k[2])
        ))
    );
    h.push_str("<div class=\"scroll\"><table><tr><th>key</th><th>distinct</th><th>sampled</th><th>files/value p50 / p95 / max</th><th>row groups/value p50 / p95 / max</th><th>of files / row groups</th></tr>");
    for f in fanout {
        let q = |v: &[f64; 3]| format!("{} / {} / {}", v[0], v[1], v[2]);
        let _ = write!(
            h,
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{} / {}</td></tr>",
            esc(&f.columns.join(", ")),
            num(f.distinct),
            if f.sampled { "yes" } else { "no" },
            q(&f.files_per_value),
            q(&f.row_groups_per_value),
            num(f.files_total),
            num(f.row_groups_total)
        );
    }
    h.push_str("</table></div>");
    if let Some(t) = twin {
        let _ = write!(
            h,
            "<p>Lake twin of <code>{}</code>: the engine held {} rows in {} units (identity <code>{}</code>), the lake {} rows at sequence {}; the {} sampled keys hold {} rows in {} row groups and {} files of the lake.</p>",
            esc(&t.engine),
            num(t.engine_rows),
            num(t.units),
            short(&t.identity),
            num(t.lake_rows),
            t.sequence,
            num(t.keys),
            num(t.predicted_rows),
            num(t.predicted_row_groups),
            num(t.predicted_files)
        );
    }
    for l in lints {
        let _ = write!(
            h,
            "<p><b>{}</b> [{}] {:.2}: {}</p>",
            esc(&l.rule),
            esc(&l.columns.join(", ")),
            l.confidence,
            esc(&l.message)
        );
    }
}
