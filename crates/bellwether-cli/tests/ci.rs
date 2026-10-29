//! `bellwether ci` (WP17) end to end: a temporary git repository whose base
//! commit declares two steps over the bench's relational corpus (a key step
//! on `events`, a rewrite step whose output is a folder beside it), with a
//! key receipt and a rewrite receipt made by `fix` under `--clock` and
//! signed. Each head changes some step and gives receipts (a file at the
//! head, a trailer in the pull request body, or nothing). Assertions cover
//! verdicts, exit codes, receipt identities and the comment update marker.
//!
//! Deliberate breaks, each failing a named test here:
//! - reading the configuration from the head:
//!   `failures_are_named_and_the_base_configuration_gates` (the head drops
//!   the step, which then is never checked);
//! - accepting any 64-hex line of the body without the step name:
//!   `a_rewrite_receipt_passes_until_its_output_changes`;
//! - skipping the digest comparison for rewrites: the same test's
//!   `data_changed` step;
//! - taking `explain`'s intact flag as the certificate check: the facade's
//!   `tests/threat_model.rs`;
//! - posting a second comment instead of updating: the Action's comment
//!   script, tested by `.github/workflows/ci-action.yml`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use bellwether::{
    EngineOptions, FixOutcome, FixResult, GateMode, GateReport, GateRequest, GateRequirements,
    GateStep, GivenReceipt, ReceiptSource, WhyReport,
};
use bellwether_bench::relational::{generate_relational, RelationalParams};
use common::{counts_output, path, run, sparse_keys, typed};
use serde_json::json;

const CLOCK: &str = "2026-10-27T12:00:00.000Z";

fn git(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "ci")
        .env("GIT_AUTHOR_EMAIL", "ci@example.com")
        .env("GIT_COMMITTER_NAME", "ci")
        .env("GIT_COMMITTER_EMAIL", "ci@example.com")
        .env("GIT_AUTHOR_DATE", "2026-10-27T12:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-10-27T12:00:00Z")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

struct Fx {
    _dir: Option<tempfile::TempDir>,
    /// The repository, as git spells its top level.
    repo: PathBuf,
    events: PathBuf,
    out_after: PathBuf,
    /// The trust file's text, as committed.
    trust: String,
    base: String,
    key_receipt: String,
    key_certificate: String,
    key_workload: String,
    rw_receipt: String,
    rw_workload: String,
}

impl Fx {
    /// A commit on top of `parent` writing `files` (`None` removes one).
    fn commit(&self, parent: &str, files: &[(&str, Option<&str>)]) -> String {
        git(&self.repo, &["checkout", "-q", "--detach", parent]);
        for (f, text) in files {
            let p = self.repo.join(f);
            match text {
                Some(t) => {
                    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
                    std::fs::write(&p, t).unwrap();
                }
                None => std::fs::remove_file(&p).unwrap(),
            }
        }
        git(&self.repo, &["add", "-A"]);
        git(&self.repo, &["commit", "-q", "-m", "change"]);
        git(&self.repo, &["rev-parse", "HEAD"])
    }

    fn config(&self, gate: &str, why: bool, positions_workload: &str) -> String {
        format!(
            "ledger = \"lake/events/.bellwether\"\ngate = \"{gate}\"\nrequire_signature = \
             true\ntrusted_keys = \".bellwether/trusted-keys.toml\"\n\n[ci]\nwhy = {why}\n\n\
             [[step]]\nname = \"positions\"\npaths = [\"pipelines/positions.sql\"]\nroot = \
             \".\"\nworkload = \"{positions_workload}\"\n\n[[step]]\nname = \
             \"repo_counts\"\npaths = [\"pipelines/counts/*.py\"]\nroot = \
             \"../out-after\"\nworkload = \"{}\"\n",
            self.rw_workload
        )
    }

    /// `bellwether ci` over `base..head`, with the body when given; the
    /// comment written, stdout.
    fn ci(&self, base: &str, head: &str, body: Option<&str>, extra: &[&str], code: i32) -> Ci {
        let comment = self.repo.parent().unwrap().join("comment.md");
        let _ = std::fs::remove_file(&comment);
        let body_file = self.repo.parent().unwrap().join("body.md");
        let mut args = vec![
            "--clock",
            CLOCK,
            "ci",
            "--repo",
            path(&self.repo),
            "--base",
            base,
            "--head",
            head,
            "--comment-out",
            path(&comment),
        ];
        if let Some(b) = body {
            std::fs::write(&body_file, b).unwrap();
            args.extend(["--pr-body", path(&body_file)]);
        }
        args.extend_from_slice(extra);
        let stdout = run(&args, code);
        Ci {
            stdout,
            comment: std::fs::read_to_string(&comment).unwrap(),
        }
    }

    fn json(&self, base: &str, head: &str, body: Option<&str>, code: i32) -> GateReport {
        typed(&self.ci(base, head, body, &["--json"], code).stdout)
    }
}

struct Ci {
    stdout: String,
    comment: String,
}

/// The relational corpus under `<repo>/lake`, a key receipt and a rewrite
/// receipt made by `fix` (signed), and the base commit declaring both
/// steps.
fn fixture() -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let fx = fixture_in(dir.path());
    Fx {
        _dir: Some(dir),
        ..fx
    }
}

/// [`fixture`] under `dir`, which outlives it.
fn fixture_in(dir: &Path) -> Fx {
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q"]);
    let repo = PathBuf::from(git(&repo, &["rev-parse", "--show-toplevel"]));
    std::fs::write(repo.join(".gitignore"), "lake/\n").unwrap();
    let lake = repo.join("lake");
    generate_relational(
        &lake,
        &RelationalParams {
            seed: 15,
            repos: 3_000,
            events: 30_000,
            event_rows_per_row_group: 1_000,
            event_row_groups_per_file: 5,
            repo_rows_per_row_group: 100,
            restated_fraction: 0.2,
            null_fraction: 0.01,
            keysets: vec![10],
        },
    )
    .unwrap();
    let events = lake.join("events");
    let ev = path(&events);
    let keys = dir.join("keys-sparse.parquet");
    let k = sparse_keys(&events, 5, &keys);

    // The signing key lives outside the repository; its trust file is
    // committed.
    let key = dir.join("ci.key");
    run(
        &["keys", "generate", "--out", path(&key), "--name", "ci"],
        0,
    );
    let trust_file = repo.join(".bellwether/trusted-keys.toml");
    std::fs::create_dir_all(trust_file.parent().unwrap()).unwrap();
    run(
        &[
            "keys",
            "trust",
            "--file",
            path(&trust_file),
            "--add",
            &format!("{}.pub", path(&key)),
        ],
        0,
    );
    let cli = |args: &[&str], code| run(&[&["--clock", CLOCK, "--json"], args].concat(), code);

    // The key step: why over the sparse keys, then fix its recommendation.
    let why: WhyReport = typed(&cli(&["why", ev, "--from", path(&keys)], 0));
    let key_workload = why.workload.as_ref().unwrap().hash.to_hex();
    let proposal = why.recommended.as_ref().expect("a recommendation").proposal;
    let fix: FixResult = typed(&cli(
        &[
            "fix",
            ev,
            "--proposal",
            &proposal.to_hex(),
            "--workload",
            &key_workload,
            "--signing-key",
            path(&key),
        ],
        0,
    ));
    let FixOutcome::Applied { receipt, .. } = &fix.outcome else {
        panic!("{:?}", fix.outcome)
    };
    let key_receipt = receipt.hash.to_hex();
    let key_certificate = fix.certificate.as_ref().unwrap().hash.to_hex();

    // The rewrite step: a filter pushdown over a consumer plan (as
    // tests/relational.rs), its outputs reconciled and attested.
    let plan = |nodes: serde_json::Value| {
        serde_json::to_vec(&json!({"version": 1, "root": 2, "nodes": nodes})).unwrap()
    };
    let scan = json!({"op": "scan", "dataset": "main", "columns": ["repo_id"]});
    let before_plan = dir.join("before.json");
    std::fs::write(
        &before_plan,
        plan(json!([scan,
            {"op": "aggregate", "input": 0, "group_by": ["repo_id"], "measures": [{"function": "count"}]},
            {"op": "semi_join", "input": 1, "columns": ["repo_id"], "keys": {"from": "run"}}])),
    )
    .unwrap();
    let after_plan = dir.join("after.json");
    std::fs::write(
        &after_plan,
        plan(json!([scan,
            {"op": "semi_join", "input": 0, "columns": ["repo_id"], "keys": {"from": "run"}},
            {"op": "aggregate", "input": 1, "group_by": ["repo_id"], "measures": [{"function": "count"}]}])),
    )
    .unwrap();
    let why: WhyReport = typed(&cli(
        &[
            "why",
            ev,
            "--from",
            path(&keys),
            "--plan",
            path(&before_plan),
        ],
        0,
    ));
    let rw_workload = why.workload.as_ref().unwrap().hash.to_hex();
    let evidence = why.evidence.hash.unwrap().to_hex();
    let p = run(
        &[
            "propose",
            ev,
            "--evidence",
            &evidence,
            "--before",
            "count every repository's events, then keep the run's keys",
            "--after",
            "keep the run's keys, then count their events",
            "--invariant",
            "the same (repo_id, count) rows",
            "--before-plan",
            path(&before_plan),
            "--after-plan",
            path(&after_plan),
        ],
        0,
    );
    let rw_proposal = p.trim().trim_start_matches("proposal ").to_string();
    let (bo, ao) = (lake.join("out-before"), lake.join("out-after"));
    counts_output(&events, &k, &bo);
    counts_output(&events, &k, &ao);
    let fix: FixResult = typed(&cli(
        &[
            "fix",
            ev,
            "--proposal",
            &rw_proposal,
            "--workload",
            &rw_workload,
            "--before-output",
            path(&bo),
            "--after-output",
            path(&ao),
            "--key",
            "repo_id",
            "--signing-key",
            path(&key),
        ],
        0,
    ));
    let FixOutcome::Attested { receipt, .. } = &fix.outcome else {
        panic!("{:?}", fix.outcome)
    };
    let rw_receipt = receipt.hash.to_hex();

    let mut fx = Fx {
        trust: std::fs::read_to_string(&trust_file).unwrap(),
        repo,
        events,
        out_after: ao,
        base: String::new(),
        key_receipt,
        key_certificate,
        key_workload,
        rw_receipt,
        rw_workload,
        _dir: None,
    };
    let config = fx.config("require", false, &fx.key_workload);
    std::fs::create_dir_all(fx.repo.join("pipelines/counts")).unwrap();
    std::fs::write(
        fx.repo.join("pipelines/positions.sql"),
        "select * from positions\n",
    )
    .unwrap();
    std::fs::write(fx.repo.join("pipelines/counts/agg.py"), "count()\n").unwrap();
    std::fs::write(fx.repo.join(".bellwether/ci.toml"), config).unwrap();
    git(&fx.repo, &["add", "-A"]);
    git(&fx.repo, &["commit", "-q", "-m", "base"]);
    fx.base = git(&fx.repo, &["rev-parse", "HEAD"]);
    fx
}

#[test]
fn a_changed_step_with_a_signed_key_receipt_passes() {
    let fx = fixture();
    let head = fx.commit(
        &fx.base,
        &[
            (
                "pipelines/positions.sql",
                Some("select id from positions\n"),
            ),
            (
                ".bellwether/receipts/positions",
                Some(&format!("{}\n", fx.key_receipt)),
            ),
        ],
    );
    let c = fx.ci(&fx.base, &head, None, &[], 0);
    assert!(c.comment.starts_with("<!-- bellwether-ci -->\n"));

    // --json is the facade's GateReport, byte for byte (ADR 010).
    let line = fx.ci(&fx.base, &head, None, &["--json"], 0).stdout;
    let trusted = bellwether::parse_trusted_keys(
        &fx.trust,
        &format!(
            ".bellwether/trusted-keys.toml at the base commit {}",
            &fx.base[..12]
        ),
    )
    .unwrap();
    let engine = bellwether::Engine::open(
        path(&fx.events),
        EngineOptions {
            // The ledger as `ci` spells it: the configuration's path under
            // the repository.
            index: Some(
                fx.repo
                    .join("lake/events/.bellwether")
                    .display()
                    .to_string(),
            ),
            trusted_keys: Some(trusted),
            clock: Some(CLOCK.into()),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let report = engine
        .ci_gate(&GateRequest {
            gate: GateMode::Require,
            why: false,
            base: fx.base.clone(),
            head: head.clone(),
            steps: vec![GateStep {
                name: "positions".into(),
                requirements: GateRequirements {
                    root: fx.events.join(".").display().to_string(),
                    workload: bellwether::types::Hash::from_hex(&fx.key_workload).unwrap(),
                    require_signature: true,
                },
                receipt: Some(GivenReceipt {
                    hash: bellwether::types::Hash::from_hex(&fx.key_receipt).unwrap(),
                    from: ReceiptSource::File,
                }),
                problem: None,
            }],
            unchanged: vec!["repo_counts".into()],
        })
        .unwrap();
    assert_eq!(
        line,
        format!("{}\n", serde_json::to_string(&report).unwrap())
    );

    // The same receipt given in the body instead of a file.
    let head = fx.commit(
        &fx.base,
        &[("pipelines/positions.sql", Some("select 2 from positions\n"))],
    );
    let r = fx.json(
        &fx.base,
        &head,
        Some(&format!(
            "Narrow the positions query.\r\n\r\nBellwether-Receipt(positions): {}\r\n",
            fx.key_receipt
        )),
        0,
    );
    assert_eq!(r.steps[0].receipt.unwrap().from, ReceiptSource::Trailer);
    assert!(r.steps[0].pass);

    // No receipt under require fails the changed step.
    let r = fx.json(&fx.base, &head, None, 1);
    assert!(!r.steps[0].pass);
    assert!(r.steps[0].receipt.is_none());

    // No receipt under advise with why: the funnel and the command, exit 0.
    let advise = fx.commit(
        &fx.base,
        &[(
            ".bellwether/ci.toml",
            Some(&fx.config("advise", true, &fx.key_workload)),
        )],
    );
    let head = fx.commit(
        &advise,
        &[("pipelines/positions.sql", Some("select 3 from positions\n"))],
    );
    let r: GateReport = typed(&fx.ci(&advise, &head, None, &["--json"], 0).stdout);
    assert!(r.steps[0].evidence.is_some(), "{r:?}");
}

#[test]
fn a_rewrite_receipt_passes_until_its_output_changes() {
    let fx = fixture();
    let head = fx.commit(
        &fx.base,
        &[("pipelines/counts/agg.py", Some("count(distinct)\n"))],
    );
    let trailer = format!("Bellwether-Receipt(repo_counts): {}\n", fx.rw_receipt);
    let r = fx.json(&fx.base, &head, Some(&trailer), 0);
    assert_eq!(r.outcome, bellwether::GateOutcome::Pass, "{r:?}");
    let v = r.steps[0].verdict.as_ref().unwrap();
    let facts = v.facts.as_ref().unwrap();
    assert_eq!(facts.change, Some(bellwether::ChangeKind::Rewrite));
    assert!(facts.attest_only);
    assert!(facts.realized.is_none());

    // A 64-hex line without the step's name is not a receipt for it.
    for body in [
        format!("{}\n", fx.rw_receipt),
        format!("Bellwether-Receipt(positions): {}\n", fx.rw_receipt),
        format!("Bellwether-Receipt(repo_count): {}\n", fx.rw_receipt),
        format!("bellwether-receipt(repo_counts): {}\n", fx.rw_receipt),
        format!("Bellwether-Receipt(repo_counts): {} \n", fx.rw_receipt),
    ] {
        let r = fx.json(&fx.base, &head, Some(&body), 1);
        assert!(r.steps[0].receipt.is_none(), "{body:?}: {r:?}");
    }

    // One row of the output changed: the receipt is about other data.
    change_one_count(&fx.out_after.join("part.parquet"));
    let r = fx.json(&fx.base, &head, Some(&trailer), 1);
    let v = r.steps[0].verdict.as_ref().unwrap();
    assert_eq!(v.failure.as_ref().unwrap().code(), "data_changed", "{v:?}");
    assert_eq!(v.failed_check(), Some(bellwether::GateCheck::Data));
    let c = fx.ci(&fx.base, &head, Some(&trailer), &[], 1);
    assert!(
        c.stdout.contains("data_changed (data check): "),
        "{}",
        c.stdout
    );
}

/// Rewrites the `(repo_id, count)` file with its first count one higher.
fn change_one_count(file: &Path) {
    use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(file).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    let b = &batches[0];
    let counts = b
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut changed: Vec<i64> = counts.values().to_vec();
    changed[0] += 1;
    let batch = RecordBatch::try_new(
        b.schema(),
        vec![
            b.column_by_name("repo_id").unwrap().clone(),
            std::sync::Arc::new(Int64Array::from(changed)) as ArrayRef,
        ],
    )
    .unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(file).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[test]
fn failures_are_named_and_the_base_configuration_gates() {
    let fx = fixture();
    // The head drops `positions` from the configuration and changes both
    // steps; `repo_counts` is given a certificate hash.
    let config = fx.config("require", false, &fx.key_workload);
    let dropped = config[..config.find("[[step]]").unwrap()].to_string()
        + &config[config.rfind("[[step]]").unwrap()..];
    let head = fx.commit(
        &fx.base,
        &[
            ("pipelines/positions.sql", Some("select 4 from positions\n")),
            ("pipelines/counts/agg.py", Some("count(*)\n")),
            (".bellwether/ci.toml", Some(&dropped)),
        ],
    );
    let body = format!("Bellwether-Receipt(repo_counts): {}\n", fx.key_certificate);
    let r = fx.json(&fx.base, &head, Some(&body), 1);
    assert_eq!(
        r.steps.iter().map(|s| s.step.as_str()).collect::<Vec<_>>(),
        ["positions", "repo_counts"]
    );
    assert_eq!(
        r.steps[1]
            .verdict
            .as_ref()
            .unwrap()
            .failure
            .as_ref()
            .unwrap()
            .code(),
        "wrong_kind"
    );

    // The base declares another workload for `positions`: the key's
    // certificate covered its own.
    let other = fx.commit(
        &fx.base,
        &[(
            ".bellwether/ci.toml",
            Some(&fx.config("require", false, &fx.rw_workload)),
        )],
    );
    let head = fx.commit(
        &other,
        &[
            ("pipelines/positions.sql", Some("select 5 from positions\n")),
            (
                ".bellwether/receipts/positions",
                Some(&format!("{}\n", fx.key_receipt)),
            ),
        ],
    );
    let r = fx.json(&other, &head, None, 1);
    assert_eq!(
        r.steps[0]
            .verdict
            .as_ref()
            .unwrap()
            .failure
            .as_ref()
            .unwrap()
            .code(),
        "wrong_workload"
    );

    // A receipt file that is not a hash, and two different receipts.
    let head = fx.commit(
        &fx.base,
        &[
            ("pipelines/positions.sql", Some("select 6 from positions\n")),
            (
                ".bellwether/receipts/positions",
                Some(&format!("{}\n\n", fx.key_receipt)),
            ),
        ],
    );
    let r = fx.json(&fx.base, &head, None, 1);
    assert_eq!(
        r.steps[0].problem,
        Some(bellwether::ReceiptProblem::FileNotAHash)
    );
    let head2 = fx.commit(
        &fx.base,
        &[
            ("pipelines/positions.sql", Some("select 7 from positions\n")),
            (
                ".bellwether/receipts/positions",
                Some(&format!("{}\n", fx.key_receipt)),
            ),
        ],
    );
    let r = fx.json(
        &fx.base,
        &head2,
        Some(&format!(
            "Bellwether-Receipt(positions): {}\n",
            fx.rw_receipt
        )),
        1,
    );
    assert!(
        matches!(
            r.steps[0].problem,
            Some(bellwether::ReceiptProblem::TwoReceipts { .. })
        ),
        "{r:?}"
    );

    // A changed list given instead of git's: only the listed step changes.
    let list = fx.repo.parent().unwrap().join("changed.txt");
    std::fs::write(&list, "README.md\npipelines/counts/agg.py\n").unwrap();
    let r: GateReport = typed(
        &fx.ci(
            &fx.base,
            &head2,
            None,
            &["--json", "--changed-files", path(&list)],
            1,
        )
        .stdout,
    );
    assert_eq!(r.steps.len(), 1);
    assert_eq!(r.steps[0].step, "repo_counts");
    // Nothing changed: nothing to check.
    std::fs::write(&list, "README.md\n").unwrap();
    let c = fx.ci(&fx.base, &head2, None, &["--changed-files", path(&list)], 0);
    assert!(
        c.comment
            .contains("### ✅ nothing to check: no step's paths changed"),
        "{}",
        c.comment
    );

    // A record of the chain altered in the ledger: unreadable.
    let receipt: bellwether::types::Record = serde_json::from_slice(
        &std::fs::read(
            fx.events
                .join(".bellwether/ledger")
                .join(format!("{}.rec", fx.key_receipt)),
        )
        .unwrap(),
    )
    .unwrap();
    let bellwether::types::Record::Receipt(receipt) = receipt else {
        panic!()
    };
    let replay = fx
        .events
        .join(".bellwether/ledger")
        .join(format!("{}.rec", receipt.replay.to_hex()));
    let mut bytes = std::fs::read(&replay).unwrap();
    bytes.insert(1, b' ');
    std::fs::write(&replay, bytes).unwrap();
    let r = fx.json(&fx.base, &head2, None, 1);
    let v = r.steps[0].verdict.as_ref().unwrap();
    assert_eq!(v.failure.as_ref().unwrap().code(), "unreadable", "{v:?}");
    assert_eq!(v.failed_check(), Some(bellwether::GateCheck::Chain));
}

#[test]
fn a_configuration_naming_a_path_outside_the_repository_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(repo.join("lake/data")).unwrap();
    git(&repo, &["init", "-q"]);
    let w = "a".repeat(64);
    let cases = [
        (
            format!(
                "ledger = \"lake/data/.bellwether\"\n[[step]]\nname = \"s\"\npaths = \
                 [\"../secrets.sql\"]\nroot = \".\"\nworkload = \"{w}\"\n"
            ),
            "is outside the repository",
        ),
        (
            format!(
                "ledger = \"lake/data/.bellwether\"\nrequire_signature = true\ntrusted_keys = \
                 \"/etc/keys.toml\"\n[[step]]\nname = \"s\"\npaths = [\"a.sql\"]\nroot = \
                 \".\"\nworkload = \"{w}\"\n"
            ),
            "is outside the repository",
        ),
        (
            format!(
                "ledger = \"lake/data/.bellwether\"\n[[step]]\nname = \"../s\"\npaths = \
                 [\"a.sql\"]\nroot = \".\"\nworkload = \"{w}\"\n"
            ),
            "must be letters, digits",
        ),
        (
            "ledger = \"lake/data/.bellwether\"\ngates = \"require\"\n".to_string(),
            "unknown field `gates`",
        ),
        (
            "ledger = \"lake/data/.bellwether\"\n[[step]]\nname = \"s\"\npaths = \
             [\"a.sql\"]\nroot = \".\"\nworkload = \"3f9c\"\n"
                .to_string(),
            "is not a 64-character lowercase hex hash",
        ),
    ];
    for (config, says) in cases {
        std::fs::create_dir_all(repo.join(".bellwether")).unwrap();
        std::fs::write(repo.join(".bellwether/ci.toml"), &config).unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "--allow-empty", "-m", "config"]);
        let head = git(&repo, &["rev-parse", "HEAD"]);
        let out = common::bw()
            .args([
                "ci",
                "--repo",
                path(&repo),
                "--base",
                &head,
                "--head",
                &head,
            ])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{config}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains(says), "{config}: {err}");
        assert!(
            err.contains(".bellwether/ci.toml at the base commit"),
            "{err}"
        );
    }
    // No configuration at the base: the gate cannot be defined by the head.
    let configured = git(&repo, &["rev-parse", "HEAD"]);
    git(&repo, &["rm", "-q", ".bellwether/ci.toml"]);
    git(&repo, &["commit", "-q", "-m", "drop"]);
    let unconfigured = git(&repo, &["rev-parse", "HEAD"]);
    let out = common::bw()
        .args([
            "ci",
            "--repo",
            path(&repo),
            "--base",
            &unconfigured,
            "--head",
            &configured,
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("merge the configuration first"), "{err}");
    let out = common::bw()
        .args([
            "ci",
            "--repo",
            path(&repo),
            "--base",
            "--nope",
            "--head",
            &configured,
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

/// Not a test of its own: builds the fixture repository the Action's
/// workflow (`.github/workflows/ci-action.yml`) gates, under
/// `$BELLWETHER_CI_FIXTURE`, with a passing head and a head missing its
/// receipt, and writes their commits to `shas.env` there.
#[test]
#[ignore = "run by .github/workflows/ci-action.yml"]
fn action_fixture() {
    let dir = PathBuf::from(std::env::var("BELLWETHER_CI_FIXTURE").unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    let fx = fixture_in(&dir);
    let pass = fx.commit(
        &fx.base,
        &[
            (
                "pipelines/positions.sql",
                Some("select id from positions\n"),
            ),
            (
                ".bellwether/receipts/positions",
                Some(&format!("{}\n", fx.key_receipt)),
            ),
        ],
    );
    let missing = fx.commit(&fx.base, &[("pipelines/counts/agg.py", Some("count(*)\n"))]);
    std::fs::write(
        dir.join("shas.env"),
        format!(
            "REPO={}\nBASE={}\nPASS={pass}\nMISSING={missing}\n",
            path(&fx.repo),
            fx.base
        ),
    )
    .unwrap();
}
