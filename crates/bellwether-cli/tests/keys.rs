//! `keys` and signing end to end through the CLI (ADR 029): generate, show,
//! and trust round trip; a key file others can read is refused with the fix
//! named, before anything is scored; `fix --signing-key` signs and
//! `receipt show --require-signature` gates on it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

mod common;

use std::path::Path;

use bellwether::{FixOutcome, FixResult, GeneratedKey, PublicKeyFile, ReceiptResult, WhyReport};
use common::{bw, corpus, index_dir, keys, path, typed};

/// Runs the binary with no signing or trust variables inherited, asserts
/// the exit code, and returns stdout and stderr.
fn run_env(args: &[&str], env: &[(&str, &str)], code: i32) -> (String, String) {
    let mut cmd = bw();
    cmd.env_remove("BELLWETHER_SIGNING_KEY")
        .env_remove("BELLWETHER_TRUSTED_KEYS");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.args(args).output().unwrap();
    let (stdout, stderr) = (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    );
    assert_eq!(
        out.status.code(),
        Some(code),
        "args {args:?}\nstdout: {stdout}\nstderr: {stderr}"
    );
    (stdout, stderr)
}

fn run(args: &[&str], code: i32) -> String {
    run_env(args, &[], code).0
}

fn generate(dir: &Path, name: &str) -> GeneratedKey {
    let out = dir.join(name);
    typed(&run(
        &[
            "--json",
            "keys",
            "generate",
            "--out",
            path(&out),
            "--name",
            name,
        ],
        0,
    ))
}

#[test]
fn keys_generate_show_and_trust_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("ci-key");
    let human = run(&["keys", "generate", "--out", path(&out)], 0);
    assert!(human.contains("readable by you only"), "{human}");
    assert!(human.contains("--signing-key"), "{human}");
    assert_eq!(std::fs::read(&out).unwrap().len(), 32);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&out).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
    let public = format!("{}.pub", out.display());
    let shown: PublicKeyFile = typed(&run(&["--json", "keys", "show", &public], 0));
    assert_eq!(shown.name, "ci-key", "the name defaults to the file name");
    let human = run(&["keys", "show", &public], 0);
    assert!(
        human.starts_with(&format!("key id {}", shown.key_id)),
        "{human}"
    );

    // Never overwritten.
    let (_, err) = run_env(&["keys", "generate", "--out", path(&out)], &[], 2);
    assert!(err.contains("never overwritten"), "{err}");

    let trust = dir.path().join("trusted-keys.toml");
    let human = run(
        &[
            "keys",
            "trust",
            "--file",
            path(&trust),
            "--add",
            &public,
            "--valid-from",
            "2026-01-01T00:00:00Z",
        ],
        0,
    );
    assert!(human.contains("now trusts ci-key"), "{human}");
    assert!(human.contains("from 2026-01-01T00:00:00.000Z"), "{human}");
    let text = std::fs::read_to_string(&trust).unwrap();
    assert!(text.contains("[[key]]"), "{text}");
    assert!(text.contains(&shown.key_id.to_hex()), "{text}");
    let (_, err) = run_env(
        &["keys", "trust", "--file", path(&trust), "--add", &public],
        &[],
        2,
    );
    assert!(err.contains("already trusts"), "{err}");
    let (_, err) = run_env(
        &["keys", "trust", "--file", path(&trust), "--add", path(&out)],
        &[],
        2,
    );
    assert!(err.contains("not a public key file"), "{err}");
}

/// The first candidate's proposal and the workload, from `why`.
fn why(data: &Path, kf: &Path) -> (String, String) {
    let w: WhyReport = typed(&run(&["--json", "why", path(data), "--from", path(kf)], 0));
    (
        w.candidates[0].proposal.hash.to_hex(),
        w.workload.unwrap().hash.to_hex(),
    )
}

fn setup(dir: &Path) -> (std::path::PathBuf, String, String) {
    let data = dir.join("data");
    corpus(&data, 3, 4, 25, 400);
    let kf = dir.join("k.parquet");
    keys(&kf, "id", &[5, 77, 123]);
    let (p, wl) = why(&data, &kf);
    (data, p, wl)
}

#[test]
fn an_unusable_signing_key_is_a_usage_error_before_anything_is_scored() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = setup(dir.path());
    let records = |data: &Path| {
        std::fs::read_dir(index_dir(data).join("ledger"))
            .unwrap()
            .count()
    };
    let before = records(&data);
    let key = generate(dir.path(), "ci");
    let fix = |key: &str| {
        run_env(
            &[
                "fix",
                path(&data),
                "--proposal",
                &p,
                "--workload",
                &wl,
                "--signing-key",
                key,
            ],
            &[],
            2,
        )
        .1
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&key.path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = fix(&key.path);
        assert!(err.contains("mode 0644"), "{err}");
        assert!(err.contains(&format!("chmod 600 {}", key.path)), "{err}");
    }
    let err = fix(path(&dir.path().join("absent")));
    assert!(err.contains("cannot read the signing key"), "{err}");
    let err = fix(&key.public_path);
    assert!(err.contains("public key file"), "{err}");
    // The variable is a path too, and checked the same way.
    let (_, err) = run_env(
        &["fix", path(&data), "--proposal", &p, "--workload", &wl],
        &[("BELLWETHER_SIGNING_KEY", path(&dir.path().join("absent")))],
        2,
    );
    assert!(err.contains("cannot read the signing key"), "{err}");
    assert_eq!(records(&data), before, "a refused key scored nothing");
}

#[test]
fn fix_signs_and_receipt_show_gates_on_the_signature() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = setup(dir.path());
    let key = generate(dir.path(), "ci");
    let other = generate(dir.path(), "someone");
    let trust = dir.path().join("trusted.toml");
    run(
        &[
            "keys",
            "trust",
            "--file",
            path(&trust),
            "--add",
            &key.public_path,
        ],
        0,
    );
    let untrusting = dir.path().join("other.toml");
    run(
        &[
            "keys",
            "trust",
            "--file",
            path(&untrusting),
            "--add",
            &other.public_path,
        ],
        0,
    );

    let (out, _) = run_env(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &p,
            "--workload",
            &wl,
        ],
        &[("BELLWETHER_SIGNING_KEY", &key.path)],
        0,
    );
    let fix: FixResult = typed(&out);
    let FixOutcome::Applied { receipt, .. } = &fix.outcome else {
        panic!("{:?}", fix.outcome)
    };
    assert_eq!(fix.signatures.len(), 2);
    let receipt = receipt.hash.to_hex();

    // Signed by the trusted key: the gate passes and the lines say so.
    let human = run(
        &[
            "receipt",
            "show",
            path(&data),
            &receipt,
            "--trusted-keys",
            path(&trust),
            "--require-signature",
        ],
        0,
    );
    assert_eq!(human.matches("sig=valid(ci)").count(), 2, "{human}");
    assert!(human.contains("signed by ci"), "{human}");
    // Requiring a signature without saying whom to trust is a usage error.
    let (_, err) = run_env(
        &[
            "receipt",
            "show",
            path(&data),
            &receipt,
            "--require-signature",
        ],
        &[],
        2,
    );
    assert!(err.contains("--trusted-keys"), "{err}");

    // The trust file from the environment; a file that trusts another key:
    // refused with exit 1 and the verdict named, the verdict in --json.
    let (out, _) = run_env(
        &[
            "receipt",
            "show",
            path(&data),
            &receipt,
            "--require-signature",
        ],
        &[("BELLWETHER_TRUSTED_KEYS", path(&untrusting))],
        1,
    );
    assert!(out.contains("refused: --require-signature"), "{out}");
    assert!(out.contains("untrusted"), "{out}");
    let (out, err) = run_env(
        &[
            "--json",
            "receipt",
            "show",
            path(&data),
            &receipt,
            "--trusted-keys",
            path(&untrusting),
            "--require-signature",
        ],
        &[],
        1,
    );
    let shown: ReceiptResult = typed(&out);
    assert_eq!(shown.signed, Some(false));
    assert!(err.contains("untrusted"), "{err}");

    // The ledger view and the report agree.
    let human = run(
        &[
            "ledger",
            "receipts",
            path(&data),
            "--trusted-keys",
            path(&trust),
        ],
        0,
    );
    assert!(human.contains("  signed"), "{human}");
    let human = run(
        &[
            "report",
            path(&data),
            "--html",
            path(&dir.path().join("r.html")),
            "--trusted-keys",
            path(&trust),
        ],
        0,
    );
    assert!(human.contains("1 of 1 receipts signed"), "{human}");
}

#[test]
fn fix_without_a_key_says_unsigned_and_the_gate_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = setup(dir.path());
    let human = run(
        &["fix", path(&data), "--proposal", &p, "--workload", &wl],
        0,
    );
    assert!(
        human.contains("receipt unsigned (no --signing-key"),
        "{human}"
    );
    let out = run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &p,
            "--workload",
            &wl,
            "--verify-only",
        ],
        0,
    );
    let v: FixResult = typed(&out);
    assert_eq!(v.signatures, vec![]);
    let receipts: bellwether::ReceiptsResult =
        typed(&run(&["--json", "ledger", "receipts", path(&data)], 0));
    let receipt = receipts.receipts[0].receipt.hash.to_hex();
    let human = run(&["receipt", "show", path(&data), &receipt], 0);
    assert!(human.contains("unsigned: no signature"), "{human}");
    let key = generate(dir.path(), "ci");
    let trust = dir.path().join("trusted.toml");
    run(
        &[
            "keys",
            "trust",
            "--file",
            path(&trust),
            "--add",
            &key.public_path,
        ],
        0,
    );
    let out = run(
        &[
            "receipt",
            "show",
            path(&data),
            &receipt,
            "--trusted-keys",
            path(&trust),
            "--require-signature",
        ],
        1,
    );
    assert!(out.contains("receipt unsigned"), "{out}");
    assert!(out.contains("sig=none"), "{out}");
}

/// Review round 1: a key or trust file inside the index location exits 2
/// naming where to put it; a planted signature object with a newline and
/// ESC in it cannot forge `receipt show` lines; a deleted link fails the
/// gate naming the link.
#[test]
fn the_gate_refuses_store_side_tricks() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = setup(dir.path());
    let index = index_dir(&data);
    let inside = index.join("signer");
    let (_, err) = run_env(
        &["keys", "generate", "--out", path(&inside), "--name", "ci"],
        &[],
        0,
    );
    let _ = err;
    let (_, err) = run_env(
        &[
            "fix",
            path(&data),
            "--proposal",
            &p,
            "--workload",
            &wl,
            "--signing-key",
            path(&inside),
        ],
        &[],
        2,
    );
    assert!(err.contains("inside the index location"), "{err}");
    assert!(err.contains("--signing-key"), "{err}");

    let key = generate(dir.path(), "ci");
    let trust = dir.path().join("trusted.toml");
    run(
        &[
            "keys",
            "trust",
            "--file",
            path(&trust),
            "--add",
            &key.public_path,
        ],
        0,
    );
    let planted_trust = index.join("trusted.toml");
    std::fs::copy(&trust, &planted_trust).unwrap();
    let fix: FixResult = typed(
        &run_env(
            &[
                "--json",
                "fix",
                path(&data),
                "--proposal",
                &p,
                "--workload",
                &wl,
                "--signing-key",
                &key.path,
            ],
            &[],
            0,
        )
        .0,
    );
    let FixOutcome::Applied { receipt, .. } = &fix.outcome else {
        panic!("{:?}", fix.outcome)
    };
    let receipt = receipt.hash.to_hex();
    for cmd in [
        vec![
            "receipt",
            "show",
            path(&data),
            receipt.as_str(),
            "--require-signature",
        ],
        vec!["ledger", "receipts", path(&data)],
    ] {
        let mut args = cmd.clone();
        args.extend(["--trusted-keys", path(&planted_trust)]);
        let (_, err) = run_env(&args, &[], 2);
        assert!(err.contains("choose who is trusted"), "{err}");
    }

    // A planted object beside the receipt with a newline and ESC in a field
    // name: the output stays one line per link, with no escape byte.
    let ledger = index.join("ledger");
    let sig = std::fs::read_dir(&ledger)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            let n = p.file_name().unwrap().to_string_lossy().into_owned();
            n.starts_with(&receipt) && n.contains(".sig.") && !n.contains(".lock#")
        })
        .unwrap();
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&sig).unwrap()).unwrap();
    v["x\n  receipt      signed by ci\u{1b}[2J\nok: signature verified"] = serde_json::json!(1);
    let forged = sig.with_file_name(format!(
        "{receipt}.sig.{}.json",
        bellwether::types::Hash::of(b"other").to_hex()
    ));
    std::fs::write(&forged, serde_json::to_vec(&v).unwrap()).unwrap();
    let (out, _) = run_env(
        &[
            "receipt",
            "show",
            path(&data),
            &receipt,
            "--trusted-keys",
            path(&trust),
            "--require-signature",
        ],
        &[],
        1,
    );
    assert!(!out.contains('\u{1b}'), "{out}");
    assert!(
        !out.lines().any(|l| l.starts_with("ok: signature verified")),
        "{out}"
    );
    assert!(out.contains("INVALID"), "{out}");
    std::fs::remove_file(&forged).unwrap();
    run(
        &[
            "receipt",
            "show",
            path(&data),
            &receipt,
            "--trusted-keys",
            path(&trust),
            "--require-signature",
        ],
        0,
    );

    // A deleted link: refused, the link named.
    let Some(bellwether::Score::Replayed { replay, .. }) = &fix.score else {
        panic!()
    };
    std::fs::remove_file(ledger.join(format!("{}.rec", replay.hash))).unwrap();
    let (out, _) = run_env(
        &[
            "receipt",
            "show",
            path(&data),
            &receipt,
            "--trusted-keys",
            path(&trust),
            "--require-signature",
        ],
        &[],
        1,
    );
    assert!(out.contains("refused: --require-signature"), "{out}");
    assert!(out.contains("replay") && out.contains("MISSING"), "{out}");
}

/// A receipt planted under its own BLAKE3 name with a newline and ESC in
/// `created_at`: `receipt show` and `ledger receipts` print it escaped,
/// with no forged line and no escape byte.
#[test]
fn planted_record_text_is_escaped_on_the_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let (data, p, wl) = setup(dir.path());
    let fix: FixResult = typed(&run(
        &[
            "--json",
            "fix",
            path(&data),
            "--proposal",
            &p,
            "--workload",
            &wl,
        ],
        0,
    ));
    let FixOutcome::Applied { receipt, .. } = &fix.outcome else {
        panic!("{:?}", fix.outcome)
    };
    let bellwether::types::Record::Receipt(mut r) = receipt.record.clone() else {
        panic!()
    };
    r.created_at = "x\n  receipt      signed by ci\u{1b}[2J\nok: signature verified".into();
    let bytes = serde_json::to_vec(&bellwether::types::Record::Receipt(r)).unwrap();
    let planted = bellwether::types::Hash::of(&bytes).to_hex();
    std::fs::write(
        index_dir(&data)
            .join("ledger")
            .join(format!("{planted}.rec")),
        &bytes,
    )
    .unwrap();
    for args in [
        vec!["receipt", "show", path(&data), planted.as_str()],
        vec!["ledger", "receipts", path(&data)],
    ] {
        let out = run(&args, 0);
        assert!(!out.contains('\u{1b}'), "{args:?}: {out}");
        assert!(
            !out.lines().any(|l| l.starts_with("ok: ") || l.trim_start().starts_with("receipt      signed")),
            "{args:?}: {out}"
        );
        assert!(out.contains("\\u{1b}[2J"), "{args:?}: {out}");
    }
}
