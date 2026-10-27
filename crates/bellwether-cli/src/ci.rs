//! `ci`: the receipt gate on a pull request (WP17). Reads the gate's
//! configuration from the base commit, the changed paths from git, and each
//! changed step's receipt hash from the head (`.bellwether/receipts/<step>`)
//! or the pull request body (`Bellwether-Receipt(<step>): <hash>`); the
//! checks are the facade's ([`bellwether::Engine::ci_gate`]). Formats the
//! verdict for a terminal and as the pull request comment.
//!
//! Nothing from the pull request is run: git is asked for object contents
//! (`cat-file`) and path names (`diff --name-only`) only, the configuration
//! and the trust file come from the base commit, and the only values taken
//! from the head are path names and 64-hex hashes.
//!
//! Exit codes: 0 every changed step passes (or `gate = "advise"`, or no
//! step changed); 1 a step fails under `gate = "require"`; 2 usage or a
//! malformed configuration; 4 the ledger could not be read.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use bellwether::types::Hash;
use bellwether::{
    EngineOptions, GateCheck, GateFailure, GateMode, GateOutcome, GateReport, GateRequest,
    GateRequirements, GateStep, GateVerdict, GivenReceipt, ReceiptProblem, ReceiptSource,
    StepReport, Verdict,
};
use clap::Args;
use serde::Deserialize;

use crate::human::{count, num, short, size};
use crate::{done, engine_options, Failure, Global, Out, Outcome, EXIT_FAILED, EXIT_OK};

/// The marker the comment is found and updated by.
pub(crate) const MARKER: &str = "<!-- bellwether-ci -->";

/// Where a step's receipt file lives at the head, by step name.
const RECEIPTS_DIR: &str = ".bellwether/receipts";

/// The trailer a pull request body names a step's receipt with.
const TRAILER: &str = "Bellwether-Receipt(";

/// What a step's `why` says when no candidate reads less.
const NOTHING_RECOMMENDED: &str = "why recommends no key: no candidate reads fewer bytes than the \
                                   current configuration; propose the change this pull request \
                                   makes (bellwether propose) and fix that proposal";

/// The largest receipt file read: a hash and a newline, with room to spare.
const MAX_RECEIPT_FILE: u64 = 4096;

#[derive(Args)]
pub(crate) struct CiArgs {
    /// The gate's configuration: a path in the repository, read from the
    /// base commit (never from the head or the working tree).
    #[arg(long, value_name = "PATH", default_value = ".bellwether/ci.toml")]
    config: String,
    /// The base commit of the pull request: the configuration and the
    /// trust file are read from it.
    #[arg(long, value_name = "SHA")]
    base: String,
    /// The head commit: changed paths and receipt files are read from it.
    #[arg(long, value_name = "SHA")]
    head: String,
    /// A file listing the changed paths, one per line, instead of `git
    /// diff --name-only BASE...HEAD`.
    #[arg(long, value_name = "FILE")]
    changed_files: Option<PathBuf>,
    /// Write the pull request comment (markdown, with its marker) here.
    #[arg(long, value_name = "FILE")]
    comment_out: Option<PathBuf>,
    /// The pull request body, for `Bellwether-Receipt(<step>): <hash>`
    /// lines.
    #[arg(long, value_name = "FILE")]
    pr_body: Option<PathBuf>,
    /// The repository [default: the current directory].
    #[arg(long, value_name = "DIR", default_value = ".")]
    repo: PathBuf,
}

/// `.bellwether/ci.toml`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// The index location whose ledger holds the receipts.
    ledger: String,
    /// The data root the ledger indexes [default: the folder holding a
    /// local ledger named `.bellwether`].
    root: Option<String>,
    #[serde(default)]
    gate: ModeCfg,
    #[serde(default)]
    require_signature: bool,
    trusted_keys: Option<String>,
    #[serde(default)]
    ci: CiCfg,
    #[serde(default, rename = "step")]
    steps: Vec<StepCfg>,
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ModeCfg {
    #[default]
    Require,
    Advise,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CiCfg {
    #[serde(default)]
    why: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StepCfg {
    name: String,
    paths: Vec<String>,
    root: String,
    workload: String,
}

/// A step, validated.
struct Step {
    name: String,
    paths: Vec<String>,
    root: PathBuf,
    workload: Hash,
}

#[allow(clippy::too_many_lines)] // read, validate, gate, write: one pass
pub(crate) fn run(g: &Global, out: &Out, a: &CiArgs) -> Result<Outcome, Failure> {
    let git = Git::new(&a.repo)?;
    let base = git.commit(&a.base, "--base")?;
    let head = git.commit(&a.head, "--head")?;
    let config_path = repo_path(&a.config).map_err(|why| {
        Failure::Usage(format!(
            "--config {:?} {why}; name a file in the repository",
            a.config
        ))
    })?;
    let shown = format!("{config_path} at the base commit {}", &base[..12]);
    let text = git.blob(&base, &config_path)?.ok_or_else(|| {
        Failure::Usage(format!(
            "no {config_path} at the base commit {}: the gate reads its configuration from the \
             base, so a pull request cannot change its own gate; merge the configuration first",
            &base[..12]
        ))
    })?;
    let text = String::from_utf8(text)
        .map_err(|_| Failure::Usage(format!("{shown} is not UTF-8 text")))?;
    let cfg: Config = toml::from_str(&text).map_err(|e| {
        Failure::Usage(format!(
            "{shown} is not a gate configuration: {}",
            e.message().trim()
        ))
    })?;
    if g.engine.index.is_some() {
        return Err(Failure::Usage(
            "ci reads the ledger from its configuration (ledger = ...); drop --index".into(),
        ));
    }
    let repo = git.root.clone();
    let (ledger, data_root) = locations(&cfg, &repo, &shown)?;
    let steps = steps(&cfg, &data_root, &shown)?;
    let trusted = match (&cfg.trusted_keys, cfg.require_signature) {
        (Some(p), _) => {
            let p = repo_path(p)
                .map_err(|why| Failure::Usage(format!("{shown}: trusted_keys {p:?} {why}")))?;
            let bytes = git.blob(&base, &p)?.ok_or_else(|| {
                Failure::Usage(format!(
                    "{shown}: trusted_keys names {p}, which is not in the base commit; commit \
                     the trust file `bellwether keys trust` writes"
                ))
            })?;
            let text = String::from_utf8(bytes)
                .map_err(|_| Failure::Usage(format!("{p} at the base commit is not UTF-8")))?;
            let shown = format!("{p} at the base commit {}", &base[..12]);
            Some(bellwether::parse_trusted_keys(&text, &shown)?)
        }
        (None, true) => {
            return Err(Failure::Usage(format!(
                "{shown}: require_signature = true needs trusted_keys = \"<path>\", the trust \
                 file `bellwether keys trust` writes, committed to the repository"
            )))
        }
        (None, false) => None,
    };

    let changed = match &a.changed_files {
        Some(f) => std::fs::read_to_string(f)
            .map_err(|e| {
                Failure::Usage(format!(
                    "cannot read --changed-files {} ({e}); pass a file of paths, one per line",
                    f.display()
                ))
            })?
            .lines()
            .map(|l| l.trim_end_matches('\r').to_string())
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>(),
        None => git.changed(&base, &head)?,
    };
    let body = match &a.pr_body {
        Some(f) => std::fs::read_to_string(f).map_err(|e| {
            Failure::Usage(format!(
                "cannot read --pr-body {} ({e}); pass the pull request body as a file",
                f.display()
            ))
        })?,
        None => String::new(),
    };

    let mut gate_steps = Vec::new();
    let mut unchanged = Vec::new();
    for s in &steps {
        if !changed
            .iter()
            .any(|c| s.paths.iter().any(|p| glob_match(p, c)))
        {
            unchanged.push(s.name.clone());
            continue;
        }
        let file = git.receipt(&head, &format!("{RECEIPTS_DIR}/{}", s.name))?;
        let (receipt, problem) = given(file, &trailers(&body, &s.name));
        gate_steps.push(GateStep {
            name: s.name.clone(),
            requirements: GateRequirements {
                root: s.root.display().to_string(),
                workload: s.workload,
                require_signature: cfg.require_signature,
            },
            receipt,
            problem,
        });
    }
    let request = GateRequest {
        gate: match cfg.gate {
            ModeCfg::Require => GateMode::Require,
            ModeCfg::Advise => GateMode::Advise,
        },
        why: cfg.ci.why,
        base,
        head,
        steps: gate_steps,
        unchanged,
    };
    let options = EngineOptions {
        index: Some(ledger),
        trusted_keys: trusted,
        ..engine_options(g)?
    };
    // A ledger that cannot be read exits 4, naming where it came from.
    let unreadable = |e: bellwether::types::Error| match e {
        bellwether::types::Error::Store(m) => Failure::Engine(bellwether::types::Error::Store(
            format!("{m}; check ledger in {shown}, and that this job's credentials can read it"),
        )),
        e => Failure::from(e),
    };
    let engine =
        bellwether::Engine::open(&data_root.display().to_string(), options).map_err(unreadable)?;
    let report = engine.ci_gate(&request).map_err(unreadable)?;
    if let Some(path) = &a.comment_out {
        std::fs::write(path, comment(&report, &invocation())).map_err(|e| {
            Failure::Usage(format!(
                "cannot write --comment-out {} ({e}); name a writable file",
                path.display()
            ))
        })?;
    }
    let code = if report.outcome == GateOutcome::Fail {
        EXIT_FAILED
    } else {
        EXIT_OK
    };
    done(&report, human(&report, out), code)
}

/// The ledger location and the data root, from the configuration: a local
/// path is relative to the repository.
fn locations(cfg: &Config, repo: &Path, shown: &str) -> Result<(String, PathBuf), Failure> {
    let local = |p: &str| {
        let p = p.strip_prefix("file://").unwrap_or(p);
        if Path::new(p).is_absolute() {
            PathBuf::from(p)
        } else {
            repo.join(p)
        }
    };
    let remote = cfg.ledger.contains("://") && !cfg.ledger.starts_with("file://");
    let ledger = if remote {
        cfg.ledger.clone()
    } else {
        local(&cfg.ledger).display().to_string()
    };
    let root = match &cfg.root {
        Some(r) => local(r),
        None if !remote
            && Path::new(&ledger).file_name().and_then(|n| n.to_str())
                == Some(bellwether::DEFAULT_INDEX_DIR) =>
        {
            Path::new(&ledger)
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_default()
        }
        None => {
            return Err(Failure::Usage(format!(
                "{shown}: the ledger {} is not a local folder named {}, so its data root cannot \
                 be inferred; add root = \"<the data folder>\"",
                cfg.ledger,
                bellwether::DEFAULT_INDEX_DIR
            )))
        }
    };
    if !root.is_dir() {
        return Err(Failure::Usage(format!(
            "{shown}: the data root {} is not a folder here; check root (or ledger) in the \
             configuration, or mount the lake at that path",
            root.display()
        )));
    }
    Ok((ledger, root))
}

/// The configuration's steps, validated: names usable as a file name,
/// unique; paths in the repository; roots that exist; workload hashes.
fn steps(cfg: &Config, data_root: &Path, shown: &str) -> Result<Vec<Step>, Failure> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(cfg.steps.len());
    for s in &cfg.steps {
        let named = !s.name.is_empty()
            && !s.name.starts_with('.')
            && s.name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
        if !named {
            return Err(Failure::Usage(format!(
                "{shown}: step name {:?} must be letters, digits, '_', '-', or '.', not starting \
                 with '.' (it names the file {RECEIPTS_DIR}/<name>)",
                s.name
            )));
        }
        if !seen.insert(s.name.as_str()) {
            return Err(Failure::Usage(format!(
                "{shown}: two steps are named {:?}; name each once",
                s.name
            )));
        }
        if s.paths.is_empty() {
            return Err(Failure::Usage(format!(
                "{shown}: step {} lists no paths; list the files whose change makes it changed",
                s.name
            )));
        }
        let mut paths = Vec::with_capacity(s.paths.len());
        for p in &s.paths {
            paths.push(repo_path(p).map_err(|why| {
                Failure::Usage(format!("{shown}: step {} path {p:?} {why}", s.name))
            })?);
        }
        if Path::new(&s.root).is_absolute() {
            return Err(Failure::Usage(format!(
                "{shown}: step {} root {:?} is absolute; give it relative to the data root {}",
                s.name,
                s.root,
                data_root.display()
            )));
        }
        let root = data_root.join(&s.root);
        if !root.is_dir() {
            return Err(Failure::Usage(format!(
                "{shown}: step {} root {} is not a folder; give the folder its receipt is about, \
                 relative to the data root {}",
                s.name,
                root.display(),
                data_root.display()
            )));
        }
        let workload = strict_hash(s.workload.as_bytes()).ok_or_else(|| {
            Failure::Usage(format!(
                "{shown}: step {} workload {:?} is not a 64-character lowercase hex hash; give \
                 the workload hash `bellwether why --from <keys>` printed",
                s.name, s.workload
            ))
        })?;
        out.push(Step {
            name: s.name.clone(),
            paths,
            root,
            workload,
        });
    }
    Ok(out)
}

/// A repository-relative path or glob: `Err(why)` when it is absolute,
/// climbs out with `..`, or is empty. A leading `./` is dropped.
fn repo_path(p: &str) -> Result<String, &'static str> {
    let p = p.trim_start_matches("./");
    if p.is_empty() {
        return Err("is empty");
    }
    if p.starts_with('/') || p.contains('\\') || p.contains(':') {
        return Err("is outside the repository (absolute, or not a plain relative path)");
    }
    if p.split('/').any(|s| s == ".." || s.is_empty()) {
        return Err("is outside the repository (it climbs out with .., or has an empty part)");
    }
    Ok(p.to_string())
}

/// Whether `path` matches the glob `pattern`: `*` and `?` within one path
/// part, `**` any number of parts.
fn glob_match(pattern: &str, path: &str) -> bool {
    let pat: Vec<&str> = pattern.split('/').collect();
    let parts: Vec<&str> = path.split('/').collect();
    match_parts(&pat, &parts)
}

fn match_parts(pat: &[&str], parts: &[&str]) -> bool {
    match pat.split_first() {
        None => parts.is_empty(),
        Some((&"**", rest)) => (0..=parts.len()).any(|i| match_parts(rest, &parts[i..])),
        Some((p, rest)) => match parts.split_first() {
            Some((s, more)) => match_part(p.as_bytes(), s.as_bytes()) && match_parts(rest, more),
            None => false,
        },
    }
}

fn match_part(p: &[u8], s: &[u8]) -> bool {
    match p.split_first() {
        None => s.is_empty(),
        Some((b'*', rest)) => (0..=s.len()).any(|i| match_part(rest, &s[i..])),
        Some((b'?', rest)) => !s.is_empty() && match_part(rest, &s[1..]),
        Some((c, rest)) => s.first() == Some(c) && match_part(rest, &s[1..]),
    }
}

/// Exactly 64 lowercase hex characters.
fn strict_hash(b: &[u8]) -> Option<Hash> {
    if b.len() == 64 && b.iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
        Hash::from_hex(std::str::from_utf8(b).ok()?).ok()
    } else {
        None
    }
}

/// A receipt file's hash: exactly 64 lowercase hex characters, optionally
/// followed by one newline; anything else is not a receipt.
fn receipt_file(bytes: &[u8]) -> Option<Hash> {
    let b = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    strict_hash(b)
}

/// The hashes of every `Bellwether-Receipt(<step>): <hash>` line of `body`
/// for this step, in order: the whole line, nothing around it (a carriage
/// return before the newline is allowed).
fn trailers(body: &str, step: &str) -> Vec<Hash> {
    body.split('\n')
        .filter_map(|l| {
            let l = l.strip_suffix('\r').unwrap_or(l);
            let rest = l.strip_prefix(TRAILER)?.strip_prefix(step)?;
            strict_hash(rest.strip_prefix("): ")?.as_bytes())
        })
        .collect()
}

/// A step's receipt file at the head.
#[derive(Clone, Copy)]
enum FileReceipt {
    /// No such file.
    Absent,
    /// A hash.
    Hash(Hash),
    /// A file that is not exactly a hash.
    NotAHash,
}

/// The receipt a step was given, from its file and its trailers; two
/// different hashes are a problem, never a choice.
fn given(file: FileReceipt, trailers: &[Hash]) -> (Option<GivenReceipt>, Option<ReceiptProblem>) {
    let mut all: Vec<GivenReceipt> = Vec::new();
    if let FileReceipt::Hash(h) = file {
        all.push(GivenReceipt {
            hash: h,
            from: ReceiptSource::File,
        });
    }
    all.extend(trailers.iter().map(|h| GivenReceipt {
        hash: *h,
        from: ReceiptSource::Trailer,
    }));
    if let Some(second) = all.iter().find(|r| r.hash != all[0].hash) {
        return (
            None,
            Some(ReceiptProblem::TwoReceipts {
                first: all[0],
                second: *second,
            }),
        );
    }
    match all.first() {
        Some(r) => (Some(*r), None),
        None if matches!(file, FileReceipt::NotAHash) => (None, Some(ReceiptProblem::FileNotAHash)),
        None => (None, None),
    }
}

/// Git, asked for object contents and path names only.
struct Git {
    root: PathBuf,
}

impl Git {
    fn new(repo: &Path) -> Result<Self, Failure> {
        let top = git_out(repo, &["rev-parse", "--show-toplevel"]).map_err(|e| {
            Failure::Usage(format!(
                "{} is not a git repository ({e}); run in the pipeline repository or pass --repo",
                repo.display()
            ))
        })?;
        let top = String::from_utf8_lossy(&top).trim().to_string();
        Ok(Self {
            root: PathBuf::from(top),
        })
    }

    /// The full hash of the commit `rev` names.
    fn commit(&self, rev: &str, flag: &str) -> Result<String, Failure> {
        if rev.starts_with('-') || rev.is_empty() {
            return Err(Failure::Usage(format!(
                "{flag} {rev:?} is not a commit; pass a commit hash"
            )));
        }
        let out = git_out(
            &self.root,
            &[
                "rev-parse",
                "--verify",
                "--quiet",
                "--end-of-options",
                &format!("{rev}^{{commit}}"),
            ],
        )
        .map_err(|_| {
            Failure::Usage(format!(
                "{flag} {rev} is not a commit in {}; fetch it first (actions/checkout with \
                 fetch-depth: 0)",
                self.root.display()
            ))
        })?;
        Ok(String::from_utf8_lossy(&out).trim().to_string())
    }

    /// The bytes of `path` at `commit`, or `None` when it has no such
    /// file. `cat-file` applies no filter or text conversion.
    fn blob(&self, commit: &str, path: &str) -> Result<Option<Vec<u8>>, Failure> {
        let object = format!("{commit}:{path}");
        let kind = git_out(&self.root, &["cat-file", "-t", &object]);
        match kind {
            Ok(k) if k.trim_ascii() == b"blob" => {}
            _ => return Ok(None),
        }
        git_out(&self.root, &["cat-file", "blob", &object])
            .map(Some)
            .map_err(|e| Failure::Usage(format!("cannot read {path} at {commit} ({e})")))
    }

    /// The receipt file `path` at `commit`: absent, a hash, or not a hash
    /// (anything else, a file over [`MAX_RECEIPT_FILE`] bytes included,
    /// which is never read).
    fn receipt(&self, commit: &str, path: &str) -> Result<FileReceipt, Failure> {
        let object = format!("{commit}:{path}");
        match git_out(&self.root, &["cat-file", "-t", &object]) {
            Ok(k) if k.trim_ascii() == b"blob" => {}
            Ok(_) => return Ok(FileReceipt::NotAHash),
            Err(_) => return Ok(FileReceipt::Absent),
        }
        let size = git_out(&self.root, &["cat-file", "-s", &object])
            .ok()
            .and_then(|s| String::from_utf8_lossy(&s).trim().parse::<u64>().ok());
        if size.is_none_or(|n| n > MAX_RECEIPT_FILE) {
            return Ok(FileReceipt::NotAHash);
        }
        Ok(
            match self.blob(commit, path)?.as_deref().and_then(receipt_file) {
                Some(h) => FileReceipt::Hash(h),
                None => FileReceipt::NotAHash,
            },
        )
    }

    /// The paths changed between the merge base of `base` and `head`, and
    /// `head`; a rename lists both names.
    fn changed(&self, base: &str, head: &str) -> Result<Vec<String>, Failure> {
        let out = git_out(
            &self.root,
            &[
                "diff",
                "--name-only",
                "-z",
                "--no-renames",
                "--no-ext-diff",
                "--no-textconv",
                &format!("{base}...{head}"),
            ],
        )
        .map_err(|e| {
            Failure::Usage(format!(
                "cannot list the paths changed between {} and {} ({e}); fetch both (fetch-depth: \
                 0) or pass --changed-files",
                &base[..12],
                &head[..12]
            ))
        })?;
        Ok(out
            .split(|b| *b == 0)
            .filter(|p| !p.is_empty())
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect())
    }
}

/// Runs git in `dir` with no pager, no prompt, and no system or global
/// configuration; returns stdout, or stderr's first line as the error.
fn git_out(dir: &Path, args: &[&str]) -> Result<Vec<u8>, String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_PAGER", "cat")
        .output()
        .map_err(|e| format!("cannot run git: {e}"))?;
    if out.status.success() {
        Ok(out.stdout)
    } else {
        let err = String::from_utf8_lossy(&out.stderr);
        Err(err
            .lines()
            .next()
            .unwrap_or("git failed")
            .trim()
            .to_string())
    }
}

/// `bellwether ci ...` as it was run, each argument quoted for a shell
/// when it needs it.
fn invocation() -> String {
    std::iter::once("bellwether".to_string())
        .chain(std::env::args().skip(1).map(|a| shell_quote(&a)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote(a: &str) -> String {
    let plain = !a.is_empty()
        && a.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/' | b':' | b'=' | b',')
        });
    if plain {
        a.to_string()
    } else {
        format!("'{}'", a.replace('\'', r"'\''"))
    }
}

// -- what a failure says -------------------------------------------------

/// One sentence: the failure's facts, for a terminal and the comment.
#[allow(clippy::too_many_lines)] // one arm per failure
fn failure_text(f: &GateFailure) -> String {
    match f {
        GateFailure::NotFound { ledger } => {
            format!("no record of that hash in the ledger at {ledger}")
        }
        GateFailure::WrongKind {
            record,
            expected,
            found,
        } => format!(
            "{} is {}, not {}",
            short(record),
            found.with_article(),
            expected.with_article()
        ),
        GateFailure::Unreadable {
            record,
            kind,
            reason,
        } => format!(
            "the {kind} {} is unreadable: {reason}",
            short(record),
            kind = kind.as_str()
        ),
        GateFailure::Missing { record, kind } => format!(
            "the chain's {} {} is not in the ledger",
            kind.as_str(),
            short(record)
        ),
        GateFailure::CertificateFailed {
            certificate,
            pass,
            missing_left,
            missing_right,
            mismatched,
            uncomparable,
            totals_delta,
        } => {
            let counts = format!(
                "{} missing on the left, {} missing on the right, {} mismatched, {} \
                 uncomparable, {} totals differ",
                num(*missing_left),
                num(*missing_right),
                num(*mismatched),
                num(*uncomparable),
                num(*totals_delta)
            );
            if *pass {
                format!(
                    "the certificate {} says pass but reports differences ({counts})",
                    short(certificate)
                )
            } else {
                format!("the certificate {} failed ({counts})", short(certificate))
            }
        }
        GateFailure::OtherProposal {
            record,
            kind,
            names,
            proposal,
        } => format!(
            "the {} {} is for {}, not the receipt's proposal {}",
            kind.as_str(),
            short(record),
            names.map_or_else(
                || "no proposal".to_string(),
                |p| format!("proposal {}", short(&p))
            ),
            short(proposal)
        ),
        GateFailure::StaleManifest {
            record,
            kind,
            computed,
            manifest_before,
        } => format!(
            "the {} {} was computed against manifest {}, not the receipt's manifest_before {}",
            kind.as_str(),
            short(record),
            computed.as_deref().unwrap_or("(none)"),
            manifest_before
        ),
        GateFailure::WrongWorkload {
            covered, required, ..
        } => format!(
            "the certificate covered workload {}, not the step's workload {}",
            short(covered),
            short(required)
        ),
        GateFailure::PartialCoverage { coverage, .. } => format!(
            "the certificate compared {}, not every key of the step's workload",
            coverage_text(coverage)
        ),
        GateFailure::DataChanged {
            certified,
            current,
            root,
            ..
        } => format!(
            "the certificate is about data with catalog digest {}, and {root} has digest {} now",
            certified.map_or_else(|| "(none)".to_string(), |h| short(&h)),
            short(current)
        ),
        GateFailure::Unsigned { record, kind } => {
            format!(
                "no signature beside the {} {}",
                kind.as_str(),
                short(record)
            )
        }
        GateFailure::UntrustedKey {
            record,
            kind,
            key_ids,
        } => format!(
            "the {} {} is signed only by untrusted key {}",
            kind.as_str(),
            short(record),
            key_ids.iter().map(short).collect::<Vec<_>>().join(", ")
        ),
        GateFailure::InvalidSignature {
            record,
            kind,
            key_ids,
        } => format!(
            "a signature beside the {} {} does not verify (key {})",
            kind.as_str(),
            short(record),
            key_ids.iter().map(short).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// What to do about a failure.
fn next_step(f: &GateFailure, step: &str) -> String {
    match f {
        GateFailure::NotFound { .. } | GateFailure::WrongKind { .. } => format!(
            "give the receipt hash `bellwether fix` printed for {step} (not a certificate or a \
             proposal)"
        ),
        GateFailure::Unreadable { .. } | GateFailure::Missing { .. } => {
            "the ledger was altered or is incomplete; produce a new receipt with `bellwether fix`"
                .to_string()
        }
        GateFailure::CertificateFailed { .. }
        | GateFailure::OtherProposal { .. }
        | GateFailure::StaleManifest { .. } => {
            "this receipt does not stand; produce a new one with `bellwether fix`".to_string()
        }
        GateFailure::WrongWorkload { required, .. } => format!(
            "run `bellwether fix ... --workload {}` for the step's workload",
            required.to_hex()
        ),
        GateFailure::PartialCoverage { .. } => format!(
            "verify on the step's workload: `bellwether fix ... --workload <hash>` for {step}"
        ),
        GateFailure::DataChanged { .. } => {
            "the data changed since the receipt; run `bellwether fix` again on the data as it is"
                .to_string()
        }
        GateFailure::Unsigned { .. } | GateFailure::UntrustedKey { .. } => {
            "run `bellwether fix --signing-key <file>` with a key in the trust file".to_string()
        }
        GateFailure::InvalidSignature { .. } => {
            "a signature object was altered; sign again with `bellwether fix --signing-key`"
                .to_string()
        }
    }
}

fn coverage_text(c: &bellwether::types::Coverage) -> String {
    use bellwether::types::Coverage;
    match c {
        Coverage::Full => "every key".to_string(),
        Coverage::Workload { workload, keys } => format!(
            "workload {}, {}",
            short(workload),
            count(*keys, "key", "keys")
        ),
        Coverage::StaticProof { identity } => format!("no data (static proof {identity})"),
        Coverage::Sampled { keys, fraction } => format!(
            "a sample of {} ({:.1} %)",
            count(*keys, "key", "keys"),
            fraction * 100.0
        ),
    }
}

/// The command that produces a missing receipt.
fn produce(r: &GateReport, s: &StepReport) -> Vec<String> {
    let w = s.workload.to_hex();
    let root = shell_quote(&r.root);
    let mut out = Vec::new();
    if s.root == r.root {
        // The step's data is the ledger's data root: a key (or layout)
        // receipt, from `why` and `fix`.
        if s.evidence.is_none() {
            out.push(format!("bellwether why {root} --workload {w}"));
        }
        let proposal = match &s.evidence {
            None => "<proposal why printed>".to_string(),
            Some(e) => e
                .recommended
                .as_ref()
                .map_or_else(|| "<proposal>".to_string(), |x| x.proposal.to_hex()),
        };
        out.push(format!(
            "bellwether fix {root} --proposal {proposal} --workload {w}"
        ));
    } else {
        // The step's data is an output folder: a rewrite, attested over
        // its before and after outputs.
        out.push(format!(
            "bellwether why {root} --from <keys.parquet> --plan <before.json>"
        ));
        out.push(format!(
            "bellwether propose {root} --evidence <evidence why printed> --before <text> --after \
             <text> --invariant <text> --before-plan <before.json> --after-plan <after.json>"
        ));
        out.push(format!(
            "bellwether fix {root} --proposal <proposal> --workload {w} --before-output <dir> \
             --after-output {} --key <cols>",
            shell_quote(&s.root)
        ));
    }
    out.push(format!(
        "# then commit the receipt hash to {RECEIPTS_DIR}/{} or add the line \
         `{TRAILER}{}): <receipt>` to the pull request body",
        s.step, s.step
    ));
    out
}

/// One verdict as the comment's signature column says it.
fn verdict_text(v: &Verdict) -> String {
    match v {
        Verdict::Signed { by, .. } => format!("valid: {by}"),
        Verdict::Unsigned => "missing".to_string(),
        Verdict::Untrusted { key_ids, .. } => format!(
            "untrusted key {}",
            key_ids.iter().map(short).collect::<Vec<_>>().join(", ")
        ),
        Verdict::Invalid { invalid, .. } => format!(
            "invalid (key {})",
            invalid
                .iter()
                .map(|i| short(&i.key_id))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The receipt's signature verdict, and the certificate's when it is not
/// signed too.
fn signature_cell(v: &GateVerdict) -> String {
    let Some(f) = v.facts.as_ref() else {
        return "—".to_string();
    };
    let Some(receipt) = &f.signature else {
        return "not required".to_string();
    };
    let mut s = verdict_text(receipt);
    match &f.certificate_signature {
        Some(Verdict::Signed { .. }) => {}
        Some(c) => {
            let _ = write!(s, "; certificate {}", verdict_text(c));
        }
        None => s.push_str("; certificate not read"),
    }
    s
}

fn source_text(r: &GivenReceipt, step: &str) -> String {
    match r.from {
        ReceiptSource::File => format!("{RECEIPTS_DIR}/{step}"),
        ReceiptSource::Trailer => "the pull request body".to_string(),
    }
}

fn problem_text(p: &ReceiptProblem, step: &str) -> String {
    match p {
        ReceiptProblem::FileNotAHash => {
            format!("{RECEIPTS_DIR}/{step} is not exactly one 64-character lowercase hex hash")
        }
        ReceiptProblem::TwoReceipts { first, second } => format!(
            "two different receipts were given ({} in {}, {} in {})",
            short(&first.hash),
            source_text(first, step),
            short(&second.hash),
            source_text(second, step)
        ),
    }
}

/// Why a step failed, in one line, with what to do.
fn step_failure(r: &GateReport, s: &StepReport) -> Option<String> {
    if s.pass {
        return None;
    }
    if let Some(v) = &s.verdict {
        let f = v.failure.as_ref()?;
        let check = v.failed_check().map_or("", GateCheck::as_str);
        return Some(format!(
            "{} ({check} check): {}; {}",
            f.code(),
            failure_text(f),
            next_step(f, &s.step)
        ));
    }
    let mut line = format!("no receipt given for {}", s.step);
    if let Some(p) = &s.problem {
        let _ = write!(line, " ({})", problem_text(p, &s.step));
    }
    let _ = write!(line, "; run: {}", produce(r, s)[0]);
    Some(line)
}

fn headline(r: &GateReport) -> String {
    let n = r.steps.len() as u64;
    let failed = r.steps.iter().filter(|s| !s.pass).count() as u64;
    match r.outcome {
        GateOutcome::NothingToCheck => "nothing to check: no step's paths changed".to_string(),
        GateOutcome::Pass => format!("{} verified", count(n, "step", "steps")),
        GateOutcome::Fail => format!(
            "{} of {} failed the receipt gate",
            num(failed),
            count(n, "step", "steps")
        ),
        GateOutcome::Advisory => format!(
            "advisory: {} of {} would fail the receipt gate (gate = \"advise\", reported only)",
            num(failed),
            count(n, "step", "steps")
        ),
    }
}

// -- terminal ------------------------------------------------------------

fn human(r: &GateReport, out: &Out) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "{}",
        crate::human::heading(out, &format!("ci: {}", headline(r)))
    );
    for st in &r.steps {
        let verdict = match (st.pass, r.gate) {
            (true, _) => "pass",
            (false, GateMode::Advise) => "would fail",
            (false, GateMode::Require) => "FAIL",
        };
        let given = match &st.receipt {
            Some(g) => format!(
                "receipt {} from {}",
                short(&g.hash),
                source_text(g, &st.step)
            ),
            None => "no receipt".to_string(),
        };
        let _ = writeln!(s, "  {}  {verdict}  {given}", st.step);
        if let Some(why) = step_failure(r, st) {
            let _ = writeln!(s, "    {why}");
            if st.receipt.is_none() {
                for line in produce(r, st).iter().skip(1) {
                    let _ = writeln!(s, "      {line}");
                }
            }
        }
        if let Some(e) = &st.why_error {
            let _ = writeln!(s, "    why could not run: {e}");
        }
        if let Some(e) = &st.evidence {
            if let Some(c) = &e.current {
                let _ = writeln!(
                    s,
                    "    why: the workload reads {} ({}) now",
                    count(c.row_groups, "row group", "row groups"),
                    size(c.bytes)
                );
            }
            match &e.recommended {
                Some(x) => {
                    let _ = writeln!(
                        s,
                        "    why recommends {} (proposal {})",
                        bellwether::spec::label(&x.spec),
                        short(&x.proposal)
                    );
                }
                None => {
                    let _ = writeln!(s, "    {NOTHING_RECOMMENDED}");
                }
            }
        }
    }
    if !r.unchanged.is_empty() {
        let _ = writeln!(s, "  unchanged (not evaluated): {}", r.unchanged.join(", "));
    }
    let _ = writeln!(
        s,
        "  ledger {}, base {}, head {}",
        r.ledger,
        &r.base[..12.min(r.base.len())],
        &r.head[..12.min(r.head.len())]
    );
    s
}

// -- the comment ---------------------------------------------------------

/// Markdown table cell text: no line breaks, no table or HTML syntax.
fn cell(s: &str) -> String {
    s.replace(['\n', '\r'], " ")
        .replace('|', "\\|")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn code(s: &str) -> String {
    format!("`{}`", s.replace('`', "'"))
}

/// The pull request comment, its marker first: what `--comment-out`
/// writes, byte for byte.
#[allow(clippy::too_many_lines)] // the layout, top to bottom
pub(crate) fn comment(r: &GateReport, invocation: &str) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "{MARKER}");
    let icon = match r.outcome {
        GateOutcome::Pass | GateOutcome::NothingToCheck => "✅",
        GateOutcome::Fail => "❌",
        GateOutcome::Advisory => "ℹ️",
    };
    let _ = writeln!(s, "### {icon} {}", headline(r));
    let _ = writeln!(s);
    if !r.steps.is_empty() {
        let _ = writeln!(
            s,
            "| step | receipt | certificate | bytes before → after | manifest before → after | \
             signature | verdict |"
        );
        let _ = writeln!(s, "|---|---|---|---|---|---|---|");
        for st in &r.steps {
            let _ = writeln!(s, "{}", row(r, st));
        }
        let _ = writeln!(s);
    }
    for st in r.steps.iter().filter(|st| st.receipt.is_none()) {
        let _ = writeln!(s, "#### {}: no receipt given", code(&st.step));
        let _ = writeln!(s);
        if let Some(p) = &st.problem {
            let _ = writeln!(s, "{}.", cell(&problem_text(p, &st.step)));
            let _ = writeln!(s);
        }
        if let Some(e) = &st.why_error {
            let _ = writeln!(s, "`why` could not run: {}", cell(e));
            let _ = writeln!(s);
        }
        if let Some(e) = &st.evidence {
            let _ = writeln!(
                s,
                "`why` over workload {} (evidence {}):",
                code(&short(&st.workload)),
                e.evidence
                    .map_or_else(|| "none".to_string(), |h| code(&short(&h)))
            );
            let _ = writeln!(s);
            let _ = writeln!(s, "| candidate | row groups | bytes |");
            let _ = writeln!(s, "|---|---:|---:|");
            if let Some(c) = &e.current {
                let _ = writeln!(
                    s,
                    "| current configuration | {} | {} ({}) |",
                    num(c.row_groups),
                    num(c.bytes),
                    size(c.bytes)
                );
            }
            for c in &e.candidates {
                let mark = if e.recommended.as_ref().is_some_and(|x| x.spec == c.spec) {
                    " (recommended)"
                } else {
                    ""
                };
                let _ = writeln!(
                    s,
                    "| {}{mark} | {} | {} ({}) |",
                    code(&bellwether::spec::label(&c.spec)),
                    num(c.proposed.row_groups),
                    num(c.proposed.bytes),
                    size(c.proposed.bytes)
                );
            }
            let _ = writeln!(s);
            if e.recommended.is_none() {
                let _ = writeln!(s, "{NOTHING_RECOMMENDED}.");
                let _ = writeln!(s);
            }
        }
        let _ = writeln!(s, "Produce the receipt:");
        let _ = writeln!(s);
        let _ = writeln!(s, "```sh");
        for line in produce(r, st) {
            let _ = writeln!(s, "{line}");
        }
        let _ = writeln!(s, "```");
        let _ = writeln!(s);
    }
    if !r.unchanged.is_empty() {
        let names: Vec<String> = r.unchanged.iter().map(|n| code(n)).collect();
        let _ = writeln!(s, "Unchanged, not evaluated: {}.", names.join(", "));
        let _ = writeln!(s);
    }
    let _ = writeln!(
        s,
        "<sub>bellwether {} · gate {} · ledger {} · base {} · head {} · {}</sub>",
        r.version,
        code(match r.gate {
            GateMode::Require => "require",
            GateMode::Advise => "advise",
        }),
        code(&r.ledger),
        code(&r.base),
        code(&r.head),
        code(invocation)
    );
    s
}

fn row(r: &GateReport, st: &StepReport) -> String {
    let dash = "—".to_string();
    let receipt = st
        .receipt
        .as_ref()
        .map_or_else(|| "none".to_string(), |g| code(&short(&g.hash)));
    let facts = st.verdict.as_ref().and_then(|v| v.facts.as_ref());
    let certificate = facts.and_then(|f| f.certificate.as_ref()).map_or_else(
        || dash.clone(),
        |c| {
            format!(
                "{}, {}",
                if c.pass { "pass" } else { "FAIL" },
                coverage_text(&c.coverage)
            )
        },
    );
    let bytes = facts.and_then(|f| f.replay.as_ref()).map_or_else(
        || dash.clone(),
        |p| {
            let mut b = format!("{} → {}", size(p.current.bytes), size(p.proposed.bytes));
            if let Some(x) = facts.and_then(|f| f.realized.as_ref()) {
                let _ = write!(b, " (realized {})", size(x.current.bytes));
            }
            b
        },
    );
    let manifest = facts.map_or_else(
        || dash.clone(),
        |f| {
            if f.attest_only {
                format!("{} (attest only)", f.manifest_before)
            } else {
                format!("{} → {}", f.manifest_before, f.manifest_after)
            }
        },
    );
    let signature = st
        .verdict
        .as_ref()
        .map_or_else(|| dash.clone(), signature_cell);
    let verdict = if st.pass {
        "✅ pass".to_string()
    } else {
        let mark = if r.gate == GateMode::Advise {
            "ℹ️"
        } else {
            "❌"
        };
        match st.verdict.as_ref().and_then(|v| v.failure.as_ref()) {
            Some(f) => format!(
                "{mark} {}: {}; {}",
                f.code(),
                failure_text(f),
                next_step(f, &st.step)
            ),
            None => format!("{mark} no receipt given (below: how to produce it)"),
        }
    };
    format!(
        "| {} | {receipt} | {} | {} | {} | {} | {} |",
        code(&st.step),
        cell(&certificate),
        cell(&bytes),
        cell(&manifest),
        cell(&signature),
        cell(&verdict)
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn globs_match_within_and_across_parts() {
        assert!(glob_match("pipelines/a.sql", "pipelines/a.sql"));
        assert!(glob_match("pipelines/*.py", "pipelines/x.py"));
        assert!(!glob_match("pipelines/*.py", "pipelines/sub/x.py"));
        assert!(glob_match("pipelines/**/*.py", "pipelines/sub/deep/x.py"));
        assert!(glob_match("pipelines/**/*.py", "pipelines/x.py"));
        assert!(glob_match("p?.sql", "p1.sql"));
        assert!(!glob_match("p?.sql", "p12.sql"));
    }

    #[test]
    fn paths_outside_the_repository_are_refused() {
        for p in [
            "/etc/passwd",
            "../x",
            "a/../../x",
            "a//b",
            "",
            "C:x",
            "a\\b",
        ] {
            assert!(repo_path(p).is_err(), "{p}");
        }
        assert_eq!(repo_path("./a/b.sql").unwrap(), "a/b.sql");
    }

    #[test]
    fn receipts_parse_strictly() {
        let h = "ab".repeat(32);
        assert!(receipt_file(h.as_bytes()).is_some());
        assert!(receipt_file(format!("{h}\n").as_bytes()).is_some());
        for bad in [
            format!("{h}\n\n"),
            format!(" {h}"),
            h.to_uppercase(),
            h[..63].to_string(),
            format!("{h}0"),
        ] {
            assert!(receipt_file(bad.as_bytes()).is_none(), "{bad:?}");
        }
        let body = format!(
            "Fixes it.\r\nBellwether-Receipt(daily): {h}\r\nBellwether-Receipt(other): {}\n{h}\n\
             Bellwether-Receipt(daily):  {h}\n",
            "cd".repeat(32)
        );
        assert_eq!(trailers(&body, "daily").len(), 1);
        assert_eq!(trailers(&body, "dail").len(), 0);
        assert_eq!(trailers(&body, "nope").len(), 0);
    }
}
