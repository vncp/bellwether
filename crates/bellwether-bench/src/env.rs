//! The `environment` block of `results.json` (docs/benchmark.md,
//! "Environment"). Every field is read without unsafe code; a field the
//! platform does not expose is `null`, never guessed.

use bellwether_types::Hash;
use serde::{Deserialize, Serialize};

use crate::corpus::GenParams;

/// Where and how a results file was produced.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Environment {
    /// `std::env::consts::OS`.
    pub os: String,
    /// Distribution or product version (`/etc/os-release`, `sw_vers`).
    pub os_version: Option<String>,
    /// Kernel release (`uname -r`).
    pub kernel: Option<String>,
    /// `std::env::consts::ARCH`.
    pub arch: String,
    /// CPU model string.
    pub cpu_model: Option<String>,
    /// Logical cores available to the process.
    pub cores: u64,
    /// Physical memory in bytes.
    pub memory_bytes: Option<u64>,
    /// `rustc --version` of the compiler that built the harness.
    pub rustc: String,
    /// `git rev-parse HEAD` in the working directory.
    pub commit: Option<String>,
    /// Whether tracked files had uncommitted changes.
    pub commit_dirty: Option<bool>,
    /// Storage kind the corpora were read from.
    pub storage: String,
    /// Cache state: the harness never drops the OS page cache, so results
    /// are labeled warm.
    pub cache: String,
    /// The DuckDB CLI version used by the DuckDB baseline, when it ran.
    pub duckdb_cli: Option<String>,
    /// Generator parameters of every corpus in the file.
    pub corpus_parameters: Vec<CorpusParameters>,
}

/// One corpus's identity and parameters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CorpusParameters {
    /// BLAKE3 of `corpus.json`.
    pub corpus_id: Hash,
    /// Corpus directory as given.
    pub path: String,
    /// Generator parameters (including the seed).
    pub params: GenParams,
}

fn run(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then_some(s)
}

fn file_field(path: &str, key: &str) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_once([':', '=']))
        .map(|(_, v)| v.trim().trim_matches('"').to_string())
}

/// Captures the environment for the given corpora.
#[must_use]
pub fn capture(corpus_parameters: Vec<CorpusParameters>) -> Environment {
    let os = std::env::consts::OS.to_string();
    let (os_version, cpu_model, memory_bytes) = if os == "linux" {
        (
            file_field("/etc/os-release", "PRETTY_NAME"),
            file_field("/proc/cpuinfo", "model name"),
            file_field("/proc/meminfo", "MemTotal")
                .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
                .map(|kb| kb * 1024),
        )
    } else if os == "macos" {
        (
            run("sw_vers", &["-productVersion"]),
            run("sysctl", &["-n", "machdep.cpu.brand_string"]),
            run("sysctl", &["-n", "hw.memsize"]).and_then(|v| v.parse().ok()),
        )
    } else {
        (None, None, None)
    };
    Environment {
        os,
        os_version,
        kernel: run("uname", &["-r"]),
        arch: std::env::consts::ARCH.to_string(),
        cpu_model,
        cores: std::thread::available_parallelism().map_or(1, |n| n.get() as u64),
        memory_bytes,
        rustc: env!("BELLWETHER_RUSTC_VERSION").to_string(),
        commit: run("git", &["rev-parse", "HEAD"]),
        commit_dirty: run("git", &["rev-parse", "HEAD"])
            .map(|_| run("git", &["status", "--porcelain", "--untracked-files=no"]).is_some()),
        storage: "local disk".into(),
        cache: "warm: the OS page cache is not dropped between lookups".into(),
        duckdb_cli: crate::harness::duckdb_cli_version(),
        corpus_parameters,
    }
}
