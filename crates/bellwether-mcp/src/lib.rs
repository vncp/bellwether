//! The Bellwether MCP server (WP12): the engine's agent surface over stdio.
//!
//! Every tool is one call into the facade ([`bellwether::Engine`]) with no
//! logic of its own (ADR 010): the text a tool returns is the facade's typed
//! result serialized exactly as the CLI's `--json` prints it, and the
//! structured content is an [`Envelope`] carrying the record hashes the
//! result rests on, any receipt's `explain` chain, a typed [`Refusal`], a
//! job, and the session budget.
//!
//! The trust boundary (ADR 009, `docs/threat-model.md`):
//!
//! - `observe.*` reads, and writes Evidence and cache entries only.
//! - `score.*` asks the engine to score and verify: engine Proposals,
//!   Replays, plan Evidence, and Certificates, each computed by the engine
//!   immediately before it is written. It is charged to the session budget.
//! - `propose.*` writes one Proposal with no score field (the closed input
//!   schema rejects one, nested fields included), or nothing as a dry run.
//! - `apply.receipt` takes a certificate hash only; the facade's gate
//!   refuses a failing certificate, one computed against an older manifest,
//!   and one whose data changed, before anything is written.
//!
//! No tool accepts a path to write, a shell command, or a query string to
//! execute. Logging goes to stderr; stdout is the protocol channel. Nothing
//! is read from the environment.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

pub mod args;
pub mod budget;
pub mod envelope;
pub mod handler;
pub mod jobs;
pub mod prompt;
pub mod refusal;
pub mod server;
pub mod tools;

pub use args::{BudgetArgs, EngineArgs, JoinArgs, KeyArgs};
pub use budget::{Budget, BudgetState};
pub use envelope::{Envelope, Status};
pub use handler::Handler;
pub use refusal::{Refusal, RefusalCode};
pub use server::{Config, Reply, Server};

/// The findings resource: the newest profile Evidence per root.
pub const FINDINGS: &str = "bellwether://findings";
/// The receipts resource: every receipt with its chain status.
pub const RECEIPTS: &str = "bellwether://receipts";
/// The scorecard resource: per rule id, proposals, certificates, receipts.
pub const SCORECARD: &str = "bellwether://scorecard";

/// Why [`serve_stdio`] stopped with an error.
#[derive(Debug)]
pub enum ServeError {
    /// The engine could not open the data folder (before the handshake).
    Open(bellwether::types::Error),
    /// The async runtime, the handshake, or the transport failed.
    Transport(String),
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(e) => write!(f, "{e}"),
            Self::Transport(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for ServeError {}

/// Serves `config` over stdin and stdout until the client disconnects.
/// The engine is opened first, so a bad data root fails before the
/// handshake. Diagnostics go to stderr only.
pub fn serve_stdio(config: &Config) -> Result<(), ServeError> {
    let server = Server::new(config).map_err(ServeError::Open)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| ServeError::Transport(format!("cannot start the async runtime ({e})")))?;
    runtime.block_on(async move {
        use rmcp::ServiceExt as _;
        let service = Handler::new(server)
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|e| ServeError::Transport(format!("the MCP handshake failed ({e})")))?;
        service
            .waiting()
            .await
            .map_err(|e| ServeError::Transport(format!("the MCP server stopped ({e})")))?;
        Ok(())
    })
}
