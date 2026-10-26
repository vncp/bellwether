//! The one HTTP client both adapters use: reqwest, blocking, rustls. Secrets
//! travel in headers only, never in a URL, so a transport error that quotes
//! its URL quotes nothing secret.

use std::time::Duration;

use bellwether_types::{Error, Result};

/// Connect timeout for every request.
const CONNECT: Duration = Duration::from_secs(15);
/// Timeout of a metadata request (a log read, a listing).
pub(crate) const METADATA: Duration = Duration::from_secs(120);

/// A blocking client; `query_timeout` bounds the requests that run a
/// caller's query or an export (`None`: no bound).
pub(crate) fn client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .connect_timeout(CONNECT)
        .timeout(None)
        .build()
        .map_err(|e| Error::Source(format!("cannot build an HTTP client ({e})")))
}

/// A transport error, phrased for `engine`.
pub(crate) fn transport(engine: &str, e: reqwest::Error) -> Error {
    let what = if e.is_timeout() {
        "timed out"
    } else if e.is_connect() {
        "could not connect"
    } else {
        "failed"
    };
    Error::Source(format!(
        "{engine}: the request {what} ({}); check that the engine is reachable from here",
        e.without_url()
    ))
}
