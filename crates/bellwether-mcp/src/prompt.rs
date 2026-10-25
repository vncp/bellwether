//! The one prompt, "diagnose a slow query", generated from `AGENTS.md` at
//! the repository root (WP12 addition 13): the prompt is the file, so the
//! two cannot say different things.

/// `AGENTS.md`, embedded at build time.
pub const AGENTS_MD: &str = include_str!("../../../AGENTS.md");

/// The prompt's name.
pub const NAME: &str = "diagnose_slow_query";

/// The prompt's title.
pub const TITLE: &str = "Diagnose a slow query";

/// The prompt's one optional argument.
pub const ARGUMENT: &str = "symptom";

/// The prompt's messages: `AGENTS.md`, then, when given, the symptom the
/// human described.
#[must_use]
pub fn messages(symptom: Option<&str>) -> Vec<String> {
    let mut out = vec![AGENTS_MD.to_string()];
    if let Some(s) = symptom.map(str::trim).filter(|s| !s.is_empty()) {
        out.push(format!("The slow query, as the human describes it: {s}"));
    }
    out
}
