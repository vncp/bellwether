//! Engine roots: `clickhouse://` and `kusto://` URIs.
//!
//! ```text
//! clickhouse://[user@]host[:port]/<database>[/<table>[,<table>...]][?secure=true&password=env:NAME]
//! kusto://<cluster>/<database>[/<table>[,<table>...]][?scratch=<storage uri>&endpoint=<url>&auth=default|none]
//! ```
//!
//! Records name the root without user and parameters ([`EngineUri::root`]);
//! a secret is only ever read from an environment variable (`env:NAME`), so
//! it never appears on a command line or in a record. Database and table
//! names are ASCII letters, digits, `_`, and `-`, so they are safe inside
//! the metadata statements an adapter builds.

use std::collections::BTreeMap;

use bellwether_types::{Error, Result};

/// Which adapter a root names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineKind {
    /// ClickHouse over its HTTP interface.
    ClickHouse,
    /// Kusto (Azure Data Explorer) over its REST API.
    Kusto,
}

impl EngineKind {
    /// The URI scheme and the model prefix: `clickhouse`, `kusto`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClickHouse => "clickhouse",
            Self::Kusto => "kusto",
        }
    }
}

/// Whether `s` is an engine root (`clickhouse://`, `kusto://`).
#[must_use]
pub fn is_engine_uri(s: &str) -> bool {
    s.starts_with("clickhouse://") || s.starts_with("kusto://")
}

/// `s` with user and parameters removed when it is an engine root, as
/// records and messages print it; otherwise `s` unchanged.
#[must_use]
pub fn redact(s: &str) -> String {
    EngineUri::parse(s).map_or_else(|_| s.to_string(), |u| u.root())
}

/// A parsed engine root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineUri {
    /// The adapter.
    pub kind: EngineKind,
    /// `host` or `host:port`.
    pub host: String,
    /// The user, when given (ClickHouse).
    pub user: Option<String>,
    /// The database.
    pub database: String,
    /// The tables, in the order given; empty means every table of the
    /// database.
    pub tables: Vec<String>,
    /// The query parameters, with `env:` values left as given.
    pub params: BTreeMap<String, String>,
}

fn usage(given: &str, why: &str) -> Error {
    Error::InvalidArgument {
        field: "twin_of",
        reason: format!(
            "{} is not an engine root: {why}; give clickhouse://[user@]host[:port]/<database>[/<table>] \
             or kusto://<cluster>/<database>[/<table>]",
            redact_raw(given)
        ),
    }
}

/// The scheme, host, and path of `given`, never its user or parameters.
fn redact_raw(given: &str) -> String {
    let no_query = given.split('?').next().unwrap_or(given);
    match no_query.split_once("://") {
        Some((scheme, rest)) => {
            let rest = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
            format!("{scheme}://{rest}")
        }
        None => no_query.to_string(),
    }
}

fn name_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

impl EngineUri {
    /// Parses `given`.
    pub fn parse(given: &str) -> Result<Self> {
        let (scheme, rest) = given
            .split_once("://")
            .ok_or_else(|| usage(given, "it has no scheme"))?;
        let kind = match scheme {
            "clickhouse" => EngineKind::ClickHouse,
            "kusto" => EngineKind::Kusto,
            _ => return Err(usage(given, "the scheme is not clickhouse or kusto")),
        };
        let (rest, query) = match rest.split_once('?') {
            Some((r, q)) => (r, Some(q)),
            None => (rest, None),
        };
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let (user, host) = match authority.rsplit_once('@') {
            Some((u, h)) => {
                if u.contains(':') {
                    return Err(usage(
                        given,
                        "a password is never written in the root; pass password=env:NAME",
                    ));
                }
                (Some(u.to_string()), h)
            }
            None => (None, authority),
        };
        if host.is_empty()
            || !host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-:[]".contains(&b))
        {
            return Err(usage(given, "the host is missing or malformed"));
        }
        if let Some(u) = &user {
            if !name_ok(u) {
                return Err(usage(given, "the user name is malformed"));
            }
        }
        let mut segments = path.split('/').filter(|s| !s.is_empty());
        let database = segments
            .next()
            .ok_or_else(|| usage(given, "it names no database"))?
            .to_string();
        if !name_ok(&database) {
            return Err(usage(
                given,
                "a database name is ASCII letters, digits, '_', and '-'",
            ));
        }
        let tables: Vec<String> = match segments.next() {
            None => Vec::new(),
            Some(t) => t.split(',').map(str::to_string).collect(),
        };
        if segments.next().is_some() {
            return Err(usage(given, "the path has more than a database and tables"));
        }
        if tables.iter().any(|t| !name_ok(t)) {
            return Err(usage(
                given,
                "a table name is ASCII letters, digits, '_', and '-'",
            ));
        }
        let mut params = BTreeMap::new();
        for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let v = url::form_urlencoded::parse(format!("v={v}").as_bytes())
                .next()
                .map(|(_, v)| v.into_owned())
                .unwrap_or_default();
            let allowed: &[&str] = match kind {
                EngineKind::ClickHouse => &["secure", "password", "timeout"],
                EngineKind::Kusto => &["scratch", "endpoint", "auth", "timeout"],
            };
            if !allowed.contains(&k) {
                return Err(usage(
                    given,
                    &format!(
                        "{k:?} is not a parameter of a {} root (it takes {})",
                        kind.as_str(),
                        allowed.join(", ")
                    ),
                ));
            }
            if k == "password" && !v.starts_with("env:") {
                return Err(usage(
                    given,
                    "a password is read from the environment only: password=env:NAME",
                ));
            }
            params.insert(k.to_string(), v);
        }
        Ok(Self {
            kind,
            host: host.to_string(),
            user,
            database,
            tables,
            params,
        })
    }

    /// The root records name: scheme, host, database, and tables.
    #[must_use]
    pub fn root(&self) -> String {
        let mut s = format!("{}://{}/{}", self.kind.as_str(), self.host, self.database);
        if !self.tables.is_empty() {
            s.push('/');
            s.push_str(&self.tables.join(","));
        }
        s
    }

    /// A parameter's value with `env:NAME` resolved.
    pub fn param(&self, name: &str) -> Result<Option<String>> {
        match self.params.get(name) {
            None => Ok(None),
            Some(v) => match v.strip_prefix("env:") {
                Some(var) => std::env::var(var)
                    .map(Some)
                    .map_err(|_| Error::InvalidArgument {
                        field: "twin_of",
                        reason: format!(
                            "{name}=env:{var} names an environment variable that is not set; set \
                         it, or leave {name} out"
                        ),
                    }),
                None => Ok(Some(v.clone())),
            },
        }
    }

    /// The query-timeout parameter in seconds, if given.
    pub fn timeout_secs(&self) -> Result<Option<u64>> {
        self.params
            .get("timeout")
            .map(|t| {
                t.parse::<u64>().map_err(|_| Error::InvalidArgument {
                    field: "twin_of",
                    reason: format!("timeout={t} is not a whole number of seconds"),
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_parse_and_records_name_no_user_or_parameter() {
        let u = EngineUri::parse(
            "clickhouse://reader@ch.example:8123/sales/orders,lines?password=env:CH_PW&secure=true",
        )
        .unwrap();
        assert_eq!(u.kind, EngineKind::ClickHouse);
        assert_eq!(u.user.as_deref(), Some("reader"));
        assert_eq!(u.tables, vec!["orders", "lines"]);
        assert_eq!(u.root(), "clickhouse://ch.example:8123/sales/orders,lines");
        let k =
            EngineUri::parse("kusto://help.example/db?scratch=abfss%3A%2F%2Fc%40a%2Fs").unwrap();
        assert_eq!(k.root(), "kusto://help.example/db");
        assert_eq!(k.params["scratch"], "abfss://c@a/s");
        assert_eq!(
            redact("clickhouse://u@h/db/t?password=env:X"),
            "clickhouse://h/db/t"
        );
    }

    #[test]
    fn malformed_roots_and_literal_secrets_are_refused_without_echoing_them() {
        for bad in [
            "clickhouse://u:secret@h/db",
            "clickhouse://h/db?password=secret",
            "clickhouse://h",
            "clickhouse://h/db/t;drop",
            "clickhouse://h/db/t/x",
            "clickhouse://h/db?user=x",
            "postgres://h/db",
            "kusto://h/db/t'x",
        ] {
            let e = EngineUri::parse(bad).unwrap_err().to_string();
            assert!(!e.contains("secret"), "{e}");
        }
    }
}
