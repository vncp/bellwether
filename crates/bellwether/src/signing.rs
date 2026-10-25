//! Signing keys and the trust file (ADR 029).
//!
//! A signing key is a file holding the 32-byte ed25519 seed, readable by
//! its owner only; its public half sits beside it as `<path>.pub`, JSON
//! `{"key_id", "public_key", "name", "created_at"}`. The engine reads the
//! seed from a path, never from an argument or the environment, holds it in
//! memory only, and writes nothing but the public half into the ledger.
//!
//! The trust file is TOML, one `[[key]]` table per trusted key (`key_id`,
//! `public_key`, `name`, and optionally `valid_from` and `revoked_at`). It
//! belongs to the reader and is never stored in the index location: an
//! attacker who can write the store would otherwise also choose who is
//! trusted.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use bellwether_ledger::{is_utc_millis, PublicKey, SigningKey, TrustedKey, TrustedKeys};
use bellwether_store::{now_rfc3339, rfc3339_millis};
use bellwether_types::{Error, Hash, Result};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

/// The public half of a signing key, as `<key>.pub` holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicKeyFile {
    /// BLAKE3 of the public key's 32 bytes.
    pub key_id: Hash,
    /// The public key.
    pub public_key: PublicKey,
    /// A label for people.
    pub name: String,
    /// When the key was generated, RFC 3339 UTC with milliseconds.
    pub created_at: String,
}

/// What [`generate_key`] wrote.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedKey {
    /// The seed file (mode 0600).
    pub path: String,
    /// The public half, `<path>.pub`.
    pub public_path: String,
    /// The public half's content.
    pub public: PublicKeyFile,
}

/// What [`trust_key`] did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustResult {
    /// The trust file.
    pub file: String,
    /// The key appended.
    pub key: TrustEntry,
    /// Keys the file now trusts.
    pub keys: u64,
}

/// One `[[key]]` table of the trust file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustEntry {
    /// BLAKE3 of the public key's 32 bytes.
    pub key_id: Hash,
    /// The public key.
    pub public_key: PublicKey,
    /// A label for people; verdicts name the signer by it.
    pub name: String,
    /// Signatures made before this instant do not count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<String>,
    /// Signatures made at or after this instant do not count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<String>,
}

#[derive(Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TrustFile {
    #[serde(default)]
    key: Vec<TrustEntry>,
}

fn bad(field: &'static str, reason: String) -> Error {
    Error::InvalidArgument { field, reason }
}

/// `<path>.pub`.
#[must_use]
pub fn public_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".pub");
    PathBuf::from(p)
}

/// Generates a key: 32 random bytes from the operating system written to
/// `out` (created, never overwritten, mode 0600 on Unix), and the public
/// half to `<out>.pub`. `name` defaults to the file name of `out`;
/// `created_at` to now.
pub fn generate_key(
    out: &Path,
    name: Option<&str>,
    created_at: Option<&str>,
) -> Result<GeneratedKey> {
    let pub_path = public_path(out);
    for p in [out, pub_path.as_path()] {
        if p.exists() {
            return Err(bad(
                "out",
                format!(
                    "{} already exists, and a key file is never overwritten; pass another \
                     --out path, or remove the old key yourself if you mean to replace it",
                    p.display()
                ),
            ));
        }
    }
    let name = match name {
        Some(n) if !n.trim().is_empty() => n.to_string(),
        Some(_) => return Err(bad("name", "--name must not be empty".into())),
        None => out
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("key")
            .to_string(),
    };
    let created_at = match created_at {
        Some(c) => rfc3339_millis(c)?,
        None => now_rfc3339(),
    };
    let mut seed = Zeroizing::new([0u8; 32]);
    rand_core::RngCore::try_fill_bytes(&mut rand_core::OsRng, seed.as_mut()).map_err(|e| {
        Error::Other(format!(
            "the operating system's random number generator failed ({e}); no key was written"
        ))
    })?;
    let key = SigningKey::from_seed(&seed);
    let public = PublicKeyFile {
        key_id: key.key_id(),
        public_key: key.public_key(),
        name,
        created_at,
    };
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let write_err = |p: &Path, e: std::io::Error| {
        bad(
            "out",
            format!(
                "cannot write {} ({e}); pass a writable --out path",
                p.display()
            ),
        )
    };
    let mut f = options.open(out).map_err(|e| write_err(out, e))?;
    f.write_all(seed.as_ref())
        .and_then(|()| f.sync_all())
        .map_err(|e| write_err(out, e))?;
    let mut text = serde_json::to_string_pretty(&public)?;
    text.push('\n');
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pub_path)
        .and_then(|mut f| f.write_all(text.as_bytes()))
        .map_err(|e| write_err(&pub_path, e))?;
    Ok(GeneratedKey {
        path: out.display().to_string(),
        public_path: pub_path.display().to_string(),
        public,
    })
}

/// Reads a signing key file: exactly 32 bytes, readable by its owner only
/// (on Unix, no group or other permission bits). Every refusal names the
/// file and what to do.
pub fn load_signing_key(path: &Path) -> Result<SigningKey> {
    let shown = path.display();
    let meta = std::fs::metadata(path).map_err(|e| {
        bad(
            "signing_key",
            format!(
                "cannot read the signing key {shown} ({e}); pass the file `bellwether keys \
                 generate --out <path>` wrote"
            ),
        )
    })?;
    if !meta.is_file() {
        return Err(bad(
            "signing_key",
            format!("the signing key {shown} is not a file; pass the seed file `bellwether keys generate` wrote"),
        ));
    }
    let bytes = Zeroizing::new(std::fs::read(path).map_err(|e| {
        bad(
            "signing_key",
            format!("cannot read the signing key {shown} ({e})"),
        )
    })?);
    // The shape first: a public key file passed by mistake is named as
    // one, not as a permission problem.
    if bytes.len() != 32 {
        let hint = if bytes.first() == Some(&b'{') {
            "it looks like a public key file (.pub); pass the seed file beside it"
        } else {
            "pass the 32-byte seed file `bellwether keys generate` wrote"
        };
        return Err(bad(
            "signing_key",
            format!(
                "the signing key {shown} is {} bytes, not a 32-byte ed25519 seed; {hint}",
                bytes.len()
            ),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(bad(
                "signing_key",
                format!(
                    "the signing key {shown} is readable by other users (mode {mode:04o}); \
                     restrict it to its owner with `chmod 600 {shown}` and run again"
                ),
            ));
        }
    }
    let mut seed = Zeroizing::new([0u8; 32]);
    seed.copy_from_slice(&bytes);
    Ok(SigningKey::from_seed(&seed))
}

/// Reads a public key file (`<key>.pub`), checking its key id against its
/// public key.
pub fn read_public_key(path: &Path) -> Result<PublicKeyFile> {
    let shown = path.display();
    let bytes = std::fs::read(path).map_err(|e| {
        bad(
            "public_key",
            format!("cannot read the public key file {shown} ({e}); pass the .pub file `bellwether keys generate` wrote"),
        )
    })?;
    // A seed passed by mistake is 32 binary bytes: never echo them.
    let p: PublicKeyFile = serde_json::from_slice(&bytes).map_err(|_| {
        bad(
            "public_key",
            format!(
                "{shown} is not a public key file (JSON with key_id, public_key, name, and \
                 created_at); pass the .pub file `bellwether keys generate` wrote, not the seed"
            ),
        )
    })?;
    if p.public_key.key_id() != p.key_id {
        return Err(bad(
            "public_key",
            format!(
                "{shown} says key_id {} but its public key's id is {}; the file was edited, \
                 regenerate it",
                p.key_id,
                p.public_key.key_id()
            ),
        ));
    }
    Ok(p)
}

fn window_bound(field: &'static str, flag: &str, t: &str) -> Result<String> {
    let normalized = rfc3339_millis(t).map_err(|_| {
        bad(
            field,
            format!("{flag} {t:?} is not an RFC 3339 time; write it as 2026-10-25T00:00:00Z"),
        )
    })?;
    debug_assert!(is_utc_millis(&normalized));
    Ok(normalized)
}

fn parse_trust_file(path: &Path) -> Result<(String, TrustFile)> {
    let shown = path.display();
    let text = std::fs::read_to_string(path).map_err(|e| {
        bad(
            "trusted_keys",
            format!(
                "cannot read the trust file {shown} ({e}); create it with `bellwether keys \
                 trust --file {shown} --add <key>.pub`"
            ),
        )
    })?;
    let file: TrustFile = toml::from_str(&text).map_err(|e| {
        bad(
            "trusted_keys",
            format!(
                "the trust file {shown} is not a list of [[key]] tables with key_id, \
                 public_key, name, and optionally valid_from and revoked_at: {}",
                e.to_string().trim()
            ),
        )
    })?;
    Ok((text, file))
}

fn trusted_from(path: &Path, file: &TrustFile) -> Result<TrustedKeys> {
    let shown = path.display();
    let mut keys = TrustedKeys::new();
    for (i, e) in file.key.iter().enumerate() {
        let at = format!("the trust file {shown}, key {} ({:?})", i + 1, e.name);
        if e.public_key.key_id() != e.key_id {
            return Err(bad(
                "trusted_keys",
                format!(
                    "{at}: key_id {} is not the id of its public key ({}); copy both from the \
                     key's .pub file",
                    e.key_id,
                    e.public_key.key_id()
                ),
            ));
        }
        let bound = |field, t: &Option<String>| -> Result<Option<String>> {
            t.as_deref()
                .map(|t| {
                    rfc3339_millis(t).map_err(|_| {
                        bad(
                            "trusted_keys",
                            format!("{at}: {field} {t:?} is not an RFC 3339 time"),
                        )
                    })
                })
                .transpose()
        };
        keys.insert(TrustedKey {
            name: e.name.clone(),
            public_key: e.public_key,
            valid_from: bound("valid_from", &e.valid_from)?,
            revoked_at: bound("revoked_at", &e.revoked_at)?,
        })
        .map_err(|err| bad("trusted_keys", format!("{at}: {err}")))?;
    }
    Ok(keys)
}

/// Reads a trust file. A missing or malformed file, a key id that is not
/// its public key's, a bad time, or a key listed twice is refused, naming
/// the file and the entry.
pub fn load_trusted_keys(path: &Path) -> Result<TrustedKeys> {
    let (_, file) = parse_trust_file(path)?;
    trusted_from(path, &file)
}

/// Appends the key in `public` (a `.pub` file) to the trust file `file`,
/// creating it if absent, with an optional validity window. The existing
/// file is checked first and left untouched on any refusal.
pub fn trust_key(
    file: &Path,
    public: &Path,
    valid_from: Option<&str>,
    revoked_at: Option<&str>,
) -> Result<TrustResult> {
    let p = read_public_key(public)?;
    let entry = TrustEntry {
        key_id: p.key_id,
        public_key: p.public_key,
        name: p.name,
        valid_from: valid_from
            .map(|t| window_bound("valid_from", "--valid-from", t))
            .transpose()?,
        revoked_at: revoked_at
            .map(|t| window_bound("revoked_at", "--revoked-at", t))
            .transpose()?,
    };
    let (text, existing) = if file.exists() {
        parse_trust_file(file)?
    } else {
        (String::new(), TrustFile::default())
    };
    let mut keys = trusted_from(file, &existing)?;
    if keys.get(&entry.key_id).is_some() {
        return Err(bad(
            "trusted_keys",
            format!(
                "{} already trusts key {} ({:?}); edit its [[key]] table to change its window",
                file.display(),
                entry.key_id,
                existing
                    .key
                    .iter()
                    .find(|k| k.key_id == entry.key_id)
                    .map_or("", |k| k.name.as_str())
            ),
        ));
    }
    keys.insert(TrustedKey {
        name: entry.name.clone(),
        public_key: entry.public_key,
        valid_from: entry.valid_from.clone(),
        revoked_at: entry.revoked_at.clone(),
    })
    .map_err(|e| bad("trusted_keys", e.to_string()))?;
    let block = toml::to_string(&TrustFile {
        key: vec![entry.clone()],
    })
    .map_err(|e| Error::Other(format!("cannot write the trust entry ({e})")))?;
    let mut appended = String::new();
    if !text.is_empty() {
        if !text.ends_with('\n') {
            appended.push('\n');
        }
        appended.push('\n');
    }
    appended.push_str(&block);
    std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(file)
        .and_then(|mut f| f.write_all(appended.as_bytes()))
        .map_err(|e| {
            bad(
                "trusted_keys",
                format!("cannot write the trust file {} ({e})", file.display()),
            )
        })?;
    Ok(TrustResult {
        file: file.display().to_string(),
        key: entry,
        keys: keys.len() as u64,
    })
}
