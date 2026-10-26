//! Detached signatures over receipts and certificates (ADR 029).
//!
//! A signature is not a record kind and not a field of a record: it is an
//! object beside the record it signs, at
//! `ledger/<record hash>.sig.<key id>.json`, holding canonical JSON:
//!
//! ```json
//! {"signature_version":1,"record":"<64 hex>","kind":"receipt","alg":"ed25519",
//!  "key_id":"<64 hex>","public_key":"<64 hex>","signed_at":"2026-10-25T00:00:00.000Z",
//!  "sig":"<128 hex>"}
//! ```
//!
//! The key id is BLAKE3 of the 32 public-key bytes. The signed message is
//! [`signature_message`]: the domain tag `bellwether-signature-v1\0`, the
//! record kind, a zero byte, the 32 bytes of the record hash, and the
//! `signed_at` bytes. A signature therefore cannot be moved to another
//! record, to another kind, or to another time; the record's bytes are bound
//! by its hash. Several keys may sign one record, one object each.
//!
//! Record readers never see these objects: [`crate::parse_record_path`]
//! accepts only `<hex>.rec` names, so `list`, `explain`, and every view skip
//! them.

use std::collections::BTreeMap;
use std::fmt;

use bellwether_types::{Error, Hash, Result};
use ed25519_dalek::Signer as _;
use serde::{Deserialize, Serialize};

use crate::{rfc3339, RecordKind, LEDGER_PREFIX};

/// The signature object version this build writes and reads. A newer one
/// is reported as [`InvalidReason::Newer`], never parsed.
pub const SIGNATURE_VERSION: u32 = 1;
/// The only signature algorithm.
pub const SIGNATURE_ALG: &str = "ed25519";
/// The domain tag every signed message starts with, so a signature made for
/// any other protocol never verifies here.
pub const SIGNATURE_DOMAIN: &[u8] = b"bellwether-signature-v1\0";

const SIG_INFIX: &str = ".sig.";
const SIG_EXT: &str = ".json";

/// The message a signature over `record` of `kind` at `signed_at` signs:
/// `b"bellwether-signature-v1\0" ‖ kind ‖ 0x00 ‖ record hash (32 bytes) ‖
/// signed_at`.
#[must_use]
pub fn signature_message(kind: RecordKind, record: &Hash, signed_at: &str) -> Vec<u8> {
    let kind = kind.as_str().as_bytes();
    let mut m = Vec::with_capacity(SIGNATURE_DOMAIN.len() + kind.len() + 1 + 32 + signed_at.len());
    m.extend_from_slice(SIGNATURE_DOMAIN);
    m.extend_from_slice(kind);
    m.push(0);
    m.extend_from_slice(&record.0);
    m.extend_from_slice(signed_at.as_bytes());
    m
}

/// Whether `s` is RFC 3339 in the one spelling signatures and trust windows
/// use: UTC, milliseconds, `Z` (`2026-10-25T00:00:00.000Z`). Fixed width, so
/// two such strings order as the instants they name.
#[must_use]
pub fn is_utc_millis(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 24
        && b[10] == b'T'
        && b[19] == b'.'
        && b[20..23].iter().all(u8::is_ascii_digit)
        && b[23] == b'Z'
        && rfc3339::is_rfc3339(s)
}

fn lower_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        })
}

/// Parses exactly `N` bytes of lowercase hex.
fn parse_lower_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    let b = s.as_bytes();
    if b.len() != N * 2 || !b.iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut out = [0u8; N];
    for (i, pair) in b.chunks(2).enumerate() {
        let v = |c: u8| {
            if c.is_ascii_digit() {
                c - b'0'
            } else {
                c - b'a' + 10
            }
        };
        out[i] = (v(pair[0]) << 4) | v(pair[1]);
    }
    Some(out)
}

/// Text read from the store, made safe to show on a terminal or hand to an
/// agent: every control character (newline and ESC included) is escaped as
/// `\n`, `\u{1b}`, and so on, so a planted object cannot forge output lines
/// or terminal sequences.
#[must_use]
pub fn printable(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_control() {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/// An ed25519 public key: 32 bytes, written as 64 lowercase hex characters.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PublicKey([u8; 32]);

impl PublicKey {
    /// A public key from its 32 bytes, refused unless they are a valid
    /// ed25519 point.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self> {
        ed25519_dalek::VerifyingKey::from_bytes(&bytes)
            .map(|_| Self(bytes))
            .map_err(|_| Error::Other("the 32 bytes are not an ed25519 public key".to_string()))
    }

    /// A public key from 64 lowercase hex characters.
    pub fn from_hex(s: &str) -> Result<Self> {
        let bytes = parse_lower_hex::<32>(s).ok_or_else(|| {
            Error::Other(format!(
                "public key {s:?} is not 64 lowercase hex characters"
            ))
        })?;
        Self::from_bytes(bytes)
    }

    /// The 32 bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// 64 lowercase hex characters.
    #[must_use]
    pub fn to_hex(&self) -> String {
        lower_hex(&self.0)
    }

    /// The key id: BLAKE3 of the 32 public-key bytes.
    #[must_use]
    pub fn key_id(&self) -> Hash {
        Hash::of(&self.0)
    }

    fn verifying(&self) -> Option<ed25519_dalek::VerifyingKey> {
        ed25519_dalek::VerifyingKey::from_bytes(&self.0).ok()
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey({})", self.to_hex())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for PublicKey {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// An ed25519 signing key, held only in memory. It is built from a 32-byte
/// seed and never gives the seed back: nothing in this crate returns secret
/// key material, and [`crate::Ledger::sign`] writes only the public half.
///
/// ```compile_fail
/// fn leak(key: &bellwether_ledger::SigningKey) -> [u8; 32] {
///     key.to_bytes()
/// }
/// ```
///
/// ```compile_fail
/// fn leak(key: &bellwether_ledger::SigningKey) -> &ed25519_dalek::SigningKey {
///     &key.inner
/// }
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct SigningKey {
    inner: ed25519_dalek::SigningKey,
}

impl SigningKey {
    /// The key whose 32-byte seed is `seed` (RFC 8032 secret key).
    #[must_use]
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        Self {
            inner: ed25519_dalek::SigningKey::from_bytes(seed),
        }
    }

    /// The public half.
    #[must_use]
    pub fn public_key(&self) -> PublicKey {
        PublicKey(self.inner.verifying_key().to_bytes())
    }

    /// The key id: BLAKE3 of the public key's 32 bytes.
    #[must_use]
    pub fn key_id(&self) -> Hash {
        self.public_key().key_id()
    }

    fn sign_message(&self, message: &[u8]) -> [u8; 64] {
        self.inner.sign(message).to_bytes()
    }
}

/// Shows the key id only, never secret material.
impl fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigningKey")
            .field("key_id", &self.key_id().to_hex())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// The object and its path
// ---------------------------------------------------------------------------

/// Where a signature object lives: `ledger/<record>.sig.<key id>.json`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignaturePath {
    /// The record signed.
    pub record: Hash,
    /// The signing key's id.
    pub key_id: Hash,
}

impl SignaturePath {
    /// The store path.
    #[must_use]
    pub fn path(&self) -> String {
        format!(
            "{LEDGER_PREFIX}{}{SIG_INFIX}{}{SIG_EXT}",
            self.record.to_hex(),
            self.key_id.to_hex()
        )
    }

    /// Parses a store path `[...]ledger/<hex>.sig.<hex>.json`, lowercase hex
    /// only (the only name the ledger writes). `None` for anything else.
    #[must_use]
    pub fn parse(path: &str) -> Option<Self> {
        let name = path.rsplit('/').next()?;
        let stem = name.strip_suffix(SIG_EXT)?;
        let (record, key_id) = stem.split_once(SIG_INFIX)?;
        Some(Self {
            record: Hash(parse_lower_hex::<32>(record)?),
            key_id: Hash(parse_lower_hex::<32>(key_id)?),
        })
    }
}

impl fmt::Display for SignaturePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.path())
    }
}

/// The stored object, field order as written (canonical JSON is
/// `serde_json::to_vec` of this struct).
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignatureObject {
    signature_version: u32,
    record: Hash,
    kind: String,
    alg: String,
    key_id: Hash,
    public_key: String,
    signed_at: String,
    sig: String,
}

/// The canonical bytes of the signature by `key` over `record` of `kind`
/// at `signed_at`.
pub(crate) fn signature_bytes(
    record: &Hash,
    kind: RecordKind,
    key: &SigningKey,
    signed_at: &str,
) -> Result<Vec<u8>> {
    let public = key.public_key();
    let sig = key.sign_message(&signature_message(kind, record, signed_at));
    let object = SignatureObject {
        signature_version: SIGNATURE_VERSION,
        record: *record,
        kind: kind.as_str().to_string(),
        alg: SIGNATURE_ALG.to_string(),
        key_id: public.key_id(),
        public_key: public.to_hex(),
        signed_at: signed_at.to_string(),
        sig: lower_hex(&sig),
    };
    Ok(serde_json::to_vec(&object)?)
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

/// Why a signature object is invalid.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum InvalidReason {
    /// Not a signature object this build can read: not JSON, a missing or
    /// mistyped field, an unknown field, or not in canonical form.
    Malformed {
        /// What is wrong.
        detail: String,
    },
    /// A newer `signature_version` than this build reads.
    Newer {
        /// The version found.
        found: u64,
        /// The newest version this build reads.
        supported: u32,
    },
    /// The object names another record than the one it is filed beside: it
    /// was copied or moved.
    RecordMismatch {
        /// The record the object says it signs.
        signed: Hash,
    },
    /// The object names another kind than the stored record's.
    KindMismatch {
        /// The kind the object names.
        signed: String,
        /// The stored record's kind.
        stored: RecordKind,
    },
    /// The key id in the object disagrees with its file name or with the
    /// BLAKE3 of its public key.
    KeyIdMismatch,
    /// An algorithm other than ed25519.
    UnknownAlgorithm {
        /// The algorithm named.
        alg: String,
    },
    /// Filed beside a record of a kind that is never signed (only receipts
    /// and certificates are).
    Unsignable {
        /// The stored record's kind.
        stored: RecordKind,
    },
    /// The signature does not verify over the message for this record,
    /// kind, and `signed_at` under the object's public key.
    BadSignature,
}

impl fmt::Display for InvalidReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed { detail } => write!(f, "malformed signature object: {detail}"),
            Self::Newer { found, supported } => write!(
                f,
                "newer signature version {found} (this build reads {supported}); upgrade \
                 bellwether to verify it"
            ),
            Self::RecordMismatch { signed } => write!(
                f,
                "record mismatch: the object signs {signed}, not the record it is filed beside"
            ),
            Self::KindMismatch { signed, stored } => write!(
                f,
                "kind mismatch: the object signs {signed:?}, and the record is {}",
                stored.with_article()
            ),
            Self::KeyIdMismatch => f.write_str(
                "key id mismatch: the object's key id is not its file name's, or not the BLAKE3 \
                 of its public key",
            ),
            Self::UnknownAlgorithm { alg } => {
                write!(f, "unknown algorithm {alg:?} (only ed25519 is read)")
            }
            Self::Unsignable { stored } => write!(
                f,
                "filed beside {}, and only receipts and certificates are signed",
                stored.with_article()
            ),
            Self::BadSignature => f.write_str(
                "bad signature: it does not verify over this record, kind, and signed_at",
            ),
        }
    }
}

/// One signature object beside a record, verified on its own (no trust
/// set): whether the signature is mathematically valid for this record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SignatureStatus {
    /// Verifies under the public key it carries. Whether that key is
    /// trusted is [`Verdict`]'s question.
    Valid {
        /// BLAKE3 of the public key.
        key_id: Hash,
        /// The public key.
        public_key: PublicKey,
        /// When it was signed, RFC 3339 UTC with milliseconds.
        signed_at: String,
    },
    /// Does not verify, or cannot be read.
    Invalid {
        /// The key id the object is filed under.
        key_id: Hash,
        /// Why.
        reason: InvalidReason,
    },
}

impl SignatureStatus {
    /// The key id.
    #[must_use]
    pub fn key_id(&self) -> &Hash {
        match self {
            Self::Valid { key_id, .. } | Self::Invalid { key_id, .. } => key_id,
        }
    }
}

/// Verifies one signature object's bytes, filed at `at`, beside a stored
/// record of kind `stored`. Never panics, whatever the bytes.
pub(crate) fn check_object(
    bytes: &[u8],
    at: &SignaturePath,
    stored: RecordKind,
) -> SignatureStatus {
    match check_inner(bytes, at, stored) {
        Ok((public_key, signed_at)) => SignatureStatus::Valid {
            key_id: at.key_id,
            public_key,
            signed_at,
        },
        Err(reason) => SignatureStatus::Invalid {
            key_id: at.key_id,
            reason,
        },
    }
}

fn malformed(detail: impl Into<String>) -> InvalidReason {
    InvalidReason::Malformed {
        detail: detail.into(),
    }
}

fn check_inner(
    bytes: &[u8],
    at: &SignaturePath,
    stored: RecordKind,
) -> std::result::Result<(PublicKey, String), InvalidReason> {
    if !matches!(stored, RecordKind::Receipt | RecordKind::Certificate) {
        return Err(InvalidReason::Unsignable { stored });
    }
    // The version first, from an untyped parse, so a newer object is never
    // mistaken for a malformed one.
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| malformed("not JSON"))?;
    let version = value
        .get("signature_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| malformed("no signature_version"))?;
    if version > u64::from(SIGNATURE_VERSION) {
        return Err(InvalidReason::Newer {
            found: version,
            supported: SIGNATURE_VERSION,
        });
    }
    if version == 0 {
        return Err(malformed("signature_version 0"));
    }
    let object: SignatureObject =
        // A fixed phrase, never serde's message: that would echo field
        // names and values from the store to a terminal.
        serde_json::from_value(value).map_err(|_| {
            malformed("a field is missing, unknown, or of the wrong type")
        })?;
    let canonical =
        serde_json::to_vec(&object).map_err(|_| malformed("cannot be re-serialized"))?;
    if canonical != bytes {
        return Err(malformed("not in canonical form"));
    }
    if object.alg != SIGNATURE_ALG {
        return Err(InvalidReason::UnknownAlgorithm {
            alg: printable(&object.alg),
        });
    }
    if object.record != at.record {
        return Err(InvalidReason::RecordMismatch {
            signed: object.record,
        });
    }
    if RecordKind::from_tag(&object.kind) != Some(stored) {
        return Err(InvalidReason::KindMismatch {
            signed: printable(&object.kind),
            stored,
        });
    }
    let public_key = PublicKey::from_hex(&object.public_key)
        .map_err(|_| malformed("public_key is not an ed25519 public key in lowercase hex"))?;
    if object.key_id != at.key_id || public_key.key_id() != at.key_id {
        return Err(InvalidReason::KeyIdMismatch);
    }
    if !is_utc_millis(&object.signed_at) {
        return Err(malformed("signed_at is not RFC 3339 UTC with milliseconds"));
    }
    let sig = parse_lower_hex::<64>(&object.sig)
        .ok_or_else(|| malformed("sig is not 128 lowercase hex characters"))?;
    let verifying = public_key
        .verifying()
        .ok_or_else(|| malformed("public_key is not an ed25519 public key"))?;
    let message = signature_message(stored, &at.record, &object.signed_at);
    verifying
        .verify_strict(&message, &ed25519_dalek::Signature::from_bytes(&sig))
        .map_err(|_| InvalidReason::BadSignature)?;
    Ok((public_key, object.signed_at))
}

// ---------------------------------------------------------------------------
// Trust
// ---------------------------------------------------------------------------

/// A key the reader trusts, with the window in which its signatures count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedKey {
    /// A label for people (`ci`, `release`).
    pub name: String,
    /// The public key.
    pub public_key: PublicKey,
    /// Signatures made before this instant do not count (RFC 3339 UTC with
    /// milliseconds). `None`: no lower bound.
    pub valid_from: Option<String>,
    /// Signatures made at or after this instant do not count. `None`: not
    /// revoked.
    pub revoked_at: Option<String>,
}

impl TrustedKey {
    /// The key id.
    #[must_use]
    pub fn key_id(&self) -> Hash {
        self.public_key.key_id()
    }

    /// Whether `signed_at` lies in `[valid_from, revoked_at)`. Both sides
    /// are in the fixed-width UTC millisecond spelling, so string order is
    /// time order.
    #[must_use]
    pub fn admits(&self, signed_at: &str) -> bool {
        self.valid_from
            .as_deref()
            .is_none_or(|from| from <= signed_at)
            && self.revoked_at.as_deref().is_none_or(|rev| signed_at < rev)
    }
}

/// Why a key cannot join a [`TrustedKeys`] set. Its own type: a trust set
/// is the reader's, not a ledger rule. Callers add the next step in their
/// own terms (a flag, a `[[key]]` table).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrustError {
    /// A window bound is not RFC 3339 UTC with milliseconds.
    NotUtcMillis {
        /// The key's name.
        name: String,
        /// `valid_from` or `revoked_at`.
        field: &'static str,
        /// The value given.
        value: String,
    },
    /// `valid_from` is not before `revoked_at`: no signature could count.
    EmptyWindow {
        /// The key's name.
        name: String,
        /// The window's start.
        valid_from: String,
        /// The window's end.
        revoked_at: String,
    },
    /// The key is already in the set.
    Duplicate {
        /// Its id.
        key_id: Hash,
        /// The name it is trusted under already.
        first: String,
        /// The name given now.
        second: String,
    },
}

impl fmt::Display for TrustError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotUtcMillis { name, field, value } => write!(
                f,
                "trusted key {name:?}: {field} {value:?} is not RFC 3339 UTC with milliseconds"
            ),
            Self::EmptyWindow {
                name,
                valid_from,
                revoked_at,
            } => write!(
                f,
                "trusted key {name:?}: valid_from {valid_from} is not before revoked_at \
                 {revoked_at}, so no signature could count"
            ),
            Self::Duplicate {
                key_id,
                first,
                second,
            } => write!(
                f,
                "key {key_id} is trusted twice (as {first:?} and {second:?})"
            ),
        }
    }
}

impl std::error::Error for TrustError {}

/// The keys a reader trusts. Never stored in the ledger: an attacker with
/// write access to the store would then also choose who is trusted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrustedKeys {
    keys: BTreeMap<Hash, TrustedKey>,
}

impl TrustedKeys {
    /// No trusted key: every valid signature is [`Verdict::Untrusted`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a key. Refused ([`TrustError`]): a key already present, a
    /// window bound that is not RFC 3339 UTC with milliseconds, or
    /// `valid_from` not before `revoked_at`.
    pub fn insert(&mut self, key: TrustedKey) -> std::result::Result<(), TrustError> {
        for (field, t) in [
            ("valid_from", &key.valid_from),
            ("revoked_at", &key.revoked_at),
        ] {
            if let Some(t) = t {
                if !is_utc_millis(t) {
                    return Err(TrustError::NotUtcMillis {
                        name: key.name.clone(),
                        field,
                        value: t.clone(),
                    });
                }
            }
        }
        if let (Some(from), Some(rev)) = (&key.valid_from, &key.revoked_at) {
            if from >= rev {
                return Err(TrustError::EmptyWindow {
                    name: key.name.clone(),
                    valid_from: from.clone(),
                    revoked_at: rev.clone(),
                });
            }
        }
        let id = key.key_id();
        if let Some(old) = self.keys.get(&id) {
            return Err(TrustError::Duplicate {
                key_id: id,
                first: old.name.clone(),
                second: key.name.clone(),
            });
        }
        self.keys.insert(id, key);
        Ok(())
    }

    /// The trusted key with this id.
    #[must_use]
    pub fn get(&self, key_id: &Hash) -> Option<&TrustedKey> {
        self.keys.get(key_id)
    }

    /// Every trusted key, by key id.
    pub fn iter(&self) -> impl Iterator<Item = &TrustedKey> {
        self.keys.values()
    }

    /// How many keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// A valid signature by a trusted key in its window.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedBy {
    /// The trusted key's name.
    pub by: String,
    /// Its id.
    pub key_id: Hash,
    /// When it signed.
    pub signed_at: String,
}

/// A valid signature by a trusted key, made outside the key's window.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutsideWindow {
    /// The trusted key's name.
    pub by: String,
    /// Its id.
    pub key_id: Hash,
    /// When it signed.
    pub signed_at: String,
    /// The window's start, if any.
    pub valid_from: Option<String>,
    /// The window's end (revocation), if any.
    pub revoked_at: Option<String>,
}

/// A signature object that does not verify.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidSignature {
    /// The key id it is filed under.
    pub key_id: Hash,
    /// Why.
    pub reason: InvalidReason,
}

/// A record's signatures folded against a trust set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    /// A trusted key signed it within the key's window, and every signature
    /// object beside it verifies.
    Signed {
        /// The trusted key's name.
        by: String,
        /// Its id.
        key_id: Hash,
        /// When it signed.
        signed_at: String,
    },
    /// Every signature verifies, but none is by a trusted key within its
    /// window.
    Untrusted {
        /// The keys whose valid signatures did not count.
        key_ids: Vec<Hash>,
        /// Those of them that are trusted but signed outside their window.
        outside_window: Vec<OutsideWindow>,
    },
    /// No signature object beside the record.
    Unsigned,
    /// At least one signature object does not verify. Reported even when a
    /// trusted key also signed (`signed`), never hidden by it.
    Invalid {
        /// The objects that do not verify.
        invalid: Vec<InvalidSignature>,
        /// A valid signature by a trusted key in its window, if any.
        signed: Option<SignedBy>,
    },
}

impl Verdict {
    /// Folds verified signatures against `trusted`.
    #[must_use]
    pub fn of(statuses: &[SignatureStatus], trusted: &TrustedKeys) -> Self {
        let mut invalid = Vec::new();
        let mut signed: Option<SignedBy> = None;
        let mut key_ids = Vec::new();
        let mut outside_window = Vec::new();
        for s in statuses {
            match s {
                SignatureStatus::Invalid { key_id, reason } => invalid.push(InvalidSignature {
                    key_id: *key_id,
                    reason: reason.clone(),
                }),
                SignatureStatus::Valid {
                    key_id,
                    public_key,
                    signed_at,
                } => match trusted.get(key_id).filter(|t| t.public_key == *public_key) {
                    Some(t) if t.admits(signed_at) => {
                        if signed.is_none() {
                            signed = Some(SignedBy {
                                by: t.name.clone(),
                                key_id: *key_id,
                                signed_at: signed_at.clone(),
                            });
                        }
                    }
                    Some(t) => {
                        key_ids.push(*key_id);
                        outside_window.push(OutsideWindow {
                            by: t.name.clone(),
                            key_id: *key_id,
                            signed_at: signed_at.clone(),
                            valid_from: t.valid_from.clone(),
                            revoked_at: t.revoked_at.clone(),
                        });
                    }
                    None => key_ids.push(*key_id),
                },
            }
        }
        if !invalid.is_empty() {
            return Self::Invalid { invalid, signed };
        }
        if let Some(SignedBy {
            by,
            key_id,
            signed_at,
        }) = signed
        {
            return Self::Signed {
                by,
                key_id,
                signed_at,
            };
        }
        if key_ids.is_empty() {
            Self::Unsigned
        } else {
            Self::Untrusted {
                key_ids,
                outside_window,
            }
        }
    }

    /// Whether this is [`Verdict::Signed`].
    #[must_use]
    pub fn is_signed(&self) -> bool {
        matches!(self, Self::Signed { .. })
    }

    /// One word: `signed`, `untrusted`, `unsigned`, or `invalid`.
    #[must_use]
    pub fn word(&self) -> &'static str {
        match self {
            Self::Signed { .. } => "signed",
            Self::Untrusted { .. } => "untrusted",
            Self::Unsigned => "unsigned",
            Self::Invalid { .. } => "invalid",
        }
    }

    /// The `sig=` token of a chain line: `valid(<name>)`,
    /// `untrusted(<id prefix>,...)`, `none`, or `INVALID`.
    #[must_use]
    pub fn token(&self) -> String {
        match self {
            Self::Signed { by, .. } => format!("valid({by})"),
            Self::Untrusted { key_ids, .. } => {
                let ids: Vec<String> = key_ids.iter().map(short_id).collect();
                format!("untrusted({})", ids.join(","))
            }
            Self::Unsigned => "none".to_string(),
            Self::Invalid { .. } => "INVALID".to_string(),
        }
    }
}

/// The first 12 hex characters of a key id, as chain lines show hashes.
fn short_id(h: &Hash) -> String {
    h.to_hex()[..12].to_string()
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Signed {
                by,
                key_id,
                signed_at,
            } => write!(
                f,
                "signed by {by} (key {}) at {signed_at}",
                &key_id.to_hex()[..12]
            ),
            Self::Untrusted {
                key_ids,
                outside_window,
            } => {
                let ids: Vec<String> = key_ids
                    .iter()
                    .map(|k| k.to_hex()[..12].to_string())
                    .collect();
                write!(
                    f,
                    "untrusted: signed only by keys the trust file does not admit ({})",
                    ids.join(", ")
                )?;
                for o in outside_window {
                    write!(
                        f,
                        "; {} signed at {}, outside its window [{}, {})",
                        o.by,
                        o.signed_at,
                        o.valid_from.as_deref().unwrap_or("-"),
                        o.revoked_at.as_deref().unwrap_or("-")
                    )?;
                }
                Ok(())
            }
            Self::Unsigned => f.write_str("unsigned: no signature beside it"),
            Self::Invalid { invalid, signed } => {
                f.write_str("INVALID: ")?;
                let parts: Vec<String> = invalid
                    .iter()
                    .map(|i| format!("key {}: {}", &i.key_id.to_hex()[..12], i.reason))
                    .collect();
                f.write_str(&parts.join("; "))?;
                if let Some(s) = signed {
                    write!(
                        f,
                        " (also signed by trusted key {} at {})",
                        s.by, s.signed_at
                    )?;
                }
                Ok(())
            }
        }
    }
}
