//! Content-addressed, append-only ledger of Bellwether records (ADR 006).
//!
//! Every [`Record`] lives at `ledger/<blake3>.rec` as its canonical JSON, where
//! the name is [`Record::hash`]. The ledger is where the trust boundary
//! (ADR 009) becomes enforceable: it refuses, at write time, any record that
//! would let something cross from *propose* to *apply* without a passing,
//! resolving chain of evidence. See the crate `README.md` for the full list of
//! write-time rules and the assumptions they rest on.
//!
//! # Invariants owned here
//!
//! - **Invariant 2**: a [`Receipt`] implies a passing [`Certificate`]. A
//!   receipt can only be written through [`Ledger::put_receipt`], which
//!   resolves its certificate, proposal, and replay, checks the certificate
//!   verified exactly this proposal and was computed (like the replay)
//!   against `manifest_before`, and checks that `manifest_before <
//!   manifest_after <= current`, comparing manifest sequences numerically
//!   (ADR 020; only a rewrite may leave the manifest unchanged).
//! - **Invariant 6**: every [`Evidence`](bellwether_types::Evidence) and [`Certificate`] carries
//!   provenance: non-empty inputs and an RFC 3339 `created_at`; a
//!   certificate also names its manifest version.
//! - **Invariant 3 support** (contract v2.1): every [`Replay`] names the
//!   data it read (`inputs`, non-empty); its current costs are exact; it
//!   names a cost model exactly when some proposed cost is inexact; and a
//!   key proposal's replay is exact throughout. `put_receipt` re-runs these
//!   checks on the resolved replay.
//! - **Write version**: `put` and `put_receipt` accept only the current
//!   `record_version`; older versions stay readable (ADR 017).
//!
//! # Write paths
//!
//! The only ways to write are [`Ledger::put`], [`Ledger::put_receipt`], and
//! [`Ledger::sign`], which writes a detached signature object beside a
//! stored receipt or certificate (ADR 029). The wrapped store is not
//! reachable through the ledger:
//!
//! ```compile_fail
//! fn bypass<S, T>(ledger: &bellwether_ledger::Ledger<S>)
//! where
//!     S: std::ops::Deref<Target = T>,
//!     T: bellwether_types::Store,
//! {
//!     let _ = ledger.store();
//! }
//! ```
//!
//! # Signatures
//!
//! A signature is an object beside the record, never a record kind or a
//! field ([`signature`] module docs). [`Ledger::signatures`] verifies every
//! object beside a record on its own; [`Ledger::verify_against`] folds them
//! against a reader's [`TrustedKeys`] into a [`Verdict`]. A [`SigningKey`]
//! goes in by reference and never comes back out: no call returns secret
//! key material, and nothing secret is written.
//!
//! # Append-only, no garbage collection
//!
//! There is deliberately no delete API. Ledger records are the audit trail;
//! removing one would break the hash links that make receipts re-derivable.
//! Garbage collection of ledger records is intentionally absent; the
//! retention-window GC in ADR 013 applies to segments and cache artifacts,
//! never to `ledger/`.
//!
//! # Reading
//!
//! [`Ledger::get`] verifies that the stored bytes hash to the requested name
//! (tamper evidence) and refuses a record whose `record_version` is newer than
//! [`format::RECORD_VERSION`] with [`Error::UnsupportedVersion`] (ADR 017)
//! before attempting a typed parse, whatever its kind. [`Ledger::get_raw`]
//! returns the verified bytes untouched, so fields this build does not know
//! about are never lost.

#![forbid(unsafe_code)]

mod explain;
pub mod rfc3339;
pub mod signature;

use std::fmt;
use std::ops::Deref;

use bellwether_types::{
    format, CertBody, Certificate, Error, Hash, Proposal, ProposalBody, Provenance, Receipt,
    Record, Replay, Result, RunCost, Store,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub use explain::{Chain, Link, LinkStatus};
pub use signature::{
    is_utc_millis, printable, signature_message, InvalidReason, InvalidSignature, OutsideWindow,
    PublicKey, SignaturePath, SignatureStatus, SignedBy, SigningKey, TrustError, TrustedKey,
    TrustedKeys, Verdict, SIGNATURE_ALG, SIGNATURE_DOMAIN, SIGNATURE_VERSION,
};

/// Directory under the index root that holds ledger records.
pub const LEDGER_PREFIX: &str = "ledger/";
/// File extension of a ledger record.
pub const RECORD_EXT: &str = ".rec";

/// The kind of a ledger record, matching the `record` tag in its JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    /// [`bellwether_types::Evidence`].
    Evidence,
    /// [`Proposal`].
    Proposal,
    /// [`Replay`].
    Replay,
    /// [`Certificate`].
    Certificate,
    /// [`Receipt`].
    Receipt,
}

impl RecordKind {
    /// The kind of a typed record.
    #[must_use]
    pub fn of(record: &Record) -> Self {
        match record {
            Record::Evidence(_) => Self::Evidence,
            Record::Proposal(_) => Self::Proposal,
            Record::Replay(_) => Self::Replay,
            Record::Certificate(_) => Self::Certificate,
            Record::Receipt(_) => Self::Receipt,
        }
    }

    /// The tag as it appears in JSON, for example `"certificate"`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Evidence => "evidence",
            Self::Proposal => "proposal",
            Self::Replay => "replay",
            Self::Certificate => "certificate",
            Self::Receipt => "receipt",
        }
    }

    /// The tag with an indefinite article, for messages: `"an evidence"`.
    #[must_use]
    pub fn with_article(self) -> &'static str {
        match self {
            Self::Evidence => "an evidence",
            Self::Proposal => "a proposal",
            Self::Replay => "a replay",
            Self::Certificate => "a certificate",
            Self::Receipt => "a receipt",
        }
    }

    /// The kind named by a JSON tag, if this build knows it.
    #[must_use]
    pub fn from_tag(tag: &str) -> Option<Self> {
        [
            Self::Evidence,
            Self::Proposal,
            Self::Replay,
            Self::Certificate,
            Self::Receipt,
        ]
        .into_iter()
        .find(|k| k.as_str() == tag)
    }
}

impl fmt::Display for RecordKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The store path of the record named `hash`: `ledger/<lowercase hex>.rec`.
#[must_use]
pub fn record_path(hash: &Hash) -> String {
    format!("{LEDGER_PREFIX}{}{RECORD_EXT}", hash.to_hex())
}

/// Parses a store path of the form `[...]ledger/<hex>.rec` back to its hash.
/// The inverse of [`record_path`]: only lowercase hex is accepted, because
/// that is the only name the ledger ever writes or reads. Returns `None` for
/// anything else.
#[must_use]
pub fn parse_record_path(path: &str) -> Option<Hash> {
    let name = path.rsplit('/').next()?;
    let hex = name.strip_suffix(RECORD_EXT)?;
    if !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    Hash::from_hex(hex).ok()
}

/// What a record's header says, read before any typed parse.
enum Peek {
    /// A kind and version this build reads.
    Readable(RecordKind),
    /// A newer record version than this build supports (ADR 017).
    Newer(u32),
    /// A supported version but a kind this build does not know.
    UnknownKind(String),
}

impl Peek {
    fn into_kind(self) -> Result<RecordKind> {
        match self {
            Self::Readable(k) => Ok(k),
            Self::Newer(found) => Err(unsupported(found)),
            Self::UnknownKind(tag) => Err(Error::UnknownKind {
                what: "record kind",
                found: tag,
            }),
        }
    }
}

/// The header fields, with the kind as a plain string so an unknown kind
/// never masks a version check.
#[derive(Deserialize)]
struct RawHeader {
    record: String,
    record_version: u32,
}

fn peek(bytes: &[u8]) -> Result<Peek> {
    let h: RawHeader = serde_json::from_slice(bytes)?;
    if h.record_version > format::RECORD_VERSION {
        return Ok(Peek::Newer(h.record_version));
    }
    Ok(match RecordKind::from_tag(&h.record) {
        Some(k) => Peek::Readable(k),
        None => Peek::UnknownKind(h.record),
    })
}

/// Parses verified bytes into a [`Record`]: header first, then a check that
/// `oldest_input_age` is integer nanoseconds (contract v2), so a bad age is a
/// rule violation naming the field rather than a bare serde error, then the
/// typed parse.
fn decode(bytes: &[u8]) -> Result<Record> {
    peek(bytes)?.into_kind()?;
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if let Some(age) = value.pointer("/provenance/oldest_input_age") {
        if age.as_u64().is_none() {
            return Err(rule(format!(
                "provenance oldest_input_age {age} is not a whole number of \
                 nanoseconds that fits in a u64"
            )));
        }
    }
    Ok(serde_json::from_value(value)?)
}

fn all_exact(costs: &[RunCost]) -> bool {
    costs.iter().all(|c| c.exact)
}

/// The one value `Replay.unserved` may take: runs no spec serves were
/// priced as the files' own min/max statistics read them.
const UNSERVED_PARQUET_STATS: &str = "parquet_stats";

/// Contract v2.1 replay rules, independent of where the replay is checked:
/// inputs are non-empty; current costs are exact; a cost model is named
/// exactly when some proposed cost is inexact, and it is a non-empty name
/// with no surrounding whitespace; a key proposal's replay is exact
/// throughout.
fn check_replay_content(r: &Replay, proposal: &Proposal) -> Result<()> {
    if r.inputs.is_empty() {
        return Err(rule(
            "replay names no inputs; a replay must cite the identity of the data it read \
             (at least the catalog digest), even before any index exists",
        ));
    }
    if let Some(m) = &r.model {
        if m.trim().is_empty() || m.trim() != m {
            return Err(rule(
                "replay model must be a non-empty name with no surrounding whitespace, or \
                 absent",
            ));
        }
    }
    if r.unserved
        .as_deref()
        .is_some_and(|u| u != UNSERVED_PARQUET_STATS)
    {
        return Err(rule(format!(
            "replay unserved must be {UNSERVED_PARQUET_STATS:?} or absent (absent is a raw scan)"
        )));
    }
    if !all_exact(&r.current) {
        return Err(rule(
            "replay must be exact for the current configuration in every run (ADR 005)",
        ));
    }
    let proposed_exact = all_exact(&r.proposed);
    if matches!(proposal.body, ProposalBody::Key { .. }) && !proposed_exact {
        return Err(rule(
            "replay of a key proposal must be exact in every run (ADR 005)",
        ));
    }
    match (proposed_exact, r.model.is_some()) {
        (true, true) => Err(rule(
            "replay must not name a cost model when every cost is exact",
        )),
        (false, false) => Err(rule(
            "replay with an inexact proposed cost must name the cost model behind it",
        )),
        _ => Ok(()),
    }
}

/// A record written now carries the current record version; older versions
/// stay readable but are never produced, so a reader of an older build cannot
/// misread a record that carries fields its version does not have (ADR 017).
fn check_write_version(found: u32) -> Result<()> {
    check_version(found)?;
    if found != format::RECORD_VERSION {
        return Err(rule(format!(
            "a record written now must carry record_version {}, found {found}",
            format::RECORD_VERSION
        )));
    }
    Ok(())
}

fn unsupported(found: u32) -> Error {
    Error::UnsupportedVersion {
        what: "record",
        found,
        supported: format::RECORD_VERSION,
    }
}

fn check_version(found: u32) -> Result<()> {
    if found > format::RECORD_VERSION {
        return Err(unsupported(found));
    }
    if found == 0 {
        return Err(rule("record_version 0 is not a valid record version"));
    }
    Ok(())
}

fn rule(msg: impl Into<String>) -> Error {
    Error::LedgerRule(msg.into())
}

/// The write-back check failed: the record's canonical JSON did not parse
/// back to an identical record. Durations are integer nanoseconds (contract
/// v2) and always round-trip; the remaining causes are non-finite floats,
/// which serialize as `null`, and any future type whose JSON form is lossy.
fn not_faithful(_record: &Record, why: &str) -> Error {
    rule(format!(
        "record does not round-trip through its canonical JSON ({why}). A NaN or \
         infinite float is written as null and cannot be read back."
    ))
}

fn check_created_at(what: &str, created_at: &str) -> Result<()> {
    if rfc3339::is_rfc3339(created_at) {
        Ok(())
    } else {
        Err(rule(format!(
            "{what} created_at {created_at:?} is not RFC 3339"
        )))
    }
}

/// Invariant 6 for one record's provenance.
fn check_provenance(what: &str, p: &Provenance, manifest_required: bool) -> Result<()> {
    if p.inputs.is_empty() {
        return Err(rule(format!(
            "{what} provenance names no inputs (invariant 6)"
        )));
    }
    check_created_at(&format!("{what} provenance"), &p.created_at)?;
    if manifest_required && p.manifest_version.is_none() {
        return Err(rule(format!(
            "{what} provenance names no manifest version (invariant 6)"
        )));
    }
    Ok(())
}

/// A certificate that passed must report no differences.
fn check_pass_consistent(hash: Option<&Hash>, c: &CertBody, pass: bool) -> Result<()> {
    let clean = c.missing_left == 0
        && c.missing_right == 0
        && c.mismatched == 0
        && c.totals_delta.is_empty();
    if pass && !clean {
        let name = hash.map_or_else(String::new, |h| format!(" {h}"));
        return Err(rule(format!(
            "certificate{name} says pass but reports differences \
             (missing_left={}, missing_right={}, mismatched={}, totals_delta={})",
            c.missing_left,
            c.missing_right,
            c.mismatched,
            c.totals_delta.len()
        )));
    }
    Ok(())
}

fn wrong_kind(role: &str, hash: &Hash, expected: RecordKind, found: RecordKind) -> Error {
    rule(format!(
        "{role} {hash} resolves to {}, not {}",
        found.with_article(),
        expected.with_article()
    ))
}

/// An append-only, content-addressed ledger over any [`Store`].
///
/// `S` is anything that dereferences to a store: `&MyStore`, `Box<MyStore>`,
/// `Arc<dyn Store>`, and so on. The store is not exposed again: the only
/// write paths are [`Ledger::put`] and [`Ledger::put_receipt`].
pub struct Ledger<S> {
    store: S,
}

impl<S> Ledger<S>
where
    S: Deref,
    S::Target: Store,
{
    /// Wraps a store.
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Writes a non-receipt record after checking its write-time rules and
    /// returns its hash. Idempotent: writing the same record twice yields the
    /// same hash and the same bytes.
    ///
    /// A [`Receipt`] is refused here because its manifest rule needs the
    /// caller's current manifest version; use [`Ledger::put_receipt`].
    pub fn put(&self, record: &Record) -> Result<Hash> {
        check_write_version(record.record_version())?;
        match record {
            Record::Evidence(e) => check_provenance("evidence", &e.provenance, false)?,
            Record::Proposal(p) => self.check_proposal(p)?,
            Record::Replay(r) => self.check_replay(r)?,
            Record::Certificate(c) => self.check_certificate(c)?,
            Record::Receipt(_) => {
                return Err(rule(
                    "a receipt must be written with put_receipt, which checks \
                     manifest_after against the current manifest sequence",
                ))
            }
        }
        self.write(record)
    }

    /// Writes a [`Receipt`] after checking invariant 2. This is the only way
    /// a receipt enters the ledger.
    ///
    /// Stage 4 is apply, then attest. `current_manifest` is the manifest
    /// sequence (ADR 020, a `u64` rendered as a string) current when the
    /// receipt is written. The apply produced `receipt.manifest_after`, and
    /// maintenance commits (GC, compaction) may have advanced the sequence
    /// since, so the rule is `manifest_before < manifest_after <=
    /// current_manifest`, compared numerically; `manifest_before ==
    /// manifest_after` is allowed only for a rewrite proposal. The
    /// certificate and the replay must have been computed against
    /// `receipt.manifest_before`.
    pub fn put_receipt(&self, receipt: &Receipt, current_manifest: &str) -> Result<Hash> {
        check_write_version(receipt.record_version)?;
        self.check_receipt(receipt, current_manifest)?;
        self.write(&Record::Receipt(receipt.clone()))
    }

    /// Reads and parses a record. Verifies the content hash and the record
    /// version first. Returns [`Error::NotFound`] if no such record exists.
    ///
    /// Fields unknown to this build are dropped by the typed parse; use
    /// [`Ledger::get_raw`] to keep them.
    pub fn get(&self, hash: &Hash) -> Result<Record> {
        decode(&self.get_raw(hash)?)
    }

    /// Reads a record's bytes exactly as stored, after verifying that they
    /// hash to `hash` and that the record version and kind are supported.
    /// Unknown fields are preserved because nothing is re-serialized.
    pub fn get_raw(&self, hash: &Hash) -> Result<Bytes> {
        let bytes = self.read_verified(hash)?;
        peek(&bytes)?.into_kind()?;
        Ok(bytes)
    }

    /// The kind of a stored record, read from its header only.
    pub fn kind(&self, hash: &Hash) -> Result<RecordKind> {
        let bytes = self.read_verified(hash)?;
        peek(&bytes)?.into_kind()
    }

    /// Lists records of one kind, lazily. The kind lives in the content, so
    /// each record's header is read as the iterator advances; memory is
    /// bounded by the store's path listing, not by record contents.
    ///
    /// Records this build cannot read (a newer record version, or an unknown
    /// kind) are skipped, not reported as errors; see
    /// [`Ledger::list_unreadable`].
    pub fn list(&self, kind: RecordKind) -> Result<impl Iterator<Item = Result<Hash>> + '_> {
        Ok(self.scan()?.filter_map(move |(h, p)| match p {
            Ok(Peek::Readable(k)) if k == kind => Some(Ok(h)),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        }))
    }

    /// Lists records this build cannot read: a newer record version or an
    /// unknown kind (ADR 017). Lazy, like [`Ledger::list`].
    pub fn list_unreadable(&self) -> Result<impl Iterator<Item = Result<Hash>> + '_> {
        Ok(self.scan()?.filter_map(|(h, p)| match p {
            Ok(Peek::Readable(_)) => None,
            Ok(Peek::Newer(_) | Peek::UnknownKind(_)) => Some(Ok(h)),
            Err(e) => Some(Err(e)),
        }))
    }

    /// Walks a receipt's chain and returns it structured, with a plain-text
    /// rendering via [`Chain::render`] or `Display`. A link that is missing,
    /// of the wrong kind, or unreadable is flagged, never an error; only a
    /// store failure or a `receipt` that is not a receipt fails.
    pub fn explain(&self, receipt: &Hash) -> Result<Chain> {
        explain::explain(self, receipt, None)
    }

    /// [`Ledger::explain`], with the receipt's and the certificate's
    /// signatures folded against `trusted` ([`Link::verdict`]); the
    /// rendering adds `sig=valid(<name>)`, `sig=untrusted(<id prefix>)`,
    /// `sig=none`, or `sig=INVALID` to those two lines.
    pub fn explain_verified(&self, receipt: &Hash, trusted: &TrustedKeys) -> Result<Chain> {
        explain::explain(self, receipt, Some(trusted))
    }

    /// Signs the stored receipt or certificate `record` with `key` at
    /// `signed_at` (RFC 3339 UTC with milliseconds), writing the detached
    /// object `ledger/<record>.sig.<key id>.json` (ADR 029), and returns
    /// where.
    ///
    /// Refused, writing nothing: a `signed_at` in another spelling; a hash
    /// that is not a stored, readable Receipt or Certificate; a Receipt that
    /// [`Ledger::put_receipt`] would refuse now (rules 11 to 18 re-run
    /// against the stored chain, with the receipt's own `manifest_after` as
    /// the current sequence, since the upper bound was checked when it was
    /// written); a Certificate that [`Ledger::put`] would refuse now (rules
    /// 8 to 10). A signature is therefore only made over a chain that passes
    /// the rules at signing time.
    ///
    /// The object is written with `put_if_absent`: signing again with the
    /// same key is idempotent only if the bytes are identical (ed25519 is
    /// deterministic, so the same key, record, and `signed_at` give the
    /// same bytes); a differing object already there is refused, never
    /// overwritten.
    pub fn sign(&self, record: &Hash, key: &SigningKey, signed_at: &str) -> Result<SignaturePath> {
        if !is_utc_millis(signed_at) {
            return Err(rule(format!(
                "cannot sign {record}: signed_at {signed_at:?} is not RFC 3339 UTC with \
                 milliseconds (2026-10-25T00:00:00.000Z)"
            )));
        }
        let refuse = |why: String| rule(format!("cannot sign {record}: {why}"));
        let bytes = match self.read_verified(record) {
            Err(Error::NotFound(_)) => {
                return Err(refuse("no such record in the ledger".to_string()))
            }
            other => other?,
        };
        let reason = |e: Error| match e {
            Error::LedgerRule(m) => refuse(m),
            other => other,
        };
        let kind = match decode(&bytes)? {
            Record::Receipt(r) => {
                check_write_version(r.record_version)
                    .and_then(|()| self.check_receipt(&r, &r.manifest_after))
                    .map_err(reason)?;
                RecordKind::Receipt
            }
            Record::Certificate(c) => {
                check_write_version(c.record_version)
                    .and_then(|()| self.check_certificate(&c))
                    .map_err(reason)?;
                RecordKind::Certificate
            }
            other => {
                return Err(refuse(format!(
                    "it is {}; only receipts and certificates are signed",
                    RecordKind::of(&other).with_article()
                )))
            }
        };
        let object = signature::signature_bytes(record, kind, key, signed_at)?;
        let at = SignaturePath {
            record: *record,
            key_id: key.key_id(),
        };
        let path = at.path();
        match self.store.put_if_absent(&path, Bytes::from(object.clone())) {
            Ok(_) => Ok(at),
            // Refused because something is already there (a store reports
            // that as a version conflict, or in its own words): identical
            // bytes are this signature again; anything else is refused.
            Err(e) => match self.store.get(&path) {
                Ok((existing, _)) if existing.as_ref() == object.as_slice() => Ok(at),
                Ok(_) => Err(refuse(format!(
                    "key {} already signed it with different bytes ({path}); a signature is \
                     never overwritten",
                    at.key_id
                ))),
                Err(_) => Err(e),
            },
        }
    }

    /// Every signature object beside `record`, each verified on its own
    /// (no trust set), sorted by key id. A corrupt object is
    /// [`SignatureStatus::Invalid`], never a panic or an error. The record
    /// itself is read first: absent is [`Error::NotFound`], and bytes that
    /// do not hash to its name are the ledger's tamper error, so an altered
    /// record never reports a signature status at all. Writes nothing.
    pub fn signatures(&self, record: &Hash) -> Result<Vec<SignatureStatus>> {
        let bytes = self.read_verified(record)?;
        let kind = peek(&bytes)?.into_kind()?;
        let mut at: Vec<SignaturePath> = self
            .store
            .list(LEDGER_PREFIX)?
            .iter()
            .filter_map(|p| SignaturePath::parse(p))
            .filter(|s| s.record == *record)
            .collect();
        at.sort();
        at.dedup();
        let mut out = Vec::with_capacity(at.len());
        for a in &at {
            match self.store.get(&a.path()) {
                Ok((b, _)) => out.push(signature::check_object(&b, a, kind)),
                Err(Error::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// [`Ledger::signatures`] folded against `trusted` ([`Verdict::of`]).
    /// Only a key in `trusted`, signing within its window, makes a record
    /// [`Verdict::Signed`]; the public key an object carries proves only
    /// that the object is self-consistent. Writes nothing.
    pub fn verify_against(&self, record: &Hash, trusted: &TrustedKeys) -> Result<Verdict> {
        Ok(Verdict::of(&self.signatures(record)?, trusted))
    }

    // -- internals ---------------------------------------------------------

    fn scan(&self) -> Result<impl Iterator<Item = (Hash, Result<Peek>)> + '_> {
        let paths = self.store.list(LEDGER_PREFIX)?;
        Ok(paths
            .into_iter()
            .filter_map(|p| parse_record_path(&p))
            .map(move |h| {
                let peeked = self.read_verified(&h).and_then(|b| peek(&b));
                (h, peeked)
            }))
    }

    /// Serializes, proves the bytes parse back to the identical record, then
    /// stores them. The round trip rejects anything `get` could not return
    /// faithfully: NaN or infinite floats (serialized as `null`), durations
    /// whose JSON form loses precision or overflows, and so on.
    fn write(&self, record: &Record) -> Result<Hash> {
        let bytes = serde_json::to_vec(record)?;
        match decode(&bytes) {
            Ok(back) if back == *record => {}
            Ok(_) => return Err(not_faithful(record, "parsed back to a different value")),
            Err(e) => return Err(not_faithful(record, &e.to_string())),
        }
        let hash = Hash::of(&bytes);
        self.store
            .put_content_addressed(&record_path(&hash), Bytes::from(bytes))?;
        Ok(hash)
    }

    /// Reads bytes and checks they hash to their name. A store reports an
    /// absent path as [`Error::NotFound`] (the `bellwether-store` contract);
    /// that is normalized to this record's hash. Any other store failure
    /// propagates unchanged and is never mistaken for absence.
    pub(crate) fn read_verified(&self, hash: &Hash) -> Result<Bytes> {
        let bytes = match self.store.get(&record_path(hash)) {
            Ok((bytes, _)) => bytes,
            Err(Error::NotFound(_)) => return Err(Error::NotFound(*hash)),
            Err(e) => return Err(e),
        };
        if Hash::of(&bytes) != *hash {
            return Err(rule(format!(
                "record {hash} content does not match its name; the ledger has been altered"
            )));
        }
        Ok(bytes)
    }

    /// Resolves a reference made by a record being written. Absence becomes a
    /// rule violation naming the role of the reference.
    fn resolve(&self, hash: &Hash, role: &str) -> Result<Record> {
        match self.get(hash) {
            Err(Error::NotFound(_)) => Err(rule(format!("{role} {hash} does not resolve"))),
            other => other,
        }
    }

    fn resolve_proposal(&self, hash: &Hash, role: &str) -> Result<Proposal> {
        match self.resolve(hash, role)? {
            Record::Proposal(p) => Ok(p),
            other => Err(wrong_kind(
                role,
                hash,
                RecordKind::Proposal,
                RecordKind::of(&other),
            )),
        }
    }

    /// Cited evidence may be absent (it renders as missing in `explain`), but
    /// a hash that resolves must resolve to Evidence.
    fn check_cited_evidence(&self, role: &str, cited: &[Hash]) -> Result<()> {
        for h in cited {
            match self.kind(h) {
                Ok(RecordKind::Evidence) | Err(Error::NotFound(_)) => {}
                Ok(found) => return Err(wrong_kind(role, h, RecordKind::Evidence, found)),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn check_proposal(&self, p: &Proposal) -> Result<()> {
        check_created_at("proposal", &p.created_at)?;
        self.check_cited_evidence("proposal's evidence", &p.evidence)
    }

    /// A replay must name the data it read and must be as exact as its
    /// proposal's kind allows (contract v2.1). The content checks are shared
    /// with `put_receipt`, which re-runs them on the resolved replay.
    fn check_replay(&self, r: &Replay) -> Result<()> {
        check_created_at("replay", &r.created_at)?;
        let proposal = self.resolve_proposal(&r.proposal, "replay's proposal")?;
        check_replay_content(r, &proposal)
    }

    fn check_certificate(&self, c: &Certificate) -> Result<()> {
        check_provenance("certificate", &c.provenance, true)?;
        check_pass_consistent(None, &c.body, c.pass)?;
        if let Some(p) = &c.proposal {
            self.resolve_proposal(p, "certificate's proposal")?;
        }
        Ok(())
    }

    fn check_receipt(&self, r: &Receipt, current_manifest: &str) -> Result<()> {
        check_created_at("receipt", &r.created_at)?;
        self.check_cited_evidence("receipt's evidence", &r.evidence)?;

        let cert = match self.resolve(&r.certificate, "receipt's certificate")? {
            Record::Certificate(c) => c,
            other => {
                return Err(wrong_kind(
                    "receipt's certificate",
                    &r.certificate,
                    RecordKind::Certificate,
                    RecordKind::of(&other),
                ))
            }
        };
        if !cert.pass {
            return Err(rule(format!(
                "receipt's certificate {} did not pass",
                r.certificate
            )));
        }
        check_pass_consistent(Some(&r.certificate), &cert.body, cert.pass)?;
        let proposal = self.resolve_proposal(&r.proposal, "receipt's proposal")?;
        if cert.proposal != Some(r.proposal) {
            let verified = cert
                .proposal
                .map_or_else(|| "no proposal".to_owned(), |p| format!("proposal {p}"));
            return Err(rule(format!(
                "receipt's certificate {} verified {verified}, not the receipt's proposal {}",
                r.certificate, r.proposal
            )));
        }

        let replay = match self.resolve(&r.replay, "receipt's replay")? {
            Record::Replay(x) => x,
            other => {
                return Err(wrong_kind(
                    "receipt's replay",
                    &r.replay,
                    RecordKind::Replay,
                    RecordKind::of(&other),
                ))
            }
        };
        if replay.proposal != r.proposal {
            return Err(rule(format!(
                "receipt's replay {} scored proposal {}, not the receipt's proposal {}",
                r.replay, replay.proposal, r.proposal
            )));
        }
        // Re-run the replay content rules here, as rule 9 is re-run for the
        // certificate, so a replay that never went through them (planted, or
        // written by an older build) cannot back a receipt.
        check_replay_content(&replay, &proposal).map_err(|e| match e {
            Error::LedgerRule(m) => rule(format!("receipt's replay {}: {m}", r.replay)),
            other => other,
        })?;

        // Manifest versions are sequences (ADR 020): u64s rendered as strings,
        // compared numerically.
        let before = parse_sequence("receipt's manifest_before", &r.manifest_before)?;
        let after = parse_sequence("receipt's manifest_after", &r.manifest_after)?;
        let current = parse_sequence("current manifest sequence", current_manifest)?;

        // Apply, then attest: the certificate and the replay were computed
        // against the pre-apply manifest. This is the security property.
        if cert.provenance.manifest_version.as_deref() != Some(r.manifest_before.as_str()) {
            return Err(rule(format!(
                "receipt's certificate {} was computed against manifest {:?}, not the \
                 receipt's manifest_before {:?}",
                r.certificate, cert.provenance.manifest_version, r.manifest_before
            )));
        }
        if replay.manifest_version != r.manifest_before {
            return Err(rule(format!(
                "receipt's replay {} was computed against manifest {:?}, not the \
                 receipt's manifest_before {:?}",
                r.replay, replay.manifest_version, r.manifest_before
            )));
        }
        // The apply produced `manifest_after`, so it lies after
        // `manifest_before` (a rewrite may leave the manifest unchanged).
        // Maintenance commits (GC, compaction) may have advanced the sequence
        // again before the receipt is written, so `manifest_after` need only
        // be no later than the current sequence.
        let rewrite = matches!(proposal.body, ProposalBody::Rewrite { .. });
        if after < before || (after == before && !rewrite) {
            let why = if after == before {
                format!(
                    "only a rewrite may leave the manifest unchanged, and this is a {} proposal",
                    proposal_kind(&proposal.body)
                )
            } else {
                "an apply never moves the manifest backwards".to_owned()
            };
            return Err(rule(format!(
                "receipt's manifest_after {after} is not greater than manifest_before {before}; {why}"
            )));
        }
        if after > current {
            return Err(rule(format!(
                "receipt's manifest_after {after} is greater than the current manifest \
                 sequence {current}"
            )));
        }
        Ok(())
    }
}

/// Parses a manifest sequence (ADR 020): a `u64` rendered as a canonical
/// decimal string, with no sign, whitespace, or leading zeros, so that string
/// equality and numeric equality agree.
fn parse_sequence(role: &str, s: &str) -> Result<u64> {
    let canonical =
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && (s == "0" || !s.starts_with('0'));
    canonical
        .then(|| s.parse::<u64>().ok())
        .flatten()
        .ok_or_else(|| {
            rule(format!(
                "{role} {s:?} is not a manifest sequence number (ADR 020: a u64 in \
                 canonical decimal)"
            ))
        })
}

fn proposal_kind(b: &ProposalBody) -> &'static str {
    match b {
        ProposalBody::Key { .. } => "key",
        ProposalBody::Layout { .. } => "layout",
        ProposalBody::Rewrite { .. } => "rewrite",
    }
}

impl<S> fmt::Debug for Ledger<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ledger").finish_non_exhaustive()
    }
}
