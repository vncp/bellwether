//! Reuse of footer statistics, fanout tables, and workload summaries through
//! the store's content-addressed cache (`cache/<input-hash>.bin`, with its
//! `.meta.json` age sidecar; `docs/format.md`).
//!
//! Every artifact is named by the BLAKE3 of the canonical JSON of everything
//! it is derived from: file fingerprints, the dataset schema, the source
//! configuration, the encoding version, the artifact's format version, and
//! for fanout tables every option that changes a number (columns, tuple
//! arity, table size, sketch precision, hash seed). A cached sampled table
//! can therefore never be served for a request that would be exact, and a
//! changed file changes every key derived from it. The name is what
//! `Provenance::inputs` cites, and the sidecar's `created_at` is the age the
//! Evidence reports.

use std::sync::Arc;
use std::time::Duration;

use bellwether_store::{Cache, StoreExt};
use bellwether_types::{Hash, Result};
use bytes::Bytes;
use serde::Serialize;

/// What a cache lookup produced.
#[derive(Clone, Debug)]
pub struct Cached<T> {
    /// The artifact.
    pub value: T,
    /// Its cache key (the input hash), cited in provenance.
    pub key: Hash,
    /// Age of the artifact: zero when just computed.
    pub age: Duration,
    /// Whether it came from the cache.
    pub hit: bool,
}

/// The input hash of a serializable description of an artifact's inputs.
pub fn key<T: Serialize>(inputs: &T) -> Result<Hash> {
    Hash::of_record(inputs)
}

/// Loads the artifact named `key`, or computes, stores, and returns it.
/// With `reuse == false` the artifact is always recomputed and overwritten.
/// A cached payload that does not decode is recomputed: the cache is
/// rebuildable and its format version is part of every key.
pub fn load_or_compute<S, T>(
    store: &Arc<S>,
    key: Hash,
    reuse: bool,
    decode: impl Fn(&[u8]) -> Result<T>,
    encode: impl Fn(&T) -> Result<Bytes>,
    compute: impl FnOnce() -> Result<T>,
) -> Result<Cached<T>>
where
    S: StoreExt + ?Sized,
{
    let cache = Cache::new(Arc::clone(store), "");
    if reuse {
        if let Some((bytes, age)) = cache.get_cached(&key)? {
            if let Ok(value) = decode(&bytes) {
                return Ok(Cached {
                    value,
                    key,
                    age,
                    hit: true,
                });
            }
        }
    }
    let value = compute()?;
    cache.put_cached(&key, encode(&value)?)?;
    Ok(Cached {
        value,
        key,
        age: Duration::ZERO,
        hit: false,
    })
}
