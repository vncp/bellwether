//! Content-addressed cache under `<root>/cache/<input-hash>.bin`.
//!
//! Each entry is named by the hash of the inputs it was derived from and has
//! a sidecar `<input-hash>.meta.json` holding `created_at` and the blake3 of
//! the payload. The cache is rebuildable; it carries its age so evidence
//! derived from it can state how old its inputs were (invariant 6). An entry
//! whose sidecar is missing, unparsable, or disagrees with the payload is a
//! miss, never an error.

use std::sync::Arc;
use std::time::Duration;

use bellwether_types::{Hash, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::backend::StoreExt;
use crate::manifest::{join, now_rfc3339, parse_rfc3339, CACHE_DIR};

#[derive(Serialize, Deserialize)]
struct CacheMeta {
    created_at: String,
    payload_blake3: Hash,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

/// A cache rooted at `<root>/cache/`.
#[derive(Clone, Debug)]
pub struct Cache<S: StoreExt + ?Sized> {
    store: Arc<S>,
    root: String,
}

impl<S: StoreExt + ?Sized> Cache<S> {
    /// A cache in `store` under the index root `root`.
    pub fn new(store: Arc<S>, root: &str) -> Self {
        Self {
            store,
            root: root.to_string(),
        }
    }

    /// Store path of the payload for `input_hash`.
    #[must_use]
    pub fn payload_path(&self, input_hash: &Hash) -> String {
        join(
            &self.root,
            &format!("{CACHE_DIR}/{}.bin", input_hash.to_hex()),
        )
    }

    /// Store path of the age sidecar for `input_hash`.
    #[must_use]
    pub fn meta_path(&self, input_hash: &Hash) -> String {
        join(
            &self.root,
            &format!("{CACHE_DIR}/{}.meta.json", input_hash.to_hex()),
        )
    }

    /// Stores `payload` under `input_hash`, stamped now. The payload is
    /// written before the sidecar, so a reader never sees an age for bytes
    /// that are not there.
    pub fn put_cached(&self, input_hash: &Hash, payload: Bytes) -> Result<()> {
        let meta = CacheMeta {
            created_at: now_rfc3339(),
            payload_blake3: Hash::of(&payload),
            extra: serde_json::Map::new(),
        };
        self.store
            .put_content_addressed(&self.payload_path(input_hash), payload)?;
        self.store.put_content_addressed(
            &self.meta_path(input_hash),
            Bytes::from(serde_json::to_vec(&meta)?),
        )
    }

    /// Stores a payload named by its own hash (`input_hash` is its blake3:
    /// a key set) unless that entry is already stored whole, so a repeated
    /// lookup with the same keys writes and fsyncs nothing. The
    /// entry keeps its first `created_at`; use it only for payloads whose
    /// age no reader uses. A missing, torn, or unparsable entry is written
    /// afresh by [`Self::put_cached`]. Refuses a payload that is not named
    /// by its hash.
    pub fn put_cached_once(&self, input_hash: &Hash, payload: Bytes) -> Result<()> {
        let hash = Hash::of(&payload);
        if hash != *input_hash {
            return Err(bellwether_types::Error::Other(format!(
                "a payload stored once must be named by its blake3 {hash}, not {input_hash}"
            )));
        }
        let stored = self
            .store
            .get_opt(&self.meta_path(input_hash))?
            .and_then(|(m, _)| serde_json::from_slice::<CacheMeta>(&m).ok())
            .is_some_and(|m| m.payload_blake3 == hash)
            && self
                .store
                .get_opt(&self.payload_path(input_hash))?
                .is_some_and(|(p, _)| Hash::of(&p) == hash);
        if stored {
            return Ok(());
        }
        self.put_cached(input_hash, payload)
    }

    /// The cached payload for `input_hash` and its age, or `None` on a miss.
    /// Store I/O errors other than absence are returned as errors.
    pub fn get_cached(&self, input_hash: &Hash) -> Result<Option<(Bytes, Duration)>> {
        self.get_cached_at(input_hash, chrono::Utc::now())
    }

    /// [`Self::get_cached`] with the age measured to `now` instead of the
    /// system clock (a caller's fixed clock); zero when the entry is stamped
    /// after `now`.
    pub fn get_cached_at(
        &self,
        input_hash: &Hash,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<(Bytes, Duration)>> {
        let Some((meta_bytes, _)) = self.store.get_opt(&self.meta_path(input_hash))? else {
            return Ok(None);
        };
        let Ok(meta) = serde_json::from_slice::<CacheMeta>(&meta_bytes) else {
            return Ok(None);
        };
        let Some((payload, _)) = self.store.get_opt(&self.payload_path(input_hash))? else {
            return Ok(None);
        };
        if Hash::of(&payload) != meta.payload_blake3 {
            return Ok(None);
        }
        let Ok(created) = parse_rfc3339(&meta.created_at) else {
            return Ok(None);
        };
        let age = (now - created).to_std().unwrap_or_default();
        Ok(Some((payload, age)))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::ObjectStoreBackend;
    use bellwether_types::Store;

    #[test]
    fn round_trip_with_age_and_layout() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ObjectStoreBackend::local(dir.path()).unwrap());
        let cache = Cache::new(Arc::clone(&store), "idx");
        let key = Hash::of(b"inputs");
        assert!(cache.get_cached(&key).unwrap().is_none());

        cache
            .put_cached(&key, Bytes::from_static(b"footer stats"))
            .unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let (bytes, age) = cache.get_cached(&key).unwrap().unwrap();
        assert_eq!(&bytes[..], b"footer stats");
        assert!(age >= Duration::from_millis(10), "{age:?}");
        assert!(age < Duration::from_secs(60));
        assert!(dir
            .path()
            .join(format!("idx/cache/{}.bin", key.to_hex()))
            .is_file());

        // A payload that disagrees with its sidecar is a miss.
        store
            .put_content_addressed(&cache.payload_path(&key), Bytes::from_static(b"torn"))
            .unwrap();
        assert!(cache.get_cached(&key).unwrap().is_none());
    }

    /// A key set stored once is not rewritten (its sidecar keeps its first
    /// stamp), a torn entry is rewritten whole, and a payload not named by
    /// its hash is refused.
    #[test]
    fn put_cached_once_skips_a_stored_entry_and_repairs_a_torn_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ObjectStoreBackend::local(dir.path()).unwrap());
        let cache = Cache::new(Arc::clone(&store), "idx");
        let payload = Bytes::from_static(b"key set");
        let key = Hash::of(&payload);
        cache.put_cached_once(&key, payload.clone()).unwrap();
        let meta = store.get(&cache.meta_path(&key)).unwrap().0;
        std::thread::sleep(Duration::from_millis(5));
        cache.put_cached_once(&key, payload.clone()).unwrap();
        assert_eq!(store.get(&cache.meta_path(&key)).unwrap().0, meta);
        store
            .put_content_addressed(&cache.payload_path(&key), Bytes::from_static(b"torn"))
            .unwrap();
        assert!(cache.get_cached(&key).unwrap().is_none());
        cache.put_cached_once(&key, payload.clone()).unwrap();
        assert_eq!(cache.get_cached(&key).unwrap().unwrap().0, payload);
        assert!(cache.put_cached_once(&Hash::of(b"other"), payload).is_err());
    }
}
