//! [`ObjectStoreBackend`]: the [`Store`] trait over `object_store`, with each
//! backend's native conditional write, behind a synchronous facade.
//!
//! # Conditional-write semantics per backend (verified against `object_store` 0.14.2, 2026-10-24)
//!
//! | Backend | `put_if_absent` | `put_if_version` | Version token |
//! |---|---|---|---|
//! | Local filesystem | implemented in `local.rs`: under the path lock, fsynced staging file + `hard_link`, which fails atomically if the target exists | **not supported by `object_store`** (`PutMode::Update` returns `NotImplemented`); implemented in `local.rs` under an exclusive kernel `flock` on `<path>.lock#0` taken with a deadline, compare content hash, fsynced staging file + atomic `rename` + directory fsync | blake3 of the content |
//! | S3 | `PutMode::Create` → `If-None-Match: *` | `PutMode::Update` → `If-Match: <etag>` | etag |
//! | Azure / ADLS Gen2 | `PutMode::Create` → `If-None-Match: *` | `PutMode::Update` → `If-Match: <etag>` | etag |
//! | GCS | `PutMode::Create` → `x-goog-if-generation-match: 0` | `PutMode::Update` → `x-goog-if-generation-match: <generation>` | etag + generation |
//! | Memory (`memory://`) | `PutMode::Create` | `PutMode::Update` on etag | etag |
//!
//! `object_store` 0.14 defaults S3 to `S3ConditionalPut::ETagMatch` (0.11 left
//! conditional writes off); this crate still sets it explicitly unless the
//! caller configured `aws_conditional_put`, so the behaviour does not rest on
//! the default. The only other value is `disabled`, under which every
//! `put_if_version` fails with `NotImplemented`; the `DynamoDB` lock
//! (`dynamo:<table>`) was removed upstream, and a store configured with it is
//! refused when it is opened. On S3 a conditional update of an absent object
//! is reported as `Precondition` (0.11 reported `NotFound`); both map to
//! `VersionConflict` here.

use std::future::Future;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;

use bellwether_types::{Error, Hash, Result, Store, StoreVersion};
use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::prefix::PrefixStore;
use object_store::{
    ObjectMeta, ObjectStore, ObjectStoreExt, ObjectStoreScheme, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};
use url::Url;

/// Separator between etag and generation inside a [`StoreVersion`] token.
/// Neither etags nor GCS generations contain a unit separator.
const TOKEN_SEP: char = '\u{1f}';

/// Default page size for streamed listings (ADR 018).
pub const DEFAULT_PAGE_SIZE: usize = 1000;

/// Which object-store implementation a URL selected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    /// Local filesystem (`file://` URL or a bare path).
    Local,
    /// In-process memory (`memory://`); for tests.
    Memory,
    /// Amazon S3 or an S3-compatible store (`s3://`, `s3a://`, S3 https hosts).
    S3,
    /// Azure Blob / ADLS Gen2 (`az://`, `adl://`, `azure://`, `abfs://`, `abfss://`, Azure https hosts).
    Azure,
    /// Google Cloud Storage (`gs://`).
    Gcs,
}

/// Parses a store URL (or a bare local path) into its backend kind and the
/// key prefix inside the bucket or container. Performs no I/O.
pub fn parse_store_url(url: &str) -> Result<(BackendKind, Url, String)> {
    let parsed = match Url::parse(url) {
        Ok(u) if u.scheme().len() > 1 => u,
        // A bare path, or a Windows drive letter parsed as a scheme.
        _ => {
            let abs = std::path::absolute(url).map_err(|e| Error::Store(e.to_string()))?;
            Url::from_directory_path(&abs)
                .map_err(|()| Error::Store(format!("invalid local path {url}")))?
        }
    };
    let (scheme, path) = ObjectStoreScheme::parse(&parsed)
        .map_err(|e| Error::Store(format!("unrecognised store url {url}: {e}")))?;
    let kind = match scheme {
        ObjectStoreScheme::Local => BackendKind::Local,
        ObjectStoreScheme::Memory => BackendKind::Memory,
        ObjectStoreScheme::AmazonS3 => BackendKind::S3,
        ObjectStoreScheme::MicrosoftAzure => BackendKind::Azure,
        ObjectStoreScheme::GoogleCloudStorage => BackendKind::Gcs,
        other => {
            return Err(Error::Store(format!(
                "store url {url}: backend {other:?} has no conditional PUT and is not supported"
            )))
        }
    };
    Ok((kind, parsed, path.as_ref().to_string()))
}

/// Builds the S3 builder used for `s3://` URLs: environment, then options,
/// then the URL, then `ETagMatch` conditional puts unless already configured.
pub fn s3_builder<I, K, V>(url: &str, options: I) -> Result<AmazonS3Builder>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let mut b = AmazonS3Builder::from_env();
    for (k, v) in options {
        let key: AmazonS3ConfigKey = k
            .as_ref()
            .parse()
            .map_err(|e| Error::Store(format!("{e}")))?;
        b = b.with_config(key, v);
    }
    b = b.with_url(url);
    if b.get_config_value(&AmazonS3ConfigKey::ConditionalPut)
        .is_none()
    {
        b = b.with_conditional_put(S3ConditionalPut::ETagMatch);
    }
    Ok(b)
}

fn azure_builder<I, K, V>(url: &str, options: I) -> Result<MicrosoftAzureBuilder>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let mut b = MicrosoftAzureBuilder::from_env();
    for (k, v) in options {
        let key = k
            .as_ref()
            .parse()
            .map_err(|e| Error::Store(format!("{e}")))?;
        b = b.with_config(key, v);
    }
    Ok(b.with_url(url))
}

fn gcs_builder<I, K, V>(url: &str, options: I) -> Result<GoogleCloudStorageBuilder>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let mut b = GoogleCloudStorageBuilder::from_env();
    for (k, v) in options {
        let key = k
            .as_ref()
            .parse()
            .map_err(|e| Error::Store(format!("{e}")))?;
        b = b.with_config(key, v);
    }
    Ok(b.with_url(url))
}

fn store_err(e: impl std::fmt::Display) -> Error {
    Error::Store(e.to_string())
}

fn obj_path(path: &str) -> Result<ObjPath> {
    ObjPath::parse(path).map_err(|e| Error::Store(format!("invalid path {path:?}: {e}")))
}

/// Encodes an object-store etag and optional version (GCS generation) into a
/// single opaque token.
fn encode_token(e_tag: Option<&str>, version: Option<&str>) -> StoreVersion {
    match version {
        Some(v) => StoreVersion(format!("{}{TOKEN_SEP}{v}", e_tag.unwrap_or_default())),
        None => StoreVersion(e_tag.unwrap_or_default().to_string()),
    }
}

fn decode_token(token: &StoreVersion) -> UpdateVersion {
    let non_empty = |s: &str| (!s.is_empty()).then(|| s.to_string());
    match token.0.split_once(TOKEN_SEP) {
        Some((e, v)) => UpdateVersion {
            e_tag: non_empty(e),
            version: non_empty(v),
        },
        None => UpdateVersion {
            e_tag: non_empty(&token.0),
            version: None,
        },
    }
}

/// The hash an absent path is reported under: the file stem when it is a
/// content hash, otherwise the blake3 of the path.
fn not_found_hash(path: &str) -> Hash {
    let name = path.rsplit('/').next().unwrap_or(path);
    let stem = name.split_once('.').map_or(name, |(s, _)| s);
    Hash::from_hex(stem).unwrap_or_else(|_| Hash::of(path.as_bytes()))
}

use crate::local::{self, content_token};

/// Operations beyond the [`Store`] contract that the commit protocol, GC, and
/// cache need: an existence check that distinguishes "absent" from "error",
/// and a bounded-memory paged listing.
pub trait StoreExt: Store {
    /// Reads a path, returning `None` if it does not exist.
    fn get_opt(&self, path: &str) -> Result<Option<(Bytes, StoreVersion)>>;
    /// Whether a path exists. Cheap: a metadata request, never a body read.
    fn exists(&self, path: &str) -> Result<bool>;
    /// Reads the byte range `range` of a path, or `None` if it is absent.
    fn get_range(&self, path: &str, range: std::ops::Range<u64>) -> Result<Option<Bytes>>;
    /// Streams the listing under `prefix`, calling `f` with at most
    /// `page_size` entries at a time. Never holds more than one page.
    fn list_pages(
        &self,
        prefix: &str,
        page_size: usize,
        f: &mut dyn FnMut(&[ObjectMeta]) -> Result<ControlFlow<()>>,
    ) -> Result<()>;
}

/// The backend's private runtime. Dropping a tokio runtime from inside an
/// async context panics, so when the last backend clone is dropped there the
/// runtime is shut down in the background instead.
struct OwnedRuntime(Option<tokio::runtime::Runtime>);

impl OwnedRuntime {
    fn get(&self) -> &tokio::runtime::Runtime {
        self.0.as_ref().expect("runtime present until drop")
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            if tokio::runtime::Handle::try_current().is_ok() {
                rt.shutdown_background();
            }
        }
    }
}

/// [`Store`] over `object_store`. Cheap to clone; clones share the runtime and
/// the underlying client.
#[derive(Clone)]
pub struct ObjectStoreBackend {
    inner: Arc<dyn ObjectStore>,
    kind: BackendKind,
    /// For the local backend, the filesystem directory the store is rooted at.
    local_root: Option<PathBuf>,
    rt: Arc<OwnedRuntime>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("kind", &self.kind)
            .field("inner", &self.inner.to_string())
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    /// Opens a store from a URL using credentials from the environment.
    /// `file:///dir` or a bare path selects the local filesystem (the
    /// directory is created if missing), `s3://`, `az://`/`abfss://`,
    /// `gs://` select the cloud backends, `memory://` an in-process store.
    pub fn from_url(url: &str) -> Result<Self> {
        Self::from_url_with_options(url, std::iter::empty::<(&str, &str)>())
    }

    /// Like [`Self::from_url`] with explicit backend configuration keys (the
    /// `object_store` config key names, for example `aws_access_key_id`).
    pub fn from_url_with_options<I, K, V>(url: &str, options: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let (kind, parsed, prefix) = parse_store_url(url)?;
        let base: Arc<dyn ObjectStore> = match kind {
            BackendKind::Local => {
                let dir = parsed
                    .to_file_path()
                    .map_err(|()| Error::Store(format!("invalid file url {url}")))?;
                // The URL path is the directory itself; no PrefixStore needed.
                return Self::local(dir);
            }
            BackendKind::Memory => Arc::new(InMemory::new()),
            BackendKind::S3 => Arc::new(s3_builder(url, options)?.build().map_err(store_err)?),
            BackendKind::Azure => {
                Arc::new(azure_builder(url, options)?.build().map_err(store_err)?)
            }
            BackendKind::Gcs => Arc::new(gcs_builder(url, options)?.build().map_err(store_err)?),
        };
        let inner: Arc<dyn ObjectStore> = if prefix.is_empty() {
            base
        } else {
            Arc::new(PrefixStore::new(base, prefix.as_str()))
        };
        Self::with_store(inner, kind, None)
    }

    /// Opens the local filesystem rooted at `dir`, creating it (durably) if
    /// missing.
    pub fn local(dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let dir = dir.as_ref();
        local::create_dirs_durably(&std::path::absolute(dir).map_err(store_err)?)?;
        let fs = LocalFileSystem::new_with_prefix(dir).map_err(store_err)?;
        let root = std::fs::canonicalize(dir).map_err(store_err)?;
        Self::with_store(Arc::new(fs), BackendKind::Local, Some(root))
    }

    /// An in-process memory store; exercises the same `PutMode::Update`
    /// mapping the cloud backends use.
    pub fn memory() -> Result<Self> {
        Self::with_store(Arc::new(InMemory::new()), BackendKind::Memory, None)
    }

    fn with_store(
        inner: Arc<dyn ObjectStore>,
        kind: BackendKind,
        local_root: Option<PathBuf>,
    ) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("bellwether-store")
            .enable_all()
            .build()
            .map_err(store_err)?;
        Ok(Self {
            inner,
            kind,
            local_root,
            rt: Arc::new(OwnedRuntime(Some(rt))),
        })
    }

    /// The backend kind this store was opened with.
    #[must_use]
    pub fn kind(&self) -> BackendKind {
        self.kind
    }

    /// The underlying `object_store` client, for callers that need async access.
    #[must_use]
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    /// Runs a future on the store's runtime. When called from inside another
    /// tokio runtime, the future is driven from a scoped helper thread so the
    /// synchronous facade never panics with "runtime within runtime".
    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|s| {
                s.spawn(|| self.rt.get().block_on(fut))
                    .join()
                    .expect("store helper thread panicked")
            })
        } else {
            self.rt.get().block_on(fut)
        }
    }

    async fn version_of(&self, location: &ObjPath) -> String {
        match self.inner.head(location).await {
            Ok(meta) => encode_token(meta.e_tag.as_deref(), meta.version.as_deref()).0,
            Err(object_store::Error::NotFound { .. }) => "<absent>".to_string(),
            Err(_) => "<unknown>".to_string(),
        }
    }

    fn get_opt_inner(&self, path: &str) -> Result<Option<(Bytes, StoreVersion)>> {
        let location = obj_path(path)?;
        let local = self.kind == BackendKind::Local;
        self.block_on(async {
            let res = match self.inner.get(&location).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(store_err(e)),
            };
            let meta = res.meta.clone();
            let bytes = res.bytes().await.map_err(store_err)?;
            let token = if local {
                content_token(&bytes)
            } else {
                encode_token(meta.e_tag.as_deref(), meta.version.as_deref())
            };
            Ok(Some((bytes, token)))
        })
    }

    /// For the local backend, the filesystem path of a store path.
    fn local_target(&self, path: &str) -> Result<Option<PathBuf>> {
        let Some(root) = self.local_root.as_ref() else {
            return Ok(None);
        };
        let location = obj_path(path)?;
        Ok(Some(root.join(location.as_ref())))
    }
}

impl Store for ObjectStoreBackend {
    /// Reads a path. An absent path is [`Error::NotFound`] carrying the hash
    /// named by the file stem when the path is content-addressed
    /// (`.../<64 hex>.<ext>`), otherwise the blake3 of the path string; any
    /// other failure is [`Error::Store`]. [`StoreExt::get_opt`] returns
    /// `None` instead.
    fn get(&self, path: &str) -> Result<(Bytes, StoreVersion)> {
        self.get_opt_inner(path)?
            .ok_or_else(|| Error::NotFound(not_found_hash(path)))
    }

    fn put_if_absent(&self, path: &str, body: Bytes) -> Result<StoreVersion> {
        if let Some(target) = self.local_target(path)? {
            return local::put_if_absent(&target, &body);
        }
        let location = obj_path(path)?;
        self.block_on(async {
            let opts = PutOptions::from(PutMode::Create);
            match self
                .inner
                .put_opts(&location, PutPayload::from(body), opts)
                .await
            {
                Ok(r) => Ok(encode_token(r.e_tag.as_deref(), r.version.as_deref())),
                Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => Err(Error::VersionConflict {
                    expected: "<absent>".to_string(),
                    found: self.version_of(&location).await,
                }),
                Err(e) => Err(store_err(e)),
            }
        })
    }

    fn put_if_version(
        &self,
        path: &str,
        body: Bytes,
        expected: &StoreVersion,
    ) -> Result<StoreVersion> {
        if let Some(target) = self.local_target(path)? {
            return local::put_if_version(&target, &body, expected);
        }
        let location = obj_path(path)?;
        let update = decode_token(expected);
        self.block_on(async {
            let opts = PutOptions::from(PutMode::Update(update));
            match self
                .inner
                .put_opts(&location, PutPayload::from(body), opts)
                .await
            {
                Ok(r) => Ok(encode_token(r.e_tag.as_deref(), r.version.as_deref())),
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. }
                    | object_store::Error::NotFound { .. },
                ) => Err(Error::VersionConflict {
                    expected: expected.0.clone(),
                    found: self.version_of(&location).await,
                }),
                Err(e) => Err(store_err(e)),
            }
        })
    }

    fn put_content_addressed(&self, path: &str, body: Bytes) -> Result<()> {
        if let Some(target) = self.local_target(path)? {
            return local::put_overwrite(&target, &body);
        }
        let location = obj_path(path)?;
        self.block_on(async {
            self.inner
                .put(&location, PutPayload::from(body))
                .await
                .map(|_| ())
                .map_err(store_err)
        })
    }

    /// Lists every path under `prefix`. This materializes the whole listing
    /// because the [`Store`] contract returns a `Vec`; bounded-memory callers
    /// use [`StoreExt::list_pages`] instead (ADR 018).
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        self.list_pages(prefix, DEFAULT_PAGE_SIZE, &mut |page| {
            out.extend(page.iter().map(|m| m.location.to_string()));
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(out)
    }

    fn delete(&self, path: &str) -> Result<()> {
        let location = obj_path(path)?;
        self.block_on(async {
            match self.inner.delete(&location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(store_err(e)),
            }
        })
    }
}

impl StoreExt for ObjectStoreBackend {
    fn get_opt(&self, path: &str) -> Result<Option<(Bytes, StoreVersion)>> {
        self.get_opt_inner(path)
    }

    fn get_range(&self, path: &str, range: std::ops::Range<u64>) -> Result<Option<Bytes>> {
        let location = obj_path(path)?;
        self.block_on(async {
            match self.inner.get_range(&location, range).await {
                Ok(b) => Ok(Some(b)),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(store_err(e)),
            }
        })
    }

    fn exists(&self, path: &str) -> Result<bool> {
        let location = obj_path(path)?;
        self.block_on(async {
            match self.inner.head(&location).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(e) => Err(store_err(e)),
            }
        })
    }

    fn list_pages(
        &self,
        prefix: &str,
        page_size: usize,
        f: &mut dyn FnMut(&[ObjectMeta]) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let page_size = page_size.max(1);
        let prefix = if prefix.is_empty() {
            None
        } else {
            Some(obj_path(prefix.trim_end_matches('/'))?)
        };
        // The stream is polled on the runtime one item at a time; only one
        // page of metadata is ever held. `f` runs on the calling thread, so it
        // may itself call back into this store.
        let mut stream = self.inner.list(prefix.as_ref());
        let mut page: Vec<ObjectMeta> = Vec::with_capacity(page_size.min(DEFAULT_PAGE_SIZE));
        loop {
            let next = self.block_on(stream.next());
            match next {
                Some(Ok(meta)) => {
                    page.push(meta);
                    if page.len() >= page_size {
                        if f(&page)?.is_break() {
                            return Ok(());
                        }
                        page.clear();
                    }
                }
                Some(Err(e)) => return Err(store_err(e)),
                None => break,
            }
        }
        if !page.is_empty() {
            let _ = f(&page)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn token_round_trips_etag_and_generation() {
        let t = encode_token(Some("\"abc\""), Some("1712"));
        let u = decode_token(&t);
        assert_eq!(u.e_tag.as_deref(), Some("\"abc\""));
        assert_eq!(u.version.as_deref(), Some("1712"));
        let t = encode_token(Some("etag"), None);
        assert_eq!(t.0, "etag");
        let u = decode_token(&t);
        assert_eq!(u.e_tag.as_deref(), Some("etag"));
        assert_eq!(u.version, None);
    }

    #[test]
    fn absent_get_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStoreBackend::local(dir.path()).unwrap();
        let h = Hash::of(b"rec");
        match store.get(&format!("ledger/{}.rec", h.to_hex())) {
            Err(Error::NotFound(found)) => assert_eq!(found, h),
            other => panic!("{other:?}"),
        }
        match store.get("idx/manifest.json") {
            Err(Error::NotFound(found)) => assert_eq!(found, Hash::of(b"idx/manifest.json")),
            other => panic!("{other:?}"),
        }
        assert!(store.get_opt("idx/manifest.json").unwrap().is_none());
        // `list` is a path-segment prefix, object_store style.
        store
            .put_content_addressed("a/bc", Bytes::from_static(b"x"))
            .unwrap();
        store
            .put_content_addressed("a/b/c", Bytes::from_static(b"x"))
            .unwrap();
        assert_eq!(store.list("a/b").unwrap(), vec!["a/b/c".to_string()]);
        assert_eq!(store.list("a/b/c").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn usable_and_droppable_inside_a_tokio_runtime() {
        let outer = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        outer.block_on(async {
            let store = ObjectStoreBackend::memory().unwrap();
            store.put_if_absent("p", Bytes::from_static(b"x")).unwrap();
            assert_eq!(&store.get("p").unwrap().0[..], b"x");
            drop(store);
        });
    }

    #[test]
    fn local_lock_and_staging_files_never_appear_in_listings() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStoreBackend::local(dir.path()).unwrap();
        let h = Hash::of(b"rec").to_hex();
        let rec = format!("ledger/{h}.rec");
        let v = store
            .put_if_absent(&rec, Bytes::from_static(b"one"))
            .unwrap();
        store
            .put_if_version(&rec, Bytes::from_static(b"two"), &v)
            .unwrap();
        // A crashed writer's leftover staging file.
        std::fs::write(
            dir.path()
                .join(format!("ledger/{h}.rec#98765432109876543210")),
            b"partial",
        )
        .unwrap();
        assert_eq!(store.list("ledger").unwrap(), vec![rec.clone()]);
        assert_eq!(store.list("").unwrap(), vec![rec]);
    }

    #[test]
    fn parses_scheme_and_prefix() {
        let cases = [
            ("s3://bucket/idx/root", BackendKind::S3, "idx/root"),
            ("s3a://bucket", BackendKind::S3, ""),
            ("gs://bucket/a/b", BackendKind::Gcs, "a/b"),
            ("az://container/x", BackendKind::Azure, "x"),
            (
                "abfss://container@acct.dfs.core.windows.net/lake/idx",
                BackendKind::Azure,
                "lake/idx",
            ),
            ("memory:///", BackendKind::Memory, ""),
        ];
        for (url, kind, prefix) in cases {
            let (k, _, p) = parse_store_url(url).unwrap();
            assert_eq!(k, kind, "{url}");
            assert_eq!(p, prefix, "{url}");
        }
        let (k, _, _) = parse_store_url("file:///tmp/bw").unwrap();
        assert_eq!(k, BackendKind::Local);
        let (k, _, _) = parse_store_url("relative/dir").unwrap();
        assert_eq!(k, BackendKind::Local);
        assert!(parse_store_url("ftp://host/x").is_err());
        assert!(parse_store_url("https://example.com/x").is_err());
    }

    #[test]
    fn s3_defaults_to_etag_conditional_put() {
        let b = s3_builder("s3://bucket/p", [("aws_region", "us-east-1")]).unwrap();
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::ConditionalPut)
                .as_deref(),
            Some("etag")
        );
        let b = s3_builder("s3://bucket/p", [("aws_conditional_put", "disabled")]).unwrap();
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::ConditionalPut)
                .as_deref(),
            Some("disabled"),
            "an explicit setting is kept"
        );
    }

    /// `object_store` 0.14 no longer has the `DynamoDB` lock (`dynamo:<table>`); a
    /// store configured with it is refused when it is opened, never opened
    /// without conditional puts.
    #[test]
    fn a_removed_conditional_put_mode_is_refused_at_open() {
        let r = ObjectStoreBackend::from_url_with_options(
            "s3://bucket/prefix",
            [
                ("aws_region", "us-east-1"),
                ("aws_access_key_id", "x"),
                ("aws_secret_access_key", "y"),
                ("aws_conditional_put", "dynamo:locks"),
            ],
        );
        assert!(r.is_err());
    }

    #[test]
    fn cloud_backends_build_offline() {
        let s3 = ObjectStoreBackend::from_url_with_options(
            "s3://bucket/prefix",
            [
                ("aws_region", "us-east-1"),
                ("aws_access_key_id", "x"),
                ("aws_secret_access_key", "y"),
            ],
        )
        .unwrap();
        assert_eq!(s3.kind(), BackendKind::S3);
        let az = ObjectStoreBackend::from_url_with_options(
            "abfss://container@acct.dfs.core.windows.net/prefix",
            [("azure_storage_account_key", "a2V5")],
        )
        .unwrap();
        assert_eq!(az.kind(), BackendKind::Azure);
        let gcs = ObjectStoreBackend::from_url_with_options(
            "gs://bucket/prefix",
            [("google_bucket", "bucket")],
        )
        .unwrap();
        assert_eq!(gcs.kind(), BackendKind::Gcs);
    }
}
