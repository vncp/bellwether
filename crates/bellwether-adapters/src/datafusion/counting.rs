//! [`CountingStore`]: an object store that records every byte range the
//! store it wraps serves, so a caller measures what DataFusion read at the
//! storage boundary, not what DataFusion reports.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use datafusion::object_store::path::Path;
use datafusion::object_store::{
    coalesce_ranges, CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as StoreResult,
};
use futures::stream::BoxStream;

use crate::measure::Fetch;

/// Wraps an object store and records, apart, the byte ranges DataFusion
/// asked for ([`CountingStore::requests`]) and the byte ranges the store
/// served for them ([`CountingStore::fetches`], the storage boundary).
///
/// A ranged `get` is one request served as asked. A vectored read
/// (`get_ranges`) is served as an object store serves it: requested
/// ranges closer than the coalescing gap are merged into one fetch, which
/// also reads the bytes between them. [`CountingStore::new`] merges only
/// touching ranges (gap 0, as `object_store`'s local file system reads
/// each range by itself); [`CountingStore::with_coalescing`] takes the gap
/// of a remote store (`object_store`'s default is
/// [`datafusion::object_store::OBJECT_STORE_COALESCE_DEFAULT`], 1 MiB), so
/// the bytes a coalesced read adds are measured, never hidden. A `head`
/// serves no bytes and is not recorded.
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    coalesce: u64,
    requests: Mutex<Vec<Fetch>>,
    served: Mutex<Vec<Fetch>>,
}

fn push(log: &Mutex<Vec<Fetch>>, location: &Path, range: Range<u64>) {
    log.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(Fetch {
            path: format!("/{}", location.as_ref()),
            range,
        });
}

impl CountingStore {
    /// Wraps `inner`, merging only touching ranges of a vectored read.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self::with_coalescing(inner, 0)
    }

    /// Wraps `inner`, merging the ranges of a vectored read closer than
    /// `gap` bytes, as a remote object store does.
    #[must_use]
    pub fn with_coalescing(inner: Arc<dyn ObjectStore>, gap: u64) -> Self {
        Self {
            inner,
            coalesce: gap,
            requests: Mutex::new(Vec::new()),
            served: Mutex::new(Vec::new()),
        }
    }

    /// Every range the store served, in order: the storage boundary.
    /// Paths are the store's, with a leading `/`.
    #[must_use]
    pub fn fetches(&self) -> Vec<Fetch> {
        self.served
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Every range DataFusion asked for, in order (before coalescing).
    #[must_use]
    pub fn requests(&self) -> Vec<Fetch> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Forgets every range recorded so far.
    pub fn reset(&self) {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.served
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// One fetch of the inner store, recorded as served.
    async fn serve(&self, location: &Path, range: Range<u64>) -> StoreResult<bytes::Bytes> {
        let options = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..GetOptions::default()
        };
        let r = self.inner.get_opts(location, options).await?;
        push(&self.served, location, r.range.clone());
        r.bytes().await
    }
}

impl fmt::Debug for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let head = options.head;
        let r = self.inner.get_opts(location, options).await?;
        if !head {
            push(&self.requests, location, r.range.clone());
            push(&self.served, location, r.range.clone());
        }
        Ok(r)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> StoreResult<Vec<bytes::Bytes>> {
        for r in ranges {
            push(&self.requests, location, r.clone());
        }
        coalesce_ranges(ranges, |r| self.serve(location, r), self.coalesce).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
