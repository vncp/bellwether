//! [`CountingStore`]: an object store that records every byte range the
//! store it wraps serves, so a caller measures what DataFusion read at the
//! storage boundary, not what DataFusion reports.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::object_store::path::Path;
use datafusion::object_store::{
    coalesce_ranges, CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as StoreResult,
};
use futures::stream::BoxStream;

use crate::measure::Fetch;

/// Wraps an object store and records every byte range it serves.
///
/// A ranged `get` is recorded with the range the inner store served. A
/// vectored read (`get_ranges`) is served range by range, adjacent ranges
/// merged and nothing else, so what is recorded is exactly what storage
/// served: no gap a coalescing store would read between two requested
/// ranges is hidden from the count. A `head` serves no bytes and is not
/// recorded.
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    log: Arc<Mutex<Vec<Fetch>>>,
}

impl CountingStore {
    /// Wraps `inner`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Every fetch so far, in order. Paths are the store's (no leading `/`
    /// for a local file system store).
    #[must_use]
    pub fn fetches(&self) -> Vec<Fetch> {
        self.log.lock().map(|l| l.clone()).unwrap_or_default()
    }

    /// Forgets every fetch recorded so far.
    pub fn reset(&self) {
        if let Ok(mut l) = self.log.lock() {
            l.clear();
        }
    }

    fn record(&self, location: &Path, range: Range<u64>) {
        if let Ok(mut l) = self.log.lock() {
            l.push(Fetch {
                path: format!("/{}", location.as_ref()),
                range,
            });
        }
    }

    async fn ranged(&self, location: &Path, range: Range<u64>) -> StoreResult<bytes::Bytes> {
        let options = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..GetOptions::default()
        };
        self.get_opts(location, options).await?.bytes().await
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
            self.record(location, r.range.clone());
        }
        Ok(r)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> StoreResult<Vec<bytes::Bytes>> {
        coalesce_ranges(ranges, |r| self.ranged(location, r), 0).await
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
