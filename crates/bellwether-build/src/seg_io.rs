//! Byte sources for opening segments with core's `SegmentReader`.
//!
//! Lookups read keyed segments by range through the pinned [`Snapshot`], so
//! a lookup holds one block index and the blocks it touches, not whole
//! segments (ADR 018). A keyed segment of at most [`WHOLE_SEGMENT_BYTES`]
//! is read in one ranged request instead, and held whole while it is open.
//! Segments that every probe must scan end to end (bloom and range layouts)
//! and transient segments are held in memory, one segment at a time for
//! bloom.

use std::borrow::Cow;
use std::cell::RefCell;

use bellwether_core::segment::ReadAt;
use bellwether_store::{Snapshot, StoreExt, SEGMENT_WINDOW_BYTES};
use bellwether_types::{Error, Hash, Result};
use bytes::Bytes;

/// Segment bytes: ranged reads of a committed segment, or bytes in memory.
pub(crate) enum SegmentBytes<'a, S: StoreExt + ?Sized> {
    /// A committed segment of the pinned manifest, read by range.
    Ranged {
        /// The snapshot that pins the segment.
        snapshot: &'a Snapshot<S>,
        /// Segment id.
        id: Hash,
        /// Its size from the manifest.
        size: u64,
        /// The window of the last read: its index and bytes. Probes walk a
        /// segment in key order, so most reads land in the window before
        /// and skip the snapshot's shared cache. One window per open
        /// reader (ADR 018).
        last: RefCell<Option<(u64, Bytes)>>,
    },
    /// Whole segment bytes: a verified read of a committed segment, a
    /// committed segment of at most [`WHOLE_SEGMENT_BYTES`] read by one
    /// range (its regions checked as ranged reads check them), or a
    /// transient segment built in memory.
    Memory(Bytes),
}

/// A committed keyed segment at most this large is read in one request when
/// a plan opens it, and held whole while it is open: one store request
/// instead of one per window, and value reads borrow its bytes instead of
/// copying them. A planner worker holds one segment open at a time, so
/// this is its bound (ADR 018).
pub(crate) const WHOLE_SEGMENT_BYTES: u64 = 1 << 20;

impl<'a, S: StoreExt + ?Sized> SegmentBytes<'a, S> {
    /// Ranged reads of segment `id` (of `size` bytes) pinned by `snapshot`.
    pub(crate) fn ranged(snapshot: &'a Snapshot<S>, id: Hash, size: u64) -> Self {
        Self::Ranged {
            snapshot,
            id,
            size,
            last: RefCell::new(None),
        }
    }

    /// Segment `id` (of `size` bytes) pinned by `snapshot`: read whole in
    /// one ranged request when it is at most [`WHOLE_SEGMENT_BYTES`], else
    /// [`Self::ranged`].
    pub(crate) fn committed(snapshot: &'a Snapshot<S>, id: Hash, size: u64) -> Result<Self> {
        if size > WHOLE_SEGMENT_BYTES {
            return Ok(Self::ranged(snapshot, id, size));
        }
        let bytes = snapshot.read_segment_range(&id, 0..size)?;
        if bytes.len() as u64 != size {
            return Err(Error::Store(format!(
                "segment {id}: asked for its {size} bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Self::Memory(bytes))
    }
}

fn corrupt(what: &str) -> Error {
    Error::Other(format!("corrupt segment: {what}"))
}

impl<S: StoreExt + ?Sized> ReadAt for SegmentBytes<'_, S> {
    fn size(&self) -> Result<u64> {
        match self {
            Self::Ranged { size, .. } => Ok(*size),
            Self::Memory(b) => Ok(b.len() as u64),
        }
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Cow<'_, [u8]>> {
        match self {
            Self::Ranged {
                snapshot,
                id,
                size,
                last,
            } => {
                let len64 = u64::try_from(len).map_err(|_| corrupt("read length"))?;
                let end = offset.checked_add(len64).ok_or_else(|| corrupt("range"))?;
                if end > *size {
                    return Err(corrupt("range past end"));
                }
                let window = offset / SEGMENT_WINDOW_BYTES;
                let start = window * SEGMENT_WINDOW_BYTES;
                if len > 0 && (end - 1) / SEGMENT_WINDOW_BYTES == window {
                    // Inside one window: from the last one read, or that
                    // window whole (cached by the snapshot).
                    let mut last = last.borrow_mut();
                    if last.as_ref().is_none_or(|(w, _)| *w != window) {
                        let wend = (start + SEGMENT_WINDOW_BYTES).min(*size);
                        let bytes = snapshot.read_segment_range(id, start..wend)?;
                        if bytes.len() as u64 != wend - start {
                            return Err(Error::Store(format!(
                                "segment {id}: asked for {} bytes at {start}, got {}",
                                wend - start,
                                bytes.len()
                            )));
                        }
                        *last = Some((window, bytes));
                    }
                    let (_, w) = last.as_ref().ok_or_else(|| corrupt("window"))?;
                    let at = usize::try_from(offset - start).map_err(|_| corrupt("offset"))?;
                    return Ok(Cow::Owned(w[at..at + len].to_vec()));
                }
                let bytes = snapshot.read_segment_range(id, offset..end)?;
                if bytes.len() != len {
                    return Err(Error::Store(format!(
                        "segment {id}: asked for {len} bytes at {offset}, got {}",
                        bytes.len()
                    )));
                }
                Ok(Cow::Owned(bytes.to_vec()))
            }
            Self::Memory(b) => {
                let start = usize::try_from(offset).map_err(|_| corrupt("offset"))?;
                let end = start.checked_add(len).ok_or_else(|| corrupt("range"))?;
                b.get(start..end)
                    .map(Cow::Borrowed)
                    .ok_or_else(|| corrupt("range past end"))
            }
        }
    }
}
