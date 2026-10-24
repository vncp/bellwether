//! Byte sources for opening segments with core's `SegmentReader`.
//!
//! Lookups read keyed segments by range through the pinned [`Snapshot`], so
//! a lookup holds one block index and the blocks it touches, not whole
//! segments (ADR 018). Segments that every probe must scan end to end (bloom
//! and range layouts) and transient segments are held in memory, one segment
//! at a time for bloom.

use std::borrow::Cow;

use bellwether_core::segment::ReadAt;
use bellwether_store::{Snapshot, StoreExt};
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
    },
    /// Whole segment bytes: a verified read of a committed segment, or a
    /// transient segment built in memory.
    Memory(Bytes),
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
            Self::Ranged { snapshot, id, size } => {
                let len64 = u64::try_from(len).map_err(|_| corrupt("read length"))?;
                let end = offset.checked_add(len64).ok_or_else(|| corrupt("range"))?;
                if end > *size {
                    return Err(corrupt("range past end"));
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
