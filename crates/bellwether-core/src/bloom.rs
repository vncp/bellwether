//! Split-block bloom filters per the Parquet specification (ADR 008 and its
//! amendment).
//!
//! The filter is an array of 32-byte blocks, each eight little-endian `u32`
//! words. A 64-bit hash selects a block from its upper 32 bits
//! (`((hash >> 32) * num_blocks) >> 32`) and sets one bit in each word from
//! its lower 32 bits multiplied by the eight salt constants. Sizing follows
//! the Parquet writers: `-8 * ndv / ln(1 - fpp^(1/8))` bits, rounded up to a
//! power of two between 32 bytes and 128 MiB. Structure and sizing are
//! byte-identical to the bitset Parquet embeds after its thrift header.
//!
//! The hash input is not. Bellwether's own filters hash the canonical
//! encoding from [`crate::encoding`] with xxhash64, seed 0
//! ([`CanonicalHash`]). Filters embedded in Parquet files hash Parquet's
//! PLAIN encoding of the physical value (little-endian `i32`/`i64`/floats,
//! raw bytes for byte arrays) ([`PlainHash`], built by [`parquet_plain`]).
//! The two kinds are distinct types and [`Sbbf`] is generic over its kind, so
//! a filter can only be probed with the hash it was built from. Every
//! segment records its kind as [`BloomHash`].

use std::marker::PhantomData;

use bellwether_types::{Error, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use xxhash_rust::xxh64::xxh64;

/// The eight salt constants from the Parquet specification.
pub const SALT: [u32; 8] = [
    0x47b6_137b,
    0x4497_4d91,
    0x8824_ad5b,
    0xa2b7_289d,
    0x7054_95c7,
    0x2df1_424b,
    0x9efc_4947,
    0x5c6b_fb31,
];

/// Bytes per block.
pub const BLOCK_BYTES: usize = 32;
/// Smallest bitset Parquet writes.
pub const MIN_BYTES: usize = 32;
/// Largest bitset Parquet writes.
pub const MAX_BYTES: usize = 128 * 1024 * 1024;

/// Which bytes a filter's hashes are computed from. Recorded in the segment
/// header as `bloom_hash`; an unknown value is refused on open.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BloomHash {
    /// xxhash64 over the canonical encoding (Bellwether's own filters).
    Canonical,
    /// xxhash64 over Parquet's PLAIN encoding of the physical value
    /// (reused embedded Parquet filters).
    ParquetPlain,
}

impl BloomHash {
    /// The header spelling: `"canonical"` or `"parquet_plain"`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Canonical => "canonical",
            Self::ParquetPlain => "parquet_plain",
        }
    }
}

impl Serialize for BloomHash {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for BloomHash {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.as_str() {
            "canonical" => Ok(Self::Canonical),
            "parquet_plain" => Ok(Self::ParquetPlain),
            other => Err(serde::de::Error::custom(format!(
                "unsupported bloom_hash {other:?}; this build reads \"canonical\" and \"parquet_plain\""
            ))),
        }
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::CanonicalHash {}
    impl Sealed for super::PlainHash {}
}

/// A 64-bit filter hash of a known kind. Sealed: the only kinds are
/// [`CanonicalHash`] and [`PlainHash`].
pub trait FilterHash: Copy + std::fmt::Debug + sealed::Sealed {
    /// The kind, as recorded in segment headers.
    const KIND: BloomHash;
    /// The raw 64-bit value.
    fn raw(self) -> u64;
}

/// xxhash64 (seed 0) over canonical-encoded bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CanonicalHash(u64);

impl CanonicalHash {
    /// Hashes canonical-encoded bytes.
    #[inline]
    #[must_use]
    pub fn of(canonical: &[u8]) -> Self {
        Self(xxh64(canonical, 0))
    }
}

impl FilterHash for CanonicalHash {
    const KIND: BloomHash = BloomHash::Canonical;
    fn raw(self) -> u64 {
        self.0
    }
}

/// xxhash64 (seed 0) over Parquet's PLAIN encoding of a physical value. Built
/// with the functions in [`parquet_plain`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlainHash(u64);

impl FilterHash for PlainHash {
    const KIND: BloomHash = BloomHash::ParquetPlain;
    fn raw(self) -> u64 {
        self.0
    }
}

/// Shorthand for [`CanonicalHash::of`].
#[inline]
#[must_use]
pub fn hash(canonical: &[u8]) -> CanonicalHash {
    CanonicalHash::of(canonical)
}

/// Hashes for probing filters embedded in Parquet files, which hash the PLAIN
/// encoding of the physical value.
pub mod parquet_plain {
    use super::PlainHash;
    use xxhash_rust::xxh64::xxh64;

    /// INT32 physical type (also `Int8`, `Int16`, `UInt8`..`UInt32`, `Date32`, `Time32`).
    #[must_use]
    pub fn hash_i32(v: i32) -> PlainHash {
        PlainHash(xxh64(&v.to_le_bytes(), 0))
    }
    /// INT64 physical type (also `UInt64`, `Timestamp`, `Time64`, `Date64`).
    #[must_use]
    pub fn hash_i64(v: i64) -> PlainHash {
        PlainHash(xxh64(&v.to_le_bytes(), 0))
    }
    /// FLOAT physical type.
    #[must_use]
    pub fn hash_f32(v: f32) -> PlainHash {
        PlainHash(xxh64(&v.to_le_bytes(), 0))
    }
    /// DOUBLE physical type.
    #[must_use]
    pub fn hash_f64(v: f64) -> PlainHash {
        PlainHash(xxh64(&v.to_le_bytes(), 0))
    }
    /// `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY`: the raw bytes, no length prefix.
    #[must_use]
    pub fn hash_bytes(v: &[u8]) -> PlainHash {
        PlainHash(xxh64(v, 0))
    }
}

/// Bitset size in bytes Parquet chooses for `ndv` distinct values at false
/// positive probability `fpp`.
#[must_use]
// The float casts mirror the Parquet writer's sizing exactly.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub fn num_bytes_for(ndv: u64, fpp: f64) -> usize {
    let bits = -8.0 * ndv as f64 / (1.0 - fpp.powf(1.0 / 8.0)).ln();
    optimal_num_bytes(bits as usize / 8)
}

fn optimal_num_bytes(n: usize) -> usize {
    n.clamp(MIN_BYTES, MAX_BYTES).next_power_of_two()
}

#[inline]
fn block_index(hash: u64, num_blocks: usize) -> usize {
    // The result is < num_blocks.
    #[allow(clippy::cast_possible_truncation)]
    let idx = (((hash >> 32).saturating_mul(num_blocks as u64)) >> 32) as usize;
    idx
}

#[inline]
fn mask(x: u32) -> [u32; 8] {
    let mut m = [0u32; 8];
    for (w, salt) in m.iter_mut().zip(SALT) {
        *w = 1 << (x.wrapping_mul(salt) >> 27);
    }
    m
}

/// Probes a serialized bitset without copying it. Returns `true` if `hash`
/// may be present. An empty or malformed bitset answers `true`, so a corrupt
/// filter can cost a read but never a false negative. A raw bitset carries
/// no kind, so this stays crate-private: the segment reader calls it only
/// with the kind its header's `bloom_hash` names. Callers outside the crate
/// use the typed [`Sbbf`].
#[must_use]
pub(crate) fn check_bitset<H: FilterHash>(bitset: &[u8], hash: H) -> bool {
    let hash = hash.raw();
    let num_blocks = bitset.len() / BLOCK_BYTES;
    if num_blocks == 0 || bitset.len() % BLOCK_BYTES != 0 {
        return true;
    }
    let base = block_index(hash, num_blocks) * BLOCK_BYTES;
    #[allow(clippy::cast_possible_truncation)] // low 32 bits by spec
    let m = mask(hash as u32);
    m.iter().enumerate().all(|(i, bit)| {
        let o = base + i * 4;
        let word = u32::from_le_bytes([bitset[o], bitset[o + 1], bitset[o + 2], bitset[o + 3]]);
        word & bit != 0
    })
}

/// A split-block bloom filter whose hashes are of kind `H`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sbbf<H: FilterHash = CanonicalHash> {
    blocks: Vec<[u32; 8]>,
    kind: PhantomData<H>,
}

impl<H: FilterHash> Sbbf<H> {
    /// An empty filter of `num_bytes`, rounded as Parquet does.
    #[must_use]
    pub fn with_num_bytes(num_bytes: usize) -> Self {
        Self {
            blocks: vec![[0; 8]; optimal_num_bytes(num_bytes) / BLOCK_BYTES],
            kind: PhantomData,
        }
    }

    /// An empty filter sized for `ndv` distinct values at `fpp`.
    pub fn with_ndv_fpp(ndv: u64, fpp: f64) -> Result<Self> {
        if !(fpp > 0.0 && fpp < 1.0) {
            return Err(Error::KeySpec(format!(
                "bloom fpr must be strictly between 0 and 1, got {fpp}"
            )));
        }
        Ok(Self::with_num_bytes(num_bytes_for(ndv, fpp)))
    }

    /// Reads a serialized bitset (the bytes after a Parquet bloom header)
    /// whose hashes are of kind `H`.
    pub fn from_bytes(bitset: &[u8]) -> Result<Self> {
        if bitset.is_empty() || bitset.len() % BLOCK_BYTES != 0 {
            return Err(Error::Other(format!(
                "bloom bitset length {} is not a positive multiple of {BLOCK_BYTES}",
                bitset.len()
            )));
        }
        let blocks = bitset
            .chunks_exact(BLOCK_BYTES)
            .map(|chunk| {
                let mut b = [0u32; 8];
                for (w, bytes) in b.iter_mut().zip(chunk.chunks_exact(4)) {
                    *w = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                }
                b
            })
            .collect();
        Ok(Self {
            blocks,
            kind: PhantomData,
        })
    }

    /// The hash kind.
    #[must_use]
    pub fn kind(&self) -> BloomHash {
        H::KIND
    }

    /// Bitset size in bytes.
    #[must_use]
    pub fn num_bytes(&self) -> usize {
        self.blocks.len() * BLOCK_BYTES
    }

    /// Appends the serialized bitset to `out`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.reserve(self.num_bytes());
        for b in &self.blocks {
            for w in b {
                out.extend_from_slice(&w.to_le_bytes());
            }
        }
    }

    /// The serialized bitset.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.num_bytes());
        self.write_to(&mut v);
        v
    }

    /// Inserts a hash of this filter's kind.
    pub fn insert_hash(&mut self, hash: H) {
        let hash = hash.raw();
        let i = block_index(hash, self.blocks.len());
        #[allow(clippy::cast_possible_truncation)] // low 32 bits by spec
        let m = mask(hash as u32);
        for (w, bit) in self.blocks[i].iter_mut().zip(m) {
            *w |= bit;
        }
    }

    /// Whether a hash of this filter's kind may be present.
    #[must_use]
    pub fn check_hash(&self, hash: H) -> bool {
        let hash = hash.raw();
        let i = block_index(hash, self.blocks.len());
        #[allow(clippy::cast_possible_truncation)] // low 32 bits by spec
        let m = mask(hash as u32);
        self.blocks[i].iter().zip(m).all(|(w, bit)| w & bit != 0)
    }

    /// Bitwise OR of two filters of the same kind and size.
    pub fn union_with(&mut self, other: &Self) -> Result<()> {
        if self.blocks.len() != other.blocks.len() {
            return Err(Error::Other(
                "cannot union bloom filters of different sizes".into(),
            ));
        }
        for (a, b) in self.blocks.iter_mut().zip(&other.blocks) {
            for (x, y) in a.iter_mut().zip(b) {
                *x |= y;
            }
        }
        Ok(())
    }
}

impl Sbbf<CanonicalHash> {
    /// Inserts a canonical-encoded key.
    pub fn insert(&mut self, canonical: &[u8]) {
        self.insert_hash(CanonicalHash::of(canonical));
    }

    /// Whether a canonical-encoded key may be present.
    #[must_use]
    pub fn check(&self, canonical: &[u8]) -> bool {
        self.check_hash(CanonicalHash::of(canonical))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn sizing_matches_parquet_table() {
        // Values from parquet-rs `test_num_of_bits_from_ndv_fpp`, in bits.
        assert_eq!(
            num_bytes_for(1000, 0.01),
            (9681usize / 8).next_power_of_two()
        );
        assert_eq!(num_bytes_for(0, 0.01), 32);
        assert_eq!(num_bytes_for(10, 0.1), 32);
    }

    #[test]
    fn spark_fixture_round_trips() {
        // Bitset produced by parquet-mr for strings "a0".."a9" (from parquet-rs tests).
        let bitset: &[u8] = &[
            200, 1, 80, 20, 64, 68, 8, 109, 6, 37, 4, 67, 144, 80, 96, 32, 8, 132, 43, 33, 0, 5,
            99, 65, 2, 0, 224, 44, 64, 78, 96, 4,
        ];
        let f = Sbbf::<PlainHash>::from_bytes(bitset).unwrap();
        assert_eq!(f.kind(), BloomHash::ParquetPlain);
        for i in 0..10 {
            let v = format!("a{i}");
            assert!(f.check_hash(parquet_plain::hash_bytes(v.as_bytes())));
            assert!(check_bitset(
                bitset,
                parquet_plain::hash_bytes(v.as_bytes())
            ));
        }
        assert_eq!(f.to_bytes(), bitset);
    }

    #[test]
    fn no_false_negatives() {
        let mut f = Sbbf::<CanonicalHash>::with_ndv_fpp(1000, 0.01).unwrap();
        for i in 0u32..1000 {
            f.insert(&i.to_be_bytes());
        }
        let bytes = f.to_bytes();
        for i in 0u32..1000 {
            assert!(f.check(&i.to_be_bytes()));
            assert!(check_bitset(&bytes, hash(&i.to_be_bytes())));
        }
    }

    /// The zero-copy probe answers exactly as the typed filter, members and
    /// non-members alike (and the typed filter answers exactly as the
    /// `parquet` crate: `tests/parquet_bloom_compat.rs`).
    #[test]
    fn check_bitset_agrees_with_the_typed_filter() {
        let mut f = Sbbf::<PlainHash>::with_ndv_fpp(300, 0.05).unwrap();
        for v in 0i64..300 {
            f.insert_hash(parquet_plain::hash_i64(v * 7));
        }
        let bytes = f.to_bytes();
        for v in -5000i64..5000 {
            let h = parquet_plain::hash_i64(v);
            assert_eq!(check_bitset(&bytes, h), f.check_hash(h), "probe {v}");
        }
    }

    #[test]
    fn bloom_hash_spelling_round_trips_and_unknown_is_refused() {
        for k in [BloomHash::Canonical, BloomHash::ParquetPlain] {
            let s = serde_json::to_string(&k).unwrap();
            assert_eq!(serde_json::from_str::<BloomHash>(&s).unwrap(), k);
        }
        assert_eq!(
            serde_json::to_string(&BloomHash::ParquetPlain).unwrap(),
            "\"parquet_plain\""
        );
        assert!(serde_json::from_str::<BloomHash>("\"murmur3\"").is_err());
    }
}
