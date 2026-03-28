//! File header and feature flags — spec §2.
//!
//! The file header is always the first 16 bytes of a `.bish` file.
//! The super-footer is always the last 512 bytes.

use crate::error::{BishError, BishResult};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Magic bytes at byte offset 0 and in the super-footer.
pub const BISH_MAGIC: [u8; 4] = [0x42, 0x49, 0x53, 0x48]; // "BISH"

/// Current format version supported by this library.
pub const VERSION_MAJOR: u16 = 1;
pub const VERSION_MINOR: u16 = 0;

/// Fixed size of the file header in bytes.
pub const FILE_HEADER_SIZE: usize = 16;

/// Fixed size of the super-footer in bytes — always at EOF − 512.
pub const SUPER_FOOTER_SIZE: usize = 512;

// ─────────────────────────────────────────────────────────────────────────────
// Feature flags
// ─────────────────────────────────────────────────────────────────────────────

/// Feature flags bitmask — spec §2.2.
///
/// Each flag enables an optional section or capability in the file.
/// Flags in bits 0–31 are optional: an older reader that doesn't know
/// a flag can safely ignore it.
/// Flags in bits 32–63 are required: an older reader MUST reject the
/// file if any unknown bit in this range is set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FeatureFlags(pub u64);

impl FeatureFlags {
    // ── Optional feature bits (0–31) ──────────────────────────────────────
    pub const PARTITION_INDEX:  u64 = 1 << 0;
    pub const BLOOM_FILTERS:    u64 = 1 << 1;
    pub const MVCC_DELETE_LOG:  u64 = 1 << 2;
    pub const SPARSE_ROW_INDEX: u64 = 1 << 3;
    pub const ZONE_HISTOGRAMS:  u64 = 1 << 4;
    pub const HLL_SKETCHES:     u64 = 1 << 5;
    pub const ADAPTIVE_CODEC:   u64 = 1 << 6;
    pub const VECTOR_INDEX:     u64 = 1 << 7;
    pub const CHECKSUM_CRC32C:  u64 = 1 << 8;

    // ── Required feature range ─────────────────────────────────────────────
    /// Mask for bits 32–63 — any unknown set bit here forces reader rejection.
    const REQUIRED_MASK: u64 = 0xFFFF_FFFF_0000_0000;

    /// Known required feature bits (currently none defined in v1.0).
    const KNOWN_REQUIRED: u64 = 0;

    /// Returns `true` if the given flag bit is set.
    #[inline]
    pub fn has(&self, flag: u64) -> bool {
        self.0 & flag != 0
    }

    /// Set a feature flag.
    #[inline]
    pub fn set(&mut self, flag: u64) {
        self.0 |= flag;
    }

    /// Clear a feature flag.
    #[inline]
    pub fn clear(&mut self, flag: u64) {
        self.0 &= !flag;
    }

    /// Check that no unknown required-range bits are set.
    ///
    /// Called by readers on open — returns an error if the file uses a
    /// required feature this library version doesn't support.
    pub fn check_required_features(&self) -> BishResult<()> {
        let required_bits = self.0 & Self::REQUIRED_MASK;
        let unknown_required = required_bits & !Self::KNOWN_REQUIRED;
        if unknown_required != 0 {
            return Err(BishError::UnsupportedRequiredFeature(unknown_required));
        }
        Ok(())
    }

    /// Serialise to little-endian bytes for on-disk storage.
    pub fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Deserialise from little-endian bytes.
    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FileHeader — first 16 bytes
// ─────────────────────────────────────────────────────────────────────────────

/// The fixed 16-byte file header at byte offset 0.
///
/// Layout (all little-endian):
/// ```text
/// Offset  Size  Field
/// 0       4     magic           — 0x42 0x49 0x53 0x48 ("BISH")
/// 4       2     version_major
/// 6       2     version_minor
/// 8       8     feature_flags
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub version_major: u16,
    pub version_minor: u16,
    pub feature_flags: FeatureFlags,
}

impl FileHeader {
    /// Create a v1.0 header with the given feature flags.
    pub fn new(feature_flags: FeatureFlags) -> Self {
        Self {
            version_major: VERSION_MAJOR,
            version_minor: VERSION_MINOR,
            feature_flags,
        }
    }

    /// Serialise to exactly 16 bytes.
    pub fn to_bytes(&self) -> [u8; FILE_HEADER_SIZE] {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&BISH_MAGIC);
        buf[4..6].copy_from_slice(&self.version_major.to_le_bytes());
        buf[6..8].copy_from_slice(&self.version_minor.to_le_bytes());
        buf[8..16].copy_from_slice(&self.feature_flags.to_le_bytes());
        buf
    }

    /// Deserialise from 16 bytes and validate magic + version.
    pub fn from_bytes(buf: &[u8; FILE_HEADER_SIZE]) -> BishResult<Self> {
        let magic: [u8; 4] = buf[0..4].try_into().unwrap();
        if magic != BISH_MAGIC {
            return Err(BishError::InvalidMagic(magic));
        }
        let version_major = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        let version_minor = u16::from_le_bytes(buf[6..8].try_into().unwrap());

        if version_major > VERSION_MAJOR {
            return Err(BishError::UnsupportedVersion {
                major: version_major,
                minor: version_minor,
            });
        }

        let feature_flags = FeatureFlags::from_le_bytes(buf[8..16].try_into().unwrap());
        feature_flags.check_required_features()?;

        Ok(Self {
            version_major,
            version_minor,
            feature_flags,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ChunkDirectory — pointers to each footer chunk inside the super-footer
// ─────────────────────────────────────────────────────────────────────────────

/// Location of one footer chunk within the file.
/// Five of these live inside the super-footer (chunks A–E).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChunkRef {
    /// Byte offset from the start of the file. 0 means chunk is absent.
    pub offset: u64,
    /// Byte length of the chunk (envelope + payload).
    pub length: u32,
    /// CRC32C of the chunk bytes for integrity verification.
    pub checksum: u32,
}

impl ChunkRef {
    pub fn is_present(&self) -> bool {
        self.offset != 0
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.length.to_le_bytes());
        buf[12..16].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 16]) -> Self {
        Self {
            offset: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            length: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        }
    }
}

/// Location of an optional section (partition index, delete log, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SectionRef {
    /// Byte offset from file start. 0 = section not present.
    pub offset: u64,
    /// Byte length of the section.
    pub length: u32,
}

impl SectionRef {
    pub fn is_present(&self) -> bool {
        self.offset != 0
    }

    pub fn to_bytes(&self) -> [u8; 12] {
        let mut buf = [0u8; 12];
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.length.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; 12]) -> Self {
        Self {
            offset: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            length: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SuperFooter — last 512 bytes of the file
// ─────────────────────────────────────────────────────────────────────────────

/// The 512-byte super-footer always at `EOF − 512`.
///
/// This is the first and usually only metadata a reader touches on cold open.
/// It contains everything needed to locate all other metadata and data sections.
///
/// Full byte layout — spec §9:
/// ```text
/// 0–3     magic_start       "BISH"
/// 4–5     version_major
/// 6–7     version_minor
/// 8–15    feature_flags
/// 16–23   row_group_count
/// 24–31   total_row_count
/// 32–39   schema_hash       xxHash64 of chunk A payload
/// 40–47   file_created_at   Unix nanoseconds
/// 48–55   file_modified_at  Unix nanoseconds
/// 56–71   chunk_a           ChunkRef (16 bytes)
/// 72–87   chunk_b           ChunkRef (16 bytes)
/// 88–103  chunk_c           ChunkRef (16 bytes)
/// 104–119 chunk_d           ChunkRef (16 bytes)
/// 120–135 chunk_e           ChunkRef (16 bytes)
/// 136–147 partition_index   SectionRef (12 bytes)
/// 148–159 delete_log        SectionRef (12 bytes)
/// 160–171 sparse_index      SectionRef (12 bytes)
/// 172–183 vector_index      SectionRef (12 bytes)
/// 184–499 reserved          all 0x00
/// 500–503 checksum          CRC32C of bytes 0–499
/// 504–507 magic_end         "BISH"
/// 508–511 reserved_trailer  0x00000000
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperFooter {
    pub version_major:    u16,
    pub version_minor:    u16,
    pub feature_flags:    FeatureFlags,
    pub row_group_count:  u64,
    pub total_row_count:  u64,
    /// xxHash64 of footer chunk A's payload bytes — verified after loading chunk A.
    pub schema_hash:      u64,
    pub file_created_at:  i64,
    pub file_modified_at: i64,
    /// Footer chunk A — Arrow IPC schema.
    pub chunk_a: ChunkRef,
    /// Footer chunk B — row group offsets.
    pub chunk_b: ChunkRef,
    /// Footer chunk C — column statistics.
    pub chunk_c: ChunkRef,
    /// Footer chunk D — bloom filter offsets.
    pub chunk_d: ChunkRef,
    /// Footer chunk E — user metadata.
    pub chunk_e: ChunkRef,
    /// Optional section: partition index block.
    pub partition_index: SectionRef,
    /// Optional section: MVCC delete log.
    pub delete_log: SectionRef,
    /// Optional section: sparse row index.
    pub sparse_index: SectionRef,
    /// Optional section: HNSW vector index.
    pub vector_index: SectionRef,
}

impl SuperFooter {
    /// Serialise to exactly 512 bytes, computing and embedding the CRC32C.
    pub fn to_bytes(&self) -> [u8; SUPER_FOOTER_SIZE] {
        let mut buf = [0u8; SUPER_FOOTER_SIZE];

        // 0–3: magic_start
        buf[0..4].copy_from_slice(&BISH_MAGIC);
        // 4–5: version_major
        buf[4..6].copy_from_slice(&self.version_major.to_le_bytes());
        // 6–7: version_minor
        buf[6..8].copy_from_slice(&self.version_minor.to_le_bytes());
        // 8–15: feature_flags
        buf[8..16].copy_from_slice(&self.feature_flags.to_le_bytes());
        // 16–23: row_group_count
        buf[16..24].copy_from_slice(&self.row_group_count.to_le_bytes());
        // 24–31: total_row_count
        buf[24..32].copy_from_slice(&self.total_row_count.to_le_bytes());
        // 32–39: schema_hash
        buf[32..40].copy_from_slice(&self.schema_hash.to_le_bytes());
        // 40–47: file_created_at
        buf[40..48].copy_from_slice(&self.file_created_at.to_le_bytes());
        // 48–55: file_modified_at
        buf[48..56].copy_from_slice(&self.file_modified_at.to_le_bytes());

        // 56–135: chunk directory (5 × 16 bytes)
        buf[56..72].copy_from_slice(&self.chunk_a.to_bytes());
        buf[72..88].copy_from_slice(&self.chunk_b.to_bytes());
        buf[88..104].copy_from_slice(&self.chunk_c.to_bytes());
        buf[104..120].copy_from_slice(&self.chunk_d.to_bytes());
        buf[120..136].copy_from_slice(&self.chunk_e.to_bytes());

        // 136–183: optional section refs (4 × 12 bytes)
        buf[136..148].copy_from_slice(&self.partition_index.to_bytes());
        buf[148..160].copy_from_slice(&self.delete_log.to_bytes());
        buf[160..172].copy_from_slice(&self.sparse_index.to_bytes());
        buf[172..184].copy_from_slice(&self.vector_index.to_bytes());

        // 184–499: reserved zeros (already zeroed)

        // 500–503: CRC32C of bytes 0–499
        let checksum = crc32c::crc32c(&buf[0..500]);
        buf[500..504].copy_from_slice(&checksum.to_le_bytes());

        // 504–507: magic_end
        buf[504..508].copy_from_slice(&BISH_MAGIC);

        // 508–511: reserved_trailer (already zero)

        buf
    }

    /// Deserialise from exactly 512 bytes, verifying magic and CRC32C.
    pub fn from_bytes(buf: &[u8; SUPER_FOOTER_SIZE]) -> BishResult<Self> {
        // Verify bookend magic bytes
        let magic_start: [u8; 4] = buf[0..4].try_into().unwrap();
        if magic_start != BISH_MAGIC {
            return Err(BishError::InvalidMagic(magic_start));
        }
        let magic_end: [u8; 4] = buf[504..508].try_into().unwrap();
        if magic_end != BISH_MAGIC {
            return Err(BishError::InvalidMagic(magic_end));
        }

        // Verify CRC32C before trusting any offsets
        let stored_crc = u32::from_le_bytes(buf[500..504].try_into().unwrap());
        let computed_crc = crc32c::crc32c(&buf[0..500]);
        if stored_crc != computed_crc {
            return Err(BishError::ChecksumMismatch);
        }

        let version_major   = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        let version_minor   = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let feature_flags   = FeatureFlags::from_le_bytes(buf[8..16].try_into().unwrap());
        let row_group_count = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let total_row_count = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let schema_hash     = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        let file_created_at = i64::from_le_bytes(buf[40..48].try_into().unwrap());
        let file_modified_at = i64::from_le_bytes(buf[48..56].try_into().unwrap());

        let chunk_a = ChunkRef::from_bytes(buf[56..72].try_into().unwrap());
        let chunk_b = ChunkRef::from_bytes(buf[72..88].try_into().unwrap());
        let chunk_c = ChunkRef::from_bytes(buf[88..104].try_into().unwrap());
        let chunk_d = ChunkRef::from_bytes(buf[104..120].try_into().unwrap());
        let chunk_e = ChunkRef::from_bytes(buf[120..136].try_into().unwrap());

        let partition_index = SectionRef::from_bytes(buf[136..148].try_into().unwrap());
        let delete_log      = SectionRef::from_bytes(buf[148..160].try_into().unwrap());
        let sparse_index    = SectionRef::from_bytes(buf[160..172].try_into().unwrap());
        let vector_index    = SectionRef::from_bytes(buf[172..184].try_into().unwrap());

        // Version check
        if version_major > VERSION_MAJOR {
            return Err(BishError::UnsupportedVersion { major: version_major, minor: version_minor });
        }
        feature_flags.check_required_features()?;

        Ok(Self {
            version_major,
            version_minor,
            feature_flags,
            row_group_count,
            total_row_count,
            schema_hash,
            file_created_at,
            file_modified_at,
            chunk_a,
            chunk_b,
            chunk_c,
            chunk_d,
            chunk_e,
            partition_index,
            delete_log,
            sparse_index,
            vector_index,
        })
    }
}

