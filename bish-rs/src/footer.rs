//! Footer chunk writer and complete file writer — spec §8 and §9 (T-05).
//!
//! # What this module builds
//!
//! ```text
//! BishWriter
//!   ├── writes File Header (16B)             spec §2
//!   ├── accepts N × RowGroupMeta             from writer.rs (T-03)
//!   ├── serialises Footer Chunk A — schema   spec §8.2
//!   ├── serialises Footer Chunk B — RG meta  spec §8.3
//!   ├── serialises Footer Chunk C — col stats spec §8.4
//!   └── writes Super-Footer (512B)           spec §9
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use bish::{BishWriter, WriteOptions};
//! use bish::types::{BishSchema, BishField, BishType};
//! use bish::writer::RowGroupWriter;
//! use std::fs::File;
//!
//! let schema = BishSchema::new(vec![
//!     BishField::new("id",     BishType::Int64),
//!     BishField::new("city",   BishType::Utf8),
//!     BishField::new("amount", BishType::Float64),
//! ]);
//!
//! let file = File::create("data.bish").unwrap();
//! let mut bw = BishWriter::new(file, schema.clone()).unwrap();
//!
//! let mut rg = bw.new_row_group();
//! for i in 0..10_000i64 {
//!     rg.push_i64(0, Some(i)).unwrap();
//!     rg.push_str(1, Some("BLR")).unwrap();
//!     rg.push_f64(2, Some(i as f64 * 1.5)).unwrap();
//! }
//! bw.write_row_group(rg).unwrap();
//! bw.finish().unwrap();
//! ```

use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use xxhash_rust::xxh64::xxh64;

use crate::compress::compress;
use crate::error::{BishError, BishResult};
use crate::header::{
    ChunkRef, FeatureFlags, FileHeader, SectionRef, SuperFooter,
    BISH_MAGIC, FILE_HEADER_SIZE, SUPER_FOOTER_SIZE,
};
use crate::types::{BishSchema, BishType, Codec, ZoneValue};
use crate::writer::{ColumnChunkMeta, RowGroupMeta, RowGroupWriter, WriteOptions};

// ─────────────────────────────────────────────────────────────────────────────
// Footer chunk envelope (spec §8.1)
// ─────────────────────────────────────────────────────────────────────────────

// Magic bytes for each footer chunk — spec §8.2–8.6
pub const CHUNK_A_MAGIC: [u8; 4] = [0x42, 0x53, 0x48, 0x41]; // "BSHA"
pub const CHUNK_B_MAGIC: [u8; 4] = [0x42, 0x53, 0x48, 0x42]; // "BSHB"
pub const CHUNK_C_MAGIC: [u8; 4] = [0x42, 0x53, 0x48, 0x43]; // "BSHC"
pub const CHUNK_D_MAGIC: [u8; 4] = [0x42, 0x53, 0x48, 0x44]; // "BSHD"
pub const CHUNK_E_MAGIC: [u8; 4] = [0x42, 0x53, 0x48, 0x45]; // "BSHE"

/// Write a footer chunk envelope + payload to a byte buffer.
///
/// Layout (spec §8.1):
/// ```text
/// 0–3   chunk_magic    (4B)
/// 4–7   chunk_length   (4B u32) — payload byte length
/// 8     chunk_id       (1B)     — A=0 … E=4
/// 9     codec          (1B)     — compression applied to payload
/// 10–11 reserved       (2B)     — 0x0000
/// 12–N  payload        (var)    — compressed chunk data
/// ```
fn write_chunk_envelope(
    out: &mut Vec<u8>,
    magic: [u8; 4],
    chunk_id: u8,
    payload: &[u8],
    codec: Codec,
) -> BishResult<()> {
    let compressed = compress(payload, codec)?;
    let chunk_len = compressed.len() as u32;

    out.extend_from_slice(&magic);
    out.extend_from_slice(&chunk_len.to_le_bytes());
    out.push(chunk_id);
    out.push(codec as u8);
    out.extend_from_slice(&[0u8; 2]); // reserved
    out.extend_from_slice(&compressed);
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk A — Arrow IPC schema (spec §8.2)
// ─────────────────────────────────────────────────────────────────────────────

/// Serialise the schema into footer chunk A bytes.
///
/// Payload is a raw Arrow IPC schema message (flatbuffer).
/// Any Arrow-native tool can deserialise this without knowing about .bish.
pub fn build_chunk_a(schema: &BishSchema) -> BishResult<Vec<u8>> {
    let arrow_ipc_bytes = schema.to_arrow_ipc_bytes()?;
    let mut chunk = Vec::new();
    write_chunk_envelope(&mut chunk, CHUNK_A_MAGIC, 0, &arrow_ipc_bytes, Codec::Zstd1)?;
    Ok(chunk)
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk B — row group offsets (spec §8.3)
// ─────────────────────────────────────────────────────────────────────────────

/// Serialise all row group metadata into footer chunk B bytes.
///
/// Per row group (variable length due to col_chunk_offsets array):
/// ```text
/// rg_id           (4B u32)
/// row_count       (8B u64)
/// byte_offset     (8B u64)  — from file start
/// byte_length     (8B u64)
/// temperature     (1B u8)   — 0=hot 1=cold
/// col_chunk_count (2B u16)
/// col_chunk_offsets  (8B u64 × col_chunk_count)
/// ```
pub fn build_chunk_b(rg_metas: &[RowGroupMeta]) -> BishResult<Vec<u8>> {
    // Pre-calculate total payload size to avoid reallocations
    let payload_size: usize = rg_metas.iter()
        .map(|rg| 4 + 8 + 8 + 8 + 1 + 2 + rg.columns.len() * 8)
        .sum();

    let mut payload = Vec::with_capacity(payload_size);

    for rg in rg_metas {
        payload.extend_from_slice(&rg.rg_id.to_le_bytes());
        payload.extend_from_slice(&rg.row_count.to_le_bytes());
        payload.extend_from_slice(&rg.file_offset.to_le_bytes());
        payload.extend_from_slice(&rg.byte_length.to_le_bytes());
        payload.push(rg.temperature);
        payload.extend_from_slice(&(rg.columns.len() as u16).to_le_bytes());

        for col in &rg.columns {
            payload.extend_from_slice(&col.file_offset.to_le_bytes());
        }
    }

    let mut chunk = Vec::new();
    write_chunk_envelope(&mut chunk, CHUNK_B_MAGIC, 1, &payload, Codec::Zstd1)?;
    Ok(chunk)
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk C — column statistics (spec §8.4)
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a ZoneValue's bytes for string min/max storage.
/// Fixed 32 bytes — truncated or zero-padded. Used for lexicographic
/// comparison at read time without loading actual string data.
fn zone_bytes_prefix(bytes: &[u8]) -> [u8; 32] {
    let mut buf = [0u8; 32];
    let len = bytes.len().min(32);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

/// Serialise all column stats into footer chunk C bytes.
///
/// Fixed 62 bytes per (rg, column) entry:
/// ```text
/// rg_id            (4B u32)
/// column_index     (2B u16)
/// zone_min_i64     (8B i64)  — numeric types: value; float: IEEE bits; bytes: 0
/// zone_max_i64     (8B i64)
/// zone_min_bytes   (32B)     — for Utf8/Binary: first 32 bytes of min value
/// zone_max_bytes   (32B)     — for Utf8/Binary: first 32 bytes of max value
/// null_count       (8B u64)
/// row_count        (8B u64)
/// ```
/// Total: 4+2+8+8+32+32+8+8 = 102 bytes per entry.
pub fn build_chunk_c(rg_metas: &[RowGroupMeta]) -> BishResult<Vec<u8>> {
    let entry_count: usize = rg_metas.iter().map(|rg| rg.columns.len()).sum();
    let mut payload = Vec::with_capacity(entry_count * 102);

    for rg in rg_metas {
        for col in &rg.columns {
            // numeric i64 representation (0 for string/bytes types)
            let min_i64 = col.zone_min.to_i64_bits();
            let max_i64 = col.zone_max.to_i64_bits();

            // byte prefix for string/binary min/max (zeroed for numeric types)
            let min_bytes = match &col.zone_min {
                ZoneValue::Bytes(b) => zone_bytes_prefix(b),
                _ => [0u8; 32],
            };
            let max_bytes = match &col.zone_max {
                ZoneValue::Bytes(b) => zone_bytes_prefix(b),
                _ => [0u8; 32],
            };

            payload.extend_from_slice(&rg.rg_id.to_le_bytes());          //  4
            payload.extend_from_slice(&col.column_index.to_le_bytes());   //  2
            payload.extend_from_slice(&min_i64.to_le_bytes());            //  8
            payload.extend_from_slice(&max_i64.to_le_bytes());            //  8
            payload.extend_from_slice(&min_bytes);                        // 32
            payload.extend_from_slice(&max_bytes);                        // 32
            payload.extend_from_slice(&col.null_count.to_le_bytes());     //  8
            payload.extend_from_slice(&col.row_count.to_le_bytes());      //  8
        }
    }

    let mut chunk = Vec::new();
    write_chunk_envelope(&mut chunk, CHUNK_C_MAGIC, 2, &payload, Codec::Zstd1)?;
    Ok(chunk)
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk D stub — bloom filter offsets (spec §8.5)
// T-13 will populate this; for now we write an empty chunk so
// the super-footer chunk_d slot is still valid.
// ─────────────────────────────────────────────────────────────────────────────
fn build_chunk_d_empty() -> BishResult<Vec<u8>> {
    let mut chunk = Vec::new();
    write_chunk_envelope(&mut chunk, CHUNK_D_MAGIC, 3, &[], Codec::Plain)?;
    Ok(chunk)
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk E — user metadata (spec §8.6)
// ─────────────────────────────────────────────────────────────────────────────

/// Serialise user key-value metadata into footer chunk E bytes.
///
/// Per entry:
/// ```text
/// key_len    (2B u16)
/// key_bytes  (var UTF-8)
/// value_len  (4B u32)
/// value_bytes(var)
/// ```
pub fn build_chunk_e(metadata: &[(String, String)]) -> BishResult<Vec<u8>> {
    let mut payload = Vec::new();
    for (key, value) in metadata {
        let kb = key.as_bytes();
        let vb = value.as_bytes();
        payload.extend_from_slice(&(kb.len() as u16).to_le_bytes());
        payload.extend_from_slice(kb);
        payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
        payload.extend_from_slice(vb);
    }
    let mut chunk = Vec::new();
    write_chunk_envelope(&mut chunk, CHUNK_E_MAGIC, 4, &payload, Codec::Zstd1)?;
    Ok(chunk)
}

// ─────────────────────────────────────────────────────────────────────────────
// ChunkRef builder
// ─────────────────────────────────────────────────────────────────────────────

/// Record the position and CRC32C of a chunk after writing it.
fn make_chunk_ref(offset: u64, bytes: &[u8]) -> ChunkRef {
    ChunkRef {
        offset,
        length: bytes.len() as u32,
        checksum: crc32c::crc32c(bytes),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BishWriter — the public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Writes a complete `.bish` file.
///
/// # File writing sequence
///
/// 1. `new()` — writes the 16-byte file header immediately.
/// 2. `new_row_group()` — returns a [`RowGroupWriter`] pre-configured
///    with the schema and write options.
/// 3. `write_row_group(rg)` — calls `rg.finish()`, serialises all column
///    chunks to disk, collects [`RowGroupMeta`].
/// 4. `finish()` — writes footer chunks A/B/C/D/E then the 512B
///    super-footer. File is valid and readable after this returns.
///
/// # Thread safety
/// Not thread-safe. Use one `BishWriter` per file per thread.
pub struct BishWriter<W: Write + Seek> {
    /// Buffered writer around the underlying sink (file, Vec, etc.)
    writer: BufWriter<W>,
    /// Schema — written into chunk A on `finish()`
    schema: BishSchema,
    /// WriteOptions propagated to every RowGroupWriter
    options: WriteOptions,
    /// Current write position in bytes from file start
    file_offset: u64,
    /// Accumulated row group metadata — built as row groups are written
    rg_metas: Vec<RowGroupMeta>,
    /// User metadata to include in chunk E
    user_metadata: Vec<(String, String)>,
    /// Unix nanosecond timestamp recorded when the writer was created
    created_at: i64,
}

impl<W: Write + Seek> BishWriter<W> {
    /// Create a new writer and immediately write the 16-byte file header.
    ///
    /// Feature flags are set based on `options` — currently always sets
    /// `ADAPTIVE_CODEC`. Additional flags (bloom filters, MVCC log, etc.)
    /// will be set when those features are written (T-13 onwards).
    pub fn new(sink: W, schema: BishSchema) -> BishResult<Self> {
        Self::with_options(sink, schema, WriteOptions::default())
    }

    /// Create a writer with custom [`WriteOptions`].
    pub fn with_options(sink: W, schema: BishSchema, options: WriteOptions) -> BishResult<Self> {
        schema.validate()?;

        let mut flags = FeatureFlags::default();
        if options.adaptive_codec {
            flags.set(FeatureFlags::ADAPTIVE_CODEC);
        }
        flags.set(FeatureFlags::CHECKSUM_CRC32C); // we always write page checksums

        let header = FileHeader::new(flags);
        let header_bytes = header.to_bytes();

        let mut writer = BufWriter::new(sink);
        writer.write_all(&header_bytes)?;
        writer.flush()?;

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        Ok(Self {
            writer,
            schema,
            options,
            file_offset: FILE_HEADER_SIZE as u64,
            rg_metas: Vec::new(),
            user_metadata: Vec::new(),
            created_at,
        })
    }

    /// Attach a key-value pair to the file-level metadata (chunk E).
    /// Call before `finish()`.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.user_metadata.push((key.into(), value.into()));
        self
    }

    /// Create a new [`RowGroupWriter`] pre-configured for this file's schema.
    ///
    /// The caller pushes values into it, then passes it back to
    /// [`write_row_group`].
    pub fn new_row_group(&self) -> RowGroupWriter {
        let rg_id = self.rg_metas.len() as u32;
        RowGroupWriter::new(&self.schema, rg_id, self.options.clone())
    }

    /// Serialise a completed row group to disk and record its metadata.
    ///
    /// This is the hot path — called once per row group. All column chunks
    /// are flushed to the `BufWriter` here. The returned `RowGroupMeta`
    /// is stored and used when building footer chunks B and C.
    pub fn write_row_group(&mut self, rg: RowGroupWriter) -> BishResult<&RowGroupMeta> {
        let meta = rg.finish(&mut self.writer, &mut self.file_offset)?;
        self.rg_metas.push(meta);
        Ok(self.rg_metas.last().unwrap())
    }

    /// Finalise the file.
    ///
    /// Writes footer chunks A, B, C, D (empty), E (user metadata), then
    /// the 512-byte super-footer. After this returns the file is complete
    /// and can be opened by any `.bish` reader.
    ///
    /// # Footer write sequence
    ///
    /// ```text
    /// [already on disk: file header + all row group data]
    ///
    /// ── now writing ─────────────────────────────────────
    /// chunk A  (schema)          → file_offset advances by chunk_a.len()
    /// chunk B  (RG offsets)      → file_offset advances by chunk_b.len()
    /// chunk C  (col stats)       → file_offset advances by chunk_c.len()
    /// chunk D  (bloom, empty)    → file_offset advances by chunk_d.len()
    /// chunk E  (user metadata)   → file_offset advances by chunk_e.len()
    /// super-footer (512B)        → always the last bytes of the file
    /// ```
    pub fn finish(mut self) -> BishResult<(FinishedFile, W)> {
        // ── Build all five footer chunk payloads in memory ───────────────────

        let chunk_a_bytes = build_chunk_a(&self.schema)?;
        let chunk_b_bytes = build_chunk_b(&self.rg_metas)?;
        let chunk_c_bytes = build_chunk_c(&self.rg_metas)?;
        let chunk_d_bytes = build_chunk_d_empty()?;
        let chunk_e_bytes = build_chunk_e(&self.user_metadata)?;

        // ── Record offsets BEFORE writing (we know exact sizes now) ─────────

        let chunk_a_ref = make_chunk_ref(self.file_offset, &chunk_a_bytes);
        self.file_offset += chunk_a_bytes.len() as u64;

        let chunk_b_ref = make_chunk_ref(self.file_offset, &chunk_b_bytes);
        self.file_offset += chunk_b_bytes.len() as u64;

        let chunk_c_ref = make_chunk_ref(self.file_offset, &chunk_c_bytes);
        self.file_offset += chunk_c_bytes.len() as u64;

        let chunk_d_ref = make_chunk_ref(self.file_offset, &chunk_d_bytes);
        self.file_offset += chunk_d_bytes.len() as u64;

        let chunk_e_ref = make_chunk_ref(self.file_offset, &chunk_e_bytes);
        self.file_offset += chunk_e_bytes.len() as u64;

        // ── Write all chunks to disk ─────────────────────────────────────────

        self.writer.write_all(&chunk_a_bytes)?;
        self.writer.write_all(&chunk_b_bytes)?;
        self.writer.write_all(&chunk_c_bytes)?;
        self.writer.write_all(&chunk_d_bytes)?;
        self.writer.write_all(&chunk_e_bytes)?;

        // ── Compute schema hash (xxHash64 of raw Arrow IPC bytes) ───────────
        // Re-extract from chunk A payload: envelope header is 12 bytes,
        // then the compressed payload. For the hash we use the full chunk bytes
        // so the reader can verify chunk A integrity with just the super-footer.
        let schema_hash = xxh64(&chunk_a_bytes, 0);

        // ── Build the 512-byte super-footer ──────────────────────────────────

        let modified_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        let total_row_count: u64 = self.rg_metas.iter().map(|rg| rg.row_count).sum();

        let mut flags = FeatureFlags::default();
        if self.options.adaptive_codec {
            flags.set(FeatureFlags::ADAPTIVE_CODEC);
        }
        flags.set(FeatureFlags::CHECKSUM_CRC32C);

        let super_footer = SuperFooter {
            version_major: 1,
            version_minor: 0,
            feature_flags: flags,
            row_group_count: self.rg_metas.len() as u64,
            total_row_count,
            schema_hash,
            file_created_at: self.created_at,
            file_modified_at: modified_at,
            chunk_a: chunk_a_ref,
            chunk_b: chunk_b_ref,
            chunk_c: chunk_c_ref,
            chunk_d: chunk_d_ref,
            chunk_e: chunk_e_ref,
            partition_index: SectionRef::default(),
            delete_log:      SectionRef::default(),
            sparse_index:    SectionRef::default(),
            vector_index:    SectionRef::default(),
        };

        let sf_bytes = super_footer.to_bytes();
        assert_eq!(sf_bytes.len(), SUPER_FOOTER_SIZE);

        self.writer.write_all(&sf_bytes)?;
        self.writer.flush()?;

        // ── Return a summary of the completed file ───────────────────────────

        let total_file_size = self.file_offset + SUPER_FOOTER_SIZE as u64;

        let inner_writer = self.writer.into_inner()
            .map_err(|e| BishError::Io(e.into_error()))?;

        Ok((FinishedFile {
            total_row_count,
            row_group_count: self.rg_metas.len() as u32,
            total_file_bytes: total_file_size,
            schema_hash,
            rg_metas: self.rg_metas,
        }, inner_writer))
    }
}

/// Summary returned by [`BishWriter::finish`].
///
/// Useful for logging, testing, and building higher-level tooling
/// that needs to know file stats without re-opening the file.
#[derive(Debug)]
pub struct FinishedFile {
    /// Total rows across all row groups.
    pub total_row_count: u64,
    /// Number of row groups written.
    pub row_group_count: u32,
    /// Total file size in bytes including super-footer.
    pub total_file_bytes: u64,
    /// xxHash64 of chunk A bytes — matches the value in the super-footer.
    pub schema_hash: u64,
    /// All row group metadata — mirrors what footer chunks B and C contain.
    pub rg_metas: Vec<RowGroupMeta>,
}

impl FinishedFile {
    /// Verify the expected file size matches a constant formula.
    ///
    /// A valid .bish file must be at least:
    /// file_header(16) + super_footer(512) = 528 bytes minimum.
    pub fn looks_valid(&self) -> bool {
        self.total_file_bytes >= (FILE_HEADER_SIZE + SUPER_FOOTER_SIZE) as u64
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Footer reader — deserialise chunks B and C back to structs
// Used by tests and by the upcoming BishReader (T-06)
// ─────────────────────────────────────────────────────────────────────────────

/// Parsed row group descriptor from footer chunk B.
#[derive(Debug, Clone, PartialEq)]
pub struct RgDescriptor {
    pub rg_id: u32,
    pub row_count: u64,
    pub file_offset: u64,
    pub byte_length: u64,
    pub temperature: u8,
    /// Byte offsets of each column chunk from file start.
    pub col_chunk_offsets: Vec<u64>,
}

/// Parsed column statistics from footer chunk C.
#[derive(Debug, Clone, PartialEq)]
pub struct ColStatEntry {
    pub rg_id: u32,
    pub column_index: u16,
    pub zone_min_i64: i64,
    pub zone_max_i64: i64,
    /// First 32 bytes of string min (zeroed for numeric columns).
    pub zone_min_bytes: [u8; 32],
    /// First 32 bytes of string max (zeroed for numeric columns).
    pub zone_max_bytes: [u8; 32],
    pub null_count: u64,
    pub row_count: u64,
}

impl ColStatEntry {
    /// Does a literal i64 value fall within this column's zone map?
    /// Returns `false` (skip) if the value is definitely outside [min, max].
    pub fn int_in_range(&self, value: i64) -> bool {
        value >= self.zone_min_i64 && value <= self.zone_max_i64
    }

    /// Does a float value fall within this column's zone map?
    /// NaN comparisons return true (conservative — don't skip).
    pub fn float_in_range(&self, value: f64) -> bool {
        if value.is_nan() { return true; }
        let min = f64::from_bits(self.zone_min_i64 as u64);
        let max = f64::from_bits(self.zone_max_i64 as u64);
        value >= min && value <= max
    }

    /// Does a string prefix fall within the zone map's byte range?
    pub fn bytes_in_range(&self, value: &[u8]) -> bool {
        let v: [u8; 32] = {
            let mut b = [0u8; 32];
            let len = value.len().min(32);
            b[..len].copy_from_slice(&value[..len]);
            b
        };
        v >= self.zone_min_bytes && v <= self.zone_max_bytes
    }
}

/// Deserialise the raw payload of footer chunk B into `RgDescriptor`s.
///
/// This is called by the reader after loading and decompressing chunk B.
/// The format is the exact inverse of `build_chunk_b`.
pub fn parse_chunk_b(payload: &[u8], col_count: usize) -> BishResult<Vec<RgDescriptor>> {
    let mut pos = 0;
    let mut rgs = Vec::new();

    while pos + 31 <= payload.len() {
        let rg_id       = u32::from_le_bytes(payload[pos..pos+4].try_into().unwrap()); pos += 4;
        let row_count   = u64::from_le_bytes(payload[pos..pos+8].try_into().unwrap()); pos += 8;
        let file_offset = u64::from_le_bytes(payload[pos..pos+8].try_into().unwrap()); pos += 8;
        let byte_length = u64::from_le_bytes(payload[pos..pos+8].try_into().unwrap()); pos += 8;
        let temperature = payload[pos]; pos += 1;
        let n_cols      = u16::from_le_bytes(payload[pos..pos+2].try_into().unwrap()) as usize; pos += 2;

        if pos + n_cols * 8 > payload.len() {
            return Err(BishError::Decoding(
                "chunk B truncated in col_chunk_offsets".into()
            ));
        }

        let mut col_chunk_offsets = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            col_chunk_offsets.push(
                u64::from_le_bytes(payload[pos..pos+8].try_into().unwrap())
            );
            pos += 8;
        }

        rgs.push(RgDescriptor {
            rg_id, row_count, file_offset, byte_length, temperature, col_chunk_offsets,
        });
    }

    Ok(rgs)
}

/// Deserialise the raw payload of footer chunk C into `ColStatEntry`s.
pub fn parse_chunk_c(payload: &[u8]) -> BishResult<Vec<ColStatEntry>> {
    const ENTRY_SIZE: usize = 102; // 4+2+8+8+32+32+8+8
    if payload.len() % ENTRY_SIZE != 0 {
        return Err(BishError::Decoding(format!(
            "chunk C payload length {} is not a multiple of {}", payload.len(), ENTRY_SIZE
        )));
    }

    let count = payload.len() / ENTRY_SIZE;
    let mut entries = Vec::with_capacity(count);

    for i in 0..count {
        let b = &payload[i * ENTRY_SIZE..];
        let rg_id        = u32::from_le_bytes(b[0..4].try_into().unwrap());
        let column_index = u16::from_le_bytes(b[4..6].try_into().unwrap());
        let zone_min_i64 = i64::from_le_bytes(b[6..14].try_into().unwrap());
        let zone_max_i64 = i64::from_le_bytes(b[14..22].try_into().unwrap());
        let zone_min_bytes: [u8; 32] = b[22..54].try_into().unwrap();
        let zone_max_bytes: [u8; 32] = b[54..86].try_into().unwrap();
        let null_count   = u64::from_le_bytes(b[86..94].try_into().unwrap());
        let row_count    = u64::from_le_bytes(b[94..102].try_into().unwrap());

        entries.push(ColStatEntry {
            rg_id, column_index,
            zone_min_i64, zone_max_i64,
            zone_min_bytes, zone_max_bytes,
            null_count, row_count,
        });
    }

    Ok(entries)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────


impl BishWriter<std::io::Cursor<Vec<u8>>> {
    /// Finalise and return the raw bytes of the written .bish file.
    ///
    /// Convenience for tests and in-memory tools. The returned `Vec<u8>`
    /// is a complete, valid `.bish` file that can be opened with `BishReader`.
    pub fn finish_into_bytes(self) -> BishResult<Vec<u8>> {
        let (finished, cursor) = self.finish()?;
        assert!(finished.looks_valid());
        Ok(cursor.into_inner())
    }
}

