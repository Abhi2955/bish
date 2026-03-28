//! BishReader — reads a complete `.bish` file (T-06).
//!
//! # Read protocol (spec §9.1)
//!
//! ```text
//! 1. Seek to EOF − 512, read 512-byte super-footer
//! 2. Verify BISH magic bookends + CRC32C checksum
//! 3. Load chunk A (schema) → BishSchema
//! 4. Load chunk B (RG offsets) → Vec<RgDescriptor>
//! 5. Load chunk C (col stats) → Vec<ColStatEntry>
//! 6. For each row group:
//!      a. Skip if zone map eliminates all predicates
//!      b. For each projected column:
//!           seek to col_chunk_offset
//!           read pages → decode → decompress → collect values
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use bish::reader::BishReader;
//! use std::fs::File;
//!
//! let file = File::open("data.bish").unwrap();
//! let mut reader = BishReader::open(file).unwrap();
//!
//! println!("schema: {:?}", reader.schema());
//! println!("rows:   {}", reader.total_row_count());
//!
//! let batch = reader.read_all().unwrap();
//! println!("col[0] int values: {:?}", batch.col_i64(0));
//! ```

use std::io::{Read, Seek, SeekFrom};

use crate::compress::decompress;
use crate::encoding::{
    decode_delta_i64, decode_delta_length, decode_rle_i64, decode_validity_bitmask,
};
use crate::error::{BishError, BishResult};
use crate::footer::{parse_chunk_b, parse_chunk_c, ColStatEntry, RgDescriptor};
use crate::header::{SuperFooter, BISH_MAGIC, FILE_HEADER_SIZE, SUPER_FOOTER_SIZE};
use crate::types::{BishSchema, BishType, Codec, Encoding};

// ─────────────────────────────────────────────────────────────────────────────
// Column values — the decoded output of one column chunk
// ─────────────────────────────────────────────────────────────────────────────

/// All values decoded from one column in one row group.
///
/// Only one inner `Vec` is populated per instance — the one that matches
/// the column's `BishType`. All others are empty.
#[derive(Debug, Clone, Default)]
pub struct ColumnValues {
    /// Non-null i64 values (covers Int8–Int64, UInt*, timestamps, Date32).
    pub i64_values: Vec<Option<i64>>,
    /// Non-null f32 values.
    pub f32_values: Vec<Option<f32>>,
    /// Non-null f64 values.
    pub f64_values: Vec<Option<f64>>,
    /// Non-null bool values.
    pub bool_values: Vec<Option<bool>>,
    /// Non-null byte slice values (Utf8 or Binary).
    pub bytes_values: Vec<Option<Vec<u8>>>,
}

impl ColumnValues {
    pub fn row_count(&self) -> usize {
        self.i64_values
            .len()
            .max(self.f32_values.len())
            .max(self.f64_values.len())
            .max(self.bool_values.len())
            .max(self.bytes_values.len())
    }

    /// Convenience: interpret bytes_values as UTF-8 strings.
    pub fn utf8_values(&self) -> Vec<Option<String>> {
        self.bytes_values
            .iter()
            .map(|v| v.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()))
            .collect()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RecordBatch — one row group worth of decoded columns
// ─────────────────────────────────────────────────────────────────────────────

/// A decoded row group — a columnar batch of values.
///
/// Columns are indexed by their position in the projection list,
/// not the original schema index. If you project columns `[0, 2]`,
/// `batch.columns[0]` is schema column 0 and `batch.columns[1]` is
/// schema column 2.
#[derive(Debug, Default)]
pub struct RecordBatch {
    /// Number of rows in this batch.
    pub row_count: usize,
    /// Decoded column data. Index = position in the projection.
    pub columns: Vec<ColumnValues>,
    /// Original schema column indices for each position in `columns`.
    pub column_indices: Vec<usize>,
}

impl RecordBatch {
    /// Get i64 values for the column at projection position `pos`.
    pub fn col_i64(&self, pos: usize) -> &[Option<i64>] {
        &self.columns[pos].i64_values
    }

    /// Get f64 values for the column at projection position `pos`.
    pub fn col_f64(&self, pos: usize) -> &[Option<f64>] {
        &self.columns[pos].f64_values
    }

    /// Get string values for the column at projection position `pos`.
    pub fn col_str(&self, pos: usize) -> Vec<Option<String>> {
        self.columns[pos].utf8_values()
    }

    /// Get bool values for the column at projection position `pos`.
    pub fn col_bool(&self, pos: usize) -> &[Option<bool>] {
        &self.columns[pos].bool_values
    }

    /// Get raw byte values for the column at projection position `pos`.
    pub fn col_bytes(&self, pos: usize) -> &[Option<Vec<u8>>] {
        &self.columns[pos].bytes_values
    }

    /// Merge another batch into this one (used to concatenate row groups).
    pub fn extend(&mut self, other: RecordBatch) {
        self.row_count += other.row_count;
        for (col, src) in self.columns.iter_mut().zip(other.columns.into_iter()) {
            col.i64_values.extend(src.i64_values);
            col.f32_values.extend(src.f32_values);
            col.f64_values.extend(src.f64_values);
            col.bool_values.extend(src.bool_values);
            col.bytes_values.extend(src.bytes_values);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Page header (on-disk layout, spec §4.3)
// ─────────────────────────────────────────────────────────────────────────────

/// The 20-byte page header decoded from disk.
#[derive(Debug)]
struct PageHeader {
    compressed_len: u32,
    uncompressed_len: u32,
    row_count: u32,
    codec: Codec,
    encoding: Encoding,
    has_validity: bool,
    _checksum: u32,
}

impl PageHeader {
    const SIZE: usize = 20;

    fn from_bytes(buf: &[u8; Self::SIZE]) -> BishResult<Self> {
        let compressed_len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let uncompressed_len = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let row_count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let codec = Codec::from_u8(buf[12])?;
        let encoding = Encoding::from_u8(buf[13])?;
        let page_flags = u16::from_le_bytes(buf[14..16].try_into().unwrap());
        let checksum = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let has_validity = page_flags & 0x0002 != 0;

        Ok(Self {
            compressed_len,
            uncompressed_len,
            row_count,
            codec,
            encoding,
            has_validity,
            _checksum: checksum,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BishReader
// ─────────────────────────────────────────────────────────────────────────────

/// Reads a `.bish` file with projection and zone-map predicate pushdown.
pub struct BishReader<R: Read + Seek> {
    source: R,
    schema: BishSchema,
    super_footer: SuperFooter,
    rg_descriptors: Vec<RgDescriptor>,
    col_stats: Vec<ColStatEntry>,
}

impl<R: Read + Seek> BishReader<R> {
    // ── Open ────────────────────────────────────────────────────────────────

    /// Open a `.bish` source and read metadata (super-footer + chunks A/B/C).
    ///
    /// No data pages are read yet — this is O(footer size), typically a few KB.
    pub fn open(mut source: R) -> BishResult<Self> {
        // 1. Read and verify the file header (magic + version)
        let mut hdr_buf = [0u8; FILE_HEADER_SIZE];
        source.seek(SeekFrom::Start(0))?;
        source.read_exact(&mut hdr_buf)?;
        let magic: [u8; 4] = hdr_buf[0..4].try_into().unwrap();
        if magic != BISH_MAGIC {
            return Err(BishError::InvalidMagic(magic));
        }

        // 2. Seek to EOF − 512 and read super-footer
        source.seek(SeekFrom::End(-(SUPER_FOOTER_SIZE as i64)))?;
        let mut sf_buf = [0u8; SUPER_FOOTER_SIZE];
        source.read_exact(&mut sf_buf)?;
        let super_footer = SuperFooter::from_bytes(&sf_buf)?;

        // 3. Load chunk A → schema
        let schema = Self::load_chunk_a(&mut source, &super_footer)?;

        // 4. Load chunk B → row group descriptors
        let rg_descriptors = Self::load_chunk_b(&mut source, &super_footer, schema.num_columns())?;

        // 5. Load chunk C → column statistics
        let col_stats = Self::load_chunk_c(&mut source, &super_footer)?;

        Ok(Self {
            source,
            schema,
            super_footer,
            rg_descriptors,
            col_stats,
        })
    }

    // ── Metadata accessors ──────────────────────────────────────────────────

    /// The schema of this file.
    pub fn schema(&self) -> &BishSchema {
        &self.schema
    }

    /// Total rows across all row groups.
    pub fn total_row_count(&self) -> u64 {
        self.super_footer.total_row_count
    }

    /// Number of row groups.
    pub fn row_group_count(&self) -> u64 {
        self.super_footer.row_group_count
    }

    /// Column statistics for all row groups (used for predicate pushdown).
    pub fn col_stats(&self) -> &[ColStatEntry] {
        &self.col_stats
    }

    // ── Full read — no projection, no predicates ─────────────────────────────

    /// Read all rows and all columns into a single `RecordBatch`.
    ///
    /// For large files use `scan()` instead to stream row group by row group.
    pub fn read_all(&mut self) -> BishResult<RecordBatch> {
        let all_cols: Vec<usize> = (0..self.schema.num_columns()).collect();
        self.read_columns(&all_cols)
    }

    /// Read specific columns (projection pushdown).
    ///
    /// `column_indices` are 0-based positions in the schema.
    /// Only the listed columns are read from disk — others are skipped entirely.
    pub fn read_columns(&mut self, column_indices: &[usize]) -> BishResult<RecordBatch> {
        self.scan(column_indices, &[])
    }

    // ── Scan with projection + zone-map filter ───────────────────────────────

    /// Read specific columns across all row groups, skipping row groups that
    /// can't satisfy the given integer predicates via zone map.
    ///
    /// `predicates`: list of `(column_index, min_inclusive, max_inclusive)`.
    /// A row group is skipped if ANY predicate's [min,max] range has no overlap
    /// with that column's zone map.
    ///
    /// Returns a single `RecordBatch` with all surviving rows concatenated.
    pub fn scan(
        &mut self,
        column_indices: &[usize],
        predicates: &[(usize, i64, i64)], // (col_idx, min, max)
    ) -> BishResult<RecordBatch> {
        // Validate column indices
        for &ci in column_indices {
            if ci >= self.schema.num_columns() {
                return Err(BishError::ColumnNotFound(ci.to_string()));
            }
        }

        let n_cols = column_indices.len();
        let mut result = RecordBatch {
            row_count: 0,
            columns: (0..n_cols).map(|_| ColumnValues::default()).collect(),
            column_indices: column_indices.to_vec(),
        };

        // Clone to avoid borrow conflict with self.source
        let rg_descriptors = self.rg_descriptors.clone();
        let col_stats = self.col_stats.clone();
        let schema = self.schema.clone();

        for rg in &rg_descriptors {
            // Zone map predicate pushdown — skip the whole RG if any
            // predicate range has no overlap with that column's zone map.
            if !Self::rg_passes_predicates(rg, &col_stats, predicates) {
                continue;
            }

            let rg_batch = Self::read_row_group(&mut self.source, rg, column_indices, &schema)?;

            result.extend(rg_batch);
        }

        Ok(result)
    }

    // ── Internal: chunk loaders ──────────────────────────────────────────────

    fn load_chunk_a(source: &mut R, sf: &SuperFooter) -> BishResult<BishSchema> {
        if !sf.chunk_a.is_present() {
            return Err(BishError::InvalidSchema(
                "chunk A (schema) is missing".into(),
            ));
        }
        let raw = Self::load_chunk_bytes(source, sf.chunk_a.offset, sf.chunk_a.length)?;

        // Verify chunk A against the schema hash stored in the super-footer
        let computed_hash = xxhash_rust::xxh64::xxh64(&raw, 0);
        if computed_hash != sf.schema_hash {
            return Err(BishError::SchemaHashMismatch);
        }

        // The chunk envelope is 12 bytes (4 magic + 4 len + 1 id + 1 codec + 2 reserved)
        // followed by the compressed Arrow IPC payload.
        let chunk_payload_len = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as usize;
        let codec = Codec::from_u8(raw[9])?;
        let compressed = &raw[12..12 + chunk_payload_len];
        let arrow_ipc_bytes = decompress(compressed, codec, chunk_payload_len * 4)?;

        BishSchema::from_arrow_ipc_bytes(&arrow_ipc_bytes)
    }

    fn load_chunk_b(
        source: &mut R,
        sf: &SuperFooter,
        col_count: usize,
    ) -> BishResult<Vec<RgDescriptor>> {
        if !sf.chunk_b.is_present() {
            return Ok(Vec::new()); // empty file — no row groups
        }
        let raw = Self::load_chunk_bytes(source, sf.chunk_b.offset, sf.chunk_b.length)?;
        let payload = Self::decompress_chunk_payload(&raw)?;
        parse_chunk_b(&payload, col_count)
    }

    fn load_chunk_c(source: &mut R, sf: &SuperFooter) -> BishResult<Vec<ColStatEntry>> {
        if !sf.chunk_c.is_present() {
            return Ok(Vec::new());
        }
        let raw = Self::load_chunk_bytes(source, sf.chunk_c.offset, sf.chunk_c.length)?;
        let payload = Self::decompress_chunk_payload(&raw)?;
        parse_chunk_c(&payload)
    }

    /// Read `length` bytes from `offset` in the source.
    fn load_chunk_bytes(source: &mut R, offset: u64, length: u32) -> BishResult<Vec<u8>> {
        source.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; length as usize];
        source.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Strip the 12-byte chunk envelope and decompress the payload.
    fn decompress_chunk_payload(raw: &[u8]) -> BishResult<Vec<u8>> {
        if raw.len() < 12 {
            return Err(BishError::Decoding("chunk too short".into()));
        }
        let payload_len = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as usize;
        let codec = Codec::from_u8(raw[9])?;
        let compressed = &raw[12..12 + payload_len];
        decompress(compressed, codec, payload_len * 4)
    }

    // ── Internal: zone map filter ────────────────────────────────────────────

    fn rg_passes_predicates(
        rg: &RgDescriptor,
        col_stats: &[ColStatEntry],
        predicates: &[(usize, i64, i64)],
    ) -> bool {
        for &(col_idx, pred_min, pred_max) in predicates {
            if let Some(stat) = col_stats
                .iter()
                .find(|s| s.rg_id == rg.rg_id && s.column_index == col_idx as u16)
            {
                // Zone map overlap check:
                // RG can be skipped if zone_max < pred_min OR zone_min > pred_max
                if stat.zone_max_i64 < pred_min || stat.zone_min_i64 > pred_max {
                    return false; // definitely no matching rows in this RG
                }
            }
        }
        true
    }

    // ── Internal: row group reader ───────────────────────────────────────────

    fn read_row_group(
        source: &mut R,
        rg: &RgDescriptor,
        column_indices: &[usize],
        schema: &BishSchema,
    ) -> BishResult<RecordBatch> {
        let mut columns = Vec::with_capacity(column_indices.len());

        for &ci in column_indices {
            let field = &schema.fields[ci];
            let col_offset = rg.col_chunk_offsets[ci];

            let col_values = Self::read_column_chunk(
                source,
                col_offset,
                rg.row_count as usize,
                &field.data_type,
                field.nullable,
            )?;
            columns.push(col_values);
        }

        Ok(RecordBatch {
            row_count: rg.row_count as usize,
            columns,
            column_indices: column_indices.to_vec(),
        })
    }

    // ── Internal: column chunk reader ────────────────────────────────────────

    fn read_column_chunk(
        source: &mut R,
        offset: u64,
        expected_rows: usize,
        data_type: &BishType,
        nullable: bool,
    ) -> BishResult<ColumnValues> {
        source.seek(SeekFrom::Start(offset))?;
        let mut values = ColumnValues::default();
        let mut rows_read = 0usize;

        while rows_read < expected_rows {
            // Read 20-byte page header
            let mut hdr_buf = [0u8; PageHeader::SIZE];
            source.read_exact(&mut hdr_buf)?;
            let hdr = PageHeader::from_bytes(&hdr_buf)?;

            // Read compressed page data
            let mut compressed = vec![0u8; hdr.compressed_len as usize];
            source.read_exact(&mut compressed)?;

            // Verify CRC32C
            let computed = crc32c::crc32c(&compressed);
            if computed != hdr._checksum {
                return Err(BishError::ChecksumMismatch);
            }

            // Decompress
            let raw = decompress(&compressed, hdr.codec, hdr.uncompressed_len as usize)?;

            // Split validity bitmask from value bytes
            let (validity_mask, value_bytes) = if hdr.has_validity {
                let mask_len = (hdr.row_count as usize + 7) / 8;
                (&raw[..mask_len], &raw[mask_len..])
            } else {
                (&[][..], &raw[..])
            };

            let row_count = hdr.row_count as usize;

            // Decode values based on type and encoding
            Self::decode_page(
                value_bytes,
                validity_mask,
                hdr.has_validity,
                row_count,
                hdr.encoding,
                data_type,
                nullable,
                &mut values,
            )?;

            rows_read += row_count;
        }

        Ok(values)
    }

    // ── Internal: page decoder ───────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn decode_page(
        value_bytes: &[u8],
        validity_mask: &[u8],
        has_validity: bool,
        row_count: usize,
        encoding: Encoding,
        data_type: &BishType,
        nullable: bool,
        out: &mut ColumnValues,
    ) -> BishResult<()> {
        // Build validity vec: true = value is valid (not null)
        let validity: Vec<bool> = if has_validity && nullable {
            decode_validity_bitmask(validity_mask, row_count)
        } else {
            vec![true; row_count] // no mask = all valid
        };

        match data_type {
            // ── Boolean ────────────────────────────────────────────────────
            BishType::Boolean => {
                // Bitpacked: 8 bools per byte
                for i in 0..row_count {
                    if validity[i] {
                        let bit = (value_bytes[i / 8] >> (i % 8)) & 1;
                        out.bool_values.push(Some(bit != 0));
                    } else {
                        out.bool_values.push(None);
                    }
                }
            }

            // ── Float32 ────────────────────────────────────────────────────
            BishType::Float32 => {
                for i in 0..row_count {
                    if validity[i] {
                        let v =
                            f32::from_le_bytes(value_bytes[i * 4..(i + 1) * 4].try_into().unwrap());
                        out.f32_values.push(Some(v));
                    } else {
                        out.f32_values.push(None);
                    }
                }
            }

            // ── Float64 ────────────────────────────────────────────────────
            BishType::Float64 => {
                for i in 0..row_count {
                    if validity[i] {
                        let v =
                            f64::from_le_bytes(value_bytes[i * 8..(i + 1) * 8].try_into().unwrap());
                        out.f64_values.push(Some(v));
                    } else {
                        out.f64_values.push(None);
                    }
                }
            }

            // ── Utf8 / Binary ──────────────────────────────────────────────
            BishType::Utf8 | BishType::Binary => {
                match encoding {
                    Encoding::DeltaLength => {
                        // DeltaLength encodes only non-null values.
                        // Consume one decoded value per valid slot; push None for invalid.
                        let non_null_values = decode_delta_length(value_bytes)?;
                        let mut raw_iter = non_null_values.into_iter();
                        for i in 0..row_count {
                            if validity[i] {
                                match raw_iter.next() {
                                    Some(raw) => out.bytes_values.push(Some(raw)),
                                    None => {
                                        return Err(BishError::Decoding(
                                            "DeltaLength exhausted before validity mask end".into(),
                                        ))
                                    }
                                }
                            } else {
                                out.bytes_values.push(None);
                            }
                        }
                    }
                    _ => {
                        // Plain encoding: every slot has a 4-byte length prefix.
                        // Null slots use the 0xFFFF_FFFF sentinel — detected here,
                        // not from the validity mask, so validity is a secondary check.
                        let mut pos = 0;
                        for i in 0..row_count {
                            if pos + 4 > value_bytes.len() {
                                return Err(BishError::Decoding(
                                    "plain varlen truncated at length prefix".into(),
                                ));
                            }
                            let len =
                                u32::from_le_bytes(value_bytes[pos..pos + 4].try_into().unwrap())
                                    as usize;
                            pos += 4;
                            if len == u32::MAX as usize || !validity[i] {
                                // null sentinel OR validity mask says null
                                out.bytes_values.push(None);
                            } else {
                                if pos + len > value_bytes.len() {
                                    return Err(BishError::Decoding(
                                        "plain varlen truncated in value body".into(),
                                    ));
                                }
                                out.bytes_values
                                    .push(Some(value_bytes[pos..pos + len].to_vec()));
                                pos += len;
                            }
                        }
                    }
                }
            }

            // ── All integer and temporal types ─────────────────────────────
            _ => {
                let i64_vals = match encoding {
                    Encoding::Plain => {
                        let byte_width = data_type.byte_width().unwrap_or(8);
                        Self::decode_plain_integers(value_bytes, row_count, byte_width)?
                    }
                    Encoding::Rle => decode_rle_i64(value_bytes, row_count)?,
                    Encoding::Delta => decode_delta_i64(value_bytes, row_count)?,
                    other => {
                        return Err(BishError::Decoding(format!(
                            "Encoding {:?} not supported for integer types",
                            other
                        )));
                    }
                };

                for (i, v) in i64_vals.into_iter().enumerate() {
                    if validity[i] {
                        out.i64_values.push(Some(v));
                    } else {
                        out.i64_values.push(None);
                    }
                }
            }
        }

        Ok(())
    }

    /// Decode a plain-encoded integer page — handles i8/i16/i32/i64/u* widths.
    fn decode_plain_integers(
        bytes: &[u8],
        count: usize,
        byte_width: usize,
    ) -> BishResult<Vec<i64>> {
        let expected = count * byte_width;
        if bytes.len() < expected {
            return Err(BishError::Decoding(format!(
                "plain int buffer: need {} bytes, got {}",
                expected,
                bytes.len()
            )));
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let slice = &bytes[i * byte_width..(i + 1) * byte_width];
            let v = match byte_width {
                1 => slice[0] as i8 as i64,
                2 => i16::from_le_bytes(slice.try_into().unwrap()) as i64,
                4 => i32::from_le_bytes(slice.try_into().unwrap()) as i64,
                8 => i64::from_le_bytes(slice.try_into().unwrap()),
                w => {
                    return Err(BishError::Decoding(format!(
                        "unsupported integer byte width: {}",
                        w
                    )))
                }
            };
            out.push(v);
        }
        Ok(out)
    }
}

// ── Test-facing accessors (also useful for BishReader users) ─────────────────

impl<R: Read + Seek> BishReader<R> {
    /// Access the row group descriptors (from footer chunk B).
    pub fn rg_descriptors_ref(&self) -> &[crate::footer::RgDescriptor] {
        &self.rg_descriptors
    }

    /// Access the super-footer directly.
    pub fn super_footer_ref(&self) -> &crate::header::SuperFooter {
        &self.super_footer
    }
}
