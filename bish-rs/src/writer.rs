//! Row group and column chunk writer — spec §4 (T-03).
//!
//! # Write flow
//!
//! ```text
//! RowGroupWriter::new(schema, options)
//!   └─ for each column:
//!        ColumnChunkWriter::push_i64 / push_str / push_bool / …
//!        (accumulates values into pages)
//!   └─ RowGroupWriter::finish(writer) → RowGroupMeta
//!        serialises all column chunks to the BufWriter
//! ```
//!
//! The caller (BishWriter, built in T-05) calls this for every row group
//! and collects the returned [`RowGroupMeta`] to build footer chunk B.

use std::io::{BufWriter, Write};

use crate::compress::compress;
use crate::encoding::{
    encode_bitpacked_bool, encode_delta_i64, encode_delta_length, encode_plain_f32,
    encode_plain_f64, encode_plain_i64, encode_plain_varlen, encode_rle_i64,
    encode_validity_bitmask,
};
use crate::error::{BishError, BishResult};
use crate::types::{BishField, BishSchema, BishType, Codec, Encoding, ZoneValue};

// ─────────────────────────────────────────────────────────────────────────────
// Writer options
// ─────────────────────────────────────────────────────────────────────────────

/// Tuning knobs passed to the writer at construction time.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Target number of rows per page before flushing.
    /// Default: 8192 rows.
    pub page_row_target: usize,

    /// Target uncompressed bytes per page before flushing.
    /// Whichever limit (rows or bytes) is hit first triggers a flush.
    /// Default: 1 MB.
    pub page_byte_target: usize,

    /// Default codec for all column chunks unless adaptive codec is enabled.
    pub default_codec: Codec,

    /// When true, the writer samples each page and picks the best codec
    /// instead of using `default_codec`.
    pub adaptive_codec: bool,

    /// Whether this row group is "hot" (recent/frequently accessed) or
    /// "cold" (archival). Cold RGs use heavier compression.
    pub is_cold: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            page_row_target: 8_192,
            page_byte_target: 1 << 20, // 1 MB
            default_codec: Codec::Zstd1,
            adaptive_codec: true,
            is_cold: false,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Page metadata (returned after serialisation)
// ─────────────────────────────────────────────────────────────────────────────

/// Metadata about one serialised page — stored in footer chunk B.
#[derive(Debug, Clone)]
pub struct PageMeta {
    /// Byte offset of this page from the start of the file.
    pub file_offset: u64,
    /// Compressed byte length (what's on disk).
    pub compressed_len: u32,
    /// Uncompressed byte length (needed to pre-allocate on read).
    pub uncompressed_len: u32,
    /// Number of rows in this page.
    pub row_count: u32,
    /// Codec used to compress this page.
    pub codec: Codec,
    /// Value encoding used before compression.
    pub encoding: Encoding,
    /// Whether this page has a validity bitmask preceding the value bytes.
    pub has_validity: bool,
}

/// Metadata about one serialised column chunk.
#[derive(Debug, Clone)]
pub struct ColumnChunkMeta {
    /// 0-based column index in the schema.
    pub column_index: u16,
    /// Byte offset of the first byte of this column chunk from file start.
    pub file_offset: u64,
    /// Total byte length of the serialised column chunk (all pages).
    pub byte_length: u64,
    /// Zone map minimum value.
    pub zone_min: ZoneValue,
    /// Zone map maximum value.
    pub zone_max: ZoneValue,
    /// Number of null values across all pages.
    pub null_count: u64,
    /// Total number of rows.
    pub row_count: u64,
    /// Per-page metadata for this column chunk.
    pub pages: Vec<PageMeta>,
}

/// Metadata about one serialised row group.
#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    /// 0-based row group index.
    pub rg_id: u32,
    /// Byte offset of the first column chunk in this row group.
    pub file_offset: u64,
    /// Total bytes of all column chunks in this row group.
    pub byte_length: u64,
    /// Number of rows in this row group.
    pub row_count: u64,
    /// 0=hot, 1=cold.
    pub temperature: u8,
    /// Per-column metadata.
    pub columns: Vec<ColumnChunkMeta>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Column chunk writer
// ─────────────────────────────────────────────────────────────────────────────

/// Accumulates values for one column and flushes them as pages.
///
/// Each `ColumnChunkWriter` corresponds to one column in the schema.
/// Values are pushed one at a time; when a page threshold is hit the
/// writer serialises that page and starts a new one.
pub struct ColumnChunkWriter {
    field: BishField,
    column_index: u16,
    options: WriteOptions,

    // Accumulation buffers — one per supported type family.
    // Only one is populated per writer instance (based on field type).
    i64_buf: Vec<i64>,
    f32_buf: Vec<f32>,
    f64_buf: Vec<f64>,
    bool_buf: Vec<bool>,
    bytes_buf: Vec<Option<Vec<u8>>>, // for Utf8 / Binary

    /// Validity buffer — None means field is non-nullable (no nulls possible).
    validity: Option<Vec<bool>>,

    /// Running min/max for zone map.
    zone_min: ZoneValue,
    zone_max: ZoneValue,

    /// Running null count across all pages.
    null_count: u64,
    /// Total rows pushed.
    row_count: u64,

    /// Serialised pages accumulated so far.
    pages: Vec<(Vec<u8>, PageMeta)>, // (bytes, meta) — meta has relative offsets
}

impl ColumnChunkWriter {
    /// Create a new writer for the given field.
    pub fn new(field: BishField, column_index: u16, options: WriteOptions) -> Self {
        let validity = if field.nullable {
            Some(Vec::new())
        } else {
            None
        };
        Self {
            field,
            column_index,
            options,
            i64_buf: Vec::new(),
            f32_buf: Vec::new(),
            f64_buf: Vec::new(),
            bool_buf: Vec::new(),
            bytes_buf: Vec::new(),
            validity,
            zone_min: ZoneValue::None,
            zone_max: ZoneValue::None,
            null_count: 0,
            row_count: 0,
            pages: Vec::new(),
        }
    }

    // ── Value push API ───────────────────────────────────────────────────────

    /// Push a nullable i64-compatible value (Int8–Int64, UInt*, timestamps, Date32).
    pub fn push_i64(&mut self, value: Option<i64>) -> BishResult<()> {
        self.record_validity(value.is_some());
        match value {
            Some(v) => {
                self.update_zone_i64(v);
                self.i64_buf.push(v);
            }
            None => {
                self.null_count += 1;
                self.i64_buf.push(0); // placeholder — masked by validity bitmask
            }
        }
        self.row_count += 1;
        self.maybe_flush_page()
    }

    /// Push a nullable f32 value.
    pub fn push_f32(&mut self, value: Option<f32>) -> BishResult<()> {
        self.record_validity(value.is_some());
        match value {
            Some(v) => {
                self.update_zone_f32(v);
                self.f32_buf.push(v);
            }
            None => {
                self.null_count += 1;
                self.f32_buf.push(0.0);
            }
        }
        self.row_count += 1;
        self.maybe_flush_page()
    }

    /// Push a nullable f64 value.
    pub fn push_f64(&mut self, value: Option<f64>) -> BishResult<()> {
        self.record_validity(value.is_some());
        match value {
            Some(v) => {
                self.update_zone_f64(v);
                self.f64_buf.push(v);
            }
            None => {
                self.null_count += 1;
                self.f64_buf.push(0.0);
            }
        }
        self.row_count += 1;
        self.maybe_flush_page()
    }

    /// Push a nullable bool value.
    pub fn push_bool(&mut self, value: Option<bool>) -> BishResult<()> {
        self.record_validity(value.is_some());
        match value {
            Some(v) => {
                self.bool_buf.push(v);
            }
            None => {
                self.null_count += 1;
                self.bool_buf.push(false);
            }
        }
        self.row_count += 1;
        self.maybe_flush_page()
    }

    /// Push a nullable byte slice (Utf8 or Binary).
    pub fn push_bytes(&mut self, value: Option<&[u8]>) -> BishResult<()> {
        self.record_validity(value.is_some());
        match value {
            Some(b) => {
                self.update_zone_bytes(b);
                self.bytes_buf.push(Some(b.to_vec()));
            }
            None => {
                self.null_count += 1;
                self.bytes_buf.push(None);
            }
        }
        self.row_count += 1;
        self.maybe_flush_page()
    }

    /// Push a UTF-8 string (convenience wrapper over push_bytes).
    pub fn push_str(&mut self, value: Option<&str>) -> BishResult<()> {
        self.push_bytes(value.map(|s| s.as_bytes()))
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    fn record_validity(&mut self, is_valid: bool) {
        if let Some(v) = &mut self.validity {
            v.push(is_valid);
        }
    }

    fn update_zone_i64(&mut self, v: i64) {
        match &mut self.zone_min {
            ZoneValue::None => {
                self.zone_min = ZoneValue::Int(v);
                self.zone_max = ZoneValue::Int(v);
            }
            ZoneValue::Int(min) => {
                if v < *min {
                    self.zone_min = ZoneValue::Int(v);
                }
                if let ZoneValue::Int(max) = &mut self.zone_max {
                    if v > *max {
                        *max = v;
                    }
                }
            }
            _ => {}
        }
    }

    fn update_zone_f32(&mut self, v: f32) {
        let v64 = v as f64;
        match &mut self.zone_min {
            ZoneValue::None => {
                self.zone_min = ZoneValue::Float64(v64);
                self.zone_max = ZoneValue::Float64(v64);
            }
            ZoneValue::Float64(min) => {
                if v64 < *min {
                    *min = v64;
                }
                if let ZoneValue::Float64(max) = &mut self.zone_max {
                    if v64 > *max {
                        *max = v64;
                    }
                }
            }
            _ => {}
        }
    }

    fn update_zone_f64(&mut self, v: f64) {
        match &mut self.zone_min {
            ZoneValue::None => {
                self.zone_min = ZoneValue::Float64(v);
                self.zone_max = ZoneValue::Float64(v);
            }
            ZoneValue::Float64(min) => {
                if v < *min {
                    *min = v;
                }
                if let ZoneValue::Float64(max) = &mut self.zone_max {
                    if v > *max {
                        *max = v;
                    }
                }
            }
            _ => {}
        }
    }

    fn update_zone_bytes(&mut self, v: &[u8]) {
        match &self.zone_min {
            ZoneValue::None => {
                self.zone_min = ZoneValue::Bytes(v.to_vec());
                self.zone_max = ZoneValue::Bytes(v.to_vec());
            }
            ZoneValue::Bytes(min) => {
                if v < min.as_slice() {
                    self.zone_min = ZoneValue::Bytes(v.to_vec());
                }
                if let ZoneValue::Bytes(max) = &self.zone_max {
                    if v > max.as_slice() {
                        self.zone_max = ZoneValue::Bytes(v.to_vec());
                    }
                }
            }
            _ => {}
        }
    }

    /// Flush a page if either the row or byte threshold is hit.
    fn maybe_flush_page(&mut self) -> BishResult<()> {
        let row_count = self.current_page_rows();
        let byte_est = self.current_page_bytes_estimate();
        if row_count >= self.options.page_row_target || byte_est >= self.options.page_byte_target {
            self.flush_page()?;
        }
        Ok(())
    }

    fn current_page_rows(&self) -> usize {
        // All buffers grow in lock-step — check whichever is populated
        self.i64_buf
            .len()
            .max(self.f32_buf.len())
            .max(self.f64_buf.len())
            .max(self.bool_buf.len())
            .max(self.bytes_buf.len())
    }

    fn current_page_bytes_estimate(&self) -> usize {
        // Rough: fixed-width uses exact byte width; variable-length uses average.
        match &self.field.data_type {
            t if t.byte_width().is_some() => self.current_page_rows() * t.byte_width().unwrap(),
            _ => {
                // Estimate: 32 bytes per value on average for strings
                self.bytes_buf
                    .iter()
                    .map(|v| v.as_ref().map_or(4, |b| 4 + b.len()))
                    .sum()
            }
        }
    }

    /// Serialise the current accumulation buffer into one page.
    /// Clears the buffer ready for the next page.
    fn flush_page(&mut self) -> BishResult<()> {
        if self.current_page_rows() == 0 {
            return Ok(());
        }

        let row_count = self.current_page_rows() as u32;

        // 1. Encode values
        let (encoded, encoding) = self.encode_current_buffer()?;

        // 2. Prepend validity bitmask if needed
        let uncompressed = if let Some(validity) = &self.validity {
            // Only write a bitmask when there are actual nulls in this page
            let has_null = validity.iter().any(|&v| !v);
            let mut buf = Vec::new();
            if has_null {
                let mask =
                    encode_validity_bitmask(&validity[validity.len() - row_count as usize..]);
                buf.extend_from_slice(&mask);
            }
            buf.extend_from_slice(&encoded);
            buf
        } else {
            encoded
        };

        let has_validity = self.validity.as_ref().map_or(false, |v| {
            let page_slice = &v[v.len() - row_count as usize..];
            page_slice.iter().any(|&x| !x)
        });

        let uncompressed_len = uncompressed.len() as u32;

        // 3. Determine codec
        let codec = if self.options.adaptive_codec {
            Codec::select_adaptive(
                self.options.is_cold,
                self.estimate_cardinality(),
                row_count as u64,
                self.is_sorted(),
            )
        } else {
            self.options.default_codec
        };

        // 4. Compress
        let compressed = compress(&uncompressed, codec)?;
        let compressed_len = compressed.len() as u32;

        // 5. Build page header (20 bytes on disk)
        //    Layout: page_length(4) | uncompressed_len(4) | row_count(4) |
        //            codec(1) | encoding(1) | page_flags(2) | checksum(4) | data(var)
        let page_flags: u16 = if has_validity { 0x0002 } else { 0x0000 };
        let checksum = crc32c::crc32c(&compressed);

        let mut page_bytes = Vec::with_capacity(20 + compressed.len());
        page_bytes.extend_from_slice(&compressed_len.to_le_bytes());
        page_bytes.extend_from_slice(&uncompressed_len.to_le_bytes());
        page_bytes.extend_from_slice(&row_count.to_le_bytes());
        page_bytes.push(codec as u8);
        page_bytes.push(encoding as u8);
        page_bytes.extend_from_slice(&page_flags.to_le_bytes());
        page_bytes.extend_from_slice(&checksum.to_le_bytes());
        page_bytes.extend_from_slice(&compressed);

        let meta = PageMeta {
            file_offset: 0, // set by ColumnChunkWriter::serialise when writing
            compressed_len,
            uncompressed_len,
            row_count,
            codec,
            encoding,
            has_validity,
        };

        self.pages.push((page_bytes, meta));
        self.clear_buffer();
        Ok(())
    }

    fn encode_current_buffer(&self) -> BishResult<(Vec<u8>, Encoding)> {
        match &self.field.data_type {
            BishType::Boolean => Ok((encode_bitpacked_bool(&self.bool_buf), Encoding::Bitpack)),
            BishType::Float32 => Ok((encode_plain_f32(&self.f32_buf), Encoding::Plain)),
            BishType::Float64 => Ok((encode_plain_f64(&self.f64_buf), Encoding::Plain)),
            BishType::Utf8 | BishType::Binary => {
                let cardinality_ratio =
                    self.estimate_cardinality() as f64 / self.bytes_buf.len().max(1) as f64;
                if cardinality_ratio < 0.1 {
                    // Low-cardinality strings — delta-length encoding
                    let non_null: Vec<&[u8]> =
                        self.bytes_buf.iter().filter_map(|v| v.as_deref()).collect();
                    Ok((encode_delta_length(&non_null), Encoding::DeltaLength))
                } else {
                    let refs: Vec<Option<&[u8]>> =
                        self.bytes_buf.iter().map(|v| v.as_deref()).collect();
                    Ok((encode_plain_varlen(&refs), Encoding::Plain))
                }
            }
            // All integer and temporal types go through i64
            _ => {
                let is_sorted = self.is_sorted();
                let cardinality = self.estimate_cardinality();
                let ratio = cardinality as f64 / self.i64_buf.len().max(1) as f64;

                if is_sorted {
                    Ok((encode_delta_i64(&self.i64_buf), Encoding::Delta))
                } else if ratio < 0.05 {
                    Ok((encode_rle_i64(&self.i64_buf), Encoding::Rle))
                } else {
                    Ok((encode_plain_i64(&self.i64_buf), Encoding::Plain))
                }
            }
        }
    }

    /// Rough cardinality estimate — count distinct values in the current buffer.
    /// Uses a HashSet sample on up to 256 values to stay fast.
    fn estimate_cardinality(&self) -> u64 {
        use std::collections::HashSet;
        let sample_size = self.i64_buf.len().min(256);
        if sample_size > 0 {
            let distinct: HashSet<i64> = self.i64_buf[..sample_size].iter().cloned().collect();
            return distinct.len() as u64;
        }
        let sample_size = self.bytes_buf.len().min(256);
        if sample_size > 0 {
            let distinct: HashSet<&[u8]> = self.bytes_buf[..sample_size]
                .iter()
                .filter_map(|v| v.as_deref())
                .collect();
            return distinct.len() as u64;
        }
        // Bools have cardinality ≤ 2
        2
    }

    /// Heuristic: a buffer is "sorted" if ≥ 95% of consecutive pairs are non-decreasing.
    fn is_sorted(&self) -> bool {
        if self.i64_buf.len() < 2 {
            return true;
        }
        let non_decreasing = self.i64_buf.windows(2).filter(|w| w[1] >= w[0]).count();
        non_decreasing * 100 / (self.i64_buf.len() - 1) >= 95
    }

    fn clear_buffer(&mut self) {
        self.i64_buf.clear();
        self.f32_buf.clear();
        self.f64_buf.clear();
        self.bool_buf.clear();
        self.bytes_buf.clear();
    }

    /// Flush any remaining buffered data and serialise all pages to the writer.
    /// Returns [`ColumnChunkMeta`] with all byte offsets filled in.
    pub fn finish<W: Write>(
        mut self,
        writer: &mut BufWriter<W>,
        current_file_offset: &mut u64,
    ) -> BishResult<ColumnChunkMeta> {
        // Flush any remaining partial page
        if self.current_page_rows() > 0 {
            self.flush_page()?;
        }

        let chunk_start = *current_file_offset;
        let mut total_bytes = 0u64;
        let mut page_metas = Vec::with_capacity(self.pages.len());

        for (page_bytes, mut meta) in self.pages {
            meta.file_offset = *current_file_offset;
            writer.write_all(&page_bytes)?;
            let len = page_bytes.len() as u64;
            *current_file_offset += len;
            total_bytes += len;
            page_metas.push(meta);
        }

        writer.flush()?;

        Ok(ColumnChunkMeta {
            column_index: self.column_index,
            file_offset: chunk_start,
            byte_length: total_bytes,
            zone_min: self.zone_min,
            zone_max: self.zone_max,
            null_count: self.null_count,
            row_count: self.row_count,
            pages: page_metas,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Row group writer
// ─────────────────────────────────────────────────────────────────────────────

/// Writes one row group — a horizontal slice of the table.
///
/// Create with [`RowGroupWriter::new`], push rows column-by-column,
/// then call [`RowGroupWriter::finish`] to serialise to disk.
///
/// # Example
/// ```rust,ignore
/// let mut rg = RowGroupWriter::new(schema.clone(), 0, WriteOptions::default());
///
/// // Push 3 rows across 2 columns
/// rg.push_i64(0, Some(1))?;   // user_id column
/// rg.push_i64(0, Some(2))?;
/// rg.push_i64(0, Some(3))?;
/// rg.push_str(1, Some("BLR"))?; // city column
/// rg.push_str(1, Some("MUM"))?;
/// rg.push_str(1, Some("BLR"))?;
///
/// let meta = rg.finish(&mut buf_writer, &mut offset)?;
/// ```
pub struct RowGroupWriter {
    rg_id: u32,
    options: WriteOptions,
    columns: Vec<ColumnChunkWriter>,
}

impl RowGroupWriter {
    /// Create a new row group writer for all columns in `schema`.
    pub fn new(schema: &BishSchema, rg_id: u32, options: WriteOptions) -> Self {
        let columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| ColumnChunkWriter::new(field.clone(), i as u16, options.clone()))
            .collect();
        Self {
            rg_id,
            options,
            columns,
        }
    }

    // ── Per-column push API ──────────────────────────────────────────────────

    pub fn push_i64(&mut self, col: usize, value: Option<i64>) -> BishResult<()> {
        self.col(col)?.push_i64(value)
    }
    pub fn push_f32(&mut self, col: usize, value: Option<f32>) -> BishResult<()> {
        self.col(col)?.push_f32(value)
    }
    pub fn push_f64(&mut self, col: usize, value: Option<f64>) -> BishResult<()> {
        self.col(col)?.push_f64(value)
    }
    pub fn push_bool(&mut self, col: usize, value: Option<bool>) -> BishResult<()> {
        self.col(col)?.push_bool(value)
    }
    pub fn push_str(&mut self, col: usize, value: Option<&str>) -> BishResult<()> {
        self.col(col)?.push_str(value)
    }
    pub fn push_bytes(&mut self, col: usize, value: Option<&[u8]>) -> BishResult<()> {
        self.col(col)?.push_bytes(value)
    }

    fn col(&mut self, index: usize) -> BishResult<&mut ColumnChunkWriter> {
        self.columns
            .get_mut(index)
            .ok_or_else(|| BishError::InvalidSchema(format!("Column index {} out of range", index)))
    }

    /// Total rows pushed to column 0 (all columns must have the same count).
    pub fn row_count(&self) -> u64 {
        self.columns.first().map_or(0, |c| c.row_count)
    }

    /// Serialise all column chunks to `writer` and return [`RowGroupMeta`].
    ///
    /// `current_file_offset` is updated in-place as bytes are written,
    /// so the caller always knows the current write position.
    pub fn finish<W: Write>(
        self,
        writer: &mut BufWriter<W>,
        current_file_offset: &mut u64,
    ) -> BishResult<RowGroupMeta> {
        let rg_start = *current_file_offset;
        let row_count = self.row_count();
        let temperature = if self.options.is_cold { 1u8 } else { 0u8 };
        let mut columns = Vec::with_capacity(self.columns.len());

        for col_writer in self.columns {
            let meta = col_writer.finish(writer, current_file_offset)?;
            columns.push(meta);
        }

        let byte_length = *current_file_offset - rg_start;

        Ok(RowGroupMeta {
            rg_id: self.rg_id,
            file_offset: rg_start,
            byte_length,
            row_count,
            temperature,
            columns,
        })
    }
}
