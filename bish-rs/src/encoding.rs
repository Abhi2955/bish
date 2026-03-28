//! Page-level value encoding.
//!
//! Encoding transforms raw values before compression.
//! Each encoder produces a byte buffer that the compressor then shrinks.
//!
//! Encoding is chosen per-page by [`crate::types::Encoding::select_for_type`].
//! The chosen tag is written into the page header so the reader knows
//! which decoder to call — no format sniffing needed.

use crate::error::{BishError, BishResult};
use crate::types::Encoding;

// ─────────────────────────────────────────────────────────────────────────────
// Plain encoding
// ─────────────────────────────────────────────────────────────────────────────

/// Encode fixed-width values as raw little-endian bytes.
/// No transform — what you put in is what comes out.
///
/// Used for all numeric types when no better encoding applies.
pub fn encode_plain_i8(values: &[i8]) -> Vec<u8> {
    values.iter().map(|v| *v as u8).collect()
}

pub fn encode_plain_i16(values: &[i16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 2);
    for v in values { out.extend_from_slice(&v.to_le_bytes()); }
    out
}

pub fn encode_plain_i32(values: &[i32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values { out.extend_from_slice(&v.to_le_bytes()); }
    out
}

pub fn encode_plain_i64(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for v in values { out.extend_from_slice(&v.to_le_bytes()); }
    out
}

pub fn encode_plain_f32(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values { out.extend_from_slice(&v.to_le_bytes()); }
    out
}

pub fn encode_plain_f64(values: &[f64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for v in values { out.extend_from_slice(&v.to_le_bytes()); }
    out
}

/// Encode variable-length byte slices (Utf8 / Binary).
///
/// Layout: for each value, write a 4-byte little-endian length prefix
/// followed by the raw bytes. Null values are represented by a sentinel
/// length of 0xFFFF_FFFF — only valid when the page's null bitmask
/// indicates a null at that position.
pub fn encode_plain_varlen(values: &[Option<&[u8]>]) -> Vec<u8> {
    let total_bytes: usize = values.iter()
        .map(|v| 4 + v.map_or(0, |b| b.len()))
        .sum();
    let mut out = Vec::with_capacity(total_bytes);
    for v in values {
        match v {
            Some(bytes) => {
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
            None => {
                out.extend_from_slice(&u32::MAX.to_le_bytes()); // null sentinel
            }
        }
    }
    out
}

/// Encode booleans as a packed bitmask: 8 booleans per byte, LSB first.
pub fn encode_bitpacked_bool(values: &[bool]) -> Vec<u8> {
    let byte_count = (values.len() + 7) / 8;
    let mut out = vec![0u8; byte_count];
    for (i, &v) in values.iter().enumerate() {
        if v { out[i / 8] |= 1 << (i % 8); }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// RLE encoding
// ─────────────────────────────────────────────────────────────────────────────

/// Run-length encoding for i64 values (covers all integer types via cast).
///
/// Layout per run: 8-byte value (LE i64) + 4-byte run_length (LE u32).
/// Best for low-cardinality columns (status codes, enums, boolean-like ints).
///
/// # Example
/// `[1, 1, 1, 2, 2]` → `[(1, 3), (2, 2)]` → 24 bytes instead of 40.
pub fn encode_rle_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() { return Vec::new(); }

    let mut out = Vec::new();
    let mut current = values[0];
    let mut run: u32 = 1;

    for &v in &values[1..] {
        if v == current && run < u32::MAX {
            run += 1;
        } else {
            out.extend_from_slice(&current.to_le_bytes());
            out.extend_from_slice(&run.to_le_bytes());
            current = v;
            run = 1;
        }
    }
    out.extend_from_slice(&current.to_le_bytes());
    out.extend_from_slice(&run.to_le_bytes());
    out
}

pub fn decode_rle_i64(bytes: &[u8], expected_count: usize) -> BishResult<Vec<i64>> {
    if bytes.len() % 12 != 0 {
        return Err(BishError::Decoding("RLE buffer length not a multiple of 12".into()));
    }
    let mut out = Vec::with_capacity(expected_count);
    let mut i = 0;
    while i + 12 <= bytes.len() {
        let value = i64::from_le_bytes(bytes[i..i+8].try_into().unwrap());
        let run   = u32::from_le_bytes(bytes[i+8..i+12].try_into().unwrap()) as usize;
        out.extend(std::iter::repeat(value).take(run));
        i += 12;
    }
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Delta encoding
// ─────────────────────────────────────────────────────────────────────────────

/// Delta encoding for sorted or near-sorted integer sequences.
///
/// Layout: 8-byte first value (LE i64), then N-1 deltas as variable-length
/// zigzag-encoded varints. Small deltas (typical for timestamps, sequential IDs)
/// compress extremely well after this transform.
///
/// Zigzag maps signed deltas to unsigned: 0→0, -1→1, 1→2, -2→3, 2→4 ...
/// so small negative deltas stay small.
pub fn encode_delta_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() { return Vec::new(); }

    let mut out = Vec::with_capacity(8 + values.len() * 2);
    out.extend_from_slice(&values[0].to_le_bytes());

    let mut prev = values[0];
    for &v in &values[1..] {
        let delta = v.wrapping_sub(prev);
        let zigzag = ((delta << 1) ^ (delta >> 63)) as u64;
        encode_varint(&mut out, zigzag);
        prev = v;
    }
    out
}

pub fn decode_delta_i64(bytes: &[u8], count: usize) -> BishResult<Vec<i64>> {
    if bytes.len() < 8 {
        return Err(BishError::Decoding("Delta buffer too short for first value".into()));
    }
    let mut out = Vec::with_capacity(count);
    let first = i64::from_le_bytes(bytes[0..8].try_into().unwrap());
    out.push(first);
    let mut pos = 8;
    let mut prev = first;

    while out.len() < count && pos < bytes.len() {
        let (zigzag, consumed) = decode_varint(&bytes[pos..])?;
        let delta = ((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64));
        prev = prev.wrapping_add(delta);
        out.push(prev);
        pos += consumed;
    }

    if out.len() != count {
        return Err(BishError::Decoding(format!(
            "Delta decoded {} values, expected {}", out.len(), count
        )));
    }
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Delta-length encoding (for variable-length strings/binary)
// ─────────────────────────────────────────────────────────────────────────────

/// Delta-length encoding for variable-length values.
///
/// Instead of storing a full 4-byte length per value (as plain encoding does),
/// store delta-encoded lengths as varints followed by the concatenated values.
/// Best for strings of similar length (e.g. UUIDs, fixed-format codes).
///
/// Layout:
///   - 4-byte count (LE u32)
///   - N varint-encoded length deltas (first length is absolute)
///   - Concatenated raw bytes of all values
pub fn encode_delta_length(values: &[&[u8]]) -> Vec<u8> {
    let total_data: usize = values.iter().map(|v| v.len()).sum();
    let mut out = Vec::with_capacity(4 + values.len() * 2 + total_data);
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());

    // Encode lengths as deltas
    let mut prev_len: i64 = 0;
    for v in values {
        let len = v.len() as i64;
        let delta = len - prev_len;
        let zigzag = ((delta << 1) ^ (delta >> 63)) as u64;
        encode_varint(&mut out, zigzag);
        prev_len = len;
    }

    // Concatenate raw bytes
    for v in values { out.extend_from_slice(v); }
    out
}

pub fn decode_delta_length(bytes: &[u8]) -> BishResult<Vec<Vec<u8>>> {
    if bytes.len() < 4 {
        return Err(BishError::Decoding("DeltaLength buffer too short".into()));
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut pos = 4;
    let mut lengths = Vec::with_capacity(count);
    let mut prev_len: i64 = 0;

    for _ in 0..count {
        let (zigzag, consumed) = decode_varint(&bytes[pos..])?;
        let delta = ((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64));
        prev_len += delta;
        if prev_len < 0 {
            return Err(BishError::Decoding("Negative decoded length".into()));
        }
        lengths.push(prev_len as usize);
        pos += consumed;
    }

    let mut out = Vec::with_capacity(count);
    for len in lengths {
        if pos + len > bytes.len() {
            return Err(BishError::Decoding("DeltaLength data truncated".into()));
        }
        out.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Null bitmask
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a validity bitmask: bit=1 means value is valid (not null).
/// Only written to a page when the column is nullable AND the page
/// has at least one null.
///
/// Same bit-packing layout as `encode_bitpacked_bool`.
pub fn encode_validity_bitmask(is_valid: &[bool]) -> Vec<u8> {
    encode_bitpacked_bool(is_valid)
}

pub fn decode_validity_bitmask(bytes: &[u8], count: usize) -> Vec<bool> {
    (0..count).map(|i| (bytes[i / 8] >> (i % 8)) & 1 == 1).collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Varint helpers (LEB128 unsigned)
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a u64 as a variable-length unsigned integer (LEB128).
pub fn encode_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
}

/// Decode a LEB128 varint from `bytes`. Returns `(value, bytes_consumed)`.
pub fn decode_varint(bytes: &[u8]) -> BishResult<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in bytes.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(BishError::Decoding("Varint overflow".into()));
        }
    }
    Err(BishError::Decoding("Truncated varint".into()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Dispatch — encode/decode by Encoding tag
// ─────────────────────────────────────────────────────────────────────────────

/// Encode an i64 slice using the given encoding tag.
/// All integer types are widened to i64 before calling this.
pub fn encode_i64(values: &[i64], encoding: Encoding) -> Vec<u8> {
    match encoding {
        Encoding::Plain       => encode_plain_i64(values),
        Encoding::Rle         => encode_rle_i64(values),
        Encoding::Delta       => encode_delta_i64(values),
        // Bitpack and DeltaLength don't apply to i64 — fall back to plain
        Encoding::Bitpack
        | Encoding::Dict
        | Encoding::DeltaLength => encode_plain_i64(values),
    }
}

pub fn decode_i64(bytes: &[u8], count: usize, encoding: Encoding) -> BishResult<Vec<i64>> {
    match encoding {
        Encoding::Plain => {
            if bytes.len() != count * 8 {
                return Err(BishError::Decoding(format!(
                    "Plain i64 expected {} bytes, got {}", count * 8, bytes.len()
                )));
            }
            Ok((0..count).map(|i| {
                i64::from_le_bytes(bytes[i*8..(i+1)*8].try_into().unwrap())
            }).collect())
        }
        Encoding::Rle   => decode_rle_i64(bytes, count),
        Encoding::Delta => decode_delta_i64(bytes, count),
        _ => Err(BishError::Decoding(format!("Encoding {:?} not supported for i64", encoding))),
    }
}

