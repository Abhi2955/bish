//! Page compression — wraps codec implementations behind the [`Codec`] enum.
//!
//! Every page in a `.bish` file carries a 1-byte codec tag (spec §4.4).
//! The writer calls [`compress`] after encoding; the reader calls [`decompress`]
//! before decoding. Both dispatch on the same tag so they always agree.

use crate::error::{BishError, BishResult};
use crate::types::Codec;

/// Compress `input` using the given codec. Returns the compressed bytes.
/// For `Codec::Plain`, returns a copy of the input unchanged.
pub fn compress(input: &[u8], codec: Codec) -> BishResult<Vec<u8>> {
    match codec {
        Codec::Plain => Ok(input.to_vec()),
        Codec::Lz4 => {
            Ok(lz4_flex::compress_prepend_size(input))
        }
        Codec::Zstd1 => {
            zstd::encode_all(input, 1).map_err(BishError::Io)
        }
        Codec::Zstd9 => {
            zstd::encode_all(input, 9).map_err(BishError::Io)
        }
        Codec::Snappy => {
            // Snappy not linked — fall back to LZ4 at runtime.
            // Full snappy support is in the roadmap (T-03 follow-up).
            Ok(lz4_flex::compress_prepend_size(input))
        }
        Codec::Brotli => {
            // Brotli not linked — fall back to ZSTD level 6.
            zstd::encode_all(input, 6).map_err(BishError::Io)
        }
    }
}

/// Decompress `input` back to the original bytes.
/// `uncompressed_len` is stored in the page header and used to pre-allocate.
pub fn decompress(input: &[u8], codec: Codec, uncompressed_len: usize) -> BishResult<Vec<u8>> {
    match codec {
        Codec::Plain => Ok(input.to_vec()),
        Codec::Lz4 => {
            lz4_flex::decompress_size_prepended(input)
                .map_err(|e| BishError::Decoding(format!("LZ4 decompress: {e}")))
        }
        Codec::Zstd1 | Codec::Zstd9 => {
            let mut out = Vec::with_capacity(uncompressed_len);
            zstd::stream::copy_decode(input, &mut out).map_err(BishError::Io)?;
            Ok(out)
        }
        Codec::Snappy => {
            lz4_flex::decompress_size_prepended(input)
                .map_err(|e| BishError::Decoding(format!("Snappy(lz4 fallback) decompress: {e}")))
        }
        Codec::Brotli => {
            let mut out = Vec::with_capacity(uncompressed_len);
            zstd::stream::copy_decode(input, &mut out).map_err(BishError::Io)?;
            Ok(out)
        }
    }
}
