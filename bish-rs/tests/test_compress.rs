use bish::compress::{compress, decompress};
use bish::types::Codec;

fn round_trip(codec: Codec, data: &[u8]) {
    let compressed = compress(data, codec).unwrap();
    let decompressed = decompress(&compressed, codec, data.len()).unwrap();
    assert_eq!(data, decompressed.as_slice(), "round-trip failed for {:?}", codec);
}

#[test]
fn test_plain_round_trip() {
    round_trip(Codec::Plain, b"hello .bish format");
}

#[test]
fn test_lz4_round_trip() {
    let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    round_trip(Codec::Lz4, &data);
}

#[test]
fn test_zstd1_round_trip() {
    let data: Vec<u8> = b"bish bish bish bish bish bish bish bish".repeat(100);
    round_trip(Codec::Zstd1, &data);
}

#[test]
fn test_zstd9_round_trip() {
    let data: Vec<u8> = b"bish bish bish bish bish bish bish bish".repeat(100);
    round_trip(Codec::Zstd9, &data);
}

#[test]
fn test_lz4_compresses_repetitive_data() {
    let data: Vec<u8> = vec![42u8; 8192];
    let compressed = compress(&data, Codec::Lz4).unwrap();
    assert!(
        compressed.len() < data.len() / 2,
        "LZ4 did not compress repetitive data: {} → {}",
        data.len(), compressed.len()
    );
}

#[test]
fn test_zstd9_better_than_zstd1() {
    let data: Vec<u8> = b"the quick brown fox jumps over the lazy dog ".repeat(500);
    let c1 = compress(&data, Codec::Zstd1).unwrap();
    let c9 = compress(&data, Codec::Zstd9).unwrap();
    assert!(c9.len() <= c1.len(), "ZSTD9 should be <= ZSTD1 size");
}

#[test]
fn test_empty_input() {
    for codec in [Codec::Plain, Codec::Lz4, Codec::Zstd1, Codec::Zstd9] {
        round_trip(codec, b"");
    }
}
