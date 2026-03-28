use bish::encoding::*;
use bish::types::Encoding;

#[test]
fn test_plain_i32_round_trip() {
    let vals: Vec<i32> = vec![0, 1, -1, i32::MAX, i32::MIN, 42];
    let encoded = encode_plain_i32(&vals);
    assert_eq!(encoded.len(), vals.len() * 4);
    let decoded: Vec<i32> = (0..vals.len())
        .map(|i| i32::from_le_bytes(encoded[i*4..(i+1)*4].try_into().unwrap()))
        .collect();
    assert_eq!(vals, decoded);
}

#[test]
fn test_rle_round_trip() {
    let vals: Vec<i64> = vec![1, 1, 1, 2, 2, 3, 3, 3, 3];
    let encoded = encode_rle_i64(&vals);
    // 3 runs × 12 bytes each = 36 bytes (vs 72 plain)
    assert_eq!(encoded.len(), 36);
    let decoded = decode_rle_i64(&encoded, vals.len()).unwrap();
    assert_eq!(vals, decoded);
}

#[test]
fn test_rle_single_run() {
    let vals: Vec<i64> = vec![42; 1000];
    let encoded = encode_rle_i64(&vals);
    assert_eq!(encoded.len(), 12); // one run
    let decoded = decode_rle_i64(&encoded, 1000).unwrap();
    assert_eq!(vals, decoded);
}

#[test]
fn test_delta_round_trip_sequential() {
    // Sequential timestamps — deltas are all 1_000_000 (1ms in ns)
    let start = 1_700_000_000_000_000_000i64;
    let vals: Vec<i64> = (0..100).map(|i| start + i * 1_000_000).collect();
    let encoded = encode_delta_i64(&vals);
    // 8B first value + 99 varints at 3 bytes each = 305B max vs 800B plain
    assert!(encoded.len() < 800, "delta encoded {} bytes", encoded.len());
    let decoded = decode_delta_i64(&encoded, 100).unwrap();
    assert_eq!(vals, decoded);
}

#[test]
fn test_delta_round_trip_random() {
    let vals: Vec<i64> = vec![-100, 500, -200, 1000, 0, i64::MAX / 2];
    let encoded = encode_delta_i64(&vals);
    let decoded = decode_delta_i64(&encoded, vals.len()).unwrap();
    assert_eq!(vals, decoded);
}

#[test]
fn test_delta_single_value() {
    let vals = vec![42i64];
    let enc = encode_delta_i64(&vals);
    let dec = decode_delta_i64(&enc, 1).unwrap();
    assert_eq!(vals, dec);
}

#[test]
fn test_delta_length_round_trip() {
    let strs: Vec<&[u8]> = vec![b"hello", b"world", b"bish", b"format"];
    let encoded = encode_delta_length(&strs);
    let decoded = decode_delta_length(&encoded).unwrap();
    let decoded_refs: Vec<&[u8]> = decoded.iter().map(|v| v.as_slice()).collect();
    assert_eq!(strs, decoded_refs);
}

#[test]
fn test_delta_length_empty() {
    let strs: Vec<&[u8]> = vec![b""];
    let enc = encode_delta_length(&strs);
    let dec = decode_delta_length(&enc).unwrap();
    assert_eq!(dec, vec![b"".to_vec()]);
}

#[test]
fn test_bitpacked_bool_round_trip() {
    let vals = vec![true, false, true, true, false, false, true, false, true];
    let encoded = encode_bitpacked_bool(&vals);
    assert_eq!(encoded.len(), 2); // 9 bools → 2 bytes
    let decoded = decode_validity_bitmask(&encoded, vals.len());
    assert_eq!(vals, decoded);
}

#[test]
fn test_varlen_plain_round_trip() {
    let vals: Vec<Option<&[u8]>> = vec![
        Some(b"hello"),
        None,
        Some(b"bish"),
    ];
    let encoded = encode_plain_varlen(&vals);
    // Manual decode check
    let len0 = u32::from_le_bytes(encoded[0..4].try_into().unwrap());
    assert_eq!(len0, 5);
    let len1 = u32::from_le_bytes(encoded[9..13].try_into().unwrap());
    assert_eq!(len1, u32::MAX); // null sentinel
}

#[test]
fn test_encode_i64_dispatch() {
    let vals: Vec<i64> = vec![10, 10, 10, 20, 20];
    let rle = encode_i64(&vals, Encoding::Rle);
    let plain = encode_i64(&vals, Encoding::Plain);
    assert!(rle.len() < plain.len()); // RLE wins for repetitive data
    let decoded = decode_i64(&rle, vals.len(), Encoding::Rle).unwrap();
    assert_eq!(vals, decoded);
}

#[test]
fn test_varint_round_trips() {
    for &v in &[0u64, 1, 127, 128, 255, 16383, 16384, u64::MAX / 2] {
        let mut buf = Vec::new();
        encode_varint(&mut buf, v);
        let (decoded, _) = decode_varint(&buf).unwrap();
        assert_eq!(v, decoded, "varint round-trip failed for {}", v);
    }
}
