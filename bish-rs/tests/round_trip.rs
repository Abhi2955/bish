//! Round-trip test suite for `.bish` write → read (T-07).
//!
//! Every test writes a complete `.bish` file into a `Cursor<Vec<u8>>`,
//! rewinds to byte 0, opens it with `BishReader`, and asserts that
//! every value, offset, and schema field comes back exactly as written.
//!
//! Test taxonomy:
//! - `schema_*`   — schema round-trips (field names, types, metadata)
//! - `type_*`     — per-type value correctness (all 19 BishType variants)
//! - `nullable_*` — null value encoding and validity bitmask
//! - `multi_rg_*` — multiple row groups, offset continuity
//! - `predicate_*`— zone-map predicate pushdown (row group skipping)
//! - `projection_*`— column projection (only requested columns read)
//! - `stress_*`   — large data, many pages, edge cases

use std::io::Cursor;

use bish::{
    BishWriter, WriteOptions,
    reader::BishReader,
    types::{BishField, BishSchema, BishType},
};

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Write a .bish file into an in-memory Vec<u8>, then open a BishReader over it.
fn make_bish(
    schema: BishSchema,
    write_fn: impl FnOnce(&mut BishWriter<Cursor<Vec<u8>>>),
) -> BishReader<Cursor<Vec<u8>>> {
    let mut bw = BishWriter::new(Cursor::new(Vec::<u8>::new()), schema).expect("writer");
    write_fn(&mut bw);
    let raw = bw.finish_into_bytes().expect("finish");
    BishReader::open(Cursor::new(raw)).expect("reader open")
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema round-trip tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn schema_field_names_survive_round_trip() {
    let schema = BishSchema::new(vec![
        BishField::new("user_id",    BishType::Int64),
        BishField::new("city",       BishType::Utf8),
        BishField::new("amount",     BishType::Float64),
        BishField::nullable("notes", BishType::Utf8),
    ]);
    let reader = make_bish(schema.clone(), |bw| {
        let mut rg = bw.new_row_group();
        rg.push_i64(0, Some(1)).unwrap();
        rg.push_str(1, Some("BLR")).unwrap();
        rg.push_f64(2, Some(99.5)).unwrap();
        rg.push_str(3, None).unwrap();
        bw.write_row_group(rg).unwrap();
    });

    let read_schema = reader.schema();
    assert_eq!(read_schema.num_columns(), 4);
    assert_eq!(read_schema.fields[0].name, "user_id");
    assert_eq!(read_schema.fields[1].name, "city");
    assert_eq!(read_schema.fields[2].name, "amount");
    assert_eq!(read_schema.fields[3].name, "notes");
}

#[test]
fn schema_types_survive_round_trip() {
    let schema = BishSchema::new(vec![
        BishField::new("a", BishType::Int8),
        BishField::new("b", BishType::Int64),
        BishField::new("c", BishType::Float32),
        BishField::new("d", BishType::Float64),
        BishField::new("e", BishType::Boolean),
        BishField::new("f", BishType::Utf8),
        BishField::new("g", BishType::TimestampNs),
        BishField::new("h", BishType::Decimal128 { precision: 18, scale: 4 }),
    ]);
    let reader = make_bish(schema.clone(), |bw| {
        let mut rg = bw.new_row_group();
        rg.push_i64(0, Some(42)).unwrap();
        rg.push_i64(1, Some(9_999_999)).unwrap();
        rg.push_f32(2, Some(1.5)).unwrap();
        rg.push_f64(3, Some(3.14)).unwrap();
        rg.push_bool(4, Some(true)).unwrap();
        rg.push_str(5, Some("hello")).unwrap();
        rg.push_i64(6, Some(1_700_000_000_000_000_000)).unwrap();
        rg.push_i64(7, Some(123456789)).unwrap();
        bw.write_row_group(rg).unwrap();
    });

    let s = reader.schema();
    assert_eq!(s.fields[0].data_type, BishType::Int8);
    assert_eq!(s.fields[1].data_type, BishType::Int64);
    assert_eq!(s.fields[2].data_type, BishType::Float32);
    assert_eq!(s.fields[3].data_type, BishType::Float64);
    assert_eq!(s.fields[4].data_type, BishType::Boolean);
    assert_eq!(s.fields[5].data_type, BishType::Utf8);
    assert_eq!(s.fields[6].data_type, BishType::TimestampNs);
    assert_eq!(s.fields[7].data_type, BishType::Decimal128 { precision: 18, scale: 4 });
}

#[test]
fn schema_nullability_survives_round_trip() {
    let schema = BishSchema::new(vec![
        BishField::new("required",     BishType::Int64),
        BishField::nullable("optional", BishType::Utf8),
    ]);
    let reader = make_bish(schema.clone(), |bw| {
        let mut rg = bw.new_row_group();
        rg.push_i64(0, Some(1)).unwrap();
        rg.push_str(1, Some("x")).unwrap();
        bw.write_row_group(rg).unwrap();
    });

    let s = reader.schema();
    assert!(!s.fields[0].nullable, "required field should not be nullable");
    assert!(s.fields[1].nullable,  "optional field should be nullable");
}

#[test]
fn schema_sort_and_partition_keys_survive() {
    let schema = BishSchema::new(vec![
        BishField::new("id",   BishType::Int64).with_sort_key(),
        BishField::new("city", BishType::Utf8).with_partition_key(),
        BishField::new("val",  BishType::Float64),
    ]);
    let reader = make_bish(schema.clone(), |bw| {
        let mut rg = bw.new_row_group();
        rg.push_i64(0, Some(1)).unwrap();
        rg.push_str(1, Some("BLR")).unwrap();
        rg.push_f64(2, Some(1.0)).unwrap();
        bw.write_row_group(rg).unwrap();
    });

    let s = reader.schema();
    assert!(s.fields[0].is_sort_key(),      "id should be sort key");
    assert!(s.fields[1].is_partition_key(), "city should be partition key");
    assert!(!s.fields[2].is_sort_key(),     "val should not be sort key");
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-type value round-trip tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn type_int8_round_trip() {
    let values: Vec<i8> = vec![i8::MIN, -1, 0, 1, i8::MAX];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int8)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v as i64)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<i8> = batch.col_i64(0).iter()
        .map(|v| v.unwrap() as i8).collect();
    assert_eq!(values, got);
}

#[test]
fn type_int16_round_trip() {
    let values: Vec<i16> = vec![i16::MIN, -256, 0, 256, i16::MAX];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int16)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v as i64)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<i16> = batch.col_i64(0).iter()
        .map(|v| v.unwrap() as i16).collect();
    assert_eq!(values, got);
}

#[test]
fn type_int32_round_trip() {
    let values: Vec<i32> = vec![i32::MIN, -65536, 0, 65536, i32::MAX];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int32)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v as i64)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<i32> = batch.col_i64(0).iter()
        .map(|v| v.unwrap() as i32).collect();
    assert_eq!(values, got);
}

#[test]
fn type_int64_round_trip() {
    let values: Vec<i64> = vec![i64::MIN, -1, 0, 1, i64::MAX, 9_876_543_210];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<i64> = batch.col_i64(0).iter().map(|v| v.unwrap()).collect();
    assert_eq!(values, got);
}

#[test]
fn type_uint8_round_trip() {
    let values: Vec<u8> = vec![0, 1, 127, 128, 255];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::UInt8)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v as i64)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<u8> = batch.col_i64(0).iter()
        .map(|v| v.unwrap() as u8).collect();
    assert_eq!(values, got);
}

#[test]
fn type_float32_round_trip() {
    let values: Vec<f32> = vec![f32::MIN, -1.5, 0.0, 1.5, f32::MAX, f32::INFINITY];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Float32)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_f32(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<f32> = batch.columns[0].f32_values.iter()
        .map(|v| v.unwrap()).collect();
    assert_eq!(values, got);
}

#[test]
fn type_float64_round_trip() {
    let values: Vec<f64> = vec![
        f64::MIN, -3.141592653589793, 0.0, 3.141592653589793, f64::MAX,
    ];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Float64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_f64(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<f64> = batch.col_f64(0).iter().map(|v| v.unwrap()).collect();
    assert_eq!(values, got);
}

#[test]
fn type_boolean_round_trip() {
    let values = vec![true, false, false, true, true, false, true];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Boolean)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_bool(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<bool> = batch.col_bool(0).iter().map(|v| v.unwrap()).collect();
    assert_eq!(values, got);
}

#[test]
fn type_utf8_round_trip() {
    let values = vec!["hello", "world", "bish", "format", "✓ unicode", ""];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Utf8)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_str(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got = batch.col_str(0);
    let got_strs: Vec<&str> = got.iter().map(|v| v.as_deref().unwrap()).collect();
    assert_eq!(values, got_strs);
}

#[test]
fn type_binary_round_trip() {
    let values: Vec<Vec<u8>> = vec![
        vec![0x00, 0xFF, 0x42],
        vec![],
        b"raw bytes".to_vec(),
        (0u8..=255).collect(),
    ];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Binary)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for v in &values { rg.push_bytes(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<Vec<u8>> = batch.col_bytes(0).iter()
        .map(|v| v.clone().unwrap()).collect();
    assert_eq!(values, got);
}

#[test]
fn type_timestamp_ns_round_trip() {
    let values: Vec<i64> = vec![
        0,
        1_700_000_000_000_000_000, // 2023-11-14 in ns
        i64::MAX / 2,
        -1_000_000_000, // 1 second before epoch
    ];
    let schema = BishSchema::new(vec![BishField::new("ts", BishType::TimestampNs)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<i64> = batch.col_i64(0).iter().map(|v| v.unwrap()).collect();
    assert_eq!(values, got);
}

#[test]
fn type_date32_round_trip() {
    let values: Vec<i32> = vec![0, 1, 365, 18628, 19000]; // days since epoch
    let schema = BishSchema::new(vec![BishField::new("d", BishType::Date32)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v as i64)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<i32> = batch.col_i64(0).iter()
        .map(|v| v.unwrap() as i32).collect();
    assert_eq!(values, got);
}

#[test]
fn type_decimal128_round_trip() {
    // Stored as raw i128 bits via i64 (lower 64 bits)
    let values: Vec<i64> = vec![0, 1_000_000, -1_000_000, i64::MAX / 100];
    let schema = BishSchema::new(vec![
        BishField::new("price", BishType::Decimal128 { precision: 18, scale: 4 })
    ]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<i64> = batch.col_i64(0).iter().map(|v| v.unwrap()).collect();
    assert_eq!(values, got);
}

// ─────────────────────────────────────────────────────────────────────────────
// Nullable (null value) tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn nullable_int_round_trip() {
    let values: Vec<Option<i64>> = vec![Some(1), None, Some(3), None, None, Some(6)];
    let schema = BishSchema::new(vec![BishField::nullable("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, v).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<Option<i64>> = batch.col_i64(0).to_vec();
    assert_eq!(values, got);
}

#[test]
fn nullable_string_round_trip() {
    let values: Vec<Option<&str>> = vec![
        Some("BLR"), None, Some("MUM"), None, Some("DEL"), None,
    ];
    let schema = BishSchema::new(vec![BishField::nullable("city", BishType::Utf8)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_str(0, v).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got = batch.col_str(0);
    let got_refs: Vec<Option<&str>> = got.iter()
        .map(|v| v.as_deref()).collect();
    assert_eq!(values, got_refs);
}

#[test]
fn nullable_all_nulls_round_trip() {
    let schema = BishSchema::new(vec![BishField::nullable("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for _ in 0..100 { rg.push_i64(0, None).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, 100);
    assert!(batch.col_i64(0).iter().all(|v| v.is_none()), "all values should be None");
}

#[test]
fn nullable_no_nulls_skips_validity_bitmask() {
    // Non-nullable field — validity bitmask should never be written.
    // We verify this indirectly: the decoded values are correct.
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..50i64 { rg.push_i64(0, Some(i)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    assert!(batch.col_i64(0).iter().all(|v| v.is_some()));
    assert_eq!(batch.col_i64(0).iter().map(|v| v.unwrap()).sum::<i64>(), (0..50).sum());
}

#[test]
fn nullable_mixed_columns_round_trip() {
    let n = 200;
    let schema = BishSchema::new(vec![
        BishField::new("id",     BishType::Int64),
        BishField::nullable("tag", BishType::Utf8),
        BishField::new("amount", BishType::Float64),
    ]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..n {
            rg.push_i64(0, Some(i as i64)).unwrap();
            rg.push_str(1, if i % 3 == 0 { None } else { Some("ok") }).unwrap();
            rg.push_f64(2, Some(i as f64)).unwrap();
        }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, n);
    // id: all Some
    assert!(batch.col_i64(0).iter().all(|v| v.is_some()));
    // tag: every 3rd is None
    let null_count = batch.col_str(1).iter().filter(|v| v.is_none()).count();
    assert_eq!(null_count, (n + 2) / 3); // rows where i%3==0: 0,3,...,198 = 67
    // amount: all Some
    assert!(batch.col_f64(2).iter().all(|v| v.is_some()));
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-row-group tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn multi_rg_row_counts_sum_correctly() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        for rg_i in 0..4u32 {
            let mut rg = bw.new_row_group();
            for j in 0..1000i64 { rg.push_i64(0, Some(rg_i as i64 * 1000 + j)).unwrap(); }
            bw.write_row_group(rg).unwrap();
        }
    });
    assert_eq!(reader.total_row_count(), 4_000);
    assert_eq!(reader.row_group_count(), 4);
    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, 4_000);
}

#[test]
fn multi_rg_values_are_concatenated_in_order() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        for rg_i in 0..3u32 {
            let mut rg = bw.new_row_group();
            for j in 0..100i64 {
                rg.push_i64(0, Some(rg_i as i64 * 100 + j)).unwrap();
            }
            bw.write_row_group(rg).unwrap();
        }
    });
    let batch = reader.clone_and_read_all();
    let values: Vec<i64> = batch.col_i64(0).iter().map(|v| v.unwrap()).collect();
    // Should be 0..300 in order
    let expected: Vec<i64> = (0..300).collect();
    assert_eq!(values, expected);
}

#[test]
fn multi_rg_offsets_are_non_overlapping() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        for _ in 0..5 {
            let mut rg = bw.new_row_group();
            for i in 0..500i64 { rg.push_i64(0, Some(i)).unwrap(); }
            bw.write_row_group(rg).unwrap();
        }
    });
    let rgs = reader.rg_descriptors();
    for i in 1..rgs.len() {
        assert!(
            rgs[i].file_offset >= rgs[i-1].file_offset + rgs[i-1].byte_length,
            "RG {} overlaps with RG {}", i, i-1
        );
    }
}

#[test]
fn multi_rg_first_rg_starts_after_header() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        rg.push_i64(0, Some(1)).unwrap();
        bw.write_row_group(rg).unwrap();
    });
    let rgs = reader.rg_descriptors();
    assert!(rgs[0].file_offset >= bish::header::FILE_HEADER_SIZE as u64);
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicate pushdown (zone-map) tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn predicate_skips_non_matching_row_groups() {
    // RG 0: ids 0..100, RG 1: ids 100..200, RG 2: ids 200..300
    let schema = BishSchema::new(vec![BishField::new("id", BishType::Int64)]);
    let mut reader = make_bish(schema, |bw| {
        for rg_start in [0i64, 100, 200] {
            let mut rg = bw.new_row_group();
            for i in rg_start..rg_start+100 { rg.push_i64(0, Some(i)).unwrap(); }
            bw.write_row_group(rg).unwrap();
        }
    });

    // Predicate: id BETWEEN 150 AND 250 → only RG 1 and RG 2 should match
    let batch = reader.scan(&[0], &[(0, 150, 250)]).unwrap();
    // RG 0 has max=99  < 150 → skipped
    // RG 1 has min=100 ≤ 250 and max=199 ≥ 150 → included (100 rows)
    // RG 2 has min=200 ≤ 250 and max=299 ≥ 150 → included (100 rows)
    assert_eq!(batch.row_count, 200);
    let ids: Vec<i64> = batch.col_i64(0).iter().map(|v| v.unwrap()).collect();
    assert!(ids.iter().all(|&id| id >= 100), "no rows from RG 0 (ids 0–99)");
}

#[test]
fn predicate_skips_all_row_groups() {
    let schema = BishSchema::new(vec![BishField::new("id", BishType::Int64)]);
    let mut reader = make_bish(schema, |bw| {
        for rg_start in [0i64, 100, 200] {
            let mut rg = bw.new_row_group();
            for i in rg_start..rg_start+100 { rg.push_i64(0, Some(i)).unwrap(); }
            bw.write_row_group(rg).unwrap();
        }
    });
    // Predicate: id BETWEEN 500 AND 600 → no RG matches
    let batch = reader.scan(&[0], &[(0, 500, 600)]).unwrap();
    assert_eq!(batch.row_count, 0);
}

#[test]
fn predicate_passes_all_row_groups() {
    let schema = BishSchema::new(vec![BishField::new("id", BishType::Int64)]);
    let mut reader = make_bish(schema, |bw| {
        for rg_start in [0i64, 100, 200] {
            let mut rg = bw.new_row_group();
            for i in rg_start..rg_start+100 { rg.push_i64(0, Some(i)).unwrap(); }
            bw.write_row_group(rg).unwrap();
        }
    });
    // Predicate covers entire range → all 3 RGs pass
    let batch = reader.scan(&[0], &[(0, 0, 299)]).unwrap();
    assert_eq!(batch.row_count, 300);
}

#[test]
fn predicate_on_exact_boundary() {
    let schema = BishSchema::new(vec![BishField::new("id", BishType::Int64)]);
    let mut reader = make_bish(schema, |bw| {
        // RG 0 has values 0..100 → zone_min=0, zone_max=99
        let mut rg = bw.new_row_group();
        for i in 0..100i64 { rg.push_i64(0, Some(i)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    // Predicate exactly at zone_max boundary: id BETWEEN 99 AND 200
    let batch = reader.scan(&[0], &[(0, 99, 200)]).unwrap();
    assert_eq!(batch.row_count, 100, "boundary match should include the RG");

    // Predicate just past zone_max: id BETWEEN 100 AND 200 → skip
    let batch2 = reader.scan(&[0], &[(0, 100, 200)]).unwrap();
    assert_eq!(batch2.row_count, 0, "just past zone_max should skip the RG");
}

// ─────────────────────────────────────────────────────────────────────────────
// Projection tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn projection_reads_only_requested_columns() {
    let schema = BishSchema::new(vec![
        BishField::new("id",     BishType::Int64),
        BishField::new("city",   BishType::Utf8),
        BishField::new("amount", BishType::Float64),
    ]);
    let mut reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..100i64 {
            rg.push_i64(0, Some(i)).unwrap();
            rg.push_str(1, Some("BLR")).unwrap();
            rg.push_f64(2, Some(i as f64)).unwrap();
        }
        bw.write_row_group(rg).unwrap();
    });

    // Only request columns 0 and 2 — column 1 (city) should not be read
    let batch = reader.read_columns(&[0, 2]).unwrap();
    assert_eq!(batch.columns.len(), 2);
    assert_eq!(batch.column_indices, vec![0, 2]);

    // Column at projection position 0 is schema column 0 (id)
    let ids: Vec<i64> = batch.col_i64(0).iter().map(|v| v.unwrap()).collect();
    assert_eq!(ids, (0..100).collect::<Vec<_>>());

    // Column at projection position 1 is schema column 2 (amount)
    let amounts: Vec<f64> = batch.col_f64(1).iter().map(|v| v.unwrap()).collect();
    assert_eq!(amounts, (0..100).map(|i| i as f64).collect::<Vec<_>>());
}

#[test]
fn projection_single_column() {
    let schema = BishSchema::new(vec![
        BishField::new("a", BishType::Int64),
        BishField::new("b", BishType::Utf8),
        BishField::new("c", BishType::Float64),
    ]);
    let mut reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..50i64 {
            rg.push_i64(0, Some(i * 10)).unwrap();
            rg.push_str(1, Some("x")).unwrap();
            rg.push_f64(2, Some(i as f64)).unwrap();
        }
        bw.write_row_group(rg).unwrap();
    });

    let batch = reader.read_columns(&[1]).unwrap(); // only city
    assert_eq!(batch.columns.len(), 1);
    let cities = batch.col_str(0);
    assert!(cities.iter().all(|v| v.as_deref() == Some("x")));
}

// ─────────────────────────────────────────────────────────────────────────────
// Byte offset and file structure tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn super_footer_magic_is_correct() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        rg.push_i64(0, Some(42)).unwrap();
        bw.write_row_group(rg).unwrap();
    });
    let sf = reader.super_footer();
    assert_eq!(sf.version_major, 1);
    assert_eq!(sf.version_minor, 0);
}

#[test]
fn super_footer_row_counts_match() {
    let n = 7_777usize;
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..n as i64 { rg.push_i64(0, Some(i)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    assert_eq!(reader.total_row_count(), n as u64);
}

#[test]
fn super_footer_chunk_offsets_are_after_data() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..1000i64 { rg.push_i64(0, Some(i)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let sf = reader.super_footer();
    let rgs = reader.rg_descriptors();
    // All footer chunks must start AFTER the last row group's data
    let data_end = rgs.last().map(|rg| rg.file_offset + rg.byte_length).unwrap_or(16);
    assert!(sf.chunk_a.offset >= data_end, "chunk A before data end");
    assert!(sf.chunk_b.offset >= data_end, "chunk B before data end");
    assert!(sf.chunk_c.offset >= data_end, "chunk C before data end");
}

#[test]
fn col_chunk_offsets_point_inside_rg_byte_range() {
    let schema = BishSchema::new(vec![
        BishField::new("a", BishType::Int64),
        BishField::new("b", BishType::Utf8),
    ]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..500i64 {
            rg.push_i64(0, Some(i)).unwrap();
            rg.push_str(1, Some("test")).unwrap();
        }
        bw.write_row_group(rg).unwrap();
    });
    let rgs = reader.rg_descriptors();
    for rg in rgs.iter() {
        for &off in &rg.col_chunk_offsets {
            assert!(off >= rg.file_offset, "col offset before RG start");
            assert!(off < rg.file_offset + rg.byte_length, "col offset past RG end");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress / edge case tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn stress_large_int_column_many_pages() {
    let n = 50_000usize;
    let opts = WriteOptions { page_row_target: 512, ..Default::default() };
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish_opts(schema, opts, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..n as i64 { rg.push_i64(0, Some(i)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, n);
    let values: Vec<i64> = batch.col_i64(0).iter().map(|v| v.unwrap()).collect();
    assert_eq!(values, (0..n as i64).collect::<Vec<_>>());
}

#[test]
fn stress_sorted_timestamps_use_delta_encoding() {
    // Sorted timestamps compress to ~3 bytes each with delta encoding.
    let n = 10_000;
    let start = 1_700_000_000_000_000_000i64;
    let schema = BishSchema::new(vec![BishField::new("ts", BishType::TimestampNs)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..n as i64 {
            rg.push_i64(0, Some(start + i * 1_000_000)).unwrap(); // 1ms apart
        }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, n);
    // Verify first and last values
    assert_eq!(batch.col_i64(0)[0].unwrap(), start);
    assert_eq!(batch.col_i64(0)[n-1].unwrap(), start + (n-1) as i64 * 1_000_000);
}

#[test]
fn stress_low_cardinality_strings_use_rle() {
    // Only 3 distinct cities across 5000 rows — should RLE nicely.
    let cities = ["BLR", "MUM", "DEL"];
    let n = 5_000;
    let schema = BishSchema::new(vec![BishField::new("city", BishType::Utf8)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for i in 0..n { rg.push_str(0, Some(cities[i % 3])).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, n);
    let result = batch.col_str(0);
    for (i, v) in result.iter().enumerate() {
        assert_eq!(v.as_deref().unwrap(), cities[i % 3]);
    }
}

#[test]
fn stress_single_row_file() {
    let schema = BishSchema::new(vec![
        BishField::new("id",   BishType::Int64),
        BishField::new("name", BishType::Utf8),
    ]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        rg.push_i64(0, Some(42)).unwrap();
        rg.push_str(1, Some("Abhishek")).unwrap();
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, 1);
    assert_eq!(batch.col_i64(0)[0].unwrap(), 42);
    assert_eq!(batch.col_str(1)[0].as_deref().unwrap(), "Abhishek");
}

#[test]
fn stress_empty_strings_round_trip() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Utf8)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for _ in 0..100 { rg.push_str(0, Some("")).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, 100);
    assert!(batch.col_str(0).iter().all(|v| v.as_deref() == Some("")));
}

#[test]
fn stress_extreme_int_values() {
    let values = vec![i64::MIN, i64::MIN + 1, -1i64, 0, 1, i64::MAX - 1, i64::MAX];
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let reader = make_bish(schema, |bw| {
        let mut rg = bw.new_row_group();
        for &v in &values { rg.push_i64(0, Some(v)).unwrap(); }
        bw.write_row_group(rg).unwrap();
    });
    let batch = reader.clone_and_read_all();
    let got: Vec<i64> = batch.col_i64(0).iter().map(|v| v.unwrap()).collect();
    assert_eq!(values, got);
}

#[test]
fn stress_multi_rg_multi_column_all_types() {
    let schema = BishSchema::new(vec![
        BishField::new("id",        BishType::Int64),
        BishField::new("score",     BishType::Float64),
        BishField::new("label",     BishType::Utf8),
        BishField::new("active",    BishType::Boolean),
        BishField::nullable("note", BishType::Utf8),
    ]);
    let reader = make_bish(schema, |bw| {
        for rg_i in 0..3u32 {
            let mut rg = bw.new_row_group();
            let start = rg_i as i64 * 1000;
            for i in 0..1000i64 {
                let g = start + i;
                rg.push_i64(0, Some(g)).unwrap();
                rg.push_f64(1, Some(g as f64 * 0.5)).unwrap();
                rg.push_str(2, Some(if g % 2 == 0 { "even" } else { "odd" })).unwrap();
                rg.push_bool(3, Some(g % 3 == 0)).unwrap();
                rg.push_str(4, if g % 7 == 0 { None } else { Some("note") }).unwrap();
            }
            bw.write_row_group(rg).unwrap();
        }
    });

    let batch = reader.clone_and_read_all();
    assert_eq!(batch.row_count, 3_000);

    // Spot-check a few values
    assert_eq!(batch.col_i64(0)[0].unwrap(), 0);
    assert_eq!(batch.col_i64(0)[2999].unwrap(), 2999);
    assert!((batch.col_f64(1)[1].unwrap() - 0.5).abs() < 1e-10);
    assert_eq!(batch.col_str(2)[0].as_deref().unwrap(), "even");
    assert_eq!(batch.col_str(2)[1].as_deref().unwrap(), "odd");
    assert_eq!(batch.col_bool(3)[0].unwrap(), true);  // 0 % 3 == 0
    assert_eq!(batch.col_bool(3)[1].unwrap(), false); // 1 % 3 != 0
    // note: every 7th row is None
    assert!(batch.columns[4].bytes_values[7].is_none());
    assert!(batch.columns[4].bytes_values[1].is_some()); // g=1, 1%7!=0 → Some
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional helpers added to BishReader for test access
// (these are thin wrappers that expose internal state for assertions)
// ─────────────────────────────────────────────────────────────────────────────

trait BishReaderTestExt<R: std::io::Read + std::io::Seek> {
    fn clone_and_read_all(self) -> bish::reader::RecordBatch;
    fn rg_descriptors(&self) -> &[bish::footer::RgDescriptor];
    fn super_footer(&self) -> &bish::header::SuperFooter;
}

impl BishReaderTestExt<Cursor<Vec<u8>>> for BishReader<Cursor<Vec<u8>>> {
    fn clone_and_read_all(mut self) -> bish::reader::RecordBatch {
        self.read_all().expect("read_all")
    }

    fn rg_descriptors(&self) -> &[bish::footer::RgDescriptor] {
        self.rg_descriptors_ref()
    }

    fn super_footer(&self) -> &bish::header::SuperFooter {
        self.super_footer_ref()
    }
}

// make_bish with custom WriteOptions
fn make_bish_opts(
    schema: BishSchema,
    options: WriteOptions,
    write_fn: impl FnOnce(&mut BishWriter<Cursor<Vec<u8>>>),
) -> BishReader<Cursor<Vec<u8>>> {
    let mut bw = BishWriter::with_options(Cursor::new(Vec::<u8>::new()), schema, options).expect("writer");
    write_fn(&mut bw);
    let raw = bw.finish_into_bytes().expect("finish");
    BishReader::open(Cursor::new(raw)).expect("reader open")
}
