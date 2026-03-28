use std::io::{BufWriter, Cursor};
use bish::types::{BishType, BishField, BishSchema, ZoneValue, Codec};
use bish::writer::{RowGroupWriter, WriteOptions, RowGroupMeta};
use bish::types::Encoding;

fn make_schema() -> BishSchema {
    BishSchema::new(vec![
        BishField::new("id",     BishType::Int64),
        BishField::new("city",   BishType::Utf8),
        BishField::new("amount", BishType::Float64),
        BishField::nullable("tag", BishType::Utf8),
        BishField::new("active", BishType::Boolean),
    ])
}

fn write_row_group(n_rows: usize) -> (Vec<u8>, RowGroupMeta) {
    let schema = make_schema();
    let opts = WriteOptions {
        page_row_target: 1024,
        ..Default::default()
    };
    let mut rg = RowGroupWriter::new(&schema, 0, opts);

    for i in 0..n_rows {
        rg.push_i64(0, Some(i as i64)).unwrap();
        rg.push_str(1, Some(if i % 2 == 0 { "BLR" } else { "MUM" })).unwrap();
        rg.push_f64(2, Some(i as f64 * 1.5)).unwrap();
        rg.push_str(3, if i % 5 == 0 { None } else { Some("tag") }).unwrap();
        rg.push_bool(4, Some(i % 3 == 0)).unwrap();
    }

    let buf = Vec::new();
    let cursor = Cursor::new(buf);
    let mut writer = BufWriter::new(cursor);
    let mut offset = 0u64;

    let meta = rg.finish(&mut writer, &mut offset).unwrap();
    let bytes = writer.into_inner().unwrap().into_inner();
    (bytes, meta)
}

#[test]
fn test_row_group_produces_bytes() {
    let (bytes, meta) = write_row_group(100);
    assert!(!bytes.is_empty(), "row group produced no bytes");
    assert_eq!(meta.row_count, 100);
    assert_eq!(meta.columns.len(), 5);
}

#[test]
fn test_row_group_byte_offset_tracking() {
    let (bytes, meta) = write_row_group(500);
    // The sum of all column chunk byte_lengths must equal total bytes written
    let col_total: u64 = meta.columns.iter().map(|c| c.byte_length).sum();
    assert_eq!(col_total, bytes.len() as u64);
    assert_eq!(meta.byte_length, bytes.len() as u64);
}

#[test]
fn test_zone_map_int() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let mut rg = RowGroupWriter::new(&schema, 0, WriteOptions::default());
    for v in [-10i64, 0, 5, 100, -50, 42] {
        rg.push_i64(0, Some(v)).unwrap();
    }
    let mut writer = BufWriter::new(Cursor::new(Vec::new()));
    let mut offset = 0u64;
    let meta = rg.finish(&mut writer, &mut offset).unwrap();

    let col = &meta.columns[0];
    assert_eq!(col.zone_min, ZoneValue::Int(-50));
    assert_eq!(col.zone_max, ZoneValue::Int(100));
}

#[test]
fn test_zone_map_string() {
    let schema = BishSchema::new(vec![BishField::new("s", BishType::Utf8)]);
    let mut rg = RowGroupWriter::new(&schema, 0, WriteOptions::default());
    for s in ["mango", "apple", "zebra", "banana"] {
        rg.push_str(0, Some(s)).unwrap();
    }
    let mut writer = BufWriter::new(Cursor::new(Vec::new()));
    let mut offset = 0u64;
    let meta = rg.finish(&mut writer, &mut offset).unwrap();

    let col = &meta.columns[0];
    assert_eq!(col.zone_min, ZoneValue::Bytes(b"apple".to_vec()));
    assert_eq!(col.zone_max, ZoneValue::Bytes(b"zebra".to_vec()));
}

#[test]
fn test_null_count_tracked() {
    let schema = BishSchema::new(vec![BishField::nullable("v", BishType::Utf8)]);
    let mut rg = RowGroupWriter::new(&schema, 0, WriteOptions::default());
    rg.push_str(0, Some("hello")).unwrap();
    rg.push_str(0, None).unwrap();
    rg.push_str(0, None).unwrap();
    rg.push_str(0, Some("world")).unwrap();

    let mut writer = BufWriter::new(Cursor::new(Vec::new()));
    let mut offset = 0u64;
    let meta = rg.finish(&mut writer, &mut offset).unwrap();
    assert_eq!(meta.columns[0].null_count, 2);
    assert_eq!(meta.columns[0].row_count, 4);
}

#[test]
fn test_multiple_pages_flushed() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let opts = WriteOptions {
        page_row_target: 100,   // flush every 100 rows
        ..Default::default()
    };
    let mut rg = RowGroupWriter::new(&schema, 0, opts);
    for i in 0..350i64 {
        rg.push_i64(0, Some(i)).unwrap();
    }
    let mut writer = BufWriter::new(Cursor::new(Vec::new()));
    let mut offset = 0u64;
    let meta = rg.finish(&mut writer, &mut offset).unwrap();

    // 350 rows / 100 per page = 3 full + 1 partial = 4 pages
    assert_eq!(meta.columns[0].pages.len(), 4);
    assert_eq!(meta.row_count, 350);
}

#[test]
fn test_large_row_group_produces_valid_offsets() {
    let (bytes, meta) = write_row_group(10_000);
    // Verify no column chunk offset points outside the written bytes
    for col in &meta.columns {
        assert!(col.file_offset < bytes.len() as u64);
        assert!(col.file_offset + col.byte_length <= bytes.len() as u64);
    }
}

#[test]
fn test_cold_rg_uses_zstd9() {
    let schema = BishSchema::new(vec![BishField::new("v", BishType::Int64)]);
    let opts = WriteOptions {
        is_cold: true,
        adaptive_codec: true,
        page_row_target: 50,
        ..Default::default()
    };
    let mut rg = RowGroupWriter::new(&schema, 0, opts);
    for i in 0..50i64 {
        rg.push_i64(0, Some(i)).unwrap();
    }
    let mut writer = BufWriter::new(Cursor::new(Vec::new()));
    let mut offset = 0u64;
    let meta = rg.finish(&mut writer, &mut offset).unwrap();

    // Cold RG should have selected Zstd9
    let codec = meta.columns[0].pages[0].codec;
    assert_eq!(codec, Codec::Zstd9);
}

#[test]
fn test_boolean_column() {
    let schema = BishSchema::new(vec![BishField::new("flag", BishType::Boolean)]);
    let mut rg = RowGroupWriter::new(&schema, 0, WriteOptions::default());
    for i in 0..200 {
        rg.push_bool(0, Some(i % 3 == 0)).unwrap();
    }
    let mut writer = BufWriter::new(Cursor::new(Vec::new()));
    let mut offset = 0u64;
    let meta = rg.finish(&mut writer, &mut offset).unwrap();
    assert_eq!(meta.columns[0].row_count, 200);
    // Boolean pages use Bitpack — check encoding tag
    assert_eq!(meta.columns[0].pages[0].encoding, Encoding::Bitpack);
}
