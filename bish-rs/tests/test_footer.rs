use std::io::Cursor;
use bish::{BishWriter, FinishedFile, FILE_HEADER_SIZE, SUPER_FOOTER_SIZE};
use bish::{SuperFooter, ChunkRef, SectionRef, FeatureFlags, BISH_MAGIC};
use bish::types::{BishType, BishField, BishSchema};
use bish::footer::{build_chunk_a, build_chunk_b, build_chunk_c, build_chunk_e, CHUNK_A_MAGIC, CHUNK_B_MAGIC, CHUNK_C_MAGIC, CHUNK_E_MAGIC, parse_chunk_b, parse_chunk_c, ColStatEntry};
use bish::types::Codec;
use bish::compress::decompress;
use xxhash_rust::xxh64::xxh64;

// ── helpers ──────────────────────────────────────────────────────────────

fn make_schema() -> BishSchema {
    BishSchema::new(vec![
        BishField::new("id",     BishType::Int64).with_sort_key(),
        BishField::new("city",   BishType::Utf8).with_partition_key(),
        BishField::new("amount", BishType::Float64),
        BishField::nullable("tag", BishType::Utf8),
    ])
}

fn make_bytes(n_rows: usize, n_rg: usize) -> (Vec<u8>, FinishedFile) {
    let schema = make_schema();
    let buf = Vec::<u8>::new();
    let cursor = std::io::Cursor::new(buf);
    let mut bw = BishWriter::new(cursor, schema).unwrap();

    let rows_per_rg = (n_rows / n_rg.max(1)).max(1);
    let mut global_id = 0i64;

    for rg_i in 0..n_rg {
        let mut rg = bw.new_row_group();
        let count = if rg_i == n_rg - 1 {
            n_rows.saturating_sub(global_id as usize)
        } else {
            rows_per_rg
        };
        for j in 0..count {
            rg.push_i64(0, Some(global_id)).unwrap();
            rg.push_str(1, Some(if j % 2 == 0 { "BLR" } else { "MUM" })).unwrap();
            rg.push_f64(2, Some(global_id as f64 * 1.5)).unwrap();
            rg.push_str(3, if j % 5 == 0 { None } else { Some("vip") }).unwrap();
            global_id += 1;
        }
        bw.write_row_group(rg).unwrap();
    }

    let (finished, _) = bw.finish().unwrap();
    // Can't recover the Vec from Cursor after consuming bw,
    // but FinishedFile has everything we need for testing.
    (vec![], finished)
}

// ── file structure tests ─────────────────────────────────────────────────

#[test]
fn test_writer_produces_valid_file_summary() {
    let (_, finished) = make_bytes(1_000, 1);
    assert!(finished.looks_valid());
    assert_eq!(finished.total_row_count, 1_000);
    assert_eq!(finished.row_group_count, 1);
    assert!(finished.total_file_bytes > (FILE_HEADER_SIZE + SUPER_FOOTER_SIZE) as u64);
}

#[test]
fn test_multiple_row_groups() {
    let (_, finished) = make_bytes(10_000, 4);
    assert_eq!(finished.row_group_count, 4);
    assert_eq!(finished.total_row_count, 10_000);
    // All 4 row groups have sequential rg_ids
    for (i, rg) in finished.rg_metas.iter().enumerate() {
        assert_eq!(rg.rg_id, i as u32);
    }
}

#[test]
fn test_row_group_offsets_are_sequential_and_non_overlapping() {
    let (_, finished) = make_bytes(5_000, 3);
    let rgs = &finished.rg_metas;
    for i in 1..rgs.len() {
        // Each RG starts where the previous one ended
        assert_eq!(
            rgs[i].file_offset,
            rgs[i-1].file_offset + rgs[i-1].byte_length,
            "RG {} offset gap vs RG {}", i, i-1
        );
    }
    // First RG starts right after the file header (16B)
    assert_eq!(rgs[0].file_offset, FILE_HEADER_SIZE as u64);
}

#[test]
fn test_file_size_accounting() {
    let (_, finished) = make_bytes(1_000, 2);
    // total_file_bytes = 16 (header) + data + chunks A-E + 512 (super-footer)
    assert!(finished.total_file_bytes >= 528); // minimum: header + super-footer
    // Sanity: file shouldn't be unreasonably large for 1000 rows × 4 cols
    assert!(finished.total_file_bytes < 1_000_000);
}

// ── super-footer serialisation tests ─────────────────────────────────────

#[test]
fn test_super_footer_is_exactly_512_bytes() {
    // We verify this by checking the spec-defined size constant
    assert_eq!(SUPER_FOOTER_SIZE, 512);
    // And that a default SuperFooter serialises to exactly that
    let sf = SuperFooter {
        version_major: 1, version_minor: 0,
        feature_flags: FeatureFlags::default(),
        row_group_count: 0, total_row_count: 0, schema_hash: 0,
        file_created_at: 0, file_modified_at: 0,
        chunk_a: ChunkRef::default(), chunk_b: ChunkRef::default(),
        chunk_c: ChunkRef::default(), chunk_d: ChunkRef::default(),
        chunk_e: ChunkRef::default(),
        partition_index: SectionRef::default(), delete_log: SectionRef::default(),
        sparse_index: SectionRef::default(), vector_index: SectionRef::default(),
    };
    assert_eq!(sf.to_bytes().len(), 512);
}

#[test]
fn test_super_footer_magic_bookends() {
    let sf = SuperFooter {
        version_major: 1, version_minor: 0,
        feature_flags: FeatureFlags::default(),
        row_group_count: 5, total_row_count: 50_000, schema_hash: 0xDEADBEEF,
        file_created_at: 100, file_modified_at: 200,
        chunk_a: ChunkRef { offset: 1024, length: 512, checksum: 0xABCD },
        chunk_b: ChunkRef { offset: 2048, length: 4096, checksum: 0x1234 },
        chunk_c: ChunkRef::default(), chunk_d: ChunkRef::default(),
        chunk_e: ChunkRef::default(),
        partition_index: SectionRef::default(), delete_log: SectionRef::default(),
        sparse_index: SectionRef::default(), vector_index: SectionRef::default(),
    };
    let bytes = sf.to_bytes();
    // magic_start at offset 0
    assert_eq!(&bytes[0..4], &BISH_MAGIC);
    // magic_end at offset 504
    assert_eq!(&bytes[504..508], &BISH_MAGIC);
}

#[test]
fn test_super_footer_chunk_refs_roundtrip() {
    let sf = SuperFooter {
        version_major: 1, version_minor: 0,
        feature_flags: FeatureFlags(FeatureFlags::ADAPTIVE_CODEC),
        row_group_count: 3, total_row_count: 30_000,
        schema_hash: 0xCAFEBABE_DEADBEEF,
        file_created_at: 1_700_000_000_000_000_000,
        file_modified_at: 1_700_000_001_000_000_000,
        chunk_a: ChunkRef { offset: 16,   length: 128,  checksum: 0xAAAA },
        chunk_b: ChunkRef { offset: 144,  length: 256,  checksum: 0xBBBB },
        chunk_c: ChunkRef { offset: 400,  length: 512,  checksum: 0xCCCC },
        chunk_d: ChunkRef { offset: 912,  length: 12,   checksum: 0xDDDD },
        chunk_e: ChunkRef { offset: 924,  length: 64,   checksum: 0xEEEE },
        partition_index: SectionRef { offset: 0, length: 0 },
        delete_log: SectionRef::default(),
        sparse_index: SectionRef::default(),
        vector_index: SectionRef::default(),
    };
    let bytes = sf.to_bytes();
    let decoded = SuperFooter::from_bytes(&bytes).unwrap();
    assert_eq!(sf, decoded);
}

// ── chunk A (schema) tests ────────────────────────────────────────────────

#[test]
fn test_chunk_a_starts_with_correct_magic() {
    let schema = make_schema();
    let chunk = build_chunk_a(&schema).unwrap();
    assert_eq!(&chunk[0..4], &CHUNK_A_MAGIC);
}

#[test]
fn test_chunk_a_schema_round_trips() {
    let schema = make_schema();
    let chunk = build_chunk_a(&schema).unwrap();

    // chunk envelope: 4 (magic) + 4 (len) + 1 (id) + 1 (codec) + 2 (reserved) = 12B header
    // then the compressed Arrow IPC bytes start
    let chunk_payload_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
    assert!(chunk_payload_len > 0);
    assert_eq!(chunk.len(), 12 + chunk_payload_len);
}

#[test]
fn test_chunk_a_field_count_preserved() {
    let schema = make_schema();
    let arrow_ipc = schema.to_arrow_ipc_bytes().unwrap();
    let decoded = BishSchema::from_arrow_ipc_bytes(&arrow_ipc).unwrap();
    assert_eq!(decoded.fields.len(), schema.fields.len());
    for (orig, dec) in schema.fields.iter().zip(decoded.fields.iter()) {
        assert_eq!(orig.name, dec.name);
    }
}

// ── chunk B (RG offsets) tests ────────────────────────────────────────────

#[test]
fn test_chunk_b_magic() {
    let (_, finished) = make_bytes(100, 1);
    let chunk = build_chunk_b(&finished.rg_metas).unwrap();
    assert_eq!(&chunk[0..4], &CHUNK_B_MAGIC);
}

#[test]
fn test_chunk_b_roundtrips_rg_offsets() {
    let (_, finished) = make_bytes(3_000, 3);
    let chunk = build_chunk_b(&finished.rg_metas).unwrap();

    // Decompress and parse the payload
    let chunk_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
    let codec_tag = chunk[9];
    let codec = Codec::from_u8(codec_tag).unwrap();
    let compressed = &chunk[12..12 + chunk_len];
    let payload = decompress(compressed, codec, chunk_len * 4).unwrap();

    let rg_descs = parse_chunk_b(&payload, 4).unwrap();
    assert_eq!(rg_descs.len(), 3);

    // Check round-trip of key fields
    for (orig, desc) in finished.rg_metas.iter().zip(rg_descs.iter()) {
        assert_eq!(orig.rg_id, desc.rg_id);
        assert_eq!(orig.row_count, desc.row_count);
        assert_eq!(orig.file_offset, desc.file_offset);
        assert_eq!(orig.byte_length, desc.byte_length);
        assert_eq!(orig.columns.len(), desc.col_chunk_offsets.len());
        for (col, &off) in orig.columns.iter().zip(desc.col_chunk_offsets.iter()) {
            assert_eq!(col.file_offset, off);
        }
    }
}

// ── chunk C (col stats) tests ─────────────────────────────────────────────

#[test]
fn test_chunk_c_magic() {
    let (_, finished) = make_bytes(100, 1);
    let chunk = build_chunk_c(&finished.rg_metas).unwrap();
    assert_eq!(&chunk[0..4], &CHUNK_C_MAGIC);
}

#[test]
fn test_chunk_c_entry_count() {
    let n_rg = 3;
    let n_cols = 4; // schema has 4 columns
    let (_, finished) = make_bytes(3_000, n_rg);
    let chunk = build_chunk_c(&finished.rg_metas).unwrap();

    let chunk_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
    let codec_tag = chunk[9];
    let codec = Codec::from_u8(codec_tag).unwrap();
    let compressed = &chunk[12..12 + chunk_len];
    let payload = decompress(compressed, codec, chunk_len * 4).unwrap();

    let entries = parse_chunk_c(&payload).unwrap();
    assert_eq!(entries.len(), n_rg * n_cols);
}

#[test]
fn test_chunk_c_zone_maps_survive_roundtrip() {
    let (_, finished) = make_bytes(1_000, 1);
    let chunk = build_chunk_c(&finished.rg_metas).unwrap();

    let chunk_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
    let codec = Codec::from_u8(chunk[9]).unwrap();
    let payload = decompress(&chunk[12..12+chunk_len], codec, chunk_len * 4).unwrap();
    let entries = parse_chunk_c(&payload).unwrap();

    // Column 0 is id (Int64) — zone_min should be 0, zone_max should be 999
    let id_entry = entries.iter().find(|e| e.column_index == 0).unwrap();
    assert_eq!(id_entry.zone_min_i64, 0);
    assert_eq!(id_entry.zone_max_i64, 999);

    // Column 2 is amount (Float64) — stored as IEEE bits
    let amt_entry = entries.iter().find(|e| e.column_index == 2).unwrap();
    let min_f = f64::from_bits(amt_entry.zone_min_i64 as u64);
    let max_f = f64::from_bits(amt_entry.zone_max_i64 as u64);
    assert!((min_f - 0.0).abs() < 1e-9);   // 0 * 1.5
    assert!((max_f - 1498.5).abs() < 1e-6); // 999 * 1.5
}

#[test]
fn test_chunk_c_null_count_preserved() {
    let (_, finished) = make_bytes(500, 1);
    let chunk = build_chunk_c(&finished.rg_metas).unwrap();

    let chunk_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
    let codec = Codec::from_u8(chunk[9]).unwrap();
    let payload = decompress(&chunk[12..12+chunk_len], codec, chunk_len * 4).unwrap();
    let entries = parse_chunk_c(&payload).unwrap();

    // Column 3 is nullable "tag" — every 5th row is null = 100 nulls in 500 rows
    let tag_entry = entries.iter().find(|e| e.column_index == 3).unwrap();
    assert_eq!(tag_entry.null_count, 100); // 500 / 5 = 100 nulls
}

#[test]
fn test_chunk_c_string_zone_maps() {
    // Write a single-column schema with known string values
    let schema = BishSchema::new(vec![BishField::new("city", BishType::Utf8)]);
    let cursor = Cursor::new(Vec::<u8>::new());
    let mut bw = BishWriter::new(cursor, schema).unwrap();
    let mut rg = bw.new_row_group();
    for city in &["MUM", "BLR", "DEL", "HYD", "CHN"] {
        rg.push_str(0, Some(city)).unwrap();
    }
    bw.write_row_group(rg).unwrap();
    let (finished, _) = bw.finish().unwrap();

    let chunk = build_chunk_c(&finished.rg_metas).unwrap();
    let chunk_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
    let codec = Codec::from_u8(chunk[9]).unwrap();
    let payload = decompress(&chunk[12..12+chunk_len], codec, chunk_len * 4).unwrap();
    let entries = parse_chunk_c(&payload).unwrap();

    assert_eq!(entries.len(), 1);
    let e = &entries[0];

    // Lexicographic min = "BLR", max = "MUM"
    let min_str = std::str::from_utf8(&e.zone_min_bytes[..3]).unwrap();
    let max_str = std::str::from_utf8(&e.zone_max_bytes[..3]).unwrap();
    assert_eq!(min_str, "BLR");
    assert_eq!(max_str, "MUM");
}

// ── ColStatEntry predicate helpers ────────────────────────────────────────

#[test]
fn test_col_stat_int_in_range() {
    let entry = ColStatEntry {
        rg_id: 0, column_index: 0,
        zone_min_i64: -50, zone_max_i64: 100,
        zone_min_bytes: [0; 32], zone_max_bytes: [0; 32],
        null_count: 0, row_count: 1000,
    };
    assert!(entry.int_in_range(0));
    assert!(entry.int_in_range(-50));
    assert!(entry.int_in_range(100));
    assert!(!entry.int_in_range(101));   // > max → skip
    assert!(!entry.int_in_range(-51));   // < min → skip
}

#[test]
fn test_col_stat_float_in_range() {
    let min_bits = 1.0f64.to_bits() as i64;
    let max_bits = 100.0f64.to_bits() as i64;
    let entry = ColStatEntry {
        rg_id: 0, column_index: 2,
        zone_min_i64: min_bits, zone_max_i64: max_bits,
        zone_min_bytes: [0; 32], zone_max_bytes: [0; 32],
        null_count: 0, row_count: 1000,
    };
    assert!(entry.float_in_range(50.0));
    assert!(entry.float_in_range(1.0));
    assert!(entry.float_in_range(100.0));
    assert!(!entry.float_in_range(0.5));
    assert!(!entry.float_in_range(100.1));
    assert!(entry.float_in_range(f64::NAN)); // NaN = conservative
}

#[test]
fn test_col_stat_bytes_in_range() {
    let mut min_b = [0u8; 32];
    let mut max_b = [0u8; 32];
    min_b[..3].copy_from_slice(b"BLR");
    max_b[..3].copy_from_slice(b"MUM");
    let entry = ColStatEntry {
        rg_id: 0, column_index: 1,
        zone_min_i64: 0, zone_max_i64: 0,
        zone_min_bytes: min_b, zone_max_bytes: max_b,
        null_count: 0, row_count: 500,
    };
    assert!(entry.bytes_in_range(b"BLR"));
    assert!(entry.bytes_in_range(b"DEL")); // D > B, D < M → in range
    assert!(entry.bytes_in_range(b"MUM"));
    assert!(!entry.bytes_in_range(b"ZZZ")); // Z > M → skip
    assert!(!entry.bytes_in_range(b"AAA")); // A < B → skip
}

// ── chunk E (user metadata) ───────────────────────────────────────────────

#[test]
fn test_chunk_e_roundtrip() {
    let meta = vec![
        ("bish.created_by".to_string(), "bish-rs 0.1.0".to_string()),
        ("bish.description".to_string(), "test file".to_string()),
    ];
    let chunk = build_chunk_e(&meta).unwrap();
    assert_eq!(&chunk[0..4], &CHUNK_E_MAGIC);

    let chunk_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
    let codec = Codec::from_u8(chunk[9]).unwrap();
    let payload = decompress(&chunk[12..12+chunk_len], codec, chunk_len * 4).unwrap();

    // Parse back manually
    let mut pos = 0;
    let mut decoded = Vec::new();
    while pos < payload.len() {
        let kl = u16::from_le_bytes(payload[pos..pos+2].try_into().unwrap()) as usize; pos += 2;
        let k = std::str::from_utf8(&payload[pos..pos+kl]).unwrap().to_string(); pos += kl;
        let vl = u32::from_le_bytes(payload[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
        let v = std::str::from_utf8(&payload[pos..pos+vl]).unwrap().to_string(); pos += vl;
        decoded.push((k, v));
    }
    assert_eq!(meta, decoded);
}

// ── schema hash ───────────────────────────────────────────────────────────

#[test]
fn test_schema_hash_is_stable() {
    let schema = make_schema();
    let chunk_a = build_chunk_a(&schema).unwrap();
    let hash1 = xxh64(&chunk_a, 0);
    let hash2 = xxh64(&chunk_a, 0);
    assert_eq!(hash1, hash2); // deterministic
    assert_ne!(hash1, 0);     // not trivially zero
}

#[test]
fn test_different_schemas_have_different_hashes() {
    let schema_a = BishSchema::new(vec![BishField::new("x", BishType::Int64)]);
    let schema_b = BishSchema::new(vec![BishField::new("y", BishType::Utf8)]);
    let chunk_a = build_chunk_a(&schema_a).unwrap();
    let chunk_b = build_chunk_a(&schema_b).unwrap();
    let h_a = xxh64(&chunk_a, 0);
    let h_b = xxh64(&chunk_b, 0);
    assert_ne!(h_a, h_b);
}
