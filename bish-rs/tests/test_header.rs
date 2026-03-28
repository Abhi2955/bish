use bish::{FileHeader, SuperFooter, FeatureFlags, ChunkRef, SectionRef, BISH_MAGIC, FILE_HEADER_SIZE, SUPER_FOOTER_SIZE};

#[test]
fn test_file_header_round_trip() {
    let mut flags = FeatureFlags::default();
    flags.set(FeatureFlags::BLOOM_FILTERS);
    flags.set(FeatureFlags::PARTITION_INDEX);

    let header = FileHeader::new(flags);
    let bytes = header.to_bytes();
    let decoded = FileHeader::from_bytes(&bytes).unwrap();

    assert_eq!(header, decoded);
    assert!(decoded.feature_flags.has(FeatureFlags::BLOOM_FILTERS));
    assert!(decoded.feature_flags.has(FeatureFlags::PARTITION_INDEX));
    assert!(!decoded.feature_flags.has(FeatureFlags::MVCC_DELETE_LOG));
}

#[test]
fn test_file_header_bad_magic() {
    let mut buf = [0u8; FILE_HEADER_SIZE];
    buf[0..4].copy_from_slice(b"PAR1"); // wrong magic
    assert!(FileHeader::from_bytes(&buf).is_err());
}

#[test]
fn test_super_footer_round_trip() {
    let sf = SuperFooter {
        version_major: 1,
        version_minor: 0,
        feature_flags: FeatureFlags(
            FeatureFlags::BLOOM_FILTERS | FeatureFlags::PARTITION_INDEX,
        ),
        row_group_count: 42,
        total_row_count: 1_000_000,
        schema_hash: 0xDEAD_BEEF_CAFE_1234,
        file_created_at: 1_700_000_000_000_000_000,
        file_modified_at: 1_700_000_001_000_000_000,
        chunk_a: ChunkRef { offset: 1024, length: 512, checksum: 0xABCD },
        chunk_b: ChunkRef { offset: 2048, length: 4096, checksum: 0x1234 },
        chunk_c: ChunkRef { offset: 8192, length: 2048, checksum: 0x5678 },
        chunk_d: ChunkRef { offset: 0, length: 0, checksum: 0 },
        chunk_e: ChunkRef { offset: 0, length: 0, checksum: 0 },
        partition_index: SectionRef { offset: 16, length: 128 },
        delete_log: SectionRef::default(),
        sparse_index: SectionRef::default(),
        vector_index: SectionRef::default(),
    };

    let bytes = sf.to_bytes();
    assert_eq!(bytes.len(), SUPER_FOOTER_SIZE);

    let decoded = SuperFooter::from_bytes(&bytes).unwrap();
    assert_eq!(sf, decoded);
}

#[test]
fn test_super_footer_checksum_caught() {
    let sf = SuperFooter {
        version_major: 1,
        version_minor: 0,
        feature_flags: FeatureFlags::default(),
        row_group_count: 1,
        total_row_count: 100,
        schema_hash: 0,
        file_created_at: 0,
        file_modified_at: 0,
        chunk_a: ChunkRef::default(),
        chunk_b: ChunkRef::default(),
        chunk_c: ChunkRef::default(),
        chunk_d: ChunkRef::default(),
        chunk_e: ChunkRef::default(),
        partition_index: SectionRef::default(),
        delete_log: SectionRef::default(),
        sparse_index: SectionRef::default(),
        vector_index: SectionRef::default(),
    };
    let mut bytes = sf.to_bytes();
    bytes[20] ^= 0xFF; // corrupt row_group_count
    assert!(SuperFooter::from_bytes(&bytes).is_err());
}

#[test]
fn test_feature_flags_required_rejection() {
    // Simulate a future file that sets a required-range bit this reader
    // doesn't know about
    let flags = FeatureFlags(1u64 << 32);
    assert!(flags.check_required_features().is_err());

    // Optional-range unknown bits are fine
    let flags = FeatureFlags(1u64 << 20);
    assert!(flags.check_required_features().is_ok());
}

#[test]
fn test_super_footer_is_512_bytes() {
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
fn test_bish_magic_constant() {
    assert_eq!(BISH_MAGIC, [0x42, 0x49, 0x53, 0x48]);
}
