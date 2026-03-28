use bish::types::{BishField, BishSchema, BishType, Codec, ZoneValue};

#[test]
fn test_type_round_trips_through_arrow() {
    let types = vec![
        BishType::Int8,
        BishType::Int16,
        BishType::Int32,
        BishType::Int64,
        BishType::UInt8,
        BishType::UInt16,
        BishType::UInt32,
        BishType::UInt64,
        BishType::Float32,
        BishType::Float64,
        BishType::Boolean,
        BishType::Utf8,
        BishType::Binary,
        BishType::Date32,
        BishType::TimestampNs,
        BishType::TimestampUs,
        BishType::TimestampMs,
        BishType::TimestampS,
        BishType::Decimal128 {
            precision: 18,
            scale: 4,
        },
        BishType::Vector { dim: 1536 },
        BishType::List(Box::new(BishType::Utf8)),
    ];

    for t in &types {
        let arrow_dt = t.to_arrow();
        let roundtripped = BishType::from_arrow(&arrow_dt)
            .unwrap_or_else(|e| panic!("from_arrow failed for {:?}: {}", t, e));
        assert_eq!(t, &roundtripped, "round-trip failed for {:?}", t);
    }
}

#[test]
fn test_schema_column_index() {
    let schema = BishSchema::new(vec![
        BishField::new("user_id", BishType::Utf8),
        BishField::new("city", BishType::Utf8),
        BishField::new("amount", BishType::Float64),
    ]);
    assert_eq!(schema.column_index("user_id"), Some(0));
    assert_eq!(schema.column_index("amount"), Some(2));
    assert_eq!(schema.column_index("missing"), None);
}

#[test]
fn test_schema_validate_duplicate_names() {
    let schema = BishSchema::new(vec![
        BishField::new("col", BishType::Int32),
        BishField::new("col", BishType::Utf8), // duplicate
    ]);
    assert!(schema.validate().is_err());
}

#[test]
fn test_schema_validate_sort_key_type() {
    // Vector is not orderable — should fail as sort key
    let schema = BishSchema::new(vec![BishField::new(
        "embedding",
        BishType::Vector { dim: 128 },
    )
    .with_sort_key()]);
    assert!(schema.validate().is_err());
}

#[test]
fn test_schema_validate_decimal_precision() {
    let bad = BishSchema::new(vec![BishField::new(
        "price",
        BishType::Decimal128 {
            precision: 0,
            scale: 2,
        },
    )]);
    assert!(bad.validate().is_err());

    let good = BishSchema::new(vec![BishField::new(
        "price",
        BishType::Decimal128 {
            precision: 10,
            scale: 2,
        },
    )]);
    assert!(good.validate().is_ok());
}

#[test]
fn test_zone_value_in_range() {
    let min = ZoneValue::Int(10);
    let max = ZoneValue::Int(100);
    assert!(ZoneValue::in_range(&min, &max, &ZoneValue::Int(50)));
    assert!(!ZoneValue::in_range(&min, &max, &ZoneValue::Int(200)));
    assert!(!ZoneValue::in_range(&min, &max, &ZoneValue::Int(5)));
}

#[test]
fn test_codec_adaptive_selection() {
    // Cold RG always gets ZSTD9
    assert_eq!(Codec::select_adaptive(true, 100, 1000, false), Codec::Zstd9);
    // Low cardinality → Plain (RLE handles it)
    assert_eq!(Codec::select_adaptive(false, 2, 1000, false), Codec::Plain);
    // Sorted → LZ4 (delta + fast)
    assert_eq!(Codec::select_adaptive(false, 500, 1000, true), Codec::Lz4);
    // Default → ZSTD1
    assert_eq!(
        Codec::select_adaptive(false, 500, 1000, false),
        Codec::Zstd1
    );
}

#[test]
fn test_byte_width() {
    assert_eq!(BishType::Int64.byte_width(), Some(8));
    assert_eq!(BishType::Float32.byte_width(), Some(4));
    assert_eq!(BishType::Vector { dim: 512 }.byte_width(), Some(2048));
    assert_eq!(BishType::Utf8.byte_width(), None);
}

#[test]
fn test_bloom_filter_support() {
    assert!(BishType::Utf8.supports_bloom_filter());
    assert!(BishType::Int64.supports_bloom_filter());
    assert!(!BishType::Float32.supports_bloom_filter());
    assert!(!BishType::Float64.supports_bloom_filter());
}

#[test]
fn test_field_builder_api() {
    let f = BishField::nullable("city", BishType::Utf8)
        .with_partition_key()
        .with_doc("ISO city name");

    assert!(f.nullable);
    assert!(f.is_partition_key());
    assert_eq!(f.metadata.get("bish.doc").unwrap(), "ISO city name");
}
