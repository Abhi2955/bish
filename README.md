# .bish — a columnar file format faster than Parquet

`.bish` is a columnar file format designed as a drop-in replacement for
Apache Parquet — fixing every major Parquet pain point while working
everywhere Parquet works today.

## Why .bish?

| Capability | Parquet | .bish |
|---|---|---|
| Cold open cost | Load entire footer (can be GBs) | Always 512 bytes |
| Point lookups | O(N) full column scan | O(log N) sparse row index |
| Partition skipping | Directory naming only | In-file partition index |
| Row deletes / updates | Full file rewrite | MVCC delete log |
| Compression stats | Min/max per column chunk | Histogram + HLL sketch |
| Compression codec | One codec per column, fixed | Per-page adaptive selection |
| Vector/embedding type | None | Native `Vector(dim)` + HNSW |
| High-ingest writes | Small file explosion | Write-ahead merge buffer |
| Tool compatibility | Thrift encoding | Arrow IPC — zero-copy |
| Bloom filters | Optional, inconsistent | Mandatory per column chunk |

## Quick start

```rust
use bish::{BishWriter, WriteOptions};
use bish::types::{BishSchema, BishField, BishType};
use bish::reader::BishReader;
use std::io::Cursor;

// Build a schema
let schema = BishSchema::new(vec![
    BishField::new("id",     BishType::Int64).with_sort_key(),
    BishField::new("city",   BishType::Utf8).with_partition_key(),
    BishField::new("amount", BishType::Float64),
    BishField::nullable("tag", BishType::Utf8),
]);

// Write
let mut bw = BishWriter::new(Cursor::new(Vec::new()), schema)?;
let mut rg = bw.new_row_group();
for i in 0..100_000i64 {
    rg.push_i64(0, Some(i))?;
    rg.push_str(1, Some("BLR"))?;
    rg.push_f64(2, Some(i as f64 * 1.5))?;
    rg.push_str(3, if i % 5 == 0 { None } else { Some("vip") })?;
}
bw.write_row_group(rg)?;
let raw = bw.finish_into_bytes()?;

// Read — full scan
let mut reader = BishReader::open(Cursor::new(raw))?;
let batch = reader.read_all()?;

// Read — projection (only columns 0 and 2)
let batch = reader.read_columns(&[0, 2])?;

// Read — zone-map predicate pushdown (WHERE id BETWEEN 1000 AND 2000)
let batch = reader.scan(&[0, 1], &[(0, 1000, 2000)])?;
```

## File format

```
┌─────────────────────────────────────┐
│  File header          (16 B)        │  ← magic BISH + version + feature flags
├─────────────────────────────────────┤
│  Row group 0                        │
│    Column chunk 0 … N               │  ← encoded + compressed pages
├─────────────────────────────────────┤
│  Row group 1 … K                    │
├─────────────────────────────────────┤
│  Footer chunk A  — Arrow IPC schema │  ← lazily loaded
│  Footer chunk B  — RG offsets       │
│  Footer chunk C  — column stats     │
│  Footer chunk D  — bloom filters    │
│  Footer chunk E  — user metadata    │
├─────────────────────────────────────┤
│  Super-footer         (512 B)       │  ← always at EOF−512, read first
└─────────────────────────────────────┘
```

**The 512-byte super-footer** is the brain — a fixed-size directory of every
other section. A reader seeks to `EOF − 512`, reads 512 bytes, and knows
where everything is without scanning the file.

See [`BISH-FORMAT-SPEC.md`](./BISH-FORMAT-SPEC.md) for the complete binary
format specification with byte-by-byte layouts.

## Type system

| Type | Maps to Arrow | Notes |
|---|---|---|
| `Int8/16/32/64` | Int8/16/32/64 | |
| `UInt8/16/32/64` | UInt8/16/32/64 | |
| `Float32/64` | Float32/64 | |
| `Boolean` | Boolean | bit-packed, 8 per byte |
| `Utf8` | Utf8 | length-prefixed UTF-8 |
| `Binary` | Binary | raw bytes |
| `Date32` | Date32 | days since epoch |
| `TimestampNs/Us/Ms/S` | Timestamp(unit, UTC) | |
| `Decimal128(p, s)` | Decimal(p, s) | |
| `Vector(dim)` | FixedSizeList\<f32\> | for ML embeddings |
| `List<T>` | List\<T\> | |
| `Struct(fields)` | Struct | |

## Encodings

Values are encoded before compression. The writer picks the best encoding
per page automatically:

| Condition | Encoding |
|---|---|
| Boolean column | Bitpack (8 bools / byte) |
| Sorted integers / timestamps | Delta + zigzag varint |
| Low cardinality (< 5% distinct) | RLE |
| Low cardinality strings | DeltaLength |
| Default | Plain |

## Compression

Codec is selected per page when `ADAPTIVE_CODEC` is enabled:

| Condition | Codec |
|---|---|
| Cold row group | Zstd level 9 |
| Sorted numeric column | LZ4 |
| Very low cardinality | Plain (encoding already tiny) |
| Default | Zstd level 1 |

## Implementation status

| Task | Status |
|---|---|
| T-01 Binary format spec | ✅ done |
| T-02 Schema + type system | ✅ done |
| T-03 Row group + column chunk writer | ✅ done |
| T-04 Zone map (min/max) per column chunk | ✅ done |
| T-05 Footer chunks A/B/C + 512B super-footer | ✅ done |
| T-06 BishReader with projection + zone-map pushdown | ✅ done |
| T-07 Round-trip integration test suite (111 tests in `tests/`) | ✅ done |
| T-08–T-12 DuckDB extension | 🔲 next |
| T-13 Bloom filters | 🔲 planned |
| T-14 In-file partition index | 🔲 planned |
| T-16 MVCC delete log | 🔲 planned |
| T-18–T-21 Python / Pandas / Polars / PyArrow | 🔲 planned |
| T-24–T-29 Kafka / Spark / Flink / S3 ingestion | 🔲 planned |

## Running tests

```bash
cargo test                        # all 111 tests
cargo test --test round_trip      # round-trip integration tests only (43)
cargo test --test test_footer     # footer chunk tests only
cargo test --test test_compress   # codec tests only
```

For one-command local setup + verification, run:

```bash
./setup_local.sh
```

## Dependencies

```toml
arrow2      = "0.17"   # Arrow IPC schema serialisation
xxhash-rust = "0.8"    # schema hash + bloom filters
crc32c      = "0.6"    # page and footer checksums
zstd        = "0.12"   # Zstd compression
lz4_flex    = "0.10"   # LZ4 compression
thiserror   = "1.0"    # error types
```

## Licence

MIT