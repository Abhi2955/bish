# .bish Format — Claude Code Context File

> Hand this file to Claude Code at the start of every session.
> It contains the full project origin, design decisions, current
> implementation state, and the exact next steps to continue.

---

## 1. What is .bish?

`.bish` is a columnar file format designed to replace Apache Parquet. It
solves every major Parquet pain point while being a drop-in for all the
places Parquet is used today (Spark, Kafka, Flink, DuckDB, Pandas, Polars).

**The name:** `.bish` — short, easy to pronounce, easy to type.
**Magic bytes:** `BISH` (0x42 0x49 0x53 0x48) — lucky coincidence, they match.

### Why not just use Parquet?

| Problem | Parquet | .bish |
|---|---|---|
| Footer load cost | Entire footer always loaded (can be GBs on wide tables) | 512B super-footer → lazy load only needed chunks |
| Point lookups | O(N) full column scan always | O(log N) sparse row index |
| Partition awareness | Directory naming convention only | In-file partition index block |
| Row deletes | Full file rewrite required | MVCC delete log (append-only) |
| Stats quality | Min/max only | Histogram + HyperLogLog sketch per col chunk |
| Compression | One codec per column, fixed at write time | Per-page adaptive codec selection |
| Vector/embedding type | None (stored as `LIST<FLOAT>`, ANN impossible) | Native `Vector(dim)` type + optional HNSW index |
| High-ingest writes | Small file explosion | Write-ahead merge buffer |
| Tool compatibility | Thrift encoding — needs bespoke parser | Arrow IPC schema — zero-copy for DuckDB/Polars/DataFusion |
| Bloom filters | Optional, inconsistently implemented | Mandatory per column chunk when enabled |

### The key innovations

1. **512B super-footer** — always at EOF−512. Contains only RG count, schema
   hash, and byte offsets to lazily-loaded footer chunks. Cold open cost is
   always exactly 512 bytes regardless of file width or depth.

2. **In-file partition index** — stored at file head, not as directory naming.
   `WHERE partition_col = 'Bangalore'` skips 87% of file in O(1).

3. **Three-layer skip chain** — partition index → bloom filter → zone map.
   Each layer gates the next. Skip rate compounds.

4. **MVCC delete log** — 17-byte fixed entries appended on DELETE/UPDATE.
   First single-file columnar format with first-class mutation support.

5. **Arrow IPC schema in footer chunk A** — any Arrow-native tool deserialises
   it without bespoke parsing. DuckDB, Polars, DataFusion, cuDF work natively.

---

## 2. Format spec summary (full spec in BISH-FORMAT-SPEC.md)

### File layout

```
┌──────────────────────────────────────────────┐
│  File Header          (16 bytes, fixed)       │
├──────────────────────────────────────────────┤
│  Partition Index Block (optional)             │
├──────────────────────────────────────────────┤
│  Row Group 0                                  │
│    Column Chunk 0 … N                         │
│    Bloom Filter 0 … N  (if enabled)           │
├──────────────────────────────────────────────┤
│  Row Group 1 … K                              │
├──────────────────────────────────────────────┤
│  MVCC Delete Log       (optional)             │
├──────────────────────────────────────────────┤
│  Sparse Row Index      (optional)             │
├──────────────────────────────────────────────┤
│  Footer Chunk A — Arrow IPC Schema            │
│  Footer Chunk B — Row Group Offsets           │
│  Footer Chunk C — Column Statistics           │
│  Footer Chunk D — Bloom Filter Offsets        │
│  Footer Chunk E — User Metadata               │
├──────────────────────────────────────────────┤
│  Super-Footer         (512 bytes, fixed)      │
└──────────────────────────────────────────────┘
```

### File header (16 bytes)

```
0–3   BISH magic (0x42 0x49 0x53 0x48)
4–5   version_major (u16 LE)
6–7   version_minor (u16 LE)
8–15  feature_flags (u64 LE bitmask)
```

### Feature flags

```
bit 0  PARTITION_INDEX    — file has partition index block
bit 1  BLOOM_FILTERS      — column chunks have bloom filters
bit 2  MVCC_DELETE_LOG    — file has delete log section
bit 3  SPARSE_ROW_INDEX   — file has sparse row index
bit 4  ZONE_HISTOGRAMS    — col stats include histograms
bit 5  HLL_SKETCHES       — col stats include HLL cardinality
bit 6  ADAPTIVE_CODEC     — pages carry per-page codec tags
bit 7  VECTOR_INDEX       — file has HNSW vector index
bit 8  CHECKSUM_CRC32C    — pages carry CRC32C checksums
bits 9–31  RESERVED_OPTIONAL  (ignore unknown)
bits 32–63 RESERVED_REQUIRED  (reject if unknown set bit)
```

### Page header (20 bytes, precedes every data page)

```
0–3    compressed_len   (u32 LE)
4–7    uncompressed_len (u32 LE)
8–11   row_count        (u32 LE)
12     codec tag        (u8)    — 0=Plain 1=LZ4 2=Zstd1 3=Zstd9 4=Snappy 5=Brotli
13     encoding tag     (u8)    — 0=Plain 1=RLE 2=Bitpack 3=Delta 4=Dict 5=DeltaLength
14–15  page_flags       (u16)   — bit 1 = has_validity_bitmask
16–19  crc32c           (u32)   — of compressed bytes
[data] compressed + encoded values
```

### Footer chunk envelope (12 byte header + payload)

```
0–3   magic    — BSHA / BSHB / BSHC / BSHD / BSHE
4–7   length   (u32 LE) — compressed payload length
8     chunk_id (u8)     — A=0 B=1 C=2 D=3 E=4
9     codec    (u8)     — compression applied to payload
10–11 reserved (0x0000)
12–N  payload  — compressed bytes
```

### Footer chunk C — column stats (102 bytes per entry)

```
0–3    rg_id          (u32)
4–5    column_index   (u16)
6–13   zone_min_i64   (i64) — numeric: value; float: IEEE bits; string: 0
14–21  zone_max_i64   (i64)
22–53  zone_min_bytes (32B) — first 32 bytes of string min (zeroed for numeric)
54–85  zone_max_bytes (32B)
86–93  null_count     (u64)
94–101 row_count      (u64)
```

### Super-footer (512 bytes, always at EOF−512)

```
0–3     BISH magic
4–5     version_major
6–7     version_minor
8–15    feature_flags
16–23   row_group_count  (u64)
24–31   total_row_count  (u64)
32–39   schema_hash      (u64) — xxHash64 of chunk A bytes
40–47   file_created_at  (i64 nanoseconds)
48–55   file_modified_at (i64 nanoseconds)
56–71   chunk_a  ChunkRef (offset u64 + length u32 + checksum u32)
72–87   chunk_b  ChunkRef
88–103  chunk_c  ChunkRef
104–119 chunk_d  ChunkRef
120–135 chunk_e  ChunkRef
136–147 partition_index SectionRef (offset u64 + length u32)
148–159 delete_log      SectionRef
160–171 sparse_index    SectionRef
172–183 vector_index    SectionRef
184–499 reserved (all 0x00)
500–503 CRC32C of bytes 0–499
504–507 BISH magic (bookend)
508–511 reserved (0x00000000)
```

---

## 3. Rust crate — bish-rs

### Location
`/home/claude/bish-rs/` (or wherever the repo is cloned)

### Cargo.toml dependencies

```toml
[dependencies]
arrow2      = { version = "0.17", features = ["io_ipc"], default-features = false }
xxhash-rust = { version = "0.8", features = ["xxh64"] }
crc32c      = "0.6"
thiserror   = "=1.0.50"
zstd        = { version = "0.12", default-features = false, features = ["zstdmt"] }
lz4_flex    = "0.10"
getrandom   = "=0.2.10"
```

### Source files

| File | Lines | What it does |
|---|---|---|
| `src/error.rs` | 49 | `BishError` enum + `BishResult<T>` alias |
| `src/header.rs` | 533 | `FeatureFlags`, `FileHeader`, `ChunkRef`, `SectionRef`, `SuperFooter` |
| `src/types.rs` | 944 | `BishType`, `BishField`, `BishSchema`, `ZoneValue`, `Codec`, `Encoding` |
| `src/encoding.rs` | 471 | Plain/RLE/Delta/Bitpack/DeltaLength encoders + decoders + varint |
| `src/compress.rs` | 120 | `compress()` + `decompress()` dispatching on `Codec` tag |
| `src/writer.rs` | 678 | `RowGroupWriter`, `ColumnChunkWriter`, `WriteOptions`, `RowGroupMeta`, `ColumnChunkMeta`, `PageMeta` |
| `src/footer.rs` | 668 | `BishWriter`, `build_chunk_a/b/c/d/e`, `parse_chunk_b/c`, `ColStatEntry`, `RgDescriptor`, `FinishedFile` |
| `src/reader.rs` | 685 | `BishReader`, `RecordBatch`, `ColumnValues` |
| `src/lib.rs` | 29 | Crate root wiring all modules + re-exports |

**src total: 4,177 lines (no inline tests)**

### Test files (all in `tests/`)

| File | Tests | What it covers |
|---|---|---|
| `tests/round_trip.rs` | 43 | Full write→read integration: schema, all types, nulls, multi-RG, predicates, projection, stress |
| `tests/test_compress.rs` | 7 | `compress()` / `decompress()` round-trips for all codecs |
| `tests/test_encoding.rs` | 12 | Plain / RLE / Delta / DeltaLength / Bitpack / varint round-trips |
| `tests/test_footer.rs` | 23 | Footer chunk A/B/C/E build + parse, zone-map round-trips, super-footer serialisation |
| `tests/test_header.rs` | 7 | `FileHeader` / `SuperFooter` serialisation + checksum verification |
| `tests/test_types.rs` | 10 | Arrow type round-trips, schema validation, codec adaptive selection |
| `tests/test_writer.rs` | 9 | `RowGroupWriter` byte output, zone maps, null counts, page flushing, cold codec |

**Total: 111 tests · all passing**

### Key public API

```rust
// ── Write a .bish file ──────────────────────────────────────────────────────
use bish::{BishWriter, WriteOptions};
use bish::types::{BishSchema, BishField, BishType};

let schema = BishSchema::new(vec![
    BishField::new("id",     BishType::Int64).with_sort_key(),
    BishField::new("city",   BishType::Utf8).with_partition_key(),
    BishField::new("amount", BishType::Float64),
    BishField::nullable("tag", BishType::Utf8),
]);

let mut bw = BishWriter::new(File::create("data.bish")?, schema)?;
let mut rg = bw.new_row_group();
for i in 0..100_000i64 {
    rg.push_i64(0, Some(i))?;
    rg.push_str(1, Some("BLR"))?;
    rg.push_f64(2, Some(i as f64 * 1.5))?;
    rg.push_str(3, if i % 5 == 0 { None } else { Some("vip") })?;
}
bw.write_row_group(rg)?;
let summary = bw.finish()?;
// summary.total_row_count, summary.total_file_bytes, summary.rg_metas


// ── Read a .bish file ───────────────────────────────────────────────────────
use bish::reader::BishReader;

let mut reader = BishReader::open(File::open("data.bish")?)?;
println!("schema: {:?}", reader.schema());
println!("rows:   {}", reader.total_row_count());

// Read all columns, all rows
let batch = reader.read_all()?;
println!("{:?}", batch.col_i64(0));   // id column
println!("{:?}", batch.col_str(1));   // city column
println!("{:?}", batch.col_f64(2));   // amount column

// Projection: only read columns 0 and 2
let batch = reader.read_columns(&[0, 2])?;

// Scan with zone-map predicate: WHERE id BETWEEN 1000 AND 2000
let batch = reader.scan(&[0, 1, 2], &[(0, 1000, 2000)])?;
```

### Type system

| BishType | Arrow IPC | Rust | Notes |
|---|---|---|---|
| Int8/16/32/64 | Int8/16/32/64 | i8–i64 | |
| UInt8/16/32/64 | UInt8/16/32/64 | u8–u64 | |
| Float32/64 | Float32/64 | f32/f64 | bloom filter NOT supported (NaN) |
| Boolean | Boolean | bool | bit-packed 8/byte |
| Utf8 | Utf8 | String | length-prefixed bytes |
| Binary | Binary | Vec\<u8\> | |
| Date32 | Date32 | i32 | days since epoch |
| TimestampNs/Us/Ms/S | Timestamp(unit, UTC) | i64 | |
| Decimal128(p,s) | Decimal(p,s) | i128 | precision 1–38 |
| Vector(dim) | FixedSizeList\<f32\> | Vec\<f32\> | for embeddings / ANN |
| List\<T\> | List\<T\> | Vec\<T\> | |
| Struct(fields) | Struct | — | named sub-fields |

### Encoding selection (automatic, per page)

| Condition | Encoding chosen |
|---|---|
| Boolean column | Bitpack (8 bools/byte) |
| Sorted numeric | Delta + zigzag varint |
| Cardinality < 5% | RLE |
| Low-cardinality strings | DeltaLength |
| Everything else | Plain |

### Codec selection (adaptive when `ADAPTIVE_CODEC` flag set)

| Condition | Codec chosen |
|---|---|
| Cold row group | Zstd9 (max compression) |
| Sorted numeric | Lz4 (fast + delta already applied) |
| Cardinality < 5% | Plain (RLE made it tiny anyway) |
| Default | Zstd1 (good ratio, fast) |

---

## 4. Implementation status

### Done ✅

| Task | File | Tests |
|---|---|---|
| T-01 Binary format spec | BISH-FORMAT-SPEC.md | — |
| T-02 Schema + type system | types.rs | 14 |
| T-03 Row group + column chunk writer | writer.rs | 10 |
| T-04 Zone map (min/max) per column chunk | writer.rs | 2 (in T-03 tests) |
| T-05 Footer chunks A/B/C + 512B super-footer | footer.rs | 23 |
| T-06 BishReader struct + page decode | reader.rs | 43 (in tests/round_trip.rs) |

### Partial ⚠️

| Task | Status | What's missing |
|---|---|---|
| T-06 BishReader | Done ✅ | 43 round-trip tests in `tests/round_trip.rs` |
| T-07 Round-trip tests | Done ✅ | 43 tests: all types, nulls, multi-RG, predicate pushdown, projection |
| T-31 Adaptive codec | Heuristic in writer | Dedicated codec selection tests missing |
| T-35 Format spec doc | BISH-FORMAT-SPEC.md exists | Contributor guide + extension point docs missing |

### Not started ❌ (26 tasks)

**Phase 2 — DuckDB extension (T-08 to T-12)**
- T-08: DuckDB extension scaffold (`bish-duckdb` crate, TableFunction, libduckdb-sys link)
- T-09: Schema → DuckDB LogicalType mapping in BindFunction
- T-10: Column projection + zone map predicate pushdown
- T-11: Emit decoded Arrow arrays as DuckDB DataChunk
- T-12: First benchmark vs Parquet (cold scan, projection, filtered scan)

**Phase 3 — Bloom filters + partition + MVCC (T-13 to T-17)**
- T-13: Bloom filter per column chunk (xxHash, footer chunk D — stub exists, needs population)
- T-14: In-file partition index block at file head
- T-15: Three-layer pushdown in DuckDB extension
- T-16: MVCC delete log writer (17B fixed entries: rg_id, row_offset, version_ts, op)
- T-17: Delete mask on read + `bish compact` command

**Phase 4 — Library ecosystem (T-18 to T-23)**
- T-18: Python bindings via PyO3 — `bish.read_file()` / `bish.write_file()` → PyArrow Table
- T-19: Pandas `read_bish()` / `DataFrame.to_bish()` — mirrors `pandas.read_parquet`
- T-20: Polars `scan_bish()` LazyFrame source — projection + predicate from logical plan
- T-21: PyArrow Dataset API `FileFormat` source — unlocks Ibis, Arrow Flight, DataFusion
- T-22: Spark DataSource V2 connector — Scala ReadSupport + JNI to BishReader
- T-23: Publish `bish` crate to crates.io — semver, docs.rs, MIT licence

**Phase 5 — Ingestion pipelines (T-24 to T-29)**
- T-24: Kafka Connect sink — SinkTask → .bish on S3 on partition commit
- T-25: Spark Structured Streaming sink — StreamingWrite + DataWriterFactory
- T-26: Flink BulkWriter — `BulkWriter<RowData>` via JNI, FileSink exactly-once
- T-27: Object store writer (S3/GCS) — multi-part upload, atomic rename on footer seal
- T-28: `bish ingest` — CSV/JSON → .bish with schema inference
- T-29: `bish convert` — Parquet → .bish via arrow2, preserve schema + partition layout

**Phase 6 — Advanced features (T-30 to T-32)**
- T-30: Sparse row index — sorted (sort_key → rg_id, page_offset), O(log N) point lookups
- T-31: Per-page adaptive codec — codec selection heuristic exists, needs test coverage
- T-32: Zone histograms + HLL sketches — 64-bucket histogram + HLL per column chunk in chunk C

**Phase 7 — CLI + benchmarks (T-33 to T-35)**
- T-33: `bish` CLI — inspect, convert, compact, validate, ingest, bench subcommands
- T-34: Full benchmark suite vs Parquet — 7 dimensions
- T-35: Contributor guide + extension point docs

---

## 5. The critical path — what to do next

### Immediate: T-08 DuckDB extension (do this first)

T-06 and T-07 are complete — `BishReader` is implemented and validated by 43
round-trip integration tests in `tests/round_trip.rs`. The critical path now
moves to the DuckDB extension.

The `BishWriter::finish()` refactor (returns `(FinishedFile, W)`) is done.
`finish_into_bytes()` convenience method is also available for in-memory tests.
See `tests/round_trip.rs` for 43 passing tests covering all value types,
nullability, multi-row-group files, zone-map predicate pushdown, and projection.

### T-08 DuckDB extension

Create a new crate in the workspace:

```
bish-duckdb/
  Cargo.toml
  src/
    lib.rs      — extension entry point, register_bish_functions()
    table_fn.rs — TableFunction implementation, BindFunction, ScanFunction
    types.rs    — BishType → DuckDB LogicalType mapping
```

Key crates needed:
- `duckdb = { version = "0.10", features = ["bundled"] }` OR link to system libduckdb
- `arrow2` already in bish-rs — reuse via workspace dependency

The DuckDB Rust extension API:
```rust
// Entry point
#[no_mangle]
pub extern "C" fn bish_init(db: *mut c_void) {
    // register table function
}

// Table function bind — called once, returns schema to planner
fn bish_bind(info: &BindInfo) -> LogicalType { ... }

// Table function scan — called per batch, emits DataChunk
fn bish_scan(info: &ScanInfo, output: &mut DataChunk) { ... }
```

---

## 6. Design decisions and rationale

### Why the super-footer is exactly 512 bytes

Parquet footers can be gigabytes on wide tables (10,000 cols × 50,000 row
groups = 50M column chunk metadata entries). Every reader pays this cost on
every cold open. The .bish super-footer is a fixed-size directory that points
to lazily-loaded chunks — the reader pays 512 bytes always, then loads only
what it needs.

The 512-byte budget is tight but sufficient:
- 56 bytes of file-level metadata
- 80 bytes of chunk A–E refs (5 × 16 bytes)
- 48 bytes of optional section refs (4 × 12 bytes)
- 308 bytes reserved for future use
- 8 bytes checksum + magic bookend

### Why Arrow IPC schema instead of Thrift

Parquet uses Thrift for its footer encoding. Every tool needs a Thrift parser
AND a Parquet-specific schema mapper. Arrow IPC flatbuffer is the lingua franca
of the modern data stack. DuckDB, Polars, DataFusion, cuDF all have Arrow
deserialisation built in — the .bish reader in any of those tools is a thin
wrapper, not a full parser.

### Why zone maps are in footer chunk C, not in column chunk headers

If zone maps were in column chunk headers (like Parquet), a reader scanning
for matching row groups would need to seek to each column chunk, read a header,
check min/max, potentially skip, seek to next. That's O(N) seeks for N row
groups even before reading data.

With zone maps in footer chunk C (a single contiguous block), the reader
loads chunk C once (~KB for typical files), binary searches it, and then
does exactly one seek per qualifying row group. The seeks are replaced by
memory comparisons.

### Why 102 bytes per chunk C entry (not smaller)

The 32-byte string prefix fields (`zone_min_bytes`, `zone_max_bytes`) are the
key reason it's not smaller. For string columns, the i64 bits are meaningless —
you need actual bytes for lexicographic comparison. 32 bytes covers most
practical city names, UUIDs, identifiers, ISO codes. Truncation is safe: if
the prefix matches, the reader loads the page and does exact comparison there.

### Why RLE encoding stores full i64 per run, not deltas

RLE is used for low-cardinality columns (status codes, boolean-like integers,
enum-style values). The values repeat but aren't necessarily sequential —
a `status` column might have values `[0, 0, 0, 1, 1, 2, 2, 2]`. Delta encoding
would give `[0, 0, 1, 0, 1, 0, 0]` which compresses no better than plain.
Full i64 per RLE run costs 12 bytes per run regardless of value magnitude,
but the compression ratio comes from run length, not from small deltas.

### Why cold row groups exist

Data lakes have temporal skew — recent data is queried 100× more often than
archival data. Hot row groups (small, Zstd1, recent data) allow fast access
to the tail of the file. Cold row groups (large, Zstd9, archival) maximise
compression for rarely-accessed data. The temperature byte in the RG metadata
lets the reader prioritise hot RGs in mixed queries.

### Why the MVCC delete log is a single append-only region

Not splitting it into per-row-group delete logs keeps write amplification at
zero — a DELETE never rewrites any data bytes, just appends 17 bytes to the
log region. The cost is paid on read: the reader must filter the delete log
for each row group it scans. For typical workloads (infrequent deletes vs.
frequent reads) this is the right trade-off. `bish compact` clears the debt.

### Why bloom filters are mandatory (when enabled) rather than optional

In Parquet, bloom filters are optional per column, inconsistently supported
across tools, and frequently missing on the columns where they'd help most.
In .bish, if the `BLOOM_FILTERS` feature flag is set, every column chunk
has one. This makes the predicate pushdown path deterministic — a reader
either always has blooms or never does, and the DuckDB extension can assume
their presence without per-column checks.

---

## 7. Module-by-module implementation notes

### error.rs

`BishError` has these variants:
- `Io(std::io::Error)` — I/O failures
- `Arrow(arrow2::error::Error)` — Arrow IPC parse failures
- `InvalidMagic([u8; 4])` — wrong magic bytes
- `UnsupportedVersion { major, minor }` — version too new
- `ChecksumMismatch` — CRC32C mismatch on super-footer or page
- `SchemaHashMismatch` — xxHash64 of chunk A doesn't match super-footer
- `UnsupportedType(String)` — Arrow type with no .bish equivalent
- `InvalidSchema(String)` — schema validation failure
- `UnknownCodec(u8)` — unknown codec tag byte
- `UnknownEncoding(u8)` — unknown encoding tag byte
- `ColumnNotFound(String)` — column index or name not in schema
- `UnsupportedRequiredFeature(u64)` — unknown required-range feature flag
- `Decoding(String)` — page-level decode failure

### header.rs

`FeatureFlags` is a newtype around `u64`. The `check_required_features()`
method rejects files with unknown bits in bits 32–63. Optional bits (0–31)
are silently ignored by old readers.

`SuperFooter::from_bytes()` verifies in this order:
1. magic_start == BISH
2. magic_end == BISH (at offset 504)
3. CRC32C of bytes 0–499 matches stored checksum at offset 500
4. version_major <= VERSION_MAJOR
5. feature_flags.check_required_features()

This order matters — don't use any offset from the super-footer before
verifying the checksum, because a corrupt pointer could cause an out-of-bounds
seek.

### types.rs

`BishType::to_arrow()` and `BishType::from_arrow()` are the bidirectional
Arrow IPC mapping. The tricky cases:
- `Vector { dim }` ↔ `FixedSizeList<f32>` — only FixedSizeList with f32 inner
  type maps to Vector; other FixedSizeLists are rejected
- `Decimal128 { precision, scale }` ↔ `ArrowDataType::Decimal(usize, usize)`
  — note arrow2 v0.17 uses `usize` not `u8/i8`, requires casting
- `LargeUtf8` and `LargeBinary` from Arrow map to `Utf8` and `Binary` in bish
  (we don't distinguish large vs. normal; all strings are variable-length)

`BishSchema::validate()` checks:
- No empty field names
- No duplicate field names
- At most one sort key field
- Sort key must be a partitionable (orderable) type
- Partition keys must be partitionable types
- Decimal128 precision in range 1–38
- Vector dimension > 0

### encoding.rs

**Varint encoding** uses LEB128 (unsigned). Signed deltas are zigzag-mapped
first: `(delta << 1) ^ (delta >> 63)` — this makes small negative numbers
small unsigned numbers, which then encode to fewer varint bytes.

**Delta encoding layout:**
- First 8 bytes: absolute first value (LE i64)
- Then N-1 zigzag-encoded varints, one per delta

**DeltaLength layout (for variable-length strings):**
- 4 bytes: count (u32 LE)
- N zigzag varints: length deltas (first length is absolute, rest are deltas)
- Concatenated raw bytes of all values

**Validity bitmask:** bit i = 1 means valid (not null). Same bit-packing as
boolean columns. Only written to a page when there are actual nulls in that
page AND the column is nullable.

### writer.rs

`ColumnChunkWriter` has separate typed buffers (`i64_buf`, `f32_buf`,
`f64_buf`, `bool_buf`, `bytes_buf`). Only one is populated per instance.
The zone map (`zone_min`, `zone_max` as `ZoneValue`) is updated live on
every push — no second pass.

Page flush threshold: whichever hits first:
- `page_row_target` rows (default 8192)
- `page_byte_target` bytes (default 1MB estimated)

`RowGroupWriter::finish()` calls each `ColumnChunkWriter::finish()` in
schema order. The `current_file_offset: &mut u64` parameter is threaded
through so every byte written advances the shared counter. This is how
`ColumnChunkMeta::file_offset` gets accurate absolute values.

### footer.rs

`BishWriter` is the public write entry point. It writes the file header in
`new()` and writes footer chunks + super-footer in `finish()`. The footer
is written in this order: chunks are built in memory, their byte lengths
are known, so `ChunkRef::offset` can be computed before any chunk is written
to disk (pre-calculation pattern).

`FinishedFile` is returned by `finish()`. It holds `total_row_count`,
`row_group_count`, `total_file_bytes`, `schema_hash`, and `rg_metas`.

**Known issue:** `BishWriter::finish()` consumes `self` and returns
`FinishedFile` but does NOT return the inner sink. To get the written bytes
back (needed for round-trip tests), refactor to:
```rust
pub fn finish(self) -> BishResult<(FinishedFile, W)>
```
This is the first thing to change before adding reader tests.

### reader.rs (written but untested)

`BishReader::open()` protocol:
1. Read 16-byte file header — verify BISH magic
2. Seek to `EOF - 512` — read 512-byte super-footer
3. Verify super-footer CRC32C and magic bookends
4. Verify schema hash matches chunk A
5. Load chunk A → `BishSchema` (decompress + Arrow IPC parse)
6. Load chunk B → `Vec<RgDescriptor>` (decompress + parse)
7. Load chunk C → `Vec<ColStatEntry>` (decompress + parse)
8. Return `BishReader` — no data pages read yet

`BishReader::scan()` protocol:
1. For each row group in `rg_descriptors`:
   a. Call `rg_passes_predicates()` — zone map check against i64 predicates
   b. If passes: call `read_row_group()` with projected column indices
2. Concatenate all surviving `RecordBatch`es

`read_column_chunk()` reads pages sequentially until `expected_rows` are
decoded. Each page: read 20-byte header, read compressed data, verify CRC32C,
decompress, split off validity bitmask, decode values by `(encoding, data_type)`.

**Page decode dispatch:**
- `Boolean` → bitpack decode
- `Float32/64` → plain LE bytes
- `Utf8/Binary` → DeltaLength or Plain (4-byte length prefix)
- All other (integer/temporal) → Plain/RLE/Delta based on encoding tag

---

## 8. Testing approach

### Test distribution (all tests are in `tests/`)

All tests live in `bish-rs/tests/` — no inline `#[cfg(test)]` blocks in source
files. This keeps source files clean and all tests runnable as integration tests.

| Test file | Tests | What it covers |
|---|---|---|
| `tests/round_trip.rs` | 43 | Write→read full pipeline: all types, nulls, multi-RG, predicates, projection |
| `tests/test_compress.rs` | 7 | Codec round-trips (LZ4, Zstd1, Zstd9, Plain) |
| `tests/test_encoding.rs` | 12 | Plain, RLE, Delta, DeltaLength, Bitpack, varint |
| `tests/test_footer.rs` | 23 | Chunk A/B/C/E build+parse, zone maps, super-footer serialisation |
| `tests/test_header.rs` | 7 | FileHeader, SuperFooter, FeatureFlags, CRC32C |
| `tests/test_types.rs` | 10 | Arrow type mapping, schema validation, codec selection |
| `tests/test_writer.rs` | 9 | RowGroupWriter output, zone maps, null counts, multi-page |
| **total** | **111** | |

### Test patterns used

Every test file follows the same pattern:
1. Imports via `use bish::...` (external API only — no `use super::*`)
2. Helper functions at file top level (not nested in a `mod`)
3. `#[test]` functions directly at top level

`BishWriter::finish()` returns `(FinishedFile, W)` and `finish_into_bytes()`
extracts the raw `Vec<u8>` from a `Cursor<Vec<u8>>` writer — both available
for tests needing the written bytes.

---

## 9. What "done" means for each upcoming task

### T-06 / T-07 — Reader tests + round-trip
Done when: `cargo test` passes with the 12 tests listed above. All value types
round-trip correctly. Zone map predicate pushdown demonstrably skips row groups.

### T-08 — DuckDB extension scaffold
Done when: `LOAD 'bish'; SELECT COUNT(*) FROM 'data.bish'` returns the correct
row count in DuckDB CLI. Schema is visible via `DESCRIBE SELECT * FROM 'data.bish'`.

### T-12 — First benchmark
Done when: A markdown table exists in the repo showing wall time and bytes read
for the same TPC-H or synthetic dataset in both Parquet and .bish for: full scan,
2-column projection, single-predicate filtered scan.

### T-18 — Python bindings
Done when: `pip install bish` (from local wheel), then:
```python
import bish
df = bish.read_file("data.bish")          # returns pyarrow.Table
bish.write_file(df, "out.bish")           # roundtrip
import pandas as pd
df = bish.read_bish("data.bish")          # returns pd.DataFrame
```

### T-23 — crates.io publish
Done when: `cargo add bish` works, `docs.rs/bish` shows the public API,
`BishWriter` and `BishReader` work in a fresh project with no local path deps.

---

## 10. Repository structure (target)

```
bish/
├── BISH-FORMAT-SPEC.md      ← full binary format specification (done)
├── CONTRIBUTING.md          ← contributor guide (TODO)
├── README.md                ← benchmark results + quick start (TODO)
├── Cargo.toml               ← workspace root
├── bish-rs/                 ← core Rust library (in progress)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── error.rs
│       ├── header.rs
│       ├── types.rs
│       ├── encoding.rs
│       ├── compress.rs
│       ├── writer.rs
│       ├── footer.rs
│       └── reader.rs
├── bish-cli/                ← bish inspect/convert/compact/bench (TODO)
│   ├── Cargo.toml
│   └── src/main.rs
├── bish-duckdb/             ← DuckDB extension (TODO)
│   ├── Cargo.toml
│   └── src/lib.rs
├── bish-python/             ← PyO3 Python bindings (TODO)
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/lib.rs
└── benches/                 ← benchmarks vs Parquet (TODO)
    ├── bench_scan.rs
    ├── bench_projection.rs
    └── bench_point_lookup.rs
```

---

## 11. Commands reference

```bash
# Run all tests
cargo test --lib

# Run a specific test
cargo test test_round_trip_basic

# Build only (no tests)
cargo build

# Check without building
cargo check

# Format code
cargo fmt

# Lint
cargo clippy

# Generate docs
cargo doc --open
```

---

## 12. Session startup checklist for Claude Code

When starting a new Claude Code session on this project:

1. Read this file first (`BISH_CONTEXT.md`)
2. Run `cargo test` — confirm 111 tests pass (all in `tests/`)
3. Check `git log --oneline -5` for recent changes
4. Pick up from the next pending task (currently T-08 — DuckDB extension)
5. After making changes, always run `cargo test` before considering a task done

The most important invariant: **never break the 111 existing tests**.
New code must be additive. All tests live in `bish-rs/tests/` — no inline
`#[cfg(test)]` blocks in source files.

---

*Last updated: session where T-06/T-07 (BishReader + round-trip tests) were completed and all tests migrated to `tests/`.*
*Current test count: 111 passing (in `bish-rs/tests/`).*
*Next task: T-08 — DuckDB extension scaffold.*