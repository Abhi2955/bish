# BISH FORMAT SPEC
## .bish Binary File Format — Version 1.0

**Status:** Draft  
**Magic bytes:** `BISH` (0x42 0x49 0x53 0x48)  
**Extension:** `.bish`  
**Endianness:** Little-endian throughout  
**Alignment:** No padding or alignment requirements — all fields are packed

---

## 1. File Layout Overview

A `.bish` file is laid out as a linear sequence of sections, always in this order:

```
┌─────────────────────────────────────────────┐
│  File Header          (16 bytes, fixed)      │
├─────────────────────────────────────────────┤
│  Partition Index Block (optional)            │
├─────────────────────────────────────────────┤
│  Row Group 0                                 │
│    Column Chunk 0 … N                        │
│    Bloom Filter 0 … N  (if enabled)          │
├─────────────────────────────────────────────┤
│  Row Group 1                                 │
│    …                                         │
├─────────────────────────────────────────────┤
│  Row Group K                                 │
├─────────────────────────────────────────────┤
│  MVCC Delete Log       (optional)            │
├─────────────────────────────────────────────┤
│  Sparse Row Index      (optional)            │
├─────────────────────────────────────────────┤
│  Footer Chunk A — Schema                     │
│  Footer Chunk B — Row Group Offsets          │
│  Footer Chunk C — Column Statistics          │
│  Footer Chunk D — Bloom Filter Offsets       │
│  Footer Chunk E — User Metadata              │
├─────────────────────────────────────────────┤
│  Super-Footer         (512 bytes, fixed)     │
└─────────────────────────────────────────────┘
```

**Read protocol:** A reader always seeks to `EOF − 512`, reads the super-footer,
then uses the chunk offsets inside it to load only the footer chunks it needs.
No reader ever needs to scan forward from byte 0 to open a file.

---

## 2. File Header (16 bytes, fixed)

Always at byte offset 0. Always exactly 16 bytes.

```
Offset  Size  Type    Field
──────  ────  ──────  ───────────────────────────────────────────────
0       4     u8[4]   magic          — always 0x42 0x49 0x53 0x48 ("BISH")
4       2     u16     version_major  — breaking format changes
6       2     u16     version_minor  — backward-compatible additions
8       8     u64     feature_flags  — bitmask of optional capabilities
```

### 2.1 Version semantics

| version_major | version_minor | Meaning |
|---|---|---|
| 1 | 0 | Initial release — this spec |
| 1 | x | Backward-compatible additions (readers MUST tolerate unknown minor features) |
| 2 | 0 | Breaking change — readers MAY reject if major unsupported |

A reader MUST reject a file if `version_major` is greater than the highest
major version it supports. A reader MUST NOT reject a file solely because
`version_minor` is unknown — it must read what it can.

### 2.2 Feature flags (u64 bitmask)

Each bit enables an optional section or capability. A reader that encounters
an unknown set bit in a non-reserved position SHOULD warn but MUST NOT error,
unless the bit is in the `REQUIRED` range (bits 32–63).

```
Bit   Hex          Name                      Description
───   ──────────   ───────────────────────   ──────────────────────────────────────
0     0x0000_0001  PARTITION_INDEX           File contains a partition index block
1     0x0000_0002  BLOOM_FILTERS             Column chunks have bloom filters
2     0x0000_0004  MVCC_DELETE_LOG           File contains a delete log section
3     0x0000_0008  SPARSE_ROW_INDEX          File contains a sparse row index block
4     0x0000_0010  ZONE_HISTOGRAMS           Column stats include value histograms
5     0x0000_0020  HLL_SKETCHES              Column stats include HyperLogLog sketches
6     0x0000_0040  ADAPTIVE_CODEC            Pages carry per-page codec tags
7     0x0000_0080  VECTOR_INDEX              File contains an HNSW vector index block
8     0x0000_0100  CHECKSUM_CRC32C           All pages carry CRC32C checksums
9–31  —            RESERVED_OPTIONAL         Readers must ignore unknown bits here
32–63 —            RESERVED_REQUIRED         Readers MUST reject if any unknown bit set
```

---

## 3. Partition Index Block (optional)

Present only when feature flag `PARTITION_INDEX` (bit 0) is set.
Immediately follows the file header at byte offset 16.

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u32     block_length       — total byte length of this block
4       2      u16     num_partition_keys — number of partition columns
6       2      u16     num_partitions     — number of distinct partitions
8       var    Entry[] partition_entries  — one per partition
```

Each `Entry`:
```
Size   Type    Field
─────  ──────  ──────────────────────────────────────────────────────
2      u16     key_len           — byte length of partition key value
var    u8[]    key_bytes         — UTF-8 encoded partition key value
8      u64     first_rg_offset   — byte offset of first row group in this partition
4      u32     num_row_groups    — how many row groups belong to this partition
8      u64     row_count         — total rows in this partition
```

**Usage:** A reader evaluating `WHERE partition_col = 'Bangalore'` encodes the
literal as UTF-8, binary-searches the partition entries, and seeks directly to
`first_rg_offset`. All other partitions are skipped with zero I/O.

---

## 4. Row Groups

A row group is a horizontal slice of the table — a set of rows stored as
independent column chunks side by side. Row groups have no fixed-size header;
their position is known from footer chunk B.

### 4.1 Row Group metadata (in footer chunk B, not in the data stream)

```
Field             Type    Description
───────────────   ──────  ───────────────────────────────────────────────
rg_id             u32     Monotonically increasing, 0-based
row_count         u64     Number of rows in this row group
byte_offset       u64     Byte offset of first column chunk in this RG
byte_length       u64     Total bytes consumed by all column chunks in this RG
temperature       u8      0 = hot (small, light compression), 1 = cold (large, heavy)
col_chunk_count   u16     Number of column chunks
col_chunk_offsets u64[]   One byte offset per column chunk (relative to file start)
```

### 4.2 Column Chunk

Each column chunk stores all values for one column within one row group.

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u32     chunk_length      — total byte length of this chunk
4       2      u16     column_index      — index into schema field list
6       1      u8      default_codec     — codec used unless ADAPTIVE_CODEC flag set
7       1      u8      chunk_flags       — bit 0: has_nulls, bit 1: is_dictionary
8       8      i64     zone_min_i64      — min value cast to i64 (or IEEE bits for float)
16      8      i64     zone_max_i64      — max value cast to i64 (or IEEE bits for float)
24      4      u32     null_count        — number of null values in this chunk
28      4      u32     page_count        — number of data pages
32      var    Page[]  pages             — encoded data pages
```

### 4.3 Page

A page is the smallest unit of I/O within a column chunk.

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u32     page_length       — compressed byte length of this page
4       4      u32     uncompressed_len  — length after decompression
8       4      u32     row_count         — number of rows in this page
12      1      u8      codec             — codec tag (see §4.4); overrides chunk default
13      1      u8      encoding          — encoding tag (see §4.5)
14      2      u16     page_flags        — bit 0: is_dict_page, bit 1: has_checksum
16      4      u32     checksum          — CRC32C of compressed bytes (if flag set)
20      var    u8[]    data              — compressed + encoded page data
```

### 4.4 Codec tags (u8)

```
Value  Name       Description
─────  ─────────  ──────────────────────────────────────────────────
0x00   PLAIN      No compression — raw bytes
0x01   LZ4        LZ4 block format (fast, moderate ratio)
0x02   ZSTD_1     ZSTD level 1 (fast write, good ratio)
0x03   ZSTD_9     ZSTD level 9 (slow write, best ratio — for cold RGs)
0x04   SNAPPY     Snappy (Parquet compatibility mode)
0x05   BROTLI     Brotli level 6 (high ratio for UTF-8 heavy columns)
0x06–0xEF RESERVED
0xF0–0xFF VENDOR   Vendor-specific codecs (must be negotiated out of band)
```

**Adaptive codec selection heuristic** (when `ADAPTIVE_CODEC` flag is set):
The writer samples the first 512 values of each page and applies this decision tree:

```
cardinality < 50    → RLE (repetitive categoricals)
is_sorted numeric   → DELTA encoding + LZ4
is_random uuid/hash → LZ4 (fastest for incompressible)
column is cold RG   → ZSTD_9 (highest compression)
default             → ZSTD_1
```

### 4.5 Encoding tags (u8)

```
Value  Name          Description
─────  ────────────  ─────────────────────────────────────────────────
0x00   PLAIN         Raw values, no encoding transform
0x01   RLE           Run-length encoding (value, run_length pairs)
0x02   BITPACK       Bit-packing for integer columns with small range
0x03   DELTA         Delta encoding for sorted or near-sorted integers
0x04   DICT          Dictionary encoding — requires a preceding dict page
0x05   DELTA_LENGTH  Delta encoding of string lengths + plain bytes
```

---

## 5. Bloom Filter (per column chunk, optional)

Present when feature flag `BLOOM_FILTERS` (bit 1) is set.
Stored immediately after the column chunk data it belongs to.

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u32     filter_length     — byte length of this bloom filter
4       1      u8      hash_algo         — 0x00 = xxHash64, 0x01 = MurmurHash3
5       1      u8      num_hash_funcs    — k (number of hash functions)
6       2      u16     reserved          — must be 0x0000
8       4      u32     num_bits          — m (size of bit array in bits)
12      var    u8[]    bit_array         — ceil(num_bits / 8) bytes
```

**False positive rate:** Writers SHOULD target ≤ 1% FPR. At 1% FPR,
`num_bits ≈ 9.6 × n` where n is the number of distinct values.

**Usage:** For `WHERE col = ?` queries, the reader checks the bloom filter
before decompressing any page data. If the filter returns false, the entire
column chunk is skipped.

---

## 6. MVCC Delete Log (optional)

Present when feature flag `MVCC_DELETE_LOG` (bit 2) is set.
Located after all row groups, before the sparse row index.

### 6.1 Delete log header

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u8[4]   section_magic     — 0x44 0x45 0x4C 0x47 ("DELG")
4       4      u32     entry_count       — number of log entries
8       8      u64     log_length        — total byte length of entry array
16      8      i64     min_version_ts    — earliest version_ts in this log
24      8      i64     max_version_ts    — latest version_ts in this log
```

### 6.2 Delete log entry (17 bytes each, fixed size)

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u32     rg_id             — row group containing the affected row
4       4      u32     row_offset        — row index within that row group (0-based)
8       8      i64     version_ts        — Unix nanosecond timestamp of this operation
16      1      u8      op                — 0x00 = DELETE, 0x01 = UPDATE_PTR
```

**Read protocol:** When scanning row group `rg_id`, the reader loads all
log entries where `entry.rg_id == rg_id`, builds a bitmask of deleted
`row_offset` values, and applies it as a filter mask over decoded output rows.

**Compaction:** `bish compact file.bish` rewrites only row groups that have
log entries, physically removes deleted rows, clears the log, and updates
all footer chunk offsets. The compacted file has no MVCC log.

---

## 7. Sparse Row Index (optional)

Present when feature flag `SPARSE_ROW_INDEX` (bit 3) is set.
Immediately follows the MVCC delete log (or row groups if no log).

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u8[4]   section_magic     — 0x52 0x49 0x44 0x58 ("RIDX")
4       4      u32     entry_count       — number of index entries
8       2      u16     sort_key_col      — column index of the sort key
10      2      u16     key_byte_len      — fixed byte length of each key (0 = variable)
12      var    Entry[] index_entries     — sorted by key_bytes ascending
```

Each `Entry` (fixed-length keys only, `key_byte_len > 0`):
```
Size         Type    Field
───────────  ──────  ──────────────────────────────────────────────
key_byte_len u8[]    key_bytes         — encoded sort key value
4            u32     rg_id             — row group containing this key range
4            u32     page_offset_in_rg — byte offset of page within that RG
```

**Usage:** Point lookup on `WHERE sort_key = X` binary-searches the index
entries by `key_bytes`. Returns `(rg_id, page_offset_in_rg)` — reader seeks
directly to that page. Complexity: O(log N) vs O(N) full scan in Parquet.

---

## 8. Footer Chunks

Footer chunks are the metadata payload of the file. Each is independently
loadable — readers load only the chunks they need for a given query.

### 8.1 Footer chunk envelope

Every footer chunk is wrapped in this envelope:

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u8[4]   chunk_magic       — see per-chunk magic below
4       4      u32     chunk_length      — byte length of payload (excluding envelope)
8       1      u8      chunk_id          — A=0, B=1, C=2, D=3, E=4
9       1      u8      codec             — compression codec for this chunk's payload
10      2      u16     reserved          — must be 0x0000
12      var    u8[]    payload           — compressed chunk payload
```

### 8.2 Chunk A — Schema

**Magic:** `BSHA` (0x42 0x53 0x48 0x41)

Payload is an Arrow IPC schema message (flatbuffer). Contains:

- All field names
- All field types (mapped to Arrow types)
- Field nullability flags
- Field-level key-value metadata (e.g. `{"bish.sort_key": "true"}`)
- File-level key-value metadata (e.g. `{"bish.created_by": "bish-rs 1.0.0"}`)

Using Arrow IPC schema means any Arrow-native tool (DuckDB, Polars, DataFusion,
cuDF) can deserialise the schema with zero bespoke parsing.

### 8.3 Chunk B — Row Group Offsets

**Magic:** `BSHB` (0x42 0x53 0x48 0x42)

Payload: array of row group descriptors (one per row group, in `rg_id` order):

```
Size   Type    Field
─────  ──────  ──────────────────────────────────────────────────────
4      u32     rg_id
8      u64     row_count
8      u64     byte_offset          — offset from file start
8      u64     byte_length
1      u8      temperature          — 0 = hot, 1 = cold
2      u16     col_chunk_count
var    u64[]   col_chunk_offsets    — one per column, from file start
```

### 8.4 Chunk C — Column Statistics

**Magic:** `BSHC` (0x42 0x53 0x48 0x43)

Payload: array of column stat entries (one per column per row group):

```
Size   Type    Field
─────  ──────  ──────────────────────────────────────────────────────
4      u32     rg_id
2      u16     column_index
8      i64     zone_min_i64
8      i64     zone_max_i64
4      u32     null_count
8      u64     distinct_count_hll   — HLL cardinality estimate (if HLL_SKETCHES set)
var    u8[]    histogram_bytes      — 64-bucket histogram (if ZONE_HISTOGRAMS set)
```

### 8.5 Chunk D — Bloom Filter Offsets

**Magic:** `BSHD` (0x42 0x53 0x48 0x44)

Payload: array of bloom filter location entries:

```
Size   Type    Field
─────  ──────  ──────────────────────────────────────────────────────
4      u32     rg_id
2      u16     column_index
8      u64     bloom_byte_offset    — offset from file start
4      u32     bloom_byte_length
```

### 8.6 Chunk E — User Metadata

**Magic:** `BSHE` (0x42 0x53 0x48 0x45)

Payload: array of key-value pairs, arbitrary user-defined metadata:

```
Size   Type    Field
─────  ──────  ──────────────────────────────────────────────────────
2      u16     key_len
var    u8[]    key_bytes            — UTF-8
4      u32     value_len
var    u8[]    value_bytes          — arbitrary bytes
```

---

## 9. Super-Footer (512 bytes, fixed)

Always at `EOF − 512`. Always exactly 512 bytes. This is the first and
often only metadata a reader needs to touch on cold open.

```
Offset  Size   Type    Field
──────  ─────  ──────  ──────────────────────────────────────────────
0       4      u8[4]   magic_start       — 0x42 0x49 0x53 0x48 ("BISH")
4       2      u16     version_major     — must match file header
6       2      u16     version_minor     — must match file header
8       8      u64     feature_flags     — must match file header
16      8      u64     row_group_count   — total number of row groups in file
24      8      u64     total_row_count   — total rows across all row groups
32      8      u64     schema_hash       — xxHash64 of chunk A payload (integrity check)
40      8      u64     file_created_at   — Unix nanoseconds
48      8      u64     file_modified_at  — Unix nanoseconds (updated on compact)

── Chunk directory (one entry per footer chunk, A through E) ──────────

56      8      u64     chunk_a_offset    — byte offset of chunk A from file start
64      4      u32     chunk_a_length    — byte length of chunk A (envelope + payload)
68      4      u32     chunk_a_checksum  — CRC32C of chunk A bytes

72      8      u64     chunk_b_offset
80      4      u32     chunk_b_length
84      4      u32     chunk_b_checksum

88      8      u64     chunk_c_offset
96      4      u32     chunk_c_length
100     4      u32     chunk_c_checksum

104     8      u64     chunk_d_offset
112     4      u32     chunk_d_length
116     4      u32     chunk_d_checksum

120     8      u64     chunk_e_offset
128     4      u32     chunk_e_length
132     4      u32     chunk_e_checksum

── Optional section directory ─────────────────────────────────────────

136     8      u64     partition_index_offset    — 0 if not present
144     4      u32     partition_index_length
148     8      u64     delete_log_offset          — 0 if not present
156     4      u32     delete_log_length
160     8      u64     sparse_index_offset        — 0 if not present
168     4      u32     sparse_index_length
176     8      u64     vector_index_offset        — 0 if not present
184     4      u32     vector_index_length

── Reserved space ─────────────────────────────────────────────────────

192     308    u8[]    reserved          — must be 0x00, reserved for future use

── Trailer ────────────────────────────────────────────────────────────

500     4      u32     super_footer_checksum  — CRC32C of bytes 0–499 of super-footer
504     4      u8[4]   magic_end         — 0x42 0x49 0x53 0x48 ("BISH")
508     4      u32     reserved_trailer   — must be 0x00000000
```

**Total: 512 bytes.**

### 9.1 Read protocol (pseudocode)

```rust
fn open(path: &str) -> BishFile {
    let f = File::open(path)?;
    let file_len = f.metadata()?.len();

    // Single seek — always exactly 512 bytes
    f.seek(SeekFrom::End(-512))?;
    let sf: SuperFooter = f.read_exact(512)?.parse()?;

    assert_eq!(&sf.magic_start, b"BISH");
    assert_eq!(&sf.magic_end,   b"BISH");
    verify_crc32c(&sf)?;

    BishFile { file: f, super_footer: sf, file_len }
}

fn read_schema(bf: &BishFile) -> Schema {
    // Load only chunk A — typically a few KB
    let chunk_a = bf.load_chunk(bf.super_footer.chunk_a_offset,
                                bf.super_footer.chunk_a_length)?;
    verify_crc32c(&chunk_a, bf.super_footer.chunk_a_checksum)?;
    arrow_ipc::parse_schema(chunk_a.payload)
}

fn scan(bf: &BishFile, columns: &[usize], predicate: &Expr) -> RecordBatchIter {
    // Load chunk B (RG offsets) + chunk C (stats) only
    let rg_meta   = bf.load_chunk_b()?;
    let col_stats = bf.load_chunk_c()?;

    let relevant_rgs = rg_meta.iter()
        .filter(|rg| zone_map_passes(rg, &col_stats, predicate))
        .filter(|rg| bloom_filter_passes(rg, bf, predicate))
        .collect();

    // Read only projected column chunks from relevant row groups
    RecordBatchIter::new(bf, relevant_rgs, columns)
}
```

---

## 10. Type System

| .bish type        | Arrow IPC type       | Rust type      | Notes                          |
|---|---|---|---|
| `Int8`            | Int8                 | i8             |                                |
| `Int16`           | Int16                | i16            |                                |
| `Int32`           | Int32                | i32            |                                |
| `Int64`           | Int64                | i64            |                                |
| `UInt8`           | UInt8                | u8             |                                |
| `UInt16`          | UInt16               | u16            |                                |
| `UInt32`          | UInt32               | u32            |                                |
| `UInt64`          | UInt64               | u64            |                                |
| `Float32`         | Float32              | f32            |                                |
| `Float64`         | Float64              | f64            |                                |
| `Boolean`         | Boolean              | bool           | Bit-packed, 8 bools per byte   |
| `Utf8`            | Utf8                 | String         | Length-prefixed UTF-8 bytes    |
| `Binary`          | Binary               | Vec<u8>        | Length-prefixed raw bytes      |
| `Date32`          | Date32               | i32            | Days since Unix epoch          |
| `TimestampNs`     | Timestamp(Ns, UTC)   | i64            | Nanoseconds since Unix epoch   |
| `TimestampUs`     | Timestamp(Us, UTC)   | i64            | Microseconds since Unix epoch  |
| `Decimal128(p,s)` | Decimal128           | i128           | Precision p, scale s           |
| `Vector(dim)`     | FixedSizeList(f32)   | Vec<f32>       | For ANN / embedding columns    |
| `List<T>`         | List                 | Vec<T>         | Variable-length lists          |
| `Struct`          | Struct               | —              | Named sub-fields               |

---

## 11. Versioning and Extension Rules

1. **New optional feature:** Add a new feature flag bit in the `RESERVED_OPTIONAL`
   range (bits 9–31). Older readers ignore unknown optional bits.

2. **New required feature:** Add a new feature flag bit in the `RESERVED_REQUIRED`
   range (bits 32–63). Older readers MUST reject files with unknown required bits.

3. **New footer chunk type:** Add a new chunk ID and magic. Older readers that
   don't know the chunk simply skip it using `chunk_length` from the envelope.

4. **New type:** Add to the type system table in a minor version bump. Older
   readers encountering an unknown type in a column they don't project can skip it.
   Older readers encountering an unknown type in a projected column MUST error.

5. **Breaking layout change:** Increment `version_major`. Older readers MUST
   reject if they don't support the new major version.

---

## 12. Integrity and Safety

- **Super-footer checksum:** CRC32C of super-footer bytes 0–499, stored at offset 500.
  Verifying this before using any offset prevents corrupt-pointer reads.

- **Chunk checksums:** Each footer chunk has a CRC32C stored in the super-footer's
  chunk directory. Verified on load.

- **Page checksums:** When `CHECKSUM_CRC32C` feature flag (bit 8) is set,
  each page carries a CRC32C of its compressed bytes in the page header.

- **Magic bookend:** Both `magic_start` and `magic_end` in the super-footer
  must equal `BISH`. A mismatch indicates truncation or corruption.

- **Schema hash:** `schema_hash` in the super-footer is `xxHash64(chunk_a_payload)`.
  After loading chunk A, the reader verifies this hash before trusting the schema.

---

## 13. Compatibility with Parquet Tooling

`.bish` uses Arrow IPC schema encoding (not Thrift) so any Arrow-native tool
can read the schema without a bespoke Thrift parser. The DuckDB extension,
Polars plugin, and PyArrow Dataset source all go through the Arrow C Data
Interface — the format acts as a thin translation layer between the binary
file and the Arrow in-memory representation.

**Parquet features with .bish equivalents:**

| Parquet concept       | .bish equivalent                            |
|---|---|
| Thrift footer         | Arrow IPC footer chunks (lazily loaded)     |
| Row group             | Row group (same concept, hot/cold tag added)|
| Column chunk          | Column chunk (+ bloom filter + zone stats)  |
| Page                  | Page (+ per-page adaptive codec tag)        |
| Dictionary page       | Dict page (encoding tag `0x04`)             |
| Statistics (min/max)  | Zone map in column chunk header             |
| Bloom filter (opt.)   | Bloom filter section (mandatory if enabled) |
| Partition (directory) | In-file partition index block               |

---

*End of BISH FORMAT SPEC v1.0*
*Magic: BISH — "Better Indexed Structured Headers"*