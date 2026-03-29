# Installation & Validation

End-to-end setup for the `bish` DuckDB extension.

## Quick start

```bash
# 1. Download DuckDB CLI (once)
./installation/setup.sh

# 2. Build extension + run SQL validation
./installation/validate.sh
```

## What validate.sh does

1. **Builds** `bish-duckdb` (cdylib) via `cargo build`
2. **Generates** a 1 000-row fixture `.bish` file via `gen_fixture`
3. **Loads** the extension in DuckDB CLI
4. **Asserts** `COUNT(*) = 1000` from `read_bish('fixture.bish')`
5. **Prints** `DESCRIBE` output and a few sample rows

## Build modes

```bash
./installation/validate.sh           # debug build (fast compile)
./installation/validate.sh --release # release build (faster runtime)
```

## DuckDB version

`setup.sh` downloads DuckDB **1.1.3** by default. To use a different version:

```bash
./installation/setup.sh 1.2.0
```

The downloaded binary is placed at `installation/bin/duckdb` and is not
committed to the repository (see `.gitignore`).

## How the extension loads DuckDB symbols

The `bish-duckdb` cdylib does **not** statically link DuckDB. All
`duckdb_*` symbols are resolved at load time from the DuckDB process that
calls `LOAD '/path/to/libbish_duckdb.dylib'`. This is the standard approach
for DuckDB extensions.

On macOS, `build.rs` adds `-undefined dynamic_lookup` so the linker accepts
the unresolved symbols at build time. On Linux it adds
`-Wl,--allow-shlib-undefined`.

To link DuckDB statically (useful for isolated testing or CI):

```bash
cargo build -p bish-duckdb --features duckdb-link
```

## SQL usage after installation

```sql
-- Load the extension (absolute path required)
LOAD '/absolute/path/to/libbish_duckdb.dylib';

-- Full scan
SELECT * FROM read_bish('path/to/data.bish');

-- Projection
SELECT id, city FROM read_bish('path/to/data.bish');

-- Aggregation
SELECT city, COUNT(*), AVG(amount)
FROM read_bish('path/to/data.bish')
GROUP BY city;
```
