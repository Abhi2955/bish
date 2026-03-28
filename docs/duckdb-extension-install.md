# Installing `bish` DuckDB Extension (Local Dev)

This guide documents milestone-1 build/install steps for the `bish-duckdb` extension.

## 1) Build the extension artifact

From repo root:

```bash
cargo build -p bish-duckdb --release
```

Produced library (platform dependent):
- Linux: `target/release/libbish_duckdb.so`
- macOS: `target/release/libbish_duckdb.dylib`
- Windows: `target/release/bish_duckdb.dll`

## 2) Load in DuckDB

In DuckDB SQL shell, load the binary from absolute path:

```sql
LOAD '/absolute/path/to/target/release/libbish_duckdb.so';
```

Use `.dylib` / `.dll` on macOS / Windows.

## 3) Run smoke checks

### SQL-only smoke script

Edit extension path and file path in:

- `scripts/duckdb-smoke.sql`

Then run:

```bash
duckdb -c ".read scripts/duckdb-smoke.sql"
```

### One-command smoke runner

```bash
./scripts/duckdb-smoke.sh /absolute/path/to/libbish_duckdb.so /absolute/path/to/file.bish
```

## 4) Make load repeatable

For app usage, run `LOAD '<path>'` once per connection startup in your app bootstrap.


## Offline environments

If your environment cannot reach apt/pip registries, use local artifacts documented in:

- `docs/offline-duckdb-install.md`
