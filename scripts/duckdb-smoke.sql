-- Smoke test for local bish DuckDB extension build.
-- 1) Update EXT_PATH and BISH_PATH.
-- 2) Run: duckdb -c ".read scripts/duckdb-smoke.sql"

-- Example: '/abs/path/to/target/release/libbish_duckdb.so'
LOAD '/absolute/path/to/libbish_duckdb.so';

-- Example: '/abs/path/to/fixtures/simple.bish'
SELECT * FROM read_bish('/absolute/path/to/file.bish') LIMIT 1;
