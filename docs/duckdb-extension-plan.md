# DuckDB Extension Plan (`bish-duckdb`)

This plan turns the current scaffold into an installable DuckDB extension that users can install once and load in SQL.

## User experience target

1. Install extension binary once.
2. Open DuckDB and run:
   ```sql
   LOAD '/absolute/path/to/libbish_duckdb.so';
   SELECT * FROM read_bish('path/to/file.bish');
   ```
3. Optional follow-up: support path-based implicit scans (`SELECT * FROM 'x.bish'`) through replacement scan hooks.

## Milestone 1: installable extension lifecycle

**Status:** complete.

### Delivered
- Canonical extension ABI export symbols:
  - `duckdb_extension_init`
  - `duckdb_extension_version`
- Initialization routed through `bish_init` and DB-handle-aware registration.
- Registration wiring uses DuckDB C API table-function handle lifecycle (`create -> set callbacks -> register -> destroy`).
- Local smoke assets:
  - SQL smoke script (`scripts/duckdb-smoke.sql`)
  - shell runner (`scripts/duckdb-smoke.sh`)
- Build + install documentation per platform in `docs/duckdb-extension-install.md`.

## Milestone 2: bind function + schema

- Implement real `BindFunction` callback.
- Map all `BishType` values to concrete DuckDB logical types.
- Verify nullable flags and nested types.

## Milestone 3: scan function + chunks

- Implement scan init + next callbacks.
- Decode `.bish` pages and emit `DataChunk` values.
- Validate full scans and projection.

## Milestone 4: pushdown

- Column projection pushdown.
- Zone-map predicate pushdown.
- Add metrics/logging around skipped row groups.

## Milestone 5: packaging and auto-load ergonomics

- CI builds signed extension artifacts.
- Install + load docs for CLI and embedded apps.
- Optional app startup hooks for auto-`LOAD bish` per session.
- Later: replacement scan support for direct `FROM 'file.bish'` usage.

## Acceptance criteria

- Extension artifact loads on supported targets.
- `read_bish(path)` resolves in SQL after extension load.
- Projection + filter pushdown correctness tests pass when milestones 2–4 complete.
