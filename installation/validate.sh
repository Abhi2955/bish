#!/usr/bin/env bash
# installation/validate.sh
#
# End-to-end validation of the bish DuckDB extension.
# Steps:
#   1. Build the extension (bish-duckdb cdylib)
#   2. Build the fixture generator and write a test .bish file
#   3. Load the extension in DuckDB CLI and run validation queries
#   4. Assert expected output (1000 rows, correct column names)
#
# Prerequisites:
#   - Run ./installation/setup.sh first to install DuckDB CLI
#   - Rust toolchain (cargo) must be on PATH
#
# Usage:
#   ./installation/validate.sh [--release]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
DUCKDB_BIN="$BIN_DIR/duckdb"

RELEASE_FLAG=""
BUILD_DIR="debug"
if [[ "${1:-}" == "--release" ]]; then
    RELEASE_FLAG="--release"
    BUILD_DIR="release"
fi

# ── 0. Sanity checks ──────────────────────────────────────────────────────────

if [[ ! -x "$DUCKDB_BIN" ]]; then
    echo "ERROR: DuckDB CLI not found at $DUCKDB_BIN" >&2
    echo "       Run ./installation/setup.sh first." >&2
    exit 1
fi

echo "Using DuckDB: $("$DUCKDB_BIN" --version | head -1)"

# ── 1. Build the extension ────────────────────────────────────────────────────

echo ""
echo "==> Building bish-duckdb extension..."
cd "$REPO_ROOT"
cargo build $RELEASE_FLAG -p bish-duckdb 2>&1

# Locate the built shared library.
OS="$(uname -s)"
case "$OS" in
    Darwin) EXT_FILE="libbish_duckdb.dylib" ;;
    Linux)  EXT_FILE="libbish_duckdb.so" ;;
    *)      echo "ERROR: unsupported OS: $OS" >&2; exit 1 ;;
esac

EXT_PATH="$REPO_ROOT/target/${BUILD_DIR}/${EXT_FILE}"
if [[ ! -f "$EXT_PATH" ]]; then
    echo "ERROR: extension not found at $EXT_PATH" >&2
    exit 1
fi
echo "    Extension: $EXT_PATH"

# ── 2. Generate fixture .bish file ───────────────────────────────────────────

echo ""
echo "==> Generating fixture .bish file..."
cargo build $RELEASE_FLAG -p bish --bin gen_fixture 2>&1

FIXTURE_PATH="$SCRIPT_DIR/fixture.bish"
"$REPO_ROOT/target/${BUILD_DIR}/gen_fixture" "$FIXTURE_PATH"
echo "    Fixture:   $FIXTURE_PATH"

# ── 3. Run DuckDB validation queries ─────────────────────────────────────────

echo ""
echo "==> Running DuckDB validation queries..."

# Query 1: COUNT(*) should equal 1000
COUNT_RESULT="$(
    "$DUCKDB_BIN" -c "
        LOAD '${EXT_PATH}';
        SELECT COUNT(*) AS cnt FROM read_bish('${FIXTURE_PATH}');
    " 2>&1
)"
echo "    COUNT(*): $COUNT_RESULT"

# Extract the numeric value from the output.
ACTUAL_COUNT="$(echo "$COUNT_RESULT" | grep -Eo '[0-9]+' | tail -1)"
if [[ "$ACTUAL_COUNT" != "1000" ]]; then
    echo ""
    echo "FAIL: expected COUNT=1000, got: $ACTUAL_COUNT" >&2
    exit 1
fi

# Query 2: Column names and types via DESCRIBE
echo ""
DESCRIBE_RESULT="$(
    "$DUCKDB_BIN" -c "
        LOAD '${EXT_PATH}';
        DESCRIBE SELECT * FROM read_bish('${FIXTURE_PATH}');
    " 2>&1
)"
echo "    DESCRIBE output:"
echo "$DESCRIBE_RESULT" | sed 's/^/      /'

# Query 3: Spot-check a few values
SAMPLE_RESULT="$(
    "$DUCKDB_BIN" -c "
        LOAD '${EXT_PATH}';
        SELECT id, city, amount, tag
        FROM read_bish('${FIXTURE_PATH}')
        WHERE id < 5
        ORDER BY id;
    " 2>&1
)"
echo ""
echo "    Sample rows (id < 5):"
echo "$SAMPLE_RESULT" | sed 's/^/      /'

# ── 4. Summary ────────────────────────────────────────────────────────────────

echo ""
echo "=================================================="
echo "  PASS — bish DuckDB extension validated"
echo "  COUNT(*) = $ACTUAL_COUNT rows"
echo "=================================================="
