#!/usr/bin/env bash
# installation/validate.sh
#
# End-to-end validation of the bish DuckDB extension.
#
# Steps:
#   1. Build the bish-duckdb cdylib
#   2. Append the DuckDB extension metadata footer (required by DuckDB 1.x)
#   3. Build gen_fixture and write a 1 000-row test .bish file
#   4. Load the extension in DuckDB CLI and run COUNT(*) + DESCRIBE + sample rows
#   5. Assert COUNT(*) == 1000
#
# Prerequisites:
#   - DuckDB CLI on PATH (or run ./installation/setup.sh first)
#   - Rust toolchain (cargo) on PATH
#
# Usage:
#   ./installation/validate.sh [--release]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── 0. Locate DuckDB ──────────────────────────────────────────────────────────

DUCKDB_BIN=""
for candidate in \
    "$SCRIPT_DIR/bin/duckdb" \
    "$(command -v duckdb 2>/dev/null || true)" \
    "/opt/homebrew/bin/duckdb" \
    "/usr/local/bin/duckdb" \
    "/usr/bin/duckdb"; do
    if [[ -x "$candidate" ]]; then
        DUCKDB_BIN="$candidate"
        break
    fi
done

if [[ -z "$DUCKDB_BIN" ]]; then
    echo "ERROR: DuckDB CLI not found." >&2
    echo "       Install via: brew install duckdb  OR  ./installation/setup.sh" >&2
    exit 1
fi
echo "Using DuckDB : $("$DUCKDB_BIN" --version | head -1)  ($DUCKDB_BIN)"

# ── 1. Build the extension ────────────────────────────────────────────────────

RELEASE_FLAG=""
BUILD_DIR="debug"
if [[ "${1:-}" == "--release" ]]; then
    RELEASE_FLAG="--release"
    BUILD_DIR="release"
fi

echo ""
echo "==> Building bish-duckdb extension..."
cd "$REPO_ROOT"
cargo build $RELEASE_FLAG -p bish-duckdb 2>&1

OS="$(uname -s)"
case "$OS" in
    Darwin) DYLIB="libbish_duckdb.dylib" ;;
    Linux)  DYLIB="libbish_duckdb.so" ;;
    *)      echo "ERROR: unsupported OS: $OS" >&2; exit 1 ;;
esac

DYLIB_PATH="$REPO_ROOT/target/${BUILD_DIR}/${DYLIB}"
EXT_PATH="$REPO_ROOT/target/${BUILD_DIR}/bish_duckdb.duckdb_extension"

if [[ ! -f "$DYLIB_PATH" ]]; then
    echo "ERROR: compiled library not found: $DYLIB_PATH" >&2
    exit 1
fi
echo "    Library  : $DYLIB_PATH"

# ── 2. Append DuckDB extension metadata footer ────────────────────────────────

echo ""
echo "==> Appending DuckDB extension metadata..."
python3 "$SCRIPT_DIR/append_metadata.py" \
    "$DYLIB_PATH" "$EXT_PATH" \
    --duckdb-bin "$DUCKDB_BIN"
echo "    Extension: $EXT_PATH"

# ── 3. Generate fixture .bish file ───────────────────────────────────────────

echo ""
echo "==> Generating fixture .bish file (1 000 rows)..."
cargo build $RELEASE_FLAG -p bish --bin gen_fixture 2>&1

FIXTURE_PATH="$SCRIPT_DIR/fixture.bish"
"$REPO_ROOT/target/${BUILD_DIR}/gen_fixture" "$FIXTURE_PATH"
echo "    Fixture  : $FIXTURE_PATH"

# ── 4. Run DuckDB validation queries ─────────────────────────────────────────

echo ""
echo "==> Running DuckDB SQL validation..."

COUNT_RESULT="$(
    "$DUCKDB_BIN" -unsigned -c "
        LOAD '${EXT_PATH}';
        SELECT COUNT(*) AS cnt FROM read_bish('${FIXTURE_PATH}');
    " 2>&1
)"
echo "    COUNT(*)  : $(echo "$COUNT_RESULT" | grep -Eo '[0-9]+' | tail -1)"

ACTUAL_COUNT="$(echo "$COUNT_RESULT" | grep -Eo '[0-9]+' | tail -1)"
if [[ "$ACTUAL_COUNT" != "1000" ]]; then
    echo ""
    echo "Full DuckDB output:"
    echo "$COUNT_RESULT"
    echo ""
    echo "FAIL: expected COUNT=1000, got: $ACTUAL_COUNT" >&2
    exit 1
fi

echo ""
DESCRIBE_RESULT="$(
    "$DUCKDB_BIN" -unsigned -c "
        LOAD '${EXT_PATH}';
        DESCRIBE SELECT * FROM read_bish('${FIXTURE_PATH}');
    " 2>&1
)"
echo "    DESCRIBE:"
echo "$DESCRIBE_RESULT" | sed 's/^/      /'

echo ""
SAMPLE_RESULT="$(
    "$DUCKDB_BIN" -unsigned -c "
        LOAD '${EXT_PATH}';
        SELECT id, city, amount, tag
        FROM read_bish('${FIXTURE_PATH}')
        WHERE id < 5
        ORDER BY id;
    " 2>&1
)"
echo "    Sample rows (id < 5):"
echo "$SAMPLE_RESULT" | sed 's/^/      /'

# ── 5. Summary ────────────────────────────────────────────────────────────────

echo ""
echo "=================================================="
echo "  PASS — bish DuckDB extension validated"
echo "  COUNT(*) = $ACTUAL_COUNT rows"
echo "=================================================="
