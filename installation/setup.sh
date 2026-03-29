#!/usr/bin/env bash
# installation/setup.sh
#
# Downloads the DuckDB CLI binary and places it at installation/bin/duckdb.
# Run once before validate.sh.
#
# Usage:
#   ./installation/setup.sh [duckdb-version]
#
# Default version: 1.1.3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
DUCKDB_VERSION="${1:-1.1.3}"
DUCKDB_BIN="$BIN_DIR/duckdb"

if [[ -x "$DUCKDB_BIN" ]]; then
    CURRENT_VERSION="$("$DUCKDB_BIN" --version 2>&1 | head -1 || true)"
    echo "DuckDB already installed: $CURRENT_VERSION"
    echo "  path: $DUCKDB_BIN"
    echo "  (delete $DUCKDB_BIN and re-run to upgrade)"
    exit 0
fi

mkdir -p "$BIN_DIR"

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Darwin)
        # Universal binary covers both x86_64 and arm64 (Apple Silicon).
        FILENAME="duckdb_osx_universal2.zip"
        ;;
    Linux)
        case "$ARCH" in
            x86_64)  FILENAME="duckdb_linux_amd64.zip" ;;
            aarch64) FILENAME="duckdb_linux_aarch64.zip" ;;
            *)
                echo "ERROR: unsupported Linux arch: $ARCH" >&2
                exit 1
                ;;
        esac
        ;;
    *)
        echo "ERROR: unsupported OS: $OS" >&2
        exit 1
        ;;
esac

BASE_URL="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}"
DOWNLOAD_URL="${BASE_URL}/${FILENAME}"
ZIPFILE="$BIN_DIR/${FILENAME}"

echo "Downloading DuckDB v${DUCKDB_VERSION} for ${OS}/${ARCH}..."
echo "  URL: $DOWNLOAD_URL"

if command -v curl &>/dev/null; then
    curl -fsSL -o "$ZIPFILE" "$DOWNLOAD_URL"
elif command -v wget &>/dev/null; then
    wget -q -O "$ZIPFILE" "$DOWNLOAD_URL"
else
    echo "ERROR: neither curl nor wget found" >&2
    exit 1
fi

echo "Extracting..."
cd "$BIN_DIR"
unzip -o "$ZIPFILE" duckdb
rm -f "$ZIPFILE"
chmod +x "$DUCKDB_BIN"

echo ""
echo "DuckDB installed:"
"$DUCKDB_BIN" --version
echo "  path: $DUCKDB_BIN"
echo ""
echo "Next: run ./installation/validate.sh"
