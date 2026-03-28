#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <extension_library_path> <bish_file_path>" >&2
  exit 2
fi

EXT_PATH="$1"
BISH_PATH="$2"

if [[ ! -f "$EXT_PATH" ]]; then
  echo "Extension library not found: $EXT_PATH" >&2
  exit 1
fi

if [[ ! -f "$BISH_PATH" ]]; then
  echo ".bish file not found: $BISH_PATH" >&2
  exit 1
fi

duckdb -c "LOAD '$EXT_PATH'; SELECT * FROM read_bish('$BISH_PATH') LIMIT 1;"
