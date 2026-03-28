#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'USAGE'
Usage: ./setup_local.sh [--setup-only] [--test-only] [--refresh-toolchain] [--help]

Options:
  --setup-only        Install/verify prerequisites and prefetch dependencies.
  --test-only         Run the test workflow without setup checks.
  --refresh-toolchain Force a `rustup toolchain install stable --profile minimal`.
  --help              Show this help message.
USAGE
}

SETUP_ONLY=false
TEST_ONLY=false
REFRESH_TOOLCHAIN=false

for arg in "$@"; do
  case "$arg" in
    --setup-only) SETUP_ONLY=true ;;
    --test-only) TEST_ONLY=true ;;
    --refresh-toolchain) REFRESH_TOOLCHAIN=true ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$SETUP_ONLY" == true && "$TEST_ONLY" == true ]]; then
  echo "--setup-only and --test-only cannot be used together." >&2
  exit 1
fi

require_cmd() {
  local cmd="$1"
  local install_hint="$2"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd"
    echo "Install hint: $install_hint"
    exit 1
  fi
}

run_setup() {
  echo "[1/3] Checking Rust toolchain..."
  require_cmd rustup "Install from https://rustup.rs/"
  require_cmd cargo "Install from https://rustup.rs/"

  echo "[2/3] Verifying active toolchain..."
  rustup toolchain list >/dev/null
  if [[ "$REFRESH_TOOLCHAIN" == true ]]; then
    echo "Refreshing stable toolchain..."
    rustup toolchain install stable --profile minimal
  else
    echo "Skipping toolchain refresh (use --refresh-toolchain to force)."
  fi

  echo "[3/3] Prefetching Cargo dependencies..."
  cargo fetch --locked

  echo "Setup complete."
}

run_tests() {
  echo "Running format check..."
  cargo fmt --all -- --check

  echo "Running compile check..."
  cargo check --workspace --all-targets --locked

  echo "Running test suite..."
  cargo test --workspace --locked

  echo "All checks passed."
}

if [[ "$TEST_ONLY" == false ]]; then
  run_setup
fi

if [[ "$SETUP_ONLY" == false ]]; then
  run_tests
fi
