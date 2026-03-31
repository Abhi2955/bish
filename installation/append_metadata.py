#!/usr/bin/env python3
"""
Appends the 512-byte DuckDB extension metadata footer to a compiled shared library,
producing a file that DuckDB 1.x will accept via LOAD.

Usage:
  python3 append_metadata.py <input.dylib|input.so> <output.duckdb_extension> \
      [--duckdb-version v1.5.1] [--platform osx_arm64]

Footer layout (512 bytes, appended to end of binary):
  Bytes   0- 31  reserved / zeros
  Bytes  32- 63  extension_version (32B, null-padded, empty = zeros)
  Bytes  64- 95  extension_abi_metadata (32B, null-padded, empty = zeros)
  Bytes  96-127  abi_type string (32B, null-padded) — "CPP" for classic C-API
  Bytes 128-159  duckdb_version (32B, null-padded)  — e.g. "v1.5.1"
  Bytes 160-191  duckdb_capi_version (32B, null-padded) — same as duckdb_version
  Bytes 192-223  platform (32B, null-padded)  — e.g. "osx_arm64"
  Bytes 224-255  magic (32B) — b'4' + 31 zero bytes
  Bytes 256-511  signature (256B, zeros = unsigned extension)

Reverse-engineered from:
  /opt/homebrew/Cellar/duckdb/1.5.1/include/duckdb/main/extension.hpp
  and an actual json.duckdb_extension binary.
"""

import argparse
import platform
import shutil
import struct
import subprocess
import sys
from pathlib import Path


FOOTER_SIZE = 512
FIELD_SIZE = 32
MAGIC = b'4' + b'\x00' * 31


def detect_platform() -> str:
    os_name = platform.system()
    arch = platform.machine()
    if os_name == "Darwin":
        if arch == "arm64":
            return "osx_arm64"
        else:
            return "osx_amd64"
    elif os_name == "Linux":
        if arch == "x86_64":
            return "linux_amd64"
        elif arch == "aarch64":
            return "linux_arm64"
        else:
            return f"linux_{arch}"
    elif os_name == "Windows":
        return "windows_amd64"
    return "unknown"


def detect_duckdb_version(duckdb_bin: str = "duckdb") -> str:
    """Return the DuckDB version string (e.g. 'v1.5.1')."""
    try:
        out = subprocess.check_output([duckdb_bin, "--version"],
                                       stderr=subprocess.STDOUT,
                                       text=True)
        # Output like "v1.5.1 (Variegata) 7dbb2e6..."
        return out.strip().split()[0]
    except Exception:
        return "v0.0.0"


def field(s: str) -> bytes:
    """Encode a string as a 32-byte null-padded field."""
    b = s.encode("utf-8")
    if len(b) > FIELD_SIZE:
        raise ValueError(f"Field too long ({len(b)} > {FIELD_SIZE}): {s!r}")
    return b.ljust(FIELD_SIZE, b'\x00')


def build_footer(duckdb_version: str, plat: str, abi_type: str = "CPP") -> bytes:
    footer = bytearray(FOOTER_SIZE)
    #  0- 31: reserved (zeros)
    # 32- 63: extension_version (empty = zeros)
    # 64- 95: extension_abi_metadata (empty = zeros)
    footer[96:128]  = field(abi_type)
    footer[128:160] = field(duckdb_version)
    footer[160:192] = field(duckdb_version)   # duckdb_capi_version
    footer[192:224] = field(plat)
    footer[224:256] = MAGIC
    # 256-511: signature (zeros = unsigned)
    return bytes(footer)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", help="Built shared library (.dylib / .so)")
    ap.add_argument("output", help="Output .duckdb_extension path")
    ap.add_argument("--duckdb-version", default=None,
                    help="DuckDB version string, e.g. v1.5.1 (auto-detected if omitted)")
    ap.add_argument("--platform", default=None,
                    help="DuckDB platform string, e.g. osx_arm64 (auto-detected if omitted)")
    ap.add_argument("--abi-type", default="CPP",
                    help="Extension ABI type (default: CPP)")
    ap.add_argument("--duckdb-bin", default="duckdb",
                    help="Path to duckdb binary for version detection")
    args = ap.parse_args()

    src = Path(args.input)
    if not src.exists():
        sys.exit(f"ERROR: input file not found: {src}")

    duckdb_version = args.duckdb_version or detect_duckdb_version(args.duckdb_bin)
    plat = args.platform or detect_platform()

    print(f"  duckdb_version : {duckdb_version}")
    print(f"  platform       : {plat}")
    print(f"  abi_type       : {args.abi_type}")

    footer = build_footer(duckdb_version, plat, args.abi_type)

    dst = Path(args.output)
    shutil.copy2(src, dst)
    with open(dst, "ab") as f:
        f.write(footer)

    print(f"  written        : {dst} ({dst.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
