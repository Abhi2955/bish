# Offline DuckDB Install Guide (for this repo)

This guide lets you install DuckDB in network-restricted environments by using local artifacts.

## Supported local artifacts

You can provide **any one** of the following:

1. A prebuilt DuckDB CLI binary named `duckdb`
2. A DuckDB `.deb` package
3. A Python wheel `duckdb-*.whl`

---

## Option A: Local DuckDB CLI binary

### 1) Place binary in repo

Example location:

```bash
third_party/duckdb/duckdb
```

### 2) Make executable

```bash
chmod +x third_party/duckdb/duckdb
```

### 3) Verify

```bash
./third_party/duckdb/duckdb --version
```

### 4) (Optional) Add to PATH for current shell

```bash
export PATH="$PWD/third_party/duckdb:$PATH"
which duckdb
```

---

## Option B: Local `.deb` package

### 1) Place package in repo

Example location:

```bash
third_party/duckdb/duckdb_<version>_<arch>.deb
```

### 2) Install package

```bash
sudo apt-get install -y ./third_party/duckdb/duckdb_<version>_<arch>.deb
```

> If dependency issues appear, provide all required `.deb` dependencies locally and install together.

### 3) Verify

```bash
duckdb --version
```

---

## Option C: Local Python wheel

### 1) Place wheel in repo

Example location:

```bash
third_party/duckdb/duckdb-<version>-<python>-<abi>-<platform>.whl
```

### 2) Install wheel

```bash
python -m pip install --user ./third_party/duckdb/duckdb-*.whl
```

### 3) Verify Python package

```bash
python - <<'PY'
import duckdb
print(duckdb.__version__)
PY
```

---

## Recommended repo layout

```text
third_party/
  duckdb/
    duckdb                          # optional CLI binary
    duckdb_<version>_<arch>.deb     # optional deb package
    duckdb-*.whl                    # optional python wheel
```

---

## After DuckDB is available

You can run the extension smoke flow:

```bash
./scripts/duckdb-smoke.sh /absolute/path/to/libbish_duckdb.so /absolute/path/to/file.bish
```

Or SQL script flow:

```bash
duckdb -c ".read scripts/duckdb-smoke.sql"
```

---

## Notes

- In restricted environments, avoid commands that fetch from internet (`apt-get update`, `pip install duckdb`).
- Prefer checked-in or internally mirrored artifacts.
