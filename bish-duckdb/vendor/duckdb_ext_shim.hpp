/**
 * vendor/duckdb_ext_shim.hpp
 *
 * Minimal C++ declarations needed to implement the DuckDB 1.5.x CPP ABI
 * extension entry point WITHOUT requiring a DuckDB installation.
 *
 * All method bodies live in the DuckDB shared library and are resolved at
 * runtime via dynamic linking (no definitions here, only declarations).
 *
 * Layout is pinned to DuckDB 1.5.x.  Tested against 1.5.1.
 *
 * Connection layout (duckdb/main/connection.hpp in 1.5.x):
 *   DuckDB *db                          -- 8 bytes
 *   std::shared_ptr<ClientContext>      -- 16 bytes (two raw pointers)
 *   ─────────────────────────────────────  24 bytes total
 *
 * ExtensionLoader layout (duckdb/main/extension/extension_loader.hpp):
 *   DatabaseInstance &db                -- 8 bytes (reference = pointer)
 *   std::string extension_name          -- 24 bytes (SSO string on 64-bit)
 *   std::string extension_description   -- 24 bytes
 *   optional_ptr<ExtensionInfo>         -- 8 bytes (raw pointer)
 */

#pragma once
#include <cstddef>
#include <memory>
#include <string>

// Visibility macro — functions are exported from the DuckDB shared library.
#if defined(_WIN32)
#  define DUCKDB_API __declspec(dllimport)
#else
#  define DUCKDB_API __attribute__((visibility("default")))
#endif

namespace duckdb {

// ── Forward declarations ──────────────────────────────────────────────────────
class DatabaseInstance;
class ClientContext;
class DuckDB;
struct ExtensionInfo;
struct ExtensionActiveLoad;

// ── optional_ptr<T> ──────────────────────────────────────────────────────────
// Minimal replica of DuckDB's optional_ptr (just a nullable raw pointer).
template <class T>
struct optional_ptr {
    T *ptr = nullptr;
};

// ── Connection ────────────────────────────────────────────────────────────────
// Reproduces the *data layout* of duckdb::Connection from DuckDB 1.5.x so
// that stack/heap allocation uses the correct size.
class Connection {
public:
    DUCKDB_API explicit Connection(DuckDB &database);
    DUCKDB_API explicit Connection(DatabaseInstance &database);
    DUCKDB_API ~Connection();

    // Data members — must match DuckDB 1.5.x exactly.
    DuckDB *db;                            //  8 bytes
    std::shared_ptr<ClientContext> context; // 16 bytes
};

// ── ExtensionLoader ───────────────────────────────────────────────────────────
// Only GetDatabaseInstance() is used by the shim.
class ExtensionLoader {
public:
    DUCKDB_API DatabaseInstance &GetDatabaseInstance();

    // We never construct ExtensionLoader — DuckDB constructs it and passes a
    // reference to our entry point.  Declaring a dummy private constructor
    // silences compiler warnings about uninitialised reference members.
    ExtensionLoader() = delete;

private:
    // Layout must match DuckDB 1.5.x so that method calls use correct offsets.
    DatabaseInstance &db;                         //  8 bytes
    std::string extension_name;                   // 24 bytes
    std::string extension_description;            // 24 bytes
    optional_ptr<ExtensionInfo> extension_info;   //  8 bytes
};

} // namespace duckdb
