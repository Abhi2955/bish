/**
 * bish-duckdb/src/shim.cpp
 *
 * C++ trampoline that satisfies the DuckDB 1.5.x CPP ABI extension entry point.
 *
 * DuckDB 1.5.x looks for  {stem}_duckdb_cpp_init(duckdb::ExtensionLoader&)
 * inside every extension whose metadata footer advertises abi_type = "CPP".
 *
 * This shim:
 *   1. Receives the ExtensionLoader from DuckDB.
 *   2. Obtains a DatabaseInstance reference from the loader.
 *   3. Constructs a duckdb::Connection on the stack.
 *   4. Passes the Connection pointer to the Rust registration function
 *      bish_register_with_conn(), which uses the DuckDB C API to register
 *      the read_bish() table function.
 *
 * Header strategy: if the system provides DuckDB headers (set by build.rs via
 * BISH_HAVE_DUCKDB_HEADERS=1) we include them; otherwise we fall back to the
 * bundled minimal vendor stubs in vendor/duckdb_ext_shim.hpp.  Both provide
 * the same declarations; the method bodies live in the DuckDB shared library
 * and are resolved at runtime via dynamic linking.
 */

#if defined(BISH_HAVE_DUCKDB_HEADERS)
#  include "duckdb/main/extension/extension_loader.hpp"
#  include "duckdb/main/connection.hpp"
#else
#  include "duckdb_ext_shim.hpp"
#endif

/* Rust side – defined in table_fn.rs (#[no_mangle] extern "C"). */
extern "C" {
void bish_register_with_conn(void *conn);
}

extern "C" void bish_duckdb_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
    duckdb::DatabaseInstance &db = loader.GetDatabaseInstance();
    duckdb::Connection con(db);
    bish_register_with_conn(static_cast<void *>(&con));
}
