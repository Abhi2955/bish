//! DuckDB extension scaffold for reading `.bish` files.
//!
//! Phase-2 / T-08 scope:
//! - crate scaffolding and workspace wiring
//! - table-function registration surface
//! - linkage to `libduckdb-sys`

use std::ffi::c_void;
use std::os::raw::c_char;

pub mod table_fn;
pub mod types;

pub use table_fn::{
    register_bish_functions, register_bish_functions_for_db, BishTableFunction, RegistrationError,
};

/// Canonical DuckDB extension ABI entrypoint (DuckDB 1.x).
///
/// DuckDB calls this after loading the shared library.
/// Returns 0 to indicate success.
#[no_mangle]
pub extern "C" fn duckdb_extension_init(db: *mut c_void) -> i32 {
    let _ = register_bish_functions_for_db(db);
    0
}

/// Legacy-style extension entry point used by some DuckDB loaders that
/// call `{stem}_init(db)` instead of `duckdb_extension_init`.
#[no_mangle]
pub extern "C" fn bish_duckdb_init(db: *mut c_void) {
    let _ = register_bish_functions_for_db(db);
}

/// Canonical DuckDB extension ABI version symbol.
#[no_mangle]
pub extern "C" fn duckdb_extension_version() -> *const c_char {
    static VERSION: &[u8] = b"bish_duckdb/0.1.0\0";
    VERSION.as_ptr().cast()
}

/// Returns DuckDB's runtime version string.
///
/// Calling into the C API here forces a concrete link to `libduckdb-sys` so
/// the extension crate validates native symbol resolution during build/tests.
pub fn duckdb_library_version() -> &'static str {
    #[cfg(feature = "duckdb-link")]
    unsafe {
        let ptr = libduckdb_sys::duckdb_library_version();
        if ptr.is_null() {
            "unknown"
        } else {
            // DuckDB returns a static, null-terminated UTF-8-ish C string.
            std::ffi::CStr::from_ptr(ptr).to_str().unwrap_or("unknown")
        }
    }

    #[cfg(not(feature = "duckdb-link"))]
    {
        "duckdb-link-disabled"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_duckdb_version_symbol() {
        let v = duckdb_library_version();
        assert!(!v.is_empty());
    }

    #[test]
    fn exposes_duckdb_extension_abi_symbols() {
        assert!(!duckdb_extension_version().is_null());
    }
}
