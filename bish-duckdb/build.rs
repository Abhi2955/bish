use std::path::PathBuf;

fn main() {
    // ── Linker flags for undefined DuckDB symbols ─────────────────────────────
    //
    // The cdylib extension resolves duckdb_* symbols at runtime from the host
    // DuckDB process.  Without extra flags the linker errors on undefined refs.
    //
    // macOS: -undefined dynamic_lookup defers resolution to load time.
    // Linux: --allow-shlib-undefined does the same for shared libraries.
    //
    // Not needed when built with --features duckdb-link (libduckdb-sys provides
    // all symbols at link time).
    #[cfg(not(feature = "duckdb-link"))]
    {
        let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        match target_os.as_str() {
            "macos" => {
                println!("cargo:rustc-link-arg=-undefined");
                println!("cargo:rustc-link-arg=dynamic_lookup");
            }
            "linux" => {
                println!("cargo:rustc-link-arg=-Wl,--allow-shlib-undefined");
            }
            _ => {}
        }
    }

    // ── C++ shim for the DuckDB 1.5.x CPP ABI entrypoint ────────────────────
    //
    // DuckDB 1.5.x calls  bish_duckdb_duckdb_cpp_init(duckdb::ExtensionLoader&)
    // which must be a real C++ function because ExtensionLoader is a C++ class.
    //
    // Header strategy:
    //   1. Try well-known system/Homebrew install locations.
    //   2. Fall back to vendor/duckdb_ext_shim.hpp (bundled minimal stubs,
    //      no DuckDB installation required).
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let vendor_dir = manifest_dir.join("vendor");

    // Candidate system include directories, most-specific first.
    let system_candidates: &[&str] = &[
        "/opt/homebrew/Cellar/duckdb/1.5.1/include",
        "/opt/homebrew/include",
        "/usr/local/include",
        "/usr/include",
    ];

    let (include_dir, have_system_headers) = system_candidates
        .iter()
        .find(|p| std::path::Path::new(p).join("duckdb/main/connection.hpp").exists())
        .map(|p| (p.to_string(), true))
        .unwrap_or_else(|| (vendor_dir.to_str().unwrap().to_string(), false));

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++17")
        .include(&include_dir)
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-deprecated-declarations")
        .file("src/shim.cpp");

    if have_system_headers {
        build.define("BISH_HAVE_DUCKDB_HEADERS", "1");
    }

    build.compile("bish_shim");

    // ── Force the entry-point symbol into the final dylib ────────────────────
    //
    // bish_duckdb_duckdb_cpp_init is not referenced from any Rust code, so the
    // linker would dead-strip it without these flags.
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let archive = format!("{out_dir}/libbish_shim.a");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "macos" => {
            // -force_load includes every object in the archive.
            println!("cargo:rustc-link-arg=-Wl,-force_load,{archive}");
            // Export the symbol so DuckDB can dlsym() it.
            println!("cargo:rustc-link-arg=-Wl,-exported_symbol,_bish_duckdb_duckdb_cpp_init");
            // DuckDB C++ symbols resolve at runtime from the host process.
            println!("cargo:rustc-link-arg=-undefined");
            println!("cargo:rustc-link-arg=dynamic_lookup");
        }
        "linux" => {
            // --whole-archive forces all objects in the archive to be included.
            println!(
                "cargo:rustc-link-arg=-Wl,--whole-archive,{archive},--no-whole-archive"
            );
            // DuckDB C++ symbols (Connection ctor/dtor, GetDatabaseInstance) live
            // in the host DuckDB process; allow them to be unresolved at link time.
            println!("cargo:rustc-link-arg=-Wl,--allow-shlib-undefined");
        }
        _ => {}
    }

    println!("cargo:rerun-if-changed=src/shim.cpp");
    println!("cargo:rerun-if-changed=vendor/duckdb_ext_shim.hpp");
    println!("cargo:rerun-if-changed=build.rs");
}
