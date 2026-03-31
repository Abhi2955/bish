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
    // We compile a thin C++ trampoline (shim.cpp) that:
    //   - receives the ExtensionLoader
    //   - creates a duckdb::Connection on the stack
    //   - forwards it to the Rust bish_register_with_conn() function
    //
    // The DuckDB include path is auto-detected: Homebrew on macOS, /usr on Linux.
    let include_dir = if cfg!(target_os = "macos") {
        // Prefer the Homebrew-versioned path, fall back to generic Homebrew prefix.
        let versioned = "/opt/homebrew/Cellar/duckdb/1.5.1/include";
        if std::path::Path::new(versioned).exists() {
            versioned.to_string()
        } else {
            "/opt/homebrew/include".to_string()
        }
    } else {
        "/usr/include".to_string()
    };

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .include(&include_dir)
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-deprecated-declarations")
        .file("src/shim.cpp")
        .compile("bish_shim");

    // Force the linker to include bish_duckdb_duckdb_cpp_init even though
    // no Rust code references it.  Without this the linker dead-strips the
    // symbol from the cdylib and DuckDB cannot find the entry point.
    //
    // We use -force_load on macOS (includes every object in the archive) and
    // --whole-archive on Linux.
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let archive = format!("{out_dir}/libbish_shim.a");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "macos" => {
            // -force_load ensures the shim object is fully included (not lazy-stripped).
            println!("cargo:rustc-link-arg=-Wl,-force_load,{archive}");
            // Export the CPP ABI entry point so DuckDB can dlsym() it.
            println!("cargo:rustc-link-arg=-Wl,-exported_symbol,_bish_duckdb_duckdb_cpp_init");
            // DuckDB symbols in the shim live in the host process at runtime.
            println!("cargo:rustc-link-arg=-undefined");
            println!("cargo:rustc-link-arg=dynamic_lookup");
        }
        "linux" => {
            println!("cargo:rustc-link-arg=-Wl,--whole-archive,{archive},--no-whole-archive");
            println!("cargo:rustc-link-arg=-Wl,--allow-shlib-undefined");
        }
        _ => {}
    }

    println!("cargo:rerun-if-changed=src/shim.cpp");
    println!("cargo:rerun-if-changed=build.rs");
}
