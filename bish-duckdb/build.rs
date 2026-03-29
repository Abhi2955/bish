fn main() {
    // When building the cdylib extension without linking DuckDB statically,
    // all `duckdb_*` symbols are resolved at load time from the DuckDB process.
    // Without extra linker flags the macOS and Linux linkers error on undefined symbols.
    //
    // On macOS:  -undefined dynamic_lookup defers all unresolved symbols to runtime.
    // On Linux:  --allow-shlib-undefined does the same for shared libraries.
    //
    // These flags are NOT needed when building with `--features duckdb-link` because
    // libduckdb-sys provides all symbols at link time.
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
}
