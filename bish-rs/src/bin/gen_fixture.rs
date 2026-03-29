//! Generates a test `.bish` file for use in DuckDB extension smoke tests.
//!
//! Usage:
//!   cargo run --bin gen_fixture -- <output_path>
//!
//! Writes 1 000 rows with 4 columns:
//!   id     INT64   — sequential 0..999
//!   city   VARCHAR — "BLR" (even) / "MUM" (odd)
//!   amount DOUBLE  — id * 1.5
//!   tag    VARCHAR NULLABLE — "vip" except None at every 5th row

use bish::footer::BishWriter;
use bish::types::{BishField, BishSchema, BishType};
use std::env;
use std::fs::File;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();
    let path = match args.get(1) {
        Some(p) => p.as_str(),
        None => {
            eprintln!("Usage: gen_fixture <output_path>");
            process::exit(1);
        }
    };

    let schema = BishSchema::new(vec![
        BishField::new("id", BishType::Int64),
        BishField::new("city", BishType::Utf8),
        BishField::new("amount", BishType::Float64),
        BishField::nullable("tag", BishType::Utf8),
    ]);

    let file = File::create(path).unwrap_or_else(|e| {
        eprintln!("gen_fixture: cannot create '{}': {}", path, e);
        process::exit(1);
    });

    let mut bw = BishWriter::new(file, schema).unwrap_or_else(|e| {
        eprintln!("gen_fixture: writer error: {}", e);
        process::exit(1);
    });

    let mut rg = bw.new_row_group();
    for i in 0..1000i64 {
        rg.push_i64(0, Some(i)).unwrap();
        rg.push_str(1, Some(if i % 2 == 0 { "BLR" } else { "MUM" })).unwrap();
        rg.push_f64(2, Some(i as f64 * 1.5)).unwrap();
        rg.push_str(3, if i % 5 == 0 { None } else { Some("vip") }).unwrap();
    }

    bw.write_row_group(rg).unwrap_or_else(|e| {
        eprintln!("gen_fixture: write_row_group error: {}", e);
        process::exit(1);
    });

    bw.finish().unwrap_or_else(|e| {
        eprintln!("gen_fixture: finish error: {}", e);
        process::exit(1);
    });

    eprintln!("gen_fixture: wrote 1000 rows to {}", path);
}
