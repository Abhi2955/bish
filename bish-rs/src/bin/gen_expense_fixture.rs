//! Generates an `expenses.bish` fixture for DuckDB smoke tests.
//!
//! Usage:
//!   cargo run --bin gen_expense_fixture -- <output_path>

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
            eprintln!("Usage: gen_expense_fixture <output_path>");
            process::exit(1);
        }
    };

    let schema = BishSchema::new(vec![
        BishField::new("expense_id", BishType::Int64),
        BishField::new("category", BishType::Utf8),
        BishField::new("merchant", BishType::Utf8),
        BishField::new("amount", BishType::Float64),
        BishField::new("currency", BishType::Utf8),
    ]);

    let file = File::create(path).unwrap_or_else(|e| {
        eprintln!("gen_expense_fixture: cannot create '{}': {}", path, e);
        process::exit(1);
    });

    let mut writer = BishWriter::new(file, schema).unwrap_or_else(|e| {
        eprintln!("gen_expense_fixture: writer error: {}", e);
        process::exit(1);
    });

    let rows = [
        (1_i64, "Travel", "Delta", 482.31_f64, "USD"),
        (2, "Meals", "Sweetgreen", 18.75, "USD"),
        (3, "Software", "OpenAI", 40.00, "USD"),
        (4, "Lodging", "Marriott", 289.00, "USD"),
        (5, "Transport", "Uber", 36.42, "USD"),
    ];

    let mut rg = writer.new_row_group();
    for (id, category, merchant, amount, currency) in rows {
        rg.push_i64(0, Some(id)).unwrap();
        rg.push_str(1, Some(category)).unwrap();
        rg.push_str(2, Some(merchant)).unwrap();
        rg.push_f64(3, Some(amount)).unwrap();
        rg.push_str(4, Some(currency)).unwrap();
    }

    writer.write_row_group(rg).unwrap_or_else(|e| {
        eprintln!("gen_expense_fixture: write_row_group error: {}", e);
        process::exit(1);
    });

    writer.finish().unwrap_or_else(|e| {
        eprintln!("gen_expense_fixture: finish error: {}", e);
        process::exit(1);
    });

    eprintln!("gen_expense_fixture: wrote 5 rows to {}", path);
}
