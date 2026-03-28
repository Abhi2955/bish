use bish::types::BishType;

/// Minimal representation of DuckDB bind output for a single column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckdbColumn {
    pub name: String,
    pub logical_type: String,
    pub nullable: bool,
}

/// Maps `.bish` logical types into DuckDB SQL type names.
///
/// This string-level mapping is a scaffold for T-09 where these become real
/// `duckdb_logical_type` values in the extension bind callback.
pub fn duckdb_sql_type_name(typ: &BishType) -> &'static str {
    match typ {
        BishType::Boolean => "BOOLEAN",
        BishType::Int8 => "TINYINT",
        BishType::Int16 => "SMALLINT",
        BishType::Int32 => "INTEGER",
        BishType::Int64 => "BIGINT",
        BishType::UInt8 => "UTINYINT",
        BishType::UInt16 => "USMALLINT",
        BishType::UInt32 => "UINTEGER",
        BishType::UInt64 => "UBIGINT",
        BishType::Float32 => "FLOAT",
        BishType::Float64 => "DOUBLE",
        BishType::Utf8 => "VARCHAR",
        BishType::Binary => "BLOB",
        BishType::Date32 => "DATE",
        BishType::TimestampS => "TIMESTAMP_S",
        BishType::TimestampMs => "TIMESTAMP_MS",
        BishType::TimestampUs => "TIMESTAMP",
        BishType::TimestampNs => "TIMESTAMP_NS",
        BishType::Decimal128 { .. } => "DECIMAL",
        BishType::List(_) => "LIST",
        BishType::Struct(_) => "STRUCT",
        BishType::Vector { .. } => "FLOAT[]",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bish::types::BishType;

    #[test]
    fn scalar_mappings_are_stable() {
        assert_eq!(duckdb_sql_type_name(&BishType::Int64), "BIGINT");
        assert_eq!(duckdb_sql_type_name(&BishType::Utf8), "VARCHAR");
        assert_eq!(duckdb_sql_type_name(&BishType::Binary), "BLOB");
    }

    #[test]
    fn nested_and_vector_mappings_are_present() {
        assert_eq!(
            duckdb_sql_type_name(&BishType::List(Box::new(BishType::Int32))),
            "LIST"
        );
        assert_eq!(
            duckdb_sql_type_name(&BishType::Vector { dim: 768 }),
            "FLOAT[]"
        );
    }
}
