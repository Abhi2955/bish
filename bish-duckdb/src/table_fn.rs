use bish::reader::BishReader;
use bish::types::BishType;
use std::ffi::{c_char, c_void, CString};
use std::io::{Read, Seek};
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "duckdb-link")]
use std::ffi::CStr;
#[cfg(feature = "duckdb-link")]
use std::io::BufReader;

use crate::types::{duckdb_sql_type_name, DuckdbColumn};

/// Name that will be registered as a DuckDB table function.
pub const TABLE_FUNCTION_NAME: &str = "read_bish";

/// DuckDB returns 0 for success in the C API.
#[cfg(feature = "duckdb-link")]
const DUCKDB_SUCCESS: u32 = 0;

// DuckDB logical type IDs (from duckdb.h DUCKDB_TYPE enum).
const DUCKDB_TYPE_BOOLEAN: u32 = 1;
const DUCKDB_TYPE_TINYINT: u32 = 2;
const DUCKDB_TYPE_SMALLINT: u32 = 3;
const DUCKDB_TYPE_INTEGER: u32 = 4;
const DUCKDB_TYPE_BIGINT: u32 = 5;
const DUCKDB_TYPE_UTINYINT: u32 = 6;
const DUCKDB_TYPE_USMALLINT: u32 = 7;
const DUCKDB_TYPE_UINTEGER: u32 = 8;
const DUCKDB_TYPE_UBIGINT: u32 = 9;
const DUCKDB_TYPE_FLOAT: u32 = 10;
const DUCKDB_TYPE_DOUBLE: u32 = 11;
const DUCKDB_TYPE_TIMESTAMP: u32 = 12; // microseconds
const DUCKDB_TYPE_DATE: u32 = 13;
const DUCKDB_TYPE_VARCHAR: u32 = 17;
const DUCKDB_TYPE_BLOB: u32 = 18;
const DUCKDB_TYPE_TIMESTAMP_S: u32 = 32;
const DUCKDB_TYPE_TIMESTAMP_MS: u32 = 33;
const DUCKDB_TYPE_TIMESTAMP_NS: u32 = 34;

/// Maximum rows emitted per DuckDB DataChunk call (STANDARD_VECTOR_SIZE).
#[cfg(feature = "duckdb-link")]
const DUCKDB_VECTOR_SIZE: usize = 2048;

/// Tracks whether extension registration has run in-process.
static REGISTER_CALLED: AtomicBool = AtomicBool::new(false);

/// Minimal registration errors while real DuckDB callback wiring lands.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistrationError {
    #[error("duckdb handle was null")]
    NullDatabaseHandle,
    #[error("duckdb native linkage is disabled; rebuild with --features duckdb-link")]
    DuckdbLinkDisabled,
    #[error("failed to create duckdb table function")]
    CreateTableFunctionFailed,
    #[error("duckdb rejected table function registration")]
    RegisterTableFunctionFailed,
}

/// Scaffold object for the future DuckDB table function implementation.
#[derive(Debug, Default)]
pub struct BishTableFunction;

impl BishTableFunction {
    /// Bind stage (DuckDB planner): discover schema and return logical columns.
    pub fn bind<R: Read + Seek>(reader: &mut BishReader<R>) -> Vec<DuckdbColumn> {
        reader
            .schema()
            .fields
            .iter()
            .map(|field| DuckdbColumn {
                name: field.name.clone(),
                logical_type: duckdb_sql_type_name(&field.data_type).to_string(),
                nullable: field.nullable,
            })
            .collect()
    }

    /// Scan stage (DuckDB executor): placeholder for page decoding and chunk
    /// emission into DuckDB's `DataChunk` API.
    pub fn scan_next(&mut self) -> Option<()> {
        None
    }
}

/// Maps a BishType to the DuckDB C API type enum value.
pub fn bish_type_to_duckdb_type_id(typ: &BishType) -> u32 {
    match typ {
        BishType::Boolean => DUCKDB_TYPE_BOOLEAN,
        BishType::Int8 => DUCKDB_TYPE_TINYINT,
        BishType::Int16 => DUCKDB_TYPE_SMALLINT,
        BishType::Int32 => DUCKDB_TYPE_INTEGER,
        BishType::Int64 => DUCKDB_TYPE_BIGINT,
        BishType::UInt8 => DUCKDB_TYPE_UTINYINT,
        BishType::UInt16 => DUCKDB_TYPE_USMALLINT,
        BishType::UInt32 => DUCKDB_TYPE_UINTEGER,
        BishType::UInt64 => DUCKDB_TYPE_UBIGINT,
        BishType::Float32 => DUCKDB_TYPE_FLOAT,
        BishType::Float64 => DUCKDB_TYPE_DOUBLE,
        BishType::TimestampUs => DUCKDB_TYPE_TIMESTAMP,
        BishType::TimestampS => DUCKDB_TYPE_TIMESTAMP_S,
        BishType::TimestampMs => DUCKDB_TYPE_TIMESTAMP_MS,
        BishType::TimestampNs => DUCKDB_TYPE_TIMESTAMP_NS,
        BishType::Date32 => DUCKDB_TYPE_DATE,
        BishType::Utf8 => DUCKDB_TYPE_VARCHAR,
        BishType::Binary => DUCKDB_TYPE_BLOB,
        // Complex types fall back to VARCHAR for now (T-15 will handle nested).
        BishType::Decimal128 { .. } | BishType::List(_) | BishType::Struct(_) | BishType::Vector { .. } => {
            DUCKDB_TYPE_VARCHAR
        }
    }
}

/// Registration hook called by `bish_init`.
pub fn register_bish_functions_for_db(db: *mut c_void) -> Result<(), RegistrationError> {
    if db.is_null() {
        return Err(RegistrationError::NullDatabaseHandle);
    }

    #[cfg(not(feature = "duckdb-link"))]
    {
        return Err(RegistrationError::DuckdbLinkDisabled);
    }

    #[cfg(feature = "duckdb-link")]
    {
        let tf = unsafe { duckdb_create_table_function() };
        if tf.is_null() {
            return Err(RegistrationError::CreateTableFunctionFailed);
        }

        let func_name =
            CString::new(TABLE_FUNCTION_NAME).expect("static function name is valid CStr");

        unsafe {
            duckdb_table_function_set_name(tf, func_name.as_ptr());

            // Register VARCHAR path parameter (the filename argument).
            let mut varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
            duckdb_table_function_add_parameter(tf, varchar_type);
            duckdb_destroy_logical_type(&mut varchar_type);

            duckdb_table_function_set_bind(tf, Some(bish_bind));
            duckdb_table_function_set_init(tf, Some(bish_init));
            duckdb_table_function_set_function(tf, Some(bish_scan));

            let state = duckdb_register_table_function(db, tf);
            let mut tf_to_destroy = tf;
            duckdb_destroy_table_function(&mut tf_to_destroy);

            if state != DUCKDB_SUCCESS {
                return Err(RegistrationError::RegisterTableFunctionFailed);
            }
        }

        REGISTER_CALLED.store(true, Ordering::Relaxed);
        Ok(())
    }
}

/// Test/process helper indicating whether registration has been attempted.
pub fn register_bish_functions() {
    REGISTER_CALLED.store(true, Ordering::Relaxed);
}

pub fn registration_was_called() -> bool {
    REGISTER_CALLED.load(Ordering::Relaxed)
}

// ─────────────────────────────────────────────────────────────────────────────
// T-09 / T-10 / T-11 — BindData, ScanState, and DuckDB callbacks
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "duckdb-link")]
struct BindData {
    path: String,
}

#[cfg(feature = "duckdb-link")]
struct ScanState {
    batch: bish::reader::RecordBatch,
    schema: bish::types::BishSchema,
    cursor: usize,
}

#[cfg(feature = "duckdb-link")]
unsafe extern "C" fn destroy_bind_data(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut BindData));
    }
}

#[cfg(feature = "duckdb-link")]
unsafe extern "C" fn destroy_scan_state(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut ScanState));
    }
}

/// T-09: Bind callback — reads the file path parameter, opens the .bish file,
/// maps its schema to DuckDB logical types, and registers result columns.
#[cfg(feature = "duckdb-link")]
unsafe extern "C" fn bish_bind(bind_info: *mut c_void) {
    // Retrieve the VARCHAR path parameter at index 0.
    let mut val = duckdb_bind_get_parameter(bind_info, 0);
    if val.is_null() {
        let msg = CString::new("read_bish: missing path argument").unwrap();
        duckdb_bind_set_error(bind_info, msg.as_ptr());
        return;
    }

    let path_ptr = duckdb_get_varchar(val);
    duckdb_destroy_value(&mut val);

    if path_ptr.is_null() {
        let msg = CString::new("read_bish: path argument is null").unwrap();
        duckdb_bind_set_error(bind_info, msg.as_ptr());
        return;
    }

    let path = CStr::from_ptr(path_ptr).to_string_lossy().into_owned();
    duckdb_free(path_ptr as *mut c_void);

    // Open and parse the .bish file schema.
    let file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            let msg = CString::new(format!("read_bish: cannot open '{}': {}", path, e))
                .unwrap_or_default();
            duckdb_bind_set_error(bind_info, msg.as_ptr());
            return;
        }
    };

    let mut reader = match BishReader::open(BufReader::new(file)) {
        Ok(r) => r,
        Err(e) => {
            let msg = CString::new(format!("read_bish: cannot parse '{}': {}", path, e))
                .unwrap_or_default();
            duckdb_bind_set_error(bind_info, msg.as_ptr());
            return;
        }
    };

    // Register each schema field as a result column with the correct DuckDB type.
    let schema = reader.schema().clone();
    for field in &schema.fields {
        let col_name = CString::new(field.name.as_str()).unwrap_or_default();
        let type_id = bish_type_to_duckdb_type_id(&field.data_type);
        let mut logical_type = duckdb_create_logical_type(type_id);
        duckdb_bind_add_result_column(bind_info, col_name.as_ptr(), logical_type);
        duckdb_destroy_logical_type(&mut logical_type);
    }

    // Store bind data for use in the init/scan callbacks.
    let bind_data = Box::new(BindData { path });
    duckdb_bind_set_bind_data(
        bind_info,
        Box::into_raw(bind_data) as *mut c_void,
        Some(destroy_bind_data),
    );
}

/// T-10: Init callback — opens the file and reads all data into scan state.
#[cfg(feature = "duckdb-link")]
unsafe extern "C" fn bish_init(init_info: *mut c_void) {
    let bind_ptr = duckdb_init_get_bind_data(init_info) as *const BindData;
    if bind_ptr.is_null() {
        return;
    }
    let path = &(*bind_ptr).path;

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            let msg =
                CString::new(format!("read_bish init: {}", e)).unwrap_or_default();
            duckdb_init_set_error(init_info, msg.as_ptr());
            return;
        }
    };

    let mut reader = match BishReader::open(BufReader::new(file)) {
        Ok(r) => r,
        Err(e) => {
            let msg =
                CString::new(format!("read_bish init: {}", e)).unwrap_or_default();
            duckdb_init_set_error(init_info, msg.as_ptr());
            return;
        }
    };

    let schema = reader.schema().clone();

    let batch = match reader.read_all() {
        Ok(b) => b,
        Err(e) => {
            let msg =
                CString::new(format!("read_bish init: read failed: {}", e)).unwrap_or_default();
            duckdb_init_set_error(init_info, msg.as_ptr());
            return;
        }
    };

    let state = Box::new(ScanState {
        batch,
        schema,
        cursor: 0,
    });
    duckdb_init_set_init_data(
        init_info,
        Box::into_raw(state) as *mut c_void,
        Some(destroy_scan_state),
    );
}

/// T-11: Scan callback — emits up to DUCKDB_VECTOR_SIZE rows per call into
/// the output DataChunk. Returns chunk size 0 to signal EOF.
#[cfg(feature = "duckdb-link")]
unsafe extern "C" fn bish_scan(function_info: *mut c_void, output_chunk: *mut c_void) {
    let state_ptr = duckdb_function_get_init_data(function_info) as *mut ScanState;
    if state_ptr.is_null() {
        duckdb_data_chunk_set_size(output_chunk, 0);
        return;
    }
    let state = &mut *state_ptr;

    let remaining = state.batch.row_count.saturating_sub(state.cursor);
    if remaining == 0 {
        duckdb_data_chunk_set_size(output_chunk, 0);
        return;
    }

    let chunk_size = remaining.min(DUCKDB_VECTOR_SIZE);

    // Iterate over projected columns in the batch.
    for (col_pos, col_values) in state.batch.columns.iter().enumerate() {
        // col_pos maps to the col_idx in the DataChunk.
        // The original schema index is in batch.column_indices[col_pos].
        let schema_idx = state.batch.column_indices[col_pos];
        let field = &state.schema.fields[schema_idx];

        let vec = duckdb_data_chunk_get_vector(output_chunk, col_pos as u64);

        match &field.data_type {
            // ── Integer / temporal types → i64_values ──────────────────────
            BishType::Int8
            | BishType::Int16
            | BishType::Int32
            | BishType::Int64
            | BishType::UInt8
            | BishType::UInt16
            | BishType::UInt32
            | BishType::UInt64
            | BishType::TimestampS
            | BishType::TimestampMs
            | BishType::TimestampUs
            | BishType::TimestampNs
            | BishType::Date32 => {
                emit_i64_column(vec, &col_values.i64_values, state.cursor, chunk_size);
            }

            // ── Float32 ─────────────────────────────────────────────────────
            BishType::Float32 => {
                emit_f32_column(vec, &col_values.f32_values, state.cursor, chunk_size);
            }

            // ── Float64 ─────────────────────────────────────────────────────
            BishType::Float64 => {
                emit_f64_column(vec, &col_values.f64_values, state.cursor, chunk_size);
            }

            // ── Boolean ─────────────────────────────────────────────────────
            BishType::Boolean => {
                emit_bool_column(vec, &col_values.bool_values, state.cursor, chunk_size);
            }

            // ── String / Binary / fallback ──────────────────────────────────
            _ => {
                emit_bytes_column(vec, &col_values.bytes_values, state.cursor, chunk_size);
            }
        }
    }

    state.cursor += chunk_size;
    duckdb_data_chunk_set_size(output_chunk, chunk_size as u64);
}

// ─────────────────────────────────────────────────────────────────────────────
// Column emission helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Emits a slice of Option<i64> into a DuckDB vector, setting the validity
/// bitmask for any NULL entries.
#[cfg(feature = "duckdb-link")]
unsafe fn emit_i64_column(
    vec: *mut c_void,
    values: &[Option<i64>],
    start: usize,
    count: usize,
) {
    let slice = &values[start..start + count];
    let has_nulls = slice.iter().any(|v| v.is_none());
    let data_ptr = duckdb_vector_get_data(vec) as *mut i64;

    if has_nulls {
        duckdb_vector_ensure_validity_writable(vec);
        let validity = duckdb_vector_get_validity(vec);
        for (i, val) in slice.iter().enumerate() {
            match val {
                Some(v) => *data_ptr.add(i) = *v,
                None => {
                    *data_ptr.add(i) = 0;
                    set_null(validity, i);
                }
            }
        }
    } else {
        for (i, val) in slice.iter().enumerate() {
            *data_ptr.add(i) = val.unwrap_or(0);
        }
    }
}

#[cfg(feature = "duckdb-link")]
unsafe fn emit_f32_column(
    vec: *mut c_void,
    values: &[Option<f32>],
    start: usize,
    count: usize,
) {
    let slice = &values[start..start + count];
    let has_nulls = slice.iter().any(|v| v.is_none());
    let data_ptr = duckdb_vector_get_data(vec) as *mut f32;

    if has_nulls {
        duckdb_vector_ensure_validity_writable(vec);
        let validity = duckdb_vector_get_validity(vec);
        for (i, val) in slice.iter().enumerate() {
            match val {
                Some(v) => *data_ptr.add(i) = *v,
                None => {
                    *data_ptr.add(i) = 0.0;
                    set_null(validity, i);
                }
            }
        }
    } else {
        for (i, val) in slice.iter().enumerate() {
            *data_ptr.add(i) = val.unwrap_or(0.0);
        }
    }
}

#[cfg(feature = "duckdb-link")]
unsafe fn emit_f64_column(
    vec: *mut c_void,
    values: &[Option<f64>],
    start: usize,
    count: usize,
) {
    let slice = &values[start..start + count];
    let has_nulls = slice.iter().any(|v| v.is_none());
    let data_ptr = duckdb_vector_get_data(vec) as *mut f64;

    if has_nulls {
        duckdb_vector_ensure_validity_writable(vec);
        let validity = duckdb_vector_get_validity(vec);
        for (i, val) in slice.iter().enumerate() {
            match val {
                Some(v) => *data_ptr.add(i) = *v,
                None => {
                    *data_ptr.add(i) = 0.0;
                    set_null(validity, i);
                }
            }
        }
    } else {
        for (i, val) in slice.iter().enumerate() {
            *data_ptr.add(i) = val.unwrap_or(0.0);
        }
    }
}

/// DuckDB stores booleans as u8 (1 = true, 0 = false).
#[cfg(feature = "duckdb-link")]
unsafe fn emit_bool_column(
    vec: *mut c_void,
    values: &[Option<bool>],
    start: usize,
    count: usize,
) {
    let slice = &values[start..start + count];
    let has_nulls = slice.iter().any(|v| v.is_none());
    let data_ptr = duckdb_vector_get_data(vec) as *mut u8;

    if has_nulls {
        duckdb_vector_ensure_validity_writable(vec);
        let validity = duckdb_vector_get_validity(vec);
        for (i, val) in slice.iter().enumerate() {
            match val {
                Some(v) => *data_ptr.add(i) = *v as u8,
                None => {
                    *data_ptr.add(i) = 0;
                    set_null(validity, i);
                }
            }
        }
    } else {
        for (i, val) in slice.iter().enumerate() {
            *data_ptr.add(i) = val.unwrap_or(false) as u8;
        }
    }
}

/// Emits Utf8 or Binary column values using DuckDB's string assignment API.
/// `duckdb_vector_assign_string_element_len` handles inline vs heap strings.
#[cfg(feature = "duckdb-link")]
unsafe fn emit_bytes_column(
    vec: *mut c_void,
    values: &[Option<Vec<u8>>],
    start: usize,
    count: usize,
) {
    let slice = &values[start..start + count];
    let has_nulls = slice.iter().any(|v| v.is_none());

    if has_nulls {
        duckdb_vector_ensure_validity_writable(vec);
        let validity = duckdb_vector_get_validity(vec);
        for (i, val) in slice.iter().enumerate() {
            match val {
                Some(bytes) => {
                    duckdb_vector_assign_string_element_len(
                        vec,
                        i as u64,
                        bytes.as_ptr() as *const c_char,
                        bytes.len() as u64,
                    );
                }
                None => {
                    set_null(validity, i);
                }
            }
        }
    } else {
        for (i, val) in slice.iter().enumerate() {
            if let Some(bytes) = val {
                duckdb_vector_assign_string_element_len(
                    vec,
                    i as u64,
                    bytes.as_ptr() as *const c_char,
                    bytes.len() as u64,
                );
            }
        }
    }
}

/// Clears bit `row` in a DuckDB validity mask, marking that row as NULL.
/// Validity mask: 64 entries per u64; bit=1 means valid, bit=0 means NULL.
#[cfg(feature = "duckdb-link")]
#[inline]
unsafe fn set_null(validity: *mut u64, row: usize) {
    let word = row / 64;
    let bit = row % 64;
    *validity.add(word) &= !(1u64 << bit);
}

// ─────────────────────────────────────────────────────────────────────────────
// DuckDB C API declarations (resolved at load time from DuckDB process)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "duckdb-link")]
unsafe extern "C" {
    // Table function lifecycle
    fn duckdb_create_table_function() -> *mut c_void;
    fn duckdb_destroy_table_function(table_function: *mut *mut c_void);
    fn duckdb_table_function_set_name(table_function: *mut c_void, name: *const c_char);
    fn duckdb_table_function_add_parameter(
        table_function: *mut c_void,
        logical_type: *mut c_void,
    );
    fn duckdb_table_function_set_bind(
        table_function: *mut c_void,
        bind: Option<unsafe extern "C" fn(*mut c_void)>,
    );
    fn duckdb_table_function_set_init(
        table_function: *mut c_void,
        init: Option<unsafe extern "C" fn(*mut c_void)>,
    );
    fn duckdb_table_function_set_function(
        table_function: *mut c_void,
        function: Option<unsafe extern "C" fn(*mut c_void, *mut c_void)>,
    );
    fn duckdb_register_table_function(connection: *mut c_void, function: *mut c_void) -> u32;

    // Logical types
    fn duckdb_create_logical_type(type_id: u32) -> *mut c_void;
    fn duckdb_destroy_logical_type(type_: *mut *mut c_void);

    // Bind callbacks
    fn duckdb_bind_get_parameter(info: *mut c_void, index: u64) -> *mut c_void;
    fn duckdb_bind_add_result_column(
        info: *mut c_void,
        name: *const c_char,
        logical_type: *mut c_void,
    );
    fn duckdb_bind_set_bind_data(
        info: *mut c_void,
        bind_data: *mut c_void,
        destroy: Option<unsafe extern "C" fn(*mut c_void)>,
    );
    fn duckdb_bind_set_error(info: *mut c_void, error: *const c_char);

    // Value extraction
    fn duckdb_get_varchar(value: *mut c_void) -> *mut c_char;
    fn duckdb_destroy_value(value: *mut *mut c_void);
    fn duckdb_free(ptr: *mut c_void);

    // Init callbacks
    fn duckdb_init_get_bind_data(info: *mut c_void) -> *mut c_void;
    fn duckdb_init_set_init_data(
        info: *mut c_void,
        init_data: *mut c_void,
        destroy: Option<unsafe extern "C" fn(*mut c_void)>,
    );
    fn duckdb_init_set_error(info: *mut c_void, error: *const c_char);

    // Scan callbacks
    fn duckdb_function_get_init_data(info: *mut c_void) -> *mut c_void;

    // DataChunk / vector access
    fn duckdb_data_chunk_get_vector(chunk: *mut c_void, col_idx: u64) -> *mut c_void;
    fn duckdb_data_chunk_set_size(chunk: *mut c_void, size: u64);
    fn duckdb_vector_get_data(vector: *mut c_void) -> *mut c_void;
    fn duckdb_vector_ensure_validity_writable(vector: *mut c_void);
    fn duckdb_vector_get_validity(vector: *mut c_void) -> *mut u64;
    fn duckdb_vector_assign_string_element_len(
        vector: *mut c_void,
        index: u64,
        str: *const c_char,
        str_len: u64,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use bish::footer::BishWriter;
    use bish::types::{BishField, BishSchema, BishType};
    use std::io::Cursor;

    #[test]
    fn bind_derives_columns_from_bish_schema() {
        let schema = BishSchema::new(vec![
            BishField::new("id", BishType::Int64),
            BishField::nullable("city", BishType::Utf8),
        ]);

        let mut writer = BishWriter::new(Cursor::new(Vec::new()), schema).unwrap();
        let rg = writer.new_row_group();
        writer.write_row_group(rg).unwrap();
        let raw = writer.finish_into_bytes().unwrap();

        let mut reader = BishReader::open(Cursor::new(raw)).unwrap();
        let cols = BishTableFunction::bind(&mut reader);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].logical_type, "BIGINT");
        assert!(!cols[0].nullable);
        assert_eq!(cols[1].name, "city");
        assert_eq!(cols[1].logical_type, "VARCHAR");
        assert!(cols[1].nullable);
    }

    #[test]
    fn registration_requires_non_null_db_handle() {
        REGISTER_CALLED.store(false, Ordering::Relaxed);

        let err = register_bish_functions_for_db(std::ptr::null_mut()).unwrap_err();
        assert_eq!(err, RegistrationError::NullDatabaseHandle);
        assert!(!registration_was_called());
    }

    #[test]
    fn bish_type_id_mapping_is_stable() {
        assert_eq!(bish_type_to_duckdb_type_id(&BishType::Int64), 5);
        assert_eq!(bish_type_to_duckdb_type_id(&BishType::Float64), 11);
        assert_eq!(bish_type_to_duckdb_type_id(&BishType::Utf8), 17);
        assert_eq!(bish_type_to_duckdb_type_id(&BishType::Boolean), 1);
        assert_eq!(bish_type_to_duckdb_type_id(&BishType::TimestampUs), 12);
        assert_eq!(bish_type_to_duckdb_type_id(&BishType::Date32), 13);
    }
}
