use bish::reader::BishReader;
use bish::types::BishType;
use std::ffi::c_void;
use std::io::{Read, Seek};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::types::{duckdb_sql_type_name, DuckdbColumn};

/// Name that will be registered as a DuckDB table function.
pub const TABLE_FUNCTION_NAME: &str = "read_bish";

// DuckDB logical type IDs (from duckdb.h DUCKDB_TYPE enum).
pub const DUCKDB_TYPE_BOOLEAN: u32 = 1;
pub const DUCKDB_TYPE_TINYINT: u32 = 2;
pub const DUCKDB_TYPE_SMALLINT: u32 = 3;
pub const DUCKDB_TYPE_INTEGER: u32 = 4;
pub const DUCKDB_TYPE_BIGINT: u32 = 5;
pub const DUCKDB_TYPE_UTINYINT: u32 = 6;
pub const DUCKDB_TYPE_USMALLINT: u32 = 7;
pub const DUCKDB_TYPE_UINTEGER: u32 = 8;
pub const DUCKDB_TYPE_UBIGINT: u32 = 9;
pub const DUCKDB_TYPE_FLOAT: u32 = 10;
pub const DUCKDB_TYPE_DOUBLE: u32 = 11;
pub const DUCKDB_TYPE_TIMESTAMP: u32 = 12; // microseconds
pub const DUCKDB_TYPE_DATE: u32 = 13;
pub const DUCKDB_TYPE_VARCHAR: u32 = 17;
pub const DUCKDB_TYPE_BLOB: u32 = 18;
pub const DUCKDB_TYPE_TIMESTAMP_S: u32 = 32;
pub const DUCKDB_TYPE_TIMESTAMP_MS: u32 = 33;
pub const DUCKDB_TYPE_TIMESTAMP_NS: u32 = 34;

/// Maximum rows emitted per DuckDB DataChunk call (STANDARD_VECTOR_SIZE).
const DUCKDB_VECTOR_SIZE: usize = 2048;
/// DuckDB C API success code.
const DUCKDB_SUCCESS: u32 = 0;

/// Tracks whether extension registration has run in-process.
static REGISTER_CALLED: AtomicBool = AtomicBool::new(false);

/// Minimal registration errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistrationError {
    #[error("duckdb handle was null")]
    NullDatabaseHandle,
    #[error("duckdb native linkage not available in test mode")]
    DuckdbLinkDisabled,
    #[error("failed to create duckdb table function")]
    CreateTableFunctionFailed,
    #[error("duckdb rejected table function registration")]
    RegisterTableFunctionFailed,
}

/// Scaffold object retained for Rust-level unit tests.
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
        // Complex types fall back to VARCHAR (T-15 will handle nested types).
        BishType::Decimal128 { .. }
        | BishType::List(_)
        | BishType::Struct(_)
        | BishType::Vector { .. } => DUCKDB_TYPE_VARCHAR,
    }
}

/// Registration hook called by the DuckDB extension entry point.
///
/// In test builds the DuckDB C API symbols are not available, so the
/// function returns `DuckdbLinkDisabled`. In cdylib builds all symbols
/// resolve at load time from the DuckDB host process.
pub fn register_bish_functions_for_db(db: *mut c_void) -> Result<(), RegistrationError> {
    if db.is_null() {
        return Err(RegistrationError::NullDatabaseHandle);
    }

    // In test builds the DuckDB C API is not linked; skip real registration.
    #[cfg(test)]
    {
        return Err(RegistrationError::DuckdbLinkDisabled);
    }

    // In cdylib builds create a connection from the database handle, register,
    // then disconnect.  The db pointer here is a duckdb_database handle, not
    // a duckdb_connection — we must go through duckdb_connect first.
    #[cfg(not(test))]
    unsafe {
        let mut conn: *mut c_void = std::ptr::null_mut();
        // duckdb_connect takes (duckdb_database, *mut duckdb_connection).
        // Both are pointer-to-struct wrappers; we cast to the void* equivalents
        // accepted by our extern "C" declarations.
        let state = duckdb_connect(db, &mut conn as *mut *mut c_void);
        if state != DUCKDB_SUCCESS || conn.is_null() {
            return Err(RegistrationError::RegisterTableFunctionFailed);
        }
        let result = bish_register_with_conn_impl(conn);
        duckdb_disconnect(&mut conn as *mut *mut c_void);
        result
    }
}

/// Called by the C++ shim (shim.cpp) after it constructs a duckdb::Connection.
///
/// `conn` is a `duckdb::Connection *` cast to void*.  DuckDB's C API treats
/// `duckdb_connection` as an opaque pointer to `Connection`, so passing a
/// `Connection*` here is correct.
#[no_mangle]
#[cfg(not(test))]
pub extern "C" fn bish_register_with_conn(conn: *mut c_void) {
    if conn.is_null() {
        return;
    }
    let _ = bish_register_with_conn_impl(conn);
}

/// Inner registration logic shared by both entry paths.
#[cfg(not(test))]
fn bish_register_with_conn_impl(conn: *mut c_void) -> Result<(), RegistrationError> {
    use std::ffi::CString;

    let tf = unsafe { duckdb_create_table_function() };
    if tf.is_null() {
        return Err(RegistrationError::CreateTableFunctionFailed);
    }

    let func_name =
        CString::new(TABLE_FUNCTION_NAME).expect("static function name is valid CStr");

    unsafe {
        duckdb_table_function_set_name(tf, func_name.as_ptr());

        // Path parameter (VARCHAR).
        let mut varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
        duckdb_table_function_add_parameter(tf, varchar_type);
        duckdb_destroy_logical_type(&mut varchar_type);

        duckdb_table_function_set_bind(tf, Some(bish_bind));
        duckdb_table_function_set_init(tf, Some(bish_table_init));
        duckdb_table_function_set_function(tf, Some(bish_scan));

        let rc = duckdb_register_table_function(conn, tf);
        let mut tf_to_destroy = tf;
        duckdb_destroy_table_function(&mut tf_to_destroy);

        if rc != DUCKDB_SUCCESS {
            return Err(RegistrationError::RegisterTableFunctionFailed);
        }
    }

    REGISTER_CALLED.store(true, Ordering::Relaxed);
    Ok(())
}

pub fn register_bish_functions() {
    REGISTER_CALLED.store(true, Ordering::Relaxed);
}

pub fn registration_was_called() -> bool {
    REGISTER_CALLED.load(Ordering::Relaxed)
}

// ─────────────────────────────────────────────────────────────────────────────
// T-09 / T-10 / T-11 — Bind / Init / Scan callbacks
//
// These are compiled into the cdylib but excluded from test binaries so that
// the test linker does not need to resolve the DuckDB C API symbols.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(not(test))]
struct BindData {
    path: String,
}

#[cfg(not(test))]
struct ScanState {
    batch: bish::reader::RecordBatch,
    schema: bish::types::BishSchema,
    cursor: usize,
}

#[cfg(not(test))]
unsafe extern "C" fn destroy_bind_data(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut BindData));
    }
}

#[cfg(not(test))]
unsafe extern "C" fn destroy_scan_state(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut ScanState));
    }
}

/// T-09 — Bind: reads path parameter, maps schema to DuckDB logical types.
#[cfg(not(test))]
unsafe extern "C" fn bish_bind(bind_info: *mut c_void) {
    use std::ffi::{CStr, CString};

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

    let file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            let msg =
                CString::new(format!("read_bish: cannot open '{}': {}", path, e))
                    .unwrap_or_default();
            duckdb_bind_set_error(bind_info, msg.as_ptr());
            return;
        }
    };

    let reader = match BishReader::open(std::io::BufReader::new(file)) {
        Ok(r) => r,
        Err(e) => {
            let msg =
                CString::new(format!("read_bish: cannot parse '{}': {}", path, e))
                    .unwrap_or_default();
            duckdb_bind_set_error(bind_info, msg.as_ptr());
            return;
        }
    };

    let schema = reader.schema().clone();
    for field in &schema.fields {
        let col_name = CString::new(field.name.as_str()).unwrap_or_default();
        let type_id = bish_type_to_duckdb_type_id(&field.data_type);
        let mut lt = duckdb_create_logical_type(type_id);
        duckdb_bind_add_result_column(bind_info, col_name.as_ptr(), lt);
        duckdb_destroy_logical_type(&mut lt);
    }

    let bind_data = Box::new(BindData { path });
    duckdb_bind_set_bind_data(
        bind_info,
        Box::into_raw(bind_data) as *mut c_void,
        Some(destroy_bind_data),
    );
}

/// T-10 — Init: opens the .bish file and reads all rows into ScanState.
#[cfg(not(test))]
unsafe extern "C" fn bish_table_init(init_info: *mut c_void) {
    use std::ffi::CString;

    let bind_ptr = duckdb_init_get_bind_data(init_info) as *const BindData;
    if bind_ptr.is_null() {
        return;
    }
    let path = &(*bind_ptr).path;

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            let msg = CString::new(format!("read_bish init: {}", e)).unwrap_or_default();
            duckdb_init_set_error(init_info, msg.as_ptr());
            return;
        }
    };

    let mut reader = match BishReader::open(std::io::BufReader::new(file)) {
        Ok(r) => r,
        Err(e) => {
            let msg = CString::new(format!("read_bish init: {}", e)).unwrap_or_default();
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

/// T-11 — Scan: emits up to DUCKDB_VECTOR_SIZE rows per call.
#[cfg(not(test))]
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

    for (col_pos, col_values) in state.batch.columns.iter().enumerate() {
        let schema_idx = state.batch.column_indices[col_pos];
        let field = &state.schema.fields[schema_idx];
        let vec = duckdb_data_chunk_get_vector(output_chunk, col_pos as u64);

        match &field.data_type {
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
            BishType::Float32 => {
                emit_f32_column(vec, &col_values.f32_values, state.cursor, chunk_size);
            }
            BishType::Float64 => {
                emit_f64_column(vec, &col_values.f64_values, state.cursor, chunk_size);
            }
            BishType::Boolean => {
                emit_bool_column(vec, &col_values.bool_values, state.cursor, chunk_size);
            }
            _ => {
                emit_bytes_column(vec, &col_values.bytes_values, state.cursor, chunk_size);
            }
        }
    }

    state.cursor += chunk_size;
    duckdb_data_chunk_set_size(output_chunk, chunk_size as u64);
}

// ─────────────────────────────────────────────────────────────────────────────
// Column emission helpers (cdylib only)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(not(test))]
unsafe fn emit_i64_column(vec: *mut c_void, values: &[Option<i64>], start: usize, count: usize) {
    let slice = &values[start..start + count];
    let data_ptr = duckdb_vector_get_data(vec) as *mut i64;
    if slice.iter().any(|v| v.is_none()) {
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

#[cfg(not(test))]
unsafe fn emit_f32_column(vec: *mut c_void, values: &[Option<f32>], start: usize, count: usize) {
    let slice = &values[start..start + count];
    let data_ptr = duckdb_vector_get_data(vec) as *mut f32;
    if slice.iter().any(|v| v.is_none()) {
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

#[cfg(not(test))]
unsafe fn emit_f64_column(vec: *mut c_void, values: &[Option<f64>], start: usize, count: usize) {
    let slice = &values[start..start + count];
    let data_ptr = duckdb_vector_get_data(vec) as *mut f64;
    if slice.iter().any(|v| v.is_none()) {
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

#[cfg(not(test))]
unsafe fn emit_bool_column(
    vec: *mut c_void,
    values: &[Option<bool>],
    start: usize,
    count: usize,
) {
    let slice = &values[start..start + count];
    let data_ptr = duckdb_vector_get_data(vec) as *mut u8;
    if slice.iter().any(|v| v.is_none()) {
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

#[cfg(not(test))]
unsafe fn emit_bytes_column(
    vec: *mut c_void,
    values: &[Option<Vec<u8>>],
    start: usize,
    count: usize,
) {
    use std::ffi::c_char;
    let slice = &values[start..start + count];
    if slice.iter().any(|v| v.is_none()) {
        duckdb_vector_ensure_validity_writable(vec);
        let validity = duckdb_vector_get_validity(vec);
        for (i, val) in slice.iter().enumerate() {
            match val {
                Some(bytes) => duckdb_vector_assign_string_element_len(
                    vec,
                    i as u64,
                    bytes.as_ptr() as *const c_char,
                    bytes.len() as u64,
                ),
                None => set_null(validity, i),
            }
        }
    } else {
        for (i, val) in slice.iter().enumerate() {
            if let Some(bytes) = val {
                duckdb_vector_assign_string_element_len(
                    vec,
                    i as u64,
                    bytes.as_ptr() as *const std::ffi::c_char,
                    bytes.len() as u64,
                );
            }
        }
    }
}

/// Clears bit `row` in a DuckDB validity mask (bit=0 → NULL).
#[cfg(not(test))]
#[inline]
unsafe fn set_null(validity: *mut u64, row: usize) {
    *validity.add(row / 64) &= !(1u64 << (row % 64));
}

// ─────────────────────────────────────────────────────────────────────────────
// DuckDB C API — resolved at runtime from the host DuckDB process.
// Not compiled in test mode to avoid linker errors.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(not(test))]
unsafe extern "C" {
    // Database / connection lifecycle.
    fn duckdb_connect(database: *mut c_void, out_connection: *mut *mut c_void) -> u32;
    fn duckdb_disconnect(connection: *mut *mut c_void);

    fn duckdb_create_table_function() -> *mut c_void;
    fn duckdb_destroy_table_function(table_function: *mut *mut c_void);
    fn duckdb_table_function_set_name(table_function: *mut c_void, name: *const std::ffi::c_char);
    fn duckdb_table_function_add_parameter(table_function: *mut c_void, logical_type: *mut c_void);
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

    fn duckdb_create_logical_type(type_id: u32) -> *mut c_void;
    fn duckdb_destroy_logical_type(type_: *mut *mut c_void);

    fn duckdb_bind_get_parameter(info: *mut c_void, index: u64) -> *mut c_void;
    fn duckdb_bind_add_result_column(
        info: *mut c_void,
        name: *const std::ffi::c_char,
        logical_type: *mut c_void,
    );
    fn duckdb_bind_set_bind_data(
        info: *mut c_void,
        bind_data: *mut c_void,
        destroy: Option<unsafe extern "C" fn(*mut c_void)>,
    );
    fn duckdb_bind_set_error(info: *mut c_void, error: *const std::ffi::c_char);

    fn duckdb_get_varchar(value: *mut c_void) -> *mut std::ffi::c_char;
    fn duckdb_destroy_value(value: *mut *mut c_void);
    fn duckdb_free(ptr: *mut c_void);

    fn duckdb_init_get_bind_data(info: *mut c_void) -> *mut c_void;
    fn duckdb_init_set_init_data(
        info: *mut c_void,
        init_data: *mut c_void,
        destroy: Option<unsafe extern "C" fn(*mut c_void)>,
    );
    fn duckdb_init_set_error(info: *mut c_void, error: *const std::ffi::c_char);

    fn duckdb_function_get_init_data(info: *mut c_void) -> *mut c_void;

    fn duckdb_data_chunk_get_vector(chunk: *mut c_void, col_idx: u64) -> *mut c_void;
    fn duckdb_data_chunk_set_size(chunk: *mut c_void, size: u64);
    fn duckdb_vector_get_data(vector: *mut c_void) -> *mut c_void;
    fn duckdb_vector_ensure_validity_writable(vector: *mut c_void);
    fn duckdb_vector_get_validity(vector: *mut c_void) -> *mut u64;
    fn duckdb_vector_assign_string_element_len(
        vector: *mut c_void,
        index: u64,
        str: *const std::ffi::c_char,
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
