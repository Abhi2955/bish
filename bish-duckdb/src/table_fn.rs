use bish::reader::BishReader;
use std::ffi::{c_char, c_void, CString};
use std::io::{Read, Seek};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::types::{duckdb_sql_type_name, DuckdbColumn};

/// Name that will be registered as a DuckDB table function.
pub const TABLE_FUNCTION_NAME: &str = "read_bish";

/// DuckDB returns 0 for success in the C API.
const DUCKDB_SUCCESS: u32 = 0;

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

/// Registration hook called by `bish_init`.
///
/// This starts milestone-1 usable registration by creating and registering an
/// actual DuckDB table function handle via C API.
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

#[cfg(feature = "duckdb-link")]
unsafe extern "C" fn bish_bind(_bind_info: *mut c_void) {
    // T-09 will convert BishSchema -> DuckDB logical types in this callback.
}

#[cfg(feature = "duckdb-link")]
unsafe extern "C" fn bish_init(_init_info: *mut c_void) {
    // T-10 will initialize projection/filter state for pushdown.
}

#[cfg(feature = "duckdb-link")]
unsafe extern "C" fn bish_scan(_function_info: *mut c_void, _output_chunk: *mut c_void) {
    // T-11 will emit decoded values into DuckDB DataChunk.
}

#[cfg(feature = "duckdb-link")]
unsafe extern "C" {
    fn duckdb_create_table_function() -> *mut c_void;
    fn duckdb_destroy_table_function(table_function: *mut *mut c_void);
    fn duckdb_table_function_set_name(table_function: *mut c_void, name: *const c_char);
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
}
