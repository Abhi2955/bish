//! # .bish type system
//!
//! Every column in a .bish file has a [`BishType`]. Types map 1-to-1 onto
//! Arrow IPC types so any Arrow-native tool (DuckDB, Polars, DataFusion)
//! can round-trip through the schema without a bespoke parser.
//!
//! ## Type encoding on disk
//!
//! Types are never stored raw in the binary — they live inside the Arrow IPC
//! schema in footer chunk A. The [`BishType`] enum is the in-memory
//! representation; [`arrow2::datatypes::DataType`] is the on-disk form.

use arrow2::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, TimeUnit, Schema as ArrowSchema,
};
use std::collections::HashMap;
use std::collections::BTreeMap;
use crate::error::{BishError, BishResult};

// ─────────────────────────────────────────────────────────────────────────────
// BishType
// ─────────────────────────────────────────────────────────────────────────────

/// Every value stored in a `.bish` column has one of these types.
///
/// The enum variants map exactly onto Arrow IPC data types — see
/// [`BishType::to_arrow`] and [`BishType::from_arrow`] for the
/// bidirectional mapping.
///
/// # Example
/// ```
/// use bish::types::BishType;
///
/// let t = BishType::Utf8;
/// assert_eq!(t.byte_width(), None); // variable-length
///
/// let t = BishType::Int64;
/// assert_eq!(t.byte_width(), Some(8)); // fixed 8 bytes per value
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum BishType {
    // ── Signed integers ───────────────────────────────────────────────────
    /// 8-bit signed integer. Range: -128 to 127.
    Int8,
    /// 16-bit signed integer. Range: -32,768 to 32,767.
    Int16,
    /// 32-bit signed integer. Range: -2^31 to 2^31−1.
    Int32,
    /// 64-bit signed integer. Range: -2^63 to 2^63−1.
    Int64,

    // ── Unsigned integers ─────────────────────────────────────────────────
    /// 8-bit unsigned integer. Range: 0 to 255.
    UInt8,
    /// 16-bit unsigned integer. Range: 0 to 65,535.
    UInt16,
    /// 32-bit unsigned integer. Range: 0 to 2^32−1.
    UInt32,
    /// 64-bit unsigned integer. Range: 0 to 2^64−1.
    UInt64,

    // ── Floating point ────────────────────────────────────────────────────
    /// 32-bit IEEE 754 float.
    Float32,
    /// 64-bit IEEE 754 float.
    Float64,

    // ── Boolean ───────────────────────────────────────────────────────────
    /// Boolean — bit-packed on disk, 8 booleans per byte.
    Boolean,

    // ── Variable-length binary ────────────────────────────────────────────
    /// Variable-length UTF-8 string. Stored as length-prefixed bytes.
    /// Zone map uses lexicographic min/max.
    Utf8,
    /// Variable-length raw bytes. No encoding assumed.
    Binary,

    // ── Temporal ─────────────────────────────────────────────────────────
    /// Days since Unix epoch (1970-01-01) as i32.
    Date32,
    /// Nanoseconds since Unix epoch as i64, always UTC.
    /// Preferred timestamp type — maximum precision.
    TimestampNs,
    /// Microseconds since Unix epoch as i64, always UTC.
    /// Use when nanosecond precision is unnecessary.
    TimestampUs,
    /// Milliseconds since Unix epoch as i64, always UTC.
    TimestampMs,
    /// Seconds since Unix epoch as i64, always UTC.
    TimestampS,

    // ── Fixed-point decimal ───────────────────────────────────────────────
    /// 128-bit fixed-point decimal with configurable precision and scale.
    ///
    /// `precision`: total number of significant digits (1–38).
    /// `scale`: number of digits after the decimal point.
    ///
    /// Stored as a 128-bit signed integer; the actual value is
    /// `raw_value / 10^scale`.
    Decimal128 {
        precision: u8,
        scale: i8,
    },

    // ── Vector / embedding ────────────────────────────────────────────────
    /// Fixed-length array of 32-bit floats — for ML embeddings and ANN search.
    ///
    /// `dim`: the embedding dimension (e.g. 1536 for OpenAI ada-002).
    ///
    /// Stored as a contiguous block of `dim × 4` bytes per value.
    /// Optional HNSW vector index block enables ANN queries directly.
    Vector {
        dim: u32,
    },

    // ── Nested ────────────────────────────────────────────────────────────
    /// Variable-length list of elements of a single inner type.
    ///
    /// Stored using Arrow's list encoding: an offsets buffer (i32[])
    /// followed by a values buffer of the inner type.
    List(Box<BishType>),

    /// Struct with named fields — each field has its own [`BishType`].
    ///
    /// Stored as Arrow struct encoding: one child array per field,
    /// aligned by row index.
    Struct(Vec<BishField>),
}

impl BishType {
    /// Fixed byte width per value, or `None` for variable-length types.
    ///
    /// Used by the writer to estimate page sizes and choose encodings.
    pub fn byte_width(&self) -> Option<usize> {
        match self {
            BishType::Int8 | BishType::UInt8 | BishType::Boolean => Some(1),
            BishType::Int16 | BishType::UInt16 => Some(2),
            BishType::Int32 | BishType::UInt32 | BishType::Date32 | BishType::Float32 => Some(4),
            BishType::Int64
            | BishType::UInt64
            | BishType::Float64
            | BishType::TimestampNs
            | BishType::TimestampUs
            | BishType::TimestampMs
            | BishType::TimestampS => Some(8),
            BishType::Decimal128 { .. } => Some(8), // stored as i64 in v0.1; full i128 in future
            BishType::Vector { dim } => Some(*dim as usize * 4),
            // Variable-length — byte width unknown at schema time
            BishType::Utf8 | BishType::Binary | BishType::List(_) | BishType::Struct(_) => None,
        }
    }

    /// Whether this type is numeric (supports arithmetic zone maps).
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            BishType::Int8
                | BishType::Int16
                | BishType::Int32
                | BishType::Int64
                | BishType::UInt8
                | BishType::UInt16
                | BishType::UInt32
                | BishType::UInt64
                | BishType::Float32
                | BishType::Float64
                | BishType::Decimal128 { .. }
                | BishType::TimestampNs
                | BishType::TimestampUs
                | BishType::TimestampMs
                | BishType::TimestampS
                | BishType::Date32
        )
    }

    /// Whether this type supports bloom filter point lookups.
    ///
    /// All types except `Float32`, `Float64`, and nested types support
    /// bloom filters — NaN semantics make float equality unreliable.
    pub fn supports_bloom_filter(&self) -> bool {
        !matches!(
            self,
            BishType::Float32 | BishType::Float64 | BishType::List(_) | BishType::Struct(_)
        )
    }

    /// Whether this type can be used as a partition key.
    pub fn is_partitionable(&self) -> bool {
        matches!(
            self,
            BishType::Int8
                | BishType::Int16
                | BishType::Int32
                | BishType::Int64
                | BishType::UInt8
                | BishType::UInt16
                | BishType::UInt32
                | BishType::UInt64
                | BishType::Utf8
                | BishType::Date32
                | BishType::TimestampNs
                | BishType::TimestampUs
                | BishType::TimestampMs
                | BishType::TimestampS
                | BishType::Boolean
        )
    }

    /// Convert to the corresponding Arrow [`DataType`].
    ///
    /// This is how `.bish` types are serialised into footer chunk A —
    /// the Arrow IPC schema carries Arrow types, not bish types.
    /// On read, [`BishType::from_arrow`] inverts this mapping.
    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            BishType::Int8 => ArrowDataType::Int8,
            BishType::Int16 => ArrowDataType::Int16,
            BishType::Int32 => ArrowDataType::Int32,
            BishType::Int64 => ArrowDataType::Int64,
            BishType::UInt8 => ArrowDataType::UInt8,
            BishType::UInt16 => ArrowDataType::UInt16,
            BishType::UInt32 => ArrowDataType::UInt32,
            BishType::UInt64 => ArrowDataType::UInt64,
            BishType::Float32 => ArrowDataType::Float32,
            BishType::Float64 => ArrowDataType::Float64,
            BishType::Boolean => ArrowDataType::Boolean,
            BishType::Utf8 => ArrowDataType::Utf8,
            BishType::Binary => ArrowDataType::Binary,
            BishType::Date32 => ArrowDataType::Date32,
            BishType::TimestampNs => {
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            BishType::TimestampUs => {
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            BishType::TimestampMs => {
                ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
            }
            BishType::TimestampS => {
                ArrowDataType::Timestamp(TimeUnit::Second, Some("UTC".into()))
            }
            BishType::Decimal128 { precision, scale } => {
                ArrowDataType::Decimal(*precision as usize, *scale as usize)
            }
            // Vector → Arrow FixedSizeList<f32>
            BishType::Vector { dim } => ArrowDataType::FixedSizeList(
                Box::new(ArrowField::new("item", ArrowDataType::Float32, false)),
                *dim as usize,
            ),
            // List<T> → Arrow List<T>
            BishType::List(inner) => ArrowDataType::List(Box::new(ArrowField::new(
                "item",
                inner.to_arrow(),
                true, // inner values are nullable
            ))),
            // Struct → Arrow Struct
            BishType::Struct(fields) => ArrowDataType::Struct(
                fields.iter().map(|f| f.to_arrow_field()).collect(),
            ),
        }
    }

    /// Reconstruct a [`BishType`] from an Arrow [`DataType`].
    ///
    /// Called when opening a `.bish` file — chunk A contains an Arrow IPC
    /// schema and this converts each field's Arrow type back to a BishType.
    pub fn from_arrow(dt: &ArrowDataType) -> BishResult<Self> {
        match dt {
            ArrowDataType::Int8 => Ok(BishType::Int8),
            ArrowDataType::Int16 => Ok(BishType::Int16),
            ArrowDataType::Int32 => Ok(BishType::Int32),
            ArrowDataType::Int64 => Ok(BishType::Int64),
            ArrowDataType::UInt8 => Ok(BishType::UInt8),
            ArrowDataType::UInt16 => Ok(BishType::UInt16),
            ArrowDataType::UInt32 => Ok(BishType::UInt32),
            ArrowDataType::UInt64 => Ok(BishType::UInt64),
            ArrowDataType::Float32 => Ok(BishType::Float32),
            ArrowDataType::Float64 => Ok(BishType::Float64),
            ArrowDataType::Boolean => Ok(BishType::Boolean),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(BishType::Utf8),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(BishType::Binary),
            ArrowDataType::Date32 => Ok(BishType::Date32),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(BishType::TimestampNs),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => Ok(BishType::TimestampUs),
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => Ok(BishType::TimestampMs),
            ArrowDataType::Timestamp(TimeUnit::Second, _) => Ok(BishType::TimestampS),
            ArrowDataType::Decimal(precision, scale) => Ok(BishType::Decimal128 {
                precision: *precision as u8,
                scale: *scale as i8,
            }),
            // FixedSizeList<f32> → Vector
            ArrowDataType::FixedSizeList(inner_field, dim) => {
                if inner_field.data_type() == &ArrowDataType::Float32 {
                    Ok(BishType::Vector { dim: *dim as u32 })
                } else {
                    Err(BishError::UnsupportedType(format!(
                        "FixedSizeList inner type must be Float32 for Vector, got {:?}",
                        inner_field.data_type()
                    )))
                }
            }
            ArrowDataType::List(inner_field) => {
                let inner = BishType::from_arrow(inner_field.data_type())?;
                Ok(BishType::List(Box::new(inner)))
            }
            ArrowDataType::Struct(arrow_fields) => {
                let fields = arrow_fields
                    .iter()
                    .map(BishField::from_arrow_field)
                    .collect::<BishResult<Vec<_>>>()?;
                Ok(BishType::Struct(fields))
            }
            other => Err(BishError::UnsupportedType(format!(
                "Arrow type {:?} has no .bish equivalent",
                other
            ))),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BishField
// ─────────────────────────────────────────────────────────────────────────────

/// A single named, typed, nullable column field in a `.bish` schema.
///
/// Fields appear in schema order. The column's position in this list is
/// its `column_index` — used everywhere in the binary format to reference
/// a column without repeating its name.
#[derive(Debug, Clone, PartialEq)]
pub struct BishField {
    /// Column name. Must be valid UTF-8. Must be unique within the schema.
    pub name: String,

    /// The type of values stored in this column.
    pub data_type: BishType,

    /// Whether this column can contain null values.
    ///
    /// If `false`, the writer guarantees no nulls and the null bitmask
    /// is omitted from pages, saving space.
    pub nullable: bool,

    /// Arbitrary key-value metadata attached to this field.
    ///
    /// Reserved keys (prefix `bish.`):
    /// - `bish.sort_key` = `"true"` — this column is the file's sort key
    /// - `bish.partition_key` = `"true"` — this column is a partition key
    /// - `bish.doc` — human-readable description of this column
    pub metadata: HashMap<String, String>,
}

impl BishField {
    /// Create a non-nullable field with no metadata.
    pub fn new(name: impl Into<String>, data_type: BishType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
            metadata: HashMap::new(),
        }
    }

    /// Create a nullable field with no metadata.
    pub fn nullable(name: impl Into<String>, data_type: BishType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            metadata: HashMap::new(),
        }
    }

    /// Mark this field as the file's sort key.
    pub fn with_sort_key(mut self) -> Self {
        self.metadata
            .insert("bish.sort_key".into(), "true".into());
        self
    }

    /// Mark this field as a partition key.
    pub fn with_partition_key(mut self) -> Self {
        self.metadata
            .insert("bish.partition_key".into(), "true".into());
        self
    }

    /// Attach a human-readable description to this field.
    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.metadata.insert("bish.doc".into(), doc.into());
        self
    }

    /// Returns `true` if this field is marked as the sort key.
    pub fn is_sort_key(&self) -> bool {
        self.metadata.get("bish.sort_key").map_or(false, |v| v == "true")
    }

    /// Returns `true` if this field is marked as a partition key.
    pub fn is_partition_key(&self) -> bool {
        self.metadata
            .get("bish.partition_key")
            .map_or(false, |v| v == "true")
    }

    /// Convert to an Arrow [`Field`] for IPC serialisation into footer chunk A.
    pub fn to_arrow_field(&self) -> ArrowField {
        let btree_meta: BTreeMap<String,String> = self.metadata.iter()
            .map(|(k,v)|(k.clone(),v.clone())).collect();
        let mut af = ArrowField::new(
            &self.name,
            self.data_type.to_arrow(),
            self.nullable,
        );
        af.metadata = btree_meta;
        af
    }

    /// Reconstruct a [`BishField`] from an Arrow [`Field`] during file open.
    pub fn from_arrow_field(f: &ArrowField) -> BishResult<Self> {
        Ok(Self {
            name: f.name.clone(),
            data_type: BishType::from_arrow(f.data_type())?,
            nullable: f.is_nullable,
            metadata: f.metadata.iter().map(|(k,v)|(k.clone(),v.clone())).collect(),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BishSchema
// ─────────────────────────────────────────────────────────────────────────────

/// The schema of a `.bish` file — an ordered list of fields plus file-level
/// metadata.
///
/// The schema is stored in footer chunk A as an Arrow IPC schema message.
/// It is loaded by every reader as the first thing after parsing the
/// 512-byte super-footer.
///
/// # Example
/// ```
/// use bish::types::{BishSchema, BishField, BishType};
///
/// let schema = BishSchema::new(vec![
///     BishField::new("user_id",   BishType::Utf8).with_sort_key(),
///     BishField::new("city",      BishType::Utf8).with_partition_key(),
///     BishField::new("amount",    BishType::Float64),
///     BishField::nullable("tags", BishType::List(Box::new(BishType::Utf8))),
/// ]);
///
/// assert_eq!(schema.num_columns(), 4);
/// assert_eq!(schema.column_index("amount"), Some(2));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct BishSchema {
    /// Ordered list of fields. Position = column_index used in binary format.
    pub fields: Vec<BishField>,

    /// File-level key-value metadata.
    ///
    /// Reserved keys (prefix `bish.`):
    /// - `bish.created_by`  — e.g. `"bish-rs 0.1.0"`
    /// - `bish.created_at`  — ISO-8601 creation timestamp
    /// - `bish.description` — human-readable file description
    pub metadata: HashMap<String, String>,
}

impl BishSchema {
    /// Create a schema from a list of fields with no file metadata.
    pub fn new(fields: Vec<BishField>) -> Self {
        Self {
            fields,
            metadata: HashMap::new(),
        }
    }

    /// Attach file-level metadata to this schema.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Number of columns in this schema.
    pub fn num_columns(&self) -> usize {
        self.fields.len()
    }

    /// Look up a field by name. Returns `None` if the name doesn't exist.
    pub fn field(&self, name: &str) -> Option<&BishField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Return the 0-based column index for a field name.
    ///
    /// This index is used throughout the binary format — in column chunk
    /// headers, zone map entries, bloom filter entries, etc.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    /// Return all fields marked as partition keys, in schema order.
    pub fn partition_keys(&self) -> Vec<&BishField> {
        self.fields.iter().filter(|f| f.is_partition_key()).collect()
    }

    /// Return the field marked as the sort key, if any.
    pub fn sort_key(&self) -> Option<&BishField> {
        self.fields.iter().find(|f| f.is_sort_key())
    }

    /// Validate the schema — checks uniqueness of field names and type rules.
    pub fn validate(&self) -> BishResult<()> {
        // Field names must be unique
        let mut seen = std::collections::HashSet::new();
        for f in &self.fields {
            if f.name.is_empty() {
                return Err(BishError::InvalidSchema("Field name must not be empty".into()));
            }
            if !seen.insert(&f.name) {
                return Err(BishError::InvalidSchema(format!(
                    "Duplicate field name: '{}'",
                    f.name
                )));
            }
        }

        // At most one sort key
        let sort_keys: Vec<_> = self.fields.iter().filter(|f| f.is_sort_key()).collect();
        if sort_keys.len() > 1 {
            return Err(BishError::InvalidSchema(
                "Schema may have at most one sort key field".into(),
            ));
        }

        // Sort key must be a partitionable (orderable) type
        if let Some(sk) = sort_keys.first() {
            if !sk.data_type.is_partitionable() {
                return Err(BishError::InvalidSchema(format!(
                    "Sort key field '{}' has type {:?} which is not orderable",
                    sk.name, sk.data_type
                )));
            }
        }

        // Partition keys must be partitionable types
        for pk in self.partition_keys() {
            if !pk.data_type.is_partitionable() {
                return Err(BishError::InvalidSchema(format!(
                    "Partition key field '{}' has type {:?} which is not partitionable",
                    pk.name, pk.data_type
                )));
            }
        }

        // Decimal precision must be in range 1–38
        for f in &self.fields {
            if let BishType::Decimal128 { precision, .. } = &f.data_type {
                if *precision == 0 || *precision > 38 {
                    return Err(BishError::InvalidSchema(format!(
                        "Field '{}': Decimal128 precision must be 1–38, got {}",
                        f.name, precision
                    )));
                }
            }
            // Vector dimension must be > 0
            if let BishType::Vector { dim } = &f.data_type {
                if *dim == 0 {
                    return Err(BishError::InvalidSchema(format!(
                        "Field '{}': Vector dimension must be > 0",
                        f.name
                    )));
                }
            }
        }

        Ok(())
    }

    // ── Arrow IPC round-trip ─────────────────────────────────────────────

    /// Serialise this schema to an Arrow IPC schema message (flatbuffer bytes).
    ///
    /// This is the payload stored in footer chunk A. Every Arrow-native tool
    /// can deserialise it without knowing anything about .bish.
    pub fn to_arrow_ipc_bytes(&self) -> BishResult<Vec<u8>> {
        let arrow_schema = self.to_arrow_schema();
        let mut buf = Vec::new();
        let mut writer = arrow2::io::ipc::write::StreamWriter::new(
            &mut buf,
            arrow2::io::ipc::write::WriteOptions { compression: None },
        );
        writer.start(&arrow_schema, None)?;
        // We only write the schema — no record batches
        writer.finish()?;
        Ok(buf)
    }

    /// Deserialise a [`BishSchema`] from Arrow IPC schema bytes (footer chunk A).
    pub fn from_arrow_ipc_bytes(bytes: &[u8]) -> BishResult<Self> {
        use arrow2::io::ipc::read::{read_stream_metadata, StreamReader};
        use std::io::Cursor;

        let mut cursor = Cursor::new(bytes);
        let metadata = read_stream_metadata(&mut cursor)?;
        let arrow_schema = metadata.schema;

        let fields = arrow_schema
            .fields
            .iter()
            .map(BishField::from_arrow_field)
            .collect::<BishResult<Vec<_>>>()?;

        let file_metadata: HashMap<String,String> = arrow_schema.metadata.iter().map(|(k,v)|(k.clone(),v.clone())).collect();

        Ok(Self {
            fields,
            metadata: file_metadata,
        })
    }

    /// Convert to an Arrow [`Schema`] for use with Arrow APIs.
    pub fn to_arrow_schema(&self) -> ArrowSchema {
        ArrowSchema {
            fields: self.fields.iter().map(|f| f.to_arrow_field()).collect(),
            metadata: self.metadata.iter().map(|(k,v)|(k.clone(),v.clone())).collect(),
        }
    }

    /// Reconstruct from an Arrow [`Schema`].
    pub fn from_arrow_schema(s: &ArrowSchema) -> BishResult<Self> {
        let fields = s
            .fields
            .iter()
            .map(BishField::from_arrow_field)
            .collect::<BishResult<Vec<_>>>()?;
        Ok(Self {
            fields,
            metadata: s.metadata.iter().map(|(k,v)|(k.clone(),v.clone())).collect(),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ZoneValue — typed min/max for zone maps
// ─────────────────────────────────────────────────────────────────────────────

/// A typed value used to represent zone map min and max.
///
/// Zone maps are stored on disk as raw `i64` bits (see spec §4.2) and
/// reinterpreted as the appropriate type on read. This enum is the
/// in-memory typed form.
#[derive(Debug, Clone, PartialEq)]
pub enum ZoneValue {
    Int(i64),
    UInt(u64),
    Float32(f32),
    Float64(f64),
    Bytes(Vec<u8>), // for Utf8 and Binary — lexicographic min/max
    Bool(bool),
    None,           // column is all-null — no meaningful min/max
}

impl ZoneValue {
    /// Cast this zone value to an i64 for on-disk storage.
    ///
    /// Integers are stored as-is (sign-extended).
    /// Floats are stored as their IEEE 754 bit pattern.
    /// Strings are stored as 0 (the full value is in a separate byte buffer).
    pub fn to_i64_bits(&self) -> i64 {
        match self {
            ZoneValue::Int(v) => *v,
            ZoneValue::UInt(v) => *v as i64,
            ZoneValue::Float32(v) => v.to_bits() as i64,
            ZoneValue::Float64(v) => v.to_bits() as i64,
            ZoneValue::Bool(v) => *v as i64,
            ZoneValue::Bytes(_) | ZoneValue::None => 0,
        }
    }

    /// Does a value of `typ` fit the zone map range [min, max]?
    ///
    /// Returns `true` if the value MIGHT be present (zone map cannot rule it out).
    /// Returns `false` if the value is DEFINITELY not present — the row group
    /// can be skipped entirely.
    pub fn in_range(min: &ZoneValue, max: &ZoneValue, value: &ZoneValue) -> bool {
        match (min, max, value) {
            (ZoneValue::Int(lo), ZoneValue::Int(hi), ZoneValue::Int(v)) => v >= lo && v <= hi,
            (ZoneValue::UInt(lo), ZoneValue::UInt(hi), ZoneValue::UInt(v)) => v >= lo && v <= hi,
            (ZoneValue::Float64(lo), ZoneValue::Float64(hi), ZoneValue::Float64(v)) => {
                v >= lo && v <= hi
            }
            (ZoneValue::Bytes(lo), ZoneValue::Bytes(hi), ZoneValue::Bytes(v)) => {
                v >= lo && v <= hi
            }
            // Null zone means no data — nothing can be in range
            (ZoneValue::None, _, _) | (_, ZoneValue::None, _) => false,
            // Mixed types — conservatively say it might match (don't skip)
            _ => true,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Codec and Encoding tag enums (from spec §4.4 and §4.5)
// ─────────────────────────────────────────────────────────────────────────────

/// Compression codec applied to a page's raw bytes.
/// Stored as a 1-byte tag per page (spec §4.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Codec {
    Plain   = 0x00,
    Lz4     = 0x01,
    Zstd1   = 0x02,
    Zstd9   = 0x03,
    Snappy  = 0x04,
    Brotli  = 0x05,
}

impl Codec {
    pub fn from_u8(v: u8) -> BishResult<Self> {
        match v {
            0x00 => Ok(Codec::Plain),
            0x01 => Ok(Codec::Lz4),
            0x02 => Ok(Codec::Zstd1),
            0x03 => Ok(Codec::Zstd9),
            0x04 => Ok(Codec::Snappy),
            0x05 => Ok(Codec::Brotli),
            other => Err(BishError::UnknownCodec(other)),
        }
    }

    /// Pick the best codec for a page based on a data sample heuristic.
    /// Called by the writer when `ADAPTIVE_CODEC` feature flag is set.
    pub fn select_adaptive(
        is_cold_rg: bool,
        cardinality_estimate: u64,
        total_values: u64,
        is_sorted: bool,
    ) -> Self {
        if is_cold_rg {
            return Codec::Zstd9; // maximise compression for archival data
        }
        let ratio = if total_values == 0 {
            1.0
        } else {
            cardinality_estimate as f64 / total_values as f64
        };
        if ratio < 0.05 {
            Codec::Plain // RLE encoding makes compression almost irrelevant
        } else if is_sorted {
            Codec::Lz4 // delta encoding + LZ4 — fast and effective
        } else {
            Codec::Zstd1 // good default — better ratio than LZ4, fast enough
        }
    }
}

/// Value encoding transform applied before compression.
/// Stored as a 1-byte tag per page (spec §4.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    Plain       = 0x00,
    Rle         = 0x01,
    Bitpack     = 0x02,
    Delta       = 0x03,
    Dict        = 0x04,
    DeltaLength = 0x05,
}

impl Encoding {
    pub fn from_u8(v: u8) -> BishResult<Self> {
        match v {
            0x00 => Ok(Encoding::Plain),
            0x01 => Ok(Encoding::Rle),
            0x02 => Ok(Encoding::Bitpack),
            0x03 => Ok(Encoding::Delta),
            0x04 => Ok(Encoding::Dict),
            0x05 => Ok(Encoding::DeltaLength),
            other => Err(BishError::UnknownEncoding(other)),
        }
    }

    /// Pick the best encoding for a column type.
    pub fn select_for_type(bish_type: &BishType, is_sorted: bool, cardinality_ratio: f64) -> Self {
        match bish_type {
            BishType::Boolean => Encoding::Bitpack,
            BishType::Utf8 | BishType::Binary => {
                if cardinality_ratio < 0.1 {
                    Encoding::Dict // low cardinality strings — big win
                } else {
                    Encoding::DeltaLength // encode length deltas + plain bytes
                }
            }
            t if t.is_numeric() => {
                if is_sorted {
                    Encoding::Delta
                } else if cardinality_ratio < 0.05 {
                    Encoding::Rle
                } else {
                    Encoding::Plain
                }
            }
            _ => Encoding::Plain,
        }
    }
}

