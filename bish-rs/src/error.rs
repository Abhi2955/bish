//! Error types for the `.bish` format library.

use thiserror::Error;

/// All errors that can occur when reading or writing `.bish` files.
#[derive(Error, Debug)]
pub enum BishError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow2::error::Error),

    #[error("Invalid magic bytes — expected BISH, got {0:?}")]
    InvalidMagic([u8; 4]),

    #[error("Unsupported version: major={major}, minor={minor}")]
    UnsupportedVersion { major: u16, minor: u16 },

    #[error("Super-footer checksum mismatch — file may be corrupt")]
    ChecksumMismatch,

    #[error("Schema hash mismatch — chunk A may be corrupt")]
    SchemaHashMismatch,

    #[error("Unsupported Arrow type: {0}")]
    UnsupportedType(String),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("Unknown codec tag: 0x{0:02X}")]
    UnknownCodec(u8),

    #[error("Unknown encoding tag: 0x{0:02X}")]
    UnknownEncoding(u8),

    #[error("Decoding error: {0}")]
    Decoding(String),

    #[error("Column not found: '{0}'")]
    ColumnNotFound(String),

    #[error("Feature flag 0x{0:016X} is required but not supported by this reader")]
    UnsupportedRequiredFeature(u64),
}

/// Convenience alias used throughout the crate.
pub type BishResult<T> = Result<T, BishError>;
