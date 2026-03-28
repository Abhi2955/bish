//! # bish — the .bish columnar file format

pub mod error;
pub mod header;
pub mod types;

pub use error::{BishError, BishResult};
pub use header::{
    ChunkRef, FeatureFlags, FileHeader, SectionRef, SuperFooter,
    BISH_MAGIC, FILE_HEADER_SIZE, SUPER_FOOTER_SIZE, VERSION_MAJOR, VERSION_MINOR,
};
pub use types::{
    BishField, BishSchema, BishType, Codec, Encoding, ZoneValue,
};

pub mod encoding;
pub mod compress;
pub mod writer;

pub use writer::{ColumnChunkMeta, ColumnChunkWriter, PageMeta, RowGroupMeta, RowGroupWriter, WriteOptions};

pub mod footer;
pub use footer::{
    BishWriter, ColStatEntry, FinishedFile, RgDescriptor,
    parse_chunk_b, parse_chunk_c,
};

pub mod reader;
pub use reader::{BishReader, ColumnValues, RecordBatch};
