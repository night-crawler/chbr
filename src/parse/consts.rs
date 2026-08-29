pub const NEED_GLOBAL_DICTIONARY_BIT: u64 = 1u64 << 8;
pub const HAS_ADDITIONAL_KEYS_BIT: u64 = 1u64 << 9;
pub const NEED_UPDATE_DICTIONARY_BIT: u64 = 1u64 << 10;

pub const TUINT8: u64 = 0;
pub const TUINT16: u64 = 1;
pub const TUINT32: u64 = 2;
pub const TUINT64: u64 = 3;

pub const LOW_CARDINALITY_VERSION: u64 = 1;

/// ClickHouse code: `DEFAULT_NATIVE_BINARY_MAX_NUM_COLUMNS`
pub const MAX_NUM_COLUMNS: usize = 1_000_000;

/// ClickHouse code: `DEFAULT_NATIVE_BINARY_MAX_NUM_ROWS`
pub const MAX_NUM_ROWS: u64 = 1_000_000_000_000;

/// ClickHouse code: `ColumnDynamic::MAX_DYNAMIC_TYPES_LIMIT`
pub const MAX_DYNAMIC_TYPES: usize = 254;
