use crate::slice::ByteView;
use crate::types::{Field, JsonColumnHeader, Offsets, Type};
use crate::{i256, u256};
use chrono_tz::Tz;
use core::fmt;
use zerocopy::little_endian::{F32, F64, I16, I32, I64, I128, U16, U32, U64, U128};
use zerocopy::{FromBytes, Unaligned};

pub enum Mark<'a> {
    Empty,
    Bool(&'a [u8]),
    Int8(ByteView<'a, i8>),
    Int16(ByteView<'a, I16>),
    Int32(ByteView<'a, I32>),
    Int64(ByteView<'a, I64>),
    Int128(ByteView<'a, I128>),
    Int256(ByteView<'a, i256>),
    UInt8(ByteView<'a, u8>),
    UInt16(ByteView<'a, U16>),
    UInt32(ByteView<'a, U32>),
    UInt64(ByteView<'a, U64>),
    UInt128(ByteView<'a, U128>),
    UInt256(ByteView<'a, u256>),
    Float32(ByteView<'a, F32>),
    Float64(ByteView<'a, F64>),
    BFloat16(ByteView<'a, U16>),
    Decimal32(u8, &'a [u8]),
    Decimal64(u8, &'a [u8]),
    Decimal128(u8, &'a [u8]),
    Decimal256(u8, &'a [u8]),
    String(Vec<u32>, &'a [u8]),
    FixedString(usize, &'a [u8]),
    Uuid(&'a [u8]),
    Date(&'a [u8]),
    Date32(&'a [u8]),
    DateTime(Tz, &'a [u8]),
    DateTime64(u8, Tz, &'a [u8]),
    Ipv4(&'a [u8]),
    Ipv6(&'a [u8]),
    Point(&'a [u8]),
    Ring(Box<Mark<'a>>),
    Polygon(Box<Mark<'a>>),
    MultiPolygon(Box<Mark<'a>>),
    LineString(Box<Mark<'a>>),
    MultiLineString(Box<Mark<'a>>),

    Enum8(Vec<(&'a str, i8)>, &'a [u8]),
    Enum16(Vec<(&'a str, i16)>, &'a [u8]),

    LowCardinality {
        index_type: Type<'a>,
        indices: Box<Mark<'a>>,
        global_dictionary: Option<Box<Mark<'a>>>,
        additional_keys: Option<Box<Mark<'a>>>,
    },
    Array(Offsets<'a>, Box<Mark<'a>>),
    Tuple(Vec<Mark<'a>>),
    Nullable(&'a [u8], Box<Mark<'a>>),
    Map {
        offsets: Offsets<'a>,
        keys: Box<Mark<'a>>,
        values: Box<Mark<'a>>,
    },
    Variant {
        discriminators: &'a [u8],
        types: Vec<Mark<'a>>,
    },
    Nested(Vec<Field<'a>>, &'a [u8]),
    Dynamic(Vec<usize>, Vec<Mark<'a>>),

    Json {
        columns: Box<Mark<'a>>,
        headers: Vec<JsonColumnHeader<'a>>,
    },
}

impl Mark<'_> {
    pub fn size(&self) -> Option<usize> {
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Bool(_) => Some(1),
            Self::Int8(_) => Some(1),
            Self::Int16(_) => Some(2),
            Self::Int32(_) => Some(4),
            Self::Int64(_) => Some(8),
            Self::Int128(_) => Some(16),
            Self::Int256(_) => Some(32),
            Self::UInt8(_) => Some(1),
            Self::UInt16(_) => Some(2),
            Self::UInt32(_) => Some(4),
            Self::UInt64(_) => Some(8),
            Self::UInt128(_) => Some(16),
            Self::UInt256(_) => Some(32),

            Self::Float32(_) => Some(4),
            Self::Float64(_) => Some(8),
            Self::BFloat16(_) => Some(2),

            Self::Uuid(_) => Some(16),

            Self::Decimal32(_, _) => Some(4),
            Self::Decimal64(_, _) => Some(8),
            Self::Decimal128(_, _) => Some(16),
            Self::Decimal256(_, _) => Some(32),

            Self::FixedString(size, _) => Some(*size),

            Self::Ipv4(_) => Some(4),
            Self::Ipv6(_) => Some(16),

            Self::Date(_) => Some(2),
            Self::Date32(_) => Some(4),
            Self::DateTime(_, _) => Some(4),
            Self::DateTime64(_, _, _) => Some(8),
            Self::Enum8(_, _) => Some(1),
            Self::Enum16(_, _) => Some(2),

            // Point is represented by its X and Y coordinates, stored as a Tuple(Float64, Float64).
            Self::Point(_) => Some(16),

            // For completeness, everything below is variable in size
            Self::Ring(_) => None,
            Self::Polygon(_) => None,
            Self::MultiPolygon(_) => None,
            Self::LineString(_) => None,
            Self::MultiLineString(_) => None,
            Self::Map { .. } => None,

            Self::Array(_, _) => None,

            Self::Tuple(_) => None,

            Self::Variant { .. } => None,
            Self::Dynamic(_, _) => None,
            Self::Json { .. } => None,

            Self::Nullable(_, _) => None,
            Self::LowCardinality { .. } => None,
            Self::String(_, _) => None,
            Self::Nested(_, _) => None,
            Self::Empty => None,
        }
    }
}

impl fmt::Debug for Mark<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn dbg_slice(f: &mut fmt::Formatter<'_>, name: &str, bytes: &[u8]) -> fmt::Result {
            f.debug_struct(name)
                .field("len_bytes", &bytes.len())
                .field("ptr", &bytes.as_ptr())
                .finish()
        }
        fn dbg_bv<T: Unaligned + FromBytes + Copy>(
            f: &mut fmt::Formatter<'_>,
            name: &str,
            bv: &ByteView<'_, T>,
        ) -> fmt::Result {
            let bytes = bv.as_bytes();
            f.debug_struct(name)
                .field("len_bytes", &bytes.len())
                .field("len", &bv.len())
                .field("ptr", &bytes.as_ptr())
                .finish()
        }
        use Mark::{
            Array, BFloat16, Bool, Date, Date32, DateTime, DateTime64, Decimal32, Decimal64,
            Decimal128, Decimal256, Dynamic, Empty, Enum8, Enum16, FixedString, Float32, Float64,
            Int8, Int16, Int32, Int64, Int128, Int256, Ipv4, Ipv6, Json, LineString,
            LowCardinality, Map, MultiLineString, MultiPolygon, Nested, Nullable, Point, Polygon,
            Ring, String, Tuple, UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Uuid, Variant,
        };
        match self {
            Empty => f.write_str("Empty"),

            // simple &[u8] cases
            Bool(b) | Uuid(b) | Date(b) | Date32(b) | Ipv4(b) | Ipv6(b) | Point(b) => dbg_slice(
                f,
                core::any::type_name::<Self>().rsplit("::").next().unwrap(),
                b,
            ),

            // ByteView-backed numeric columns
            Int8(v) => dbg_bv(f, "Int8", v),
            Int16(v) => dbg_bv(f, "Int16", v),
            Int32(v) => dbg_bv(f, "Int32", v),
            Int64(v) => dbg_bv(f, "Int64", v),
            Int128(v) => dbg_bv(f, "Int128", v),
            Int256(v) => dbg_bv(f, "Int256", v),
            UInt8(v) => dbg_bv(f, "UInt8", v),
            UInt16(v) => dbg_bv(f, "UInt16", v),
            UInt32(v) => dbg_bv(f, "UInt32", v),
            UInt64(v) => dbg_bv(f, "UInt64", v),
            UInt128(v) => dbg_bv(f, "UInt128", v),
            UInt256(v) => dbg_bv(f, "UInt256", v),
            Float32(v) => dbg_bv(f, "Float32", v),
            Float64(v) => dbg_bv(f, "Float64", v),
            BFloat16(v) => dbg_bv(f, "BFloat16", v),

            Decimal32(scale, b) => f
                .debug_struct("Decimal32")
                .field("scale", scale)
                .field("len_bytes", &b.len())
                .field("ptr", &b.as_ptr())
                .finish(),
            Decimal64(scale, b) => f
                .debug_struct("Decimal64")
                .field("scale", scale)
                .field("len_bytes", &b.len())
                .field("ptr", &b.as_ptr())
                .finish(),
            Decimal128(scale, b) => f
                .debug_struct("Decimal128")
                .field("scale", scale)
                .field("len_bytes", &b.len())
                .field("ptr", &b.as_ptr())
                .finish(),
            Decimal256(scale, b) => f
                .debug_struct("Decimal256")
                .field("scale", scale)
                .field("len_bytes", &b.len())
                .field("ptr", &b.as_ptr())
                .finish(),

            String(offsets, data) => f
                .debug_struct("String")
                .field("offsets_len", &offsets.len())
                .field("len_bytes", &data.len())
                .field("ptr", &data.as_ptr())
                .finish(),
            FixedString(n, data) => f
                .debug_struct("FixedString")
                .field("fixed_len", n)
                .field("len_bytes", &data.len())
                .field("ptr", &data.as_ptr())
                .finish(),

            DateTime(tz, data) => f
                .debug_struct("DateTime")
                .field("tz", tz)
                .field("len_bytes", &data.len())
                .field("ptr", &data.as_ptr())
                .finish(),
            DateTime64(scale, tz, data) => f
                .debug_struct("DateTime64")
                .field("scale", scale)
                .field("tz", tz)
                .field("len_bytes", &data.len())
                .field("ptr", &data.as_ptr())
                .finish(),

            Enum8(map, data) => f
                .debug_struct("Enum8")
                .field("mapping_len", &map.len())
                .field("len_bytes", &data.len())
                .field("ptr", &data.as_ptr())
                .finish(),
            Enum16(map, data) => f
                .debug_struct("Enum16")
                .field("mapping_len", &map.len())
                .field("len_bytes", &data.len())
                .field("ptr", &data.as_ptr())
                .finish(),

            Ring(inner) => f.debug_tuple("Ring").field(inner).finish(),
            Polygon(inner) => f.debug_tuple("Polygon").field(inner).finish(),
            MultiPolygon(inner) => f.debug_tuple("MultiPolygon").field(inner).finish(),
            LineString(inner) => f.debug_tuple("LineString").field(inner).finish(),
            MultiLineString(inner) => f.debug_tuple("MultiLineString").field(inner).finish(),

            LowCardinality {
                index_type,
                indices,
                global_dictionary,
                additional_keys,
            } => f
                .debug_struct("LowCardinality")
                .field("index_type", index_type)
                .field("indices", indices)
                .field("global_dictionary", global_dictionary)
                .field("additional_keys", additional_keys)
                .finish(),

            Array(off, inner) => f
                .debug_struct("Array")
                .field("offsets_len", &off.len())
                .field("values", inner)
                .finish(),

            Tuple(items) => f.debug_tuple("VarTuple").field(items).finish(),

            Nullable(nulls, col) => f
                .debug_struct("Nullable")
                .field("nulls_bytes", &nulls.len())
                .field("nulls_ptr", &nulls.as_ptr())
                .field("column", col)
                .finish(),

            Map {
                offsets,
                keys,
                values,
            } => f
                .debug_struct("Map")
                .field("offsets_len", &offsets.len())
                .field("keys", keys)
                .field("values", values)
                .finish(),

            Variant {
                discriminators,
                types,
            } => f
                .debug_struct("Variant")
                .field("disc_bytes", &discriminators.len())
                .field("disc_ptr", &discriminators.as_ptr())
                .field("types", types)
                .finish(),

            Nested(fields, bytes) => f
                .debug_struct("Nested")
                .field("fields_len", &fields.len())
                .field("len_bytes", &bytes.len())
                .field("ptr", &bytes.as_ptr())
                .finish(),

            Dynamic(layout, cols) => f
                .debug_struct("Dynamic")
                .field("layout", layout)
                .field("columns", cols)
                .finish(),

            Json { columns, headers } => f
                .debug_struct("Json")
                .field("columns", columns)
                .field("headers", headers)
                .finish(),
        }
    }
}
