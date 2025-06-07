use crate::slice::ByteView;
use crate::types::{Field, Offsets, Type};
use crate::{i256, u256};
use chrono_tz::Tz;
use zerocopy::little_endian::{F32, F64, I16, I32, I64, I128, U16, U32, U64, U128};

pub enum Marker<'a> {
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
    Ring(Box<Marker<'a>>),
    Polygon(Box<Marker<'a>>),
    MultiPolygon(Box<Marker<'a>>),
    LineString(Box<Marker<'a>>),
    MultiLineString(Box<Marker<'a>>),

    Enum8(Vec<(&'a str, i8)>, &'a [u8]),
    Enum16(Vec<(&'a str, i16)>, &'a [u8]),

    LowCardinality {
        index_type: Type<'a>,
        indices: Box<Marker<'a>>,
        global_dictionary: Option<Box<Marker<'a>>>,
        additional_keys: Option<Box<Marker<'a>>>,
    },
    Array(Offsets<'a>, Box<Marker<'a>>),
    VarTuple(Vec<Marker<'a>>),
    FixTuple(Type<'a>, &'a [u8]),
    Nullable(&'a [u8], Box<Marker<'a>>),
    Map {
        offsets: Offsets<'a>,
        keys: Box<Marker<'a>>,
        values: Box<Marker<'a>>,
    },
    Variant {
        discriminators: &'a [u8],
        types: Vec<Marker<'a>>,
    },
    Nested(Vec<Field<'a>>, &'a [u8]),
    Dynamic(Vec<usize>, Vec<Marker<'a>>),

    Json {
        columns: Box<Marker<'a>>,
        data: Vec<Marker<'a>>,
    },
}
