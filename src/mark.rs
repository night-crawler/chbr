use core::fmt;
use std::fmt::Debug;

use chrono_tz::Tz;
use zerocopy::{
    FromBytes, Unaligned,
    little_endian::{F32, F64, I16, I32, I64, I128, U16, U32, U64, U128},
};

use crate::value::Value;
use crate::{
    Bf16Data, Date16Data, Date32Data, DateTime32Data, DateTime64Data, Decimal32Data, Decimal64Data,
    Decimal128Data, Decimal256Data, I256, Ipv4Data, Ipv6Data, U256, UuidData,
    slice::ByteView,
    types::{JsonColumnHeader, Offsets},
};

#[derive(Debug)]
pub struct Map<'a> {
    pub offsets: Offsets<'a>,
    pub keys: Box<Mark<'a>>,
    pub values: Box<Mark<'a>>,
}

#[derive(Debug)]
pub struct Variant<'a> {
    pub offsets: Vec<usize>,
    pub discriminators: &'a [u8],
    pub types: Vec<Mark<'a>>,
}

impl Variant<'_> {
    #[inline]
    pub fn get(&self, index: usize) -> Option<Value> {
        let discriminator = (*self.discriminators.get(index)?) as usize;
        let in_type_index = *self.offsets.get(index)?;
        self.types
            .get(discriminator)
            .and_then(|mark| mark.get(in_type_index))
    }
}

#[derive(Debug)]
pub struct LowCardinality<'a> {
    pub is_nullable: bool,
    pub indices: Box<Mark<'a>>,
    pub global_dictionary: Option<Box<Mark<'a>>>,
    pub additional_keys: Option<Box<Mark<'a>>>,
}

impl LowCardinality<'_> {
    #[inline]
    pub fn value_index(&self, index: usize) -> Option<usize> {
        let value_index = match self.indices.as_ref() {
            Mark::UInt8(indices) => indices.get(index).copied()? as usize,
            Mark::UInt16(indices) => indices.get(index)?.get() as usize,
            Mark::UInt32(indices) => indices.get(index)?.get() as usize,
            Mark::UInt64(indices) => usize::try_from(indices.get(index)?.get()).unwrap(),
            _ => unreachable!("unexpected indices type in LowCardinality "),
        };

        Some(value_index)
    }
}

#[derive(Debug)]
pub struct Nested<'a> {
    pub col_names: Vec<&'a str>,
    pub array_of_tuples: Box<Mark<'a>>,
}

#[derive(Debug)]
pub struct Json<'a> {
    pub paths: Vec<&'a str>,
    pub headers: Vec<JsonColumnHeader<'a>>,
}

#[derive(Debug)]
pub struct Array<'a> {
    pub offsets: Offsets<'a>,
    pub values: Box<Mark<'a>>,
}

#[derive(Debug)]
pub struct Decimal32<'a> {
    pub precision: u8,
    pub data: ByteView<'a, Decimal32Data>,
}

#[derive(Debug)]
pub struct Decimal64<'a> {
    pub precision: u8,
    pub data: ByteView<'a, Decimal64Data>,
}

#[derive(Debug)]
pub struct Decimal128<'a> {
    pub precision: u8,
    pub data: ByteView<'a, Decimal128Data>,
}

#[derive(Debug)]
pub struct Decimal256<'a> {
    pub precision: u8,
    pub data: ByteView<'a, Decimal256Data>,
}

#[derive(Debug)]
pub struct FixedString<'a> {
    pub size: usize,
    pub data: &'a [u8],
}

#[derive(Debug)]
pub struct DateTime<'a> {
    pub tz: Tz,
    pub data: ByteView<'a, DateTime32Data>,
}

#[derive(Debug)]
pub struct DateTime64<'a> {
    pub precision: u8,
    pub tz: Tz,
    pub data: ByteView<'a, DateTime64Data>,
}

#[derive(Debug)]
pub struct Enum8<'a> {
    pub variants: Vec<(&'a str, i8)>,
    pub data: ByteView<'a, i8>,
}

#[derive(Debug)]
pub struct Enum16<'a> {
    pub variants: Vec<(&'a str, i16)>,
    pub data: ByteView<'a, I16>,
}

#[derive(Debug)]
pub struct Dynamic<'a> {
    pub offsets: Vec<usize>,
    pub discriminators: Vec<usize>,
    pub columns: Vec<Mark<'a>>,
}

#[derive(Debug)]
pub struct Nullable<'a> {
    pub mask: &'a [u8],
    pub data: Box<Mark<'a>>,
}

#[derive(Debug)]
pub struct Tuple<'a> {
    pub values: Vec<Mark<'a>>,
}

pub enum Mark<'a> {
    Empty,
    Bool(&'a [u8]),
    Int8(ByteView<'a, i8>),
    Int16(ByteView<'a, I16>),
    Int32(ByteView<'a, I32>),
    Int64(ByteView<'a, I64>),
    Int128(ByteView<'a, I128>),
    Int256(ByteView<'a, I256>),
    UInt8(ByteView<'a, u8>),
    UInt16(ByteView<'a, U16>),
    UInt32(ByteView<'a, U32>),
    UInt64(ByteView<'a, U64>),
    UInt128(ByteView<'a, U128>),
    UInt256(ByteView<'a, U256>),
    Float32(ByteView<'a, F32>),
    Float64(ByteView<'a, F64>),
    BFloat16(ByteView<'a, Bf16Data>),
    Decimal32(Decimal32<'a>),
    Decimal64(Decimal64<'a>),
    Decimal128(Decimal128<'a>),
    Decimal256(Decimal256<'a>),
    String(Vec<&'a str>),
    FixedString(FixedString<'a>),
    Uuid(ByteView<'a, UuidData>),
    Date(ByteView<'a, Date16Data>),
    Date32(ByteView<'a, Date32Data>),
    DateTime(DateTime<'a>),
    DateTime64(DateTime64<'a>),
    Ipv4(ByteView<'a, Ipv4Data>),
    Ipv6(ByteView<'a, Ipv6Data>),
    Point(&'a [u8]),
    Ring(Box<Mark<'a>>),
    Polygon(Box<Mark<'a>>),
    MultiPolygon(Box<Mark<'a>>),
    LineString(Box<Mark<'a>>),
    MultiLineString(Box<Mark<'a>>),

    Enum8(Enum8<'a>),
    Enum16(Enum16<'a>),

    LowCardinality(LowCardinality<'a>),
    Array(Array<'a>),
    Tuple(Tuple<'a>),
    Nullable(Nullable<'a>),
    Map(Map<'a>),
    Variant(Variant<'a>),
    Nested(Nested<'a>),
    Dynamic(Dynamic<'a>),

    Json(Json<'a>),
}

impl Mark<'_> {
    pub const fn size(&self) -> Option<usize> {
        #[expect(clippy::match_same_arms)]
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

            Self::Decimal32(_) => Some(4),
            Self::Decimal64(_) => Some(8),
            Self::Decimal128(_) => Some(16),
            Self::Decimal256(_) => Some(32),

            Self::FixedString(f) => Some(f.size),

            Self::Ipv4(_) => Some(4),
            Self::Ipv6(_) => Some(16),

            Self::Date(_) => Some(2),
            Self::Date32(_) => Some(4),
            Self::DateTime { .. } => Some(4),
            Self::DateTime64 { .. } => Some(8),
            Self::Enum8(_) => Some(1),
            Self::Enum16(_) => Some(2),

            // Point is represented by its X and Y coordinates, stored as a Tuple(Float64, Float64).
            Self::Point(_) => Some(16),

            // For completeness, everything below is variable in size
            Self::Ring(_) => None,
            Self::Polygon(_) => None,
            Self::MultiPolygon(_) => None,
            Self::LineString(_) => None,
            Self::MultiLineString(_) => None,
            Self::Map { .. } => None,

            Self::Array(_) => None,

            Self::Tuple(_) => None,

            Self::Variant { .. } => None,
            Self::Dynamic(_) => None,
            Self::Json { .. } => None,

            Self::Nullable(_) => None,
            Self::LowCardinality { .. } => None,
            Self::String(_) => None,
            Self::Nested { .. } => None,
            Self::Empty => None,
        }
    }
}

impl Debug for Mark<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn dbg_slice(f: &mut fmt::Formatter<'_>, name: &str, bytes: &[u8]) -> fmt::Result {
            f.debug_struct(name)
                .field("len_bytes", &bytes.len())
                .field("ptr", &bytes.as_ptr())
                .finish()
        }
        fn dbg_bv<T: Unaligned + FromBytes + Copy + Debug>(
            f: &mut fmt::Formatter<'_>,
            name: &str,
            bv: &ByteView<'_, T>,
        ) -> fmt::Result {
            let bytes = bv.as_bytes();
            f.debug_struct(name)
                .field("len", &bytes.len())
                .field("data", &bv.as_slice())
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

            Bool(b) | Point(b) => dbg_slice(
                f,
                core::any::type_name::<Self>().rsplit("::").next().unwrap(),
                b,
            ),

            Ipv4(v) => dbg_bv(f, "Ipv4", v),
            Ipv6(v) => dbg_bv(f, "Ipv6", v),
            Date32(v) => dbg_bv(f, "Date32", v),
            Date(v) => dbg_bv(f, "Date", v),
            Uuid(v) => dbg_bv(f, "Uuid", v),
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

            Decimal32(d) => f
                .debug_struct("Decimal32")
                .field("scale", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),
            Decimal64(d) => f
                .debug_struct("Decimal64")
                .field("scale", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),
            Decimal128(d) => f
                .debug_struct("Decimal128")
                .field("scale", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),
            Decimal256(d) => f
                .debug_struct("Decimal256")
                .field("scale", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),

            String(data) => f.debug_struct("String").field("data", data).finish(),
            FixedString(ff) => f
                .debug_struct("FixedString")
                .field("fixed_len", &ff.size)
                .field("data", &ff.data)
                .finish(),

            DateTime(d) => f
                .debug_struct("DateTime")
                .field("tz", &d.tz)
                .field("data", &d.data.as_slice())
                .finish(),
            DateTime64(d) => f
                .debug_struct("DateTime64")
                .field("tz", &d.tz)
                .field("precision", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),

            Enum8(e) => f
                .debug_struct("Enum8")
                .field("data", &e.data.as_slice())
                .finish(),
            Enum16(e) => f.debug_struct("Enum16").field("map", &e).finish(),

            Ring(inner) => f.debug_tuple("Ring").field(inner).finish(),
            Polygon(inner) => f.debug_tuple("Polygon").field(inner).finish(),
            MultiPolygon(inner) => f.debug_tuple("MultiPolygon").field(inner).finish(),
            LineString(inner) => f.debug_tuple("LineString").field(inner).finish(),
            MultiLineString(inner) => f.debug_tuple("MultiLineString").field(inner).finish(),

            LowCardinality(lc) => f
                .debug_struct("LowCardinality")
                .field("indices", &lc.indices)
                .field("global_dictionary", &lc.global_dictionary)
                .field("additional_keys", &lc.additional_keys)
                .finish(),

            Array(a) => f
                .debug_struct("Array")
                .field("offsets_len", &a.offsets.len())
                .field("values", &a.values)
                .finish(),

            Tuple(items) => f.debug_tuple("Tuple").field(items).finish(),

            Nullable(n) => f.debug_struct("Nullable").field("data", n).finish(),

            Map(m) => f
                .debug_struct("Map")
                .field("offsets_len", &m.offsets.len())
                .field("keys", &m.keys)
                .field("values", &m.values)
                .finish(),

            Variant(v) => f
                .debug_struct("Variant")
                .field("disc_bytes", &v.discriminators.len())
                .field("types", &v.types)
                .field("offsets", &v.offsets)
                .finish(),

            Nested(n) => f
                .debug_struct("Nested")
                .field("col_names", &n.col_names)
                .field("array_of_tuples", &n.array_of_tuples)
                .finish(),

            Dynamic(d) => f
                .debug_struct("Dynamic")
                .field("discriminators", &d.discriminators)
                .field("columns", &d.columns)
                .finish(),

            Json(j) => f
                .debug_struct("Json")
                .field("paths", &j.paths)
                .field("headers", &j.headers)
                .finish(),
        }
    }
}

impl Mark<'_> {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Mark::Empty => "Empty",
            Mark::Bool(_) => "Bool",
            Mark::Int8(_) => "Int8",
            Mark::Int16(_) => "Int16",
            Mark::Int32(_) => "Int32",
            Mark::Int64(_) => "Int64",
            Mark::Int128(_) => "Int128",
            Mark::Int256(_) => "Int256",
            Mark::UInt8(_) => "UInt8",
            Mark::UInt16(_) => "UInt16",
            Mark::UInt32(_) => "UInt32",
            Mark::UInt64(_) => "UInt64",
            Mark::UInt128(_) => "UInt128",
            Mark::UInt256(_) => "UInt256",
            Mark::Float32(_) => "Float32",
            Mark::Float64(_) => "Float64",
            Mark::BFloat16(_) => "BFloat16",
            Mark::Decimal32(_) => "Decimal32",
            Mark::Decimal64(_) => "Decimal64",
            Mark::Decimal128(_) => "Decimal128",
            Mark::Decimal256(_) => "Decimal256",
            Mark::String(_) => "String",
            Mark::FixedString(_) => "FixedString",
            Mark::Uuid(_) => "Uuid",
            Mark::Date(_) => "Date",
            Mark::Date32(_) => "Date32",
            Mark::DateTime(_) => "DateTime",
            Mark::DateTime64(_) => "DateTime64",
            Mark::Ipv4(_) => "Ipv4",
            Mark::Ipv6(_) => "Ipv6",
            Mark::Point(_) => "Point",
            Mark::Ring(_) => "Ring",
            Mark::Polygon(_) => "Polygon",
            Mark::MultiPolygon(_) => "MultiPolygon",
            Mark::LineString(_) => "LineString",
            Mark::MultiLineString(_) => "MultiLineString",
            Mark::Enum8(_) => "Enum8",
            Mark::Enum16(_) => "Enum16",
            Mark::LowCardinality(_) => "LowCardinality",
            Mark::Array(_) => "Array",
            Mark::Tuple(_) => "Tuple",
            Mark::Nullable(_) => "Nullable",
            Mark::Map(_) => "Map",
            Mark::Variant(_) => "Variant",
            Mark::Nested(_) => "Nested",
            Mark::Dynamic(_) => "Dynamic",
            Mark::Json(_) => "Json",
        }
    }
}
