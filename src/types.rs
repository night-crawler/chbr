#![allow(dead_code)]

use crate::parse::typ::parse_type;
use crate::slice::ByteView;
pub use chrono_tz::Tz;
use zerocopy::little_endian::U64;
use crate::marker::Marker;

pub type Offsets<'a> = ByteView<'a, U64>;


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type<'a> {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,

    Float32,
    Float64,

    BFloat16,

    Decimal32(u8),
    Decimal64(u8),
    Decimal128(u8),
    Decimal256(u8),

    String,
    FixedString(usize),

    Uuid,

    Date,
    Date32,
    DateTime(Tz),
    DateTime64(u8, Tz),

    Ipv4,
    Ipv6,

    Point,

    /// Ring is a simple polygon without holes stored as an array of points: Array(Point).
    Ring,

    /// Polygon is a polygon with holes stored as an array of rings: Array(Ring).
    /// First element of outer array is the outer shape of polygon and all the following
    /// elements are holes.
    Polygon,

    /// MultiPolygon consists of multiple polygons and is stored as an array of polygons:
    /// Array(Polygon).
    MultiPolygon,

    /// LineString is a line stored as an array of points: Array(Point).
    LineString,

    /// MultiLineString is multiple lines stored as an array of LineString: Array(LineString).
    MultiLineString,

    Enum8(Vec<(&'a str, i8)>),
    Enum16(Vec<(&'a str, i16)>),

    LowCardinality(Box<Type<'a>>),

    Array(Box<Type<'a>>),

    Tuple(Vec<Type<'a>>),

    Nullable(Box<Type<'a>>),

    Map(Box<Type<'a>>, Box<Type<'a>>),

    Variant(Vec<Type<'a>>),

    Nested(Vec<Field<'a>>),

    Dynamic,
    Json,
}

impl<'a> Type<'a> {
    pub fn is_nullable(&self) -> bool {
        matches!(self, Type::Nullable(_))
    }
    pub fn strip_null(&self) -> &Type<'a> {
        match self {
            Type::Nullable(inner) => inner,
            _ => self,
        }
    }
}

#[derive(Debug)]
pub struct JsonColumnHeader<'a> {
    pub path_version: u64,
    pub max_types: usize,
    pub total_types: usize,
    pub typ: Box<Type<'a>>,
    pub variant_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Field<'a> {
    pub name: &'a str,
    pub typ: Type<'a>,
}

impl<'a> Type<'a> {
    pub fn size(&self) -> Option<usize> {
        match self {
            Self::Bool => Some(1),
            Self::Int8 => Some(1),
            Self::Int16 => Some(2),
            Self::Int32 => Some(4),
            Self::Int64 => Some(8),
            Self::Int128 => Some(16),
            Self::Int256 => Some(32),
            Self::UInt8 => Some(1),
            Self::UInt16 => Some(2),
            Self::UInt32 => Some(4),
            Self::UInt64 => Some(8),
            Self::UInt128 => Some(16),
            Self::UInt256 => Some(32),

            Self::Float32 => Some(4),
            Self::Float64 => Some(8),
            Self::BFloat16 => Some(2),

            Self::Uuid => Some(16),

            Self::Decimal32(_) => Some(4),
            Self::Decimal64(_) => Some(8),
            Self::Decimal128(_) => Some(16),
            Self::Decimal256(_) => Some(32),

            Self::FixedString(size) => Some(*size),

            Self::Ipv4 => Some(4),
            Self::Ipv6 => Some(16),

            Self::Date => Some(2),
            Self::Date32 => Some(4),
            Self::DateTime(_) => Some(4),
            Self::DateTime64(_, _) => Some(8),
            Self::Enum8(_) => Some(1),
            Self::Enum16(_) => Some(2),

            // Point is represented by its X and Y coordinates, stored as a Tuple(Float64, Float64).
            Self::Point => Some(16),
            
            // For completeness, everything below is variable in size
            Self::Ring => None,
            Self::Polygon => None,
            Self::MultiPolygon => None,
            Self::LineString => None,
            Self::MultiLineString => None,
            Self::Map(_, _) => None,

            Self::Array(_) => None,

            // we can calculate the size for the tuple of fixed size types, but still we'll need
            // to parse nested columns later, so it's not worth it
            Self::Tuple(_) => None,

            // TODO: is it always variable?
            Self::Variant(_) => None,
            Self::Dynamic => None,
            Self::Json => None,

            Self::Nullable(_) => None,
            Self::LowCardinality(_) => None,
            Self::String => None,
            Self::Nested(_) => None
        }
    }

    pub fn from_bytes(s: &[u8]) -> Result<Type, crate::error::Error> {
        let (remainder, typ) =
            parse_type(s).map_err(|e| crate::error::Error::Parse(e.to_string()))?;
        if !remainder.trim_ascii().is_empty() {
            return Err(crate::error::Error::Parse(format!(
                "Unparsed remainder: {remainder:?}"
            )));
        }

        Ok(typ)
    }

    pub fn into_fixed_size_marker(self, data: &'a [u8]) -> crate::Result<Marker<'a>> {
        let q = match self {
            Type::Bool => Marker::Bool(data),
            Type::Int8 => Marker::Int8(ByteView::try_from(data)?),
            Type::Int16 => Marker::Int16(ByteView::try_from(data)?),
            Type::Int32 => Marker::Int32(ByteView::try_from(data)?),
            Type::Int64 => Marker::Int64(ByteView::try_from(data)?),
            Type::Int128 => Marker::Int128(ByteView::try_from(data)?),
            Type::Int256 => Marker::Int256(ByteView::try_from(data)?),
            Type::UInt8 => Marker::UInt8(ByteView::try_from(data)?),
            Type::UInt16 => Marker::UInt16(ByteView::try_from(data)?),
            Type::UInt32 => Marker::UInt32(ByteView::try_from(data)?),
            Type::UInt64 => Marker::UInt64(ByteView::try_from(data)?),
            Type::UInt128 => Marker::UInt128(ByteView::try_from(data)?),
            Type::UInt256 => Marker::UInt256(ByteView::try_from(data)?),
            Type::Float32 => Marker::Float32(ByteView::try_from(data)?),
            Type::Float64 => Marker::Float64(ByteView::try_from(data)?),
            Type::BFloat16 => Marker::BFloat16(ByteView::try_from(data)?),
            Type::Decimal32(scale) => Marker::Decimal32(scale, data),
            Type::Decimal64(scale) => Marker::Decimal64(scale, data),
            Type::Decimal128(scale) => Marker::Decimal128(scale, data),
            Type::Decimal256(scale) => Marker::Decimal256(scale, data),
            Type::FixedString(size) => Marker::FixedString(size, data),
            Type::Uuid => Marker::Uuid(data),
            Type::Date => Marker::Date(data),
            Type::Date32 => Marker::Date32(data),
            Type::DateTime(tz) => Marker::DateTime(tz, data),
            Type::DateTime64(precision, tz) => Marker::DateTime64(precision, tz, data),
            Type::Ipv4 => Marker::Ipv4(data),
            Type::Ipv6 => Marker::Ipv6(data),
            Type::Point => Marker::Point(data),

            Type::Tuple(inner) => Marker::FixTuple(Type::Tuple(inner), data),

            Type::Enum8(values) => Marker::Enum8(values, data),
            Type::Enum16(values) => Marker::Enum16(values, data),

            _ => unimplemented!("Const size is not implemented for type: {:?}", self),
        };

        Ok(q)
    }
}

#[macro_export]
macro_rules! t {
    ($name:ident) => {
        Type::$name
    };
    ($name:ident ( $($inner:tt)* )) => {
        Type::$name( $($inner)* )
    };
}

#[macro_export]
macro_rules! bt {
    ($name:ident) => {
        Box::new(Type::$name)
    };
    ($name:ident ( $($inner:tt)* )) => {
        Box::new(Type::$name( $($inner)* ))
    };
}
