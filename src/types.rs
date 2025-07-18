pub use chrono_tz::Tz;
use zerocopy::little_endian::U64;

use crate::{
    mark::{
        DateTime, DateTime64, Decimal32, Decimal64, Decimal128, Decimal256, Enum8, Enum16,
        FixedString, Mark,
    },
    parse::typ::parse_type,
    slice::ByteView,
};

pub type Offsets<'a> = ByteView<'a, U64>;

pub(crate) trait OffsetIndexPair {
    fn offset_indices(&self, index: usize) -> crate::Result<Option<(usize, usize)>>;
    fn get_cast<T>(&self, index: usize) -> crate::Result<Option<T>>
    where
        T: TryFrom<u64>;
    fn last_or_default(&self) -> crate::Result<usize>;
}

impl OffsetIndexPair for Offsets<'_> {
    #[inline(always)]
    fn offset_indices(&self, index: usize) -> crate::Result<Option<(usize, usize)>> {
        let start = if index == 0 {
            0
        } else {
            let Some(start) = self.get_cast(index.saturating_sub(1))? else {
                return Ok(None);
            };
            start
        };

        let Some(end) = self.get_cast(index)? else {
            return Ok(None);
        };
        Ok(Some((start, end)))
    }

    fn get_cast<T>(&self, index: usize) -> crate::Result<Option<T>>
    where
        T: TryFrom<u64>,
    {
        let Some(value) = self.get(index).map(|v| v.get()) else {
            return Ok(None);
        };
        let value = T::try_from(value).map_err(|_| crate::Error::Overflow(value.to_string()))?;
        Ok(Some(value))
    }

    fn last_or_default(&self) -> crate::Result<usize> {
        if let Some(last) = self.last().map(|last| last.get()) {
            let last =
                usize::try_from(last).map_err(|_| crate::Error::Overflow(last.to_string()))?;
            Ok(last)
        } else {
            Ok(usize::default())
        }
    }
}

#[derive(Debug)]
pub struct MapHeader<'a> {
    pub key: TypeHeader<'a>,
    pub value: TypeHeader<'a>,
}

#[derive(Debug)]
pub struct DynamicHeader<'a> {
    pub types: Vec<Type<'a>>,
    pub headers: Vec<TypeHeader<'a>>,
}

#[derive(Debug)]
pub struct JsonHeader<'a> {
    pub paths: Vec<&'a str>,
    pub col_headers: Vec<JsonColumnHeader<'a>>,
    pub type_headers: Vec<TypeHeader<'a>>,
}

#[derive(Debug)]
pub enum TypeHeader<'a> {
    Empty,
    Tuple(Vec<TypeHeader<'a>>),
    Json(Box<JsonHeader<'a>>),
    Map(Box<MapHeader<'a>>),
    Variant(Vec<TypeHeader<'a>>),
    Array(Box<TypeHeader<'a>>),
    Dynamic(Box<DynamicHeader<'a>>),
    Nullable(Box<TypeHeader<'a>>),
    Nested(Vec<TypeHeader<'a>>),
}

impl<'a> TypeHeader<'a> {
    #[inline]
    pub fn into_array(self) -> TypeHeader<'a> {
        match self {
            TypeHeader::Array(inner) => *inner,
            e => unreachable!("Unexpected type header: {e:?}"),
        }
    }

    #[inline]
    pub fn into_tuple(self) -> Vec<TypeHeader<'a>> {
        match self {
            TypeHeader::Tuple(t) => t,
            e => unreachable!("Unexpected type header: {e:?}"),
        }
    }

    #[inline]
    pub fn into_map(self) -> MapHeader<'a> {
        match self {
            TypeHeader::Map(map) => *map,
            e => unreachable!("Unexpected type header: {e:?}"),
        }
    }

    #[inline]
    pub fn into_variant(self) -> Vec<TypeHeader<'a>> {
        match self {
            TypeHeader::Variant(variants) => variants,
            e => unreachable!("Unexpected type header: {e:?}"),
        }
    }

    #[inline]
    pub fn into_json(self) -> JsonHeader<'a> {
        match self {
            TypeHeader::Json(json) => *json,
            e => unreachable!("Unexpected type header: {e:?}"),
        }
    }

    #[inline]
    pub fn into_dynamic(self) -> DynamicHeader<'a> {
        match self {
            TypeHeader::Dynamic(d) => *d,
            e => unreachable!("Unexpected type header: {e:?}"),
        }
    }

    #[inline]
    pub fn into_nested(self) -> Vec<TypeHeader<'a>> {
        match self {
            TypeHeader::Nested(n) => n,
            e => unreachable!("Unexpected type header: {e:?}"),
        }
    }

    #[inline]
    pub fn into_nullable(self) -> TypeHeader<'a> {
        match self {
            TypeHeader::Nullable(inner) => *inner,
            TypeHeader::Empty => TypeHeader::Empty,
            e => unreachable!("Unexpected type header: {e:?}"),
        }
    }
}

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

    /// Point is represented by its X and Y coordinates, stored as a Tuple(Float64, Float64).
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

    SharedVariant,
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
    pub mark: Mark<'a>,
    pub discriminators: &'a [u8],
    pub offsets: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Field<'a> {
    pub name: &'a str,
    pub typ: Type<'a>,
}

impl<'a> Type<'a> {
    pub fn size(&self) -> Option<usize> {
        #[expect(clippy::match_same_arms)]
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
            Self::Point => None,

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
            Self::Nested(_) => None,
            Self::SharedVariant => None,
        }
    }

    pub fn from_bytes(s: &[u8]) -> Result<Type, crate::Error> {
        let (remainder, typ) = parse_type(s).map_err(|e| crate::Error::Parse(e.to_string()))?;
        if !remainder.trim_ascii().is_empty() {
            return Err(crate::Error::Parse(format!(
                "Unparsed remainder: {remainder:?}"
            )));
        }

        Ok(typ)
    }

    pub fn into_fixed_size_marker(self, data: &'a [u8]) -> crate::Result<Mark<'a>> {
        let mark = match self {
            Type::Bool => Mark::Bool(data),
            Type::Int8 => Mark::Int8(ByteView::try_from(data)?),
            Type::Int16 => Mark::Int16(ByteView::try_from(data)?),
            Type::Int32 => Mark::Int32(ByteView::try_from(data)?),
            Type::Int64 => Mark::Int64(ByteView::try_from(data)?),
            Type::Int128 => Mark::Int128(ByteView::try_from(data)?),
            Type::Int256 => Mark::Int256(ByteView::try_from(data)?),
            Type::UInt8 => Mark::UInt8(ByteView::try_from(data)?),
            Type::UInt16 => Mark::UInt16(ByteView::try_from(data)?),
            Type::UInt32 => Mark::UInt32(ByteView::try_from(data)?),
            Type::UInt64 => Mark::UInt64(ByteView::try_from(data)?),
            Type::UInt128 => Mark::UInt128(ByteView::try_from(data)?),
            Type::UInt256 => Mark::UInt256(ByteView::try_from(data)?),
            Type::Float32 => Mark::Float32(ByteView::try_from(data)?),
            Type::Float64 => Mark::Float64(ByteView::try_from(data)?),
            Type::BFloat16 => Mark::BFloat16(ByteView::try_from(data)?),
            Type::Decimal32(precision) => Mark::Decimal32(Decimal32 {
                precision,
                data: ByteView::try_from(data)?,
            }),
            Type::Decimal64(precision) => Mark::Decimal64(Decimal64 {
                precision,
                data: ByteView::try_from(data)?,
            }),
            Type::Decimal128(precision) => Mark::Decimal128(Decimal128 {
                precision,
                data: ByteView::try_from(data)?,
            }),
            Type::Decimal256(precision) => Mark::Decimal256(Decimal256 {
                precision,
                data: ByteView::try_from(data)?,
            }),
            Type::FixedString(size) => Mark::FixedString(FixedString { size, data }),
            Type::Uuid => Mark::Uuid(ByteView::try_from(data)?),
            Type::Date => Mark::Date(ByteView::try_from(data)?),
            Type::Date32 => Mark::Date32(ByteView::try_from(data)?),
            Type::DateTime(tz) => Mark::DateTime(DateTime {
                tz,
                data: ByteView::try_from(data)?,
            }),
            Type::DateTime64(precision, tz) => Mark::DateTime64(DateTime64 {
                precision,
                tz,
                data: ByteView::try_from(data)?,
            }),
            Type::Ipv4 => Mark::Ipv4(ByteView::try_from(data)?),
            Type::Ipv6 => Mark::Ipv6(ByteView::try_from(data)?),

            Type::Enum8(variants) => Mark::Enum8(Enum8 {
                variants,
                data: ByteView::try_from(data)?,
            }),
            Type::Enum16(variants) => Mark::Enum16(Enum16 {
                variants,
                data: ByteView::try_from(data)?,
            }),

            _ => unimplemented!("Const size is not implemented for type: {:?}", self),
        };

        Ok(mark)
    }
}
