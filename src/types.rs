#![allow(dead_code)]

use crate::parse::parse_type;
pub use chrono_tz::Tz;
use unsigned_varint::decode;

pub fn u64_le(buf: &[u8]) -> crate::Result<(u64, &[u8])> {
    if buf.len() < 8 {
        return Err(crate::error::Error::UnexpectedEndOfInput);
    }
    let (bytes, rest) = buf.split_at(8);
    let n = u64::from_le_bytes(bytes.try_into().unwrap());
    Ok((n, rest))
}

pub struct Data<'a> {
    pub data: &'a [u8],
    pub num_rows: usize,
    pub num_columns: usize,
}

pub enum BlockMarker<'a> {
    Bool(Data<'a>),
    Int8(Data<'a>),
    Int16(Data<'a>),
    Int32(Data<'a>),
    Int64(Data<'a>),
    Int128(Data<'a>),
    Int256(Data<'a>),
    UInt8(Data<'a>),
    UInt16(Data<'a>),
    UInt32(Data<'a>),
    UInt64(Data<'a>),
    UInt128(Data<'a>),
    UInt256(Data<'a>),
    Float32(Data<'a>),
    Float64(Data<'a>),
    BFloat16(Data<'a>),
    Decimal32(u8, Data<'a>),
    Decimal64(u8, Data<'a>),
    Decimal128(u8, Data<'a>),
    Decimal256(u8, Data<'a>),
    String(Data<'a>),
    FixedString(usize, Data<'a>),
    Uuid(Data<'a>),
    Date(Data<'a>),
    Date32(Data<'a>),
    DateTime(Tz, Data<'a>),
    DateTime64(u8, Tz, Data<'a>),
    Ipv4(Data<'a>),
    Ipv6(Data<'a>),
    Point(Data<'a>),
    Ring(Box<BlockMarker<'a>>),
    Polygon(Box<BlockMarker<'a>>),
    MultiPolygon(Box<BlockMarker<'a>>),
    LineString(Box<BlockMarker<'a>>),
    MultiLineString(Box<BlockMarker<'a>>),

    Enum8(Vec<(&'a str, i8)>, Data<'a>),
    Enum16(Vec<(&'a str, i16)>, Data<'a>),

    LowCardinality(Box<Type<'a>>, Data<'a>),
    Array(Vec<usize>, Box<BlockMarker<'a>>),
    VarTuple(Vec<BlockMarker<'a>>),
    FixTuple(Type<'a>, Data<'a>),
    Nullable(Box<Type<'a>>, Data<'a>),
    Map(Vec<usize>, Box<BlockMarker<'a>>, Box<BlockMarker<'a>>),
    Variant(&'a [u8], Vec<BlockMarker<'a>>),
    Nested(Vec<Field<'a>>, Data<'a>),
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

            Self::String => None,
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
            Self::Ring => None,
            Self::Polygon => None,
            Self::MultiPolygon => None,
            Self::LineString => None,
            Self::MultiLineString => None,
            Self::Map(_, _) => None,

            Self::Array(_) => None,

            Self::Tuple(inner) => {
                let mut size = 0;
                for typ in inner {
                    size += typ.size()?;
                }
                Some(size)
            }

            // TODO: is it always variable?
            Self::Variant(_) => None,

            _ => unimplemented!("Size not implemented for type: {:?}", self),
        }
    }

    pub fn from_str(s: &str) -> Result<Type, crate::error::Error> {
        let (remainder, typ) =
            parse_type(s).map_err(|e| crate::error::Error::Parse(e.to_string()))?;
        if !remainder.trim().is_empty() {
            return Err(crate::error::Error::Parse(format!(
                "Unparsed remainder: {remainder}"
            )));
        }

        Ok(typ)
    }

    pub fn transcode_remainder(
        self,
        remainder: &'a [u8],
        num_cols: usize,
        num_rows: usize,
    ) -> crate::Result<(BlockMarker<'a>, usize)> {
        if let Some(size) = self.size() {
            let data_size = size * num_rows;
            let data = Data {
                data: &remainder[..data_size],
                num_rows,
                num_columns: num_cols,
            };
            return Ok((self.into_fixed_size_marker(data), data_size));
        }

        #[allow(clippy::single_match)]
        match self {
            Self::String => {
                let mut buf = remainder;
                let start_ptr = buf.as_ptr();
                let mut n = 0;
                for _ in 0..num_rows {
                    (n, buf) = decode::usize(buf)?;
                    buf = &buf[n..];
                }
                let end = buf.as_ptr();
                let data_size = end as usize - start_ptr as usize;
                let data = Data {
                    data: &remainder[..data_size],
                    num_rows,
                    num_columns: num_cols,
                };
                return Ok((BlockMarker::String(data), data_size));
            }

            Self::Tuple(inner) => {
                let mut buf = remainder;
                let mut blocks = vec![];

                let mut total_size = 0;
                for typ in inner {
                    let (block, size) = typ.transcode_remainder(buf, num_cols, num_rows)?;
                    blocks.push(block);
                    buf = &buf[size..];
                    total_size += size;
                }

                let block = BlockMarker::VarTuple(blocks);
                return Ok((block, total_size));
            }

            Self::Array(inner) => {
                let mut buf = remainder;
                let mut offsets = vec![];
                let mut n = 0;
                for _ in 0..num_rows {
                    (n, buf) = u64_le(buf)?;
                    offsets.push(n as usize);
                }
                let (inner_block, col_data_size) =
                    inner.transcode_remainder(buf, num_cols, n as usize)?;
                let block = BlockMarker::Array(offsets, Box::new(inner_block));
                let complete_size = col_data_size + size_of::<u64>() * num_rows;
                return Ok((block, complete_size));
            }

            Self::Ring => {
                let (points, size) = Type::Array(Box::new(Type::Point))
                    .transcode_remainder(remainder, num_cols, num_rows)?;
                let wrapped = BlockMarker::Ring(Box::new(points));
                return Ok((wrapped, size));
            }
            Self::Polygon => {
                let (rings, size) = Type::Array(Box::new(Type::Ring))
                    .transcode_remainder(remainder, num_cols, num_rows)?;
                let wrapped = BlockMarker::Polygon(Box::new(rings));
                return Ok((wrapped, size));
            }
            Self::MultiPolygon => {
                let (polygons, size) = Type::Array(Box::new(Type::Polygon))
                    .transcode_remainder(remainder, num_cols, num_rows)?;
                let wrapped = BlockMarker::MultiPolygon(Box::new(polygons));
                return Ok((wrapped, size));
            }
            Self::LineString => {
                let (points, size) = Type::Array(Box::new(Type::Point))
                    .transcode_remainder(remainder, num_cols, num_rows)?;
                let wrapped = BlockMarker::LineString(Box::new(points));
                return Ok((wrapped, size));
            }
            Self::MultiLineString => {
                let (points, size) = Type::Array(Box::new(Type::LineString))
                    .transcode_remainder(remainder, num_cols, num_rows)?;
                let wrapped = BlockMarker::MultiLineString(Box::new(points));
                return Ok((wrapped, size));
            }
            Self::Map(key, val) => {
                let mut buf = remainder;
                let mut offsets = vec![];
                let mut n = 0;
                for _ in 0..num_rows {
                    (n, buf) = u64_le(buf)?;
                    offsets.push(n as usize);
                }
                let (key_block, key_size) = key.transcode_remainder(buf, num_cols, n as usize)?;
                buf = &buf[key_size..];
                let (val_block, val_size) = val.transcode_remainder(buf, num_cols, n as usize)?;

                let block = BlockMarker::Map(offsets, Box::new(key_block), Box::new(val_block));
                let complete_size = key_size + val_size + size_of::<u64>() * num_rows;
                return Ok((block, complete_size));
            }

            // https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/src/Columns/ColumnVariant.h
            // https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/src/Columns/ColumnVariant.cpp
            Self::Variant(variants) => {}
            _ => {}
        }

        todo!()
    }

    pub fn into_fixed_size_marker(self, data: Data<'a>) -> BlockMarker<'a> {
        match self {
            Type::Bool => BlockMarker::Bool(data),
            Type::Int8 => BlockMarker::Int8(data),
            Type::Int16 => BlockMarker::Int16(data),
            Type::Int32 => BlockMarker::Int32(data),
            Type::Int64 => BlockMarker::Int64(data),
            Type::Int128 => BlockMarker::Int128(data),
            Type::Int256 => BlockMarker::Int256(data),
            Type::UInt8 => BlockMarker::UInt8(data),
            Type::UInt16 => BlockMarker::UInt16(data),
            Type::UInt32 => BlockMarker::UInt32(data),
            Type::UInt64 => BlockMarker::UInt64(data),
            Type::UInt128 => BlockMarker::UInt128(data),
            Type::UInt256 => BlockMarker::UInt256(data),
            Type::Float32 => BlockMarker::Float32(data),
            Type::Float64 => BlockMarker::Float64(data),
            Type::BFloat16 => BlockMarker::BFloat16(data),
            Type::Decimal32(scale) => BlockMarker::Decimal32(scale, data),
            Type::Decimal64(scale) => BlockMarker::Decimal64(scale, data),
            Type::Decimal128(scale) => BlockMarker::Decimal128(scale, data),
            Type::Decimal256(scale) => BlockMarker::Decimal256(scale, data),
            Type::String => BlockMarker::String(data),
            Type::FixedString(size) => BlockMarker::FixedString(size, data),
            Type::Uuid => BlockMarker::Uuid(data),
            Type::Date => BlockMarker::Date(data),
            Type::Date32 => BlockMarker::Date32(data),
            Type::DateTime(tz) => BlockMarker::DateTime(tz, data),
            Type::DateTime64(precision, tz) => BlockMarker::DateTime64(precision, tz, data),
            Type::Ipv4 => BlockMarker::Ipv4(data),
            Type::Ipv6 => BlockMarker::Ipv6(data),
            Type::Point => BlockMarker::Point(data),

            Type::Tuple(inner) => BlockMarker::FixTuple(Type::Tuple(inner), data),

            Type::Enum8(values) => BlockMarker::Enum8(values, data),
            Type::Enum16(values) => BlockMarker::Enum16(values, data),

            _ => unimplemented!("Block marker not implemented for type: {:?}", self),
        }
    }
}
