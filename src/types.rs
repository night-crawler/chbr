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

pub fn u64_varuint(buf: &[u8]) -> crate::Result<(usize, &[u8])> {
    let (n, rest) = decode::u64(buf)?;
    Ok((n as usize, rest))
}

pub struct Data<'a> {
    pub data: &'a [u8],
    pub num_rows: usize,
}

pub enum Marker<'a> {
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
    Ring(Box<Marker<'a>>),
    Polygon(Box<Marker<'a>>),
    MultiPolygon(Box<Marker<'a>>),
    LineString(Box<Marker<'a>>),
    MultiLineString(Box<Marker<'a>>),

    Enum8(Vec<(&'a str, i8)>, Data<'a>),
    Enum16(Vec<(&'a str, i16)>, Data<'a>),

    LowCardinality(Box<Type<'a>>, Data<'a>),
    Array(Vec<usize>, Box<Marker<'a>>),
    VarTuple(Vec<Marker<'a>>),
    FixTuple(Type<'a>, Data<'a>),
    Nullable(&'a [u8], Box<Marker<'a>>),
    Map(Vec<usize>, Box<Marker<'a>>, Box<Marker<'a>>),
    Variant(&'a [u8], Vec<Marker<'a>>),
    Nested(Vec<Field<'a>>, Data<'a>),
    Dynamic(&'a [u8], Vec<Marker<'a>>),

    Json {
        columns: Box<Marker<'a>>,
        data: Vec<Marker<'a>>
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


#[derive(Debug)]
struct JsonColumnHeader<'a> {
    path_version: u64,
    max_types: usize,
    total_types: usize,
    typ: Box<Type<'a>>,
    variant_version: u64,
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
            Self::Dynamic => None,
            Self::Json => None,

            Self::Nullable(_) => None,

            _ => unimplemented!("Size is not implemented for type: {:?}", self),
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
        num_rows: usize,
    ) -> crate::Result<(Marker<'a>, usize)> {
        if let Some(size) = self.size() {
            let data_size = size * num_rows;
            let data = Data {
                data: &remainder[..data_size],
                num_rows,
            };
            return Ok((self.into_fixed_size_marker(data), data_size));
        }

        match self {
            Self::Nullable(inner) => {
                let (mask, buf) = remainder.split_at(num_rows);
                println!("{:?}", mask);
                let (inner_block, size) = inner.transcode_remainder(buf, num_rows)?;

                let block = Marker::Nullable(mask, Box::new(inner_block));
                return Ok((block, size + num_rows));
            }

            Self::String => {
                let mut buf = remainder;
                let start_ptr = buf.as_ptr();
                let mut n;
                for _ in 0..num_rows {
                    (n, buf) = u64_varuint(buf)?;
                    buf = &buf[n..];
                }
                let end = buf.as_ptr();
                let data_size = end as usize - start_ptr as usize;
                let data = Data {
                    data: &remainder[..data_size],
                    num_rows,
                };
                return Ok((Marker::String(data), data_size));
            }

            Self::Tuple(inner) => {
                let mut buf = remainder;
                let mut blocks = vec![];

                let mut total_size = 0;
                for typ in inner {
                    let (block, size) = typ.transcode_remainder(buf, num_rows)?;
                    blocks.push(block);
                    buf = &buf[size..];
                    total_size += size;
                }

                let block = Marker::VarTuple(blocks);
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
                println!("{:?}", offsets);
                let (inner_block, col_data_size) =
                    inner.transcode_remainder(buf,  n as usize)?;
                let block = Marker::Array(offsets, Box::new(inner_block));
                let complete_size = col_data_size + size_of::<u64>() * num_rows;
                return Ok((block, complete_size));
            }

            Self::Ring => {
                let (points, size) = Type::Array(Box::new(Type::Point))
                    .transcode_remainder(remainder,  num_rows)?;
                let wrapped = Marker::Ring(Box::new(points));
                return Ok((wrapped, size));
            }
            Self::Polygon => {
                let (rings, size) = Type::Array(Box::new(Type::Ring))
                    .transcode_remainder(remainder,  num_rows)?;
                let wrapped = Marker::Polygon(Box::new(rings));
                return Ok((wrapped, size));
            }
            Self::MultiPolygon => {
                let (polygons, size) = Type::Array(Box::new(Type::Polygon))
                    .transcode_remainder(remainder, num_rows)?;
                let wrapped = Marker::MultiPolygon(Box::new(polygons));
                return Ok((wrapped, size));
            }
            Self::LineString => {
                let (points, size) = Type::Array(Box::new(Type::Point))
                    .transcode_remainder(remainder, num_rows)?;
                let wrapped = Marker::LineString(Box::new(points));
                return Ok((wrapped, size));
            }
            Self::MultiLineString => {
                let (points, size) = Type::Array(Box::new(Type::LineString))
                    .transcode_remainder(remainder, num_rows)?;
                let wrapped = Marker::MultiLineString(Box::new(points));
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
                let (key_block, key_size) = key.transcode_remainder(buf, n as usize)?;
                buf = &buf[key_size..];
                let (val_block, val_size) = val.transcode_remainder(buf, n as usize)?;

                let block = Marker::Map(offsets, Box::new(key_block), Box::new(val_block));
                let complete_size = key_size + val_size + size_of::<u64>() * num_rows;
                return Ok((block, complete_size));
            }

            // https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/src/Columns/ColumnVariant.h
            // https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/src/Columns/ColumnVariant.cpp
            Self::Variant(types) => {
                const NULL_DISCR: u8 = 255;
                let (mode, buf) = u64_le(remainder)?;
                if mode != 0 {
                    panic!();
                }
                let (discriminators, mut buf) = buf.split_at(num_rows);

                let mut num_rows_per_discriminant = vec![0usize; types.len()];
                for &discriminator in discriminators {
                    if discriminator == NULL_DISCR {
                        continue;
                    }
                    num_rows_per_discriminant[discriminator as usize] += 1;
                }

                let mut blocks = Vec::with_capacity(types.len());

                for (idx, typ) in types.into_iter().enumerate() {
                    let rows_here = num_rows_per_discriminant[idx];
                    let (sub_block, sz) = typ.transcode_remainder(buf,  rows_here)?;
                    blocks.push(sub_block);
                    buf = &buf[sz..];
                }

                let consumed = remainder.len() - buf.len();
                let marker = Marker::Variant(discriminators, blocks);
                return Ok((marker, consumed));
            }

            Self::Dynamic => {
                let (version, mut buf) = u64_le(remainder)?;
                let num_types;
                if version == 1 {
                    (_, buf) = u64_varuint(buf)?;
                }
                (num_types, buf) = u64_varuint(buf)?;
                println!("num_types: {num_types}");

                let mut types = Vec::with_capacity(num_types);
                let mut name_len;
                for _ in 0..num_types {
                    (name_len, buf) = u64_varuint(buf)?;
                    let name = unsafe { std::str::from_utf8_unchecked(&buf[..name_len]) };
                    buf = &buf[name_len..];
                    let typ = Type::from_str(name)?;
                    types.push(typ);
                }

                println!("{:?}", types);

                // skip stats I guess?
                buf = &buf[8..];

                let mut discriminator;
                let mut counters = vec![0usize; num_types];
                let discriminator_start = buf;
                for _ in 0..num_rows {
                    (discriminator, buf) = u64_varuint(buf)?;
                    if discriminator == 0 {
                        continue;
                    }
                    counters[discriminator - 1] += 1;
                }
                let discriminators_size = discriminator_start.len() - buf.len();
                let discriminators = &discriminator_start[..discriminators_size];

                let mut markers = Vec::with_capacity(num_types);
                for (index, typ) in types.into_iter().enumerate() {
                    let typ_rows = counters[index];
                    let (marker, sz) = typ.transcode_remainder(buf, typ_rows)?;
                    markers.push(marker);
                    buf = &buf[sz..];
                }

                let marker = Marker::Dynamic(discriminators, markers);
                let consumed = remainder.len() - buf.len();

                return Ok((marker, consumed));
            }
            Self::Json => {
                let (_version, mut buf) = u64_le(remainder)?;

                let num_paths;
                (_, buf) = u64_varuint(buf)?;
                (num_paths, buf) = u64_varuint(buf)?;

                let (subcols, size) = Type::String.transcode_remainder(buf, num_paths)?;
                buf = &buf[size..];

                let mut col_headers = Vec::with_capacity(num_paths);

                for _ in 0..num_paths {
                    let version;
                    (version, buf) = u64_le(buf)?;

                    let max_types;
                    (max_types, buf) = u64_varuint(buf)?;

                    let total_types;
                    (total_types, buf) = u64_varuint(buf)?;

                    let name_len;
                    (name_len, buf) = u64_varuint(buf)?;

                    let name = unsafe { std::str::from_utf8_unchecked(&buf[..name_len]) };
                    let typ = Type::from_str(name)?;
                    buf = &buf[name_len..];

                    let variant;
                    (variant, buf) = u64_le(buf)?;

                    let header = JsonColumnHeader {
                        path_version: version,
                        max_types,
                        total_types,
                        typ: Box::new(typ),
                        variant_version: variant,
                    };
                    col_headers.push(header);
                }

                println!("{:#?}", col_headers);

                let mut final_cols = Vec::with_capacity(num_paths);
                for (index, header) in col_headers.into_iter().enumerate() {
                    let discriminators;
                    (discriminators, buf) = buf.split_at(num_rows);

                    let local_rows = discriminators.iter().filter(|&&d| d == index as u8).count();

                    let (marker, sz) = header.typ.transcode_remainder(buf,  local_rows)?;
                    buf = &buf[sz..];
                    final_cols.push(marker);
                }

                let marker = Marker::Json {
                    columns: Box::new(subcols),
                    data: final_cols,
                };
                let consumed = buf.as_ptr() as usize - 
                    remainder.as_ptr() as usize;

                return Ok((marker, consumed));
            }

            _ => {}
        }

        todo!()
    }

    pub fn into_fixed_size_marker(self, data: Data<'a>) -> Marker<'a> {
        match self {
            Type::Bool => Marker::Bool(data),
            Type::Int8 => Marker::Int8(data),
            Type::Int16 => Marker::Int16(data),
            Type::Int32 => Marker::Int32(data),
            Type::Int64 => Marker::Int64(data),
            Type::Int128 => Marker::Int128(data),
            Type::Int256 => Marker::Int256(data),
            Type::UInt8 => Marker::UInt8(data),
            Type::UInt16 => Marker::UInt16(data),
            Type::UInt32 => Marker::UInt32(data),
            Type::UInt64 => Marker::UInt64(data),
            Type::UInt128 => Marker::UInt128(data),
            Type::UInt256 => Marker::UInt256(data),
            Type::Float32 => Marker::Float32(data),
            Type::Float64 => Marker::Float64(data),
            Type::BFloat16 => Marker::BFloat16(data),
            Type::Decimal32(scale) => Marker::Decimal32(scale, data),
            Type::Decimal64(scale) => Marker::Decimal64(scale, data),
            Type::Decimal128(scale) => Marker::Decimal128(scale, data),
            Type::Decimal256(scale) => Marker::Decimal256(scale, data),
            Type::String => Marker::String(data),
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

            _ => unimplemented!("Block marker not implemented for type: {:?}", self),
        }
    }
}
