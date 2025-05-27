#![allow(dead_code)]

use crate::parse::parse_type;
pub use chrono_tz::Tz;
use phf::phf_map;
use unsigned_varint::decode;

pub struct Data<'a> {
    pub data: &'a [u8],
    pub num_rows: usize,
    pub num_columns: usize,
}

pub enum BlockMarker<'a> {
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
    DateTime(Tz, Data<'a>),
    DateTime64(usize, Tz, Data<'a>),
    Ipv4(Data<'a>),
    Ipv6(Data<'a>),
    Point(Data<'a>),
    Ring(Data<'a>),
    Polygon(Data<'a>),
    MultiPolygon(Data<'a>),
    
    Enum8(Vec<(String, i8)>),
    Enum16(Vec<(String, i16)>),
    
    LowCardinality(Box<Type<'a>>, Data<'a>),
    Array(Box<Type<'a>>, Data<'a>),
    Tuple(Vec<Type<'a>>, Data<'a>),
    Nullable(Box<Type<'a>>, Data<'a>),
    Map(Box<Type<'a>>, Box<Type<'a>>, Data<'a>),
    Variant(Vec<Type<'a>>, Data<'a>),
    Nested(Vec<Field<'a>>, Data<'a>),
}


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type<'a> {
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
    DateTime(Tz),
    DateTime64(usize, Tz),

    Ipv4,
    Ipv6,

    Point,
    Ring,
    Polygon,
    MultiPolygon,
    
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

impl Type<'_> {
    pub fn size(&self) -> Option<usize> {
        match self {
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
            
            _ => unimplemented!("Size not implemented for type: {:?}", self),
        }
    }
    
    pub fn from_str(s: &str) -> Result<Type, crate::error::Error> {
        let primitive = PRIMITIVES.get(s).cloned();
        if let Some(primitive) = primitive {
            return Ok(primitive);
        }
        let (remainder, typ) = parse_type(s).map_err(|e| crate::error::Error::Parse(e.to_string()))?;
        if !remainder.trim().is_empty() {
            return Err(crate::error::Error::Parse(format!("Unparsed remainder: {remainder}")));
        }

        Ok(typ)
    }
    
    pub fn transcode_remainder(self, remainder: &[u8], num_cols: usize, num_rows: usize) -> crate::Result<(BlockMarker, usize)> {
        if let Some(size) = self.size() {
            let data_size = size * num_rows;
            let data = Data {
                data: &remainder[..data_size],
                num_rows,
                num_columns: num_cols,
            };
            return Ok((self.into_marker(data), data_size));
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
            },
            _ => {}
        }
        
        
        todo!()
        
    }
    
    pub fn into_marker(self, data: Data) -> BlockMarker {
        match self {
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
            Type::DateTime(tz) => BlockMarker::DateTime(tz, data),
            Type::DateTime64(precision, tz) => BlockMarker::DateTime64(precision, tz, data),
            Type::Ipv4 => BlockMarker::Ipv4(data),
            Type::Ipv6 => BlockMarker::Ipv6(data),
            Type::Point => BlockMarker::Point(data),
            Type::Ring => BlockMarker::Ring(data),
            Type::Polygon => BlockMarker::Polygon(data),
            Type::MultiPolygon => BlockMarker::MultiPolygon(data),

            _ => unimplemented!("Block marker not implemented for type: {:?}", self)
        }
    }
}

static PRIMITIVES: phf::Map<&'static str, Type> = phf_map! {
    "Int8" => Type::Int8,
    "Int16" => Type::Int16,
    "Int32" => Type::Int32,
    "Int64" => Type::Int64,
    "Int128" => Type::Int128,
    "Int256" => Type::Int256,
    "UInt8" => Type::UInt8,
    "UInt16" => Type::UInt16,
    "UInt32" => Type::UInt32,
    "UInt64" => Type::UInt64,
    "UInt128" => Type::UInt128,
    "UInt256" => Type::UInt256,

    "Float32" => Type::Float32,
    "Float64" => Type::Float64,
    "BFloat16" => Type::BFloat16,

    "Ipv4" => Type::Ipv4,
    "Ipv6" => Type::Ipv6,

    "MultiPolygon" => Type::MultiPolygon,
    "Polygon" => Type::Polygon,
    "Ring" => Type::Ring,
    "Point" => Type::Point,

    "UUID" => Type::Uuid,
};
