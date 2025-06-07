use chrono_tz::Tz;
use rust_decimal::Decimal;
use std::net::{Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

use crate::mark::Mark;
use crate::{i256, u256};

use crate::types::{Field, Offsets, Type};

#[derive(Debug)]
pub enum Value<'a> {
    Empty,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Int128(i128),
    Int256(i256),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(u128),
    UInt256(u256),
    Float32(f32),
    Float64(f64),
    BFloat16(f32),
    Decimal32(Decimal),
    Decimal64(Decimal),
    Decimal128(Decimal),
    Decimal256(Decimal),
    String(&'a str),
    Uuid(Uuid),
    Date(chrono::NaiveDate),
    Date32(chrono::NaiveDate),
    DateTime(chrono::DateTime<Tz>),
    DateTime64(chrono::DateTime<Tz>),
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
    Point((f64, f64)),
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
    FixTuple(Type<'a>, &'a [u8]),
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
        data: Vec<Mark<'a>>,
    },
}

impl<'a> Value<'a> {
    fn as_str(&self) -> &'static str {
        match self {
            Value::Empty => "Empty",
            Value::Bool(_) => "Bool",
            Value::Int8(_) => "Int8",
            Value::Int16(_) => "Int16",
            Value::Int32(_) => "Int32",
            Value::Int64(_) => "Int64",
            Value::Int128(_) => "Int128",
            Value::Int256(_) => "Int256",
            Value::UInt8(_) => "UInt8",
            Value::UInt16(_) => "UInt16",
            Value::UInt32(_) => "UInt32",
            Value::UInt64(_) => "UInt64",
            Value::UInt128(_) => "UInt128",
            Value::UInt256(_) => "UInt256",
            Value::Float32(_) => "Float32",
            Value::Float64(_) => "Float64",
            Value::BFloat16(_) => "BFloat16",
            Value::Decimal32(_) => "Decimal32",
            Value::Decimal64(_) => "Decimal64",
            Value::Decimal128(_) => "Decimal128",
            Value::Decimal256(_) => "Decimal256",
            Value::String(_) => "String",
            Value::Uuid(_) => "Uuid",
            Value::Date(_) | Value::Date32(_) => "Date",
            Value::DateTime(_) | Value::DateTime64(_) => "DateTime",
            Value::Ipv4(_) => "Ipv4",
            Value::Ipv6(_) => "Ipv6",
            Value::Point(_) => "Point",
            _ => todo!(),
        }
    }
}

use crate::error::Error;
macro_rules! impl_try_from_value {
    ($variant:ident, $ty:ty) => {
        impl<'a> TryFrom<Value<'a>> for $ty {
            type Error = Error;

            fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
                match value {
                    Value::$variant(v) => Ok(v),
                    other => Err(Error::MismatchedType(other.as_str(), stringify!($ty))),
                }
            }
        }
    };
}

impl_try_from_value!(Bool, bool);
impl_try_from_value!(Int8, i8);
impl_try_from_value!(Int16, i16);
impl_try_from_value!(Int32, i32);
impl_try_from_value!(Int64, i64);
impl_try_from_value!(Int128, i128);
impl_try_from_value!(Int256, i256);

impl_try_from_value!(UInt8, u8);
impl_try_from_value!(UInt16, u16);
impl_try_from_value!(UInt32, u32);
impl_try_from_value!(UInt64, u64);
impl_try_from_value!(UInt128, u128);
impl_try_from_value!(UInt256, u256);

impl_try_from_value!(Float64, f64);
impl_try_from_value!(Float32, f32);

impl_try_from_value!(Ipv4, Ipv4Addr);
impl_try_from_value!(Ipv6, Ipv6Addr);

impl_try_from_value!(Uuid, Uuid);
impl_try_from_value!(Point, (f64, f64));
