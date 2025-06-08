use crate::error::Error;
use crate::mark::Mark;
use crate::parse::parse_var_str;
use crate::{i256, u256};
use chrono_tz::Tz;
use rust_decimal::Decimal;
use std::hint::unreachable_unchecked;
use std::net::{Ipv4Addr, Ipv6Addr};
use uuid::Uuid;
use zerocopy::little_endian::{F32, F64, I16, I32, I64, I128, U16, U32, U64, U128};

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

    StringSlice(usize, &'a [u8]),
    Int8Slice(&'a [i8]),
    Int16Slice(&'a [I16]),
    Int32Slice(&'a [I32]),
    Int64Slice(&'a [I64]),
    Int128Slice(&'a [I128]),
    Int256Slice(&'a [i256]),
    UInt8Slice(&'a [u8]),
    UInt16Slice(&'a [U16]),
    UInt32Slice(&'a [U32]),
    UInt64Slice(&'a [U64]),
    UInt128Slice(&'a [U128]),
    UInt256Slice(&'a [u256]),
    Float32Slice(&'a [F32]),
    Float64Slice(&'a [F64]),

    LowCardinalitySlice {
        indices: Box<Value<'a>>,
        additional_keys: &'a Mark<'a>,
    },
}

impl Value<'_> {
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

impl_try_from_value!(String, &'a str);

impl_try_from_value!(Int64Slice, &'a [I64]);

impl_try_from_value!(Bool, bool);
impl_try_from_value!(Int256, i256);

impl_try_from_value!(UInt256, u256);

impl_try_from_value!(Float64, f64);
impl_try_from_value!(Float32, f32);

impl_try_from_value!(Ipv4, Ipv4Addr);
impl_try_from_value!(Ipv6, Ipv6Addr);

impl_try_from_value!(Uuid, Uuid);
impl_try_from_value!(Point, (f64, f64));

pub struct StringSliceIterator<'a> {
    data: &'a [u8],
    count: usize,
    index: usize,
}

impl<'a> Iterator for StringSliceIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }
        let s;
        (self.data, s) = parse_var_str(self.data).unwrap();
        self.index += 1;
        Some(s)
    }
}

impl<'a> TryFrom<Value<'a>> for StringSliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::StringSlice(count, data) => Ok(Self {
                data,
                count,
                index: 0,
            }),
            other => Err(Error::MismatchedType(other.as_str(), "StringSliceIterator")),
        }
    }
}

macro_rules! impl_try_from_integer_value {
    ($($target:ty),+ $(,)?) => {
        $(
            impl<'a> core::convert::TryFrom<Value<'a>> for $target {
                type Error = Error;

                fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {

                    match value {
                        Value::Int8(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("i8", stringify!($target), v.to_string())
                        }),
                        Value::Int16(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("i16", stringify!($target), v.to_string())
                        }),
                        Value::Int32(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("i32", stringify!($target), v.to_string())
                        }),
                        Value::Int64(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("i64", stringify!($target), v.to_string())
                        }),
                        Value::Int128(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("i128", stringify!($target), v.to_string())
                        }),

                        Value::UInt8(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("u8", stringify!($target), v.to_string())
                        }),
                        Value::UInt16(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("u16", stringify!($target), v.to_string())
                        }),
                        Value::UInt32(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("u32", stringify!($target), v.to_string())
                        }),
                        Value::UInt64(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("u64", stringify!($target), v.to_string())
                        }),
                        Value::UInt128(v) => <$target>::try_from(v).map_err(|_| {
                            Error::ValueOutOfRange("u128", stringify!($target), v.to_string())
                        }),

                        other => Err(Error::MismatchedType(other.as_str(), stringify!($target))),
                    }
                }
            }
        )+
    };
}

impl_try_from_integer_value!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize,
);

pub struct SliceUsizeIterator<'a> {
    value: Value<'a>,
    index: usize,
    len: usize,
}

impl<'a> TryFrom<Value<'a>> for SliceUsizeIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::UInt8Slice(x) => Ok(Self {
                value,
                index: 0,
                len: x.len(),
            }),
            Value::UInt16Slice(x) => Ok(Self {
                value,
                index: 0,
                len: x.len(),
            }),
            Value::UInt32Slice(x) => Ok(Self {
                value,
                index: 0,
                len: x.len(),
            }),
            Value::UInt64Slice(x) => Ok(Self {
                value,
                index: 0,
                len: x.len(),
            }),
            _ => Err(Error::MismatchedType(value.as_str(), "SliceIndexIterator")),
        }
    }
}

impl Iterator for SliceUsizeIterator<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }

        let result = match &self.value {
            Value::UInt8Slice(bv) => bv.get(self.index).copied().map(usize::from),
            Value::UInt16Slice(bv) => bv.get(self.index).map(|v| v.get() as usize),
            Value::UInt32Slice(bv) => bv.get(self.index).map(|v| v.get() as usize),
            Value::UInt64Slice(bv) => {
                if let Some(value) = bv.get(self.index).map(|v| v.get()) {
                    usize::try_from(value).ok()
                } else {
                    None
                }
            }
            _ => unsafe { unreachable_unchecked() },
        };

        self.index += 1;
        result
    }
}

pub struct LowCardinalitySliceIterator<'a> {
    indices: SliceUsizeIterator<'a>,
    additional_keys: &'a Mark<'a>,
}

impl<'a> TryFrom<Value<'a>> for LowCardinalitySliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::LowCardinalitySlice {
                indices,
                additional_keys,
            } => Ok(Self {
                indices: SliceUsizeIterator::try_from(*indices)?,
                additional_keys,
            }),
            other => Err(Error::MismatchedType(
                other.as_str(),
                "LowCardinalitySliceIterator",
            )),
        }
    }
}

impl<'a> Iterator for LowCardinalitySliceIterator<'a> {
    type Item = Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.indices.next()?;
        self.additional_keys.get(index)
    }
}
