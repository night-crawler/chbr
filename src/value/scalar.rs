use core::{convert::TryFrom, hint::cold_path};
use std::{
    hint::unreachable_unchecked,
    net::{Ipv4Addr, Ipv6Addr},
    ops::Range,
};

use chrono_tz::Tz;
use half::bf16;
use rust_decimal::Decimal;
use uuid::Uuid;

use super::{Value, short_type_name};
use crate::{
    Bf16Data, Date16Data, Date32Data, DateTime32Data, DateTime64Data, Decimal32Data, Decimal64Data,
    Decimal128Data, I256, Ipv4Data, Ipv6Data, U256, UuidData, error::Error, zc,
};

macro_rules! impl_try_from_value_slice {
    ($variant:ident, $ty:ty) => {
        impl<'a> TryFrom<Value<'a>> for $ty {
            type Error = Error;

            fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
                match value {
                    Value::$variant(v) => Ok(v),
                    Value::Empty => Ok(&[]),
                    other => Err(other.mismatched_type(stringify!($ty))),
                }
            }
        }
    };
}

macro_rules! impl_try_from_value {
    ($variant:ident, $ty:ty) => {
        impl<'a> TryFrom<Value<'a>> for $ty {
            type Error = Error;

            fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
                match value {
                    Value::$variant(v) => Ok(v),
                    other => Err(other.mismatched_type(stringify!($ty))),
                }
            }
        }
    };
}
impl_try_from_value_slice!(Int8Slice, &'a [i8]);
impl_try_from_value_slice!(Int16Slice, &'a [zc::I16]);
impl_try_from_value_slice!(Int32Slice, &'a [zc::I32]);
impl_try_from_value_slice!(Int64Slice, &'a [zc::I64]);
impl_try_from_value_slice!(Int128Slice, &'a [zc::I128]);

impl_try_from_value_slice!(UInt8Slice, &'a [u8]);
impl_try_from_value_slice!(UInt16Slice, &'a [zc::U16]);
impl_try_from_value_slice!(UInt32Slice, &'a [zc::U32]);
impl_try_from_value_slice!(UInt64Slice, &'a [zc::U64]);
impl_try_from_value_slice!(UInt128Slice, &'a [zc::U128]);

impl_try_from_value_slice!(Float32Slice, &'a [zc::F32]);
impl_try_from_value_slice!(Float64Slice, &'a [zc::F64]);

impl_try_from_value_slice!(UuidSlice, &'a [UuidData]);
impl_try_from_value_slice!(Date16Slice, &'a [Date16Data]);
impl_try_from_value_slice!(Date32Slice, &'a [Date32Data]);
impl_try_from_value_slice!(Ipv4Slice, &'a [Ipv4Data]);
impl_try_from_value_slice!(Ipv6Slice, &'a [Ipv6Data]);

impl_try_from_value!(Bool, bool);
impl_try_from_value!(Int256, &'a I256);

impl_try_from_value!(UInt256, &'a U256);

impl_try_from_value!(Float64, f64);
impl_try_from_value!(Float32, f32);
impl_try_from_value!(BFloat16, bf16);
impl_try_from_value_slice!(BFloat16Slice, &'a [Bf16Data]);

impl_try_from_value!(Ipv4, Ipv4Addr);

impl<'a> TryFrom<Value<'a>> for Ipv6Addr {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Ipv6(v) => Ok(Ipv6Addr::from(*v)),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl<'a> TryFrom<Value<'a>> for Uuid {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Uuid(uuid_data) => {
                let [hi, lo] = uuid_data.0;
                Ok(Uuid::from_u64_pair(hi.get(), lo.get()))
            }
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl TryFrom<Value<'_>> for chrono::DateTime<Tz> {
    type Error = Error;

    fn try_from(value: Value<'_>) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime(index, d) => {
                let value = d
                    .data
                    .get(index)
                    .expect("bug: we checked the boundary before creating the Value")
                    .with_tz(d.tz);
                Ok(value)
            }
            Value::DateTime64(index, d) => {
                let value = d
                    .data
                    .get(index)
                    .expect("bug: we checked the boundary before creating the Value")
                    .with_tz_and_precision(d.tz, d.precision)?;
                Ok(value)
            }
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl TryFrom<Value<'_>> for chrono::NaiveDate {
    type Error = Error;

    fn try_from(value: Value<'_>) -> Result<Self, Self::Error> {
        match value {
            Value::Date32(dt) | Value::Date(dt) => Ok(dt),
            other => Err(other.mismatched_type("Date/Date64")),
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
                        Value::Int8(v) => match <$target>::try_from(v) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("i8", stringify!($target), v.to_string()))
                            }
                        },
                        Value::Int16(v) => match <$target>::try_from(v) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("i16", stringify!($target), v.to_string()))
                            }
                        },
                        Value::Int32(v) => match <$target>::try_from(v) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("i32", stringify!($target), v.to_string()))
                            }
                        },
                        Value::Int64(v) => match <$target>::try_from(v) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("i64", stringify!($target), v.to_string()))
                            }
                        },
                        Value::Int128(v) => match <$target>::try_from(v.get()) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("i128", stringify!($target), v.to_string()))
                            }
                        },

                        Value::UInt8(v) => match <$target>::try_from(v) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("u8", stringify!($target), v.to_string()))
                            }
                        },
                        Value::UInt16(v) => match <$target>::try_from(v) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("u16", stringify!($target), v.to_string()))
                            }
                        },
                        Value::UInt32(v) => match <$target>::try_from(v) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("u32", stringify!($target), v.to_string()))
                            }
                        },
                        Value::UInt64(v) => match <$target>::try_from(v) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("u64", stringify!($target), v.to_string()))
                            }
                        },
                        Value::UInt128(v) => match <$target>::try_from(v.get()) {
                            Ok(value) => Ok(value),
                            Err(_) => {
                                cold_path();
                                Err(Error::ValueOutOfRange("u128", stringify!($target), v.to_string()))
                            }
                        },

                        other => Err(other.mismatched_type(stringify!($target))),
                    }
                }
            }
        )+
    };
}

impl_try_from_integer_value!(
    u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, i128, u128
);

// TODO: also isize iterator?
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
            _ => Err(value.mismatched_type(short_type_name::<Self>())),
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

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for SliceUsizeIterator<'_> {}
impl<'a> TryFrom<Value<'a>> for Decimal {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal32(index, mark) => Ok(mark.data[index].with_precision(mark.precision)),
            Value::Decimal64(index, mark) => Ok(mark.data[index].with_precision(mark.precision)),
            Value::Decimal128(index, mark) => mark.data[index].with_precision(mark.precision),
            Value::Decimal256(_, _) => {
                cold_path();
                Err(Error::NotImplemented(
                    "Decimal256 is not yet supported".to_owned(),
                ))
            }
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}
pub struct BoolSliceIterator<'a> {
    data: std::slice::Iter<'a, u8>,
}

impl<'a> TryFrom<Value<'a>> for BoolSliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::BoolSlice(data) => Ok(BoolSliceIterator { data: data.iter() }),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl Iterator for BoolSliceIterator<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|&byte| byte != 0)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.data.size_hint()
    }
}

impl ExactSizeIterator for BoolSliceIterator<'_> {}

pub struct DateTime32SliceIterator<'a> {
    tz: Tz,
    slice: std::slice::Iter<'a, DateTime32Data>,
}

impl<'a> TryFrom<Value<'a>> for DateTime32SliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime32Slice { tz, slice } => Ok(Self {
                tz,
                slice: slice.iter(),
            }),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl Iterator for DateTime32SliceIterator<'_> {
    type Item = chrono::DateTime<Tz>;

    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|dt| dt.with_tz(self.tz))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

impl ExactSizeIterator for DateTime32SliceIterator<'_> {}

pub struct DateTime64SliceIterator<'a> {
    tz: Tz,
    precision: u8,
    slice: std::slice::Iter<'a, DateTime64Data>,
}

impl<'a> TryFrom<Value<'a>> for DateTime64SliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime64Slice {
                tz,
                precision,
                slice,
            } => Ok(Self {
                tz,
                precision,
                slice: slice.iter(),
            }),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl Iterator for DateTime64SliceIterator<'_> {
    type Item = crate::Result<chrono::DateTime<Tz>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.slice
            .next()
            .map(|dt| dt.with_tz_and_precision(self.tz, self.precision))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

impl ExactSizeIterator for DateTime64SliceIterator<'_> {}
pub struct Decimal32SliceIterator<'a> {
    precision: u8,
    slice: std::slice::Iter<'a, Decimal32Data>,
}

impl<'a> TryFrom<Value<'a>> for Decimal32SliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal32Slice { precision, slice } => Ok(Self {
                precision,
                slice: slice.iter(),
            }),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl Iterator for Decimal32SliceIterator<'_> {
    type Item = Decimal;

    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|v| v.with_precision(self.precision))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

impl ExactSizeIterator for Decimal32SliceIterator<'_> {}

pub struct Decimal64SliceIterator<'a> {
    precision: u8,
    slice: std::slice::Iter<'a, Decimal64Data>,
}

impl<'a> TryFrom<Value<'a>> for Decimal64SliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal64Slice { precision, slice } => Ok(Self {
                precision,
                slice: slice.iter(),
            }),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl Iterator for Decimal64SliceIterator<'_> {
    type Item = Decimal;

    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|v| v.with_precision(self.precision))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

impl ExactSizeIterator for Decimal64SliceIterator<'_> {}

pub struct Decimal128SliceIterator<'a> {
    precision: u8,
    slice: std::slice::Iter<'a, Decimal128Data>,
}

impl<'a> TryFrom<Value<'a>> for Decimal128SliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal128Slice { precision, slice } => Ok(Self {
                precision,
                slice: slice.iter(),
            }),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl Iterator for Decimal128SliceIterator<'_> {
    type Item = crate::Result<Decimal>;

    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|v| v.with_precision(self.precision))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

impl ExactSizeIterator for Decimal128SliceIterator<'_> {}
pub struct Enum8SliceIterator<'a> {
    variants: &'a [(&'a str, i8)],
    data: std::slice::Iter<'a, i8>,
}

impl<'a> TryFrom<Value<'a>> for Enum8SliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Enum8Slice { mark, range } => {
                let range: Range<usize> = range.into();
                let data = &mark.data[range];
                Ok(Self {
                    variants: &mark.variants,
                    data: data.iter(),
                })
            }
            other => Err(other.mismatched_type("Enum8Iterator")),
        }
    }
}

impl<'a> Iterator for Enum8SliceIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.data.next()?;
        if let Ok(index) = self.variants.binary_search_by_key(value, |(_, id)| *id) {
            return Some(self.variants[index].0);
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.data.size_hint()
    }
}

impl ExactSizeIterator for Enum8SliceIterator<'_> {}

pub struct Enum16SliceIterator<'a> {
    variants: &'a [(&'a str, i16)],
    data: std::slice::Iter<'a, zc::I16>,
}

impl<'a> TryFrom<Value<'a>> for Enum16SliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Enum16Slice { mark, range } => {
                let range: Range<usize> = range.into();
                let data = &mark.data[range];
                Ok(Self {
                    variants: &mark.variants,
                    data: data.iter(),
                })
            }
            other => Err(other.mismatched_type("Enum16Iterator")),
        }
    }
}

impl<'a> Iterator for Enum16SliceIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.data.next()?.get();
        if let Ok(index) = self.variants.binary_search_by_key(&value, |(_, id)| *id) {
            return Some(self.variants[index].0);
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.data.size_hint()
    }
}

impl ExactSizeIterator for Enum16SliceIterator<'_> {}
