use crate::error::Error;
use crate::mark::Mark;
use crate::parse::parse_var_str;
use crate::types::{OffsetIndexPair as _, Offsets};
use crate::{i256, u256};
use chrono_tz::Tz;
use rust_decimal::Decimal;
use std::hint::unreachable_unchecked;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::ops::Range;
use uuid::Uuid;
use zerocopy::little_endian::{F32, F64, I16, I32, I64, I128, U16, U32, U64, U128};

#[derive(Debug, Clone)]
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

    ArraySlice {
        offsets: &'a Offsets<'a>,
        data: &'a Mark<'a>,
        slice_indices: Range<usize>,
    },

    Tuple(usize, &'a [Mark<'a>]),
    Map {
        offsets: &'a Offsets<'a>,
        keys: &'a Mark<'a>,
        values: &'a Mark<'a>,
        index: usize,
    },

    MapSlice {
        offsets: &'a Offsets<'a>,
        keys: &'a Mark<'a>,
        values: &'a Mark<'a>,
        slice_indices: Range<usize>,
    },

    TupleSlice {
        inner: &'a [Mark<'a>],
        slice_indices: Range<usize>,
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
            Value::StringSlice(_, _) => "StringSlice",
            Value::Int8Slice(_) => "Int8Slice",
            Value::Int16Slice(_) => "Int16Slice",
            Value::Int32Slice(_) => "Int32Slice",
            Value::Int64Slice(_) => "Int64Slice",
            Value::Int128Slice(_) => "Int128Slice",
            Value::Int256Slice(_) => "Int256Slice",
            Value::UInt8Slice(_) => "UInt8Slice",
            Value::UInt16Slice(_) => "UInt16Slice",
            Value::UInt32Slice(_) => "UInt32Slice",
            Value::UInt64Slice(_) => "UInt64Slice",
            Value::UInt128Slice(_) => "UInt128Slice",
            Value::UInt256Slice(_) => "UInt256Slice",
            Value::Float32Slice(_) => "Float32Slice",
            Value::Float64Slice(_) => "Float64Slice",
            Value::LowCardinalitySlice { .. } => "LowCardinalitySlice",
            Value::ArraySlice { .. } => "ArraySlice",
            Value::Tuple(_, _) => "Tuple",
            Value::Map { .. } => "Map",
            Value::MapSlice { .. } => "MapSlice",
            Value::TupleSlice { .. } => "TupleSlice",
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

impl_try_from_value!(Int8Slice, &'a [i8]);
impl_try_from_value!(Int16Slice, &'a [I16]);
impl_try_from_value!(Int32Slice, &'a [I32]);
impl_try_from_value!(Int64Slice, &'a [I64]);
impl_try_from_value!(Int128Slice, &'a [I128]);

impl_try_from_value!(UInt8Slice, &'a [u8]);
impl_try_from_value!(UInt16Slice, &'a [U16]);
impl_try_from_value!(UInt32Slice, &'a [U32]);
impl_try_from_value!(UInt64Slice, &'a [U64]);
impl_try_from_value!(UInt128Slice, &'a [U128]);

impl_try_from_value!(Bool, bool);
impl_try_from_value!(Int256, i256);

impl_try_from_value!(UInt256, u256);

impl_try_from_value!(Float64, f64);
impl_try_from_value!(Float32, f32);

impl_try_from_value!(Ipv4, Ipv4Addr);
impl_try_from_value!(Ipv6, Ipv6Addr);

impl_try_from_value!(Uuid, Uuid);

// impl_try_from_value!(Point, (f64, f64));

impl TryFrom<Value<'_>> for chrono::DateTime<Tz> {
    type Error = Error;

    fn try_from(value: Value<'_>) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime64(dt) | Value::DateTime(dt) => Ok(dt),
            other => Err(Error::MismatchedType(other.as_str(), "DateTime")),
        }
    }
}

impl TryFrom<Value<'_>> for chrono::NaiveDate {
    type Error = Error;

    fn try_from(value: Value<'_>) -> Result<Self, Self::Error> {
        match value {
            Value::Date32(dt) | Value::Date(dt) => Ok(dt),
            other => Err(Error::MismatchedType(other.as_str(), "DateTime")),
        }
    }
}

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

pub struct ArraySliceIterator<'a> {
    offsets: &'a Offsets<'a>,
    data: &'a Mark<'a>,
    slice_indices: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for ArraySliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::ArraySlice {
                offsets,
                data,
                slice_indices,
            } => Ok(Self {
                offsets,
                data,
                slice_indices,
            }),
            other => Err(Error::MismatchedType(other.as_str(), "ArraySliceIterator")),
        }
    }
}

impl<'a> Iterator for ArraySliceIterator<'a> {
    type Item = Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice_idx = self.slice_indices.next()?;

        let (start, end) = self.offsets.offset_indices(slice_idx).unwrap()?;
        Some(self.data.slice(start..end))
    }
}

impl<'a, T> TryFrom<Value<'a>> for Option<T>
where
    T: TryFrom<Value<'a>, Error = Error>,
{
    type Error = Error;

    #[inline]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Empty => Ok(None),
            other => T::try_from(other).map(Some),
        }
    }
}

macro_rules! impl_try_from_tuple {
    ($len:literal, $( $idx:tt => $T:ident ),+ $(,)?) => {
        impl<'a, $( $T , )+> core::convert::TryFrom<Value<'a>> for ( $( $T , )+ )
        where
            $( $T : core::convert::TryFrom<Value<'a>, Error = Error>, )+
        {
            type Error = Error;

            #[inline]
            fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
                match value {
                    Value::Tuple(row, values) => {
                        if values.len() != $len {
                            return Err(Error::MismatchedType(
                                concat!("Tuple with ", stringify!($len), " elements"),
                                concat!("Tuple", stringify!($len)),
                            ));
                        }

                        Ok((
                            $(
                                {
                                    let field_val = values[$idx]
                                        .get(row)
                                        .ok_or(Error::IndexOutOfBounds(
                                            row,
                                            concat!("Tuple", stringify!($len)),
                                        ))?;
                                    <$T>::try_from(field_val)?
                                },
                            )+
                        ))
                    }
                    other => Err(Error::MismatchedType(
                        other.as_str(),
                        concat!("Tuple", stringify!($len)),
                    )),
                }
            }
        }
    };
}

impl_try_from_tuple!(1, 0 => A);
impl_try_from_tuple!(2, 0 => A, 1 => B);
impl_try_from_tuple!(3, 0 => A, 1 => B, 2 => C);
impl_try_from_tuple!(4, 0 => A, 1 => B, 2 => C, 3 => D);
impl_try_from_tuple!(5, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E);
impl_try_from_tuple!(6, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F);
impl_try_from_tuple!(7, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G);
impl_try_from_tuple!(8, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H);
impl_try_from_tuple!(9, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H, 8 => I);
impl_try_from_tuple!(10, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H, 8 => I, 9 => J);

use core::convert::TryFrom;
use core::marker::PhantomData;

pub struct MapIterator<'a, K, V> {
    keys: &'a Mark<'a>,
    values: &'a Mark<'a>,
    range: Range<usize>,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> TryFrom<Value<'a>> for MapIterator<'a, K, V>
where
    K: TryFrom<Value<'a>, Error = Error>,
    V: TryFrom<Value<'a>, Error = Error>,
{
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Map {
                offsets,
                keys,
                values,
                index,
            } => {
                // Resolve (start, end) for the requested row in the Map column
                let (start, end) = offsets
                    .offset_indices(index)?
                    .ok_or(Error::IndexOutOfBounds(index, "Map"))?;

                Ok(Self {
                    keys,
                    values,
                    range: start..end,
                    _marker: PhantomData,
                })
            }
            other => Err(Error::MismatchedType(other.as_str(), "MapIterator")),
        }
    }
}

impl<'a, K, V> Iterator for MapIterator<'a, K, V>
where
    K: TryFrom<Value<'a>, Error = Error>,
    V: TryFrom<Value<'a>, Error = Error>,
{
    type Item = Result<(K, V), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.range.next()?;

        let raw_key = self.keys.get(idx)?;
        let raw_value = self.values.get(idx)?;

        Some(K::try_from(raw_key).and_then(|k| V::try_from(raw_value).map(|v| (k, v))))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.range.end - self.range.start;
        (remaining, Some(remaining))
    }
}

impl<'a, K, V> ExactSizeIterator for MapIterator<'a, K, V>
where
    K: TryFrom<Value<'a>, Error = Error>,
    V: TryFrom<Value<'a>, Error = Error>,
{
}

pub struct MapSliceIterator<'a, K, V> {
    offsets: &'a Offsets<'a>,
    keys: &'a Mark<'a>,
    values: &'a Mark<'a>,
    slice_indices: Range<usize>,
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> TryFrom<Value<'a>> for MapSliceIterator<'a, K, V>
where
    K: TryFrom<Value<'a>, Error = Error>,
    V: TryFrom<Value<'a>, Error = Error>,
{
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::MapSlice {
                offsets,
                keys,
                values,
                slice_indices,
            } => Ok(Self {
                offsets,
                keys,
                values,
                slice_indices,
                _marker: PhantomData,
            }),
            other => Err(Error::MismatchedType(other.as_str(), "MapSliceIterator")),
        }
    }
}

impl<'a, K, V> Iterator for MapSliceIterator<'a, K, V>
where
    K: TryFrom<Value<'a>, Error = Error>,
    V: TryFrom<Value<'a>, Error = Error>,
{
    type Item = Result<MapIterator<'a, K, V>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice_idx = self.slice_indices.next()?;
        let (start, end) = self.offsets.offset_indices(slice_idx).unwrap()?;

        Some(Ok(MapIterator {
            keys: self.keys,
            values: self.values,
            range: start..end,
            _marker: PhantomData,
        }))
    }
}

pub struct TupleSliceIterator<'a> {
    inner: &'a [Mark<'a>],
    slice_indices: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for TupleSliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::TupleSlice {
                inner,
                slice_indices,
            } => Ok(Self {
                inner,
                slice_indices,
            }),
            other => Err(Error::MismatchedType(other.as_str(), "TupleSliceIterator")),
        }
    }
}

impl<'a> Iterator for TupleSliceIterator<'a> {
    type Item = Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let row_idx = self.slice_indices.next()?;
        Some(Value::Tuple(row_idx, self.inner))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.slice_indices.end - self.slice_indices.start;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for TupleSliceIterator<'_> {}

impl<'a> TryFrom<Value<'a>> for Decimal {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal32(v)
            | Value::Decimal64(v)
            | Value::Decimal128(v)
            | Value::Decimal256(v) => Ok(v),
            other => Err(Error::MismatchedType(other.as_str(), "Decimal")),
        }
    }
}
