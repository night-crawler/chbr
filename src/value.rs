use crate::error::Error;
use crate::mark::{
    Mark, MarkArray, MarkDateTime, MarkDateTime64, MarkDecimal32, MarkDecimal64, MarkDecimal128,
    MarkDecimal256, MarkEnum8, MarkEnum16, MarkFixedString, MarkLowCardinality, MarkMap,
    MarkNested, MarkNullable, MarkTuple,
};
use crate::types::{OffsetIndexPair as _, Offsets};
use crate::{
    Bf16Data, ByteSliceExt as _, Date16Data, Date32Data, DateTime32Data, DateTime64Data,
    Decimal32Data, Decimal64Data, Decimal128Data, Decimal256Data, Ipv4Data, Ipv6Data, TinyRange,
    UuidData, i256, u256,
};
use chrono_tz::Tz;
use core::convert::TryFrom;
use core::marker::PhantomData;
use half::bf16;
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
    Int128(&'a I128),
    Int256(&'a i256),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    UInt128(&'a U128),
    UInt256(&'a u256),
    Float32(f32),
    Float64(f64),
    BFloat16(bf16),
    Decimal32(usize, &'a MarkDecimal32<'a>),
    Decimal64(usize, &'a MarkDecimal64<'a>),
    Decimal128(usize, &'a MarkDecimal128<'a>),
    Decimal256(usize, &'a MarkDecimal256<'a>),
    String(&'a str),
    Uuid(&'a UuidData),
    Date(chrono::NaiveDate),
    Date32(chrono::NaiveDate),
    DateTime(usize, &'a MarkDateTime<'a>),
    DateTime64(usize, &'a MarkDateTime64<'a>),
    Ipv4(Ipv4Addr),
    Ipv6(&'a Ipv6Data),

    StringSlice(&'a [&'a str]),
    BoolSlice(&'a [u8]),
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
    BFloat16Slice(&'a [Bf16Data]),

    Decimal32Slice {
        precision: u8,
        slice: &'a [Decimal32Data],
    },
    Decimal64Slice {
        precision: u8,
        slice: &'a [Decimal64Data],
    },
    Decimal128Slice {
        precision: u8,
        slice: &'a [Decimal128Data],
    },
    Decimal256Slice {
        precision: u8,
        slice: &'a [Decimal256Data],
    },

    UuidSlice(&'a [UuidData]),
    Date16Slice(&'a [Date16Data]),
    Date32Slice(&'a [Date32Data]),
    DateTime32Slice {
        tz: Tz,
        slice: &'a [DateTime32Data],
    },
    DateTime64Slice {
        tz: Tz,
        precision: u8,
        slice: &'a [DateTime64Data],
    },

    Ipv4Slice(&'a [Ipv4Data]),
    Ipv6Slice(&'a [Ipv6Data]),

    LowCardinalitySlice {
        idx: TinyRange,
        mark_lc: &'a MarkLowCardinality<'a>,
    },

    ArraySlice {
        mark_array: &'a MarkArray<'a>,
        slice_indices: TinyRange,
    },

    Tuple {
        index: usize,
        mark: &'a MarkTuple<'a>,
    },
    Map {
        mark_map: &'a MarkMap<'a>,
        index: usize,
    },

    MapSlice {
        mark_map: &'a MarkMap<'a>,
        slice_indices: TinyRange,
    },

    TupleSlice {
        mark: &'a MarkTuple<'a>,
        slice_indices: TinyRange,
    },

    NullableSlice {
        mark_nullable: &'a MarkNullable<'a>,
        slice_indices: TinyRange,
    },

    Nested {
        mark_nested: &'a MarkNested<'a>,
        index: usize,
    },

    NestedSlice {
        mark_nested: &'a MarkNested<'a>,
        slice_indices: TinyRange,
    },

    FixedStringSlice {
        mark_fs: &'a MarkFixedString<'a>,
        indices: TinyRange,
    },

    Enum8Slice {
        mark: &'a MarkEnum8<'a>,
        slice_indices: TinyRange,
    },

    Enum16Slice {
        mark: &'a MarkEnum16<'a>,
        slice_indices: TinyRange,
    },
}

impl Value<'_> {
    const fn as_str(&self) -> &'static str {
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
            Value::Decimal32(_, _) => "Decimal32",
            Value::Decimal64(_, _) => "Decimal64",
            Value::Decimal128(_, _) => "Decimal128",
            Value::Decimal256(_, _) => "Decimal256",
            Value::String(_) => "String",
            Value::Uuid(_) => "Uuid",
            Value::Date(_) | Value::Date32(_) => "Date",
            Value::DateTime(_, _) => "DateTime",
            Value::DateTime64(_, _) => "DateTime64",
            Value::Ipv4(_) => "Ipv4",
            Value::Ipv6(_) => "Ipv6",
            Value::StringSlice(_) => "StringSlice",
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
            Value::Tuple { .. } => "Tuple",
            Value::Map { .. } => "Map",
            Value::MapSlice { .. } => "MapSlice",
            Value::TupleSlice { .. } => "TupleSlice",
            Value::BoolSlice(_) => "BoolSlice",
            Value::UuidSlice(_) => "UuidSlice",
            Value::Date16Slice(_) => "Date16Slice",
            Value::Date32Slice(_) => "Date32Slice",
            Value::DateTime32Slice { .. } => "DateTime32Slice",
            Value::DateTime64Slice { .. } => "DateTime64Slice",
            Value::Ipv4Slice(_) => "Ipv4Slice",
            Value::Ipv6Slice(_) => "Ipv6Slice",
            Value::NullableSlice { .. } => "NullableSlice",
            Value::Decimal32Slice { .. } => "Decimal32Slice",
            Value::Decimal64Slice { .. } => "Decimal64Slice",
            Value::Decimal128Slice { .. } => "Decimal128Slice",
            Value::Decimal256Slice { .. } => "Decimal256Slice",
            Value::Nested { .. } => "Nested",
            Value::NestedSlice { .. } => "NestedSlice",
            Value::FixedStringSlice { .. } => "FixedStringSlice",
            Value::Enum8Slice { .. } => "Enum8SliceIterator",
            Value::Enum16Slice { .. } => "Enum16SliceIterator",
            Value::BFloat16Slice(_) => "BFloat16Slice",
        }
    }
}

macro_rules! impl_try_from_value {
    ($variant:ident, $ty:ty) => {
        impl<'a> TryFrom<Value<'a>> for $ty {
            type Error = Error;

            #[inline(always)]
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
impl_try_from_value!(StringSlice, &'a [&'a str]);

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

impl_try_from_value!(UuidSlice, &'a [UuidData]);
impl_try_from_value!(Date16Slice, &'a [Date16Data]);
impl_try_from_value!(Date32Slice, &'a [Date32Data]);
impl_try_from_value!(Ipv4Slice, &'a [Ipv4Data]);
impl_try_from_value!(Ipv6Slice, &'a [Ipv6Data]);

impl_try_from_value!(Bool, bool);
impl_try_from_value!(Int256, &'a i256);

impl_try_from_value!(UInt256, &'a u256);

impl_try_from_value!(Float64, f64);
impl_try_from_value!(Float32, f32);
impl_try_from_value!(BFloat16, bf16);
impl_try_from_value!(BFloat16Slice, &'a [Bf16Data]);

impl_try_from_value!(Ipv4, Ipv4Addr);

impl<'a> TryFrom<Value<'a>> for Ipv6Addr {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Ipv6(v) => Ok(Ipv6Addr::from(*v)),
            other => Err(Error::MismatchedType(other.as_str(), "&\'aIpv6Data")),
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
            other => Err(Error::MismatchedType(other.as_str(), "DateTime")),
        }
    }
}

impl TryFrom<Value<'_>> for chrono::DateTime<Tz> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'_>) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime(index, d) => {
                // we checked the boundary before creating the Value
                let value = d.data.get(index).unwrap().with_tz(d.tz);
                Ok(value)
            }
            Value::DateTime64(index, d) => {
                let value = d
                    .data
                    .get(index)
                    .unwrap()
                    .with_tz_and_precision(d.tz, d.precision);
                let Some(value) = value else {
                    return Err(Error::Overflow("DateTime64 value out of range".to_owned()));
                };

                Ok(value)
            }
            other => Err(Error::MismatchedType(other.as_str(), "DateTime")),
        }
    }
}

impl TryFrom<Value<'_>> for chrono::NaiveDate {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'_>) -> Result<Self, Self::Error> {
        match value {
            Value::Date32(dt) | Value::Date(dt) => Ok(dt),
            other => Err(Error::MismatchedType(other.as_str(), "DateTime")),
        }
    }
}

macro_rules! impl_try_from_integer_value {
    ($($target:ty),+ $(,)?) => {
        $(
            impl<'a> core::convert::TryFrom<Value<'a>> for $target {
                type Error = Error;

                #[inline(always)]
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
                        Value::Int128(v) => <$target>::try_from(v.get()).map_err(|_| {
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
                        Value::UInt128(v) => <$target>::try_from(v.get()).map_err(|_| {
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

    #[inline(always)]
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

    #[inline(always)]
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

pub struct LowCardinalitySliceIterator<'a> {
    indices: SliceUsizeIterator<'a>,
    additional_keys: &'a Mark<'a>,
}

impl<'a> TryFrom<Value<'a>> for LowCardinalitySliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::LowCardinalitySlice { idx, mark_lc: lc } => {
                let Some(additional_keys) = lc.additional_keys.as_ref() else {
                    return Err(Error::MismatchedType(
                        "LowCardinalitySliceIterator",
                        "LowCardinalitySlice with no additional keys",
                    ));
                };

                let sliced = lc.indices.slice(idx.into());

                Ok(Self {
                    indices: SliceUsizeIterator::try_from(sliced)?,
                    additional_keys,
                })
            }
            other => Err(Error::MismatchedType(
                other.as_str(),
                "LowCardinalitySliceIterator",
            )),
        }
    }
}

impl<'a> Iterator for LowCardinalitySliceIterator<'a> {
    type Item = Value<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.indices.next()?;
        self.additional_keys.get(index)
    }
}

pub struct ArraySliceIterator<'a> {
    mark_array: &'a MarkArray<'a>,
    slice_indices: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for ArraySliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::ArraySlice {
                mark_array,
                slice_indices,
            } => Ok(Self {
                mark_array,
                slice_indices: slice_indices.into(),
            }),
            other => Err(Error::MismatchedType(other.as_str(), "ArraySliceIterator")),
        }
    }
}

impl<'a> Iterator for ArraySliceIterator<'a> {
    type Item = Value<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let slice_idx = self.slice_indices.next()?;

        let (start, end) = self.mark_array.offsets.offset_indices(slice_idx).unwrap()?;
        Some(self.mark_array.values.slice(start..end))
    }
}

impl<'a, T> TryFrom<Value<'a>> for Option<T>
where
    T: TryFrom<Value<'a>, Error = Error>,
{
    type Error = Error;

    #[inline(always)]
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

            #[inline(always)]
            fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
                match value {
                    Value::Tuple { index, mark } => {
                        let values = &mark.values;
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
                                        .get(index)
                                        .ok_or(Error::IndexOutOfBounds(
                                            index,
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

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Map { mark_map, index } => {
                // Resolve (start, end) for the requested row in the Map column
                let (start, end) = mark_map
                    .offsets
                    .offset_indices(index)?
                    .ok_or(Error::IndexOutOfBounds(index, "Map"))?;

                Ok(Self {
                    keys: &mark_map.keys,
                    values: &mark_map.values,
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

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::MapSlice {
                mark_map,
                slice_indices,
            } => Ok(Self {
                offsets: &mark_map.offsets,
                keys: &mark_map.keys,
                values: &mark_map.values,
                slice_indices: slice_indices.into(),
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

    #[inline(always)]
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
    mark: &'a MarkTuple<'a>,
    slice_indices: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for TupleSliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::TupleSlice {
                mark,
                slice_indices,
            } => Ok(Self {
                mark,
                slice_indices: slice_indices.into(),
            }),
            other => Err(Error::MismatchedType(other.as_str(), "TupleSliceIterator")),
        }
    }
}

impl<'a> Iterator for TupleSliceIterator<'a> {
    type Item = Value<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let row_idx = self.slice_indices.next()?;
        Some(Value::Tuple {
            index: row_idx,
            mark: self.mark,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.slice_indices.end - self.slice_indices.start;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for TupleSliceIterator<'_> {}

impl<'a> TryFrom<Value<'a>> for Decimal {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal32(index, mark) => Ok(mark.data[index].with_precision(mark.precision)),
            Value::Decimal64(index, mark) => Ok(mark.data[index].with_precision(mark.precision)),
            Value::Decimal128(index, mark) => mark.data[index].with_precision(mark.precision),
            Value::Decimal256(_, _) => Err(Error::NotImplemented(
                "Decimal256 is not yet supported".to_owned(),
            )),
            other => Err(Error::MismatchedType(other.as_str(), "Decimal")),
        }
    }
}

pub struct BoolSliceIterator<'a> {
    data: std::slice::Iter<'a, u8>,
}

impl<'a> TryFrom<Value<'a>> for BoolSliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::BoolSlice(data) => Ok(BoolSliceIterator { data: data.iter() }),
            other => Err(Error::MismatchedType(other.as_str(), "BoolSliceIterator")),
        }
    }
}

impl Iterator for BoolSliceIterator<'_> {
    type Item = bool;

    #[inline(always)]
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

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime32Slice { tz, slice } => Ok(Self {
                tz,
                slice: slice.iter(),
            }),
            other => Err(Error::MismatchedType(
                other.as_str(),
                "DateTime32SliceIterator",
            )),
        }
    }
}

impl Iterator for DateTime32SliceIterator<'_> {
    type Item = chrono::DateTime<Tz>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|dt| dt.with_tz(self.tz))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

pub struct DateTime64SliceIterator<'a> {
    tz: Tz,
    precision: u8,
    slice: std::slice::Iter<'a, DateTime64Data>,
}

impl<'a> TryFrom<Value<'a>> for DateTime64SliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
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
            other => Err(Error::MismatchedType(
                other.as_str(),
                "DateTime64SliceIterator",
            )),
        }
    }
}

impl Iterator for DateTime64SliceIterator<'_> {
    type Item = Option<chrono::DateTime<Tz>>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.slice
            .next()
            .map(|dt| dt.with_tz_and_precision(self.tz, self.precision))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

pub struct NullableSliceIterator<'a> {
    mark_nullable: &'a MarkNullable<'a>,
    slice_indices: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for NullableSliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::NullableSlice {
                mark_nullable,
                slice_indices,
            } => Ok(Self {
                mark_nullable,
                slice_indices: slice_indices.into(),
            }),
            other => Err(Error::MismatchedType(
                other.as_str(),
                "NullableSliceIterator",
            )),
        }
    }
}

impl<'a> Iterator for NullableSliceIterator<'a> {
    type Item = Value<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.slice_indices.next()?;
        if self.mark_nullable.mask.get(index).copied()? == 1 {
            return Some(Value::Empty);
        }
        self.mark_nullable.data.get(index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.slice_indices.end - self.slice_indices.start;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for NullableSliceIterator<'_> {}

pub struct Decimal32SliceIterator<'a> {
    precision: u8,
    slice: std::slice::Iter<'a, Decimal32Data>,
}

impl<'a> TryFrom<Value<'a>> for Decimal32SliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal32Slice { precision, slice } => Ok(Self {
                precision,
                slice: slice.iter(),
            }),
            other => Err(Error::MismatchedType(
                other.as_str(),
                "Decimal32SliceIterator",
            )),
        }
    }
}

impl Iterator for Decimal32SliceIterator<'_> {
    type Item = Decimal;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|v| v.with_precision(self.precision))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

pub struct Decimal64SliceIterator<'a> {
    precision: u8,
    slice: std::slice::Iter<'a, Decimal64Data>,
}

impl<'a> TryFrom<Value<'a>> for Decimal64SliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal64Slice { precision, slice } => Ok(Self {
                precision,
                slice: slice.iter(),
            }),
            other => Err(Error::MismatchedType(
                other.as_str(),
                "Decimal64SliceIterator",
            )),
        }
    }
}

impl Iterator for Decimal64SliceIterator<'_> {
    type Item = Decimal;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|v| v.with_precision(self.precision))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

pub struct Decimal128SliceIterator<'a> {
    precision: u8,
    slice: std::slice::Iter<'a, Decimal128Data>,
}

impl<'a> TryFrom<Value<'a>> for Decimal128SliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Decimal128Slice { precision, slice } => Ok(Self {
                precision,
                slice: slice.iter(),
            }),
            other => Err(Error::MismatchedType(
                other.as_str(),
                "Decimal128SliceIterator",
            )),
        }
    }
}

impl Iterator for Decimal128SliceIterator<'_> {
    type Item = crate::Result<Decimal>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.slice.next().map(|v| v.with_precision(self.precision))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slice.size_hint()
    }
}

pub struct NestedIterator<'a> {
    col_names: &'a [&'a str],
    tuple_slice: TupleSliceIterator<'a>,
}

impl<'a> TryFrom<Value<'a>> for NestedIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Nested { mark_nested, index } => {
                let value = mark_nested
                    .array_of_tuples
                    .get(index)
                    .ok_or(Error::IndexOutOfBounds(index, "Nested"))?;
                let tuple_slice: TupleSliceIterator = value.try_into()?;
                Ok(Self {
                    col_names: &mark_nested.col_names,
                    tuple_slice,
                })
            }
            other => Err(Error::MismatchedType(other.as_str(), "NestedIterator")),
        }
    }
}

impl<'a> Iterator for NestedIterator<'a> {
    type Item = NestedItemsIterator<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.tuple_slice.next()?;
        let Value::Tuple { index: row, mark } = value else {
            return None;
        };

        let mark_iter = mark.values.iter().zip(self.col_names);

        Some(NestedItemsIterator {
            mark_ter: mark_iter,
            row,
        })
    }
}

pub struct NestedItemsIterator<'a> {
    mark_ter: std::iter::Zip<std::slice::Iter<'a, Mark<'a>>, std::slice::Iter<'a, &'a str>>,
    row: usize,
}

impl<'a> Iterator for NestedItemsIterator<'a> {
    type Item = (&'a str, Value<'a>);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let (mark, col_name) = self.mark_ter.next()?;
        let value = mark.get(self.row)?;
        Some((col_name, value))
    }
}

pub struct NestedSliceIterator<'a> {
    col_names: &'a [&'a str],
    array_of_tuples: &'a Mark<'a>,
    slice_indices: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for NestedSliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::NestedSlice {
                mark_nested,
                slice_indices,
            } => Ok(Self {
                col_names: &mark_nested.col_names,
                array_of_tuples: &mark_nested.array_of_tuples,
                slice_indices: slice_indices.into(),
            }),
            other => Err(Error::MismatchedType(other.as_str(), "NestedSliceIterator")),
        }
    }
}

impl<'a> Iterator for NestedSliceIterator<'a> {
    type Item = Result<NestedIterator<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice_idx = self.slice_indices.next()?;
        let Some(val) = self.array_of_tuples.get(slice_idx) else {
            return Some(Err(Error::IndexOutOfBounds(slice_idx, "NestedSlice")));
        };

        let tuple_slice: TupleSliceIterator = match val.try_into() {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        Some(Ok(NestedIterator {
            col_names: self.col_names,
            tuple_slice,
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.slice_indices.end - self.slice_indices.start;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for NestedSliceIterator<'_> {}

pub struct FixedStringSliceIterator<'a> {
    mark_fs: &'a MarkFixedString<'a>,
    slice_indices: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for FixedStringSliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::FixedStringSlice { mark_fs, indices } => Ok(Self {
                mark_fs,
                slice_indices: indices.into(),
            }),
            other => Err(Error::MismatchedType(
                other.as_str(),
                "FixedStringSliceIterator",
            )),
        }
    }
}

impl<'a> Iterator for FixedStringSliceIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let slice_idx = self.slice_indices.next()?;
        let start = slice_idx * self.mark_fs.size;
        let end = start + self.mark_fs.size;

        if end > self.mark_fs.data.len() {
            return None;
        }

        let slice = &self.mark_fs.data[start..end].rtrim_zeros();
        Some(unsafe { std::str::from_utf8_unchecked(slice) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.slice_indices.end - self.slice_indices.start;
        (remaining, Some(remaining))
    }
}

pub struct Enum8SliceIterator<'a> {
    variants: &'a [(&'a str, i8)],
    data: std::slice::Iter<'a, i8>,
}

impl<'a> TryFrom<Value<'a>> for Enum8SliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Enum8Slice {
                mark,
                slice_indices,
            } => {
                let slice_indices: Range<usize> = slice_indices.into();
                let data = &mark.data[slice_indices];
                Ok(Self {
                    variants: &mark.variants,
                    data: data.iter(),
                })
            }
            other => Err(Error::MismatchedType(other.as_str(), "Enum8Iterator")),
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

pub struct Enum16SliceIterator<'a> {
    variants: &'a [(&'a str, i16)],
    data: std::slice::Iter<'a, I16>,
}

impl<'a> TryFrom<Value<'a>> for Enum16SliceIterator<'a> {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::Enum16Slice {
                mark,
                slice_indices,
            } => {
                let slice_indices: Range<usize> = slice_indices.into();
                let data = &mark.data[slice_indices];
                Ok(Self {
                    variants: &mark.variants,
                    data: data.iter(),
                })
            }
            other => Err(Error::MismatchedType(other.as_str(), "Enum16Iterator")),
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
