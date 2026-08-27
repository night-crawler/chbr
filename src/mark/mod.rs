mod json;
pub mod lc;

pub use json::Json;

use crate::{
    Bf16Data, ByteExt as _, Date16Data, Date32Data, DateTime32Data, DateTime64Data, Decimal32Data,
    Decimal64Data, Decimal128Data, Decimal256Data, Error, I256, Ipv4Data, Ipv6Data, U256, UuidData,
    macros::{define_int_getters, define_ip_getters, define_opt_getters, define_slice_fns},
    slice::ByteView,
    types::{OffsetIndexPair as _, Offsets},
    value::{MapIterator, Value},
    zc,
};
use chrono::{DateTime as ChronoDateTime, TimeZone};
use chrono_tz::Tz;
use core::fmt;
use std::{
    fmt::Debug,
    hint::cold_path,
    marker::PhantomData,
    ops::{Deref, Range},
};
use uuid::Uuid;

pub enum Mark<'a> {
    Empty,
    Bool(BoolView<'a>),
    Int8(ByteView<'a, i8>),
    Int16(ByteView<'a, zc::I16>),
    Int32(ByteView<'a, zc::I32>),
    Int64(ByteView<'a, zc::I64>),
    Int128(ByteView<'a, zc::I128>),
    Int256(ByteView<'a, I256>),
    UInt8(ByteView<'a, u8>),
    UInt16(ByteView<'a, zc::U16>),
    UInt32(ByteView<'a, zc::U32>),
    UInt64(ByteView<'a, zc::U64>),
    UInt128(ByteView<'a, zc::U128>),
    UInt256(ByteView<'a, U256>),
    Float32(ByteView<'a, zc::F32>),
    Float64(ByteView<'a, zc::F64>),
    BFloat16(ByteView<'a, Bf16Data>),
    Decimal32(Decimal32<'a>),
    Decimal64(Decimal64<'a>),
    Decimal128(Decimal128<'a>),
    Decimal256(Decimal256<'a>),
    String(StringView<'a>),
    FixedString(FixedString<'a>),
    Uuid(ByteView<'a, UuidData>),
    Date(ByteView<'a, Date16Data>),
    Date32(ByteView<'a, Date32Data>),
    DateTime(DateTime<'a>),
    DateTime64(DateTime64<'a>),
    Ipv4(ByteView<'a, Ipv4Data>),
    Ipv6(ByteView<'a, Ipv6Data>),

    Enum8(Enum8<'a>),
    Enum16(Enum16<'a>),

    LowCardinality(lc::LowCardinality<'a>),
    Array(Array<'a>),
    Tuple(Tuple<'a>),
    Nullable(Nullable<'a>),
    Map(Map<'a>),
    Variant(Variant<'a>),
    Nested(Nested<'a>),
    NamedTuple(NamedTuple<'a>),
    Dynamic(Dynamic<'a>),

    Json(Json<'a>),
}

impl<'a> Mark<'a> {
    pub const fn size(&self) -> Option<usize> {
        #[expect(clippy::match_same_arms)]
        match self {
            Self::Bool(_) => Some(1),
            Self::Int8(_) => Some(1),
            Self::Int16(_) => Some(2),
            Self::Int32(_) => Some(4),
            Self::Int64(_) => Some(8),
            Self::Int128(_) => Some(16),
            Self::Int256(_) => Some(32),
            Self::UInt8(_) => Some(1),
            Self::UInt16(_) => Some(2),
            Self::UInt32(_) => Some(4),
            Self::UInt64(_) => Some(8),
            Self::UInt128(_) => Some(16),
            Self::UInt256(_) => Some(32),

            Self::Float32(_) => Some(4),
            Self::Float64(_) => Some(8),
            Self::BFloat16(_) => Some(2),

            Self::Uuid(_) => Some(16),

            Self::Decimal32(_) => Some(4),
            Self::Decimal64(_) => Some(8),
            Self::Decimal128(_) => Some(16),
            Self::Decimal256(_) => Some(32),

            Self::FixedString(f) => Some(f.size),

            Self::Ipv4(_) => Some(4),
            Self::Ipv6(_) => Some(16),

            Self::Date(_) => Some(2),
            Self::Date32(_) => Some(4),
            Self::DateTime { .. } => Some(4),
            Self::DateTime64 { .. } => Some(8),
            Self::Enum8(_) => Some(1),
            Self::Enum16(_) => Some(2),

            // For completeness, everything below is variable in size
            Self::Map { .. } => None,

            Self::Array(_) => None,

            Self::Tuple(_) => None,

            Self::Variant { .. } => None,
            Self::Dynamic(_) => None,
            Self::Json { .. } => None,

            Self::Nullable(_) => None,
            Self::LowCardinality { .. } => None,
            Self::String(_) => None,
            Self::Nested { .. } => None,
            Self::NamedTuple { .. } => None,
            Self::Empty => None,
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Mark::Empty => "Empty",
            Mark::Bool(_) => "Bool",
            Mark::Int8(_) => "Int8",
            Mark::Int16(_) => "Int16",
            Mark::Int32(_) => "Int32",
            Mark::Int64(_) => "Int64",
            Mark::Int128(_) => "Int128",
            Mark::Int256(_) => "Int256",
            Mark::UInt8(_) => "UInt8",
            Mark::UInt16(_) => "UInt16",
            Mark::UInt32(_) => "UInt32",
            Mark::UInt64(_) => "UInt64",
            Mark::UInt128(_) => "UInt128",
            Mark::UInt256(_) => "UInt256",
            Mark::Float32(_) => "Float32",
            Mark::Float64(_) => "Float64",
            Mark::BFloat16(_) => "BFloat16",
            Mark::Decimal32(_) => "Decimal32",
            Mark::Decimal64(_) => "Decimal64",
            Mark::Decimal128(_) => "Decimal128",
            Mark::Decimal256(_) => "Decimal256",
            Mark::String(_) => "String",
            Mark::FixedString(_) => "FixedString",
            Mark::Uuid(_) => "Uuid",
            Mark::Date(_) => "Date",
            Mark::Date32(_) => "Date32",
            Mark::DateTime(_) => "DateTime",
            Mark::DateTime64(_) => "DateTime64",
            Mark::Ipv4(_) => "Ipv4",
            Mark::Ipv6(_) => "Ipv6",
            Mark::Enum8(_) => "Enum8",
            Mark::Enum16(_) => "Enum16",
            Mark::LowCardinality(_) => "LowCardinality",
            Mark::Array(_) => "Array",
            Mark::Tuple(_) => "Tuple",
            Mark::Nullable(_) => "Nullable",
            Mark::Map(_) => "Map",
            Mark::Variant(_) => "Variant",
            Mark::Nested(_) => "Nested",
            Mark::NamedTuple(_) => "NamedTuple",
            Mark::Dynamic(_) => "Dynamic",
            Mark::Json(_) => "Json",
        }
    }

    pub fn get(&'a self, index: usize) -> crate::Result<Option<Value<'a>>> {
        match self {
            Mark::Empty => Ok(None),
            Mark::Bool(b) => Ok(b.get(index).map(Value::Bool)),
            Mark::Int8(bv) => Ok(bv.get(index).copied().map(Value::Int8)),
            Mark::Int16(bv) => Ok(bv.get(index).map(|v| v.get()).map(Value::Int16)),
            Mark::Int32(bv) => Ok(bv.get(index).map(|v| v.get()).map(Value::Int32)),
            Mark::Int64(bv) => Ok(bv.get(index).map(|v| v.get()).map(Value::Int64)),
            Mark::Int128(bv) => Ok(bv.get(index).map(Value::Int128)),
            Mark::Int256(bv) => Ok(bv.get(index).map(Value::Int256)),
            Mark::UInt8(bv) => Ok(bv.get(index).copied().map(Value::UInt8)),
            Mark::UInt16(bv) => Ok(bv.get(index).map(|v| v.get()).map(Value::UInt16)),
            Mark::UInt32(bv) => Ok(bv.get(index).map(|v| v.get()).map(Value::UInt32)),
            Mark::UInt64(bv) => Ok(bv.get(index).map(|v| v.get()).map(Value::UInt64)),
            Mark::UInt128(bv) => Ok(bv.get(index).map(Value::UInt128)),
            Mark::UInt256(bv) => Ok(bv.get(index).map(Value::UInt256)),
            Mark::Float32(bv) => Ok(bv.get(index).map(|v| v.get()).map(Value::Float32)),
            Mark::Float64(bv) => Ok(bv.get(index).map(|v| v.get()).map(Value::Float64)),
            Mark::BFloat16(bv) => Ok(bv.get(index).copied().map(Into::into).map(Value::BFloat16)),
            Mark::Decimal32(d) => Ok(d.get(index)),
            Mark::Decimal64(d) => Ok(d.get(index)),
            Mark::Decimal128(d) => Ok(d.get(index)),
            Mark::Decimal256(d) => Ok(d.get(index)),
            Mark::String(strings) => Ok(strings.get(index).map(Value::String)),
            Mark::FixedString(fs) => Ok(fs.get(index)),
            Mark::Uuid(bv) => Ok(bv.get(index).map(Value::Uuid)),
            Mark::Date(bv) => Ok(bv.get(index).copied().map(Into::into).map(Value::Date)),
            Mark::Date32(bv) => Ok(bv.get(index).copied().map(Into::into).map(Value::Date32)),
            Mark::DateTime(d) => Ok(d.get(index)),
            Mark::DateTime64(d) => Ok(d.get(index)),
            Mark::Ipv4(data) => Ok(data.get(index).copied().map(Into::into).map(Value::Ipv4)),
            Mark::Ipv6(data) => Ok(data.get(index).map(Value::Ipv6)),
            Mark::Enum8(v) => Ok(v.get(index)),
            Mark::Enum16(v) => Ok(v.get(index)),
            Mark::LowCardinality(lc) => lc.get(index),
            Mark::Array(a) => a.get(index),
            Mark::Tuple(mark) => Ok(Some(Value::Tuple { mark, index })),
            Mark::Nullable(n) => n.get(index),
            Mark::Map(mark) => Ok(Some(Value::Map { mark, index })),
            Mark::Variant(v) => v.get(index),
            Mark::Nested(n) => n.get(index),
            Mark::NamedTuple(n) => n.get(index),
            Mark::Dynamic(d) => d.get(index),
            Mark::Json(j) => Ok(j.get(index)),
        }
    }

    pub fn slice(&'a self, idx: Range<usize>) -> crate::Result<Value<'a>> {
        match self {
            Mark::Empty => {
                if !idx.is_empty() {
                    cold_path();
                    return Err(Error::IndexOutOfBounds(idx.end, "Empty"));
                }
                Ok(Value::Empty)
            }
            Mark::Bool(bv) => Ok(Value::BoolSlice(checked_slice(bv.data, idx, "Bool")?)),
            Mark::Int8(bv) => Ok(Value::Int8Slice(checked_slice(bv.as_slice(), idx, "Int8")?)),
            Mark::Int16(bv) => Ok(Value::Int16Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Int16",
            )?)),
            Mark::Int32(bv) => Ok(Value::Int32Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Int32",
            )?)),
            Mark::Int64(bv) => Ok(Value::Int64Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Int64",
            )?)),
            Mark::Int128(bv) => Ok(Value::Int128Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Int128",
            )?)),
            Mark::Int256(bv) => Ok(Value::Int256Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Int256",
            )?)),
            Mark::UInt8(bv) => Ok(Value::UInt8Slice(checked_slice(
                bv.as_slice(),
                idx,
                "UInt8",
            )?)),
            Mark::UInt16(bv) => Ok(Value::UInt16Slice(checked_slice(
                bv.as_slice(),
                idx,
                "UInt16",
            )?)),
            Mark::UInt32(bv) => Ok(Value::UInt32Slice(checked_slice(
                bv.as_slice(),
                idx,
                "UInt32",
            )?)),
            Mark::UInt64(bv) => Ok(Value::UInt64Slice(checked_slice(
                bv.as_slice(),
                idx,
                "UInt64",
            )?)),
            Mark::UInt128(bv) => Ok(Value::UInt128Slice(checked_slice(
                bv.as_slice(),
                idx,
                "UInt128",
            )?)),
            Mark::UInt256(bv) => Ok(Value::UInt256Slice(checked_slice(
                bv.as_slice(),
                idx,
                "UInt256",
            )?)),
            Mark::Float32(bv) => Ok(Value::Float32Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Float32",
            )?)),
            Mark::Float64(bv) => Ok(Value::Float64Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Float64",
            )?)),
            Mark::BFloat16(bv) => Ok(Value::BFloat16Slice(checked_slice(
                bv.as_slice(),
                idx,
                "BFloat16",
            )?)),
            Mark::Uuid(bv) => Ok(Value::UuidSlice(checked_slice(bv.as_slice(), idx, "Uuid")?)),
            Mark::Date(bv) => Ok(Value::Date16Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Date",
            )?)),
            Mark::Date32(bv) => Ok(Value::Date32Slice(checked_slice(
                bv.as_slice(),
                idx,
                "Date32",
            )?)),
            Mark::Ipv4(bv) => Ok(Value::Ipv4Slice(checked_slice(bv.as_slice(), idx, "Ipv4")?)),
            Mark::Ipv6(bv) => Ok(Value::Ipv6Slice(checked_slice(bv.as_slice(), idx, "Ipv6")?)),
            Mark::String(sv) => Ok(Value::StringSlice(checked_slice(
                sv.data.as_slice(),
                idx,
                "String",
            )?)),

            Mark::Decimal32(d) => Ok(Value::Decimal32Slice {
                precision: d.precision,
                slice: checked_slice(d.data.as_slice(), idx, "Decimal32")?,
            }),
            Mark::Decimal64(d) => Ok(Value::Decimal64Slice {
                precision: d.precision,
                slice: checked_slice(d.data.as_slice(), idx, "Decimal64")?,
            }),
            Mark::Decimal128(d) => Ok(Value::Decimal128Slice {
                precision: d.precision,
                slice: checked_slice(d.data.as_slice(), idx, "Decimal128")?,
            }),
            Mark::Decimal256(d) => Ok(Value::Decimal256Slice {
                precision: d.precision,
                slice: checked_slice(d.data.as_slice(), idx, "Decimal256")?,
            }),
            Mark::FixedString(mark) => Ok(Value::FixedStringSlice {
                mark,
                range: idx.try_into()?,
            }),

            Mark::DateTime(d) => Ok(Value::DateTime32Slice {
                tz: d.tz,
                slice: checked_slice(d.data.as_slice(), idx, "DateTime")?,
            }),
            Mark::DateTime64(d) => Ok(Value::DateTime64Slice {
                precision: d.precision,
                tz: d.tz,
                slice: checked_slice(d.data.as_slice(), idx, "DateTime64")?,
            }),
            Mark::Enum8(mark) => Ok(Value::Enum8Slice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::Enum16(mark) => Ok(Value::Enum16Slice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::LowCardinality(mark) => Ok(Value::LowCardinalitySlice {
                range: idx.try_into()?,
                mark,
            }),
            Mark::Array(mark) => Ok(Value::ArraySlice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::Tuple(mark) => Ok(Value::TupleSlice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::Nullable(mark) => Ok(Value::NullableSlice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::Map(mark) => Ok(Value::MapSlice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::Nested(mark) => Ok(Value::NestedSlice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::NamedTuple(mark) => Ok(Value::NamedTupleSlice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::Variant(mark) => Ok(Value::VariantSlice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::Dynamic(mark) => Ok(Value::DynamicSlice {
                mark,
                range: idx.try_into()?,
            }),
            Mark::Json(mark) => mark.slice(idx),
        }
    }

    #[inline]
    pub fn get_str(&'a self, index: usize) -> crate::Result<Option<&'a str>> {
        match self {
            Mark::String(strings) => Ok(strings.get(index)),
            Mark::FixedString(fs) => Ok(fs.get_str(index)),
            Mark::LowCardinality(lc) => lc.get_str(index),
            mark => {
                cold_path();
                Err(Error::MismatchedType(mark.as_str(), "&str"))
            }
        }
    }

    #[inline]
    pub fn get_opt_str(&'a self, index: usize) -> crate::Result<Option<Option<&'a str>>> {
        let Mark::Nullable(Nullable { mask, data }) = self else {
            // convenience wrapper
            let value = self.get_str(index)?;
            return Ok(Some(value));
        };

        if mask.get(index) == Some(&1) {
            return Ok(Some(None));
        }

        Ok(Some(data.get_str(index)?))
    }

    #[expect(clippy::needless_pass_by_value)]
    #[inline]
    pub fn get_datetime<T: TimeZone>(
        &'a self,
        index: usize,
        tz: T,
    ) -> crate::Result<Option<ChronoDateTime<T>>> {
        match self {
            Mark::DateTime(d) => {
                let value = d
                    .data
                    .get(index)
                    .map(|dt| dt.with_tz(d.tz))
                    .map(|dt| dt.with_timezone(&tz));
                Ok(value)
            }
            Mark::DateTime64(d) => {
                let Some(dt) = d.data.get(index) else {
                    return Ok(None);
                };
                let Some(dt) = dt.with_tz_and_precision(d.tz, d.precision) else {
                    cold_path();
                    return Err(Error::Overflow("DateTime64 value out of range".to_owned()));
                };
                Ok(Some(dt.with_timezone(&tz)))
            }
            _ => {
                cold_path();
                Err(Error::MismatchedType(self.as_str(), "DateTime"))
            }
        }
    }

    #[inline]
    pub fn slice_lc_strs(&'a self, idx: Range<usize>) -> crate::Result<lc::StrIter<'a>> {
        let Mark::LowCardinality(lc) = self else {
            cold_path();
            return Err(Error::MismatchedType(self.as_str(), "LowCardinality"));
        };

        let Some(keys) = &lc.additional_keys else {
            cold_path();
            return Err(Error::CorruptedData(
                "LowCardinality marker without additional keys".to_owned(),
            ));
        };

        let Mark::String(keys) = keys.as_ref() else {
            cold_path();
            return Err(Error::MismatchedType(keys.as_str(), "String"));
        };

        let indices = lc.indices.iter(idx)?;

        Ok(lc::StrIter { indices, keys })
    }

    #[inline]
    pub fn get_array_lc_strs(
        &'a self,
        index: usize,
    ) -> crate::Result<Option<impl Iterator<Item = crate::Result<&'a str>>>> {
        if matches!(self, Mark::Empty) {
            cold_path();
            return Ok(None);
        }

        let Mark::Array(array) = self else {
            cold_path();
            return Err(Error::MismatchedType(self.as_str(), "Array"));
        };

        let Some((start, end)) = array.offsets.offset_indices(index)? else {
            return Ok(None);
        };

        if matches!(array.values.as_ref(), Mark::Empty) {
            return Ok(Some(ArrayLcStrIter { inner: None }));
        }

        let it = array.values.slice_lc_strs(start..end)?;
        Ok(Some(ArrayLcStrIter { inner: Some(it) }))
    }

    #[inline]
    pub fn get_map<K, V>(&'a self, index: usize) -> crate::Result<Option<MapIterator<'a, K, V>>> {
        let Mark::Map(map) = self else {
            cold_path();
            return Err(Error::MismatchedType(self.as_str(), "Map"));
        };
        let Some((start, end)) = map.offsets.offset_indices(index)? else {
            return Ok(None);
        };

        let it = MapIterator {
            keys: &map.keys,
            values: &map.values,
            range: start..end,
            _marker: PhantomData,
        };

        Ok(Some(it))
    }

    #[inline]
    pub fn get_arr_bool_iter(
        &'a self,
        index: usize,
    ) -> crate::Result<Option<impl Iterator<Item = bool>>> {
        let Mark::Array(arr) = self else {
            cold_path();
            return Err(Error::MismatchedType(self.as_str(), "Array"));
        };

        let Some((start, end)) = arr.offsets.offset_indices(index)? else {
            return Ok(None);
        };

        let slice = match arr.values.as_ref() {
            Mark::Bool(bv) => &bv.data[start..end],
            Mark::Empty => &[],
            other => {
                cold_path();
                return Err(Error::MismatchedType(other.as_str(), "Int8"));
            }
        };

        Ok(Some(slice.iter().copied().map(|b| b != 0)))
    }
    #[inline]
    pub fn get_bool(&'a self, index: usize) -> crate::Result<Option<bool>> {
        match self {
            Mark::Bool(bv) => {
                let value = bv.get(index);
                Ok(value)
            }
            _ => {
                cold_path();
                Err(Error::MismatchedType(self.as_str(), "bool"))
            }
        }
    }

    define_ip_getters!((Ipv4, std::net::Ipv4Addr), (Ipv6, std::net::Ipv6Addr));

    define_int_getters!(
        (Int8, i8, std::convert::identity),
        (Int16, i16, zc::I16::get),
        (Int32, i32, zc::I32::get),
        (Int64, i64, zc::I64::get),
        (Int128, i128, zc::I128::get),
        (UInt8, u8, std::convert::identity),
        (UInt16, u16, zc::U16::get),
        (UInt32, u32, zc::U32::get),
        (UInt64, u64, zc::U64::get),
        (UInt128, u128, zc::U128::get),
        (Float32, f32, zc::F32::get),
        (Float64, f64, zc::F64::get),
        (Uuid, Uuid, Uuid::from),
    );

    define_opt_getters!(
        (Ipv4, std::net::Ipv4Addr),
        (Ipv6, std::net::Ipv6Addr),
        (Uuid, Uuid),
        (i8, i8),
        (i16, i16),
        (i32, i32),
        (i64, i64),
        (i128, i128),
        (u8, u8),
        (u16, u16),
        (u32, u32),
        (u64, u64),
        (u128, u128),
        (f64, f64),
        (f32, f32),
    );

    define_slice_fns!(
        (Int8, i8),
        (Int16, zc::I16),
        (Int32, zc::I32),
        (Int64, zc::I64),
        (Int128, zc::I128),
        (Int256, I256),
        (UInt8, u8),
        (UInt16, zc::U16),
        (UInt32, zc::U32),
        (UInt64, zc::U64),
        (UInt128, zc::U128),
        (UInt256, U256),
        (Float32, zc::F32),
        (Float64, zc::F64),
        (BFloat16, Bf16Data),
        (String, &'a str),
        (Uuid, UuidData),
        (Date, Date16Data),
        (Date32, Date32Data),
        (Ipv4, Ipv4Data),
        (Ipv6, Ipv6Data),
    );
}

#[inline]
fn checked_slice<'a, T>(
    data: &'a [T],
    range: Range<usize>,
    kind: &'static str,
) -> crate::Result<&'a [T]> {
    let end = range.end;
    let Some(slice) = data.get(range) else {
        cold_path();
        return Err(Error::IndexOutOfBounds(end, kind));
    };
    Ok(slice)
}

macro_rules! impl_get {
    ($ty:ident, $variant:ident) => {
        impl<'a> $ty<'a> {
            #[inline]
            pub const fn get(&'a self, index: usize) -> Option<Value<'a>> {
                if self.data.len() <= index {
                    None
                } else {
                    Some(Value::$variant(index, self))
                }
            }
        }
    };
}

macro_rules! impl_get_many {
    ($($ty:ident),+ $(,)?) => {
        $( impl_get!($ty, $ty); )+
    };
}

#[derive(Debug)]
pub struct Map<'a> {
    pub offsets: Offsets<'a>,
    pub keys: Box<Mark<'a>>,
    pub values: Box<Mark<'a>>,
}

#[derive(Debug)]
pub struct Variant<'a> {
    pub offsets: Vec<usize>,
    pub discriminators: &'a [u8],
    pub types: Vec<Mark<'a>>,
}

impl Variant<'_> {
    /// Discriminator byte marking a NULL row.
    pub const NULL_DISCRIMINATOR: u8 = 255;

    #[inline]
    pub fn get(&self, index: usize) -> crate::Result<Option<Value<'_>>> {
        let Some(&discriminator) = self.discriminators.get(index) else {
            return Ok(None);
        };
        let Some(&in_type_index) = self.offsets.get(index) else {
            return Ok(None);
        };
        let Some(mark) = self.types.get(discriminator as usize) else {
            return Ok(None);
        };
        mark.get(in_type_index)
    }
}

#[derive(Debug)]
pub struct Nested<'a> {
    pub col_names: Vec<&'a str>,
    pub array_of_tuples: Box<Mark<'a>>,
}

impl Nested<'_> {
    #[inline]
    pub fn get(&self, index: usize) -> crate::Result<Option<Value<'_>>> {
        // verify the index is present
        if self.array_of_tuples.get(index)?.is_none() {
            return Ok(None);
        }
        Ok(Some(Value::Nested { mark: self, index }))
    }
}

#[derive(Debug)]
pub struct NamedTuple<'a> {
    pub col_names: Vec<&'a str>,
    pub tuple: Box<Mark<'a>>,
}

impl<'a> NamedTuple<'a> {
    #[inline]
    pub fn mark(&self, name: &str) -> crate::Result<&Mark<'a>> {
        let Mark::Tuple(tuple) = self.tuple.as_ref() else {
            cold_path();
            return Err(Error::MismatchedType(self.tuple.as_str(), "Tuple"));
        };
        crate::mark_by_name(&self.col_names, &tuple.values, name)
    }

    #[inline]
    pub fn get(&self, index: usize) -> crate::Result<Option<Value<'_>>> {
        if self.tuple.get(index)?.is_none() {
            return Ok(None);
        }
        Ok(Some(Value::NamedTuple { mark: self, index }))
    }
}

#[derive(Debug)]
pub struct Array<'a> {
    pub offsets: Offsets<'a>,
    pub values: Box<Mark<'a>>,
}

impl Array<'_> {
    #[inline]
    pub fn get(&self, index: usize) -> crate::Result<Option<Value<'_>>> {
        let Some((start, end)) = self.offsets.offset_indices(index)? else {
            return Ok(None);
        };
        Ok(Some(self.values.slice(start..end)?))
    }
}

#[derive(Debug)]
pub struct Decimal32<'a> {
    pub precision: u8,
    pub data: ByteView<'a, Decimal32Data>,
}

#[derive(Debug)]
pub struct Decimal64<'a> {
    pub precision: u8,
    pub data: ByteView<'a, Decimal64Data>,
}

#[derive(Debug)]
pub struct Decimal128<'a> {
    pub precision: u8,
    pub data: ByteView<'a, Decimal128Data>,
}

#[derive(Debug)]
pub struct Decimal256<'a> {
    pub precision: u8,
    pub data: ByteView<'a, Decimal256Data>,
}

#[derive(Debug)]
pub struct FixedString<'a> {
    pub size: usize,
    pub data: &'a [u8],
}

impl<'a> FixedString<'a> {
    #[inline]
    pub(crate) fn get_str(&self, index: usize) -> Option<&'a str> {
        let offset = self.size.checked_mul(index)?;
        let end = offset.checked_add(self.size)?;
        let slice = self.data.get(offset..end)?.rtrim_zeros();
        Some(unsafe { std::str::from_utf8_unchecked(slice) })
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<Value<'a>> {
        self.get_str(index).map(Value::String)
    }
}

#[derive(Debug)]
pub struct DateTime<'a> {
    pub tz: Tz,
    pub data: ByteView<'a, DateTime32Data>,
}

#[derive(Debug)]
pub struct DateTime64<'a> {
    pub precision: u8,
    pub tz: Tz,
    pub data: ByteView<'a, DateTime64Data>,
}

impl_get_many!(
    Decimal32, Decimal64, Decimal128, Decimal256, DateTime, DateTime64
);

#[derive(Debug)]
pub struct Enum8<'a> {
    pub variants: Vec<(&'a str, i8)>,
    pub data: ByteView<'a, i8>,
}

impl Enum8<'_> {
    #[inline]
    pub fn get(&self, index: usize) -> Option<Value<'_>> {
        let variant = *self.data.get(index)?;
        if let Ok(index) = self.variants.binary_search_by_key(&variant, |(_, id)| *id) {
            return Some(Value::String(self.variants[index].0));
        }
        // actually, at this point it's broken, but we trust clickhouse!
        None
    }
}

#[derive(Debug)]
pub struct Enum16<'a> {
    pub variants: Vec<(&'a str, i16)>,
    pub data: ByteView<'a, zc::I16>,
}

impl Enum16<'_> {
    #[inline]
    pub fn get(&self, index: usize) -> Option<Value<'_>> {
        let variant = self.data.get(index)?.get();
        if let Ok(index) = self.variants.binary_search_by_key(&variant, |(_, id)| *id) {
            return Some(Value::String(self.variants[index].0));
        }
        None
    }
}

#[derive(Debug)]
pub struct Dynamic<'a> {
    pub offsets: Vec<usize>,
    pub discriminators: Vec<usize>,
    pub columns: Vec<Mark<'a>>,
}

impl Dynamic<'_> {
    #[inline]
    pub fn get(&self, index: usize) -> crate::Result<Option<Value<'_>>> {
        let Some(&discriminator) = self.discriminators.get(index) else {
            return Ok(None);
        };
        let Some(&in_type_index) = self.offsets.get(index) else {
            return Ok(None);
        };
        let Some(mark) = self.columns.get(discriminator) else {
            return Ok(None);
        };
        mark.get(in_type_index)
    }
}

#[derive(Debug)]
pub struct Nullable<'a> {
    pub mask: &'a [u8],
    pub data: Box<Mark<'a>>,
}

impl Nullable<'_> {
    #[inline]
    pub fn get(&self, index: usize) -> crate::Result<Option<Value<'_>>> {
        if self.mask.get(index) == Some(&1) {
            return Ok(Some(Value::Empty));
        }

        self.data.get(index)
    }
}

#[derive(Debug)]
pub struct Tuple<'a> {
    pub values: Vec<Mark<'a>>,
}

#[derive(Debug)]
pub struct StringView<'a> {
    pub data: Vec<&'a str>,
}

impl<'a> Deref for StringView<'a> {
    type Target = [&'a str];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl StringView<'_> {
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&str> {
        self.data.get(index).copied()
    }
}

#[derive(Debug)]
pub struct BoolView<'a> {
    pub data: &'a [u8],
}

impl BoolView<'_> {
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<bool> {
        self.data.get(index).map(|&val| val == 1)
    }
}

struct ArrayLcStrIter<'a> {
    inner: Option<lc::StrIter<'a>>,
}

impl<'a> Iterator for ArrayLcStrIter<'a> {
    type Item = crate::Result<&'a str>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.as_mut()?.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            Some(it) => it.size_hint(),
            None => (0, Some(0)),
        }
    }
}

impl ExactSizeIterator for ArrayLcStrIter<'_> {}

impl Debug for Mark<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn dbg_slice(f: &mut fmt::Formatter<'_>, name: &str, bytes: &[u8]) -> fmt::Result {
            f.debug_struct(name)
                .field("len_bytes", &bytes.len())
                .field("ptr", &bytes.as_ptr())
                .finish()
        }
        fn dbg_bv<T: zc::Unaligned + zc::FromBytes + Copy + Debug>(
            f: &mut fmt::Formatter<'_>,
            name: &str,
            bv: &ByteView<'_, T>,
        ) -> fmt::Result {
            let bytes = bv.as_bytes();
            f.debug_struct(name)
                .field("len", &bytes.len())
                .field("data", &bv.as_slice())
                .finish()
        }
        use Mark::{
            Array, BFloat16, Bool, Date, Date32, DateTime, DateTime64, Decimal32, Decimal64,
            Decimal128, Decimal256, Dynamic, Empty, Enum8, Enum16, FixedString, Float32, Float64,
            Int8, Int16, Int32, Int64, Int128, Int256, Ipv4, Ipv6, Json, LowCardinality, Map,
            NamedTuple, Nested, Nullable, String, Tuple, UInt8, UInt16, UInt32, UInt64, UInt128,
            UInt256, Uuid, Variant,
        };
        match self {
            Empty => f.write_str("Empty"),

            Bool(b) => dbg_slice(
                f,
                core::any::type_name::<Self>()
                    .rsplit("::")
                    .next()
                    .expect("bug: must have at least one element always"),
                b.data,
            ),

            Ipv4(v) => dbg_bv(f, "Ipv4", v),
            Ipv6(v) => dbg_bv(f, "Ipv6", v),
            Date32(v) => dbg_bv(f, "Date32", v),
            Date(v) => dbg_bv(f, "Date", v),
            Uuid(v) => dbg_bv(f, "Uuid", v),
            Int8(v) => dbg_bv(f, "Int8", v),
            Int16(v) => dbg_bv(f, "Int16", v),
            Int32(v) => dbg_bv(f, "Int32", v),
            Int64(v) => dbg_bv(f, "Int64", v),
            Int128(v) => dbg_bv(f, "Int128", v),
            Int256(v) => dbg_bv(f, "Int256", v),
            UInt8(v) => dbg_bv(f, "UInt8", v),
            UInt16(v) => dbg_bv(f, "UInt16", v),
            UInt32(v) => dbg_bv(f, "UInt32", v),
            UInt64(v) => dbg_bv(f, "UInt64", v),
            UInt128(v) => dbg_bv(f, "UInt128", v),
            UInt256(v) => dbg_bv(f, "UInt256", v),
            Float32(v) => dbg_bv(f, "Float32", v),
            Float64(v) => dbg_bv(f, "Float64", v),
            BFloat16(v) => dbg_bv(f, "BFloat16", v),

            Decimal32(d) => f
                .debug_struct("Decimal32")
                .field("scale", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),
            Decimal64(d) => f
                .debug_struct("Decimal64")
                .field("scale", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),
            Decimal128(d) => f
                .debug_struct("Decimal128")
                .field("scale", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),
            Decimal256(d) => f
                .debug_struct("Decimal256")
                .field("scale", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),

            String(data) => f.debug_struct("String").field("data", data).finish(),
            FixedString(ff) => f
                .debug_struct("FixedString")
                .field("fixed_len", &ff.size)
                .field("data", &ff.data)
                .finish(),

            DateTime(d) => f
                .debug_struct("DateTime")
                .field("tz", &d.tz)
                .field("data", &d.data.as_slice())
                .finish(),
            DateTime64(d) => f
                .debug_struct("DateTime64")
                .field("tz", &d.tz)
                .field("precision", &d.precision)
                .field("data", &d.data.as_slice())
                .finish(),

            Enum8(e) => f
                .debug_struct("Enum8")
                .field("data", &e.data.as_slice())
                .finish(),
            Enum16(e) => f.debug_struct("Enum16").field("map", &e).finish(),

            LowCardinality(lc) => f
                .debug_struct("LowCardinality")
                .field("indices", &lc.indices)
                .field("global_dictionary", &lc.global_dictionary)
                .field("additional_keys", &lc.additional_keys)
                .finish(),

            Array(a) => f
                .debug_struct("Array")
                .field("offsets_len", &a.offsets.len())
                .field("values", &a.values)
                .finish(),

            Tuple(items) => f.debug_tuple("Tuple").field(items).finish(),

            Nullable(n) => f.debug_struct("Nullable").field("data", n).finish(),

            Map(m) => f
                .debug_struct("Map")
                .field("offsets_len", &m.offsets.len())
                .field("keys", &m.keys)
                .field("values", &m.values)
                .finish(),

            Variant(v) => f
                .debug_struct("Variant")
                .field("disc_bytes", &v.discriminators.len())
                .field("types", &v.types)
                .field("offsets", &v.offsets)
                .finish(),

            Nested(n) => f
                .debug_struct("Nested")
                .field("col_names", &n.col_names)
                .field("array_of_tuples", &n.array_of_tuples)
                .finish(),

            NamedTuple(n) => f
                .debug_struct("NamedTuple")
                .field("col_names", &n.col_names)
                .field("tuple", &n.tuple)
                .finish(),

            Dynamic(d) => f
                .debug_struct("Dynamic")
                .field("discriminators", &d.discriminators)
                .field("columns", &d.columns)
                .finish(),

            Json(j) => f
                .debug_struct("Json")
                .field("paths", &j.paths)
                .field("headers", &j.headers)
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests;
