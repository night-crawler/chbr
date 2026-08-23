use std::hint::cold_path;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::ops::Range;

use chrono::NaiveDate;
use chrono_tz::Tz;
use rust_decimal::Decimal;
use uuid::Uuid;
use zerocopy::little_endian::{F32, F64, I16, I32, I64, I128, U16, U32, U64, U128};

use super::{ReadSlice, Readable, TryRead};
use crate::error::Error;
use crate::mark::{
    BoolView, DateTime as DateTimeMark, DateTime64 as DateTime64Mark, Decimal32 as Decimal32Mark,
    Decimal64 as Decimal64Mark, Decimal128 as Decimal128Mark, Enum8 as Enum8Mark,
    Enum16 as Enum16Mark, FixedString as FixedStringMark, Mark, StringView,
};
use crate::slice::ByteView;
use crate::value::Value;
use crate::{
    Bf16Data, ByteExt as _, Date16Data, Date32Data, I256, Ipv4Data, Ipv6Data, U256, UuidData,
};

macro_rules! col_view {
    ($($name:ident, $variant:ident, $elem:ty, $item:ty, |$v:ident| $conv:expr;)+) => {
        $(
            #[derive(Clone, Copy)]
            pub struct $name<'a>(pub &'a ByteView<'a, $elem>);

            impl<'a> TryFrom<&'a Mark<'a>> for $name<'a> {
                type Error = Error;

                #[inline]
                fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
                    match value {
                        Mark::$variant(v) => Ok(Self(v)),
                        other => {
                            cold_path();
                            Err(Error::MismatchedType(other.as_str(), stringify!($variant)))
                        }
                    }
                }
            }

            impl<'a> TryRead<'a> for $name<'a> {
                type Item = $item;

                #[inline(always)]
                fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
                    let Some($v) = self.0.as_slice().get(idx) else {
                        cold_path();
                        return Err(Error::IndexOutOfBounds(idx, stringify!($variant)));
                    };
                    Ok($conv)
                }
            }

            impl<'a> ReadSlice<'a> for $name<'a> {
                type Elem = $elem;

                #[inline(always)]
                fn try_read_slice(
                    &self,
                    range: Range<usize>,
                ) -> crate::Result<&'a [Self::Elem]> {
                    let end = range.end;
                    let Some(slice) = self.0.as_slice().get(range) else {
                        cold_path();
                        return Err(Error::IndexOutOfBounds(end, stringify!($variant)));
                    };
                    Ok(slice)
                }
            }
        )+
    };
}

col_view! {
    ColI8, Int8, i8, i8, |v| *v;
    ColI16, Int16, I16, i16, |v| v.get();
    ColI32, Int32, I32, i32, |v| v.get();
    ColI64, Int64, I64, i64, |v| v.get();
    ColI128, Int128, I128, i128, |v| v.get();
    ColI256, Int256, I256, &'a I256, |v| v;
    ColU8, UInt8, u8, u8, |v| *v;
    ColU16, UInt16, U16, u16, |v| v.get();
    ColU32, UInt32, U32, u32, |v| v.get();
    ColU64, UInt64, U64, u64, |v| v.get();
    ColU128, UInt128, U128, u128, |v| v.get();
    ColU256, UInt256, U256, &'a U256, |v| v;
    ColF32, Float32, F32, f32, |v| v.get();
    ColF64, Float64, F64, f64, |v| v.get();
    ColBf16, BFloat16, Bf16Data, half::bf16, |v| half::bf16::from(*v);
    ColUuid, Uuid, UuidData, Uuid, |v| Uuid::from(*v);
    ColIpv4, Ipv4, Ipv4Data, Ipv4Addr, |v| Ipv4Addr::from(*v);
    ColIpv6, Ipv6, Ipv6Data, Ipv6Addr, |v| Ipv6Addr::from(*v);
    ColDate, Date, Date16Data, NaiveDate, |v| NaiveDate::from(*v);
    ColDate32, Date32, Date32Data, NaiveDate, |v| NaiveDate::from(*v);
}

#[derive(Clone, Copy)]
pub struct ColUsize<'a>(pub &'a Mark<'a>);

impl<'a> TryRead<'a> for ColUsize<'a> {
    type Item = usize;
    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let value = match self.0 {
            Mark::UInt8(v) => v.get(idx).map(|v| usize::from(*v)),
            Mark::UInt16(v) => v.get(idx).map(|v| v.get() as usize),
            Mark::UInt32(v) => v.get(idx).map(|v| v.get() as usize),
            Mark::UInt64(v) => match v.get(idx) {
                Some(v) => Some(usize::try_from(v.get())?),
                None => None,
            },
            _ => {
                cold_path();
                unreachable!("unsupported index type for usize")
            }
        };
        let Some(value) = value else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "UInt8/16/32/64"));
        };
        Ok(value)
    }
}

impl<'a> TryFrom<&'a Mark<'a>> for ColUsize<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::UInt8(_) | Mark::UInt16(_) | Mark::UInt32(_) | Mark::UInt64(_) => {
                Ok(ColUsize(value))
            }
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "UInt8/16/32/64"))
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct ColBool<'a>(pub &'a BoolView<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for ColBool<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Bool(v) => Ok(Self(v)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "Bool"))
            }
        }
    }
}

impl<'a> TryRead<'a> for ColBool<'a> {
    type Item = bool;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "Bool"));
        };
        Ok(value)
    }
}

impl<'a> ReadSlice<'a> for ColBool<'a> {
    /// Raw mask bytes; `1` is `true`.
    type Elem = u8;

    #[inline(always)]
    fn try_read_slice(&self, range: Range<usize>) -> crate::Result<&'a [Self::Elem]> {
        let end = range.end;
        let Some(slice) = self.0.data.get(range) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(end, "Bool"));
        };
        Ok(slice)
    }
}

#[derive(Clone, Copy)]
pub struct ColStr<'a>(pub &'a StringView<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for ColStr<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::String(s) => Ok(ColStr(s)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "String"))
            }
        }
    }
}

impl<'a> TryRead<'a> for ColStr<'a> {
    type Item = &'a str;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "String"));
        };
        Ok(value)
    }
}

impl<'a> ReadSlice<'a> for ColStr<'a> {
    type Elem = &'a str;

    #[inline(always)]
    fn try_read_slice(&self, range: Range<usize>) -> crate::Result<&'a [Self::Elem]> {
        let end = range.end;
        let Some(slice) = self.0.data.get(range) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(end, "String"));
        };
        Ok(slice)
    }
}

#[derive(Clone, Copy)]
pub struct ColFixedStr<'a>(pub &'a FixedStringMark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for ColFixedStr<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::FixedString(fs) => Ok(Self(fs)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "FixedString"))
            }
        }
    }
}

impl<'a> TryRead<'a> for ColFixedStr<'a> {
    type Item = &'a str;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let data: &'a [u8] = self.0.data;
        let offset = self.0.size * idx;
        let Some(bytes) = data.get(offset..offset + self.0.size) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "FixedString"));
        };
        let bytes = bytes.rtrim_zeros();
        Ok(unsafe { std::str::from_utf8_unchecked(bytes) })
    }
}

#[derive(Clone, Copy)]
pub struct ColEnum8<'a>(pub &'a Enum8Mark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for ColEnum8<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Enum8(e) => Ok(Self(e)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "Enum8"))
            }
        }
    }
}

impl<'a> TryRead<'a> for ColEnum8<'a> {
    type Item = &'a str;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(&variant) = self.0.data.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "Enum8"));
        };
        let pos = self
            .0
            .variants
            .binary_search_by_key(&variant, |(_, id)| *id)
            .map_err(|_| {
                cold_path();
                Error::CorruptedData(format!("invalid Enum8 discriminant: {variant}"))
            })?;
        Ok(self.0.variants[pos].0)
    }
}

#[derive(Clone, Copy)]
pub struct ColEnum16<'a>(pub &'a Enum16Mark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for ColEnum16<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Enum16(e) => Ok(Self(e)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "Enum16"))
            }
        }
    }
}

impl<'a> TryRead<'a> for ColEnum16<'a> {
    type Item = &'a str;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(variant) = self.0.data.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "Enum16"));
        };
        let variant = variant.get();
        let pos = self
            .0
            .variants
            .binary_search_by_key(&variant, |(_, id)| *id)
            .map_err(|_| {
                cold_path();
                Error::CorruptedData(format!("invalid Enum16 discriminant: {variant}"))
            })?;
        Ok(self.0.variants[pos].0)
    }
}

#[derive(Clone, Copy)]
pub struct ColDateTime<'a>(pub &'a DateTimeMark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for ColDateTime<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::DateTime(dt) => Ok(Self(dt)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "DateTime"))
            }
        }
    }
}

impl<'a> TryRead<'a> for ColDateTime<'a> {
    type Item = chrono::DateTime<Tz>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.data.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "DateTime"));
        };
        Ok(value.with_tz(self.0.tz))
    }
}

#[derive(Clone, Copy)]
pub struct ColDateTime64<'a>(pub &'a DateTime64Mark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for ColDateTime64<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::DateTime64(dt) => Ok(Self(dt)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "DateTime64"))
            }
        }
    }
}

impl<'a> TryRead<'a> for ColDateTime64<'a> {
    type Item = chrono::DateTime<Tz>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.data.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "DateTime64"));
        };
        match value.with_tz_and_precision(self.0.tz, self.0.precision) {
            Some(dt) => Ok(dt),
            None => {
                cold_path();
                Err(Error::ValueOutOfRange(
                    "DateTime64",
                    "DateTime<Tz>",
                    value.0.get().to_string(),
                ))
            }
        }
    }
}

macro_rules! col_decimal {
    ($($name:ident, $variant:ident, $mark:ty, |$v:ident, $precision:ident| $conv:expr;)+) => {
        $(
            #[derive(Clone, Copy)]
            pub struct $name<'a>(pub &'a $mark);

            impl<'a> TryFrom<&'a Mark<'a>> for $name<'a> {
                type Error = Error;

                #[inline]
                fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
                    match value {
                        Mark::$variant(d) => Ok(Self(d)),
                        other => {
                            cold_path();
                            Err(Error::MismatchedType(other.as_str(), stringify!($variant)))
                        }
                    }
                }
            }

            impl<'a> TryRead<'a> for $name<'a> {
                type Item = Decimal;

                #[inline(always)]
                fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
                    let Some($v) = self.0.data.get(idx) else {
                        cold_path();
                        return Err(Error::IndexOutOfBounds(idx, stringify!($variant)));
                    };
                    let $precision = self.0.precision;
                    $conv
                }
            }
        )+
    };
}

col_decimal! {
    ColDecimal32, Decimal32, Decimal32Mark<'a>, |v, p| Ok(v.with_precision(p));
    ColDecimal64, Decimal64, Decimal64Mark<'a>, |v, p| Ok(v.with_precision(p));
    ColDecimal128, Decimal128, Decimal128Mark<'a>, |v, p| v.with_precision(p);
}

/// It's an escape hatch for runtime-typed columns like [`Mark::Variant`], [`Mark::Dynamic`],
/// or [`Mark::Json`].
///
/// Accepts any [`Mark`] and yields [`Value`]s.
///
/// If you don't want the Value explicitly for some reason, use normal columns.
#[derive(Clone, Copy)]
pub struct ColValue<'a>(pub &'a Mark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for ColValue<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        Ok(Self(value))
    }
}

impl<'a> TryRead<'a> for ColValue<'a> {
    type Item = Value<'a>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, self.0.as_str()));
        };
        Ok(value)
    }
}

/// Implements canonical default readers for scalar rust types; see [`Readable`].
macro_rules! readable {
    ($($item:ty => $reader:ty;)+) => {
        $(
            impl<'a> Readable<'a> for $item {
                type Reader = $reader;
            }
        )+
    };
}

readable! {
    i8 => ColI8<'a>;
    i16 => ColI16<'a>;
    i32 => ColI32<'a>;
    i64 => ColI64<'a>;
    i128 => ColI128<'a>;
    &'a I256 => ColI256<'a>;
    u8 => ColU8<'a>;
    u16 => ColU16<'a>;
    u32 => ColU32<'a>;
    u64 => ColU64<'a>;
    u128 => ColU128<'a>;
    &'a U256 => ColU256<'a>;
    f32 => ColF32<'a>;
    f64 => ColF64<'a>;
    half::bf16 => ColBf16<'a>;
    bool => ColBool<'a>;
    &'a str => ColStr<'a>;
    Uuid => ColUuid<'a>;
    Ipv4Addr => ColIpv4<'a>;
    Ipv6Addr => ColIpv6<'a>;
    NaiveDate => ColDate<'a>;
    chrono::DateTime<Tz> => ColDateTime<'a>;
    Value<'a> => ColValue<'a>;
}
