use std::hint::cold_path;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::ops::Range;

use chrono::NaiveDate;
use chrono_tz::Tz;
use rust_decimal::Decimal;

use super::{ReadSlice, Readable, TryRead};
use crate::error::Error;
use crate::mark::{
    BoolView, DateTime as DateTimeMark, DateTime64 as DateTime64Mark, Decimal32 as Decimal32Mark,
    Decimal64 as Decimal64Mark, Decimal128 as Decimal128Mark, Enum8 as Enum8Mark,
    Enum16 as Enum16Mark, FixedString as FixedStringMark, Mark, StringView,
};
use crate::slice::ByteView;
use crate::{Bf16Data, ByteExt as _, Date16Data, Date32Data, Ipv4Data, Ipv6Data, UuidData, zc};

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
    I8, Int8, i8, i8, |v| *v;
    I16, Int16, zc::I16, i16, |v| v.get();
    I32, Int32, zc::I32, i32, |v| v.get();
    I64, Int64, zc::I64, i64, |v| v.get();
    I128, Int128, zc::I128, i128, |v| v.get();
    I256, Int256, crate::I256, &'a crate::I256, |v| v;
    U8, UInt8, u8, u8, |v| *v;
    U16, UInt16, zc::U16, u16, |v| v.get();
    U32, UInt32, zc::U32, u32, |v| v.get();
    U64, UInt64, zc::U64, u64, |v| v.get();
    U128, UInt128, zc::U128, u128, |v| v.get();
    U256, UInt256, crate::U256, &'a crate::U256, |v| v;
    F32, Float32, zc::F32, f32, |v| v.get();
    F64, Float64, zc::F64, f64, |v| v.get();
    Bf16, BFloat16, Bf16Data, half::bf16, |v| half::bf16::from(*v);
    Uuid, Uuid, UuidData, uuid::Uuid, |v| uuid::Uuid::from(*v);
    Ipv4, Ipv4, Ipv4Data, Ipv4Addr, |v| Ipv4Addr::from(*v);
    Ipv6, Ipv6, Ipv6Data, Ipv6Addr, |v| Ipv6Addr::from(*v);
    Date, Date, Date16Data, NaiveDate, |v| NaiveDate::from(*v);
    Date32, Date32, Date32Data, NaiveDate, |v| NaiveDate::from(*v);
}

#[derive(Clone, Copy)]
pub struct Usize<'a>(pub &'a Mark<'a>);

impl<'a> TryRead<'a> for Usize<'a> {
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
            other => {
                cold_path();
                return Err(Error::MismatchedType(other.as_str(), "UInt8/16/32/64"));
            }
        };
        let Some(value) = value else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "UInt8/16/32/64"));
        };
        Ok(value)
    }
}

impl<'a> TryFrom<&'a Mark<'a>> for Usize<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::UInt8(_) | Mark::UInt16(_) | Mark::UInt32(_) | Mark::UInt64(_) => {
                Ok(Usize(value))
            }
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "UInt8/16/32/64"))
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct Bool<'a>(pub &'a BoolView<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for Bool<'a> {
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

impl<'a> TryRead<'a> for Bool<'a> {
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

impl<'a> ReadSlice<'a> for Bool<'a> {
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
pub struct Str<'a>(pub &'a StringView<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for Str<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::String(s) => Ok(Str(s)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "String"))
            }
        }
    }
}

impl<'a> TryRead<'a> for Str<'a> {
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

impl<'a> ReadSlice<'a> for Str<'a> {
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
pub struct FixedStr<'a>(pub &'a FixedStringMark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for FixedStr<'a> {
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

impl<'a> TryRead<'a> for FixedStr<'a> {
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
pub struct Enum8<'a>(pub &'a Enum8Mark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for Enum8<'a> {
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

impl<'a> TryRead<'a> for Enum8<'a> {
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
pub struct Enum16<'a>(pub &'a Enum16Mark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for Enum16<'a> {
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

impl<'a> TryRead<'a> for Enum16<'a> {
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
pub struct DateTime<'a>(pub &'a DateTimeMark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for DateTime<'a> {
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

impl<'a> TryRead<'a> for DateTime<'a> {
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
pub struct DateTime64<'a>(pub &'a DateTime64Mark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for DateTime64<'a> {
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

impl<'a> TryRead<'a> for DateTime64<'a> {
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
    Decimal32, Decimal32, Decimal32Mark<'a>, |v, p| Ok(v.with_precision(p));
    Decimal64, Decimal64, Decimal64Mark<'a>, |v, p| Ok(v.with_precision(p));
    Decimal128, Decimal128, Decimal128Mark<'a>, |v, p| v.with_precision(p);
}

/// It's an escape hatch for runtime-typed columns like [`Mark::Variant`], [`Mark::Dynamic`],
/// or [`Mark::Json`].
///
/// Accepts any [`Mark`] and yields [`Value`]s.
///
/// If you don't want the Value explicitly for some reason, use normal columns.
#[derive(Clone, Copy)]
pub struct Value<'a>(pub &'a Mark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for Value<'a> {
    type Error = Error;

    #[inline]
    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        Ok(Self(value))
    }
}

impl<'a> TryRead<'a> for Value<'a> {
    type Item = crate::value::Value<'a>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get(idx)? else {
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
    i8 => I8<'a>;
    i16 => I16<'a>;
    i32 => I32<'a>;
    i64 => I64<'a>;
    i128 => I128<'a>;
    &'a crate::I256 => I256<'a>;
    u8 => U8<'a>;
    u16 => U16<'a>;
    u32 => U32<'a>;
    u64 => U64<'a>;
    u128 => U128<'a>;
    &'a crate::U256 => U256<'a>;
    f32 => F32<'a>;
    f64 => F64<'a>;
    half::bf16 => Bf16<'a>;
    bool => Bool<'a>;
    &'a str => Str<'a>;
    uuid::Uuid => Uuid<'a>;
    Ipv4Addr => Ipv4<'a>;
    Ipv6Addr => Ipv6<'a>;
    NaiveDate => Date<'a>;
    chrono::DateTime<Tz> => DateTime<'a>;
    crate::value::Value<'a> => Value<'a>;
}
