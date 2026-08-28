use std::hint::cold_path;
use std::ops::Range;

use bstr::BStr;

use super::{ReadSlice, Readable, TryRead};
use crate::ByteExt as _;
use crate::error::{Error, decode_utf8};
use crate::mark::{FixedString as FixedStringMark, Mark, StringView};

/// Reads raw ClickHouse `String` values without assuming UTF-8.
#[derive(Clone, Copy)]
pub struct Bytes<'a>(pub &'a StringView<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for Bytes<'a> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::String(strings) => Ok(Self(strings)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "String"))
            }
        }
    }
}

impl<'a> TryRead<'a> for Bytes<'a> {
    type Item = &'a BStr;

    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "String"));
        };
        Ok(value)
    }
}

impl<'a> ReadSlice<'a> for Bytes<'a> {
    type Elem = &'a BStr;

    fn try_read_slice(&self, range: Range<usize>) -> crate::Result<&'a [Self::Elem]> {
        let Some(slice) = self.0.data.get(range.clone()) else {
            cold_path();
            return Err(Error::RangeOutOfBounds(range, "String"));
        };
        Ok(slice)
    }
}

/// Reads ClickHouse `String` values as `&str`.
///
/// [`TryFrom`] validates the complete column once.
#[derive(Clone, Copy)]
pub struct Str<'a>(&'a StringView<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for Str<'a> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        let reader = Bytes::try_from(value)?;
        for value in &reader.0.data {
            decode_utf8(value)?;
        }
        Ok(Self(reader.0))
    }
}

impl<'a> TryRead<'a> for Str<'a> {
    type Item = &'a str;

    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "String"));
        };
        // SAFETY: construction validated every value in the column.
        Ok(unsafe { std::str::from_utf8_unchecked(value) })
    }
}

/// Reads ClickHouse `String` values as `&str` without any UTF-8 validation.
/// You are responsible for your own UB.
#[derive(Clone, Copy)]
pub struct TrustedStr<'a>(&'a StringView<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for TrustedStr<'a> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        let reader = Bytes::try_from(value)?;
        Ok(Self(reader.0))
    }
}

impl<'a> TryRead<'a> for TrustedStr<'a> {
    type Item = &'a str;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "String"));
        };
        debug_assert!(
            std::str::from_utf8(value).is_ok(),
            "TrustedStr read invalid UTF-8"
        );
        // SAFETY: choosing this reader asserts the column is valid UTF-8.
        Ok(unsafe { std::str::from_utf8_unchecked(value) })
    }
}

#[derive(Clone, Copy)]
pub struct FixedBytes<'a>(pub &'a FixedStringMark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for FixedBytes<'a> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::FixedString(fixed) => Ok(Self(fixed)),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "FixedString"))
            }
        }
    }
}

impl<'a> TryRead<'a> for FixedBytes<'a> {
    type Item = &'a BStr;

    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get_bstr(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "FixedString"));
        };
        Ok(value)
    }
}

/// Reads ClickHouse `FixedString` values as `&str`.
///
/// [`TryFrom`] validates the complete column once.
#[derive(Clone, Copy)]
pub struct FixedStr<'a>(&'a FixedStringMark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for FixedStr<'a> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        let reader = FixedBytes::try_from(value)?;
        if reader.0.size != 0 {
            for bytes in reader.0.data.chunks_exact(reader.0.size) {
                decode_utf8(bytes.rtrim_zeros())?;
            }
        }
        Ok(Self(reader.0))
    }
}

impl<'a> TryRead<'a> for FixedStr<'a> {
    type Item = &'a str;

    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get_bstr(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "FixedString"));
        };
        // SAFETY: construction validated every value in the column.
        Ok(unsafe { std::str::from_utf8_unchecked(value) })
    }
}

/// Reads ClickHouse `FixedString` values as `&str` without any UTF-8
/// validation.
#[derive(Clone, Copy)]
pub struct TrustedFixedStr<'a>(&'a FixedStringMark<'a>);

impl<'a> TryFrom<&'a Mark<'a>> for TrustedFixedStr<'a> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        FixedBytes::try_from(value).map(|reader| Self(reader.0))
    }
}

impl<'a> TryRead<'a> for TrustedFixedStr<'a> {
    type Item = &'a str;

    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value) = self.0.get_bstr(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "FixedString"));
        };
        debug_assert!(
            std::str::from_utf8(value).is_ok(),
            "TrustedFixedStr read invalid UTF-8"
        );
        // SAFETY: choosing this reader asserts the column is valid UTF-8.
        Ok(unsafe { std::str::from_utf8_unchecked(value) })
    }
}

impl<'a> Readable<'a> for &'a str {
    type Reader = Str<'a>;
}

impl<'a> Readable<'a> for &'a BStr {
    type Reader = Bytes<'a>;
}
