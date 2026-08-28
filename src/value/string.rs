use core::{convert::TryFrom, hint::cold_path};
use std::ops::Range;

use bstr::BStr;

use super::Value;
use crate::{ByteExt as _, error::Error, mark::FixedString};

impl<'a> TryFrom<Value<'a>> for &'a BStr {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::String(value) => Ok(value),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), stringify!(&'a BStr)))
            }
        }
    }
}

impl<'a> TryFrom<Value<'a>> for &'a [&'a BStr] {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::StringSlice(value) => Ok(value),
            Value::Empty => Ok(&[]),
            other => {
                cold_path();
                Err(Error::MismatchedType(
                    other.as_str(),
                    stringify!(&'a [&'a BStr]),
                ))
            }
        }
    }
}

impl<'a> TryFrom<Value<'a>> for &'a str {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::String(value) => crate::error::decode_utf8(value),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "&str"))
            }
        }
    }
}

pub struct FixedStringSliceIterator<'a> {
    mark: &'a FixedString<'a>,
    range: Range<usize>,
}

impl<'a> TryFrom<Value<'a>> for FixedStringSliceIterator<'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::FixedStringSlice { mark, range } => Ok(Self {
                mark,
                range: range.into(),
            }),
            other => {
                cold_path();
                Err(Error::MismatchedType(
                    other.as_str(),
                    "FixedStringSliceIterator",
                ))
            }
        }
    }
}

impl<'a> Iterator for FixedStringSliceIterator<'a> {
    type Item = &'a BStr;

    fn next(&mut self) -> Option<Self::Item> {
        let slice_idx = self.range.next()?;
        let start = slice_idx * self.mark.size;
        let end = start + self.mark.size;

        if end > self.mark.data.len() {
            return None;
        }

        Some(BStr::new(self.mark.data[start..end].rtrim_zeros()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl ExactSizeIterator for FixedStringSliceIterator<'_> {}
