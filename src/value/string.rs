use core::convert::TryFrom;
use std::ops::Range;

use bstr::BStr;

use super::{Value, short_type_name};
use crate::ByteExt as _;
use crate::error::Error;
use crate::mark::{FixedString, StringIter};

impl<'a> TryFrom<Value<'a>> for &'a BStr {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::String(value) => Ok(value),
            other => Err(other.mismatched_type(stringify!(&'a BStr))),
        }
    }
}

impl<'a> TryFrom<Value<'a>> for StringIter<'a, 'a> {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::StringSlice { mark, range } => Ok(mark.range_iter(range.into())),
            Value::Empty => Ok(crate::mark::string::empty_iter()),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
        }
    }
}

impl<'a> TryFrom<Value<'a>> for &'a str {
    type Error = Error;

    fn try_from(value: Value<'a>) -> Result<Self, Self::Error> {
        match value {
            Value::String(value) => crate::error::decode_utf8(value),
            other => Err(other.mismatched_type(short_type_name::<Self>())),
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
            other => Err(other.mismatched_type(short_type_name::<Self>())),
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
