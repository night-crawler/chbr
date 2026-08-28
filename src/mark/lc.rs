use crate::mark::{Mark, checked_slice};
use crate::value::{LowCardinalitySliceIterator, SliceUsizeIterator, Value};
use crate::{Error, zc};
use bstr::BStr;
use std::hint::cold_path;
use std::ops::Range;

#[derive(Debug)]
pub struct LowCardinality<'a> {
    pub is_nullable: bool,
    pub indices: Indices<'a>,
    pub global_dictionary: Option<Box<Mark<'a>>>,
    pub additional_keys: Option<Box<Mark<'a>>>,
}

impl LowCardinality<'_> {
    pub fn slice(&self, range: Range<usize>) -> crate::Result<LowCardinalitySliceIterator<'_>> {
        let Some(additional_keys) = self.additional_keys.as_ref() else {
            return Err(Error::MismatchedType(
                "LowCardinalitySliceIterator",
                "LowCardinalitySlice with no additional keys",
            ));
        };

        let sliced = self.indices.slice(range)?;

        Ok(LowCardinalitySliceIterator {
            indices: SliceUsizeIterator::try_from(sliced)?,
            additional_keys,
        })
    }

    #[inline(always)]
    pub fn value_index(&self, index: usize) -> crate::Result<Option<usize>> {
        self.indices.get(index)
    }

    #[inline(always)]
    pub(crate) fn get_str(&self, index: usize) -> crate::Result<Option<&BStr>> {
        let Some(keys) = &self.additional_keys else {
            cold_path();
            return Err(Error::CorruptedData(
                "LowCardinality marker without additional keys".to_owned(),
            ));
        };

        let Some(value_index) = self.value_index(index)? else {
            return Ok(None);
        };
        if value_index == 0 && self.is_nullable {
            return Ok(None);
        }

        let Mark::String(keys) = keys.as_ref() else {
            cold_path();
            return Err(Error::MismatchedType(keys.as_str(), "&BStr"));
        };
        Ok(keys.get(value_index))
    }

    pub fn get(&self, index: usize) -> crate::Result<Option<Value<'_>>> {
        // https://github.com/ClickHouse/clickhouse-go/blob/71a2b475e899afe9626f40af513bcf25aa3098a2/lib/column/lowcardinality.go#L191
        let Some(keys) = &self.additional_keys else {
            return Ok(None);
        };

        let Some(value_index) = self.value_index(index)? else {
            return Ok(None);
        };
        if value_index == 0 && self.is_nullable {
            return Ok(Some(Value::Empty));
        }

        // fast path for LowCardinality with String keys
        if let Mark::String(keys) = keys.as_ref() {
            return Ok(keys.get(value_index).map(Value::String));
        }

        keys.get(value_index)
    }
}

impl<'a> Indices<'a> {
    #[inline(always)]
    pub fn get(self, index: usize) -> crate::Result<Option<usize>> {
        match self {
            Self::Empty => Ok(None),
            Self::U8(indices) => Ok(indices.get(index).copied().map(usize::from)),
            Self::U16(indices) => Ok(indices.get(index).map(|value| usize::from(value.get()))),
            Self::U32(indices) => Ok(indices.get(index).map(|value| value.get() as usize)),
            Self::U64(indices) => Ok(indices
                .get(index)
                .map(|value| usize::try_from(value.get()))
                .transpose()?),
        }
    }

    pub const fn is_empty(self) -> bool {
        match self {
            Self::Empty => true,
            Self::U8(indices) => indices.is_empty(),
            Self::U16(indices) => indices.is_empty(),
            Self::U32(indices) => indices.is_empty(),
            Self::U64(indices) => indices.is_empty(),
        }
    }

    pub fn all_zero(self) -> bool {
        match self {
            Self::Empty => true,
            Self::U8(indices) => indices.iter().all(|&value| value == 0),
            Self::U16(indices) => indices.iter().all(|value| value.get() == 0),
            Self::U32(indices) => indices.iter().all(|value| value.get() == 0),
            Self::U64(indices) => indices.iter().all(|value| value.get() == 0),
        }
    }

    pub fn slice(self, range: Range<usize>) -> crate::Result<Value<'a>> {
        match self {
            Self::Empty => {
                if range.start != 0 || range.end != 0 {
                    cold_path();
                    return Err(Error::RangeOutOfBounds(range, "Empty"));
                }
                Ok(Value::Empty)
            }
            Self::U8(indices) => Ok(Value::UInt8Slice(checked_slice(indices, range, "UInt8")?)),
            Self::U16(indices) => Ok(Value::UInt16Slice(checked_slice(indices, range, "UInt16")?)),
            Self::U32(indices) => Ok(Value::UInt32Slice(checked_slice(indices, range, "UInt32")?)),
            Self::U64(indices) => Ok(Value::UInt64Slice(checked_slice(indices, range, "UInt64")?)),
        }
    }

    #[inline]
    pub(crate) fn iter(self, range: Range<usize>) -> crate::Result<IndexIter<'a>> {
        match self {
            Self::Empty => {
                if range.start != 0 || range.end != 0 {
                    return Err(Error::RangeOutOfBounds(range, "Empty"));
                }
                Ok(IndexIter::U8([].iter()))
            }
            Self::U8(indices) => Ok(IndexIter::U8(
                checked_slice(indices, range, "UInt8")?.iter(),
            )),
            Self::U16(indices) => Ok(IndexIter::U16(
                checked_slice(indices, range, "UInt16")?.iter(),
            )),
            Self::U32(indices) => Ok(IndexIter::U32(
                checked_slice(indices, range, "UInt32")?.iter(),
            )),
            Self::U64(indices) => Ok(IndexIter::U64(
                checked_slice(indices, range, "UInt64")?.iter(),
            )),
        }
    }
}

pub(crate) enum IndexIter<'a> {
    U8(std::slice::Iter<'a, u8>),
    U16(std::slice::Iter<'a, zc::U16>),
    U32(std::slice::Iter<'a, zc::U32>),
    U64(std::slice::Iter<'a, zc::U64>),
}

impl Iterator for IndexIter<'_> {
    type Item = crate::Result<usize>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let value = match self {
            Self::U8(iter) => usize::from(*iter.next()?),
            Self::U16(iter) => usize::from(iter.next()?.get()),
            Self::U32(iter) => iter.next()?.get() as usize,
            Self::U64(iter) => {
                let value = iter.next()?.get();
                return match usize::try_from(value) {
                    Ok(value) => Some(Ok(value)),
                    Err(error) => Some(Err(Error::from(error))),
                };
            }
        };
        Some(Ok(value))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::U8(iter) => iter.size_hint(),
            Self::U16(iter) => iter.size_hint(),
            Self::U32(iter) => iter.size_hint(),
            Self::U64(iter) => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for IndexIter<'_> {}

/// Iterator over raw string keys of a `LowCardinality` column slice.
/// Waiting for: <https://github.com/rust-lang/rust/issues/63063>
pub struct StrIter<'a> {
    pub(crate) indices: IndexIter<'a>,
    pub(crate) keys: &'a [&'a BStr],
}

impl<'a> Iterator for StrIter<'a> {
    type Item = crate::Result<&'a BStr>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = match self.indices.next()? {
            Ok(index) => index,
            Err(error) => {
                cold_path();
                return Some(Err(error));
            }
        };
        let Some(value) = self.keys.get(index).copied() else {
            cold_path();
            return Some(Err(Error::IndexOutOfBounds(
                index,
                "LowCardinality dictionary",
            )));
        };
        Some(Ok(value))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indices.size_hint()
    }
}

impl ExactSizeIterator for StrIter<'_> {}

#[derive(Clone, Copy, Debug)]
pub enum Indices<'a> {
    Empty,
    U8(&'a [u8]),
    U16(&'a [zc::U16]),
    U32(&'a [zc::U32]),
    U64(&'a [zc::U64]),
}

impl<'a> TryFrom<Mark<'a>> for Indices<'a> {
    type Error = Error;

    fn try_from(mark: Mark<'a>) -> Result<Self, Self::Error> {
        match mark {
            Mark::Empty => Ok(Self::Empty),
            Mark::UInt8(indices) => Ok(Self::U8(indices.as_slice())),
            Mark::UInt16(indices) => Ok(Self::U16(indices.as_slice())),
            Mark::UInt32(indices) => Ok(Self::U32(indices.as_slice())),
            Mark::UInt64(indices) => Ok(Self::U64(indices.as_slice())),
            other => {
                cold_path();
                Err(Error::CorruptedData(format!(
                    "unexpected LowCardinality indices type: {}",
                    other.as_str()
                )))
            }
        }
    }
}

pub struct ArrayLcStrIter<'a> {
    pub inner: Option<StrIter<'a>>,
}

impl<'a> Iterator for ArrayLcStrIter<'a> {
    type Item = crate::Result<&'a BStr>;

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
