use crate::arr::VarArray;
use crate::{ByteExt as _, value::Value};
use bstr::BStr;
use std::fmt;
use std::hint::cold_path;
use std::ops::Range;

pub struct FixedString<'a> {
    pub size: usize,
    pub data: &'a [u8],
}

impl std::fmt::Debug for FixedString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Render the packed records as trimmed `BStr` values: readable text when the
        // bytes are valid UTF-8, escaped bytes otherwise. Avoids the raw `[12, 22, …]`.
        struct Records<'a> {
            data: &'a [u8],
            size: usize,
        }
        impl std::fmt::Debug for Records<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut list = f.debug_list();
                if self.size == 0 {
                    return list.finish();
                }
                for chunk in self.data.chunks(self.size) {
                    list.entry(&BStr::new(chunk.rtrim_zeros()));
                }
                list.finish()
            }
        }
        f.debug_struct("FixedString")
            .field("size", &self.size)
            .field(
                "values",
                &Records {
                    data: self.data,
                    size: self.size,
                },
            )
            .finish()
    }
}

impl<'a> FixedString<'a> {
    pub fn get_bstr(&self, index: usize) -> Option<&'a BStr> {
        let offset = self.size.checked_mul(index)?;
        let end = offset.checked_add(self.size)?;
        Some(BStr::new(self.data.get(offset..end)?.rtrim_zeros()))
    }

    pub fn get(&self, index: usize) -> Option<Value<'a>> {
        self.get_bstr(index).map(Value::String)
    }
}

pub struct StringView<'a> {
    data: &'a [u8],
    offsets: VarArray,
    lengths: VarArray,
}

pub const EMPTY_STRINGS: StringView<'static> = StringView {
    data: &[],
    offsets: VarArray::empty(1),
    lengths: VarArray::empty(1),
};

static EMPTY: StringView<'static> = EMPTY_STRINGS;

pub fn empty_iter() -> StringIter<'static, 'static> {
    EMPTY.range_iter(0..0)
}

impl<'a> StringView<'a> {
    pub(crate) fn new(data: &'a [u8], rows: usize) -> Self {
        Self {
            data,
            offsets: VarArray::uninit(rows, 2),
            lengths: VarArray::uninit(rows, 1),
        }
    }

    #[inline(always)]
    pub(crate) fn set(&mut self, index: usize, offset: usize, len: usize) {
        self.offsets.set(index, offset as u64);
        self.lengths.set(index, len as u64);
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.offsets.len()
    }

    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "offsets and lengths index a slice that lives in this address space"
    )]
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&'a BStr> {
        if index >= self.offsets.len() {
            cold_path();
            return None;
        }

        let offset = self.offsets.get(index) as usize;
        let end = offset + self.lengths.get(index) as usize;

        Some(BStr::new(self.data.get(offset..end)?))
    }

    #[inline]
    pub const fn iter(&self) -> StringIter<'a, '_> {
        self.range_iter(0..self.len())
    }

    #[inline]
    pub const fn range_iter(&self, range: Range<usize>) -> StringIter<'a, '_> {
        StringIter { view: self, range }
    }
}

impl fmt::Debug for StringView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

pub struct StringIter<'data, 'view> {
    view: &'view StringView<'data>,
    range: Range<usize>,
}

impl<'data> Iterator for StringIter<'data, '_> {
    type Item = &'data BStr;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.view.get(self.range.next()?)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl ExactSizeIterator for StringIter<'_, '_> {}
