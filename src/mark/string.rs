use crate::{ByteExt as _, value::Value};
use bstr::BStr;
use std::ops::Deref;

#[derive(Debug)]
pub struct FixedString<'a> {
    pub size: usize,
    pub data: &'a [u8],
}

impl<'a> FixedString<'a> {
    #[inline]
    pub(crate) fn get_bstr(&self, index: usize) -> Option<&'a BStr> {
        let offset = self.size.checked_mul(index)?;
        let end = offset.checked_add(self.size)?;
        Some(BStr::new(self.data.get(offset..end)?.rtrim_zeros()))
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<Value<'a>> {
        self.get_bstr(index).map(Value::String)
    }
}

#[derive(Debug)]
pub struct StringView<'a> {
    pub data: Vec<&'a BStr>,
}

impl<'a> Deref for StringView<'a> {
    type Target = [&'a BStr];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a> StringView<'a> {
    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&'a BStr> {
        self.data.get(index).copied()
    }
}
