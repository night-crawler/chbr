use crate::{ByteExt as _, value::Value};
use bstr::BStr;
use std::ops::Deref;

pub struct FixedString<'a> {
    pub(crate) size: usize,
    pub(crate) data: &'a [u8],
}

impl std::fmt::Debug for FixedString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Render the packed records as trimmed `BStr` values: readable text when the
        // bytes are valid UTF-8, escaped bytes otherwise. Avoids the raw `[12, 22, …]`.
        struct Records<'a> {
            data: &'a [u8],
            size: usize,
        }
        impl std::fmt::Debug for Records<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    pub(crate) fn get_bstr(&self, index: usize) -> Option<&'a BStr> {
        let offset = self.size.checked_mul(index)?;
        let end = offset.checked_add(self.size)?;
        Some(BStr::new(self.data.get(offset..end)?.rtrim_zeros()))
    }

    pub(crate) fn get(&self, index: usize) -> Option<Value<'a>> {
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
    pub(crate) fn get(&self, index: usize) -> Option<&'a BStr> {
        self.data.get(index).copied()
    }
}
