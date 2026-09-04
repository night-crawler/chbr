use crate::zc;
use core::hint::cold_path;
use core::{
    fmt,
    mem::size_of,
    ops::{Index, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive},
};

#[repr(transparent)]
pub struct ByteView<'a, T: zc::Unaligned + zc::FromBytes + Copy> {
    data: &'a [T],
}

impl<'a, T: zc::Unaligned + zc::FromBytes + Copy> TryFrom<&'a [u8]> for ByteView<'a, T> {
    type Error = crate::Error;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len().is_multiple_of(size_of::<T>()) {
            let len = bytes.len() / size_of::<T>();
            // SAFETY:
            //  - `T` is read-only
            //  - Mem is initialized
            //  - `T: Unaligned` guarantees `&T` is valid at any address
            //  - `T: FromBytes` guarantees that initialized bytes are valid `T`
            // - `len * size_of::<T>() == bytes.len()`
            let data = unsafe { core::slice::from_raw_parts(bytes.as_ptr().cast::<T>(), len) };
            Ok(Self { data })
        } else {
            cold_path();
            Err(Self::Error::Length(bytes.len()))
        }
    }
}

impl<'a, T: zc::Unaligned + zc::FromBytes + Copy> ByteView<'a, T> {
    #[inline]
    pub(crate) const fn len(&self) -> usize {
        self.data.len()
    }

    #[expect(dead_code)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub(crate) fn get(&self, index: usize) -> Option<&'a T> {
        self.data.get(index)
    }

    #[inline(always)]
    pub(crate) const fn as_slice(&self) -> &'a [T] {
        self.data
    }
}

impl<T: zc::Unaligned + zc::FromBytes + Copy> Index<usize> for ByteView<'_, T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl<T> fmt::Debug for ByteView<'_, T>
where
    T: zc::Unaligned + zc::FromBytes + Copy + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteView")
            .field("len_bytes", &size_of_val(self.data))
            .field("data", &self.data)
            .finish()
    }
}

macro_rules! impl_slice_index {
    ($range:ty) => {
        impl<'a, T> Index<$range> for ByteView<'a, T>
        where
            T: zc::Unaligned + zc::FromBytes + Copy,
        {
            type Output = [T];

            fn index(&self, idx: $range) -> &Self::Output {
                &self.as_slice()[idx]
            }
        }
    };
}

impl_slice_index!(Range<usize>);
impl_slice_index!(RangeFrom<usize>);
impl_slice_index!(RangeTo<usize>);
impl_slice_index!(RangeInclusive<usize>);
impl_slice_index!(RangeToInclusive<usize>);
impl_slice_index!(RangeFull);

#[cfg(test)]
mod tests {
    use pretty_assertions::{assert_eq, assert_ne};
    use testresult::TestResult;

    use super::*;

    fn to_le_bytes(nums: &[u64]) -> Vec<u8> {
        let mut v = Vec::with_capacity(nums.len() * 8);
        for &n in nums {
            v.extend_from_slice(&n.to_le_bytes());
        }
        v
    }

    #[test]
    fn byte_view_reads_aligned_and_unaligned() -> TestResult {
        let nums = [1u64, 0xfeed_beef_dead_cafe, 0x0123_4567_89ab_cdef];

        let raw = to_le_bytes(&nums);
        let view_aligned = ByteView::<zc::U64>::try_from(raw.as_slice())?;

        assert_eq!(view_aligned.len(), nums.len());
        for (i, &expect) in nums.iter().enumerate() {
            assert_eq!(view_aligned[i].get(), expect);
        }

        let mut padded = vec![0xAAu8];
        padded.extend_from_slice(&raw);
        let misaligned = &padded[1..];

        let ptr = misaligned.as_ptr() as usize;
        assert_ne!(
            ptr % 8,
            0,
            "test setup failed: slice is still 8-byte aligned"
        );

        let view_unaligned = ByteView::<zc::U64>::try_from(misaligned)?;

        for (i, &expect) in nums.iter().enumerate() {
            assert_eq!(view_unaligned[i].get(), expect);
        }

        Ok(())
    }

    #[test]
    fn construction_fails_if_length_is_wrong() {
        let bad = [0u8; 115];
        match ByteView::<zc::U64>::try_from(bad.as_slice()) {
            Ok(_) => {
                panic!("Expected error, but got Ok");
            }
            Err(crate::Error::Length(e)) => {
                assert_eq!(e, 115);
            }
            _ => {
                panic!("Expected LengthError, but got a different error");
            }
        }
    }
}
