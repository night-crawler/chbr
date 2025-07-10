use core::{
    fmt,
    marker::PhantomData,
    mem::size_of,
    ops::{Index, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive},
};

use zerocopy::{FromBytes, Unaligned};

#[repr(transparent)]
pub struct ByteView<'a, T: Unaligned + FromBytes + Copy> {
    bytes: &'a [u8],
    _pd: PhantomData<&'a T>,
}

impl<'a, T: Unaligned + FromBytes + Copy> TryFrom<&'a [u8]> for ByteView<'a, T> {
    type Error = crate::Error;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() % size_of::<T>() == 0 {
            Ok(Self {
                bytes,
                _pd: PhantomData,
            })
        } else {
            Err(Self::Error::Length(bytes.len()))
        }
    }
}

impl<'a, T: Unaligned + FromBytes + Copy> ByteView<'a, T> {
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len() / size_of::<T>()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn get(&self, index: usize) -> Option<&T> {
        if self.is_empty() || index >= self.len() {
            return None;
        }
        Some(&self[index])
    }

    pub fn last(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self[self.len() - 1])
        }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &'a [T] {
        let n_elements = self.len();
        unsafe { core::slice::from_raw_parts(self.bytes.as_ptr().cast::<T>(), n_elements) }
    }
}

impl<T: Unaligned + FromBytes + Copy> Index<usize> for ByteView<'_, T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        let size = size_of::<T>();
        assert!(index < self.len(), "index out of bounds");

        // SAFETY:
        // - `idx` has been bounds-checked.
        // - `size` is the exact size of `T`.
        // - `T: Unaligned` promises that `&T` is valid at any address.
        unsafe { &*self.bytes.as_ptr().add(index * size).cast::<T>() }
    }
}

impl<T> fmt::Debug for ByteView<'_, T>
where
    T: Unaligned + FromBytes + Copy + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteView")
            .field("len_bytes", &self.bytes.len())
            .field("data", &self.as_slice())
            .finish()
    }
}

macro_rules! impl_slice_index {
    ($range:ty) => {
        impl<'a, T> Index<$range> for ByteView<'a, T>
        where
            T: Unaligned + FromBytes + Copy,
        {
            type Output = [T];

            #[inline]
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
    use zerocopy::byteorder::{LittleEndian, U64};

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
        let view_aligned = ByteView::<U64<LittleEndian>>::try_from(raw.as_slice())?;

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

        let view_unaligned = ByteView::<U64<LittleEndian>>::try_from(misaligned)?;

        for (i, &expect) in nums.iter().enumerate() {
            assert_eq!(view_unaligned[i].get(), expect);
        }

        Ok(())
    }

    #[test]
    fn construction_fails_if_length_is_wrong() {
        let bad = [0u8; 115];
        match ByteView::<U64<LittleEndian>>::try_from(bad.as_slice()) {
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
