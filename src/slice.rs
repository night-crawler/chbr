use core::{marker::PhantomData, ops::Index};

use zerocopy::{FromBytes, Unaligned};

#[repr(transparent)]
pub struct ByteView<'a, T: Unaligned + FromBytes + Copy> {
    bytes: &'a [u8],
    _pd: PhantomData<&'a T>,
}

impl<'a, T: Unaligned + FromBytes + Copy> TryFrom<&'a [u8]> for ByteView<'a, T> {
    type Error = crate::error::Error;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() % size_of::<T>() == 0 {
            Ok(Self {
                bytes,
                _pd: PhantomData,
            })
        } else {
            Err(Self::Error::LengthError(bytes.len()))
        }
    }
}

impl<'a, T: Unaligned + FromBytes + Copy> ByteView<'a, T> {
    pub fn len(&self) -> usize {
        self.bytes.len() / size_of::<T>()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

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

    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

impl<'a, T: Unaligned + FromBytes + Copy + Default> ByteView<'a, T> {
    pub fn last_or_default(&self) -> T {
        if let Some(last) = self.last() {
            *last
        } else {
            T::default()
        }
    }
}

impl<'a, T: Unaligned + FromBytes + Copy> Index<usize> for ByteView<'a, T> {
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


use core::{any::type_name, fmt, mem::size_of};

impl<'a, T> fmt::Debug for ByteView<'a, T>
where
    T: Unaligned + FromBytes + Copy,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteView")
            .field("T", &type_name::<T>())
            .field("size_of_T", &size_of::<T>())
            .field("len_T", &self.len())
            .field("len_bytes", &self.bytes.len())
            .finish()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};
    use testresult::TestResult;
    use zerocopy::byteorder::{LittleEndian, U64};

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
            Err(crate::error::Error::LengthError(e)) => {
                assert_eq!(e, 115);
            }
            _ => {
                panic!("Expected LengthError, but got a different error");
            }
        }
    }
}
