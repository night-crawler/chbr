use std::ops::Range;

use super::{ColStr, FromVariant, ReadSlice, Readable, TryRead};
use crate::error::Error;
use crate::mark::{Mark, Variant as VariantMark};
use crate::types::{OffsetIndexPair as _, Offsets};
use zerocopy::little_endian::{U16, U32, U64};

#[derive(Clone, Copy)]
pub struct ColNullable<'a, Inner> {
    pub mask: &'a [u8],
    pub inner: Inner,
}

impl<'a, Inner> TryFrom<&'a Mark<'a>> for ColNullable<'a, Inner>
where
    Inner: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    Error: From<<Inner as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Nullable(n) => Ok(ColNullable {
                mask: n.mask,
                inner: Inner::try_from(n.data.as_ref())?,
            }),
            other => Err(Error::MismatchedType(other.as_str(), "Nullable")),
        }
    }
}

impl<'a, Inner: TryRead<'a> + 'a> TryRead<'a> for ColNullable<'a, Inner> {
    type Item = Option<Inner::Item>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(mask) = self.mask.get(idx) else {
            return Err(Error::IndexOutOfBounds(idx, "Nullable"));
        };
        if *mask == 1 {
            return Ok(None);
        }
        Ok(Some(self.inner.try_read(idx)?))
    }
}

#[derive(Clone, Copy)]
enum LcIndices<'a> {
    Empty,
    U8(&'a [u8]),
    U16(&'a [U16]),
    U32(&'a [U32]),
    U64(&'a [U64]),
}

impl<'a> LcIndices<'a> {
    fn resolve(mark: &'a Mark<'a>) -> Result<Self, Error> {
        Ok(match mark {
            Mark::Empty => LcIndices::Empty,
            Mark::UInt8(bv) => LcIndices::U8(bv.as_slice()),
            Mark::UInt16(bv) => LcIndices::U16(bv.as_slice()),
            Mark::UInt32(bv) => LcIndices::U32(bv.as_slice()),
            Mark::UInt64(bv) => LcIndices::U64(bv.as_slice()),
            other => {
                return Err(Error::CorruptedData(format!(
                    "unexpected LowCardinality indices type: {}",
                    other.as_str()
                )));
            }
        })
    }

    #[inline(always)]
    fn get(self, idx: usize) -> Option<usize> {
        match self {
            LcIndices::Empty => None,
            LcIndices::U8(s) => Some(usize::from(*s.get(idx)?)),
            LcIndices::U16(s) => Some(usize::from(s.get(idx)?.get())),
            LcIndices::U32(s) => Some(s.get(idx)?.get() as usize),
            LcIndices::U64(s) => Some(usize::try_from(s.get(idx)?.get()).unwrap()),
        }
    }
}

#[derive(Clone, Copy)]
pub struct ColLc<'a, Inner> {
    indices: LcIndices<'a>,
    dict: Option<Inner>,
}

pub type ColLcStr<'a> = ColLc<'a, ColStr<'a>>;

impl<'a, Inner> TryFrom<&'a Mark<'a>> for ColLc<'a, Inner>
where
    Inner: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    Error: From<<Inner as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::LowCardinality(lc) if !lc.is_nullable => {
                let indices = LcIndices::resolve(lc.indices.as_ref())?;
                let dict = match lc.additional_keys.as_deref() {
                    Some(keys) => Some(Inner::try_from(keys)?),
                    None => None,
                };
                Ok(ColLc { indices, dict })
            }
            Mark::LowCardinality(_) => Err(Error::MismatchedType(
                "LowCardinality(Nullable)",
                "LowCardinality",
            )),
            other => Err(Error::MismatchedType(other.as_str(), "LowCardinality")),
        }
    }
}

impl<'a, Inner: TryRead<'a> + 'a> TryRead<'a> for ColLc<'a, Inner> {
    type Item = Inner::Item;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value_index) = self.indices.get(idx) else {
            return Err(Error::IndexOutOfBounds(idx, "LowCardinality"));
        };
        let Some(dict) = self.dict.as_ref() else {
            return Err(Error::CorruptedData(
                "LowCardinality dictionary is missing".to_owned(),
            ));
        };
        dict.try_read(value_index)
    }
}

#[derive(Clone, Copy)]
pub struct ColLcNullable<'a, Inner> {
    indices: LcIndices<'a>,
    dict: Option<Inner>,
}

pub type ColLcNullableStr<'a> = ColLcNullable<'a, ColStr<'a>>;

impl<'a, Inner> TryFrom<&'a Mark<'a>> for ColLcNullable<'a, Inner>
where
    Inner: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    Error: From<<Inner as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::LowCardinality(lc) if lc.is_nullable => {
                let indices = LcIndices::resolve(lc.indices.as_ref())?;
                let dict = match lc.additional_keys.as_deref() {
                    Some(keys) => Some(Inner::try_from(keys)?),
                    None => None,
                };
                Ok(ColLcNullable { indices, dict })
            }
            Mark::LowCardinality(_) => Err(Error::MismatchedType(
                "LowCardinality",
                "LowCardinality(Nullable)",
            )),
            other => Err(Error::MismatchedType(other.as_str(), "LowCardinality")),
        }
    }
}

impl<'a, Inner: TryRead<'a> + 'a> TryRead<'a> for ColLcNullable<'a, Inner> {
    type Item = Option<Inner::Item>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value_index) = self.indices.get(idx) else {
            return Err(Error::IndexOutOfBounds(idx, "LowCardinality"));
        };
        if value_index == 0 {
            return Ok(None);
        }
        let Some(dict) = self.dict.as_ref() else {
            return Err(Error::CorruptedData(
                "LowCardinality dictionary is missing".to_owned(),
            ));
        };
        Ok(Some(dict.try_read(value_index)?))
    }
}

#[derive(Clone, Copy)]
pub struct ColArray<'a, Inner: TryRead<'a>> {
    pub offsets: &'a Offsets<'a>,
    pub values: Inner,
}

impl<'a, Inner> TryFrom<&'a Mark<'a>> for ColArray<'a, Inner>
where
    Inner: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    Error: From<<Inner as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Array(arr) => Ok(ColArray {
                offsets: &arr.offsets,
                values: Inner::try_from(arr.values.as_ref())?,
            }),
            // `Nested(...)` is stored as an array of tuples.
            Mark::Nested(n) => Self::try_from(n.array_of_tuples.as_ref()),
            other => Err(Error::MismatchedType(other.as_str(), "Array")),
        }
    }
}

impl<'a, Inner: TryRead<'a> + 'a> TryRead<'a> for ColArray<'a, Inner> {
    type Item = ArrayIter<'a, Inner>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some((s, e)) = self.offsets.offset_indices(idx)? else {
            return Err(Error::IndexOutOfBounds(idx, "Array"));
        };
        Ok(ArrayIter {
            inner: self.values,
            range: s..e,
            _marker: std::marker::PhantomData,
        })
    }
}

pub struct ArrayIter<'a, Inner: TryRead<'a>> {
    inner: Inner,
    range: Range<usize>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, Inner: TryRead<'a>> ArrayIter<'a, Inner> {
    pub fn try_collect_vec(self) -> crate::Result<Vec<Inner::Item>> {
        let mut out = Vec::with_capacity(self.range.len());
        for item in self {
            out.push(item?);
        }
        Ok(out)
    }
}

impl<'a, Inner: TryRead<'a>> Iterator for ArrayIter<'a, Inner> {
    type Item = crate::Result<Inner::Item>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let i = self.range.next()?;
        Some(self.inner.try_read(i))
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl<'a, Inner: TryRead<'a>> ExactSizeIterator for ArrayIter<'a, Inner> {}

impl<'a, Inner> Readable<'a> for ArrayIter<'a, Inner>
where
    Inner: TryRead<'a> + TryFrom<&'a Mark<'a>> + 'a,
    Error: From<<Inner as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Reader = ColArray<'a, Inner>;
}

impl<'a, Inner: ReadSlice<'a>> ArrayIter<'a, Inner> {
    /// The contiguous backing slice for this array cell.
    #[inline]
    pub fn try_as_slice(&self) -> crate::Result<&'a [Inner::Elem]> {
        self.inner.try_read_slice(self.range.clone())
    }
}

#[derive(Clone, Copy)]
pub struct ColMap<'a, K: TryRead<'a>, V: TryRead<'a>> {
    pub offsets: &'a Offsets<'a>,
    pub keys: K,
    pub values: V,
}

impl<'a, K, V> TryFrom<&'a Mark<'a>> for ColMap<'a, K, V>
where
    K: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    V: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    Error: From<<K as TryFrom<&'a Mark<'a>>>::Error> + From<<V as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Map(m) => Ok(ColMap {
                offsets: &m.offsets,
                keys: K::try_from(m.keys.as_ref())?,
                values: V::try_from(m.values.as_ref())?,
            }),
            other => Err(Error::MismatchedType(other.as_str(), "Map")),
        }
    }
}

impl<'a, K: TryRead<'a> + 'a, V: TryRead<'a> + 'a> TryRead<'a> for ColMap<'a, K, V> {
    type Item = MapIter<'a, K, V>;
    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some((s, e)) = self.offsets.offset_indices(idx)? else {
            return Err(Error::IndexOutOfBounds(idx, "Map"));
        };
        Ok(MapIter {
            keys: self.keys,
            values: self.values,
            range: s..e,
            _marker: std::marker::PhantomData,
        })
    }
}

pub struct MapIter<'a, K: TryRead<'a>, V: TryRead<'a>> {
    keys: K,
    values: V,
    range: Range<usize>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, K: TryRead<'a>, V: TryRead<'a>> Iterator for MapIter<'a, K, V> {
    type Item = crate::Result<(K::Item, V::Item)>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let i = self.range.next()?;
        Some(
            self.keys
                .try_read(i)
                .and_then(|k| Ok((k, self.values.try_read(i)?))),
        )
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl<'a, K: TryRead<'a>, V: TryRead<'a>> ExactSizeIterator for MapIter<'a, K, V> {}

impl<'a, K, V> Readable<'a> for MapIter<'a, K, V>
where
    K: TryRead<'a> + TryFrom<&'a Mark<'a>> + 'a,
    V: TryRead<'a> + TryFrom<&'a Mark<'a>> + 'a,
    Error: From<<K as TryFrom<&'a Mark<'a>>>::Error> + From<<V as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Reader = ColMap<'a, K, V>;
}

/// Reads a `Variant(...)` column into a `T` enum implementing [`FromVariant`].
/// Normally, it should be derived via `#[derive(FromVariant)]`.
///
/// This will error on NULLs, use [`ColVariantNullable`] for nullable columns.
pub struct ColVariant<'a, T: FromVariant<'a>> {
    mark: &'a VariantMark<'a>,
    readers: T::Readers,
}

impl<'a, T: FromVariant<'a>> Clone for ColVariant<'a, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T: FromVariant<'a>> Copy for ColVariant<'a, T> {}

impl<'a, T: FromVariant<'a>> TryFrom<&'a Mark<'a>> for ColVariant<'a, T> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Variant(v) => Ok(Self {
                mark: v,
                readers: T::from_marks(&v.types)?,
            }),
            other => Err(Error::MismatchedType(other.as_str(), "Variant")),
        }
    }
}

impl<'a, T: FromVariant<'a> + 'a> TryRead<'a> for ColVariant<'a, T> {
    type Item = T;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(&discriminator) = self.mark.discriminators.get(idx) else {
            return Err(Error::IndexOutOfBounds(idx, "Variant"));
        };
        if discriminator == VariantMark::NULL_DISCRIMINATOR {
            return Err(Error::MismatchedType(
                "Null",
                "non-null Variant row (use ColVariantNullable)",
            ));
        }
        // `offsets` and `discriminators` always have the same length.
        T::read(
            &self.readers,
            discriminator as usize,
            self.mark.offsets[idx],
        )
    }
}

/// Same as [`ColVariant`] that yields `None` for NULL rows.
pub struct ColVariantNullable<'a, T: FromVariant<'a>> {
    inner: ColVariant<'a, T>,
}

impl<'a, T: FromVariant<'a>> Clone for ColVariantNullable<'a, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T: FromVariant<'a>> Copy for ColVariantNullable<'a, T> {}

impl<'a, T: FromVariant<'a>> TryFrom<&'a Mark<'a>> for ColVariantNullable<'a, T> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: ColVariant::try_from(value)?,
        })
    }
}

impl<'a, T: FromVariant<'a> + 'a> TryRead<'a> for ColVariantNullable<'a, T> {
    type Item = Option<T>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let mark = self.inner.mark;
        let Some(&discriminator) = mark.discriminators.get(idx) else {
            return Err(Error::IndexOutOfBounds(idx, "Variant"));
        };
        if discriminator == VariantMark::NULL_DISCRIMINATOR {
            return Ok(None);
        }
        T::read(
            &self.inner.readers,
            discriminator as usize,
            mark.offsets[idx],
        )
        .map(Some)
    }
}

#[derive(Clone, Copy)]
pub struct ColTuple<T>(pub T);

macro_rules! impl_col_tuple {
    ($n:literal, $($idx:tt => $t:ident),+) => {
        impl<'a, $($t,)+> TryFrom<&'a Mark<'a>> for ColTuple<($($t,)+)>
        where
            $(
                $t: TryRead<'a> + TryFrom<&'a Mark<'a>>,
                Error: From<<$t as TryFrom<&'a Mark<'a>>>::Error>,
            )+
        {
            type Error = Error;

            fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
                let tuple = match value {
                    Mark::Tuple(t) => t,
                    Mark::NamedTuple(nt) => match nt.tuple.as_ref() {
                        Mark::Tuple(t) => t,
                        other => return Err(Error::MismatchedType(other.as_str(), "Tuple")),
                    },
                    other => return Err(Error::MismatchedType(other.as_str(), "Tuple")),
                };

                if tuple.values.len() != $n {
                    return Err(Error::MismatchedType("Tuple", "Tuple with matching arity"));
                }

                Ok(Self(($($t::try_from(&tuple.values[$idx])?,)+)))
            }
        }

        impl<'a, $($t: TryRead<'a> + 'a,)+> TryRead<'a> for ColTuple<($($t,)+)> {
            type Item = ($($t::Item,)+);

            #[inline(always)]
            fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
                Ok(($(self.0.$idx.try_read(idx)?,)+))
            }
        }
    };
}

impl_col_tuple!(1, 0 => A);
impl_col_tuple!(2, 0 => A, 1 => B);
impl_col_tuple!(3, 0 => A, 1 => B, 2 => C);
impl_col_tuple!(4, 0 => A, 1 => B, 2 => C, 3 => D);
impl_col_tuple!(5, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E);
impl_col_tuple!(6, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F);
impl_col_tuple!(7, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G);
impl_col_tuple!(8, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H);
impl_col_tuple!(9, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H, 8 => I);
impl_col_tuple!(10, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H, 8 => I, 9 => J);
