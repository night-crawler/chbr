use super::{FromVariant, ReadSlice, Readable, Str, TryRead};
use crate::error::Error;
use crate::mark::{Mark, Variant as VariantMark};
use crate::types::{OffsetIndexPair as _, Offsets};
use chbr::zc;
use std::hint::cold_path;
use std::ops::Range;

#[derive(Clone, Copy)]
pub struct Nullable<'a, Inner> {
    pub mask: &'a [u8],
    pub inner: Inner,
}

impl<'a, Inner> TryFrom<&'a Mark<'a>> for Nullable<'a, Inner>
where
    Inner: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    Error: From<<Inner as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Nullable(n) => Ok(Nullable {
                mask: n.mask,
                inner: Inner::try_from(n.data.as_ref())?,
            }),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "Nullable"))
            }
        }
    }
}

impl<'a, Inner: TryRead<'a> + 'a> TryRead<'a> for Nullable<'a, Inner> {
    type Item = Option<Inner::Item>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(mask) = self.mask.get(idx) else {
            cold_path();
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
    U16(&'a [zc::U16]),
    U32(&'a [zc::U32]),
    U64(&'a [zc::U64]),
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
                cold_path();
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
pub struct Lc<'a, Inner> {
    indices: LcIndices<'a>,
    dict: Option<Inner>,
}

pub type LcStr<'a> = Lc<'a, Str<'a>>;

impl<'a, Inner> TryFrom<&'a Mark<'a>> for Lc<'a, Inner>
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
                Ok(Lc { indices, dict })
            }
            Mark::LowCardinality(_) => {
                cold_path();
                Err(Error::MismatchedType(
                    "LowCardinality(Nullable)",
                    "LowCardinality",
                ))
            }
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "LowCardinality"))
            }
        }
    }
}

impl<'a, Inner: TryRead<'a> + 'a> TryRead<'a> for Lc<'a, Inner> {
    type Item = Inner::Item;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value_index) = self.indices.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "LowCardinality"));
        };
        let Some(dict) = self.dict.as_ref() else {
            cold_path();
            return Err(Error::CorruptedData(
                "LowCardinality dictionary is missing".to_owned(),
            ));
        };
        dict.try_read(value_index)
    }
}

#[derive(Clone, Copy)]
pub struct LcNullable<'a, Inner> {
    indices: LcIndices<'a>,
    dict: Option<Inner>,
}

pub type LcNullableStr<'a> = LcNullable<'a, Str<'a>>;

impl<'a, Inner> TryFrom<&'a Mark<'a>> for LcNullable<'a, Inner>
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
                Ok(LcNullable { indices, dict })
            }
            Mark::LowCardinality(_) => {
                cold_path();
                Err(Error::MismatchedType(
                    "LowCardinality",
                    "LowCardinality(Nullable)",
                ))
            }
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "LowCardinality"))
            }
        }
    }
}

impl<'a, Inner: TryRead<'a> + 'a> TryRead<'a> for LcNullable<'a, Inner> {
    type Item = Option<Inner::Item>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(value_index) = self.indices.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "LowCardinality"));
        };
        if value_index == 0 {
            return Ok(None);
        }
        let Some(dict) = self.dict.as_ref() else {
            cold_path();
            return Err(Error::CorruptedData(
                "LowCardinality dictionary is missing".to_owned(),
            ));
        };
        Ok(Some(dict.try_read(value_index)?))
    }
}

#[derive(Clone, Copy)]
pub struct Array<'a, Inner: TryRead<'a>> {
    pub offsets: &'a Offsets<'a>,
    pub values: Inner,
}

impl<'a, Inner> TryFrom<&'a Mark<'a>> for Array<'a, Inner>
where
    Inner: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    Error: From<<Inner as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Array(arr) => Ok(Array {
                offsets: &arr.offsets,
                values: Inner::try_from(arr.values.as_ref())?,
            }),
            // `Nested(...)` is stored as an array of tuples.
            Mark::Nested(n) => Self::try_from(n.array_of_tuples.as_ref()),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "Array"))
            }
        }
    }
}

impl<'a, Inner: TryRead<'a> + 'a> TryRead<'a> for Array<'a, Inner> {
    type Item = ArrayIter<'a, Inner>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some((s, e)) = self.offsets.offset_indices(idx)? else {
            cold_path();
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
    #[inline]
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
    type Reader = Array<'a, Inner>;
}

impl<'a, Inner: ReadSlice<'a>> ArrayIter<'a, Inner> {
    /// The contiguous backing slice for this array cell.
    #[inline]
    pub fn try_as_slice(&self) -> crate::Result<&'a [Inner::Elem]> {
        self.inner.try_read_slice(self.range.clone())
    }
}

#[derive(Clone, Copy)]
pub struct Map<'a, K: TryRead<'a>, V: TryRead<'a>> {
    pub offsets: &'a Offsets<'a>,
    pub keys: K,
    pub values: V,
}

impl<'a, K, V> TryFrom<&'a Mark<'a>> for Map<'a, K, V>
where
    K: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    V: TryRead<'a> + TryFrom<&'a Mark<'a>>,
    Error: From<<K as TryFrom<&'a Mark<'a>>>::Error> + From<<V as TryFrom<&'a Mark<'a>>>::Error>,
{
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Map(m) => Ok(Map {
                offsets: &m.offsets,
                keys: K::try_from(m.keys.as_ref())?,
                values: V::try_from(m.values.as_ref())?,
            }),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "Map"))
            }
        }
    }
}

impl<'a, K: TryRead<'a> + 'a, V: TryRead<'a> + 'a> TryRead<'a> for Map<'a, K, V> {
    type Item = MapIter<'a, K, V>;
    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some((s, e)) = self.offsets.offset_indices(idx)? else {
            cold_path();
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
    type Reader = Map<'a, K, V>;
}

/// Reads a `Variant(...)` column into a `T` enum implementing [`FromVariant`].
/// Normally, it should be derived via `#[derive(FromVariant)]`.
///
/// This will error on NULLs, use [`VariantNullable`] for nullable columns.
pub struct Variant<'a, T: FromVariant<'a>> {
    mark: &'a VariantMark<'a>,
    readers: T::Readers,
}

impl<'a, T: FromVariant<'a>> Clone for Variant<'a, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T: FromVariant<'a>> Copy for Variant<'a, T> {}

impl<'a, T: FromVariant<'a>> TryFrom<&'a Mark<'a>> for Variant<'a, T> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        match value {
            Mark::Variant(v) => Ok(Self {
                mark: v,
                readers: T::from_marks(&v.types)?,
            }),
            other => {
                cold_path();
                Err(Error::MismatchedType(other.as_str(), "Variant"))
            }
        }
    }
}

impl<'a, T: FromVariant<'a> + 'a> TryRead<'a> for Variant<'a, T> {
    type Item = T;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let Some(&discriminator) = self.mark.discriminators.get(idx) else {
            cold_path();
            return Err(Error::IndexOutOfBounds(idx, "Variant"));
        };
        if discriminator == VariantMark::NULL_DISCRIMINATOR {
            cold_path();
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

/// Same as [`Variant`] that yields `None` for NULL rows.
pub struct VariantNullable<'a, T: FromVariant<'a>> {
    inner: Variant<'a, T>,
}

impl<'a, T: FromVariant<'a>> Clone for VariantNullable<'a, T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T: FromVariant<'a>> Copy for VariantNullable<'a, T> {}

impl<'a, T: FromVariant<'a>> TryFrom<&'a Mark<'a>> for VariantNullable<'a, T> {
    type Error = Error;

    fn try_from(value: &'a Mark<'a>) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: Variant::try_from(value)?,
        })
    }
}

impl<'a, T: FromVariant<'a> + 'a> TryRead<'a> for VariantNullable<'a, T> {
    type Item = Option<T>;

    #[inline(always)]
    fn try_read(&self, idx: usize) -> crate::Result<Self::Item> {
        let mark = self.inner.mark;
        let Some(&discriminator) = mark.discriminators.get(idx) else {
            cold_path();
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
pub struct Tuple<T>(pub T);

macro_rules! impl_col_tuple {
    ($n:literal, $($idx:tt => $t:ident),+) => {
        impl<'a, $($t,)+> TryFrom<&'a Mark<'a>> for Tuple<($($t,)+)>
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
                        other => {
                            cold_path();
                            return Err(Error::MismatchedType(other.as_str(), "Tuple"));
                        }
                    },
                    other => {
                        cold_path();
                        return Err(Error::MismatchedType(other.as_str(), "Tuple"));
                    }
                };

                if tuple.values.len() != $n {
                    cold_path();
                    return Err(Error::MismatchedType("Tuple", "Tuple with matching arity"));
                }

                Ok(Self(($($t::try_from(&tuple.values[$idx])?,)+)))
            }
        }

        impl<'a, $($t: TryRead<'a> + 'a,)+> TryRead<'a> for Tuple<($($t,)+)> {
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
