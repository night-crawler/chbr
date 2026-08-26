use std::hint::cold_path;
use std::ops::Range;

mod composite;
mod json;
mod scalar;
#[cfg(test)]
mod tests;

pub use composite::*;
pub use json::*;
pub use scalar::*;

/// Reads values by index from an underlying storage, i.e., from a [`crate::Mark`].
///
/// Indexes are interpreted within the context for which this trait is implemented. For example,
/// for simple cases it reads from a transparent Mark wrapper, otherwise - from unpacked Mark.
pub trait TryRead<'a>: Copy {
    type Item;

    fn try_read(&self, idx: usize) -> crate::Result<Self::Item>;
}

/// Constructs a reader over selected columns of one [`crate::ParsedBlock`].
///
/// `block.col_names[i]` names `block.markers[i]`. `from_block` selects the
/// required `Mark`s and builds `Self` from their readers.
///
/// `try_read(row)` passes the same block row index to every selected reader.
/// It combines their items into `Self::Item`.
pub trait FromBlock<'a>: TryRead<'a> {
    /// Looks up a [`crate::Mark`] by name in the [`crate::ParsedBlock`] and constructs the instance
    /// if it can.
    fn from_block(block: &'a crate::ParsedBlock<'a>) -> crate::Result<Self>;

    #[inline]
    fn rows(block: &'a crate::ParsedBlock<'a>) -> crate::Result<RowsIter<'a, Self>> {
        Ok(RowsIter::new(Self::from_block(block)?, block.num_rows))
    }

    /// Iterates all rows of all `blocks` as one flat stream.
    #[inline]
    fn iter_blocks(blocks: &'a [crate::ParsedBlock<'a>]) -> BlocksRows<'a, Self> {
        BlocksRows {
            blocks: blocks.iter(),
            rows_iter: None,
        }
    }
}

/// Provides direct access to an underlying slice. Implemented for types where it makes sense,
/// i.e., arrays of u128.
///
/// Exists because an element-wise `try_read` loop adds bounds checks with result handling branch
/// per each value, so it can't be vectorized.
pub trait ReadSlice<'a>: TryRead<'a> {
    type Elem;

    /// Checks the requested range once and returns the slice as is.
    fn try_read_slice(&self, range: Range<usize>) -> crate::Result<&'a [Self::Elem]>;
}

/// Constructs instances for [`Variant`] and [`VariantNullable`].
///
/// ClickHouse stores variants in canonical type order.
/// Declared enum variants must use the same order.
/// The discriminator contains an index, not a type name.
pub trait FromVariant<'a>: Sized {
    /// One reader for each entry in [`crate::mark::Variant::types`], in the same order.
    type Readers: Copy;

    /// Builds the readers from [`crate::mark::Variant::types`].
    ///
    /// `marks[i]` initializes the reader for enum variant `i`.
    fn from_marks(marks: &'a [crate::mark::Mark<'a>]) -> crate::Result<Self::Readers>;

    /// Constructs the enum variant selected by `discriminator`.
    ///
    /// [`Variant::try_read`] passes `v.discriminators[row]` as `discriminator` and
    /// `v.offsets[row]` as `idx`.
    ///
    /// The implementation passes `idx` to the selected reader. It wraps the
    /// returned item in the enum variant at the same position.
    fn read(readers: &Self::Readers, discriminator: usize, idx: usize) -> crate::Result<Self>;
}

/// Defines the default reader for a field in a `#[derive(FromVariant)]` enum. Generally, allows
/// setting the inverse relation between a rust type and a corresponding reader, for examples see
/// [`Str`].
///
/// For [`crate::Mark::Variant`], enum variant `i` corresponds to the i-th type.
/// Without `#[col(reader = ...)]`, the derive uses the default `<FieldType as Readable>::Reader`
/// for that child [`crate::Mark`].
///
/// For example, `Readable<'a> for &'a str` defines `ColStr<'a>` as its default
/// reader. A variant `Text(&'a str)` therefore uses `ColStr<'a>`.
///
/// You need to specify the reader explicitly (`#[col(reader = ...)]`) when the corresponding
/// child [`crate::Mark`] needs a `FixedString`, `Enum8`, or `Enum16` reader.
pub trait Readable<'a>: Sized {
    type Reader: TryRead<'a, Item = Self>
        + TryFrom<&'a crate::mark::Mark<'a>, Error = crate::error::Error>;
}

pub struct RowsIter<'a, R: TryRead<'a>> {
    reader: R,
    range: Range<usize>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, R: TryRead<'a>> RowsIter<'a, R> {
    #[inline]
    pub const fn new(reader: R, num_rows: usize) -> Self {
        Self {
            reader,
            range: 0..num_rows,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, R: TryRead<'a>> Iterator for RowsIter<'a, R> {
    type Item = crate::Result<R::Item>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let i = self.range.next()?;
        Some(self.reader.try_read(i))
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl<'a, R: TryRead<'a>> ExactSizeIterator for RowsIter<'a, R> {}

/// Flat row iterator over multiple blocks; see [`FromBlock::iter_blocks`].
pub struct BlocksRows<'a, R: FromBlock<'a>> {
    blocks: std::slice::Iter<'a, crate::ParsedBlock<'a>>,
    rows_iter: Option<RowsIter<'a, R>>,
}

impl<'a, R: FromBlock<'a>> Iterator for BlocksRows<'a, R> {
    type Item = crate::Result<R::Item>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = &mut self.rows_iter
                && let Some(item) = current.next()
            {
                return Some(item);
            }
            let block = self.blocks.next()?;
            match R::rows(block) {
                Ok(rows) => self.rows_iter = Some(rows),
                Err(err) => {
                    cold_path();
                    return Some(Err(err));
                }
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let current = match &self.rows_iter {
            Some(it) => it.size_hint().0,
            None => 0,
        };
        let remaining: usize = self.blocks.clone().map(|b| b.num_rows).sum();
        (current, Some(current + remaining))
    }
}
