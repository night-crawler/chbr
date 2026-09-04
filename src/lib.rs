extern crate self as chbr;

use std::hint::cold_path;

use chrono::NaiveDate;
use chrono_tz::Tz;
use log::debug;
use std::collections::HashSet;
use std::{
    collections::HashMap,
    iter::Peekable,
    net::{Ipv4Addr, Ipv6Addr},
    ops::Range,
};
use uuid::Uuid;

pub(crate) mod conv;
pub mod error;
mod macros;
pub mod mark;
pub mod parse;
pub mod reader;
pub mod slice;
pub(crate) mod types;
pub mod value;
pub mod zc;

pub use bstr::BStr;
pub use chbr_derive::{FromBlock, FromVariant};
pub use error::Error;
// Same name as the derive macro on purpose (macro vs type namespace):
// `use crate::FromBlock;` imports both, serde-style.
pub use reader::{FromBlock, FromVariant};

pub type Result<T> = std::result::Result<T, Error>;

fn mark_by_name<'a, T>(col_names: &[&str], columns: &'a [T], name: &str) -> Result<&'a T> {
    let column = col_names
        .iter()
        .zip(columns)
        .find_map(|(column_name, column)| (*column_name == name).then_some(column));
    match column {
        Some(column) => Ok(column),
        None => {
            cold_path();
            Err(Error::ColumnNotFound(name.to_owned()))
        }
    }
}

pub(crate) trait ByteExt {
    fn rtrim_zeros(&self) -> &[u8];
}

impl ByteExt for [u8] {
    fn rtrim_zeros(&self) -> &[u8] {
        let mut end = self.len();
        while end > 0 && self[end - 1] == 0 {
            end -= 1;
        }
        &self[..end]
    }
}

/// This range represents a starting offset and a length, as opposed to the
/// Rust's range, which stores start and end positions.
/// In particular, this range encodes row numbers/offsets within a ClickHouse block,
/// so it should not be wildly huge. Nevertheless, if the end position exceeds [`u32::MAX`],
/// we still have a good chance of not failing to convert the [`Range<usize>`] to [`TinyRange`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TinyRange {
    pub start: u32,
    pub length: u32,
}

impl From<TinyRange> for Range<usize> {
    fn from(value: TinyRange) -> Self {
        let start = value.start as usize;
        Range {
            start,
            end: start + value.length as usize,
        }
    }
}

impl TryFrom<Range<usize>> for TinyRange {
    type Error = Error;

    fn try_from(value: Range<usize>) -> std::result::Result<Self, Self::Error> {
        let Ok(start) = u32::try_from(value.start) else {
            cold_path();
            return Err(Error::ValueOutOfRange(
                "usize",
                "u32",
                value.start.to_string(),
            ));
        };

        let Some(raw_length) = value.end.checked_sub(value.start) else {
            cold_path();
            return Err(Error::ValueOutOfRange(
                "Range<usize>",
                "TinyRange",
                format!("{}..{}", value.start, value.end),
            ));
        };
        let Ok(length) = u32::try_from(raw_length) else {
            cold_path();
            return Err(Error::ValueOutOfRange(
                "usize",
                "u32",
                raw_length.to_string(),
            ));
        };

        Ok(TinyRange { start, length })
    }
}

macro_rules! transparent_newtype {
    ( $( $vis:vis $name:ident ( $inner:ty ) ; )+ ) => {
        $(
            #[repr(transparent)]
            #[derive(
                Clone,
                Copy,
                Eq,
                Hash,
                Ord,
                PartialEq,
                PartialOrd,
                Debug,
                Default,
                zerocopy::FromBytes,
                zerocopy::Unaligned,
            )]
            $vis struct $name(pub(crate) $inner);
        )+
    };
}

macro_rules! impl_from {
    ( $src:ty => $dst:ty , |$v:ident| $body:expr ) => {
        impl From<$src> for $dst {
            #[inline]
            fn from($v: $src) -> Self {
                $body
            }
        }
    };
}

transparent_newtype! {
    pub I256 ([u8; 32]);
    pub U256 ([u8; 32]);
    pub UuidData([zc::U64; 2]);
    pub Ipv4Data (zc::U32);
    pub Ipv6Data ([u8; 16]);
    pub Date16Data (zc::U16);
    pub Date32Data (zc::I32);
    pub DateTime32Data (zc::U32);
    pub DateTime64Data (zc::I64);
    pub Decimal32Data (zc::I32);
    pub Decimal64Data (zc::I64);
    pub Decimal128Data (zc::I128);
    pub Decimal256Data (I256);
    pub Bf16Data ([u8; 2]);
}

impl_from!(Bf16Data => half::bf16, |value| half::bf16::from_le_bytes(value.0));
impl_from!(Ipv6Data => Ipv6Addr, |d| Ipv6Addr::from(d.0));
impl_from!(Ipv4Data => Ipv4Addr, |d| Ipv4Addr::from(d.0.get()));
impl_from!(UuidData => Uuid, |d| {
    let [hi, lo] = d.0;
    Uuid::from_u64_pair(hi.get(), lo.get())
});
impl_from!(Date16Data => NaiveDate, |d| conv::date16(d.0.get()));
impl_from!(Date32Data => NaiveDate, |d| conv::date32(d.0.get()));
impl_from!(DateTime32Data => chrono::DateTime<chrono::Utc>, |d| conv::datetime32(d.0.get()));

impl DateTime64Data {
    pub(crate) fn with_tz_and_precision(
        &self,
        tz: Tz,
        precision: u8,
    ) -> Result<chrono::DateTime<Tz>> {
        conv::datetime64_tz(self.0.get(), precision, tz)
    }
}

impl DateTime32Data {
    #[inline(always)]
    pub(crate) fn with_tz(&self, tz: Tz) -> chrono::DateTime<Tz> {
        conv::datetime32_tz(self.0.get(), tz)
    }
}

impl Decimal32Data {
    pub(crate) fn with_scale(&self, scale: u8) -> rust_decimal::Decimal {
        let value = self.0.get();
        rust_decimal::Decimal::new(i64::from(value), u32::from(scale))
    }
}

impl Decimal64Data {
    pub(crate) fn with_scale(&self, scale: u8) -> rust_decimal::Decimal {
        let value = self.0.get();
        rust_decimal::Decimal::new(value, u32::from(scale))
    }
}

impl Decimal128Data {
    pub(crate) fn with_scale(&self, scale: u8) -> Result<rust_decimal::Decimal> {
        if u32::from(scale) > rust_decimal::Decimal::MAX_SCALE {
            cold_path();
            return Err(Error::NotImplemented(format!(
                "Decimal128 with scale {scale} (rust_decimal supports at most {})",
                rust_decimal::Decimal::MAX_SCALE
            )));
        }
        let value = self.0.get();
        match rust_decimal::Decimal::try_from_i128_with_scale(value, u32::from(scale)) {
            Ok(value) => Ok(value),
            Err(_) => {
                cold_path();
                Err(Error::Overflow(value.to_string()))
            }
        }
    }
}

pub struct ParsedBlock<'a> {
    pub markers: Box<[mark::Mark<'a>]>,
    pub col_names: Box<[&'a str]>,
    pub num_rows: usize,
}

impl<'a> ParsedBlock<'a> {
    pub fn mark(&self, name: &str) -> Result<&mark::Mark<'a>> {
        mark_by_name(&self.col_names, &self.markers, name)
    }

    fn reorder(&mut self, order: &HashMap<&str, usize>) -> Result<()> {
        let num_cols = self.col_names.len();
        let col_names = std::mem::take(&mut self.col_names).into_iter();
        let markers = std::mem::take(&mut self.markers).into_iter();

        let mut triples = Vec::with_capacity(num_cols);
        let mut num_used = 0;
        for (index, (col_name, marker)) in col_names.into_iter().zip(markers).enumerate() {
            let sort_key = if let Some(key) = order.get(col_name).copied() {
                num_used += 1;
                key
            } else {
                // if the column is not in the order, we put it at the end
                num_cols + index
            };
            triples.push((col_name, marker, sort_key));
        }

        if num_used < order.len() {
            cold_path();
            let present_columns = triples
                .iter()
                .map(|(name, _, _)| *name)
                .collect::<HashSet<_>>();
            let mut missing = order.keys().copied().collect::<HashSet<_>>();
            missing.retain(|name| !present_columns.contains(name));

            return Err(Error::InvalidColumnOrder(format!(
                "Got unexpected columns: {missing:?}; present: {present_columns:?}"
            )));
        }

        triples.sort_unstable_by_key(|(_, _, sort_key)| *sort_key);

        let mut col_names = Vec::with_capacity(num_cols);
        let mut markers = Vec::with_capacity(num_cols);
        for (col_name, marker, _) in triples {
            col_names.push(col_name);
            markers.push(marker);
        }
        self.col_names = col_names.into_boxed_slice();
        self.markers = markers.into_boxed_slice();

        Ok(())
    }
}

#[derive(Clone)]
pub struct BlocksIterator<'data: 'iter, 'iter> {
    blocks: Peekable<std::slice::Iter<'iter, ParsedBlock<'data>>>,
    block_row: usize,
}

impl<'data, 'iter> BlocksIterator<'data, 'iter> {
    pub fn new(blocks: &'iter [ParsedBlock<'data>]) -> Self {
        Self {
            blocks: blocks.iter().peekable(),
            block_row: 0,
        }
    }

    pub fn new_ordered(blocks: &'iter mut [ParsedBlock<'data>], order: &[&str]) -> Result<Self> {
        reorder_block_cols(blocks, order)?;
        Ok(Self {
            blocks: blocks.iter().peekable(),
            block_row: 0,
        })
    }
}

pub(crate) fn reorder_block_cols(blocks: &mut [ParsedBlock<'_>], order: &[&str]) -> Result<()> {
    let order_map = order
        .iter()
        .enumerate()
        .map(|(index, name)| (*name, index))
        .collect::<HashMap<_, _>>();
    for block in blocks.iter_mut() {
        block.reorder(&order_map)?;
    }

    if let Some(first) = blocks.first() {
        debug!("reordered: {:?}", first.col_names);
    }

    Ok(())
}

pub struct BlockRow<'data: 'iter, 'iter> {
    col_names: &'iter [&'data str],
    cols: &'iter [mark::Mark<'data>],
    row_index: usize,
}

impl<'data, 'iter> BlockRow<'data, 'iter> {
    pub const fn cols(&self) -> &'iter [mark::Mark<'data>] {
        self.cols
    }

    pub const fn col_names(&self) -> &'iter [&'data str] {
        self.col_names
    }

    pub const fn row_index(&self) -> usize {
        self.row_index
    }
}

impl<'data, 'iter> Iterator for BlocksIterator<'data, 'iter> {
    type Item = BlockRow<'data, 'iter>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let block = *self.blocks.peek()?;
            if self.block_row >= block.num_rows {
                self.blocks.next();
                self.block_row = 0;
                continue;
            }

            let block_row = BlockRow {
                col_names: &block.col_names,
                cols: &block.markers,
                row_index: self.block_row,
            };
            self.block_row += 1;

            break Some(block_row);
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut blocks = self.blocks.clone();
        let mut remaining = match blocks.next() {
            Some(block) => block.num_rows.saturating_sub(self.block_row),
            None => 0,
        };
        for block in blocks {
            remaining += block.num_rows;
        }
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for BlocksIterator<'_, '_> {}

pub fn iter_blocks<'data, 'iter>(
    blocks: &'iter [ParsedBlock<'data>],
) -> BlocksIterator<'data, 'iter> {
    BlocksIterator::new(blocks)
}

pub fn iter_blocks_ordered<'data, 'iter>(
    blocks: &'iter mut [ParsedBlock<'data>],
    order: &[&str],
) -> Result<BlocksIterator<'data, 'iter>> {
    BlocksIterator::new_ordered(blocks, order)
}

#[cfg(test)]
pub(crate) mod common {
    use std::{io::Read as _, path::Path, sync::Once};

    use log::LevelFilter;

    static INIT: Once = Once::new();

    pub fn init_logger() {
        INIT.call_once(|| {
            use std::io::Write as _;
            env_logger::builder()
                .format(|buf, record| {
                    writeln!(
                        buf,
                        "{} [{:<5}] {}:{} {}",
                        buf.timestamp_millis(),
                        record.level(),
                        record.file().unwrap_or("<unknown>"),
                        record.line().unwrap_or(0),
                        record.args()
                    )
                })
                .filter_level(LevelFilter::Debug)
                .is_test(true)
                .init();
        });
    }

    pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Vec<u8>> {
        init_logger();
        let mut file = std::fs::File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slice::ByteView;

    fn block<'a>(names: &[&'a str], cells: &'a [u8]) -> ParsedBlock<'a> {
        let markers = cells
            .iter()
            .map(|cell| mark::Mark::UInt8(ByteView::try_from(std::slice::from_ref(cell)).unwrap()))
            .collect();
        ParsedBlock {
            markers,
            col_names: names.into(),
            num_rows: 1,
        }
    }

    fn cells(block: &ParsedBlock<'_>) -> Vec<u8> {
        block
            .markers
            .iter()
            .map(|mark| mark.get_u8(0).unwrap().unwrap())
            .collect()
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn tiny_range_round_trips_when_end_exceeds_u32_max() -> Result<()> {
        let range = (u32::MAX as usize - 1)..(u32::MAX as usize + 10);
        let tiny = TinyRange::try_from(range.clone())?;
        assert_eq!(tiny, TinyRange { start: u32::MAX - 1, length: 11 });
        assert_eq!(Range::<usize>::from(tiny), range);
        Ok(())
    }

    #[test]
    fn reorder_moves_markers_with_names_and_keeps_unrequested_tail() -> Result<()> {
        let mut blocks = [block(&["a", "b", "c", "d", "e"], &[0, 1, 2, 3, 4])];
        reorder_block_cols(&mut blocks, &["e", "c", "a"])?;
        assert_eq!(*blocks[0].col_names, ["e", "c", "a", "b", "d"]);
        assert_eq!(cells(&blocks[0]), [4, 2, 0, 1, 3]);
        Ok(())
    }

    #[test]
    fn reorder_reports_missing_requested_columns() {
        let mut blocks = [block(&["a", "b"], &[0, 1])];
        let err = reorder_block_cols(&mut blocks, &["b", "zzz"]).unwrap_err();
        assert!(
            matches!(&err, Error::InvalidColumnOrder(msg) if msg.contains("zzz")),
            "{err}"
        );
    }

    #[test]
    fn decimal128_unsupported_scale_is_not_implemented() {
        let data = Decimal128Data(zc::I128::new(1));
        for scale in [29u8, 38] {
            let err = data.with_scale(scale).unwrap_err();
            assert!(
                matches!(&err, Error::NotImplemented(msg) if msg.contains(&format!("scale {scale}"))),
                "{err}"
            );
        }
        assert_eq!(
            data.with_scale(28).unwrap(),
            rust_decimal::Decimal::try_from_i128_with_scale(1, 28).unwrap()
        );
    }

    #[test]
    fn decimal128_value_overflow_stays_overflow() {
        let data = Decimal128Data(zc::I128::new(i128::MAX));
        let err = data.with_scale(0).unwrap_err();
        assert!(matches!(err, Error::Overflow(_)), "{err}");
    }
}
