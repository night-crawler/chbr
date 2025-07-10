use std::{
    collections::{HashMap, HashSet},
    iter::Peekable,
    net::{Ipv4Addr, Ipv6Addr},
    ops::Range,
};

use chrono::{NaiveDate, TimeZone};
use chrono_tz::Tz;
use uuid::Uuid;
use zerocopy::little_endian::{I32, I64, I128, U16, U32, U64};

use crate::{
    conv::{date16, date32, datetime32, datetime32_tz, datetime64_tz},
    mark::Mark,
    value::Value,
};

pub mod conv;
pub mod error;
pub mod index;
mod macros;
pub mod mark;
pub mod parse;
pub mod slice;
pub mod types;
pub mod value;

pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) trait ByteExt {
    fn rtrim_zeros(&self) -> &[u8];
}

impl ByteExt for [u8] {
    #[inline(always)]
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
/// so it should not be wildly huge. Nevertheless, if the end position exceeds `u32::MAX`,
/// we still have a good chance of not failing to convert the Range<usize> to TinyRange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TinyRange {
    pub start: u32,
    pub length: u32,
}

impl From<TinyRange> for Range<usize> {
    #[inline(always)]
    fn from(value: TinyRange) -> Self {
        Range {
            start: value.start as usize,
            end: (value.start + value.length) as usize,
        }
    }
}

impl TryFrom<Range<usize>> for TinyRange {
    type Error = Error;

    #[inline(always)]
    fn try_from(value: Range<usize>) -> std::result::Result<Self, Self::Error> {
        let start = u32::try_from(value.start)
            .map_err(|_| Error::ValueOutOfRange("usize", "u32", value.start.to_string()))?;

        let length = u32::try_from(value.end - value.start).map_err(|_| {
            Error::ValueOutOfRange("usize", "u32", (value.end - value.start).to_string())
        })?;

        Ok(TinyRange { start, length })
    }
}

#[macro_export]
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
            $vis struct $name(pub $inner);
        )+
    };
}

#[macro_export]
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
    pub UuidData([U64; 2]);
    pub Ipv4Data (U32);
    pub Ipv6Data ([u8; 16]);
    pub Date16Data (U16);
    pub Date32Data (I32);
    pub DateTime32Data (U32);
    pub DateTime64Data (I64);
    pub Decimal32Data (I32);
    pub Decimal64Data (I64);
    pub Decimal128Data (I128);
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
impl_from!(Date16Data => NaiveDate, |d| date16(d.0.get()));
impl_from!(Date32Data => NaiveDate, |d| date32(d.0.get()));
impl_from!(DateTime32Data => chrono::DateTime<chrono::Utc>, |d| datetime32(d.0.get()));

impl DateTime64Data {
    #[inline(always)]
    pub fn with_tz_and_precision(&self, tz: Tz, precision: u8) -> Option<chrono::DateTime<Tz>> {
        datetime64_tz(self.0.get(), precision, tz)
    }
}

impl DateTime32Data {
    #[inline(always)]
    pub fn with_tz(&self, tz: Tz) -> chrono::DateTime<Tz> {
        datetime32_tz(self.0.get(), tz)
    }
}

impl Decimal32Data {
    #[inline(always)]
    pub fn with_precision(&self, precision: u8) -> rust_decimal::Decimal {
        let value = self.0.get();
        rust_decimal::Decimal::new(i64::from(value), u32::from(precision))
    }
}

impl Decimal64Data {
    #[inline(always)]
    pub fn with_precision(&self, precision: u8) -> rust_decimal::Decimal {
        let value = self.0.get();
        rust_decimal::Decimal::new(value, u32::from(precision))
    }
}

impl Decimal128Data {
    #[inline(always)]
    pub fn with_precision(&self, precision: u8) -> Result<rust_decimal::Decimal> {
        let value = self.0.get();
        let value = rust_decimal::Decimal::try_from_i128_with_scale(value, u32::from(precision))
            .map_err(|_| Error::Overflow(value.to_string()))?;
        Ok(value)
    }
}

pub struct ParsedBlock<'a> {
    pub markers: Vec<Mark<'a>>,
    pub col_names: Vec<&'a str>,
    pub num_rows: usize,
}

impl ParsedBlock<'_> {
    fn reorder(&mut self, order: &HashMap<&str, usize>) -> Result<()> {
        let num_cols = self.col_names.len();
        let col_names = std::mem::replace(&mut self.col_names, Vec::with_capacity(num_cols));
        let markers = std::mem::replace(&mut self.markers, Vec::with_capacity(num_cols));

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
            let present_columns = triples
                .iter()
                .map(|(name, _, _)| *name)
                .collect::<HashSet<_>>();
            let mut missing = order.keys().copied().collect::<HashSet<_>>();
            missing.retain(|name| !present_columns.contains(name));

            return Err(Error::InvalidColumnOrder(format!(
                "Got unexpected columns: {missing:?}"
            )));
        }

        triples.sort_unstable_by_key(|(_, _, sort_key)| *sort_key);

        for (col_name, marker, _) in triples {
            self.col_names.push(col_name);
            self.markers.push(marker);
        }

        Ok(())
    }
}

pub struct BlocksIterator<'a> {
    blocks: Peekable<std::slice::Iter<'a, ParsedBlock<'a>>>,
    block_row: usize,
}

impl<'a> BlocksIterator<'a> {
    #[inline]
    pub fn new(blocks: &'a [ParsedBlock<'a>]) -> Self {
        Self {
            blocks: blocks.iter().peekable(),
            block_row: 0,
        }
    }

    pub fn new_ordered(blocks: &'a mut [ParsedBlock<'a>], order: &[&str]) -> Result<Self> {
        let order_map = order
            .iter()
            .enumerate()
            .map(|(index, name)| (*name, index))
            .collect::<HashMap<_, _>>();
        for block in blocks.iter_mut() {
            block.reorder(&order_map)?;
        }

        Ok(Self {
            blocks: blocks.iter().peekable(),
            block_row: 0,
        })
    }
}

pub struct BlockRow<'a> {
    col_names: &'a [&'a str],
    cols: &'a [Mark<'a>],
    col_index: usize,
    row_index: usize,
}

impl<'a> BlockRow<'a> {
    pub fn cols(&self) -> &'a [Mark<'a>] {
        self.cols
    }

    pub fn col_names(&self) -> &'a [&'a str] {
        self.col_names
    }

    pub fn row_index(&self) -> usize {
        self.row_index
    }

    pub fn col_index(&self) -> usize {
        self.col_index
    }
}

impl<'a> Iterator for BlockRow<'a> {
    type Item = (&'a str, ColumnAccessor<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let col_name = self.col_names.get(self.col_index)?;
        let marker = self.cols.get(self.col_index)?;

        self.col_index += 1;

        Some((
            col_name,
            ColumnAccessor {
                col_name,
                marker,
                row_index: self.row_index,
            },
        ))
    }
}

impl<'a> Iterator for BlocksIterator<'a> {
    type Item = BlockRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let block = self.blocks.peek()?;
            if self.block_row >= block.num_rows {
                self.blocks.next();
                self.block_row = 0;
                continue;
            }

            let block_row = BlockRow {
                col_names: &block.col_names,
                cols: &block.markers,
                col_index: 0,
                row_index: self.block_row,
            };
            self.block_row += 1;

            break Some(block_row);
        }
    }
}

pub struct ColumnAccessor<'a> {
    pub col_name: &'a str,
    pub marker: &'a Mark<'a>,
    row_index: usize,
}

/// Provides access to the column value and allows to avoid constructing new
/// Value instances. For small types it can have a large performance impact.
impl<'a> ColumnAccessor<'a> {
    #[inline]
    pub fn get(self) -> Value<'a> {
        // row index is private and created by us, so it should always be valid, thus safe
        // to unwrap
        self.marker.get(self.row_index).unwrap()
    }

    #[inline]
    pub fn into_str(self) -> Result<&'a str> {
        let str = self.marker.get_str(self.row_index)?;
        Ok(str.unwrap())
    }

    #[inline]
    pub fn into_opt_str(self) -> Result<Option<&'a str>> {
        let str = self.marker.get_opt_str(self.row_index)?;
        Ok(str.unwrap())
    }

    #[inline]
    pub fn into_datetime<T: TimeZone>(self, tz: T) -> Result<chrono::DateTime<T>> {
        let dt = self.marker.get_datetime(self.row_index, tz)?;
        Ok(dt.unwrap())
    }

    #[inline]
    pub fn into_uuid(self) -> Result<Uuid> {
        let uuid = self.marker.get_uuid(self.row_index)?;
        Ok(uuid.unwrap())
    }

    #[inline]
    pub fn into_ipv4(self) -> Result<Ipv4Addr> {
        let ipv4 = self.marker.get_ipv4(self.row_index)?;
        Ok(ipv4.unwrap())
    }

    #[inline]
    pub fn into_ipv6(self) -> Result<Ipv6Addr> {
        let ipv6 = self.marker.get_ipv6(self.row_index)?;
        Ok(ipv6.unwrap())
    }

    #[inline]
    pub fn into_opt_ipv6(self) -> Result<Option<Ipv6Addr>> {
        let ipv6 = self.marker.get_opt_ipv6(self.row_index)?;
        Ok(ipv6.unwrap())
    }

    #[inline]
    pub fn into_bool(self) -> Result<bool> {
        let value = self.marker.get_bool(self.row_index)?;
        Ok(value.unwrap())
    }

    #[inline]
    pub fn into_f64(self) -> Result<f64> {
        let value = self.marker.get_f64(self.row_index)?;
        Ok(value.unwrap())
    }

    #[inline]
    pub fn into_array_lc_strs(self) -> Result<impl Iterator<Item = &'a str>> {
        let it = self.marker.get_array_lc_strs(self.row_index)?.unwrap();
        Ok(it.into_iter())
    }
}

pub fn iter_blocks<'a>(blocks: &'a [ParsedBlock]) -> BlocksIterator<'a> {
    BlocksIterator::new(blocks)
}

pub fn iter_blocks_ordered<'a>(
    blocks: &'a mut [ParsedBlock<'a>],
    order: &[&str],
) -> Result<BlocksIterator<'a>> {
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
