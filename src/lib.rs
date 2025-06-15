use crate::conv::{date16, date32, datetime32, datetime32_tz, datetime64_tz};
use crate::mark::Mark;
use crate::value::Value;
use chrono::NaiveDate;
use chrono_tz::Tz;
use std::iter::Peekable;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::ops::Range;
use uuid::Uuid;
use zerocopy::little_endian::{I32, I64, I128, U16, U32, U64};

pub mod conv;
pub mod error;
pub mod index;
pub mod mark;
pub mod parse;
pub mod slice;
pub mod types;
pub mod value;

#[macro_export]
macro_rules! transparent_newtype {
    ( $( $vis:vis $name:ident ( $inner:ty ) ; )+ ) => {
        $(
            #[allow(non_camel_case_types)]
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
    pub i256 ([u8; 32]);
    pub u256 ([u8; 32]);
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
    pub Decimal256Data (i256);
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
            .map_err(|_| error::Error::Overflow(value.to_string()))?;
        Ok(value)
    }
}

pub type Result<T> = std::result::Result<T, error::Error>;

pub(crate) trait ByteSliceExt {
    fn rtrim_zeros(&self) -> &[u8];
}

impl ByteSliceExt for [u8] {
    #[inline(always)]
    fn rtrim_zeros(&self) -> &[u8] {
        let mut end = self.len();
        while end > 0 && self[end - 1] == 0 {
            end -= 1;
        }
        &self[..end]
    }
}

pub struct ParsedBlock<'a> {
    pub cols: Vec<Mark<'a>>,
    pub col_names: Vec<&'a str>,
    pub num_rows: usize,
}

pub struct BlockRow<'a> {
    col_names: &'a [&'a str],
    cols: &'a [Mark<'a>],
    col_index: usize,
    row_index: usize,
}

pub struct ColumnAccessor<'a> {
    pub col_name: &'a str,
    pub marker: &'a Mark<'a>,
    row_index: usize,
}

impl<'a> ColumnAccessor<'a> {
    pub fn get(self) -> Value<'a> {
        self.marker.get(self.row_index).unwrap()
    }

    pub fn get_str(self) -> Option<&'a str> {
        self.marker.get_str(self.row_index).unwrap()
    }
}

impl<'a> Iterator for BlockRow<'a> {
    type Item = (&'a str, ColumnAccessor<'a>);

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

pub struct BlockIterator<'a> {
    blocks: Peekable<std::slice::Iter<'a, ParsedBlock<'a>>>,
    block_row: usize,
}

impl<'a> BlockIterator<'a> {
    #[inline(always)]
    pub fn new(blocks: &'a [ParsedBlock<'a>]) -> Self {
        Self {
            blocks: blocks.iter().peekable(),
            block_row: 0,
        }
    }
}

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
    type Error = error::Error;

    #[inline(always)]
    fn try_from(value: Range<usize>) -> std::result::Result<Self, Self::Error> {
        let start = u32::try_from(value.start)
            .map_err(|_| error::Error::ValueOutOfRange("usize", "u32", value.start.to_string()))?;

        let length = u32::try_from(value.end - value.start).map_err(|_| {
            error::Error::ValueOutOfRange("usize", "u32", (value.end - value.start).to_string())
        })?;

        Ok(TinyRange { start, length })
    }
}

impl<'a> Iterator for BlockIterator<'a> {
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
                cols: &block.cols,
                col_index: 0,
                row_index: self.block_row,
            };
            self.block_row += 1;

            break Some(block_row);
        }
    }
}

#[cfg(test)]
pub(crate) mod common {
    use log::LevelFilter;
    use once_cell::sync::OnceCell;
    use std::io::Read as _;
    use std::path::Path;

    static INIT: OnceCell<()> = OnceCell::new();

    pub fn init_logger() {
        INIT.get_or_init(|| {
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
