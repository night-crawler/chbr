use crate::conv::{date16, date32, datetime32, datetime32_tz, datetime64_tz};
use crate::index::IndexableColumn;
use chrono::NaiveDate;
use chrono_tz::Tz;
use std::net::{Ipv4Addr, Ipv6Addr};
use uuid::Uuid;
use zerocopy::little_endian::{I32, I64, I128, U16, U32, U64};

mod conv;
pub mod error;
pub mod index;
pub mod mark;
pub mod parse;
mod slice;
pub mod types;
mod value;

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
}

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
    pub fn with_tz_and_precision(&self, tz: Tz, precision: u8) -> Option<chrono::DateTime<Tz>> {
        datetime64_tz(self.0.get(), precision, tz)
    }
}

impl DateTime32Data {
    pub fn with_tz(&self, tz: Tz) -> chrono::DateTime<Tz> {
        datetime32_tz(self.0.get(), tz)
    }
}

impl Decimal32Data {
    pub fn with_precision(&self, precision: u8) -> rust_decimal::Decimal {
        let value = self.0.get();
        rust_decimal::Decimal::new(i64::from(value), u32::from(precision))
    }
}

impl Decimal64Data {
    pub fn with_precision(&self, precision: u8) -> rust_decimal::Decimal {
        let value = self.0.get();
        rust_decimal::Decimal::new(value, u32::from(precision))
    }
}

impl Decimal128Data {
    pub fn with_precision(&self, precision: u8) -> Result<rust_decimal::Decimal> {
        let value = self.0.get();
        let value = rust_decimal::Decimal::try_from_i128_with_scale(value, u32::from(precision))
            .map_err(|_| error::Error::Overflow(value.to_string()))?;
        Ok(value)
    }
}

pub type Result<T> = std::result::Result<T, error::Error>;

pub struct ParsedBlock<'a> {
    pub cols: Vec<IndexableColumn<'a>>,
    pub index: usize,
    pub col_names: Vec<&'a str>,
    pub num_rows: usize,
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
            env_logger::builder()
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
