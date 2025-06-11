use crate::conv::{date16, date32, datetime32, datetime32_tz, datetime64_tz};
use crate::index::IndexableColumn;
use chrono::NaiveDate;
use std::net::{Ipv4Addr, Ipv6Addr};
use chrono_tz::Tz;
use uuid::Uuid;
use zerocopy::little_endian::{I32, I64, U16, U32, U64};

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
    pub Date16 (U16);
    pub Date32 (I32);
    pub DateTime32 (U32);
    pub DateTime64 (I64);
}

impl_from!(Ipv6Data => Ipv6Addr, |d| Ipv6Addr::from(d.0));
impl_from!(Ipv4Data => Ipv4Addr, |d| Ipv4Addr::from(d.0.get()));
impl_from!(UuidData => Uuid, |d| {
    let [hi, lo] = d.0;
    Uuid::from_u64_pair(hi.get(), lo.get())
});
impl_from!(Date16 => NaiveDate, |d| date16(d.0.get()));
impl_from!(Date32 => NaiveDate, |d| date32(d.0.get()));
impl_from!(DateTime32 => chrono::DateTime<chrono::Utc>, |d| datetime32(d.0.get()));

impl DateTime64 {
    pub fn with_tz_and_precision(&self, tz: Tz, precision: u8) -> Option<chrono::DateTime<Tz>> {
        datetime64_tz(self.0.get(), precision, tz)
    }
}

impl DateTime32 {
    pub fn with_tz(&self, tz: Tz) -> chrono::DateTime<Tz> {
        datetime32_tz(self.0.get(), tz)
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
