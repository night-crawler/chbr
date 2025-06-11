use crate::index::IndexableColumn;
use std::net::Ipv6Addr;
use zerocopy::little_endian::U64;

mod conv;
pub mod error;
pub mod index;
pub mod mark;
pub mod parse;
mod slice;
pub mod types;
mod value;

#[allow(non_camel_case_types)]
#[repr(C)]
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
pub struct i256(pub [u8; 32]);

#[repr(C)]
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
pub struct UuidData(pub [U64; 2]);

#[repr(C)]
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
pub struct Octets(pub [u8; 16]);

impl From<Octets> for Ipv6Addr {
    fn from(data: Octets) -> Self {
        Ipv6Addr::from(data.0)
    }
}
impl From<UuidData> for uuid::Uuid {
    fn from(data: UuidData) -> Self {
        let [b1, b2] = data.0;
        uuid::Uuid::from_u64_pair(b1.get(), b2.get())
    }
}

#[allow(non_camel_case_types)]
#[repr(C)]
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
pub struct u256(pub [u8; 32]);

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
