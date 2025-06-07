use crate::index::IndexableColumn;

pub mod error;
pub mod index;
pub mod mark;
pub mod parse;
mod slice;
pub mod types;
mod value;

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
#[allow(non_camel_case_types)]
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
#[allow(non_camel_case_types)]
pub struct u256(pub [u8; 32]);

pub type Result<T> = std::result::Result<T, error::Error>;

pub struct ParsedBlock<'a> {
    pub cols: Vec<IndexableColumn<'a>>,
    pub index: usize,
    pub col_names: Vec<&'a str>,
    pub num_rows: usize,
}

#[cfg(test)]
pub mod common {
    use log::LevelFilter;
    use once_cell::sync::OnceCell;

    static INIT: OnceCell<()> = OnceCell::new();

    pub fn init_logger() {
        INIT.get_or_init(|| {
            env_logger::builder()
                .filter_level(LevelFilter::Debug)
                .is_test(true)
                .init();
        });
    }
}
