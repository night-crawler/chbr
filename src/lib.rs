use crate::types::Marker;

pub mod error;
pub mod parse;
mod slice;
pub mod types;


pub type Result<T> = std::result::Result<T, error::Error>;


pub struct ParsedBlock<'a> {
    pub markers: Vec<Marker<'a>>,
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
