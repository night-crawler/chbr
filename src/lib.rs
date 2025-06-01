pub mod error;
mod parse;
mod types;

use crate::types::BlockMarker;
use unsigned_varint::decode;

pub type Result<T> = std::result::Result<T, error::Error>;

pub fn parse_block(data: &[u8]) -> Result<()> {
    let mut markers: Vec<BlockMarker> = vec![];
    let mut remainder = data;
    let mut num;

    (num, remainder) = decode::usize(remainder)?;
    let num_columns = num;

    (num, remainder) = decode::usize(remainder)?;
    let num_rows = num;

    for _ in 0..num_columns {
        (num, remainder) = decode::usize(remainder)?;
        let name = unsafe { str::from_utf8_unchecked(&remainder[..num]) };
        remainder = &remainder[num..];

        (num, remainder) = decode::usize(remainder)?;
        let type_name = unsafe { str::from_utf8_unchecked(&remainder[..num]) };
        remainder = &remainder[num..];

        println!("--- {num_rows} {num_columns} {name:?}, {type_name:?}");
        let typ = types::Type::from_str(type_name)?;

        let (marker, len) = typ.transcode_remainder(remainder, num_columns, num_rows)?;
        remainder = &remainder[len..];
        markers.push(marker);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn it_works() -> Result<()> {
        let mut file = std::fs::File::open("./sample.native")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        parse_block(&buf)?;

        Ok(())
    }

    #[test]
    fn array() -> Result<()> {
        let mut file = std::fs::File::open("./array.native")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        parse_block(&buf)?;

        Ok(())
    }

    #[test]
    fn tuple() -> Result<()> {
        let mut file = std::fs::File::open("./tuple.native")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        parse_block(&buf)?;

        Ok(())
    }

    #[test]
    fn variant() -> Result<()> {
        let mut file = std::fs::File::open("./variant.native")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        parse_block(&buf).unwrap();

        Ok(())
    }

    #[test]
    fn dynamic() -> Result<()> {
        let mut file = std::fs::File::open("./dynamic.native")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        parse_block(&buf).unwrap();

        Ok(())
    }

    #[test]
    fn nullable_string() -> Result<()> {
        let mut file = std::fs::File::open("./nullable_string.native")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        parse_block(&buf).unwrap();

        Ok(())
    }

    #[test]
    fn json() -> Result<()> {
        let mut file = std::fs::File::open("./json.native")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        parse_block(&buf).unwrap();

        Ok(())
    }
}
