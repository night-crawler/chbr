use crate::error::Error;
use crate::parse::typ::parse_type;
use crate::slice::ByteView;
use crate::types::{Offsets, Type};
use log::trace;
use unsigned_varint::decode;
use zerocopy::{LittleEndian, U64};

pub mod block;
pub mod column;
mod consts;
pub mod typ;

pub type IResult<I, O, E = Error> = Result<(I, O), E>;

fn parse_varuint<T>(input: &[u8]) -> IResult<&[u8], T>
where
    T: TryFrom<u64>,
{
    let (value, rest) =
        decode::u64(input).map_err(|e| Error::Parse(format!("failed to decode u64: {e:?}")))?;

    let Ok(value) = T::try_from(value) else {
        return Err(Error::Overflow(value.to_string()));
    };

    Ok((rest, value))
}

fn parse_u64<T>(input: &[u8]) -> IResult<&[u8], T>
where
    T: TryFrom<u64>,
{
    if input.len() < 8 {
        return Err(Error::Length(8));
    }
    let (bytes, rest) = input.split_at(8);
    let value = u64::from_le_bytes(bytes.try_into().unwrap());

    let Ok(value) = T::try_from(value) else {
        return Err(Error::Overflow(value.to_string()));
    };

    Ok((rest, value))
}

fn parse_var_str_bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, len) = parse_varuint(input)?;
    if input.len() < len {
        return Err(Error::Length(len));
    }
    trace!("len={len}, data: {:x?}", &input[..len]);

    let (str_bytes, remainder) = input.split_at(len);
    Ok((remainder, str_bytes))
}

pub(crate) fn parse_var_str(input: &[u8]) -> IResult<&[u8], &str> {
    let (input, len) = parse_varuint(input)?;
    if input.len() < len {
        return Err(Error::UnexpectedEndOfInput);
    }

    let (str_bytes, remainder) = input.split_at(len);

    let str_value =
        std::str::from_utf8(str_bytes).map_err(|e| Error::Utf8Decode(e, str_bytes.to_vec()))?;
    Ok((remainder, str_value))
}

fn parse_var_str_type(input: &[u8]) -> IResult<&[u8], Type> {
    let (input, str_bytes) = parse_var_str_bytes(input)?;
    std::str::from_utf8(str_bytes).map_err(|e| Error::Utf8Decode(e, str_bytes.to_vec()))?;
    let (_, typ) = parse_type(str_bytes)?;
    Ok((input, typ))
}

fn parse_offsets(input: &[u8], num_rows: usize) -> IResult<&[u8], Offsets> {
    let (offsets, input) = input.split_at(num_rows * size_of::<u64>());
    let offsets = ByteView::<U64<LittleEndian>>::try_from(offsets)?;

    Ok((input, offsets))
}
