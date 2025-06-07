use crate::parse::typ::parse_type;
use crate::slice::ByteView;
use crate::types::Type;
use log::trace;
use nom::IResult;
use nom::error::{ErrorKind, FromExternalError as _};
use unsigned_varint::decode;
use zerocopy::{LittleEndian, U64};

pub mod block;
pub mod column;
mod consts;
pub mod typ;

fn parse_varuint(input: &[u8]) -> IResult<&[u8], usize> {
    let (value, rest) = decode::u64(input).map_err(|e| {
        nom::Err::Failure(nom::error::Error::from_external_error(
            input,
            ErrorKind::Fail,
            e,
        ))
    })?;
    Ok((rest, value as usize))
}

fn parse_u64(input: &[u8]) -> IResult<&[u8], u64> {
    if input.len() < 8 {
        return Err(nom::Err::Incomplete(nom::Needed::new(8)));
    }
    let (bytes, rest) = input.split_at(8);
    let n = u64::from_le_bytes(bytes.try_into().unwrap());
    Ok((rest, n))
}

fn parse_var_str_bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, len) = parse_varuint(input)?;
    if input.len() < len {
        return Err(nom::Err::Incomplete(nom::Needed::new(len)));
    }
    trace!("len={len}, data: {:x?}", &input[..len]);

    let (str_bytes, remainder) = input.split_at(len);
    Ok((remainder, str_bytes))
}

fn parse_var_str(input: &[u8]) -> IResult<&[u8], &str> {
    let (input, len) = parse_varuint(input)?;
    if input.len() < len {
        return Err(nom::Err::Incomplete(nom::Needed::new(len)));
    }
    trace!("parse_dyn_str: len={len}, data: {:x?}", &input[..len]);

    let (str_bytes, remainder) = input.split_at(len);
    let str_value = std::str::from_utf8(str_bytes).map_err(|e| {
        nom::Err::Failure(nom::error::Error::from_external_error(
            input,
            ErrorKind::Fail,
            e,
        ))
    })?;
    Ok((remainder, str_value))
}

fn parse_var_str_type(input: &[u8]) -> IResult<&[u8], Type> {
    let (input, str_bytes) = parse_var_str_bytes(input)?;
    std::str::from_utf8(str_bytes).map_err(|e| {
        nom::Err::Failure(nom::error::Error::from_external_error(
            input,
            ErrorKind::Fail,
            e,
        ))
    })?;
    let (_, typ) = parse_type(str_bytes)?;
    Ok((input, typ))
}

fn parse_offsets(input: &[u8], num_rows: usize) -> IResult<&[u8], ByteView<U64<LittleEndian>>> {
    let (offsets, input) = input.split_at(num_rows * size_of::<u64>());
    let offsets = ByteView::<U64<LittleEndian>>::try_from(offsets).map_err(|e| {
        nom::Err::Failure(nom::error::Error::from_external_error(
            input,
            ErrorKind::Fail,
            e,
        ))
    })?;

    Ok((input, offsets))
}
