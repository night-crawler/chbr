use std::hint::cold_path;

use crate::{
    error::Error,
    parse::typ::parse_type,
    slice::ByteView,
    types::{Offsets, Type},
    zc,
};

pub mod block;
pub mod column;
mod consts;
pub mod header;
pub mod typ;

pub type IResult<I, O, E = Error> = Result<(I, O), E>;

#[inline]
fn parse_varuint<T>(input: &[u8]) -> IResult<&[u8], T>
where
    T: TryFrom<u64>,
{
    let (value, rest) = get_unsigned_leb128(input)?;

    let Ok(value) = T::try_from(value) else {
        cold_path();
        return Err(Error::Overflow(value.to_string()));
    };

    Ok((rest, value))
}

#[inline(always)]
fn get_unsigned_leb128(input: &[u8]) -> Result<(u64, &[u8]), Error> {
    const DATA: u8 = 0x7F;
    const CONT: u8 = 0x80;

    macro_rules! read {
        ($idx:expr, $shift:expr, $acc:ident, $len:ident) => {{
            if $len <= $idx {
                cold_path();
                return Err(Error::Length($idx));
            }
            let byte = input[$idx];
            $acc |= (u64::from(byte & DATA)) << $shift;
            if byte & CONT == 0 {
                return Ok(($acc, &input[$idx + 1..]));
            }
        }};
    }

    let len = input.len();
    if len == 0 {
        cold_path();
        return Err(Error::Length(0));
    }

    let mut acc: u64 = 0;

    read!(0, 0, acc, len);
    read!(1, 7, acc, len);
    read!(2, 14, acc, len);
    read!(3, 21, acc, len);
    read!(4, 28, acc, len);
    read!(5, 35, acc, len);
    read!(6, 42, acc, len);
    read!(7, 49, acc, len);
    read!(8, 56, acc, len);

    if len <= 9 {
        cold_path();
        return Err(Error::Length(9));
    }

    let b9 = input[9];
    if b9 & CONT != 0 || b9 > 1 {
        cold_path();
        return Err(Error::Overflow("varuint too large for u64".into()));
    }

    acc |= u64::from(b9) << 63;
    Ok((acc, &input[10..]))
}

#[inline]
fn parse_u64<T>(input: &[u8]) -> IResult<&[u8], T>
where
    T: TryFrom<u64>,
{
    if input.len() < 8 {
        cold_path();
        return Err(Error::Length(8));
    }
    let (bytes, rest) = input.split_at(8);
    let value = u64::from_le_bytes(bytes.try_into().expect("we checked"));

    let Ok(value) = T::try_from(value) else {
        cold_path();
        return Err(Error::Overflow(value.to_string()));
    };

    Ok((rest, value))
}

#[inline]
fn parse_var_str_bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, len) = parse_varuint(input)?;
    if input.len() < len {
        cold_path();
        return Err(Error::Length(len));
    }

    let (str_bytes, remainder) = input.split_at(len);
    Ok((remainder, str_bytes))
}

pub(crate) fn parse_var_str(input: &[u8]) -> IResult<&[u8], &str> {
    let (input, len) = parse_varuint(input)?;
    if input.len() < len {
        cold_path();
        return Err(Error::UnexpectedEndOfInput);
    }

    let (str_bytes, remainder) = input.split_at(len);

    let str_value = std::str::from_utf8(str_bytes).map_err(|e| {
        cold_path();
        Error::Utf8Decode(e, str_bytes.to_vec())
    })?;
    Ok((remainder, str_value))
}

fn parse_var_str_type(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    let (input, str_bytes) = parse_var_str_bytes(input)?;
    std::str::from_utf8(str_bytes).map_err(|e| {
        cold_path();
        Error::Utf8Decode(e, str_bytes.to_vec())
    })?;
    let (_, typ) = parse_type(str_bytes)?;
    Ok((input, typ))
}

#[inline]
fn take_elements<'a>(
    input: &'a [u8],
    left: usize,
    right: usize,
    description: &str,
) -> IResult<&'a [u8], &'a [u8]> {
    let byte_len = left.checked_mul(right).ok_or_else(|| {
        cold_path();
        Error::Overflow(format!("{description}: {left} * {right}"))
    })?;
    let Some((data, input)) = input.split_at_checked(byte_len) else {
        cold_path();
        return Err(Error::Length(byte_len));
    };
    Ok((input, data))
}

fn parse_offsets(input: &[u8], num_rows: usize) -> IResult<&[u8], Offsets<'_>> {
    let (input, offsets) = take_elements(input, num_rows, size_of::<u64>(), "offset byte length")?;
    let offsets = ByteView::<zc::U64>::try_from(offsets)?;

    Ok((input, offsets))
}
