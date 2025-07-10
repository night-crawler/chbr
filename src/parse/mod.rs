use zerocopy::{LittleEndian, U64};

use crate::{
    error::Error,
    parse::typ::parse_type,
    slice::ByteView,
    types::{Offsets, Type},
};

pub mod block;
pub mod column;
mod consts;
pub mod typ;

pub type IResult<I, O, E = Error> = Result<(I, O), E>;

fn parse_varuint<T>(input: &[u8]) -> IResult<&[u8], T>
where
    T: TryFrom<u64>,
{
    let (value, rest) = get_unsigned_leb128(input)?;

    let Ok(value) = T::try_from(value) else {
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
        return Err(Error::Length(9));
    }

    let b9 = input[9];
    if b9 & CONT != 0 || b9 > 1 {
        return Err(Error::Overflow("varuint too large for u64".into()));
    }

    acc |= u64::from(b9) << 63;
    Ok((acc, &input[10..]))
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
