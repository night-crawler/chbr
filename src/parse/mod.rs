use std::hint::cold_path;

use crate::{error::Error, slice::ByteView, types::Offsets, zc};

pub mod block;
pub mod column;
mod consts;
pub mod header;
pub mod typ;

pub type IResult<I, O, E = Error> = Result<(I, O), E>;

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

fn get_unsigned_leb128(input: &[u8]) -> Result<(u64, &[u8]), Error> {
    const DATA: u8 = 0x7F;
    const CONT: u8 = 0x80;

    let mut acc: u64 = 0;

    if let Some(head) = input.first_chunk::<10>() {
        macro_rules! read {
            ($idx:expr, $shift:expr) => {{
                let byte = head[$idx];
                acc |= u64::from(byte & DATA) << $shift;
                if byte & CONT == 0 {
                    return Ok((acc, &input[$idx + 1..]));
                }
            }};
        }

        read!(0, 0);
        read!(1, 7);
        read!(2, 14);
        read!(3, 21);
        read!(4, 28);
        read!(5, 35);
        read!(6, 42);
        read!(7, 49);
        read!(8, 56);

        let b9 = head[9];
        if b9 & CONT != 0 || b9 > 1 {
            cold_path();
            return Err(Error::Overflow("varuint too large for u64".into()));
        }

        acc |= u64::from(b9) << 63;
        return Ok((acc, &input[10..]));
    }

    // Slow path: fewer than 10 bytes left; check the length at every byte.
    let len = input.len();
    if len == 0 {
        cold_path();
        return Err(Error::Length(0));
    }

    macro_rules! read_checked {
        ($idx:expr, $shift:expr) => {{
            if len <= $idx {
                cold_path();
                return Err(Error::Length($idx));
            }
            let byte = input[$idx];
            acc |= u64::from(byte & DATA) << $shift;
            if byte & CONT == 0 {
                return Ok((acc, &input[$idx + 1..]));
            }
        }};
    }

    read_checked!(0, 0);
    read_checked!(1, 7);
    read_checked!(2, 14);
    read_checked!(3, 21);
    read_checked!(4, 28);
    read_checked!(5, 35);
    read_checked!(6, 42);
    read_checked!(7, 49);
    read_checked!(8, 56);

    // len <= 9 here: a continuation bit ran past the end of the buffer.
    cold_path();
    Err(Error::Length(9))
}

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

    let str_value = match std::str::from_utf8(str_bytes) {
        Ok(str_value) => str_value,
        Err(e) => {
            cold_path();
            return Err(Error::Utf8Decode(e, str_bytes.to_vec()));
        }
    };
    Ok((remainder, str_value))
}

fn take_elements<'a>(
    input: &'a [u8],
    left: usize,
    right: usize,
    description: &str,
) -> IResult<&'a [u8], &'a [u8]> {
    let Some(byte_len) = left.checked_mul(right) else {
        cold_path();
        return Err(Error::Overflow(format!("{description}: {left} * {right}")));
    };
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
