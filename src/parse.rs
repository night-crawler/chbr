use crate::types::{Field, Type};
use chrono_tz::Tz;
use chrono_tz::Tz::UTC;
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::take_while1;
use nom::character::complete::{alphanumeric1, char, digit1, multispace0, multispace1};
use nom::combinator::{map, map_res, recognize};
use nom::error::ParseError;
use nom::multi::{many0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair};
use nom::{IResult, Parser, bytes::complete::tag};

fn ws<'a, O, E, F>(inner: F) -> impl Parser<&'a str, Output = O, Error = E>
where
    E: ParseError<&'a str>,
    F: Parser<&'a str, Output = O, Error = E>,
{
    delimited(multispace0, inner, multispace0)
}

fn parse_u8(input: &str) -> Result<u8, std::num::ParseIntError> {
    input.parse()
}

fn parse_decimal_type(input: &str) -> IResult<&str, Type> {
    let (input, (precision, scale)) = preceded(
        tag("Decimal"),
        delimited(
            ws(char('(')),
            separated_pair(
                map_res(digit1, parse_u8),
                ws(char(',')),
                map_res(digit1, parse_u8),
            ),
            ws(char(')')),
        ),
    )
    .parse(input)?;

    let typ = match precision {
        0..10 => Type::Decimal32(scale),
        10..19 => Type::Decimal64(scale),
        19..39 => Type::Decimal128(scale),
        39..77 => Type::Decimal256(scale),
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Fail,
            )));
        }
    };

    Ok((input, typ))
}

fn parse_string(input: &str) -> IResult<&str, Type> {
    map(tag("String"), |_| Type::String).parse(input)
}

fn parse_fixed_string(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("FixedString"),
            delimited(
                ws(char('(')),
                map_res(digit1, |s: &str| s.parse::<usize>()),
                ws(char(')')),
            ),
        ),
        Type::FixedString,
    )
    .parse(input)
}

fn parse_int_primitives(input: &str) -> IResult<&str, Type> {
    alt((
        map(tag("UUID"), |_| Type::Uuid),
        map(tag("Bool"), |_| Type::Bool),
        map(tag("UInt256"), |_| Type::UInt256),
        map(tag("Int256"), |_| Type::Int256),
        map(tag("UInt128"), |_| Type::UInt128),
        map(tag("Int128"), |_| Type::Int128),
        map(tag("UInt64"), |_| Type::UInt64),
        map(tag("Int64"), |_| Type::Int64),
        map(tag("UInt32"), |_| Type::UInt32),
        map(tag("Int32"), |_| Type::Int32),
        map(tag("UInt16"), |_| Type::UInt16),
        map(tag("Int16"), |_| Type::Int16),
        map(tag("UInt8"), |_| Type::UInt8),
        map(tag("Int8"), |_| Type::Int8),
    ))
    .parse(input)
}

fn parse_float_primitives(input: &str) -> IResult<&str, Type> {
    alt((
        map(tag("Float64"), |_| Type::Float64),
        map(tag("Float32"), |_| Type::Float32),
        map(tag("BFloat16"), |_| Type::BFloat16),
    ))
    .parse(input)
}

fn parse_inet_primitives(input: &str) -> IResult<&str, Type> {
    alt((
        map(tag("IPv6"), |_| Type::Ipv6),
        map(tag("IPv4"), |_| Type::Ipv4),
    ))
    .parse(input)
}

fn parse_datetime64(input: &str) -> IResult<&str, Type> {
    let (input, (precision, tz)) = preceded(
        tag("DateTime64"),
        delimited(
            ws(char('(')),
            separated_pair(
                map_res(digit1, |s: &str| s.parse::<u8>()),
                ws(char(',')),
                delimited(ws(char('\'')), take_while1(|c| c != '\''), ws(char('\''))),
            ),
            ws(char(')')),
        ),
    )
    .parse(input)?;

    let tz = Tz::from_str(tz)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Fail)))?;
    Ok((input, Type::DateTime64(precision, tz)))
}

fn parse_tuple(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("Tuple"),
            delimited(
                ws(char('(')),
                separated_list1(ws(char(',')), parse_type),
                ws(char(')')),
            ),
        ),
        Type::Tuple,
    )
        .parse(input)
}

fn parse_date_primitives(input: &str) -> IResult<&str, Type> {
    alt((
        parse_datetime64,
        map(tag("DateTime64"), |_| Type::DateTime64(3, UTC)),
        map(tag("DateTime"), |_| Type::DateTime(UTC)),
        map(tag("Date32"), |_| Type::Date32),
        map(tag("Date"), |_| Type::Date),
    ))
    .parse(input)
}

fn parse_geo_primitives(input: &str) -> IResult<&str, Type> {
    alt((
        map(tag("LineString"), |_| Type::LineString),
        map(tag("MultiLineString"), |_| Type::MultiLineString),
        map(tag("MultiPolygon"), |_| Type::MultiPolygon),
        map(tag("Polygon"), |_| Type::Polygon),
        map(tag("Ring"), |_| Type::Ring),
        map(tag("Point"), |_| Type::Point),
    ))
    .parse(input)
}

fn parse_other_primitives(input: &str) -> IResult<&str, Type> {
    alt((
        map(tag("Dynamic"), |_| Type::Dynamic),
    ))
        .parse(input)
}

fn parse_primitive_type(input: &str) -> IResult<&str, Type> {
    alt((
        parse_string,
        parse_int_primitives,
        parse_float_primitives,
        parse_fixed_string,
        parse_date_primitives,
        parse_inet_primitives,
        parse_geo_primitives,
    ))
    .parse(input)
}

fn parse_nullable(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("Nullable"),
            delimited(ws(char('(')), parse_type, ws(char(')'))),
        ),
        |inner| Type::Nullable(Box::new(inner)),
    )
    .parse(input)
}

fn parse_map(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("Map"),
            delimited(
                ws(char('(')),
                separated_pair(parse_type, ws(char(',')), parse_type),
                ws(char(')')),
            ),
        ),
        |(k, v)| Type::Map(Box::new(k), Box::new(v)),
    )
    .parse(input)
}

fn parse_array(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("Array"),
            delimited(ws(char('(')), parse_type, ws(char(')'))),
        ),
        |inner| Type::Array(Box::new(inner)),
    )
    .parse(input)
}

fn parse_variant(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("Variant"),
            delimited(
                ws(char('(')),
                separated_list1(ws(char(',')), parse_type),
                ws(char(')')),
            ),
        ),
        Type::Variant,
    )
    .parse(input)
}

fn parse_lowcardinality(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("LowCardinality"),
            delimited(ws(char('(')), parse_type, ws(char(')'))),
        ),
        |inner| Type::LowCardinality(Box::new(inner)),
    )
    .parse(input)
}

fn parse_nested(input: &str) -> IResult<&str, Type> {
    let (input, pairs) = preceded(
        tag("Nested"),
        delimited(
            ws(char('(')),
            separated_list1(
                ws(char(',')),
                separated_pair(
                    recognize(pair(alphanumeric1, many0(alt((alphanumeric1, tag("_")))))),
                    multispace1,
                    parse_type,
                ),
            ),
            ws(char(')')),
        ),
    )
    .parse(input)?;

    let fields = pairs
        .into_iter()
        .map(|(name, typ)| Field { name, typ })
        .collect::<Vec<_>>();

    Ok((input, Type::Nested(fields)))
}

fn parse_enum8(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("Enum8"),
            delimited(
                ws(char('(')),
                separated_list1(
                    ws(char(',')),
                    separated_pair(
                        delimited(ws(char('\'')), take_while1(|c| c != '\''), ws(char('\''))),
                        ws(char('=')),
                        map_res(digit1, |s: &str| s.parse::<i8>()),
                    ),
                ),
                ws(char(')')),
            ),
        ),
        Type::Enum8,
    )
    .parse(input)
}

fn parse_enum16(input: &str) -> IResult<&str, Type> {
    map(
        preceded(
            tag("Enum16"),
            delimited(
                ws(char('(')),
                separated_list1(
                    ws(char(',')),
                    separated_pair(
                        delimited(ws(char('\'')), take_while1(|c| c != '\''), ws(char('\''))),
                        ws(char('=')),
                        map_res(digit1, |s: &str| s.parse::<i16>()),
                    ),
                ),
                ws(char(')')),
            ),
        ),
        Type::Enum16,
    )
    .parse(input)
}

pub fn parse_type(input: &str) -> IResult<&str, Type> {
    alt((
        parse_lowcardinality,
        parse_nullable,
        parse_primitive_type,
        parse_array,
        parse_map,
        parse_tuple,
        parse_decimal_type,
        parse_variant,
        parse_nested,
        parse_enum8,
        parse_enum16,
        parse_other_primitives,
    ))
    .parse(input)
}

mod tests {
    use super::*;
    #[test]
    fn decimal() {
        let input = "Decimal(9, 9)";
        let result = parse_decimal_type(input);
        assert!(result.is_ok());
    }

    #[test]
    fn int64() {
        let input = "Int64";
        let result = parse_int_primitives(input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().1, Type::Int64);
    }

    #[test]
    fn map() {
        let input = "Map(Int32, String)";
        let (_, typ) = parse_map(input).unwrap();
        assert_eq!(
            typ,
            Type::Map(Box::new(Type::Int32), Box::new(Type::String))
        );
    }

    #[test]
    fn map_nullable() {
        let input = "Map(Int32, Nullable(LowCardinality(String)))";
        let (_, typ) = parse_map(input).unwrap();
        assert_eq!(
            typ,
            Type::Map(
                Box::new(Type::Int32),
                Box::new(Type::Nullable(Box::new(Type::LowCardinality(Box::new(
                    Type::String
                )))))
            )
        );
    }

    #[test]
    fn array() {
        let input = "Array(Int32)";
        let (_, typ) = parse_array(input).unwrap();
        assert_eq!(typ, Type::Array(Box::new(Type::Int32)));
    }

    #[test]
    fn variant() {
        let input = "Variant(Array(UInt64), String, UInt64)";
        let (_, typ) = parse_variant(input).unwrap();
        assert_eq!(
            typ,
            Type::Variant(vec![
                Type::Array(Box::new(Type::UInt64)),
                Type::String,
                Type::UInt64
            ])
        );
    }

    #[test]
    fn array_nested() {
        let input = "Array(Nested(child_id UInt64, child_name String, scores Array(UInt32)))";
        let (_, typ) = parse_type(input).unwrap();
        assert_eq!(
            typ,
            Type::Array(Box::new(Type::Nested(vec![
                Field {
                    name: "child_id",
                    typ: Type::UInt64
                },
                Field {
                    name: "child_name",
                    typ: Type::String
                },
                Field {
                    name: "scores",
                    typ: Type::Array(Box::new(Type::UInt32))
                }
            ])))
        );
    }

    #[test]
    fn enum8() {
        let input = "Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3)";
        let (_, typ) = parse_type(input).unwrap();
        assert_eq!(
            typ,
            Type::Enum8(vec![("Red", 1), ("Green", 2), ("Blue", 3)])
        );
    }

    #[test]
    fn enum16() {
        let input = "Enum16('Foo' = 1000, 'Bar' = 2000)";
        let (_, typ) = parse_type(input).unwrap();
        assert_eq!(typ, Type::Enum16(vec![("Foo", 1000), ("Bar", 2000)]));
    }
}
