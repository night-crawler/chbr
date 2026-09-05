/// Deliberately doesn't support escaping because it's pain in `derive` and pain in general.
use std::hint::cold_path;
use std::str::{FromStr, from_utf8};

use chrono_tz::{Tz, Tz::UTC};
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map, map_res, opt, recognize, verify},
    error::{ErrorKind, FromExternalError as _, ParseError},
    multi::{separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, separated_pair},
};

use crate::types::{Field, Type};

fn parse_num<T>(input: &[u8]) -> Result<T, nom::error::Error<&[u8]>>
where
    T: FromStr,
{
    let s = match from_utf8(input) {
        Ok(s) => s,
        Err(e) => {
            return Err(nom::error::Error::from_external_error(
                input,
                ErrorKind::Fail,
                e,
            ));
        }
    };
    match s.parse::<T>() {
        Ok(parsed) => Ok(parsed),
        Err(e) => Err(nom::error::Error::from_external_error(
            input,
            ErrorKind::Fail,
            e,
        )),
    }
}

fn ws<'a, O, E, F>(inner: F) -> impl Parser<&'a [u8], Output = O, Error = E>
where
    E: ParseError<&'a [u8]>,
    F: Parser<&'a [u8], Output = O, Error = E>,
{
    delimited(multispace0, inner, multispace0)
}

fn parse_decimal_type(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    let (input, (precision, scale)) = preceded(
        tag("Decimal"),
        delimited(
            ws(char('(')),
            separated_pair(
                map_res(digit1, parse_num::<u8>),
                ws(char(',')),
                map_res(digit1, parse_num::<u8>),
            ),
            ws(char(')')),
        ),
    )
    .parse(input)?;

    if scale > precision {
        cold_path();
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            ErrorKind::Fail,
        )));
    }

    let typ = match precision {
        0..10 => Type::Decimal32(scale),
        10..19 => Type::Decimal64(scale),
        19..39 => Type::Decimal128(scale),
        39..77 => Type::Decimal256(scale),
        _ => {
            cold_path();
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                ErrorKind::Fail,
            )));
        }
    };

    Ok((input, typ))
}

fn parse_string(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(tag("String"), |_| Type::String).parse(input)
}

fn parse_fixed_string(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(
        preceded(
            tag("FixedString"),
            delimited(
                ws(char('(')),
                map_res(digit1, |s: &[u8]| parse_num::<usize>(s)),
                ws(char(')')),
            ),
        ),
        Type::FixedString,
    )
    .parse(input)
}

fn parse_int_primitives(input: &[u8]) -> IResult<&[u8], Type<'_>> {
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

fn parse_float_primitives(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    alt((
        map(tag("Float64"), |_| Type::Float64),
        map(tag("Float32"), |_| Type::Float32),
        map(tag("BFloat16"), |_| Type::BFloat16),
    ))
    .parse(input)
}

fn parse_inet_primitives(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    alt((
        map(tag("IPv6"), |_| Type::Ipv6),
        map(tag("IPv4"), |_| Type::Ipv4),
    ))
    .parse(input)
}

/// `'Europe/Berlin'` -> [`Tz`]
fn quoted_tz(input: &[u8]) -> IResult<&[u8], Tz> {
    map_res(
        delimited(ws(char('\'')), take_while1(|c| c != b'\''), ws(char('\''))),
        |tz: &[u8]| {
            // SAFETY: I hope caller validated the input as UTF-8 before parsing
            Tz::from_str(unsafe { std::str::from_utf8_unchecked(tz) })
                .map_err(|_| nom::error::Error::new(tz, ErrorKind::Fail))
        },
    )
    .parse(input)
}

/// `DateTime64(N)` or `DateTime64(N, 'tz')`
fn parse_datetime64(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(
        preceded(
            tag("DateTime64"),
            delimited(
                ws(char('(')),
                pair(
                    map_res(digit1, parse_num::<u8>),
                    opt(preceded(ws(char(',')), quoted_tz)),
                ),
                ws(char(')')),
            ),
        ),
        |(precision, tz)| Type::DateTime64(precision, tz.unwrap_or(UTC)),
    )
    .parse(input)
}

/// `DateTime('tz')`
fn parse_datetime_tz(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(
        preceded(
            tag("DateTime"),
            delimited(ws(char('(')), quoted_tz, ws(char(')'))),
        ),
        Type::DateTime,
    )
    .parse(input)
}

fn parse_tuple(input: &[u8]) -> IResult<&[u8], Type<'_>> {
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

fn parse_date_primitives(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    alt((
        parse_datetime64,
        map(tag("DateTime64"), |_| Type::DateTime64(3, UTC)),
        parse_datetime_tz,
        map(tag("DateTime"), |_| Type::DateTime(UTC)),
        map(tag("Date32"), |_| Type::Date32),
        map(tag("Date"), |_| Type::Date),
    ))
    .parse(input)
}

fn parse_geo_primitives(input: &[u8]) -> IResult<&[u8], Type<'_>> {
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
fn parse_json_path(input: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        delimited(char('`'), take_while1(|c| c != b'`'), char('`')),
        take_while1(|c: u8| !c.is_ascii_whitespace() && c != b',' && c != b')'),
    ))
    .parse(input)
}

fn parse_json_setting(input: &[u8]) -> IResult<&[u8], Option<Field<'_>>> {
    map(
        pair(
            alt((tag("max_dynamic_paths"), tag("max_dynamic_types"))),
            preceded(ws(char('=')), digit1),
        ),
        |_| None,
    )
    .parse(input)
}

fn parse_json_skip(input: &[u8]) -> IResult<&[u8], Option<Field<'_>>> {
    map(
        preceded(
            alt((tag("SKIP REGEXP"), tag("SKIP"))),
            preceded(
                multispace1,
                alt((
                    delimited(char('\''), take_while1(|c| c != b'\''), char('\'')),
                    parse_json_path,
                )),
            ),
        ),
        |_| None,
    )
    .parse(input)
}

fn parse_json_typed_path(input: &[u8]) -> IResult<&[u8], Option<Field<'_>>> {
    map(
        separated_pair(parse_json_path, multispace1, parse_type),
        |(name, typ)| {
            Some(Field {
                name: unsafe { std::str::from_utf8_unchecked(name) },
                typ,
            })
        },
    )
    .parse(input)
}

fn parse_json(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    let (input, arguments) = preceded(
        tag("JSON"),
        opt(delimited(
            ws(char('(')),
            separated_list0(
                ws(char(',')),
                alt((parse_json_setting, parse_json_skip, parse_json_typed_path)),
            ),
            ws(char(')')),
        )),
    )
    .parse(input)?;

    let mut typed_paths = match arguments {
        Some(arguments) => arguments.into_iter().flatten().collect::<Vec<_>>(),
        None => Vec::new(),
    };
    typed_paths.sort_unstable_by_key(|field| field.name);
    Ok((input, Type::Json(typed_paths)))
}

fn parse_other_primitives(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    alt((
        // `Dynamic` | `Dynamic(max_types=32)` -> Type::Dynamic
        map(
            pair(
                tag("Dynamic"),
                opt(delimited(
                    ws(char('(')),
                    pair(tag("max_types"), preceded(ws(char('=')), digit1)),
                    ws(char(')')),
                )),
            ),
            |_| Type::Dynamic,
        ),
        map(tag("SharedVariant"), |_| Type::SharedVariant),
        map(tag("Nothing"), |_| Type::Nothing),
    ))
    .parse(input)
}

fn parse_primitive_type(input: &[u8]) -> IResult<&[u8], Type<'_>> {
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

fn parse_nullable(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(
        preceded(
            tag("Nullable"),
            delimited(ws(char('(')), parse_type, ws(char(')'))),
        ),
        |inner| Type::Nullable(Box::new(inner)),
    )
    .parse(input)
}

fn parse_map(input: &[u8]) -> IResult<&[u8], Type<'_>> {
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

fn parse_array(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(
        preceded(
            tag("Array"),
            delimited(ws(char('(')), parse_type, ws(char(')'))),
        ),
        |inner| Type::Array(Box::new(inner)),
    )
    .parse(input)
}

fn parse_variant(input: &[u8]) -> IResult<&[u8], Type<'_>> {
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

fn parse_lowcardinality(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(
        preceded(
            tag("LowCardinality"),
            delimited(ws(char('(')), parse_type, ws(char(')'))),
        ),
        |inner| Type::LowCardinality(Box::new(inner)),
    )
    .parse(input)
}

fn parse_named_tuple(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    let (input, fields) = parse_pairs("Tuple", input)?;
    let fields = map_fields(fields);

    Ok((input, Type::NamedTuple(fields)))
}

fn parse_nested(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    let (input, pairs) = parse_pairs("Nested", input)?;
    let fields = map_fields(pairs);

    Ok((input, Type::Nested(fields)))
}

fn map_fields<'a>(pairs: Vec<(&'a [u8], Type<'a>)>) -> Vec<Field<'a>> {
    pairs
        .into_iter()
        .map(|(name, typ)| Field {
            name: unsafe { std::str::from_utf8_unchecked(name) },
            typ,
        })
        .collect::<Vec<_>>()
}

fn parse_identifier(input: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        delimited(char('`'), take_while1(|c| c != b'`'), char('`')),
        take_while1(|c: u8| c.is_ascii_alphanumeric() || c == b'_'),
    ))
    .parse(input)
}

fn parse_pairs<'a>(
    name: &'static str,
    input: &'a [u8],
) -> IResult<&'a [u8], Vec<(&'a [u8], Type<'a>)>> {
    let (input, pairs) = preceded(
        tag(name),
        delimited(
            ws(char('(')),
            separated_list1(
                ws(char(',')),
                separated_pair(parse_identifier, multispace1, parse_type),
            ),
            ws(char(')')),
        ),
    )
    .parse(input)?;

    Ok((input, pairs))
}

fn parse_enum_variants<'a, T>(
    name: &'static str,
    input: &'a [u8],
) -> IResult<&'a [u8], Vec<(&'a str, T)>>
where
    T: FromStr + PartialOrd,
{
    map(
        verify(
            preceded(
                tag(name),
                delimited(
                    ws(char('(')),
                    separated_list1(
                        ws(char(',')),
                        separated_pair(
                            delimited(ws(char('\'')), take_while1(|c| c != b'\''), ws(char('\''))),
                            ws(char('=')),
                            map_res(recognize(pair(opt(char('-')), digit1)), parse_num::<T>),
                        ),
                    ),
                    ws(char(')')),
                ),
            ),
            |pairs: &Vec<(&[u8], T)>| pairs.windows(2).all(|w| w[0].1 < w[1].1),
        ),
        |pairs| {
            pairs
                .into_iter()
                .map(|(name, id)| (unsafe { std::str::from_utf8_unchecked(name) }, id))
                .collect()
        },
    )
    .parse(input)
}

fn parse_enum8(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(
        |input| parse_enum_variants::<i8>("Enum8", input),
        Type::Enum8,
    )
    .parse(input)
}

fn parse_enum16(input: &[u8]) -> IResult<&[u8], Type<'_>> {
    map(
        |input| parse_enum_variants::<i16>("Enum16", input),
        Type::Enum16,
    )
    .parse(input)
}

pub fn parse_type(input: &[u8]) -> IResult<&[u8], Type<'_>> {
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
        parse_named_tuple,
        parse_enum8,
        parse_enum16,
        parse_json,
        parse_other_primitives,
    ))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal() {
        let input = b"Decimal(9, 9)";
        let (_, typ) = parse_decimal_type(input).unwrap();
        assert_eq!(typ, Type::Decimal32(9));
    }

    #[test]
    fn decimal_scale_exceeds_precision() {
        assert!(parse_decimal_type(b"Decimal(9, 10)").is_err());
        assert!(parse_decimal_type(b"Decimal(18, 30)").is_err());
        assert!(parse_decimal_type(b"Decimal(38, 40)").is_err());
    }

    #[test]
    fn decimal_precision_out_of_range() {
        assert!(parse_decimal_type(b"Decimal(77, 0)").is_err());
    }

    #[test]
    fn int64() {
        let input = b"Int64";
        let result = parse_int_primitives(input);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().1, Type::Int64);
    }

    #[test]
    fn map() {
        let input = b"Map(Int32, String)";
        let (_, typ) = parse_map(input).unwrap();
        assert_eq!(
            typ,
            Type::Map(Box::new(Type::Int32), Box::new(Type::String))
        );
    }

    #[test]
    fn map_nullable() {
        let input = b"Map(Int32, Nullable(LowCardinality(String)))";
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
        let input = b"Array(Int32)";
        let (_, typ) = parse_array(input).unwrap();
        assert_eq!(typ, Type::Array(Box::new(Type::Int32)));
    }

    #[test]
    fn variant() {
        let input = b"Variant(Array(UInt64), String, UInt64)";
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
    fn dynamic_max_types() {
        for input in [
            &b"Dynamic"[..],
            b"Dynamic(max_types=0)",
            b"Dynamic(max_types = 5)",
        ] {
            let (rest, typ) = parse_type(input).unwrap();
            assert!(rest.is_empty(), "{}", String::from_utf8_lossy(input));
            assert_eq!(typ, Type::Dynamic);
        }
    }

    #[test]
    fn array_nested() {
        let input = b"Array(Nested(child_id UInt64, child_name String, scores Array(UInt32)))";
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
    fn array_named_tuple() {
        let input = b"Array(Tuple(kind String, agent_symbols Bool, file_or_func_id UInt128, addr_or_line UInt64))";
        let (_, typ) = parse_type(input).unwrap();
        assert_eq!(
            typ,
            Type::Array(Box::new(Type::NamedTuple(vec![
                Field {
                    name: "kind",
                    typ: Type::String
                },
                Field {
                    name: "agent_symbols",
                    typ: Type::Bool
                },
                Field {
                    name: "file_or_func_id",
                    typ: Type::UInt128
                },
                Field {
                    name: "addr_or_line",
                    typ: Type::UInt64
                },
            ])))
        );
    }

    #[test]
    fn enum8() {
        let input = b"Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3)";
        let (_, typ) = parse_type(input).unwrap();
        assert_eq!(
            typ,
            Type::Enum8(vec![("Red", 1), ("Green", 2), ("Blue", 3)])
        );
    }

    #[test]
    fn enum16() {
        let input = b"Enum16('Foo' = 1000, 'Bar' = 2000)";
        let (_, typ) = parse_type(input).unwrap();
        assert_eq!(typ, Type::Enum16(vec![("Foo", 1000), ("Bar", 2000)]));
    }

    #[test]
    fn enum16_negative() {
        let input = b"Enum16('Min' = -32768, 'Neg' = -5000, 'Pos' = 5000)";
        let (_, typ) = parse_type(input).unwrap();
        assert_eq!(
            typ,
            Type::Enum16(vec![("Min", -32768), ("Neg", -5000), ("Pos", 5000)])
        );
    }

    #[test]
    fn enum_rejects_unsorted_or_duplicate_ids() {
        assert!(parse_type(b"Enum8('B' = 2, 'A' = 1)").is_err());
        assert!(parse_type(b"Enum16('A' = 1, 'B' = 1)").is_err());
        assert!(parse_type(b"Enum8('Blue' = -23, 'Green' = 2, 'Red' = 11)").is_ok());
    }

    #[test]
    fn json_with_typed_paths_and_settings() {
        let typ = Type::from_bytes(
            b"JSON(max_dynamic_paths=2, `nested.name` String, a UInt64, \
              max_dynamic_types=4, SKIP ignored, SKIP REGEXP '^private')",
        )
        .unwrap();
        assert_eq!(
            typ,
            Type::Json(vec![
                Field {
                    name: "a",
                    typ: Type::UInt64,
                },
                Field {
                    name: "nested.name",
                    typ: Type::String,
                },
            ])
        );
    }

    #[test]
    fn json_type_rejects_unparsed_arguments() {
        assert!(Type::from_bytes(b"JSON(a UInt64) trailing").is_err());
        assert!(Type::from_bytes(b"JSON(unknown_setting=1)").is_err());
    }

    #[test]
    fn json_without_arguments() {
        assert_eq!(Type::from_bytes(b"JSON").unwrap(), Type::Json(vec![]));
        assert_eq!(Type::from_bytes(b"JSON()").unwrap(), Type::Json(vec![]));
    }

    #[test]
    fn named_tuple_identifier_may_start_with_underscore() {
        let typ = Type::from_bytes(b"Tuple(_id UInt64, name String)").unwrap();
        let Type::NamedTuple(fields) = typ else {
            panic!("expected NamedTuple, got {typ:?}");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name).collect();
        assert_eq!(names, ["_id", "name"]);
    }

    #[test]
    fn named_tuple_and_nested_backquoted_names() {
        let typ = Type::from_bytes(b"Tuple(`my field` UInt64, `1x` String, plain Int8)").unwrap();
        let Type::NamedTuple(fields) = typ else {
            panic!("expected NamedTuple, got {typ:?}");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name).collect();
        assert_eq!(names, ["my field", "1x", "plain"]);

        let typ = Type::from_bytes(b"Nested(`a.b` UInt64, c String)").unwrap();
        let Type::Nested(fields) = typ else {
            panic!("expected Nested, got {typ:?}");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name).collect();
        assert_eq!(names, ["a.b", "c"]);

        assert!(Type::from_bytes(b"Tuple(`unterminated UInt64)").is_err());
    }

    #[test]
    fn nothing() {
        assert_eq!(
            Type::from_bytes(b"Array(Nothing)").unwrap(),
            Type::Array(Box::new(Type::Nothing))
        );
        assert_eq!(
            Type::from_bytes(b"Nullable(Nothing)").unwrap(),
            Type::Nullable(Box::new(Type::Nothing))
        );
    }

    #[test]
    fn datetime_forms() {
        use chrono_tz::{Asia::Tokyo, Europe::Berlin};
        assert_eq!(Type::from_bytes(b"DateTime").unwrap(), Type::DateTime(UTC));
        assert_eq!(
            Type::from_bytes(b"DateTime('Europe/Berlin')").unwrap(),
            Type::DateTime(Berlin)
        );
        assert_eq!(
            Type::from_bytes(b"DateTime64").unwrap(),
            Type::DateTime64(3, UTC)
        );
        assert_eq!(
            Type::from_bytes(b"DateTime64(6)").unwrap(),
            Type::DateTime64(6, UTC)
        );
        assert_eq!(
            Type::from_bytes(b"DateTime64(9, 'Asia/Tokyo')").unwrap(),
            Type::DateTime64(9, Tokyo)
        );
        assert_eq!(
            Type::from_bytes(b"Nullable(DateTime64(3))").unwrap(),
            Type::Nullable(Box::new(Type::DateTime64(3, UTC)))
        );
        assert!(Type::from_bytes(b"DateTime('Mars/Olympus')").is_err());
        assert!(Type::from_bytes(b"DateTime64(3, 'Mars/Olympus')").is_err());
        assert!(Type::from_bytes(b"DateTime64('UTC')").is_err());
    }
}
