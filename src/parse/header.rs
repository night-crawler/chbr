use crate::Error;
use crate::parse::block::ParseContext;
use crate::parse::typ::parse_type;
use crate::parse::{IResult, parse_u64, parse_var_str, parse_varuint};
use crate::types::{DynamicHeader, Field, MapHeader, Type, TypeHeader};
use log::debug;

pub(crate) fn variant_header<'a>(
    ctx: &ParseContext<'a>,
    inner: &[Type<'a>],
) -> IResult<&'a [u8], Vec<TypeHeader<'a>>> {
    let (mut input, mode) = parse_u64::<u64>(ctx.input)?;
    if mode != 0 {
        return Err(Error::Parse(format!(
            "Variant mode {mode} is not supported, only 0 is allowed"
        )));
    }

    let mut headers = Vec::with_capacity(inner.len());
    for typ in inner {
        let th;
        (input, th) = typ.decode_header(ctx.fork(input))?;
        headers.push(th);
    }
    Ok((input, headers))
}

pub(crate) fn dynamic_header<'a>(ctx: &ParseContext<'a>) -> IResult<&'a [u8], DynamicHeader<'a>> {
    let (mut input, version) = parse_u64::<u64>(ctx.input)?;
    if version == 1 {
        let legacy_columns: u64;
        (input, legacy_columns) = parse_varuint(input)?;
        debug!("Legacy columns: {legacy_columns}");
    }

    let (mut input, num_types) = parse_varuint::<usize>(input)?;
    let mut type_names = Vec::with_capacity(num_types + 1);
    for _ in 0..num_types {
        let t;
        (input, t) = parse_var_str(input)?;
        type_names.push(t);
    }
    type_names.push("SharedVariant");
    // https://github.com/ClickHouse/clickhouse-go/blob/a27396fbf07ca38de1d452c5b366b3a37ce45f56/lib/column/dynamic.go#L366
    type_names.sort_unstable();

    debug!("Dynamic type names (sorted): {type_names:?}");

    let mut types = Vec::with_capacity(num_types + 1);
    for name in type_names {
        let typ;
        (_, typ) = parse_type(name.as_bytes())?;
        types.push(typ);
    }

    debug!("Dynamic types: {types:?}");

    let headers;
    (input, headers) = variant_header(&ctx.fork(input), &types)?;

    Ok((input, DynamicHeader { types, headers }))
}


pub(crate) fn map_header<'a>(ctx: &ParseContext<'a>, key: &Type<'a>, val: &Type<'a>) -> IResult<&'a [u8], MapHeader<'a>> {
    let (input, key_th) = key.decode_header(ctx.clone())?;
    let ctx = ctx.fork(input);
    let (input, val_th) = val.decode_header(ctx)?;
    let h = MapHeader {
        key: key_th,
        value: val_th,
    };

    Ok((input, h))
}

pub(crate) fn nested_header<'a>(ctx: &ParseContext<'a>, fields: &[Field<'a>]) -> IResult<&'a [u8], Vec<TypeHeader<'a>>> {
    let mut input = ctx.input;
    let mut headers = Vec::with_capacity(fields.len());
    for field in fields {
        let th;
        (input, th) = field.typ.decode_header(ctx.fork(input))?;
        headers.push(th);
    }

    Ok((input, headers))
}
