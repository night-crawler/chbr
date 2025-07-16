use std::hint::unreachable_unchecked;

use log::debug;

use crate::{
    Error,
    mark::Mark,
    parse::{
        IResult, block::ParseContext, column::string, consts::LOW_CARDINALITY_VERSION, parse_u64,
        parse_var_str, parse_var_str_type, parse_varuint, typ::parse_type,
    },
    types::{DynamicHeader, Field, JsonColumnHeader, JsonHeader, MapHeader, Type, TypeHeader},
};

pub fn variant<'a>(
    ctx: &ParseContext<'a>,
    inner: &[Type<'a>],
) -> IResult<&'a [u8], Vec<TypeHeader<'a>>> {
    let (input, mode) = parse_u64::<u64>(ctx.input)?;
    if mode != 0 {
        return Err(Error::Parse(format!(
            "Variant mode {mode} is not supported, only 0 is allowed"
        )));
    }
    many(&ctx.fork(input), inner.iter())
}

pub fn dynamic<'a>(ctx: &ParseContext<'a>) -> IResult<&'a [u8], DynamicHeader<'a>> {
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
    (input, headers) = variant(&ctx.fork(input), &types)?;

    Ok((input, DynamicHeader { types, headers }))
}

pub fn map<'a>(
    ctx: &ParseContext<'a>,
    key: &Type<'a>,
    val: &Type<'a>,
) -> IResult<&'a [u8], MapHeader<'a>> {
    let (input, key_th) = key.decode_header(ctx)?;
    let (input, val_th) = val.decode_header(&ctx.fork(input))?;
    let h = MapHeader {
        key: key_th,
        value: val_th,
    };

    Ok((input, h))
}

pub fn nested<'a>(
    ctx: &ParseContext<'a>,
    fields: &[Field<'a>],
) -> IResult<&'a [u8], Vec<TypeHeader<'a>>> {
    many(ctx, fields.iter().map(|f| &f.typ))
}

pub fn point<'a>() -> TypeHeader<'a> {
    TypeHeader::Tuple(vec![TypeHeader::Empty, TypeHeader::Empty])
}

pub fn ring<'a>() -> TypeHeader<'a> {
    TypeHeader::Array(Box::new(point()))
}

pub fn polygon<'a>() -> TypeHeader<'a> {
    TypeHeader::Array(Box::new(ring()))
}

pub fn multi_polygon<'a>() -> TypeHeader<'a> {
    TypeHeader::Array(Box::new(polygon()))
}

pub fn tuple<'a>(
    ctx: &ParseContext<'a>,
    inner: &[Type<'a>],
) -> IResult<&'a [u8], Vec<TypeHeader<'a>>> {
    many(ctx, inner.iter())
}

pub fn lc<'a>(ctx: &ParseContext<'a>) -> IResult<&'a [u8], TypeHeader<'a>> {
    let (input, version) = parse_u64::<u64>(ctx.input)?;
    debug!("LowCardinality version: {version}");
    if version == LOW_CARDINALITY_VERSION {
        return Ok((input, TypeHeader::Empty));
    }

    Err(Error::Parse(format!(
        "LowCardinality version {version} is not supported, only {LOW_CARDINALITY_VERSION} is \
         allowed"
    )))
}

pub fn json<'a>(ctx: &ParseContext<'a>) -> IResult<&'a [u8], JsonHeader<'a>> {
    let (input, version) = parse_u64::<u64>(ctx.input)?;
    debug!("JSON version: {version}");

    let (input, num_paths_old) = parse_varuint::<u64>(input)?;
    debug!("num_paths_old: {num_paths_old}");

    let (input, num_paths) = parse_varuint(input)?;
    let (mut input, paths) = string(&ctx.fork(input).with_num_rows(num_paths))?;
    let Mark::String(paths) = paths else {
        unsafe { unreachable_unchecked() };
    };

    let mut col_headers = Vec::with_capacity(num_paths);

    for _ in 0..num_paths {
        let header;
        (input, header) = json_column(&ctx.fork(input))?;
        col_headers.push(header);
    }

    let (input, type_headers) = many(
        &ctx.fork(input),
        col_headers.iter().map(|ch| ch.typ.as_ref()),
    )?;

    let header = JsonHeader {
        paths,
        col_headers,
        type_headers,
    };

    Ok((input, header))
}

fn json_column<'a>(ctx: &ParseContext<'a>) -> IResult<&'a [u8], JsonColumnHeader<'a>> {
    let (input, version) = parse_u64(ctx.input)?;
    let (input, max_types) = parse_varuint(input)?;
    let (input, total_types) = parse_varuint(input)?;
    let (input, typ) = parse_var_str_type(input)?;
    let (input, variant) = parse_u64(input)?;

    Ok((
        input,
        JsonColumnHeader {
            path_version: version,
            max_types,
            total_types,
            typ: Box::new(typ),
            variant_version: variant,
            mark: Mark::Empty,
            discriminators: &[],

            // The JSON header has been parsed and initialized with num_rows coming from the top
            // level. In case it's a stand-alone JSON, then everything is fine: we could initialize
            // the right number of offsets here. But if we have Array(JSON), then on this level
            // we get num_rows of the Array itself, and not the number of rows we need to read of
            // the JSON itself. For this reason, this field will be resized later.
            offsets: vec![],
        },
    ))
}

fn many<'a, 'b>(
    ctx: &ParseContext<'a>,
    types: impl Iterator<Item = &'b Type<'a>>,
) -> IResult<&'a [u8], Vec<TypeHeader<'a>>>
where
    'a: 'b,
{
    let mut headers = Vec::new();
    let mut ctx = ctx.clone();
    for typ in types {
        let (input, th) = typ.decode_header(&ctx)?;
        headers.push(th);
        ctx = ctx.fork(input);
    }
    Ok((ctx.input, headers))
}
