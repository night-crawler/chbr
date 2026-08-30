use std::hint::{cold_path, unreachable_unchecked};

use log::debug;

use crate::{
    Error,
    mark::Mark,
    parse::{
        IResult,
        block::ParseContext,
        column::string,
        consts::{LOW_CARDINALITY_VERSION, MAX_DYNAMIC_TYPES},
        parse_u64, parse_var_str, parse_varuint,
    },
    types::{DynamicHeader, Field, JsonColumnHeader, JsonHeader, MapHeader, Type, TypeHeader},
};

pub fn variant<'a>(
    ctx: &ParseContext<'a>,
    inner: &[Type<'a>],
) -> IResult<&'a [u8], Vec<TypeHeader<'a>>> {
    let (input, mode) = parse_u64::<u64>(ctx.input)?;
    if mode != 0 {
        cold_path();
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
    if num_types > MAX_DYNAMIC_TYPES {
        cold_path();
        return Err(Error::CorruptedData(format!(
            "Dynamic column has too many types: {num_types} (max {MAX_DYNAMIC_TYPES})"
        )));
    }
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
        types.push(Type::from_bytes(name.as_bytes())?);
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

pub fn named_tuple<'a>(
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

    Err({
        cold_path();
        Error::Parse(format!(
            "LowCardinality version {version} is not supported, only {LOW_CARDINALITY_VERSION} is \
             allowed"
        ))
    })
}

pub fn json<'a>(
    ctx: &ParseContext<'a>,
    typed_paths: &[Field<'a>],
) -> IResult<&'a [u8], JsonHeader<'a>> {
    let (input, version) = parse_u64::<u64>(ctx.input)?;
    debug!("JSON version: {version}");
    if version != 0 {
        cold_path();
        return Err(Error::NotImplemented(format!(
            "JSON serialization version {version}"
        )));
    }

    let (input, max_dynamic_paths) = parse_varuint::<u64>(input)?;
    debug!("JSON max dynamic paths: {max_dynamic_paths}");

    let (input, num_dynamic_paths) = parse_varuint(input)?;
    let (mut input, dynamic_paths) = string(&ctx.fork(input).with_num_rows(num_dynamic_paths))?;
    let Mark::String(dynamic_paths) = dynamic_paths else {
        unsafe { unreachable_unchecked() };
    };

    let cap = typed_paths.len() + num_dynamic_paths.min(input.len());
    let mut paths = Vec::with_capacity(cap);
    let mut col_headers = Vec::with_capacity(cap);
    for field in typed_paths {
        paths.push(field.name);
        col_headers.push(JsonColumnHeader {
            _path_version: 0,
            _max_types: 0,
            _total_types: 1,
            types: vec![field.typ.clone()],
            _variant_version: 0,
            is_typed: true,
            type_headers: vec![],
            mark: Mark::Empty,
        });
    }
    for path in dynamic_paths.data {
        paths.push(crate::error::decode_utf8(path)?);
    }

    for _ in 0..num_dynamic_paths {
        let header;
        (input, header) = json_column(&ctx.fork(input))?;
        col_headers.push(header);
    }

    for col_header in &mut col_headers {
        let type_headers;
        (input, type_headers) = many(&ctx.fork(input), col_header.types.iter())?;
        col_header.type_headers = type_headers;
    }

    Ok((input, JsonHeader { paths, col_headers }))
}

fn json_column<'a>(ctx: &ParseContext<'a>) -> IResult<&'a [u8], JsonColumnHeader<'a>> {
    let (input, version) = parse_u64(ctx.input)?;
    if version != 1 {
        cold_path();
        return Err(Error::NotImplemented(format!(
            "JSON dynamic path serialization version {version}"
        )));
    }

    let (input, max_types) = parse_varuint(input)?;
    let (mut input, total_types) = parse_varuint(input)?;
    if total_types > MAX_DYNAMIC_TYPES {
        cold_path();
        return Err(Error::CorruptedData(format!(
            "JSON dynamic path has too many types: {total_types} (max {MAX_DYNAMIC_TYPES})"
        )));
    }
    let mut type_names = Vec::with_capacity(total_types + 1);
    for _ in 0..total_types {
        let type_name;
        (input, type_name) = parse_var_str(input)?;
        type_names.push(type_name);
    }
    type_names.push("SharedVariant");
    type_names.sort_unstable();

    let mut types = Vec::with_capacity(type_names.len());
    for type_name in type_names {
        types.push(Type::from_bytes(type_name.as_bytes())?);
    }

    let (input, variant_version) = parse_u64(input)?;
    if variant_version != 0 {
        cold_path();
        return Err(Error::NotImplemented(format!(
            "JSON dynamic path Variant serialization version {variant_version}"
        )));
    }

    Ok((
        input,
        JsonColumnHeader {
            _path_version: version,
            _max_types: max_types,
            _total_types: total_types,
            types,
            _variant_version: variant_version,
            is_typed: false,
            type_headers: vec![],
            mark: Mark::Empty,
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
