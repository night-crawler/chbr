use crate::error::Error;
use crate::mark::Mark;
use crate::parse::{parse_var_str_bytes, IResult};
use crate::parse::block::ParseContext;
use crate::parse::consts::{
    HAS_ADDITIONAL_KEYS_BIT, LOW_CARDINALITY_VERSION, NEED_GLOBAL_DICTIONARY_BIT,
    NEED_UPDATE_DICTIONARY_BIT, TUINT8, TUINT16, TUINT32, TUINT64,
};
use crate::parse::{parse_offsets, parse_u64,  parse_var_str_type, parse_varuint};
use crate::types::{JsonColumnHeader, OffsetIndexPair as _, Type};
use crate::{bt, t};
use log::{debug, info};

impl<'a> Type<'a> {
    pub(crate) fn decode_prefix(&self, mut ctx: ParseContext<'a>) -> IResult<&'a [u8], ()> {
        info!("Decoding prefix for type: {:?}", self);
        match self {
            Type::Nullable(inner) => {
                let (input, ()) = inner.decode_prefix(ctx.clone())?;
                return Ok((input, ()));
            }
            Type::Tuple(inner) => {
                for typ in inner {
                    let (input, ()) = typ.decode_prefix(ctx.clone())?;
                    ctx = ctx.fork(input);
                }

                return Ok((ctx.input, ()));
            }
            Type::Map(key, val) => {
                let inner_tuple = t!(Tuple(vec![*key.clone(), *val.clone()]));
                let (input, ()) = inner_tuple.decode_prefix(ctx.clone())?;
                return Ok((input, ()));
            }
            Type::Variant(inner) => {
                for typ in inner {
                    typ.decode_prefix(ctx.clone())?;
                    ctx = ctx.fork(ctx.input);
                }
                return Ok((ctx.input, ()));
            }
            Type::LowCardinality(_) => {
                let (input, version) = parse_u64::<u64>(ctx.input)?;
                info!("LowCardinality version: {version}");
                if version != LOW_CARDINALITY_VERSION {
                    return Err(Error::Parse(format!(
                        "LowCardinality version {version} is not supported, only {LOW_CARDINALITY_VERSION} is allowed"
                    )));
                }
                return Ok((input, ()));
            }
            Type::Array(inner) => {
                let (input, ()) = inner.decode_prefix(ctx.clone())?;
                return Ok((input, ()));
            }
            Type::Dynamic => {
                let (mut input, version) = parse_u64::<u64>(ctx.input)?;
                if version == 1 {
                    let legacy_columns: u64;
                    (input, legacy_columns) = parse_varuint(input)?;
                    debug!("Legacy columns: {legacy_columns}");
                }

                return Ok((input, ()));
            }
            Type::Json => {
                let (input, version) = parse_u64::<u64>(ctx.input)?;
                debug!("JSON version: {version}");
                return Ok((input, ()));
            }
            _ => {}
        }
        debug!("Nothing decoded for {:?}", self);
        Ok((ctx.input, ()))
    }

    pub(crate) fn decode(self, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
        if let Some(size) = self.size() {
            let (data, input) = ctx.input.split_at(size * ctx.num_rows);
            let marker = self.into_fixed_size_marker(data)?;
            return Ok((input, marker));
        }

        match self {
            Type::String => string(ctx),
            Type::Array(inner) => array(*inner, ctx),
            #[allow(clippy::match_same_arms)]
            Type::Ring => t!(Array(bt!(Point))).decode(ctx),
            Type::Polygon => t!(Array(bt!(Ring))).decode(ctx),
            Type::MultiPolygon => t!(Array(bt!(Polygon))).decode(ctx),
            Type::LineString => t!(Array(bt!(Point))).decode(ctx),
            Type::MultiLineString => t!(Array(bt!(LineString))).decode(ctx),
            Type::Tuple(inner) => tuple(inner, ctx),
            Type::Map(key, value) => map(*key, *value, ctx),
            Type::Variant(inner) => variant(inner, ctx),
            Type::LowCardinality(inner) => lc(*inner, ctx),
            Type::Nullable(inner) => nullable(*inner, ctx),
            Type::Dynamic => dynamic(ctx),
            Type::Json => json(ctx),
            _ => {
                todo!("Not implemented for {self:?}")
            }
        }
    }
}

fn json(ctx: ParseContext) -> IResult<&[u8], Mark> {
    let (input, num_paths_old) = parse_varuint::<u64>(ctx.input)?;
    debug!("num_paths_old: {num_paths_old}");

    let (input, num_paths) = parse_varuint(input)?;
    let (mut input, subcols) = Type::String.decode(ctx.fork(input).with_num_rows(num_paths))?;

    let mut col_headers = Vec::with_capacity(num_paths);

    for _ in 0..num_paths {
        let header;
        (input, header) = json_column_header(ctx.fork(input))?;
        col_headers.push(header);
    }

    let mut final_headers = Vec::with_capacity(num_paths);
    for mut header in col_headers {
        let discriminators;
        (discriminators, input) = input.split_at(ctx.num_rows);

        let local_rows = discriminators.iter().filter(|&&d| d != 255).count();
        let marker;
        (input, marker) = header
            .typ
            .clone()
            .decode(ctx.fork(input).with_num_rows(local_rows))?;
        header.mark = marker;
        final_headers.push(header);
    }

    let marker = Mark::Json {
        columns: Box::new(subcols),
        headers: final_headers,
    };

    let todo_wtf_is_it = ctx.num_rows * 8;
    let _wtf;
    (_wtf, input) = input.split_at(todo_wtf_is_it);

    Ok((input, marker))
}

fn dynamic(ctx: ParseContext) -> IResult<&[u8], Mark> {
    let (mut input, num_types) = parse_varuint(ctx.input)?;
    debug!("num_types: {num_types}");

    let mut types = Vec::with_capacity(num_types);
    for _ in 0..num_types {
        let typ;
        (input, typ) = parse_var_str_type(input)?;
        types.push(typ);
    }

    debug!("{:?}", types);

    // skip stats I guess?
    input = &input[8..];

    let mut discriminators = Vec::with_capacity(ctx.num_rows);
    let mut counters = vec![0usize; num_types];
    for _ in 0..ctx.num_rows {
        let discriminator;
        (input, discriminator) = parse_varuint(input)?;
        discriminators.push(discriminator);
        if discriminator == 0 {
            continue;
        }
        counters[discriminator - 1] += 1;
    }

    let mut markers = Vec::with_capacity(num_types);
    for (index, typ) in types.into_iter().enumerate() {
        let marker;
        (input, marker) = typ.decode(ctx.fork(input).with_num_rows(counters[index]))?;
        markers.push(marker);
    }

    let marker = Mark::Dynamic(discriminators, markers);
    Ok((input, marker))
}

fn nullable<'a>(inner: Type<'a>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let (mask, input) = ctx.input.split_at(ctx.num_rows);
    let (input, marker) = inner.decode(ctx.fork(input))?;
    Ok((input, Mark::Nullable(mask, Box::new(marker))))
}

fn lc<'a>(inner: Type<'a>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let (mut input, flags) = parse_u64::<u64>(ctx.input)?;
    info!("LowCardinality flags: {flags:#x}");
    let has_additional_keys = flags & HAS_ADDITIONAL_KEYS_BIT != 0;
    let needs_global_dictionary = flags & NEED_GLOBAL_DICTIONARY_BIT != 0;
    let _needs_update_dictionary = flags & NEED_UPDATE_DICTIONARY_BIT != 0;

    let index_type = match flags & 0xff {
        TUINT8 => Type::UInt8,
        TUINT16 => Type::UInt16,
        TUINT32 => Type::UInt32,
        TUINT64 => Type::UInt64,
        x => {
            return Err(Error::Parse(format!("LowCardinality: bad index type: {x}")));
        }
    };

    let base_inner = inner.strip_null().clone();

    let mut global_dictionary = None;
    if needs_global_dictionary {
        let cnt: usize;
        (input, cnt) = parse_u64(input)?;

        let dict_marker;
        (input, dict_marker) = base_inner
            .clone()
            .decode(ctx.fork(input).with_num_rows(cnt))?;
        global_dictionary = Some(Box::new(dict_marker));
    }

    let mut additional_keys = None;
    if has_additional_keys {
        let cnt: usize;
        (input, cnt) = parse_u64(input)?;

        let dict_marker;
        (input, dict_marker) = base_inner.decode(ctx.fork(input).with_num_rows(cnt))?;
        additional_keys = Some(Box::new(dict_marker));
    }

    let rows_here: usize;
    (input, rows_here) = parse_u64(input)?;
    if rows_here != ctx.num_rows {
        return Err(Error::Parse(format!(
            "LowCardinality: expected {} rows, got {rows_here}",
            ctx.num_rows
        )));
    }

    let (input, indices_marker) = index_type.clone().decode(ctx.fork(input))?;
    let marker = Mark::LowCardinality {
        index_type,
        indices: Box::new(indices_marker),
        global_dictionary,
        additional_keys,
    };

    Ok((input, marker))
}

fn variant<'a>(inner: Vec<Type<'a>>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    const NULL_DISCR: u8 = 255;
    let (input, mode) = parse_u64::<u64>(ctx.input)?;

    if mode != 0 {
        return Err(Error::Parse(format!(
            "Variant mode {mode} is not supported, only 0 is allowed"
        )));
    }

    let (discriminators, mut input) = input.split_at(ctx.num_rows);
    let mut row_counts = vec![0; inner.len()];
    for &discriminator in discriminators {
        if discriminator == NULL_DISCR {
            continue;
        }
        row_counts[discriminator as usize] += 1;
    }

    let mut markers = Vec::with_capacity(inner.len());

    for (idx, typ) in inner.into_iter().enumerate() {
        let marker;
        (input, marker) = typ.decode(ctx.fork(input).with_num_rows(row_counts[idx]))?;
        markers.push(marker);
    }

    let marker = Mark::Variant {
        discriminators,
        types: markers,
    };

    Ok((input, marker))
}

fn map<'a>(key: Type<'a>, value: Type<'a>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let (input, offsets) = parse_offsets(ctx.input, ctx.num_rows)?;
    let n = offsets.last_or_default()?;

    let (input, keys) = key.decode(ctx.fork(input).with_num_rows(n))?;
    let (input, values) = value.decode(ctx.fork(input).with_num_rows(n))?;

    let marker = Mark::Map {
        offsets,
        keys: keys.into(),
        values: values.into(),
    };

    Ok((input, marker))
}

fn tuple<'a>(inner: Vec<Type<'a>>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let mut markers = Vec::with_capacity(inner.len());
    let mut input = ctx.input;
    for typ in inner {
        let marker;
        (input, marker) = typ.decode(ctx.fork(input))?;
        markers.push(marker);
    }
    Ok((input, Mark::Tuple(markers)))
}

fn array<'a>(inner: Type<'a>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let (input, offsets) = parse_offsets(ctx.input, ctx.num_rows)?;
    let num_rows = offsets.last_or_default()?;
    debug!("Array num_rows: {}", num_rows);

    if num_rows == 0 {
        return Ok((input, Mark::Array(offsets, Box::new(Mark::Empty))));
    }

    let (input, inner_block) = inner.decode(ctx.fork(input).with_num_rows(num_rows))?;
    Ok((input, Mark::Array(offsets, Box::new(inner_block))))
}

fn string(ctx: ParseContext) -> IResult<&[u8], Mark> {
    let mut input = ctx.input;
    let mut offsets = Vec::with_capacity(ctx.num_rows);
    let mut offset = 0;
    let mut prev = ctx.input;
    for _ in 0..ctx.num_rows {
        let s;
        (input, s) = parse_var_str_bytes(input)?;
        let complete_len = s.as_ptr() as usize - prev.as_ptr() as usize + s.len();
        
        offset += complete_len;
        offsets.push(offset);
        prev = input;
    }
    println!("{:?}", offsets);
    
    assert_eq!(offsets.len(), ctx.num_rows);

    Ok((input, Mark::String(offsets, &ctx.input[..offset])))
}

fn json_column_header(ctx: ParseContext<'_>) -> IResult<&[u8], JsonColumnHeader> {
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
        },
    ))
}
