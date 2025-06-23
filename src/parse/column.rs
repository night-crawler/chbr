use crate::error::Error;
use crate::mark::{
    Mark, MarkArray, MarkDynamic, MarkJson, MarkLowCardinality, MarkMap, MarkNested, MarkNullable,
    MarkTuple, MarkVariant,
};
use crate::parse::block::ParseContext;
use crate::parse::consts::{
    HAS_ADDITIONAL_KEYS_BIT, LOW_CARDINALITY_VERSION, NEED_GLOBAL_DICTIONARY_BIT,
    NEED_UPDATE_DICTIONARY_BIT, TUINT8, TUINT16, TUINT32, TUINT64,
};
use crate::parse::typ::parse_type;
use crate::parse::{IResult, parse_var_str_bytes};
use crate::parse::{parse_offsets, parse_u64, parse_var_str, parse_var_str_type, parse_varuint};
use crate::types::{Field, JsonColumnHeader, OffsetIndexPair as _, Type};
use crate::{bt, t};
use log::debug;
use std::hint::unreachable_unchecked;

impl<'a> Type<'a> {
    pub(crate) fn decode_prefix(&self, mut ctx: ParseContext<'a>) -> IResult<&'a [u8], ()> {
        debug!("Decoding prefix for type: {self:?}");
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
                debug!("LowCardinality version: {version}");
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
            Type::Point => {
                // Point is represented by its X and Y coordinates, stored as a Tuple(Float64, Float64).
                let inner = t!(Tuple(vec![t!(Float64), t!(Float64)]));
                inner.decode(ctx)
            }
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
            Type::Nested(fields) => nested(fields, ctx),
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
    let (mut input, columns) = Type::String.decode(ctx.fork(input).with_num_rows(num_paths))?;
    let Mark::String(columns) = columns else {
        unsafe { unreachable_unchecked() };
    };

    let mut headers = Vec::with_capacity(num_paths);

    for _ in 0..num_paths {
        let header;
        (input, header) = json_column_header(ctx.fork(input))?;
        headers.push(header);
    }

    for header in &mut headers {
        let discriminators;
        (discriminators, input) = input.split_at(ctx.num_rows);

        let offsets = &mut header.offsets;
        let mut counter = 0usize;

        for (discriminator, offset) in discriminators.iter().copied().zip(offsets.iter_mut()) {
            *offset = counter;
            if discriminator != 255 {
                counter += 1;
            }
        }

        let marker;
        (input, marker) = header
            .typ
            .clone()
            .decode(ctx.fork(input).with_num_rows(counter))?;
        header.mark = marker;
        header.discriminators = discriminators;
    }

    let marker = Mark::Json(MarkJson {
        paths: columns,
        headers,
    });

    let todo_wtf_is_it = ctx.num_rows * 8;
    let _wtf;
    (_wtf, input) = input.split_at(todo_wtf_is_it);

    Ok((input, marker))
}

fn dynamic(ctx: ParseContext) -> IResult<&[u8], Mark> {
    let (mut input, num_types) = parse_varuint::<usize>(ctx.input)?;

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

    input = &input[8..];

    let mut discriminators = Vec::with_capacity(ctx.num_rows);
    let mut offsets = vec![0usize; ctx.num_rows];
    let mut row_counts = vec![0usize; types.len()];

    for offset in &mut offsets {
        let disc;
        (input, disc) = parse_varuint(input)?;

        *offset = row_counts[disc];
        row_counts[disc] += 1;

        discriminators.push(disc);
    }

    let mut columns = Vec::with_capacity(types.len());
    for (i, typ) in types.into_iter().enumerate() {
        if matches!(typ, Type::SharedVariant) {
            columns.push(Mark::Empty);
            continue;
        }

        let read_rows = row_counts[i];
        debug!(
            "Decoding dynamic column {i}: {typ:?}; remainder: {}; read rows: {read_rows}",
            input.len()
        );
        let marker;
        (input, marker) = typ.decode(ctx.fork(input).with_num_rows(read_rows))?;
        columns.push(marker);
    }

    let marker = Mark::Dynamic(MarkDynamic {
        offsets,
        discriminators,
        columns,
    });
    Ok((input, marker))
}

fn nullable<'a>(inner: Type<'a>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let (mask, input) = ctx.input.split_at(ctx.num_rows);
    let (input, marker) = inner.decode(ctx.fork(input))?;
    let mark_nullable = MarkNullable {
        mask,
        data: Box::new(marker),
    };
    Ok((input, Mark::Nullable(mark_nullable)))
}

fn lc<'a>(inner: Type<'a>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    if ctx.num_rows == 0 {
        return Ok((
            ctx.input,
            Mark::LowCardinality(MarkLowCardinality {
                indices: Box::new(Mark::Empty),
                global_dictionary: None,
                additional_keys: Some(Box::new(Mark::Empty)),
            }),
        ));
    }

    let (mut input, flags) = parse_u64::<u64>(ctx.input)?;
    let has_additional_keys = flags & HAS_ADDITIONAL_KEYS_BIT != 0;

    // why not supported?
    // https://github.com/ClickHouse/clickhouse-go/blob/main/lib/column/lowcardinality.go#L191
    let needs_global_dictionary = flags & NEED_GLOBAL_DICTIONARY_BIT != 0;
    let needs_update_dictionary = flags & NEED_UPDATE_DICTIONARY_BIT != 0;

    debug!(
        "LowCardinality rows: {} \
         has_additional_keys: {has_additional_keys}; \
         needs_global_dictionary: {needs_global_dictionary}; \
         needs_update_dictionary: {needs_update_dictionary}",
        ctx.num_rows
    );

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

    let (input, indices_marker) = index_type.decode(ctx.fork(input))?;
    let marker = Mark::LowCardinality(MarkLowCardinality {
        indices: Box::new(indices_marker),
        global_dictionary,
        additional_keys,
    });

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
    let mut offsets = Vec::with_capacity(ctx.num_rows);
    let mut row_counts = vec![0; inner.len()];
    for &discriminator in discriminators {
        offsets.push(row_counts[discriminator as usize]);
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

    let marker = Mark::Variant(MarkVariant {
        offsets,
        discriminators,
        types: markers,
    });

    Ok((input, marker))
}

fn map<'a>(key: Type<'a>, value: Type<'a>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let (input, offsets) = parse_offsets(ctx.input, ctx.num_rows)?;
    let n = offsets.last_or_default()?;

    debug!("Map got {n} rows");

    let (input, keys) = key.decode(ctx.fork(input).with_num_rows(n))?;
    let (input, values) = value.decode(ctx.fork(input).with_num_rows(n))?;

    let marker = Mark::Map(MarkMap {
        offsets,
        keys: keys.into(),
        values: values.into(),
    });

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

    let marker = MarkTuple { values: markers };
    Ok((input, Mark::Tuple(marker)))
}

fn array<'a>(inner: Type<'a>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let (input, offsets) = parse_offsets(ctx.input, ctx.num_rows)?;
    let num_rows = offsets.last_or_default()?;
    debug!("Array num_rows: {}", num_rows);
    debug!("offsets: {:?}", offsets);

    if num_rows == 0 {
        return Ok((
            input,
            Mark::Array(MarkArray {
                offsets,
                values: Box::new(Mark::Empty),
            }),
        ));
    }

    let (input, inner_block) = inner.decode(ctx.fork(input).with_num_rows(num_rows))?;
    Ok((
        input,
        Mark::Array(MarkArray {
            offsets,
            values: Box::new(inner_block),
        }),
    ))
}

fn string(ctx: ParseContext) -> IResult<&[u8], Mark> {
    let mut input = ctx.input;
    let mut strings = Vec::with_capacity(ctx.num_rows);
    for _ in 0..ctx.num_rows {
        let s;
        (input, s) = parse_var_str_bytes(input)?;
        strings.push(unsafe { std::str::from_utf8_unchecked(s) });
    }

    Ok((input, Mark::String(strings)))
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
            discriminators: &[],
            offsets: vec![0; ctx.num_rows],
        },
    ))
}

fn nested<'a>(fields: Vec<Field<'a>>, ctx: ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    debug!("Decoding Nested with {} fields", fields.len());

    let mut inner_types = Vec::with_capacity(fields.len());
    let mut col_names = Vec::with_capacity(fields.len());
    for f in fields {
        inner_types.push(f.typ);
        col_names.push(f.name);
    }

    let tuple_type = bt!(Tuple(inner_types));
    let array_of_tuples = t!(Array(tuple_type));

    let (input, inner_mark) = array_of_tuples.decode(ctx)?;

    let mark = Mark::Nested(MarkNested {
        col_names,
        array_of_tuples: Box::new(inner_mark),
    });

    Ok((input, mark))
}
