use std::hint::unreachable_unchecked;

use log::debug;

use crate::parse::header::{dynamic_header, map_header, nested_header, variant_header};
use crate::types::{DynamicHeader, JsonHeader, MapHeader, TypeHeader};
use crate::{
    error::Error,
    macros::{bt, t},
    mark::{Array, Dynamic, Json, LowCardinality, Map, Mark, Nested, Nullable, Tuple, Variant},
    parse::{
        IResult,
        block::ParseContext,
        consts::{
            HAS_ADDITIONAL_KEYS_BIT, LOW_CARDINALITY_VERSION, NEED_GLOBAL_DICTIONARY_BIT,
            NEED_UPDATE_DICTIONARY_BIT, TUINT8, TUINT16, TUINT32, TUINT64,
        },
        parse_offsets, parse_u64, parse_var_str_bytes, parse_var_str_type, parse_varuint,
    },
    types::{Field, JsonColumnHeader, OffsetIndexPair as _, Type},
};

impl<'a> Type<'a> {
    pub(crate) fn decode_header(
        &self,
        mut ctx: ParseContext<'a>,
    ) -> IResult<&'a [u8], TypeHeader<'a>> {
        debug!("Decoding header for type: {self:?}");
        match self {
            Type::Nullable(inner) => {
                let (input, th) = inner.decode_header(ctx.clone())?;
                return Ok((input, th));
            }
            Type::Tuple(inner) => {
                let mut headers = Vec::with_capacity(inner.len());
                for typ in inner {
                    let (input, th) = typ.decode_header(ctx.clone())?;
                    headers.push(th);
                    ctx = ctx.fork(input);
                }

                return Ok((ctx.input, TypeHeader::Tuple(headers)));
            }
            Type::Map(key, val) => {
                let (input, h) = map_header(&ctx, key, val)?;
                return Ok((input, TypeHeader::Map(h.into())));
            }
            Type::Variant(inner) => {
                let (input, headers) = variant_header(&ctx, inner)?;
                return Ok((input, TypeHeader::Variant(headers)));
            }
            Type::LowCardinality(_) => {
                let (input, version) = parse_u64::<u64>(ctx.input)?;
                debug!("LowCardinality version: {version}");
                if version != LOW_CARDINALITY_VERSION {
                    return Err(Error::Parse(format!(
                        "LowCardinality version {version} is not supported, only \
                         {LOW_CARDINALITY_VERSION} is allowed"
                    )));
                }
                return Ok((input, TypeHeader::Empty));
            }
            Type::Array(inner) => {
                let (input, th) = inner.decode_header(ctx.clone())?;
                return Ok((input, TypeHeader::Array(th.into())));
            }
            Type::Dynamic => {
                let (input, header) = dynamic_header(&ctx)?;
                return Ok((input, TypeHeader::Dynamic(header.into())));
            }
            Type::Json => {
                let (input, version) = parse_u64::<u64>(ctx.input)?;
                debug!("JSON version: {version}");
                // todo: parse
                return Ok((input, TypeHeader::Empty));
            }
            Type::Nested(fields) => {
                let (input, header) = nested_header(&ctx, fields)?;
                return Ok((input, TypeHeader::Nested(header.into())));
            }
            _ => {}
        }
        debug!("Nothing decoded for {:?}", self);
        Ok((ctx.input, TypeHeader::Empty))
    }

    pub(crate) fn decode(
        self,
        ctx: ParseContext<'a>,
        header: TypeHeader<'a>,
    ) -> IResult<&'a [u8], Mark<'a>> {
        if let Some(size) = self.size() {
            let (data, input) = ctx.input.split_at(size * ctx.num_rows);
            let marker = self.into_fixed_size_marker(data)?;
            return Ok((input, marker));
        }

        match self {
            Type::String => string(ctx),
            Type::Array(inner) => array(*inner, &ctx, header.into_array()),
            Type::Point => {
                // Point is represented by its X and Y coordinates, stored as a Tuple(Float64, Float64).
                let inner = t!(Tuple(vec![t!(Float64), t!(Float64)]));
                inner.decode(ctx, TypeHeader::Empty)
            }
            #[expect(clippy::match_same_arms)]
            Type::Ring => t!(Array(bt!(Point))).decode(ctx, header.into_array()),
            Type::Polygon => t!(Array(bt!(Ring))).decode(ctx, header.into_array()),
            Type::MultiPolygon => t!(Array(bt!(Polygon))).decode(ctx, header.into_array()),
            Type::LineString => t!(Array(bt!(Point))).decode(ctx, header.into_array()),
            Type::MultiLineString => t!(Array(bt!(LineString))).decode(ctx, header.into_array()),
            Type::Tuple(inner) => tuple(inner, &ctx, header.into_tuple()),
            Type::Map(key, value) => map(*key, *value, &ctx, header.into_map()),
            Type::Variant(inner) => variant(inner, &ctx, header.into_variant()),
            Type::LowCardinality(inner) => lc(inner.as_ref(), &ctx),
            Type::Nullable(inner) => nullable(*inner, &ctx, header.into_nullable()),
            Type::Dynamic => dynamic(&ctx, header.into_dynamic()),
            Type::Json => json(&ctx, header.into_json()),
            Type::Nested(fields) => nested(fields, ctx, header.into_nested()),
            _ => {
                todo!("Not implemented for {self:?}")
            }
        }
    }
}

fn json<'a>(ctx: &ParseContext<'a>, _x: JsonHeader<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let (input, num_paths_old) = parse_varuint::<u64>(ctx.input)?;
    debug!("num_paths_old: {num_paths_old}");

    let (input, num_paths) = parse_varuint(input)?;
    let (mut input, paths) = string(ctx.fork(input).with_num_rows(num_paths))?;
    let Mark::String(paths) = paths else {
        unsafe { unreachable_unchecked() };
    };

    let mut headers = Vec::with_capacity(num_paths);

    for _ in 0..num_paths {
        let header;
        (input, header) = json_column_header(&ctx.fork(input))?;
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
            .decode(ctx.fork(input).with_num_rows(counter), TypeHeader::Empty)?;
        header.mark = marker;
        header.discriminators = discriminators;
    }

    let marker = Mark::Json(Json { paths, headers });

    // https://github.com/ClickHouse/clickhouse-go/blob/71a2b475e899afe9626f40af513bcf25aa3098a2/lib/column/json.go#L569-L572
    let shared_data_size = ctx.num_rows * 8;
    let _shared_data;
    (_shared_data, input) = input.split_at(shared_data_size);

    Ok((input, marker))
}

fn dynamic<'a>(ctx: &ParseContext<'a>, header: DynamicHeader<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let types = header.types;
    let mut discriminators = Vec::with_capacity(ctx.num_rows);
    let mut offsets = vec![0usize; ctx.num_rows];
    let mut row_counts = vec![0usize; types.len()];

    let mut input = ctx.input;

    for offset in &mut offsets {
        let disc;
        (input, disc) = parse_varuint(input)?;

        *offset = row_counts[disc];
        row_counts[disc] += 1;

        discriminators.push(disc);
    }

    let mut columns = Vec::with_capacity(types.len());
    for ((i, typ), header) in types.into_iter().enumerate().zip(header.headers) {
        if matches!(typ, Type::SharedVariant) {
            columns.push(Mark::Empty);
            continue;
        }

        let read_rows = row_counts[i];
        debug!(
            "Decoding dynamic column {i}: {typ:?}, {header:?}; remainder: {}; read rows: {read_rows}",
            input.len()
        );
        let marker;
        (input, marker) = typ.decode(ctx.fork(input).with_num_rows(read_rows), header)?;
        columns.push(marker);
    }

    let marker = Mark::Dynamic(Dynamic {
        offsets,
        discriminators,
        columns,
    });

    Ok((input, marker))
}

fn nullable<'a>(
    inner: Type<'a>,
    ctx: &ParseContext<'a>,
    header: TypeHeader<'a>,
) -> IResult<&'a [u8], Mark<'a>> {
    let (mask, input) = ctx.input.split_at(ctx.num_rows);
    // here we pass through the header
    let (input, marker) = inner.decode(ctx.fork(input), header)?;
    let mark_nullable = Nullable {
        mask,
        data: Box::new(marker),
    };
    Ok((input, Mark::Nullable(mark_nullable)))
}

fn lc<'a>(inner: &Type<'a>, ctx: &ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    if ctx.num_rows == 0 {
        return Ok((
            ctx.input,
            Mark::LowCardinality(LowCardinality {
                is_nullable: inner.is_nullable(),
                indices: Box::new(Mark::Empty),
                global_dictionary: None,
                additional_keys: Some(Box::new(Mark::Empty)),
            }),
        ));
    }

    let (mut input, flags) = parse_u64::<u64>(ctx.input)?;
    let has_additional_keys = flags & HAS_ADDITIONAL_KEYS_BIT != 0;

    // why not supported?
    // https://github.com/ClickHouse/clickhouse-go/blob/71a2b475e899afe9626f40af513bcf25aa3098a2/lib/column/lowcardinality.go#L191
    let needs_global_dictionary = flags & NEED_GLOBAL_DICTIONARY_BIT != 0;
    let needs_update_dictionary = flags & NEED_UPDATE_DICTIONARY_BIT != 0;

    debug!(
        "LowCardinality rows: {} has_additional_keys: {has_additional_keys}; \
         needs_global_dictionary: {needs_global_dictionary}; needs_update_dictionary: \
         {needs_update_dictionary}",
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
            .decode(ctx.fork(input).with_num_rows(cnt), TypeHeader::Empty)?;
        global_dictionary = Some(Box::new(dict_marker));
    }

    let mut additional_keys = None;
    if has_additional_keys {
        let cnt: usize;
        (input, cnt) = parse_u64(input)?;

        let dict_marker;
        (input, dict_marker) =
            base_inner.decode(ctx.fork(input).with_num_rows(cnt), TypeHeader::Empty)?;
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

    let (input, indices_marker) = index_type.decode(ctx.fork(input), TypeHeader::Empty)?;
    let marker = Mark::LowCardinality(LowCardinality {
        is_nullable: inner.is_nullable(),
        indices: Box::new(indices_marker),
        global_dictionary,
        additional_keys,
    });

    Ok((input, marker))
}

fn variant<'a>(
    inner: Vec<Type<'a>>,
    ctx: &ParseContext<'a>,
    headers: Vec<TypeHeader<'a>>,
) -> IResult<&'a [u8], Mark<'a>> {
    const NULL_DISCR: u8 = 255;

    let input = ctx.input;

    let (discriminators, mut input) = input.split_at(ctx.num_rows);
    let mut offsets = Vec::with_capacity(ctx.num_rows);
    let mut row_counts = vec![0; inner.len()];
    for &discriminator in discriminators {
        offsets.push(row_counts[discriminator as usize]);
        if discriminator == NULL_DISCR {
            continue;
        }
        if let Some(count) = row_counts.get_mut(discriminator as usize) {
            *count += 1;
        } else {
            return Err(Error::Parse(format!(
                "Variant: discriminator {discriminator} out of bounds for inner types length {}",
                inner.len()
            )));
        }
    }

    let mut markers = Vec::with_capacity(inner.len());

    for ((idx, typ), header) in inner.into_iter().enumerate().zip(headers) {
        let marker;
        (input, marker) = typ.decode(ctx.fork(input).with_num_rows(row_counts[idx]), header)?;
        markers.push(marker);
    }

    let marker = Mark::Variant(Variant {
        offsets,
        discriminators,
        types: markers,
    });

    Ok((input, marker))
}

fn map<'a>(
    key: Type<'a>,
    value: Type<'a>,
    ctx: &ParseContext<'a>,
    header: MapHeader<'a>,
) -> IResult<&'a [u8], Mark<'a>> {
    let (input, offsets) = parse_offsets(ctx.input, ctx.num_rows)?;
    let n = offsets.last_or_default()?;

    debug!("Map got {n} rows");

    let (input, keys) = key.decode(ctx.fork(input).with_num_rows(n), header.key)?;
    let (input, values) = value.decode(ctx.fork(input).with_num_rows(n), header.value)?;

    let marker = Mark::Map(Map {
        offsets,
        keys: keys.into(),
        values: values.into(),
    });

    Ok((input, marker))
}

fn tuple<'a>(
    inner: Vec<Type<'a>>,
    ctx: &ParseContext<'a>,
    headers: Vec<TypeHeader<'a>>,
) -> IResult<&'a [u8], Mark<'a>> {
    let mut markers = Vec::with_capacity(inner.len());
    let mut input = ctx.input;
    for (typ, header) in inner.into_iter().zip(headers) {
        let marker;
        (input, marker) = typ.decode(ctx.fork(input), header)?;
        markers.push(marker);
    }

    let marker = Tuple { values: markers };
    Ok((input, Mark::Tuple(marker)))
}

fn array<'a>(
    inner: Type<'a>,
    ctx: &ParseContext<'a>,
    header: TypeHeader<'a>,
) -> IResult<&'a [u8], Mark<'a>> {
    let (input, offsets) = parse_offsets(ctx.input, ctx.num_rows)?;
    let num_rows = offsets.last_or_default()?;
    debug!("offsets: {:?}", offsets);
    debug!("Array num_rows: {}", num_rows);

    if num_rows == 0 {
        return Ok((
            input,
            Mark::Array(Array {
                offsets,
                values: Box::new(Mark::Empty),
            }),
        ));
    }

    let (input, inner_block) = inner.decode(ctx.fork(input).with_num_rows(num_rows), header)?;
    Ok((
        input,
        Mark::Array(Array {
            offsets,
            values: Box::new(inner_block),
        }),
    ))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "ParseContext can't be consumed"
)]
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

fn json_column_header<'a>(ctx: &ParseContext<'a>) -> IResult<&'a [u8], JsonColumnHeader<'a>> {
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

fn nested<'a>(
    fields: Vec<Field<'a>>,
    ctx: ParseContext<'a>,
    headers: Vec<TypeHeader<'a>>,
) -> IResult<&'a [u8], Mark<'a>> {
    debug!("Decoding Nested with {} fields", fields.len());

    let mut inner_types = Vec::with_capacity(fields.len());
    let mut col_names = Vec::with_capacity(fields.len());
    for f in fields {
        inner_types.push(f.typ);
        col_names.push(f.name);
    }

    let tuple_type = bt!(Tuple(inner_types));
    let array_of_tuples = t!(Array(tuple_type));
    let header = TypeHeader::Array(Box::new(TypeHeader::Tuple(headers)));

    let (input, inner_mark) = array_of_tuples.decode(ctx, header)?;

    let mark = Mark::Nested(Nested {
        col_names,
        array_of_tuples: Box::new(inner_mark),
    });

    Ok((input, mark))
}
