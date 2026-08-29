use bstr::BStr;
use log::debug;

use std::hint::cold_path;

use crate::mark::StringView;
use crate::{
    error::Error,
    macros::{bt, t},
    mark::{self, Array, Dynamic, Json, Map, Mark, NamedTuple, Nested, Nullable, Tuple, Variant},
    parse::{
        IResult,
        block::ParseContext,
        consts::{
            HAS_ADDITIONAL_KEYS_BIT, NEED_GLOBAL_DICTIONARY_BIT, NEED_UPDATE_DICTIONARY_BIT,
            TUINT8, TUINT16, TUINT32, TUINT64,
        },
        header, parse_offsets, parse_u64, parse_var_str_bytes, take_elements,
    },
    types::{DynamicHeader, Field, JsonHeader, MapHeader, OffsetIndexPair as _, Type, TypeHeader},
};

impl<'a> Type<'a> {
    pub(crate) fn decode_header(
        &self,
        ctx: &ParseContext<'a>,
    ) -> IResult<&'a [u8], TypeHeader<'a>> {
        debug!("Decoding header for type: {self:?}");
        if ctx.num_rows == 0 {
            cold_path();
            // CPP NativeWriter serializes prefixes only when the block has rows, so we would just
            // fail to parse anything at all.
            return Ok((ctx.input, self.empty_header()));
        }

        match self {
            Type::Nullable(inner) => {
                let (input, th) = inner.decode_header(ctx)?;
                Ok((input, th))
            }
            Type::Tuple(inner) => {
                let (input, headers) = header::tuple(ctx, inner)?;
                Ok((input, TypeHeader::Tuple(headers)))
            }
            Type::Map(key, val) => {
                let (input, h) = header::map(ctx, key, val)?;
                Ok((input, TypeHeader::Map(h.into())))
            }
            Type::Variant(inner) => {
                let (input, headers) = header::variant(ctx, inner)?;
                Ok((input, TypeHeader::Variant(headers)))
            }
            Type::LowCardinality(_) => {
                let (input, header) = header::lc(ctx)?;
                Ok((input, header))
            }
            Type::Array(inner) => {
                let (input, th) = inner.decode_header(ctx)?;
                Ok((input, TypeHeader::Array(th.into())))
            }
            Type::Dynamic => {
                let (input, header) = header::dynamic(ctx)?;
                Ok((input, TypeHeader::Dynamic(header.into())))
            }
            Type::Json(typed_paths) => {
                let (input, header) = header::json(ctx, typed_paths)?;
                Ok((input, TypeHeader::Json(header.into())))
            }
            Type::Nested(fields) => {
                let (input, header) = header::nested(ctx, fields)?;
                Ok((input, TypeHeader::Nested(header)))
            }
            Type::NamedTuple(fields) => {
                let (input, header) = header::named_tuple(ctx, fields)?;
                Ok((input, TypeHeader::Nested(header)))
            }
            Type::Point => Ok((ctx.input, header::point())),
            Type::Ring | Type::LineString => Ok((ctx.input, header::ring())),
            Type::Polygon | Type::MultiLineString => Ok((ctx.input, header::polygon())),
            Type::MultiPolygon => Ok((ctx.input, header::multi_polygon())),
            _ => {
                debug!("Nothing decoded for {:?}", self);
                Ok((ctx.input, TypeHeader::Empty))
            }
        }
    }

    /// A zero-block corner case.
    fn empty_header(&self) -> TypeHeader<'a> {
        match self {
            Type::Nullable(inner) => inner.empty_header(),
            Type::Array(inner) => TypeHeader::Array(Box::new(inner.empty_header())),
            Type::Tuple(inner) => TypeHeader::Tuple(inner.iter().map(Type::empty_header).collect()),
            Type::Map(key, val) => TypeHeader::Map(Box::new(MapHeader {
                key: key.empty_header(),
                value: val.empty_header(),
            })),
            Type::Variant(inner) => {
                TypeHeader::Variant(inner.iter().map(Type::empty_header).collect())
            }
            Type::Dynamic => TypeHeader::Dynamic(Box::new(DynamicHeader {
                types: Vec::new(),
                headers: Vec::new(),
            })),
            Type::Json(_) => TypeHeader::Json(Box::new(JsonHeader {
                paths: Vec::new(),
                col_headers: Vec::new(),
            })),
            Type::Nested(fields) | Type::NamedTuple(fields) => {
                TypeHeader::Nested(fields.iter().map(|f| f.typ.empty_header()).collect())
            }
            Type::Point => header::point(),
            Type::Ring | Type::LineString => header::ring(),
            Type::Polygon | Type::MultiLineString => header::polygon(),
            Type::MultiPolygon => header::multi_polygon(),
            _ => TypeHeader::Empty,
        }
    }

    pub(crate) fn decode(
        self,
        ctx: ParseContext<'a>,
        header: TypeHeader<'a>,
    ) -> IResult<&'a [u8], Mark<'a>> {
        debug!("Decoding type: {self:?} with header: {header:?}");

        if let Some(size) = self.size() {
            let (input, data) = take_elements(ctx.input, size, ctx.num_rows, "column byte length")?;
            let marker = self.into_fixed_size_marker(data)?;
            return Ok((input, marker));
        }

        match self {
            Type::String => string(&ctx),
            Type::Array(inner) => array(*inner, &ctx, header.into_array()),
            Type::Point => t!(Tuple(vec![t!(Float64), t!(Float64)])).decode(ctx, header),
            Type::Ring | Type::LineString => t!(Array(bt!(Point))).decode(ctx, header),
            Type::Polygon | Type::MultiLineString => t!(Array(bt!(Ring))).decode(ctx, header),
            Type::MultiPolygon => t!(Array(bt!(Polygon))).decode(ctx, header),
            Type::Tuple(inner) => tuple(inner, &ctx, header.into_tuple()),
            Type::Map(key, value) => map(*key, *value, &ctx, header.into_map()),
            Type::Variant(inner) => variant(inner, &ctx, header.into_variant()),
            Type::LowCardinality(inner) => lc(inner.as_ref(), &ctx),
            Type::Nullable(inner) => nullable(*inner, &ctx, header.into_nullable()),
            Type::Dynamic => dynamic(&ctx, header.into_dynamic()),
            Type::Json(_) => json(&ctx, header.into_json()),
            Type::Nested(fields) => nested(fields, ctx, header.into_nested()),
            Type::NamedTuple(fields) => named_tuple(fields, &ctx, header.into_nested()),
            _ => {
                cold_path();
                Err(Error::NotImplemented(format!("decode for {self:?}")))
            }
        }
    }
}

/// BASIC-mode discriminator
struct Discriminators<'a> {
    /// Raw discriminators slice
    raw: &'a [u8],
    /// Index of the type's sub-column
    offsets: Vec<u32>,
    /// Number of rows in each type's sub-column
    row_counts: Vec<u32>,
}

impl<'a> Discriminators<'a> {
    fn parse(
        input: &'a [u8],
        num_rows: usize,
        num_types: usize,
        what: &'static str,
    ) -> IResult<&'a [u8], Discriminators<'a>> {
        if u32::try_from(num_rows).is_err() {
            cold_path();
            return Err(Error::Overflow(format!("{what}: {num_rows} rows")));
        }
        let Some((raw, rest)) = input.split_at_checked(num_rows) else {
            cold_path();
            return Err(Error::Length(num_rows));
        };

        let mut offsets = vec![0u32; num_rows];
        let mut row_counts = vec![0u32; num_types];
        for (raw, offset) in raw.iter().copied().zip(offsets.iter_mut()) {
            if raw == Variant::NULL_DISCRIMINATOR {
                continue;
            }
            let Some(row_count) = row_counts.get_mut(usize::from(raw)) else {
                cold_path();
                return Err(Error::CorruptedData(format!(
                    "{what} discriminator {raw} out of bounds for {num_types} types"
                )));
            };
            *offset = *row_count;
            *row_count += 1;
        }

        Ok((
            rest,
            Discriminators {
                raw,
                offsets,
                row_counts,
            },
        ))
    }
}

fn json<'a>(
    ctx: &ParseContext<'a>,
    JsonHeader {
        paths,
        mut col_headers,
    }: JsonHeader<'a>,
) -> IResult<&'a [u8], Mark<'a>> {
    let mut input = ctx.input;
    let num_rows = ctx.num_rows;

    for col_header in &mut col_headers {
        if col_header.is_typed {
            let Some(typ) = col_header.types.pop() else {
                cold_path();
                return Err(Error::CorruptedData(
                    "typed JSON path is missing its type".to_owned(),
                ));
            };
            let Some(type_header) = col_header.type_headers.pop() else {
                cold_path();
                return Err(Error::CorruptedData(
                    "typed JSON path is missing its type header".to_owned(),
                ));
            };
            (input, col_header.mark) = typ.decode(ctx.fork(input), type_header)?;
            continue;
        }

        let (
            remainder,
            Discriminators {
                raw: raw_discriminators,
                offsets,
                row_counts,
            },
        ) = Discriminators::parse(input, num_rows, col_header.types.len(), "JSON path")?;
        input = remainder;

        let mut columns = Vec::with_capacity(col_header.types.len());
        for (((index, typ), type_header), read_rows) in col_header
            .types
            .drain(..)
            .enumerate()
            .zip(col_header.type_headers.drain(..))
            .zip(row_counts)
        {
            if matches!(typ, Type::SharedVariant) {
                columns.push(Mark::Empty);
                continue;
            }
            let marker;
            (input, marker) = typ.decode(
                ctx.fork(input).with_num_rows(read_rows as usize),
                type_header,
            )?;
            debug!("Decoded JSON path type {index} with {read_rows} rows");
            columns.push(marker);
        }
        col_header.mark = Mark::Dynamic(Dynamic {
            offsets,
            discriminators: raw_discriminators,
            columns,
        });
    }

    let marker = Mark::Json(Json::new(paths, col_headers, num_rows)?);

    let (input, shared_data_offsets) =
        take_elements(input, num_rows, 8, "JSON shared data offsets")?;
    if shared_data_offsets
        .chunks_exact(8)
        .any(|offset| offset != [0; 8])
    {
        cold_path();
        return Err(Error::NotImplemented(
            "non-empty JSON shared data".to_owned(),
        ));
    }

    Ok((input, marker))
}

fn dynamic<'a>(ctx: &ParseContext<'a>, header: DynamicHeader<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let types = header.types;

    let (
        mut input,
        Discriminators {
            raw: discriminators,
            offsets,
            row_counts,
        },
    ) = Discriminators::parse(ctx.input, ctx.num_rows, types.len(), "Dynamic")?;

    let mut columns = Vec::with_capacity(types.len());
    for ((i, typ), header) in types.into_iter().enumerate().zip(header.headers) {
        if matches!(typ, Type::SharedVariant) {
            columns.push(Mark::Empty);
            continue;
        }

        let read_rows = row_counts[i] as usize;
        debug!(
            "Decoding dynamic column {i}: {typ:?}, {header:?}; remainder: {}; read rows: \
             {read_rows}",
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
    let Some((mask, input)) = ctx.input.split_at_checked(ctx.num_rows) else {
        cold_path();
        return Err(Error::Length(ctx.num_rows));
    };
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
            Mark::LowCardinality(mark::lc::LowCardinality {
                is_nullable: inner.is_nullable(),
                indices: mark::lc::Indices::Empty,
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
            cold_path();
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
        cold_path();
        return Err(Error::Parse(format!(
            "LowCardinality: expected {} rows, got {rows_here}",
            ctx.num_rows
        )));
    }

    let (input, indices_marker) = index_type.decode(ctx.fork(input), TypeHeader::Empty)?;
    let marker = Mark::LowCardinality(mark::lc::LowCardinality {
        is_nullable: inner.is_nullable(),
        indices: indices_marker.try_into()?,
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
    let (
        mut input,
        Discriminators {
            raw: discriminators,
            offsets,
            row_counts,
        },
    ) = Discriminators::parse(ctx.input, ctx.num_rows, inner.len(), "Variant")?;

    let mut markers = Vec::with_capacity(inner.len());

    for ((idx, typ), header) in inner.into_iter().enumerate().zip(headers) {
        let marker;
        (input, marker) = typ.decode(
            ctx.fork(input).with_num_rows(row_counts[idx] as usize),
            header,
        )?;
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

pub(super) fn string<'a>(ctx: &ParseContext<'a>) -> IResult<&'a [u8], Mark<'a>> {
    let mut input = ctx.input;
    let mut strings = Vec::with_capacity(ctx.num_rows);
    for _ in 0..ctx.num_rows {
        let s;
        (input, s) = parse_var_str_bytes(input)?;
        strings.push(BStr::new(s));
    }

    Ok((input, Mark::String(StringView { data: strings })))
}

fn named_tuple<'a>(
    fields: Vec<Field<'a>>,
    ctx: &ParseContext<'a>,
    headers: Vec<TypeHeader<'a>>,
) -> IResult<&'a [u8], Mark<'a>> {
    debug!("Decoding NamedTuple with {} fields", fields.len());

    let mut inner_types = Vec::with_capacity(fields.len());
    let mut col_names = Vec::with_capacity(fields.len());
    for f in fields {
        inner_types.push(f.typ);
        col_names.push(f.name);
    }

    let (input, tuple_mark) = tuple(inner_types, ctx, headers)?;

    let mark = Mark::NamedTuple(NamedTuple {
        col_names,
        tuple: Box::new(tuple_mark),
    });

    Ok((input, mark))
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
