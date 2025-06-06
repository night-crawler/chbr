use crate::parse::block::ParseContext;
use crate::parse::{parse_offsets, parse_u64, parse_var_str_bytes};
use crate::types::{
    Data, HAS_ADDITIONAL_KEYS_BIT, LOW_CARDINALITY_VERSION, Marker, NEED_GLOBAL_DICTIONARY_BIT,
    NEED_UPDATE_DICTIONARY_BIT, TUINT8, TUINT16, TUINT32, TUINT64, Type, u64_le,
};
use crate::{bt, t};
use log::{debug, error, info};
use nom::error::ErrorKind;
use nom::{IResult, Needed};
use zerocopy::U64;

impl<'a> Type<'a> {
    pub(crate) fn decode_prefix(&self, ctx: ParseContext<'a>) -> IResult<&'a [u8], ()> {
        info!("Decoding prefix for type: {:?}", self);
        match self {
            Type::Tuple(inner) => {
                for typ in inner {
                    typ.decode_prefix(ctx.clone())?;
                }
            }
            Type::Map(key, val) => {
                let inner_tuple = t!(Tuple(vec![*key.clone(), *val.clone()]));
                inner_tuple.decode_prefix(ctx.clone())?;
            }
            Type::Variant(inner) => {
                for typ in inner {
                    typ.decode_prefix(ctx.clone())?;
                }
            }
            Type::LowCardinality(_) => {
                let (input, version) = parse_u64(ctx.input)?;
                info!("LowCardinality version: {version}");
                if version != LOW_CARDINALITY_VERSION {
                    return Err(nom::Err::Failure(nom::error::make_error(
                        ctx.input,
                        ErrorKind::Fail,
                    )));
                }
                return Ok((input, ()));
            }
            _ => {}
        }
        Ok((ctx.input, ()))
    }

    pub(crate) fn decode(self, ctx: ParseContext<'a>) -> IResult<&'a [u8], Marker<'a>> {
        if let Some(size) = self.size() {
            let (data, input) = ctx.input.split_at(size * ctx.num_rows);
            let data = Data {
                data,
                num_rows: ctx.num_rows,
            };
            return Ok((input, self.into_fixed_size_marker(data)));
        }

        match self {
            Type::String => self.decode_string(ctx),
            Type::Array(inner) => Self::decode_array(*inner, ctx),
            Type::Ring => t!(Array(bt!(Point))).decode(ctx),
            Type::Polygon => t!(Array(bt!(Ring))).decode(ctx),
            Type::MultiPolygon => t!(Array(bt!(Polygon))).decode(ctx),
            Type::LineString => t!(Array(bt!(Point))).decode(ctx),
            Type::MultiLineString => t!(Array(bt!(LineString))).decode(ctx),
            Type::Tuple(inner) => Self::decode_tuple(inner, ctx),
            Type::Map(key, value) => Self::decode_map(*key, *value, ctx),
            Type::Variant(inner) => Self::decode_variant(inner, ctx),
            Type::LowCardinality(inner) => Self::decode_lc(*inner, ctx),
            _ => {
                todo!("Not implemented for {self:?}")
            }
        }
    }

    pub(crate) fn decode_lc(
        inner: Type<'a>,
        ctx: ParseContext<'a>,
    ) -> IResult<&'a [u8], Marker<'a>> {
        let (mut input, flags) = parse_u64(ctx.input)?;
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
                error!("LowCardinality: bad index type: {x}");
                return Err(nom::Err::Failure(nom::error::make_error(
                    ctx.input,
                    ErrorKind::Fail,
                )));
            }
        };

        let base_inner = inner.strip_null().clone();

        let mut global_dictionary = None;
        if needs_global_dictionary {
            let cnt;
            (input, cnt) = parse_u64(input)?;

            let dict_marker;
            (input, dict_marker) = base_inner
                .clone()
                .decode(ctx.fork(input).with_num_rows(cnt as usize))?;
            global_dictionary = Some(Box::new(dict_marker));
        }

        let mut additional_keys = None;
        if has_additional_keys {
            let cnt;
            (input, cnt) = parse_u64(input)?;

            let dict_marker;
            (input, dict_marker) =
                base_inner.decode(ctx.fork(input).with_num_rows(cnt as usize))?;
            additional_keys = Some(Box::new(dict_marker));
        }

        let rows_here;
        (input, rows_here) = parse_u64(input)?;
        if rows_here as usize != ctx.num_rows {
            return Err(nom::Err::Incomplete(Needed::Unknown));
        }

        let (input, indices_marker) = index_type.clone().decode(ctx.fork(input))?;
        let marker = Marker::LowCardinality {
            index_type,
            indices: Box::new(indices_marker),
            global_dictionary,
            additional_keys,
        };

        Ok((input, marker))
    }

    pub(crate) fn decode_variant(
        inner: Vec<Type<'a>>,
        ctx: ParseContext<'a>,
    ) -> IResult<&'a [u8], Marker<'a>> {
        const NULL_DISCR: u8 = 255;
        let (input, mode) = parse_u64(ctx.input)?;

        if mode != 0 {
            return Err(nom::Err::Failure(nom::error::make_error(
                ctx.input,
                ErrorKind::Fail,
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

        let marker = Marker::Variant {
            discriminators,
            types: markers,
        };

        Ok((input, marker))
    }

    pub(crate) fn decode_map(
        key: Type<'a>,
        value: Type<'a>,
        ctx: ParseContext<'a>,
    ) -> IResult<&'a [u8], Marker<'a>> {
        let (input, offsets) = parse_offsets(ctx.input, ctx.num_rows)?;
        let n = offsets.last().copied().unwrap_or(U64::from(0)).get() as usize;

        let (input, keys) = key.decode(ctx.fork(input).with_num_rows(n))?;
        let (input, values) = value.decode(ctx.fork(input).with_num_rows(n))?;

        let marker = Marker::Map {
            offsets,
            keys: keys.into(),
            values: values.into(),
        };

        Ok((input, marker))
    }

    pub(crate) fn decode_tuple(
        inner: Vec<Type<'a>>,
        ctx: ParseContext<'a>,
    ) -> IResult<&'a [u8], Marker<'a>> {
        let mut markers = Vec::with_capacity(inner.len());
        let mut input = ctx.input;
        for typ in inner {
            let marker;
            (input, marker) = typ.decode(ctx.fork(input))?;
            markers.push(marker);
        }
        Ok((input, Marker::VarTuple(markers)))
    }

    pub(crate) fn decode_array(
        inner: Type<'a>,
        ctx: ParseContext<'a>,
    ) -> IResult<&'a [u8], Marker<'a>> {
        let (input, _) = inner.decode_prefix(ctx.clone())?;
        let (input, offsets) = parse_offsets(input, ctx.num_rows)?;
        let num_rows = offsets.last().copied().unwrap_or(U64::from(0)).get() as usize;
        debug!("Array num_rows: {}", num_rows);

        let (input, inner_block) = inner.decode(ctx.fork(input).with_num_rows(num_rows))?;
        Ok((input, inner_block))
    }

    pub(crate) fn decode_string(self, ctx: ParseContext<'a>) -> IResult<&'a [u8], Marker<'a>> {
        let mut input = ctx.input;
        for _ in 0..ctx.num_rows {
            (input, _) = parse_var_str_bytes(input)?;
        }
        let data = Data {
            data: ctx.input,
            num_rows: ctx.num_rows,
        };

        Ok((input, Marker::String(data)))
    }
}
