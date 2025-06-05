use crate::parse::block::ParseContext;
use crate::parse::{parse_offsets, parse_var_str_bytes};
use crate::types::{Data, Marker, Type};
use crate::{bt, t};
use log::debug;
use nom::IResult;
use zerocopy::U64;

impl<'a> Type<'a> {
    pub(crate) fn decode_prefix(&self, ctx: ParseContext<'a>) -> IResult<&'a [u8], ()> {
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
            _ => {
                todo!()
            }
        }
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
        inner.decode_prefix(ctx.clone())?;
        let (input, offsets) = parse_offsets(ctx.input, ctx.num_rows)?;
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
