use std::ops::Deref;

use log::debug;

use crate::{
    ParsedBlock,
    parse::{IResult, parse_var_str, parse_varuint, typ::parse_type},
};

#[derive(Debug, Clone)]
pub struct ParseContext<'a> {
    pub initial: &'a [u8],

    pub input: &'a [u8],
    pub num_columns: usize,
    pub num_rows: usize,
    pub col_id: usize,

    pub column_name: &'a str,
}

impl Deref for ParseContext<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.input
    }
}

impl<'a> ParseContext<'a> {
    pub fn fork(&self, input: &'a [u8]) -> ParseContext<'a> {
        ParseContext {
            initial: self.initial,
            input,
            num_columns: self.num_columns,
            num_rows: self.num_rows,
            col_id: self.col_id,
            column_name: self.column_name,
        }
    }
    pub fn with_column_name(self, column_name: &'a str) -> ParseContext<'a> {
        ParseContext {
            column_name,
            ..self
        }
    }

    pub fn with_num_rows(self, num_rows: usize) -> ParseContext<'a> {
        ParseContext { num_rows, ..self }
    }
}

pub fn parse_single(input: &[u8]) -> IResult<&[u8], ParsedBlock> {
    if input.is_empty() {
        return Ok((
            input,
            ParsedBlock {
                markers: Vec::new(),
                col_names: Vec::new(),
                num_rows: 0,
            },
        ));
    }

    let mut parse_context = ParseContext {
        initial: input,
        input,
        num_columns: 0,
        num_rows: 0,
        col_id: 0,
        column_name: "",
    };

    let (input, num_columns) = parse_varuint(input)?;
    let (mut input, num_rows) = parse_varuint(input)?;

    debug!("num_columns={} num_rows={}", num_columns, num_rows);

    parse_context.num_columns = num_columns;
    parse_context.num_rows = num_rows;

    let mut markers = Vec::with_capacity(num_columns);
    let mut col_names = Vec::with_capacity(num_columns);

    for index in 0..num_columns {
        debug!("Parsing column {} of {num_columns}", index + 1);
        parse_context.col_id = index;

        let column_name;
        (input, column_name) = parse_var_str(input)?;
        debug!("column name: {column_name}");
        parse_context.column_name = column_name;
        col_names.push(column_name);

        let column_type;
        (input, column_type) = parse_var_str(input)?;
        debug!("{column_name}: column type: {column_type}");

        // convert back to bytes, converting to string needed to ensure encoding
        // and fail earlier, can be removed later
        let (_, typ) = parse_type(column_type.as_bytes())?;
        debug!("column type parsed: {:?}", typ);

        let ctx = parse_context.fork(input);
        let header;
        (input, header) = typ.decode_header(ctx.clone())?;

        let marker;
        (input, marker) = typ.decode(ctx.fork(input), header)?;
        debug!("Decoded, remaining bytes: {}", input.len());

        markers.push(marker);
    }

    Ok((
        input,
        ParsedBlock {
            markers,
            col_names,
            num_rows,
        },
    ))
}

pub fn parse_many(mut input: &[u8]) -> Result<Vec<ParsedBlock>, crate::parse::Error> {
    let mut blocks = Vec::new();
    while !input.is_empty() {
        let block;
        (input, block) = parse_single(input)?;
        blocks.push(block);
    }

    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    use super::*;
    use crate::common::load;

    macro_rules! test_file {
        (
            $(
                $name:ident => $file:expr
            ),* $(,)?
        ) => {
            $(
                #[test]
                fn $name() -> TestResult {
                    let buf = load($file)?;
                    parse_many(&buf)?;
                    Ok(())
                }
            )*
        }
    }

    test_file! {
        a_lot_of_types => "./testdata/sample.native",
        array_lc_string => "./testdata/array_lc_string.native",
        array => "./testdata/array.native",
        tuple => "./testdata/tuple.native",
        variant => "./testdata/variant.native",
        dynamic => "./testdata/dynamic.native",
        nullable_string => "./testdata/nullable_string.native",
        json => "./testdata/json.native",
        array_nullable_int64 => "./testdata/array_nullable_int64.native",
        array_lc_nullable_string => "./testdata/array_lc_nullable_string.native",
        array_string => "./testdata/array_string.native",
        map_nullable_lc_string => "./testdata/map_nullable_lc_string.native",
        events => "./testdata/events.native",
        plain_strings => "./testdata/plain_strings.native",
        metric_activity => "./testdata/metric_activity.native",
        array_of_nested => "./testdata/array_of_nested.native",
        // json_arr => "./testdata/json_arr.native",
    }

    #[test]
    fn json_arr() -> TestResult {
        let buf = load("./testdata/json_arr.native")?;
        parse_many(&buf)?;
        Ok(())
    }
}
