use crate::ParsedBlock;
use crate::parse::IResult;
use crate::parse::typ::parse_type;
use crate::parse::{parse_var_str, parse_varuint};
use log::debug;
use std::ops::Deref;

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

pub fn parse_block(input: &[u8]) -> IResult<&[u8], ParsedBlock> {
    if input.is_empty() {
        return Ok((
            input,
            ParsedBlock {
                cols: Vec::new(),
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

    let mut columns = Vec::with_capacity(num_columns);
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
        debug!("column type: {column_type}");

        // convert back to bytes, converting to string needed to ensure encoding
        // and fail earlier, can be removed later
        let (_, typ) = parse_type(column_type.as_bytes())?;
        debug!("column type parsed: {:?}", typ);

        let ctx = parse_context.fork(input);
        (input, ()) = typ.decode_prefix(ctx.clone())?;

        let marker;
        (input, marker) = typ.decode(ctx.fork(input))?;
        debug!("Decoded, remaining bytes: {}", input.len());

        columns.push(marker);
    }

    Ok((
        input,
        ParsedBlock {
            cols: columns,
            col_names,
            num_rows,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::load;
    use testresult::TestResult;

    #[test]
    fn a_lot_of_types() -> TestResult {
        let buf = load("./test_data/sample.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn array_lc_string() -> TestResult {
        let buf = load("./test_data/array_lc_string.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn array() -> TestResult {
        let buf = load("./test_data/array.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn tuple() -> TestResult {
        let buf = load("./test_data/tuple.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn variant() -> TestResult {
        let buf = load("./test_data/variant.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn dynamic() -> TestResult {
        let buf = load("./test_data/dynamic.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn nullable_string() -> TestResult {
        let buf = load("./test_data/nullable_string.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn json() -> TestResult {
        let buf = load("./test_data/json.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn array_nullable_int64() -> TestResult {
        let buf = load("./test_data/array_nullable_int64.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn array_lc_nullable_string() -> TestResult {
        let buf = load("./test_data/array_lc_nullable_string.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn array_string() -> TestResult {
        let buf = load("./test_data/array_string.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn map_nullable_lc_string() -> TestResult {
        let buf = load("./test_data/map_nullable_lc_string.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn events() -> TestResult {
        let buf = load("./test_data/events.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn plain_strings() -> TestResult {
        let buf = load("./test_data/plain_strings.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn metric_activity() -> TestResult {
        let buf = load("./test_data/metric_activity.native")?;
        parse_block(&buf)?;
        Ok(())
    }

    #[test]
    fn array_of_nested() -> TestResult {
        let buf = load("./test_data/array_of_nested.native")?;
        parse_block(&buf)?;
        Ok(())
    }
}
