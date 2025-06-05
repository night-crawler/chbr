use std::ops::Deref;
use crate::types::{Field};
use log::{debug};
use nom::{IResult};
use crate::parse::{parse_var_str, parse_varuint};
use crate::parse::typ::parse_type;

#[derive(Debug, Clone)]
pub struct ParseContext<'a> {
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



impl <'a> ParseContext<'a> {
    pub fn fork(&self, input: &'a [u8]) -> ParseContext<'a> {
        ParseContext {
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
        ParseContext {
            num_rows,
            ..self
        }
    }
}

pub fn parse_block(input: &[u8]) -> IResult<&[u8], Vec<Field>> {
    let mut parse_context = ParseContext {
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

    for index in 0..num_columns {
        debug!("Parsing column {index} of {num_columns}");
        parse_context.col_id = index;
        
        let column_name;
        (input, column_name) = parse_var_str(input)?;
        debug!("column name: {column_name}");
        parse_context.column_name = column_name;
        
        let column_type;
        (input, column_type) = parse_var_str(input)?;
        debug!("column type: {column_type}");
        
        // convert back to bytes, converting to string needed to ensure encoding
        // and fail earlier, can be removed later
        let (_, typ) = parse_type(column_type.as_bytes())?;
        debug!("column type parsed: {:?}", typ);

        let ctx = parse_context.fork(input);
        let marker;
        (input, marker) = typ.decode(ctx)?;
        
    }


    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use testresult::TestResult;
    use crate::common::init_logger;

    #[test]
    fn it_works() -> TestResult {
        init_logger();
        
        let mut file = std::fs::File::open("./sample.native")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        parse_block(&buf)?;

        Ok(())
    }
}