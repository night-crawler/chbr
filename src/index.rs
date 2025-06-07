use crate::mark::Mark;
use crate::value::Value;

#[derive(Debug)]
pub enum IndexableColumn<'a> {
    Stateless(Mark<'a>),
    Stateful { marker: Mark<'a> },
}

impl<'a> From<Mark<'a>> for IndexableColumn<'a> {
    fn from(marker: Mark<'a>) -> Self {
        match marker {
            Mark::Bool(_)
            | Mark::Int8(_)
            | Mark::Int16(_)
            | Mark::Int32(_)
            | Mark::Int64(_)
            | Mark::Int128(_)
            | Mark::Int256(_)
            | Mark::UInt8(_)
            | Mark::UInt16(_)
            | Mark::UInt32(_)
            | Mark::UInt64(_)
            | Mark::UInt128(_)
            | Mark::UInt256(_)
            | Mark::Float32(_)
            | Mark::Float64(_)
            | Mark::BFloat16(_)
            | Mark::Uuid(_)
            | Mark::Decimal32(_, _)
            | Mark::Decimal64(_, _)
            | Mark::Decimal128(_, _)
            | Mark::Decimal256(_, _)
            | Mark::FixedString(_, _)
            | Mark::Ipv4(_)
            | Mark::Ipv6(_)
            | Mark::Date(_)
            | Mark::Date32(_)
            | Mark::DateTime(_, _)
            | Mark::DateTime64(_, _, _)
            | Mark::Enum8(_, _)
            | Mark::Enum16(_, _) => IndexableColumn::Stateless(marker),
            _ => IndexableColumn::Stateful { marker },
        }
    }
}

impl<'a> IndexableColumn<'a> {
    pub fn get(&self, index: usize) -> Option<Value<'a>> {
        match self {
            IndexableColumn::Stateless(m) => match m {
                Mark::Empty => None,
                Mark::Bool(values) => values.get(index).map(|&v| Value::Bool(v != 0)),
                Mark::Int8(bc) => bc.get(index).copied().map(Value::Int8),
                Mark::Int16(bv) => bv.get(index).map(|v| v.get()).map(Value::Int16),
                Mark::Int32(bv) => bv.get(index).map(|v| v.get()).map(Value::Int32),
                Mark::Int64(bv) => bv.get(index).map(|v| v.get()).map(Value::Int64),
                Mark::Int128(bv) => bv.get(index).map(|v| v.get()).map(Value::Int128),
                Mark::Int256(bv) => bv.get(index).copied().map(Value::Int256),
                Mark::UInt8(bv) => bv.get(index).copied().map(Value::UInt8),
                Mark::UInt16(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt16),
                Mark::UInt32(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt32),
                Mark::UInt64(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt64),
                Mark::UInt128(bv) => bv.get(index).map(|v| v.get()).map(Value::UInt128),
                Mark::UInt256(bv) => bv.get(index).copied().map(Value::UInt256),
                Mark::Float32(bv) => bv.get(index).map(|v| v.get()).map(Value::Float32),
                Mark::Float64(bv) => bv.get(index).map(|v| v.get()).map(Value::Float64),
                Mark::BFloat16(_) => {
                    todo!()
                }
                Mark::Decimal32(_, _) => todo!(),
                Mark::Decimal64(_, _) => todo!(),
                Mark::Decimal128(_, _) => todo!(),
                Mark::Decimal256(_, _) => todo!(),
                Mark::String(offsets, buf) => {
                    let offset = offsets.get(index).map(|&o| o as usize)?;
                    let next_offset = offsets
                        .get(index + 1)
                        .map(|&o| o as usize)
                        .unwrap_or(buf.len());
                    let slice = &buf[offset..next_offset];

                    Some(Value::String(unsafe {
                        std::str::from_utf8_unchecked(slice)
                    }))
                }
                Mark::FixedString(_, _) => todo!(),
                Mark::Uuid(_) => todo!(),
                Mark::Date(_) => todo!(),
                Mark::Date32(_) => todo!(),
                Mark::DateTime(_, _) => todo!(),
                Mark::DateTime64(_, _, _) => todo!(),
                Mark::Ipv4(_) => todo!(),
                Mark::Ipv6(_) => todo!(),
                Mark::Point(_) => todo!(),
                Mark::Ring(_) => todo!(),
                Mark::Polygon(_) => todo!(),
                Mark::MultiPolygon(_) => todo!(),
                Mark::LineString(_) => todo!(),
                Mark::MultiLineString(_) => todo!(),
                Mark::Enum8(_, _) => todo!(),
                Mark::Enum16(_, _) => todo!(),
                Mark::LowCardinality { .. } => todo!(),
                Mark::Array(offsets, marker) => {
                    println!("{:?}", offsets);
                    println!("{:?}", marker);

                    todo!()
                }
                Mark::VarTuple(_) => todo!(),
                Mark::FixTuple(_, _) => todo!(),
                Mark::Nullable(_, _) => todo!(),
                Mark::Map { .. } => todo!(),
                Mark::Variant { .. } => todo!(),
                Mark::Nested(_, _) => todo!(),
                Mark::Dynamic(_, _) => todo!(),
                Mark::Json { .. } => todo!(),
            },
            IndexableColumn::Stateful { .. } => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::init_logger;
    use crate::parse::block::parse_block;
    use pretty_assertions::assert_eq;
    use std::io::Read;
    use testresult::TestResult;
    #[test]
    fn index() -> TestResult {
        init_logger();
        let mut file = std::fs::File::open("./array.native")?;
        // random was a bad idea, it looks like parser broke
        // 0,[]
        // 128969003,[1]
        // 214500519,[1]
        // 301458964,[]
        // 475251162,[]
        // 1228122092,"[1, 2, 3, 4, 5]"
        // 1873422981,"[1, 2, 3, 4]"
        // 2172352370,"[1, 2, 3]"
        // 2181458171,"[1, 2]"
        // 2793473513,[]
        // 3697287021,"[1, 2, 3]"

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let (_, block) = parse_block(&buf)?;

        let index_marker = &block.cols[0];

        let indices = (0..block.num_rows)
            .filter_map(|i| index_marker.get(i))
            .map(|v| i64::try_from(v).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            indices,
            vec![
                0, 128969003, 214500519, 301458964, 475251162, 1228122092, 1873422981, 2172352370,
                2181458171, 2793473513, 3697287021
            ]
        );

        let arr_marker = &block.cols[1];
        println!("{:?}", arr_marker);

        arr_marker.get(0);

        Ok(())
    }
}
