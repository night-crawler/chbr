use crate::mark::Mark;
use crate::parse::parse_var_str;
use crate::types::OffsetIndexPair as _;
use crate::value::Value;
use std::ops::Range;

#[derive(Debug)]
pub enum IndexableColumn<'a> {
    Stateless(Mark<'a>),
    Stateful { marker: Mark<'a> },
}

impl<'a> From<Mark<'a>> for IndexableColumn<'a> {
    fn from(marker: Mark<'a>) -> Self {
        if marker.size().is_some() {
            return IndexableColumn::Stateless(marker);
        }
        IndexableColumn::Stateful { marker }
    }
}

impl<'a> Mark<'a> {
    fn get(&'a self, index: usize) -> Option<Value<'a>> {
        match self {
            Mark::Empty => None,
            Mark::Bool(bytes) => bytes.get(index).map(|&v| Value::Bool(v != 0)),
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
                let start = if index == 0 {
                    0
                } else {
                    offsets.get(index.saturating_sub(1)).copied()?
                };

                let end = offsets.get(index).copied().unwrap_or(buf.len());
                let slice = &buf[start..end];

                let (_, s) = parse_var_str(slice).unwrap();

                Some(Value::String(s))
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
            Mark::LowCardinality {
                indices,
                // https://github.com/ClickHouse/clickhouse-go/blob/main/lib/column/lowcardinality.go#L191
                global_dictionary: _unused,
                additional_keys,
            } => {
                let value_index: usize = indices.get(index)?.try_into().unwrap();
                let Some(keys) = additional_keys else {
                    return None;
                };

                Some(keys.get(value_index)?)
            }
            Mark::Array(_, _) => todo!(),

            Mark::Tuple(_) => todo!(),
            Mark::Nullable(_, _) => todo!(),
            Mark::Map { .. } => todo!(),
            Mark::Variant { .. } => todo!(),
            Mark::Nested(_, _) => todo!(),
            Mark::Dynamic(_, _) => todo!(),
            Mark::Json { .. } => todo!(),
        }
    }

    fn slice(&'a self, idx: Range<usize>) -> Value<'a> {
        match self {
            Mark::Empty => {
                if !idx.is_empty() {
                    panic!("Index out of bounds for empty marker");
                }
                Value::Empty
            }
            Mark::Bool(_) => todo!(),
            Mark::Int8(bv) => Value::Int8Slice(&bv[idx]),
            Mark::Int16(bv) => Value::Int16Slice(&bv[idx]),
            Mark::Int32(bv) => Value::Int32Slice(&bv[idx]),
            Mark::Int64(bv) => Value::Int64Slice(&bv[idx]),
            Mark::Int128(bv) => Value::Int128Slice(&bv[idx]),
            Mark::Int256(bv) => Value::Int256Slice(&bv[idx]),
            Mark::UInt8(bv) => Value::UInt8Slice(&bv[idx]),
            Mark::UInt16(bv) => Value::UInt16Slice(&bv[idx]),
            Mark::UInt32(bv) => Value::UInt32Slice(&bv[idx]),
            Mark::UInt64(bv) => Value::UInt64Slice(&bv[idx]),
            Mark::UInt128(bv) => Value::UInt128Slice(&bv[idx]),
            Mark::UInt256(bv) => Value::UInt256Slice(&bv[idx]),
            Mark::Float32(bv) => Value::Float32Slice(&bv[idx]),
            Mark::Float64(bv) => Value::Float64Slice(&bv[idx]),
            Mark::BFloat16(_) => todo!(),
            Mark::Decimal32(_, _) => todo!(),
            Mark::Decimal64(_, _) => todo!(),
            Mark::Decimal128(_, _) => todo!(),
            Mark::Decimal256(_, _) => todo!(),
            Mark::String(offsets, data) => {
                let count = idx.len();
                let Range { start, .. } = idx;
                let start = start.saturating_sub(1);

                let data_start = if start == 0 {
                    data
                } else {
                    &data[offsets[start]..]
                };

                Value::StringSlice(count, data_start)
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
            Mark::Array(_, _) => todo!(),
            Mark::Tuple(_) => todo!(),
            Mark::Nullable(_, _) => todo!(),
            Mark::Map { .. } => todo!(),
            Mark::Variant { .. } => todo!(),
            Mark::Nested(_, _) => todo!(),
            Mark::Dynamic(_, _) => todo!(),
            Mark::Json { .. } => todo!(),
        }
    }
}

impl<'a> IndexableColumn<'a> {
    pub fn get(&'a self, index: usize) -> Option<Value<'a>> {
        match self {
            IndexableColumn::Stateless(m) => m.get(index),
            IndexableColumn::Stateful { marker } => match marker {
                Mark::Array(offsets, marker) => {
                    let (start, end) = offsets.offset_indices(index).unwrap()?;
                    Some(marker.slice(start..end))
                }

                Mark::String(_, _) => marker.get(index),
                Mark::LowCardinality { .. } => marker.get(index),
                _ => todo!(),
            },
        }
    }

    pub fn slice(&'a self, idx: Range<usize>) -> Value<'a> {
        match self {
            IndexableColumn::Stateless(marker) => match marker {
                Mark::Empty => {
                    if !idx.is_empty() {
                        panic!("Index out of bounds for empty marker");
                    }
                    Value::Empty
                }
                Mark::Bool(_) => todo!(),
                Mark::Int8(bv) => Value::Int8Slice(&bv[idx]),
                Mark::Int16(bv) => Value::Int16Slice(&bv[idx]),
                Mark::Int32(bv) => Value::Int32Slice(&bv[idx]),
                Mark::Int64(bv) => Value::Int64Slice(&bv[idx]),
                Mark::Int128(bv) => Value::Int128Slice(&bv[idx]),
                Mark::Int256(bv) => Value::Int256Slice(&bv[idx]),
                Mark::UInt8(bv) => Value::UInt8Slice(&bv[idx]),
                Mark::UInt16(bv) => Value::UInt16Slice(&bv[idx]),
                Mark::UInt32(bv) => Value::UInt32Slice(&bv[idx]),
                Mark::UInt64(bv) => Value::UInt64Slice(&bv[idx]),
                Mark::UInt128(bv) => Value::UInt128Slice(&bv[idx]),
                Mark::UInt256(bv) => Value::UInt256Slice(&bv[idx]),
                Mark::Float32(bv) => Value::Float32Slice(&bv[idx]),
                Mark::Float64(bv) => Value::Float64Slice(&bv[idx]),
                Mark::BFloat16(_) => todo!(),
                Mark::Decimal32(_, _) => todo!(),
                Mark::Decimal64(_, _) => todo!(),
                Mark::Decimal128(_, _) => todo!(),
                Mark::Decimal256(_, _) => todo!(),
                Mark::String(_, _) => todo!(),
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
                Mark::Array(_, _) => todo!(),
                Mark::Tuple(_) => todo!(),
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
    use crate::common::{init_logger, load};
    use crate::parse::block::parse_block;
    use crate::value::StringSliceIterator;
    use pretty_assertions::assert_eq;
    use std::io::Read as _;
    use testresult::TestResult;
    use zerocopy::little_endian::I64;

    #[test]
    fn int_array() -> TestResult {
        init_logger();
        let mut file = std::fs::File::open("./test_data/array.native")?;
        // random() for id was a bad idea, it looks like parser is broken
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

        let expected_ids = [
            0, 128969003, 214500519, 301458964, 475251162, 1228122092, 1873422981, 2172352370,
            2181458171, 2793473513, 3697287021,
        ];

        assert_eq!(indices, expected_ids);

        let expected_arrays = [
            vec![],
            vec![1],
            vec![1],
            vec![],
            vec![],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 3, 4],
            vec![1, 2, 3],
            vec![1, 2],
            vec![],
            vec![1, 2, 3],
        ];

        let arr_marker = &block.cols[1];

        let mut arrays = Vec::new();
        for index in 0..block.num_rows {
            let v: &[I64] = arr_marker.get(index).unwrap().try_into()?;
            arrays.push(v);
        }

        assert_eq!(arrays, expected_arrays);

        Ok(())
    }

    #[test]
    fn plain_strings() -> TestResult {
        init_logger();
        let mut file = std::fs::File::open("./test_data/plain_strings.native")?;
        // 0,hello
        // 1,world
        // 2,clickhouse
        // 3,test
        // 4,example
        // 5,data

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let (_, block) = parse_block(&buf)?;

        let expected_strings = ["hello", "world", "clickhouse", "test", "example", "data"];

        let strings_marker = &block.cols[1];
        for (i, expected) in expected_strings.iter().enumerate() {
            let value: &str = strings_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn plain_strings_array() -> TestResult {
        init_logger();
        let mut file = std::fs::File::open("./test_data/plain_strings_array.native")?;

        // 0,"['apple', 'banana', 'cherry']"
        // 1,"['date', 'elderberry']"
        // 2,"['fig', 'grape', 'honeydew']"
        // 3,['kiwi']
        // 4,[]
        // 5,"['lemon', 'mango']"

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let (_, block) = parse_block(&buf)?;

        let expected_arrays = [
            vec!["apple", "banana", "cherry"],
            vec!["date", "elderberry"],
            vec!["fig", "grape", "honeydew"],
            vec!["kiwi"],
            vec![],
            vec!["lemon", "mango"],
        ];

        let strings_marker = &block.cols[1];

        for (i, expected) in expected_arrays.iter().enumerate() {
            let it: StringSliceIterator = strings_marker.get(i).unwrap().try_into()?;
            let actual = it.collect::<Vec<_>>();

            assert_eq!(actual, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }

    #[test]
    fn lc_string() -> TestResult {
        let buf = load("./test_data/plain_lc_string.native")?;
        let (_, block) = parse_block(&buf)?;

        // 0,apple
        // 1,banana
        // 2,cherry
        // 3,date
        // 4,elderberry
        // 5,fig

        let expected_strings = ["apple", "banana", "cherry", "date", "elderberry", "fig"];

        let strings_marker = &block.cols[1];
        for (i, expected) in expected_strings.iter().enumerate() {
            let value: &str = strings_marker.get(i).unwrap().try_into()?;
            assert_eq!(value, *expected, "Mismatch at index {i}");
        }

        Ok(())
    }
}
