use crate::mark::Mark;
use crate::types::{OffsetIndexPair as _, Offsets};
use std::marker::PhantomData;
use std::ops::Range;

pub struct ColBool<'a>(&'a Mark<'a>);
pub struct ColStr<'a>(&'a Mark<'a>);
pub struct ColUsize<'a>(&'a Mark<'a>);


pub trait Read<'a> {
    type Item;
    fn read(&'a self, idx: usize) -> Self::Item;
}

impl<'a> Read<'a> for Mark<'a> {
    type Item = bool;

    #[inline(always)]
    fn read(&'a self, idx: usize) -> Self::Item {
        self.get_bool(idx).unwrap().unwrap()
    }
}

pub struct ColArray<'a, Inner: Read<'a>> {
    offsets: &'a Offsets<'a>,
    values: Inner,
}

impl<'a, Inner: Read<'a> + 'a> Read<'a> for ColArray<'a, Inner> {
    type Item = ArrayIter<'a, Inner>;

    #[inline(always)]
    fn read(&'a self, idx: usize) -> Self::Item {
        let (s, e) = self.offsets.offset_indices(idx).unwrap().unwrap();
        ArrayIter {
            inner: &self.values,
            range: s..e,
        }
    }
}

pub struct ArrayIter<'a, Inner: Read<'a>> {
    inner: &'a Inner,
    range: Range<usize>,
}

impl<'a, Inner: Read<'a>> Iterator for ArrayIter<'a, Inner> {
    type Item = Inner::Item;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let i = self.range.next()?;
        Some(self.inner.read(i))
    }
}


impl<'a> Read<'a> for ColBool<'a> {
    type Item = bool;
    #[inline(always)]
    fn read(&'a self, idx: usize) -> Self::Item {
        self.0.get_bool(idx).unwrap().unwrap()
    }
}

impl<'a> Read<'a> for ColStr<'a> {
    type Item = &'a str;
    #[inline(always)]
    fn read(&'a self, idx: usize) -> Self::Item {
        self.0.get_str(idx).unwrap().unwrap()
    }
}

impl<'a> Read<'a> for ColUsize<'a> {
    type Item = usize;
    #[inline(always)]
    fn read(&'a self, idx: usize) -> Self::Item {
        match self.0 {
            Mark::UInt8(v) => usize::from(v[idx]),
            Mark::UInt16(v) => v[idx].get() as usize,
            Mark::UInt32(v) => v[idx].get() as usize,
            Mark::UInt64(v) => usize::try_from(v[idx].get()).unwrap(),
            _ => unreachable!("unsupported index type for LowCardinality"),
        }
    }
}

pub struct ColMap<'a, K: Read<'a>, V: Read<'a>> {
    offsets: &'a Offsets<'a>,
    keys: K,
    values: V,
}

impl<'a, K: Read<'a> + 'a, V: Read<'a> + 'a> Read<'a> for ColMap<'a, K, V> {
    type Item = MapIter<'a, K, V>;
    #[inline(always)]
    fn read(&'a self, idx: usize) -> Self::Item {
        let (s, e) = self.offsets.offset_indices(idx).unwrap().unwrap();
        MapIter {
            keys: &self.keys,
            values: &self.values,
            range: s..e,
        }
    }
}

pub struct MapIter<'a, K: Read<'a>, V: Read<'a>> {
    keys: &'a K,
    values: &'a V,
    range: Range<usize>,
}

impl<'a, K: Read<'a>, V: Read<'a>> Iterator for MapIter<'a, K, V> {
    type Item = (K::Item, V::Item);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let i = self.range.next()?;
        Some((self.keys.read(i), self.values.read(i)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.range.end - self.range.start;
        (n, Some(n))
    }
}

pub struct ColLowCardinality<'a, Idx: Read<'a, Item = usize>, Val: Read<'a>> {
    indices: Idx,
    keys: Val,
    _marker: PhantomData<&'a ()>,
}

impl<'a, Idx, Val> Read<'a> for ColLowCardinality<'a, Idx, Val>
where
    Idx: Read<'a, Item = usize>,
    Val: Read<'a>,
{
    type Item = Val::Item;

    #[inline(always)]
    fn read(&'a self, idx: usize) -> Self::Item {
        let k = self.indices.read(idx);
        self.keys.read(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::load;
    use crate::parse::block::parse_block;
    use std::collections::HashMap;
    use testresult::TestResult;

    #[test]
    fn array_map_sample_typed() -> TestResult {
        type ArrMap<'a> = ColArray<'a, ColMap<'a, ColStr<'a>, ColStr<'a>>>;

        let buf = load("./test_data/array_map_sample.native")?;
        let (_, block) = parse_block(&buf)?;

        let m = &block.cols[1];
        let Mark::Array(map_mark) = m else {
            panic!("meh")
        };

        let Mark::Map(mark_map) = map_mark.values.as_ref() else {
            panic!("meh");
        };

        let reader: ArrMap = ColArray {
            offsets: &map_mark.offsets,
            values: ColMap {
                offsets: &mark_map.offsets,
                keys: ColStr(&mark_map.keys),
                values: ColStr(&mark_map.values),
            },
        };

        let expected: [Vec<HashMap<&str, &str>>; 6] = [
            vec![
                HashMap::from([("a", "apple"), ("b", "banana")]),
                HashMap::from([("c", "cherry")]),
            ],
            vec![
                HashMap::from([("d", "date")]),
                HashMap::from([("e", "elderberry"), ("f", "fig")]),
            ],
            vec![HashMap::from([("g", "grape"), ("h", "honeydew")])],
            vec![HashMap::from([("i", "kiwi")])],
            vec![],
            vec![HashMap::from([("j", "lemon"), ("k", "mango")])],
        ];

        for row in 0..block.num_rows {
            let outer = reader.read(row); 
            let mut actual_row = Vec::new();

            for mp in outer {
                let mut h = HashMap::new();
                for (k, v) in mp {
                    h.insert(k, v);
                }
                actual_row.push(h);
            }

            assert_eq!(actual_row, expected[row], "mismatch at top-level row {row}");
        }

        Ok(())
    }
}
