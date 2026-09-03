#[cfg(feature = "serde1")]
use std::borrow::Cow;
use std::{hint::cold_path, ops::Range};

use crate::{Error, TinyRange, mark::Mark, value::Value};

#[derive(Debug)]
pub struct Json<'a> {
    pub(crate) paths: Box<[&'a str]>,
    /// One column per path, same order as `paths`.
    pub(crate) columns: Box<[Mark<'a>]>,
    rows: usize,
    #[cfg(feature = "serde1")]
    nodes: Box<[JsonPathNode<'a>]>,
}

#[cfg(feature = "serde1")]
#[derive(Debug)]
struct JsonPathNode<'a> {
    key: Cow<'a, str>,
    leaf: Option<usize>,
    first_child: Option<usize>,
    next_sibling: Option<usize>,
}

impl<'a> Json<'a> {
    pub(crate) fn new(
        paths: Vec<&'a str>,
        columns: Vec<Mark<'a>>,
        rows: usize,
    ) -> crate::Result<Self> {
        if paths.len() != columns.len() {
            return Err(Error::CorruptedData(format!(
                "JSON has {} paths but {} columns",
                paths.len(),
                columns.len()
            )));
        }

        #[cfg(feature = "serde1")]
        let nodes = {
            let mut tree = PathTree::new();
            for (path_index, path) in paths.iter().enumerate() {
                tree.insert_path(path_index, path)?;
            }
            tree.nodes.into_boxed_slice()
        };

        Ok(Self {
            paths: paths.into_boxed_slice(),
            columns: columns.into_boxed_slice(),
            rows,
            #[cfg(feature = "serde1")]
            nodes,
        })
    }

    #[cfg(feature = "serde1")]
    pub(crate) const fn root(&self) -> usize {
        PathTree::ROOT
    }

    #[cfg(feature = "serde1")]
    pub(crate) fn node_key(&'a self, node: usize) -> &'a str {
        self.nodes[node].key.as_ref()
    }

    #[cfg(feature = "serde1")]
    pub(crate) fn node_leaf(&self, node: usize) -> Option<usize> {
        self.nodes[node].leaf
    }

    #[cfg(feature = "serde1")]
    pub(crate) fn first_child(&self, node: usize) -> Option<usize> {
        self.nodes[node].first_child
    }

    #[cfg(feature = "serde1")]
    pub(crate) fn next_sibling(&self, node: usize) -> Option<usize> {
        self.nodes[node].next_sibling
    }

    pub(crate) const fn contains_row(&self, row: usize) -> bool {
        row < self.rows
    }

    pub(crate) const fn get(&'a self, row: usize) -> Option<Value<'a>> {
        if row < self.rows {
            Some(Value::Json {
                mark: self,
                index: row,
            })
        } else {
            None
        }
    }

    pub(crate) fn slice(&'a self, range: Range<usize>) -> crate::Result<Value<'a>> {
        if range.start > range.end || range.end > self.rows {
            cold_path();
            return Err(Error::RangeOutOfBounds(range, "Json"));
        }
        Ok(Value::JsonSlice {
            mark: self,
            range: TinyRange::try_from(range)?,
        })
    }

    pub(crate) fn value(
        &'a self,
        path_index: usize,
        row: usize,
    ) -> crate::Result<Option<Value<'a>>> {
        let Some(column) = self.columns.get(path_index) else {
            return Err(Error::CorruptedData(format!(
                "JSON path index {path_index} has no column"
            )));
        };
        column.get(row)
    }
}

#[cfg(feature = "serde1")]
struct PathTree<'a> {
    nodes: Vec<JsonPathNode<'a>>,
}

#[cfg(feature = "serde1")]
impl<'a> PathTree<'a> {
    const ROOT: usize = 0;

    fn new() -> Self {
        Self {
            nodes: vec![JsonPathNode {
                key: Cow::Borrowed(""),
                leaf: None,
                first_child: None,
                next_sibling: None,
            }],
        }
    }

    fn insert_path(&mut self, path_index: usize, path: &'a str) -> crate::Result<()> {
        let mut parent = Self::ROOT;
        for raw_key in path.split('.') {
            let key = decode_key(raw_key);
            parent = match self.find_child(parent, key.as_ref()) {
                Some(child) => child,
                None => self.push_child(parent, key),
            };
        }

        if self.nodes[parent].leaf.replace(path_index).is_some() {
            return Err(Error::CorruptedData(format!(
                "duplicate JSON path {path:?}"
            )));
        }
        Ok(())
    }

    fn find_child(&self, parent: usize, key: &str) -> Option<usize> {
        let mut child = self.nodes[parent].first_child;
        while let Some(index) = child {
            let node = &self.nodes[index];
            if node.key == key {
                return Some(index);
            }
            child = node.next_sibling;
        }
        None
    }

    fn push_child(&mut self, parent: usize, key: Cow<'a, str>) -> usize {
        let index = self.nodes.len();
        self.nodes.push(JsonPathNode {
            key,
            leaf: None,
            first_child: None,
            next_sibling: None,
        });

        let Some(mut sibling) = self.nodes[parent].first_child else {
            self.nodes[parent].first_child = Some(index);
            return index;
        };
        while let Some(next) = self.nodes[sibling].next_sibling {
            sibling = next;
        }
        self.nodes[sibling].next_sibling = Some(index);
        index
    }
}

#[cfg(feature = "serde1")]
fn decode_key(key: &str) -> Cow<'_, str> {
    let bytes = key.as_bytes();
    let mut index = 0;
    while index + 2 < bytes.len()
        && !(bytes[index] == b'%'
            && bytes[index + 1] == b'2'
            && matches!(bytes[index + 2], b'E' | b'e'))
    {
        index += 1;
    }
    if index + 2 >= bytes.len() {
        return Cow::Borrowed(key);
    }

    let mut decoded = String::with_capacity(key.len());
    let mut copied = 0;
    while index + 2 < bytes.len() {
        if bytes[index] == b'%'
            && bytes[index + 1] == b'2'
            && matches!(bytes[index + 2], b'E' | b'e')
        {
            decoded.push_str(&key[copied..index]);
            decoded.push('.');
            index += 3;
            copied = index;
        } else {
            index += 1;
        }
    }
    decoded.push_str(&key[copied..]);
    Cow::Owned(decoded)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "serde1")]
    use super::decode_key;
    #[cfg(feature = "serde1")]
    use std::borrow::Cow;

    #[cfg(feature = "serde1")]
    #[test]
    fn decodes_only_escaped_dots() {
        assert!(matches!(decode_key("plain"), Cow::Borrowed("plain")));
        assert_eq!(decode_key("a%2Eb"), "a.b");
        assert_eq!(decode_key("a%2eb%20c"), "a.b%20c");
    }
}
