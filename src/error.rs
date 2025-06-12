use std::fmt::Debug;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("UInt decode error")]
    VarUIntDecode(#[from] unsigned_varint::decode::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Overflow: {0}")]
    Overflow(String),

    #[error("Unexpected end of input")]
    UnexpectedEndOfInput,

    #[error("Length error: {0}")]
    Length(usize),

    #[error("Mismatched type: Internal type is {0}, but asked to get {1}")]
    MismatchedType(&'static str, &'static str),

    #[error("Utf8 decode error: {0}; bytes: {1:0x?}")]
    Utf8Decode(std::str::Utf8Error, Vec<u8>),

    #[error("Nom: {0}")]
    Nom(String),

    #[error("Conversion out of range: {0} for {1}, got {2}")]
    ValueOutOfRange(&'static str, &'static str, String),

    #[error("Index {0} out of bounds for {1}")]
    IndexOutOfBounds(usize, &'static str),
}

impl<T> From<nom::Err<T>> for Error
where
    T: Debug,
{
    fn from(value: nom::Err<T>) -> Self {
        Self::Nom(format!("{:?}", value))
    }
}
