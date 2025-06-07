#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("UInt decode error")]
    UIntDecode(#[from] unsigned_varint::decode::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Unexpected end of input")]
    UnexpectedEndOfInput,

    #[error("Length error: {0}")]
    LengthError(usize),

    #[error("Mismatched type: Internal type is {0}, but asked to get {1}")]
    MismatchedType(&'static str, &'static str),
}
