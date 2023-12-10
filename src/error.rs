#[derive(Debug)]
pub enum DmapError {
    /// Represents an empty source.
    EmptySource,

    /// Represents a failure to read from input.
    ReadError { source: std::io::Error },

    /// Represents invalid conditions when reading from input.
    CorruptDmapError(&'static str),

    /// Represents a failure to intepret data from input.
    CastError { position: usize, kind: &'static str },

    /// Represents all other cases of `std::io::Error`.
    IOError(std::io::Error),

    /// Represents an attempt to extract the wrong type of data.
    ExtractionError,

    /// Represents an invalid key for a DMAP type.
    KeyError(i8),

    /// Represents a failure to read a DMAP record.
    RecordError(String),

    /// Represents an invalid scalar field.
    ScalarError(String),

    /// Represents an invalid vector field.
    VectorError(String),
}
impl std::error::Error for DmapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            DmapError::ReadError { ref source } => Some(source),
            _ => None,
        }
    }
}

impl std::fmt::Display for DmapError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            DmapError::EmptySource => {
                write!(f, "Source contains no data")
            }
            DmapError::ReadError { .. } => {
                write!(f, "Read error")
            }
            DmapError::CorruptDmapError(s) => {
                write!(f, "{s}")
            }
            DmapError::CastError {
                ref position,
                ref kind,
            } => {
                write!(f, "Unable to interpet value at {position:?} as {kind:?}")
            }
            DmapError::IOError(ref err) => err.fmt(f),
            DmapError::ExtractionError => {
                write!(f, "Extraction error")
            }
            DmapError::KeyError(ref key) => {
                write!(f, "Invalid key '{:?}'", key)
            }
            DmapError::RecordError(ref s) => {
                write!(f, "{s:?}")
            }
            DmapError::ScalarError(ref s) => {
                write!(f, "{s:?}")
            }
            DmapError::VectorError(ref s) => {
                write!(f, "{s:?}")
            }
        }
    }
}

impl From<std::io::Error> for DmapError {
    fn from(err: std::io::Error) -> Self {
        DmapError::IOError(err)
    }
}
