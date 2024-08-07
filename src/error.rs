use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DmapError {
    /// Represents invalid conditions when reading from input.
    #[error("{0}")]
    CorruptStream(&'static str),

    /// Unable to read the buffer
    #[error("{0}")]
    Io(#[from] std::io::Error),

    /// Represents an invalid key for a DMAP type.
    #[error("{0}")]
    InvalidKey(i8),

    #[error("{0}")]
    InvalidRecord(String),

    #[error("{0}")]
    InvalidScalar(String),

    #[error("{0}")]
    InvalidVector(String),
}

impl From<DmapError> for PyErr {
    fn from(value: DmapError) -> Self {
        let msg = value.to_string();
        match value {
            DmapError::CorruptStream(..) => PyIOError::new_err(msg),
            DmapError::Io(..) => PyIOError::new_err(msg),
            _ => PyValueError::new_err(msg),
        }
    }
}
