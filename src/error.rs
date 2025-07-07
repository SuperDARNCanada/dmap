//! Error type for `dmap`.
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::PyErr;
use thiserror::Error;

/// Enum of the possible error variants that may be encountered.
#[derive(Error, Debug)]
pub enum DmapError {
    /// Represents invalid conditions when reading from input.
    #[error("{0}")]
    CorruptStream(&'static str),

    /// Unable to read from a buffer.
    #[error("{0}")]
    Io(#[from] std::io::Error),

    /// Invalid key for a DMAP type. Valid keys are defined [here](https://github.com/SuperDARN/rst/blob/main/codebase/general/src.lib/dmap.1.25/include/dmap.h)
    #[error("{0}")]
    InvalidKey(i8),

    /// An issue with parsing a record. This is a broad error that is returned by higher-level
    /// functions (ones that are reading/writing files, as opposed to single-record operations).
    #[error("{0}")]
    InvalidRecord(String),

    /// Error interpreting data as a valid DMAP scalar.
    #[error("{0}")]
    InvalidScalar(String),

    /// Error interpreting data as a valid DMAP vector.
    #[error("{0}")]
    InvalidVector(String),

    /// Errors when reading in multiple records
    #[error("First error: {1}\nRecords with errors: {0:?}")]
    BadRecords(Vec<usize>, String),
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
