pub mod error;
pub mod formats;
pub mod types;

use crate::formats::{DmapRecord, IqdatRecord, RawacfRecord};
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::fs::File;
use std::path::{Path, PathBuf};

#[pyfunction]
fn read_rawacf(infile: PathBuf) -> PyResult<PyList> {
    let file = File::open(infile)?;
    let contents = RawacfRecord::read_records(file)?;

    Ok(vec![].collect())
}

/// Functions for SuperDARN DMAP file format I/O.
#[pymodule]
fn dmap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_rawacf, m)?)?;
    Ok(())
}
