pub mod error;
pub mod formats;
pub mod types;

use crate::formats::dmap::Record;
use crate::formats::rawacf::RawacfRecord;
use crate::types::GenericDmap;
use indexmap::IndexMap;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

#[pyfunction]
fn read_rawacf(infile: PathBuf) -> PyResult<Vec<IndexMap<String, GenericDmap>>> {
    let file = File::open(infile)?;
    match RawacfRecord::read_records(file) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyValueError::new_err(format!("{e}"))),
    }
}

/// Functions for SuperDARN DMAP file format I/O.
#[pymodule]
fn dmap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_rawacf, m)?)?;
    Ok(())
}

/// Writes DmapRecords to path as a Vec<u8>
///
/// # Failures
/// If file cannot be created at path or data cannot be written to file.
pub fn to_file<P: AsRef<Path>, T: Record>(path: P, dmap_records: &Vec<T>) -> std::io::Result<()> {
    let mut stream = vec![];
    for rec in dmap_records {
        stream.append(&mut rec.to_dmap());
    }
    let mut file = File::create(path)?;
    file.write_all(&stream)?;
    Ok(())
}
