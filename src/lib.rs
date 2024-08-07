//! # dmap
//!
//! `dmap` is an I/O library for SuperDARN DMAP files.
//! This library has a Python API using pyo3 that supports
//! reading and writing whole files.
//!
//! For more information about DMAP files, see [RST](https://radar-software-toolkit-rst.readthedocs.io/en/latest/)
//! or [pyDARNio](https://pydarnio.readthedocs.io/en/latest/).

pub mod error;
pub mod formats;
pub mod types;

use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::formats::fitacf::FitacfRecord;
use crate::formats::grid::GridRecord;
use crate::formats::iqdat::IqdatRecord;
use crate::formats::map::MapRecord;
use crate::formats::rawacf::RawacfRecord;
use crate::formats::snd::SndRecord;
use crate::types::DmapField;
use indexmap::IndexMap;
use itertools::Either;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

/// Reads an IQDAT file, returning a list of dictionaries containing the fields.
#[pyfunction]
fn read_iqdat(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    let file = File::open(infile)?;
    match IqdatRecord::read_records(file) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a RAWACF file, returning a list of dictionaries containing the fields.
#[pyfunction]
fn read_rawacf(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    let file = File::open(infile)?;
    match RawacfRecord::read_records(file) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a FITACF file, returning a list of dictionaries containing the fields.
#[pyfunction]
fn read_fitacf(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    let file = File::open(infile)?;
    match FitacfRecord::read_records(file) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a SND file, returning a list of dictionaries containing the fields.
#[pyfunction]
fn read_snd(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    let file = File::open(infile)?;
    match SndRecord::read_records(file) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a GRID file, returning a list of dictionaries containing the fields.
#[pyfunction]
fn read_grid(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    let file = File::open(infile)?;
    match GridRecord::read_records(file) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a MAP file, returning a list of dictionaries containing the fields.
#[pyfunction]
fn read_map(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    let file = File::open(infile)?;
    match MapRecord::read_records(file) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Checks that a list of dictionaries contains valid IQDAT records, then writes to outfile.
#[pyfunction]
fn write_iqdat(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        fields.par_iter_mut().enumerate().partition_map(|(i, rec)| {
            match IqdatRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            }
        });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

/// Checks that a list of dictionaries contains valid RAWACF records, then writes to outfile.
#[pyfunction]
fn write_rawacf(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        fields.par_iter_mut().enumerate().partition_map(|(i, rec)| {
            match RawacfRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            }
        });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

/// Checks that a list of dictionaries contains valid FITACF records, then writes to outfile.
#[pyfunction]
fn write_fitacf(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        fields.par_iter_mut().enumerate().partition_map(|(i, rec)| {
            match FitacfRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            }
        });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

/// Checks that a list of dictionaries contains valid GRID records, then writes to outfile.
#[pyfunction]
fn write_grid(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        fields.par_iter_mut().enumerate().partition_map(|(i, rec)| {
            match GridRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            }
        });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

/// Checks that a list of dictionaries contains valid MAP records, then writes to outfile.
#[pyfunction]
fn write_map(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        fields.par_iter_mut().enumerate().partition_map(|(i, rec)| {
            match MapRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            }
        });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

/// Checks that a list of dictionaries contains valid SND records, then writes to outfile.
#[pyfunction]
fn write_snd(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        fields.par_iter_mut().enumerate().partition_map(|(i, rec)| {
            match SndRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            }
        });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

/// Functions for SuperDARN DMAP file format I/O.
#[pymodule]
fn dmap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_iqdat, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid, m)?)?;
    m.add_function(wrap_pyfunction!(read_map, m)?)?;
    m.add_function(wrap_pyfunction!(write_iqdat, m)?)?;
    m.add_function(wrap_pyfunction!(write_rawacf, m)?)?;
    m.add_function(wrap_pyfunction!(write_fitacf, m)?)?;
    m.add_function(wrap_pyfunction!(write_grid, m)?)?;
    m.add_function(wrap_pyfunction!(write_map, m)?)?;
    m.add_function(wrap_pyfunction!(write_snd, m)?)?;

    Ok(())
}
