//! `dmap` is an I/O library for SuperDARN DMAP files.
//! This library has a Python API using pyo3 that supports
//! reading and writing whole files.
//!
//! For more information about DMAP files, see [RST](https://radar-software-toolkit-rst.readthedocs.io/en/latest/)
//! or [pyDARNio](https://pydarnio.readthedocs.io/en/latest/).

pub mod error;
pub mod formats;
pub mod types;

use std::ffi::OsStr;
use crate::error::DmapError;
use crate::formats::dmap::{GenericRecord, Record};
use crate::formats::fitacf::FitacfRecord;
use crate::formats::grid::GridRecord;
use crate::formats::iqdat::IqdatRecord;
use crate::formats::map::MapRecord;
use crate::formats::rawacf::RawacfRecord;
use crate::formats::snd::SndRecord;
use crate::types::DmapField;
use indexmap::IndexMap;
use pyo3::prelude::*;
use rayon::iter::Either;
use rayon::prelude::*;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use bzip2::Compression;
use bzip2::read::BzEncoder;

/// Write bytes to file.
///
/// If the extension of `outfile` is `.bz2`, the bytes will be compressed using
/// bzip2 before being written.
fn write_to_file(bytes: Vec<u8>, outfile: &PathBuf) -> Result<(), std::io::Error> {
    let mut file = File::create(outfile)?;
    let mut out_bytes: Vec<u8> = vec![];
    match outfile.extension() {
        Some(ext) if ext == OsStr::new("bz2") => {
            let mut compressor = BzEncoder::new(bytes.as_slice(), Compression::best());
            compressor.read_to_end(&mut out_bytes)?;
        }
        _ => { out_bytes = bytes }
    }
    file.write_all(&out_bytes)
}

/// Write generic DMAP to `outfile`
pub fn write_dmap(mut recs: Vec<GenericRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match rec.to_bytes() {
                Err(e) => Either::Left((i, e)),
                Ok(y) => Either::Right(y),
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Attempts to convert `recs` to `GenericRecord` then write to `outfile`.
pub fn try_write_dmap(
    mut recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut().enumerate().partition_map(|(i, rec)| {
            match GenericRecord::try_from(rec) {
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
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Write IQDAT records to `outfile`.
pub fn write_iqdat(mut recs: Vec<IqdatRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match rec.to_bytes() {
                Err(e) => Either::Left((i, e)),
                Ok(y) => Either::Right(y),
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Attempts to convert `recs` to `IqdatRecord` then write to `outfile`.
pub fn try_write_iqdat(
    mut recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut().enumerate().partition_map(|(i, rec)| {
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
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Write RAWACF records to `outfile`.
pub fn write_rawacf(mut recs: Vec<RawacfRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match rec.to_bytes() {
                Err(e) => Either::Left((i, e)),
                Ok(y) => Either::Right(y),
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Attempts to convert `recs` to `RawacfRecord` then write to `outfile`.
pub fn try_write_rawacf(
    mut recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut().enumerate().partition_map(|(i, rec)| {
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
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Write FITACF records to `outfile`.
pub fn write_fitacf(mut recs: Vec<FitacfRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match rec.to_bytes() {
                Err(e) => Either::Left((i, e)),
                Ok(y) => Either::Right(y),
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Attempts to convert `recs` to `FitacfRecord` then write to `outfile`.
pub fn try_write_fitacf(
    mut recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut().enumerate().partition_map(|(i, rec)| {
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
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Write GRID records to `outfile`.
pub fn write_grid(mut recs: Vec<GridRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match rec.to_bytes() {
                Err(e) => Either::Left((i, e)),
                Ok(y) => Either::Right(y),
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Attempts to convert `recs` to `GridRecord` then write to `outfile`.
pub fn try_write_grid(
    mut recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match GridRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Write MAP records to `outfile`.
pub fn write_map(mut recs: Vec<MapRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match rec.to_bytes() {
                Err(e) => Either::Left((i, e)),
                Ok(y) => Either::Right(y),
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Attempts to convert `recs` to `MapRecord` then write to `outfile`.
pub fn try_write_map(
    mut recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match MapRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Write SND records to `outfile`.
pub fn write_snd(mut recs: Vec<SndRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match rec.to_bytes() {
                Err(e) => Either::Left((i, e)),
                Ok(y) => Either::Right(y),
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Attempts to convert `recs` to `SndRecord` then write to `outfile`.
pub fn try_write_snd(
    mut recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match SndRecord::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            });
    if errors.len() > 0 {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Reads a generic DMAP file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_dmap")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_dmap_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    match GenericRecord::read_file(&infile) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads an IQDAT file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_iqdat")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_iqdat_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    match IqdatRecord::read_file(&infile) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a RAWACF file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_rawacf")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_rawacf_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    match RawacfRecord::read_file(&infile) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a FITACF file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_fitacf")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_fitacf_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    match FitacfRecord::read_file(&infile) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a GRID file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_grid")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_grid_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    match GridRecord::read_file(&infile) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a MAP file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_map")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_map_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    match MapRecord::read_file(&infile) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Reads a SND file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_snd")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_snd_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    match SndRecord::read_file(&infile) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.data).collect();
            Ok(new_recs)
        }
        Err(e) => Err(PyErr::from(e)),
    }
}

/// Checks that a list of dictionaries contains DMAP records, then writes to outfile.
/// **NOTE:** No type checking is done, so the fields may not be written as the expected
/// DMAP type, e.g. `stid` might be written as n `i8` instead of a `i16` as this function
/// does not know that typically `stid` is an `i16`.
#[pyfunction]
#[pyo3(name = "write_dmap")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_dmap_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_dmap(recs, &outfile).map_err(|e| PyErr::from(e))
}

/// Checks that a list of dictionaries contains valid IQDAT records, then writes to outfile.
#[pyfunction]
#[pyo3(name = "write_iqdat")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_iqdat_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_iqdat(recs, &outfile).map_err(|e| PyErr::from(e))
}

/// Checks that a list of dictionaries contains valid RAWACF records, then writes to outfile.
#[pyfunction]
#[pyo3(name = "write_rawacf")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_rawacf_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_rawacf(recs, &outfile).map_err(|e| PyErr::from(e))
}

/// Checks that a list of dictionaries contains valid FITACF records, then writes to outfile.
#[pyfunction]
#[pyo3(name = "write_fitacf")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_fitacf_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_fitacf(recs, &outfile).map_err(|e| PyErr::from(e))
}

/// Checks that a list of dictionaries contains valid GRID records, then writes to outfile.
#[pyfunction]
#[pyo3(name = "write_grid")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_grid_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_grid(recs, &outfile).map_err(|e| PyErr::from(e))
}

/// Checks that a list of dictionaries contains valid MAP records, then writes to outfile.
#[pyfunction]
#[pyo3(name = "write_map")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_map_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_map(recs, &outfile).map_err(|e| PyErr::from(e))
}

/// Checks that a list of dictionaries contains valid SND records, then writes to outfile.
#[pyfunction]
#[pyo3(name = "write_snd")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_snd_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_snd(recs, &outfile).map_err(|e| PyErr::from(e))
}

/// Functions for SuperDARN DMAP file format I/O.
#[pymodule]
fn dmap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_dmap_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_dmap_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_iqdat_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_rawacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_fitacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_grid_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_map_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_snd_py, m)?)?;

    Ok(())
}
