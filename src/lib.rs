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
use crate::formats::dmap::{GenericRecord, Record};
use crate::formats::fitacf::FitacfRecord;
use crate::formats::grid::GridRecord;
use crate::formats::iqdat::IqdatRecord;
use crate::formats::map::MapRecord;
use crate::formats::rawacf::RawacfRecord;
use crate::formats::snd::SndRecord;
use crate::types::DmapField;
use bzip2::read::BzEncoder;
use bzip2::Compression;
use indexmap::IndexMap;
use pyo3::prelude::*;
use rayon::iter::Either;
use rayon::prelude::*;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

/// Write bytes to file.
///
/// Ordinarily, this function opens the file in `append` mode. If the extension of `outfile` is
/// `.bz2`, the bytes will be compressed using bzip2 before being written, and the file is instead
/// opened in `create_new` mode, meaning it will fail if a file already exists at the given path.
fn write_to_file(bytes: Vec<u8>, outfile: &PathBuf) -> Result<(), std::io::Error> {
    let mut out_bytes: Vec<u8> = vec![];
    let mut file: File = OpenOptions::new().append(true).create(true).open(outfile)?;
    match outfile.extension() {
        Some(ext) if ext == OsStr::new("bz2") => {
            let mut compressor = BzEncoder::new(bytes.as_slice(), Compression::best());
            compressor.read_to_end(&mut out_bytes)?;
        }
        _ => {
            out_bytes = bytes;
        }
    }
    file.write_all(&out_bytes)
}

/// Writes a collection of `impl Record`s to `outfile`
fn write_generic<'a>(mut recs: Vec<impl Record<'a>>, outfile: &PathBuf) -> Result<(), DmapError> {
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match rec.to_bytes() {
                Err(e) => Either::Left((i, e)),
                Ok(y) => Either::Right(y),
            });
    if !errors.is_empty() {
        Err(DmapError::InvalidRecord(format!(
            "Corrupted records: {errors:?}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Write generic DMAP to `outfile`
pub fn write_dmap(recs: Vec<GenericRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    write_generic(recs, outfile)
}

/// Write IQDAT records to `outfile`.
pub fn write_iqdat(recs: Vec<IqdatRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    write_generic(recs, outfile)
}

/// Write RAWACF records to `outfile`.
pub fn write_rawacf(recs: Vec<RawacfRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    write_generic(recs, outfile)
}

/// Write FITACF records to `outfile`.
pub fn write_fitacf(recs: Vec<FitacfRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    write_generic(recs, outfile)
}

/// Write GRID records to `outfile`.
pub fn write_grid(recs: Vec<GridRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    write_generic(recs, outfile)
}

/// Write MAP records to `outfile`.
pub fn write_map(recs: Vec<MapRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    write_generic(recs, outfile)
}

/// Write SND records to `outfile`.
pub fn write_snd(recs: Vec<SndRecord>, outfile: &PathBuf) -> Result<(), DmapError> {
    write_generic(recs, outfile)
}

/// Attempts to convert `recs` to `T` then append to `outfile`.
fn try_write_generic<T: for<'a> Record<'a>>(
    mut recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError>
where
    for<'a> <T as TryFrom<&'a mut IndexMap<String, DmapField>>>::Error: Send + Debug,
{
    let mut bytes: Vec<u8> = vec![];
    let (errors, rec_bytes): (Vec<_>, Vec<_>) =
        recs.par_iter_mut()
            .enumerate()
            .partition_map(|(i, rec)| match T::try_from(rec) {
                Err(e) => Either::Left((i, e)),
                Ok(x) => match x.to_bytes() {
                    Err(e) => Either::Left((i, e)),
                    Ok(y) => Either::Right(y),
                },
            });
    if !errors.is_empty() {
        Err(DmapError::BadRecords(
            errors.iter().map(|(i, _)| *i).collect(), errors[0].1.to_string()
        ))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    write_to_file(bytes, outfile)?;
    Ok(())
}

/// Attempts to convert `recs` to `GenericRecord` then append to `outfile`.
pub fn try_write_dmap(
    recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    try_write_generic::<GenericRecord>(recs, outfile)
}

/// Attempts to convert `recs` to `IqdatRecord` then append to `outfile`.
pub fn try_write_iqdat(
    recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    try_write_generic::<IqdatRecord>(recs, outfile)
}

/// Attempts to convert `recs` to `RawacfRecord` then append to `outfile`.
pub fn try_write_rawacf(
    recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    try_write_generic::<RawacfRecord>(recs, outfile)
}

/// Attempts to convert `recs` to `FitacfRecord` then append to `outfile`.
pub fn try_write_fitacf(
    recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    try_write_generic::<FitacfRecord>(recs, outfile)
}

/// Attempts to convert `recs` to `GridRecord` then append to `outfile`.
pub fn try_write_grid(
    recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    try_write_generic::<GridRecord>(recs, outfile)
}

/// Attempts to convert `recs` to `MapRecord` then append to `outfile`.
pub fn try_write_map(
    recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    try_write_generic::<MapRecord>(recs, outfile)
}

/// Attempts to convert `recs` to `SndRecord` then append to `outfile`.
pub fn try_write_snd(
    recs: Vec<IndexMap<String, DmapField>>,
    outfile: &PathBuf,
) -> Result<(), DmapError> {
    try_write_generic::<SndRecord>(recs, outfile)
}

/// Read in a DMAP file
pub fn read_dmap(infile: PathBuf) -> Result<Vec<GenericRecord>, DmapError> {
    GenericRecord::read_file(&infile)
}

/// Read in an IQDAT file
pub fn read_iqdat(infile: PathBuf) -> Result<Vec<IqdatRecord>, DmapError> {
    IqdatRecord::read_file(&infile)
}

/// Read in a RAWACF file
pub fn read_rawacf(infile: PathBuf) -> Result<Vec<RawacfRecord>, DmapError> {
    RawacfRecord::read_file(&infile)
}

/// Read in a FITACF file
pub fn read_fitacf(infile: PathBuf) -> Result<Vec<FitacfRecord>, DmapError> {
    FitacfRecord::read_file(&infile)
}

/// Read in a GRID file
pub fn read_grid(infile: PathBuf) -> Result<Vec<GridRecord>, DmapError> {
    GridRecord::read_file(&infile)
}

/// Read in a MAP file
pub fn read_map(infile: PathBuf) -> Result<Vec<MapRecord>, DmapError> {
    MapRecord::read_file(&infile)
}

/// Read in an SND file
pub fn read_snd(infile: PathBuf) -> Result<Vec<SndRecord>, DmapError> {
    SndRecord::read_file(&infile)
}

/// Reads the data from infile into a collection of `IndexMap`s
fn read_generic<T: for<'a> Record<'a> + Send>(
    infile: PathBuf,
) -> Result<Vec<IndexMap<String, DmapField>>, DmapError> {
    match T::read_file(&infile) {
        Ok(recs) => {
            let new_recs = recs.into_iter().map(|rec| rec.inner()).collect();
            Ok(new_recs)
        }
        Err(e) => Err(e),
    }
}

/// Reads a generic DMAP file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_dmap")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_dmap_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    read_generic::<GenericRecord>(infile).map_err(PyErr::from)
}

/// Reads an IQDAT file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_iqdat")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_iqdat_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    read_generic::<IqdatRecord>(infile).map_err(PyErr::from)
}

/// Reads a RAWACF file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_rawacf")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_rawacf_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    read_generic::<RawacfRecord>(infile).map_err(PyErr::from)
}

/// Reads a FITACF file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_fitacf")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_fitacf_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    read_generic::<FitacfRecord>(infile).map_err(PyErr::from)
}

/// Reads a GRID file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_grid")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_grid_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    read_generic::<GridRecord>(infile).map_err(PyErr::from)
}

/// Reads a MAP file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_map")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_map_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    read_generic::<MapRecord>(infile).map_err(PyErr::from)
}

/// Reads an SND file, returning a list of dictionaries containing the fields.
#[pyfunction]
#[pyo3(name = "read_snd")]
#[pyo3(text_signature = "(infile: str, /)")]
fn read_snd_py(infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
    read_generic::<SndRecord>(infile).map_err(PyErr::from)
}

/// Checks that a list of dictionaries contains DMAP records, then appends to outfile.
///
/// **NOTE:** No type checking is done, so the fields may not be written as the expected
/// DMAP type, e.g. `stid` might be written one byte instead of two as this function
/// does not know that typically `stid` is two bytes.
#[pyfunction]
#[pyo3(name = "write_dmap")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_dmap_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_dmap(recs, &outfile).map_err(PyErr::from)
}

/// Checks that a list of dictionaries contains valid IQDAT records, then appends to outfile.
#[pyfunction]
#[pyo3(name = "write_iqdat")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_iqdat_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_iqdat(recs, &outfile).map_err(PyErr::from)
}

/// Checks that a list of dictionaries contains valid RAWACF records, then appends to outfile.
#[pyfunction]
#[pyo3(name = "write_rawacf")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_rawacf_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_rawacf(recs, &outfile).map_err(PyErr::from)
}

/// Checks that a list of dictionaries contains valid FITACF records, then appends to outfile.
#[pyfunction]
#[pyo3(name = "write_fitacf")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_fitacf_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_fitacf(recs, &outfile).map_err(PyErr::from)
}

/// Checks that a list of dictionaries contains valid GRID records, then appends to outfile.
#[pyfunction]
#[pyo3(name = "write_grid")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_grid_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_grid(recs, &outfile).map_err(PyErr::from)
}

/// Checks that a list of dictionaries contains valid MAP records, then appends to outfile.
#[pyfunction]
#[pyo3(name = "write_map")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_map_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_map(recs, &outfile).map_err(PyErr::from)
}

/// Checks that a list of dictionaries contains valid SND records, then appends to outfile.
#[pyfunction]
#[pyo3(name = "write_snd")]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
fn write_snd_py(recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    try_write_snd(recs, &outfile).map_err(PyErr::from)
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
