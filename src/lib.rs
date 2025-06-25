//! `dmap` is an I/O library for SuperDARN DMAP files.
//! This library has a Python API using pyo3 that supports
//! reading and writing whole files.
//!
//! For more information about DMAP files, see [RST](https://radar-software-toolkit-rst.readthedocs.io/en/latest/)
//! or [pyDARNio](https://pydarnio.readthedocs.io/en/latest/).

pub mod error;
pub mod formats;
pub mod types;
pub mod record;

use crate::error::DmapError;
use crate::formats::dmap::DmapRecord;
use crate::formats::fitacf::FitacfRecord;
use crate::formats::grid::GridRecord;
use crate::formats::iqdat::IqdatRecord;
use crate::formats::map::MapRecord;
use crate::formats::rawacf::RawacfRecord;
use crate::formats::snd::SndRecord;
use crate::record::Record;
use crate::types::DmapField;
use bzip2::read::BzEncoder;
use bzip2::Compression;
use indexmap::IndexMap;
use paste::paste;
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
fn bytes_to_file(bytes: Vec<u8>, outfile: &PathBuf) -> Result<(), std::io::Error> {
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

/// Writes a collection of `Record`s to `outfile`.
///
/// Prefer using the specific functions, e.g. `write_dmap`, `write_rawacf`, etc. for their
/// specific field checks.
pub fn write_records<'a>(mut recs: Vec<impl Record<'a>>, outfile: &PathBuf) -> Result<(), DmapError> {
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
    bytes_to_file(bytes, outfile)?;
    Ok(())
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
            errors.iter().map(|(i, _)| *i).collect(),
            errors[0].1.to_string(),
        ))?
    }
    bytes.par_extend(rec_bytes.into_par_iter().flatten());
    bytes_to_file(bytes, outfile)?;
    Ok(())
}

/// This macro generates two functions for writing to file. The first, `write_[type]`, takes in
/// records of type `[Type]Record`, while the second, `try_write_[type]`, takes in `Vec<IndexMap>`
/// and attempts to coerce into `[Type]Record` then write to file.
macro_rules! write_rust {
    ($type:ident) => {
        paste! { 
            /// Write $type:upper records to `outfile`.
            pub fn [< write_ $type >](recs: Vec<[< $type:camel Record >]>, outfile: &PathBuf) -> Result<(), DmapError> {
                write_records(recs, outfile)
            }

            /// Attempts to convert `recs` to `[< $type:camel Record >]` then append to `outfile`.
            pub fn [< try_write_ $type >](
                recs: Vec<IndexMap<String, DmapField>>,
                outfile: &PathBuf,
            ) -> Result<(), DmapError> {
                try_write_generic::<[< $type:camel Record >]>(recs, outfile)
            }
        }
    }
}

write_rust!(iqdat);
write_rust!(rawacf);
write_rust!(fitacf);
write_rust!(grid);
write_rust!(map);
write_rust!(snd);
write_rust!(dmap);

macro_rules! read_type {
    ($type:ident) => {
        paste! {
            /// Read in a $type:upper file
            pub fn [< read_ $type >](infile: PathBuf) -> Result<Vec<[< $type:camel Record >]>, DmapError> {
                [< $type:camel Record >]::read_file(&infile)
            }
        }
    }
}

read_type!(iqdat);
read_type!(rawacf);
read_type!(fitacf);
read_type!(grid);
read_type!(map);
read_type!(snd);
read_type!(dmap);

/// Reads the data from infile into `Vec<IndexMap>`.
///
/// Returns `Err` if any records are corrupted.
fn read_generic<T: for<'a> Record<'a> + Send>(
    infile: PathBuf,
) -> Result<Vec<IndexMap<String, DmapField>>, DmapError> {
    Ok(T::read_file(&infile)?
        .into_iter()
        .map(|rec| rec.inner())
        .collect())
}

/// Reads the data from infile into a tuple of `([IndexMap], int|None)`, where
/// all valid records are returned, plus optionally the byte of the first record
/// with a corruption within the file. Compatible with RST behaviour.
fn read_lax<T: for<'a> Record<'a> + Send>(
    infile: PathBuf,
) -> Result<(Vec<IndexMap<String, DmapField>>, Option<usize>), DmapError> {
    let result = T::read_file_lax(&infile)?;
    Ok((
        result.0.into_iter().map(|rec| rec.inner()).collect(),
        result.1,
    ))
}

/// Creates functions for reading DMAP files for the Python API. 
/// 
/// Generates two functions: `read_[type]` and `read_[type]_lax`, for strict and lax 
/// reading, respectively.
macro_rules! read_py {
    ($name:ident, $py_name:literal, $lax_name:literal) => { 
        paste! {
            /// Reads a $name:upper file, returning a tuple of
            /// (list of dictionaries containing the fields, byte where first corrupted record starts).
            #[pyfunction]
            #[pyo3(name = $lax_name)]
            #[pyo3(text_signature = "(infile: str, /)")]
            fn [< read_ $name _lax_py >](
                infile: PathBuf,
            ) -> PyResult<(Vec<IndexMap<String, DmapField>>, Option<usize>)> {
                read_lax::<[< $name:camel Record >]>(infile).map_err(PyErr::from)
            }

            /// Reads a $name:upper file, returning a list of dictionaries containing the fields.
            #[pyfunction]
            #[pyo3(name = $py_name)]
            #[pyo3(text_signature = "(infile: str, /)")]
            fn [< read_ $name _py >](infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
                read_generic::<[< $name:camel Record >]>(infile).map_err(PyErr::from)
            }
        }
    }
}

read_py!(iqdat, "read_iqdat", "read_iqdat_lax");
read_py!(rawacf, "read_rawacf", "read_rawacf_lax");
read_py!(fitacf, "read_fitacf", "read_fitacf_lax");
read_py!(grid, "read_grid", "read_grid_lax");
read_py!(map, "read_map", "read_map_lax");
read_py!(snd, "read_snd", "read_snd_lax");
read_py!(dmap, "read_dmap", "read_dmap_lax");

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

/// Generates functions exposed to the Python API for writing specific file types.
macro_rules! write_py {
    ($name:ident, $fn_name:literal) => {
        paste! {
            /// Checks that a list of dictionaries contains valid $name:upper records, then appends to outfile.
            #[pyfunction]
            #[pyo3(name = $fn_name)]
            #[pyo3(text_signature = "(recs: list[dict], outfile: str, /)")]
            fn [< write_ $name _py >](recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
                [< try_write_ $name >](recs, &outfile).map_err(PyErr::from)
            }
        }
    }
}

write_py!(iqdat, "write_iqdat");
write_py!(rawacf, "write_rawacf");
write_py!(fitacf, "write_fitacf");
write_py!(grid, "write_grid");
write_py!(map, "write_map");
write_py!(snd, "write_snd");

/// Functions for SuperDARN DMAP file format I/O.
#[pymodule]
fn dmap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Strict read functions
    m.add_function(wrap_pyfunction!(read_dmap_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_py, m)?)?;

    // Lax read functions
    m.add_function(wrap_pyfunction!(read_dmap_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_lax_py, m)?)?;

    // Write functions
    m.add_function(wrap_pyfunction!(write_dmap_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_iqdat_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_rawacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_fitacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_grid_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_map_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_snd_py, m)?)?;

    Ok(())
}
