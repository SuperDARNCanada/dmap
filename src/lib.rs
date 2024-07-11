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
use itertools::{Either, Itertools};
use pyo3::prelude::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

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

#[pyfunction]
fn write_iqdat(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    for dict in fields.iter_mut() {
        let rec = IqdatRecord::try_from(dict)?;
        bytes.extend(rec.to_bytes()?);
    }
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

#[pyfunction]
fn write_rawacf(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    // for dict in fields.iter_mut() {
    //     let rec = RawacfRecord::try_from(dict)?;
    //     bytes.extend(rec.to_bytes()?);
    // }
    let (errors, rec_bytes): (Vec<(usize, DmapError)>, Vec<Vec<u8>>) = fields
        .par_iter_mut()
        .enumerate()
        .partition_map(|(i, rec)| match RawacfRecord::try_from(rec) {
            Err(e) => Either::Left((i, e)),
            Ok(x) => Either::Right(x.to_bytes()),
        });
    if errors.len() > 0 {
        Err(DmapError::RecordError(format!(
            "Corrupted records: {errors}"
        )))?
    }
    bytes.par_extend(rec_bytes.into_par_iter());
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

#[pyfunction]
fn write_fitacf(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    for dict in fields.iter_mut() {
        let rec = FitacfRecord::try_from(dict)?;
        bytes.extend(rec.to_bytes()?);
    }
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

#[pyfunction]
fn write_grid(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    for dict in fields.iter_mut() {
        let rec = GridRecord::try_from(dict)?;
        bytes.extend(rec.to_bytes()?);
    }
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

#[pyfunction]
fn write_map(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    for dict in fields.iter_mut() {
        let rec = MapRecord::try_from(dict)?;
        bytes.extend(rec.to_bytes()?);
    }
    let mut file = File::create(outfile)?;
    file.write_all(&bytes)?;
    Ok(())
}

#[pyfunction]
fn write_snd(mut fields: Vec<IndexMap<String, DmapField>>, outfile: PathBuf) -> PyResult<()> {
    let mut bytes: Vec<u8> = vec![];
    for dict in fields.iter_mut() {
        let rec = SndRecord::try_from(dict)?;
        bytes.extend(rec.to_bytes()?);
    }
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

// /// Writes DmapRecords to path as a Vec<u8>
// ///
// /// # Failures
// /// If file cannot be created at path or data cannot be written to file.
// pub fn to_file<P: AsRef<Path>, T: Record>(path: P, dmap_records: &Vec<T>) -> std::io::Result<()> {
//     let mut stream = vec![];
//     for rec in dmap_records {
//         stream.append(&mut rec.to_bytes());
//     }
//     let mut file = File::create(path)?;
//     file.write_all(&stream)?;
//     Ok(())
// }
