use crate::compression;
use crate::formats::dmap::DmapRecord;
use crate::formats::fitacf::FitacfRecord;
use crate::formats::grid::GridRecord;
use crate::formats::iqdat::IqdatRecord;
use crate::formats::map::MapRecord;
use crate::formats::rawacf::RawacfRecord;
use crate::formats::snd::SndRecord;
use crate::record::Record;
use crate::types::DmapField;
use crate::{
    try_write_dmap, try_write_fitacf, try_write_grid, try_write_iqdat, try_write_map,
    try_write_rawacf, try_write_snd,
};
use indexmap::IndexMap;
use paste::paste;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::{Bound, PyAny, PyResult, Python};
use std::path::PathBuf;

/// Creates functions for reading DMAP files for the Python API.
///
/// Generates functions for:
///
/// * `read_[name]` - reads a file, raising an error on a corrupted file
/// * `read_[name]_lax` - reads a file, returning the records and the byte where corruption starts, if corrupted.
/// * `read_[name]_bytes` - reads from bytes, similar to `read_[name]`
/// * `read_[name]_bytes_lax` - reads from bytes, similar to `read_[name]_lax`
/// * `read_[name]_by_indices` - reads specific records from a file by index
/// * `read_[name]_by_indices_lax` - reads specific records from a file by index, returning the byte offset where corruption begins, if applicable
/// * `read_[name]_by_indices_bytes` - reads specific records from a byte buffer by index
/// * `read_[name]_by_indices_bytes_lax` - reads specific records from a byte buffer by index, returning the byte offset where corruption begins, if applicable
/// * `read_[name]_metadata` - reads only metadata fields from records in a file
/// * `read_[name]_metadata_by_indices` - reads only metadata fields from specific records in a file by index
///
/// where `[name]` is one of the supported DMAP record types.
macro_rules! read_py {
    (
        $name:ident,
        $py_name:literal,
        $lax_name:literal,
        $bytes_name:literal,
        $lax_bytes_name:literal,
        $by_indices_name:literal,
        $by_indices_name_lax:literal,
        $bytes_by_indices_name:literal,
        $bytes_by_indices_name_lax:literal,
        $metadata_name:literal,
        $metadata_by_indices_name:literal
    ) => {
        paste! {
            #[doc = "Reads a `" $name:upper "` file, returning a list of dictionaries containing the fields." ]
            #[pyfunction]
            #[pyo3(name = $py_name)]
            #[pyo3(text_signature = "(infile: str, /)")]
            fn [< read_ $name _py >](infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
                Ok([< $name:camel Record >]::read_file(&infile)
                    .map_err(PyErr::from)?
                    .into_iter()
                    .map(|rec| rec.inner())
                    .collect()
                )
            }

            #[doc = "Reads a `" $name:upper "` file, returning a tuple of" ]
            #[doc = "(list of dictionaries containing the fields, byte where first corrupted record starts). "]
            #[pyfunction]
            #[pyo3(name = $lax_name)]
            #[pyo3(text_signature = "(infile: str, /)")]
            fn [< read_ $name _lax_py >](
                infile: PathBuf,
            ) -> PyResult<(Vec<IndexMap<String, DmapField>>, Option<usize>)> {
                let result = [< $name:camel Record >]::read_file_lax(&infile).map_err(PyErr::from)?;
                Ok((
                    result.0.into_iter().map(|rec| rec.inner()).collect(),
                    result.1,
                ))
            }

            #[doc = "Read in `" $name:upper "` records from bytes, returning `List[Dict]` of the records." ]
            #[pyfunction]
            #[pyo3(name = $bytes_name)]
            #[pyo3(text_signature = "(buf: bytes, /)")]
            fn [< read_ $name _bytes_py >](bytes: &[u8]) -> PyResult<Vec<IndexMap<String, DmapField>>> {
                Ok([< $name:camel Record >]::read_records(bytes)?
                    .into_iter()
                    .map(|rec| rec.inner())
                    .collect()
                )
            }

            #[doc = "Reads a `" $name:upper "` file, returning a tuple of" ]
            #[doc = "(list of dictionaries containing the fields, byte where first corrupted record starts). "]
            #[pyfunction]
            #[pyo3(name = $lax_bytes_name)]
            #[pyo3(text_signature = "(buf: bytes, /)")]
            fn [< read_ $name _bytes_lax_py >](
                bytes: &[u8],
            ) -> PyResult<(Vec<IndexMap<String, DmapField>>, Option<usize>)> {
                let result = [< $name:camel Record >]::read_records_lax(bytes).map_err(PyErr::from)?;
                Ok((
                    result.0.into_iter().map(|rec| rec.inner()).collect(),
                    result.1,
                ))
            }

            #[doc = "Reads a `" $name:upper "` file, returning the `nth` record(s)." ]
            #[pyfunction]
            #[pyo3(name = $by_indices_name)]
            #[pyo3(text_signature = "(infile: str, indices: tuple[int], /)")]
            fn [< read_ $name _by_indices_py >](infile: PathBuf, indices: Vec<i32>) -> PyResult<Vec<IndexMap<String, DmapField>>> {
                Ok([< $name:camel Record >]::read_file_by_indices(&infile, &indices)
                    .map_err(PyErr::from)?
                    .into_iter()
                    .map(|rec| rec.inner())
                    .collect()
                )
            }

            #[doc = "Reads a `" $name:upper "` file, returning the `nth` record(s), and the byte index where corruption starts, if applicable" ]
            #[pyfunction]
            #[pyo3(name = $by_indices_name_lax)]
            #[pyo3(text_signature = "(infile: str, indices: tuple[int], /)")]
            fn [< read_ $name _by_indices_lax_py >](
                infile: PathBuf, indices: Vec<i32>
            ) -> PyResult<(Vec<IndexMap<String, DmapField>>, Option<usize>)> {
                let result = [< $name:camel Record >]::read_file_by_indices_lax(infile, &indices).map_err(PyErr::from)?;
                Ok((
                    result.0.into_iter().map(|rec| rec.inner()).collect(),
                    result.1,
                ))
            }

            #[doc = "Reads a `" $name:upper "` buffer, returning the `nth` record(s)." ]
            #[pyfunction]
            #[pyo3(name = $bytes_by_indices_name)]
            #[pyo3(text_signature = "(buf: bytes, indices: tuple[int], /)")]
            fn [< read_ $name _bytes_by_indices_py >](buf: &[u8], indices: Vec<i32>) -> PyResult<Vec<IndexMap<String, DmapField>>> {
                Ok([< $name:camel Record >]::read_nth_records(buf, &indices)
                    .map_err(PyErr::from)?
                    .into_iter()
                    .map(|rec| rec.inner())
                    .collect()
                )
            }

            #[doc = "Reads a `" $name:upper "` buffer, returning the `nth` record(s) and the byte index where record corruption starts, if applicable." ]
            #[pyfunction]
            #[pyo3(name = $bytes_by_indices_name_lax)]
            #[pyo3(text_signature = "(buf: bytes, indices: tuple[int], /)")]
            fn [< read_ $name _bytes_by_indices_lax_py >](
                buf: &[u8], indices: Vec<i32>
            ) -> PyResult<(Vec<IndexMap<String, DmapField>>, Option<usize>)> {
                let result = [< $name:camel Record >]::read_nth_records_lax(buf, &indices).map_err(PyErr::from)?;
                Ok((
                    result.0.into_iter().map(|rec| rec.inner()).collect(),
                    result.1,
                ))
            }

            #[doc = "Reads a `" $name:upper "` file, returning a list of dictionaries containing the only the metadata fields." ]
            #[pyfunction]
            #[pyo3(name = $metadata_name)]
            #[pyo3(text_signature = "(infile: str, /)")]
            fn [< read_ $name _metadata_py >](infile: PathBuf) -> PyResult<Vec<IndexMap<String, DmapField>>> {
                Ok([< $name:camel Record >]::read_file_metadata(&infile)
                    .map_err(PyErr::from)?
                )
            }

            #[doc = "Reads a `" $name:upper "` file, returning the `nth` records' metadata fields." ]
            #[pyfunction]
            #[pyo3(name = $metadata_by_indices_name)]
            #[pyo3(text_signature = "(infile: str, indices: tuple[int], /)")]
            fn [< read_ $name _metadata_by_indices_py >](infile: PathBuf, indices: Vec<i32>) -> PyResult<Vec<IndexMap<String, DmapField>>> {
                Ok([< $name:camel Record >]::read_file_metadata_by_indices(&infile, &indices)
                    .map_err(PyErr::from)?
                )
            }
        }
    }
}

read_py!(
    iqdat,
    "read_iqdat",
    "read_iqdat_lax",
    "read_iqdat_bytes",
    "read_iqdat_bytes_lax",
    "read_iqdat_by_indices",
    "read_iqdat_by_indices_lax",
    "read_iqdat_by_indices_bytes",
    "read_iqdat_by_indices_bytes_lax",
    "read_iqdat_metadata",
    "read_iqdat_metadata_by_indices"
);
read_py!(
    rawacf,
    "read_rawacf",
    "read_rawacf_lax",
    "read_rawacf_bytes",
    "read_rawacf_bytes_lax",
    "read_rawacf_by_indices",
    "read_rawacf_by_indices_lax",
    "read_rawacf_by_indices_bytes",
    "read_rawacf_by_indices_bytes_lax",
    "read_rawacf_metadata",
    "read_rawacf_metadata_by_indices"
);
read_py!(
    fitacf,
    "read_fitacf",
    "read_fitacf_lax",
    "read_fitacf_bytes",
    "read_fitacf_bytes_lax",
    "read_fitacf_by_indices",
    "read_fitacf_by_indices_lax",
    "read_fitacf_by_indices_bytes",
    "read_fitacf_by_indices_bytes_lax",
    "read_fitacf_metadata",
    "read_fitacf_metadata_by_indices"
);
read_py!(
    grid,
    "read_grid",
    "read_grid_lax",
    "read_grid_bytes",
    "read_grid_bytes_lax",
    "read_grid_by_indices",
    "read_grid_by_indices_lax",
    "read_grid_by_indices_bytes",
    "read_grid_by_indices_bytes_lax",
    "read_grid_metadata",
    "read_grid_metadata_by_indices"
);
read_py!(
    map,
    "read_map",
    "read_map_lax",
    "read_map_bytes",
    "read_map_bytes_lax",
    "read_map_by_indices",
    "read_map_by_indices_lax",
    "read_map_by_indices_bytes",
    "read_map_by_indices_bytes_lax",
    "read_map_metadata",
    "read_map_metadata_by_indices"
);
read_py!(
    snd,
    "read_snd",
    "read_snd_lax",
    "read_snd_bytes",
    "read_snd_bytes_lax",
    "read_snd_by_indices",
    "read_snd_by_indices_lax",
    "read_snd_by_indices_bytes",
    "read_snd_by_indices_bytes_lax",
    "read_snd_metadata",
    "read_snd_metadata_by_indices"
);
read_py!(
    dmap,
    "read_dmap",
    "read_dmap_lax",
    "read_dmap_bytes",
    "read_dmap_bytes_lax",
    "read_dmap_by_indices",
    "read_dmap_by_indices_lax",
    "read_dmap_by_indices_bytes",
    "read_dmap_by_indices_bytes_lax",
    "read_dmap_metadata",
    "read_dmap_metadata_by_indices"
);

/// Checks that a list of dictionaries contains DMAP records, then appends to outfile.
///
/// **NOTE:** No type checking is done, so the fields may not be written as the expected
/// DMAP type, e.g. `stid` might be written one byte instead of two as this function
/// does not know that typically `stid` is two bytes.
#[pyfunction]
#[pyo3(name = "write_dmap")]
#[pyo3(signature = (recs, outfile, /, bz2))]
#[pyo3(text_signature = "(recs: list[dict], outfile: str, /, bz2: bool = False)")]
fn write_dmap_py(
    recs: Vec<IndexMap<String, DmapField>>,
    outfile: PathBuf,
    bz2: bool,
) -> PyResult<()> {
    try_write_dmap(recs, &outfile, bz2).map_err(PyErr::from)
}

/// Checks that a list of dictionaries contains valid DMAP records, then converts them to bytes.
/// Returns `list[bytes]`, one entry per record.
///
/// **NOTE:** No type checking is done, so the fields may not be written as the expected
/// DMAP type, e.g. `stid` might be written one byte instead of two as this function
/// does not know that typically `stid` is two bytes.
#[pyfunction]
#[pyo3(name = "write_dmap_bytes")]
#[pyo3(signature = (recs, /, bz2))]
#[pyo3(text_signature = "(recs: list[dict], /, bz2: bool = False)")]
fn write_dmap_bytes_py(
    py: Python,
    recs: Vec<IndexMap<String, DmapField>>,
    bz2: bool,
) -> PyResult<Py<PyAny>> {
    let mut bytes = DmapRecord::try_into_bytes(recs).map_err(PyErr::from)?;
    if bz2 {
        bytes = compression::compress_bz2(&bytes).map_err(PyErr::from)?;
    }
    Ok(PyBytes::new(py, &bytes).into())
}

/// Generates functions exposed to the Python API for writing specific file types.
macro_rules! write_py {
    ($name:ident, $fn_name:literal, $bytes_name:literal) => {
        paste! {
            #[doc = "Checks that a list of dictionaries contains valid `" $name:upper "` records, then appends to outfile." ]
            #[pyfunction]
            #[pyo3(name = $fn_name)]
            #[pyo3(signature = (recs, outfile, /, bz2))]
            #[pyo3(text_signature = "(recs: list[dict], outfile: str, /, bz2: bool = False)")]
            fn [< write_ $name _py >](recs: Vec<IndexMap<String, DmapField>>, outfile: PathBuf, bz2: bool) -> PyResult<()> {
                [< try_write_ $name >](recs, &outfile, bz2).map_err(PyErr::from)
            }

            #[doc = "Checks that a list of dictionaries contains valid `" $name:upper "` records, then converts them to bytes." ]
            #[doc = "Returns `list[bytes]`, one entry per record." ]
            #[pyfunction]
            #[pyo3(name = $bytes_name)]
            #[pyo3(signature = (recs, /, bz2))]
            #[pyo3(text_signature = "(recs: list[dict], /, bz2: bool = False)")]
            fn [< write_ $name _bytes_py >](py: Python, recs: Vec<IndexMap<String, DmapField>>, bz2: bool) -> PyResult<Py<PyAny>> {
                let mut bytes = [< $name:camel Record >]::try_into_bytes(recs).map_err(PyErr::from)?;
                if bz2 {
                    bytes = compression::compress_bz2(&bytes).map_err(PyErr::from)?;
                }
                Ok(PyBytes::new(py, &bytes).into())
            }
        }
    }
}

// **NOTE** dmap type not included in this list, since it has a more descriptive docstring.
write_py!(iqdat, "write_iqdat", "write_iqdat_bytes");
write_py!(rawacf, "write_rawacf", "write_rawacf_bytes");
write_py!(fitacf, "write_fitacf", "write_fitacf_bytes");
write_py!(grid, "write_grid", "write_grid_bytes");
write_py!(map, "write_map", "write_map_bytes");
write_py!(snd, "write_snd", "write_snd_bytes");

/// Functions for SuperDARN DMAP file format I/O.
#[pymodule]
fn dmap_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
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

    // Read functions from byte buffer
    m.add_function(wrap_pyfunction!(read_dmap_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_bytes_py, m)?)?;

    // Read select records from byte buffer
    m.add_function(wrap_pyfunction!(read_dmap_bytes_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_bytes_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_bytes_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_bytes_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_bytes_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_bytes_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_bytes_by_indices_py, m)?)?;

    // Read select records from byte buffer, without raising error
    m.add_function(wrap_pyfunction!(read_dmap_bytes_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_bytes_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_bytes_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_bytes_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_bytes_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_bytes_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_bytes_by_indices_lax_py, m)?)?;

    // Lax read functions from byte buffer
    m.add_function(wrap_pyfunction!(read_dmap_bytes_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_bytes_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_bytes_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_bytes_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_bytes_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_bytes_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_bytes_lax_py, m)?)?;

    // Write functions
    m.add_function(wrap_pyfunction!(write_dmap_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_iqdat_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_rawacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_fitacf_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_grid_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_map_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_snd_py, m)?)?;

    // Convert records to bytes
    m.add_function(wrap_pyfunction!(write_dmap_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_iqdat_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_rawacf_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_fitacf_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_snd_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_grid_bytes_py, m)?)?;
    m.add_function(wrap_pyfunction!(write_map_bytes_py, m)?)?;

    // Read records by index
    m.add_function(wrap_pyfunction!(read_dmap_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_by_indices_py, m)?)?;

    // Read records by index, but report corrupt records
    m.add_function(wrap_pyfunction!(read_dmap_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_by_indices_lax_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_by_indices_lax_py, m)?)?;

    // Read only the metadata from files
    m.add_function(wrap_pyfunction!(read_dmap_metadata_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_metadata_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_metadata_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_metadata_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_metadata_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_metadata_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_metadata_py, m)?)?;

    // Read only the metadata of select records from files
    m.add_function(wrap_pyfunction!(read_dmap_metadata_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_iqdat_metadata_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_rawacf_metadata_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_fitacf_metadata_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_snd_metadata_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_grid_metadata_by_indices_py, m)?)?;
    m.add_function(wrap_pyfunction!(read_map_metadata_by_indices_py, m)?)?;

    Ok(())
}
