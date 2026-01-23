//! Utility functions for file operations.

use crate::compression::compress_bz2;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// Write bytes to file.
///
/// Ordinarily, this function opens the file in `append` mode. If the extension of `outfile` is
/// `.bz2` or `bz2` is `true`, the bytes will be compressed using bzip2 before being written.
///
/// # Errors
/// If opening the file in append mode is not possible (permissions, path doesn't exist, etc.). See [`File::open`].
///
/// If an error is encountered when writing the bytes to the filesystem. See [`Write::write_all`]
pub(crate) fn bytes_to_file<P: AsRef<Path>>(
    bytes: Vec<u8>,
    outfile: P,
    bz2: bool,
) -> Result<(), std::io::Error> {
    let compress_file: bool =
        bz2 || matches!(outfile.as_ref().extension(), Some(ext) if ext == OsStr::new("bz2"));
    let mut file: File = OpenOptions::new().append(true).create(true).open(outfile)?;
    if compress_file {
        write_bytes_bz2(bytes, &mut file)
    } else {
        file.write_all(&bytes)
    }
}

/// Writes `bytes` to a [`Write`] implementor, compressing with [`bzip2::BzEncoder`] first.
///
/// # Errors
/// From [`compress_bz2`] or [`Write::write_all`].
pub(crate) fn write_bytes_bz2<W: Write>(
    bytes: Vec<u8>,
    writer: &mut W,
) -> Result<(), std::io::Error> {
    let out_bytes: Vec<u8> = compress_bz2(&bytes)?;
    writer.write_all(&out_bytes)
}
