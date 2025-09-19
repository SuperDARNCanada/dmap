//! Utility functions for file operations.

use bzip2::{read::BzEncoder, Compression};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

/// Write bytes to file.
///
/// Ordinarily, this function opens the file in `append` mode. If the extension of `outfile` is
/// `.bz2`, the bytes will be compressed using bzip2 before being written.
///
/// # Errors
/// If opening the file in append mode is not possible (permissions, path doesn't exist, etc.). See [`std::fs::File::open`].
///
/// If an error is encountered when compressing the bytes.
///
/// If an error is encountered when writing the bytes to the filesystem. See [`std::io::Write::write_all`]
pub(crate) fn bytes_to_file<P: AsRef<Path>>(
    bytes: Vec<u8>,
    outfile: P,
) -> Result<(), std::io::Error> {
    let mut out_bytes: Vec<u8> = vec![];
    let compress_file: bool =
        matches!(outfile.as_ref().extension(), Some(ext) if ext == OsStr::new("bz2"));
    let mut file: File = OpenOptions::new().append(true).create(true).open(outfile)?;
    if compress_file {
        let mut compressor = BzEncoder::new(bytes.as_slice(), Compression::best());
        compressor.read_to_end(&mut out_bytes)?;
    } else {
        out_bytes = bytes;
    }

    file.write_all(&out_bytes)
}
