//! Utility functions for file operations.

use crate::compression::{compress_bz2, detect_bz2};
use crate::types::DmapType;
use crate::DmapError;
use bzip2::read::BzDecoder;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Write};
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

/// Set up the stream for reading. Autodetects and decompresses BZ2.
pub(crate) fn create_stream<'b>(dmap_data: &'b mut impl Read) -> Result<Box<dyn Read + 'b>, DmapError> {
    let (is_bz2, chunk) = detect_bz2(dmap_data)?;
    if is_bz2 {
        Ok(Box::new(BzDecoder::new(chunk)))
    } else {
        Ok(Box::new(chunk))
    }
}

/// Parses `dmap_data` into discrete chunks, each corresponding to a DMAP record.  
pub(crate) fn split_into_slices(
    mut dmap_data: impl Read,
) -> Result<Vec<Cursor<Vec<u8>>>, DmapError> {
    let mut buffer: Vec<u8> = vec![];
    create_stream(&mut dmap_data)?.read_to_end(&mut buffer)?;

    let mut slices: Vec<_> = vec![];
    let mut rec_start: usize = 0;
    let mut rec_size: usize;
    let mut rec_end: usize;

    while ((rec_start + 2 * i32::size()) as u64) < buffer.len() as u64 {
        rec_size =
            i32::from_le_bytes(buffer[rec_start + 4..rec_start + 8].try_into().unwrap()) as usize; // advance 4 bytes, skipping the "code" field
        rec_end = rec_start + rec_size; // error-checking the size is conducted in Self::parse_record()
        if rec_end > buffer.len() {
            return Err(DmapError::InvalidRecord(format!("Record {} starting at byte {} has size greater than remaining length of buffer ({} > {})", slices.len(), rec_start, rec_size, buffer.len() - rec_start)));
        } else if rec_size == 0 {
            return Err(DmapError::InvalidRecord(format!(
                "Record {} starting at byte {} has non-positive size {} <= 0",
                slices.len(),
                rec_start,
                rec_size
            )));
        }
        slices.push(Cursor::new(buffer[rec_start..rec_end].to_vec()));
        rec_start = rec_end;
    }
    if rec_start != buffer.len() {
        return Err(DmapError::InvalidRecord(format!(
            "Record {} starting at byte {} incomplete; has size of {} bytes",
            slices.len() + 1,
            rec_start,
            buffer.len() - rec_start
        )));
    }
    Ok(slices)
}
