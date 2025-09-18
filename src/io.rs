use bzip2::{Compression, read::BzEncoder};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

/// Write bytes to file.
///
/// Ordinarily, this function opens the file in `append` mode. If the extension of `outfile` is
/// `.bz2`, the bytes will be compressed using bzip2 before being written, and the file is instead
/// opened in `create_new` mode, meaning it will fail if a file already exists at the given path.
pub(crate) fn bytes_to_file<P: AsRef<Path>>(bytes: Vec<u8>, outfile: P) -> Result<(), std::io::Error> {
    let mut out_bytes: Vec<u8> = vec![];
    let compress_file: bool = match outfile.as_ref().extension() {
        Some(ext) if ext == OsStr::new("bz2") => { true },
        _ => { false }
    };
    let mut file: File = OpenOptions::new().append(true).create(true).open(outfile)?;
    if compress_file {
        let mut compressor = BzEncoder::new(bytes.as_slice(), Compression::best());
        compressor.read_to_end(&mut out_bytes)?;
    } else {
        out_bytes = bytes;
    }
    
    file.write_all(&out_bytes)
}