//! Utility functions for detecting compression on a stream.
//!
//! Currently only supports bz2 compression detection.

use std::io::{Chain, Cursor, Error, Read};

type PartiallyReadStream<T> = Chain<Cursor<[u8; 3]>, T>;

/// Detects bz2 compression on the input `stream`. Returns a reader
/// which includes all data from `stream`.
///
/// # Errors
/// See [`std::io::Read::read_exact`].
pub(crate) fn detect_bz2<T>(mut stream: T) -> Result<(bool, PartiallyReadStream<T>), Error>
where
    T: for<'a> Read,
{
    // Read the first 3 bytes to detect bz2 compression
    let mut buffer = [0u8; 3];
    stream.read_exact(&mut buffer)?;

    // valid bz2 blocks start with "BZh", which is 425a68 in hex.
    let is_bz2 = buffer == [0x42, 0x5a, 0x68];
    let full_stream = Cursor::new(buffer).chain(stream);
    Ok((is_bz2, full_stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bzip2::{read::BzDecoder, read::BzEncoder, Compression};

    #[test]
    fn bz2_detection() -> Result<(), Error> {
        let data = "Hello world".as_bytes();
        let compressor = BzEncoder::new(data, Compression::best());

        let (result, stream) = detect_bz2(compressor)?;
        assert_eq!(result, true);
        let mut returned_stream = vec![];
        let mut decompressed = BzDecoder::new(stream);
        let _ = decompressed.read_to_end(&mut returned_stream);
        assert_eq!(returned_stream, b"Hello world");

        let data = "Hello world".as_bytes();
        let (result, mut stream) = detect_bz2(data)?;
        assert_eq!(result, false);
        let mut returned_stream = vec![];
        let _ = stream.read_to_end(&mut returned_stream);
        assert_eq!(returned_stream, b"Hello world");

        Ok(())
    }
}
