//! Defines the [`Record`] trait, which contains the shared behaviour that all DMAP records must have.

use crate::compression::detect_bz2;
use crate::error::DmapError;
use crate::io;
use crate::types::{
    parse_scalar, parse_vector, parse_vector_header, read_data, DmapField, DmapType, DmapVec,
    Fields,
};
use bzip2::read::BzDecoder;
use indexmap::IndexMap;
use itertools::izip;
use rayon::iter::Either;
use rayon::prelude::*;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;

/// DMAP record template.
///
/// This trait defines functionality for parsing bytes into records, converting records to bytes,
/// and reading from / writing to files.
pub trait Record<'a>:
    Debug + Send + Sync + TryFrom<IndexMap<String, DmapField>, Error = DmapError>
{
    /// Creates a new object from the parsed scalars and vectors.
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<Self, DmapError>
    where
        Self: Sized;

    /// Gets the underlying data of `self`.
    fn inner(self) -> IndexMap<String, DmapField>;

    /// Returns the field with name `key`, if it exists in the record.
    fn get(&self, key: &str) -> Option<&DmapField>;

    /// Returns the names of all fields stored in the record.
    fn keys(&self) -> Vec<&String>;

    /// Returns whether `name` is a metadata field of the record.
    fn is_metadata_field(name: &str) -> bool;

    /// Reads from `dmap_data` and parses into `Vec<Self>`.
    ///
    /// Returns `DmapError` if `dmap_data` cannot be read or contains invalid data.
    fn read_first_record(mut dmap_data: impl Read) -> Result<Self, DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let mut stream: Box<dyn Read>;
        let (is_bz2, chunk) = detect_bz2(&mut dmap_data)?;
        if is_bz2 {
            stream = Box::new(BzDecoder::new(chunk));
        } else {
            stream = Box::new(chunk);
        }

        let mut buffer = [0; 8]; // record size should be an i32 of the data
        stream
            .read_exact(&mut buffer)
            .map_err(|_| DmapError::CorruptStream("Unable to read size of first record"))?;

        let rec_size = i32::from_le_bytes(buffer[4..8].try_into().unwrap()) as usize; // advance 4 bytes, skipping the "code" field
        if rec_size == 0 {
            return Err(DmapError::InvalidRecord(format!(
                "Record 0 starting at byte 0 has non-positive size {} <= 0",
                rec_size
            )));
        }

        let mut rec = vec![0; rec_size];
        rec[0..8].clone_from_slice(&buffer[..]);
        stream.read_exact(&mut rec[8..])?;
        let first_rec = Self::parse_record(&mut Cursor::new(rec))?;

        Ok(first_rec)
    }

    /// Reads from `dmap_data` and parses into `Vec<Self>`.
    ///
    /// Returns `DmapError` if `dmap_data` cannot be read or contains invalid data.
    fn read_records(mut dmap_data: impl Read) -> Result<Vec<Self>, DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let mut buffer: Vec<u8> = vec![];
        let (is_bz2, mut chunk) = detect_bz2(&mut dmap_data)?;
        if is_bz2 {
            let mut stream = BzDecoder::new(chunk);
            stream.read_to_end(&mut buffer)?;
        } else {
            chunk.read_to_end(&mut buffer)?;
        }

        let mut slices: Vec<_> = vec![];
        let mut rec_start: usize = 0;
        let mut rec_size: usize;
        let mut rec_end: usize;

        while ((rec_start + 2 * i32::size()) as u64) < buffer.len() as u64 {
            rec_size = i32::from_le_bytes(buffer[rec_start + 4..rec_start + 8].try_into().unwrap())
                as usize; // advance 4 bytes, skipping the "code" field
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
        let mut dmap_results: Vec<Result<Self, DmapError>> = vec![];
        dmap_results.par_extend(
            slices
                .par_iter_mut()
                .map(|cursor| Self::parse_record(cursor)),
        );

        let mut dmap_records: Vec<Self> = vec![];
        let mut bad_recs: Vec<usize> = vec![];
        let mut dmap_errors: Vec<DmapError> = vec![];
        for (i, rec) in dmap_results.into_iter().enumerate() {
            match rec {
                Ok(x) => dmap_records.push(x),
                Err(e) => {
                    dmap_errors.push(e);
                    bad_recs.push(i);
                }
            }
        }
        if !dmap_errors.is_empty() {
            return Err(DmapError::BadRecords(bad_recs, dmap_errors[0].to_string()));
        }
        Ok(dmap_records)
    }

    /// Reads metadata of records from `dmap_data` and parses into `Vec<Self>`.
    ///
    /// Returns `DmapError` if `dmap_data` cannot be read or contains invalid data.
    fn read_metadata(
        mut dmap_data: impl Read,
    ) -> Result<Vec<IndexMap<String, DmapField>>, DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let mut buffer: Vec<u8> = vec![];
        let (is_bz2, mut chunk) = detect_bz2(&mut dmap_data)?;
        if is_bz2 {
            let mut stream = BzDecoder::new(chunk);
            stream.read_to_end(&mut buffer)?;
        } else {
            chunk.read_to_end(&mut buffer)?;
        }

        let mut slices: Vec<_> = vec![];
        let mut rec_start: usize = 0;
        let mut rec_size: usize;
        let mut rec_end: usize;

        while ((rec_start + 2 * i32::size()) as u64) < buffer.len() as u64 {
            rec_size = i32::from_le_bytes(buffer[rec_start + 4..rec_start + 8].try_into().unwrap())
                as usize; // advance 4 bytes, skipping the "code" field
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
        let mut dmap_results: Vec<Result<IndexMap<String, DmapField>, DmapError>> = vec![];
        dmap_results.par_extend(
            slices
                .par_iter_mut()
                .map(|cursor| Self::parse_metadata(cursor)),
        );

        let mut dmap_records: Vec<IndexMap<String, DmapField>> = vec![];
        let mut bad_recs: Vec<usize> = vec![];
        let mut dmap_errors: Vec<DmapError> = vec![];
        for (i, rec) in dmap_results.into_iter().enumerate() {
            match rec {
                Ok(x) => dmap_records.push(x),
                Err(e) => {
                    dmap_errors.push(e);
                    bad_recs.push(i);
                }
            }
        }
        if !dmap_errors.is_empty() {
            return Err(DmapError::BadRecords(bad_recs, dmap_errors[0].to_string()));
        }

        Ok(dmap_records)
    }

    /// Reads from `dmap_data` and parses into `Vec<Self>`.
    ///
    /// Returns a 2-tuple, where the first entry is the good records from the front of the buffer,
    /// and the second entry is the byte where the first corrupted record starts, if applicable.
    fn read_records_lax(mut dmap_data: impl Read) -> Result<(Vec<Self>, Option<usize>), DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let mut buffer: Vec<u8> = vec![];
        let (is_bz2, mut chunk) = detect_bz2(&mut dmap_data)?;
        if is_bz2 {
            let mut stream = BzDecoder::new(chunk);
            stream.read_to_end(&mut buffer)?;
        } else {
            chunk.read_to_end(&mut buffer)?;
        }

        let mut dmap_records: Vec<Self> = vec![];
        let mut bad_byte: Option<usize> = None;

        let mut slices: Vec<_> = vec![];
        let mut rec_start: usize = 0;
        let mut rec_size: usize;
        let mut rec_end: usize;

        let mut rec_starts = vec![];
        while ((rec_start + 2 * i32::size()) as u64) < buffer.len() as u64 {
            rec_size = i32::from_le_bytes(buffer[rec_start + 4..rec_start + 8].try_into().unwrap())
                as usize; // advance 4 bytes, skipping the "code" field
            rec_end = rec_start + rec_size; // error-checking the size is conducted in Self::parse_record()
            if rec_end > buffer.len() || rec_size == 0 {
                bad_byte = Some(rec_start);
                break;
                // rec_start = buffer.len(); // break from loop
            } else {
                rec_starts.push(rec_start);
                slices.push(Cursor::new(buffer[rec_start..rec_end].to_vec()));
                rec_start = rec_end;
            }
        }
        if rec_start != buffer.len() {
            bad_byte = Some(rec_start);
        }
        let mut dmap_results: Vec<Result<Self, DmapError>> = vec![];
        dmap_results.par_extend(
            slices
                .par_iter_mut()
                .map(|cursor| Self::parse_record(cursor)),
        );

        for (i, rec) in dmap_results.into_iter().enumerate() {
            if let Ok(x) = rec {
                dmap_records.push(x);
            } else {
                bad_byte = Some(rec_starts[i]);
                break;
            }
        }
        Ok((dmap_records, bad_byte))
    }

    /// Read a DMAP file of type `Self`
    fn read_file<P: AsRef<Path>>(infile: P) -> Result<Vec<Self>, DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let file = File::open(infile)?;
        Self::read_records(file)
    }

    /// Read a DMAP file of type `Self`.
    ///
    /// If the file is corrupted, it will return the leading uncorrupted records as well as the
    /// position corresponding to the start of the first corrupted record.
    fn read_file_lax<P: AsRef<Path>>(infile: P) -> Result<(Vec<Self>, Option<usize>), DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let file = File::open(infile)?;
        Self::read_records_lax(file)
    }

    /// Reads the first record of a DMAP file of type `Self`.
    fn sniff_file<P: AsRef<Path>>(infile: P) -> Result<Self, DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let file = File::open(infile)?;
        Self::read_first_record(file)
    }

    /// Read the metadata from a DMAP file of type `Self`
    fn read_file_metadata<P: AsRef<Path>>(
        infile: P,
    ) -> Result<Vec<IndexMap<String, DmapField>>, DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let file = File::open(infile)?;
        Self::read_metadata(file)
    }

    /// Reads a record from `cursor`, only keeping the metadata fields.
    fn parse_metadata(
        cursor: &mut Cursor<Vec<u8>>,
    ) -> Result<IndexMap<String, DmapField>, DmapError>
    where
        Self: Sized,
    {
        let bytes_already_read = cursor.position();
        let _code = read_data::<i32>(cursor).map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret code at byte {}: {e}",
                bytes_already_read
            ))
        })?;
        let size = read_data::<i32>(cursor).map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret size at byte {}: {e}",
                bytes_already_read + i32::size() as u64
            ))
        })?;

        // adding 8 bytes because code and size are part of the record.
        if size as u64 > cursor.get_ref().len() as u64 - cursor.position() + 2 * i32::size() as u64
        {
            return Err(DmapError::InvalidRecord(format!(
                "Record size {size} at byte {} bigger than remaining buffer {}",
                cursor.position() - i32::size() as u64,
                cursor.get_ref().len() as u64 - cursor.position() + 2 * i32::size() as u64
            )));
        } else if size <= 0 {
            return Err(DmapError::InvalidRecord(format!("Record size {size} <= 0")));
        }

        let num_scalars = read_data::<i32>(cursor).map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret number of scalars at byte {}: {e}",
                cursor.position() - i32::size() as u64
            ))
        })?;
        let num_vectors = read_data::<i32>(cursor).map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret number of vectors at byte {}: {e}",
                cursor.position() - i32::size() as u64
            ))
        })?;
        if num_scalars <= 0 {
            return Err(DmapError::InvalidRecord(format!(
                "Number of scalars {num_scalars} at byte {} <= 0",
                cursor.position() - 2 * i32::size() as u64
            )));
        } else if num_vectors <= 0 {
            return Err(DmapError::InvalidRecord(format!(
                "Number of vectors {num_vectors} at byte {} <= 0",
                cursor.position() - i32::size() as u64
            )));
        } else if num_scalars + num_vectors > size {
            return Err(DmapError::InvalidRecord(format!(
                "Number of scalars {num_scalars} plus vectors {num_vectors} greater than size '{size}'")));
        }

        let mut fields: IndexMap<String, DmapField> = IndexMap::new();
        for _ in 0..num_scalars {
            let (name, val) = parse_scalar(cursor)?;
            fields.insert(name, val);
        }
        for _ in 0..num_vectors {
            let here = cursor.position();
            let (name, dtype, _dims, num_elements) = parse_vector_header(cursor, size)?;
            if Self::is_metadata_field(&name) {
                cursor.set_position(here);
                let (_, val) = parse_vector(cursor, size)?;
                fields.insert(name.to_string(), val);
            } else {
                let vec_data_size = dtype.size() as u64 * num_elements as u64;
                let here = cursor.position();
                cursor.set_position(here + vec_data_size);
            }
        }

        if cursor.position() - bytes_already_read != size as u64 {
            return Err(DmapError::InvalidRecord(format!(
                "Bytes read {} does not match the records size field {}",
                cursor.position() - bytes_already_read,
                size
            )));
        }

        Ok(fields)
    }

    /// Reads a record from `cursor`.
    fn parse_record(cursor: &mut Cursor<Vec<u8>>) -> Result<Self, DmapError>
    where
        Self: Sized,
    {
        let bytes_already_read = cursor.position();
        let _code = read_data::<i32>(cursor).map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret code at byte {}: {e}",
                bytes_already_read
            ))
        })?;
        let size = read_data::<i32>(cursor).map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret size at byte {}: {e}",
                bytes_already_read + i32::size() as u64
            ))
        })?;

        // adding 8 bytes because code and size are part of the record.
        if size as u64 > cursor.get_ref().len() as u64 - cursor.position() + 2 * i32::size() as u64
        {
            return Err(DmapError::InvalidRecord(format!(
                "Record size {size} at byte {} bigger than remaining buffer {}",
                cursor.position() - i32::size() as u64,
                cursor.get_ref().len() as u64 - cursor.position() + 2 * i32::size() as u64
            )));
        } else if size <= 0 {
            return Err(DmapError::InvalidRecord(format!("Record size {size} <= 0")));
        }

        let num_scalars = read_data::<i32>(cursor).map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret number of scalars at byte {}: {e}",
                cursor.position() - i32::size() as u64
            ))
        })?;
        let num_vectors = read_data::<i32>(cursor).map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret number of vectors at byte {}: {e}",
                cursor.position() - i32::size() as u64
            ))
        })?;
        if num_scalars <= 0 {
            return Err(DmapError::InvalidRecord(format!(
                "Number of scalars {num_scalars} at byte {} <= 0",
                cursor.position() - 2 * i32::size() as u64
            )));
        } else if num_vectors <= 0 {
            return Err(DmapError::InvalidRecord(format!(
                "Number of vectors {num_vectors} at byte {} <= 0",
                cursor.position() - i32::size() as u64
            )));
        } else if num_scalars + num_vectors > size {
            return Err(DmapError::InvalidRecord(format!(
                "Number of scalars {num_scalars} plus vectors {num_vectors} greater than size '{size}'")));
        }

        let mut fields: IndexMap<String, DmapField> = IndexMap::new();
        for _ in 0..num_scalars {
            let (name, val) = parse_scalar(cursor)?;
            fields.insert(name, val);
        }
        for _ in 0..num_vectors {
            let (name, val) = parse_vector(cursor, size)?;
            fields.insert(name, val);
        }

        if cursor.position() - bytes_already_read != size as u64 {
            return Err(DmapError::InvalidRecord(format!(
                "Bytes read {} does not match the records size field {}",
                cursor.position() - bytes_already_read,
                size
            )));
        }

        Self::new(&mut fields)
    }

    /// Checks the validity of an `IndexMap` as a representation of a DMAP record.
    ///
    /// Validity checks include ensuring that no unfamiliar entries exist, that all required
    /// scalar and vector fields exist, that all scalar and vector fields are of the expected
    /// type, and that vector fields which are expected to have the same dimensions do indeed
    /// have the same dimensions.
    fn check_fields(
        field_dict: &mut IndexMap<String, DmapField>,
        fields_for_type: &Fields,
    ) -> Result<(), DmapError> {
        let unsupported_keys: Vec<&String> = field_dict
            .keys()
            .filter(|&k| !fields_for_type.all_fields.contains(&&**k))
            .collect();
        if !unsupported_keys.is_empty() {
            Err(DmapError::InvalidRecord(format!(
                "Unsupported fields {:?}, fields supported are {:?}",
                unsupported_keys, fields_for_type.all_fields
            )))?
        }

        for (field, expected_type) in fields_for_type.scalars_required.iter() {
            match field_dict.get(&field.to_string()) {
                Some(DmapField::Scalar(x)) if &x.get_type() == expected_type => {}
                Some(DmapField::Scalar(x)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} has incorrect type {}, expected {}",
                    field,
                    x.get_type(),
                    expected_type
                )))?,
                Some(_) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a vector, expected scalar",
                    field
                )))?,
                None => Err(DmapError::InvalidRecord(format!(
                    "Field {field:?} ({:?}) missing: fields {:?}",
                    &field.to_string(),
                    field_dict.keys()
                )))?,
            }
        }
        for (field, expected_type) in fields_for_type.scalars_optional.iter() {
            match field_dict.get(&field.to_string()) {
                Some(DmapField::Scalar(x)) if &x.get_type() == expected_type => {}
                Some(DmapField::Scalar(x)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} has incorrect type {}, expected {}",
                    field,
                    x.get_type(),
                    expected_type
                )))?,
                Some(_) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a vector, expected scalar",
                    field
                )))?,
                None => {}
            }
        }
        for (field, expected_type) in fields_for_type.vectors_required.iter() {
            match field_dict.get(&field.to_string()) {
                Some(DmapField::Scalar(_)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(DmapField::Vector(x)) if &x.get_type() != expected_type => {
                    Err(DmapError::InvalidRecord(format!(
                        "Field {field} has incorrect type {:?}, expected {expected_type:?}",
                        x.get_type()
                    )))?
                }
                Some(&DmapField::Vector(_)) => {}
                None => Err(DmapError::InvalidRecord(format!("Field {field} missing")))?,
            }
        }
        for (field, expected_type) in fields_for_type.vectors_optional.iter() {
            match field_dict.get(&field.to_string()) {
                Some(&DmapField::Scalar(_)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(DmapField::Vector(x)) if &x.get_type() != expected_type => {
                    Err(DmapError::InvalidRecord(format!(
                        "Field {field} has incorrect type {}, expected {expected_type}",
                        x.get_type()
                    )))?
                }
                _ => {}
            }
        }
        // This block checks that grouped vector fields have the same dimensionality
        for vec_group in fields_for_type.vector_dim_groups.iter() {
            let vecs: Vec<(&str, &DmapVec)> = vec_group
                .iter()
                .filter_map(|&name| match field_dict.get(&name.to_string()) {
                    Some(DmapField::Vector(ref x)) => Some((name, x)),
                    Some(_) => None,
                    None => None,
                })
                .collect();
            if vecs.len() > 1 {
                let mut vec_iter = vecs.iter();
                let first = vec_iter.next().expect("Iterator broken");
                if !vec_iter.all(|(_, v)| v.shape() == first.1.shape()) {
                    let error_vec: Vec<(&str, &[usize])> =
                        vecs.iter().map(|(k, v)| (*k, v.shape())).collect();
                    Err(DmapError::InvalidRecord(format!(
                        "Vector fields have inconsistent dimensions: {:?}",
                        error_vec
                    )))?
                }
            }
        }
        Ok(())
    }

    /// Attempts to massage the entries of an `IndexMap` into the proper types for a DMAP record.
    fn coerce(
        fields_dict: &mut IndexMap<String, DmapField>,
        fields_for_type: &Fields,
    ) -> Result<Self, DmapError> {
        let unsupported_keys: Vec<&String> = fields_dict
            .keys()
            .filter(|&k| !fields_for_type.all_fields.contains(&&**k))
            .collect();
        if !unsupported_keys.is_empty() {
            Err(DmapError::InvalidRecord(format!(
                "Unsupported fields {:?}, fields supported are {:?}",
                unsupported_keys, fields_for_type.all_fields
            )))?
        }

        for (field, expected_type) in fields_for_type.scalars_required.iter() {
            match fields_dict.get(&field.to_string()) {
                Some(DmapField::Scalar(x)) if &x.get_type() != expected_type => {
                    fields_dict.insert(
                        field.to_string(),
                        DmapField::Scalar(x.cast_as(expected_type)?),
                    );
                }
                Some(DmapField::Scalar(_)) => {}
                Some(_) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a vector, expected scalar",
                    field
                )))?,
                None => Err(DmapError::InvalidRecord(format!(
                    "Field {field:?} ({:?}) missing: fields {:?}",
                    &field.to_string(),
                    fields_dict.keys()
                )))?,
            }
        }
        for (field, expected_type) in fields_for_type.scalars_optional.iter() {
            match fields_dict.get(&field.to_string()) {
                Some(DmapField::Scalar(x)) if &x.get_type() == expected_type => {}
                Some(DmapField::Scalar(x)) => {
                    fields_dict.insert(
                        field.to_string(),
                        DmapField::Scalar(x.cast_as(expected_type)?),
                    );
                }
                Some(_) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a vector, expected scalar",
                    field
                )))?,
                None => {}
            }
        }
        for (field, expected_type) in fields_for_type.vectors_required.iter() {
            match fields_dict.get(&field.to_string()) {
                Some(DmapField::Scalar(_)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(DmapField::Vector(x)) if &x.get_type() != expected_type => {
                    Err(DmapError::InvalidRecord(format!(
                        "Field {field} has incorrect type {:?}, expected {expected_type:?}",
                        x.get_type()
                    )))?
                }
                Some(DmapField::Vector(_)) => {}
                None => Err(DmapError::InvalidRecord(format!("Field {field} missing")))?,
            }
        }
        for (field, expected_type) in fields_for_type.vectors_optional.iter() {
            match fields_dict.get(&field.to_string()) {
                Some(&DmapField::Scalar(_)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(DmapField::Vector(x)) if &x.get_type() != expected_type => {
                    Err(DmapError::InvalidRecord(format!(
                        "Field {field} has incorrect type {}, expected {expected_type}",
                        x.get_type()
                    )))?
                }
                _ => {}
            }
        }

        Self::new(fields_dict)
    }

    /// Attempts to copy `self` to a raw byte representation.
    fn to_bytes(&self) -> Result<Vec<u8>, DmapError>;

    /// Converts the entries of an `IndexMap` into a raw byte representation, including metadata
    /// about the entries `(DMAP key, name\[, dimensions\])`.
    ///
    /// If all is good, returns a tuple containing:
    /// * the number of scalar fields
    /// * the number of vector fields
    /// * the raw bytes
    fn data_to_bytes(
        data: &IndexMap<String, DmapField>,
        fields_for_type: &Fields,
    ) -> Result<(i32, i32, Vec<u8>), DmapError> {
        let mut data_bytes: Vec<u8> = vec![];
        let mut num_scalars: i32 = 0;
        let mut num_vectors: i32 = 0;

        // let scalar_fields = data.keys().filter(|k| )
        for (field, _) in fields_for_type.scalars_required.iter() {
            match data.get(&field.to_string()) {
                Some(x @ DmapField::Scalar(_)) => {
                    data_bytes.extend(field.as_bytes());
                    data_bytes.extend([0]); // null-terminate string
                    data_bytes.append(&mut x.as_bytes());
                    num_scalars += 1;
                }
                Some(_) => Err(DmapError::InvalidScalar(format!(
                    "Field {field} is a vector, expected scalar"
                )))?,
                None => Err(DmapError::InvalidRecord(format!(
                    "Field {field} missing from record"
                )))?,
            }
        }
        for (field, _) in fields_for_type.scalars_optional.iter() {
            if let Some(x) = data.get(&field.to_string()) {
                match x {
                    DmapField::Scalar(_) => {
                        data_bytes.extend(field.as_bytes());
                        data_bytes.extend([0]); // null-terminate string
                        data_bytes.append(&mut x.as_bytes());
                        num_scalars += 1;
                    }
                    DmapField::Vector(_) => Err(DmapError::InvalidScalar(format!(
                        "Field {field} is a vector, expected scalar"
                    )))?,
                }
            }
        }
        for (field, _) in fields_for_type.vectors_required.iter() {
            match data.get(&field.to_string()) {
                Some(x @ DmapField::Vector(_)) => {
                    data_bytes.extend(field.as_bytes());
                    data_bytes.extend([0]); // null-terminate string
                    data_bytes.append(&mut x.as_bytes());
                    num_vectors += 1;
                }
                Some(_) => Err(DmapError::InvalidVector(format!(
                    "Field {field} is a scalar, expected vector"
                )))?,
                None => Err(DmapError::InvalidRecord(format!(
                    "Field {field} missing from record"
                )))?,
            }
        }
        for (field, _) in fields_for_type.vectors_optional.iter() {
            if let Some(x) = data.get(&field.to_string()) {
                match x {
                    DmapField::Vector(_) => {
                        data_bytes.extend(field.as_bytes());
                        data_bytes.extend([0]); // null-terminate string
                        data_bytes.append(&mut x.as_bytes());
                        num_vectors += 1;
                    }
                    DmapField::Scalar(_) => Err(DmapError::InvalidVector(format!(
                        "Field {field} is a scalar, expected vector"
                    )))?,
                }
            }
        }

        Ok((num_scalars, num_vectors, data_bytes))
    }

    /// Converts the entries of a `Record` into a raw byte representation, for debugging the conversion.
    ///
    /// If all is good, returns a vector containing tuples of:
    /// * `String`: the name of the field (`"header"` denoting the record header)
    /// * `usize`: where the serialized bytes of the field start in the record byte representation
    /// * `Vec<u8>` the byte representation of the field.
    fn inspect_bytes(
        &self,
        fields_for_type: &Fields,
    ) -> Result<Vec<(String, usize, Vec<u8>)>, DmapError> {
        let mut data_bytes: Vec<Vec<u8>> = vec![];
        let mut indices: Vec<usize> = vec![16]; // start at 16 to account for header
        let mut fields: Vec<String> = vec![];

        let (mut num_scalars, mut num_vectors) = (0, 0);

        for (field, _) in fields_for_type.scalars_required.iter() {
            fields.push(field.to_string());
            match self.get(field) {
                Some(x @ DmapField::Scalar(_)) => {
                    let mut bytes = vec![];
                    bytes.extend(field.as_bytes());
                    bytes.extend([0]); // null-terminate string
                    bytes.append(&mut x.as_bytes());
                    indices.push(indices[indices.len() - 1] + bytes.len());
                    data_bytes.push(bytes);
                    num_scalars += 1;
                }
                Some(_) => Err(DmapError::InvalidScalar(format!(
                    "Field {field} is a vector, expected scalar"
                )))?,
                None => Err(DmapError::InvalidRecord(format!(
                    "Field {field} missing from record"
                )))?,
            }
        }
        for (field, _) in fields_for_type.scalars_optional.iter() {
            fields.push(field.to_string());
            if let Some(x) = self.get(field) {
                match x {
                    DmapField::Scalar(_) => {
                        let mut bytes = vec![];
                        bytes.extend(field.as_bytes());
                        bytes.extend([0]); // null-terminate string
                        bytes.append(&mut x.as_bytes());
                        indices.push(indices[indices.len() - 1] + bytes.len());
                        data_bytes.push(bytes);
                        num_scalars += 1;
                    }
                    DmapField::Vector(_) => Err(DmapError::InvalidScalar(format!(
                        "Field {field} is a vector, expected scalar"
                    )))?,
                }
            }
        }
        for (field, _) in fields_for_type.vectors_required.iter() {
            fields.push(field.to_string());
            match self.get(field) {
                Some(x @ DmapField::Vector(_)) => {
                    let mut bytes = vec![];
                    bytes.extend(field.as_bytes());
                    bytes.extend([0]); // null-terminate string
                    bytes.append(&mut x.as_bytes());
                    indices.push(indices[indices.len() - 1] + bytes.len());
                    data_bytes.push(bytes);
                    num_vectors += 1;
                }
                Some(_) => Err(DmapError::InvalidVector(format!(
                    "Field {field} is a scalar, expected vector"
                )))?,
                None => Err(DmapError::InvalidRecord(format!(
                    "Field {field} missing from record"
                )))?,
            }
        }
        for (field, _) in fields_for_type.vectors_optional.iter() {
            fields.push(field.to_string());
            if let Some(x) = self.get(field) {
                match x {
                    DmapField::Vector(_) => {
                        let mut bytes = vec![];
                        bytes.extend(field.as_bytes());
                        bytes.extend([0]); // null-terminate string
                        bytes.append(&mut x.as_bytes());
                        indices.push(indices[indices.len() - 1] + data_bytes.len());
                        data_bytes.push(bytes);
                        num_vectors += 1;
                    }
                    DmapField::Scalar(_) => Err(DmapError::InvalidVector(format!(
                        "Field {field} is a scalar, expected vector"
                    )))?,
                }
            }
        }

        // Now build up the header
        let num_bytes: usize = data_bytes.iter().map(|x| x.len()).sum();
        let mut bytes: Vec<u8> = vec![];
        bytes.extend((65537_i32).as_bytes()); // No idea why this is what it is, copied from backscatter
        bytes.extend((num_bytes as i32 + 16).as_bytes()); // +16 for code, length, num_scalars, num_vectors
        bytes.extend(num_scalars.as_bytes());
        bytes.extend(num_vectors.as_bytes());

        // Accumulate all the results into one big `Vec`
        let mut field_info: Vec<(String, usize, Vec<u8>)> = vec![("header".to_string(), 0, bytes)];
        for (f, (s, b)) in izip!(
            fields.into_iter(),
            izip!(indices[..indices.len() - 1].iter(), data_bytes.into_iter())
        ) {
            field_info.push((f, *s, b));
        }

        Ok(field_info)
    }

    /// Creates the byte represenation of a collection of [`Record`]s.
    ///
    /// Ordering of the members is preserved.
    fn into_bytes(recs: &Vec<Self>) -> Result<Vec<u8>, DmapError> {
        let mut bytes: Vec<u8> = vec![];
        let (errors, rec_bytes): (Vec<_>, Vec<_>) =
            recs.par_iter()
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
        Ok(bytes)
    }

    /// Attempts to convert `recs` to `Self` then convert to bytes.
    fn try_into_bytes(recs: Vec<IndexMap<String, DmapField>>) -> Result<Vec<u8>, DmapError> {
        let mut bytes: Vec<u8> = vec![];
        let (errors, rec_bytes): (Vec<_>, Vec<_>) =
            recs.into_par_iter()
                .enumerate()
                .partition_map(|(i, rec)| match Self::try_from(rec) {
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
        Ok(bytes)
    }

    /// Writes a collection of `Record`s to `outfile`.
    ///
    /// Prefer using the specific functions, e.g. `write_dmap`, `write_rawacf`, etc. for their
    /// specific field checks.
    fn write_to_file<P: AsRef<Path>>(recs: &Vec<Self>, outfile: P) -> Result<(), DmapError> {
        let bytes: Vec<u8> = Self::into_bytes(recs)?;
        io::bytes_to_file(bytes, outfile)?;
        Ok(())
    }
}

macro_rules! create_record_type {
    ($format:ident, $fields:ident) => {
        paste::paste! {
            use crate::types::{DmapType, DmapField};
            use crate::error::DmapError;
            use indexmap::IndexMap;
            use crate::record::Record;

            #[doc = "Struct containing the checked fields of a single `" $format:upper "` record." ]
            #[derive(Debug, PartialEq, Clone)]
            pub struct [< $format:camel Record >] {
                pub data: IndexMap<String, DmapField>,
            }

            impl Record<'_> for [< $format:camel Record>] {
                fn inner(self) -> IndexMap<String, DmapField> {
                    self.data
                }
                fn get(&self, key: &str) -> Option<&DmapField> {
                    self.data.get(key)
                }
                fn keys(&self) -> Vec<&String> {
                    self.data.keys().collect()
                }
                fn new(fields: &mut IndexMap<String, DmapField>) -> Result<[< $format:camel Record>], DmapError> {
                    match Self::check_fields(fields, &$fields) {
                        Ok(_) => {}
                        Err(e) => Err(e)?,
                    }

                    Ok([< $format:camel Record >] {
                        data: fields.to_owned(),
                    })
                }
                fn to_bytes(&self) -> Result<Vec<u8>, DmapError> {
                    let (num_scalars, num_vectors, mut data_bytes) =
                        Self::data_to_bytes(&self.data, &$fields)?;

                    let mut bytes: Vec<u8> = vec![];
                    bytes.extend((65537_i32).as_bytes()); // No idea why this is what it is, copied from backscatter
                    bytes.extend((data_bytes.len() as i32 + 16).as_bytes()); // +16 for code, length, num_scalars, num_vectors
                    bytes.extend(num_scalars.as_bytes());
                    bytes.extend(num_vectors.as_bytes());
                    bytes.append(&mut data_bytes); // consumes data_bytes
                    Ok(bytes)
                }
                fn is_metadata_field(name: &str) -> bool {
                    !$fields.data_fields.iter().any(|e| e == &name)
                }
            }

            impl TryFrom<&mut IndexMap<String, DmapField>> for [< $format:camel Record >] {
                type Error = DmapError;

                fn try_from(value: &mut IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
                    Self::coerce(value, &$fields)
                }
            }

            impl TryFrom<IndexMap<String, DmapField>> for [< $format:camel Record >] {
                type Error = DmapError;

                fn try_from(mut value: IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
                    Self::coerce(&mut value, &$fields)
                }
            }

            #[cfg(test)]
            mod tests {
                use super::*;
                use std::path::PathBuf;

                /// Creates a test to ensure that the record is still able to be read, even when missing
                /// some of the optional fields.
                #[test]
                fn test_missing_optional_fields() -> Result<(), DmapError> {
                    let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($format)));
                    let data = [< $format:camel Record >]::sniff_file(&filename).expect("Unable to sniff file");
                    let recs = data.inner();

                    for field in $fields.scalars_optional.iter().chain($fields.vectors_optional.iter()) {
                        let mut cloned_rec = recs.clone();
                        let _ = cloned_rec.shift_remove(field.0);
                        let _ = [< $format:camel Record >]::try_from(&mut cloned_rec)?;
                    }
                    Ok(())
                }

                /// Creates a test to ensure that the record is not able to be read when missing
                /// some of the required fields.
                #[test]
                fn test_missing_required_fields() -> Result<(), DmapError> {
                    let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($format)));
                    let data = [< $format:camel Record >]::sniff_file(&filename).expect("Unable to sniff file");
                    let recs = data.inner();

                    for field in $fields.scalars_required.iter().chain($fields.vectors_required.iter()) {
                        let mut cloned_rec = recs.clone();
                        let _ = cloned_rec.shift_remove(field.0);
                        let res = [< $format:camel Record >]::try_from(&mut cloned_rec);
                        assert!(res.is_err());
                    }
                    Ok(())
                }
            }
        }
    }
}

pub(crate) use create_record_type;
