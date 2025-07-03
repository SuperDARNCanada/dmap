//! Defines the `Record` trait, which contains the shared behaviour that all
//! DMAP records must have.

use crate::error::DmapError;
use crate::types::{parse_scalar, parse_vector, read_data, DmapField, DmapType, DmapVec, Fields};
use bzip2::read::BzDecoder;
use indexmap::IndexMap;
use rayon::prelude::*;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::PathBuf;

pub trait Record<'a>:
    Debug + Send + TryFrom<&'a mut IndexMap<String, DmapField>, Error = DmapError>
{
    /// Gets the underlying data of `self`.
    fn inner(self) -> IndexMap<String, DmapField>;

    /// Reads from `dmap_data` and parses into `Vec<Self>`.
    ///
    /// Returns `DmapError` if `dmap_data` cannot be read or contains invalid data.
    fn read_records(mut dmap_data: impl Read) -> Result<Vec<Self>, DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let mut buffer: Vec<u8> = vec![];
        dmap_data.read_to_end(&mut buffer)?;

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
            } else if rec_size <= 0 {
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
        if dmap_errors.len() > 0 {
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
        dmap_data.read_to_end(&mut buffer)?;

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
            if rec_end > buffer.len() || rec_size <= 0 {
                bad_byte = Some(rec_start);
                break;
                // rec_start = buffer.len(); // break from loop
            } else {
                rec_starts.push(rec_start);
                slices.push(Cursor::new(buffer[rec_start..rec_end].to_vec()));
                rec_start = rec_end;
            }
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
    fn read_file(infile: &PathBuf) -> Result<Vec<Self>, DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let file = File::open(infile)?;
        match infile.extension() {
            Some(ext) if ext == OsStr::new("bz2") => {
                let compressor = BzDecoder::new(file);
                Self::read_records(compressor)
            }
            _ => Self::read_records(file),
        }
    }

    /// Read a DMAP file of type `Self`.
    ///
    /// If the file is corrupted, it will return the leading uncorrupted records as well as the
    /// position corresponding to the start of the first corrupted record.
    fn read_file_lax(infile: &PathBuf) -> Result<(Vec<Self>, Option<usize>), DmapError>
    where
        Self: Sized,
        Self: Send,
    {
        let file = File::open(infile)?;
        match infile.extension() {
            Some(ext) if ext == OsStr::new("bz2") => {
                let compressor = BzDecoder::new(file);
                Self::read_records_lax(compressor)
            }
            _ => Self::read_records_lax(file),
        }
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

    /// Creates a new object from the parsed scalars and vectors.
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<Self, DmapError>
    where
        Self: Sized;

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
    fn coerce<T: Record<'a>>(
        fields_dict: &mut IndexMap<String, DmapField>,
        fields_for_type: &Fields,
    ) -> Result<T, DmapError> {
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

        T::new(fields_dict)
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

            impl [< $format:camel Record >] {
                /// Returns the field with name `key`, if it exists in the record.
                pub fn get(&self, key: &String) -> Option<&DmapField> {
                    self.data.get(key)
                }

                /// Returns the names of all fields stored in the record.
                pub fn keys(&self) -> Vec<&String> {
                    self.data.keys().collect()
                }
            }

            impl Record<'_> for [< $format:camel Record>] {
                fn inner(self) -> IndexMap<String, DmapField> {
                    self.data
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
            }

            impl TryFrom<&mut IndexMap<String, DmapField>> for [< $format:camel Record >] {
                type Error = DmapError;

                fn try_from(value: &mut IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
                    Self::coerce::<[< $format:camel Record>]>(value, &$fields)
                }
            }
        }
    }
}

pub(crate) use create_record_type;
