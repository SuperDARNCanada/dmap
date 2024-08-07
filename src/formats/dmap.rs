use crate::error::DmapError;
use crate::types::{parse_scalar, parse_vector, read_data, DmapField, DmapType, Fields, DmapVec};
use indexmap::IndexMap;
use rayon::prelude::*;
use std::fmt::Debug;
use std::io::{Cursor, Read};

pub trait Record: Debug {
    /// Reads from dmap_data and parses into a collection of Records.
    ///
    /// # Failures
    /// If dmap_data cannot be read or contains invalid data.
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
            slices.push(Cursor::new(buffer[rec_start..rec_end].to_vec()));
            rec_start = rec_end;
        }
        let mut dmap_results: Vec<Result<Self, DmapError>> = vec![];
        dmap_results.par_extend(
            slices
                .par_iter_mut()
                .map(|cursor| Self::parse_record(cursor)),
        );

        let mut dmap_records: Vec<Self> = vec![];
        for (i, rec) in dmap_results.into_iter().enumerate() {
            dmap_records.push(match rec {
                Err(e) => Err(DmapError::InvalidRecord(format!("{e}: record {i}")))?,
                Ok(x) => x,
            });
        }

        Ok(dmap_records)
    }

    /// Reads a record starting from cursor position
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

    /// Creates a new object from the parsed scalars and vectors
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<Self, DmapError>
    where
        Self: Sized;

    fn check_fields(
        field_dict: &mut IndexMap<String, DmapField>,
        fields_for_type: &Fields,
    ) -> Result<(), DmapError> {
        let unsupported_keys: Vec<&String> = field_dict
            .keys()
            .filter(|&k| !fields_for_type.all_fields.contains(&&**k))
            .collect();
        if unsupported_keys.len() > 0 {
            Err(DmapError::InvalidRecord(format!(
                "Unsupported fields {:?}, fields supported are {:?}",
                unsupported_keys, fields_for_type.all_fields
            )))?
        }

        for (field, expected_type) in fields_for_type.scalars_required.iter() {
            match field_dict.get(&field.to_string()) {
                Some(&DmapField::Scalar(ref x)) if &x.get_type() == expected_type => {}
                Some(&DmapField::Scalar(ref x)) => Err(DmapError::InvalidRecord(format!(
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
                Some(&DmapField::Scalar(ref x)) if &x.get_type() == expected_type => {}
                Some(&DmapField::Scalar(ref x)) => Err(DmapError::InvalidRecord(format!(
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
                Some(&DmapField::Scalar(_)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(&DmapField::Vector(ref x)) if &x.get_type() != expected_type => {
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
                Some(&DmapField::Vector(ref x)) if &x.get_type() != expected_type => {
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
                if !vec_iter.all(|(_, ref v)| v.shape() == first.1.shape()) {
                    Err(DmapError::InvalidRecord(format!(
                        "Vector fields have inconsistent dimensions: {:?}",
                        vecs
                    )))?
                }
            }
        }
        Ok(())
    }

    fn coerce<T: Record>(
        fields_dict: &mut IndexMap<String, DmapField>,
        fields_for_type: &Fields,
    ) -> Result<T, DmapError> {
        let unsupported_keys: Vec<&String> = fields_dict
            .keys()
            .filter(|&k| !fields_for_type.all_fields.contains(&&**k))
            .collect();
        if unsupported_keys.len() > 0 {
            Err(DmapError::InvalidRecord(format!(
                "Unsupported fields {:?}, fields supported are {:?}",
                unsupported_keys, fields_for_type.all_fields
            )))?
        }

        for (field, expected_type) in fields_for_type.scalars_required.iter() {
            match fields_dict.get(&field.to_string()) {
                Some(&DmapField::Scalar(ref x)) if &x.get_type() != expected_type => {
                    fields_dict.insert(
                        field.to_string(),
                        DmapField::Scalar(x.cast_as(expected_type)?),
                    );
                }
                Some(&DmapField::Scalar(_)) => {}
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
                Some(&DmapField::Scalar(ref x)) if &x.get_type() == expected_type => {}
                Some(&DmapField::Scalar(ref x)) => {
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
                Some(&DmapField::Scalar(_)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(&DmapField::Vector(ref x)) if &x.get_type() != expected_type => {
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
            match fields_dict.get(&field.to_string()) {
                Some(&DmapField::Scalar(_)) => Err(DmapError::InvalidRecord(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(&DmapField::Vector(ref x)) if &x.get_type() != expected_type => {
                    Err(DmapError::InvalidRecord(format!(
                        "Field {field} has incorrect type {}, expected {expected_type}",
                        x.get_type()
                    )))?
                }
                _ => {}
            }
        }

        Ok(T::new(fields_dict)?)
    }

    /// Converts a DmapRecord with metadata to a vector of raw bytes for writing
    fn to_bytes(&self) -> Result<Vec<u8>, DmapError>;

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
