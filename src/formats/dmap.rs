use crate::error::DmapError;
use crate::types::GenericDmap::{Scalar, Vec1D, Vec2D, Vec3D};
use crate::types::{parse_scalar, parse_vector, read_data, Atom, GenericDmap, InDmap};
use indexmap::IndexMap;
use numpy::ndarray::Array;
use std::io::{Cursor, Read};

pub trait Record {
    /// Reads from dmap_data and parses into a collection of RawDmapRecord's.
    ///
    /// # Failures
    /// If dmap_data cannot be read or contains invalid data.
    fn read_records(mut dmap_data: impl Read) -> Result<Vec<Self>, DmapError>
    where
        Self: Sized,
    {
        let mut buffer: Vec<u8> = vec![];

        dmap_data.read_to_end(&mut buffer)?;

        let mut cursor = Cursor::new(buffer);
        let mut dmap_records: Vec<Self> = vec![];

        while cursor.position() < cursor.get_ref().len() as u64 {
            dmap_records.push(Self::parse_record(&mut cursor)?);
        }
        Ok(dmap_records)
    }

    /// Reads a record starting from cursor position
    fn parse_record(cursor: &mut Cursor<Vec<u8>>) -> Result<Self, DmapError>
    where
        Self: Sized,
    {
        let bytes_already_read = cursor.position();
        let _code = match read_data(cursor, Atom::INT(0))? {
            Atom::INT(i) => Ok(i),
            data => Err(DmapError::RecordError(format!(
                "Cannot interpret code '{}' at byte {}",
                data, bytes_already_read
            ))),
        }?;
        let size = match read_data(cursor, Atom::INT(0))? {
            Atom::INT(i) => Ok(i),
            data => Err(DmapError::RecordError(format!(
                "Cannot interpret size '{}' at byte {}",
                data,
                bytes_already_read + Atom::INT(0).get_num_bytes()
            ))),
        }?;

        // adding 8 bytes because code and size are part of the record.
        if size as u64
            > cursor.get_ref().len() as u64 - cursor.position() + 2 * Atom::INT(0).get_num_bytes()
        {
            return Err(DmapError::RecordError(format!(
                "Record size {size} at byte {} bigger than remaining buffer {}",
                cursor.position() - Atom::INT(0).get_num_bytes(),
                cursor.get_ref().len() as u64 - cursor.position()
                    + 2 * Atom::INT(0).get_num_bytes()
            )));
        } else if size <= 0 {
            return Err(DmapError::RecordError(format!("Record size {size} <= 0")));
        }

        let num_scalars = match read_data(cursor, Atom::INT(0))? {
            Atom::INT(i) => Ok(i),
            data => Err(DmapError::RecordError(format!(
                "Cannot interpret number of scalars at byte {}",
                cursor.position() - data.get_num_bytes()
            ))),
        }?;
        let num_vectors = match read_data(cursor, Atom::INT(0))? {
            Atom::INT(i) => Ok(i),
            data => Err(DmapError::RecordError(format!(
                "Cannot interpret number of vectors at byte {}",
                cursor.position() - data.get_num_bytes()
            ))),
        }?;
        if num_scalars <= 0 {
            return Err(DmapError::RecordError(format!(
                "Number of scalars {num_scalars} at byte {} <= 0",
                cursor.position() - 2 * Atom::INT(0).get_num_bytes()
            )));
        } else if num_vectors <= 0 {
            return Err(DmapError::RecordError(format!(
                "Number of vectors {num_vectors} at byte {} <= 0",
                cursor.position() - Atom::INT(0).get_num_bytes()
            )));
        } else if num_scalars + num_vectors > size {
            return Err(DmapError::RecordError(format!(
                "Number of scalars {num_scalars} plus vectors {num_vectors} greater than size '{size}'")));
        }

        let mut fields: IndexMap<String, GenericDmap> = IndexMap::new();
        for _ in 0..num_scalars {
            let (name, val) = parse_scalar(cursor)?;
            fields.insert(name, Scalar(val.data));
        }
        for _ in 0..num_vectors {
            let (name, val) = parse_vector(cursor, size)?;
            match val.dimensions.len() {
                1 => {
                    fields.insert(name, Vec1D(Array::from_vec(val.data)));
                }
                2 => {
                    fields.insert(
                        name,
                        Vec2D(
                            Array::from_shape_vec(
                                (val.dimensions[0] as usize, val.dimensions[1] as usize),
                                val.data,
                            )
                            .map_err(|e| DmapError::VectorError(format!("{e}")))?,
                        ),
                    );
                }
                3 => {
                    fields.insert(
                        name,
                        Vec3D(
                            Array::from_shape_vec(
                                (
                                    val.dimensions[0] as usize,
                                    val.dimensions[1] as usize,
                                    val.dimensions[2] as usize,
                                ),
                                val.data,
                            )
                            .map_err(|e| DmapError::VectorError(format!("{e}")))?,
                        ),
                    );
                }
                _ => {
                    return Err(DmapError::VectorError(format!(
                        "Vector {name} has invalid number of dimensions {}",
                        val.dimensions.len()
                    )))
                }
            };
        }

        if cursor.position() - bytes_already_read != size as u64 {
            return Err(DmapError::RecordError(format!(
                "Bytes read {} does not match the records size field {}",
                cursor.position() - bytes_already_read,
                size
            )));
        }

        Self::new(&mut fields)
    }

    /// Creates a new object from the parsed scalars and vectors
    fn new(fields: &mut IndexMap<String, GenericDmap>) -> Result<Self, DmapError>
    where
        Self: Sized;

    /// Converts a DmapRecord with metadata to a vector of raw bytes for writing
    fn to_dmap(&self) -> Vec<u8> {
        let (num_scalars, num_vectors, mut data_bytes) = self.to_bytes();
        let mut bytes: Vec<u8> = vec![];
        bytes.extend((65537_i32).data_to_bytes()); // No idea why this is what it is, copied from backscatter
        bytes.extend((data_bytes.len() as i32 + 16).data_to_bytes()); // +16 for code, length, num_scalars, num_vectors
        bytes.extend(num_scalars.data_to_bytes());
        bytes.extend(num_vectors.data_to_bytes());
        bytes.append(&mut data_bytes); // consumes data_bytes
        bytes
    }

    /// Converts only the data within the record to bytes (no metadata)
    fn to_bytes(&self) -> (i32, i32, Vec<u8>);
}