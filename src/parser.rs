use std::io::Cursor;
use numpy::ndarray::ArrayD;
use indexmap::IndexMap;
use crate::{DmapError, Record};
use crate::types::{DmapField, DmapScalar, DmapType, DmapVec, Type};

/// DMAP record header information
pub(crate) struct Header {
    pub size: i32,
    pub _code: i32,
    pub num_scalars: i32,
    pub num_vectors: i32,
}

/// Parser for interpreting byte streams as DMAP records.
pub(crate) struct Parser {
    cursor: Cursor<Vec<u8>>,
    start_byte: u64,
    is_corrupt: bool,
}

impl From<Cursor<Vec<u8>>> for Parser {
    fn from(cursor: Cursor<Vec<u8>>) -> Self {
        let start_byte = cursor.position();
        Self {
            cursor,
            start_byte,
            is_corrupt: false,
        }
    }
}

impl Parser {
    /// Creates a `Parser` over a buffer of `Vec<u8>`
    pub(crate) fn new(buf: Vec<u8>) -> Self {
        Self {
            cursor: Cursor::new(buf),
            start_byte: 0,
            is_corrupt: false
        }
    }

    /// Resets to the beginning of the stream.
    pub(crate) fn reset(&mut self) -> &mut Parser {
        self.cursor.set_position(0);
        self.start_byte = 0;
        self.is_corrupt = false;
        self
    }

    /// Gets the cursor position.
    pub(crate) fn position(&self) -> u64 {
        self.cursor.position()
    }

    /// Updates the position of the internal cursor.
    pub(crate) fn set_position(&mut self, pos: u64) {
        self.cursor.set_position(pos)
    }

    /// Reads a DMAP record header.
    pub(crate) fn read_record_header(&mut self) -> Result<Header, DmapError> {
        let bytes_already_read = self.position();
        let _code = self.read_data::<i32>().map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret code at byte {}: {e}",
                bytes_already_read
            ))
        })?;
        let size = self.read_data::<i32>().map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret size at byte {}: {e}",
                bytes_already_read + i32::size() as u64
            ))
        })?;

        // adding 8 bytes because code and size are part of the record.
        if size as u64 > self.cursor.get_ref().len() as u64 - self.position() + 2 * i32::size() as u64
        {
            return Err(DmapError::InvalidRecord(format!(
                "Record size {size} at byte {} bigger than remaining buffer {}",
                self.position() - i32::size() as u64,
                self.cursor.get_ref().len() as u64 - self.position() + 2 * i32::size() as u64
            )));
        } else if size <= 0 {
            return Err(DmapError::InvalidRecord(format!("Record size {size} <= 0")));
        }

        let num_scalars = self.read_data::<i32>().map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret number of scalars at byte {}: {e}",
                self.position() - i32::size() as u64
            ))
        })?;
        let num_vectors = self.read_data::<i32>().map_err(|e| {
            DmapError::InvalidRecord(format!(
                "Cannot interpret number of vectors at byte {}: {e}",
                self.position() - i32::size() as u64
            ))
        })?;
        if num_scalars <= 0 {
            Err(DmapError::InvalidRecord(format!(
                "Number of scalars {num_scalars} at byte {} <= 0",
                self.position() - 2 * i32::size() as u64
            )))
        } else if num_vectors <= 0 {
            Err(DmapError::InvalidRecord(format!(
                "Number of vectors {num_vectors} at byte {} <= 0",
                self.position() - i32::size() as u64
            )))
        } else if num_scalars + num_vectors > size {
            Err(DmapError::InvalidRecord(format!(
                "Number of scalars {num_scalars} plus vectors {num_vectors} greater than size '{size}'")))
        } else {
            Ok(Header {size, _code, num_scalars, num_vectors })
        }
    }

    /// Parses a scalar starting from the `cursor` position.
    ///
    /// Interprets the bytes starting from the `cursor` position in the following order:
    /// 1. `name`: a null-terminated string
    /// 2. `type`: an i32 key, which maps to a data type (see [`Type`])
    /// 3. `data`: the actual data as raw bytes.
    #[inline]
    pub(crate) fn parse_scalar(&mut self) -> Result<(String, DmapField), DmapError> {
        let (name, data_type) = self.parse_scalar_header()?;
        let data: DmapScalar = match data_type {
            Type::Char => DmapScalar::Char(self.read_data::<i8>()?),
            Type::Short => DmapScalar::Short(self.read_data::<i16>()?),
            Type::Int => DmapScalar::Int(self.read_data::<i32>()?),
            Type::Long => DmapScalar::Long(self.read_data::<i64>()?),
            Type::Uchar => DmapScalar::Uchar(self.read_data::<u8>()?),
            Type::Ushort => DmapScalar::Ushort(self.read_data::<u16>()?),
            Type::Uint => DmapScalar::Uint(self.read_data::<u32>()?),
            Type::Ulong => DmapScalar::Ulong(self.read_data::<u64>()?),
            Type::Float => DmapScalar::Float(self.read_data::<f32>()?),
            Type::Double => DmapScalar::Double(self.read_data::<f64>()?),
            Type::String => DmapScalar::String(self.read_data::<String>()?),
        };

        Ok((name, DmapField::Scalar(data)))
    }

    /// Grabs the name and data type key from the cursor.
    #[inline]
    pub(crate) fn parse_scalar_header(&mut self) -> Result<(String, Type), DmapError> {
        let name = self.read_data::<String>().map_err(|e| {
            DmapError::InvalidField(format!("Invalid name, byte {}: {e}", self.position()))
        })?;
        let data_type_key = self.read_data::<i8>().map_err(|e| {
            DmapError::InvalidField(format!(
                "Invalid data type for field '{name}', byte {}: {e}",
                self.position() - i8::size() as u64
            ))
        })?;
        let data_type = Type::from_key(data_type_key)?;

        Ok((name, data_type))
    }

    /// Parses a header for a vector starting from the `cursor` position.
    ///
    /// Interprets the bytes in `cursor` as follows:
    /// 1. `name`: a null-terminated string
    /// 2. `type`: a key indicating the data type ([`Type`])
    /// 3. `num_dims`: the number of dimensions in the array, as an `i32`.
    /// 4. `dims`: the dimensions themselves, as a list of `num_dims` `i32`s, in column-major order.
    pub(crate) fn parse_vector_header(
        &mut self,
        record_size: i32,
    ) -> Result<(String, Type, Vec<usize>, i32), DmapError> {
        let (name, data_type) = self.parse_scalar_header()?;

        let vector_dimension = self.read_data::<i32>()?;
        if vector_dimension > record_size {
            return Err(DmapError::InvalidVector(format!(
                "Parsed number of vector dimensions {vector_dimension} for field `{name}` at byte {} are larger \
            than record size {record_size}",
                self.position() - i32::size() as u64,
            )));
        } else if vector_dimension <= 0 {
            return Err(DmapError::InvalidVector(format!(
                "Parsed number of vector dimensions {vector_dimension} for field `{name}` at byte {} are zero or \
            negative",
                self.position() - i32::size() as u64,
            )));
        }

        let mut dimensions: Vec<usize> = vec![];
        let mut total_elements = 1;
        for _ in 0..vector_dimension {
            let dim = self.read_data::<i32>()?;
            if dim <= 0 && name != "slist" {
                return Err(DmapError::InvalidVector(format!(
                    "Vector `{name}` dimension {dim} at byte {} is zero or negative",
                    self.position() - i32::size() as u64,
                )));
            } else if dim > record_size {
                return Err(DmapError::InvalidVector(format!(
                    "Vector `{name}` dimension {dim} at byte {} exceeds record size {record_size}",
                    self.position() - i32::size() as u64,
                )));
            }
            dimensions.push(usize::try_from(dim)?);
            total_elements *= dim;
        }
        dimensions = dimensions.into_iter().rev().collect(); // reverse the dimensions, stored in column-major order
        if total_elements * i32::try_from(data_type.size())? > record_size {
            return Err(DmapError::InvalidVector(format!(
                "Vector `{name}` size starting at byte {} exceeds record size ({} > {record_size})",
                self.position() - u64::try_from(vector_dimension)? * u64::try_from(i32::size())?,
                total_elements * i32::try_from(data_type.size())?,
            )));
        }

        Ok((name, data_type, dimensions, total_elements))
    }

    /// Parses a vector starting from the current position.
    ///
    /// Interprets the bytes in `self.cursor` as follows:
    /// 1. `name`: a null-terminated string
    /// 2. `type`: a key indicating the data type ([`Type`])
    /// 3. `num_dims`: the number of dimensions in the array, as an `i32`.
    /// 4. `dims`: the dimensions themselves, as a list of `num_dims` `i32`s, in column-major order.
    /// 5. `data`: the data itself, of type `type` with shape `dims`, stored in column-major order.
    pub(crate) fn parse_vector(
        &mut self,
        record_size: i32,
    ) -> Result<(String, DmapField), DmapError> {
        let start_position = self.position();
        let (name, data_type, dimensions, total_elements) = self.parse_vector_header(record_size)?;

        macro_rules! dmapvec_from_cursor {
            ($type:ty, $enum_var:path, $dims:ident, $parser:ident, $num_elements:ident, $name:ident) => {
                $enum_var(
                    ArrayD::from_shape_vec($dims, $parser.read_vector::<$type>($num_elements)?)
                        .map_err(|e| {
                            DmapError::InvalidVector(format!(
                                "Could not read in vector field {}: {e}",
                                $name
                            ))
                        })?,
                )
            };
        }

        let vector: DmapVec = match data_type {
            Type::Char => {
                dmapvec_from_cursor!(i8, DmapVec::Char, dimensions, self, total_elements, name)
            }
            Type::Short => dmapvec_from_cursor!(
                i16,
                DmapVec::Short,
                dimensions,
                self,
                total_elements,
                name
            ),
            Type::Int => {
                dmapvec_from_cursor!(i32, DmapVec::Int, dimensions, self, total_elements, name)
            }
            Type::Long => {
                dmapvec_from_cursor!(i64, DmapVec::Long, dimensions, self, total_elements, name)
            }
            Type::Uchar => {
                dmapvec_from_cursor!(u8, DmapVec::Uchar, dimensions, self, total_elements, name)
            }
            Type::Ushort => dmapvec_from_cursor!(
                u16,
                DmapVec::Ushort,
                dimensions,
                self,
                total_elements,
                name
            ),
            Type::Uint => {
                dmapvec_from_cursor!(u32, DmapVec::Uint, dimensions, self, total_elements, name)
            }
            Type::Ulong => dmapvec_from_cursor!(
                u64,
                DmapVec::Ulong,
                dimensions,
                self,
                total_elements,
                name
            ),
            Type::Float => dmapvec_from_cursor!(
                f32,
                DmapVec::Float,
                dimensions,
                self,
                total_elements,
                name
            ),
            Type::Double => dmapvec_from_cursor!(
                f64,
                DmapVec::Double,
                dimensions,
                self,
                total_elements,
                name
            ),
            Type::String => {
                return Err(DmapError::InvalidVector(format!(
                    "Invalid type {data_type} for DMAP vector {name}"
                )))
            }
        };

        let num_bytes = self.position() - start_position;
        if num_bytes > u64::try_from(record_size)? {
            return Err(DmapError::InvalidVector(format!(
                "Vector `{name}` occupies more bytes than record ({num_bytes} > {record_size})"
            )));
        }

        Ok((name, DmapField::Vector(vector)))
    }

    /// Read the raw data (excluding metadata) for a DMAP vector of type `T` from `cursor`.
    fn read_vector<T: DmapType>(&mut self, num_elements: i32) -> Result<Vec<T>, DmapError> {
        let mut data: Vec<T> = vec![];
        for _ in 0..num_elements {
            data.push(self.read_data::<T>()?);
        }
        Ok(data)
    }

    /// Reads a singular value of type `T` from the current position.
    #[inline]
    pub(crate) fn read_data<T: DmapType>(&mut self) -> Result<T, DmapError> {
        let position = usize::try_from(self.position())?;
        let stream = self.cursor.get_mut();

        if position > stream.len() {
            return Err(DmapError::CorruptStream("Cursor extends out of buffer"));
        }
        if stream.len() - position < T::size() {
            return Err(DmapError::CorruptStream(
                "Byte offsets into buffer are not properly aligned",
            ));
        }

        let data_size = match T::size() {
            0 => {
                // String type
                let mut byte_counter = 0;
                while stream[position + byte_counter] != 0 {
                    byte_counter += 1;
                    if position + byte_counter >= stream.len() {
                        return Err(DmapError::CorruptStream("String is improperly terminated"));
                    }
                }
                byte_counter + 1
            }
            x => x,
        };
        let data: &[u8] = &stream[position..position + data_size];
        let parsed_data = T::from_bytes(data)?;

        self.cursor.set_position({ position + data_size } as u64);

        Ok(parsed_data)
    }

    /// Reads a record from `parser`.
    pub(crate) fn parse_record<'a, T: Record<'a>>(&mut self) -> Result<T, DmapError>
    where
        Self: Sized,
    {
        let header = self.read_record_header()?;

        let mut fields: IndexMap<String, DmapField> = IndexMap::new();
        for _ in 0..header.num_scalars {
            let (name, val) = self.parse_scalar()?;
            fields.insert(name, val);
        }
        for _ in 0..header.num_vectors {
            let (name, val) = self.parse_vector(header.size)?;
            fields.insert(name, val);
        }

        if self.position() != header.size as u64 {
            return Err(DmapError::InvalidRecord(format!(
                "Bytes read {} does not match the records size field {}",
                self.position(),
                header.size
            )));
        }

        self.set_position(0);  // reset the cursor to the start

        T::new(&mut fields)
    }

    /// Reads a record from `self`, only keeping the metadata fields.
    pub(crate) fn parse_metadata<'a, T: Record<'a>>(&mut self) -> Result<IndexMap<String, DmapField>, DmapError>
    where
        Self: Sized,
    {
        let header = self.read_record_header()?;

        let mut fields: IndexMap<String, DmapField> = IndexMap::new();
        for _ in 0..header.num_scalars {
            let (name, val) = self.parse_scalar()?;
            fields.insert(name, val);
        }
        for _ in 0..header.num_vectors {
            let here = self.position();
            let (name, dtype, _dims, num_elements) = self.parse_vector_header(header.size)?;
            if T::is_metadata_field(&name) {
                self.set_position(here);
                let (_, val) = self.parse_vector(header.size)?;
                fields.insert(name.to_string(), val);
            } else {
                let vec_data_size = dtype.size() as u64 * num_elements as u64;
                let here = self.position();
                self.set_position(here + vec_data_size);
            }
        }

        if self.position() != header.size as u64 {
            return Err(DmapError::InvalidRecord(format!(
                "Bytes read {} does not match the records size field {}",
                self.position(),
                header.size
            )));
        }

        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use numpy::array;
    use super::*;

    #[test]
    fn test_read_vec() {

        let bytes: Vec<u8> = vec![1, 0, 1, 0];
        let mut parser = Parser::new(bytes.clone());
        let data = parser.read_vector::<u8>(4);
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), vec![1, 0, 1, 0]);

        parser.reset();
        let data = parser.read_vector::<u16>(2);
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), vec![1, 1]);

        parser.reset();
        let data = parser.read_vector::<i8>(4);
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), vec![1, 0, 1, 0]);

        parser.reset();
        let data = parser.read_vector::<i16>(2);
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), vec![1, 1]);
    }

    #[test]
    fn test_read_data() {
        // bytes are little-endian, so this will come out to 1 no matter if you interpret the first
        // number of bytes as u8, u16, u32, u64, i8, i16, i32, or i64.
        let bytes: Vec<u8> = vec![1, 0, 0, 0, 0, 0, 0, 0];
        let mut parser = Parser::new(bytes);
        let data = parser.read_data::<u8>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), 1);

        parser.reset();
        let data = parser.read_data::<u16>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), 1);

        parser.reset();
        let data = parser.read_data::<u32>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), 1);

        parser.reset();
        let data = parser.read_data::<u64>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), 1);

        parser.reset();
        let data = parser.read_data::<i8>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), 1);

        parser.reset();
        let data = parser.read_data::<i16>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), 1);

        parser.reset();
        let data = parser.read_data::<i32>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), 1);

        parser.reset();
        let data = parser.read_data::<i64>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), 1);

        // This read_data call should return an error, since i64 is bigger than the remaining buffer
        parser.set_position(1);
        let data = parser.read_data::<i64>();
        assert!(data.is_err());

        // This read_data call should return an error, since the cursor is past the end of the buffer
        parser.set_position(4);
        let data = parser.read_data::<i64>();
        assert!(data.is_err());

        let bytes: Vec<u8> = vec![116, 101, 115, 116, 0]; // b"test\0"
        let mut parser = Parser::new(bytes);
        let data = parser.read_data::<String>();
        assert!(data.is_ok());
        assert_eq!(data.unwrap(), "test".to_string());

        let bytes: Vec<u8> = vec![116, 101, 115, 116]; // b"test", not null-terminated
        let mut parser = Parser::new(bytes);
        let data = parser.read_data::<String>();
        assert!(data.is_err());
    }

    #[test]
    fn test_parse_header() -> Result<(), DmapError> {
        let name: Vec<u8> = vec![116, 101, 115, 116, 0, Type::Char.key() as u8];
        let num_bytes = name.len();
        let mut parser = Parser::new(name);
        let res = parser.parse_scalar_header();
        assert_eq!(res?, ("test".to_string(), Type::Char));
        assert_eq!(parser.position(), num_bytes as u64);

        parser.set_position(2);
        let res = parser.parse_scalar_header();
        assert_eq!(res?, ("st".to_string(), Type::Char));

        parser.set_position(5);
        let res = parser.parse_scalar();
        assert!(res.is_err());

        let name: Vec<u8> = vec![116, 101, 115, 116, Type::Char.key() as u8]; // name not null-terminated
        let mut parser = Parser::new(name);
        let res = parser.parse_scalar_header();
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn test_parse_scalar() -> Result<(), DmapError> {
        let mut name: Vec<u8> = vec![116, 101, 115, 116, 0]; // "test" in bytes
        let mut data: Vec<u8> = vec![Type::Char.key() as u8, 25, 56];
        name.append(&mut data);
        let num_bytes = name.len();
        let mut parser = Parser::new(name);
        let res = parser.parse_scalar();
        assert_eq!(res?, ("test".to_string(), 25i8.into()));
        assert_eq!(parser.position(), (num_bytes - 1) as u64);

        parser.set_position(1);
        let res = parser.parse_scalar();
        assert_eq!(res?, ("est".to_string(), 25i8.into()));

        parser.set_position(4);
        let res = parser.parse_scalar();
        assert_eq!(res?, ("".to_string(), 25i8.into()));

        parser.set_position(5);
        let res = parser.parse_scalar();
        assert!(res.is_err());

        // This test should highlight the problem when the name is not null-terminated. The bytes of the `type` are
        // consumed as part of the scalar name, until a 0 is encountered.
        let mut name: Vec<u8> = vec![116, 101, 115, 116]; // b"test" , not null-terminated
        let mut data: Vec<u8> = vec![Type::Char.key() as u8, 25];
        name.append(&mut data);
        let mut parser = Parser::new(name);
        let res = parser.parse_scalar();
        assert!(res.is_err());

        // This test should highlight the problem when a string field is not null-terminated.
        let mut name: Vec<u8> = vec![116, 101, 115, 116, 0]; // "test"
        let mut data: Vec<u8> = vec![Type::String.key() as u8, 116, 101, 115, 116]; // b"test" , not null-terminated
        name.append(&mut data);
        let mut parser = Parser::new(name);
        let res = parser.parse_scalar();
        assert!(res.is_err());

        let mut name: Vec<u8> = vec![116, 101, 115, 116, 0]; // "test"
        let mut data: Vec<u8> = vec![Type::String.key() as u8, 116, 101, 115, 116, 0]; // b"test\0"
        name.append(&mut data);
        let mut parser = Parser::new(name);
        let res = parser.parse_scalar();
        assert_eq!(res?, ("test".to_string(), "test".to_string().into()));

        Ok(())
    }

    #[test]
    fn test_parse_vector() -> Result<(), DmapError> {
        let mut name: Vec<u8> = vec![116, 101, 115, 116, 0]; // "test" in bytes
        let mut data: Vec<u8> = vec![Type::Char.key() as u8, 1, 0, 0, 0, 1, 0, 0, 0, 25];
        name.append(&mut data);
        let num_bytes = name.len();
        let mut parser = Parser::new(name);
        let res = parser.parse_vector(15);
        assert_eq!(res?, ("test".to_string(), array![25i8].into_dyn().into()));
        assert_eq!(parser.position(), num_bytes as u64);

        let mut name: Vec<u8> = vec![116, 101, 115, 116, 0]; // "test" in bytes
        let mut data: Vec<u8> = vec![
            Type::Char.key() as u8,
            2,
            0,
            0,
            0,
            3,
            0,
            0,
            0,
            2,
            0,
            0,
            0,
            1,
            2,
            3,
            4,
            5,
            6,
        ];
        name.append(&mut data);
        let num_bytes = name.len();
        let mut parser = Parser::new(name);
        let res = parser.parse_vector(24);
        assert_eq!(
            res?,
            (
                "test".to_string(),
                array![[1i8, 2, 3], [4, 5, 6]].into_dyn().into()
            )
        );
        assert_eq!(parser.position(), num_bytes as u64);

        parser.reset();
        let res = parser.parse_vector(3);
        assert!(res.is_err()); // size (all dimensions multiplied together) greater than record size (6 > 3)

        let mut name: Vec<u8> = vec![116, 101, 115, 116, 0]; // "test" in bytes
        let mut data: Vec<u8> = vec![
            Type::Char.key() as u8,
            100,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            1,
            2,
            3,
            4,
            5,
            6,
        ];
        name.append(&mut data);
        let mut parser = Parser::new(name);
        let res = parser.parse_vector(24);
        assert!(res.is_err()); // number of dimensions greater than record size (100 > 24)

        Ok(())
    }
}