pub mod formats;

use std::collections::HashMap;
use std::io::Cursor;

type Result<T> = std::result::Result<T, DmapError>;

#[derive(Debug)]
pub enum DmapError {
    /// Represents an empty source.
    EmptySource,

    /// Represents a failure to read from input.
    ReadError { source: std::io::Error },

    /// Represents invalid conditions when reading from input.
    CorruptDmapError(&'static str),

    /// Represents a failure to intepret data from input.
    CastError { position: usize, kind: &'static str },

    /// Represents all other cases of `std::io::Error`.
    IOError(std::io::Error),

    /// Represents an attempt to extract the wrong type of data.
    ExtractionError,

    /// Represents an invalid key for a DMAP type.
    KeyError(i8),

    /// Represents a failure to read a DMAP record.
    RecordError(String),

    /// Represents an invalid scalar field.
    ScalarError(String),

    /// Represents an invalid vector field.
    VectorError(String),
}
impl std::error::Error for DmapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            DmapError::ReadError { ref source } => Some(source),
            _ => None,
        }
    }
}

impl std::fmt::Display for DmapError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            DmapError::EmptySource => {
                write!(f, "Source contains no data")
            }
            DmapError::ReadError { .. } => {
                write!(f, "Read error")
            }
            DmapError::CorruptDmapError(s) => {
                write!(f, "{s}")
            }
            DmapError::CastError {
                ref position,
                ref kind,
            } => {
                write!(f, "Unable to interpet value at {position:?} as {kind:?}")
            }
            DmapError::IOError(ref err) => err.fmt(f),
            DmapError::ExtractionError => {
                write!(f, "Extraction error")
            }
            DmapError::KeyError(ref key) => {
                write!(f, "Invalid key '{:?}'", key)
            }
            DmapError::RecordError(ref s) => {
                write!(f, "{s:?}")
            }
            DmapError::ScalarError(ref s) => {
                write!(f, "{s:?}")
            }
            DmapError::VectorError(ref s) => {
                write!(f, "{s:?}")
            }
        }
    }
}

impl From<std::io::Error> for DmapError {
    fn from(err: std::io::Error) -> Self {
        DmapError::IOError(err)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub enum DmapType {
    DMAP,
    CHAR(i8),
    SHORT(i16),
    INT(i32),
    FLOAT(f32),
    DOUBLE(f64),
    STRING(String),
    LONG(i64),
    UCHAR(u8),
    USHORT(u16),
    UINT(u32),
    ULONG(u64),
}
impl DmapType {
    /// Gets the number of bytes needed to represent the data.
    fn get_num_bytes(&self) -> u64 {
        match self {
            DmapType::CHAR { .. } => 1,
            DmapType::SHORT { .. } => 2,
            DmapType::INT { .. } => 4,
            DmapType::FLOAT { .. } => 4,
            DmapType::DOUBLE { .. } => 8,
            DmapType::LONG { .. } => 8,
            DmapType::UCHAR { .. } => 1,
            DmapType::USHORT { .. } => 2,
            DmapType::UINT { .. } => 4,
            DmapType::ULONG { .. } => 8,
            _ => 0,
        }
    }

    /// Gets the data type from a numeric key.
    fn get_type_from_key(key: i8) -> Result<DmapType> {
        match key {
            0 => Ok(DmapType::DMAP),
            1 => Ok(DmapType::CHAR(0)),
            2 => Ok(DmapType::SHORT(0)),
            3 => Ok(DmapType::INT(0)),
            4 => Ok(DmapType::FLOAT(0.0)),
            8 => Ok(DmapType::DOUBLE(0.0)),
            9 => Ok(DmapType::STRING("".to_string())),
            10 => Ok(DmapType::LONG(0)),
            16 => Ok(DmapType::UCHAR(0)),
            17 => Ok(DmapType::USHORT(0)),
            18 => Ok(DmapType::UINT(0)),
            19 => Ok(DmapType::ULONG(0)),
            _ => Err(DmapError::KeyError(key)),
        }
    }
}
impl std::fmt::Display for DmapType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DmapType::DMAP => write!(f, "DMAP"),
            DmapType::CHAR(x) => write!(f, "{}", x),
            DmapType::SHORT(x) => write!(f, "{}", x),
            DmapType::INT(x) => write!(f, "{}", x),
            DmapType::FLOAT(x) => write!(f, "{}", x),
            DmapType::DOUBLE(x) => write!(f, "{}", x),
            DmapType::STRING(x) => write!(f, "{:?}", x),
            DmapType::LONG(x) => write!(f, "{}", x),
            DmapType::UCHAR(x) => write!(f, "{}", x),
            DmapType::USHORT(x) => write!(f, "{}", x),
            DmapType::UINT(x) => write!(f, "{}", x),
            DmapType::ULONG(x) => write!(f, "{}", x),
        }
    }
}
impl From<u8> for DmapType {
    fn from(value: u8) -> Self {
        DmapType::UCHAR(value)
    }
}
impl From<u16> for DmapType {
    fn from(value: u16) -> Self {
        DmapType::USHORT(value)
    }
}
impl From<u32> for DmapType {
    fn from(value: u32) -> Self {
        DmapType::UINT(value)
    }
}
impl From<u64> for DmapType {
    fn from(value: u64) -> Self {
        DmapType::ULONG(value)
    }
}
impl From<i8> for DmapType {
    fn from(value: i8) -> Self {
        DmapType::CHAR(value)
    }
}
impl From<i16> for DmapType {
    fn from(value: i16) -> Self {
        DmapType::SHORT(value)
    }
}
impl From<i32> for DmapType {
    fn from(value: i32) -> Self {
        DmapType::INT(value)
    }
}
impl From<i64> for DmapType {
    fn from(value: i64) -> Self {
        DmapType::LONG(value)
    }
}
impl From<f32> for DmapType {
    fn from(value: f32) -> Self {
        DmapType::FLOAT(value)
    }
}
impl From<f64> for DmapType {
    fn from(value: f64) -> Self {
        DmapType::DOUBLE(value)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct RawDmapScalar {
    pub data: DmapType,
    pub mode: i8,
}

#[derive(Debug, Clone)]
pub struct RawDmapVector {
    pub mode: i8,
    pub dimensions: Vec<i32>,
    pub data: Vec<DmapType>,
}
impl PartialEq for RawDmapVector {
    fn eq(&self, other: &Self) -> bool {
        let mut equal = self.mode == other.mode;
        for (d1, d2) in self.dimensions.iter().zip(other.dimensions.iter()) {
            equal = equal && d1 == d2;
        }
        for (a1, a2) in self.data.iter().zip(other.data.iter()) {
            equal = equal && a1 == a2;
        }
        equal
    }
}

#[derive(Debug, Default)]
pub struct DmapDifference {
    pub unique_scalars_1: Vec<String>,
    pub unique_scalars_2: Vec<String>,
    pub unique_vectors_1: Vec<String>,
    pub unique_vectors_2: Vec<String>,
    pub different_scalars: HashMap<String, (RawDmapScalar, RawDmapScalar)>,
    pub different_vectors: HashMap<String, (RawDmapVector, RawDmapVector)>,
}
impl std::fmt::Display for DmapDifference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut repr = String::new();
        if self.is_empty() {
            return write!(f, "No differences{}", repr);
        }
        if !self.unique_scalars_1.is_empty() {
            repr.push_str("Unique scalar fields in left record: ");
            repr.push_str(&format!("{:?}\n", self.unique_scalars_1));
        }
        if !self.unique_scalars_2.is_empty() {
            repr.push_str("Unique scalar fields in right record: ");
            repr.push_str(&format!("{:?}\n", self.unique_scalars_2));
        }
        if !self.unique_vectors_1.is_empty() {
            repr.push_str("Unique vector fields in left record: ");
            repr.push_str(&format!("{:?}\n", self.unique_vectors_1));
        }
        if !self.unique_vectors_2.is_empty() {
            repr.push_str("Unique vector fields in right record: ");
            repr.push_str(&format!("{:?}\n", self.unique_vectors_2));
        }
        if !self.different_scalars.is_empty() {
            repr.push_str("Different scalar values\n=======================\n");
            for (key, (left_scal, right_scal)) in self.different_scalars.iter() {
                repr.push_str(&format!(
                    "{}:\n\t{:?}\n\t{:?}\n",
                    key, left_scal, right_scal
                ));
            }
        }
        if !self.different_vectors.is_empty() {
            repr.push_str("Different vector values\n=======================\n");
            for (key, (left_vec, right_vec)) in self.different_vectors.iter() {
                repr.push_str(&format!("{}:\n\t", key));
                if left_vec.data.len() > 10 {
                    repr.push_str(&format!(
                        "RawDmapVector {{ mode: {}, dimensions: {:?}, data: ... }}",
                        left_vec.mode, left_vec.dimensions
                    ));
                } else {
                    repr.push_str(&format!("{:?}\n\t", left_vec));
                }
                if right_vec.data.len() > 10 {
                    repr.push_str(&format!(
                        "RawDmapVector {{ mode: {}, dimensions: {:?}, data: ... }}",
                        right_vec.mode, right_vec.dimensions
                    ));
                } else {
                    repr.push_str(&format!("{:?}\n", right_vec));
                }
            }
        }
        write!(f, "{}", repr)
    }
}
impl DmapDifference {
    /// Returns if the DmapDifference is empty, i.e. there were no differences
    pub fn is_empty(&self) -> bool {
        self.unique_scalars_1.len() == 0
            && self.unique_scalars_2.len() == 0
            && self.unique_vectors_1.len() == 0
            && self.unique_vectors_2.len() == 0
            && self.different_scalars.is_empty()
            && self.different_vectors.is_empty()
    }
}

/// Trait for types that can be stored in DMAP files
pub trait InDmap {
    fn get_inner_value(data: DmapType) -> Result<Self>
    where
        Self: Sized;
    fn to_bytes(&self, name: &str) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.extend(name.to_string().data_to_bytes());
        // bytes.push(0);  // null-terminate String
        bytes.push(Self::get_dmap_key());
        bytes.extend(self.data_to_bytes());
        bytes
    }
    fn get_dmap_key() -> u8;
    fn data_to_bytes(&self) -> Vec<u8>;
}
impl InDmap for i8 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::CHAR(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        1
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for i16 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::SHORT(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        2
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for i32 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::INT(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        3
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for f32 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::FLOAT(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        4
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for f64 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::DOUBLE(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        8
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for String {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::STRING(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        9
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.as_bytes().to_vec();
        bytes.push(0); // null-terminate
        bytes
    }
}
impl InDmap for u8 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::UCHAR(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        16
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for u16 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::USHORT(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        17
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for u32 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::UINT(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        18
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for i64 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::LONG(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        10
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
impl InDmap for u64 {
    fn get_inner_value(data: DmapType) -> Result<Self> {
        if let DmapType::ULONG(x) = data {
            Ok(x)
        } else {
            Err(DmapError::ExtractionError)
        }
    }
    fn get_dmap_key() -> u8 {
        19
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct DmapVec<T>
where
    T: InDmap,
{
    pub dimensions: Vec<i32>,
    pub data: Vec<T>,
}
impl<T> DmapVec<T>
where
    T: InDmap,
{
    fn to_bytes(&self, name: &str) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![];
        bytes.extend(name.to_string().data_to_bytes());
        bytes.push(T::get_dmap_key());
        bytes.extend((self.dimensions.len() as i32).data_to_bytes());
        let reversed_dims: Vec<i32> = self.dimensions.clone().into_iter().rev().collect(); // reverse back into column-major order
        for dim in reversed_dims {
            bytes.extend(dim.data_to_bytes());
        }
        for val in self.data.iter() {
            bytes.extend(val.data_to_bytes());
        }
        bytes
    }
}

/// Gets scalar value from scalars HashMap and unwraps into the built-in type
pub fn get_scalar_val<T: InDmap>(
    scalars: &mut HashMap<String, RawDmapScalar>,
    name: &str,
) -> Result<T> {
    if let Some(RawDmapScalar { data, mode: _ }) = scalars.remove(name) {
        Ok(T::get_inner_value(data)?)
    } else {
        Err(DmapError::ScalarError(format!("{} not found", name)))
    }
}

/// Gets vector value from vectors HashMap and unwraps into the built-in type
pub fn get_vector_val<T: InDmap>(
    vectors: &mut HashMap<String, RawDmapVector>,
    name: &str,
) -> Result<DmapVec<T>> {
    if let Some(RawDmapVector {
        data,
        mode: _,
        dimensions,
    }) = vectors.remove(name)
    {
        let arr = data
            .into_iter()
            .map(|x| {
                T::get_inner_value(x).unwrap_or_else(|_| panic!("error getting vector {name}"))
            })
            .collect();
        Ok(DmapVec {
            data: arr,
            dimensions,
        })
    } else {
        Err(DmapError::VectorError(format!("{} not found", name)))
    }
}

/// Reads a scalar starting from cursor position
fn parse_scalar(cursor: &mut Cursor<Vec<u8>>) -> Result<(String, RawDmapScalar)> {
    let mode = 6;
    let name = match read_data(cursor, DmapType::STRING("".to_string()))? {
        DmapType::STRING(s) => Ok(s),
        _ => Err(DmapError::ScalarError(format!("Invalid scalar name"))),
    }?;
    let data_type_key = match read_data(cursor, DmapType::CHAR(0))? {
        DmapType::CHAR(c) => Ok(c),
        _ => Err(DmapError::ScalarError(format!("Invalid data type"))),
    }?;

    let data_type = DmapType::get_type_from_key(data_type_key)?;
    let data = match data_type {
        DmapType::DMAP => Err(DmapError::ScalarError(format!(
            "Scalar field cannot be DMAP data type"
        )))?,
        _ => read_data(cursor, data_type)?,
    };

    Ok((name, RawDmapScalar { data, mode }))
}

/// Reads a vector starting from cursor position
fn parse_vector(cursor: &mut Cursor<Vec<u8>>, record_size: i32) -> Result<(String, RawDmapVector)> {
    let mode = 7;
    let name = match read_data(cursor, DmapType::STRING("".to_string()))? {
        DmapType::STRING(s) => Ok(s),
        _ => Err(DmapError::VectorError(format!("Invalid vector name"))),
    }?;
    let data_type_key = match read_data(cursor, DmapType::CHAR(0))? {
        DmapType::CHAR(c) => Ok(c),
        _ => Err(DmapError::VectorError(format!("Invalid data type"))),
    }?;

    let data_type = DmapType::get_type_from_key(data_type_key)?;
    if let DmapType::DMAP = data_type {
        Err(DmapError::VectorError(format!(
            "Vector field cannot be DMAP data type"
        )))?
    }

    let vector_dimension = match read_data(cursor, DmapType::INT(0))? {
        DmapType::INT(i) => Ok(i),
        _ => Err(DmapError::VectorError(format!("Invalid vector dimension"))),
    }?;

    if vector_dimension > record_size {
        return Err(DmapError::VectorError(format!(
            "Parsed number of vector dimensions are larger \
            than record size"
        )));
    } else if vector_dimension <= 0 {
        return Err(DmapError::VectorError(format!(
            "Parsed number of vector dimensions are zero or \
            negative"
        )));
    }

    let mut dimensions: Vec<i32> = vec![];
    let mut total_elements = 1;
    for _ in 0..vector_dimension {
        let dim = match read_data(cursor, DmapType::INT(0))? {
            DmapType::INT(val) => Ok(val),
            _ => Err(DmapError::VectorError(format!(
                "Vector dimensions could not be parsed"
            ))),
        }?;
        if dim <= 0 && name != "slist" {
            return Err(DmapError::VectorError(format!(
                "Vector dimension is zero or negative"
            )));
        } else if dim > record_size {
            return Err(DmapError::VectorError(format!(
                "Vector dimension exceeds record size"
            )));
        }
        dimensions.push(dim);
        total_elements *= dim;
    }
    dimensions = dimensions.into_iter().rev().collect(); // reverse the dimensions, stored in column-major order

    if total_elements * data_type.get_num_bytes() as i32 > record_size {
        return Err(DmapError::VectorError(format!(
            "Vector size exceeds record size"
        )));
    }

    let mut data = vec![];
    for _ in 0..total_elements {
        data.push(read_data(cursor, data_type.clone())?);
    }
    Ok((
        name,
        RawDmapVector {
            mode,
            dimensions,
            data,
        },
    ))
}

/// Reads a singular value of type data_type starting from cursor position
fn read_data(cursor: &mut Cursor<Vec<u8>>, data_type: DmapType) -> Result<DmapType> {
    let position = cursor.position() as usize;
    let stream = cursor.get_mut();

    if position > stream.len() {
        return Err(DmapError::CorruptDmapError("Cursor extends out of buffer"));
    }
    if stream.len() - position < data_type.get_num_bytes() as usize {
        return Err(DmapError::CorruptDmapError(
            "Byte offsets into buffer are not properly aligned",
        ));
    }

    let mut data_size = data_type.get_num_bytes() as usize;
    let data: &[u8] = &stream[position..position + data_size];
    let parsed_data = match data_type {
        DmapType::DMAP => Err(DmapError::CorruptDmapError("Data type 'DMAP' unreadable"))?,
        DmapType::UCHAR { .. } => DmapType::UCHAR(data[0]),
        DmapType::CHAR { .. } => {
            DmapType::CHAR(bytemuck::try_pod_read_unaligned::<i8>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "i8",
                }
            })?)
        }
        DmapType::SHORT { .. } => {
            DmapType::SHORT(bytemuck::try_pod_read_unaligned::<i16>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "i16",
                }
            })?)
        }
        DmapType::USHORT { .. } => {
            DmapType::USHORT(bytemuck::try_pod_read_unaligned::<u16>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "u16",
                }
            })?)
        }
        DmapType::INT { .. } => {
            DmapType::INT(bytemuck::try_pod_read_unaligned::<i32>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "i32",
                }
            })?)
        }
        DmapType::UINT { .. } => {
            DmapType::UINT(bytemuck::try_pod_read_unaligned::<u32>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "u32",
                }
            })?)
        }
        DmapType::LONG { .. } => {
            DmapType::LONG(bytemuck::try_pod_read_unaligned::<i64>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "i64",
                }
            })?)
        }
        DmapType::ULONG { .. } => {
            DmapType::ULONG(bytemuck::try_pod_read_unaligned::<u64>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "u64",
                }
            })?)
        }
        DmapType::FLOAT { .. } => {
            DmapType::FLOAT(bytemuck::try_pod_read_unaligned::<f32>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "f32",
                }
            })?)
        }
        DmapType::DOUBLE { .. } => {
            DmapType::DOUBLE(bytemuck::try_pod_read_unaligned::<f64>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "f64",
                }
            })?)
        }
        DmapType::STRING { .. } => {
            let mut byte_counter = 0;
            while stream[position + byte_counter] != 0 {
                byte_counter += 1;
                if position + byte_counter >= stream.len() {
                    return Err(DmapError::CorruptDmapError(
                        "String is improperly terminated",
                    ));
                }
            }
            let data = String::from_utf8(stream[position..position + byte_counter].to_owned())
                .map_err(|_| DmapError::CastError {
                    position,
                    kind: "String",
                })?;
            data_size = byte_counter + 1;
            DmapType::STRING(data)
        }
    };
    cursor.set_position({ position + data_size } as u64);

    Ok(parsed_data)
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn record_to_bytes() {
    //     let scalar = RawDmapScalar::new(DmapType::CHAR(10));
    //     let mut scalars = HashMap::new();
    //     scalars.insert("scal".to_string(), scalar);
    //
    //     let dimensions = vec![3];
    //     let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
    //     let vector = RawDmapVector::new(dimensions, data);
    //     let mut vectors = HashMap::new();
    //     vectors.insert("arr".to_string(), vector);
    //
    //     let rec = RawDmapRecord {
    //         num_scalars: 1,
    //         num_vectors: 1,
    //         scalar_list: vec!["scal".to_string()],
    //         vector_list: vec!["arr".to_string()],
    //         scalars,
    //         vectors,
    //     };
    //
    //     assert_eq!(
    //         rec.to_bytes(),
    //         vec![
    //             1, 0, 1, 0, 39, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 115, 99, 97, 108, 0, 1, 10, 97,
    //             114, 114, 0, 1, 1, 0, 0, 0, 3, 0, 0, 0, 0, 1, 2
    //         ]
    //     )
    // }

    // #[test]
    // fn record_get_values() {
    //     let scalar = RawDmapScalar::new(DmapType::CHAR(10));
    //     let mut scalars = HashMap::new();
    //     scalars.insert("scal".to_string(), scalar);
    //
    //     let dimensions = vec![3];
    //     let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
    //     let vector = RawDmapVector::new(dimensions, data);
    //     let mut vectors = HashMap::new();
    //     vectors.insert("arr".to_string(), vector);
    //
    //     let rec = RawDmapRecord {
    //         num_scalars: 1,
    //         num_vectors: 1,
    //         scalar_list: vec!["scal".to_string()],
    //         vector_list: vec!["arr".to_string()],
    //         scalars,
    //         vectors,
    //     };
    //
    //     assert_eq!(
    //         10,
    //         get_scalar_val::<i8>(&mut rec.scalars.clone(), "scal")
    //             .expect("Unable to recover scalar")
    //     );
    //     // assert_eq!(
    //     //     vec![0, 1, 2],
    //     //     get_vector_val::<i8>(&rec.vectors, "arr").expect("Unable to recover vector")
    //     // );
    // }

    // #[test]
    // fn same_record_no_differences() {
    //     let scalar = RawDmapScalar::new(DmapType::CHAR(10));
    //     let mut scalars = HashMap::new();
    //     scalars.insert("scal".to_string(), scalar);
    //
    //     let dimensions = vec![3];
    //     let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
    //     let vector = RawDmapVector::new(dimensions, data);
    //     let mut vectors = HashMap::new();
    //     vectors.insert("arr".to_string(), vector);
    //
    //     let rec1 = RawDmapRecord {
    //         num_scalars: 1,
    //         num_vectors: 1,
    //         scalar_list: vec!["scal".to_string()],
    //         vector_list: vec!["arr".to_string()],
    //         scalars: scalars.clone(),
    //         vectors: vectors.clone(),
    //     };
    //
    //     let rec2 = RawDmapRecord {
    //         num_scalars: 1,
    //         num_vectors: 1,
    //         scalar_list: vec!["scal".to_string()],
    //         vector_list: vec!["arr".to_string()],
    //         scalars,
    //         vectors,
    //     };
    //
    //     let differences = rec1.find_differences(&rec2);
    //     assert!(differences.is_empty());
    // }

    // #[test]
    // fn record_differences() {
    //     let scalar = RawDmapScalar::new(DmapType::CHAR(10));
    //     let mut scalars = HashMap::new();
    //     scalars.insert("scal".to_string(), scalar);
    //
    //     let dimensions = vec![3];
    //     let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
    //     let vector = RawDmapVector::new(dimensions, data);
    //     let mut vectors = HashMap::new();
    //     vectors.insert("arr".to_string(), vector);
    //
    //     let rec1 = RawDmapRecord {
    //         num_scalars: 1,
    //         num_vectors: 1,
    //         scalar_list: vec!["scal".to_string()],
    //         vector_list: vec!["arr".to_string()],
    //         scalars: scalars.clone(),
    //         vectors: vectors.clone(),
    //     };
    //
    //     let dimensions = vec![12];
    //     let data = vec![
    //         DmapType::INT(0),
    //         DmapType::INT(1),
    //         DmapType::INT(2),
    //         DmapType::INT(0),
    //         DmapType::INT(1),
    //         DmapType::INT(2),
    //         DmapType::INT(0),
    //         DmapType::INT(1),
    //         DmapType::INT(2),
    //         DmapType::INT(0),
    //         DmapType::INT(1),
    //         DmapType::INT(2),
    //     ];
    //     let vector = RawDmapVector::new(dimensions, data);
    //     scalars.insert(
    //         "scal".to_string(),
    //         RawDmapScalar::new(DmapType::ULONG(123456)),
    //     );
    //     vectors.insert("arr".to_string(), vector);
    //     let rec2 = RawDmapRecord {
    //         num_scalars: 1,
    //         num_vectors: 1,
    //         scalar_list: vec!["scal".to_string()],
    //         vector_list: vec!["arr".to_string()],
    //         scalars,
    //         vectors,
    //     };
    //
    //     let differences = rec1.find_differences(&rec2);
    //     // println!("{}", differences);
    //     assert_eq!(false, differences.is_empty());
    // }
}
