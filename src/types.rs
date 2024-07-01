use crate::error::DmapError;
use numpy::ndarray::{Array, ArrayBase, ArrayD, Dim, IntoDimension, IxDynImpl};
use numpy::IxDyn;
use std::collections::HashMap;
use std::io::Cursor;

type Result<T> = std::result::Result<T, DmapError>;

/// Enum of the different data types supported by the DMAP format.
#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub enum Atom {
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
impl Atom {
    /// Gets the number of bytes needed to represent the data.
    pub(crate) fn get_num_bytes(&self) -> u64 {
        match self {
            Atom::CHAR { .. } => 1,
            Atom::SHORT { .. } => 2,
            Atom::INT { .. } => 4,
            Atom::FLOAT { .. } => 4,
            Atom::DOUBLE { .. } => 8,
            Atom::LONG { .. } => 8,
            Atom::UCHAR { .. } => 1,
            Atom::USHORT { .. } => 2,
            Atom::UINT { .. } => 4,
            Atom::ULONG { .. } => 8,
            _ => 0,
        }
    }

    /// Gets the data type from a numeric key.
    fn get_type_from_key(key: i8) -> Result<Atom> {
        match key {
            0 => Ok(Atom::DMAP),
            1 => Ok(Atom::CHAR(0)),
            2 => Ok(Atom::SHORT(0)),
            3 => Ok(Atom::INT(0)),
            4 => Ok(Atom::FLOAT(0.0)),
            8 => Ok(Atom::DOUBLE(0.0)),
            9 => Ok(Atom::STRING("".to_string())),
            10 => Ok(Atom::LONG(0)),
            16 => Ok(Atom::UCHAR(0)),
            17 => Ok(Atom::USHORT(0)),
            18 => Ok(Atom::UINT(0)),
            19 => Ok(Atom::ULONG(0)),
            _ => Err(DmapError::KeyError(key)),
        }
    }
}
impl std::fmt::Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Atom::DMAP => write!(f, "DMAP"),
            Atom::CHAR(x) => write!(f, "{}", x),
            Atom::SHORT(x) => write!(f, "{}", x),
            Atom::INT(x) => write!(f, "{}", x),
            Atom::FLOAT(x) => write!(f, "{}", x),
            Atom::DOUBLE(x) => write!(f, "{}", x),
            Atom::STRING(x) => write!(f, "{:?}", x),
            Atom::LONG(x) => write!(f, "{}", x),
            Atom::UCHAR(x) => write!(f, "{}", x),
            Atom::USHORT(x) => write!(f, "{}", x),
            Atom::UINT(x) => write!(f, "{}", x),
            Atom::ULONG(x) => write!(f, "{}", x),
        }
    }
}
impl From<u8> for Atom {
    fn from(value: u8) -> Self {
        Atom::UCHAR(value)
    }
}
impl From<u16> for Atom {
    fn from(value: u16) -> Self {
        Atom::USHORT(value)
    }
}
impl From<u32> for Atom {
    fn from(value: u32) -> Self {
        Atom::UINT(value)
    }
}
impl From<u64> for Atom {
    fn from(value: u64) -> Self {
        Atom::ULONG(value)
    }
}
impl From<i8> for Atom {
    fn from(value: i8) -> Self {
        Atom::CHAR(value)
    }
}
impl From<i16> for Atom {
    fn from(value: i16) -> Self {
        Atom::SHORT(value)
    }
}
impl From<i32> for Atom {
    fn from(value: i32) -> Self {
        Atom::INT(value)
    }
}
impl From<i64> for Atom {
    fn from(value: i64) -> Self {
        Atom::LONG(value)
    }
}
impl From<f32> for Atom {
    fn from(value: f32) -> Self {
        Atom::FLOAT(value)
    }
}
impl From<f64> for Atom {
    fn from(value: f64) -> Self {
        Atom::DOUBLE(value)
    }
}
impl From<String> for Atom {
    fn from(value: String) -> Self {
        Atom::STRING(value)
    }
}

#[derive(Debug, Clone)]
pub struct Molecule {
    data: ArrayD<Atom>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DmapScalar {
    pub data: Atom,
    pub mode: i8,
}

#[derive(Debug, Clone)]
pub struct DmapVector {
    pub mode: i8,
    pub dimensions: Vec<i32>,
    pub data: Vec<Atom>,
}
impl PartialEq for DmapVector {
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

#[derive(Debug, Clone)]
#[repr(C)]
pub enum GenericDmap {
    Scalar(Atom),
    Vector(Molecule),
}


/// Trait for types that can be stored in DMAP files
pub trait InDmap {
    fn get_inner_value(data: Atom) -> Result<Self>
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::CHAR(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::SHORT(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::INT(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::FLOAT(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::DOUBLE(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::STRING(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::UCHAR(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::USHORT(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::UINT(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::LONG(x) = data {
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
    fn get_inner_value(data: Atom) -> Result<Self> {
        if let Atom::ULONG(x) = data {
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
    pub(crate) fn to_bytes(&self, name: &str) -> Vec<u8> {
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
    scalars: &mut HashMap<String, DmapScalar>,
    name: &str,
) -> Result<T> {
    if let Some(DmapScalar { data, mode: _ }) = scalars.remove(name) {
        Ok(T::get_inner_value(data)?)
    } else {
        Err(DmapError::ScalarError(format!("{} not found", name)))
    }
}

/// Gets vector value from vectors HashMap and unwraps into the built-in type
// pub fn get_vector_val<T: InDmap>(
//     vectors: &mut HashMap<String, DmapVector>,
//     name: &str,
// ) -> Result<DmapVec<T>> {
//     if let Some(DmapVector {
//         data,
//         mode: _,
//         dimensions,
//     }) = vectors.remove(name)
//     {
//         let arr = data
//             .into_iter()
//             .map(|x| {
//                 T::get_inner_value(x).unwrap_or_else(|_| panic!("error getting vector {name}"))
//             })
//             .collect();
//         Ok(DmapVec {
//             data: arr,
//             dimensions,
//         })
//     } else {
//         Err(DmapError::VectorError(format!("{} not found", name)))
//     }
// }
pub fn get_vector_val<T: InDmap>(
    vectors: &mut HashMap<String, DmapVector>,
    name: &str,
) -> Result<Array<T, IxDyn>> {
    if let Some(DmapVector {
        data,
        mode: _,
        dimensions,
    }) = vectors.remove(name)
    {
        let raw_vec = data
            .into_iter()
            .map(|x| {
                T::get_inner_value(x).unwrap_or_else(|_| panic!("error getting vector {name}"))
            })
            .collect();
        let arr = Array::from_shape_vec(<Vec<i32> as TryInto<Vec<usize>>>::try_into(dimensions)?.into_dimension(), raw_vec)?;
        Ok(arr)
    } else {
        Err(DmapError::VectorError(format!("{} not found", name)))
    }
}

/// Reads a scalar starting from cursor position
pub(crate) fn parse_scalar(cursor: &mut Cursor<Vec<u8>>) -> Result<(String, DmapScalar)> {
    let mode = 6;
    let name = match read_data(cursor, Atom::STRING("".to_string()))? {
        Atom::STRING(s) => Ok(s),
        data => Err(DmapError::ScalarError(format!(
            "Invalid scalar name '{}' starting at byte {}",
            data,
            cursor.position() - data.get_num_bytes()
        ))),
    }?;
    let data_type_key = match read_data(cursor, Atom::CHAR(0))? {
        Atom::CHAR(c) => Ok(c),
        data => Err(DmapError::ScalarError(format!(
            "Invalid data type '{}' for field '{}', byte {}",
            data,
            name,
            cursor.position() - data.get_num_bytes()
        ))),
    }?;

    let data_type = Atom::get_type_from_key(data_type_key)?;
    let data = match data_type {
        Atom::DMAP => Err(DmapError::ScalarError(format!(
            "Scalar field '{}' at byte {} cannot have DMAP data type",
            name,
            cursor.position() - 1
        )))?,
        _ => read_data(cursor, data_type)?,
    };

    Ok((name, DmapScalar { data, mode }))
}

/// Reads a vector starting from cursor position
pub(crate) fn parse_vector(
    cursor: &mut Cursor<Vec<u8>>,
    record_size: i32,
) -> Result<(String, DmapVector)> {
    let mode = 7;
    let name = match read_data(cursor, Atom::STRING("".to_string()))? {
        Atom::STRING(s) => Ok(s),
        data => Err(DmapError::VectorError(format!(
            "Invalid vector name '{}' starting at byte {}",
            data,
            cursor.position() - data.get_num_bytes()
        ))),
    }?;
    let data_type_key = match read_data(cursor, Atom::CHAR(0))? {
        Atom::CHAR(c) => Ok(c),
        data => Err(DmapError::VectorError(format!(
            "Invalid data type for '{}', byte {}",
            name,
            cursor.position() - data.get_num_bytes()
        ))),
    }?;

    let data_type = Atom::get_type_from_key(data_type_key)?;
    if let Atom::DMAP = data_type {
        Err(DmapError::VectorError(format!(
            "Vector field '{}' at byte {} cannot have DMAP data type",
            name,
            cursor.position()
        )))?
    }

    let vector_dimension = match read_data(cursor, Atom::INT(0))? {
        Atom::INT(i) => Ok(i),
        data => Err(DmapError::VectorError(format!(
            "Invalid vector dimension {} for field '{}', byte {}",
            data,
            name,
            cursor.position() - data.get_num_bytes()
        ))),
    }?;

    if vector_dimension > record_size {
        return Err(DmapError::VectorError(format!(
            "Parsed number of vector dimensions {} for field '{}' at byte {} are larger \
            than record size {}",
            vector_dimension,
            name,
            cursor.position() - Atom::INT(0).get_num_bytes(),
            record_size
        )));
    } else if vector_dimension <= 0 {
        return Err(DmapError::VectorError(format!(
            "Parsed number of vector dimensions {} for field '{}' at byte {} are zero or \
            negative",
            vector_dimension,
            name,
            cursor.position() - Atom::INT(0).get_num_bytes()
        )));
    }

    let mut dimensions: Vec<i32> = vec![];
    let mut total_elements = 1;
    for _ in 0..vector_dimension {
        let dim = match read_data(cursor, Atom::INT(0))? {
            Atom::INT(val) => Ok(val),
            data => Err(DmapError::VectorError(format!(
                "Vector dimension at byte {} could not be parsed for field '{}'",
                cursor.position() - data.get_num_bytes(),
                name
            ))),
        }?;
        if dim <= 0 && name != "slist" {
            return Err(DmapError::VectorError(format!(
                "Vector dimension {} at byte {} is zero or negative for field '{}'",
                dim,
                cursor.position() - Atom::INT(0).get_num_bytes(),
                name
            )));
        } else if dim > record_size {
            return Err(DmapError::VectorError(format!(
                "Vector dimension {} at byte {} for field '{}' exceeds record size {} ",
                dim,
                cursor.position() - Atom::INT(0).get_num_bytes(),
                name,
                record_size,
            )));
        }
        dimensions.push(dim);
        total_elements *= dim;
    }
    dimensions = dimensions.into_iter().rev().collect(); // reverse the dimensions, stored in column-major order

    if total_elements * data_type.get_num_bytes() as i32 > record_size {
        return Err(DmapError::VectorError(format!(
            "Vector size {} starting at byte {} for field '{}' exceeds record size {}",
            total_elements * data_type.get_num_bytes() as i32,
            cursor.position() - vector_dimension as u64 * Atom::INT(0).get_num_bytes(),
            name,
            record_size
        )));
    }

    let mut data = vec![];
    for _ in 0..total_elements {
        data.push(read_data(cursor, data_type.clone())?);
    }
    Ok((
        name,
        DmapVector {
            mode,
            dimensions,
            data,
        },
    ))
}

/// Reads a singular value of type data_type starting from cursor position
pub(crate) fn read_data(cursor: &mut Cursor<Vec<u8>>, data_type: Atom) -> Result<Atom> {
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
        Atom::DMAP => Err(DmapError::CorruptDmapError("Data type 'DMAP' unreadable"))?,
        Atom::UCHAR { .. } => Atom::UCHAR(data[0]),
        Atom::CHAR { .. } => {
            Atom::CHAR(bytemuck::try_pod_read_unaligned::<i8>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "i8",
                }
            })?)
        }
        Atom::SHORT { .. } => {
            Atom::SHORT(bytemuck::try_pod_read_unaligned::<i16>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "i16",
                }
            })?)
        }
        Atom::USHORT { .. } => {
            Atom::USHORT(bytemuck::try_pod_read_unaligned::<u16>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "u16",
                }
            })?)
        }
        Atom::INT { .. } => {
            Atom::INT(bytemuck::try_pod_read_unaligned::<i32>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "i32",
                }
            })?)
        }
        Atom::UINT { .. } => {
            Atom::UINT(bytemuck::try_pod_read_unaligned::<u32>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "u32",
                }
            })?)
        }
        Atom::LONG { .. } => {
            Atom::LONG(bytemuck::try_pod_read_unaligned::<i64>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "i64",
                }
            })?)
        }
        Atom::ULONG { .. } => {
            Atom::ULONG(bytemuck::try_pod_read_unaligned::<u64>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "u64",
                }
            })?)
        }
        Atom::FLOAT { .. } => {
            Atom::FLOAT(bytemuck::try_pod_read_unaligned::<f32>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "f32",
                }
            })?)
        }
        Atom::DOUBLE { .. } => {
            Atom::DOUBLE(bytemuck::try_pod_read_unaligned::<f64>(data).map_err(|_| {
                DmapError::CastError {
                    position,
                    kind: "f64",
                }
            })?)
        }
        Atom::STRING { .. } => {
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
            Atom::STRING(data)
        }
    };
    cursor.set_position({ position + data_size } as u64);

    Ok(parsed_data)
}


// #[derive(Debug, Default)]
// pub struct DmapDifference {
//     pub unique_scalars_1: Vec<String>,
//     pub unique_scalars_2: Vec<String>,
//     pub unique_vectors_1: Vec<String>,
//     pub unique_vectors_2: Vec<String>,
//     pub different_scalars: HashMap<String, (DmapScalar, DmapScalar)>,
//     pub different_vectors: HashMap<String, (DmapVector, DmapVector)>,
// }
// impl std::fmt::Display for DmapDifference {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let mut repr = String::new();
//         if self.is_empty() {
//             return write!(f, "No differences{}", repr);
//         }
//         if !self.unique_scalars_1.is_empty() {
//             repr.push_str("Unique scalar fields in left record: ");
//             repr.push_str(&format!("{:?}\n", self.unique_scalars_1));
//         }
//         if !self.unique_scalars_2.is_empty() {
//             repr.push_str("Unique scalar fields in right record: ");
//             repr.push_str(&format!("{:?}\n", self.unique_scalars_2));
//         }
//         if !self.unique_vectors_1.is_empty() {
//             repr.push_str("Unique vector fields in left record: ");
//             repr.push_str(&format!("{:?}\n", self.unique_vectors_1));
//         }
//         if !self.unique_vectors_2.is_empty() {
//             repr.push_str("Unique vector fields in right record: ");
//             repr.push_str(&format!("{:?}\n", self.unique_vectors_2));
//         }
//         if !self.different_scalars.is_empty() {
//             repr.push_str("Different scalar values\n=======================\n");
//             for (key, (left_scal, right_scal)) in self.different_scalars.iter() {
//                 repr.push_str(&format!(
//                     "{}:\n\t{:?}\n\t{:?}\n",
//                     key, left_scal, right_scal
//                 ));
//             }
//         }
//         if !self.different_vectors.is_empty() {
//             repr.push_str("Different vector values\n=======================\n");
//             for (key, (left_vec, right_vec)) in self.different_vectors.iter() {
//                 repr.push_str(&format!("{}:\n\t", key));
//                 if left_vec.data.len() > 10 {
//                     repr.push_str(&format!(
//                         "RawDmapVector {{ mode: {}, dimensions: {:?}, data: ... }}",
//                         left_vec.mode, left_vec.dimensions
//                     ));
//                 } else {
//                     repr.push_str(&format!("{:?}\n\t", left_vec));
//                 }
//                 if right_vec.data.len() > 10 {
//                     repr.push_str(&format!(
//                         "RawDmapVector {{ mode: {}, dimensions: {:?}, data: ... }}",
//                         right_vec.mode, right_vec.dimensions
//                     ));
//                 } else {
//                     repr.push_str(&format!("{:?}\n", right_vec));
//                 }
//             }
//         }
//         write!(f, "{}", repr)
//     }
// }
// impl DmapDifference {
//     /// Returns if the DmapDifference is empty, i.e. there were no differences
//     pub fn is_empty(&self) -> bool {
//         self.unique_scalars_1.len() == 0
//             && self.unique_scalars_2.len() == 0
//             && self.unique_vectors_1.len() == 0
//             && self.unique_vectors_2.len() == 0
//             && self.different_scalars.is_empty()
//             && self.different_vectors.is_empty()
//     }
// }
