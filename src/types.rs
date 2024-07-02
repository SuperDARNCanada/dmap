use crate::error::DmapError;
use indexmap::IndexMap;
use numpy::ndarray::{Array1, Array2, Array3};
use numpy::array::PyArray;
use std::io::Cursor;
use pyo3::{IntoPy, PyObject, Python};

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
impl IntoPy<PyObject> for Atom {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Atom::DMAP => PyObject::from("DMAP".to_string()),
            Atom::CHAR(x) => PyObject::from(x),
            Atom::SHORT(x) => PyObject::from(x),
            Atom::INT(x) => PyObject::from(x),
            Atom::FLOAT(x) => PyObject::from(x),
            Atom::DOUBLE(x) => PyObject::from(x),
            Atom::STRING(x) => PyObject::from(x),
            Atom::LONG(x) => PyObject::from(x),
            Atom::UCHAR(x) => PyObject::from(x),
            Atom::USHORT(x) => PyObject::from(x),
            Atom::UINT(x) => PyObject::from(x),
            Atom::ULONG(x) => PyObject::from(x),
        }
    }
}
impl From<u8> for Atom {
    fn from(value: u8) -> Self {
        Atom::UCHAR(value)
    }
}
impl TryFrom<Atom> for u8 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<u8, Self::Error> {
        match value {
            Atom::UCHAR(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not UCHAR".to_string(),
            )),
        }
    }
}
impl From<u16> for Atom {
    fn from(value: u16) -> Self {
        Atom::USHORT(value)
    }
}
impl TryFrom<Atom> for u16 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<u16, Self::Error> {
        match value {
            Atom::USHORT(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not USHORT".to_string(),
            )),
        }
    }
}
impl From<u32> for Atom {
    fn from(value: u32) -> Self {
        Atom::UINT(value)
    }
}
impl TryFrom<Atom> for u32 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<u32, Self::Error> {
        match value {
            Atom::UINT(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not UINT".to_string(),
            )),
        }
    }
}
impl From<u64> for Atom {
    fn from(value: u64) -> Self {
        Atom::ULONG(value)
    }
}
impl TryFrom<Atom> for u64 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<u64, Self::Error> {
        match value {
            Atom::ULONG(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not ULONG".to_string(),
            )),
        }
    }
}
impl From<i8> for Atom {
    fn from(value: i8) -> Self {
        Atom::CHAR(value)
    }
}
impl TryFrom<Atom> for i8 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<i8, Self::Error> {
        match value {
            Atom::CHAR(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not CHAR".to_string(),
            )),
        }
    }
}
impl From<i16> for Atom {
    fn from(value: i16) -> Self {
        Atom::SHORT(value)
    }
}
impl TryFrom<Atom> for i16 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<i16, Self::Error> {
        match value {
            Atom::SHORT(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not SHORT".to_string(),
            )),
        }
    }
}
impl From<i32> for Atom {
    fn from(value: i32) -> Self {
        Atom::INT(value)
    }
}
impl TryFrom<Atom> for i32 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<i32, Self::Error> {
        match value {
            Atom::INT(x) => Ok(x),
            _ => Err(DmapError::ScalarError("Scalar type is not INT".to_string())),
        }
    }
}
impl From<i64> for Atom {
    fn from(value: i64) -> Self {
        Atom::LONG(value)
    }
}
impl TryFrom<Atom> for i64 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<i64, Self::Error> {
        match value {
            Atom::LONG(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not LONG".to_string(),
            )),
        }
    }
}
impl From<f32> for Atom {
    fn from(value: f32) -> Self {
        Atom::FLOAT(value)
    }
}
impl TryFrom<Atom> for f32 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<f32, Self::Error> {
        match value {
            Atom::FLOAT(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not FLOAT".to_string(),
            )),
        }
    }
}
impl From<f64> for Atom {
    fn from(value: f64) -> Self {
        Atom::DOUBLE(value)
    }
}
impl TryFrom<Atom> for f64 {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<f64, Self::Error> {
        match value {
            Atom::DOUBLE(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not DOUBLE".to_string(),
            )),
        }
    }
}
impl From<String> for Atom {
    fn from(value: String) -> Self {
        Atom::STRING(value)
    }
}
impl TryFrom<Atom> for String {
    type Error = DmapError;
    fn try_from(value: Atom) -> std::result::Result<String, Self::Error> {
        match value {
            Atom::STRING(x) => Ok(x),
            _ => Err(DmapError::ScalarError(
                "Scalar type is not STRING".to_string(),
            )),
        }
    }
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
    Vec1D(Array1<Atom>),
    Vec2D(Array2<Atom>),
    Vec3D(Array3<Atom>),
}
impl GenericDmap {
    pub fn to_bytes(&self, name: &str) -> Vec<u8> {
        todo!()
    }
}
impl IntoPy<PyObject> for GenericDmap {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            GenericDmap::Scalar(x) => x.into_py(py),
            GenericDmap::Vec1D(x) => PyArray::from_owned_array_bound(py, x).collect(),
            GenericDmap::Vec2D(x) => PyArray::from_owned_array_bound(py, x).collect(),
            GenericDmap::Vec3D(x) => PyArray::from_owned_array_bound(py, x).collect(),
        }
    }
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


pub fn check_scalar<T: InDmap + TryFrom<Atom>>(
    fields: &mut IndexMap<String, GenericDmap>,
    name: &str,
) -> Result<()> {
    match fields.get(name) {
        Some(&GenericDmap::Scalar(data)) => T::try_from(data)
            .map(|_| ())
            .map_err(|_| DmapError::ScalarError(format!("Incorrect type for {name}"))),
        Some(_) => Err(DmapError::ScalarError(format!("{name} is a vector field"))),
        None => Err(DmapError::ScalarError(format!("{name} is not in record"))),
    }
}

pub fn check_scalar_opt<T: InDmap + TryFrom<Atom>>(
    fields: &mut IndexMap<String, GenericDmap>,
    name: &str,
) -> Result<()> {
    match fields.get(name) {
        Some(&GenericDmap::Scalar(data)) => T::try_from(data)
            .map(|_| ())
            .map_err(|_| DmapError::ScalarError(format!("Incorrect type for {name}"))),
        Some(_) => Err(DmapError::ScalarError(format!("{name} is a vector field"))),
        None => Ok(()),
    }
}

pub fn check_vector<T: InDmap + TryFrom<Atom>>(
    fields: &mut IndexMap<String, GenericDmap>,
    name: &str,
) -> Result<()> {
    match fields.get(name) {
        Some(GenericDmap::Vec1D(data)) => T::try_from(data[0].clone())
            .map(|_| ())
            .map_err(|e| DmapError::VectorError(format!("Incorrect type for {name}"))),
        Some(GenericDmap::Vec2D(data)) => T::try_from(data[[0, 0]].clone())
            .map(|_| ())
            .map_err(|e| DmapError::VectorError(format!("Incorrect type for {name}"))),
        Some(GenericDmap::Vec3D(data)) => T::try_from(data[[0, 0, 0]].clone())
            .map(|_| ())
            .map_err(|e| DmapError::VectorError(format!("Incorrect type for {name}"))),
        Some(GenericDmap::Scalar(_)) => {
            Err(DmapError::VectorError(format!("{name} is a scalar field")))
        }
        None => Err(DmapError::VectorError(format!("{name} not in record"))),
    }
}

pub fn check_vector_opt<T: InDmap + TryFrom<Atom>>(
    fields: &mut IndexMap<String, GenericDmap>,
    name: &str,
) -> Result<()> {
    match fields.get(name) {
        Some(GenericDmap::Vec1D(data)) => T::try_from(data[0].clone())
            .map(|_| ())
            .map_err(|e| DmapError::VectorError(format!("Incorrect type for {name}"))),
        Some(GenericDmap::Vec2D(data)) => T::try_from(data[[0, 0]].clone())
            .map(|_| ())
            .map_err(|e| DmapError::VectorError(format!("Incorrect type for {name}"))),
        Some(GenericDmap::Vec3D(data)) => T::try_from(data[[0, 0, 0]].clone())
            .map(|_| ())
            .map_err(|e| DmapError::VectorError(format!("Incorrect type for {name}"))),
        Some(GenericDmap::Scalar(_)) => {
            Err(DmapError::VectorError(format!("{name} is a scalar field")))
        }
        None => Ok(()),
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
