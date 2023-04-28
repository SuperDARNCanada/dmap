pub mod formats;

use bytemuck::PodCastError;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::Path;

type Result<T> = std::result::Result<T, DmapError>;

#[derive(Debug, Clone)]
pub enum DmapError {
    BadVal(String, DmapType),
    Message(String),
    CastError(String, PodCastError),
}

impl Error for DmapError {}

impl Display for DmapError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DmapError::Message(msg) => write!(f, "{}", msg),
            DmapError::BadVal(msg, val) => write!(f, "{}: {:?}", msg, val),
            DmapError::CastError(msg, err) => write!(f, "{}: {}", msg, err.to_string()),
        }
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
    fn all_keys() -> Vec<i8> {
        vec![0, 1, 2, 3, 4, 8, 9, 10, 16, 17, 18, 19]
    }

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
            _ => Err(DmapError::Message(format!(
                "Invalid key for DMAP type: {}",
                key
            ))),
        }
    }

    /// Gets the numeric key for the data type.
    fn get_key(&self) -> i8 {
        match self {
            DmapType::DMAP => 0,
            DmapType::CHAR(..) => 1,
            DmapType::SHORT(..) => 2,
            DmapType::INT(..) => 3,
            DmapType::FLOAT(..) => 4,
            DmapType::DOUBLE(..) => 8,
            DmapType::STRING(..) => 9,
            DmapType::LONG(..) => 10,
            DmapType::UCHAR(..) => 16,
            DmapType::USHORT(..) => 17,
            DmapType::UINT(..) => 18,
            DmapType::ULONG(..) => 19,
        }
    }

    /// Converts into raw bytes
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            DmapType::DMAP => vec![],
            DmapType::CHAR(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::UCHAR(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::SHORT(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::USHORT(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::INT(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::UINT(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::LONG(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::ULONG(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::FLOAT(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::DOUBLE(x) => bytemuck::bytes_of(x).to_vec(),
            DmapType::STRING(x) => {
                let mut bytes = vec![];
                bytes.append(&mut x.as_bytes().to_vec());
                bytes.push(0); // Rust String not null-terminated
                bytes
            }
        }
    }
}

impl Display for DmapType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

#[derive(Debug, PartialEq, Clone)]
pub struct RawDmapScalar {
    pub data: DmapType,
    mode: i8,
}

impl RawDmapScalar {
    /// Converts into raw bytes
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![];
        bytes.append(&mut DmapType::CHAR(self.data.get_key()).to_bytes());
        bytes.append(&mut self.data.to_bytes());
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct RawDmapVector {
    mode: i8,
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

impl RawDmapVector {
    /// Converts into raw bytes
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![];
        bytes.append(&mut DmapType::CHAR(self.data[0].get_key()).to_bytes());
        bytes.append(&mut DmapType::INT(self.dimensions.len() as i32).to_bytes());
        for dim in self.dimensions.clone() {
            bytes.append(&mut DmapType::INT(dim).to_bytes());
        }
        for val in self.data.clone() {
            bytes.append(&mut val.to_bytes());
        }
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct RawDmapRecord {
    pub num_scalars: i32,
    pub num_vectors: i32,
    pub scalar_list: Vec<String>,
    pub vector_list: Vec<String>,
    pub scalars: HashMap<String, RawDmapScalar>,
    pub vectors: HashMap<String, RawDmapVector>,
}

impl PartialEq for RawDmapRecord {
    fn eq(&self, other: &Self) -> bool {
        if !(self.num_scalars == other.num_scalars && self.num_vectors == other.num_vectors) {
            return false;
        }
        for (s1, s2) in self.scalar_list.iter().zip(other.scalar_list.iter()) {
            if !(s1 == s2) {
                return false;
            }
            let scal1 = self.scalars.get(s1);
            let scal2 = other.scalars.get(s2);
            if !(scal1 == scal2) {
                return false;
            }
        }
        for (a1, a2) in self.vector_list.iter().zip(other.vector_list.iter()) {
            if !(a1 == a2) {
                return false;
            }
            let arr1 = self.vectors.get(a1);
            let arr2 = self.vectors.get(a2);
            if !(arr1 == arr2) {
                return false;
            }
        }
        true
    }
}

impl RawDmapRecord {
    /// Converts into raw bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut container: Vec<u8> = vec![];
        let code = 65537; // No idea why this is what it is, copied from backscatter

        let mut data_bytes: Vec<u8> = vec![];
        for scalar in &self.scalar_list {
            data_bytes.extend(scalar.as_bytes());
            data_bytes.push(0); // null-terminate string
            data_bytes.extend(
                self.scalars
                    .get(scalar)
                    .expect(&*format!("{scalar} missing from record"))
                    .to_bytes(),
            );
        }
        for vector in &self.vector_list {
            data_bytes.extend(vector.as_bytes());
            data_bytes.push(0); // null-terminate string
            data_bytes.extend(
                self.vectors
                    .get(vector)
                    .expect(&*format!("{vector} missing from record"))
                    .to_bytes(),
            );
        }

        container.extend(DmapType::INT(code).to_bytes());
        container.extend(DmapType::INT(data_bytes.len() as i32 + 16).to_bytes()); // +16 for code, length, num_scalars, num_vectors
        container.extend(DmapType::INT(self.num_scalars).to_bytes());
        container.extend(DmapType::INT(self.num_vectors).to_bytes());
        container.extend(data_bytes);
        container
    }
}

/// Trait for types that can be stored in DMAP files
pub trait InDmap {
    fn get_inner_value(data: &DmapType) -> Result<Self>
    where
        Self: Sized;
}
impl InDmap for i8 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::CHAR(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!("cannot interpret {data} as i8")));
        }
    }
}
impl InDmap for i16 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::SHORT(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as i16"
            )));
        }
    }
}
impl InDmap for i32 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::INT(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as i32"
            )));
        }
    }
}
impl InDmap for f32 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::FLOAT(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as f32"
            )));
        }
    }
}
impl InDmap for f64 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::DOUBLE(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as f64"
            )));
        }
    }
}
impl InDmap for String {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::STRING(x) = data {
            return Ok(x.clone());
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as String"
            )));
        }
    }
}
impl InDmap for u8 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::UCHAR(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!("cannot interpret {data} as u8")));
        }
    }
}
impl InDmap for u16 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::USHORT(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as u16"
            )));
        }
    }
}
impl InDmap for u32 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::UINT(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as u32"
            )));
        }
    }
}
impl InDmap for i64 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::LONG(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as i64"
            )));
        }
    }
}
impl InDmap for u64 {
    fn get_inner_value(data: &DmapType) -> Result<Self> {
        if let DmapType::ULONG(x) = data {
            return Ok(*x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as u64"
            )));
        }
    }
}

/// Gets scalar value from record and unwraps into the built-in type
pub fn get_scalar_val<T: InDmap>(record: &RawDmapRecord, name: &str) -> Result<T> {
    if let Some(RawDmapScalar { data, mode: _ }) = record.scalars.get(name) {
        Ok(T::get_inner_value(data)?)
    } else {
        Err(DmapError::Message(format!("{} not found", name)))
    }
}

/// Gets vector value from record and unwraps into the built-in type
pub fn get_vector_val<T: InDmap>(record: &RawDmapRecord, name: &str) -> Result<Vec<T>> {
    if let Some(RawDmapVector {
        data,
        mode: _,
        dimensions: _,
    }) = record.vectors.get(name)
    {
        let arr = data
            .iter()
            .map(|x| T::get_inner_value(x).expect(&format!("error getting vector {name}")))
            .collect();
        Ok(arr)
    } else {
        Err(DmapError::Message(format!("{} not found", name)))
    }
}

/// Reads a record starting from cursor position
fn parse_record(cursor: &mut Cursor<Vec<u8>>) -> Result<RawDmapRecord> {
    let bytes_already_read = cursor.position();
    let _code = match read_data(cursor, DmapType::INT(0))? {
        DmapType::INT(i) => Ok(i),
        _ => Err(DmapError::Message("PARSE RECORD: Invalid code".to_string())),
    }?;
    let size = match read_data(cursor, DmapType::INT(0))? {
        DmapType::INT(i) => Ok(i),
        _ => Err(DmapError::Message("PARSE RECORD: Invalid size".to_string())),
    }?;

    // adding 8 bytes because code and size are part of the record.
    if size as u64
        > cursor.get_ref().len() as u64 - cursor.position() + 2 * DmapType::INT(0).get_num_bytes()
    {
        return Err(DmapError::Message(
            "PARSE RECORD: Integrity check shows record size bigger than \
            remaining buffer. Data is likely corrupted"
                .to_string(),
        ));
    } else if size <= 0 {
        return Err(DmapError::Message(
            "PARSE RECORD: Integrity check shows record size <= 0. \
            Data is likely corrupted"
                .to_string(),
        ));
    }

    let num_scalars = match read_data(cursor, DmapType::INT(0))? {
        DmapType::INT(i) => Ok(i),
        _ => Err(DmapError::Message(
            "PARSE RECORD: Invalid number of scalars".to_string(),
        )),
    }?;
    let num_vectors = match read_data(cursor, DmapType::INT(0))? {
        DmapType::INT(i) => Ok(i),
        _ => Err(DmapError::Message(
            "PARSE RECORD: Invalid number of vectors".to_string(),
        )),
    }?;
    if num_scalars <= 0 {
        return Err(DmapError::Message(
            "PARSE RECORD: Number of scalars is 0 or negative.".to_string(),
        ));
    } else if num_vectors <= 0 {
        return Err(DmapError::Message(
            "PARSE RECORD: Number of vectors is 0 or negative.".to_string(),
        ));
    } else if num_scalars + num_vectors > size {
        return Err(DmapError::Message(
            "PARSE RECORD: Invalid number of record elements. \
            Vector or scalar field is likely corrupted."
                .to_string(),
        ));
    }

    let mut scalar_list: Vec<String> = vec![];
    let mut scalars = HashMap::new();
    for _ in 0..num_scalars {
        let (name, val) = parse_scalar(cursor)?;
        scalar_list.push(name.clone());
        scalars.insert(name, val);
    }

    let mut vector_list: Vec<String> = vec![];
    let mut vectors = HashMap::new();
    for _ in 0..num_vectors {
        let (name, val) = parse_vector(cursor, size)?;
        vector_list.push(name.clone());
        vectors.insert(name, val);
    }

    if cursor.position() - bytes_already_read != size as u64 {
        return Err(DmapError::Message(format!(
            "PARSE RECORD: Bytes read {} does not match the records size field {}",
            cursor.position() - bytes_already_read,
            size
        )));
    }

    Ok(RawDmapRecord {
        num_scalars,
        scalars,
        num_vectors,
        vectors,
        scalar_list,
        vector_list,
    })
}

/// Reads a scalar starting from cursor position
fn parse_scalar(cursor: &mut Cursor<Vec<u8>>) -> Result<(String, RawDmapScalar)> {
    let mode = 6;
    let name = match read_data(cursor, DmapType::STRING("".to_string()))? {
        DmapType::STRING(s) => Ok(s),
        _ => Err(DmapError::Message(
            "PARSE SCALAR: Invalid scalar name".to_string(),
        )),
    }?;
    let data_type_key = match read_data(cursor, DmapType::CHAR(0))? {
        DmapType::CHAR(c) => Ok(c),
        _ => Err(DmapError::Message(
            "PARSE SCALAR: Invalid data type".to_string(),
        )),
    }?;

    if !DmapType::all_keys().contains(&data_type_key) {
        return Err(DmapError::BadVal(
            "PARSE SCALAR: Data type is corrupted. Record is likely \
            corrupted"
                .to_string(),
            DmapType::CHAR(data_type_key),
        ));
    }

    let data_type = DmapType::get_type_from_key(data_type_key)?;

    let data = match data_type {
        DmapType::DMAP => {
            parse_record(cursor)?;
            DmapType::DMAP
        }
        _ => read_data(cursor, data_type)?,
    };

    Ok((name, RawDmapScalar { data, mode }))
}

/// Reads an vector starting from cursor position
fn parse_vector(cursor: &mut Cursor<Vec<u8>>, record_size: i32) -> Result<(String, RawDmapVector)> {
    let mode = 7;
    let name = match read_data(cursor, DmapType::STRING("".to_string()))? {
        DmapType::STRING(s) => Ok(s),
        _ => Err(DmapError::Message(
            "PARSE VECTOR: Invalid vector name".to_string(),
        )),
    }?;
    let data_type_key = match read_data(cursor, DmapType::CHAR(0))? {
        DmapType::CHAR(c) => Ok(c),
        _ => Err(DmapError::Message(
            "PARSE VECTOR: Invalid data type".to_string(),
        )),
    }?;

    if !DmapType::all_keys().contains(&data_type_key) {
        return Err(DmapError::Message(
            "PARSE VECTOR: Data type is corrupted. Record is likely \
            corrupted"
                .to_string(),
        ));
    }

    let data_type = DmapType::get_type_from_key(data_type_key)?;

    let vector_dimension = match read_data(cursor, DmapType::INT(0))? {
        DmapType::INT(i) => Ok(i),
        _ => Err(DmapError::Message(
            "PARSE VECTOR: Invalid vector dimension".to_string(),
        )),
    }?;

    if vector_dimension > record_size {
        return Err(DmapError::Message(
            "PARSE VECTOR: Parsed # of vector dimensions are larger \
            than record size. Record is likely corrupted"
                .to_string(),
        ));
    } else if vector_dimension <= 0 {
        return Err(DmapError::Message(
            "PARSE VECTOR: Parsed # of vector dimensions are zero or \
            negative. Record is likely corrupted"
                .to_string(),
        ));
    }

    let mut dimensions: Vec<i32> = vec![];
    let mut total_elements = 1;
    for _ in 0..vector_dimension {
        let dim = match read_data(cursor, DmapType::INT(0))? {
            DmapType::INT(val) => Ok(val),
            _ => Err(DmapError::Message(
                "PARSE VECTOR: Vector dimensions could not be parsed".to_string(),
            )),
        }?;
        if dim <= 0 {
            return Err(DmapError::Message(
                "PARSE VECTOR: Vector dimension is zero or negative. \
                Record is likely corrupted"
                    .to_string(),
            ));
        } else if dim > record_size {
            return Err(DmapError::Message(
                "PARSE VECTOR: Vector dimension exceeds record size".to_string(),
            ));
        }
        dimensions.push(dim);
        total_elements = total_elements * dim;
    }

    if total_elements > record_size {
        return Err(DmapError::Message(
            "PARSE VECTOR: Total vector elements > record size.".to_string(),
        ));
    } else if total_elements * data_type.get_num_bytes() as i32 > record_size {
        return Err(DmapError::Message(
            "PARSE VECTOR: Vector size exceeds record size. Data is \
            likely corrupted"
                .to_string(),
        ));
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
        return Err(DmapError::Message(
            "READ DATA: Cursor extends out of buffer. Data is likely corrupted".to_string(),
        ));
    }
    if stream.len() - position < data_type.get_num_bytes() as usize {
        return Err(DmapError::Message(
            "READ DATA: Byte offsets into buffer are not properly aligned. \
        Data is likely corrupted"
                .to_string(),
        ));
    }

    let mut data_size = data_type.get_num_bytes() as usize;
    let data: &[u8] = &stream[position..position + data_size];
    let parsed_data = match data_type {
        DmapType::DMAP => parse_record(cursor).map(|_| DmapType::DMAP)?,
        DmapType::UCHAR { .. } => DmapType::UCHAR(data[0]),
        DmapType::CHAR { .. } => {
            DmapType::CHAR(*bytemuck::try_from_bytes::<i8>(data).map_err(|_| {
                DmapError::Message("READ DATA: Unable to interpret char".to_string())
            })?)
        }
        DmapType::SHORT { .. } => {
            DmapType::SHORT(bytemuck::try_pod_read_unaligned::<i16>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret short".to_string(), e)
            })?)
        }
        DmapType::USHORT { .. } => {
            DmapType::USHORT(*bytemuck::try_from_bytes::<u16>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret ushort".to_string(), e)
            })?)
        }
        DmapType::INT { .. } => {
            DmapType::INT(bytemuck::try_pod_read_unaligned::<i32>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret int".to_string(), e)
            })?)
        }
        DmapType::UINT { .. } => {
            DmapType::UINT(*bytemuck::try_from_bytes::<u32>(data).map_err(|_| {
                DmapError::Message("READ DATA: Unable to interpret uint".to_string())
            })?)
        }
        DmapType::LONG { .. } => {
            DmapType::LONG(*bytemuck::try_from_bytes::<i64>(data).map_err(|_| {
                DmapError::Message("READ DATA: Unable to interpret long".to_string())
            })?)
        }
        DmapType::ULONG { .. } => {
            DmapType::ULONG(*bytemuck::try_from_bytes::<u64>(data).map_err(|_| {
                DmapError::Message("READ DATA: Unable to interpret ulong".to_string())
            })?)
        }
        DmapType::FLOAT { .. } => {
            DmapType::FLOAT(bytemuck::try_pod_read_unaligned::<f32>(data).map_err(|_| {
                DmapError::Message("READ DATA: Unable to interpret float".to_string())
            })?)
        }
        DmapType::DOUBLE { .. } => {
            DmapType::DOUBLE(bytemuck::try_pod_read_unaligned::<f64>(data).map_err(|_| {
                DmapError::Message("READ DATA: Unable to interpret double".to_string())
            })?)
        }
        DmapType::STRING { .. } => {
            let mut byte_counter = 0;
            while stream[position + byte_counter] != 0 {
                byte_counter += 1;
                if position + byte_counter >= stream.len() {
                    return Err(DmapError::Message(
                        "READ DATA: String is improperly terminated. \
                    Dmap record is corrupted"
                            .to_string(),
                    ));
                }
            }
            let data = String::from_utf8(stream[position..position + byte_counter].to_owned())
                .map_err(|_| {
                    DmapError::Message("READ DATA: Unable to interpret string".to_string())
                })?;
            data_size = byte_counter + 1;
            DmapType::STRING(data)
        }
    };
    cursor.set_position({ position + data_size } as u64);

    Ok(parsed_data)
}

/// Reads from dmap_data and parses into a collection of RawDmapRecord's.
///
/// # Failures
/// If dmap_data cannot be read or contains invalid data.
pub fn read_records(mut dmap_data: impl Read) -> Result<Vec<RawDmapRecord>> {
    let mut buffer: Vec<u8> = vec![];

    dmap_data
        .read_to_end(&mut buffer)
        .map_err(|_| DmapError::Message("Could not read data".to_string()))?;

    let mut cursor = Cursor::new(buffer);
    let mut dmap_records: Vec<RawDmapRecord> = vec![];

    while cursor.position() < cursor.get_ref().len() as u64 {
        dmap_records.push(parse_record(&mut cursor)?);
    }
    Ok(dmap_records)
}

/// Writes dmap_records to path as a Vec<u8>
///
/// # Failures
/// If file cannot be created at path or data cannot be written to file.
pub fn to_file<P: AsRef<Path>>(path: P, dmap_records: &Vec<RawDmapRecord>) -> std::io::Result<()> {
    let mut stream = vec![];
    for rec in dmap_records {
        stream.append(&mut rec.to_bytes());
    }
    let mut file = File::create(path)?;
    file.write_all(&stream)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    impl RawDmapVector {
        fn new(dimensions: Vec<i32>, data: Vec<DmapType>) -> RawDmapVector {
            RawDmapVector {
                dimensions,
                mode: 7,
                data,
            }
        }
    }

    impl RawDmapScalar {
        fn new(data: DmapType) -> RawDmapScalar {
            RawDmapScalar { data, mode: 6 }
        }
    }

    #[test]
    fn string_to_bytes() {
        let s = DmapType::STRING("Test".to_string());
        assert_eq!(s.to_bytes(), vec![84, 101, 115, 116, 0])
    }

    #[test]
    fn int_to_bytes() {
        let i = DmapType::INT(10);
        assert_eq!(i.to_bytes(), vec![10, 0, 0, 0]) // little-endian
    }

    #[test]
    fn scalar_to_bytes() {
        let scalar = RawDmapScalar::new(DmapType::CHAR(10));
        assert_eq!(scalar.to_bytes(), vec![1, 10])
    }

    #[test]
    fn vector_to_bytes() {
        let dimensions = vec![3];
        let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
        let vector = RawDmapVector::new(dimensions, data);
        assert_eq!(vector.to_bytes(), vec![1, 1, 0, 0, 0, 3, 0, 0, 0, 0, 1, 2])
    }

    #[test]
    fn record_to_bytes() {
        let scalar = RawDmapScalar::new(DmapType::CHAR(10));
        let mut scalars = HashMap::new();
        scalars.insert("scal".to_string(), scalar);

        let dimensions = vec![3];
        let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
        let vector = RawDmapVector::new(dimensions, data);
        let mut vectors = HashMap::new();
        vectors.insert("arr".to_string(), vector);

        let rec = RawDmapRecord {
            num_scalars: 1,
            num_vectors: 1,
            scalar_list: vec!["scal".to_string()],
            vector_list: vec!["arr".to_string()],
            scalars,
            vectors,
        };

        assert_eq!(
            rec.to_bytes(),
            vec![
                1, 0, 1, 0, 39, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 115, 99, 97, 108, 0, 1, 10, 97,
                114, 114, 0, 1, 1, 0, 0, 0, 3, 0, 0, 0, 0, 1, 2
            ]
        )
    }

    #[test]
    fn record_get_values() {
        let scalar = RawDmapScalar::new(DmapType::CHAR(10));
        let mut scalars = HashMap::new();
        scalars.insert("scal".to_string(), scalar);

        let dimensions = vec![3];
        let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
        let vector = RawDmapVector::new(dimensions, data);
        let mut vectors = HashMap::new();
        vectors.insert("arr".to_string(), vector);

        let rec = RawDmapRecord {
            num_scalars: 1,
            num_vectors: 1,
            scalar_list: vec!["scal".to_string()],
            vector_list: vec!["arr".to_string()],
            scalars,
            vectors,
        };

        assert_eq!(
            10,
            get_scalar_val::<i8>(&rec, "scal").expect("Unable to recover scalar")
        );
        assert_eq!(
            vec![0, 1, 2],
            get_vector_val::<i8>(&rec, "arr").expect("Unable to recover vector")
        );
    }
}
