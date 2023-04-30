pub mod formats;

use crate::formats::DmapRecord;
use bytemuck::PodCastError;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
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

    pub fn find_differences(&self, other: &RawDmapRecord) -> DmapDifference {
        let self_scalars = self
            .scalar_list
            .iter()
            .map(|s| s.clone())
            .collect::<HashSet<String>>();
        let other_scalars = other
            .scalar_list
            .iter()
            .map(|s| s.clone())
            .collect::<HashSet<String>>();
        let self_unique_scalars = self_scalars
            .difference(&other_scalars)
            .map(|s| s.clone())
            .collect();
        let other_unique_scalars = other_scalars
            .difference(&self_scalars)
            .map(|s| s.clone())
            .collect();

        let self_vectors = self
            .vector_list
            .iter()
            .map(|v| v.clone())
            .collect::<HashSet<String>>();
        let other_vectors = other
            .vector_list
            .iter()
            .map(|v| v.clone())
            .collect::<HashSet<String>>();
        let self_unique_vectors: Vec<String> = self_vectors
            .difference(&other_vectors)
            .map(|v| v.clone())
            .collect();
        let other_unique_vectors: Vec<String> = other_vectors
            .difference(&self_vectors)
            .map(|v| v.clone())
            .collect();

        let intersecting_scalars = self_scalars.intersection(&other_scalars);
        let intersecting_vectors = self_vectors.intersection(&other_vectors);
        let mut different_scalars = HashMap::new();
        let mut different_vectors = HashMap::new();

        for scalar in intersecting_scalars {
            let val_1 = self.scalars.get(scalar).unwrap();
            let val_2 = other.scalars.get(scalar).unwrap();
            if val_1 != val_2 {
                different_scalars.insert(scalar.clone(), (val_1.clone(), val_2.clone()));
            }
        }
        for vector in intersecting_vectors {
            let val_1 = self.vectors.get(vector).unwrap();
            let val_2 = other.vectors.get(vector).unwrap();
            if val_1 != val_2 {
                different_vectors.insert(vector.clone(), (val_1.clone(), val_2.clone()));
            }
        }
        DmapDifference {
            unique_scalars_1: self_unique_scalars,
            unique_scalars_2: other_unique_scalars,
            unique_vectors_1: self_unique_vectors,
            unique_vectors_2: other_unique_vectors,
            different_scalars,
            different_vectors,
        }
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
impl Display for DmapDifference {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut repr: String = format!("");
        if self.is_empty() {
            return write!(f, "No differences{}", repr);
        }
        if self.unique_scalars_1.len() != 0 {
            repr.push_str("Unique scalar fields in left record: ");
            repr.push_str(&format!("{:?}\n", self.unique_scalars_1));
        }
        if self.unique_scalars_2.len() != 0 {
            repr.push_str("Unique scalar fields in right record: ");
            repr.push_str(&format!("{:?}\n", self.unique_scalars_2));
        }
        if self.unique_vectors_1.len() != 0 {
            repr.push_str("Unique vector fields in left record: ");
            repr.push_str(&format!("{:?}\n", self.unique_vectors_1));
        }
        if self.unique_vectors_2.len() != 0 {
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!("cannot interpret {data} as i8")));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as i16"
            )));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as i32"
            )));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as f32"
            )));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as f64"
            )));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as String"
            )));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!("cannot interpret {data} as u8")));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as u16"
            )));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as u32"
            )));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as i64"
            )));
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
            return Ok(x);
        } else {
            return Err(DmapError::Message(format!(
                "cannot interpret {data} as u64"
            )));
        }
    }
    fn get_dmap_key() -> u8 {
        19
    }
    fn data_to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

#[derive(Debug, PartialEq, Clone)]
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
        Err(DmapError::Message(format!("{} not found", name)))
    }
}

/// Gets vector value from vectors HashMap and unwraps into the built-in type
pub fn get_vector_val<T: InDmap>(
    vectors: &mut std::collections::HashMap<String, RawDmapVector>,
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
            .map(|x| T::get_inner_value(x).expect(&format!("error getting vector {name}")))
            .collect();
        Ok(DmapVec {
            data: arr,
            dimensions: dimensions.clone(),
        })
    } else {
        Err(DmapError::Message(format!("{} not found", name)))
    }
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
        DmapType::DMAP => Err(DmapError::Message(
            "PARSE SCALAR: Trying to read DMAP data type for a scalar".to_string(),
        ))?,
        _ => read_data(cursor, data_type)?,
    };

    Ok((name, RawDmapScalar { data, mode }))
}

/// Reads a vector starting from cursor position
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
    if let DmapType::DMAP = data_type {
        Err(DmapError::Message(
            "PARSE VECTOR: Trying to read DMAP data type for a vector".to_string(),
        ))?
    }

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
        if dim <= 0 && name != "slist" {
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
    dimensions = dimensions.into_iter().rev().collect(); // reverse the dimensions, stored in column-major order

    if total_elements * data_type.get_num_bytes() as i32 > record_size {
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
        DmapType::DMAP => Err(DmapError::Message(
            "READ DATA: Data type DMAP unreadable".to_string(),
        ))?,
        DmapType::UCHAR { .. } => DmapType::UCHAR(data[0]),
        DmapType::CHAR { .. } => {
            DmapType::CHAR(bytemuck::try_pod_read_unaligned::<i8>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret char".to_string(), e)
            })?)
        }
        DmapType::SHORT { .. } => {
            DmapType::SHORT(bytemuck::try_pod_read_unaligned::<i16>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret short".to_string(), e)
            })?)
        }
        DmapType::USHORT { .. } => {
            DmapType::USHORT(bytemuck::try_pod_read_unaligned::<u16>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret ushort".to_string(), e)
            })?)
        }
        DmapType::INT { .. } => {
            DmapType::INT(bytemuck::try_pod_read_unaligned::<i32>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret int".to_string(), e)
            })?)
        }
        DmapType::UINT { .. } => {
            DmapType::UINT(bytemuck::try_pod_read_unaligned::<u32>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret uint".to_string(), e)
            })?)
        }
        DmapType::LONG { .. } => {
            DmapType::LONG(bytemuck::try_pod_read_unaligned::<i64>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret long".to_string(), e)
            })?)
        }
        DmapType::ULONG { .. } => {
            DmapType::ULONG(bytemuck::try_pod_read_unaligned::<u64>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret ulong".to_string(), e)
            })?)
        }
        DmapType::FLOAT { .. } => {
            DmapType::FLOAT(bytemuck::try_pod_read_unaligned::<f32>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret float".to_string(), e)
            })?)
        }
        DmapType::DOUBLE { .. } => {
            DmapType::DOUBLE(bytemuck::try_pod_read_unaligned::<f64>(data).map_err(|e| {
                DmapError::CastError("READ DATA: Unable to interpret double".to_string(), e)
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
            get_scalar_val::<i8>(&mut rec.scalars.clone(), "scal")
                .expect("Unable to recover scalar")
        );
        // assert_eq!(
        //     vec![0, 1, 2],
        //     get_vector_val::<i8>(&rec.vectors, "arr").expect("Unable to recover vector")
        // );
    }

    #[test]
    fn same_record_no_differences() {
        let scalar = RawDmapScalar::new(DmapType::CHAR(10));
        let mut scalars = HashMap::new();
        scalars.insert("scal".to_string(), scalar);

        let dimensions = vec![3];
        let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
        let vector = RawDmapVector::new(dimensions, data);
        let mut vectors = HashMap::new();
        vectors.insert("arr".to_string(), vector);

        let rec1 = RawDmapRecord {
            num_scalars: 1,
            num_vectors: 1,
            scalar_list: vec!["scal".to_string()],
            vector_list: vec!["arr".to_string()],
            scalars: scalars.clone(),
            vectors: vectors.clone(),
        };

        let rec2 = RawDmapRecord {
            num_scalars: 1,
            num_vectors: 1,
            scalar_list: vec!["scal".to_string()],
            vector_list: vec!["arr".to_string()],
            scalars,
            vectors,
        };

        let differences = rec1.find_differences(&rec2);
        assert!(differences.is_empty());
    }

    #[test]
    fn record_differences() {
        let scalar = RawDmapScalar::new(DmapType::CHAR(10));
        let mut scalars = HashMap::new();
        scalars.insert("scal".to_string(), scalar);

        let dimensions = vec![3];
        let data = vec![DmapType::CHAR(0), DmapType::CHAR(1), DmapType::CHAR(2)];
        let vector = RawDmapVector::new(dimensions, data);
        let mut vectors = HashMap::new();
        vectors.insert("arr".to_string(), vector);

        let rec1 = RawDmapRecord {
            num_scalars: 1,
            num_vectors: 1,
            scalar_list: vec!["scal".to_string()],
            vector_list: vec!["arr".to_string()],
            scalars: scalars.clone(),
            vectors: vectors.clone(),
        };

        let dimensions = vec![12];
        let data = vec![
            DmapType::INT(0),
            DmapType::INT(1),
            DmapType::INT(2),
            DmapType::INT(0),
            DmapType::INT(1),
            DmapType::INT(2),
            DmapType::INT(0),
            DmapType::INT(1),
            DmapType::INT(2),
            DmapType::INT(0),
            DmapType::INT(1),
            DmapType::INT(2),
        ];
        let vector = RawDmapVector::new(dimensions, data);
        scalars.insert(
            "scal".to_string(),
            RawDmapScalar::new(DmapType::ULONG(123456)),
        );
        vectors.insert("arr".to_string(), vector);
        let rec2 = RawDmapRecord {
            num_scalars: 1,
            num_vectors: 1,
            scalar_list: vec!["scal".to_string()],
            vector_list: vec!["arr".to_string()],
            scalars,
            vectors,
        };

        let differences = rec1.find_differences(&rec2);
        // println!("{}", differences);
        assert_eq!(false, differences.is_empty());
    }
}
