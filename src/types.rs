//! Low-level data types within DMAP records.
use crate::error::DmapError;
use indexmap::IndexMap;
use ndarray::ArrayD;
use paste::paste;
use std::cmp::PartialEq;
use std::fmt::{Display, Formatter};
use zerocopy::{AsBytes, ByteOrder, FromBytes, LittleEndian};

type Result<T> = std::result::Result<T, DmapError>;

/// Defines the fields of a record and their [`Type`].
pub struct Fields<'a> {
    /// The names of all fields of the record type
    pub all_fields: Vec<&'a str>,
    /// The name and Type of each required scalar field
    pub scalars_required: Vec<(&'a str, Type)>,
    /// The name and Type of each optional scalar field
    pub scalars_optional: Vec<(&'a str, Type)>,
    /// The name and Type of each required vector field
    pub vectors_required: Vec<(&'a str, Type)>,
    /// The name and Type of each optional vector field
    pub vectors_optional: Vec<(&'a str, Type)>,
    /// Groups of vector fields which must have identical dimensions
    pub vector_dim_groups: Vec<Vec<&'a str>>,
    /// The name of each field which is a data (as opposed to metadata) field
    pub data_fields: Vec<&'a str>,
}

/// The possible data types that a scalar or vector field may have.
///
/// **Note**: `String` type is not supported for vector fields.
#[derive(Debug, PartialEq, Clone)]
pub enum Type {
    Char,
    Short,
    Int,
    Long,
    Uchar,
    Ushort,
    Uint,
    Ulong,
    Float,
    Double,
    String,
}
impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Char => write!(f, "CHAR"),
            Self::Short => write!(f, "SHORT"),
            Self::Int => write!(f, "INT"),
            Self::Float => write!(f, "FLOAT"),
            Self::Double => write!(f, "DOUBLE"),
            Self::String => write!(f, "STRING"),
            Self::Long => write!(f, "LONG"),
            Self::Uchar => write!(f, "UCHAR"),
            Self::Ushort => write!(f, "USHORT"),
            Self::Uint => write!(f, "UINT"),
            Self::Ulong => write!(f, "ULONG"),
        }
    }
}
impl Type {
    /// Converts from DMAP key to corresponding `Type` (see [here](https://github.com/SuperDARN/rst/blob/main/codebase/general/src.lib/dmap.1.25/include/dmap.h)).
    /// Returns the `Type` if the key is supported, otherwise raises `DmapError`
    pub(crate) fn from_key(key: i8) -> Result<Self> {
        let data = match key {
            1 => Self::Char,
            2 => Self::Short,
            3 => Self::Int,
            10 => Self::Long,
            16 => Self::Uchar,
            17 => Self::Ushort,
            18 => Self::Uint,
            19 => Self::Ulong,
            4 => Self::Float,
            8 => Self::Double,
            9 => Self::String,
            x => Err(DmapError::InvalidKey(x))?,
        };
        Ok(data)
    }
    /// Returns the corresponding key for the `Type` variant.
    pub(crate) fn key(&self) -> i8 {
        match self {
            Self::Char => 1,
            Self::Short => 2,
            Self::Int => 3,
            Self::Long => 10,
            Self::Uchar => 16,
            Self::Ushort => 17,
            Self::Uint => 18,
            Self::Ulong => 19,
            Self::Float => 4,
            Self::Double => 8,
            Self::String => 9,
        }
    }
    /// The size in bytes of the data for `Type`
    pub fn size(&self) -> usize {
        match self {
            Self::Char => 1,
            Self::Short => 2,
            Self::Int => 4,
            Self::Long => 8,
            Self::Uchar => 1,
            Self::Ushort => 2,
            Self::Uint => 4,
            Self::Ulong => 8,
            Self::Float => 4,
            Self::Double => 8,
            Self::String => 0,
        }
    }
}

/// A scalar field in a DMAP record.
//#[derive(Debug, Clone, PartialEq, FromPyObject, IntoPyObject)]
#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub enum DmapScalar {
    Char(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Uchar(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Float(f32),
    Double(f64),
    String(String),
}
impl DmapScalar {
    /// Gets the corresponding `Type`
    pub(crate) fn get_type(&self) -> Type {
        match self {
            Self::Char(_) => Type::Char,
            Self::Short(_) => Type::Short,
            Self::Int(_) => Type::Int,
            Self::Long(_) => Type::Long,
            Self::Uchar(_) => Type::Uchar,
            Self::Ushort(_) => Type::Ushort,
            Self::Uint(_) => Type::Uint,
            Self::Ulong(_) => Type::Ulong,
            Self::Float(_) => Type::Float,
            Self::Double(_) => Type::Double,
            Self::String(_) => Type::String,
        }
    }

    /// Converts `self` into a new `Type`, if possible.
    pub(crate) fn cast_as(&self, new_type: &Type) -> Result<Self> {
        match new_type {
            Type::Char => Ok(Self::Char(i8::try_from(self.clone())?)),
            Type::Short => Ok(Self::Short(i16::try_from(self.clone())?)),
            Type::Int => Ok(Self::Int(i32::try_from(self.clone())?)),
            Type::Long => Ok(Self::Long(i64::try_from(self.clone())?)),
            Type::Uchar => Ok(Self::Uchar(u8::try_from(self.clone())?)),
            Type::Ushort => Ok(Self::Ushort(u16::try_from(self.clone())?)),
            Type::Uint => Ok(Self::Uint(u32::try_from(self.clone())?)),
            Type::Ulong => Ok(Self::Ulong(u64::try_from(self.clone())?)),
            Type::Float => Ok(Self::Float(f32::try_from(self.clone())?)),
            Type::Double => Ok(Self::Double(f64::try_from(self.clone())?)),
            Type::String => Err(DmapError::InvalidScalar(
                "Unable to cast value to String".to_string(),
            )),
        }
    }
    /// Copies the data and metadata (`Type` key) to raw bytes
    pub(crate) fn as_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = DmapType::as_bytes(&self.get_type().key()).to_vec();
        let mut data_bytes: Vec<u8> = match self {
            Self::Char(x) => DmapType::as_bytes(x),
            Self::Short(x) => DmapType::as_bytes(x),
            Self::Int(x) => DmapType::as_bytes(x),
            Self::Long(x) => DmapType::as_bytes(x),
            Self::Uchar(x) => DmapType::as_bytes(x),
            Self::Ushort(x) => DmapType::as_bytes(x),
            Self::Uint(x) => DmapType::as_bytes(x),
            Self::Ulong(x) => DmapType::as_bytes(x),
            Self::Float(x) => DmapType::as_bytes(x),
            Self::Double(x) => DmapType::as_bytes(x),
            Self::String(x) => DmapType::as_bytes(x),
        };
        bytes.append(&mut data_bytes);
        bytes
    }
}
impl Display for DmapScalar {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Char(x) => write!(f, "CHAR {x}"),
            Self::Short(x) => write!(f, "SHORT {x}"),
            Self::Int(x) => write!(f, "INT {x}"),
            Self::Float(x) => write!(f, "FLOAT {x}"),
            Self::Double(x) => write!(f, "DOUBLE {x}"),
            Self::String(x) => write!(f, "STRING {x}"),
            Self::Long(x) => write!(f, "LONG {x}"),
            Self::Uchar(x) => write!(f, "UCHAR {x}"),
            Self::Ushort(x) => write!(f, "USHORT {x}"),
            Self::Uint(x) => write!(f, "UINT {x}"),
            Self::Ulong(x) => write!(f, "ULONG {x}"),
        }
    }
}

macro_rules! vec_to_bytes {
    ($bytes:ident, $x:ident) => {{
        $bytes.extend(($x.ndim() as i32).to_le_bytes());
        for &dim in $x.shape().iter().rev() {
            $bytes.extend((dim as i32).to_le_bytes());
        }
        for y in $x.iter() {
            $bytes.append(&mut DmapType::as_bytes(y).to_vec());
        }
    }};
}

/// A vector field in a DMAP record.
#[derive(Clone, Debug, PartialEq)]
pub enum DmapVec {
    Char(ArrayD<i8>),
    Short(ArrayD<i16>),
    Int(ArrayD<i32>),
    Long(ArrayD<i64>),
    Uchar(ArrayD<u8>),
    Ushort(ArrayD<u16>),
    Uint(ArrayD<u32>),
    Ulong(ArrayD<u64>),
    Float(ArrayD<f32>),
    Double(ArrayD<f64>),
}
impl DmapVec {
    /// Gets the corresponding [`Type`] of the vector.
    #[inline]
    pub(crate) fn get_type(&self) -> Type {
        match self {
            DmapVec::Char(_) => Type::Char,
            DmapVec::Short(_) => Type::Short,
            DmapVec::Int(_) => Type::Int,
            DmapVec::Long(_) => Type::Long,
            DmapVec::Uchar(_) => Type::Uchar,
            DmapVec::Ushort(_) => Type::Ushort,
            DmapVec::Uint(_) => Type::Uint,
            DmapVec::Ulong(_) => Type::Ulong,
            DmapVec::Float(_) => Type::Float,
            DmapVec::Double(_) => Type::Double,
        }
    }
    /// Copies the data and metadata (dimensions, [`Type`] key) to raw bytes
    #[inline]
    pub(crate) fn as_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = DmapType::as_bytes(&self.get_type().key()).to_vec();
        match self {
            DmapVec::Char(x) => vec_to_bytes!(bytes, x),
            DmapVec::Short(x) => vec_to_bytes!(bytes, x),
            DmapVec::Int(x) => vec_to_bytes!(bytes, x),
            DmapVec::Long(x) => vec_to_bytes!(bytes, x),
            DmapVec::Uchar(x) => vec_to_bytes!(bytes, x),
            DmapVec::Ushort(x) => vec_to_bytes!(bytes, x),
            DmapVec::Uint(x) => vec_to_bytes!(bytes, x),
            DmapVec::Ulong(x) => vec_to_bytes!(bytes, x),
            DmapVec::Float(x) => vec_to_bytes!(bytes, x),
            DmapVec::Double(x) => vec_to_bytes!(bytes, x),
        };
        bytes
    }

    /// Gets the dimensions of the vector, in row-major order.
    /// ## Example
    /// ```
    /// use ndarray::array;
    /// use dmap::types::DmapVec;
    ///
    /// let arr = DmapVec::Char(array![0, 1, 2, 3, 4].into_dyn());
    /// assert_eq!(arr.shape(), &[5]);
    ///
    /// let arr = DmapVec::Uint(array![[0, 1, 2], [3, 4, 5]].into_dyn());
    /// assert_eq!(arr.shape(), &[2, 3]);
    /// ```
    #[must_use]
    pub fn shape(&self) -> &[usize] {
        match self {
            DmapVec::Char(x) => x.shape(),
            DmapVec::Short(x) => x.shape(),
            DmapVec::Int(x) => x.shape(),
            DmapVec::Long(x) => x.shape(),
            DmapVec::Uchar(x) => x.shape(),
            DmapVec::Ushort(x) => x.shape(),
            DmapVec::Uint(x) => x.shape(),
            DmapVec::Ulong(x) => x.shape(),
            DmapVec::Float(x) => x.shape(),
            DmapVec::Double(x) => x.shape(),
        }
    }
}

/// Generates trait implementations for infallible conversion into [`DmapVec`] and fallible conversion
/// back.
///
/// Example: `vec_impls!(ArrayD<i8>, DmapVec::Char)` will generate `impl From<ArrayD<i8>> for
/// DmapVec` and `impl TryFrom<DmapVec> for ArrayD<i8>` code blocks.
macro_rules! vec_impls {
    ($type:ty, $enum_var:path) => {
        impl From<$type> for DmapVec {
            #[inline]
            fn from(value: $type) -> Self {
                $enum_var(value)
            }
        }

        impl TryFrom<DmapVec> for $type {
            type Error = DmapError;

            fn try_from(value: DmapVec) -> std::result::Result<Self, Self::Error> {
                if let $enum_var(x) = value {
                    Ok(x)
                } else {
                    Err(DmapError::InvalidVector(format!(
                        "Cannot convert to {}",
                        stringify!($type)
                    )))
                }
            }
        }

        impl From<$type> for DmapField {
            #[inline]
            fn from(value: $type) -> Self {
                DmapField::Vector($enum_var(value))
            }
        }

        impl TryFrom<DmapField> for $type {
            type Error = DmapError;

            fn try_from(value: DmapField) -> std::result::Result<Self, Self::Error> {
                match value {
                    DmapField::Vector(x) => x.try_into(),
                    _ => Err(Self::Error::InvalidVector(format!(
                        "Cannot interpret as {}",
                        stringify!($type)
                    ))),
                }
            }
        }
    };
}

vec_impls!(ArrayD<i8>, DmapVec::Char);
vec_impls!(ArrayD<i16>, DmapVec::Short);
vec_impls!(ArrayD<i32>, DmapVec::Int);
vec_impls!(ArrayD<i64>, DmapVec::Long);
vec_impls!(ArrayD<u8>, DmapVec::Uchar);
vec_impls!(ArrayD<u16>, DmapVec::Ushort);
vec_impls!(ArrayD<u32>, DmapVec::Uint);
vec_impls!(ArrayD<u64>, DmapVec::Ulong);
vec_impls!(ArrayD<f32>, DmapVec::Float);
vec_impls!(ArrayD<f64>, DmapVec::Double);

/// A generic field of a DMAP record.
///
/// This is the type that is stored in a DMAP record, representing either a scalar or
/// vector field.
#[derive(Debug, Clone, PartialEq)]
#[repr(C)]
pub enum DmapField {
    Vector(DmapVec),
    Scalar(DmapScalar),
}
impl DmapField {
    /// Converts the field and metadata (`Type` key and dimensions if applicable) to raw bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::Scalar(x) => x.as_bytes(),
            Self::Vector(x) => x.as_bytes(),
        }
    }
}

/// Macro for implementing conversion traits between primitives and [`DmapField`], [`DmapScalar`]
/// types.
///
/// Example: `scalar_impls(i8, DmapScalar::Char)` will implement:
///   `From<i8> for DmapField`
///   `TryFrom<DmapField> for i8`
macro_rules! scalar_impls {
    ($type:ty, $enum_var:path, $type_var:path) => {
        impl From<$type> for DmapField {
            fn from(value: $type) -> Self {
                DmapField::Scalar($enum_var(value))
            }
        }
        impl TryFrom<DmapField> for $type {
            type Error = DmapError;

            fn try_from(value: DmapField) -> std::result::Result<Self, Self::Error> {
                match value {
                    DmapField::Scalar(x) => x.try_into(),
                    _ => Err(Self::Error::InvalidScalar(format!(
                        "Cannot interpret {value:?} as {}",
                        stringify!($type)
                    ))),
                }
            }
        }
    };
}

scalar_impls!(i8, DmapScalar::Char, Type::Char);
scalar_impls!(i16, DmapScalar::Short, Type::Short);
scalar_impls!(i32, DmapScalar::Int, Type::Int);
scalar_impls!(i64, DmapScalar::Long, Type::Long);
scalar_impls!(u8, DmapScalar::Uchar, Type::Uchar);
scalar_impls!(u16, DmapScalar::Ushort, Type::Ushort);
scalar_impls!(u32, DmapScalar::Uint, Type::Uint);
scalar_impls!(u64, DmapScalar::Ulong, Type::Ulong);
scalar_impls!(f32, DmapScalar::Float, Type::Float);
scalar_impls!(f64, DmapScalar::Double, Type::Double);
scalar_impls!(String, DmapScalar::String, Type::String);

/// Trait for raw types that can be stored in DMAP files.
pub trait DmapType: std::fmt::Debug {
    /// Size in bytes of the type.
    fn size() -> usize
    where
        Self: Sized;
    /// Create a copy of the data as raw bytes.
    fn as_bytes(&self) -> Vec<u8>;
    /// Convert raw bytes to `Self`
    ///
    /// # Errors
    /// If the bytes are not a valid DMAP record of type `Self`.
    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
    /// Get the `Type` variant that represents `self`
    fn dmap_type() -> Type;
}

/// Macro for implementing [`DmapType`] trait for primitive types.
/// Example: `type_impls!(i8, Type::Char, 1)`
macro_rules! type_impls {
    // This variant captures single-byte types
    ($type:ty, $enum_var:path, 1) => {
        impl DmapType for $type {
            #[inline]
            fn size() -> usize { 1 }

            #[inline]
            fn as_bytes(&self) -> Vec<u8> {
                AsBytes::as_bytes(self).to_vec()
            }

            #[inline]
            fn from_bytes(bytes: &[u8]) -> Result<Self>
            where
                Self: Sized,
            {
                Self::read_from(bytes).ok_or(DmapError::CorruptStream("Unable to interpret bytes"))
            }

            #[inline]
            fn dmap_type() -> Type { $enum_var }
        }
    };
    // This variant captures multi-byte primitive types
    ($type:ty, $enum_var:path, $num_bytes:expr) => {
        paste! {
            impl DmapType for $type {
                #[inline]
                fn size() -> usize { $num_bytes }

                #[inline]
                fn as_bytes(&self) -> Vec<u8> {
                    let mut bytes = [0; $num_bytes];
                    LittleEndian::[< write_ $type >](&mut bytes, *self);
                    bytes.to_vec()
                }

                #[inline]
                fn from_bytes(bytes: &[u8]) -> Result<Self>
                where
                    Self: Sized,
                {
                    Self::read_from(bytes).ok_or(DmapError::CorruptStream("Unable to interpret bytes"))
                }

                #[inline]
                fn dmap_type() -> Type { $enum_var }
            }
        }
    }
}

type_impls!(i8, Type::Char, 1);
type_impls!(i16, Type::Short, 2);
type_impls!(i32, Type::Int, 4);
type_impls!(i64, Type::Long, 8);
type_impls!(u8, Type::Uchar, 1);
type_impls!(u16, Type::Ushort, 2);
type_impls!(u32, Type::Uint, 4);
type_impls!(u64, Type::Ulong, 8);
type_impls!(f32, Type::Float, 4);
type_impls!(f64, Type::Double, 8);

// This implementation differs significantly from the others, so it doesn't use the macro
impl DmapType for String {
    #[inline]
    fn size() -> usize {
        0
    }

    #[inline]
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = self.as_bytes().to_vec();
        bytes.push(0); // null-terminate
        bytes
    }

    #[inline]
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let data = String::from_utf8(bytes.to_owned())
            .map_err(|_| DmapError::InvalidScalar("Cannot convert bytes to String".to_string()))?;
        Ok(data.trim_end_matches(char::from(0)).to_string())
    }

    #[inline]
    fn dmap_type() -> Type {
        Type::String
    }
}

impl TryFrom<DmapScalar> for u8 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(u8::try_from(x)?),
            DmapScalar::Short(x) => Ok(u8::try_from(x)?),
            DmapScalar::Int(x) => Ok(u8::try_from(x)?),
            DmapScalar::Long(x) => Ok(u8::try_from(x)?),
            DmapScalar::Uchar(x) => Ok(x),
            DmapScalar::Ushort(x) => Ok(u8::try_from(x)?),
            DmapScalar::Uint(x) => Ok(u8::try_from(x)?),
            DmapScalar::Ulong(x) => Ok(u8::try_from(x)?),
            DmapScalar::Float(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f32 to u8"
            ))),
            DmapScalar::Double(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f64 to u8"
            ))),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to u8"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for u16 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(u16::try_from(x)?),
            DmapScalar::Short(x) => Ok(u16::try_from(x)?),
            DmapScalar::Int(x) => Ok(u16::try_from(x)?),
            DmapScalar::Long(x) => Ok(u16::try_from(x)?),
            DmapScalar::Uchar(x) => Ok(x as u16),
            DmapScalar::Ushort(x) => Ok(x),
            DmapScalar::Uint(x) => Ok(u16::try_from(x)?),
            DmapScalar::Ulong(x) => Ok(u16::try_from(x)?),
            DmapScalar::Float(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f32 to u16"
            ))),
            DmapScalar::Double(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f64 to u16"
            ))),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to u16"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for u32 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(u32::try_from(x)?),
            DmapScalar::Short(x) => Ok(u32::try_from(x)?),
            DmapScalar::Int(x) => Ok(u32::try_from(x)?),
            DmapScalar::Long(x) => Ok(u32::try_from(x)?),
            DmapScalar::Uchar(x) => Ok(x as u32),
            DmapScalar::Ushort(x) => Ok(x as u32),
            DmapScalar::Uint(x) => Ok(x),
            DmapScalar::Ulong(x) => Ok(u32::try_from(x)?),
            DmapScalar::Float(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f32 to u32"
            ))),
            DmapScalar::Double(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f64 to u32"
            ))),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to u32"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for u64 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(u64::try_from(x)?),
            DmapScalar::Short(x) => Ok(u64::try_from(x)?),
            DmapScalar::Int(x) => Ok(u64::try_from(x)?),
            DmapScalar::Long(x) => Ok(u64::try_from(x)?),
            DmapScalar::Uchar(x) => Ok(x as u64),
            DmapScalar::Ushort(x) => Ok(x as u64),
            DmapScalar::Uint(x) => Ok(x as u64),
            DmapScalar::Ulong(x) => Ok(x),
            DmapScalar::Float(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f32 to u64"
            ))),
            DmapScalar::Double(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f64 to u64"
            ))),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to u64"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for i8 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(x),
            DmapScalar::Short(x) => Ok(i8::try_from(x)?),
            DmapScalar::Int(x) => Ok(i8::try_from(x)?),
            DmapScalar::Long(x) => Ok(i8::try_from(x)?),
            DmapScalar::Uchar(x) => Ok(i8::try_from(x)?),
            DmapScalar::Ushort(x) => Ok(i8::try_from(x)?),
            DmapScalar::Uint(x) => Ok(i8::try_from(x)?),
            DmapScalar::Ulong(x) => Ok(i8::try_from(x)?),
            DmapScalar::Float(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f32 to i8"
            ))),
            DmapScalar::Double(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f64 to i8"
            ))),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to i8"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for i16 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(x as i16),
            DmapScalar::Short(x) => Ok(x),
            DmapScalar::Int(x) => Ok(i16::try_from(x)?),
            DmapScalar::Long(x) => Ok(i16::try_from(x)?),
            DmapScalar::Uchar(x) => Ok(x as i16),
            DmapScalar::Ushort(x) => Ok(i16::try_from(x)?),
            DmapScalar::Uint(x) => Ok(i16::try_from(x)?),
            DmapScalar::Ulong(x) => Ok(i16::try_from(x)?),
            DmapScalar::Float(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f32 to i16"
            ))),
            DmapScalar::Double(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f64 to i16"
            ))),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to i16"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for i32 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(x as i32),
            DmapScalar::Short(x) => Ok(x as i32),
            DmapScalar::Int(x) => Ok(x),
            DmapScalar::Long(x) => Ok(i32::try_from(x)?),
            DmapScalar::Uchar(x) => Ok(x as i32),
            DmapScalar::Ushort(x) => Ok(x as i32),
            DmapScalar::Uint(x) => Ok(i32::try_from(x)?),
            DmapScalar::Ulong(x) => Ok(i32::try_from(x)?),
            DmapScalar::Float(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f32 to i32"
            ))),
            DmapScalar::Double(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f64 to i32"
            ))),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to i32"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for i64 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(x as i64),
            DmapScalar::Short(x) => Ok(x as i64),
            DmapScalar::Int(x) => Ok(x as i64),
            DmapScalar::Long(x) => Ok(x),
            DmapScalar::Uchar(x) => Ok(x as i64),
            DmapScalar::Ushort(x) => Ok(x as i64),
            DmapScalar::Uint(x) => Ok(x as i64),
            DmapScalar::Ulong(x) => Ok(i64::try_from(x)?),
            DmapScalar::Float(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f32 to i64"
            ))),
            DmapScalar::Double(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x}_f64 to i64"
            ))),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to i64"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for f32 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(x as f32),
            DmapScalar::Short(x) => Ok(x as f32),
            DmapScalar::Int(x) => Ok(x as f32),
            DmapScalar::Long(x) => Ok(x as f32),
            DmapScalar::Uchar(x) => Ok(x as f32),
            DmapScalar::Ushort(x) => Ok(x as f32),
            DmapScalar::Uint(x) => Ok(x as f32),
            DmapScalar::Ulong(x) => Ok(x as f32),
            DmapScalar::Float(x) => Ok(x),
            DmapScalar::Double(x) => Ok(x as f32),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to f32"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for f64 {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::Char(x) => Ok(x as f64),
            DmapScalar::Short(x) => Ok(x as f64),
            DmapScalar::Int(x) => Ok(x as f64),
            DmapScalar::Long(x) => Ok(x as f64),
            DmapScalar::Uchar(x) => Ok(x as f64),
            DmapScalar::Ushort(x) => Ok(x as f64),
            DmapScalar::Uint(x) => Ok(x as f64),
            DmapScalar::Ulong(x) => Ok(x as f64),
            DmapScalar::Float(x) => Ok(f64::from(x)),
            DmapScalar::Double(x) => Ok(x),
            DmapScalar::String(x) => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to f64"
            ))),
        }
    }
}
impl TryFrom<DmapScalar> for String {
    type Error = DmapError;
    fn try_from(value: DmapScalar) -> std::result::Result<Self, Self::Error> {
        match value {
            DmapScalar::String(x) => Ok(x),
            x => Err(DmapError::InvalidScalar(format!(
                "Unable to convert {x} to String"
            ))),
        }
    }
}

/// Verify that `name` exists in `fields` and is of the correct [`Type`].
///
/// # Errors
/// If `name` is not in `fields`.
///
/// If `name` is in `fields`, but is not a [`DmapField::Scalar`] of `expected_type`.
pub fn check_scalar(
    fields: &IndexMap<String, DmapField>,
    name: &str,
    expected_type: &Type,
) -> Result<()> {
    match fields.get(name) {
        Some(DmapField::Scalar(data)) if data.get_type() == *expected_type => Ok(()),
        Some(DmapField::Scalar(data)) => Err(DmapError::InvalidScalar(format!(
            "{name} is of type {}, expected {}",
            data.get_type(),
            expected_type
        ))),
        Some(_) => Err(DmapError::InvalidScalar(format!(
            "{name} is a vector field"
        ))),
        None => Err(DmapError::InvalidScalar(format!("{name} is not in record"))),
    }
}

/// If `name` is in `fields`, verify that it is of the correct [`Type`].
///
/// # Errors
/// If `name` is in `fields`, but is not a [`DmapField::Scalar`] of `expected_type`.
pub fn check_scalar_opt(
    fields: &IndexMap<String, DmapField>,
    name: &str,
    expected_type: &Type,
) -> Result<()> {
    match fields.get(name) {
        Some(DmapField::Scalar(data)) if data.get_type() == *expected_type => Ok(()),
        Some(DmapField::Scalar(data)) => Err(DmapError::InvalidScalar(format!(
            "{name} is of type {}, expected {}",
            data.get_type(),
            expected_type
        ))),
        Some(_) => Err(DmapError::InvalidScalar(format!(
            "{name} is a vector field"
        ))),
        None => Ok(()),
    }
}

/// Verify that `name` exists in `fields` and is of the correct [`Type`].
///
/// # Errors
/// If `name` is not in `fields`.
///
/// If `name` is in `fields`, but is not a [`DmapField::Vector`] of `expected_type`.
pub fn check_vector(
    fields: &IndexMap<String, DmapField>,
    name: &str,
    expected_type: &Type,
) -> Result<()> {
    match fields.get(name) {
        Some(DmapField::Vector(data)) if data.get_type() != *expected_type => {
            Err(DmapError::InvalidVector(format!(
                "{name} is of type {}, expected {}",
                data.get_type(),
                expected_type
            )))
        }
        Some(DmapField::Scalar(_)) => Err(DmapError::InvalidVector(format!(
            "{name} is a scalar field"
        ))),
        None => Err(DmapError::InvalidVector(format!("{name} not in record"))),
        _ => Ok(()),
    }
}

/// If `name` is in `fields`, verify that it is of the correct [`Type`].
///
/// # Errors
/// If `name` is in `fields`, but is not a [`DmapField::Vector`] of `expected_type`.
pub fn check_vector_opt(
    fields: &IndexMap<String, DmapField>,
    name: &str,
    expected_type: &Type,
) -> Result<()> {
    match fields.get(name) {
        Some(DmapField::Vector(data)) if data.get_type() != *expected_type => {
            Err(DmapError::InvalidVector(format!(
                "{name} is of type {}, expected {}",
                data.get_type(),
                expected_type
            )))
        }
        Some(DmapField::Scalar(_)) => Err(DmapError::InvalidVector(format!(
            "{name} is a scalar field"
        ))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    #[test]
    fn dmaptype() -> Result<()> {
        assert_eq!(i8::size(), 1);
        assert_eq!(u8::size(), 1);
        assert_eq!(i16::size(), 2);
        assert_eq!(u16::size(), 2);
        assert_eq!(i32::size(), 4);
        assert_eq!(u32::size(), 4);
        assert_eq!(f32::size(), 4);
        assert_eq!(i64::size(), 8);
        assert_eq!(u64::size(), 8);
        assert_eq!(f64::size(), 8);

        assert_eq!(i8::dmap_type(), Type::Char);
        assert_eq!(u8::dmap_type(), Type::Uchar);
        assert_eq!(i16::dmap_type(), Type::Short);
        assert_eq!(u16::dmap_type(), Type::Ushort);
        assert_eq!(i32::dmap_type(), Type::Int);
        assert_eq!(u32::dmap_type(), Type::Uint);
        assert_eq!(f32::dmap_type(), Type::Float);
        assert_eq!(i64::dmap_type(), Type::Long);
        assert_eq!(u64::dmap_type(), Type::Ulong);
        assert_eq!(f64::dmap_type(), Type::Double);

        assert_eq!(vec![1], DmapType::as_bytes(&i8::from_bytes(&[1])?));
        assert_eq!(vec![1], DmapType::as_bytes(&u8::from_bytes(&[1])?));
        assert_eq!(vec![1, 0], DmapType::as_bytes(&i16::from_bytes(&[1, 0])?));
        assert_eq!(vec![1, 0], DmapType::as_bytes(&u16::from_bytes(&[1, 0])?));
        assert_eq!(
            vec![1, 0, 0, 0],
            DmapType::as_bytes(&i32::from_bytes(&[1, 0, 0, 0])?)
        );
        assert_eq!(
            vec![1, 2, 3, 4],
            DmapType::as_bytes(&u32::from_bytes(&[1, 2, 3, 4])?)
        );
        assert_eq!(
            vec![1, 0, 0, 0],
            DmapType::as_bytes(&f32::from_bytes(&[1, 0, 0, 0])?)
        );
        assert_eq!(
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            DmapType::as_bytes(&u64::from_bytes(&[1, 2, 3, 4, 5, 6, 7, 8])?)
        );
        assert_eq!(
            vec![1, 0, 0, 0, 1, 2, 3, 4],
            DmapType::as_bytes(&i64::from_bytes(&[1, 0, 0, 0, 1, 2, 3, 4])?)
        );
        assert_eq!(
            vec![1, 2, 3, 4, 4, 32, 2, 1],
            DmapType::as_bytes(&f64::from_bytes(&[1, 2, 3, 4, 4, 32, 2, 1])?)
        );
        Ok(())
    }

    #[test]
    fn types() -> Result<()> {
        assert_eq!(Type::from_key(1)?, Type::Char);
        assert_eq!(Type::from_key(2)?, Type::Short);
        assert_eq!(Type::from_key(3)?, Type::Int);
        assert_eq!(Type::from_key(10)?, Type::Long);
        assert_eq!(Type::from_key(16)?, Type::Uchar);
        assert_eq!(Type::from_key(17)?, Type::Ushort);
        assert_eq!(Type::from_key(18)?, Type::Uint);
        assert_eq!(Type::from_key(19)?, Type::Ulong);
        assert_eq!(Type::from_key(4)?, Type::Float);
        assert_eq!(Type::from_key(8)?, Type::Double);
        assert_eq!(Type::from_key(9)?, Type::String);
        assert!(Type::from_key(-1).is_err());
        assert!(Type::from_key(15).is_err());
        assert!(Type::from_key(0).is_err());

        assert_eq!(Type::Char.key(), 1);
        assert_eq!(Type::Short.key(), 2);
        assert_eq!(Type::Int.key(), 3);
        assert_eq!(Type::Long.key(), 10);
        assert_eq!(Type::Uchar.key(), 16);
        assert_eq!(Type::Ushort.key(), 17);
        assert_eq!(Type::Uint.key(), 18);
        assert_eq!(Type::Ulong.key(), 19);
        assert_eq!(Type::Float.key(), 4);
        assert_eq!(Type::Double.key(), 8);
        assert_eq!(Type::String.key(), 9);

        assert_eq!(Type::Char.size(), 1);
        assert_eq!(Type::Short.size(), 2);
        assert_eq!(Type::Int.size(), 4);
        assert_eq!(Type::Long.size(), 8);
        assert_eq!(Type::Uchar.size(), 1);
        assert_eq!(Type::Ushort.size(), 2);
        assert_eq!(Type::Uint.size(), 4);
        assert_eq!(Type::Ulong.size(), 8);
        assert_eq!(Type::Float.size(), 4);
        assert_eq!(Type::Double.size(), 8);
        assert_eq!(Type::String.size(), 0);

        Ok(())
    }

    #[test]
    fn dmapscalar() -> Result<()> {
        assert_eq!(DmapScalar::Char(0).get_type(), Type::Char);
        assert_eq!(DmapScalar::Short(0).get_type(), Type::Short);
        assert_eq!(DmapScalar::Int(0).get_type(), Type::Int);
        assert_eq!(DmapScalar::Long(0).get_type(), Type::Long);
        assert_eq!(DmapScalar::Uchar(0).get_type(), Type::Uchar);
        assert_eq!(DmapScalar::Ushort(0).get_type(), Type::Ushort);
        assert_eq!(DmapScalar::Uint(0).get_type(), Type::Uint);
        assert_eq!(DmapScalar::Ulong(0).get_type(), Type::Ulong);
        assert_eq!(DmapScalar::Float(0.0).get_type(), Type::Float);
        assert_eq!(DmapScalar::Double(0.0).get_type(), Type::Double);
        assert_eq!(
            DmapScalar::String("test".to_string()).get_type(),
            Type::String
        );

        let x = DmapScalar::Char(-1);
        assert_eq!(x.cast_as(&Type::Short)?, DmapScalar::Short(-1));
        assert!(x.cast_as(&Type::Float).is_ok());
        assert!(x.cast_as(&Type::Uchar).is_err());
        assert!(x.cast_as(&Type::String).is_err());

        let x = DmapScalar::Uchar(255);
        assert_eq!(x.cast_as(&Type::Short)?, DmapScalar::Short(255));
        assert!(x.cast_as(&Type::Char).is_err());
        assert!(x.cast_as(&Type::Float).is_ok());
        assert!(x.cast_as(&Type::Uchar).is_ok());
        assert!(x.cast_as(&Type::String).is_err());

        let x = DmapScalar::Short(256);
        assert_eq!(x.cast_as(&Type::Short)?, DmapScalar::Short(256));
        assert!(x.cast_as(&Type::Char).is_err());
        assert_eq!(x.cast_as(&Type::Ushort)?, DmapScalar::Ushort(256));
        assert!(x.cast_as(&Type::Uchar).is_err());
        assert!(x.cast_as(&Type::Float).is_ok());
        assert!(x.cast_as(&Type::String).is_err());

        let x = DmapScalar::Float(1.0);
        assert!(x.cast_as(&Type::Double).is_ok());
        assert!(x.cast_as(&Type::Char).is_err());
        assert!(x.cast_as(&Type::Uchar).is_err());
        assert!(x.cast_as(&Type::Float).is_ok());
        assert!(x.cast_as(&Type::String).is_err());

        let x = DmapScalar::String("test".to_string());
        assert!(x.cast_as(&Type::Char).is_err());
        assert!(x.cast_as(&Type::Short).is_err());
        assert!(x.cast_as(&Type::Int).is_err());
        assert!(x.cast_as(&Type::Long).is_err());
        assert!(x.cast_as(&Type::Uchar).is_err());
        assert!(x.cast_as(&Type::Ushort).is_err());
        assert!(x.cast_as(&Type::Uint).is_err());
        assert!(x.cast_as(&Type::Ulong).is_err());
        assert!(x.cast_as(&Type::Float).is_err());
        assert!(x.cast_as(&Type::Double).is_err());

        assert_eq!(
            DmapScalar::Char(8).as_bytes(),
            vec![Type::Char.key() as u8, 8]
        );
        assert_eq!(
            DmapScalar::Short(256).as_bytes(),
            vec![Type::Short.key() as u8, 0, 1]
        );
        assert_eq!(
            DmapScalar::Int(256).as_bytes(),
            vec![Type::Int.key() as u8, 0, 1, 0, 0]
        );
        assert_eq!(
            DmapScalar::Long(512).as_bytes(),
            vec![Type::Long.key() as u8, 0, 2, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            DmapScalar::Uchar(8).as_bytes(),
            vec![Type::Uchar.key() as u8, 8]
        );
        assert_eq!(
            DmapScalar::Ushort(256).as_bytes(),
            vec![Type::Ushort.key() as u8, 0, 1]
        );
        assert_eq!(
            DmapScalar::Uint(256).as_bytes(),
            vec![Type::Uint.key() as u8, 0, 1, 0, 0]
        );
        assert_eq!(
            DmapScalar::Ulong(512).as_bytes(),
            vec![Type::Ulong.key() as u8, 0, 2, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            DmapScalar::Float(0.0).as_bytes(),
            vec![Type::Float.key() as u8, 0, 0, 0, 0]
        );
        assert_eq!(
            DmapScalar::Double(0.0).as_bytes(),
            vec![Type::Double.key() as u8, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            DmapScalar::String("test".to_string()).as_bytes(),
            vec![Type::String.key() as u8, 116, 101, 115, 116, 0]
        );

        Ok(())
    }

    #[test]
    fn dmapvec() -> Result<()> {
        let arr = DmapVec::Char(array![0, 1, 2, 3, 4].into_dyn());
        assert_eq!(arr.get_type(), Type::Char);
        let arr = DmapVec::Uchar(array![0, 1, 2, 3, 4].into_dyn());
        assert_eq!(arr.get_type(), Type::Uchar);
        let arr = DmapVec::Short(array![0, 1, 2, 3, 4].into_dyn());
        assert_eq!(arr.get_type(), Type::Short);
        let arr = DmapVec::Ushort(array![0, 1, 2, 3, 4].into_dyn());
        assert_eq!(arr.get_type(), Type::Ushort);
        let arr = DmapVec::Int(array![0, 1, 2, 3, 4].into_dyn());
        assert_eq!(arr.get_type(), Type::Int);
        let arr = DmapVec::Uint(array![[0, 1, 2], [3, 4, 5]].into_dyn());
        assert_eq!(arr.get_type(), Type::Uint);
        let arr = DmapVec::Long(array![0, 1, 2, 3, 4].into_dyn());
        assert_eq!(arr.get_type(), Type::Long);
        let arr = DmapVec::Ulong(array![0, 1, 2, 3, 4].into_dyn());
        assert_eq!(arr.get_type(), Type::Ulong);
        let arr = DmapVec::Float(array![0.0, 1.0, 2.0, 3.0, 4.0].into_dyn());
        assert_eq!(arr.get_type(), Type::Float);
        let arr = DmapVec::Double(array![0.0, 1.0, 2.0, 3.0, 4.0].into_dyn());
        assert_eq!(arr.get_type(), Type::Double);

        Ok(())
    }

    #[test]
    fn check_fields_in_indexmap() -> Result<()> {
        use ndarray::array;

        let mut rec = IndexMap::<String, DmapField>::new();
        let res = check_scalar(&rec, "test", &Type::Char);
        assert!(res.is_err());
        let res = check_scalar_opt(&rec, "test", &Type::Char);
        assert!(res.is_ok());
        let res = check_vector(&rec, "test", &Type::Char);
        assert!(res.is_err());
        let res = check_vector_opt(&rec, "test", &Type::Char);
        assert!(res.is_ok());

        let res = rec.insert("test".to_string(), DmapField::from(1i32));
        assert!(res.is_none());
        let res = check_scalar(&rec, "test", &Type::Int);
        assert!(res.is_ok());
        let res = check_scalar_opt(&rec, "test", &Type::Char);
        assert!(res.is_err());
        let res = check_scalar_opt(&rec, "test", &Type::Int);
        assert!(res.is_ok());
        let res = check_vector(&rec, "test", &Type::Char);
        assert!(res.is_err());
        let res = check_vector_opt(&rec, "test", &Type::Char);
        assert!(res.is_err());

        let test_vec = array![1.0f32, 2.0f32].into_dyn();
        let res = rec.insert("test_vec".to_string(), test_vec.into());
        assert!(res.is_none());
        let res = check_scalar(&rec, "test_vec", &Type::Float);
        assert!(res.is_err());
        let res = check_scalar_opt(&rec, "test_vec", &Type::Float);
        assert!(res.is_err());
        let res = check_vector(&rec, "test_vec", &Type::Float);
        assert!(res.is_ok());
        let res = check_vector(&rec, "test_vec", &Type::Double);
        assert!(res.is_err());
        let res = check_vector_opt(&rec, "test_vec", &Type::Float);
        assert!(res.is_ok());
        let res = check_vector_opt(&rec, "test_vec", &Type::Int);
        assert!(res.is_err());

        Ok(())
    }
}
