use crate::types::{DmapField, DmapScalar, DmapVec};

use numpy::{PyArray, PyArrayMethods};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{Bound, FromPyObject, PyAny, PyResult, Python};

impl<'py> IntoPyObject<'py> for DmapScalar {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            Self::Char(x) => x.into_pyobject(py)?.into_any(),
            Self::Short(x) => x.into_pyobject(py)?.into_any(),
            Self::Int(x) => x.into_pyobject(py)?.into_any(),
            Self::Long(x) => x.into_pyobject(py)?.into_any(),
            Self::Uchar(x) => x.into_pyobject(py)?.into_any(),
            Self::Ushort(x) => x.into_pyobject(py)?.into_any(),
            Self::Uint(x) => x.into_pyobject(py)?.into_any(),
            Self::Ulong(x) => x.into_pyobject(py)?.into_any(),
            Self::Float(x) => x.into_pyobject(py)?.into_any(),
            Self::Double(x) => x.into_pyobject(py)?.into_any(),
            Self::String(x) => x.into_pyobject(py)?.into_any(),
        })
    }
}

impl<'py> FromPyObject<'py> for DmapScalar {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(x) = ob.extract::<String>() {
            Ok(DmapScalar::String(x))
        } else if let Ok(x) = ob.extract::<i8>() {
            Ok(DmapScalar::Char(x))
        } else if let Ok(x) = ob.extract::<i16>() {
            Ok(DmapScalar::Short(x))
        } else if let Ok(x) = ob.extract::<i32>() {
            Ok(DmapScalar::Int(x))
        } else if let Ok(x) = ob.extract::<i64>() {
            Ok(DmapScalar::Long(x))
        } else if let Ok(x) = ob.extract::<u8>() {
            Ok(DmapScalar::Uchar(x))
        } else if let Ok(x) = ob.extract::<u16>() {
            Ok(DmapScalar::Ushort(x))
        } else if let Ok(x) = ob.extract::<u32>() {
            Ok(DmapScalar::Uint(x))
        } else if let Ok(x) = ob.extract::<u64>() {
            Ok(DmapScalar::Ulong(x))
        } else if let Ok(x) = ob.extract::<f32>() {
            Ok(DmapScalar::Float(x))
        } else if let Ok(x) = ob.extract::<f64>() {
            Ok(DmapScalar::Double(x))
        } else {
            Err(PyValueError::new_err("Could not extract scalar"))
        }
    }
}

impl<'py> IntoPyObject<'py> for DmapVec {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> std::result::Result<Self::Output, Self::Error> {
        Ok(match self {
            DmapVec::Char(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Short(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Int(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Long(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Uchar(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Ushort(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Uint(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Ulong(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Float(x) => PyArray::from_owned_array(py, x).into_any(),
            DmapVec::Double(x) => PyArray::from_owned_array(py, x).into_any(),
        })
    }
}

impl<'py> FromPyObject<'py> for DmapVec {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(x) = ob.downcast::<PyArray<u8, _>>() {
            Ok(DmapVec::Uchar(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<u16, _>>() {
            Ok(DmapVec::Ushort(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<u32, _>>() {
            Ok(DmapVec::Uint(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<u64, _>>() {
            Ok(DmapVec::Ulong(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<i8, _>>() {
            Ok(DmapVec::Char(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<i16, _>>() {
            Ok(DmapVec::Short(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<i32, _>>() {
            Ok(DmapVec::Int(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<i64, _>>() {
            Ok(DmapVec::Long(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<f32, _>>() {
            Ok(DmapVec::Float(x.to_owned_array()))
        } else if let Ok(x) = ob.downcast::<PyArray<f64, _>>() {
            Ok(DmapVec::Double(x.to_owned_array()))
        } else {
            Err(PyValueError::new_err("Could not extract vector"))
        }
    }
}

impl<'py> IntoPyObject<'py> for DmapField {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            DmapField::Scalar(x) => x.into_pyobject(py)?.into_any(),
            DmapField::Vector(x) => x.into_pyobject(py)?.into_any(),
        })
    }
}

impl<'py> FromPyObject<'py> for DmapField {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(x) = ob.extract::<DmapVec>() {
            Ok(DmapField::Vector(x))
        } else if let Ok(x) = ob.extract::<DmapScalar>() {
            Ok(DmapField::Scalar(x))
        } else {
            Err(PyValueError::new_err("Could not extract field"))
        }
    }
}
