use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{DmapField, DmapType, Type};
use indexmap::IndexMap;
use std::convert::TryFrom;

static SCALAR_FIELDS: [(&str, Type); 12] = [
    ("start.year", Type::Short),
    ("start.month", Type::Short),
    ("start.day", Type::Short),
    ("start.hour", Type::Short),
    ("start.minute", Type::Short),
    ("start.second", Type::Double),
    ("end.year", Type::Short),
    ("end.month", Type::Short),
    ("end.day", Type::Short),
    ("end.hour", Type::Short),
    ("end.minute", Type::Short),
    ("end.second", Type::Double),
];

static SCALAR_FIELDS_OPT: [(&str, Type); 0] = [];

static VECTOR_FIELDS: [(&str, Type); 30] = [
    ("stid", Type::Short),
    ("channel", Type::Short),
    ("nvec", Type::Short),
    ("freq", Type::Float),
    ("major.revision", Type::Short),
    ("minor.revision", Type::Short),
    ("program.id", Type::Short),
    ("noise.mean", Type::Float),
    ("noise.sd", Type::Float),
    ("gsct", Type::Short),
    ("v.min", Type::Float),
    ("v.max", Type::Float),
    ("p.min", Type::Float),
    ("p.max", Type::Float),
    ("w.min", Type::Float),
    ("w.max", Type::Float),
    ("ve.min", Type::Float),
    ("ve.max", Type::Float),
    ("vector.mlat", Type::Float),
    ("vector.mlon", Type::Float),
    ("vector.kvect", Type::Float),
    ("vector.stid", Type::Short),
    ("vector.channel", Type::Short),
    ("vector.index", Type::Int),
    ("vector.vel.median", Type::Float),
    ("vector.vel.sd", Type::Float),
    ("vector.pwr.median", Type::Float),
    ("vector.pwr.sd", Type::Float),
    ("vector.wdt.median", Type::Float),
    ("vector.wdt.sd", Type::Float),
];

static VECTOR_FIELDS_OPT: [(&str, Type); 0] = [];

static GRID_FIELDS: [&str; 42] = [
    "start.year",
    "start.month",
    "start.day",
    "start.hour",
    "start.minute",
    "start.second",
    "end.year",
    "end.month",
    "end.day",
    "end.hour",
    "end.minute",
    "end.second",
    "stid",
    "channel",
    "nvec",
    "freq",
    "major.revision",
    "minor.revision",
    "program.id",
    "noise.mean",
    "noise.sd",
    "gsct",
    "v.min",
    "v.max",
    "p.min",
    "p.max",
    "w.min",
    "w.max",
    "ve.min",
    "ve.max",
    "vector.mlat",
    "vector.mlon",
    "vector.kvect",
    "vector.stid",
    "vector.channel",
    "vector.index",
    "vector.vel.median",
    "vector.vel.sd",
    "vector.pwr.median",
    "vector.pwr.sd",
    "vector.wdt.median",
    "vector.wdt.sd",
];

#[derive(Debug, PartialEq)]
pub struct GridRecord {
    pub(crate) data: IndexMap<String, DmapField>,
}

impl Record for GridRecord {
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<GridRecord, DmapError> {
        match Self::check_fields(
            fields,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
            &GRID_FIELDS,
        ) {
            Ok(_) => {}
            Err(e) => Err(e)?,
        }

        Ok(GridRecord {
            data: fields.to_owned(),
        })
    }
    fn to_bytes(&self) -> Result<Vec<u8>, DmapError> {
        let (num_scalars, num_vectors, mut data_bytes) = Self::data_to_bytes(
            &self.data,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
        )?;

        let mut bytes: Vec<u8> = vec![];
        bytes.extend((65537_i32).as_bytes()); // No idea why this is what it is, copied from backscatter
        bytes.extend((data_bytes.len() as i32 + 16).as_bytes()); // +16 for code, length, num_scalars, num_vectors
        bytes.extend(num_scalars.as_bytes());
        bytes.extend(num_vectors.as_bytes());
        bytes.append(&mut data_bytes); // consumes data_bytes
        Ok(bytes)
    }
}

impl TryFrom<&mut IndexMap<String, DmapField>> for GridRecord {
    type Error = DmapError;

    fn try_from(value: &mut IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
        Ok(Self::coerce::<GridRecord>(
            value,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
            &GRID_FIELDS,
        )?)
    }
}
