use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{DmapField, DmapType, Fields, Type};
use indexmap::IndexMap;
use lazy_static::lazy_static;
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

static VECTOR_FIELDS: [(&str, Type); 18] = [
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
];

static VECTOR_FIELDS_OPT: [(&str, Type); 13] = [
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
    ("vector.srng", Type::Float)
];

lazy_static! {
    static ref MATCHED_VECS: Vec<Vec<&'static str>> = vec![
        vec![
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
        ],
        vec![
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
        ],
    ];
    static ref GRID_FIELDS: Fields<'static> = Fields {
        all_fields: {
            let mut fields: Vec<&str> = vec![];
            fields.extend(SCALAR_FIELDS.clone().into_iter().map(|x| x.0));
            fields.extend(SCALAR_FIELDS_OPT.clone().into_iter().map(|x| x.0));
            fields.extend(VECTOR_FIELDS.clone().into_iter().map(|x| x.0));
            fields.extend(VECTOR_FIELDS_OPT.clone().into_iter().map(|x| x.0));
            fields
        },
        scalars_required: SCALAR_FIELDS.to_vec(),
        scalars_optional: SCALAR_FIELDS_OPT.to_vec(),
        vectors_required: VECTOR_FIELDS.to_vec(),
        vectors_optional: VECTOR_FIELDS_OPT.to_vec(),
        vector_dim_groups: MATCHED_VECS.clone(),
    };
}

#[derive(Debug, PartialEq, Clone)]
pub struct GridRecord {
    pub(crate) data: IndexMap<String, DmapField>,
}

impl Record for GridRecord {
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<GridRecord, DmapError> {
        match Self::check_fields(fields, &GRID_FIELDS) {
            Ok(_) => {}
            Err(e) => Err(e)?,
        }

        Ok(GridRecord {
            data: fields.to_owned(),
        })
    }
    fn to_bytes(&self) -> Result<Vec<u8>, DmapError> {
        let (num_scalars, num_vectors, mut data_bytes) =
            Self::data_to_bytes(&self.data, &GRID_FIELDS)?;

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
        Ok(Self::coerce::<GridRecord>(value, &GRID_FIELDS)?)
    }
}
