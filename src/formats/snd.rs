use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{DmapField, DmapType, Type};
use indexmap::IndexMap;
use std::convert::TryFrom;

static SCALAR_FIELDS: [(&str, Type); 37] = [
    ("radar.revision.major", Type::Char),
    ("radar.revision.minor", Type::Char),
    ("origin.code", Type::Char),
    ("origin.time", Type::String),
    ("origin.command", Type::String),
    ("cp", Type::Short),
    ("stid", Type::Short),
    ("time.yr", Type::Short),
    ("time.mo", Type::Short),
    ("time.dy", Type::Short),
    ("time.hr", Type::Short),
    ("time.mt", Type::Short),
    ("time.sc", Type::Short),
    ("time.us", Type::Int),
    ("nave", Type::Short),
    ("lagfr", Type::Short),
    ("smsep", Type::Short),
    ("noise.search", Type::Float),
    ("noise.mean", Type::Float),
    ("channel", Type::Short),
    ("bmnum", Type::Short),
    ("bmazm", Type::Float),
    ("scan", Type::Short),
    ("rxrise", Type::Short),
    ("intt.sc", Type::Short),
    ("intt.us", Type::Int),
    ("nrang", Type::Short),
    ("frang", Type::Short),
    ("rsep", Type::Short),
    ("xcf", Type::Short),
    ("tfreq", Type::Short),
    ("noise.sky", Type::Float),
    ("combf", Type::String),
    ("fitacf.revision.major", Type::Int),
    ("fitacf.revision.minor", Type::Int),
    ("snd.revision.major", Type::Short),
    ("snd.revision.minor", Type::Short),
];

static SCALAR_FIELDS_OPT: [(&str, Type); 0] = [];

static VECTOR_FIELDS: [(&str, Type); 7] = [
    ("slist", Type::Short),
    ("qflg", Type::Char),
    ("gflg", Type::Char),
    ("v", Type::Float),
    ("v_e", Type::Float),
    ("p_l", Type::Float),
    ("w_l", Type::Float),
];

static VECTOR_FIELDS_OPT: [(&str, Type); 3] = [
    ("x_qflg", Type::Char),
    ("phi0", Type::Float),
    ("phi0_e", Type::Float),
];

static SND_FIELDS: [&str; 47] = [
    "radar.revision.major",
    "radar.revision.minor",
    "origin.code",
    "origin.time",
    "origin.command",
    "cp",
    "stid",
    "time.yr",
    "time.mo",
    "time.dy",
    "time.hr",
    "time.mt",
    "time.sc",
    "time.us",
    "nave",
    "lagfr",
    "smsep",
    "noise.search",
    "noise.mean",
    "channel",
    "bmnum",
    "bmazm",
    "scan",
    "rxrise",
    "intt.sc",
    "intt.us",
    "nrang",
    "frang",
    "rsep",
    "xcf",
    "tfreq",
    "noise.sky",
    "combf",
    "fitacf.revision.major",
    "fitacf.revision.minor",
    "snd.revision.major",
    "snd.revision.minor",
    "slist",
    "qflg",
    "gflg",
    "v",
    "v_e",
    "p_l",
    "w_l",
    "x_qflg",
    "phi0",
    "phi0_e",
];

#[derive(Debug, PartialEq)]
pub struct SndRecord {
    pub(crate) data: IndexMap<String, DmapField>,
}

impl Record for SndRecord {
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<SndRecord, DmapError> {
        match Self::check_fields(
            fields,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
            &SND_FIELDS,
        ) {
            Ok(_) => {}
            Err(e) => Err(e)?,
        }

        Ok(SndRecord {
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

impl TryFrom<&mut IndexMap<String, DmapField>> for SndRecord {
    type Error = DmapError;

    fn try_from(value: &mut IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
        Ok(Self::coerce::<SndRecord>(
            value,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
            &SND_FIELDS,
        )?)
    }
}
