use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{DmapField, DmapType, Type};
use indexmap::IndexMap;
use std::convert::TryFrom;

static SCALAR_FIELDS: [(&str, Type); 50] = [
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
    ("txpow", Type::Short),
    ("nave", Type::Short),
    ("atten", Type::Short),
    ("lagfr", Type::Short),
    ("smsep", Type::Short),
    ("ercod", Type::Short),
    ("stat.agc", Type::Short),
    ("stat.lopwr", Type::Short),
    ("noise.search", Type::Float),
    ("noise.mean", Type::Float),
    ("channel", Type::Short),
    ("bmnum", Type::Short),
    ("bmazm", Type::Float),
    ("scan", Type::Short),
    ("offset", Type::Short),
    ("rxrise", Type::Short),
    ("intt.sc", Type::Short),
    ("intt.us", Type::Int),
    ("txpl", Type::Short),
    ("mpinc", Type::Short),
    ("mppul", Type::Short),
    ("mplgs", Type::Short),
    ("nrang", Type::Short),
    ("frang", Type::Short),
    ("rsep", Type::Short),
    ("xcf", Type::Short),
    ("tfreq", Type::Short),
    ("mxpwr", Type::Int),
    ("lvmax", Type::Int),
    ("combf", Type::String),
    ("iqdata.revision.major", Type::Int),
    ("iqdata.revision.minor", Type::Int),
    ("seqnum", Type::Int),
    ("chnnum", Type::Int),
    ("smpnum", Type::Int),
    ("skpnum", Type::Int),
];

static SCALAR_FIELDS_OPT: [(&str, Type); 2] = [("mplgexs", Type::Short), ("ifmode", Type::Short)];

static VECTOR_FIELDS: [(&str, Type); 9] = [
    ("ptab", Type::Short),
    ("ltab", Type::Short),
    ("tsc", Type::Int),
    ("tus", Type::Int),
    ("tatten", Type::Short),
    ("tnoise", Type::Float),
    ("toff", Type::Int),
    ("tsze", Type::Int),
    ("data", Type::Short),
];

static VECTOR_FIELDS_OPT: [(&str, Type); 0] = [];

static IQDAT_FIELDS: [&str; 61] = [
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
    "txpow",
    "nave",
    "atten",
    "lagfr",
    "smsep",
    "ercod",
    "stat.agc",
    "stat.lopwr",
    "noise.search",
    "noise.mean",
    "channel",
    "bmnum",
    "bmazm",
    "scan",
    "offset",
    "rxrise",
    "intt.sc",
    "intt.us",
    "txpl",
    "mpinc",
    "mppul",
    "mplgs",
    "nrang",
    "frang",
    "rsep",
    "xcf",
    "tfreq",
    "mxpwr",
    "lvmax",
    "combf",
    "iqdata.revision.major",
    "iqdata.revision.minor",
    "seqnum",
    "chnnum",
    "smpnum",
    "skpnum",
    "mplgexs",
    "ifmode",
    "ptab",
    "ltab",
    "tsc",
    "tus",
    "tatten",
    "tnoise",
    "toff",
    "tsze",
    "data",
];

#[derive(Debug, PartialEq)]
pub struct IqdatRecord {
    pub(crate) data: IndexMap<String, DmapField>,
}

impl Record for IqdatRecord {
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<IqdatRecord, DmapError> {
        match Self::check_fields(
            fields,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
            &IQDAT_FIELDS,
        ) {
            Ok(_) => {}
            Err(e) => Err(e)?,
        }

        Ok(IqdatRecord {
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

impl TryFrom<&mut IndexMap<String, DmapField>> for IqdatRecord {
    type Error = DmapError;

    fn try_from(value: &mut IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
        Ok(Self::coerce::<IqdatRecord>(
            value,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
            &IQDAT_FIELDS,
        )?)
    }
}
