use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{DmapField, Type};
use indexmap::IndexMap;

static SCALAR_FIELDS: [(&str, Type); 47] = [
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
    ("rawacf.revision.major", Type::Int),
    ("rawacf.revision.minor", Type::Int),
    ("thr", Type::Float),
];

static SCALAR_FIELDS_OPT: [(&str, Type); 2] = [("mplgexs", Type::Short), ("ifmode", Type::Short)];

static VECTOR_FIELDS: [(&str, Type); 5] = [
    ("ptab", Type::Short),
    ("ltab", Type::Short),
    ("pwr0", Type::Float),
    ("slist", Type::Short),
    ("acfd", Type::Float),
];

static VECTOR_FIELDS_OPT: [(&str, Type); 1] = [("xcfd", Type::Float)];

static RAWACF_FIELDS: [&str; 55] = [
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
    "rawacf.revision.major",
    "rawacf.revision.minor",
    "thr",
    "mplgexs",
    "ifmode",
    "ptab",
    "ltab",
    "pwr0",
    "slist",
    "acfd",
    "xcfd",
];

pub struct RawacfRecord {
    pub(crate) data: IndexMap<String, DmapField>,
}

impl Record for RawacfRecord {
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<RawacfRecord, DmapError> {
        let unsupported_keys: Vec<&String> = fields
            .keys()
            .filter(|&k| !RAWACF_FIELDS.contains(&&**k))
            .collect();
        if unsupported_keys.len() > 0 {
            Err(DmapError::RecordError(format!(
                "Unsupported fields {:?}, fields supported are {RAWACF_FIELDS:?}",
                unsupported_keys
            )))?
        }

        for (field, expected_type) in SCALAR_FIELDS.iter() {
            match fields.get(&field.to_string()) {
                Some(&DmapField::Scalar(ref x)) if &x.get_type() == expected_type => {}
                Some(&DmapField::Scalar(ref x)) => Err(DmapError::RecordError(format!(
                    "Field {} has incorrect type {}, expected {}",
                    field,
                    x.get_type(),
                    expected_type
                )))?,
                Some(_) => Err(DmapError::RecordError(format!(
                    "Field {} is a vector, expected scalar",
                    field
                )))?,
                None => Err(DmapError::RecordError(format!("Field {field:?} ({:?}) missing: fields {:?}", &field.to_string(), fields.keys())))?,
            }
        }
        for (field, expected_type) in SCALAR_FIELDS_OPT.iter() {
            match fields.get(&field.to_string()) {
                Some(&DmapField::Scalar(ref x)) if &x.get_type() == expected_type => {}
                Some(&DmapField::Scalar(ref x)) => Err(DmapError::RecordError(format!(
                    "Field {} has incorrect type {}, expected {}",
                    field,
                    x.get_type(),
                    expected_type
                )))?,
                Some(_) => Err(DmapError::RecordError(format!(
                    "Field {} is a vector, expected scalar",
                    field
                )))?,
                None => {}
            }
        }
        for (field, expected_type) in VECTOR_FIELDS.iter() {
            match fields.get(&field.to_string()) {
                Some(&DmapField::Scalar(_)) => Err(DmapError::RecordError(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(&DmapField::Vector(ref x)) if &x.get_type() != expected_type => Err(DmapError::RecordError(format!(
                    "Field {field} has incorrect type {:?}, expected {expected_type:?}",
                    x.get_type()
                )))?,
                Some(&DmapField::Vector(_)) => {},
                None => Err(DmapError::RecordError(format!("Field {field} missing")))?,
            }
        }
        for (field, expected_type) in VECTOR_FIELDS_OPT.iter() {
            match fields.get(&field.to_string()) {
                Some(&DmapField::Scalar(_)) => Err(DmapError::RecordError(format!(
                    "Field {} is a scalar, expected vector",
                    field
                )))?,
                Some(&DmapField::Vector(ref x)) if &x.get_type() != expected_type => {
                    Err(DmapError::RecordError(format!(
                        "Field {field} has incorrect type {}, expected {expected_type}",
                        x.get_type()
                    )))?
                }
                _ => {}
            }
        }

        Ok(RawacfRecord {
            data: fields.to_owned(),
        })
    }
    fn to_bytes(&self) -> (i32, i32, Vec<u8>) {
        let mut data_bytes: Vec<u8> = vec![];
        let mut num_scalars: i32 = 0;
        let mut num_vectors: i32 = 0;

        for (field, _) in SCALAR_FIELDS.iter() {
            if let Some(x) = self.data.get(*field) {
                data_bytes.extend(field.as_bytes());
                // data_bytes.extend([0]); // null-terminate string
                // data_bytes.extend(dmap_key)
                data_bytes.extend(x.as_bytes());
                num_scalars += 1;
            }
        }
        for (field, _) in SCALAR_FIELDS_OPT.iter() {
            if let Some(x) = self.data.get(*field) {
                data_bytes.extend(field.as_bytes());
                // data_bytes.extend([0]); // null-terminate string
                // data_bytes.extend(dmap_key)
                data_bytes.extend(x.as_bytes());
                num_scalars += 1;
            }
        }
        for (field, _) in VECTOR_FIELDS.iter() {
            if let Some(x) = self.data.get(*field) {
                data_bytes.extend(field.as_bytes());
                // data_bytes.extend([0]); // null-terminate string
                // data_bytes.extend(dmap_key)
                data_bytes.extend(x.as_bytes());
                num_vectors += 1;
            }
        }
        for (field, _) in VECTOR_FIELDS_OPT.iter() {
            if let Some(x) = self.data.get(*field) {
                data_bytes.extend(field.as_bytes());
                // data_bytes.extend([0]); // null-terminate string
                // data_bytes.extend(dmap_key)
                data_bytes.extend(x.as_bytes());
                num_vectors += 1;
            }
        }

        (num_scalars, num_vectors, data_bytes)
    }
}
