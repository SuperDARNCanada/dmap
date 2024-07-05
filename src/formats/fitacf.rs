use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{DmapField, Type};
use indexmap::IndexMap;

static SCALAR_FIELDS: [(&str, Type); 49] = [
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
    ("fitacf.revision.major", Type::Int),
    ("fitacf.revision.minor", Type::Int),
    ("noise.sky", Type::Float),
    ("noise.lag0", Type::Float),
    ("noise.vel", Type::Float),
];

static SCALAR_FIELDS_OPT: [(&str, Type); 4] = [("mplgexs", Type::Short), ("ifmode", Type::Short), ("algorithm", Type::String), ("tdiff", Type::Float)];

static VECTOR_FIELDS: [(&str, Type); 20] = [
    ("ptab", Type::Short),
    ("ltab", Type::Short),
    ("pwr0", Type::Float),
    ("slist", Type::Short),
    ("nlag", Type::Short),
    ("qflg", Type::Char),
    ("gflg", Type::Char),
    ("p_l", Type::Float),
    ("p_l_e", Type::Float),
    ("p_s", Type::Float),
    ("p_s_e", Type::Float),
    ("v", Type::Float),
    ("v_e", Type::Float),
    ("w_l", Type::Float),
    ("w_l_e", Type::Float),
    ("w_s", Type::Float),
    ("w_s_e", Type::Float),
    ("sd_l", Type::Float),
    ("sd_s", Type::Float),
    ("sd_phi", Type::Float),
];

static VECTOR_FIELDS_OPT: [(&str, Type); 22] = [
    ("x_qflg", Type::Char),
    ("x_gflg", Type::Char),
    ("x_p_l", Type::Float),
    ("x_p_l_e", Type::Float),
    ("x_p_s", Type::Float),
    ("x_p_s_e", Type::Float),
    ("x_v", Type::Float),
    ("x_v_e", Type::Float),
    ("x_w_l", Type::Float),
    ("x_w_l_e", Type::Float),
    ("x_w_s", Type::Float),
    ("x_w_s_e", Type::Float),
    ("phi0", Type::Float),
    ("phi0_e", Type::Float),
    ("elv", Type::Float),
    ("elv_fitted", Type::Float),
    ("elv_error", Type::Float),
    ("elv_low", Type::Float),
    ("elv_high", Type::Float),
    ("x_sd_l", Type::Float),
    ("x_sd_s", Type::Float),
    ("x_sd_phi", Type::Float),
];

static FITACF_FIELDS: [&str; 95] = [
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
    "algorithm",
    "combf",
    "fitacf.revision.major",
    "fitacf.revision.minor",
    "noise.sky",
    "noise.lag0",
    "noise.vel",
    "tdiff",
    "mplgexs",
    "ifmode",
    "ptab",
    "ltab",
    "pwr0",
    "slist",
    "nlag",
    "qflg",
    "gflg",
    "p_l",
    "p_l_e",
    "p_s",
    "p_s_e",
    "v",
    "v_e",
    "w_l",
    "w_l_e",
    "w_s",
    "w_s_e",
    "sd_l",
    "sd_s",
    "sd_phi",
    "x_qflg",
    "x_gflg",
    "x_p_l",
    "x_p_l_e",
    "x_p_s",
    "x_p_s_e",
    "x_v",
    "x_v_e",
    "x_w_l",
    "x_w_l_e",
    "x_w_s",
    "x_w_s_e",
    "phi0",
    "phi0_e",
    "elv",
    "elv_fitted",
    "elv_error",
    "elv_low",
    "elv_high",
    "x_sd_l",
    "x_sd_s",
    "x_sd_phi",
];

pub struct FitacfRecord {
    pub(crate) data: IndexMap<String, DmapField>,
}

impl Record for FitacfRecord {
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<FitacfRecord, DmapError> {
        let unsupported_keys: Vec<&String> = fields
            .keys()
            .filter(|&k| !FITACF_FIELDS.contains(&&**k))
            .collect();
        if unsupported_keys.len() > 0 {
            Err(DmapError::RecordError(format!(
                "Unsupported fields {:?}, fields supported are {FITACF_FIELDS:?}",
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

        Ok(FitacfRecord {
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
