use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{DmapField, DmapType, Type};
use indexmap::IndexMap;
use std::convert::TryFrom;

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

static SCALAR_FIELDS_OPT: [(&str, Type); 4] = [
    ("mplgexs", Type::Short),
    ("ifmode", Type::Short),
    ("algorithm", Type::String),
    ("tdiff", Type::Float),
];

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
        match Self::check_fields(
            fields,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
            &FITACF_FIELDS,
        ) {
            Ok(_) => {}
            Err(e) => Err(e)?,
        }

        Ok(FitacfRecord {
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

impl TryFrom<&mut IndexMap<String, DmapField>> for FitacfRecord {
    type Error = DmapError;

    fn try_from(value: &mut IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
        Ok(Self::coerce::<FitacfRecord>(
            value,
            &SCALAR_FIELDS,
            &SCALAR_FIELDS_OPT,
            &VECTOR_FIELDS,
            &VECTOR_FIELDS_OPT,
            &FITACF_FIELDS,
        )?)
    }
}
