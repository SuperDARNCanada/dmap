use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{check_scalar, check_scalar_opt, check_vector, check_vector_opt, GenericDmap};
use indexmap::IndexMap;

static SCALAR_FIELDS: [&str; 47] = [
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
];

static SCALAR_FIELDS_OPT: [&str; 2] = ["mplgexs", "ifmode"];

static VECTOR_FIELDS: [&str; 5] = ["ptab", "ltab", "pwr0", "slist", "acfd"];

static VECTOR_FIELDS_OPT: [&str; 1] = ["xcfd"];

pub struct RawacfRecord {
    pub(crate) data: IndexMap<String, GenericDmap>,
}

impl Record for RawacfRecord {
    fn new(fields: &mut IndexMap<String, GenericDmap>) -> Result<RawacfRecord, DmapError> {
        for k in fields.keys() {}

        // scalar fields
        check_scalar::<i8>(fields, "radar.revision.major")?;
        check_scalar::<i8>(fields, "radar.revision.minor")?;
        check_scalar::<i8>(fields, "origin.code")?;
        check_scalar::<String>(fields, "origin.time")?;
        check_scalar::<String>(fields, "origin.command")?;
        check_scalar::<i16>(fields, "cp")?;
        check_scalar::<i16>(fields, "stid")?;
        check_scalar::<i16>(fields, "time.yr")?;
        check_scalar::<i16>(fields, "time.mo")?;
        check_scalar::<i16>(fields, "time.dy")?;
        check_scalar::<i16>(fields, "time.hr")?;
        check_scalar::<i16>(fields, "time.mt")?;
        check_scalar::<i16>(fields, "time.sc")?;
        check_scalar::<i32>(fields, "time.us")?;
        check_scalar::<i16>(fields, "txpow")?;
        check_scalar::<i16>(fields, "nave")?;
        check_scalar::<i16>(fields, "atten")?;
        check_scalar::<i16>(fields, "lagfr")?;
        check_scalar::<i16>(fields, "smsep")?;
        check_scalar::<i16>(fields, "ercod")?;
        check_scalar::<i16>(fields, "stat.agc")?;
        check_scalar::<i16>(fields, "stat.lopwr")?;
        check_scalar::<f32>(fields, "noise.search")?;
        check_scalar::<f32>(fields, "noise.mean")?;
        check_scalar::<i16>(fields, "channel")?;
        check_scalar::<i16>(fields, "bmnum")?;
        check_scalar::<f32>(fields, "bmazm")?;
        check_scalar::<i16>(fields, "scan")?;
        check_scalar::<i16>(fields, "offset")?;
        check_scalar::<i16>(fields, "rxrise")?;
        check_scalar::<i16>(fields, "intt.sc")?;
        check_scalar::<i32>(fields, "intt.us")?;
        check_scalar::<i16>(fields, "txpl")?;
        check_scalar::<i16>(fields, "mpinc")?;
        check_scalar::<i16>(fields, "mppul")?;
        check_scalar::<i16>(fields, "mplgs")?;
        check_scalar_opt::<i16>(fields, "mplgexs")?;
        check_scalar_opt::<i16>(fields, "ifmode")?;
        check_scalar::<i16>(fields, "nrang")?;
        check_scalar::<i16>(fields, "frang")?;
        check_scalar::<i16>(fields, "rsep")?;
        check_scalar::<i16>(fields, "xcf")?;
        check_scalar::<i16>(fields, "tfreq")?;
        check_scalar::<i32>(fields, "mxpwr")?;
        check_scalar::<i32>(fields, "lvmax")?;
        check_scalar::<String>(fields, "combf")?;
        check_scalar::<i32>(fields, "rawacf.revision.major")?;
        check_scalar::<i32>(fields, "rawacf.revision.minor")?;
        check_scalar::<f32>(fields, "thr")?;

        // vector fields
        check_vector::<i16>(fields, "ptab")?;
        check_vector::<i16>(fields, "ltab")?;
        check_vector::<f32>(fields, "pwr0")?;
        check_vector::<i16>(fields, "slist")?;
        check_vector::<f32>(fields, "acfd")?;
        check_vector_opt::<f32>(fields, "xcfd")?;

        Ok(RawacfRecord {
            data: fields.to_owned(),
        })
    }
    fn to_bytes(&self) -> (i32, i32, Vec<u8>) {
        let mut data_bytes: Vec<u8> = vec![];
        let mut num_scalars: i32 = 0;
        let mut num_vectors: i32 = 0;

        for &field in SCALAR_FIELDS.iter() {
            if let Some(x) = self.data.get(field) {
                data_bytes.extend(x.to_bytes(field));
                num_scalars += 1;
            }
        }
        for &field in SCALAR_FIELDS_OPT.iter() {
            if let Some(x) = self.data.get(field) {
                data_bytes.extend(x.to_bytes(field));
                num_scalars += 1;
            }
        }
        for &field in VECTOR_FIELDS.iter() {
            if let Some(x) = self.data.get(field) {
                data_bytes.extend(x.to_bytes(field));
                num_vectors += 1;
            }
        }
        for &field in VECTOR_FIELDS_OPT.iter() {
            if let Some(x) = self.data.get(field) {
                data_bytes.extend(x.to_bytes(field));
                num_vectors += 1;
            }
        }

        (num_scalars, num_vectors, data_bytes)
    }
}
