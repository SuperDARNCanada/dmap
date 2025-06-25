use crate::record::create_record_type;
use crate::types::{Fields, Type};
use lazy_static::lazy_static;

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

static VECTOR_FIELDS: [(&str, Type); 3] = [
    ("ptab", Type::Short),
    ("ltab", Type::Short),
    ("pwr0", Type::Float),
];

static VECTOR_FIELDS_OPT: [(&str, Type); 39] = [
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

/// This defines the groups of vector fields that must have the same dimensionality.
static MATCHED_VECS: [[&str; 39]; 1] = [[
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
]];

lazy_static! {
    static ref FITACF_FIELDS: Fields<'static> = Fields {
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
        vector_dim_groups: {
            let mut grouped_vecs: Vec<Vec<&str>> = vec![];
            for group in MATCHED_VECS.iter() {
                grouped_vecs.push(group.to_vec())
            }
            grouped_vecs
        }
    };
}

create_record_type!(fitacf, FITACF_FIELDS);
