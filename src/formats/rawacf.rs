use crate::record::create_record_type;
use crate::types::{Fields, Type};
use lazy_static::lazy_static;

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

lazy_static! {
    static ref RAWACF_FIELDS: Fields<'static> = Fields {
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
        vector_dim_groups: vec![],
    };
}

create_record_type!(rawacf, RAWACF_FIELDS);
