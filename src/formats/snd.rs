use crate::record::create_record_type;
use crate::types::{Fields, Type};
use lazy_static::lazy_static;

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

static VECTOR_FIELDS: [(&str, Type); 0] = [];

static VECTOR_FIELDS_OPT: [(&str, Type); 10] = [
    ("slist", Type::Short),
    ("qflg", Type::Char),
    ("gflg", Type::Char),
    ("v", Type::Float),
    ("v_e", Type::Float),
    ("p_l", Type::Float),
    ("w_l", Type::Float),
    ("x_qflg", Type::Char),
    ("phi0", Type::Float),
    ("phi0_e", Type::Float),
];

static MATCHED_VECS: [[&str; 10]; 1] = [[
    "slist", "qflg", "gflg", "v", "v_e", "p_l", "w_l", "x_qflg", "phi0", "phi0_e",
]];

lazy_static! {
    static ref SND_FIELDS: Fields<'static> = Fields {
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
        vector_dim_groups: MATCHED_VECS.to_vec().iter().map(|x| x.to_vec()).collect(),
    };
}

create_record_type!(snd, SND_FIELDS);
