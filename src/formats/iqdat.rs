use crate::record::create_record_type;
use crate::types::{Fields, Type};
use lazy_static::lazy_static;

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

/// This defines the groups of vector fields that must have the same dimensionality.
static MATCHED_VECS: [[&str; 6]; 1] = [["tsc", "tus", "tatten", "tnoise", "toff", "tsze"]];

static VECTOR_FIELDS_OPT: [(&str, Type); 0] = [];

lazy_static! {
    static ref IQDAT_FIELDS: Fields<'static> = Fields {
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

create_record_type!(iqdat, IQDAT_FIELDS);
