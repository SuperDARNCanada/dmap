use crate::record::create_record_type;
use crate::types::{Fields, Type};
use lazy_static::lazy_static;

static SCALAR_FIELDS: [(&str, Type); 12] = [
    ("start.year", Type::Short),
    ("start.month", Type::Short),
    ("start.day", Type::Short),
    ("start.hour", Type::Short),
    ("start.minute", Type::Short),
    ("start.second", Type::Double),
    ("end.year", Type::Short),
    ("end.month", Type::Short),
    ("end.day", Type::Short),
    ("end.hour", Type::Short),
    ("end.minute", Type::Short),
    ("end.second", Type::Double),
];

static SCALAR_FIELDS_OPT: [(&str, Type); 0] = [];

static VECTOR_FIELDS: [(&str, Type); 18] = [
    ("stid", Type::Short),
    ("channel", Type::Short),
    ("nvec", Type::Short),
    ("freq", Type::Float),
    ("major.revision", Type::Short),
    ("minor.revision", Type::Short),
    ("program.id", Type::Short),
    ("noise.mean", Type::Float),
    ("noise.sd", Type::Float),
    ("gsct", Type::Short),
    ("v.min", Type::Float),
    ("v.max", Type::Float),
    ("p.min", Type::Float),
    ("p.max", Type::Float),
    ("w.min", Type::Float),
    ("w.max", Type::Float),
    ("ve.min", Type::Float),
    ("ve.max", Type::Float),
];

static VECTOR_FIELDS_OPT: [(&str, Type); 13] = [
    ("vector.mlat", Type::Float),
    ("vector.mlon", Type::Float),
    ("vector.kvect", Type::Float),
    ("vector.stid", Type::Short),
    ("vector.channel", Type::Short),
    ("vector.index", Type::Int),
    ("vector.vel.median", Type::Float),
    ("vector.vel.sd", Type::Float),
    ("vector.pwr.median", Type::Float),
    ("vector.pwr.sd", Type::Float),
    ("vector.wdt.median", Type::Float),
    ("vector.wdt.sd", Type::Float),
    ("vector.srng", Type::Float),
];

lazy_static! {
    static ref MATCHED_VECS: Vec<Vec<&'static str>> = vec![
        vec![
            "stid",
            "channel",
            "nvec",
            "freq",
            "major.revision",
            "minor.revision",
            "program.id",
            "noise.mean",
            "noise.sd",
            "gsct",
            "v.min",
            "v.max",
            "p.min",
            "p.max",
            "w.min",
            "w.max",
            "ve.min",
            "ve.max",
        ],
        vec![
            "vector.mlat",
            "vector.mlon",
            "vector.kvect",
            "vector.stid",
            "vector.channel",
            "vector.index",
            "vector.vel.median",
            "vector.vel.sd",
            "vector.pwr.median",
            "vector.pwr.sd",
            "vector.wdt.median",
            "vector.wdt.sd",
        ],
    ];
    static ref GRID_FIELDS: Fields<'static> = Fields {
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
        vector_dim_groups: MATCHED_VECS.clone(),
    };
}

create_record_type!(grid, GRID_FIELDS);
