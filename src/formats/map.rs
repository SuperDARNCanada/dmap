use crate::error::DmapError;
use crate::formats::dmap::Record;
use crate::types::{DmapField, DmapType, Fields, Type};
use indexmap::IndexMap;
use lazy_static::lazy_static;

static SCALAR_FIELDS: [(&str, Type); 35] = [
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
    ("map.major.revision", Type::Short),
    ("map.minor.revision", Type::Short),
    ("doping.level", Type::Short),
    ("model.wt", Type::Short),
    ("error.wt", Type::Short),
    ("IMF.flag", Type::Short),
    ("hemisphere", Type::Short),
    ("fit.order", Type::Short),
    ("latmin", Type::Float),
    ("chi.sqr", Type::Double),
    ("chi.sqr.dat", Type::Double),
    ("rms.err", Type::Double),
    ("lon.shft", Type::Float),
    ("lat.shft", Type::Float),
    ("mlt.start", Type::Double),
    ("mlt.end", Type::Double),
    ("mlt.av", Type::Double),
    ("pot.drop", Type::Double),
    ("pot.drop.err", Type::Double),
    ("pot.max", Type::Double),
    ("pot.max.err", Type::Double),
    ("pot.min", Type::Double),
    ("pot.min.err", Type::Double),
];

static SCALAR_FIELDS_OPT: [(&str, Type); 13] = [
    ("source", Type::String),
    ("IMF.delay", Type::Short),
    ("IMF.Bx", Type::Double),
    ("IMF.By", Type::Double),
    ("IMF.Bz", Type::Double),
    ("IMF.Vx", Type::Double),
    ("IMF.tilt", Type::Double),
    ("IMF.Kp", Type::Double),
    ("model.angle", Type::String),
    ("model.level", Type::String),
    ("model.tilt", Type::String),
    ("model.name", Type::String),
    ("noigrf", Type::Short),
];

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

static VECTOR_FIELDS_OPT: [(&str, Type); 23] = [
    ("vector.mlat", Type::Float),
    ("vector.mlon", Type::Float),
    ("vector.kvect", Type::Float),
    ("vector.stid", Type::Short),
    ("vector.channel", Type::Short),
    ("vector.index", Type::Int),
    ("vector.srng", Type::Float),
    ("vector.vel.median", Type::Float),
    ("vector.vel.sd", Type::Float),
    ("vector.pwr.median", Type::Float),
    ("vector.pwr.sd", Type::Float),
    ("vector.wdt.median", Type::Float),
    ("vector.wdt.sd", Type::Float),
    ("N", Type::Double),
    ("N+1", Type::Double),
    ("N+2", Type::Double),
    ("N+3", Type::Double),
    ("model.mlat", Type::Float),
    ("model.mlon", Type::Float),
    ("model.kvect", Type::Float),
    ("model.vel.median", Type::Float),
    ("boundary.mlat", Type::Float),
    ("boundary.mlon", Type::Float),
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
        vec!["N", "N+1", "N+2", "N+3",],
        vec![
            "model.mlat",
            "model.mlon",
            "model.kvect",
            "model.vel.median",
        ],
        vec!["boundary.mlat", "boundary.mlon",]
    ];
    static ref MAP_FIELDS: Fields<'static> = Fields {
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

#[derive(Debug, PartialEq, Clone)]
pub struct MapRecord {
    pub(crate) data: IndexMap<String, DmapField>,
}

impl Record for MapRecord {
    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<MapRecord, DmapError> {
        match Self::check_fields(fields, &MAP_FIELDS) {
            Ok(_) => {}
            Err(e) => Err(e)?,
        }

        Ok(MapRecord {
            data: fields.to_owned(),
        })
    }
    fn to_bytes(&self) -> Result<Vec<u8>, DmapError> {
        let (num_scalars, num_vectors, mut data_bytes) =
            Self::data_to_bytes(&self.data, &MAP_FIELDS)?;

        let mut bytes: Vec<u8> = vec![];
        bytes.extend((65537_i32).as_bytes()); // No idea why this is what it is, copied from backscatter
        bytes.extend((data_bytes.len() as i32 + 16).as_bytes()); // +16 for code, length, num_scalars, num_vectors
        bytes.extend(num_scalars.as_bytes());
        bytes.extend(num_vectors.as_bytes());
        bytes.append(&mut data_bytes); // consumes data_bytes
        Ok(bytes)
    }
}

impl TryFrom<&mut IndexMap<String, DmapField>> for MapRecord {
    type Error = DmapError;

    fn try_from(value: &mut IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
        Ok(Self::coerce::<MapRecord>(value, &MAP_FIELDS)?)
    }
}
