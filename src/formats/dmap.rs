//! Defines the `DmapRecord` struct which implements `Record`, which can be used
//! for reading/writing DMAP files without checking that certain fields are or
//! are not present, or have a given type.

use crate::error::DmapError;
use crate::record::Record;
use crate::types::{DmapField, DmapType};
use indexmap::IndexMap;

#[derive(Debug, PartialEq, Clone)]
pub struct DmapRecord {
    pub data: IndexMap<String, DmapField>,
}

impl DmapRecord {
    pub fn get(&self, key: &String) -> Option<&DmapField> {
        self.data.get(key)
    }
    pub fn keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }
}

impl Record<'_> for DmapRecord {
    fn inner(self) -> IndexMap<String, DmapField> {
        self.data
    }

    fn new(fields: &mut IndexMap<String, DmapField>) -> Result<DmapRecord, DmapError> {
        Ok(DmapRecord {
            data: fields.to_owned(),
        })
    }
    fn to_bytes(&self) -> Result<Vec<u8>, DmapError> {
        let mut data_bytes: Vec<u8> = vec![];
        let mut num_scalars: i32 = 0;
        let mut num_vectors: i32 = 0;

        // Do a first pass, to get all the scalar fields
        for (name, val) in self.data.iter() {
            if let x @ DmapField::Scalar(_) = val {
                data_bytes.extend(name.as_bytes());
                data_bytes.extend([0]); // null-terminate string
                data_bytes.append(&mut x.as_bytes());
                num_scalars += 1;
            }
        }
        // Do a second pass to convert all the vector fields
        for (name, val) in self.data.iter() {
            if let x @ DmapField::Vector(_) = val {
                data_bytes.extend(name.as_bytes());
                data_bytes.extend([0]); // null-terminate string
                data_bytes.append(&mut x.as_bytes());
                num_vectors += 1;
            }
        }
        let mut bytes: Vec<u8> = vec![];
        bytes.extend((65537_i32).as_bytes()); // No idea why this is what it is, copied from backscatter
        bytes.extend((data_bytes.len() as i32 + 16).as_bytes()); // +16 for code, length, num_scalars, num_vectors
        bytes.extend(num_scalars.as_bytes());
        bytes.extend(num_vectors.as_bytes());
        bytes.append(&mut data_bytes); // consumes data_bytes
        Ok(bytes)
    }
}

impl TryFrom<&mut IndexMap<String, DmapField>> for DmapRecord {
    type Error = DmapError;

    fn try_from(value: &mut IndexMap<String, DmapField>) -> Result<Self, Self::Error> {
        DmapRecord::new(value)
    }
}
