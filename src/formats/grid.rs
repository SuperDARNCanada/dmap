use std::collections::HashMap;
use numpy::ndarray::Array1;
use serde::{Deserialize, Serialize};
use crate::error::DmapError;
use crate::formats::dmap::DmapRecord;
use crate::types::{DmapScalar, DmapVector, GenericDmap, get_scalar_val, get_vector_val, InDmap};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct GridRecord {
    // scalar fields
    pub start_year: i16,
    pub start_month: i16,
    pub start_day: i16,
    pub start_hour: i16,
    pub start_minute: i16,
    pub start_second: f64,
    pub end_year: i16,
    pub end_month: i16,
    pub end_day: i16,
    pub end_hour: i16,
    pub end_minute: i16,
    pub end_second: f64,

    // vector fields
    pub station_ids: Array1<i16>,
    pub channels: Array1<i16>,
    pub num_vectors: Array1<i16>,
    pub freq: Array1<f32>,
    pub grid_major_revision: Array1<i16>,
    pub grid_minor_revision: Array1<i16>,
    pub program_ids: Array1<i16>,
    pub noise_mean: Array1<f32>,
    pub noise_stddev: Array1<f32>,
    pub groundscatter: Array1<i16>,
    pub velocity_min: Array1<f32>,
    pub velocity_max: Array1<f32>,
    pub power_min: Array1<f32>,
    pub power_max: Array1<f32>,
    pub spectral_width_min: Array1<f32>,
    pub spectral_width_max: Array1<f32>,
    pub velocity_error_min: Array1<f32>,
    pub velocity_error_max: Array1<f32>,
    pub magnetic_lat: Array1<f32>,
    pub magnetic_lon: Array1<f32>,
    pub magnetic_azi: Array1<f32>,
    pub station_id_vector: Array1<i16>,
    pub channel_vector: Array1<i16>,
    pub grid_cell_index: Array1<i32>,
    pub velocity_median: Array1<f32>,
    pub velocity_stddev: Array1<f32>,
    pub power_median: Array1<f32>,
    pub power_stddev: Array1<f32>,
    pub spectral_width_median: Array1<f32>,
    pub spectral_width_stddev: Array1<f32>,
}
impl DmapRecord for GridRecord {
    fn new(
        scalars: &mut HashMap<String, DmapScalar>,
        vectors: &mut HashMap<String, DmapVector>,
    ) -> Result<GridRecord, DmapError> {
        // scalar fields
        let start_year = get_scalar_val::<i16>(scalars, "start.year")?;
        let start_month = get_scalar_val::<i16>(scalars, "start.month")?;
        let start_day = get_scalar_val::<i16>(scalars, "start.day")?;
        let start_hour = get_scalar_val::<i16>(scalars, "start.hour")?;
        let start_minute = get_scalar_val::<i16>(scalars, "start.minute")?;
        let start_second = get_scalar_val::<f64>(scalars, "start.second")?;
        let end_year = get_scalar_val::<i16>(scalars, "end.year")?;
        let end_month = get_scalar_val::<i16>(scalars, "end.month")?;
        let end_day = get_scalar_val::<i16>(scalars, "end.day")?;
        let end_hour = get_scalar_val::<i16>(scalars, "end.hour")?;
        let end_minute = get_scalar_val::<i16>(scalars, "end.minute")?;
        let end_second = get_scalar_val::<f64>(scalars, "end.second")?;

        // vector fields
        let station_ids = get_vector_val::<i16>(vectors, "stid")?.into();
        let channels = get_vector_val::<i16>(vectors, "channel")?.into();
        let num_vectors = get_vector_val::<i16>(vectors, "nvec")?.into();
        let freq = get_vector_val::<f32>(vectors, "freq")?.into();
        let grid_major_revision = get_vector_val::<i16>(vectors, "major.revision")?.into();
        let grid_minor_revision = get_vector_val::<i16>(vectors, "minor.revision")?.into();
        let program_ids = get_vector_val::<i16>(vectors, "program.id")?.into();
        let noise_mean = get_vector_val::<f32>(vectors, "noise.mean")?.into();
        let noise_stddev = get_vector_val::<f32>(vectors, "noise.sd")?.into();
        let groundscatter = get_vector_val::<i16>(vectors, "gsct")?.into();
        let velocity_min = get_vector_val::<f32>(vectors, "v.min")?.into();
        let velocity_max = get_vector_val::<f32>(vectors, "v.max")?.into();
        let power_min = get_vector_val::<f32>(vectors, "p.min")?.into();
        let power_max = get_vector_val::<f32>(vectors, "p.max")?.into();
        let spectral_width_min = get_vector_val::<f32>(vectors, "w.min")?.into();
        let spectral_width_max = get_vector_val::<f32>(vectors, "w.max")?.into();
        let velocity_error_min = get_vector_val::<f32>(vectors, "ve.min")?.into();
        let velocity_error_max = get_vector_val::<f32>(vectors, "ve.max")?.into();
        let magnetic_lat = get_vector_val::<f32>(vectors, "vector.mlat")?.into();
        let magnetic_lon = get_vector_val::<f32>(vectors, "vector.mlon")?.into();
        let magnetic_azi = get_vector_val::<f32>(vectors, "vector.kvect")?.into();
        let station_id_vector = get_vector_val::<i16>(vectors, "vector.stid")?.into();
        let channel_vector = get_vector_val::<i16>(vectors, "vector.channel")?.into();
        let grid_cell_index = get_vector_val::<i32>(vectors, "vector.index")?.into();
        let velocity_median = get_vector_val::<f32>(vectors, "vector.vel.median")?.into();
        let velocity_stddev = get_vector_val::<f32>(vectors, "vector.vel.sd")?.into();
        let power_median = get_vector_val::<f32>(vectors, "vector.pwr.median")?.into();
        let power_stddev = get_vector_val::<f32>(vectors, "vector.pwr.sd")?.into();
        let spectral_width_median = get_vector_val::<f32>(vectors, "vector.wdt.median")?.into();
        let spectral_width_stddev = get_vector_val::<f32>(vectors, "vector.wdt.sd")?.into();

        Ok(GridRecord {
            start_year,
            start_month,
            start_day,
            start_hour,
            start_minute,
            start_second,
            end_year,
            end_month,
            end_day,
            end_hour,
            end_minute,
            end_second,
            station_ids,
            channels,
            num_vectors,
            freq,
            grid_major_revision,
            grid_minor_revision,
            program_ids,
            noise_mean,
            noise_stddev,
            groundscatter,
            velocity_min,
            velocity_max,
            power_min,
            power_max,
            spectral_width_min,
            spectral_width_max,
            velocity_error_min,
            velocity_error_max,
            magnetic_lat,
            magnetic_lon,
            magnetic_azi,
            station_id_vector,
            channel_vector,
            grid_cell_index,
            velocity_median,
            velocity_stddev,
            power_median,
            power_stddev,
            spectral_width_median,
            spectral_width_stddev,
        })
    }
    fn to_bytes(&self) -> (i32, i32, Vec<u8>) {
        let mut data_bytes: Vec<u8> = vec![];
        let num_scalars: i32 = 12; // number of required scalar fields

        // scalar fields
        data_bytes.extend(self.start_year.to_bytes("start.year"));
        data_bytes.extend(self.start_month.to_bytes("start.month"));
        data_bytes.extend(self.start_day.to_bytes("start.day"));
        data_bytes.extend(self.start_hour.to_bytes("start.hour"));
        data_bytes.extend(self.start_minute.to_bytes("start.minute"));
        data_bytes.extend(self.start_second.to_bytes("start.second"));
        data_bytes.extend(self.end_year.to_bytes("end.year"));
        data_bytes.extend(self.end_month.to_bytes("end.month"));
        data_bytes.extend(self.end_day.to_bytes("end.day"));
        data_bytes.extend(self.end_hour.to_bytes("end.hour"));
        data_bytes.extend(self.end_minute.to_bytes("end.minute"));
        data_bytes.extend(self.end_second.to_bytes("end.second"));

        // vector fields
        let num_vectors: i32 = 30;
        data_bytes.extend(self.station_ids.to_bytes("stid"));
        data_bytes.extend(self.channels.to_bytes("channel"));
        data_bytes.extend(self.num_vectors.to_bytes("nvec"));
        data_bytes.extend(self.freq.to_bytes("freq"));
        data_bytes.extend(self.grid_major_revision.to_bytes("major.revision"));
        data_bytes.extend(self.grid_minor_revision.to_bytes("minor.revision"));
        data_bytes.extend(self.program_ids.to_bytes("program.id"));
        data_bytes.extend(self.noise_mean.to_bytes("noise.mean"));
        data_bytes.extend(self.noise_stddev.to_bytes("noise.sd"));
        data_bytes.extend(self.groundscatter.to_bytes("gsct"));
        data_bytes.extend(self.velocity_min.to_bytes("v.min"));
        data_bytes.extend(self.velocity_max.to_bytes("v.max"));
        data_bytes.extend(self.power_min.to_bytes("p.min"));
        data_bytes.extend(self.power_max.to_bytes("p.max"));
        data_bytes.extend(self.spectral_width_min.to_bytes("w.min"));
        data_bytes.extend(self.spectral_width_max.to_bytes("w.max"));
        data_bytes.extend(self.velocity_error_min.to_bytes("ve.min"));
        data_bytes.extend(self.velocity_error_max.to_bytes("ve.max"));
        data_bytes.extend(self.magnetic_lat.to_bytes("vector.mlat"));
        data_bytes.extend(self.magnetic_lon.to_bytes("vector.mlon"));
        data_bytes.extend(self.magnetic_azi.to_bytes("vector.kvect"));
        data_bytes.extend(self.station_id_vector.to_bytes("vector.stid"));
        data_bytes.extend(self.channel_vector.to_bytes("vector.channel"));
        data_bytes.extend(self.grid_cell_index.to_bytes("vector.index"));
        data_bytes.extend(self.velocity_median.to_bytes("vector.vel.median"));
        data_bytes.extend(self.velocity_stddev.to_bytes("vector.vel.sd"));
        data_bytes.extend(self.power_median.to_bytes("vector.pwr.median"));
        data_bytes.extend(self.power_stddev.to_bytes("vector.pwr.sd"));
        data_bytes.extend(self.spectral_width_median.to_bytes("vector.wdt.median"));
        data_bytes.extend(self.spectral_width_stddev.to_bytes("vector.wdt.sd"));

        (num_scalars, num_vectors, data_bytes)
    }
    fn to_dict(&self) -> HashMap<String, GenericDmap> {
        let mut map = HashMap::new();

        // scalar fields
        map.insert("start.year".to_string(), self.start_year.into());
        map.insert("start.month".to_string(), self.start_month.into());
        map.insert("start.day".to_string(), self.start_day.into());
        map.insert("start.hour".to_string(), self.start_hour.into());
        map.insert("start.minute".to_string(), self.start_minute.into());
        map.insert("start.second".to_string(), self.start_second.into());
        map.insert("end.year".to_string(), self.end_year.into());
        map.insert("end.month".to_string(), self.end_month.into());
        map.insert("end.day".to_string(), self.end_day.into());
        map.insert("end.hour".to_string(), self.end_hour.into());
        map.insert("end.minute".to_string(), self.end_minute.into());
        map.insert("end.second".to_string(), self.end_second.into());

        // vector fields
        map.insert("stid".to_string(), self.station_ids.clone().into());
        map.insert("channel".to_string(), self.channels.clone().into());
        map.insert("nvec".to_string(), self.num_vectors.clone().into());
        map.insert("freq".to_string(), self.freq.clone().into());
        map.insert(
            "major.revision".to_string(),
            self.grid_major_revision.clone().into(),
        );
        map.insert(
            "minor.revision".to_string(),
            self.grid_minor_revision.clone().into(),
        );
        map.insert("program.id".to_string(), self.program_ids.clone().into());
        map.insert("noise.mean".to_string(), self.noise_mean.clone().into());
        map.insert("noise.sd".to_string(), self.noise_stddev.clone().into());
        map.insert("gsct".to_string(), self.groundscatter.clone().into());
        map.insert("v.min".to_string(), self.velocity_min.clone().into());
        map.insert("v.max".to_string(), self.velocity_max.clone().into());
        map.insert("p.min".to_string(), self.power_min.clone().into());
        map.insert("p.max".to_string(), self.power_max.clone().into());
        map.insert(
            "w.min".to_string(),
            self.spectral_width_min.clone().into(),
        );
        map.insert(
            "w.max".to_string(),
            self.spectral_width_max.clone().into(),
        );
        map.insert(
            "ve.min".to_string(),
            self.velocity_error_min.clone().into(),
        );
        map.insert(
            "ve.max".to_string(),
            self.velocity_error_max.clone().into(),
        );
        map.insert(
            "vector.mlat".to_string(),
            self.magnetic_lat.clone().into(),
        );
        map.insert(
            "vector.mlon".to_string(),
            self.magnetic_lon.clone().into(),
        );
        map.insert(
            "vector.kvect".to_string(),
            self.magnetic_azi.clone().into(),
        );
        map.insert(
            "vector.stid".to_string(),
            self.station_id_vector.clone().into(),
        );
        map.insert(
            "vector.channel".to_string(),
            self.channel_vector.clone().into(),
        );
        map.insert(
            "vector.index".to_string(),
            self.grid_cell_index.clone().into(),
        );
        map.insert(
            "vector.vel.median".to_string(),
            self.velocity_median.clone().into(),
        );
        map.insert(
            "vector.vel.sd".to_string(),
            self.velocity_stddev.clone().into(),
        );
        map.insert(
            "vector.pwr.median".to_string(),
            self.power_median.clone().into(),
        );
        map.insert(
            "vector.pwr.sd".to_string(),
            self.power_stddev.clone().into(),
        );
        map.insert(
            "vector.wdt.median".to_string(),
            self.spectral_width_median.clone().into(),
        );
        map.insert(
            "vector.wdt.sd".to_string(),
            self.spectral_width_stddev.clone().into(),
        );

        map
    }
}
