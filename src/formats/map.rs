use std::collections::HashMap;
use numpy::ndarray::Array1;
use serde::{Deserialize, Serialize};
use crate::error::DmapError;
use crate::formats::dmap::DmapRecord;
use crate::types::{DmapScalar, DmapVector, GenericDmap, get_scalar_val, get_vector_val, InDmap};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct MapRecord {
    // scalar fields
    start_year: i16,
    start_month: i16,
    start_day: i16,
    start_hour: i16,
    start_minute: i16,
    start_sec: f64,
    end_year: i16,
    end_month: i16,
    end_day: i16,
    end_hour: i16,
    end_minute: i16,
    end_second: f64,
    map_major_revision: i16,
    map_minor_revision: i16,
    source: Option<String>, // map_addfit field
    doping_level: i16,
    model_weight: i16,
    error_weight: i16,
    imf_flag: i16,
    imf_delay: Option<i16>,      // map_addimf fields
    imf_bx: Option<f64>,         // map_addimf fields
    imf_by: Option<f64>,         // map_addimf fields
    imf_bz: Option<f64>,         // map_addimf fields
    imf_vx: Option<f64>,         // map_addimf fields
    imf_tilt: Option<f64>,       // map_addimf fields
    imf_kp: Option<f64>,         // map_addimf fields
    model_angle: Option<String>, // map_addmodel fields
    model_level: Option<String>, // map_addmodel fields
    model_tilt: Option<String>,  // map_addmodel fields
    model_name: Option<String>,  // map_addmodel fields
    hemisphere: i16,
    igrf_flag: Option<i16>,
    fit_order: i16,
    min_latitude: f32,
    chi_squared: f64,
    chi_squared_data: f64,
    rms_error: f64,
    longitude_pole_shift: f32,
    latitude_pole_shift: f32,
    magnetic_local_time_start: f64,
    magnetic_local_time_end: f64,
    magnetic_local_time_mid: f64,
    potential_drop: f64,
    potential_drop_error: f64,
    max_potential: f64,
    max_potential_error: f64,
    min_potential: f64,
    min_potential_error: f64,

    // vector fields
    station_ids: Array1<i16>,
    channels: Array1<i16>,
    num_vectors: Array1<i16>,
    frequencies: Array1<f32>,
    major_revisions: Array1<i16>,
    minor_revisions: Array1<i16>,
    program_ids: Array1<i16>,
    noise_means: Array1<f32>,
    noise_std_devs: Array1<f32>,
    groundscatter_flags: Array1<i16>,
    min_velocities: Array1<f32>,
    max_velocities: Array1<f32>,
    min_powers: Array1<f32>,
    max_powers: Array1<f32>,
    min_spectral_width: Array1<f32>,
    max_spectral_width: Array1<f32>,
    velocity_errors_min: Array1<f32>,
    velocity_errors_max: Array1<f32>,
    magnetic_latitudes: Array1<f32>,           // partial fields
    magnetic_longitudes: Array1<f32>,          // partial fields
    magnetic_azimuth: Array1<f32>,             // partial fields
    vector_station_ids: Array1<i16>,           // partial fields
    vector_channels: Array1<i16>,              // partial fields
    vector_index: Array1<i32>,                 // partial fields
    vector_velocity_median: Array1<f32>,       // partial fields
    vector_velocity_std_dev: Array1<f32>,      // partial fields
    vector_power_median: Option<Array1<f32>>,  // -ext fields
    vector_power_std_dev: Option<Array1<f32>>, // -ext fields
    vector_spectral_width_median: Option<Array1<f32>>, // -ext fields
    vector_spectral_width_std_dev: Option<Array1<f32>>, // -ext fields
    l_value: Option<Array1<f64>>,              // map_addfit fields
    m_value: Option<Array1<f64>>,              // map_addfit fields
    coefficient_value: Option<Array1<f64>>,    // map_addfit fields
    sigma_error: Option<Array1<f64>>,          // map_addfit fields
    model_magnetic_latitude: Option<Array1<f32>>, // map_addhmb fields
    model_magnetic_longitude: Option<Array1<f32>>, // map_addhmb fields
    model_magnetic_azimuth: Option<Array1<f32>>, // map_addhmb fields
    model_velocity_median: Option<Array1<f32>>, // map_addhmb fields
    boundary_magnetic_latitude: Option<Array1<f32>>, // map_addhmb fields
    boundary_magnetic_longitude: Option<Array1<f32>>, // map_addhmb fields
}
impl DmapRecord for MapRecord {
    fn new(
        scalars: &mut HashMap<String, DmapScalar>,
        vectors: &mut HashMap<String, DmapVector>,
    ) -> Result<MapRecord, DmapError> {
        let start_year = get_scalar_val::<i16>(scalars, "start.year")?;
        let start_month = get_scalar_val::<i16>(scalars, "start.month")?;
        let start_day = get_scalar_val::<i16>(scalars, "start.day")?;
        let start_hour = get_scalar_val::<i16>(scalars, "start.hour")?;
        let start_minute = get_scalar_val::<i16>(scalars, "start.minute")?;
        let start_sec = get_scalar_val::<f64>(scalars, "start.second")?;
        let end_year = get_scalar_val::<i16>(scalars, "end.year")?;
        let end_month = get_scalar_val::<i16>(scalars, "end.month")?;
        let end_day = get_scalar_val::<i16>(scalars, "end.day")?;
        let end_hour = get_scalar_val::<i16>(scalars, "end.hour")?;
        let end_minute = get_scalar_val::<i16>(scalars, "end.minute")?;
        let end_second = get_scalar_val::<f64>(scalars, "end.second")?;
        let map_major_revision = get_scalar_val::<i16>(scalars, "map.major.revision")?;
        let map_minor_revision = get_scalar_val::<i16>(scalars, "map.minor.revision")?;
        let source = get_scalar_val::<String>(scalars, "source").ok();
        let doping_level = get_scalar_val::<i16>(scalars, "doping.level")?;
        let model_weight = get_scalar_val::<i16>(scalars, "model.wt")?;
        let error_weight = get_scalar_val::<i16>(scalars, "error.wt")?;
        let imf_flag = get_scalar_val::<i16>(scalars, "IMF.flag")?;
        let imf_delay = get_scalar_val::<i16>(scalars, "IMF.delay").ok();
        let imf_bx = get_scalar_val::<f64>(scalars, "IMF.Bx").ok();
        let imf_by = get_scalar_val::<f64>(scalars, "IMF.By").ok();
        let imf_bz = get_scalar_val::<f64>(scalars, "IMF.Bz").ok();
        let imf_vx = get_scalar_val::<f64>(scalars, "IMF.Vx").ok();
        let imf_tilt = get_scalar_val::<f64>(scalars, "IMF.tilt").ok();
        let imf_kp = get_scalar_val::<f64>(scalars, "IMT.Kp").ok();
        let model_angle = get_scalar_val::<String>(scalars, "model.angle").ok();
        let model_level = get_scalar_val::<String>(scalars, "model.level").ok();
        let model_tilt = get_scalar_val::<String>(scalars, "model.tilt").ok();
        let model_name = get_scalar_val::<String>(scalars, "model.name").ok();
        let hemisphere = get_scalar_val::<i16>(scalars, "hemisphere")?;
        let igrf_flag = get_scalar_val::<i16>(scalars, "noigrf").ok();
        let fit_order = get_scalar_val::<i16>(scalars, "fit.order")?;
        let min_latitude = get_scalar_val::<f32>(scalars, "latmin")?;
        let chi_squared = get_scalar_val::<f64>(scalars, "chi.sqr")?;
        let chi_squared_data = get_scalar_val::<f64>(scalars, "chi.sqr.dat")?;
        let rms_error = get_scalar_val::<f64>(scalars, "rms.err")?;
        let longitude_pole_shift = get_scalar_val::<f32>(scalars, "lon.shft")?;
        let latitude_pole_shift = get_scalar_val::<f32>(scalars, "lat.shft")?;
        let magnetic_local_time_start = get_scalar_val::<f64>(scalars, "mlt.start")?;
        let magnetic_local_time_end = get_scalar_val::<f64>(scalars, "mlt.end")?;
        let magnetic_local_time_mid = get_scalar_val::<f64>(scalars, "mlt.av")?;
        let potential_drop = get_scalar_val::<f64>(scalars, "pot.drop")?;
        let potential_drop_error = get_scalar_val::<f64>(scalars, "pot.drop.err")?;
        let max_potential = get_scalar_val::<f64>(scalars, "pot.max")?;
        let max_potential_error = get_scalar_val::<f64>(scalars, "pot.max.err")?;
        let min_potential = get_scalar_val::<f64>(scalars, "pot.min")?;
        let min_potential_error = get_scalar_val::<f64>(scalars, "pot.min.err")?;

        // vector fields
        let station_ids = get_vector_val::<i16>(vectors, "stid")?.into();
        let channels = get_vector_val::<i16>(vectors, "channel")?.into();
        let num_vectors = get_vector_val::<i16>(vectors, "nvec")?.into();
        let frequencies = get_vector_val::<f32>(vectors, "freq")?.into();
        let major_revisions = get_vector_val::<i16>(vectors, "major.revision")?.into();
        let minor_revisions = get_vector_val::<i16>(vectors, "minor.revision")?.into();
        let program_ids = get_vector_val::<i16>(vectors, "program.id")?.into();
        let noise_means = get_vector_val::<f32>(vectors, "noise.mean")?.into();
        let noise_std_devs = get_vector_val::<f32>(vectors, "noise.sd")?.into();
        let groundscatter_flags = get_vector_val::<i16>(vectors, "gsct")?.into();
        let min_velocities = get_vector_val::<f32>(vectors, "v.min")?.into();
        let max_velocities = get_vector_val::<f32>(vectors, "v.max")?.into();
        let min_powers = get_vector_val::<f32>(vectors, "p.min")?.into();
        let max_powers = get_vector_val::<f32>(vectors, "p.max")?.into();
        let min_spectral_width = get_vector_val::<f32>(vectors, "w.min")?.into();
        let max_spectral_width = get_vector_val::<f32>(vectors, "w.max")?.into();
        let velocity_errors_min = get_vector_val::<f32>(vectors, "ve.min")?.into();
        let velocity_errors_max = get_vector_val::<f32>(vectors, "ve.max")?.into();
        let magnetic_latitudes = get_vector_val::<f32>(vectors, "vector.mlat")?.into();
        let magnetic_longitudes = get_vector_val::<f32>(vectors, "vector.mlon")?.into();
        let magnetic_azimuth = get_vector_val::<f32>(vectors, "vector.kvect")?.into();
        let vector_station_ids = get_vector_val::<i16>(vectors, "vector.stid")?.into();
        let vector_channels = get_vector_val::<i16>(vectors, "vector.channel")?.into();
        let vector_index = get_vector_val::<i32>(vectors, "vector.index")?.into();
        let vector_velocity_median = get_vector_val::<f32>(vectors, "vector.vel.median")?.into();
        let vector_velocity_std_dev = get_vector_val::<f32>(vectors, "vector.vel.sd")?.into();
        let vector_power_median = get_vector_val::<f32>(vectors, "vector.pwr.median")
            .ok()
            .into();
        let vector_power_std_dev = get_vector_val::<f32>(vectors, "vector.pwr.sd").ok().into();
        let vector_spectral_width_median = get_vector_val::<f32>(vectors, "vector.wdt.median")
            .ok()
            .into();
        let vector_spectral_width_std_dev =
            get_vector_val::<f32>(vectors, "vector.wdt.sd").ok().into();
        let l_value = get_vector_val::<f64>(vectors, "N").ok().into();
        let m_value = get_vector_val::<f64>(vectors, "N+1").ok().into();
        let coefficient_value = get_vector_val::<f64>(vectors, "N+2").ok().into();
        let sigma_error = get_vector_val::<f64>(vectors, "N+3").ok().into();
        let model_magnetic_latitude = get_vector_val::<f32>(vectors, "model.mlat").ok().into();
        let model_magnetic_longitude = get_vector_val::<f32>(vectors, "model.mlon").ok().into();
        let model_magnetic_azimuth = get_vector_val::<f32>(vectors, "model.kvect").ok().into();
        let model_velocity_median = get_vector_val::<f32>(vectors, "model.vel.median")
            .ok()
            .into();
        let boundary_magnetic_latitude =
            get_vector_val::<f32>(vectors, "boundary.mlat").ok().into();
        let boundary_magnetic_longitude =
            get_vector_val::<f32>(vectors, "boundary.mlon").ok().into();

        Ok(MapRecord {
            start_year,
            start_month,
            start_day,
            start_hour,
            start_minute,
            start_sec,
            end_year,
            end_month,
            end_day,
            end_hour,
            end_minute,
            end_second,
            map_major_revision,
            map_minor_revision,
            source,
            doping_level,
            model_weight,
            error_weight,
            imf_flag,
            imf_delay,
            imf_bx,
            imf_by,
            imf_bz,
            imf_vx,
            imf_tilt,
            imf_kp,
            model_angle,
            model_level,
            model_tilt,
            model_name,
            hemisphere,
            igrf_flag,
            fit_order,
            min_latitude,
            chi_squared,
            chi_squared_data,
            rms_error,
            longitude_pole_shift,
            latitude_pole_shift,
            magnetic_local_time_start,
            magnetic_local_time_end,
            magnetic_local_time_mid,
            potential_drop,
            potential_drop_error,
            max_potential,
            max_potential_error,
            min_potential,
            min_potential_error,
            station_ids,
            channels,
            num_vectors,
            frequencies,
            major_revisions,
            minor_revisions,
            program_ids,
            noise_means,
            noise_std_devs,
            groundscatter_flags,
            min_velocities,
            max_velocities,
            min_powers,
            max_powers,
            min_spectral_width,
            max_spectral_width,
            velocity_errors_min,
            velocity_errors_max,
            magnetic_latitudes,            // partial fields
            magnetic_longitudes,           // partial fields
            magnetic_azimuth,              // partial fields
            vector_station_ids,            // partial fields
            vector_channels,               // partial fields
            vector_index,                  // partial fields
            vector_velocity_median,        // partial fields
            vector_velocity_std_dev,       // partial fields
            vector_power_median,           // -ext fields
            vector_power_std_dev,          // -ext fields
            vector_spectral_width_median,  // -ext fields
            vector_spectral_width_std_dev, // -ext fields
            l_value,                       // map_addfit fields
            m_value,                       // map_addfit fields
            coefficient_value,             // map_addfit fields
            sigma_error,                   // map_addfit fields
            model_magnetic_latitude,       // map_addhmb fields
            model_magnetic_longitude,      // map_addhmb fields
            model_magnetic_azimuth,        // map_addhmb fields
            model_velocity_median,         // map_addhmb fields
            boundary_magnetic_latitude,    // map_addhmb fields
            boundary_magnetic_longitude,   // map_addhmb fields
        })
    }
    fn to_bytes(&self) -> (i32, i32, Vec<u8>) {
        let mut data_bytes: Vec<u8> = vec![];
        let mut num_scalars: i32 = 43; // number of required scalar fields

        // scalar fields
        data_bytes.extend(self.start_year.to_bytes("start.year"));
        data_bytes.extend(self.start_month.to_bytes("start.month"));
        data_bytes.extend(self.start_day.to_bytes("start.day"));
        data_bytes.extend(self.start_hour.to_bytes("start.hour"));
        data_bytes.extend(self.start_minute.to_bytes("start.minute"));
        data_bytes.extend(self.start_sec.to_bytes("start.second"));
        data_bytes.extend(self.end_year.to_bytes("end.year"));
        data_bytes.extend(self.end_month.to_bytes("end.month"));
        data_bytes.extend(self.end_day.to_bytes("end.day"));
        data_bytes.extend(self.end_hour.to_bytes("end.hour"));
        data_bytes.extend(self.end_minute.to_bytes("end.minute"));
        data_bytes.extend(self.end_second.to_bytes("end.second"));
        data_bytes.extend(self.map_major_revision.to_bytes("map.major.revision"));
        data_bytes.extend(self.map_minor_revision.to_bytes("map.minor.revision"));
        if let Some(x) = &self.source {
            data_bytes.extend(x.to_bytes("source"));
            num_scalars += 1;
        }
        data_bytes.extend(self.doping_level.to_bytes("doping.level"));
        data_bytes.extend(self.model_weight.to_bytes("model.wt"));
        data_bytes.extend(self.error_weight.to_bytes("error.wt"));
        data_bytes.extend(self.imf_flag.to_bytes("IMF.flag"));
        if let Some(x) = &self.imf_delay {
            data_bytes.extend(x.to_bytes("IMF.delay"));
            num_scalars += 1;
        } // map_addimf fields
        if let Some(x) = &self.imf_bx {
            data_bytes.extend(x.to_bytes("IMF.Bx"));
            num_scalars += 1;
        } // map_addimf fields
        if let Some(x) = &self.imf_by {
            data_bytes.extend(x.to_bytes("IMF.By"));
            num_scalars += 1;
        } // map_addimf fields
        if let Some(x) = &self.imf_bz {
            data_bytes.extend(x.to_bytes("IMF.Bz"));
            num_scalars += 1;
        } // map_addimf fields
        if let Some(x) = &self.imf_vx {
            data_bytes.extend(x.to_bytes("IMF.Vx"));
            num_scalars += 1;
        } // map_addimf fields
        if let Some(x) = &self.imf_tilt {
            data_bytes.extend(x.to_bytes("IMF.tilt"));
            num_scalars += 1;
        } // map_addimf fields
        if let Some(x) = &self.imf_kp {
            data_bytes.extend(x.to_bytes("IMF.Kp"));
            num_scalars += 1;
        } // map_addimf fields
        if let Some(x) = &self.model_angle {
            data_bytes.extend(x.to_bytes("model.angle"));
            num_scalars += 1;
        } // map_addmodel fields
        if let Some(x) = &self.model_level {
            data_bytes.extend(x.to_bytes("model.level"));
            num_scalars += 1;
        } // map_addmodel fields
        if let Some(x) = &self.model_tilt {
            data_bytes.extend(x.to_bytes("model.tilt"));
            num_scalars += 1;
        } // map_addmodel fields
        if let Some(x) = &self.model_name {
            data_bytes.extend(x.to_bytes("model.name"));
            num_scalars += 1;
        } // map_addmodel fields
        data_bytes.extend(self.hemisphere.to_bytes("hemisphere"));
        if let Some(x) = &self.igrf_flag {
            data_bytes.extend(x.to_bytes("noigrf"));
            num_scalars += 1;
        }
        data_bytes.extend(self.fit_order.to_bytes("fit.order"));
        data_bytes.extend(self.min_latitude.to_bytes("latmin"));
        data_bytes.extend(self.chi_squared.to_bytes("chi.sqr"));
        data_bytes.extend(self.chi_squared_data.to_bytes("chi.sqr.dat"));
        data_bytes.extend(self.rms_error.to_bytes("rms.err"));
        data_bytes.extend(self.longitude_pole_shift.to_bytes("lon.shft"));
        data_bytes.extend(self.latitude_pole_shift.to_bytes("lat.shft"));
        data_bytes.extend(self.magnetic_local_time_start.to_bytes("mlt.start"));
        data_bytes.extend(self.magnetic_local_time_end.to_bytes("mlt.end"));
        data_bytes.extend(self.magnetic_local_time_mid.to_bytes("mlt.av"));
        data_bytes.extend(self.potential_drop.to_bytes("pot.drop"));
        data_bytes.extend(self.potential_drop_error.to_bytes("pot.drop.err"));
        data_bytes.extend(self.max_potential.to_bytes("pot.max"));
        data_bytes.extend(self.max_potential_error.to_bytes("pot.max.err"));
        data_bytes.extend(self.min_potential.to_bytes("pot.min"));
        data_bytes.extend(self.min_potential_error.to_bytes("pot.min.err"));

        // vector fields
        let mut num_vectors = 26;
        data_bytes.extend(self.station_ids.to_bytes("stid"));
        data_bytes.extend(self.channels.to_bytes("channel"));
        data_bytes.extend(self.num_vectors.to_bytes("nvec"));
        data_bytes.extend(self.frequencies.to_bytes("freq"));
        data_bytes.extend(self.major_revisions.to_bytes("major.revision"));
        data_bytes.extend(self.minor_revisions.to_bytes("minor.revision"));
        data_bytes.extend(self.program_ids.to_bytes("program.id"));
        data_bytes.extend(self.noise_means.to_bytes("noise.mean"));
        data_bytes.extend(self.noise_std_devs.to_bytes("noise.sd"));
        data_bytes.extend(self.groundscatter_flags.to_bytes("gsct"));
        data_bytes.extend(self.min_velocities.to_bytes("v.min"));
        data_bytes.extend(self.max_velocities.to_bytes("v.max"));
        data_bytes.extend(self.min_powers.to_bytes("p.min"));
        data_bytes.extend(self.max_powers.to_bytes("p.max"));
        data_bytes.extend(self.min_spectral_width.to_bytes("w.min"));
        data_bytes.extend(self.max_spectral_width.to_bytes("w.max"));
        data_bytes.extend(self.velocity_errors_min.to_bytes("ve.min"));
        data_bytes.extend(self.velocity_errors_max.to_bytes("ve.max"));
        data_bytes.extend(self.magnetic_latitudes.to_bytes("vector.mlat"));
        data_bytes.extend(self.magnetic_longitudes.to_bytes("vector.mlon"));
        data_bytes.extend(self.magnetic_azimuth.to_bytes("vector.kvect"));
        data_bytes.extend(self.vector_station_ids.to_bytes("vector.stid"));
        data_bytes.extend(self.vector_channels.to_bytes("vector.channel"));
        data_bytes.extend(self.vector_index.to_bytes("vector.index"));
        data_bytes.extend(self.vector_velocity_median.to_bytes("vector.vel.median"));
        data_bytes.extend(self.vector_velocity_std_dev.to_bytes("vector.vel.sd"));
        if let Some(x) = &self.vector_power_median {
            data_bytes.extend(x.to_bytes("vector.pwr.median"));
            num_vectors += 1;
        }
        if let Some(x) = &self.vector_power_std_dev {
            data_bytes.extend(x.to_bytes("vector.pwr.sd"));
            num_vectors += 1;
        }
        if let Some(x) = &self.vector_spectral_width_median {
            data_bytes.extend(x.to_bytes("vector.wdt.median"));
            num_vectors += 1;
        }
        if let Some(x) = &self.vector_spectral_width_std_dev {
            data_bytes.extend(x.to_bytes("vector.wdt.sd"));
            num_vectors += 1;
        }
        if let Some(x) = &self.l_value {
            data_bytes.extend(x.to_bytes("N"));
            num_vectors += 1;
        }
        if let Some(x) = &self.m_value {
            data_bytes.extend(x.to_bytes("N+1"));
            num_vectors += 1;
        }
        if let Some(x) = &self.coefficient_value {
            data_bytes.extend(x.to_bytes("N+2"));
            num_vectors += 1;
        }
        if let Some(x) = &self.sigma_error {
            data_bytes.extend(x.to_bytes("N+3"));
            num_vectors += 1;
        }
        if let Some(x) = &self.model_magnetic_latitude {
            data_bytes.extend(x.to_bytes("model.mlat"));
            num_vectors += 1;
        }
        if let Some(x) = &self.model_magnetic_longitude {
            data_bytes.extend(x.to_bytes("model.mlon"));
            num_vectors += 1;
        }
        if let Some(x) = &self.model_magnetic_azimuth {
            data_bytes.extend(x.to_bytes("model.kvect"));
            num_vectors += 1;
        }
        if let Some(x) = &self.model_velocity_median {
            data_bytes.extend(x.to_bytes("model.vel.median"));
            num_vectors += 1;
        }
        if let Some(x) = &self.boundary_magnetic_latitude {
            data_bytes.extend(x.to_bytes("boundary.mlat"));
            num_vectors += 1;
        }
        if let Some(x) = &self.boundary_magnetic_longitude {
            data_bytes.extend(x.to_bytes("boundary.mlon"));
            num_vectors += 1;
        }

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
        map.insert("start.second".to_string(), self.start_sec.into());
        map.insert("end.year".to_string(), self.end_year.into());
        map.insert("end.month".to_string(), self.end_month.into());
        map.insert("end.day".to_string(), self.end_day.into());
        map.insert("end.hour".to_string(), self.end_hour.into());
        map.insert("end.minute".to_string(), self.end_minute.into());
        map.insert("end.second".to_string(), self.end_second.into());
        map.insert(
            "map.major.revision".to_string(),
            self.map_major_revision.into(),
        );
        map.insert(
            "map.minor.revision".to_string(),
            self.map_minor_revision.into(),
        );
        if let Some(x) = &self.source {
            map.insert("source".to_string(), x.into());
        }
        map.insert("doping.level".to_string(), self.doping_level.into());
        map.insert("model.wt".to_string(), self.model_weight.into());
        map.insert("error.wt".to_string(), self.error_weight.into());
        map.insert("IMF.flag".to_string(), self.imf_flag.into());
        if let Some(x) = &self.imf_delay {
            map.insert("IMF.delay".to_string(), x.into());
        }
        if let Some(x) = &self.imf_bx {
            map.insert("IMF.Bx".to_string(), x.into());
        }
        if let Some(x) = &self.imf_by {
            map.insert("IMF.By".to_string(), x.into());
        }
        if let Some(x) = &self.imf_bz {
            map.insert("IMF.Bz".to_string(), x.into());
        }
        if let Some(x) = &self.imf_vx {
            map.insert("IMF.Vx".to_string(), x.into());
        }
        if let Some(x) = &self.imf_tilt {
            map.insert("IMF.tilt".to_string(), x.into());
        }
        if let Some(x) = &self.imf_kp {
            map.insert("IMF.Kp".to_string(), x.into());
        }
        if let Some(x) = &self.model_angle {
            map.insert("model.angle".to_string(), x.into());
        }
        if let Some(x) = &self.model_level {
            map.insert("model.level".to_string(), x.into());
        }
        if let Some(x) = &self.model_tilt {
            map.insert("model.tilt".to_string(), x.into());
        }
        if let Some(x) = &self.model_name {
            map.insert("model.name".to_string(), x.into());
        }
        if let Some(x) = &self.igrf_flag {
            map.insert("noigrf".to_string(), x.into());
        }
        map.insert("hemisphere".to_string(), self.hemisphere.into());
        map.insert("fit.order".to_string(), self.fit_order.into());
        map.insert("latmin".to_string(), self.min_latitude.into());
        map.insert("chi.sqr".to_string(), self.chi_squared.into());
        map.insert("chi.sqr.dat".to_string(), self.chi_squared_data.into());
        map.insert("rms.err".to_string(), self.rms_error.into());
        map.insert("lon.shft".to_string(), self.longitude_pole_shift.into());
        map.insert("lat.shft".to_string(), self.latitude_pole_shift.into());
        map.insert(
            "mlt.start".to_string(),
            self.magnetic_local_time_start.into(),
        );
        map.insert(
            "mlt.end".to_string(),
            self.magnetic_local_time_end.into(),
        );
        map.insert("mlt.av".to_string(), self.magnetic_local_time_mid.into());
        map.insert("pot.drop".to_string(), self.potential_drop.into());
        map.insert(
            "pot.drop.err".to_string(),
            self.potential_drop_error.into(),
        );
        map.insert("pot.max".to_string(), self.max_potential.into());
        map.insert(
            "pot.max.err".to_string(),
            self.max_potential_error.into(),
        );
        map.insert("pot.min".to_string(), self.min_potential.into());
        map.insert(
            "pot.min.err".to_string(),
            self.min_potential_error.into(),
        );

        // vector fields
        map.insert("stid".to_string(), self.station_ids.clone().into());
        map.insert("channel".to_string(), self.channels.clone().into());
        map.insert("nvec".to_string(), self.num_vectors.clone().into());
        map.insert("freq".to_string(), self.frequencies.clone().into());
        map.insert(
            "major.revision".to_string(),
            self.major_revisions.clone().into(),
        );
        map.insert(
            "minor.revision".to_string(),
            self.minor_revisions.clone().into(),
        );
        map.insert("program.id".to_string(), self.program_ids.clone().into());
        map.insert("noise.mean".to_string(), self.noise_means.clone().into());
        map.insert(
            "noise.sd".to_string(),
            self.noise_std_devs.clone().into(),
        );
        map.insert(
            "gsct".to_string(),
            self.groundscatter_flags.clone().into(),
        );
        map.insert("v.min".to_string(), self.min_velocities.clone().into());
        map.insert("v.max".to_string(), self.max_velocities.clone().into());
        map.insert("p.min".to_string(), self.min_powers.clone().into());
        map.insert("p.max".to_string(), self.max_powers.clone().into());
        map.insert(
            "w.min".to_string(),
            self.min_spectral_width.clone().into(),
        );
        map.insert(
            "w.max".to_string(),
            self.max_spectral_width.clone().into(),
        );
        map.insert(
            "ve.min".to_string(),
            self.velocity_errors_min.clone().into(),
        );
        map.insert(
            "ve.max".to_string(),
            self.velocity_errors_max.clone().into(),
        );
        map.insert(
            "vector.mlat".to_string(),
            self.magnetic_latitudes.clone().into(),
        );
        map.insert(
            "vector.mlon".to_string(),
            self.magnetic_longitudes.clone().into(),
        );
        map.insert(
            "vector.kvect".to_string(),
            self.magnetic_azimuth.clone().into(),
        );
        map.insert(
            "vector.stid".to_string(),
            self.vector_station_ids.clone().into(),
        );
        map.insert(
            "vector.channel".to_string(),
            self.vector_channels.clone().into(),
        );
        map.insert(
            "vector.index".to_string(),
            self.vector_index.clone().into(),
        );
        map.insert(
            "vector.vel.median".to_string(),
            self.vector_velocity_median.clone().into(),
        );
        map.insert(
            "vector.vel.sd".to_string(),
            self.vector_velocity_std_dev.clone().into(),
        );
        if let Some(x) = &self.vector_power_median {
            map.insert("vector.pwr.median".to_string(), x.clone().into());
        }
        if let Some(x) = &self.vector_power_std_dev {
            map.insert("vector.pwr.sd".to_string(), x.clone().into());
        }
        if let Some(x) = &self.vector_spectral_width_median {
            map.insert("vector.wdt.median".to_string(), x.clone().into());
        }
        if let Some(x) = &self.vector_spectral_width_std_dev {
            map.insert("vector.wdt.sd".to_string(), x.clone().into());
        }
        if let Some(x) = &self.l_value {
            map.insert("N".to_string(), x.clone().into());
        }
        if let Some(x) = &self.m_value {
            map.insert("N+1".to_string(), x.clone().into());
        }
        if let Some(x) = &self.coefficient_value {
            map.insert("N+2".to_string(), x.clone().into());
        }
        if let Some(x) = &self.sigma_error {
            map.insert("N+3".to_string(), x.clone().into());
        }
        if let Some(x) = &self.model_magnetic_latitude {
            map.insert("model.mlat".to_string(), x.clone().into());
        }
        if let Some(x) = &self.model_magnetic_longitude {
            map.insert("model.mlon".to_string(), x.clone().into());
        }
        if let Some(x) = &self.model_magnetic_azimuth {
            map.insert("model.kvect".to_string(), x.clone().into());
        }
        if let Some(x) = &self.model_velocity_median {
            map.insert("model.vel.median".to_string(), x.clone().into());
        }
        if let Some(x) = &self.boundary_magnetic_latitude {
            map.insert("boundary.mlat".to_string(), x.clone().into());
        }
        if let Some(x) = &self.boundary_magnetic_longitude {
            map.insert("boundary.mlon".to_string(), x.clone().into());
        }

        map
    }
}
