use std::collections::HashMap;
use numpy::ndarray::{Array1, Array2, Array3};
use serde::{Deserialize, Serialize};
use crate::error::DmapError;
use crate::formats::dmap::DmapRecord;
use crate::types::{DmapScalar, DmapVector, GenericDmap, get_scalar_val, get_vector_val, InDmap};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RawacfRecord {
    // scalar fields
    pub radar_revision_major: i8,
    pub radar_revision_minor: i8,
    pub origin_code: i8,
    pub origin_time: String,
    pub origin_command: String,
    pub control_program: i16,
    pub station_id: i16,
    pub year: i16,
    pub month: i16,
    pub day: i16,
    pub hour: i16,
    pub minute: i16,
    pub second: i16,
    pub microsecond: i32,
    pub tx_power: i16,
    pub num_averages: i16,
    pub attenuation: i16,
    pub lag_to_first_range: i16,
    pub sample_separation: i16,
    pub error_code: i16,
    pub agc_status: i16,
    pub low_power_status: i16,
    pub search_noise: f32,
    pub mean_noise: f32,
    pub channel: i16,
    pub beam_num: i16,
    pub beam_azimuth: f32,
    pub scan_flag: i16,
    pub offset: i16,
    pub rx_rise_time: i16,
    pub intt_second: i16,
    pub intt_microsecond: i32,
    pub tx_pulse_length: i16,
    pub multi_pulse_increment: i16,
    pub num_pulses: i16,
    pub num_lags: i16,
    pub num_lags_extras: Option<i16>,
    pub if_mode: Option<i16>,
    pub num_ranges: i16,
    pub first_range: i16,
    pub range_sep: i16,
    pub xcf_flag: i16,
    pub tx_freq: i16,
    pub max_power: i32,
    pub max_noise_level: i32,
    pub comment: String,
    pub rawacf_revision_major: i32,
    pub rawacf_revision_minor: i32,
    pub threshold: f32,

    // vector fields
    pub pulse_table: Array1<i16>,
    pub lag_table: Array2<i16>,
    pub lag_zero_power: Array1<f32>,
    pub range_list: Array1<i16>,
    pub acfs: Array3<f32>,
    pub xcfs: Option<Array3<f32>>,
}
impl DmapRecord for RawacfRecord {
    fn new(
        scalars: &mut HashMap<String, DmapScalar>,
        vectors: &mut HashMap<String, DmapVector>,
    ) -> Result<RawacfRecord, DmapError> {
        // scalar fields
        let radar_revision_major = get_scalar_val::<i8>(scalars, "radar.revision.major")?;
        let radar_revision_minor = get_scalar_val::<i8>(scalars, "radar.revision.minor")?;
        let origin_code = get_scalar_val::<i8>(scalars, "origin.code")?;
        let origin_time = get_scalar_val::<String>(scalars, "origin.time")?;
        let origin_command = get_scalar_val::<String>(scalars, "origin.command")?;
        let control_program = get_scalar_val::<i16>(scalars, "cp")?;
        let station_id = get_scalar_val::<i16>(scalars, "stid")?;
        let year = get_scalar_val::<i16>(scalars, "time.yr")?;
        let month = get_scalar_val::<i16>(scalars, "time.mo")?;
        let day = get_scalar_val::<i16>(scalars, "time.dy")?;
        let hour = get_scalar_val::<i16>(scalars, "time.hr")?;
        let minute = get_scalar_val::<i16>(scalars, "time.mt")?;
        let second = get_scalar_val::<i16>(scalars, "time.sc")?;
        let microsecond = get_scalar_val::<i32>(scalars, "time.us")?;
        let tx_power = get_scalar_val::<i16>(scalars, "txpow")?;
        let num_averages = get_scalar_val::<i16>(scalars, "nave")?;
        let attenuation = get_scalar_val::<i16>(scalars, "atten")?;
        let lag_to_first_range = get_scalar_val::<i16>(scalars, "lagfr")?;
        let sample_separation = get_scalar_val::<i16>(scalars, "smsep")?;
        let error_code = get_scalar_val::<i16>(scalars, "ercod")?;
        let agc_status = get_scalar_val::<i16>(scalars, "stat.agc")?;
        let low_power_status = get_scalar_val::<i16>(scalars, "stat.lopwr")?;
        let search_noise = get_scalar_val::<f32>(scalars, "noise.search")?;
        let mean_noise = get_scalar_val::<f32>(scalars, "noise.mean")?;
        let channel = get_scalar_val::<i16>(scalars, "channel")?;
        let beam_num = get_scalar_val::<i16>(scalars, "bmnum")?;
        let beam_azimuth = get_scalar_val::<f32>(scalars, "bmazm")?;
        let scan_flag = get_scalar_val::<i16>(scalars, "scan")?;
        let offset = get_scalar_val::<i16>(scalars, "offset")?;
        let rx_rise_time = get_scalar_val::<i16>(scalars, "rxrise")?;
        let intt_second = get_scalar_val::<i16>(scalars, "intt.sc")?;
        let intt_microsecond = get_scalar_val::<i32>(scalars, "intt.us")?;
        let tx_pulse_length = get_scalar_val::<i16>(scalars, "txpl")?;
        let multi_pulse_increment = get_scalar_val::<i16>(scalars, "mpinc")?;
        let num_pulses = get_scalar_val::<i16>(scalars, "mppul")?;
        let num_lags = get_scalar_val::<i16>(scalars, "mplgs")?;
        let num_lags_extras = get_scalar_val::<i16>(scalars, "mplgexs").ok();
        let if_mode = get_scalar_val::<i16>(scalars, "ifmode").ok();
        let num_ranges = get_scalar_val::<i16>(scalars, "nrang")?;
        let first_range = get_scalar_val::<i16>(scalars, "frang")?;
        let range_sep = get_scalar_val::<i16>(scalars, "rsep")?;
        let xcf_flag = get_scalar_val::<i16>(scalars, "xcf")?;
        let tx_freq = get_scalar_val::<i16>(scalars, "tfreq")?;
        let max_power = get_scalar_val::<i32>(scalars, "mxpwr")?;
        let max_noise_level = get_scalar_val::<i32>(scalars, "lvmax")?;
        let comment = get_scalar_val::<String>(scalars, "combf")?;
        let rawacf_revision_major = get_scalar_val::<i32>(scalars, "rawacf.revision.major")?;
        let rawacf_revision_minor = get_scalar_val::<i32>(scalars, "rawacf.revision.minor")?;
        let threshold = get_scalar_val::<f32>(scalars, "thr")?;

        // vector fields
        let pulse_table = get_vector_val::<i16>(vectors, "ptab")?.into();
        let lag_table = get_vector_val::<i16>(vectors, "ltab")?.into();
        let lag_zero_power = get_vector_val::<f32>(vectors, "pwr0")?.into();
        let range_list = get_vector_val::<i16>(vectors, "slist")?.into();
        let acfs = get_vector_val::<f32>(vectors, "acfd")?.into();
        let xcfs = get_vector_val::<f32>(vectors, "xcfd").ok().into();

        Ok(RawacfRecord {
            radar_revision_major,
            radar_revision_minor,
            origin_code,
            origin_time,
            origin_command,
            control_program,
            station_id,
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            tx_power,
            num_averages,
            attenuation,
            lag_to_first_range,
            sample_separation,
            error_code,
            agc_status,
            low_power_status,
            search_noise,
            mean_noise,
            channel,
            beam_num,
            beam_azimuth,
            scan_flag,
            offset,
            rx_rise_time,
            intt_second,
            intt_microsecond,
            tx_pulse_length,
            multi_pulse_increment,
            num_pulses,
            num_lags,
            num_lags_extras,
            if_mode,
            num_ranges,
            first_range,
            range_sep,
            xcf_flag,
            tx_freq,
            max_power,
            max_noise_level,
            comment,
            rawacf_revision_major,
            rawacf_revision_minor,
            threshold,
            pulse_table,
            lag_table,
            lag_zero_power,
            range_list,
            acfs,
            xcfs,
        })
    }
    fn to_bytes(&self) -> (i32, i32, Vec<u8>) {
        let mut data_bytes: Vec<u8> = vec![];
        let mut num_scalars: i32 = 47; // number of required scalar fields

        // scalar fields
        data_bytes.extend(self.radar_revision_major.to_bytes("radar.revision.major"));
        data_bytes.extend(self.radar_revision_minor.to_bytes("radar.revision.minor"));
        data_bytes.extend(self.origin_code.to_bytes("origin.code"));
        data_bytes.extend(self.origin_time.to_bytes("origin.time"));
        data_bytes.extend(self.origin_command.to_bytes("origin.command"));
        data_bytes.extend(self.control_program.to_bytes("cp"));
        data_bytes.extend(self.station_id.to_bytes("stid"));
        data_bytes.extend(self.year.to_bytes("time.yr"));
        data_bytes.extend(self.month.to_bytes("time.mo"));
        data_bytes.extend(self.day.to_bytes("time.dy"));
        data_bytes.extend(self.hour.to_bytes("time.hr"));
        data_bytes.extend(self.minute.to_bytes("time.mt"));
        data_bytes.extend(self.second.to_bytes("time.sc"));
        data_bytes.extend(self.microsecond.to_bytes("time.us"));
        data_bytes.extend(self.tx_power.to_bytes("txpow"));
        data_bytes.extend(self.num_averages.to_bytes("nave"));
        data_bytes.extend(self.attenuation.to_bytes("atten"));
        data_bytes.extend(self.lag_to_first_range.to_bytes("lagfr"));
        data_bytes.extend(self.sample_separation.to_bytes("smsep"));
        data_bytes.extend(self.error_code.to_bytes("ercod"));
        data_bytes.extend(self.agc_status.to_bytes("stat.agc"));
        data_bytes.extend(self.low_power_status.to_bytes("stat.lopwr"));
        data_bytes.extend(self.search_noise.to_bytes("noise.search"));
        data_bytes.extend(self.mean_noise.to_bytes("noise.mean"));
        data_bytes.extend(self.channel.to_bytes("channel"));
        data_bytes.extend(self.beam_num.to_bytes("bmnum"));
        data_bytes.extend(self.beam_azimuth.to_bytes("bmazm"));
        data_bytes.extend(self.scan_flag.to_bytes("scan"));
        data_bytes.extend(self.offset.to_bytes("offset"));
        data_bytes.extend(self.rx_rise_time.to_bytes("rxrise"));
        data_bytes.extend(self.intt_second.to_bytes("intt.sc"));
        data_bytes.extend(self.intt_microsecond.to_bytes("intt.us"));
        data_bytes.extend(self.tx_pulse_length.to_bytes("txpl"));
        data_bytes.extend(self.multi_pulse_increment.to_bytes("mpinc"));
        data_bytes.extend(self.num_pulses.to_bytes("mppul"));
        data_bytes.extend(self.num_lags.to_bytes("mplgs"));
        if let Some(x) = self.num_lags_extras {
            data_bytes.extend(x.to_bytes("mplgexs"));
            num_scalars += 1;
        }
        if let Some(x) = self.if_mode {
            data_bytes.extend(x.to_bytes("ifmode"));
            num_scalars += 1;
        }
        data_bytes.extend(self.num_ranges.to_bytes("nrang"));
        data_bytes.extend(self.first_range.to_bytes("frang"));
        data_bytes.extend(self.range_sep.to_bytes("rsep"));
        data_bytes.extend(self.xcf_flag.to_bytes("xcf"));
        data_bytes.extend(self.tx_freq.to_bytes("tfreq"));
        data_bytes.extend(self.max_power.to_bytes("mxpwr"));
        data_bytes.extend(self.max_noise_level.to_bytes("lvmax"));
        data_bytes.extend(self.comment.to_bytes("combf"));
        data_bytes.extend(self.rawacf_revision_major.to_bytes("rawacf.revision.major"));
        data_bytes.extend(self.rawacf_revision_minor.to_bytes("rawacf.revision.minor"));
        data_bytes.extend(self.threshold.to_bytes("thr"));

        // vector fields
        let mut num_vectors: i32 = 5;
        data_bytes.extend(self.pulse_table.to_bytes("ptab"));
        data_bytes.extend(self.lag_table.to_bytes("ltab"));
        data_bytes.extend(self.lag_zero_power.to_bytes("pwr0"));
        data_bytes.extend(self.range_list.to_bytes("slist"));
        data_bytes.extend(self.acfs.to_bytes("acfd"));
        if let Some(x) = &self.xcfs {
            data_bytes.extend(x.to_bytes("xcfd"));
            num_vectors += 1;
        }

        (num_scalars, num_vectors, data_bytes)
    }
    fn to_dict(&self) -> HashMap<String, GenericDmap> {
        let mut map = HashMap::new();

        // scalar fields
        map.insert(
            "radar.revision.major".to_string(),
            self.radar_revision_major.into(),
        );
        map.insert(
            "radar.revision.minor".to_string(),
            self.radar_revision_minor.into(),
        );
        map.insert("origin.code".to_string(), self.origin_code.into());
        map.insert("origin.time".to_string(), self.origin_time.into());
        map.insert("origin.command".to_string(), self.origin_command.into());
        map.insert("cp".to_string(), self.control_program.into());
        map.insert("stid".to_string(), self.station_id.into());
        map.insert("time.yr".to_string(), self.year.into());
        map.insert("time.mo".to_string(), self.month.into());
        map.insert("time.dy".to_string(), self.day.into());
        map.insert("time.hr".to_string(), self.hour.into());
        map.insert("time.mt".to_string(), self.minute.into());
        map.insert("time.sc".to_string(), self.second.into());
        map.insert("time.us".to_string(), self.microsecond.into());
        map.insert("txpow".to_string(), self.tx_power.into());
        map.insert("nave".to_string(), self.num_averages.into());
        map.insert("atten".to_string(), self.attenuation.into());
        map.insert("lagfr".to_string(), self.lag_to_first_range.into());
        map.insert("smsep".to_string(), self.sample_separation.into());
        map.insert("ercod".to_string(), self.error_code.into());
        map.insert("stat.agc".to_string(), self.agc_status.into());
        map.insert("stat.lopwr".to_string(), self.low_power_status.into());
        map.insert("noise.search".to_string(), self.search_noise.into());
        map.insert("noise.mean".to_string(), self.mean_noise.into());
        map.insert("channel".to_string(), self.channel.into());
        map.insert("bmnum".to_string(), self.beam_num.into());
        map.insert("bmazm".to_string(), self.beam_azimuth.into());
        map.insert("scan".to_string(), self.scan_flag.into());
        map.insert("offset".to_string(), self.offset.into());
        map.insert("rxrise".to_string(), self.rx_rise_time.into());
        map.insert("intt.sc".to_string(), self.intt_second.into());
        map.insert("intt.us".to_string(), self.intt_microsecond.into());
        map.insert("txpl".to_string(), self.tx_pulse_length.into());
        map.insert("mpinc".to_string(), self.multi_pulse_increment.into());
        map.insert("mppul".to_string(), self.num_pulses.into());
        map.insert("mplgs".to_string(), self.num_lags.into());
        if let Some(x) = self.num_lags_extras {
            map.insert("mplgexs".to_string(), x.into());
        }
        if let Some(x) = self.if_mode {
            map.insert("ifmode".to_string(), x.into());
        }
        map.insert("nrang".to_string(), self.num_ranges.into());
        map.insert("frang".to_string(), self.first_range.into());
        map.insert("rsep".to_string(), self.range_sep.into());
        map.insert("xcf".to_string(), self.xcf_flag.into());
        map.insert("tfreq".to_string(), self.tx_freq.into());
        map.insert("mxpwr".to_string(), self.max_power.into());
        map.insert("lvmax".to_string(), self.max_noise_level.into());
        map.insert("combf".to_string(), self.comment.into());
        map.insert(
            "rawacf.revision.major".to_string(),
            self.rawacf_revision_major.into(),
        );
        map.insert(
            "rawacf.revision.minor".to_string(),
            self.rawacf_revision_minor.into(),
        );
        map.insert("thr".to_string(), self.threshold.into());

        // vector fields
        map.insert("ptab".to_string(), self.pulse_table.clone());
        map.insert("ltab".to_string(), self.lag_table.clone());
        map.insert("pwr0".to_string(), self.lag_zero_power.clone());
        map.insert("slist".to_string(), self.range_list.clone());
        map.insert("acfd".to_string(), self.acfs.clone());
        if let Some(x) = &self.xcfs {
            map.insert("xcfd".to_string(), x.clone());
        }

        map
    }
}
