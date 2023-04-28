use crate::{get_scalar_val, get_vector_val, DmapError, RawDmapRecord};
use std::error::Error;
use std::fmt;
use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct FileFormatError {
    details: String,
}
impl Error for FileFormatError {}
impl Display for FileFormatError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

pub struct RawacfRecord {
    // scalar fields
    radar_revision_major: i8,
    radar_revision_minor: i8,
    origin_code: i8,
    origin_time: String,
    origin_command: String,
    control_program: i16,
    station_id: i16,
    year: i16,
    month: i16,
    day: i16,
    hour: i16,
    minute: i16,
    second: i16,
    microsecond: i32,
    tx_power: i16,
    num_averages: i16,
    attenuation: i16,
    lag_to_first_range: i16,
    pub sample_separation: i16,
    error_code: i16,
    agc_status: i16,
    low_power_status: i16,
    search_noise: f32,
    mean_noise: f32,
    channel: i16,
    beam_num: i16,
    beam_azimuth: f32,
    scan_flag: i16,
    offset: i16,
    rx_rise_time: i16,
    intt_second: i16,
    intt_microsecond: i32,
    tx_pulse_length: i16,
    pub multi_pulse_increment: i16,
    pub num_pulses: i16,
    pub num_lags: i16,
    num_lags_extras: Option<i16>,
    if_mode: Option<i16>,
    num_ranges: i16,
    first_range: i16,
    range_sep: i16,
    xcf_flag: i16,
    tx_freq: i16,
    max_power: i32,
    max_noise_level: i32,
    comment: String,
    rawacf_revision_major: i32,
    rawacf_revision_minor: i32,
    threshold: f32,

    // vector fields
    pub pulse_table: Vec<i16>,
    pub lag_table: Vec<i16>,
    lag_zero_power: Vec<f32>,
    range_list: Vec<i16>,
    acfs: Vec<f32>,
    xcfs: Option<Vec<f32>>,
}

impl RawacfRecord {
    pub fn new(record: &RawDmapRecord) -> Result<RawacfRecord, DmapError> {
        // scalar fields
        let radar_revision_major = get_scalar_val::<i8>(record, "radar.revision.major")?;
        let radar_revision_minor = get_scalar_val::<i8>(record, "radar.revision.minor")?;
        let origin_code = get_scalar_val::<i8>(record, "origin.code")?;
        let origin_time = get_scalar_val::<String>(record, "origin.time")?;
        let origin_command = get_scalar_val::<String>(record, "origin.command")?;
        let control_program = get_scalar_val::<i16>(record, "cp")?;
        let station_id = get_scalar_val::<i16>(record, "stid")?;
        let year = get_scalar_val::<i16>(record, "time.yr")?;
        let month = get_scalar_val::<i16>(record, "time.mo")?;
        let day = get_scalar_val::<i16>(record, "time.dy")?;
        let hour = get_scalar_val::<i16>(record, "time.hr")?;
        let minute = get_scalar_val::<i16>(record, "time.mt")?;
        let second = get_scalar_val::<i16>(record, "time.sc")?;
        let microsecond = get_scalar_val::<i32>(record, "time.us")?;
        let tx_power = get_scalar_val::<i16>(record, "txpow")?;
        let num_averages = get_scalar_val::<i16>(record, "nave")?;
        let attenuation = get_scalar_val::<i16>(record, "atten")?;
        let lag_to_first_range = get_scalar_val::<i16>(record, "lagfr")?;
        let sample_separation = get_scalar_val::<i16>(record, "smsep")?;
        let error_code = get_scalar_val::<i16>(record, "ercod")?;
        let agc_status = get_scalar_val::<i16>(record, "stat.agc")?;
        let low_power_status = get_scalar_val::<i16>(record, "stat.lopwr")?;
        let search_noise = get_scalar_val::<f32>(record, "noise.search")?;
        let mean_noise = get_scalar_val::<f32>(record, "noise.mean")?;
        let channel = get_scalar_val::<i16>(record, "channel")?;
        let beam_num = get_scalar_val::<i16>(record, "bmnum")?;
        let beam_azimuth = get_scalar_val::<f32>(record, "bmazm")?;
        let scan_flag = get_scalar_val::<i16>(record, "scan")?;
        let offset = get_scalar_val::<i16>(record, "offset")?;
        let rx_rise_time = get_scalar_val::<i16>(record, "rxrise")?;
        let intt_second = get_scalar_val::<i16>(record, "intt.sc")?;
        let intt_microsecond = get_scalar_val::<i32>(record, "intt.us")?;
        let tx_pulse_length = get_scalar_val::<i16>(record, "txpl")?;
        let multi_pulse_increment = get_scalar_val::<i16>(record, "mpinc")?;
        let num_pulses = get_scalar_val::<i16>(record, "mppul")?;
        let num_lags = get_scalar_val::<i16>(record, "mplgs")?;
        let num_lags_extras = get_scalar_val::<i16>(record, "mplgexs").ok();
        let if_mode = get_scalar_val::<i16>(record, "ifmode").ok();
        let num_ranges = get_scalar_val::<i16>(record, "nrang")?;
        let first_range = get_scalar_val::<i16>(record, "frang")?;
        let range_sep = get_scalar_val::<i16>(record, "rsep")?;
        let xcf_flag = get_scalar_val::<i16>(record, "xcf")?;
        let tx_freq = get_scalar_val::<i16>(record, "tfreq")?;
        let max_power = get_scalar_val::<i32>(record, "mxpwr")?;
        let max_noise_level = get_scalar_val::<i32>(record, "lvmax")?;
        let comment = get_scalar_val::<String>(record, "combf")?;
        let rawacf_revision_major = get_scalar_val::<i32>(record, "rawacf.revision.major")?;
        let rawacf_revision_minor = get_scalar_val::<i32>(record, "rawacf.revision.minor")?;
        let threshold = get_scalar_val::<f32>(record, "thr")?;

        // vector fields
        let pulse_table = get_vector_val::<i16>(record, "ptab")?;
        let lag_table = get_vector_val::<i16>(record, "ltab")?;
        let lag_zero_power = get_vector_val::<f32>(record, "pwr0")?;
        let range_list = get_vector_val::<i16>(record, "slist")?;
        let acfs = get_vector_val::<f32>(record, "acfd")?;
        let xcfs = get_vector_val::<f32>(record, "xcfs").ok();

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
}

pub struct FitacfRecord {
    // scalar fields
    radar_revision_major: i8,
    radar_revision_minor: i8,
    origin_code: i8,
    origin_time: String,
    origin_command: String,
    control_program: i16,
    station_id: i16,
    year: i16,
    month: i16,
    day: i16,
    hour: i16,
    minute: i16,
    second: i16,
    microsecond: i32,
    tx_power: i16,
    num_averages: i16,
    attenuation: i16,
    lag_to_first_range: i16,
    sample_separation: i16,
    error_code: i16,
    agc_status: i16,
    low_power_status: i16,
    search_noise: f32,
    mean_noise: f32,
    channel: i16,
    beam_num: i16,
    beam_azimuth: f32,
    scan_flag: i16,
    offset: i16,
    rx_rise_time: i16,
    intt_second: i16,
    intt_microsecond: i32,
    tx_pulse_length: i16,
    multi_pulse_increment: i16,
    num_pulses: i16,
    num_lags: i16,
    num_lags_extras: Option<i16>,
    if_mode: Option<i16>,
    num_ranges: i16,
    first_range: i16,
    range_sep: i16,
    xcf_flag: i16,
    tx_freq: i16,
    max_power: i32,
    max_noise_level: i32,
    comment: String,
    algorithm: Option<String>,
    fitacf_revision_major: i32,
    fitacf_revision_minor: i32,
    sky_noise: f32,
    lag_zero_noise: f32,
    velocity_noise: f32,
    tdiff: Option<f32>,

    // vector fields
    pulse_table: Vec<i16>,
    lag_table: Vec<i16>,
    lag_zero_power: Vec<f32>,
    range_list: Vec<i16>,
    fitted_points: Vec<i16>,
    quality_flag: Vec<i8>,
    ground_flag: Vec<i8>,
    lambda_power: Vec<f32>,
    lambda_power_error: Vec<f32>,
    sigma_power: Vec<f32>,
    sigma_power_error: Vec<f32>,
    velocity: Vec<f32>,
    velocity_error: Vec<f32>,
    lambda_spectral_width: Vec<f32>,
    lambda_spectral_width_error: Vec<f32>,
    sigma_spectral_width: Vec<f32>,
    sigma_spectral_width_error: Vec<f32>,
    lambda_std_dev: Vec<f32>,
    sigma_std_dev: Vec<f32>,
    phi_std_dev: Vec<f32>,
    xcf_quality_flag: Option<Vec<i8>>,
    xcf_ground_flag: Option<Vec<i8>>,
    lambda_xcf_power: Option<Vec<f32>>,
    lambda_xcf_power_error: Option<Vec<f32>>,
    sigma_xcf_power: Option<Vec<f32>>,
    sigma_xcf_power_error: Option<Vec<f32>>,
    xcf_velocity: Option<Vec<f32>>,
    xcf_velocity_error: Option<Vec<f32>>,
    lambda_xcf_spectral_width: Option<Vec<f32>>,
    lambda_xcf_spectral_width_error: Option<Vec<f32>>,
    sigma_xcf_spectral_width: Option<Vec<f32>>,
    sigma_xcf_spectral_width_error: Option<Vec<f32>>,
    lag_zero_phi: Option<Vec<f32>>,
    lag_zero_phi_error: Option<Vec<f32>>,
    elevation: Option<Vec<f32>>,
    elevation_fitted: Option<Vec<f32>>,
    elevation_error: Option<Vec<f32>>,
    elevation_low: Option<Vec<f32>>,
    elevation_high: Option<Vec<f32>>,
    lambda_xcf_std_dev: Option<Vec<f32>>,
    sigma_xcf_std_dev: Option<Vec<f32>>,
    phi_xcf_std_dev: Option<Vec<f32>>,
}

impl FitacfRecord {
    pub fn new(record: &RawDmapRecord) -> Result<FitacfRecord, DmapError> {
        // scalar fields
        let radar_revision_major = get_scalar_val::<i8>(record, "radar.revision.major")?;
        let radar_revision_minor = get_scalar_val::<i8>(record, "radar.revision.minor")?;
        let origin_code = get_scalar_val::<i8>(record, "origin.code")?;
        let origin_time = get_scalar_val::<String>(record, "origin.time")?;
        let origin_command = get_scalar_val::<String>(record, "origin.command")?;
        let control_program = get_scalar_val::<i16>(record, "cp")?;
        let station_id = get_scalar_val::<i16>(record, "stid")?;
        let year = get_scalar_val::<i16>(record, "time.yr")?;
        let month = get_scalar_val::<i16>(record, "time.mo")?;
        let day = get_scalar_val::<i16>(record, "time.dy")?;
        let hour = get_scalar_val::<i16>(record, "time.hr")?;
        let minute = get_scalar_val::<i16>(record, "time.mt")?;
        let second = get_scalar_val::<i16>(record, "time.sc")?;
        let microsecond = get_scalar_val::<i32>(record, "time.us")?;
        let tx_power = get_scalar_val::<i16>(record, "txpow")?;
        let num_averages = get_scalar_val::<i16>(record, "nave")?;
        let attenuation = get_scalar_val::<i16>(record, "atten")?;
        let lag_to_first_range = get_scalar_val::<i16>(record, "lagfr")?;
        let sample_separation = get_scalar_val::<i16>(record, "smsep")?;
        let error_code = get_scalar_val::<i16>(record, "ercod")?;
        let agc_status = get_scalar_val::<i16>(record, "stat.agc")?;
        let low_power_status = get_scalar_val::<i16>(record, "stat.lopwr")?;
        let search_noise = get_scalar_val::<f32>(record, "noise.search")?;
        let mean_noise = get_scalar_val::<f32>(record, "noise.mean")?;
        let channel = get_scalar_val::<i16>(record, "channel")?;
        let beam_num = get_scalar_val::<i16>(record, "bmnum")?;
        let beam_azimuth = get_scalar_val::<f32>(record, "bmazm")?;
        let scan_flag = get_scalar_val::<i16>(record, "scan")?;
        let offset = get_scalar_val::<i16>(record, "offset")?;
        let rx_rise_time = get_scalar_val::<i16>(record, "rxrise")?;
        let intt_second = get_scalar_val::<i16>(record, "intt.sc")?;
        let intt_microsecond = get_scalar_val::<i32>(record, "intt.us")?;
        let tx_pulse_length = get_scalar_val::<i16>(record, "txpl")?;
        let multi_pulse_increment = get_scalar_val::<i16>(record, "mpinc")?;
        let num_pulses = get_scalar_val::<i16>(record, "mppul")?;
        let num_lags = get_scalar_val::<i16>(record, "mplgs")?;
        let num_lags_extras = get_scalar_val::<i16>(record, "mplgexs").ok();
        let if_mode = get_scalar_val::<i16>(record, "ifmode").ok();
        let num_ranges = get_scalar_val::<i16>(record, "nrang")?;
        let first_range = get_scalar_val::<i16>(record, "frang")?;
        let range_sep = get_scalar_val::<i16>(record, "rsep")?;
        let xcf_flag = get_scalar_val::<i16>(record, "xcf")?;
        let tx_freq = get_scalar_val::<i16>(record, "tfreq")?;
        let max_power = get_scalar_val::<i32>(record, "mxpwr")?;
        let max_noise_level = get_scalar_val::<i32>(record, "lvmax")?;
        let algorithm = get_scalar_val(record, "algorithm").ok();
        let comment = get_scalar_val::<String>(record, "combf")?;
        let fitacf_revision_major = get_scalar_val::<i32>(record, "fitacf.revision.major")?;
        let fitacf_revision_minor = get_scalar_val::<i32>(record, "fitacf.revision.minor")?;
        let sky_noise = get_scalar_val::<f32>(record, "noise.sky")?;
        let lag_zero_noise = get_scalar_val::<f32>(record, "noise.lag0")?;
        let velocity_noise = get_scalar_val::<f32>(record, "noise.vel")?;
        let tdiff = get_scalar_val::<f32>(record, "tdiff").ok();

        // vector fields
        let pulse_table = get_vector_val::<i16>(record, "ptab")?;
        let lag_table = get_vector_val::<i16>(record, "ltab")?;
        let lag_zero_power = get_vector_val::<f32>(record, "pwr0")?;
        let range_list = get_vector_val::<i16>(record, "slist")?;
        let fitted_points = get_vector_val::<i16>(record, "nlag")?;
        let quality_flag = get_vector_val::<i8>(record, "qflg")?;
        let ground_flag = get_vector_val::<i8>(record, "gflg")?;
        let lambda_power = get_vector_val::<f32>(record, "p_l")?;
        let lambda_power_error = get_vector_val::<f32>(record, "p_l_e")?;
        let sigma_power = get_vector_val::<f32>(record, "p_s")?;
        let sigma_power_error = get_vector_val::<f32>(record, "p_s_e")?;
        let velocity = get_vector_val::<f32>(record, "v")?;
        let velocity_error = get_vector_val::<f32>(record, "v_e")?;
        let lambda_spectral_width = get_vector_val::<f32>(record, "w_l")?;
        let lambda_spectral_width_error = get_vector_val::<f32>(record, "w_l_e")?;
        let sigma_spectral_width = get_vector_val::<f32>(record, "w_s")?;
        let sigma_spectral_width_error = get_vector_val::<f32>(record, "w_s_e")?;
        let lambda_std_dev = get_vector_val::<f32>(record, "sd_l")?;
        let sigma_std_dev = get_vector_val::<f32>(record, "sd_s")?;
        let phi_std_dev = get_vector_val::<f32>(record, "sd_phi")?;
        let xcf_quality_flag = get_vector_val::<i8>(record, "x_qflg").ok();
        let xcf_ground_flag = get_vector_val::<i8>(record, "x_gflg").ok();
        let lambda_xcf_power = get_vector_val::<f32>(record, "x_p_l").ok();
        let lambda_xcf_power_error = get_vector_val::<f32>(record, "x_p_l_e").ok();
        let sigma_xcf_power = get_vector_val::<f32>(record, "x_p_s").ok();
        let sigma_xcf_power_error = get_vector_val::<f32>(record, "x_p_s_e").ok();
        let xcf_velocity = get_vector_val::<f32>(record, "x_v").ok();
        let xcf_velocity_error = get_vector_val::<f32>(record, "x_v_e").ok();
        let lambda_xcf_spectral_width = get_vector_val::<f32>(record, "x_w_l").ok();
        let lambda_xcf_spectral_width_error = get_vector_val::<f32>(record, "x_w_l_e").ok();
        let sigma_xcf_spectral_width = get_vector_val::<f32>(record, "x_w_s").ok();
        let sigma_xcf_spectral_width_error = get_vector_val::<f32>(record, "x_w_s_e").ok();
        let lag_zero_phi = get_vector_val::<f32>(record, "phi0").ok();
        let lag_zero_phi_error = get_vector_val::<f32>(record, "phi0_e").ok();
        let elevation = get_vector_val::<f32>(record, "elv").ok();
        let elevation_fitted = get_vector_val::<f32>(record, "elv_fitted").ok();
        let elevation_error = get_vector_val::<f32>(record, "elv_error").ok();
        let elevation_low = get_vector_val::<f32>(record, "elv_low").ok();
        let elevation_high = get_vector_val::<f32>(record, "elv_high").ok();
        let lambda_xcf_std_dev = get_vector_val::<f32>(record, "x_sd_l").ok();
        let sigma_xcf_std_dev = get_vector_val::<f32>(record, "x_sd_s").ok();
        let phi_xcf_std_dev = get_vector_val::<f32>(record, "x_sd_phi").ok();

        Ok(FitacfRecord {
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
            algorithm,
            fitacf_revision_major,
            fitacf_revision_minor,
            sky_noise,
            lag_zero_noise,
            velocity_noise,
            tdiff,
            pulse_table,
            lag_table,
            lag_zero_power,
            range_list,
            fitted_points,
            quality_flag,
            ground_flag,
            lambda_power,
            lambda_power_error,
            sigma_power,
            sigma_power_error,
            velocity,
            velocity_error,
            lambda_spectral_width,
            lambda_spectral_width_error,
            sigma_spectral_width,
            sigma_spectral_width_error,
            lambda_std_dev,
            sigma_std_dev,
            phi_std_dev,
            xcf_quality_flag,
            xcf_ground_flag,
            lambda_xcf_power,
            lambda_xcf_power_error,
            sigma_xcf_power,
            sigma_xcf_power_error,
            xcf_velocity,
            xcf_velocity_error,
            lambda_xcf_spectral_width,
            lambda_xcf_spectral_width_error,
            sigma_xcf_spectral_width,
            sigma_xcf_spectral_width_error,
            lag_zero_phi,
            lag_zero_phi_error,
            elevation,
            elevation_fitted,
            elevation_error,
            elevation_low,
            elevation_high,
            lambda_xcf_std_dev,
            sigma_xcf_std_dev,
            phi_xcf_std_dev,
        })
    }
}
