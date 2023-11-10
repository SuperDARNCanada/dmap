use crate::{
    get_scalar_val, get_vector_val, parse_scalar, parse_vector, read_data, DmapError, DmapType,
    DmapVec, InDmap, RawDmapScalar, RawDmapVector,
};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::Path;

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

/// Writes DmapRecords to path as a Vec<u8>
///
/// # Failures
/// If file cannot be created at path or data cannot be written to file.
pub fn to_file<P: AsRef<Path>, T: DmapRecord>(
    path: P,
    dmap_records: &Vec<T>,
) -> std::io::Result<()> {
    let mut stream = vec![];
    for rec in dmap_records {
        stream.append(&mut rec.to_dmap());
    }
    let mut file = File::create(path)?;
    file.write_all(&stream)?;
    Ok(())
}

pub trait DmapRecord {
    /// Reads from dmap_data and parses into a collection of RawDmapRecord's.
    ///
    /// # Failures
    /// If dmap_data cannot be read or contains invalid data.
    fn read_records(mut dmap_data: impl Read) -> Result<Vec<Self>, DmapError>
    where
        Self: Sized,
    {
        let mut buffer: Vec<u8> = vec![];

        dmap_data
            .read_to_end(&mut buffer)
            .map_err(|_| DmapError::new("Could not read data"))?;

        let mut cursor = Cursor::new(buffer);
        let mut dmap_records: Vec<Self> = vec![];

        while cursor.position() < cursor.get_ref().len() as u64 {
            dmap_records.push(Self::parse_record(&mut cursor)?);
        }
        Ok(dmap_records)
    }

    /// Reads a record starting from cursor position
    fn parse_record(cursor: &mut Cursor<Vec<u8>>) -> Result<Self, DmapError>
    where
        Self: Sized,
    {
        let bytes_already_read = cursor.position();
        let _code = match read_data(cursor, DmapType::INT(0))? {
            DmapType::INT(i) => Ok(i),
            _ => Err(DmapError::new("PARSE RECORD: Invalid code")),
        }?;
        let size = match read_data(cursor, DmapType::INT(0))? {
            DmapType::INT(i) => Ok(i),
            _ => Err(DmapError::new("PARSE RECORD: Invalid size")),
        }?;

        // adding 8 bytes because code and size are part of the record.
        if size as u64
            > cursor.get_ref().len() as u64 - cursor.position()
                + 2 * DmapType::INT(0).get_num_bytes()
        {
            return Err(DmapError::new(
                "PARSE RECORD: Integrity check shows record size bigger than \
                remaining buffer. Data is likely corrupted",
            ));
        } else if size <= 0 {
            return Err(DmapError::new(
                "PARSE RECORD: Integrity check shows record size <= 0. \
                Data is likely corrupted",
            ));
        }

        let num_scalars = match read_data(cursor, DmapType::INT(0))? {
            DmapType::INT(i) => Ok(i),
            _ => Err(DmapError::new("PARSE RECORD: Invalid number of scalars")),
        }?;
        let num_vectors = match read_data(cursor, DmapType::INT(0))? {
            DmapType::INT(i) => Ok(i),
            _ => Err(DmapError::new("PARSE RECORD: Invalid number of vectors")),
        }?;
        if num_scalars <= 0 {
            return Err(DmapError::new(
                "PARSE RECORD: Number of scalars is 0 or negative.",
            ));
        } else if num_vectors <= 0 {
            return Err(DmapError::new(
                "PARSE RECORD: Number of vectors is 0 or negative.",
            ));
        } else if num_scalars + num_vectors > size {
            return Err(DmapError::new(
                "PARSE RECORD: Invalid number of record elements. \
                Vector or scalar field is likely corrupted.",
            ));
        }

        let mut scalars = HashMap::new();
        for _ in 0..num_scalars {
            let (name, val) = parse_scalar(cursor)?;
            scalars.insert(name, val);
        }

        let mut vectors = HashMap::new();
        for _ in 0..num_vectors {
            let (name, val) = parse_vector(cursor, size)?;
            vectors.insert(name, val);
        }

        if cursor.position() - bytes_already_read != size as u64 {
            return Err(DmapError::new(
                format!(
                    "PARSE RECORD: Bytes read {} does not match the records size field {}",
                    cursor.position() - bytes_already_read,
                    size
                )
                .as_str(),
            ));
        }

        Self::new(&mut scalars, &mut vectors)
    }

    /// Creates a new object from the parsed scalars and vectors
    fn new(
        scalars: &mut HashMap<String, RawDmapScalar>,
        vectors: &mut HashMap<String, RawDmapVector>,
    ) -> Result<Self, DmapError>
    where
        Self: Sized;

    /// Converts a DmapRecord with metadata to a vector of raw bytes for writing
    fn to_dmap(&self) -> Vec<u8> {
        let (num_scalars, num_vectors, mut data_bytes) = self.to_bytes();
        let mut bytes: Vec<u8> = vec![];
        bytes.extend((65537_i32).data_to_bytes()); // No idea why this is what it is, copied from backscatter
        bytes.extend((data_bytes.len() as i32 + 16).data_to_bytes()); // +16 for code, length, num_scalars, num_vectors
        bytes.extend(num_scalars.data_to_bytes());
        bytes.extend(num_vectors.data_to_bytes());
        bytes.append(&mut data_bytes); // consumes data_bytes
        bytes
    }

    /// Converts only the data within the record to bytes (no metadata)
    fn to_bytes(&self) -> (i32, i32, Vec<u8>);
}

#[derive(Debug, PartialEq, Clone)]
pub struct IqdatRecord {
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
    pub num_lags_extras: Option<i16>, // not present in pyDARNio
    pub if_mode: Option<i16>,         // not present in pyDARNio
    pub num_ranges: i16,
    pub first_range: i16,
    pub range_sep: i16,
    pub xcf_flag: i16,
    pub tx_freq: i16,
    pub max_power: i32,
    pub max_noise_level: i32,
    pub comment: String,
    pub iqdat_revision_major: i32,
    pub iqdat_revision_minor: i32,
    pub num_sequences: i32,
    pub num_channels: i32,
    pub num_samples: i32,
    pub num_skipped_samples: i32,

    // vector fields
    pub pulse_table: DmapVec<i16>,
    pub lag_table: DmapVec<i16>,
    pub seconds_past_epoch: DmapVec<i32>,
    pub microseconds_past_epoch: DmapVec<i32>,
    pub sequence_attenuation: DmapVec<i16>,
    pub sequence_noise: DmapVec<f32>,
    pub sequence_offset: DmapVec<i32>,
    pub sequence_size: DmapVec<i32>,
    // pub bad_sequence_flag: DmapVec<i32>,     // not present in pyDARNio
    // pub bad_pulse_flag: DmapVec<i32>,        // not present in pyDARNio
    pub data: DmapVec<i16>,
}
impl DmapRecord for IqdatRecord {
    fn new(
        scalars: &mut HashMap<String, RawDmapScalar>,
        vectors: &mut HashMap<String, RawDmapVector>,
    ) -> Result<IqdatRecord, DmapError> {
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
        let iqdat_revision_major = get_scalar_val::<i32>(scalars, "iqdata.revision.major")?;
        let iqdat_revision_minor = get_scalar_val::<i32>(scalars, "iqdata.revision.minor")?;
        let num_sequences = get_scalar_val::<i32>(scalars, "seqnum")?;
        let num_channels = get_scalar_val::<i32>(scalars, "chnnum")?;
        let num_samples = get_scalar_val::<i32>(scalars, "smpnum")?;
        let num_skipped_samples = get_scalar_val::<i32>(scalars, "skpnum")?;

        // vector fields
        let pulse_table = get_vector_val::<i16>(vectors, "ptab")?;
        let lag_table = get_vector_val::<i16>(vectors, "ltab")?;
        let seconds_past_epoch = get_vector_val::<i32>(vectors, "tsc")?;
        let microseconds_past_epoch = get_vector_val::<i32>(vectors, "tus")?;
        let sequence_attenuation = get_vector_val::<i16>(vectors, "tatten")?;
        let sequence_noise = get_vector_val::<f32>(vectors, "tnoise")?;
        let sequence_offset = get_vector_val::<i32>(vectors, "toff")?;
        let sequence_size = get_vector_val::<i32>(vectors, "tsze")?;
        // let bad_sequence_flag = get_vector_val::<i32>(vectors, "tbadtr")?;
        // let bad_pulse_flag = get_vector_val::<i32>(vectors, "badtr")?;
        let data = get_vector_val::<i16>(vectors, "data")?;

        Ok(IqdatRecord {
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
            iqdat_revision_major,
            iqdat_revision_minor,
            num_sequences,
            num_channels,
            num_samples,
            num_skipped_samples,
            pulse_table,
            lag_table,
            seconds_past_epoch,
            microseconds_past_epoch,
            sequence_attenuation,
            sequence_noise,
            sequence_offset,
            sequence_size,
            // bad_sequence_flag,
            // bad_pulse_flag,
            data,
        })
    }
    fn to_bytes(&self) -> (i32, i32, Vec<u8>) {
        let mut data_bytes: Vec<u8> = vec![];
        let mut num_scalars: i32 = 50; // number of required scalar fields

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
        data_bytes.extend(self.iqdat_revision_major.to_bytes("iqdata.revision.major"));
        data_bytes.extend(self.iqdat_revision_minor.to_bytes("iqdata.revision.minor"));
        data_bytes.extend(self.comment.to_bytes("combf"));
        data_bytes.extend(self.num_sequences.to_bytes("seqnum"));
        data_bytes.extend(self.num_channels.to_bytes("chnnum"));
        data_bytes.extend(self.num_samples.to_bytes("smpnum"));
        data_bytes.extend(self.num_skipped_samples.to_bytes("skpnum"));

        // vector fields
        let num_vectors = 9;
        data_bytes.extend(self.pulse_table.to_bytes("ptab"));
        data_bytes.extend(self.lag_table.to_bytes("ltab"));
        data_bytes.extend(self.seconds_past_epoch.to_bytes("tsc"));
        data_bytes.extend(self.microseconds_past_epoch.to_bytes("tus"));
        data_bytes.extend(self.sequence_attenuation.to_bytes("tatten"));
        data_bytes.extend(self.sequence_noise.to_bytes("tnoise"));
        data_bytes.extend(self.sequence_offset.to_bytes("toff"));
        data_bytes.extend(self.sequence_size.to_bytes("tsze"));
        // data_bytes.extend(self.bad_sequence_flag.to_bytes("tbadtr"));    // not present in pyDARNio
        // data_bytes.extend(self.bad_pulse_flag.to_bytes("badtr"));    // not present in pyDARNio
        data_bytes.extend(self.data.to_bytes("data"));

        (num_scalars, num_vectors, data_bytes)
    }
}

#[derive(Debug, PartialEq, Clone)]
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
    pub pulse_table: DmapVec<i16>,
    pub lag_table: DmapVec<i16>,
    pub lag_zero_power: DmapVec<f32>,
    pub range_list: DmapVec<i16>,
    pub acfs: DmapVec<f32>,
    pub xcfs: Option<DmapVec<f32>>,
}
impl DmapRecord for RawacfRecord {
    fn new(
        scalars: &mut HashMap<String, RawDmapScalar>,
        vectors: &mut HashMap<String, RawDmapVector>,
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
        let pulse_table = get_vector_val::<i16>(vectors, "ptab")?;
        let lag_table = get_vector_val::<i16>(vectors, "ltab")?;
        let lag_zero_power = get_vector_val::<f32>(vectors, "pwr0")?;
        let range_list = get_vector_val::<i16>(vectors, "slist")?;
        let acfs = get_vector_val::<f32>(vectors, "acfd")?;
        let xcfs = get_vector_val::<f32>(vectors, "xcfd").ok();

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
}

#[derive(Debug, PartialEq, Clone)]
pub struct FitacfRecord {
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
    pub algorithm: Option<String>,
    pub fitacf_revision_major: i32,
    pub fitacf_revision_minor: i32,
    pub sky_noise: f32,
    pub lag_zero_noise: f32,
    pub velocity_noise: f32,
    pub tdiff: Option<f32>,

    // vector fields
    pub pulse_table: DmapVec<i16>,
    pub lag_table: DmapVec<i16>,
    pub lag_zero_power: DmapVec<f32>,
    pub range_list: DmapVec<i16>,
    pub fitted_points: DmapVec<i16>,
    pub quality_flag: DmapVec<i8>,
    pub ground_flag: DmapVec<i8>,
    pub lambda_power: DmapVec<f32>,
    pub lambda_power_error: DmapVec<f32>,
    pub sigma_power: DmapVec<f32>,
    pub sigma_power_error: DmapVec<f32>,
    pub velocity: DmapVec<f32>,
    pub velocity_error: DmapVec<f32>,
    pub lambda_spectral_width: DmapVec<f32>,
    pub lambda_spectral_width_error: DmapVec<f32>,
    pub sigma_spectral_width: DmapVec<f32>,
    pub sigma_spectral_width_error: DmapVec<f32>,
    pub lambda_std_dev: DmapVec<f32>,
    pub sigma_std_dev: DmapVec<f32>,
    pub phi_std_dev: DmapVec<f32>,
    pub xcf_quality_flag: Option<DmapVec<i8>>,
    pub xcf_ground_flag: Option<DmapVec<i8>>,
    pub lambda_xcf_power: Option<DmapVec<f32>>,
    pub lambda_xcf_power_error: Option<DmapVec<f32>>,
    pub sigma_xcf_power: Option<DmapVec<f32>>,
    pub sigma_xcf_power_error: Option<DmapVec<f32>>,
    pub xcf_velocity: Option<DmapVec<f32>>,
    pub xcf_velocity_error: Option<DmapVec<f32>>,
    pub lambda_xcf_spectral_width: Option<DmapVec<f32>>,
    pub lambda_xcf_spectral_width_error: Option<DmapVec<f32>>,
    pub sigma_xcf_spectral_width: Option<DmapVec<f32>>,
    pub sigma_xcf_spectral_width_error: Option<DmapVec<f32>>,
    pub lag_zero_phi: Option<DmapVec<f32>>,
    pub lag_zero_phi_error: Option<DmapVec<f32>>,
    pub elevation: Option<DmapVec<f32>>,
    pub elevation_fitted: Option<DmapVec<f32>>,
    pub elevation_error: Option<DmapVec<f32>>,
    pub elevation_low: Option<DmapVec<f32>>,
    pub elevation_high: Option<DmapVec<f32>>,
    pub lambda_xcf_std_dev: Option<DmapVec<f32>>,
    pub sigma_xcf_std_dev: Option<DmapVec<f32>>,
    pub phi_xcf_std_dev: Option<DmapVec<f32>>,
}
impl DmapRecord for FitacfRecord {
    fn new(
        scalars: &mut HashMap<String, RawDmapScalar>,
        vectors: &mut HashMap<String, RawDmapVector>,
    ) -> Result<FitacfRecord, DmapError> {
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
        let algorithm = get_scalar_val::<String>(scalars, "algorithm").ok();
        let comment = get_scalar_val::<String>(scalars, "combf")?;
        let fitacf_revision_major = get_scalar_val::<i32>(scalars, "fitacf.revision.major")?;
        let fitacf_revision_minor = get_scalar_val::<i32>(scalars, "fitacf.revision.minor")?;
        let sky_noise = get_scalar_val::<f32>(scalars, "noise.sky")?;
        let lag_zero_noise = get_scalar_val::<f32>(scalars, "noise.lag0")?;
        let velocity_noise = get_scalar_val::<f32>(scalars, "noise.vel")?;
        let tdiff = get_scalar_val::<f32>(scalars, "tdiff").ok();

        // vector fields
        let pulse_table = get_vector_val::<i16>(vectors, "ptab")?;
        let lag_table = get_vector_val::<i16>(vectors, "ltab")?;
        let lag_zero_power = get_vector_val::<f32>(vectors, "pwr0")?;
        let range_list = get_vector_val::<i16>(vectors, "slist")?;
        let fitted_points = get_vector_val::<i16>(vectors, "nlag")?;
        let quality_flag = get_vector_val::<i8>(vectors, "qflg")?;
        let ground_flag = get_vector_val::<i8>(vectors, "gflg")?;
        let lambda_power = get_vector_val::<f32>(vectors, "p_l")?;
        let lambda_power_error = get_vector_val::<f32>(vectors, "p_l_e")?;
        let sigma_power = get_vector_val::<f32>(vectors, "p_s")?;
        let sigma_power_error = get_vector_val::<f32>(vectors, "p_s_e")?;
        let velocity = get_vector_val::<f32>(vectors, "v")?;
        let velocity_error = get_vector_val::<f32>(vectors, "v_e")?;
        let lambda_spectral_width = get_vector_val::<f32>(vectors, "w_l")?;
        let lambda_spectral_width_error = get_vector_val::<f32>(vectors, "w_l_e")?;
        let sigma_spectral_width = get_vector_val::<f32>(vectors, "w_s")?;
        let sigma_spectral_width_error = get_vector_val::<f32>(vectors, "w_s_e")?;
        let lambda_std_dev = get_vector_val::<f32>(vectors, "sd_l")?;
        let sigma_std_dev = get_vector_val::<f32>(vectors, "sd_s")?;
        let phi_std_dev = get_vector_val::<f32>(vectors, "sd_phi")?;
        let xcf_quality_flag = get_vector_val::<i8>(vectors, "x_qflg").ok();
        let xcf_ground_flag = get_vector_val::<i8>(vectors, "x_gflg").ok();
        let lambda_xcf_power = get_vector_val::<f32>(vectors, "x_p_l").ok();
        let lambda_xcf_power_error = get_vector_val::<f32>(vectors, "x_p_l_e").ok();
        let sigma_xcf_power = get_vector_val::<f32>(vectors, "x_p_s").ok();
        let sigma_xcf_power_error = get_vector_val::<f32>(vectors, "x_p_s_e").ok();
        let xcf_velocity = get_vector_val::<f32>(vectors, "x_v").ok();
        let xcf_velocity_error = get_vector_val::<f32>(vectors, "x_v_e").ok();
        let lambda_xcf_spectral_width = get_vector_val::<f32>(vectors, "x_w_l").ok();
        let lambda_xcf_spectral_width_error = get_vector_val::<f32>(vectors, "x_w_l_e").ok();
        let sigma_xcf_spectral_width = get_vector_val::<f32>(vectors, "x_w_s").ok();
        let sigma_xcf_spectral_width_error = get_vector_val::<f32>(vectors, "x_w_s_e").ok();
        let lag_zero_phi = get_vector_val::<f32>(vectors, "phi0").ok();
        let lag_zero_phi_error = get_vector_val::<f32>(vectors, "phi0_e").ok();
        let elevation = get_vector_val::<f32>(vectors, "elv").ok();
        let elevation_fitted = get_vector_val::<f32>(vectors, "elv_fitted").ok();
        let elevation_error = get_vector_val::<f32>(vectors, "elv_error").ok();
        let elevation_low = get_vector_val::<f32>(vectors, "elv_low").ok();
        let elevation_high = get_vector_val::<f32>(vectors, "elv_high").ok();
        let lambda_xcf_std_dev = get_vector_val::<f32>(vectors, "x_sd_l").ok();
        let sigma_xcf_std_dev = get_vector_val::<f32>(vectors, "x_sd_s").ok();
        let phi_xcf_std_dev = get_vector_val::<f32>(vectors, "x_sd_phi").ok();

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
    fn to_bytes(&self) -> (i32, i32, Vec<u8>) {
        let mut data_bytes: Vec<u8> = vec![];
        let mut num_scalars: i32 = 49; // number of required scalar fields

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
        if let Some(x) = &self.algorithm {
            data_bytes.extend(x.to_bytes("algorithm"));
            num_scalars += 1;
        }
        data_bytes.extend(self.fitacf_revision_major.to_bytes("fitacf.revision.major"));
        data_bytes.extend(self.fitacf_revision_minor.to_bytes("fitacf.revision.minor"));
        data_bytes.extend(self.sky_noise.to_bytes("noise.sky"));
        data_bytes.extend(self.lag_zero_noise.to_bytes("noise.lag0"));
        data_bytes.extend(self.velocity_noise.to_bytes("noise.vel"));
        if let Some(x) = self.tdiff {
            data_bytes.extend(x.to_bytes("tdiff"));
            num_scalars += 1;
        }

        // vector fields
        let mut num_vectors: i32 = 20;
        data_bytes.extend(self.pulse_table.to_bytes("ptab"));
        data_bytes.extend(self.lag_table.to_bytes("ltab"));
        data_bytes.extend(self.lag_zero_power.to_bytes("pwr0"));
        data_bytes.extend(self.range_list.to_bytes("slist"));
        data_bytes.extend(self.fitted_points.to_bytes("nlag"));
        data_bytes.extend(self.quality_flag.to_bytes("qflg"));
        data_bytes.extend(self.ground_flag.to_bytes("gflg"));
        data_bytes.extend(self.lambda_power.to_bytes("p_l"));
        data_bytes.extend(self.lambda_power_error.to_bytes("p_l_e"));
        data_bytes.extend(self.sigma_power.to_bytes("p_s"));
        data_bytes.extend(self.sigma_power_error.to_bytes("p_s_e"));
        data_bytes.extend(self.velocity.to_bytes("v"));
        data_bytes.extend(self.velocity_error.to_bytes("v_e"));
        data_bytes.extend(self.lambda_spectral_width.to_bytes("w_l"));
        data_bytes.extend(self.lambda_spectral_width_error.to_bytes("w_l_e"));
        data_bytes.extend(self.sigma_spectral_width.to_bytes("w_s"));
        data_bytes.extend(self.sigma_spectral_width_error.to_bytes("w_s_e"));
        data_bytes.extend(self.lambda_std_dev.to_bytes("sd_l"));
        data_bytes.extend(self.sigma_std_dev.to_bytes("sd_s"));
        data_bytes.extend(self.phi_std_dev.to_bytes("sd_phi"));
        if let Some(x) = &self.xcf_quality_flag {
            data_bytes.extend(x.to_bytes("x_qflg"));
            num_vectors += 1;
        }
        if let Some(x) = &self.xcf_ground_flag {
            data_bytes.extend(x.to_bytes("x_gflg"));
            num_vectors += 1;
        }
        if let Some(x) = &self.lambda_xcf_power {
            data_bytes.extend(x.to_bytes("x_p_l"));
            num_vectors += 1;
        }
        if let Some(x) = &self.lambda_xcf_power_error {
            data_bytes.extend(x.to_bytes("x_p_l_e"));
            num_vectors += 1;
        }
        if let Some(x) = &self.sigma_xcf_power {
            data_bytes.extend(x.to_bytes("x_p_s"));
            num_vectors += 1;
        }
        if let Some(x) = &self.sigma_xcf_power_error {
            data_bytes.extend(x.to_bytes("x_p_s_e"));
            num_vectors += 1;
        }
        if let Some(x) = &self.xcf_velocity {
            data_bytes.extend(x.to_bytes("x_v"));
            num_vectors += 1;
        }
        if let Some(x) = &self.xcf_velocity_error {
            data_bytes.extend(x.to_bytes("x_v_e"));
            num_vectors += 1;
        }
        if let Some(x) = &self.lambda_xcf_spectral_width {
            data_bytes.extend(x.to_bytes("x_w_l"));
            num_vectors += 1;
        }
        if let Some(x) = &self.lambda_xcf_spectral_width_error {
            data_bytes.extend(x.to_bytes("x_w_l_e"));
            num_vectors += 1;
        }
        if let Some(x) = &self.sigma_xcf_spectral_width {
            data_bytes.extend(x.to_bytes("x_w_s"));
            num_vectors += 1;
        }
        if let Some(x) = &self.sigma_xcf_spectral_width_error {
            data_bytes.extend(x.to_bytes("x_w_s_e"));
            num_vectors += 1;
        }
        if let Some(x) = &self.lag_zero_phi {
            data_bytes.extend(x.to_bytes("phi0"));
            num_vectors += 1;
        }
        if let Some(x) = &self.lag_zero_phi_error {
            data_bytes.extend(x.to_bytes("phi0_e"));
            num_vectors += 1;
        }
        if let Some(x) = &self.elevation {
            data_bytes.extend(x.to_bytes("elv"));
            num_vectors += 1;
        }
        if let Some(x) = &self.elevation_fitted {
            data_bytes.extend(x.to_bytes("elv_fitted"));
            num_vectors += 1;
        }
        if let Some(x) = &self.elevation_error {
            data_bytes.extend(x.to_bytes("elv_error"));
            num_vectors += 1;
        }
        if let Some(x) = &self.elevation_low {
            data_bytes.extend(x.to_bytes("elv_low"));
            num_vectors += 1;
        }
        if let Some(x) = &self.elevation_high {
            data_bytes.extend(x.to_bytes("elv_high"));
            num_vectors += 1;
        }
        if let Some(x) = &self.lambda_xcf_std_dev {
            data_bytes.extend(x.to_bytes("x_sd_l"));
            num_vectors += 1;
        }
        if let Some(x) = &self.sigma_xcf_std_dev {
            data_bytes.extend(x.to_bytes("x_sd_s"));
            num_vectors += 1;
        }
        if let Some(x) = &self.phi_xcf_std_dev {
            data_bytes.extend(x.to_bytes("x_sd_phi"));
            num_vectors += 1;
        }

        (num_scalars, num_vectors, data_bytes)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct GridRecord {
    // scalar fields
    pub start_year: i16,
    pub start_month: i16,
    pub start_day: i16,
    pub start_hour: i16,
    pub start_minute: i16,
    pub start_second: i16,
    pub end_year: i16,
    pub end_month: i16,
    pub end_day: i16,
    pub end_hour: i16,
    pub end_minute: i16,
    pub end_second: i16,

    // vector fields
    pub station_ids: DmapVec<i16>,
    pub channels: DmapVec<i16>,
    pub num_vectors: DmapVec<i16>,
    pub freq: DmapVec<f32>,
    pub grid_major_revision: DmapVec<i16>,
    pub grid_minor_revision: DmapVec<i16>,
    pub program_ids: DmapVec<i16>,
    pub noise_mean: DmapVec<f32>,
    pub noise_stddev: DmapVec<f32>,
    pub groundscatter: DmapVec<i16>,
    pub velocity_min: DmapVec<f32>,
    pub velocity_max: DmapVec<f32>,
    pub power_min: DmapVec<f32>,
    pub power_max: DmapVec<f32>,
    pub spectral_width_min: DmapVec<f32>,
    pub spectral_width_max: DmapVec<f32>,
    pub velocity_error_min: DmapVec<f32>,
    pub velocity_error_max: DmapVec<f32>,
    pub magnetic_lat: DmapVec<f32>,
    pub magnetic_lon: DmapVec<f32>,
    pub magnetic_azi: DmapVec<f32>,
    pub station_id_vector: DmapVec<i16>,
    pub channel_vector: DmapVec<i16>,
    pub grid_cell_index: DmapVec<i32>,
    pub velocity_median: DmapVec<f32>,
    pub velocity_stddev: DmapVec<f32>,
    pub power_median: DmapVec<f32>,
    pub power_stddev: DmapVec<f32>,
    pub spectral_width_median: DmapVec<f32>,
    pub spectral_width_stddev: DmapVec<f32>,
}
impl DmapRecord for GridRecord {
    fn new(
        scalars: &mut HashMap<String, RawDmapScalar>,
        vectors: &mut HashMap<String, RawDmapVector>,
    ) -> Result<GridRecord, DmapError> {
        // scalar fields
        let start_year = get_scalar_val::<i16>(scalars, "start.year")?;
        let start_month = get_scalar_val::<i16>(scalars, "start.month")?;
        let start_day = get_scalar_val::<i16>(scalars, "start.day")?;
        let start_hour = get_scalar_val::<i16>(scalars, "start.hour")?;
        let start_minute = get_scalar_val::<i16>(scalars, "start.minute")?;
        let start_second = get_scalar_val::<i16>(scalars, "start.second")?;
        let end_year = get_scalar_val::<i16>(scalars, "end.year")?;
        let end_month = get_scalar_val::<i16>(scalars, "end.month")?;
        let end_day = get_scalar_val::<i16>(scalars, "end.day")?;
        let end_hour = get_scalar_val::<i16>(scalars, "end.hour")?;
        let end_minute = get_scalar_val::<i16>(scalars, "end.minute")?;
        let end_second = get_scalar_val::<i16>(scalars, "end.second")?;

        // vector fields
        let station_ids = get_vector_val::<i16>(vectors, "stid")?;
        let channels = get_vector_val::<i16>(vectors, "channel")?;
        let num_vectors = get_vector_val::<i16>(vectors, "nvec")?;
        let freq = get_vector_val::<f32>(vectors, "freq")?;
        let grid_major_revision = get_vector_val::<i16>(vectors, "major.revision")?;
        let grid_minor_revision = get_vector_val::<i16>(vectors, "minor.revision")?;
        let program_ids = get_vector_val::<i16>(vectors, "program.id")?;
        let noise_mean = get_vector_val::<f32>(vectors, "noise.mean")?;
        let noise_stddev = get_vector_val::<f32>(vectors, "noise.sd")?;
        let groundscatter = get_vector_val::<i16>(vectors, "gsct")?;
        let velocity_min = get_vector_val::<f32>(vectors, "v.min")?;
        let velocity_max = get_vector_val::<f32>(vectors, "v.max")?;
        let power_min = get_vector_val::<f32>(vectors, "p.min")?;
        let power_max = get_vector_val::<f32>(vectors, "p.max")?;
        let spectral_width_min = get_vector_val::<f32>(vectors, "w.min")?;
        let spectral_width_max = get_vector_val::<f32>(vectors, "w.max")?;
        let velocity_error_min = get_vector_val::<f32>(vectors, "ve.min")?;
        let velocity_error_max = get_vector_val::<f32>(vectors, "ve.max")?;
        let magnetic_lat = get_vector_val::<f32>(vectors, "vector.mlat")?;
        let magnetic_lon = get_vector_val::<f32>(vectors, "vector.mlon")?;
        let magnetic_azi = get_vector_val::<f32>(vectors, "vector.kvect")?;
        let station_id_vector = get_vector_val::<i16>(vectors, "vector.stid")?;
        let channel_vector = get_vector_val::<i16>(vectors, "vector.channel")?;
        let grid_cell_index = get_vector_val::<i32>(vectors, "vector.index")?;
        let velocity_median = get_vector_val::<f32>(vectors, "vector.vel.median")?;
        let velocity_stddev = get_vector_val::<f32>(vectors, "vector.vel.sd")?;
        let power_median = get_vector_val::<f32>(vectors, "vector.pwr.median")?;
        let power_stddev = get_vector_val::<f32>(vectors, "vector.pwr.sd")?;
        let spectral_width_median = get_vector_val::<f32>(vectors, "vector.wdt.median")?;
        let spectral_width_stddev = get_vector_val::<f32>(vectors, "vector.wdt.sd")?;

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
        let mut num_scalars: i32 = 12; // number of required scalar fields

        // scalar fields
        data_bytes.extend(self.start_year.to_bytes("start.year"))?;
        data_bytes.extend(self.start_month.to_bytes("start.month"))?;
        data_bytes.extend(self.start_day.to_bytes("start.day"))?;
        data_bytes.extend(self.start_hour.to_bytes("start.hour"))?;
        data_bytes.extend(self.start_minute.to_bytes("start.minute"))?;
        data_bytes.extend(self.start_second.to_bytes("start.second"))?;
        data_bytes.extend(self.end_year.to_bytes("end.year"))?;
        data_bytes.extend(self.end_month.to_bytes("end.month"))?;
        data_bytes.extend(self.end_day.to_bytes("end.day"))?;
        data_bytes.extend(self.end_hour.to_bytes("end.hour"))?;
        data_bytes.extend(self.end_minute.to_bytes("end.minute"))?;
        data_bytes.extend(self.end_second.to_bytes("end.second"))?;

        // vector fields
        data_bytes.extend(self.station_ids.to_bytes("stid"))?;
        data_bytes.extend(self.channels.to_bytes("channel"))?;
        data_bytes.extend(self.num_vectors.to_bytes("nvec"))?;
        data_bytes.extend(self.freq.to_bytes("freq"))?;
        data_bytes.extend(self.grid_major_revision.to_bytes("major.revision"))?;
        data_bytes.extend(self.grid_minor_revision.to_bytes("minor.revision"))?;
        data_bytes.extend(self.program_ids.to_bytes("program.id"))?;
        data_bytes.extend(self.noise_mean.to_bytes("noise.mean"))?;
        data_bytes.extend(self.noise_stddev.to_bytes("noise.sd"))?;
        data_bytes.extend(self.groundscatter.to_bytes("gsct"))?;
        data_bytes.extend(self.velocity_min.to_bytes("v.min"))?;
        data_bytes.extend(self.velocity_max.to_bytes("v.max"))?;
        data_bytes.extend(self.power_min.to_bytes("p.min"))?;
        data_bytes.extend(self.power_max.to_bytes("p.max"))?;
        data_bytes.extend(self.spectral_width_min.to_bytes("w.min"))?;
        data_bytes.extend(self.spectral_width_max.to_bytes("w.max"))?;
        data_bytes.extend(self.velocity_error_min.to_bytes("ve.min"))?;
        data_bytes.extend(self.velocity_error_max.to_bytes("ve.max"))?;
        data_bytes.extend(self.magnetic_lat.to_bytes("vector.mlat"))?;
        data_bytes.extend(self.magnetic_lon.to_bytes("vector.mlon"))?;
        data_bytes.extend(self.magnetic_azi.to_bytes("vector.kvect"))?;
        data_bytes.extend(self.station_id_vector.to_bytes("vector.stid"))?;
        data_bytes.extend(self.channel_vector.to_bytes("vector.channel"))?;
        data_bytes.extend(self.grid_cell_index.to_bytes("vector.index"))?;
        data_bytes.extend(self.velocity_median.to_bytes("vector.vel.median"))?;
        data_bytes.extend(self.velocity_stddev.to_bytes("vector.vel.sd"))?;
        data_bytes.extend(self.power_median.to_bytes("vector.pwr.median"))?;
        data_bytes.extend(self.power_stddev.to_bytes("vector.pwr.sd"))?;
        data_bytes.extend(self.spectral_width_median.to_bytes("vector.wdt.median"))?;
        data_bytes.extend(self.spectral_width_stddev.to_bytes("vector.wdt.sd"))?;

        (num_scalars, num_vectors, data_bytes)
    }
}

#[derive(Debug, PartialEq, Clone)]
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
    imf_delay: i16,
    imf_bx: f64,
    imf_by: f64,
    imf_bz: f64,
    imf_vx: f64,
    imf_tilt: f64,
    imf_kp: f64,
    model_angle: Option<String>, // map_addmodel fields
    model_level: Option<String>, // map_addmodel fields
    model_tilt: Option<String>,  // map_addmodel fields
    model_name: Option<String>,  // map_addmodel fields
    hemisphere: i16,
    igrf_flag: i16,
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
    station_ids: DmapVec<i16>,
    channels: DmapVec<i16>,
    num_vectors: DmapVec<i16>,
    frequencies: DmapVec<f32>,
    major_revisions: DmapVec<i16>,
    minor_revisions: DmapVec<i16>,
    program_ids: DmapVec<i16>,
    noise_means: DmapVec<f32>,
    noise_std_devs: DmapVec<f32>,
    groundscatter_flags: DmapVec<i16>,
    min_velocities: DmapVec<f32>,
    max_velocities: DmapVec<f32>,
    min_powers: DmapVec<f32>,
    max_powers: DmapVec<f32>,
    min_spectral_width: DmapVec<f32>,
    max_spectral_width: DmapVec<f32>,
    velocity_errors_min: DmapVec<f32>,
    velocity_errors_max: DmapVec<f32>,
    magnetic_latitudes: DmapVec<f32>,           // partial fields
    magnetic_longitudes: DmapVec<f32>,          // partial fields
    magnetic_azimuth: DmapVec<f32>,             // partial fields
    vector_station_ids: DmapVec<i16>,           // partial fields
    vector_channels: DmapVec<i16>,              // partial fields
    vector_index: DmapVec<i32>,                 // partial fields
    vector_velocity_median: DmapVec<f32>,       // partial fields
    vector_velocity_std_dev: DmapVec<f32>,      // partial fields
    vector_power_median: Option<DmapVec<f32>>,  // -ext fields
    vector_power_std_dev: Option<DmapVec<f32>>, // -ext fields
    vector_spectral_width_median: Option<DmapVec<f32>>, // -ext fields
    vector_spectral_width_std_dev: Option<DmapVec<f32>>, // -ext fields
    l_value: Option<DmapVec<f64>>,              // map_addfit fields
    m_value: Option<DmapVec<f64>>,              // map_addfit fields
    coefficient_value: Option<DmapVec<f64>>,    // map_addfit fields
    sigma_error: Option<DmapVec<f64>>,          // map_addfit fields
    model_magnetic_latitude: Option<DmapVec<f32>>, // map_addhmb fields
    model_magnetic_longitude: Option<DmapVec<f32>>, // map_addhmb fields
    model_magnetic_azimuth: Option<DmapVec<f32>>, // map_addhmb fields
    model_velocity_median: Option<DmapVec<f32>>, // map_addhmb fields
    boundary_magnetic_latitude: Option<DmapVec<f32>>, // map_addhmb fields
    boundary_magnetic_longitude: Option<DmapVec<f32>>, // map_addhmb fields
}
impl DmapRecord for MapRecord {
    fn new(
        scalars: &mut HashMap<String, RawDmapScalar>,
        vectors: &mut HashMap<String, RawDmapVector>,
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
        let imf_delay = get_scalar_val::<i16>(scalars, "IMF.delay")?;
        let imf_bx = get_scalar_val::<f64>(scalars, "IMF.Bx")?;
        let imf_by = get_scalar_val::<f64>(scalars, "IMF.By")?;
        let imf_bz = get_scalar_val::<f64>(scalars, "IMF.Bz")?;
        let imf_vx = get_scalar_val::<f64>(scalars, "IMF.Vx")?;
        let imf_tilt = get_scalar_val::<f64>(scalars, "IMF.tilt")?;
        let imf_kp = get_scalar_val::<f64>(scalars, "IMT.Kp")?;
        let model_angle = get_scalar_val::<String>(scalars, "model.angle").ok();
        let model_level = get_scalar_val::<String>(scalars, "model.level").ok();
        let model_tilt = get_scalar_val::<String>(scalars, "model.tilt").ok();
        let model_name = get_scalar_val::<String>(scalars, "model.name").ok();
        let hemisphere = get_scalar_val::<i16>(scalars, "hemisphere")?;
        let igrf_flag = get_scalar_val::<i16>(scalars, "noigrf")?;
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
        let station_ids = get_vector_val::<i16>(vectors, "stid")?;
        let channels = get_vector_val::<i16>(vectors, "channel")?;
        let num_vectors = get_vector_val::<i16>(vectors, "nvec")?;
        let frequencies = get_vector_val::<f32>(vectors, "freq")?;
        let major_revisions = get_vector_val::<i16>(vectors, "major.revision")?;
        let minor_revisions = get_vector_val::<i16>(vectors, "minor.revision")?;
        let program_ids = get_vector_val::<i16>(vectors, "program.id")?;
        let noise_means = get_vector_val::<f32>(vectors, "noise.mean")?;
        let noise_std_devs = get_vector_val::<f32>(vectors, "noise.sd")?;
        let groundscatter_flags = get_vector_val::<i16>(vectors, "gsct")?;
        let min_velocities = get_vector_val::<f32>(vectors, "v.min")?;
        let max_velocities = get_vector_val::<f32>(vectors, "v.max")?;
        let min_powers = get_vector_val::<f32>(vectors, "p.min")?;
        let max_powers = get_vector_val::<f32>(vectors, "p.max")?;
        let min_spectral_width = get_vector_val::<f32>(vectors, "w.min")?;
        let max_spectral_width = get_vector_val::<f32>(vectors, "w.max")?;
        let velocity_errors_min = get_vector_val::<f32>(vectors, "ve.min")?;
        let velocity_errors_max = get_vector_val::<f32>(vectors, "ve.max")?;
        let magnetic_latitudes = get_vector_val::<f32>(vectors, "vector.mlat")?;
        let magnetic_longitudes = get_vector_val::<f32>(vectors, "vector.mlon")?;
        let magnetic_azimuth = get_vector_val::<f32>(vectors, "vector.kvect")?;
        let vector_station_ids = get_vector_val::<i16>(vectors, "vector.stid")?;
        let vector_channels = get_vector_val::<i16>(vectors, "vector.channel")?;
        let vector_index = get_vector_val::<i32>(vectors, "vector.index")?;
        let vector_velocity_median = get_vector_val::<f32>(vectors, "vector.vel.median")?;
        let vector_velocity_std_dev = get_vector_val::<f32>(vectors, "vector.vel.sd")?;
        let vector_power_median = get_vector_val::<f32>(vectors, "vector.pwr.median").ok();
        let vector_power_std_dev = get_vector_val::<f32>(vectors, "vector.pwr.sd").ok();
        let vector_spectral_width_median = get_vector_val::<f32>(vectors, "vector.wdt.median").ok();
        let vector_spectral_width_std_dev = get_vector_val::<f32>(vectors, "vector.wdt.sd").ok();
        let l_value = get_vector_val::<f64>(vectors, "N").ok();
        let m_value = get_vector_val::<f64>(vectors, "N+1").ok();
        let coefficient_value = get_vector_val::<f64>(vectors, "N+2").ok();
        let sigma_error = get_vector_val::<f64>(vectors, "N+3").ok();
        let model_magnetic_latitude = get_vector_val::<f32>(vectors, "model.mlat").ok();
        let model_magnetic_longitude = get_vector_val::<f32>(vectors, "model.mlon").ok();
        let model_magnetic_azimuth = get_vector_val::<f32>(vectors, "model.kvect").ok();
        let model_velocity_median = get_vector_val::<f32>(vectors, "model.vel.median").ok();
        let boundary_magnetic_latitude = get_vector_val::<f32>(vectors, "boundary.mlat").ok();
        let boundary_magnetic_longitude = get_vector_val::<f32>(vectors, "boundary.mlon").ok();

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
        data_bytes.extend(self.imf_delay.to_bytes("IMF.delay"));
        data_bytes.extend(self.imf_bx.to_bytes("IMF.Bx"));
        data_bytes.extend(self.imf_by.to_bytes("IMF.By"));
        data_bytes.extend(self.imf_bz.to_bytes("IMF.Bz"));
        data_bytes.extend(self.imf_vx.to_bytes("IMF.Vx"));
        data_bytes.extend(self.imf_tilt.to_bytes("IMF.tilt"));
        data_bytes.extend(self.imf_kp.to_bytes("IMF.Kp"));
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
        data_bytes.extend(self.igrf_flag.to_bytes("noigrf"));
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
}
