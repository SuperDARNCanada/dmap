use dmap::*;
use itertools::izip;
use paste::paste;
use std::fs::{remove_file, File};
use std::io::Write;
use std::path::PathBuf;

/// Create tests for I/O on a given DMAP type.
macro_rules! make_test {
    ($record_type:ident) => {
        paste! {
            #[test]
            fn [< test_ $record_type _io >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let mut tempfile: PathBuf = filename.clone();
                tempfile.set_file_name(format!("tmp.{}", stringify!($record_type)));

                let data = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");

                _ = [< $record_type:camel Record >]::write_to_file(&data, &tempfile, false).expect("Unable to write to file");
                let new_recs = [< $record_type:camel Record >]::read_file(&tempfile).expect("Cannot read tempfile");
                for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
                    assert_eq!(read_rec, written_rec)
                }

                // Clean up tempfile
                remove_file(&tempfile).expect("Unable to delete tempfile");
            }

            #[test]
            fn [< test_ $record_type _bz2_io >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}.bz2", stringify!($record_type)));
                let mut tempfile: PathBuf = filename.clone();
                tempfile.set_file_name(format!("tmp.{}.bz2", stringify!($record_type)));

                let data = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");

                _ = [< $record_type:camel Record >]::write_to_file(&data, &tempfile, false).expect("Unable to write to file");
                let new_recs = [< $record_type:camel Record >]::read_file(&tempfile).expect("Cannot read tempfile");
                for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
                    assert_eq!(read_rec, written_rec)
                }

                // Clean up tempfile
                remove_file(&tempfile).expect("Unable to delete tempfile");
            }


            #[test]
            fn [< test_ $record_type _lax_io >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let mut tempfile: PathBuf = filename.clone();
                tempfile.set_file_name(format!("tmp.{}.corrupt", stringify!($record_type)));

                let _ = std::fs::copy(filename.clone(), tempfile.clone()).expect("Could not copy to tempfile");
                let mut file = File::options().append(true).open(tempfile.clone()).unwrap();
                writeln!(&mut file, "not a valid record").expect("Could not write to tempfile");

                let data = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");
                let (lax_data, bad_byte) = [< $record_type:camel Record >]::read_file_lax(&tempfile).expect("Unable to read tempfile");
                assert!(bad_byte.is_some());
                assert_eq!(bad_byte.unwrap(), (file.metadata().expect("Couldn't read tempfile metadata").len() as usize - 19));
                for (ref read_rec, ref lax_rec) in izip!(data.iter(), lax_data.iter()) {
                    assert_eq!(read_rec, lax_rec)
                }

                // Clean up tempfile
                remove_file(&tempfile).expect("Unable to delete tempfile");
            }

            #[test]
            fn [< test_ $record_type _generic_io >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let mut tempfile: PathBuf = filename.clone();
                tempfile.set_file_name(format!("tmp.{}.generic", stringify!($record_type)));

                let gen_data = DmapRecord::read_file(&filename).expect("Unable to read file");
                _ = DmapRecord::write_to_file(&gen_data, &tempfile, false).expect("Unable to write to file");
                let new_recs = DmapRecord::read_file(&tempfile).expect("Cannot read tempfile");
                for (new_rec, ref_rec) in izip!(new_recs.iter(), gen_data.iter()) {
                    assert_eq!(new_rec, ref_rec)
                }

                // Clean up tempfile
                remove_file(&tempfile).expect("Unable to delete tempfile");
            }

            #[test]
            fn [< test_ $record_type _sniff >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let data = [< $record_type:camel Record >]::read_file_by_indices(&filename, &[0]).expect("Unable to sniff file");
                let all_recs = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");
                assert_eq!(data[0], all_recs[0]);
            }

            #[test]
            fn [< test_ $record_type _sniff_last >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let data = [< $record_type:camel Record >]::read_file_by_indices(&filename, &[-1]).expect("Unable to sniff file");
                let all_recs = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");
                assert_eq!(data[0], all_recs[all_recs.len()-1]);
            }

            #[test]
            fn [< test_ $record_type _sniff_oob >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let all_recs = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");
                let num_recs = all_recs.len();
                assert!([< $record_type:camel Record >]::read_file_by_indices(&filename, &[num_recs as i32]).is_err());
                assert!([< $record_type:camel Record >]::read_file_by_indices(&filename, &[-(num_recs as i32)]).is_err());
                assert!([< $record_type:camel Record >]::read_file_by_indices(&filename, &[-(num_recs as i32 + 1_i32)]).is_err());
                let data = [< $record_type:camel Record >]::read_file_by_indices(&filename, &[-1, num_recs as i32 - 1_i32]).expect("Can't sniff last rec");
                assert_eq!(data[0], data[1]);
                assert_eq!(data[0], all_recs[all_recs.len()-1]);
            }

            #[test]
            fn [< test_ $record_type _sniff_first_and_last >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let data = [< $record_type:camel Record >]::read_file_by_indices(&filename, &[0, -1]).expect("Unable to sniff file");
                let all_recs = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");
                assert_eq!(data[0], all_recs[0]);
                assert_eq!(data[1], all_recs[all_recs.len()-1]);
            }

            #[test]
            fn [< test_ $record_type _metadata >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let data = [< $record_type:camel Record >]::read_file_metadata(&filename).expect("Unable to read file metadata");
                let all_recs = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");
                assert_eq!(data.len(), all_recs.len());
                for (mdata_rec, ref_rec) in izip!(data.iter(), all_recs.iter()) {
                    assert!(mdata_rec.keys().len() < ref_rec.keys().len())
                }
            }

            #[test]
            fn [< test_ $record_type _read_api >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let via_record = [< $record_type:camel Record >]::read_file(&filename).expect("Unable to read file");
                let via_api = [< read_ $record_type >](&filename).expect("Unable to read file through Rust API");
                assert_eq!(via_api, via_record);
            }

            #[test]
            fn [< test_ $record_type _read_bytes_api >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let bytes = std::fs::read(&filename).expect("Unable to read file as bytes");
                let via_record = [< $record_type:camel Record >]::read_records(bytes.as_slice())
                    .expect("Unable to read records from bytes");
                let via_api = [< read_ $record_type _bytes >](bytes.as_slice())
                    .expect("Unable to read bytes through Rust API");
                assert_eq!(via_api, via_record);
            }

            #[test]
            fn [< test_ $record_type _read_by_indices_api >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let via_record = [< $record_type:camel Record >]::read_file_by_indices(&filename, &[0]).expect("Unable to sniff file");
                let via_api = [< read_ $record_type _by_indices >](&filename, &[0]).expect("Unable to sniff file through Rust API");
                assert_eq!(via_api, via_record);
            }

            #[test]
            fn [< test_ $record_type _read_by_indices_lax_api >]() {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let mut tempfile: PathBuf = filename.clone();
                tempfile.set_file_name(format!("tmp.{}.read_by_indices_lax_api.corrupt", stringify!($record_type)));
                let _ = std::fs::copy(filename.clone(), tempfile.clone()).expect("Could not copy to tempfile");
                let mut file = File::options().append(true).open(tempfile.clone()).unwrap();
                writeln!(&mut file, "not a valid record").expect("Could not write to tempfile");
                let via_record = [< $record_type:camel Record >]::read_file_by_indices_lax(&tempfile, &[0])
                    .expect("Unable to read indexed tempfile lax");
                let via_api = [< read_ $record_type _by_indices_lax >](&tempfile, &[0])
                    .expect("Unable to read indexed tempfile lax through Rust API");
                assert_eq!(via_api, via_record);
                remove_file(&tempfile).expect("Unable to delete tempfile");
            }

            #[test]
            fn [< test_ $record_type _read_bytes_by_indices_api >]() {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let bytes = std::fs::read(&filename).expect("Unable to read test file");
                let via_record = [< $record_type:camel Record >]::read_nth_records(bytes.as_slice(), &[0])
                    .expect("Unable to read indexed records from bytes");
                let via_api = [< read_ $record_type _bytes_by_indices >](bytes.as_slice(), &[0])
                    .expect("Unable to read indexed records from bytes through Rust API");
                assert_eq!(via_api, via_record);
            }

            #[test]
            fn [< test_ $record_type _read_bytes_by_indices_lax_api >]() {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let mut bytes = std::fs::read(&filename).expect("Unable to read test file");
                bytes.extend_from_slice(b"not a valid record\n");
                let via_record = [< $record_type:camel Record >]::read_nth_records_lax(bytes.as_slice(), &[0])
                    .expect("Unable to read indexed records from bytes lax");
                let via_api = [< read_ $record_type _bytes_by_indices_lax >](bytes.as_slice(), &[0])
                    .expect("Unable to read indexed records from bytes lax through Rust API");
                assert_eq!(via_api, via_record);
            }

            #[test]
            fn [< test_ $record_type _metadata_api >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let via_record = [< $record_type:camel Record >]::read_file_metadata(&filename)
                    .expect("Unable to read metadata");
                let via_api = [< read_ $record_type _metadata >](&filename).expect("Unable to read metadata through Rust API");
                assert_eq!(via_api, via_record);
            }

            #[test]
            fn [< test_ $record_type _metadata_by_indices_api >]() {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let via_record = [< $record_type:camel Record >]::read_file_metadata_by_indices(&filename, &[0])
                    .expect("Unable to read indexed metadata");
                let via_api = [< read_ $record_type _metadata_by_indices >](&filename, &[0])
                    .expect("Unable to read indexed metadata through Rust API");
                assert_eq!(via_api, via_record);
            }

            #[test]
            fn [< test_ $record_type _read_lax_api >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let mut tempfile: PathBuf = filename.clone();
                tempfile.set_file_name(format!("tmp.{}.read_api.corrupt", stringify!($record_type)));
                let _ = std::fs::copy(filename.clone(), tempfile.clone()).expect("Could not copy to tempfile");
                let mut file = File::options().append(true).open(tempfile.clone()).unwrap();
                writeln!(&mut file, "not a valid record").expect("Could not write to tempfile");
                let via_record = [< $record_type:camel Record >]::read_file_lax(&tempfile)
                    .expect("Unable to read tempfile lax");
                let via_api = [< read_ $record_type _lax >](&tempfile).expect("Unable to read tempfile lax through Rust API");
                assert_eq!(via_api, via_record);
                remove_file(&tempfile).expect("Unable to delete tempfile");
            }

            #[test]
            fn [< test_ $record_type _read_bytes_lax_api >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let mut bytes = std::fs::read(&filename).expect("Unable to read file as bytes");
                bytes.extend_from_slice(b"not a valid record\n");
                let via_record = [< $record_type:camel Record >]::read_records_lax(bytes.as_slice())
                        .expect("Unable to read bytes lax");
                let via_api = [< read_ $record_type _bytes_lax >](bytes.as_slice())
                        .expect("Unable to read bytes lax through Rust API");
                assert_eq!(via_api, via_record);
            }

            #[test]
            fn [< test_ $record_type _dmap_read_api >] () {
                let filename: PathBuf = PathBuf::from(format!("tests/test_files/test.{}", stringify!($record_type)));
                let via_record = DmapRecord::read_file(&filename).expect("Unable to read file");
                let via_api = read_dmap(&filename).expect("Unable to read through Rust API");
                assert_eq!(via_api, via_record);
            }
        }
    };
}

make_test!(iqdat);
make_test!(rawacf);
make_test!(fitacf);
make_test!(grid);
make_test!(map);
make_test!(snd);
