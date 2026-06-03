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
        }
    };
}

make_test!(iqdat);
make_test!(rawacf);
make_test!(fitacf);
make_test!(grid);
make_test!(map);
make_test!(snd);
