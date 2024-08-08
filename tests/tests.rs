use dmap::formats::dmap::Record;
use dmap::formats::fitacf::FitacfRecord;
use dmap::formats::grid::GridRecord;
use dmap::formats::iqdat::IqdatRecord;
use dmap::formats::map::MapRecord;
use dmap::formats::rawacf::RawacfRecord;
use dmap::formats::snd::SndRecord;
use itertools::izip;
use std::fs::{remove_file, File};
use std::io::Write;
use std::path::Path;

#[test]
fn read_write_iqdat() {
    let file = File::open(Path::new("tests/test_files/test.iqdat")).expect("test file not found");
    let contents = IqdatRecord::read_records(file).expect("unable to read test file contents");

    let outfile = "tests/test_files/temp.iqdat";
    let mut file = File::create(outfile).expect("Unable to create temp file");

    let bytes: Vec<u8> = contents
        .iter()
        .map(|x| x.to_bytes().expect("Unable to convert record to bytes"))
        .flatten()
        .collect();
    file.write_all(&bytes)
        .expect("Unable to write bytes to temp file");

    let test_file = File::open(outfile).expect("test file unwritten");
    let test_contents =
        IqdatRecord::read_records(test_file).expect("unable to read temp file contents");
    for (ref read_rec, ref written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file(outfile).expect("Unable to delete file");
}

#[test]
fn read_write_rawacf() {
    let file = File::open(Path::new("tests/test_files/test.rawacf")).expect("test file not found");
    let contents = RawacfRecord::read_records(file).expect("unable to read test file contents");

    let outfile = "tests/test_files/temp.rawacf";
    let mut file = File::create(outfile).expect("Unable to create temp file");

    let bytes: Vec<u8> = contents
        .iter()
        .map(|x| x.to_bytes().expect("Unable to convert record to bytes"))
        .flatten()
        .collect();
    file.write_all(&bytes)
        .expect("Unable to write bytes to temp file");

    let test_file = File::open(outfile).expect("test file unwritten");
    let test_contents =
        RawacfRecord::read_records(test_file).expect("unable to read temp file contents");
    for (ref read_rec, ref written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file(outfile).expect("Unable to delete file");
}

#[test]
fn read_write_fitacf() {
    let file = File::open(Path::new("tests/test_files/test.fitacf")).expect("test file not found");
    let contents = FitacfRecord::read_records(file).expect("unable to read test file contents");

    let outfile = "tests/test_files/temp.fitacf";
    let mut file = File::create(outfile).expect("Unable to create temp file");

    let bytes: Vec<u8> = contents
        .iter()
        .map(|x| x.to_bytes().expect("Unable to convert record to bytes"))
        .flatten()
        .collect();
    file.write_all(&bytes)
        .expect("Unable to write bytes to temp file");

    let test_file = File::open(outfile).expect("test file unwritten");
    let test_contents =
        FitacfRecord::read_records(test_file).expect("unable to read temp file contents");
    for (ref read_rec, ref written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file(outfile).expect("Unable to delete file");
}

#[test]
fn read_write_grid() {
    let file = File::open(Path::new("tests/test_files/test.grid")).expect("test file not found");
    let contents = GridRecord::read_records(file).expect("unable to read test file contents");

    let outfile = "tests/test_files/temp.grid";
    let mut file = File::create(outfile).expect("Unable to create temp file");

    let bytes: Vec<u8> = contents[..2]
        .iter()
        .map(|x| x.to_bytes().expect("Unable to convert record to bytes"))
        .flatten()
        .collect();
    file.write_all(&bytes)
        .expect("Unable to write bytes to temp file");

    let test_file = File::open(outfile).expect("test file unwritten");
    let test_contents =
        GridRecord::read_records(test_file).expect("unable to read temp file contents");
    for (ref read_rec, ref written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file(outfile).expect("Unable to delete file");
}

#[test]
fn read_write_map() {
    let file = File::open(Path::new("tests/test_files/test.map")).expect("test file not found");
    let contents = MapRecord::read_records(file).expect("unable to read test file contents");

    let outfile = "tests/test_files/temp.map";
    let mut file = File::create(outfile).expect("Unable to create temp file");

    let bytes: Vec<u8> = contents
        .iter()
        .map(|x| x.to_bytes().expect("Unable to convert record to bytes"))
        .flatten()
        .collect();
    file.write_all(&bytes)
        .expect("Unable to write bytes to temp file");

    let test_file = File::open(outfile).expect("test file unwritten");
    let test_contents =
        MapRecord::read_records(test_file).expect("unable to read temp file contents");
    for (ref read_rec, ref written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file(outfile).expect("Unable to delete file");
}

#[test]
fn read_write_snd() {
    let file = File::open(Path::new("tests/test_files/test.snd")).expect("test file not found");
    let contents = SndRecord::read_records(file).expect("unable to read test file contents");

    let outfile = "tests/test_files/temp.snd";
    let mut file = File::create(outfile).expect("Unable to create temp file");

    let bytes: Vec<u8> = contents
        .iter()
        .map(|x| x.to_bytes().expect("Unable to convert record to bytes"))
        .flatten()
        .collect();
    file.write_all(&bytes)
        .expect("Unable to write bytes to temp file");

    let test_file = File::open(outfile).expect("test file unwritten");
    let test_contents =
        SndRecord::read_records(test_file).expect("unable to read temp file contents");
    for (ref read_rec, ref written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file(outfile).expect("Unable to delete file");
}
