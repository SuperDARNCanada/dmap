use dmap;
use itertools::izip;
use std::fs::{File, remove_file};
use std::path::Path;

#[test]
fn test_read_write_iqdat() {
    let file = File::open(Path::new("tests/test_files/test.iqdat"))
        .expect("test file not found");
    let contents = dmap::read_records(file).expect("unable to read test file contents");

    dmap::to_file("tests/test_files/temp.iqdat", &contents).expect("unable to write to file");
    let test_file = File::open("tests/test_files/temp.iqdat").expect("test file unwritten");
    let test_contents = dmap::read_records(test_file).expect("unable to read temp file contents");
    for (read_rec, written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file("tests/test_files/temp.iqdat").expect("Unable to delete file");
}

#[test]
fn test_read_write_rawacf() {
    let file = File::open(Path::new("tests/test_files/test.rawacf"))
        .expect("test file not found");
    let contents = dmap::read_records(file).expect("unable to read test file contents");

    dmap::to_file("tests/test_files/temp.rawacf", &contents).expect("unable to write to file");
    let test_file = File::open("tests/test_files/temp.rawacf").expect("test file unwritten");
    let test_contents = dmap::read_records(test_file).expect("unable to read temp file contents");
    for (read_rec, written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file("tests/test_files/temp.rawacf").expect("Unable to delete file");
}

#[test]
fn test_read_write_fitacf() {
    let file = File::open(Path::new("tests/test_files/test.fitacf"))
        .expect("test file not found");
    let contents = dmap::read_records(file).expect("unable to read test file contents");

    dmap::to_file("tests/test_files/temp.fitacf", &contents).expect("unable to write to file");
    let test_file = File::open("tests/test_files/temp.fitacf").expect("test file unwritten");
    let test_contents = dmap::read_records(test_file).expect("unable to read temp file contents");
    for (read_rec, written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file("tests/test_files/temp.fitacf").expect("Unable to delete file");
}

#[test]
fn test_read_write_map() {
    let file = File::open(Path::new("tests/test_files/test.map"))
        .expect("test file not found");
    let contents = dmap::read_records(file).expect("unable to read test file contents");

    dmap::to_file("tests/test_files/temp.map", &contents).expect("unable to write to file");
    let test_file = File::open("tests/test_files/temp.map").expect("test file unwritten");
    let test_contents = dmap::read_records(test_file).expect("unable to read temp file contents");
    for (read_rec, written_rec) in izip!(contents.iter(), test_contents.iter()) {
        assert_eq!(read_rec, written_rec)
    }
    remove_file("tests/test_files/temp.map").expect("Unable to delete file");
}

#[test]
fn identical_file_comparison() {
    let file1 = File::open(Path::new("tests/test_files/test.map"))
        .expect("test file not found");
    let file2 = File::open(Path::new("tests/test_files/test.map"))
        .expect("test file not found");
    let contents1 = dmap::read_records(file1).expect("unable to read test file contents");
    let contents2 = dmap::read_records(file2).expect("unable to read test file contents");
    for (rec1, rec2) in izip!(contents1, contents2) {
        let differences = rec1.find_differences(&rec2);
        assert!(differences.is_empty())
    }
}

#[test]
fn different_file_comparison() {
    let file1 = File::open(Path::new("tests/test_files/test.rawacf"))
        .expect("test file not found");
    let file2 = File::open(Path::new("tests/test_files/test.map"))
        .expect("test file not found");
    let contents1 = dmap::read_records(file1).expect("unable to read test file contents");
    let contents2 = dmap::read_records(file2).expect("unable to read test file contents");
    for (rec1, rec2) in izip!(contents1, contents2) {
        let differences = rec1.find_differences(&rec2);
        println!("{:?}", differences);
        assert_eq!(true, differences.is_empty())
    }
}