use dmap::formats::dmap::Record;
use dmap::formats::fitacf::FitacfRecord;
use dmap::formats::grid::GridRecord;
use dmap::formats::iqdat::IqdatRecord;
use dmap::formats::map::MapRecord;
use dmap::formats::rawacf::RawacfRecord;
use dmap::formats::snd::SndRecord;
use itertools::izip;
use std::fs::remove_file;
use std::path::PathBuf;
use dmap::{write_iqdat, write_rawacf, write_fitacf, write_grid, write_map, write_snd};

#[test]
fn read_write_iqdat() {
    let path = PathBuf::from("tests/test_files/test.iqdat");
    let tempfile = PathBuf::from("tests/test_files/tmp.iqdat");
    let mut path_bz2: PathBuf = path.clone();
    path_bz2.set_file_name("test.iqdat.bz2");
    let mut temp_bz2: PathBuf = tempfile.clone();
    temp_bz2.set_file_name("tmp.iqdat.bz2");

    println!("{} {}", path_bz2.display(), temp_bz2.display());

    // Read in test files and verify they have the same contents (both regular and zipped versions)
    let data = IqdatRecord::read_dmap(&path).expect("Unable to read test.iqdat");
    let zipped_recs = IqdatRecord::read_dmap(&path_bz2).expect("Cannot read test.iqdat.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), zipped_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a regular file, and then read back in and compare contents
    _ = write_iqdat(data.clone(), &tempfile).expect("Unable to write tmp.iqdat");
    let new_recs = IqdatRecord::read_dmap(&tempfile).expect("Cannot read tmp.iqdat");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a zipped file, and then read back in and compare contents
    _ = write_iqdat(data.clone(), &temp_bz2).expect("Unable to write tmp.iqdat.bz2");
    let new_recs = IqdatRecord::read_dmap(&temp_bz2).expect("Cannot read tmp.iqdat.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Clean up the temp files
    remove_file(&tempfile).expect("Unable to delete tmp.iqdat");
    remove_file(&temp_bz2).expect("Unable to delete tmp.iqdat.bz2");
}

#[test]
fn read_write_rawacf() {
    let path = PathBuf::from("tests/test_files/test.rawacf");
    let tempfile = PathBuf::from("tests/test_files/tmp.rawacf");
    let mut path_bz2: PathBuf = path.clone();
    path_bz2.set_file_name("test.rawacf.bz2");
    let mut temp_bz2: PathBuf = tempfile.clone();
    temp_bz2.set_file_name("tmp.rawacf.bz2");

    println!("{} {}", path_bz2.display(), temp_bz2.display());

    // Read in test files and verify they have the same contents (both regular and zipped versions)
    let data = RawacfRecord::read_dmap(&path).expect("Unable to read test.rawacf");
    let zipped_recs = RawacfRecord::read_dmap(&path_bz2).expect("Cannot read test.rawacf.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), zipped_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a regular file, and then read back in and compare contents
    _ = write_rawacf(data.clone(), &tempfile).expect("Unable to write tmp.rawacf");
    let new_recs = RawacfRecord::read_dmap(&tempfile).expect("Cannot read tmp.rawacf");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a zipped file, and then read back in and compare contents
    _ = write_rawacf(data.clone(), &temp_bz2).expect("Unable to write tmp.rawacf.bz2");
    let new_recs = RawacfRecord::read_dmap(&temp_bz2).expect("Cannot read tmp.rawacf.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Clean up the temp files
    remove_file(&tempfile).expect("Unable to delete tmp.rawacf");
    remove_file(&temp_bz2).expect("Unable to delete tmp.rawacf.bz2");
}

#[test]
fn read_write_fitacf() {
    let path = PathBuf::from("tests/test_files/test.fitacf");
    let tempfile = PathBuf::from("tests/test_files/tmp.fitacf");
    let mut path_bz2: PathBuf = path.clone();
    path_bz2.set_file_name("test.fitacf.bz2");
    let mut temp_bz2: PathBuf = tempfile.clone();
    temp_bz2.set_file_name("tmp.fitacf.bz2");

    println!("{} {}", path_bz2.display(), temp_bz2.display());

    // Read in test files and verify they have the same contents (both regular and zipped versions)
    let data = FitacfRecord::read_dmap(&path).expect("Unable to read test.fitacf");
    let zipped_recs = FitacfRecord::read_dmap(&path_bz2).expect("Cannot read test.fitacf.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), zipped_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a regular file, and then read back in and compare contents
    _ = write_fitacf(data.clone(), &tempfile).expect("Unable to write tmp.fitacf");
    let new_recs = FitacfRecord::read_dmap(&tempfile).expect("Cannot read tmp.fitacf");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a zipped file, and then read back in and compare contents
    _ = write_fitacf(data.clone(), &temp_bz2).expect("Unable to write tmp.fitacf.bz2");
    let new_recs = FitacfRecord::read_dmap(&temp_bz2).expect("Cannot read tmp.fitacf.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Clean up the temp files
    remove_file(&tempfile).expect("Unable to delete tmp.fitacf");
    remove_file(&temp_bz2).expect("Unable to delete tmp.fitacf.bz2");
}

#[test]
fn read_write_grid() {
    let path = PathBuf::from("tests/test_files/test.grid");
    let tempfile = PathBuf::from("tests/test_files/tmp.grid");
    let mut path_bz2: PathBuf = path.clone();
    path_bz2.set_file_name("test.grid.bz2");
    let mut temp_bz2: PathBuf = tempfile.clone();
    temp_bz2.set_file_name("tmp.grid.bz2");

    println!("{} {}", path_bz2.display(), temp_bz2.display());

    // Read in test files and verify they have the same contents (both regular and zipped versions)
    let data = GridRecord::read_dmap(&path).expect("Unable to read test.grid");
    let zipped_recs = GridRecord::read_dmap(&path_bz2).expect("Cannot read test.grid.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), zipped_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a regular file, and then read back in and compare contents
    _ = write_grid(data.clone(), &tempfile).expect("Unable to write tmp.grid");
    let new_recs = GridRecord::read_dmap(&tempfile).expect("Cannot read tmp.grid");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a zipped file, and then read back in and compare contents
    _ = write_grid(data.clone(), &temp_bz2).expect("Unable to write tmp.grid.bz2");
    let new_recs = GridRecord::read_dmap(&temp_bz2).expect("Cannot read tmp.grid.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Clean up the temp files
    remove_file(&tempfile).expect("Unable to delete tmp.grid");
    remove_file(&temp_bz2).expect("Unable to delete tmp.grid.bz2");
}

#[test]
fn read_write_map() {
    let path = PathBuf::from("tests/test_files/test.map");
    let tempfile = PathBuf::from("tests/test_files/tmp.map");
    let mut path_bz2: PathBuf = path.clone();
    path_bz2.set_file_name("test.map.bz2");
    let mut temp_bz2: PathBuf = tempfile.clone();
    temp_bz2.set_file_name("tmp.map.bz2");

    println!("{} {}", path_bz2.display(), temp_bz2.display());

    // Read in test files and verify they have the same contents (both regular and zipped versions)
    let data = MapRecord::read_dmap(&path).expect("Unable to read test.map");
    let zipped_recs = MapRecord::read_dmap(&path_bz2).expect("Cannot read test.map.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), zipped_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a regular file, and then read back in and compare contents
    _ = write_map(data.clone(), &tempfile).expect("Unable to write tmp.map");
    let new_recs = MapRecord::read_dmap(&tempfile).expect("Cannot read tmp.map");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a zipped file, and then read back in and compare contents
    _ = write_map(data.clone(), &temp_bz2).expect("Unable to write tmp.map.bz2");
    let new_recs = MapRecord::read_dmap(&temp_bz2).expect("Cannot read tmp.map.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Clean up the temp files
    remove_file(&tempfile).expect("Unable to delete tmp.map");
    remove_file(&temp_bz2).expect("Unable to delete tmp.map.bz2");
}

#[test]
fn read_write_snd() {
    let path = PathBuf::from("tests/test_files/test.snd");
    let tempfile = PathBuf::from("tests/test_files/tmp.snd");
    let mut path_bz2: PathBuf = path.clone();
    path_bz2.set_file_name("test.snd.bz2");
    let mut temp_bz2: PathBuf = tempfile.clone();
    temp_bz2.set_file_name("tmp.snd.bz2");

    println!("{} {}", path_bz2.display(), temp_bz2.display());

    // Read in test files and verify they have the same contents (both regular and zipped versions)
    let data = SndRecord::read_dmap(&path).expect("Unable to read test.snd");
    let zipped_recs = SndRecord::read_dmap(&path_bz2).expect("Cannot read test.snd.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), zipped_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a regular file, and then read back in and compare contents
    _ = write_snd(data.clone(), &tempfile).expect("Unable to write tmp.snd");
    let new_recs = SndRecord::read_dmap(&tempfile).expect("Cannot read tmp.snd");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Write to a zipped file, and then read back in and compare contents
    _ = write_snd(data.clone(), &temp_bz2).expect("Unable to write tmp.snd.bz2");
    let new_recs = SndRecord::read_dmap(&temp_bz2).expect("Cannot read tmp.snd.bz2");
    for (ref read_rec, ref written_rec) in izip!(data.iter(), new_recs.iter()) {
        assert_eq!(read_rec, written_rec)
    }

    // Clean up the temp files
    remove_file(&tempfile).expect("Unable to delete tmp.snd");
    remove_file(&temp_bz2).expect("Unable to delete tmp.snd.bz2");
}
