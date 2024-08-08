use criterion::{criterion_group, criterion_main, Criterion};
use dmap::formats::dmap::Record;
use dmap::formats::fitacf::FitacfRecord;
use dmap::formats::grid::GridRecord;
use dmap::formats::iqdat::IqdatRecord;
use dmap::formats::map::MapRecord;
use dmap::formats::rawacf::RawacfRecord;
use dmap::formats::snd::SndRecord;
use std::fs::File;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Read IQDAT", |b| b.iter(|| read_iqdat()));
    c.bench_function("Read RAWACF", |b| b.iter(|| read_rawacf()));
    c.bench_function("Read FITACF", |b| b.iter(|| read_fitacf()));
    c.bench_function("Read GRID", |b| b.iter(|| read_grid()));
    c.bench_function("Read SND", |b| b.iter(|| read_snd()));
    c.bench_function("Read MAP", |b| b.iter(|| read_map()));
    // c.bench_function("Read Full-size RAWACF", |b| {
    //     b.iter(|| read_fullsize_rawacf())
    // });
    // c.bench_function("Read Full-size FITACF", |b| {
    //     b.iter(|| read_fullsize_fitacf())
    // });

    // let records = read_iqdat();
    // c.bench_with_input(
    //     BenchmarkId::new("Write IQDAT", "IQDAT Records"),
    //     &records,
    //     |b, s| b.iter(|| write_iqdat(s)),
    // );
}

fn read_fitacf() -> Vec<FitacfRecord> {
    let file = File::open("tests/test_files/test.fitacf").expect("Test file not found");
    FitacfRecord::read_records(file).unwrap()
}

fn read_rawacf() -> Vec<RawacfRecord> {
    let file = File::open("tests/test_files/test.rawacf").expect("Test file not found");
    RawacfRecord::read_records(file).unwrap()
}

fn read_fullsize_rawacf() -> Vec<RawacfRecord> {
    let file = File::open("tests/test_files/20210607.1801.00.cly.a.rawacf.mean")
        .expect("Test file not found");
    RawacfRecord::read_records(file).unwrap()
}

fn read_fullsize_fitacf() -> Vec<FitacfRecord> {
    let file =
        File::open("tests/test_files/20210607.1801.00.cly.a.fitacf").expect("Test file not found");
    FitacfRecord::read_records(file).unwrap()
}

fn read_iqdat() -> Vec<IqdatRecord> {
    let file = File::open("tests/test_files/test.iqdat").expect("Test file not found");
    IqdatRecord::read_records(file).unwrap()
}

// fn write_iqdat(records: &Vec<RawDmapRecord>) {
//     let file = File::open("tests/test_files/test.iqdat").expect("Test file not found");
//     dmap::read_records(file).unwrap();
//     dmap::to_file("tests/test_files/temp.iqdat", records).unwrap();
// }

fn read_grid() -> Vec<GridRecord> {
    let file = File::open("tests/test_files/test.grid").expect("Test file not found");
    GridRecord::read_records(file).unwrap()
}

fn read_map() -> Vec<MapRecord> {
    let file = File::open("tests/test_files/test.map").expect("Test file not found");
    MapRecord::read_records(file).unwrap()
}

fn read_snd() -> Vec<SndRecord> {
    let file = File::open("tests/test_files/test.snd").expect("Test file not found");
    SndRecord::read_records(file).unwrap()
}
criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
