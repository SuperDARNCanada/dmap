use criterion::{criterion_group, criterion_main, Criterion};
use dmap::formats::fitacf::FitacfRecord;
use dmap::formats::grid::GridRecord;
use dmap::formats::iqdat::IqdatRecord;
use dmap::formats::map::MapRecord;
use dmap::formats::rawacf::RawacfRecord;
use dmap::formats::snd::SndRecord;
use dmap::record::Record;
use paste::paste;
use std::fs::File;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Read IQDAT", |b| b.iter(|| read_iqdat()));
    c.bench_function("Read RAWACF", |b| b.iter(|| read_rawacf()));
    c.bench_function("Read FITACF", |b| b.iter(|| read_fitacf()));
    c.bench_function("Read GRID", |b| b.iter(|| read_grid()));
    c.bench_function("Read SND", |b| b.iter(|| read_snd()));
    c.bench_function("Read MAP", |b| b.iter(|| read_map()));

    // let records = read_iqdat();
    // c.bench_with_input(
    //     BenchmarkId::new("Write IQDAT", "IQDAT Records"),
    //     &records,
    //     |b, s| b.iter(|| write_iqdat(s)),
    // );
}

/// Generates benchmark functions for a given DMAP record type.
macro_rules! read_type {
    ($type:ident) => {
        paste! {
            fn [< read_ $type >]() -> Vec<[< $type:camel Record >]> {
                let file = File::open(format!("tests/test_files/test.{}", stringify!($type))).expect("Test file not found");
                [< $type:camel Record >]::read_records(file).unwrap()
            }
        }
    }
}

read_type!(iqdat);
read_type!(rawacf);
read_type!(fitacf);
read_type!(grid);
read_type!(map);
read_type!(snd);

// fn write_iqdat(records: &Vec<RawDmapRecord>) {
//     let file = File::open("tests/test_files/test.iqdat").expect("Test file not found");
//     dmap::read_records(file).unwrap();
//     dmap::to_file("tests/test_files/temp.iqdat", records).unwrap();
// }

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(benches);
