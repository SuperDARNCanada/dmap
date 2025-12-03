use criterion::{criterion_group, criterion_main, Criterion};
use dmap::formats::dmap::DmapRecord;
use dmap::formats::fitacf::FitacfRecord;
use dmap::formats::grid::GridRecord;
use dmap::formats::iqdat::IqdatRecord;
use dmap::formats::map::MapRecord;
use dmap::formats::rawacf::RawacfRecord;
use dmap::formats::snd::SndRecord;
use dmap::record::Record;
use dmap::types::DmapField;
use indexmap::IndexMap;
use paste::paste;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Read IQDAT", |b| b.iter(|| read_iqdat()));
    c.bench_function("Read RAWACF", |b| b.iter(|| read_rawacf()));
    c.bench_function("Read FITACF", |b| b.iter(|| read_fitacf()));
    c.bench_function("Read GRID", |b| b.iter(|| read_grid()));
    c.bench_function("Read SND", |b| b.iter(|| read_snd()));
    c.bench_function("Read MAP", |b| b.iter(|| read_map()));
    c.bench_function("Read DMAP", |b| b.iter(|| read_dmap()));
    
    c.bench_function("Read bzipped IQDAT", |b| b.iter(|| read_iqdat_bz2()));
    c.bench_function("Read bzipped RAWACF", |b| b.iter(|| read_rawacf_bz2()));
    c.bench_function("Read bzipped FITACF", |b| b.iter(|| read_fitacf_bz2()));
    c.bench_function("Read bzipped GRID", |b| b.iter(|| read_grid_bz2()));
    c.bench_function("Read bzipped SND", |b| b.iter(|| read_snd_bz2()));
    c.bench_function("Read bzipped MAP", |b| b.iter(|| read_map_bz2()));
    c.bench_function("Read bzipped DMAP", |b| b.iter(|| read_dmap_bz2()));
    
    c.bench_function("Read IQDAT metadata", |b| b.iter(|| read_iqdat_metadata()));
    c.bench_function("Read RAWACF metadata", |b| b.iter(|| read_rawacf_metadata()));
    c.bench_function("Read FITACF metadata", |b| b.iter(|| read_fitacf_metadata()));
    c.bench_function("Read GRID metadata", |b| b.iter(|| read_grid_metadata()));
    c.bench_function("Read SND metadata", |b| b.iter(|| read_snd_metadata()));
    c.bench_function("Read MAP metadata", |b| b.iter(|| read_map_metadata()));
    c.bench_function("Read DMAP metadata", |b| b.iter(|| read_dmap_metadata()));
    
    // let records = read_iqdat();
    // c.bench_with_input(
    //     BenchmarkId::new("Write IQDAT", "IQDAT Records"),
    //     &records,
    //     |b, s| b.iter(|| write_iqdat(s)),
    // );
}

/// Generates benchmark functions for a given DMAP record type.
macro_rules! read_type {
    ($type:ident, $name:literal) => {
        paste! {
            fn [< read_ $type >]() -> Vec<[< $type:camel Record >]> {
                [< $type:camel Record >]::read_file(format!("tests/test_files/test.{}", $name)).unwrap()
            }
            
            fn [< read_ $type _bz2 >]() -> Vec<[< $type:camel Record >]> {
                [< $type:camel Record >]::read_file(format!("tests/test_files/test.{}.bz2", $name)).unwrap()
            }

            fn [< read_ $type _metadata >]() -> Vec<IndexMap<String, DmapField>> {
                [< $type:camel Record >]::read_file_metadata(format!("tests/test_files/test.{}", $name)).unwrap()
            }
        }
    }
}

read_type!(iqdat, "iqdat");
read_type!(rawacf, "rawacf");
read_type!(fitacf, "fitacf");
read_type!(grid, "grid");
read_type!(map, "map");
read_type!(snd, "snd");
read_type!(dmap, "rawacf");

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = criterion_benchmark
}
criterion_main!(benches);
