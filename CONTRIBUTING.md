# Developer Guidelines

Any contributions are welcome! Below is a brief description of the project structure.

## Structure

### `src/record.rs`
This file contains the `Record` trait, which defines a set of functions that specific DMAP formats must implement.
For example, `read_file(infile: &PathBuf) -> Result<Vec<Self>, DmapError>` is defined in the `Record` trait, and handles
reading in records from a file at the specified path. This function is generic, in that it doesn't know what type of records
(RAWACF, FITACF, etc.) are expected. Also, since it is a trait function, you can only use it through a struct which implements
the trait. For example, the `FitacfRecord` struct defined in `src/formats/fitacf.rs` implements the `Record` trait, and so
you can call `FitacfRecord::read_file(...)` to read a FITACF file, but you couldn't invoke `Record::read_file(...)`.

### `src/types.rs`
This file defines necessary structs and enums for encapsulating basic types (`i8`, `u32`, `String`, etc.) into
objects like `DmapField`, `DmapScalar`, `DmapVec`, etc. that abstract over the supported underlying types.
For instance, when reading scalar from a DMAP file, the underlying data type is inferred from the `type` field in the 
scalar's metadata, so it can't be known beforehand. This requires some encapsulating type, `DmapScalar` in this case,
which contains the metadata of the field and has a known size for the stack memory. 

This file defines the `Fields` struct, which is used to hold the names and types of the required and optional
scalar and vector fields for a type of DMAP record (RAWACF, FITACF, etc.).

This file defines the `DmapType` trait and implements it for supported data types that can be in DMAP records, namely
`u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64`, `f32`, `f64`, and `String`. The implementation of the trait for
these types only means that other types, e.g. `i128`, cannot be stored in DMAP records.

Lastly, functions for parsing scalars and vectors from a byte buffer are defined in this file.

### `src/formats`
This directory holds the files that define the DMAP record formats: IQDAT, RAWACF, FITACF, GRID, MAP, SND, and the generic DMAP.
If you are defining a new DMAP format, you will need to make a new file in this directory following the structure of the
existing files. Essentially, you define the scalar and vector fields, both required and optional, and the groups of vector
fields which must have identical dimensions, then call a macro to autogenerate the struct code for you. 

### `src/compression.rs`
This file contains the automatic bz2 detection function. If more compression types are to be supported, this is where you would
put them.

### `src/io.rs`
This file contains the function `bytes_to_file`, which handles writing a `Vec<u8>` to `AsRef<Path>`. If the path
ends in `.bz2`, the function will compress first with bz2.

### `tests`
In `tests.rs`, integration tests for reading and writing all file types are present. Small example files
are contained in `tests/test_files`.

### `benches/io_benchmarking.rs`
This file contains benchmarking functions for checking the performance of the basic read functions.