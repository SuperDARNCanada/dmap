# Dmap

This project exposes both Rust and Python APIs for handling DMAP I/O.
I/O can be conducted either directly to/from files or byte buffers.

The SuperDARN DMAP file formats are all supported (IQDAT, RAWACF, FITACF, GRID, MAP, and SND)
as well as a generic DMAP format that is unaware of any required fields or types (e.g. char, int32) for any fields.

## Installation

### Rust
1. Add the crate to your dependencies in your `Cargo.toml` file
2. Add `use dmap::*;` to your imports.

### Python
This package is registered on PyPI as `darn-dmap`, you can install the package with your package manager.

### From source
If you want to build from source, you first need to have Rust installed on your machine. Then:
1. Clone the repository: `git clone https://github.com/SuperDARNCanada/dmap`
2. Run `cargo build` in the repository directory
3. If wanting to install the Python API, create a virtual environment and source it, then install `maturin`
4. In the project directory, run `maturin develop` to build and install the Python bindings. This will make a wheel file based on your operating system and architecture that you can install directly on any compatible machine.
