A library for SuperDARN DMAP file I/O
=====================================

[<img alt="github" src="https://img.shields.io/badge/github-SuperDARNCanada/dmap-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/SuperDARNCanada/dmap)


The SuperDARN DMAP file formats are all supported (IQDAT, RAWACF, FITACF, GRID, MAP, and SND)
as well as a generic DMAP format that is unaware of any required fields or types (e.g. char, int32) for any fields.
For more information on DMAP please see [RST Documentation](https://radar-software-toolkit-rst.readthedocs.io/en/latest/).

## Installation

### Package manager
This package is registered on PyPI as `darn-dmap`, you can install the package with your package manager, e.g. `pip install darn-dmap`.

### From source
If you want to build from source, you first need to have Rust installed on your machine. Then:
1. Clone the repository: `git clone https://github.com/SuperDARNCanada/dmap`
2. Create a virtual environment and source it, then install `maturin`
3. In the project directory, run `maturin develop` to build and install the Python bindings. This will make a wheel file based on your operating system and architecture that you can install directly on any compatible machine.

## Usage

### The basics

The basic code to read and write a DMAP file is:
```python
import dmap

file = "path/to/rawacf_file"
data, _ = dmap.read_rawacf(file)  # returns `tuple[list[dict], Optional[int]]`
outfile = "path/to/outfile.rawacf"
dmap.write_rawacf(data, outfile)  # writes binary data to `outfile`
raw_bytes = dmap.write_rawacf(data)  # returns a `bytes` object
```
`dmap.read_rawacf(...)` reads the file into a list of dictionaries, returning the list as well as the byte where any corrupted records start.

The supported reading functions are:

- `read_iqdat`, 
- `read_rawacf`, 
- `read_fitacf`, 
- `read_grid`, 
- `read_map`,  
- `read_snd`, and
- `read_dmap`.

The supported writing functions are:

- `write_iqdat`, 
- `write_rawacf`,
- `write_fitacf`, 
- `write_grid`, 
- `write_map`, 
- `write_snd`, and
- `write_dmap`.

### Accessing data fields
To see the names of the variables you've loaded in and now have access to, try using the `keys()` method:
```python
print(data[0].keys())
```
which will tell you all the variables in the first (zeroth) record.

Let's say you loaded in a MAP file, and wanted to grab the cross polar-cap potentials for each record:
```python
import dmap
file = "20150302.n.map"
map_data, _ = dmap.read_map(file)

cpcps=[rec['pot.drop'] for rec in map_data]
```

### I/O on a bz2 compressed file

dmap will handle compressing and decompressing `.bz2` files seamlessly, detecting the compression automatically. E.g.
```python
import dmap
fitacf_file = "path/to/file.bz2"
data, _ = dmap.read_fitacf(fitacf_file)
dmap.write_fitacf(data, "temp.fitacf.bz2")
```
will read in the compressed file, then also write out a new compressed file. Note that compression on the writing side
will only be done when writing to file, as the detection is done based on the file extension of the output file.

### Generic I/O
dmap supports generic DMAP I/O, without verifying the field names and types. The file must still
be properly formatted as a DMAP file, but otherwise no checks are conducted.

**NOTE:** When using the generic writing function `write_dmap`, scalar fields will possibly be resized; e.g., the `stid`
field may be stored as an 8-bit integer, as opposed to a 16-bit integer as usual. As such, reading with a specific method
(e.g. `read_fitacf`) on a file written using `write_dmap` will likely not pass the FITACF format checks.

```python
import dmap
generic_file = "path/to/file"  # can be iqdat, rawacf, fitacf, grid, map, snd, and optionally .bz2 compressed
data, _ = dmap.read_dmap(generic_file)
dmap.write_dmap(data, "temp.generic.fitacf")  # fitacf as an example
data2, bad_byte = dmap.read_rawacf("temp.generic.fitacf")  # This will fail due to different types for scalar fields
assert bad_byte == 0  # The first record should be corrupted, i.e. not be a valid FITACF record
assert len(data2) == 0  # No valid records encountered
```

### Handling corrupted data files
The self-describing data format of DMAP files makes it susceptible to corruption. The metadata fields which describe
how to interpret the following bytes are very important, and so any corruption will lead to the remainder of the file being
effectively useless. dmap is able to handle corruption in two ways. The keyword argument `mode` of the `read_rawacf`, etc.
functions allows you to choose how to handle corrupt records. 

In `"lax"` mode (the default), no error is raised if a corrupt file is read, and the byte where the corrupted records start is 
returned along with the non-corrupted records. 
In `"strict"` mode, the I/O functions will raise an error if a corrupted record is encountered. 

```python
import dmap

corrupted_file = "path/to/file"
data, bad_byte = dmap.read_dmap(corrupted_file, mode="lax")
assert bad_byte > 0

good_file = "path/to/file"
data, bad_byte = dmap.read_dmap(good_file, mode="lax")
assert bad_byte is None
```
In both uses of the above example, `data` will be a list of all records extracted from the file, but may be
considerably smaller than the file.

```python
import dmap

corrupted_file = "path/to/file"
try:
    data = dmap.read_dmap(corrupted_file, mode="strict")
    had_error = False
except:
    had_error = True
assert had_error

good_file = "path/to/file"
try:
    data = dmap.read_dmap(good_file, mode="strict")
    had_error = False
except:
    had_error = True
assert had_error is False
```

### Stream I/O
`dmap` also can conduct read/write operations from/to Python `bytes` objects directly. These bytes must be formatted in 
accordance with the DMAP format. Simply pass in a `bytes` object to any of the `read_[type]` functions instead of a path
and the input will be parsed.

While not the recommended way to read data from a DMAP file, the following example shows the use of these byte I/O functions:
```python
import dmap
file = "path/to/file.fitacf"
with open(file, 'rb') as f:  # 'rb' specifies to open the binary (b) file as read-only (r)
    raw_bytes = f.read()  # reads the file in its entirety
data, _ = dmap.read_dmap(raw_bytes)
binary_data = dmap.write_fitacf(data)
assert binary_data == raw_bytes
```
As a note, this binary data can be compressed ~2x typically using zlib, or with another compression utility. This is quite 
useful if sending data over a network where speed and bandwidth must be considered. Note that the binary writing functions
don't compress automatically, an external package like `zlib` or `bzip2` must be used.

### File "sniffing"
If you only want to inspect a file, without actually needing access to all of the data, you can use the `read_[type]`
functions in `"sniff"` mode. This will only read in the first record from a file, and works on both compressed and 
non-compressed files. Note that this mode does not work with bytes objects directly.

```python
import dmap
path = "path/to/file"
first_rec = dmap.read_dmap(path, mode="sniff")
```
