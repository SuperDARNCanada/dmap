"""
Integration tests for the Python API of darn-dmap.
"""

import bz2
import dmap
import numpy as np
import pytest
import os


# Path to this file
HERE = os.path.dirname(__file__)
FORMATS = ("iqdat", "rawacf", "fitacf", "grid", "map", "snd")
FILE_LENGTHS = (247688, 73528, 10780, 4612, 32668, 1659)
DATA_FIELDS = (
    ("data"), 
    ("pwr0", "slist", "acfd", "xcfd"),
    (
        "slist",
        "nlag",
        "qflg",
        "gflg",
        "p_l",
        "p_l_e",
        "p_s",
        "p_s_e",
        "v",
        "v_e",
        "w_l",
        "w_l_e",
        "w_s",
        "w_s_e",
        "sd_l",
        "sd_s",
        "sd_phi",
        "x_qflg",
        "x_gflg",
        "x_p_l",
        "x_p_l_e",
        "x_p_s",
        "x_p_s_e",
        "x_v",
        "x_v_e",
        "x_w_l",
        "x_w_l_e",
        "x_w_s",
        "x_w_s_e",
        "phi0",
        "phi0_e",
        "elv",
        "elv_fitted",
        "elv_error",
        "elv_low",
        "elv_high",
        "x_sd_l",
        "x_sd_s",
        "x_sd_phi"
    ),
    (
        "vector.mlat",
        "vector.mlon",
        "vector.kvect",
        "vector.stid",
        "vector.channel",
        "vector.index",
        "vector.vel.median",
        "vector.vel.sd",
        "vector.pwr.median",
        "vector.pwr.sd",
        "vector.wdt.median",
        "vector.wdt.sd",
        "vector.srng"
    ),
    (
        "vector.mlat", 
        "vector.mlon", 
        "vector.kvect", 
        "vector.stid", 
        "vector.channel", 
        "vector.index", 
        "vector.srng", 
        "vector.vel.median", 
        "vector.vel.sd", 
        "vector.pwr.median", 
        "vector.pwr.sd", 
        "vector.wdt.median", 
        "vector.wdt.sd"
    ),
    ("slist", "qflg", "gflg", "v", "v_e", "p_l", "w_l", "x_qflg", "phi0", "phi0_e")
)

def compare_recs(data1, data2):
    """Compare two `list[dict]`s, checking they are identical."""
    assert len(data1) == len(data2)
    for rec1, rec2 in zip(data1, data2):
        assert rec1.keys() == rec2.keys()
        for k in rec1.keys():
            val1 = rec1[k]
            val2 = rec2[k]
            assert type(val1) is type(val2), k
            if isinstance(val1, np.ndarray):
                assert np.allclose(val1, val2)
            elif isinstance(val1, float):
                assert np.isclose(val1, val2)
            else:
                assert val1 == val2, k
    return True


@pytest.mark.parametrize("fmt", FORMATS)
def test_dmap(fmt):
    data = dmap.read_dmap(f"{HERE}/test_files/test.{fmt}", mode="strict")
    assert len(data) == 2


@pytest.mark.parametrize("fmt", FORMATS)
def test_dmap_lax(fmt):
    data, bad_byte = dmap.read_dmap(f"{HERE}/test_files/test.{fmt}", mode="lax")
    assert len(data) == 2
    assert bad_byte is None


@pytest.mark.parametrize("fmt", FORMATS)
def test_dmap_bz2(fmt):
    data = dmap.read_dmap(f"{HERE}/test_files/test.{fmt}.bz2", mode="strict")
    assert len(data) == 2


@pytest.mark.parametrize("fmt", FORMATS)
def test_dmap_bz2_lax(fmt):
    data, bad_byte = dmap.read_dmap(f"{HERE}/test_files/test.{fmt}.bz2", mode="lax")
    assert len(data) == 2
    assert bad_byte is None


@pytest.mark.parametrize("fmt", FORMATS)
def test_dmap_sniff(fmt):
    data = dmap.read_dmap(f"{HERE}/test_files/test.{fmt}", mode="strict", indices=(0,))
    assert isinstance(data, list)
    assert len(data) == 1
    assert isinstance(data[0], dict)


@pytest.mark.parametrize("fmt", FORMATS)
def test_sniff_against_specific(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    data = dmap.read_dmap(infile, mode="strict", indices=(0,))
    data2 = getattr(dmap, f"read_{fmt}")(infile, mode="strict", indices=(0,))
    assert compare_recs(data, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_sniff_against_specific_strict(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    data2 = getattr(dmap, f"read_{fmt}")(infile, mode="strict", indices=[0])
    assert compare_recs(data1[:1], data2)
    data2 = getattr(dmap, f"read_{fmt}")(infile, mode="strict", indices=(-1,))
    assert compare_recs(data1[-1:], data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_dmap_sniff_last(fmt):
    data = dmap.read_dmap(f"{HERE}/test_files/test.{fmt}", mode="strict", indices=(-1,))
    assert isinstance(data, list)
    assert len(data) == 1
    assert isinstance(data[0], dict)


@pytest.mark.parametrize("fmt", FORMATS)
def test_sniff_against_specific(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    data = dmap.read_dmap(infile, mode="strict", indices=(-1,))
    data2 = getattr(dmap, f"read_{fmt}")(infile, mode="strict", indices=(-1,))
    assert compare_recs(data, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_sniff_outofbounds(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    data = dmap.read_dmap(infile, mode="strict")
    with pytest.raises(ValueError):
        _ = getattr(dmap, f"read_{fmt}")(infile, mode="strict", indices=(len(data),))
    with pytest.raises(ValueError):
        _ = getattr(dmap, f"read_{fmt}")(infile, mode="strict", indices=(-len(data)-1,))

@pytest.mark.parametrize("fmt", FORMATS)
def test_sniff_dmap_metadata(fmt):
    data = dmap.read_dmap(f"{HERE}/test_files/test.{fmt}", mode="metadata", indices=(0,))
    assert isinstance(data, list)
    assert len(data) == 1
    assert isinstance(data[0], dict)

@pytest.mark.parametrize("fmt", FORMATS)
def test_read_indices_lax(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="lax")
    data2 = getattr(dmap, f"read_{fmt}")(infile, mode="lax", indices=(0,))
    assert data2[1] is None
    compare_recs(data1[0][:1], data2[0])

@pytest.mark.parametrize("fmt", FORMATS)
def test_read_metadata_indices(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="metadata")[:1]
    data2 = getattr(dmap, f"read_{fmt}")(infile, mode="metadata", indices=(0,))
    compare_recs(data1, data2)

@pytest.mark.parametrize("fmt", FORMATS)
def test_file_vs_bytes_read(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    with open(infile, "rb") as f:
        raw_bytes = f.read()

    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    data2 = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")
    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_reading_compressed_vs_not(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    data2 = getattr(dmap, f"read_{fmt}")(infile + ".bz2", mode="strict")
    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_file_vs_bytes_read_bz2(fmt):
    infile = f"{HERE}/test_files/test.{fmt}.bz2"
    with open(infile, "rb") as f:
        raw_bytes = f.read()

    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    data2 = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")
    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt,bad_at", zip(FORMATS, FILE_LENGTHS))
def test_corrupted(fmt, bad_at):
    infile = f"{HERE}/test_files/test.{fmt}"
    with open(infile, "rb") as f:
        raw_bytes = f.read()
    data1 = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")

    corrupted_bytes = raw_bytes + b"this is not valid DMAP data"
    with pytest.raises(ValueError):
        _ = getattr(dmap, f"read_{fmt}")(corrupted_bytes, mode="strict")
    data2, bad_byte = getattr(dmap, f"read_{fmt}")(corrupted_bytes, mode="lax")
    assert bad_byte == bad_at

    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt,bad_at", zip(FORMATS, FILE_LENGTHS))
def test_corrupted_bz2(fmt, bad_at):
    infile = f"{HERE}/test_files/test.{fmt}"
    with open(infile, "rb") as f:
        raw_bytes = f.read()
    data1 = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")

    corrupted_bytes = bz2.compress(raw_bytes + b"this is not valid DMAP data")
    with pytest.raises(ValueError):
        _ = getattr(dmap, f"read_{fmt}")(corrupted_bytes, mode="strict")
    data2, bad_byte = getattr(dmap, f"read_{fmt}")(corrupted_bytes, mode="lax")
    assert bad_byte == bad_at

    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_roundtrip(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    raw_bytes = getattr(dmap, f"write_{fmt}")(data1)
    data2 = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")
    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_roundtrip_bz2(fmt):
    infile = f"{HERE}/test_files/test.{fmt}.bz2"

    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    raw_bytes = getattr(dmap, f"write_{fmt}")(data1)
    data2 = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")
    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_roundtrip_dmap(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data1 = dmap.read_dmap(infile, mode="strict")
    raw_bytes = dmap.write_dmap(data1)
    data2 = dmap.read_dmap(raw_bytes, mode="strict")
    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_roundtrip_dmap_bz2(fmt):
    infile = f"{HERE}/test_files/test.{fmt}.bz2"

    data1 = dmap.read_dmap(infile, mode="strict")
    raw_bytes = dmap.write_dmap(data1)
    data2 = dmap.read_dmap(raw_bytes, mode="strict")
    assert compare_recs(data1, data2)


@pytest.mark.parametrize("fmt", FORMATS)
def test_extra_key_write(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    data[0]["test"] = 1.0
    with pytest.raises(ValueError):
        _ = getattr(dmap, f"write_{fmt}")(data)


@pytest.mark.parametrize("fmt", FORMATS)
def test_missing_key_write(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    del data[0]["stid"]
    with pytest.raises(ValueError):
        _ = getattr(dmap, f"write_{fmt}")(data)


@pytest.mark.parametrize("fmt", FORMATS)
def test_key_wrong_type_write(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    if isinstance(data[0]["stid"], np.ndarray):
        data[0]["stid"] = np.array(data[0]["stid"], dtype=np.float64)
    else:
        data[0]["stid"] = float(data[0]["stid"])
    with pytest.raises(ValueError):
        _ = getattr(dmap, f"write_{fmt}")(data)


def test_extra_key_dmap():
    infile = f"{HERE}/test_files/test.rawacf"

    data = dmap.read_dmap(infile, mode="strict")
    data[0]["test"] = 1.0
    _ = dmap.write_dmap(data)


def test_missing_key_dmap():
    infile = f"{HERE}/test_files/test.rawacf"

    data = dmap.read_dmap(infile, mode="strict")
    del data[0]["stid"]
    _ = dmap.write_dmap(data)


def test_key_wrong_type_dmap():
    infile = f"{HERE}/test_files/test.rawacf"

    data = dmap.read_dmap(infile, mode="strict")
    if isinstance(data[0]["stid"], np.ndarray):
        data[0]["stid"] = np.array(data[0]["stid"], dtype=np.float64)
    else:
        data[0]["stid"] = float(data[0]["stid"])
    _ = dmap.write_dmap(data)


@pytest.mark.parametrize("fmt", FORMATS)
def test_extra_key_read(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data = dmap.read_dmap(infile, mode="strict")
    data[0]["test"] = 1.0
    raw_bytes = dmap.write_dmap(data)

    with pytest.raises(ValueError):
        _ = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")


@pytest.mark.parametrize("fmt", FORMATS)
def test_missing_key_read(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    del data[0]["stid"]
    raw_bytes = dmap.write_dmap(data)

    with pytest.raises(ValueError):
        _ = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")


@pytest.mark.parametrize("fmt", FORMATS)
def test_key_wrong_type_read(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"

    data = getattr(dmap, f"read_{fmt}")(infile, mode="strict")
    if isinstance(data[0]["stid"], np.ndarray):
        data[0]["stid"] = np.array(data[0]["stid"], dtype=np.float64)
    else:
        data[0]["stid"] = float(data[0]["stid"])
    raw_bytes = dmap.write_dmap(data)

    with pytest.raises(ValueError):
        _ = getattr(dmap, f"read_{fmt}")(raw_bytes, mode="strict")


@pytest.mark.parametrize("fmt,data_fields", zip(FORMATS, DATA_FIELDS))
def test_read_metadata(fmt, data_fields):
    infile = f"{HERE}/test_files/test.{fmt}"
    data = getattr(dmap, f"read_{fmt}")(infile, mode="metadata")
    for rec in data:
        assert not any([f in rec for f in data_fields])

