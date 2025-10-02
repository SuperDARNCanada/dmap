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
    data = dmap.read_dmap(f"{HERE}/test_files/test.{fmt}", mode="sniff")
    assert isinstance(data, dict)


@pytest.mark.parametrize("fmt", FORMATS)
def test_sniff_against_specific(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    data = dmap.read_dmap(infile, mode="sniff")
    data2 = getattr(dmap, f"read_{fmt}")(infile, mode="sniff")
    assert compare_recs([data], [data2])


@pytest.mark.parametrize("fmt", FORMATS)
def test_sniff_against_specific_strict(fmt):
    infile = f"{HERE}/test_files/test.{fmt}"
    data1 = getattr(dmap, f"read_{fmt}")(infile, mode="strict")[0]
    data2 = getattr(dmap, f"read_{fmt}")(infile, mode="sniff")
    assert compare_recs([data1], [data2])


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
