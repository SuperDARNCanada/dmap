"""
Wrappers around the `dmap_rs` Python API.

Each file type will have one function for calling any type of reading (strict, lax, bytes, sniff, metadata) or any type of writing
(regular, bytes).
"""

from typing import Union, Optional
from . import dmap_rs


def read_dispatcher(
    source: Union[str, bytes], fmt: str, mode: str, indices: Optional[Union[tuple[int], list[int]]] = None
) -> Union[list[dict], tuple[list[dict], Optional[int]]]:
    """
    Reads in DMAP data from `source`.

    Parameters
    ----------
    source: Union[str, bytes]
        Where to read data from. If input is of type `str`, this is interpreted as the path to a file.
        If input is of type `bytes`, this is interpreted as the raw data itself.
    fmt: str
        DMAP format being read. One of `["dmap", "iqdat", "rawacf", "fitacf", "grid", "map", "snd"]`.
    mode: str
        Mode in which to read the data, one of `["strict", "lax", "metadata"]`. In `strict` mode, any corruption
        in the data will raise an error. In `lax` mode, all valid records will be returned in a tuple along with
        the byte index of `source` where the corruption starts. In `metadata` mode, `source` must be a `str`,
        and only the metadata fields of the records are returned.
    indices: tuple
        Collection of indices to read. Supports negative indexing.

    Returns
    -------
    If `mode` is `strict` or `metadata`, returns `list[dict]` which is the parsed records.
    If `mode` is `lax`, returns `tuple[list[dict], Optional[int]]`, where the first element is the records which were parsed,
        and the second is the byte index where `source` was no longer a valid record of type `fmt`.
    If `mode` is `metadata`, returns `list[dict]` of the metadata from the records.
    """
    if fmt not in ["dmap", "iqdat", "rawacf", "fitacf", "grid", "map", "snd"]:
        raise ValueError(
            f"invalid fmt `{fmt}`: expected one of ['dmap', 'iqdat', 'rawacf', 'fitacf', 'grid', 'map', 'snd']"
        )

    if mode not in ["strict", "lax", "metadata"]:
        raise ValueError(f"invalid mode `{mode}`: expected `strict`, `lax`, or `metadata`")

    if mode == "metadata" and not isinstance(source, str):
        raise TypeError(
            f"invalid type for `source` {type(source)} in `metadata` mode: expected `str`"
        )

    if indices is not None and not isinstance(indices, tuple) and not isinstance(indices, list):
        raise TypeError(
            f"invalid type for `indices` {type(source)}: expected `tuple` or `list`"
        )

    if not isinstance(source, bytes) and not isinstance(source, str):
        raise TypeError(
            f"invalid type for `source` {type(source)}: expected `str` or `bytes`"
        )

    # Construct the darn-dmap function name dynamically based on parameters:
    # fn_name = read_[fmt][_metadata][_by_indices][_bytes][_lax]
    # All possibilites for, e.g., a FITACF file:
    #   read_fitacf
    #   read_fitacf_lax
    #   read_fitacf_bytes
    #   read_fitacf_bytes_lax
    #   read_fitacf_by_indices
    #   read_fitacf_by_indices_lax
    #   read_fitacf_by_indices_bytes
    #   read_fitacf_by_indices_bytes_lax
    #   read_fitacf_metadata
    #   read_fitacf_metadata_by_indices
    if indices is not None and len(indices) > 0:
        indices_given = True
        args = (source, indices)
    else:
        indices_given = False
        args = (source,)

    fn_name = (
        f"read_{fmt}"
        f"{'_metadata' if mode == 'metadata' else ''}"
        f"{'_by_indices' if indices_given else ''}"
        f"{'_bytes' if isinstance(source, bytes) else ''}"
        f"{'_lax' if mode == 'lax' else ''}"
    )

    return getattr(dmap_rs, fn_name)(*args)


def write_dispatcher(
    source: list[dict], fmt: str, outfile: Union[None, str], bz2: bool,
) -> Union[None, bytes]:
    """
    Writes DMAP data from `source` to either a `bytes` object or to `outfile`.

    Parameters
    ----------
    source: list[dict]
        list of DMAP records as dictionaries.
    fmt: str
        DMAP format being read. One of `["dmap", "iqdat", "rawacf", "fitacf", "grid", "map", "snd"]`.
    outfile: Union[None, str]
        If `None`, returns the data as a `bytes` object. If this is a string, then this is interpreted as a path
        and data will be written to the filesystem. If the file ends in the `.bz2` extension, the data will be
        compressed using bzip2.
    bz2: bool
        If `True`, the data will be compressed with `bzip2`.
    """
    if fmt not in ["dmap", "iqdat", "rawacf", "fitacf", "grid", "map", "snd"]:
        raise ValueError(
            f"invalid fmt `{fmt}`: expected one of ['dmap', 'iqdat', 'rawacf', 'fitacf', 'grid', 'map', 'snd']"
        )
    if outfile is None:
        return getattr(dmap_rs, f"write_{fmt}_bytes")(source, bz2=bz2)
    elif isinstance(outfile, str):
        getattr(dmap_rs, f"write_{fmt}")(source, outfile, bz2=bz2)
    else:
        raise TypeError(
            f"invalid type for `outfile` {type(outfile)}: expected `str` or `None`"
        )


def read_dmap(
    source: Union[str, bytes], mode: str = "lax", indices: Optional[Union[tuple[int], list[int]]] = None
) -> Union[list[dict], tuple[list[dict], Optional[int]]]:
    """
    Reads in DMAP data from `source`.

    Parameters
    ----------
    source: Union[str, bytes]
        Where to read data from. If input is of type `str`, this is interpreted as the path to a file.
        If input is of type `bytes`, this is interpreted as the raw data itself.
    mode: str
        Mode in which to read the data, one of `["strict", "lax", "metadata"]`.
        In `strict` mode, any corruption in the data will raise an error.
        In `lax` mode, all valid records will be returned in a tuple along with the byte index of `source`
            where the corruption starts.
        In `metadata` mode, `source` must be a `str`, and only the metadata fields of the records are returned.
    indices: Optional[Union[tuple[int], list[int]]]
        Collection of indices to read. Supports negative indexing. If given, only records corresponding to the indices
        are read.

    Returns
    -------
    If `mode` is `strict` or `metadata`, returns `list[dict]` which is the parsed records.
    If `mode` is `lax`, returns `tuple[list[dict], Optional[int]]`, where the first element is the records which were parsed,
        and the second is the byte index where `source` was no longer a valid record of type `fmt`.
    If `mode` is `metadata`, returns `list[dict]` of the metadata from the records.
    """
    return read_dispatcher(source, "dmap", mode, indices)


def read_iqdat(
    source: Union[str, bytes], mode: str = "lax", indices: Optional[Union[tuple[int], list[int]]] = None
) -> Union[dict, list[dict], tuple[list[dict], Optional[int]]]:
    """ 
    Reads in IQDAT data from `source`.

    Parameters
    ----------
    source: Union[str, bytes]
        Where to read data from. If input is of type `str`, this is interpreted as the path to a file.
        If input is of type `bytes`, this is interpreted as the raw data itself.
    mode: str
        Mode in which to read the data, one of `["strict", "lax", "metadata"]`.
        In `strict` mode, any corruption in the data will raise an error.
        In `lax` mode, all valid records will be returned in a tuple along with the byte index of `source`
            where the corruption starts.
        In `metadata` mode, `source` must be a `str`, and only the metadata fields of the records are returned.
    indices: Optional[Union[tuple[int], list[int]]]
        Collection of indices to read. Supports negative indexing. If given, only records corresponding to the indices
        are read.

    Returns
    -------
    If `mode` is `strict` or `metadata`, returns `list[dict]` which is the parsed records.
    If `mode` is `lax`, returns `tuple[list[dict], Optional[int]]`, where the first element is the records which were parsed,
        and the second is the byte index where `source` was no longer a valid record of type `fmt`.
    If `mode` is `metadata`, returns `list[dict]` of the metadata from the records.
    """
    return read_dispatcher(source, "iqdat", mode, indices)


def read_rawacf(
    source: Union[str, bytes], mode: str = "lax", indices: Optional[Union[tuple[int], list[int]]] = None
) -> Union[dict, list[dict], tuple[list[dict], Optional[int]]]:
    """ 
    Reads in RAWACF data from `source`.

    Parameters
    ----------
    source: Union[str, bytes]
        Where to read data from. If input is of type `str`, this is interpreted as the path to a file.
        If input is of type `bytes`, this is interpreted as the raw data itself.
    mode: str
        Mode in which to read the data, one of `["strict", "lax", "metadata"]`.
        In `strict` mode, any corruption in the data will raise an error.
        In `lax` mode, all valid records will be returned in a tuple along with the byte index of `source`
            where the corruption starts.
        In `metadata` mode, `source` must be a `str`, and only the metadata fields of the records are returned.
    indices: Optional[Union[tuple[int], list[int]]]
        Collection of indices to read. Supports negative indexing. If given, only records corresponding to the indices
        are read.

    Returns
    -------
    If `mode` is `strict` or `metadata`, returns `list[dict]` which is the parsed records.
    If `mode` is `lax`, returns `tuple[list[dict], Optional[int]]`, where the first element is the records which were parsed,
        and the second is the byte index where `source` was no longer a valid record of type `fmt`.
    If `mode` is `metadata`, returns `list[dict]` of the metadata from the records.
    """
    return read_dispatcher(source, "rawacf", mode, indices)


def read_fitacf(
    source: Union[str, bytes], mode: str = "lax", indices: Optional[Union[tuple[int], list[int]]] = None
) -> Union[dict, list[dict], tuple[list[dict], Optional[int]]]:
    """ 
    Reads in FITACF data from `source`.

    Parameters
    ----------
    source: Union[str, bytes]
        Where to read data from. If input is of type `str`, this is interpreted as the path to a file.
        If input is of type `bytes`, this is interpreted as the raw data itself.
    mode: str
        Mode in which to read the data, one of `["strict", "lax", "metadata"]`.
        In `strict` mode, any corruption in the data will raise an error.
        In `lax` mode, all valid records will be returned in a tuple along with the byte index of `source`
            where the corruption starts.
        In `metadata` mode, `source` must be a `str`, and only the metadata fields of the records are returned.
    indices: Optional[Union[tuple[int], list[int]]]
        Collection of indices to read. Supports negative indexing. If given, only records corresponding to the indices
        are read.

    Returns
    -------
    If `mode` is `strict` or `metadata`, returns `list[dict]` which is the parsed records.
    If `mode` is `lax`, returns `tuple[list[dict], Optional[int]]`, where the first element is the records which were parsed,
        and the second is the byte index where `source` was no longer a valid record of type `fmt`.
    If `mode` is `metadata`, returns `list[dict]` of the metadata from the records.
    """
    return read_dispatcher(source, "fitacf", mode, indices)


def read_grid(
    source: Union[str, bytes], mode: str = "lax", indices: Optional[Union[tuple[int], list[int]]] = None
) -> Union[dict, list[dict], tuple[list[dict], Optional[int]]]:
    """ 
    Reads in GRID data from `source`.

    Parameters
    ----------
    source: Union[str, bytes]
        Where to read data from. If input is of type `str`, this is interpreted as the path to a file.
        If input is of type `bytes`, this is interpreted as the raw data itself.
    mode: str
        Mode in which to read the data, one of `["strict", "lax", "metadata"]`.
        In `strict` mode, any corruption in the data will raise an error.
        In `lax` mode, all valid records will be returned in a tuple along with the byte index of `source`
            where the corruption starts.
        In `metadata` mode, `source` must be a `str`, and only the metadata fields of the records are returned.
    indices: Optional[Union[tuple[int], list[int]]]
        Collection of indices to read. Supports negative indexing. If given, only records corresponding to the indices
        are read.

    Returns
    -------
    If `mode` is `strict` or `metadata`, returns `list[dict]` which is the parsed records.
    If `mode` is `lax`, returns `tuple[list[dict], Optional[int]]`, where the first element is the records which were parsed,
        and the second is the byte index where `source` was no longer a valid record of type `fmt`.
    If `mode` is `metadata`, returns `list[dict]` of the metadata from the records.
    """
    return read_dispatcher(source, "grid", mode, indices)


def read_map(
    source: Union[str, bytes], mode: str = "lax", indices: Optional[Union[tuple[int], list[int]]] = None
) -> Union[dict, list[dict], tuple[list[dict], Optional[int]]]:
    """ 
    Reads in MAP data from `source`.

    Parameters
    ----------
    source: Union[str, bytes]
        Where to read data from. If input is of type `str`, this is interpreted as the path to a file.
        If input is of type `bytes`, this is interpreted as the raw data itself.
    mode: str
        Mode in which to read the data, one of `["strict", "lax", "metadata"]`.
        In `strict` mode, any corruption in the data will raise an error.
        In `lax` mode, all valid records will be returned in a tuple along with the byte index of `source`
            where the corruption starts.
        In `metadata` mode, `source` must be a `str`, and only the metadata fields of the records are returned.
    indices: Optional[Union[tuple[int], list[int]]]
        Collection of indices to read. Supports negative indexing. If given, only records corresponding to the indices
        are read.

    Returns
    -------
    If `mode` is `strict` or `metadata`, returns `list[dict]` which is the parsed records.
    If `mode` is `lax`, returns `tuple[list[dict], Optional[int]]`, where the first element is the records which were parsed,
        and the second is the byte index where `source` was no longer a valid record of type `fmt`.
    If `mode` is `metadata`, returns `list[dict]` of the metadata from the records.
    """
    return read_dispatcher(source, "map", mode, indices)


def read_snd(
    source: Union[str, bytes], mode: str = "lax", indices: Optional[Union[tuple[int], list[int]]] = None
) -> Union[dict, list[dict], tuple[list[dict], Optional[int]]]:
    """ 
    Reads in SND data from `source`.

    Parameters
    ----------
    source: Union[str, bytes]
        Where to read data from. If input is of type `str`, this is interpreted as the path to a file.
        If input is of type `bytes`, this is interpreted as the raw data itself.
    mode: str
        Mode in which to read the data, one of `["strict", "lax", "metadata"]`.
        In `strict` mode, any corruption in the data will raise an error.
        In `lax` mode, all valid records will be returned in a tuple along with the byte index of `source`
            where the corruption starts.
        In `metadata` mode, `source` must be a `str`, and only the metadata fields of the records are returned.
    indices: Optional[Union[tuple[int], list[int]]]
        Collection of indices to read. Supports negative indexing. If given, only records corresponding to the indices
        are read.

    Returns
    -------
    If `mode` is `strict` or `metadata`, returns `list[dict]` which is the parsed records.
    If `mode` is `lax`, returns `tuple[list[dict], Optional[int]]`, where the first element is the records which were parsed,
        and the second is the byte index where `source` was no longer a valid record of type `fmt`.
    If `mode` is `metadata`, returns `list[dict]` of the metadata from the records.
    """
    return read_dispatcher(source, "snd", mode, indices)


def write_dmap(
    source: list[dict], outfile: Union[None, str] = None, bz2: bool = False,
) -> Union[None, bytes]:
    """
    Writes DMAP data from `source` to either a `bytes` object or to `outfile`.

    Parameters
    ----------
    source: list[dict]
        list of DMAP records as dictionaries.
    outfile: Union[None, str]
        If `None`, returns the data as a `bytes` object. If this is a string, then this is interpreted as a path
        and data will be written to the filesystem. If the file ends in the `.bz2` extension, the data will be
        compressed using bzip2.
    bz2: bool
        If `True`, the data will be compressed with `bzip2`.
    """
    return write_dispatcher(source, "dmap", outfile, bz2=bz2)


def write_iqdat(
    source: list[dict], outfile: Union[None, str] = None, bz2: bool = False,
) -> Union[None, bytes]:
    """
    Writes IQDAT data from `source` to either a `bytes` object or to `outfile`.

    Parameters
    ----------
    source: list[dict]
        list of IQDAT records as dictionaries.
    outfile: Union[None, str]
        If `None`, returns the data as a `bytes` object. If this is a string, then this is interpreted as a path
        and data will be written to the filesystem. If the file ends in the `.bz2` extension, the data will be
        compressed using bzip2.
    bz2: bool
        If `True`, the data will be compressed with `bzip2`.
    """
    return write_dispatcher(source, "iqdat", outfile, bz2=bz2)


def write_rawacf(
    source: list[dict], outfile: Union[None, str] = None, bz2: bool = False,
) -> Union[None, bytes]:
    """
    Writes RAWACF data from `source` to either a `bytes` object or to `outfile`.

    Parameters
    ----------
    source: list[dict]
        list of RAWACF records as dictionaries.
    outfile: Union[None, str]
        If `None`, returns the data as a `bytes` object. If this is a string, then this is interpreted as a path
        and data will be written to the filesystem. If the file ends in the `.bz2` extension, the data will be
        compressed using bzip2.
    bz2: bool
        If `True`, the data will be compressed with `bzip2`.
    """
    return write_dispatcher(source, "rawacf", outfile, bz2=bz2)


def write_fitacf(
    source: list[dict], outfile: Union[None, str] = None, bz2: bool = False,
) -> Union[None, bytes]:
    """
    Writes FITACF data from `source` to either a `bytes` object or to `outfile`.

    Parameters
    ----------
    source: list[dict]
        list of FITACF records as dictionaries.
    outfile: Union[None, str]
        If `None`, returns the data as a `bytes` object. If this is a string, then this is interpreted as a path
        and data will be written to the filesystem. If the file ends in the `.bz2` extension, the data will be
        compressed using bzip2.
    bz2: bool
        If `True`, the data will be compressed with `bzip2`.
    """
    return write_dispatcher(source, "fitacf", outfile, bz2=bz2)


def write_grid(
    source: list[dict], outfile: Union[None, str] = None, bz2: bool = False,
) -> Union[None, bytes]:
    """
    Writes GRID data from `source` to either a `bytes` object or to `outfile`.

    Parameters
    ----------
    source: list[dict]
        list of GRID records as dictionaries.
    outfile: Union[None, str]
        If `None`, returns the data as a `bytes` object. If this is a string, then this is interpreted as a path
        and data will be written to the filesystem. If the file ends in the `.bz2` extension, the data will be
        compressed using bzip2.
    bz2: bool
        If `True`, the data will be compressed with `bzip2`.
    """
    return write_dispatcher(source, "grid", outfile, bz2=bz2)


def write_map(
    source: list[dict], outfile: Union[None, str] = None, bz2: bool = False,
) -> Union[None, bytes]:
    """
    Writes MAP data from `source` to either a `bytes` object or to `outfile`.

    Parameters
    ----------
    source: list[dict]
        list of MAP records as dictionaries.
    outfile: Union[None, str]
        If `None`, returns the data as a `bytes` object. If this is a string, then this is interpreted as a path
        and data will be written to the filesystem. If the file ends in the `.bz2` extension, the data will be
        compressed using bzip2.
    bz2: bool
        If `True`, the data will be compressed with `bzip2`.
    """
    return write_dispatcher(source, "map", outfile, bz2=bz2)


def write_snd(
    source: list[dict], outfile: Union[None, str] = None, bz2: bool = False,
) -> Union[None, bytes]:
    """
    Writes SND data from `source` to either a `bytes` object or to `outfile`.

    Parameters
    ----------
    source: list[dict]
        list of SND records as dictionaries.
    outfile: Union[None, str]
        If `None`, returns the data as a `bytes` object. If this is a string, then this is interpreted as a path
        and data will be written to the filesystem. If the file ends in the `.bz2` extension, the data will be
        compressed using bzip2.
    bz2: bool
        If `True`, the data will be compressed with `bzip2`.
    """
    return write_dispatcher(source, "snd", outfile, bz2=bz2)
