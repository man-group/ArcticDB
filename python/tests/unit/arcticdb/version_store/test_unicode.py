"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.

We have special handling in the codebase when working with unicode Python strings, since we need to take the GIL
to handle them. This file checks that our APIs work even when passed unicode string."""
import datetime

import numpy as np
import pandas as pd
import pytest

from arcticdb.util.test import assert_frame_equal, random_strings_of_length

from arcticdb.version_store.library import Library
from arcticdb.version_store.library import UpdatePayload
from arcticdb_ext.storage import NoDataFoundException

unicode_str = "\u0420\u043e\u0441\u0441\u0438\u044f"
copyright = "My Thing Not Your's \u00A9"
trademark = "My Word Not Your's \u2122"
metadata = {copyright: trademark}
symbol = "sym"


def unicode_strs_df(start_date: pd.Timestamp, num_rows: int) -> pd.DataFrame:
    index = [start_date + datetime.timedelta(days=i) for i in range(num_rows)]
    df = pd.DataFrame(
        index=index,
        data={"a": random_strings_of_length(num_rows, 10), trademark: np.arange(num_rows), copyright: [unicode_str] * num_rows},
    )
    return df


@pytest.mark.parametrize("parallel", (True, False))
@pytest.mark.parametrize("multi_index", (True, False))
def test_write(lmdb_version_store_tiny_segment, parallel, multi_index):
    lib = lmdb_version_store_tiny_segment
    start = pd.Timestamp("2018-01-02")
    num_rows = 100
    if multi_index:
        index = pd.MultiIndex.from_arrays([[start + datetime.timedelta(days=i) for i in range(num_rows)], [unicode_str] * num_rows])
    else:
        index = pd.date_range(start=start, periods=num_rows)

    df = pd.DataFrame(
        index=index,
        data={"a": random_strings_of_length(num_rows, 10), trademark: np.arange(num_rows), copyright: [unicode_str] * num_rows},
    )

    if parallel:
        lib.write(symbol, df, parallel=True)
        lib.compact_incomplete(symbol, append=False, convert_int_to_float=False, metadata=metadata)
    else:
        lib.write(symbol, df, metadata=metadata)

    lib.create_column_stats(symbol, column_stats={trademark: {"MINMAX"}})
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df)
    assert vit.metadata == metadata


def test_write_metadata(lmdb_version_store):
    lmdb_version_store.write("sym", [1, 2, 3], metadata=metadata)
    assert lmdb_version_store.read("sym").metadata == metadata


def test_batch_write_metadata(lmdb_version_store):
    syms = [f"sym_{i}" for i in range(100)]
    metadata_vector = [metadata] * 100
    lmdb_version_store.batch_write_metadata(symbols=syms, metadata_vector=metadata_vector)

    for s in syms:
        assert lmdb_version_store.read(s).metadata == metadata


def test_batch_append(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    start = pd.Timestamp("2018-01-02")
    df1 = unicode_strs_df(start, 100)
    lib.batch_write(symbols=[symbol], data_vector=[df1])
    vit = lib.batch_read([symbol])[symbol]
    assert_frame_equal(vit.data, df1)

    df2 = unicode_strs_df(start + datetime.timedelta(days=100), 100)
    lib.batch_append(symbols=[symbol], data_vector=[df2])
    vit = lib.batch_read([symbol])[symbol]
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)


def test_batch_write_with_metadata(lmdb_version_store):
    df1 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03")],
        data={"a": ["123", unicode_str]},
    )

    lmdb_version_store.batch_write(symbols=[symbol], data_vector=[df1])
    vit = lmdb_version_store.batch_read([symbol])[symbol]
    assert_frame_equal(vit.data, df1)

    meta = {"a": 1, "b": unicode_str}
    lmdb_version_store.batch_write_metadata(symbols=[symbol], metadata_vector=[meta])
    vits = lmdb_version_store.batch_read_metadata([symbol])
    metadata = vits[symbol].metadata
    assert metadata == meta


def test_append(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    start = pd.Timestamp("2018-01-02")
    df1 = unicode_strs_df(start, 100)
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)

    df2 = unicode_strs_df(start + datetime.timedelta(days=100), 100)
    lib.append(symbol, df2)
    vit = lib.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)


@pytest.mark.parametrize("api_method", ("write", "append"))
def test_staged_append(lmdb_version_store_tiny_segment, api_method):
    lib = lmdb_version_store_tiny_segment
    df1 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03")],
        data={copyright: ["123", unicode_str]},
    )
    lib.write(symbol, df1)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)

    expected = [df1]
    for i in range(20):
        start = pd.Timestamp("2018-01-02") + datetime.timedelta(days=(i + 1) * 2)
        index = pd.date_range(start=start, periods=2)
        df = pd.DataFrame(
            index=index,
            data={copyright: ["123", unicode_str]},
        )
        if api_method == "write":
            lib.write(symbol, df, parallel=True)
        elif api_method == "append":
            lib.append(symbol, df, incomplete=True)
        else:
            raise RuntimeError("Unexpected api_method")
        expected.append(df)

    lib.compact_incomplete(symbol, append=True, convert_int_to_float=False)
    vit = lib.read(symbol)
    expected = pd.concat(expected)
    assert_frame_equal(vit.data, expected)


def test_update(lmdb_version_store_tiny_segment):
    lib = lmdb_version_store_tiny_segment
    start = pd.Timestamp("2018-01-02")
    df1 = unicode_strs_df(start, 100)
    lib.update(symbol, df1, upsert=True)
    vit = lib.read(symbol)
    assert_frame_equal(vit.data, df1)

    df2 = unicode_strs_df(start + datetime.timedelta(days=100), 100)
    lib.update(symbol, df2)
    vit = lib.read(symbol)
    expected = pd.concat([df1, df2])
    assert_frame_equal(vit.data, expected)

    df1_new = unicode_strs_df(start + datetime.timedelta(days=1), 100)
    lib.update(symbol, df1_new)
    vit = lib.read(symbol)
    expected = pd.concat([df1, df2])
    expected.update(df1_new)
    assert_frame_equal(vit.data, expected, check_dtype=False)  # disable check_dtype to pass with older Pandas versions


def test_batch_update(lmdb_version_store):
    lib = lmdb_version_store
    adb_lib = Library("desc", lib)
    sym_1 = "sym_1"
    sym_2 = "sym_2"

    df1 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03")],
        data={copyright: ["123", unicode_str]},
    )
    lmdb_version_store.write(sym_1, df1)
    lmdb_version_store.write(sym_2, df1)

    df2 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-03"), pd.Timestamp("2018-01-05")],
        data={copyright: ["456", trademark]},
    )
    df3 = pd.DataFrame(
        index=[pd.Timestamp("2018-02-04"), pd.Timestamp("2018-02-05")],
        data={copyright: ["789", trademark]},
    )

    update_payloads = [UpdatePayload(sym_1, df2), UpdatePayload(sym_2, df3)]
    adb_lib.update_batch(update_payloads)

    vit = adb_lib.read(sym_1)
    expected = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03"), pd.Timestamp("2018-01-05")],
        data ={copyright: ["123", "456", trademark]}
    )
    assert_frame_equal(vit.data, expected)

    vit = adb_lib.read(sym_2)
    expected = pd.concat([df1, df3])
    assert_frame_equal(vit.data, expected)


def test_snapshots(lmdb_version_store):
    """We should probably validate against snapshots with unicode names like we do for symbols, but these tests check
    the status quo.

    Monday: 8667974441 to validate against this.
    """
    start = pd.Timestamp("2018-01-02")
    index = pd.date_range(start=start, periods=4)

    df = pd.DataFrame(
        index=index,
        data={"a": ["123", unicode_str, copyright, trademark], trademark: [1, 2, 3, 4], copyright: [unicode_str] * 4},
    )

    # Test snapshots with unicode names
    lmdb_version_store.write(symbol, df, metadata=metadata)
    lmdb_version_store.snapshot(copyright)
    lmdb_version_store.snapshot(unicode_str, metadata=metadata)
    lmdb_version_store.write(symbol, [1, 2, 3])

    vit = lmdb_version_store.read(symbol, as_of=copyright)
    assert_frame_equal(vit.data, df)

    snapshots = lmdb_version_store.list_snapshots()
    assert snapshots == {copyright: None, unicode_str: metadata}

    # Test deleting a snapshot with unicode name
    lmdb_version_store.delete_snapshot(copyright)
    snapshots = lmdb_version_store.list_snapshots()
    assert snapshots == {unicode_str: metadata}
    with pytest.raises(NoDataFoundException):
        lmdb_version_store.read(symbol, as_of=copyright)
    vit = lmdb_version_store.read(symbol, as_of=unicode_str)
    assert_frame_equal(vit.data, df)

    # Test adding to a snapshot with unicode name
    lmdb_version_store.write("new_sym", df, metadata=metadata)
    lmdb_version_store.add_to_snapshot(unicode_str, ["new_sym"])
    lmdb_version_store.delete("new_sym")
    vit = lmdb_version_store.read("new_sym", as_of=unicode_str)
    assert_frame_equal(vit.data, df)

    # Test list_versions
    vers = lmdb_version_store.list_versions("new_sym")
    assert len(vers) == 1
    assert vers[0]["snapshots"] == [unicode_str]

    # Test removing from a snapshot with unicode name
    lmdb_version_store.remove_from_snapshot(unicode_str, ["new_sym"], versions=[0])
    assert lmdb_version_store.list_versions("new_sym") == []
    with pytest.raises(NoDataFoundException):
        lmdb_version_store.read("new_sym", as_of=copyright)


@pytest.mark.parametrize("batch", (True, False))
def test_get_info(lmdb_version_store, batch):
    start = pd.Timestamp("2018-01-02")
    index = pd.date_range(start=start, periods=4)
    unicode_str = "ab"

    df_1 = pd.DataFrame(
        index=index,
        data={"a": ["123", unicode_str, copyright, trademark], trademark: [1, 2, 3, 4], copyright: [unicode_str] * 4},
    )
    df_1.index.set_names([unicode_str])

    df_2 = pd.DataFrame(
        index=index,
        data={unicode_str: [1, 2, 3, 4], trademark: [1, 2, 3, 4], copyright: [unicode_str] * 4},
    )
    df_2.index.set_names([unicode_str])
    lmdb_version_store.write("sym_1", df_1, metadata=metadata)
    lmdb_version_store.write("sym_2", df_2, metadata=metadata)

    if batch:
        res = lmdb_version_store.batch_get_info(symbols=["sym_1", "sym_2"])
        assert len(res) == 2
        assert list(df_1.columns) == res[0]["col_names"]["columns"]
        assert list(df_2.columns) == res[1]["col_names"]["columns"]
    else:
        for sym, df in [("sym_1", df_1), ("sym_2", df_2)]:
            res = lmdb_version_store.get_info(sym)
            assert list(df.columns) == res["col_names"]["columns"]
            # assert res["col_names"]["index"] == [unicode_str]  # index names are not exposed by get_info, seems to be a bug 8667920777


def sample_nested_structures():
    return [
        {"a": ["abc", "def", copyright, trademark, unicode_str], "b": random_strings_of_length(num=8, length=5, unique=False)},
        (random_strings_of_length(num=10, length=6, unique=True), random_strings_of_length(num=10, length=9, unique=True)),
    ]


@pytest.mark.parametrize("batch_read", (True, False))
def test_recursively_written_data_with_metadata(lmdb_version_store, batch_read):
    samples = sample_nested_structures()

    for idx, sample in enumerate(samples):
        sym = "sym_recursive" + str(idx)
        metadata = {unicode_str: 1}
        lmdb_version_store.write(sym, sample, metadata=metadata, recursive_normalizers=True)
        if batch_read:
            vit = lmdb_version_store.batch_read([sym])[sym]
        else:
            vit = lmdb_version_store.read(sym)
        assert sample == vit.data
        assert vit.symbol == sym
        assert vit.metadata == metadata


def test_recursively_written_data_with_metadata_batch_write(lmdb_version_store):
    samples = sample_nested_structures()
    syms = [f"sym_{i}" for i in range(len(samples))]
    metadata = [{unicode_str: i} for i in range(len(samples))]

    lmdb_version_store.batch_write(symbols=syms, data_vector=samples, metadata_vector=metadata)

    res = lmdb_version_store.batch_read(syms)
    assert len(res) == len(syms)

    for i, sym in enumerate(syms):
        assert sym in res
        assert res[sym].symbol == sym
        assert res[sym].data == samples[i]
        assert res[sym].metadata == metadata[i]
