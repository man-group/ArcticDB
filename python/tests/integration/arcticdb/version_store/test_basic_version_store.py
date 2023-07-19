"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import itertools
import time
import sys

import mock
import numpy as np
import os
import math
import pandas as pd
import pytest
import random
import string
from collections import namedtuple
from datetime import datetime
from numpy.testing import assert_array_equal
from pytz import timezone

from arcticdb.exceptions import (
    ArcticNativeNotYetImplemented,
    InternalException,
    NoSuchVersionException,
    StreamDescriptorMismatch,
)
from arcticdb_ext.storage import NoDataFoundException
from arcticdb.flattener import Flattener
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store._custom_normalizers import CustomNormalizer, register_normalizer
from arcticdb.version_store._store import UNSUPPORTED_S3_CHARS, MAX_SYMBOL_SIZE, VersionedItem
from arcticdb_ext.exceptions import _ArcticLegacyCompatibilityException
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb_ext.version_store import NoSuchVersionException, StreamDescriptorMismatch
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata  # Importing from arcticdb dynamically loads arcticc.pb2
from arcticdb.util.test import (
    sample_dataframe,
    sample_dataframe_only_strings,
    get_sample_dataframe,
    assert_frame_equal,
    assert_series_equal,
)
from arcticdb_ext.tools import AZURE_SUPPORT
from tests.util.date import DateRange


if sys.platform == "linux":
    SMOKE_TEST_VERSION_STORES = [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "s3_version_store_v1",
        "s3_version_store_v2",
        "mongo_version_store",
    ]
else:
    # leave out Mongo as spinning up a Mongo instance in Windows CI is fiddly, and Mongo support is only
    # currently required for Linux for internal use.
    # We also skip it on Mac as github actions containers don't work with macos
    SMOKE_TEST_VERSION_STORES = [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "s3_version_store_v1",
        "s3_version_store_v2",
    ]  # SKIP_WIN and SKIP_MAC

if AZURE_SUPPORT:
    SMOKE_TEST_VERSION_STORES.append("azure_version_store")


@pytest.fixture()
def symbol():
    return "sym" + str(random.randint(0, 10000))


def assert_equal_value(data, expected):
    received = data.reindex(sorted(data.columns), axis=1)
    expected = expected.reindex(sorted(expected.columns), axis=1)
    assert_frame_equal(received, expected)


def test_simple_flow(lmdb_version_store_no_symbol_list, symbol):
    df = sample_dataframe()
    modified_df = pd.DataFrame({"col": [1, 2, 3, 4]})
    lmdb_version_store_no_symbol_list.write(symbol, df, metadata={"blah": 1})
    assert lmdb_version_store_no_symbol_list.read(symbol).metadata == {"blah": 1}

    lmdb_version_store_no_symbol_list.write(symbol, modified_df)
    vitem = lmdb_version_store_no_symbol_list.read(symbol)
    assert_frame_equal(vitem.data, modified_df)
    assert lmdb_version_store_no_symbol_list.list_symbols() == [symbol]

    lmdb_version_store_no_symbol_list.delete(symbol)
    assert lmdb_version_store_no_symbol_list.list_symbols() == lmdb_version_store_no_symbol_list.list_versions() == []


@pytest.mark.parametrize("special_char", ["$", ",", ":", "=", "@", "-", "_", ".", "~"])
def test_special_chars(s3_version_store, special_char):
    """Test chars with special URI encoding under RFC 3986"""
    sym = f"prefix{special_char}postfix"
    df = sample_dataframe()
    s3_version_store.write(sym, df)
    vitem = s3_version_store.read(sym)
    assert_frame_equal(vitem.data, df)


@pytest.mark.parametrize("version_store", SMOKE_TEST_VERSION_STORES)
def test_with_prune(request, version_store, symbol):
    version_store = request.getfixturevalue(version_store)
    df = sample_dataframe()
    modified_df = sample_dataframe()

    version_store.write(symbol, df, metadata={"something": "something"}, prune_previous_version=True)
    version_store.write(symbol, modified_df, prune_previous_version=True)

    assert len(version_store.list_versions()) == 1

    version_store.snapshot("my_snap")

    final_df = sample_dataframe()
    version_store.write(symbol, final_df, prune_previous_version=True)
    version_store.snapshot("my_snap2")

    # previous versions should have been deleted by now.
    assert len([ver for ver in version_store.list_versions() if not ver["deleted"]]) == 1
    # previous versions should be accessible through snapshot
    assert_frame_equal(version_store.read(symbol, as_of="my_snap").data, modified_df)
    assert_frame_equal(version_store.read(symbol, as_of="my_snap2").data, final_df)


def test_prune_previous_versions_explicit_method(lmdb_version_store, symbol):
    # Given
    df = sample_dataframe()
    modified_df = sample_dataframe(1)

    lmdb_version_store.write(symbol, df, metadata={"something": "something"}, prune_previous_version=True)
    lmdb_version_store.write(symbol, modified_df, prune_previous_version=False)

    lmdb_version_store.snapshot("my_snap")

    final_df = sample_dataframe(2)
    lmdb_version_store.write(symbol, final_df, prune_previous_version=False)

    # When
    lmdb_version_store.prune_previous_versions(symbol)

    # Then - only latest version and snapshots should survive
    assert_frame_equal(lmdb_version_store.read(symbol).data, final_df)
    assert len([ver for ver in lmdb_version_store.list_versions() if not ver["deleted"]]) == 1
    assert_frame_equal(lmdb_version_store.read(symbol, as_of="my_snap").data, modified_df)


def test_prune_previous_versions_nothing_to_do(lmdb_version_store, symbol):
    df = sample_dataframe()
    lmdb_version_store.write(symbol, df)

    # When
    lmdb_version_store.prune_previous_versions(symbol)

    # Then
    result = lmdb_version_store.read(symbol).data
    assert_frame_equal(result, df)
    assert len(lmdb_version_store.list_versions(symbol)) == 1
    assert len([ver for ver in lmdb_version_store.list_versions(symbol) if not ver["deleted"]]) == 1


def test_prune_previous_versions_no_snapshot(lmdb_version_store, symbol):
    # Given
    df = sample_dataframe()
    modified_df = sample_dataframe(1)

    lmdb_version_store.write(symbol, df, metadata={"something": "something"}, prune_previous_version=True)
    lmdb_version_store.write(symbol, modified_df, prune_previous_version=False)

    final_df = sample_dataframe(2)
    lmdb_version_store.write(symbol, final_df, prune_previous_version=False)

    # When
    lmdb_version_store.prune_previous_versions(symbol)

    # Then - only latest version should survive
    assert_frame_equal(lmdb_version_store.read(symbol).data, final_df)
    assert len([ver for ver in lmdb_version_store.list_versions() if not ver["deleted"]]) == 1


def test_prune_previous_versions_multiple_times(lmdb_version_store, symbol):
    # Given
    df = sample_dataframe()
    modified_df = sample_dataframe(1)

    lmdb_version_store.write(symbol, df, metadata={"something": "something"}, prune_previous_version=True)
    lmdb_version_store.write(symbol, modified_df, prune_previous_version=False)

    # When
    lmdb_version_store.prune_previous_versions(symbol)
    lmdb_version_store.prune_previous_versions(symbol)

    # Then - only latest version should survive
    assert_frame_equal(lmdb_version_store.read(symbol).data, modified_df)
    assert len([ver for ver in lmdb_version_store.list_versions() if not ver["deleted"]]) == 1

    # Let's write and prune again
    final_df = sample_dataframe(2)
    lmdb_version_store.write(symbol, final_df, prune_previous_version=False)
    lmdb_version_store.prune_previous_versions(symbol)
    assert_frame_equal(lmdb_version_store.read(symbol).data, final_df)
    assert len([ver for ver in lmdb_version_store.list_versions() if not ver["deleted"]]) == 1


def test_prune_previous_versions_write_batch(lmdb_version_store):
    """Verify that the batch write method correctly prunes previous versions when the corresponding option is specified.
    """
    # Given
    lib = lmdb_version_store
    lib_tool = lib.library_tool()
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    # When
    lib.batch_write([sym1, sym2], [df0, df0])
    lib.batch_write([sym1, sym2], [df1, df1], prune_previous_version=True)

    # Then - only latest version and keys should survive
    assert len(lib.list_versions(sym1)) == 1
    assert len(lib.list_versions(sym2)) == 1
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == 2
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 2

    # Then - we got 3 version keys per symbol: version 0, version 0 tombstone, version 1
    keys_for_sym1 = lib_tool.find_keys_for_id(KeyType.VERSION, sym1)
    keys_for_sym2 = lib_tool.find_keys_for_id(KeyType.VERSION, sym2)

    assert len(keys_for_sym1) == 3
    assert len(keys_for_sym2) == 3
    # Then - we got 4 symbol keys: 2 for each of the writes
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 4


def test_prune_previous_versions_batch_write_metadata(lmdb_version_store):
    """Verify that the batch write metadata method correctly prunes previous versions when the corresponding option is specified.
    """
    # Given
    lib = lmdb_version_store
    lib_tool = lib.library_tool()
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    meta0 = {"a": 0}
    meta1 = {"a": 1}

    # When
    lib.batch_write([sym1, sym2], [None, None], metadata_vector=[meta0, meta0])
    lib.batch_write_metadata([sym1, sym2], [meta1, meta1], prune_previous_version=True)

    # Then - only latest version and keys should survive
    assert len(lib.list_versions(sym1)) == 1
    assert len(lib.list_versions(sym2)) == 1
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == 2
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 2

    # Then - we got 3 version keys per symbol: version 0, version 0 tombstone, version 1
    keys_for_sym1 = lib_tool.find_keys_for_id(KeyType.VERSION, sym1)
    keys_for_sym2 = lib_tool.find_keys_for_id(KeyType.VERSION, sym2)

    assert len(keys_for_sym1) == 3
    assert len(keys_for_sym2) == 3
    # Then - we got 2 symbol keys: 1 for each of the writes
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 2


def test_prune_previous_versions_append_batch(lmdb_version_store):
    """Verify that the batch append method correctly prunes previous versions when the corresponding option is specified.
    """
    # Given
    lib = lmdb_version_store
    lib_tool = lib.library_tool()
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))

    # When
    lib.batch_write([sym1, sym2], [df0, df0])
    lib.batch_append([sym1, sym2], [df1, df1], prune_previous_version=True)

    # Then - only latest version and index keys should survive. Data keys remain the same
    assert len(lib.list_versions(sym1)) == 1
    assert len(lib.list_versions(sym2)) == 1
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == 2
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 4

    # Then - we got 3 version keys per symbol: version 0, version 0 tombstone, version 1
    keys_for_sym1 = lib_tool.find_keys_for_id(KeyType.VERSION, sym1)
    keys_for_sym2 = lib_tool.find_keys_for_id(KeyType.VERSION, sym2)

    assert len(keys_for_sym1) == 3
    assert len(keys_for_sym2) == 3
    # Then - we got 4 symbol keys: 2 for each of the writes
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 4


def test_deleting_unknown_symbol(lmdb_version_store, symbol):
    df = sample_dataframe()

    lmdb_version_store.write(symbol, df, metadata={"something": "something"})

    assert_frame_equal(lmdb_version_store.read(symbol).data, df)

    # Should not raise.
    lmdb_version_store.delete("does_not_exist")


def test_negative_cases(lmdb_version_store, symbol):
    df = sample_dataframe()
    # To stay consistent with arctic this doesn't throw.
    lmdb_version_store.delete("does_not_exist")

    # Creating a snapshot in an empty library should not create it.
    lmdb_version_store.snapshot("empty_snapshot")
    with pytest.raises(NoDataFoundException):
        lmdb_version_store.delete_snapshot("empty_snapshot")

    with pytest.raises(NoDataFoundException):
        lmdb_version_store.read("does_not_exist")
    with pytest.raises(NoDataFoundException):
        lmdb_version_store.read("does_not_exist", "empty_snapshots")

    with pytest.raises(NoDataFoundException):
        lmdb_version_store.delete_snapshot("does_not_exist")
    lmdb_version_store.write(symbol, df)
    lmdb_version_store.delete(symbol)


@pytest.mark.parametrize(
    "lib_type",
    [
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "lmdb_version_store_no_symbol_list",
        "s3_version_store_v1",
        "s3_version_store_v2",
    ],
)
def test_list_symbols_regex(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    lib.write("asdf", {"foo": "bar"}, metadata={"a": 1, "b": 10})
    lib.write("furble", {"foo": "bar"}, metadata={"a": 1, "b": 10})
    lib.snapshot("snap2")
    assert "asdf" in lib.list_symbols(regex="asd")
    assert "furble" not in lib.list_symbols(regex="asd")
    assert "asdf" in lib.list_symbols(snapshot="snap2", regex="asd")
    assert "furble" not in lib.list_symbols(snapshot="snap2", regex="asd")
    assert lib.read("asdf").data == {"foo": "bar"}
    assert list(sorted(lib.list_symbols())) == sorted(["asdf", "furble"])


def test_list_symbols_prefix(object_version_store):
    blahs = ["blah_asdf201901", "blah_asdf201802", "blah_asdf201803", "blah_asdf201903"]
    nahs = ["nah_asdf201801", "nah_asdf201802", "nah_asdf201803"]

    for sym in itertools.chain(blahs, nahs):
        object_version_store.write(sym, sample_dataframe(10))
    assert set(object_version_store.list_symbols(prefix="blah_")) == set(blahs)
    assert set(object_version_store.list_symbols(prefix="nah_")) == set(nahs)


def test_mixed_df_without_pickling_enabled(lmdb_version_store):
    mixed_type_df = pd.DataFrame({"a": [1, 2, "a"]})
    with pytest.raises(Exception):
        lmdb_version_store.write("sym", mixed_type_df)


def test_dataframe_fallback_with_pickling_enabled(lmdb_version_store_allows_pickling):
    mixed_type_df = pd.DataFrame({"a": [1, 2, "a", None]})
    lmdb_version_store_allows_pickling.write("sym", mixed_type_df)


def test_range_index(lmdb_version_store, sym):
    d1 = {
        "x": np.arange(10, 20, dtype=np.int64),
        "y": np.arange(20, 30, dtype=np.int64),
        "z": np.arange(30, 40, dtype=np.int64),
    }
    idx = pd.RangeIndex(-1, -11, -1)
    df = pd.DataFrame(d1, index=idx)
    lmdb_version_store.write(sym, df)

    vit = lmdb_version_store.read(sym)
    assert_frame_equal(df, vit.data)

    vit = lmdb_version_store.read(sym, columns=["y"])
    expected = pd.DataFrame({"y": d1["y"]}, index=idx)
    assert_frame_equal(expected, vit.data)


def test_date_range(lmdb_version_store):
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(100), index=pd.date_range(initial_timestamp, periods=100))
    sym = "date_test"
    lmdb_version_store.write(sym, df)
    start_offset = 2
    end_offset = 5
    query_start_ts = initial_timestamp + pd.DateOffset(start_offset)
    query_end_ts = initial_timestamp + pd.DateOffset(end_offset)

    # Should return everything from given start to end of the index
    data_start = lmdb_version_store.read(sym, date_range=(query_start_ts, None)).data
    assert query_start_ts == data_start.index[0]
    assert data_start[data_start.columns[0]][0] == start_offset

    # Should return everything from start of index to the given end.
    data_end = lmdb_version_store.read(sym, date_range=(None, query_end_ts)).data
    assert query_end_ts == data_end.index[-1]
    assert data_end[data_end.columns[0]][-1] == end_offset

    data_closed = lmdb_version_store.read(sym, date_range=(query_start_ts, query_end_ts)).data
    assert query_start_ts == data_closed.index[0]
    assert query_end_ts == data_closed.index[-1]
    assert data_closed[data_closed.columns[0]][0] == start_offset
    assert data_closed[data_closed.columns[0]][-1] == end_offset


def test_date_range_none(lmdb_version_store):
    sym = "date_test2"
    rows = 100
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(rows), index=pd.date_range(initial_timestamp, periods=100))
    lmdb_version_store.write(sym, df)
    # Should just return everything
    data = lmdb_version_store.read(sym, date_range=(None, None)).data
    assert len(data) == rows


def test_date_range_start_equals_end(lmdb_version_store):
    sym = "date_test2"
    rows = 100
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(rows), index=pd.date_range(initial_timestamp, periods=100))
    lmdb_version_store.write(sym, df)
    start_offset = 2
    query_start_ts = initial_timestamp + pd.DateOffset(start_offset)
    # Should just return everything
    data = lmdb_version_store.read(sym, date_range=(query_start_ts, query_start_ts)).data
    assert len(data) == 1
    assert data[data.columns[0]][0] == start_offset


def test_get_info(lmdb_version_store):
    sym = "get_info_test"
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    df.index.name = "dt_index"
    lmdb_version_store.write(sym, df)
    info = lmdb_version_store.get_info(sym)
    assert int(info["rows"]) == 10
    assert info["type"] == "pandasdf"
    assert info["col_names"]["columns"] == ["col1"]
    assert info["col_names"]["index"] == ["dt_index"]
    assert info["index_type"] == "index"


@pytest.mark.parametrize(
    "lib_type", ["lmdb_version_store_v1", "lmdb_version_store_v2", "s3_version_store_v1", "s3_version_store_v2"]
)
def test_get_info_version(request, lib_type):
    lib = request.getfixturevalue(lib_type)
    # given
    sym = "get_info_version_test"
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lib.write(sym, df)
    df = pd.DataFrame(data={"col1": np.arange(20)}, index=pd.date_range(pd.Timestamp(0), periods=20))
    lib.write(sym, df, prune_previous_version=False)

    # when
    info_0 = lib.get_info(sym, version=0)
    info_1 = lib.get_info(sym, version=1)
    latest_version = lib.get_info(sym)

    # then
    assert latest_version == info_1
    assert info_0["rows"] == 10
    assert info_1["rows"] == 20
    assert info_1["last_update"] > info_0["last_update"]


def test_get_info_date_range(lmdb_version_store):
    # given
    sym = "test_get_info_date_range"
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lmdb_version_store.write(sym, df)
    df = pd.DataFrame(data={"col1": np.arange(20)}, index=pd.date_range(pd.Timestamp(0), periods=20))
    lmdb_version_store.write(sym, df, prune_previous_version=False)

    # when
    info_0 = lmdb_version_store.get_info(sym, version=0)
    info_1 = lmdb_version_store.get_info(sym, version=1)
    latest_version = lmdb_version_store.get_info(sym)

    # then
    assert latest_version == info_1
    assert info_1["date_range"] == lmdb_version_store.get_timerange_for_symbol(sym, version=1)
    assert info_0["date_range"] == lmdb_version_store.get_timerange_for_symbol(sym, version=0)


def test_get_info_version_no_columns_nat(lmdb_version_store):
    sym = "test_get_info_version_no_columns_nat"
    column_names = ["a", "b", "c"]
    df = pd.DataFrame(columns=column_names)
    df["b"] = df["b"].astype("int64")
    lmdb_version_store.write(sym, df, dynamic_strings=True, coerce_columns={"a": float, "b": int, "c": str})
    info = lmdb_version_store.get_info(sym)
    assert np.isnat(info["date_range"][0]) == True
    assert np.isnat(info["date_range"][1]) == True


def test_get_info_version_empty_nat(lmdb_version_store):
    sym = "test_get_info_version_empty_nat"
    lmdb_version_store.write(sym, pd.DataFrame())
    info = lmdb_version_store.get_info(sym)
    assert np.isnat(info["date_range"][0]) == True
    assert np.isnat(info["date_range"][1]) == True


def test_update_times(lmdb_version_store):
    # given
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lmdb_version_store.write("sym_1", df)
    df = pd.DataFrame(data={"col1": np.arange(20)}, index=pd.date_range(pd.Timestamp(0), periods=20))
    lmdb_version_store.write("sym_1", df, prune_previous_version=False)
    df = pd.DataFrame(data={"col1": np.arange(15)}, index=pd.date_range(pd.Timestamp(0), periods=15))
    lmdb_version_store.write("sym_2", df)

    # when
    update_times_default = lmdb_version_store.update_times(["sym_1", "sym_2"])
    update_times_versioned = lmdb_version_store.update_times(["sym_1", "sym_1", "sym_2"], as_ofs=[0, 1, None])

    # then
    assert len(update_times_default) == 2
    assert update_times_default[0] < update_times_default[1]
    assert len(update_times_versioned) == 3
    assert update_times_versioned[0] < update_times_versioned[1] < update_times_versioned[2]


def test_get_info_multi_index(lmdb_version_store):
    dtidx = pd.date_range(pd.Timestamp("2016-01-01"), periods=3)
    vals = np.arange(3, dtype=np.uint32)
    multi_df = pd.DataFrame({"col1": [1, 4, 9]}, index=pd.MultiIndex.from_arrays([dtidx, vals]))
    sym = "multi_info_test"
    lmdb_version_store.write(sym, multi_df)
    info = lmdb_version_store.get_info(sym)
    assert int(info["rows"]) == 3
    assert info["type"] == "pandasdf"
    assert info["col_names"]["columns"] == ["col1"]
    assert len(info["col_names"]["index"]) == 2
    assert info["index_type"] == "multi_index"


def test_get_info_index_column(lmdb_version_store, sym):
    df = pd.DataFrame([[1, 2, 3, 4, 5, 6]], columns=["A", "B", "C", "D", "E", "F"])

    lmdb_version_store.write(sym, df)
    info = lmdb_version_store.get_info(sym)
    assert info["col_names"]["index"] == [None]
    assert info["col_names"]["columns"] == ["A", "B", "C", "D", "E", "F"]

    lmdb_version_store.write(sym, df.set_index("B"))
    info = lmdb_version_store.get_info(sym)
    assert info["col_names"]["index"] == ["B"]
    assert info["col_names"]["columns"] == ["A", "C", "D", "E", "F"]

    lmdb_version_store.write(sym, df.set_index(["A", "B"]))
    info = lmdb_version_store.get_info(sym)
    assert info["col_names"]["index"] == ["A", "B"]
    assert info["col_names"]["columns"] == ["C", "D", "E", "F"]

    lmdb_version_store.write(sym, df.set_index(["A", "B"], append=True))
    info = lmdb_version_store.get_info(sym)
    assert info["col_names"]["index"] == [None, "A", "B"]
    assert info["col_names"]["columns"] == ["C", "D", "E", "F"]


def test_empty_pd_series(lmdb_version_store):
    sym = "empty_s"
    series = pd.Series()
    lmdb_version_store.write(sym, series)
    assert lmdb_version_store.read(sym).data.empty


def test_empty_df(lmdb_version_store):
    sym = "empty_s"
    df = pd.DataFrame()
    lmdb_version_store.write(sym, df)
    # if no index information is provided, we assume a datetimeindex
    assert lmdb_version_store.read(sym).data.empty


def test_empty_ndarr(lmdb_version_store):
    sym = "empty_s"
    ndarr = np.array([])
    lmdb_version_store.write(sym, ndarr)
    assert_array_equal(lmdb_version_store.read(sym).data, ndarr)


# See AN-765 for why we need no_symbol_list fixture
def test_large_symbols(lmdb_version_store_no_symbol_list):
    with pytest.raises(ArcticNativeNotYetImplemented):
        lmdb_version_store_no_symbol_list.write("a" * (MAX_SYMBOL_SIZE + 1), 1)

    for _ in range(5):
        valid_sized_sym = "a" * random.randint(1, MAX_SYMBOL_SIZE - 1)
        lmdb_version_store_no_symbol_list.write(valid_sized_sym, 1)
        assert lmdb_version_store_no_symbol_list.read(valid_sized_sym).data == 1

        valid_punctuations = "".join(list(set(string.punctuation) - set(UNSUPPORTED_S3_CHARS)))
        valid_char_sym = "".join(
            [random.choice(string.ascii_letters + string.digits + valid_punctuations) for _ in range(12)]
        )

        lmdb_version_store_no_symbol_list.write(valid_char_sym, 1)
        assert lmdb_version_store_no_symbol_list.read(valid_char_sym).data == 1


def test_unsupported_chars_in_symbols(lmdb_version_store):
    for ch in UNSUPPORTED_S3_CHARS:
        with pytest.raises(ArcticNativeNotYetImplemented):
            lmdb_version_store.write(ch, 1)

    for _ in range(5):
        valid_punctuations = "".join(list(set(string.punctuation) - set(UNSUPPORTED_S3_CHARS)))
        valid_char_sym = "".join(
            [random.choice(string.ascii_letters + string.digits + valid_punctuations) for _ in range(12)]
        )

        lmdb_version_store.write(valid_char_sym, 1)
        assert lmdb_version_store.read(valid_char_sym).data == 1


def test_partial_read_pickled_df(lmdb_version_store):
    will_be_pickled = [1, 2, 3]
    lmdb_version_store.write("blah", will_be_pickled)
    assert lmdb_version_store.read("blah").data == will_be_pickled

    with pytest.raises(InternalException):
        lmdb_version_store.read("blah", columns=["does_not_matter"])

    with pytest.raises(InternalException):
        lmdb_version_store.read("blah", date_range=(DateRange(pd.Timestamp("1970-01-01"), pd.Timestamp("2027-12-31"))))


def test_is_pickled(lmdb_version_store):
    will_be_pickled = [1, 2, 3]
    lmdb_version_store.write("blah", will_be_pickled)
    assert lmdb_version_store.is_symbol_pickled("blah") is True

    df = pd.DataFrame({"a": np.arange(3)})
    lmdb_version_store.write("normal", df)
    assert lmdb_version_store.is_symbol_pickled("normal") is False


def test_is_pickled_by_version(lmdb_version_store):
    symbol = "test"
    will_be_pickled = [1, 2, 3]
    lmdb_version_store.write(symbol, will_be_pickled)

    not_pickled = pd.DataFrame({"a": np.arange(3)})
    lmdb_version_store.write(symbol, not_pickled)

    assert lmdb_version_store.is_symbol_pickled(symbol) is False
    assert lmdb_version_store.is_symbol_pickled(symbol, 0) is True
    assert lmdb_version_store.is_symbol_pickled(symbol, 1) is False


def test_is_pickled_by_snapshot(lmdb_version_store):
    symbol = "test"
    will_be_pickled = [1, 2, 3]
    snap1 = "snap1"
    lmdb_version_store.write(symbol, will_be_pickled)
    lmdb_version_store.snapshot(snap1)

    snap2 = "snap2"
    not_pickled = pd.DataFrame({"a": np.arange(3)})
    lmdb_version_store.write(symbol, not_pickled)
    lmdb_version_store.snapshot(snap2)

    assert lmdb_version_store.is_symbol_pickled(symbol) is False
    assert lmdb_version_store.is_symbol_pickled(symbol, snap1) is True
    assert lmdb_version_store.is_symbol_pickled(symbol, snap2) is False


def test_is_pickled_by_timestamp(lmdb_version_store):
    symbol = "test"
    will_be_pickled = [1, 2, 3]
    lmdb_version_store.write(symbol, will_be_pickled)
    time_after_first_write = pd.Timestamp.utcnow()
    time.sleep(0.1)

    not_pickled = pd.DataFrame({"a": np.arange(3)})
    lmdb_version_store.write(symbol, not_pickled)

    with pytest.raises(NoDataFoundException):
        lmdb_version_store.read(symbol, pd.Timestamp(0))
    assert lmdb_version_store.is_symbol_pickled(symbol) is False
    assert lmdb_version_store.is_symbol_pickled(symbol, time_after_first_write) is True
    assert lmdb_version_store.is_symbol_pickled(symbol, pd.Timestamp(np.iinfo(np.int64).max)) is False


@pytest.mark.parametrize("version_store", SMOKE_TEST_VERSION_STORES)
def test_list_versions(request, version_store):
    version_store = request.getfixturevalue(version_store)
    version_store.write("a", 1)  # a, v0
    version_store.write("b", 1)  # b, v0
    version_store.write("c", 1)  # c, v0
    version_store.write("a", 2)  # a, v1
    version_store.write("a", 1)  # a, v2
    version_store.snapshot("snap1")
    version_store.write("b", 3)  # b, v1
    version_store.snapshot("snap2")
    version_store.write("c", 3)  # c, v1
    version_store.snapshot("snap3")
    versions = version_store.list_versions()

    assert len(versions) == 3 + 2 + 2  # a-3, b-2, c-2
    sorted_versions_for_a = sorted([v for v in versions if v["symbol"] == "a"], key=lambda x: x["version"])
    assert len(sorted_versions_for_a) == 3
    assert sorted_versions_for_a[0]["snapshots"] == []
    assert set(sorted_versions_for_a[2]["snapshots"]) == {"snap1", "snap2", "snap3"}

    def get_tuples_from_version_info(v_infos):
        res = set()
        for v_info in v_infos:
            res.add((v_info["symbol"], v_info["version"]))
        return res

    assert get_tuples_from_version_info(version_store.list_versions(snapshot="snap1")) == {("a", 2), ("b", 0), ("c", 0)}
    assert get_tuples_from_version_info(version_store.list_versions(snapshot="snap2")) == {("a", 2), ("b", 1), ("c", 0)}
    assert get_tuples_from_version_info(version_store.list_versions(snapshot="snap3")) == {("a", 2), ("b", 1), ("c", 1)}


def test_list_versions_deleted_flag(lmdb_version_store):
    lmdb_version_store.write("symbol", pd.DataFrame(), metadata=1)
    lmdb_version_store.write("symbol", pd.DataFrame(), metadata=2, prune_previous_version=False)
    lmdb_version_store.write("symbol", pd.DataFrame(), metadata=3, prune_previous_version=False)
    lmdb_version_store.snapshot("snapshot")

    versions = lmdb_version_store.list_versions("symbol")
    assert len(versions) == 3
    versions = sorted(versions, key=lambda v: v["version"])
    assert not versions[2]["deleted"]
    assert versions[2]["snapshots"] == ["snapshot"]

    lmdb_version_store.delete_version("symbol", 2)
    versions = lmdb_version_store.list_versions("symbol")
    assert len(versions) == 3
    versions = sorted(versions, key=lambda v: v["version"])

    assert not versions[0]["deleted"]
    assert not versions[0]["snapshots"]
    assert not versions[1]["deleted"]
    assert not versions[1]["snapshots"]
    assert versions[2]["deleted"]
    assert versions[2]["snapshots"] == ["snapshot"]


def test_list_versions_with_snapshots(lmdb_version_store):
    lib = lmdb_version_store
    lib.write("a", 0)  # v0
    lib.write("b", 0)  # v0
    lib.snapshot("snap1")  # a_v0, b_v0
    lv1 = lib.list_versions()
    assert len(lv1) == 2
    assert lv1[0]["snapshots"] == ["snap1"]
    lib.write("a", 1)
    lib.write("b", 1)
    lib.write("c", 0)
    lib.snapshot("snap2")
    lib.snapshot("snap3")
    items_for_a = lib.list_versions("a")
    assert len(items_for_a) == 2
    # v0 in a single snapshot
    assert set([v["snapshots"] for v in items_for_a if v["version"] == 0][0]) == {"snap1"}
    # v1 in a two snapshots
    assert set([v["snapshots"] for v in items_for_a if v["version"] == 1][0]) == {"snap2", "snap3"}


def test_read_ts(lmdb_version_store):
    lmdb_version_store.write("a", 1)  # v0
    time.sleep(0.001)  # In case utcnow() has a lower precision and returning a timestamp before the write (#496)
    time_after_first_write = pd.Timestamp.utcnow()

    assert lmdb_version_store.read("a", as_of=time_after_first_write).version == 0
    time.sleep(0.1)
    lmdb_version_store.write("a", 2)  # v1
    time.sleep(0.11)

    lmdb_version_store.write("a", 3)  # v2
    time.sleep(0.1)
    lmdb_version_store.write("a", 4)  # v3
    lmdb_version_store.snapshot("snap3")
    versions = lmdb_version_store.list_versions()
    assert len(versions) == 4
    sorted_versions_for_a = sorted([v for v in versions if v["symbol"] == "a"], key=lambda x: x["version"])
    ts_for_v1 = sorted_versions_for_a[1]["date"]
    vitem = lmdb_version_store.read("a", as_of=ts_for_v1)
    assert vitem.version == 1
    assert vitem.data == 2

    ts_for_v2 = sorted_versions_for_a[0]["date"]
    vitem = lmdb_version_store.read("a", as_of=ts_for_v2)
    assert vitem.version == 0
    assert vitem.data == 1

    with pytest.raises(NoDataFoundException):
        lmdb_version_store.read("a", as_of=pd.Timestamp(0))

    brexit_almost_over = pd.Timestamp(np.iinfo(np.int64).max)  # Timestamp("2262-04-11 23:47:16.854775807")
    vitem = lmdb_version_store.read("a", as_of=brexit_almost_over)
    assert vitem.version == 3
    assert vitem.data == 4

    vitem = lmdb_version_store.read("a", as_of=time_after_first_write)
    assert vitem.version == 0
    assert vitem.data == 1


def test_negative_strides(version_store_factory):
    lmdb_version_store = version_store_factory(col_per_group=2, row_per_segment=2)
    negative_stride_np = np.array([[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11, 12]], np.int32)[::-1]
    lmdb_version_store.write("negative_strides", negative_stride_np)
    vit = lmdb_version_store.read("negative_strides")
    assert_array_equal(negative_stride_np, vit.data)
    negative_stride_df = pd.DataFrame(negative_stride_np)
    lmdb_version_store.write("negative_strides_df", negative_stride_df)
    vit2 = lmdb_version_store.read("negative_strides_df")
    assert_frame_equal(negative_stride_df, vit2.data)


def test_dynamic_strings(lmdb_version_store):
    row = pd.Series(["A", "B", "C", "Aaba", "Baca", "CABA", "dog", "cat"])
    df = pd.DataFrame({"x": row})
    lmdb_version_store.write("strings", df, dynamic_strings=True)
    vit = lmdb_version_store.read("strings")
    assert_frame_equal(vit.data, df)


def test_dynamic_strings_non_contigous(lmdb_version_store):
    df = sample_dataframe_only_strings(100, 0, 100)
    series = df.iloc[-1]
    series.name = None
    lmdb_version_store.write("strings", series, dynamic_strings=True)
    vit = lmdb_version_store.read("strings")
    assert_series_equal(vit.data, series)


def test_dynamic_strings_with_none(lmdb_version_store):
    row = pd.Series(
        [
            "A",
            "An_inordinately_long_string_for_no_sensible_purpose",
            None,
            "B",
            "C",
            None,
            "Baca",
            "CABA",
            None,
            "dog",
            "cat",
            None,
        ]
    )
    df = pd.DataFrame({"x": row})
    lmdb_version_store.write("strings", df, dynamic_strings=True)
    vit = lmdb_version_store.read("strings")
    assert_frame_equal(vit.data, df)


def test_dynamic_strings_with_none_first_element(lmdb_version_store):
    row = pd.Series(
        [
            None,
            "A",
            "An_inordinately_long_string_for_no_sensible_purpose",
            None,
            "B",
            "C",
            None,
            "Baca",
            "CABA",
            None,
            "dog",
            "cat",
            None,
        ]
    )
    df = pd.DataFrame({"x": row})
    lmdb_version_store.write("strings", df, dynamic_strings=True)
    vit = lmdb_version_store.read("strings")
    assert_frame_equal(vit.data, df)


def test_dynamic_strings_with_all_nones(lmdb_version_store):
    df = pd.DataFrame({"x": [None, None]})
    lmdb_version_store.write("strings", df, dynamic_strings=True)
    data = lmdb_version_store.read("strings")
    assert data.data["x"][0] is None
    assert data.data["x"][1] is None


def test_dynamic_strings_with_all_nones_update(lmdb_version_store):
    df = pd.DataFrame(
        {"col_1": ["a", "b"], "col_2": [0.1, 0.2]}, index=[pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")]
    )
    update_df = pd.DataFrame({"col_1": [np.nan], "col_2": [0.1]}, index=[pd.Timestamp("2022-01-01")])
    lmdb_version_store.write("strings", df, dynamic_strings=True)
    with pytest.raises(StreamDescriptorMismatch):
        # nan causes col_1 is considered to be a float column
        # Won't accept that as a string column even with DS enabled
        lmdb_version_store.update("strings", update_df, dynamic_strings=True)

    lmdb_version_store.update("strings", update_df.astype({"col_1": "object"}), dynamic_strings=True)

    data = lmdb_version_store.read("strings")
    assert math.isnan(data.data["col_1"][pd.Timestamp("2022-01-01")])
    assert data.data["col_1"][pd.Timestamp("2022-01-02")] == "b"

    lmdb_version_store.write("strings", df, dynamic_strings=True)
    lmdb_version_store.update(
        "strings", update_df, dynamic_strings=True, coerce_columns={"col_1": object, "col_2": "float"}
    )

    data = lmdb_version_store.read("strings")
    assert math.isnan(data.data["col_1"][pd.Timestamp("2022-01-01")])
    assert data.data["col_1"][pd.Timestamp("2022-01-02")] == "b"


def test_dynamic_strings_with_nan(lmdb_version_store):
    row = pd.Series(
        [
            np.nan,
            "A",
            "An_inordinately_long_string_for_no_sensible_purpose",
            np.nan,
            "B",
            "C",
            None,
            "Baca",
            "CABA",
            None,
            "dog",
            "cat",
            np.nan,
        ]
    )

    df = pd.DataFrame({"x": row})
    lmdb_version_store.write("strings", df, dynamic_strings=True)
    vit = lmdb_version_store.read("strings")
    assert_frame_equal(vit.data, df)


def test_metadata_with_snapshots(lmdb_version_store):
    symbol_metadata1 = {"test": "data_meta"}
    symbol_metadata2 = {"test": "should_not_be_returned"}
    snap_metadata = {"test": "snap_meta"}
    lmdb_version_store.write("symbol", 1, metadata=symbol_metadata1)
    lmdb_version_store.snapshot("snap1", metadata=snap_metadata)
    lmdb_version_store.write("symbol", 2, metadata=symbol_metadata2)

    meta = lmdb_version_store.read_metadata("symbol", as_of="snap1").metadata
    assert meta == symbol_metadata1
    snapshot = lmdb_version_store.list_snapshots()
    assert snapshot["snap1"] == snap_metadata


def equals(x, y):
    if isinstance(x, tuple) or isinstance(x, list):
        assert len(x) == len(y)
        for vx, vy in zip(x, y):
            equals(vx, vy)
    elif isinstance(x, dict):
        assert isinstance(y, dict)
        assert set(x.keys()) == set(y.keys())
        for k in x.keys():
            equals(x[k], y[k])
    elif isinstance(x, np.ndarray):
        assert isinstance(y, np.ndarray)
        assert np.allclose(x, y)
    else:
        assert x == y


def test_recursively_written_data(lmdb_version_store):
    samples = [
        {"a": np.arange(5), "b": np.arange(8)},  # dict of np arrays
        (np.arange(5), np.arange(6)),  # tuple of np arrays
        [np.arange(9), np.arange(12), (1, 2)],  # list of numpy arrays and a python tuple
        ({"a": np.arange(5), "b": [1, 2, 3]}),  # dict of np arrays and a python list
    ]

    for idx, sample in enumerate(samples):
        lmdb_version_store.write("sym_recursive" + str(idx), sample, recursive_normalizers=True)
        lmdb_version_store.write("sym_pickle" + str(idx), sample)  # pickled writes
        recursive_data = lmdb_version_store.read("sym_recursive" + str(idx)).data
        pickled_data = lmdb_version_store.read("sym_pickle" + str(idx)).data
        equals(sample, recursive_data)
        equals(pickled_data, recursive_data)


def test_recursively_written_data_with_metadata(lmdb_version_store):
    samples = [
        {"a": np.arange(5), "b": np.arange(8)},  # dict of np arrays
        (np.arange(5), np.arange(6)),  # tuple of np arrays
    ]

    for idx, sample in enumerate(samples):
        vit = lmdb_version_store.write(
            "sym_recursive" + str(idx), sample, metadata={"something": 1}, recursive_normalizers=True
        )
        recursive_data = lmdb_version_store.read("sym_recursive" + str(idx)).data
        equals(sample, recursive_data)
        assert vit.metadata == {"something": 1}


def test_recursively_written_data_with_nones(lmdb_version_store):
    sample = {"a": np.arange(5), "b": np.arange(8), "c": None}

    lmdb_version_store.write("sym_recursive", sample, recursive_normalizers=True)
    lmdb_version_store.write("sym_pickle", sample)  # pickled writes
    recursive_data = lmdb_version_store.read("sym_recursive").data
    pickled_data = lmdb_version_store.read("sym_recursive").data
    equals(sample, recursive_data)
    equals(pickled_data, recursive_data)


def test_recursive_nested_data(lmdb_version_store):
    sample_data = {"a": {"b": {"c": {"d": np.arange(24)}}}}
    fl = Flattener()
    assert fl.can_flatten(sample_data)
    assert fl.is_dict_like(sample_data)
    metast, to_write = fl.create_meta_structure(sample_data, "sym")
    assert len(to_write) == 1
    equals(list(to_write.values())[0], np.arange(24))

    lmdb_version_store.write("s", sample_data, recursive_normalizers=True)
    equals(lmdb_version_store.read("s").data, sample_data)


def test_named_tuple_flattening_rejected():
    fl = Flattener()
    SomeThing = namedtuple("SomeThing", "prop another_prop")
    nt = SomeThing(1, 2)
    assert fl.can_flatten(nt) is False


def test_data_directly_msgpackable(lmdb_version_store):
    data = {"a": [1, 2, 3], "b": {"c": 5}}
    fl = Flattener()
    meta, to_write = fl.create_meta_structure(data, "sym")
    assert len(to_write) == 0
    assert meta["leaf"] is True
    lmdb_version_store.write("s", data, recursive_normalizers=True)
    equals(lmdb_version_store.read("s").data, data)


class AlmostAList(list):
    pass


class AlmostAListNormalizer(CustomNormalizer):
    NESTED_STRUCTURE = True

    def normalize(self, item, **kwargs):
        if not isinstance(item, AlmostAList):
            return None
        return list(item), NormalizationMetadata.CustomNormalizerMeta()

    def denormalize(self, item, norm_meta):
        return AlmostAList(item)


def test_recursive_normalizer_with_custom_class():
    list_like_obj = AlmostAList([1, 2, 3])
    fl = Flattener()
    assert not fl.is_normalizable_to_nested_structure(list_like_obj)  # normalizer not registered yet

    register_normalizer(AlmostAListNormalizer())
    # Should be normalizable now.
    fl = Flattener()
    assert fl.is_normalizable_to_nested_structure(list_like_obj)


def test_really_large_symbol_for_recursive_data(lmdb_version_store):
    data = {"a" * 100: {"b" * 100: {"c" * 1000: {"d": np.arange(5)}}}}
    lmdb_version_store.write("s" * 100, data, recursive_normalizers=True)
    fl = Flattener()
    metastruct, to_write = fl.create_meta_structure(data, "s" * 100)
    assert len(list(to_write.keys())[0]) < fl.MAX_KEY_LENGTH
    equals(lmdb_version_store.read("s" * 100).data, data)


def test_nested_custom_types(lmdb_version_store):
    data = AlmostAList([1, 2, 3, AlmostAList([5, np.arange(6)])])
    fl = Flattener()
    meta, to_write = fl.create_meta_structure(data, "sym")
    equals(list(to_write.values())[0], np.arange(6))
    lmdb_version_store.write("sym", data, recursive_normalizers=True)
    got_back = lmdb_version_store.read("sym").data
    assert isinstance(got_back, AlmostAList)
    assert isinstance(got_back[3], AlmostAList)
    assert got_back[0] == 1


def test_batch_operations(object_version_store_prune_previous):
    multi_data = {"sym1": np.arange(8), "sym2": np.arange(9), "sym3": np.arange(10)}

    for _ in range(10):
        object_version_store_prune_previous.batch_write(list(multi_data.keys()), list(multi_data.values()))
        result = object_version_store_prune_previous.batch_read(list(multi_data.keys()))
        assert len(result) == 3
        equals(result["sym1"].data, np.arange(8))
        equals(result["sym2"].data, np.arange(9))
        equals(result["sym3"].data, np.arange(10))


def test_batch_read_tombstoned_version_via_snapshot(lmdb_version_store):  # AN-285
    lib = lmdb_version_store
    lib.write("a", 0)
    lib.snapshot("s")
    lib.write("a", 1, prune_previous_version=True)

    actual = lib.batch_read(["a"], as_ofs=[0])  # Other version query types are not implemented
    assert actual["a"].data == 0
    meta = lib.batch_read_metadata(["a"], as_ofs=[0])
    assert meta["a"].version == 0


def test_batch_write(lmdb_version_store_tombstone_and_sync_passive):
    lmdb_version_store = lmdb_version_store_tombstone_and_sync_passive
    multi_data = {"sym1": np.arange(8), "sym2": np.arange(9), "sym3": np.arange(10)}
    metadata = {"sym1": {"key1": "val1"}, "sym2": None, "sym3": None}

    sequential_results = {}
    for sym, data in multi_data.items():
        lmdb_version_store.write(sym, data, metadata=metadata[sym])
        vitem = lmdb_version_store.read(sym)
        sequential_results[vitem.symbol] = vitem
    lmdb_version_store.batch_write(
        list(multi_data.keys()), list(multi_data.values()), metadata_vector=(metadata[sym] for sym in multi_data)
    )
    batch_results = lmdb_version_store.batch_read(list(multi_data.keys()))
    assert len(batch_results) == len(sequential_results)
    equals(batch_results["sym1"].data, sequential_results["sym1"].data)
    equals(batch_results["sym1"].metadata, sequential_results["sym1"].metadata)
    equals(batch_results["sym2"].metadata, sequential_results["sym2"].metadata)
    assert sequential_results["sym1"].version == 0
    assert batch_results["sym1"].version == 1

    assert len(lmdb_version_store.list_versions()) == 6


def test_batch_write_then_read(lmdb_version_store):
    symbol = "sym_d_1"
    data = pd.Series(index=[0], data=[1])

    # Write, then delete a symbol
    lmdb_version_store.write(symbol=symbol, data=data)
    lmdb_version_store.delete(symbol)

    # Batch write the same data to the same symbol
    lmdb_version_store.batch_write(symbols=[symbol], data_vector=[data])
    lmdb_version_store.read(symbol)


@pytest.mark.parametrize("factory_name", ["version_store_factory", "s3_store_factory"])
def test_batch_write_then_list_symbol_without_cache(request, factory_name):
    factory = request.getfixturevalue(factory_name)
    lib = factory(symbol_list=False, segment_row_size=10)
    df = pd.DataFrame([1, 2, 3])
    for idx in range(10):
        lib.version_store.clear()
        symbols = [f"s{idx}-{i}" for i in [1, 2, 3]]
        lib.batch_write(symbols=symbols, data_vector=[df, df, df])
        assert set(lib.list_symbols()) == set(symbols)


def test_batch_roundtrip_metadata(lmdb_version_store_tombstone_and_sync_passive):
    lmdb_version_store = lmdb_version_store_tombstone_and_sync_passive
    metadatas = {}
    for x in range(10):
        symbol = "Sym_{}".format(x)
        metadatas[symbol] = {"a": x}

    for symbol in metadatas:
        lmdb_version_store.write(symbol, 12)

    symbols = []
    metas = []
    for sym, meta in metadatas.items():
        symbols.append(sym)
        metas.append(meta)

    write_result = lmdb_version_store.batch_write_metadata(symbols, metas)
    assert all(type(w) == VersionedItem for w in write_result)
    vits = lmdb_version_store.batch_read_metadata(symbols)

    for sym, returned in vits.items():
        assert returned.metadata == metadatas[sym]


def test_write_composite_data_with_user_meta(lmdb_version_store):
    multi_data = {"sym1": np.arange(8), "sym2": np.arange(9), "sym3": np.arange(10)}
    lmdb_version_store.write("sym", multi_data, metadata={"a": 1})
    vitem = lmdb_version_store.read("sym")
    assert vitem.metadata == {"a": 1}
    equals(vitem.data["sym1"], np.arange(8))


def test_force_delete(lmdb_version_store):
    df1 = sample_dataframe()
    lmdb_version_store.write("sym1", df1)
    df2 = sample_dataframe(seed=1)
    lmdb_version_store.write("sym1", df2)
    df3 = sample_dataframe(seed=2)
    lmdb_version_store.write("sym2", df3)
    df4 = sample_dataframe(seed=3)
    lmdb_version_store.write("sym2", df4)
    lmdb_version_store.version_store.force_delete_symbol("sym2")
    with pytest.raises(NoDataFoundException):
        lmdb_version_store.read("sym2")

    assert_frame_equal(lmdb_version_store.read("sym1").data, df2)
    assert_frame_equal(lmdb_version_store.read("sym1", as_of=0).data, df1)


def test_dataframe_with_NaN_in_timestamp_column(lmdb_version_store):
    normal_df = pd.DataFrame({"col": [pd.Timestamp("now"), pd.NaT]})
    lmdb_version_store.write("normal", normal_df)
    assert_frame_equal(normal_df, lmdb_version_store.read("normal").data)


def test_dataframe_with_nan_and_nat_in_timestamp_column(lmdb_version_store):
    df_with_NaN_mixed_in_ts = pd.DataFrame({"col": [pd.Timestamp("now"), pd.NaT, np.NaN]})
    lmdb_version_store.write("mixed_nan", df_with_NaN_mixed_in_ts)
    returned_df = lmdb_version_store.read("mixed_nan").data
    # NaN will now be converted to NaT
    isinstance(returned_df["col"][2], type(pd.NaT))


def test_dataframe_with_nan_and_nat_only(lmdb_version_store):
    df_with_nan_and_nat_only = pd.DataFrame({"col": [pd.NaT, pd.NaT, np.NaN]})  # Sample will be pd.NaT
    lmdb_version_store.write("nan_nat", df_with_nan_and_nat_only)
    assert_frame_equal(lmdb_version_store.read("nan_nat").data, pd.DataFrame({"col": [pd.NaT, pd.NaT, pd.NaT]}))


def test_coercion_to_float(lmdb_version_store_string_coercion):
    lib = lmdb_version_store_string_coercion
    df = pd.DataFrame({"col": [np.NaN, "1", np.NaN]})
    # col is now an Object column with all NaNs
    df["col"][1] = np.NaN

    assert df["col"].dtype == np.object_

    if sys.platform != "win32":  # SKIP_WIN Windows always uses dynamic strings
        with pytest.raises(ArcticNativeNotYetImplemented):
            # Needs pickling due to the obj column
            lib.write("test", df)

    lib.write("test", df, coerce_columns={"col": float})
    returned = lib.read("test").data
    # Should be a float now.
    assert returned["col"].dtype != np.object_


def test_coercion_to_str_with_dynamic_strings(lmdb_version_store_string_coercion):
    # assert that the getting sample function is not called
    lib = lmdb_version_store_string_coercion
    df = pd.DataFrame({"col": [None, None, "hello", "world"]})
    assert df["col"].dtype == np.object_

    if sys.platform != "win32":  # SKIP_WIN Windows always uses dynamic strings
        with pytest.raises(ArcticNativeNotYetImplemented):
            lib.write("sym", df)

    with mock.patch(
        "arcticdb.version_store._normalization.get_sample_from_non_empty_arr", return_value="hello"
    ) as sample_mock:
        # This should succeed but uses the slow path
        lib.write("sym", df, dynamic_strings=True)
        sample_mock.assert_called()

    with mock.patch("arcticdb.version_store._normalization.get_sample_from_non_empty_arr") as sample_mock:
        # This should skip the sample deduction
        lib.write("sym_coerced", df, dynamic_strings=True, coerce_columns={"col": str})
        sample_mock.assert_not_called()


def test_find_version(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    # def test_find_version(lmdb_version_store):
    #     lib = lmdb_version_store
    sym = "test_find_version"

    # Version 0 is alive and in a snapshot
    lib.write(sym, 0)
    lib.snapshot("snap_0")
    time_0 = datetime.utcnow()

    # Version 1 is only available in snap_1
    lib.write(sym, 1)
    lib.snapshot("snap_1")
    time_1 = datetime.utcnow()
    lib.delete_version(sym, 1)

    # Version 2 is fully deleted
    lib.write(sym, 2)
    time_2 = datetime.utcnow()
    lib.delete_version(sym, 2)

    # Version 3 is not in any snapshots
    lib.write(sym, 3)
    time_3 = datetime.utcnow()

    # Latest
    assert lib._find_version(sym).version == 3
    # By version number
    assert lib._find_version(sym, as_of=0).version == 0
    assert lib._find_version(sym, as_of=1).version == 1
    assert lib._find_version(sym, as_of=2) is None
    assert lib._find_version(sym, as_of=3).version == 3
    assert lib._find_version(sym, as_of=1000) is None
    # By negative version number
    assert lib._find_version(sym, as_of=-1).version == 3
    assert lib._find_version(sym, as_of=-2) is None
    assert lib._find_version(sym, as_of=-3).version == 1
    assert lib._find_version(sym, as_of=-4).version == 0
    assert lib._find_version(sym, as_of=-1000) is None
    # By snapshot
    assert lib._find_version(sym, as_of="snap_0").version == 0
    assert lib._find_version(sym, as_of="snap_1").version == 1
    with pytest.raises(NoDataFoundException):
        lib._find_version(sym, as_of="snap_1000")
    # By timestamp
    assert lib._find_version(sym, as_of=time_0).version == 0
    assert lib._find_version(sym, as_of=time_1).version == 0
    assert lib._find_version(sym, as_of=time_2).version == 0
    assert lib._find_version(sym, as_of=time_3).version == 3


def test_library_deletion_lmdb(lmdb_version_store):
    # lmdb uses fast deletion
    lmdb_version_store.write("a", 1)
    lmdb_version_store.write("b", 1)

    lmdb_version_store.snapshot("snap")
    assert len(lmdb_version_store.list_symbols()) == 2
    lmdb_version_store.version_store.clear()
    assert len(lmdb_version_store.list_symbols()) == 0
    lib_tool = lmdb_version_store.library_tool()
    assert lib_tool.count_keys(KeyType.VERSION) == 0
    assert lib_tool.count_keys(KeyType.TABLE_INDEX) == 0


def test_resolve_defaults(version_store_factory):
    lib = version_store_factory()
    proto_cfg = lib._lib_cfg.lib_desc.version.write_options
    assert lib.resolve_defaults("recursive_normalizers", proto_cfg, False) is False
    os.environ["recursive_normalizers"] = "True"
    assert lib.resolve_defaults("recursive_normalizers", proto_cfg, False, uppercase=False) is True

    lib2 = version_store_factory(dynamic_strings=True, reuse_name=True)
    proto_cfg = lib2._lib_cfg.lib_desc.version.write_options
    assert lib2.resolve_defaults("dynamic_strings", proto_cfg, False) is True
    del os.environ["recursive_normalizers"]


def test_batch_read_meta(lmdb_version_store_tombstone_and_sync_passive):
    lmdb_version_store = lmdb_version_store_tombstone_and_sync_passive
    lib = lmdb_version_store
    for idx in range(10):
        lib.write("sym" + str(idx), idx, metadata={"meta": idx})

    results_dict = lib.batch_read_metadata(["sym" + str(idx) for idx in range(10)])
    assert results_dict["sym1"].metadata == {"meta": 1}
    assert results_dict["sym5"].metadata == {"meta": 5}

    assert lib.read_metadata("sym6").metadata == results_dict["sym6"].metadata

    assert results_dict["sym2"].data is None


def test_batch_read_meta_with_missing(lmdb_version_store_tombstone_and_sync_passive):
    lmdb_version_store = lmdb_version_store_tombstone_and_sync_passive
    lib = lmdb_version_store
    for idx in range(10):
        lib.write("sym" + str(idx), idx, metadata={"meta": idx})

    results_dict = lib.batch_read_metadata(["sym1", "sym_doesnotexist", "sym2"])

    assert results_dict["sym1"].metadata == {"meta": 1}
    assert "sym_doesnotexist" not in results_dict


def test_list_versions_with_deleted_symbols(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
    lib.write("a", 1)
    lib.snapshot("snap")
    lib.write("a", 2)
    # At this point version 0 of 'a' is pruned but is still in the snapshot.
    versions = lib.list_versions()
    assert len(versions) == 2
    deleted = [v for v in versions if v["deleted"]]
    not_deleted = [v for v in versions if not v["deleted"]]
    assert len(deleted) == 1
    assert deleted[0]["symbol"] == "a"
    assert deleted[0]["version"] == 0

    assert not_deleted[0]["version"] == 1

    assert lib.read("a").data == 2


def test_read_with_asof_version_for_snapshotted_version(lmdb_version_store_tombstone_and_pruning):
    lib = lmdb_version_store_tombstone_and_pruning
    lib.write("a", 1)
    lib.snapshot("snap")
    lib.write("a", 2)
    lib.write("b", 1)
    lib.write("b", 2)

    with pytest.raises(Exception):
        # This raises as the first version of b was not in a snapshot and is now pruned
        lib.read("b", as_of=0)

    # Even though version 0 was pruned, it's still in a snapshot so this should work
    assert lib.read("a", as_of=0).data == 1


def test_get_tombstone_deletion_state_without_delayed_del(version_store_factory, sym):
    lib = version_store_factory(use_tombstones=True, delayed_deletes=False)
    lib.write(sym, 1)

    lib.write(sym, 2)

    lib.snapshot("snap")
    lib.write(sym, 3, prune_previous_version=True)
    tombstoned_version_map = lib.version_store._get_all_tombstoned_versions(sym)
    # v0 and v1
    assert len(tombstoned_version_map) == 2
    assert tombstoned_version_map[0] is False
    assert tombstoned_version_map[1] is True

    lib.write(sym, 3)
    lib.delete_version(sym, 2)
    tombstoned_version_map = lib.version_store._get_all_tombstoned_versions(sym)
    assert len(tombstoned_version_map) == 3
    assert tombstoned_version_map[2] is False


def test_get_timerange_for_symbol(lmdb_version_store, sym):
    lib = lmdb_version_store
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(100), index=pd.date_range(initial_timestamp, periods=100))
    lib.write(sym, df)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == datetime(2019, 1, 1)


def test_get_timerange_for_symbol_tz(lmdb_version_store, sym):
    lib = lmdb_version_store
    dt1 = timezone("US/Eastern").localize(datetime(2021, 4, 1))
    dt2 = timezone("US/Eastern").localize(datetime(2021, 4, 1, 3))
    dst = pd.DataFrame({"a": [0, 1]}, index=[dt1, dt2])
    lib.write(sym, dst)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == dt1
    assert maxts == dt2


def test_get_timerange_for_symbol_dst(lmdb_version_store, sym):
    lib = lmdb_version_store
    dst = pd.DataFrame({"a": [0, 1]}, index=[datetime(2021, 4, 1), datetime(2021, 4, 1, 3)])
    lib.write(sym, dst)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == datetime(2021, 4, 1)
    assert maxts == datetime(2021, 4, 1, 3)


def test_batch_read_meta_with_tombstones(lmdb_version_store_tombstone_and_sync_passive):
    lib = lmdb_version_store_tombstone_and_sync_passive
    lib.write("sym1", 1, {"meta1": 1})
    lib.write("sym1", 1, {"meta1": 2})
    lib.write("sym2", 1, {"meta2": 1})
    lib.write("sym2", 1, {"meta2": 2})
    lib.delete_version("sym1", 0)
    lib.delete_version("sym2", 0)
    results_dict = lib.batch_read_metadata(["sym1", "sym2", "sym3"])
    assert results_dict["sym1"].metadata == {"meta1": 2}
    assert results_dict["sym2"].metadata == {"meta2": 2}
    assert "sym3" not in results_dict
    assert lib.read_metadata("sym1").metadata == results_dict["sym1"].metadata


def test_batch_read_meta_with_pruning(version_store_factory):
    lib = version_store_factory(use_tombstones=True, prune_previous_version=True, sync_passive=True)
    lib.write("sym1", 1, {"meta1": 1})
    lib.write("sym1", 1, {"meta1": 2})
    lib.write("sym1", 3, {"meta1": 3})
    lib.write("sym2", 1, {"meta2": 1})
    lib.write("sym2", 1, {"meta2": 2})
    lib.write("sym2", 3, {"meta2": 3})
    # v0 and v1 should be pruned by now.
    results_dict = lib.batch_read_metadata(["sym1", "sym2", "sym3"])
    assert results_dict["sym1"].metadata == {"meta1": 3}
    assert results_dict["sym2"].metadata == {"meta2": 3}
    assert "sym3" not in results_dict
    assert lib.read_metadata("sym1").metadata == results_dict["sym1"].metadata


def test_batch_read_meta_multiple_versions(object_version_store):
    lib = object_version_store
    lib.write("sym1", 1, {"meta1": 1})
    lib.write("sym1", 2, {"meta1": 2})
    lib.write("sym1", 3, {"meta1": 3})

    lib.write("sym2", 1, {"meta2": 1})
    lib.write("sym2", 2, {"meta2": 2})
    lib.write("sym2", 3, {"meta2": 3})
    lib.write("sym2", 4, {"meta2": 4})

    lib.write("sym3", 1, {"meta3": 1})

    results_dict = lib.batch_read_metadata_multi(["sym1", "sym2", "sym1", "sym3", "sym2"], as_ofs=[1, 1, 0, 0, 3])
    assert results_dict["sym1"][1].metadata == {"meta1": 2}
    assert results_dict["sym2"][1].metadata == {"meta2": 2}
    assert results_dict["sym1"][0].metadata == {"meta1": 1}
    assert results_dict["sym3"][0].metadata == {"meta3": 1}
    assert results_dict["sym2"][3].metadata == {"meta2": 4}


def test_list_symbols(lmdb_version_store):
    lib = lmdb_version_store

    lib.write("a", 1)
    lib.write("b", 1)

    assert set(lib.list_symbols()) == set(lib.list_symbols(use_symbol_list=False))
    assert set(lib.list_symbols()) == set(lib.list_symbols(use_symbol_list=True))

    assert set(lib.list_symbols(regex="a")) == set(lib.list_symbols(regex="a", use_symbol_list=False))


def test_get_index(object_version_store):
    lib = object_version_store

    symbol = "thing"
    lib.write(symbol, 1)
    idx = lib.read_index(symbol)
    assert len(idx) == 1
    assert idx.iloc[0]["version_id"] == 0

    lib.write(symbol, 1, prune_previous_version=False)
    idx = lib.read_index(symbol)
    assert idx.iloc[0]["version_id"] == 1

    lib.snapshot("snap")
    lib.write(symbol, 3, prune_previous_version=False)
    idx = lib.read_index(symbol)
    assert idx.iloc[0]["version_id"] == 2

    idx = lib.read_index(symbol, as_of="snap")
    assert idx.iloc[0]["version_id"] == 1

    idx = lib.read_index(symbol, as_of=0)
    assert idx.iloc[0]["version_id"] == 0


def test_snapshot_empty_segment(lmdb_version_store):
    lib = lmdb_version_store

    lib.write("a", 1)
    lib.write("b", 1)

    lib.snapshot("snap")
    lib.delete("a")
    assert lib.read("a", as_of="snap").data == 1
    lib.write("c", 1)
    lib.snapshot("snap2", versions={})
    lib.delete("c")
    assert lib.has_symbol("c") is False


def test_columns_as_nparrary(lmdb_version_store, sym):
    lib = lmdb_version_store
    d = {"col1": [1, 2], "col2": [3, 4]}
    lib.write(sym, pd.DataFrame(data=d))

    assert all(lib.read(sym).data.columns == ["col1", "col2"])

    col = ["col2"]
    vit = lib.read(sym, columns=col)
    assert all(vit.data.columns == ["col2"])
    assert all(vit.data["col2"] == [3, 4])

    col = np.asarray(["col2"])
    vit = lib.read(sym, columns=col)

    assert all(vit.data.columns == ["col2"])
    assert all(vit.data["col2"] == [3, 4])


def test_simple_recursive_normalizer(object_version_store):
    object_version_store.write(
        "rec_norm", data={"a": np.arange(5), "b": np.arange(8), "c": None}, recursive_normalizers=True
    )


def test_dynamic_schema_similar_index_column(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.Series(dr, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_series_equal(returned, date_series)


def test_dynamic_schema_similar_index_column_dataframe(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.DataFrame({"date": np.arange(len(dr))}, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_frame_equal(returned, date_series)


def test_dynamic_schema_similar_index_column_dataframe_multiple_col(lmdb_version_store_dynamic_schema):
    lib = lmdb_version_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.DataFrame({"col": np.arange(len(dr)), "date": np.arange(len(dr))}, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_frame_equal(returned, date_series)


def test_restore_version(version_store_factory):
    lmdb_version_store = version_store_factory(col_per_group=2, row_per_segment=2)
    symbol = "test_restore_version"
    df1 = get_sample_dataframe(20, 4)
    df1.index = pd.DatetimeIndex([pd.Timestamp.now()] * len(df1))
    metadata = {"a": 43}
    lmdb_version_store.write(symbol, df1, metadata=metadata)
    df2 = get_sample_dataframe(20, 6)
    df2.index = df1.index + pd.Timedelta(hours=1)
    second_write_item = lmdb_version_store.write(symbol, df2, prune_previous_version=False)
    assert second_write_item.version == 1
    restore_item = lmdb_version_store.restore_version(symbol, as_of=0)
    assert restore_item.version == 2
    assert restore_item.metadata == metadata
    latest = lmdb_version_store.read(symbol)
    assert_frame_equal(latest.data, df1)
    assert latest.metadata == metadata


@pytest.mark.parametrize("ver", (3, "snap"))
def test_restore_version_not_found(lmdb_version_store, ver):
    lib: NativeVersionStore = lmdb_version_store
    lib.write("abc", 1)
    lib.write("abc", 2)
    lib.write("bcd", 9)
    lib.snapshot("snap", versions={"bcd": 0})
    with pytest.raises(NoSuchVersionException, match=r"\Wabc\W.*" + str(ver)):
        lib.restore_version("abc", ver)


def test_restore_version_latest_is_noop(lmdb_version_store):
    symbol = "test_restore_version"
    df1 = get_sample_dataframe(2, 4)
    df1.index = pd.DatetimeIndex([pd.Timestamp.now()] * len(df1))
    metadata = {"a": 43}
    lmdb_version_store.write(symbol, df1)
    df2 = get_sample_dataframe(2, 6)
    df2.index = df1.index + pd.Timedelta(hours=1)
    second_write_item = lmdb_version_store.write(symbol, df2, prune_previous_version=False, metadata=metadata)
    assert second_write_item.version == 1
    restore_item = lmdb_version_store.restore_version(symbol, as_of=1)
    assert restore_item.version == 1
    assert restore_item.metadata == metadata
    latest = lmdb_version_store.read(symbol)
    assert_frame_equal(latest.data, df2)
    assert latest.metadata == metadata
    assert latest.version == 1


def test_restore_version_ndarray(lmdb_version_store):
    symbol = "test_restore_version_ndarray"
    arr1 = np.array([0, 2, 4])
    metadata = {"b": 42}
    lmdb_version_store.write(symbol, arr1, metadata=metadata)
    arr2 = np.array([0, 4, 8])
    second_write_item = lmdb_version_store.write(symbol, arr2, prune_previous_version=False)
    assert second_write_item.version == 1
    restore_item = lmdb_version_store.restore_version(symbol, as_of=0)
    assert restore_item.version == 2
    assert restore_item.metadata == metadata
    latest = lmdb_version_store.read(symbol)
    assert_array_equal(latest.data, arr1)
    assert latest.metadata == metadata


def test_batch_restore_version(lmdb_version_store_tombstone):
    lmdb_version_store = lmdb_version_store_tombstone
    symbols = []
    for i in range(5):
        symbols.append("sym_{}".format(i))

    dfs = []
    for j in range(5):
        df = get_sample_dataframe(1000, j)
        df.index = pd.DatetimeIndex([pd.Timestamp.now()] * len(df))
        dfs.append(df)

    for x, symbol in enumerate(symbols):
        for y, df in enumerate(dfs):
            metadata = {"a": x + y}
            lmdb_version_store.write(symbol, df, metadata=metadata)

    versions = [z for z, _ in enumerate(symbols)]
    vits = lmdb_version_store.batch_restore_version(symbols, versions)
    for c, vit in enumerate(vits):
        expected_meta = {"a": c * 2}
        assert vit.metadata == expected_meta

    for d, symbol in enumerate(symbols):
        read_df = lmdb_version_store.read(symbol).data
        assert_frame_equal(read_df, dfs[d])


def test_batch_append(lmdb_version_store_tombstone, three_col_df):
    lmdb_version_store = lmdb_version_store_tombstone
    multi_data = {"sym1": three_col_df(), "sym2": three_col_df(1), "sym3": three_col_df(2)}
    metadata = {"sym1": {"key1": "val1"}, "sym2": None, "sym3": None}

    lmdb_version_store.batch_write(
        list(multi_data.keys()), list(multi_data.values()), metadata_vector=(metadata[sym] for sym in multi_data)
    )

    multi_append = {"sym1": three_col_df(10), "sym2": three_col_df(11), "sym3": three_col_df(12)}
    append_metadata = {"sym1": {"key1": "val2"}, "sym2": None, "sym3": "val3"}
    append_result = lmdb_version_store.batch_append(
        list(multi_append.keys()),
        list(multi_append.values()),
        metadata_vector=[append_metadata[sym] for sym in multi_append],
    )
    assert all(type(v) == VersionedItem for v in append_result)

    for sym in multi_data.keys():
        expected = pd.concat((multi_data[sym], multi_append[sym]))
        vit = lmdb_version_store.read(sym)
        assert_frame_equal(expected, vit.data)
        assert vit.metadata == append_metadata[sym]


def test_batch_read_date_range(lmdb_version_store_tombstone_and_sync_passive):
    lmdb_version_store = lmdb_version_store_tombstone_and_sync_passive
    symbols = []
    for i in range(5):
        symbols.append("sym_{}".format(i))

    base_date = pd.Timestamp("2019-06-01")

    dfs = []
    for j in range(5):
        df = get_sample_dataframe(1000, j)
        df.index = pd.date_range(base_date + pd.DateOffset(j), periods=len(df))
        dfs.append(df)

    for x, symbol in enumerate(symbols):
        lmdb_version_store.write(symbol, dfs[x])

    date_ranges = []
    for j in range(5):
        date_range = pd.date_range(base_date + pd.DateOffset(j + 100), periods=500)
        date_ranges.append(date_range)

    result_dict = lmdb_version_store.batch_read(symbols, date_ranges=date_ranges)
    for x, sym in enumerate(result_dict.keys()):
        vit = result_dict[sym]
        date_range = date_ranges[x]
        start = date_range[0]
        end = date_range[-1]
        assert_frame_equal(vit.data, dfs[x].loc[start:end])


def test_batch_read_columns(lmdb_version_store_tombstone_and_sync_passive):
    lmdb_version_store = lmdb_version_store_tombstone_and_sync_passive
    columns_of_interest = ["strings", "uint8"]
    number_of_requests = 5
    symbols = []
    for i in range(number_of_requests):
        symbols.append("sym_{}".format(i))

    base_date = pd.Timestamp("2019-06-01")
    dfs = []
    for j in range(number_of_requests):
        df = get_sample_dataframe(1000, j)
        df.index = pd.date_range(base_date + pd.DateOffset(j), periods=len(df))
        dfs.append(df)

    for x, symbol in enumerate(symbols):
        lmdb_version_store.write(symbol, dfs[x])

    result_dict = lmdb_version_store.batch_read(symbols, columns=[columns_of_interest] * number_of_requests)
    for x, sym in enumerate(result_dict.keys()):
        vit = result_dict[sym]
        assert_equal_value(vit.data, dfs[x][columns_of_interest])


def test_batch_read_symbol_doesnt_exist(lmdb_version_store):
    sym1 = "sym1"
    sym2 = "sym2"
    lmdb_version_store.write(sym1, 1)
    with pytest.raises(NoDataFoundException):
        _ = lmdb_version_store.batch_read([sym1, sym2])


def test_batch_read_version_doesnt_exist(lmdb_version_store):
    sym1 = "sym1"
    sym2 = "sym2"
    lmdb_version_store.write(sym1, 1)
    lmdb_version_store.write(sym2, 2)
    with pytest.raises(NoDataFoundException):
        _ = lmdb_version_store.batch_read([sym1, sym2], as_ofs=[0, 1])


def test_index_keys_start_end_index(lmdb_version_store, sym):
    idx = pd.date_range("2022-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": range(len(idx))}, index=idx)
    lmdb_version_store.write(sym, df)

    lt = lmdb_version_store.library_tool()
    key = lt.find_keys_for_id(KeyType.TABLE_INDEX, sym)[0]
    assert key.start_index == 1640995200000000000
    assert key.end_index == 1649548800000000001


def test_dynamic_schema_column_hash_update(lmdb_version_store_column_buckets):
    lib = lmdb_version_store_column_buckets
    idx = pd.date_range("2022-01-01", periods=10, freq="D")
    l = len(idx)
    df = pd.DataFrame(
        {"a": range(l), "b": range(1, l + 1), "c": range(2, l + 2), "d": range(3, l + 3), "e": range(4, l + 4)},
        index=idx,
    )

    lib.write("symbol", df)

    x = 3
    idx2 = pd.date_range("2022-01-03", periods=2, freq="D")
    l = len(idx2)
    df2 = pd.DataFrame(
        {
            "a": range(x, l + x),
            "b": range(1 + x, l + 1 + x),
            "c": range(2 + x, l + 2 + x),
            "d": range(3 + x, l + 3 + x),
            "e": range(4 + x, l + 4 + x),
        },
        index=idx2,
    )

    lib.update("symbol", df2)
    vit = lib.read("symbol")
    df.update(df2)
    assert_frame_equal(vit.data.astype("float"), df)


def test_dynamic_schema_column_hash_append(lmdb_version_store_column_buckets):
    lib = lmdb_version_store_column_buckets
    idx = pd.date_range("2022-01-01", periods=10, freq="D")
    l = len(idx)
    df = pd.DataFrame(
        {"a": range(l), "b": range(1, l + 1), "c": range(2, l + 2), "d": range(3, l + 3), "e": range(4, l + 4)},
        index=idx,
    )

    lib.write("symbol", df)

    x = 3
    idx2 = pd.date_range("2022-01-13", periods=2, freq="D")
    l = len(idx2)
    df2 = pd.DataFrame(
        {
            "a": range(x, l + x),
            "b": range(1 + x, l + 1 + x),
            "c": range(2 + x, l + 2 + x),
            "d": range(3 + x, l + 3 + x),
            "e": range(4 + x, l + 4 + x),
        },
        index=idx2,
    )

    lib.append("symbol", df2)
    vit = lib.read("symbol")
    new_df = pd.concat([df, df2])
    assert_frame_equal(vit.data, new_df)


def test_dynamic_schema_column_hash(lmdb_version_store_column_buckets):
    lib = lmdb_version_store_column_buckets
    idx = pd.date_range("2022-01-01", periods=10, freq="D")
    l = len(idx)
    df = pd.DataFrame(
        {"a": range(l), "b": range(1, l + 1), "c": range(2, l + 2), "d": range(3, l + 3), "e": range(4, l + 4)},
        index=idx,
    )

    lib.write("symbol", df)

    read_df = lib.read("symbol").data
    assert_frame_equal(df, read_df)

    read_df = lib.read("symbol", columns=["a", "b"]).data
    assert_frame_equal(df[["a", "b"]], read_df)

    read_df = lib.read("symbol", columns=["a", "e"]).data
    assert_frame_equal(df[["a", "e"]], read_df)

    read_df = lib.read("symbol", columns=["a", "c"]).data
    assert_frame_equal(df[["a", "c"]], read_df)


def test_list_versions_without_snapshots(lmdb_version_store):
    lib = lmdb_version_store
    lib.write("symbol_1", 0)
    lib.write("symbol_1", 1)
    lib.snapshot("snap_1")
    lib.write("symbol_1", 2)
    lib.write("symbol_1", 3)
    lib.snapshot("snap_2")
    lib.snapshot("snap_3")

    versions = lib.list_versions(symbol="symbol_1", skip_snapshots=False)
    assert len(versions[0]["snapshots"]) == 2
    assert len(versions[2]["snapshots"]) == 1
    versions = lib.list_versions(symbol="symbol_1", skip_snapshots=True)
    assert len(versions[0]["snapshots"]) == 0
    assert len(versions[2]["snapshots"]) == 0


@pytest.mark.parametrize("batch", (True, False))
@pytest.mark.parametrize("method", ("append", "update"))  # "write" is implied
def test_modification_methods_dont_return_input_data(lmdb_version_store, batch, method):  # AN-650
    lib: NativeVersionStore = lmdb_version_store

    def do(op, start, n):
        date_range = pd.date_range(start, periods=n)
        mk_data = lambda: pd.DataFrame({"col": [1] * n}, index=date_range)
        kwargs = dict(date_range=date_range) if op == "update" else {}
        if batch:
            out = getattr(lib, "batch_" + op)(["x", "y"], [mk_data(), mk_data()], **kwargs)
            assert all(getattr(i, "data", None) is None for i in out)
        else:
            out = getattr(lib, op)("x", mk_data(), **kwargs)
            assert out.data is None

    do("write", 0, 2)
    if method == "append":
        do("append", "1970-01-03", 1)
    else:
        try:
            do("update", 1, 1)
        except AttributeError as e:  # batch_update don't exist at the time of writing
            pytest.skip(str(e))


@pytest.mark.parametrize("method", ("append", "update"))
@pytest.mark.parametrize("num", (5, 50, 1001))
def test_diff_long_stream_descriptor_mismatch(lmdb_version_store, method, num):
    lib: NativeVersionStore = lmdb_version_store
    lib.write("x", pd.DataFrame({f"col{i}": [i, i + 1, i + 2] for i in range(num)}, index=pd.date_range(0, periods=3)))
    bad_row = {f"col{i}": ["a"] if i % 20 == 4 else [i] for i in (0, *range(3, num + 1))}
    try:
        if method == "append":
            lib.append("x", pd.DataFrame(bad_row, index=pd.date_range("1970-01-04", periods=1)))
        else:
            dr = pd.date_range("1970-01-02", periods=1)
            lib.update("x", pd.DataFrame(bad_row, index=dr), date_range=dr)
        assert False, "should throw"
    except StreamDescriptorMismatch as e:
        assert not isinstance(e, _ArcticLegacyCompatibilityException)
        msg = str(e)
        for i in (1, 2, *(x for x in range(num) if x % 20 == 4), num):
            assert f"FD<name=col{i}, type=TD<type=INT64, dim=0>" in msg
            if i % 20 == 4:
                assert f"FD<name=col{i}, type=TD<type=UTF" in msg


def test_get_non_existing_columns_in_series(lmdb_version_store, sym):
    lib = lmdb_version_store
    dst = pd.Series(index=pd.date_range(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-02-01")), data=0.0)
    lib.write(sym, dst)
    assert lmdb_version_store.read(sym, columns=["col1"]).data.empty


def test_get_existing_columns_in_series(lmdb_version_store, sym):
    lib = lmdb_version_store
    dst = pd.Series(index=pd.date_range(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-02-01")), data=0.0, name="col1")
    lib.write(sym, dst)
    assert not lmdb_version_store.read(sym, columns=["col1", "col2"]).data.empty
    if __name__ == "__main__":
        pytest.main()
