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
    ArcticDbNotYetImplemented,
    ArcticNativeNotYetImplemented,
    InternalException,
    UserInputException,
)
from arcticdb import QueryBuilder, ReadRequest
from arcticdb.flattener import Flattener
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store._custom_normalizers import CustomNormalizer, register_normalizer
import arcticdb.version_store._normalization
from arcticdb.version_store._store import VersionedItem
from arcticdb_ext.exceptions import _ArcticLegacyCompatibilityException, StorageException
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb_ext.version_store import NoSuchVersionException, StreamDescriptorMismatch, ManualClockVersionStore
from arcticc.pb2.descriptors_pb2 import NormalizationMetadata  # Importing from arcticdb dynamically loads arcticc.pb2
from arcticdb.util.test import (
    sample_dataframe,
    sample_dataframe_only_strings,
    get_sample_dataframe,
    assert_frame_equal,
    assert_series_equal,
    config_context,
    distinct_timestamps,
)
from tests.util.date import DateRange


@pytest.fixture()
def symbol():
    return "sym" + str(random.randint(0, 10000))


def assert_equal_value(data, expected):
    received = data.reindex(sorted(data.columns), axis=1)
    expected = expected.reindex(sorted(expected.columns), axis=1)
    assert_frame_equal(received, expected)


def assert_equal(received, expected):
    assert_frame_equal(received, expected)
    assert received.equals(expected)


def test_simple_flow(basic_store_no_symbol_list, symbol):
    df = sample_dataframe()
    modified_df = pd.DataFrame({"col": [1, 2, 3, 4]})
    basic_store_no_symbol_list.write(symbol, df, metadata={"blah": 1})
    assert basic_store_no_symbol_list.read(symbol).metadata == {"blah": 1}

    basic_store_no_symbol_list.write(symbol, modified_df)
    vitem = basic_store_no_symbol_list.read(symbol)
    assert_equal(vitem.data, modified_df)
    assert basic_store_no_symbol_list.list_symbols() == [symbol]

    basic_store_no_symbol_list.delete(symbol)
    assert basic_store_no_symbol_list.list_symbols() == basic_store_no_symbol_list.list_versions() == []


def test_special_chars(object_version_store):
    """Test chars with special URI encoding under RFC 3986"""
    errors = []
    # we are doing manual iteration due to a limitation that should be fixed by issue #1053
    for special_char in list("$@=;/:+ ,?\\{^}%`[]\"'~#|!-_.()"):
        try:
            sym = f"prefix{special_char}postfix"
            df = sample_dataframe()
            object_version_store.write(sym, df)
            vitem = object_version_store.read(sym)
            assert_equal(vitem.data, df)
        except AssertionError as e:
            errors.append(f"Failed for character {special_char}: {str(e)}")
    assert not errors, "errors occurred:\n" + "\n".join(errors)


@pytest.mark.parametrize("breaking_char", [chr(0), "\0", "*", "<", ">"])
def test_s3_breaking_chars(object_version_store, breaking_char):
    """Test that chars that are not supported are raising the appropriate exception and that we fail on write without
    corrupting the db.
    """
    sym = f"prefix{breaking_char}postfix"
    df = sample_dataframe()
    with pytest.raises(UserInputException):
        object_version_store.write(sym, df)

    assert sym not in object_version_store.list_symbols()


@pytest.mark.parametrize("prefix", ["", "prefix"])
@pytest.mark.parametrize("suffix", ["", "suffix"])
def test_symbol_names_with_all_chars(object_version_store, prefix, suffix):
    # Create symbol names with each character (except '\' because Azure replaces it with '/' in some cases)
    names = [f"{prefix}{chr(i)}{suffix}" for i in range(256) if chr(i) != "\\"]
    df = sample_dataframe()

    written_symbols = set()
    for name in names:
        try:
            object_version_store.write(name, df)
            written_symbols.add(name)
        # We should only fail with UserInputException (indicating that name validation failed)
        except UserInputException:
            pass

    assert set(object_version_store.list_symbols()) == written_symbols


@pytest.mark.parametrize("unhandled_char", [chr(0), chr(30), chr(127), chr(128)])
def test_unhandled_chars_default(object_version_store, unhandled_char):
    """Test that by default, the problematic chars are raising an exception"""
    sym = f"prefix{unhandled_char}postfix"
    df = sample_dataframe()
    with pytest.raises(UserInputException):
        object_version_store.write(sym, df)
    syms = object_version_store.list_symbols()
    assert sym not in syms


@pytest.mark.parametrize("unhandled_char", [chr(0), chr(30), chr(127), chr(128)])
def test_unhandled_chars_update_upsert(object_version_store, unhandled_char):
    df = pd.DataFrame(
        {"col_1": ["a", "b"], "col_2": [0.1, 0.2]}, index=[pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")]
    )
    sym = f"prefix{unhandled_char}postfix"
    with pytest.raises(UserInputException):
        object_version_store.update(sym, df, upsert=True)
    syms = object_version_store.list_symbols()
    assert sym not in syms


@pytest.mark.parametrize("unhandled_char", [chr(0), chr(30), chr(127), chr(128)])
def test_unhandled_chars_append(object_version_store, unhandled_char):
    df = pd.DataFrame(
        {"col_1": ["a", "b"], "col_2": [0.1, 0.2]}, index=[pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")]
    )
    sym = f"prefix{unhandled_char}postfix"
    with pytest.raises(UserInputException):
        object_version_store.append(sym, df)
    syms = object_version_store.list_symbols()
    assert sym not in syms


@pytest.mark.parametrize("unhandled_char", [chr(127), chr(128)])
def test_unhandled_chars_already_present_write(object_version_store, three_col_df, unhandled_char):
    sym = f"prefix{unhandled_char}postfix"
    with config_context("VersionStore.NoStrictSymbolCheck", 1):
        object_version_store.write(sym, three_col_df())
    vitem = object_version_store.read(sym)
    object_version_store.write(sym, three_col_df(1))
    new_vitem = object_version_store.read(sym)
    assert not vitem.data.equals(new_vitem.data)


@pytest.mark.parametrize("unhandled_char", [chr(127), chr(128)])
def test_unhandled_chars_already_present_append(object_version_store, three_col_df, unhandled_char):
    sym = f"prefix{unhandled_char}postfix"
    with config_context("VersionStore.NoStrictSymbolCheck", 1):
        object_version_store.write(sym, three_col_df(1))

    vitem = object_version_store.read(sym)
    object_version_store.append(sym, three_col_df(10))
    new_vitem = object_version_store.read(sym)
    assert not vitem.data.equals(new_vitem.data)
    assert len(vitem.data) != len(new_vitem.data)


@pytest.mark.parametrize("unhandled_char", [chr(127), chr(128)])
def test_unhandled_chars_already_present_update(object_version_store, unhandled_char):
    df = pd.DataFrame(
        {"col_1": ["a", "b"], "col_2": [0.1, 0.2]}, index=[pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")]
    )
    update_df = pd.DataFrame(
        {"col_1": ["c", "d"], "col_2": [0.2, 0.3]}, index=[pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")]
    )
    sym = f"prefix{unhandled_char}postfix"
    with config_context("VersionStore.NoStrictSymbolCheck", 1):
        object_version_store.write(sym, df)

    vitem = object_version_store.read(sym)
    object_version_store.update(sym, update_df)
    new_vitem = object_version_store.read(sym)
    assert not vitem.data.equals(new_vitem.data)
    assert len(vitem.data) == len(new_vitem.data)


# See AN-765 for why we need no_symbol_list fixture
def test_large_symbols(basic_store_no_symbol_list):
    # The following restrictions should be checked in the cpp layer's name_validation
    MAX_SYMBOL_SIZE = 254
    # TODO: Make too long name on LMDB raise a friendlier UserInputException (instead of InternalException [E_INVALID_ARGUMENT])
    with pytest.raises((UserInputException, InternalException)):
        basic_store_no_symbol_list.write("a" * (MAX_SYMBOL_SIZE + 1), 1)

    valid_sized_sym = "a" * MAX_SYMBOL_SIZE
    basic_store_no_symbol_list.write(valid_sized_sym, 1)
    assert basic_store_no_symbol_list.read(valid_sized_sym).data == 1


def test_empty_symbol_name_2(object_version_store):
    lib = object_version_store
    df = sample_dataframe()
    exceptions = []
    with pytest.raises(UserInputException) as e:
        lib.write("", df)
    exceptions.append(str(e.value).lower())
    with pytest.raises(UserInputException) as e:
        lib.append("", df, write_if_missing=True)
    exceptions.append(str(e.value).lower())
    with pytest.raises(UserInputException) as e:
        lib.update("", df, upsert=True)
    exceptions.append(str(e.value).lower())
    with pytest.raises(UserInputException) as e:
        lib.write_metadata("", {})
    exceptions.append(str(e.value).lower())
    with pytest.raises(UserInputException) as e:
        lib.batch_write([""], [df])
    exceptions.append(str(e.value).lower())
    with pytest.raises(UserInputException) as e:
        lib.batch_append([""], [df])
    exceptions.append(str(e.value).lower())
    with pytest.raises(UserInputException) as e:
        lib.batch_write_metadata([""], [{}])
    exceptions.append(str(e.value).lower())
    assert all("symbol" in e for e in exceptions)


@pytest.mark.parametrize(
    "method", ("write", "append", "update", "write_metadata", "batch_write", "batch_append", "batch_write_metadata")
)
def test_empty_symbol_name(lmdb_version_store_v1, method):
    first_arg = [""] if method.startswith("batch_") else ""
    df = sample_dataframe()
    second_arg = [df] if method.startswith("batch_") else df
    with pytest.raises(UserInputException) as e:
        getattr(lmdb_version_store_v1, method)(first_arg, second_arg)
    assert "symbol" in str(e.value).lower()


@pytest.mark.parametrize("method", ("snapshot", "delete_snapshot"))
def test_empty_snapshot_name(lmdb_version_store_v1, method):
    with pytest.raises(UserInputException) as e:
        getattr(lmdb_version_store_v1, method)("")
    assert "snapshot" in str(e.value).lower()


def test_with_prune(object_and_mem_and_lmdb_version_store, symbol):
    version_store = object_and_mem_and_lmdb_version_store
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
    assert_equal(version_store.read(symbol, as_of="my_snap").data, modified_df)
    assert_equal(version_store.read(symbol, as_of="my_snap2").data, final_df)


def test_prune_previous_versions_explicit_method(basic_store, symbol):
    # Given
    df = sample_dataframe()
    modified_df = sample_dataframe(1)

    basic_store.write(symbol, df, metadata={"something": "something"}, prune_previous_version=True)
    basic_store.write(symbol, modified_df, prune_previous_version=False)

    basic_store.snapshot("my_snap")

    final_df = sample_dataframe(2)
    basic_store.write(symbol, final_df, prune_previous_version=False)

    # When
    basic_store.prune_previous_versions(symbol)

    # Then - only latest version and snapshots should survive
    assert_equal(basic_store.read(symbol).data, final_df)
    assert len([ver for ver in basic_store.list_versions() if not ver["deleted"]]) == 1
    assert_equal(basic_store.read(symbol, as_of="my_snap").data, modified_df)


def test_prune_previous_versions_nothing_to_do(basic_store, symbol):
    df = sample_dataframe()
    basic_store.write(symbol, df)

    # When
    basic_store.prune_previous_versions(symbol)

    # Then
    result = basic_store.read(symbol).data
    assert_equal(result, df)
    assert len(basic_store.list_versions(symbol)) == 1
    assert len([ver for ver in basic_store.list_versions(symbol) if not ver["deleted"]]) == 1


def test_prune_previous_versions_no_snapshot(basic_store, symbol):
    # Given
    df = sample_dataframe()
    modified_df = sample_dataframe(1)

    basic_store.write(symbol, df, metadata={"something": "something"}, prune_previous_version=True)
    basic_store.write(symbol, modified_df, prune_previous_version=False)

    final_df = sample_dataframe(2)
    basic_store.write(symbol, final_df, prune_previous_version=False)

    # When
    basic_store.prune_previous_versions(symbol)

    # Then - only latest version should survive
    assert_equal(basic_store.read(symbol).data, final_df)
    assert len([ver for ver in basic_store.list_versions() if not ver["deleted"]]) == 1


def test_prune_previous_versions_multiple_times(basic_store, symbol):
    # Given
    df = sample_dataframe()
    modified_df = sample_dataframe(1)

    basic_store.write(symbol, df, metadata={"something": "something"}, prune_previous_version=True)
    basic_store.write(symbol, modified_df, prune_previous_version=False)

    # When
    basic_store.prune_previous_versions(symbol)
    basic_store.prune_previous_versions(symbol)

    # Then - only latest version should survive
    assert_equal(basic_store.read(symbol).data, modified_df)
    assert len([ver for ver in basic_store.list_versions() if not ver["deleted"]]) == 1

    # Let's write and prune again
    final_df = sample_dataframe(2)
    basic_store.write(symbol, final_df, prune_previous_version=False)
    basic_store.prune_previous_versions(symbol)
    assert_equal(basic_store.read(symbol).data, final_df)
    assert len([ver for ver in basic_store.list_versions() if not ver["deleted"]]) == 1


def test_prune_previous_versions_write_batch(basic_store):
    """Verify that the batch write method correctly prunes previous versions when the corresponding option is specified."""
    # Given
    lib = basic_store
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


def test_prune_previous_versions_batch_write_metadata(basic_store):
    """Verify that the batch write metadata method correctly prunes previous versions when the corresponding option is specified."""
    # Given
    lib = basic_store
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


def test_prune_previous_versions_append_batch(basic_store):
    """Verify that the batch append method correctly prunes previous versions when the corresponding option is specified."""
    # Given
    lib = basic_store
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


def test_batch_append_unicode(basic_store):
    symbol = "test_append_unicode"
    uc = "\u0420\u043e\u0441\u0441\u0438\u044f"

    df1 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03")],
        data={"a": ["123", uc]},
    )
    basic_store.batch_write(symbols=[symbol], data_vector=[df1])
    vit = basic_store.batch_read([symbol])[symbol]
    assert_equal(vit.data, df1)

    df2 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-04"), pd.Timestamp("2018-01-05")],
        data={"a": ["123", uc]},
    )
    basic_store.batch_append(symbols=[symbol], data_vector=[df2])
    vit = basic_store.batch_read([symbol])[symbol]
    expected = pd.concat([df1, df2])
    assert_equal(vit.data, expected)


def test_batch_write_metadata_unicode(basic_store):
    symbol = "test_append_unicode"
    uc = "\u0420\u043e\u0441\u0441\u0438\u044f"
    df1 = pd.DataFrame(
        index=[pd.Timestamp("2018-01-02"), pd.Timestamp("2018-01-03")],
        data={"a": ["123", uc]},
    )

    basic_store.batch_write(symbols=[symbol], data_vector=[df1])
    vit = basic_store.batch_read([symbol])[symbol]
    assert_equal(vit.data, df1)

    meta = {"a": 1, "b": uc}
    basic_store.batch_write_metadata(symbols=[symbol], metadata_vector=[meta])
    vits = basic_store.batch_read_metadata([symbol])
    metadata = vits[symbol].metadata
    assert metadata == meta


def test_deleting_unknown_symbol(basic_store, symbol):
    df = sample_dataframe()

    basic_store.write(symbol, df, metadata={"something": "something"})

    assert_equal(basic_store.read(symbol).data, df)

    # Should not raise.
    basic_store.delete("does_not_exist")


def test_negative_cases(basic_store, symbol):
    df = sample_dataframe()
    # To stay consistent with arctic this doesn't throw.
    basic_store.delete("does_not_exist")

    with pytest.raises(NoSuchVersionException):
        basic_store.snapshot("empty_snapshot")
    with pytest.raises(NoSuchVersionException):
        basic_store.snapshot("empty_snapshot", versions={"non-exist-symbol": 0})
    with pytest.raises(NoDataFoundException):
        basic_store.delete_snapshot("empty_snapshot")

    with pytest.raises(NoDataFoundException):
        basic_store.read("does_not_exist")
    with pytest.raises(NoDataFoundException):
        basic_store.read("does_not_exist", "empty_snapshots")

    with pytest.raises(NoDataFoundException):
        basic_store.delete_snapshot("does_not_exist")
    basic_store.write(symbol, df)
    basic_store.delete(symbol)


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


def test_mixed_df_without_pickling_enabled(basic_store):
    mixed_type_df = pd.DataFrame({"a": [1, 2, "a"]})
    with pytest.raises(Exception):
        basic_store.write("sym", mixed_type_df)


def test_dataframe_fallback_with_pickling_enabled(basic_store_allows_pickling):
    mixed_type_df = pd.DataFrame({"a": [1, 2, "a", None]})
    basic_store_allows_pickling.write("sym", mixed_type_df)


def test_range_index(basic_store, sym):
    d1 = {
        "x": np.arange(10, 20, dtype=np.int64),
        "y": np.arange(20, 30, dtype=np.int64),
        "z": np.arange(30, 40, dtype=np.int64),
    }
    idx = pd.RangeIndex(-1, -11, -1)
    df = pd.DataFrame(d1, index=idx)
    basic_store.write(sym, df)

    vit = basic_store.read(sym)
    assert_equal(df, vit.data)

    vit = basic_store.read(sym, columns=["y"])
    expected = pd.DataFrame({"y": d1["y"]}, index=idx)
    assert_equal(expected, vit.data)


@pytest.mark.pipeline
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_date_range(basic_store, use_date_range_clause):
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(100), index=pd.date_range(initial_timestamp, periods=100))
    sym = "date_test"
    basic_store.write(sym, df)
    start_offset = 2
    end_offset = 5

    query_start_ts = initial_timestamp + pd.DateOffset(start_offset)
    query_end_ts = initial_timestamp + pd.DateOffset(end_offset)

    # Should return everything from given start to end of the index
    date_range = (query_start_ts, None)
    if use_date_range_clause:
        q = QueryBuilder()
        q = q.date_range(date_range)
        data_start = basic_store.read(sym, query_builder=q).data
    else:
        data_start = basic_store.read(sym, date_range=date_range).data
    assert query_start_ts == data_start.index[0]
    assert data_start[data_start.columns[0]][0] == start_offset

    # Should return everything from start of index to the given end.
    date_range = (None, query_end_ts)
    if use_date_range_clause:
        q = QueryBuilder()
        q = q.date_range(date_range)
        data_end = basic_store.read(sym, query_builder=q).data
    else:
        data_end = basic_store.read(sym, date_range=date_range).data
    assert query_end_ts == data_end.index[-1]
    assert data_end[data_end.columns[0]][-1] == end_offset

    date_range = (query_start_ts, query_end_ts)
    if use_date_range_clause:
        q = QueryBuilder()
        q = q.date_range(date_range)
        data_closed = basic_store.read(sym, query_builder=q).data
    else:
        data_closed = basic_store.read(sym, date_range=date_range).data
    assert query_start_ts == data_closed.index[0]
    assert query_end_ts == data_closed.index[-1]
    assert data_closed[data_closed.columns[0]][0] == start_offset
    assert data_closed[data_closed.columns[0]][-1] == end_offset


@pytest.mark.pipeline
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_date_range_none(basic_store, use_date_range_clause):
    sym = "date_test2"
    rows = 100
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(rows), index=pd.date_range(initial_timestamp, periods=100))
    basic_store.write(sym, df)
    date_range = (None, None)
    # Should just return everything
    if use_date_range_clause:
        q = QueryBuilder()
        q = q.date_range(date_range)
        data = basic_store.read(sym, query_builder=q).data
    else:
        data = basic_store.read(sym, date_range=(None, None)).data
    assert len(data) == rows


@pytest.mark.pipeline
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_date_range_start_equals_end(basic_store, use_date_range_clause):
    sym = "date_test2"
    rows = 100
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(rows), index=pd.date_range(initial_timestamp, periods=100))
    basic_store.write(sym, df)
    start_offset = 2
    query_start_ts = initial_timestamp + pd.DateOffset(start_offset)
    date_range = (query_start_ts, query_start_ts)
    # Should just return everything
    if use_date_range_clause:
        q = QueryBuilder()
        q = q.date_range(date_range)
        data = basic_store.read(sym, query_builder=q).data
    else:
        data = basic_store.read(sym, date_range=date_range).data
    assert len(data) == 1
    assert data[data.columns[0]][0] == start_offset


@pytest.mark.pipeline
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_date_range_row_sliced(basic_store_tiny_segment, use_date_range_clause):
    lib = basic_store_tiny_segment
    sym = "test_date_range_row_sliced"
    # basic_store_tiny_segment produces 2x2 segments
    num_rows = 6
    index = pd.date_range("2000-01-01", periods=num_rows, freq="D")
    df = pd.DataFrame({"col": np.arange(num_rows)}, index=index)
    lib.write(sym, df)

    expected = df.iloc[1:-1]

    date_range = (index[1], index[-2])
    if use_date_range_clause:
        q = QueryBuilder()
        q = q.date_range(date_range)
        received = lib.read(sym, query_builder=q).data
    else:
        received = lib.read(sym, date_range=date_range).data
    assert_equal(expected, received)

    date_range = (index[0] + pd.Timedelta(12, unit="h"), index[-1] - pd.Timedelta(12, unit="h"))
    if use_date_range_clause:
        received = lib.read(sym, query_builder=q).data
    else:
        received = lib.read(sym, date_range=date_range).data
    assert_equal(expected, received)


def test_get_info(basic_store):
    sym = "get_info_test"
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    df.index.name = "dt_index"
    basic_store.write(sym, df)
    info = basic_store.get_info(sym)
    assert int(info["rows"]) == 10
    assert info["type"] == "pandasdf"
    assert info["col_names"]["columns"] == ["col1"]
    assert info["col_names"]["index"] == ["dt_index"]
    assert info["index_type"] == "index"


def test_get_info_version(object_and_mem_and_lmdb_version_store):
    lib = object_and_mem_and_lmdb_version_store
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


def test_get_info_date_range(basic_store):
    # given
    sym = "test_get_info_date_range"
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    basic_store.write(sym, df)
    df = pd.DataFrame(data={"col1": np.arange(20)}, index=pd.date_range(pd.Timestamp(0), periods=20))
    basic_store.write(sym, df, prune_previous_version=False)

    # when
    info_0 = basic_store.get_info(sym, version=0)
    info_1 = basic_store.get_info(sym, version=1)
    latest_version = basic_store.get_info(sym)

    # then
    assert latest_version == info_1
    assert info_1["date_range"] == basic_store.get_timerange_for_symbol(sym, version=1)
    assert info_0["date_range"] == basic_store.get_timerange_for_symbol(sym, version=0)


def test_get_info_version_no_columns_nat(basic_store):
    sym = "test_get_info_version_no_columns_nat"
    column_names = ["a", "b", "c"]
    df = pd.DataFrame(columns=column_names)
    df["b"] = df["b"].astype("int64")
    basic_store.write(sym, df, dynamic_strings=True, coerce_columns={"a": float, "b": int, "c": str})
    info = basic_store.get_info(sym)
    assert np.isnat(info["date_range"][0])
    assert np.isnat(info["date_range"][1])


def test_get_info_version_empty_nat(basic_store):
    sym = "test_get_info_version_empty_nat"
    basic_store.write(sym, pd.DataFrame())
    info = basic_store.get_info(sym)
    assert np.isnat(info["date_range"][0])
    assert np.isnat(info["date_range"][1])


def test_get_info_non_timestamp_index_date_range(basic_store):
    lib = basic_store
    sym = "test_get_info_non_timestamp_index_date_range"
    # Row-range indexed
    lib.write(sym, pd.DataFrame({"col": [1, 2, 3]}))
    info = lib.get_info(sym)
    assert np.isnat(info["date_range"][0])
    assert np.isnat(info["date_range"][1])
    # int64 indexed
    lib.write(sym, pd.DataFrame({"col": [1, 2, 3]}), index=pd.Index([4, 5, 6], dtype=np.int64))
    info = lib.get_info(sym)
    assert np.isnat(info["date_range"][0])
    assert np.isnat(info["date_range"][1])


def test_get_info_unsorted_timestamp_index_date_range(basic_store):
    lib = basic_store
    sym = "test_get_info_unsorted_timestamp_index_date_range"
    lib.write(
        sym,
        pd.DataFrame(
            {"col": [1, 2, 3]},
            index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-02")],
        ),
    )
    info = lib.get_info(sym)
    assert np.isnat(info["date_range"][0])
    assert np.isnat(info["date_range"][1])


def test_get_info_pickled(basic_store):
    lib = basic_store
    sym = "test_get_info_pickled"
    lib.write(sym, 1)
    info = lib.get_info(sym)
    assert info["col_names"]["columns"] == ["bytes"]
    assert info["input_type"] == "msg_pack_frame"
    assert np.isnat(info["date_range"][0]) and np.isnat(info["date_range"][1])
    assert info["sorted"] == "UNKNOWN"
    assert info["rows"] is None


def test_batch_get_info_pickled(basic_store):
    lib = basic_store
    sym = "test_get_info_pickled"
    lib.write(sym, 1)
    info = lib.batch_get_info([sym])[0]
    assert info["col_names"]["columns"] == ["bytes"]
    assert info["input_type"] == "msg_pack_frame"
    assert np.isnat(info["date_range"][0]) and np.isnat(info["date_range"][1])
    assert info["sorted"] == "UNKNOWN"
    assert info["rows"] is None


def test_update_times(basic_store):
    # given
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    basic_store.write("sym_1", df)
    df = pd.DataFrame(data={"col1": np.arange(20)}, index=pd.date_range(pd.Timestamp(0), periods=20))
    basic_store.write("sym_1", df, prune_previous_version=False)
    df = pd.DataFrame(data={"col1": np.arange(15)}, index=pd.date_range(pd.Timestamp(0), periods=15))
    basic_store.write("sym_2", df)

    # when
    update_times_default = basic_store.update_times(["sym_1", "sym_2"])
    update_times_versioned = basic_store.update_times(["sym_1", "sym_1", "sym_2"], as_ofs=[0, 1, None])

    # then
    assert len(update_times_default) == 2
    assert update_times_default[0] < update_times_default[1]
    assert len(update_times_versioned) == 3
    assert update_times_versioned[0] < update_times_versioned[1] < update_times_versioned[2]


def test_get_info_multi_index(basic_store):
    dtidx = pd.date_range(pd.Timestamp("2016-01-01"), periods=3)
    vals = np.arange(3, dtype=np.uint32)
    multi_df = pd.DataFrame({"col1": [1, 4, 9]}, index=pd.MultiIndex.from_arrays([dtidx, vals]))
    sym = "multi_info_test"
    basic_store.write(sym, multi_df)
    info = basic_store.get_info(sym)
    assert int(info["rows"]) == 3
    assert info["type"] == "pandasdf"
    assert info["col_names"]["columns"] == ["col1"]
    assert len(info["col_names"]["index"]) == 2
    assert info["index_type"] == "multi_index"


def test_get_info_index_column(basic_store, sym):
    df = pd.DataFrame([[1, 2, 3, 4, 5, 6]], columns=["A", "B", "C", "D", "E", "F"])

    basic_store.write(sym, df)
    info = basic_store.get_info(sym)
    assert info["col_names"]["index"] == [None]
    assert info["col_names"]["columns"] == ["A", "B", "C", "D", "E", "F"]

    basic_store.write(sym, df.set_index("B"))
    info = basic_store.get_info(sym)
    assert info["col_names"]["index"] == ["B"]
    assert info["col_names"]["columns"] == ["A", "C", "D", "E", "F"]

    basic_store.write(sym, df.set_index(["A", "B"]))
    info = basic_store.get_info(sym)
    assert info["col_names"]["index"] == ["A", "B"]
    assert info["col_names"]["columns"] == ["C", "D", "E", "F"]

    basic_store.write(sym, df.set_index(["A", "B"], append=True))
    info = basic_store.get_info(sym)
    assert info["col_names"]["index"] == [None, "A", "B"]
    assert info["col_names"]["columns"] == ["C", "D", "E", "F"]


def test_empty_pd_series(basic_store):
    sym = "empty_s"
    series = pd.Series()
    basic_store.write(sym, series)
    assert basic_store.read(sym).data.empty
    # basic_store.update(sym, series)
    # assert basic_store.read(sym).data.empty
    basic_store.append(sym, series)
    assert basic_store.read(sym).data.empty


def test_empty_df(basic_store):
    sym = "empty_s"
    df = pd.DataFrame()
    basic_store.write(sym, df)
    # if no index information is provided, we assume a datetimeindex
    assert basic_store.read(sym).data.empty
    basic_store.update(sym, df)
    assert basic_store.read(sym).data.empty
    basic_store.append(sym, df)
    assert basic_store.read(sym).data.empty


def test_empty_ndarr(basic_store):
    sym = "empty_s"
    ndarr = np.array([])
    basic_store.write(sym, ndarr)
    assert_array_equal(basic_store.read(sym).data, ndarr)


def test_partial_read_pickled_df(basic_store):
    will_be_pickled = [1, 2, 3]
    basic_store.write("blah", will_be_pickled)
    assert basic_store.read("blah").data == will_be_pickled

    with pytest.raises(InternalException):
        basic_store.read("blah", columns=["does_not_matter"])

    with pytest.raises(InternalException):
        basic_store.read("blah", date_range=(DateRange(pd.Timestamp("1970-01-01"), pd.Timestamp("2027-12-31"))))


def test_is_pickled(basic_store):
    will_be_pickled = [1, 2, 3]
    basic_store.write("blah", will_be_pickled)
    assert basic_store.is_symbol_pickled("blah") is True

    df = pd.DataFrame({"a": np.arange(3)})
    basic_store.write("normal", df)
    assert basic_store.is_symbol_pickled("normal") is False


def test_is_pickled_by_version(basic_store):
    symbol = "test"
    will_be_pickled = [1, 2, 3]
    basic_store.write(symbol, will_be_pickled)

    not_pickled = pd.DataFrame({"a": np.arange(3)})
    basic_store.write(symbol, not_pickled)

    assert basic_store.is_symbol_pickled(symbol) is False
    assert basic_store.is_symbol_pickled(symbol, 0) is True
    assert basic_store.is_symbol_pickled(symbol, 1) is False


def test_is_pickled_by_snapshot(basic_store):
    symbol = "test"
    will_be_pickled = [1, 2, 3]
    snap1 = "snap1"
    basic_store.write(symbol, will_be_pickled)
    basic_store.snapshot(snap1)

    snap2 = "snap2"
    not_pickled = pd.DataFrame({"a": np.arange(3)})
    basic_store.write(symbol, not_pickled)
    basic_store.snapshot(snap2)

    assert basic_store.is_symbol_pickled(symbol) is False
    assert basic_store.is_symbol_pickled(symbol, snap1) is True
    assert basic_store.is_symbol_pickled(symbol, snap2) is False


def test_is_pickled_by_timestamp(basic_store):
    symbol = "test"
    will_be_pickled = [1, 2, 3]
    with distinct_timestamps(basic_store) as first_write_timestamps:
        basic_store.write(symbol, will_be_pickled)

    not_pickled = pd.DataFrame({"a": np.arange(3)})
    with distinct_timestamps(basic_store):
        basic_store.write(symbol, not_pickled)

    with pytest.raises(NoDataFoundException):
        basic_store.read(symbol, pd.Timestamp(0))
    assert basic_store.is_symbol_pickled(symbol) is False
    assert basic_store.is_symbol_pickled(symbol, first_write_timestamps.after) is True
    assert basic_store.is_symbol_pickled(symbol, pd.Timestamp(np.iinfo(np.int64).max)) is False


def test_list_versions(object_and_mem_and_lmdb_version_store):
    version_store = object_and_mem_and_lmdb_version_store
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


def test_list_versions_deleted_flag(basic_store):
    basic_store.write("symbol", pd.DataFrame(), metadata=1)
    basic_store.write("symbol", pd.DataFrame(), metadata=2, prune_previous_version=False)
    basic_store.write("symbol", pd.DataFrame(), metadata=3, prune_previous_version=False)
    basic_store.snapshot("snapshot")

    versions = basic_store.list_versions("symbol")
    assert len(versions) == 3
    versions = sorted(versions, key=lambda v: v["version"])
    assert not versions[2]["deleted"]
    assert versions[2]["snapshots"] == ["snapshot"]

    basic_store.delete_version("symbol", 2)
    versions = basic_store.list_versions("symbol")
    assert len(versions) == 3
    versions = sorted(versions, key=lambda v: v["version"])

    assert not versions[0]["deleted"]
    assert not versions[0]["snapshots"]
    assert not versions[1]["deleted"]
    assert not versions[1]["snapshots"]
    assert versions[2]["deleted"]
    assert versions[2]["snapshots"] == ["snapshot"]


def test_list_versions_with_snapshots(basic_store):
    lib = basic_store
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


def test_read_ts(basic_store):
    with distinct_timestamps(basic_store) as first_write_timestamps:
        basic_store.write("a", 1)  # v0
    assert basic_store.read("a", as_of=first_write_timestamps.after).version == 0
    with distinct_timestamps(basic_store):
        basic_store.write("a", 2)  # v1
    with distinct_timestamps(basic_store):
        basic_store.write("a", 3)  # v2
    basic_store.write("a", 4)  # v3

    versions = basic_store.list_versions()
    assert len(versions) == 4
    sorted_versions_for_a = sorted([v for v in versions if v["symbol"] == "a"], key=lambda x: x["version"])
    ts_for_v1 = sorted_versions_for_a[1]["date"]
    vitem = basic_store.read("a", as_of=ts_for_v1)
    assert vitem.version == 1
    assert vitem.data == 2

    ts_for_v0 = sorted_versions_for_a[0]["date"]
    vitem = basic_store.read("a", as_of=ts_for_v0)
    assert vitem.version == 0
    assert vitem.data == 1

    with pytest.raises(NoDataFoundException):
        basic_store.read("a", as_of=pd.Timestamp(0))

    brexit_almost_over = pd.Timestamp.max - pd.Timedelta(1, unit="day")  # Timestamp("2262-04-10 23:47:16.854775807")
    vitem = basic_store.read("a", as_of=brexit_almost_over)
    assert vitem.version == 3
    assert vitem.data == 4

    vitem = basic_store.read("a", as_of=first_write_timestamps.after)
    assert vitem.version == 0
    assert vitem.data == 1


def test_negative_strides(basic_store_tiny_segment):
    lmdb_version_store = basic_store_tiny_segment
    negative_stride_np = np.array([[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11, 12]], np.int32)[::-1]
    lmdb_version_store.write("negative_strides", negative_stride_np)
    vit = lmdb_version_store.read("negative_strides")
    assert_array_equal(negative_stride_np, vit.data)
    negative_stride_df = pd.DataFrame(negative_stride_np)
    lmdb_version_store.write("negative_strides_df", negative_stride_df)
    vit2 = lmdb_version_store.read("negative_strides_df")
    assert_equal(negative_stride_df, vit2.data)


def test_dynamic_strings(basic_store):
    row = pd.Series(["A", "B", "C", "Aaba", "Baca", "CABA", "dog", "cat"])
    df = pd.DataFrame({"x": row})
    basic_store.write("strings", df, dynamic_strings=True)
    vit = basic_store.read("strings")
    assert_equal(vit.data, df)


def test_dynamic_strings_non_contiguous(basic_store):
    df = sample_dataframe_only_strings(100, 0, 100)
    series = df.iloc[-1]
    series.name = None
    basic_store.write("strings", series, dynamic_strings=True)
    vit = basic_store.read("strings")
    assert_series_equal(vit.data, series)


def test_dynamic_strings_with_none(basic_store):
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
    basic_store.write("strings", df, dynamic_strings=True)
    vit = basic_store.read("strings")
    assert_equal(vit.data, df)


def test_dynamic_strings_with_none_first_element(basic_store):
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
    basic_store.write("strings", df, dynamic_strings=True)
    vit = basic_store.read("strings")
    assert_equal(vit.data, df)


def test_dynamic_strings_with_all_nones(basic_store):
    df = pd.DataFrame({"x": [None, None]})
    basic_store.write("strings", df, dynamic_strings=True)
    data = basic_store.read("strings")
    assert data.data["x"][0] is None
    assert data.data["x"][1] is None


def test_dynamic_strings_with_all_nones_update(basic_store):
    df = pd.DataFrame(
        {"col_1": ["a", "b"], "col_2": [0.1, 0.2]}, index=[pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")]
    )
    update_df = pd.DataFrame({"col_1": [np.nan], "col_2": [0.1]}, index=[pd.Timestamp("2022-01-01")])
    basic_store.write("strings", df, dynamic_strings=True)
    with pytest.raises(StreamDescriptorMismatch):
        # nan causes col_1 is considered to be a float column
        # Won't accept that as a string column even with DS enabled
        basic_store.update("strings", update_df, dynamic_strings=True)

    basic_store.update("strings", update_df.astype({"col_1": "object"}), dynamic_strings=True)

    data = basic_store.read("strings")
    assert math.isnan(data.data["col_1"][pd.Timestamp("2022-01-01")])
    assert data.data["col_1"][pd.Timestamp("2022-01-02")] == "b"

    basic_store.write("strings", df, dynamic_strings=True)
    basic_store.update("strings", update_df, dynamic_strings=True, coerce_columns={"col_1": object, "col_2": "float"})

    data = basic_store.read("strings")
    assert math.isnan(data.data["col_1"][pd.Timestamp("2022-01-01")])
    assert data.data["col_1"][pd.Timestamp("2022-01-02")] == "b"


def test_dynamic_strings_with_nan(basic_store):
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
    basic_store.write("strings", df, dynamic_strings=True)
    vit = basic_store.read("strings")
    assert_equal(vit.data, df)


def test_metadata_with_snapshots(basic_store):
    symbol_metadata1 = {"test": "data_meta"}
    symbol_metadata2 = {"test": "should_not_be_returned"}
    snap_metadata = {"test": "snap_meta"}
    basic_store.write("symbol", 1, metadata=symbol_metadata1)
    basic_store.snapshot("snap1", metadata=snap_metadata)
    basic_store.write("symbol", 2, metadata=symbol_metadata2)

    meta = basic_store.read_metadata("symbol", as_of="snap1").metadata
    assert meta == symbol_metadata1
    snapshot = basic_store.list_snapshots()
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


@pytest.mark.parametrize("batch", (True, False))
def test_recursively_written_data(basic_store, batch):
    samples = [
        {"a": np.arange(5), "b": np.arange(8)},  # dict of np arrays
        (np.arange(5), np.arange(6)),  # tuple of np arrays
        [np.arange(9), np.arange(12), (1, 2)],  # list of numpy arrays and a python tuple
        ({"a": np.arange(5), "b": [1, 2, 3]}),  # dict of np arrays and a python list
    ]

    for idx, sample in enumerate(samples):
        recursive_sym = "sym_recursive" + str(idx)
        pickled_sym = "sym_pickled" + str(idx)
        basic_store.write(recursive_sym, sample, recursive_normalizers=True)
        basic_store.write(pickled_sym, sample)  # pickled writes
        if batch:
            recursive_vit = basic_store.batch_read([recursive_sym])[recursive_sym]
            pickled_vit = basic_store.batch_read([pickled_sym])[pickled_sym]
        else:
            recursive_vit = basic_store.read(recursive_sym)
            pickled_vit = basic_store.read(pickled_sym)
        equals(sample, recursive_vit.data)
        equals(pickled_vit.data, recursive_vit.data)
        assert recursive_vit.symbol == recursive_sym
        assert pickled_vit.symbol == pickled_sym


@pytest.mark.parametrize("batch", (True, False))
def test_recursively_written_data_with_metadata(basic_store, batch):
    samples = [
        {"a": np.arange(5), "b": np.arange(8)},  # dict of np arrays
        (np.arange(5), np.arange(6)),  # tuple of np arrays
    ]

    for idx, sample in enumerate(samples):
        sym = "sym_recursive" + str(idx)
        metadata = {"something": 1}
        basic_store.write(sym, sample, metadata=metadata, recursive_normalizers=True)
        if batch:
            vit = basic_store.batch_read([sym])[sym]
        else:
            vit = basic_store.read(sym)
        equals(sample, vit.data)
        assert vit.symbol == sym
        assert vit.metadata == metadata


@pytest.mark.parametrize("batch", (True, False))
def test_recursively_written_data_with_nones(basic_store, batch):
    sample = {"a": np.arange(5), "b": np.arange(8), "c": None}
    recursive_sym = "sym_recursive"
    pickled_sym = "sym_pickled"
    basic_store.write(recursive_sym, sample, recursive_normalizers=True)
    basic_store.write(pickled_sym, sample)  # pickled writes
    if batch:
        recursive_vit = basic_store.batch_read([recursive_sym])[recursive_sym]
        pickled_vit = basic_store.batch_read([pickled_sym])[pickled_sym]
    else:
        recursive_vit = basic_store.read(recursive_sym)
        pickled_vit = basic_store.read(pickled_sym)
    equals(sample, recursive_vit.data)
    equals(pickled_vit.data, recursive_vit.data)
    assert recursive_vit.symbol == recursive_sym
    assert pickled_vit.symbol == pickled_sym


@pytest.mark.parametrize("batch", (True, False))
def test_recursive_nested_data(basic_store, batch):
    sym = "test_recursive_nested_data"
    sample_data = {"a": {"b": {"c": {"d": np.arange(24)}}}}
    fl = Flattener()
    assert fl.can_flatten(sample_data)
    assert fl.is_dict_like(sample_data)
    metast, to_write = fl.create_meta_structure(sample_data, "sym")
    assert len(to_write) == 1
    equals(list(to_write.values())[0], np.arange(24))

    basic_store.write(sym, sample_data, recursive_normalizers=True)
    if batch:
        vit = basic_store.batch_read([sym])[sym]
    else:
        vit = basic_store.read(sym)
    equals(vit.data, sample_data)
    assert vit.symbol == sym


def test_named_tuple_flattening_rejected():
    fl = Flattener()
    SomeThing = namedtuple("SomeThing", "prop another_prop")
    nt = SomeThing(1, 2)
    assert fl.can_flatten(nt) is False


def test_data_directly_msgpackable(basic_store):
    data = {"a": [1, 2, 3], "b": {"c": 5}}
    fl = Flattener()
    meta, to_write = fl.create_meta_structure(data, "sym")
    assert len(to_write) == 0
    assert meta["leaf"] is True
    basic_store.write("s", data, recursive_normalizers=True)
    equals(basic_store.read("s").data, data)


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


@pytest.mark.parametrize("batch", (True, False))
def test_really_large_symbol_for_recursive_data(basic_store, batch):
    sym = "s" * 100
    data = {"a" * 100: {"b" * 100: {"c" * 1000: {"d": np.arange(5)}}}}
    basic_store.write(sym, data, recursive_normalizers=True)
    fl = Flattener()
    metastruct, to_write = fl.create_meta_structure(data, "s" * 100)
    assert len(list(to_write.keys())[0]) < fl.MAX_KEY_LENGTH
    if batch:
        vit = basic_store.batch_read([sym])[sym]
    else:
        vit = basic_store.read(sym)
    equals(vit.data, data)
    assert vit.symbol == sym


def test_too_much_recursive_metastruct_data(monkeypatch, lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_too_much_recursive_metastruct_data"
    data = [pd.DataFrame({"col": [0]}), pd.DataFrame({"col": [1]})]
    with pytest.raises(ArcticDbNotYetImplemented) as e:
        with monkeypatch.context() as m:
            m.setattr(arcticdb.version_store._normalization, "_MAX_RECURSIVE_METASTRUCT", 1)
            lib.write(sym, data, recursive_normalizers=True)
    assert "recursive" in str(e.value).lower()


def test_nested_custom_types(basic_store):
    data = AlmostAList([1, 2, 3, AlmostAList([5, np.arange(6)])])
    fl = Flattener()
    meta, to_write = fl.create_meta_structure(data, "sym")
    equals(list(to_write.values())[0], np.arange(6))
    basic_store.write("sym", data, recursive_normalizers=True)
    got_back = basic_store.read("sym").data
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


def test_batch_write(basic_store_tombstone_and_sync_passive):
    lmdb_version_store = basic_store_tombstone_and_sync_passive
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


def test_batch_write_then_read(basic_store):
    symbol = "sym_d_1"
    data = pd.Series(index=[0], data=[1])

    # Write, then delete a symbol
    basic_store.write(symbol=symbol, data=data)
    basic_store.delete(symbol)

    # Batch write the same data to the same symbol
    basic_store.batch_write(symbols=[symbol], data_vector=[data])
    basic_store.read(symbol)


def test_batch_write_then_list_symbol_without_cache(basic_store_factory):
    factory = basic_store_factory
    lib = factory(symbol_list=False, segment_row_size=10)
    df = pd.DataFrame([1, 2, 3])
    for idx in range(10):
        lib.version_store.clear()
        symbols = [f"s{idx}-{i}" for i in [1, 2, 3]]
        lib.batch_write(symbols=symbols, data_vector=[df, df, df])
        assert set(lib.list_symbols()) == set(symbols)


def test_batch_write_missing_keys_dedup(basic_store_factory):
    """When there is duplicate data to reuse for the current write, we need to access the index key of the previous
    versions in order to refer to the corresponding keys for the deduplicated data."""
    lib = basic_store_factory(de_duplication=True)
    assert lib.lib_cfg().lib_desc.version.write_options.de_duplication

    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    lib.write("s1", df1)
    lib.write("s2", df2)

    lib_tool = lib.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    lib_tool.remove(s1_index_key)

    with pytest.raises(StorageException):
        lib.batch_write(["s1", "s2"], [df1, df2])


def test_batch_write_metadata_missing_keys(basic_store):
    lib = basic_store

    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    lib.write("s1", df1)
    lib.write("s2", df2)

    lib_tool = lib.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s2")[0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_index_key)
    with pytest.raises(StorageException):
        _ = lib.batch_write_metadata(["s1", "s2"], [{"s1_meta": 1}, {"s2_meta": 1}])


def test_batch_read_metadata_missing_keys(basic_store):
    lib = basic_store

    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    lib.write("s1", df1, metadata={"s1": "metadata"})
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    lib.write("s2", df2, metadata={"s2": "metadata"})
    lib.write("s2", df2, metadata={"s2": "more_metadata"})
    lib.write("s2", df2, metadata={"s2": "even_more_metadata"})
    lib_tool = lib.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s2")
    s2_key_to_delete = [key for key in s2_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_key_to_delete)

    vits = lib.batch_read_metadata(["s2"], [1])
    metadata = vits["s2"].metadata
    assert metadata["s2"] == "more_metadata"

    with pytest.raises(StorageException):
        _ = lib.batch_read_metadata(["s1"], [None])
    with pytest.raises(StorageException):
        _ = lib.batch_read_metadata(["s2"], [0])


def test_batch_read_metadata_multi_missing_keys(basic_store):
    lib = basic_store
    lib_tool = lib.library_tool()

    lib.write("s1", 0, metadata={"s1": "metadata"})
    key_to_delete = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    lib_tool.remove(key_to_delete)

    with pytest.raises(StorageException):
        _ = lib.batch_read_metadata_multi(["s1"], [None])


def test_batch_read_missing_keys(basic_store):
    lib = basic_store
    lib.version_store._set_validate_version_map()
    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [4, 6, 8]})
    df3 = pd.DataFrame({"a": [5, 7, 9]})
    lib.write("s1", df1)
    lib.write("s2", df2)
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    lib.write("s3", df3)
    lib.write("s3", df3)
    lib_tool = lib.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_data_key = lib_tool.find_keys_for_id(KeyType.TABLE_DATA, "s2")[0]
    s3_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s3")
    s3_key_to_delete = [key for key in s3_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_data_key)
    lib_tool.remove(s3_key_to_delete)

    # The exception thrown is different for missing version keys to everything else, and so depends on which symbol is
    # processed first
    with pytest.raises((NoDataFoundException, StorageException)):
        _ = lib.batch_read(["s1", "s2", "s3"], [None, None, 0])


def test_batch_get_info_missing_keys(basic_store):
    lib = basic_store
    lib.version_store._set_validate_version_map()
    df1 = pd.DataFrame({"a": [3, 5, 7]})
    df2 = pd.DataFrame({"a": [5, 7, 9]})
    lib.write("s1", df1)
    # Need two versions for this symbol as we're going to delete a version key, and the optimisation of storing the
    # latest index key in the version ref key means it will still work if we just write one version key and then delete
    # it
    lib.write("s2", df2)
    lib.write("s2", df1)
    lib.write("s2", df2)
    lib_tool = lib.library_tool()
    s1_index_key = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    s2_version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, "s2")
    s2_key_to_delete = [key for key in s2_version_keys if key.version_id == 0][0]
    lib_tool.remove(s1_index_key)
    lib_tool.remove(s2_key_to_delete)

    info = lib.batch_get_info(["s2"], [1])

    with pytest.raises(StorageException):
        _ = lib.batch_get_info(["s1"], [None])
    with pytest.raises(StorageException):
        _ = lib.batch_get_info(["s2"], [0])


def test_batch_roundtrip_metadata(basic_store_tombstone_and_sync_passive):
    lib = basic_store_tombstone_and_sync_passive

    metadatas = {}
    for x in range(10):
        symbol = "Sym_{}".format(x)
        metadatas[symbol] = {"a": x}

    for symbol in metadatas:
        lib.write(symbol, 12)

    symbols = []
    metas = []
    for sym, meta in metadatas.items():
        symbols.append(sym)
        metas.append(meta)

    write_result = lib.batch_write_metadata(symbols, metas)
    assert all(type(w) == VersionedItem for w in write_result)
    vits = lib.batch_read_metadata(symbols)

    for sym, returned in vits.items():
        assert returned.metadata == metadatas[sym]


def test_write_composite_data_with_user_meta(basic_store):
    multi_data = {"sym1": np.arange(8), "sym2": np.arange(9), "sym3": np.arange(10)}
    basic_store.write("sym", multi_data, metadata={"a": 1})
    vitem = basic_store.read("sym")
    assert vitem.metadata == {"a": 1}
    equals(vitem.data["sym1"], np.arange(8))


def test_force_delete(basic_store):
    df1 = sample_dataframe()
    basic_store.write("sym1", df1)
    df2 = sample_dataframe(seed=1)
    basic_store.write("sym1", df2)
    df3 = sample_dataframe(seed=2)
    basic_store.write("sym2", df3)
    df4 = sample_dataframe(seed=3)
    basic_store.write("sym2", df4)
    basic_store.version_store.force_delete_symbol("sym2")
    with pytest.raises(NoDataFoundException):
        basic_store.read("sym2")

    assert_equal(basic_store.read("sym1").data, df2)
    assert_equal(basic_store.read("sym1", as_of=0).data, df1)


def test_force_delete_with_delayed_deletes(basic_store_delayed_deletes):
    df1 = sample_dataframe()
    basic_store_delayed_deletes.write("sym1", df1)
    df2 = sample_dataframe(seed=1)
    basic_store_delayed_deletes.write("sym1", df2)
    df3 = sample_dataframe(seed=2)
    basic_store_delayed_deletes.write("sym2", df3)
    df4 = sample_dataframe(seed=3)
    basic_store_delayed_deletes.write("sym2", df4)
    basic_store_delayed_deletes.version_store.force_delete_symbol("sym2")
    with pytest.raises(NoDataFoundException):
        basic_store_delayed_deletes.read("sym2")

    assert_equal(basic_store_delayed_deletes.read("sym1").data, df2)
    assert_equal(basic_store_delayed_deletes.read("sym1", as_of=0).data, df1)


def test_dataframe_with_NaN_in_timestamp_column(basic_store):
    normal_df = pd.DataFrame({"col": [pd.Timestamp("now"), pd.NaT]})
    basic_store.write("normal", normal_df)
    assert_equal(normal_df, basic_store.read("normal").data)


def test_dataframe_with_nan_and_nat_in_timestamp_column(basic_store):
    df_with_NaN_mixed_in_ts = pd.DataFrame({"col": [pd.Timestamp("now"), pd.NaT, np.nan]})
    basic_store.write("mixed_nan", df_with_NaN_mixed_in_ts)
    returned_df = basic_store.read("mixed_nan").data
    # NaN will now be converted to NaT
    isinstance(returned_df["col"][2], type(pd.NaT))


def test_dataframe_with_nan_and_nat_only(basic_store):
    df_with_nan_and_nat_only = pd.DataFrame({"col": [pd.NaT, pd.NaT, np.nan]})  # Sample will be pd.NaT
    basic_store.write("nan_nat", df_with_nan_and_nat_only)
    assert_equal(basic_store.read("nan_nat").data, pd.DataFrame({"col": [pd.NaT, pd.NaT, pd.NaT]}))


def test_coercion_to_float(basic_store):
    lib = basic_store
    df = pd.DataFrame({"col": [np.nan, "1", np.nan]})
    # col is now an Object column with all NaNs
    df["col"][1] = np.nan

    assert df["col"].dtype == np.object_

    if sys.platform != "win32":  # SKIP_WIN Windows always uses dynamic strings
        with pytest.raises(ArcticDbNotYetImplemented):
            # Needs pickling due to the obj column
            lib.write("test", df)

    lib.write("test", df, coerce_columns={"col": float})
    returned = lib.read("test").data
    # Should be a float now.
    assert returned["col"].dtype != np.object_


def test_coercion_to_str_with_dynamic_strings(basic_store):
    # assert that the getting sample function is not called
    lib = basic_store
    df = pd.DataFrame({"col": [None, None, "hello", "world"]})
    assert df["col"].dtype == np.object_

    if sys.platform != "win32":  # SKIP_WIN Windows always uses dynamic strings
        with pytest.raises(ArcticDbNotYetImplemented):
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
    sym = "test_find_version"

    # Version 0 is alive and in a snapshot
    with distinct_timestamps(lmdb_version_store_v1) as v0_time:
        lib.write(sym, 0)
    lib.snapshot("snap_0")

    # Version 1 is only available in snap_1
    with distinct_timestamps(lmdb_version_store_v1) as v1_time:
        lib.write(sym, 1)
    lib.snapshot("snap_1")
    lib.delete_version(sym, 1)

    # Version 2 is fully deleted
    with distinct_timestamps(lmdb_version_store_v1) as v2_time:
        lib.write(sym, 2)
    lib.delete_version(sym, 2)

    # Version 3 is not in any snapshots
    with distinct_timestamps(lmdb_version_store_v1) as v3_time:
        lib.write(sym, 3)

    # Latest
    # assert lib._find_version(sym).version == 3
    # By version number
    # assert lib._find_version(sym, as_of=0).version == 0
    # assert lib._find_version(sym, as_of=1).version == 1
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
    assert lib._find_version(sym, as_of=v0_time.after).version == 0
    assert lib._find_version(sym, as_of=v1_time.after).version == 0
    assert lib._find_version(sym, as_of=v2_time.after).version == 0
    assert lib._find_version(sym, as_of=v3_time.after).version == 3


def test_library_deletion_lmdb(basic_store):
    # lmdb uses fast deletion
    basic_store.write("a", 1)
    basic_store.write("b", 1)

    basic_store.snapshot("snap")
    assert len(basic_store.list_symbols()) == 2
    basic_store.version_store.clear()
    assert len(basic_store.list_symbols()) == 0
    lib_tool = basic_store.library_tool()
    assert lib_tool.count_keys(KeyType.VERSION) == 0
    assert lib_tool.count_keys(KeyType.TABLE_INDEX) == 0


def test_resolve_defaults(basic_store_factory):
    lib = basic_store_factory()
    proto_cfg = lib._lib_cfg.lib_desc.version.write_options
    assert lib.resolve_defaults("recursive_normalizers", proto_cfg, False) is False
    os.environ["recursive_normalizers"] = "True"
    assert lib.resolve_defaults("recursive_normalizers", proto_cfg, False, uppercase=False) is True

    lib2 = basic_store_factory(dynamic_strings=True, reuse_name=True)
    proto_cfg = lib2._lib_cfg.lib_desc.version.write_options
    assert lib2.resolve_defaults("dynamic_strings", proto_cfg, False) is True
    del os.environ["recursive_normalizers"]


def test_batch_read_meta(basic_store_tombstone_and_sync_passive):
    lib = basic_store_tombstone_and_sync_passive
    for idx in range(10):
        lib.write("sym" + str(idx), idx, metadata={"meta": idx})

    results_dict = lib.batch_read_metadata(["sym" + str(idx) for idx in range(10)])
    assert results_dict["sym1"].metadata == {"meta": 1}
    assert results_dict["sym5"].metadata == {"meta": 5}

    assert lib.read_metadata("sym6").metadata == results_dict["sym6"].metadata

    assert results_dict["sym2"].data is None


def test_batch_read_metadata_symbol_doesnt_exist(basic_store_tombstone_and_sync_passive):
    lib = basic_store_tombstone_and_sync_passive
    for idx in range(10):
        lib.write("sym" + str(idx), idx, metadata={"meta": idx})

    results_dict = lib.batch_read_metadata(["sym1", "sym_doesnotexist", "sym2"])

    assert results_dict["sym1"].metadata == {"meta": 1}
    assert "sym_doesnotexist" not in results_dict


def test_list_versions_with_deleted_symbols(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
    lib.write("a", 1)
    lib.snapshot("snap")
    lib.write("a", 2)
    versions = lib.list_versions()
    # At this point version 0 of 'a' is pruned but is still in the snapshot.
    assert len(versions) == 2
    deleted = [v for v in versions if v["deleted"]]
    not_deleted = [v for v in versions if not v["deleted"]]
    assert len(deleted) == 1
    assert deleted[0]["symbol"] == "a"
    assert deleted[0]["version"] == 0

    assert not_deleted[0]["version"] == 1

    assert lib.read("a").data == 2


def test_read_with_asof_version_for_snapshotted_version(basic_store_tombstone_and_pruning):
    lib = basic_store_tombstone_and_pruning
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


def test_get_tombstone_deletion_state_without_delayed_del(basic_store_factory, sym):
    lib = basic_store_factory(use_tombstones=True, delayed_deletes=False)
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

    lib.write(f"{sym}_new", 1)
    lib.add_to_snapshot("snap", [f"{sym}_new"])
    tombstoned_version_map = lib.version_store._get_all_tombstoned_versions(sym)
    assert len(tombstoned_version_map) == 3


def test_get_tombstone_deletion_state_with_delayed_del(basic_store_factory, sym):
    lib = basic_store_factory(use_tombstones=True, delayed_deletes=True)
    lib.write(sym, 1)

    lib.write(sym, 2)

    lib.snapshot("snap")
    lib.write(sym, 3, prune_previous_version=True)
    tombstoned_version_map = lib.version_store._get_all_tombstoned_versions(sym)
    # v0 and v1
    assert len(tombstoned_version_map) == 2
    assert tombstoned_version_map[0] is True
    assert tombstoned_version_map[1] is True

    lib.write(sym, 3)
    lib.delete_version(sym, 2)
    tombstoned_version_map = lib.version_store._get_all_tombstoned_versions(sym)
    assert len(tombstoned_version_map) == 3
    assert tombstoned_version_map[2] is True

    lib.write(f"{sym}_new", 1)
    lib.add_to_snapshot("snap", [f"{sym}_new"])
    tombstoned_version_map = lib.version_store._get_all_tombstoned_versions(sym)
    assert len(tombstoned_version_map) == 3


def test_get_timerange_for_symbol(basic_store, sym):
    lib = basic_store
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(100), index=pd.date_range(initial_timestamp, periods=100))
    lib.write(sym, df)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == datetime(2019, 1, 1)


def test_get_timerange_for_symbol_tz(basic_store, sym):
    lib = basic_store
    dt1 = timezone("US/Eastern").localize(datetime(2021, 4, 1))
    dt2 = timezone("US/Eastern").localize(datetime(2021, 4, 1, 3))
    dst = pd.DataFrame({"a": [0, 1]}, index=[dt1, dt2])
    lib.write(sym, dst)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == dt1
    assert maxts == dt2


def test_get_timerange_for_symbol_dst(basic_store, sym):
    lib = basic_store
    dst = pd.DataFrame({"a": [0, 1]}, index=[datetime(2021, 4, 1), datetime(2021, 4, 1, 3)])
    lib.write(sym, dst)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == datetime(2021, 4, 1)
    assert maxts == datetime(2021, 4, 1, 3)


def test_batch_read_meta_with_tombstones(basic_store_tombstone_and_sync_passive):
    lib = basic_store_tombstone_and_sync_passive
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


def test_batch_read_meta_with_pruning(basic_store_factory):
    lib = basic_store_factory(use_tombstones=True, prune_previous_version=True, sync_passive=True)
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


def test_list_symbols(basic_store):
    lib = basic_store

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


def test_read_empty_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_read_empty_index"
    lib.write(sym, pd.DataFrame())
    assert len(lib.read_index(sym)) == 0


def test_snapshot_empty_segment(basic_store):
    lib = basic_store

    lib.write("a", 1)
    lib.write("b", 1)

    lib.snapshot("snap")
    lib.delete("a")
    assert lib.read("a", as_of="snap").data == 1
    lib.write("c", 1)
    lib.snapshot("snap2", versions={})
    lib.delete("c")
    assert lib.has_symbol("c") is False


def test_columns_as_nparray(basic_store, sym):
    lib = basic_store
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


def test_dynamic_schema_similar_index_column(basic_store_dynamic_schema):
    lib = basic_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.Series(dr, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_series_equal(returned, date_series)


def test_dynamic_schema_similar_index_column_dataframe(basic_store_dynamic_schema):
    lib = basic_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.DataFrame({"date": np.arange(len(dr))}, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_equal(returned, date_series)


def test_dynamic_schema_similar_index_column_dataframe_multiple_col(basic_store_dynamic_schema):
    lib = basic_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.DataFrame({"col": np.arange(len(dr)), "date": np.arange(len(dr))}, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_equal(returned, date_series)


def test_restore_version(basic_store_tiny_segment):
    lib = basic_store_tiny_segment
    # Triggers bug https://github.com/man-group/ArcticDB/issues/469 by freezing time
    lib.version_store = ManualClockVersionStore(lib._library)
    symbol = "test_restore_version"
    df1 = get_sample_dataframe(20, 4)
    df1.index = pd.DatetimeIndex([pd.Timestamp.now()] * len(df1))
    metadata = {"a": 43}
    lib.write(symbol, df1, metadata=metadata)
    df2 = get_sample_dataframe(20, 6)
    df2.index = df1.index + pd.Timedelta(hours=1)
    second_write_item = lib.write(symbol, df2, prune_previous_version=False)
    assert second_write_item.version == 1
    restore_item = lib.restore_version(symbol, as_of=0)
    assert restore_item.version == 2
    assert restore_item.metadata == metadata
    latest = lib.read(symbol)
    assert_equal(latest.data, df1)
    assert latest.metadata == metadata


@pytest.mark.parametrize("ver", (3, "snap"))
def test_restore_version_not_found(basic_store, ver):
    lib: NativeVersionStore = basic_store
    lib.write("abc", 1)
    lib.write("abc", 2)
    lib.write("bcd", 9)
    lib.snapshot("snap", versions={"bcd": 0})
    with pytest.raises(NoSuchVersionException, match=r"\Wabc\W.*" + str(ver)):
        lib.restore_version("abc", ver)


def test_restore_version_latest_is_noop(basic_store):
    symbol = "test_restore_version"
    df1 = get_sample_dataframe(2, 4)
    df1.index = pd.DatetimeIndex([pd.Timestamp.now()] * len(df1))
    metadata = {"a": 43}
    basic_store.write(symbol, df1)
    df2 = get_sample_dataframe(2, 6)
    df2.index = df1.index + pd.Timedelta(hours=1)
    second_write_item = basic_store.write(symbol, df2, prune_previous_version=False, metadata=metadata)
    assert second_write_item.version == 1
    restore_item = basic_store.restore_version(symbol, as_of=1)
    assert restore_item.version == 1
    assert restore_item.metadata == metadata
    latest = basic_store.read(symbol)
    assert_equal(latest.data, df2)
    assert latest.metadata == metadata
    assert latest.version == 1


def test_restore_version_ndarray(basic_store):
    symbol = "test_restore_version_ndarray"
    arr1 = np.array([0, 2, 4])
    metadata = {"b": 42}
    basic_store.write(symbol, arr1, metadata=metadata)
    arr2 = np.array([0, 4, 8])
    second_write_item = basic_store.write(symbol, arr2, prune_previous_version=False)
    assert second_write_item.version == 1
    restore_item = basic_store.restore_version(symbol, as_of=0)
    assert restore_item.version == 2
    assert restore_item.metadata == metadata
    latest = basic_store.read(symbol)
    assert_array_equal(latest.data, arr1)
    assert latest.metadata == metadata


def test_batch_restore_version(basic_store_tombstone):
    lmdb_version_store = basic_store_tombstone
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
        assert_equal(read_df, dfs[d])


def test_batch_append(basic_store_tombstone, three_col_df):
    lmdb_version_store = basic_store_tombstone
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
        assert_equal(expected, vit.data)
        assert vit.metadata == append_metadata[sym]


def test_batch_append_with_throw_exception(basic_store, three_col_df):
    multi_data = {"sym1": three_col_df(), "sym2": three_col_df(1)}
    with pytest.raises(NoSuchVersionException):
        basic_store.batch_append(
            list(multi_data.keys()),
            list(multi_data.values()),
            write_if_missing=False,
        )


@pytest.mark.pipeline
@pytest.mark.parametrize("use_date_range_clause", [True, False])
def test_batch_read_date_range(basic_store_tombstone_and_sync_passive, use_date_range_clause):
    lmdb_version_store = basic_store_tombstone_and_sync_passive
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

    if use_date_range_clause:
        qbs = []
        for date_range in date_ranges:
            q = QueryBuilder()
            q = q.date_range(date_range)
            qbs.append(q)
        result_dict = lmdb_version_store.batch_read(symbols, query_builder=qbs)
    else:
        result_dict = lmdb_version_store.batch_read(symbols, date_ranges=date_ranges)
    for x, sym in enumerate(result_dict.keys()):
        vit = result_dict[sym]
        date_range = date_ranges[x]
        start = date_range[0]
        end = date_range[-1]
        assert_equal(vit.data, dfs[x].loc[start:end])


@pytest.mark.parametrize("use_row_range_clause", [True, False])
def test_batch_read_row_range(lmdb_version_store_v1, use_row_range_clause):
    lib = lmdb_version_store_v1
    num_symbols = 5
    num_rows = 10
    symbols = [f"sym_{i}" for i in range(num_symbols)]

    dfs = []
    for j in range(num_symbols):
        df = get_sample_dataframe(num_rows, j)
        df.index = np.arange(num_rows)
        dfs.append(df)

    for idx, symbol in enumerate(symbols):
        lib.write(symbol, dfs[idx])

    row_ranges = []
    for j in range(num_symbols):
        row_ranges.append((j * (num_rows // num_symbols), ((j + 1) * (num_rows // num_symbols))))

    if use_row_range_clause:
        qbs = []
        for row_range in row_ranges:
            q = QueryBuilder()
            q = q.row_range(row_range)
            qbs.append(q)
        result_dict = lib.batch_read(symbols, query_builder=qbs)
    else:
        result_dict = lib.batch_read(symbols, row_ranges=row_ranges)
    for idx, sym in enumerate(result_dict.keys()):
        df = result_dict[sym].data
        row_range = row_ranges[idx]
        assert_equal(df, dfs[idx].iloc[row_range[0] : row_range[1]])


def test_batch_read_columns(basic_store_tombstone_and_sync_passive):
    lmdb_version_store = basic_store_tombstone_and_sync_passive
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


def test_batch_read_symbol_doesnt_exist(basic_store):
    sym1 = "sym1"
    sym2 = "sym2"
    basic_store.write(sym1, 1)
    with pytest.raises(NoDataFoundException):
        _ = basic_store.batch_read([sym1, sym2])


def test_batch_read_version_doesnt_exist(basic_store):
    sym1 = "sym1"
    sym2 = "sym2"
    basic_store.write(sym1, 1)
    basic_store.write(sym2, 2)
    with pytest.raises(NoDataFoundException):
        _ = basic_store.batch_read([sym1, sym2], as_ofs=[0, 1])


def test_read_batch_deleted_version_doesnt_exist(basic_store):
    sym1 = "mysymbol"
    basic_store.write(sym1, 0)

    basic_store.delete(sym1)
    basic_store.write(sym1, 1)
    with pytest.raises(NoSuchVersionException):
        basic_store.read(sym1, as_of=0)

    with pytest.raises(NoSuchVersionException):
        basic_store.batch_read([sym1], as_ofs=[0])


def test_index_keys_start_end_index(basic_store, sym):
    idx = pd.date_range("2022-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": range(len(idx))}, index=idx)
    basic_store.write(sym, df)

    lt = basic_store.library_tool()
    key = lt.find_keys_for_id(KeyType.TABLE_INDEX, sym)[0]
    assert key.start_index == 1640995200000000000
    assert key.end_index == 1649548800000000001


def test_dynamic_schema_column_hash_update(basic_store_column_buckets):
    lib = basic_store_column_buckets
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
    # In Pandas < 2.0, updating a `DataFrame` uniquely storing integers with
    # another `DataFrame` that is uniquely storing integers changes all the dtypes
    # to float64.
    df.update(df2)
    df = df.astype("int64", copy=False)
    assert_equal(vit.data, df)


def test_dynamic_schema_column_hash_append(basic_store_column_buckets):
    lib = basic_store_column_buckets
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
    assert_equal(vit.data, new_df)


def test_dynamic_schema_column_hash(basic_store_column_buckets):
    lib = basic_store_column_buckets
    idx = pd.date_range("2022-01-01", periods=10, freq="D")
    l = len(idx)
    df = pd.DataFrame(
        {"a": range(l), "b": range(1, l + 1), "c": range(2, l + 2), "d": range(3, l + 3), "e": range(4, l + 4)},
        index=idx,
    )

    lib.write("symbol", df)

    read_df = lib.read("symbol").data
    assert_equal(df, read_df)

    read_df = lib.read("symbol", columns=["a", "b"]).data
    assert_equal(df[["a", "b"]], read_df)

    read_df = lib.read("symbol", columns=["a", "e"]).data
    assert_equal(df[["a", "e"]], read_df)

    read_df = lib.read("symbol", columns=["a", "c"]).data
    assert_equal(df[["a", "c"]], read_df)


def test_list_versions_without_snapshots(basic_store):
    lib = basic_store
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
def test_modification_methods_dont_return_input_data(basic_store, batch, method):  # AN-650
    lib: NativeVersionStore = basic_store

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


@pytest.mark.skipif(sys.platform == "darwin", reason="Test broken on MacOS (issue #692)")
@pytest.mark.parametrize("method", ("append", "update"))
@pytest.mark.parametrize("num", (5, 50, 1001))
def test_diff_long_stream_descriptor_mismatch(basic_store, method, num):
    lib: NativeVersionStore = basic_store
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


def test_wrong_df_col_order(basic_store):
    lib = basic_store

    df1 = pd.DataFrame({"col1": [11, 12, 13], "col2": [1, 2, 3]})
    sym = "symbol"
    lib.write(sym, df1)

    df2 = pd.DataFrame({"col2": [4, 5, 6], "col1": [14, 15, 16]})
    with pytest.raises(StreamDescriptorMismatch, match="type=TD<type=INT64, dim=0>, idx="):
        lib.append(sym, df2)


def test_get_non_existing_columns_in_series(basic_store, sym):
    lib = basic_store
    dst = pd.Series(index=pd.date_range(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-02-01")), data=0.0)
    lib.write(sym, dst)
    assert basic_store.read(sym, columns=["col1"]).data.empty


def test_get_existing_columns_in_series(basic_store, sym):
    lib = basic_store
    dst = pd.Series(index=pd.date_range(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-02-01")), data=0.0, name="col1")
    lib.write(sym, dst)
    assert not basic_store.read(sym, columns=["col1", "col2"]).data.empty
    if __name__ == "__main__":
        pytest.main()


def remove_most_recent_version_key(version_store, symbol):
    lib_tool = version_store.library_tool()
    version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, symbol)
    assert len(version_keys) == 2
    version_keys.sort(key=lambda k: k.creation_ts)

    lib_tool.remove(version_keys[1])
    version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, symbol)
    assert len(version_keys) == 1
    assert version_keys[0].version_id == 0


def test_missing_first_version_key_single(basic_store):
    symbol = "test_missing_first_version_key_single"
    lib = basic_store
    lib.version_store._set_validate_version_map()
    idx = pd.date_range("2022-01-01", periods=10, freq="D")
    l = len(idx)
    df1 = pd.DataFrame({"a": range(l), "b": range(1, l + 1), "c": range(2, l + 2)}, index=idx)

    vit = lib.write(symbol, df1)

    v1_write_time = vit.timestamp
    time.sleep(1)
    df2 = pd.DataFrame({"d": range(1, l + 1), "e": range(2, l + 2), "f": range(3, l + 3)}, index=idx)
    lib.write(symbol, df2)

    remove_most_recent_version_key(basic_store, symbol)

    vit = lib.read(symbol, as_of=pd.Timestamp(v1_write_time))
    assert_equal(df1, vit.data)


def test_update_with_missing_version_key(version_store_factory):
    lmdb_version_store = version_store_factory(col_per_group=2, row_per_segment=2)
    lmdb_version_store.version_store._set_validate_version_map()
    symbol = "update_no_daterange"

    idx = pd.date_range("1970-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": np.arange(len(idx), dtype="float")}, index=idx)
    vit = lmdb_version_store.write(symbol, df)
    v1_write_time = vit.timestamp
    time.sleep(1)

    idx2 = pd.date_range("1970-01-12", periods=10, freq="D")
    df2 = pd.DataFrame({"a": np.arange(1000, 1000 + len(idx2), dtype="float")}, index=idx2)
    lmdb_version_store.update(symbol, df2)

    remove_most_recent_version_key(lmdb_version_store, symbol)

    vit = lmdb_version_store.read(symbol, as_of=pd.Timestamp(v1_write_time))
    assert_equal(vit.data, df)


def test_append_with_missing_version_key(basic_store):
    symbol = "test_append_with_missing_version_key"
    df1 = pd.DataFrame({"x": np.arange(1, 10, dtype=np.int64)})
    vit = basic_store.write(symbol, df1)
    v1_write_time = vit.timestamp
    time.sleep(1)

    df2 = pd.DataFrame({"x": np.arange(11, 20, dtype=np.int64)})
    basic_store.append(symbol, df2)

    remove_most_recent_version_key(basic_store, symbol)

    vit = basic_store.read(symbol, as_of=pd.Timestamp(v1_write_time))
    assert_equal(vit.data, df1)


def test_missing_first_version_key_batch(basic_store):
    lib = basic_store

    expected = []
    write_times = []
    symbols = []
    lib_tool = basic_store.library_tool()
    num_items = 10

    for x in range(num_items):
        idx = pd.date_range("2022-01-01", periods=10, freq="D")
        l = len(idx)
        df1 = pd.DataFrame({"a": range(l), "b": range(x, l + x), "c": range(x, l + x)}, index=idx)
        symbol = "symbol_{}".format(x)
        symbols.append(symbol)

        vit = lib.write(symbol, df1)

        write_times.append(pd.Timestamp(vit.timestamp))
        expected.append(df1)
        time.sleep(1)
        df2 = pd.DataFrame(
            {"d": range(x + 1, l + x + 1), "e": range(x + 2, l + x + 2), "f": range(x + 3, l + x + 3)}, index=idx
        )
        lib.write(symbol, df2)

        remove_most_recent_version_key(lib, symbol)

    vits = lib.batch_read(symbols, as_ofs=write_times)
    for x in range(num_items):
        assert_equal(vits[symbols[x]].data, expected[x])
