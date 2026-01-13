"""
Copyright 2026 Man Group Operations Limited

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
from collections import namedtuple
from datetime import datetime

from numpy.testing import assert_array_equal
from pytz import timezone

from arcticdb.exceptions import (
    ArcticDbNotYetImplemented,
    InternalException,
    UserInputException,
    ArcticException,
)
from arcticdb import QueryBuilder
from arcticdb.flattener import Flattener
from arcticdb.util.utils import generate_random_numpy_array, generate_random_series
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store._store import VersionedItem
from arcticdb_ext.exceptions import _ArcticLegacyCompatibilityException, StorageException
from arcticdb_ext.storage import KeyType, NoDataFoundException
from arcticdb_ext.version_store import (
    NoSuchVersionException,
    StreamDescriptorMismatch,
    ManualClockVersionStore,
    DataError,
)
from arcticdb.util.test import (
    sample_dataframe,
    sample_dataframe_only_strings,
    get_sample_dataframe,
    assert_frame_equal,
    assert_series_equal,
    config_context,
    distinct_timestamps,
)
from tests.conftest import Marks
from tests.util.date import DateRange
from arcticdb.util.test import equals
from arcticdb.version_store._store import resolve_defaults
from tests.util.mark import xfail_azure_chars
from tests.util.marking import marks


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


@pytest.mark.storage
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


@pytest.mark.storage
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
@pytest.mark.storage
def test_s3_breaking_chars(object_version_store, breaking_char):
    """Test that chars that are not supported are raising the appropriate exception and that we fail on write without
    corrupting the db.
    """
    xfail_azure_chars(object_version_store, breaking_char)
    sym = f"prefix{breaking_char}postfix"
    df = sample_dataframe()
    with pytest.raises(UserInputException):
        object_version_store.write(sym, df)

    assert sym not in object_version_store.list_symbols()


@pytest.mark.parametrize("breaking_char", [chr(0), "\0", "*", "<", ">"])
@pytest.mark.storage
def test_s3_breaking_chars_staged(object_version_store, breaking_char):
    """Test that chars that are not supported are raising the appropriate exception and that we fail on write without
    corrupting the db.
    """
    xfail_azure_chars(object_version_store, breaking_char)
    sym = f"prefix{breaking_char}postfix"
    df = sample_dataframe()
    with pytest.raises(UserInputException):
        object_version_store.write(sym, df, incomplete=True)

    assert not object_version_store.list_symbols_with_incomplete_data()


@pytest.mark.parametrize("unhandled_char", [chr(0), chr(30), chr(127), chr(128)])
@pytest.mark.storage
def test_unhandled_chars_default(object_version_store, unhandled_char):
    """Test that by default, the problematic chars are raising an exception"""
    xfail_azure_chars(object_version_store, unhandled_char)
    sym = f"prefix{unhandled_char}postfix"
    df = sample_dataframe()
    with pytest.raises(UserInputException):
        object_version_store.write(sym, df)
    syms = object_version_store.list_symbols()
    assert sym not in syms


@pytest.mark.parametrize("sym", [chr(0), chr(30), chr(127), chr(128), "", "l" * 255])
@pytest.mark.storage
def test_unhandled_chars_staged_data(object_version_store, sym):
    xfail_azure_chars(object_version_store, sym)
    """Test that by default, the problematic chars are raising an exception at staging time."""
    df = sample_dataframe()
    with pytest.raises(UserInputException):
        object_version_store.write(sym, df, parallel=True)
    assert not object_version_store.list_symbols_with_incomplete_data()


@pytest.mark.parametrize(
    "snap", [chr(32), chr(33), chr(125), chr(126), "fine", "l" * 254, chr(127), chr(128), "l" * 255, "*", "<", ">"]
)
def test_snapshot_names(object_version_store, snap):
    """We validate against these snapshot names in the V2 API, but let them go through in the V1 API to avoid disruption
    to legacy users on the V1 API."""
    xfail_azure_chars(object_version_store, snap)
    df = sample_dataframe()
    object_version_store.write("sym", df)
    object_version_store.snapshot(snap)
    object_version_store.delete("sym")
    assert not object_version_store.has_symbol("sym")
    assert object_version_store.list_snapshots() == {snap: None}
    assert_frame_equal(object_version_store.read("sym", as_of=snap).data, df)


def test_empty_snapshot_name_not_allowed(object_version_store):
    df = sample_dataframe()
    object_version_store.write("sym", df)
    with pytest.raises(UserInputException):
        object_version_store.snapshot("")
    assert not object_version_store.list_snapshots()


@pytest.mark.parametrize("unhandled_char", [chr(0), chr(30), chr(127), chr(128)])
@pytest.mark.storage
def test_unhandled_chars_update_upsert(object_version_store, unhandled_char):
    xfail_azure_chars(object_version_store, unhandled_char)
    df = pd.DataFrame(
        {"col_1": ["a", "b"], "col_2": [0.1, 0.2]}, index=[pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")]
    )
    sym = f"prefix{unhandled_char}postfix"
    with pytest.raises(UserInputException):
        object_version_store.update(sym, df, upsert=True)
    syms = object_version_store.list_symbols()
    assert sym not in syms


@pytest.mark.parametrize("unhandled_char", [chr(0), chr(30), chr(127), chr(128)])
@pytest.mark.storage
def test_unhandled_chars_append(object_version_store, unhandled_char):
    xfail_azure_chars(object_version_store, unhandled_char)
    df = pd.DataFrame(
        {"col_1": ["a", "b"], "col_2": [0.1, 0.2]}, index=[pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")]
    )
    sym = f"prefix{unhandled_char}postfix"
    with pytest.raises(UserInputException):
        object_version_store.append(sym, df)
    syms = object_version_store.list_symbols()
    assert sym not in syms


@pytest.mark.parametrize("unhandled_char", [chr(127), chr(128)])
@pytest.mark.storage
def test_unhandled_chars_already_present_write(object_version_store, three_col_df, unhandled_char):
    xfail_azure_chars(object_version_store, unhandled_char)
    sym = f"prefix{unhandled_char}postfix"
    with config_context("VersionStore.NoStrictSymbolCheck", 1):
        object_version_store.write(sym, three_col_df())
    vitem = object_version_store.read(sym)
    object_version_store.write(sym, three_col_df(1))
    new_vitem = object_version_store.read(sym)
    assert not vitem.data.equals(new_vitem.data)

    # Should still be able to use staged writes for pre-existing symbols that would fail the validation
    staged_data = three_col_df(2)
    object_version_store.write(sym, staged_data, parallel=True)
    object_version_store.compact_incomplete(sym, append=False, convert_int_to_float=False)

    assert_frame_equal(object_version_store.read(sym).data, staged_data)


@pytest.mark.parametrize("unhandled_char", [chr(127), chr(128)])
@pytest.mark.parametrize("staged", (True, False))
@pytest.mark.storage
def test_unhandled_chars_already_present_on_deleted_symbol(object_version_store, three_col_df, unhandled_char, staged):
    xfail_azure_chars(object_version_store, unhandled_char)
    sym = f"prefix{unhandled_char}postfix"
    with pytest.raises(UserInputException):
        object_version_store.write(
            sym, three_col_df()
        )  # reasonableness check - the sym we're using should fail the validation checks

    with config_context("VersionStore.NoStrictSymbolCheck", 1):
        object_version_store.write(sym, three_col_df())

    object_version_store.delete(sym)

    # Should still be able to use writes for pre-existing symbols that would fail the validation, even if they are deleted
    data = three_col_df(2)
    if staged:
        object_version_store.write(sym, data, parallel=True)
        object_version_store.compact_incomplete(sym, append=False, convert_int_to_float=False)
    else:
        object_version_store.write(sym, data)

    assert_frame_equal(object_version_store.read(sym).data, data)


@pytest.mark.parametrize("unhandled_char", [chr(127), chr(128)])
@pytest.mark.storage
def test_unhandled_chars_already_present_append(object_version_store, three_col_df, unhandled_char):
    xfail_azure_chars(object_version_store, unhandled_char)
    sym = f"prefix{unhandled_char}postfix"
    with config_context("VersionStore.NoStrictSymbolCheck", 1):
        object_version_store.write(sym, three_col_df(1))

    vitem = object_version_store.read(sym)
    object_version_store.append(sym, three_col_df(10))
    new_vitem = object_version_store.read(sym)
    assert not vitem.data.equals(new_vitem.data)
    assert len(vitem.data) != len(new_vitem.data)


@pytest.mark.parametrize("unhandled_char", [chr(127), chr(128)])
@pytest.mark.storage
def test_unhandled_chars_already_present_update(object_version_store, unhandled_char):
    xfail_azure_chars(object_version_store, unhandled_char)
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
@pytest.mark.storage
def test_large_symbols(basic_store_no_symbol_list):
    # The following restrictions should be checked in the cpp layer's name_validation
    MAX_SYMBOL_SIZE = 254
    # TODO: Make too long name on LMDB raise a friendlier UserInputException (instead of InternalException [E_INVALID_ARGUMENT])
    with pytest.raises((UserInputException, InternalException)):
        basic_store_no_symbol_list.write("a" * (MAX_SYMBOL_SIZE + 1), 1)

    valid_sized_sym = "a" * MAX_SYMBOL_SIZE
    basic_store_no_symbol_list.write(valid_sized_sym, 1)
    assert basic_store_no_symbol_list.read(valid_sized_sym).data == 1


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


def check_write_and_prune_previous_version_keys(lib_tool, sym, ver_key, latest_version_id=2):
    assert ver_key.type == KeyType.VERSION
    keys_in_tombstone_ver = lib_tool.read_to_keys(ver_key)
    assert len(keys_in_tombstone_ver) == 3
    assert keys_in_tombstone_ver[0].type == KeyType.TABLE_INDEX
    assert keys_in_tombstone_ver[1].type == KeyType.TOMBSTONE_ALL
    assert keys_in_tombstone_ver[2].type == KeyType.VERSION
    assert keys_in_tombstone_ver[0].version_id == latest_version_id
    assert keys_in_tombstone_ver[1].version_id == latest_version_id - 1
    assert keys_in_tombstone_ver[2].version_id == latest_version_id - 1


def check_append_ref_key_structure(keys_in_ref, latest_version_id=1):
    """
    The ref key for an append/write(without prune) should have the following structure:
    - TABLE_INDEX: latest index
    - TABLE_INDEX: previous index
    - VERSION: latest version
    This is due to an optimisation that we have, for more info see:
    https://github.com/man-group/ArcticDB/pull/1355
    """
    assert len(keys_in_ref) == 3
    assert keys_in_ref[0].type == KeyType.TABLE_INDEX
    assert keys_in_ref[0].version_id == latest_version_id
    assert keys_in_ref[1].type == KeyType.TABLE_INDEX
    assert keys_in_ref[1].version_id == latest_version_id - 1
    assert keys_in_ref[2].type == KeyType.VERSION
    assert keys_in_ref[2].version_id == latest_version_id


def check_regular_write_ref_key_structure(keys_in_ref, latest_version_id=1):
    """
    The ref key for after a regular write with prune should have the following structure:
    - TABLE_INDEX: latest index
    - VERSION: latest version
    """
    assert len(keys_in_ref) == 2
    assert keys_in_ref[0].type == KeyType.TABLE_INDEX
    assert keys_in_ref[0].version_id == latest_version_id
    assert keys_in_ref[1].type == KeyType.VERSION
    assert keys_in_ref[1].version_id == latest_version_id


@pytest.mark.storage
def test_prune_previous_versions_write(basic_store, sym):
    """Verify that the batch write method correctly prunes previous versions when the corresponding option is specified."""
    # Given
    lib = basic_store
    lib_tool = lib.library_tool()
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_0": ["e", "f"]}, index=pd.date_range("2000-01-05", periods=2))

    # When
    lib.write(sym, df0)
    lib.write(sym, df1)
    ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
    keys_in_ref = lib_tool.read_to_keys(ref_key)
    assert len(lib.list_versions(sym)) == 2
    check_append_ref_key_structure(keys_in_ref)

    lib.write(sym, df2, prune_previous_version=True)

    # Then - only latest version and keys should survive
    assert len(lib.list_versions(sym)) == 1
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == 1
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == 1

    ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
    keys_in_ref = lib_tool.read_to_keys(ref_key)
    check_regular_write_ref_key_structure(keys_in_ref, latest_version_id=2)

    # Then - we got 2 version keys per symbol: version 0, version 1 that contains the tombstone_all
    keys_for_sym = lib_tool.find_keys_for_id(KeyType.VERSION, sym)

    assert len(keys_for_sym) == 3
    latest_ver_key = max(keys_for_sym, key=lambda x: x.version_id)
    check_write_and_prune_previous_version_keys(lib_tool, sym, latest_ver_key)
    # Then - we got 3 symbol keys: 1 for each of the writes
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 3


@pytest.mark.storage
@pytest.mark.parametrize("versions_to_delete", [[0], [1], [0, 1]])
def test_tombstone_versions_ref_key_structure(basic_store, sym, versions_to_delete):
    """Verify that the batch write method correctly prunes previous versions when the corresponding option is specified."""
    # Given
    lib = basic_store
    lib_tool = lib.library_tool()
    df = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    num_writes = 3

    # When
    for _ in range(num_writes):
        lib.write(sym, df)
    lib.delete_versions(sym, versions=versions_to_delete)

    # Then - only latest version and keys should survive
    assert len(lib.list_versions(sym)) == num_writes - len(versions_to_delete)
    assert len(lib_tool.find_keys(KeyType.TABLE_INDEX)) == num_writes - len(versions_to_delete)
    assert len(lib_tool.find_keys(KeyType.TABLE_DATA)) == num_writes - len(versions_to_delete)

    ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
    keys_in_ref = lib_tool.read_to_keys(ref_key)
    assert len(keys_in_ref) == 2
    # the tombstone key and version key should be the highest version id
    assert keys_in_ref[0].type == KeyType.TOMBSTONE
    assert keys_in_ref[0].version_id == max(versions_to_delete)
    assert keys_in_ref[1].type == KeyType.VERSION
    assert keys_in_ref[1].version_id == max(versions_to_delete)
    keys_in_ver = lib_tool.read_to_keys(keys_in_ref[1])
    assert len(keys_in_ver) == len(versions_to_delete) + 1
    for key in keys_in_ver[:-1]:
        assert key.type == KeyType.TOMBSTONE
        assert key.version_id in versions_to_delete

    # the last key should be the version and it should point to the previous version
    assert keys_in_ver[-1].type == KeyType.VERSION
    assert keys_in_ver[-1].version_id == num_writes - 1


@pytest.mark.storage
def test_prune_previous_versions_write_batch(basic_store):
    """Verify that the batch write method correctly prunes previous versions when the corresponding option is specified."""
    # Given
    lib = basic_store
    lib_tool = lib.library_tool()
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    syms = [sym1, sym2]
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_0": ["e", "f"]}, index=pd.date_range("2000-01-05", periods=2))

    # When
    lib.batch_write(syms, [df0, df0])
    lib.batch_write(syms, [df1, df1])

    for sym in syms:
        ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
        keys_in_ref = lib_tool.read_to_keys(ref_key)
        assert len(lib.list_versions(sym)) == 2
        check_append_ref_key_structure(keys_in_ref)

    lib.batch_write(syms, [df2, df2], prune_previous_version=True)
    for sym in syms:
        ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
        keys_in_ref = lib_tool.read_to_keys(ref_key)
        assert len(lib.list_versions(sym)) == 1
        check_regular_write_ref_key_structure(keys_in_ref, latest_version_id=2)

        # Then - only latest version and keys should survive
        assert len(lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, sym)) == 1
        assert len(lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)) == 1

        # Then - we got 2 version keys per symbol: version 0, version 1 that contains the tombstone_all
        keys_for_sym = lib_tool.find_keys_for_id(KeyType.VERSION, sym)

        assert len(keys_for_sym) == 3
        latest_ver_key = max(keys_for_sym, key=lambda x: x.version_id)
        check_write_and_prune_previous_version_keys(lib_tool, sym, latest_ver_key)
    # Then - we got 6 symbol keys: 1 for each of the writes
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 6


@pytest.mark.storage
def test_prune_previous_versions_batch_write_metadata(basic_store):
    """Verify that the batch write metadata method correctly prunes previous versions when the corresponding option is specified."""
    # Given
    lib = basic_store
    lib_tool = lib.library_tool()
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    syms = [sym1, sym2]
    meta0 = {"a": 0}
    meta1 = {"a": 1}
    meta2 = {"a": 2}

    # When
    lib.batch_write([sym1, sym2], [None, None], metadata_vector=[meta0, meta0])
    lib.batch_write([sym1, sym2], [None, None], metadata_vector=[meta1, meta1])
    for sym in syms:
        ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
        keys_in_ref = lib_tool.read_to_keys(ref_key)
        assert len(lib.list_versions(sym)) == 2
        check_append_ref_key_structure(keys_in_ref)

    lib.batch_write_metadata([sym1, sym2], [meta2, meta2], prune_previous_version=True)

    for sym in syms:
        ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
        keys_in_ref = lib_tool.read_to_keys(ref_key)
        assert len(lib.list_versions(sym)) == 1
        check_regular_write_ref_key_structure(keys_in_ref, latest_version_id=2)

        # Then - only latest version and keys should survive
        assert len(lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, sym)) == 1
        assert len(lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)) == 1

        # Then - we got 2 version keys per symbol: version 0, version 1 that contains the tombstone_all
        keys_for_sym = lib_tool.find_keys_for_id(KeyType.VERSION, sym)

        assert len(keys_for_sym) == 3
        latest_ver_key = max(keys_for_sym, key=lambda x: x.version_id)
        check_write_and_prune_previous_version_keys(lib_tool, sym, latest_ver_key)

    # Then - we got 4 symbol keys: 1 for each of the batch_write calls
    # batch_write_metadata should not create any new symbol keys
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 4


@pytest.mark.storage
def test_prune_previous_versions_append_batch(basic_store):
    """Verify that the batch append method correctly prunes previous versions when the corresponding option is specified."""
    # Given
    lib = basic_store
    lib_tool = lib.library_tool()
    sym1 = "test_symbol1"
    sym2 = "test_symbol2"
    syms = [sym1, sym2]
    df0 = pd.DataFrame({"col_0": ["a", "b"]}, index=pd.date_range("2000-01-01", periods=2))
    df1 = pd.DataFrame({"col_0": ["c", "d"]}, index=pd.date_range("2000-01-03", periods=2))
    df2 = pd.DataFrame({"col_0": ["e", "f"]}, index=pd.date_range("2000-01-05", periods=2))

    # When
    lib.batch_write(syms, [df0, df0])
    lib.batch_append(syms, [df1, df1])

    for sym in syms:
        ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
        keys_in_ref = lib_tool.read_to_keys(ref_key)
        assert len(lib.list_versions(sym)) == 2
        check_append_ref_key_structure(keys_in_ref)

    lib.batch_append(syms, [df2, df2], prune_previous_version=True)

    for sym in syms:
        ref_key = lib_tool.find_keys_for_id(KeyType.VERSION_REF, sym)[0]
        keys_in_ref = lib_tool.read_to_keys(ref_key)
        assert len(lib.list_versions(sym)) == 1
        check_regular_write_ref_key_structure(keys_in_ref, latest_version_id=2)

        # Then - only latest version and index keys should survive. Data keys remain the same
        assert len(lib.list_versions(sym)) == 1
        assert len(lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, sym)) == 1
        assert len(lib_tool.find_keys_for_id(KeyType.TABLE_DATA, sym)) == 3

        # Then - we got 2 version keys per symbol: version 0, version 1 that contains the tombstone_all
        keys_for_sym = lib_tool.find_keys_for_id(KeyType.VERSION, sym)

        assert len(keys_for_sym) == 3
        latest_ver_key = max(keys_for_sym, key=lambda x: x.version_id)
        check_write_and_prune_previous_version_keys(lib_tool, sym, latest_ver_key)
    # Then - we got 6 symbol keys: 1 for each of the writes
    assert len(lib_tool.find_keys(KeyType.SYMBOL_LIST)) == 6


def test_batch_append_after_delete_upsert(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    lib.write("sym", 1)
    lib.write("sym1", 1)
    lib.write("sym1", 1)
    lib.batch_delete_symbols(["sym", "sym1"])

    df = sample_dataframe()
    df1 = sample_dataframe()
    results = lib.batch_append(["sym", "sym1"], [df, df1])
    assert results[0].version == 1
    assert results[1].version == 2
    assert_frame_equal(lib.read("sym").data, df)
    assert_frame_equal(lib.read("sym1").data, df1)


@pytest.mark.storage
def test_deleting_unknown_symbol(basic_store, symbol):
    df = sample_dataframe()

    basic_store.write(symbol, df, metadata={"something": "something"})

    assert_equal(basic_store.read(symbol).data, df)

    # Should not raise.
    basic_store.delete("does_not_exist")


@pytest.mark.storage
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


@pytest.mark.storage
def test_list_symbols_prefix(object_version_store):
    blahs = ["blah_asdf201901", "blah_asdf201802", "blah_asdf201803", "blah_asdf201903"]
    nahs = ["nah_asdf201801", "nah_asdf201802", "nah_asdf201803"]

    for sym in itertools.chain(blahs, nahs):
        object_version_store.write(sym, sample_dataframe(10))

    assert set(object_version_store.list_symbols(prefix="blah_")) == set(blahs)
    assert set(object_version_store.list_symbols(prefix="nah_")) == set(nahs)


@pytest.mark.storage
def test_mixed_df_without_pickling_enabled(basic_store):
    mixed_type_df = pd.DataFrame({"a": [1, 2, "a"]})
    with pytest.raises(Exception):
        basic_store.write("sym", mixed_type_df)


@pytest.mark.storage
def test_dataframe_fallback_with_pickling_enabled(basic_store_allows_pickling):
    mixed_type_df = pd.DataFrame({"a": [1, 2, "a", None]})
    basic_store_allows_pickling.write("sym", mixed_type_df)


@pytest.mark.storage
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


@pytest.mark.parametrize("use_date_range_clause", [True, False])
@marks([Marks.pipeline, Marks.storage])
def test_date_range(basic_store, use_date_range_clause, any_output_format):
    basic_store._set_output_format_for_pipeline_tests(any_output_format)
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


@pytest.mark.parametrize("use_date_range_clause", [True, False])
@marks([Marks.pipeline, Marks.storage])
def test_date_range_none(basic_store, use_date_range_clause, any_output_format):
    basic_store._set_output_format_for_pipeline_tests(any_output_format)
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


@pytest.mark.parametrize("use_date_range_clause", [True, False])
@marks([Marks.pipeline, Marks.storage])
def test_date_range_start_equals_end(basic_store, use_date_range_clause, any_output_format):
    basic_store._set_output_format_for_pipeline_tests(any_output_format)
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


@pytest.mark.parametrize("use_date_range_clause", [True, False])
@marks([Marks.pipeline, Marks.storage])
def test_date_range_row_sliced(basic_store_tiny_segment, use_date_range_clause, any_output_format):
    lib = basic_store_tiny_segment
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


@pytest.mark.parametrize("index_name", ["blah", None, "col1"])
@pytest.mark.storage
def test_get_info(basic_store, index_name):
    sym = "get_info_test"
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    df.index.name = index_name
    basic_store.write(sym, df)
    info = basic_store.get_info(sym)
    assert int(info["rows"]) == 10
    assert info["type"] == "pandasdf"
    assert info["col_names"]["columns"] == ["col1"]
    assert info["col_names"]["index"] == [index_name]
    assert info["index_type"] == "index"


@pytest.mark.parametrize("index_name", ["blah", None, "col1"])
@pytest.mark.storage
def test_get_info_series(basic_store, index_name):
    """Index names are not handled very well at the moment for series. Since the normalization metadata is complex,
    and any changes need to be backwards compatible with old readers, it does not seem worthwhile changing these
    odd behaviours now."""
    sym = "get_info_series_test"
    series = pd.Series(np.arange(10), name="col1", index=pd.date_range(pd.Timestamp(0), periods=10))
    series.index.name = index_name
    basic_store.write(sym, series)
    info = basic_store.get_info(sym)
    assert int(info["rows"]) == 10
    assert info["type"] == "pandasseries"
    assert info["col_names"]["columns"] == [index_name, "col1"] if index_name else ["col1"]
    assert info["col_names"]["index"] == []
    assert info["index_type"] == "NA"


@pytest.mark.storage
@pytest.mark.parametrize("index_name", ["blah", None])
def test_get_info_series_multiindex(basic_store, index_name):
    """Index names are not handled very well at the moment for series. Since the normalization metadata is complex,
    and any changes need to be backwards compatible with old readers, it does not seem worthwhile changing these
    odd behaviours now."""
    sym = "get_info_series_test"
    dtidx = pd.date_range(pd.Timestamp("2016-01-01"), periods=10)
    vals = np.arange(10, dtype=np.uint32)
    series = pd.Series(np.arange(10), name="col1", index=pd.MultiIndex.from_arrays([dtidx, vals]))
    series.index.name = index_name
    basic_store.write(sym, series)
    info = basic_store.get_info(sym)
    assert int(info["rows"]) == 10
    assert info["type"] == "pandasseries"
    assert info["col_names"]["columns"] == ["index", "__fkidx__1", "col1"] if index_name else ["col1"]
    assert info["col_names"]["index"] == []
    assert info["index_type"] == "NA"


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_get_info_version_no_columns_nat(basic_store):
    sym = "test_get_info_version_no_columns_nat"
    column_names = ["a", "b", "c"]
    df = pd.DataFrame(columns=column_names)
    df["b"] = df["b"].astype("int64")
    basic_store.write(sym, df, dynamic_strings=True, coerce_columns={"a": float, "b": int, "c": str})
    info = basic_store.get_info(sym)
    assert np.isnat(info["date_range"][0])
    assert np.isnat(info["date_range"][1])


@pytest.mark.storage
def test_get_info_version_empty_nat(basic_store):
    sym = "test_get_info_version_empty_nat"
    basic_store.write(sym, pd.DataFrame())
    info = basic_store.get_info(sym)
    assert np.isnat(info["date_range"][0])
    assert np.isnat(info["date_range"][1])


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_update_time(basic_store):
    lib: NativeVersionStore = basic_store

    nparr = generate_random_numpy_array(50, np.float32, seed=None)
    series = generate_random_series(np.uint64, 50, "numbers")
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=pd.date_range(pd.Timestamp(0), periods=10))
    lib.write("sym1", nparr)
    lib.write("sym1", series)
    lib.snapshot("snap")
    lib.write("sym1", df)

    # Negative number for as_of works as expected (dataframes)
    assert lib.update_time("sym1") == lib.update_time("sym1", -1) == lib.update_time("sym1", 2)
    # Snapshots are accepted (series)
    assert lib.update_time("sym1", 1) == lib.update_time("sym1", -2) == lib.update_time("sym1", "snap")
    # and now check for np array
    assert lib.update_time("sym1", 0) == lib.update_time("sym1", -3)

    # Times are ordered
    assert lib.update_time("sym1") > lib.update_time("sym1", 1) > lib.update_time("sym1", 0)

    # Correct exception thrown if symbol does not exist
    with pytest.raises(NoDataFoundException):
        lib.update_time("sym12")

    # Correct exception thrown if version does not exist
    with pytest.raises(NoDataFoundException):
        lib.update_time("sym1", 11)


@pytest.mark.storage
@pytest.mark.parametrize(
    "index_names",
    [("blah", None), (None, None), (None, "blah"), ("blah1", "blah2"), ("col1", "col2"), ("col1", "col1")],
)
def test_get_info_multi_index(basic_store, index_names):
    dtidx = pd.date_range(pd.Timestamp("2016-01-01"), periods=3)
    vals = np.arange(3, dtype=np.uint32)
    multi_df = pd.DataFrame({"col1": [1, 4, 9]}, index=pd.MultiIndex.from_arrays([dtidx, vals]))
    multi_df.index.set_names(index_names, inplace=True)
    sym = "multi_info_test"
    basic_store.write(sym, multi_df)
    info = basic_store.get_info(sym)
    assert int(info["rows"]) == 3
    assert info["type"] == "pandasdf"
    assert info["col_names"]["columns"] == ["col1"]
    actual_index_names = info["col_names"]["index"]
    assert len(actual_index_names) == 2
    assert actual_index_names == list(index_names)
    assert info["index_type"] == "multi_index"


@pytest.mark.storage
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


@pytest.mark.storage
def test_empty_pd_series(basic_store):
    sym = "empty_s"
    series = pd.Series()
    basic_store.write(sym, series)
    assert basic_store.read(sym).data.empty
    # basic_store.update(sym, series)
    # assert basic_store.read(sym).data.empty
    basic_store.append(sym, series)
    assert basic_store.read(sym).data.empty


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_is_pickled(basic_store):
    will_be_pickled = [1, 2, 3]
    basic_store.write("blah", will_be_pickled)
    assert basic_store.is_symbol_pickled("blah") is True

    df = pd.DataFrame({"a": np.arange(3)})
    basic_store.write("normal", df)
    assert basic_store.is_symbol_pickled("normal") is False


@pytest.mark.storage
def test_is_pickled_by_version(basic_store):
    symbol = "test"
    will_be_pickled = [1, 2, 3]
    basic_store.write(symbol, will_be_pickled)

    not_pickled = pd.DataFrame({"a": np.arange(3)})
    basic_store.write(symbol, not_pickled)

    assert basic_store.is_symbol_pickled(symbol) is False
    assert basic_store.is_symbol_pickled(symbol, 0) is True
    assert basic_store.is_symbol_pickled(symbol, 1) is False


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_list_versions_deleted_flag(basic_store):
    basic_store.write("symbol", pd.DataFrame(), metadata=1)
    basic_store.write("symbol", pd.DataFrame(), metadata=2, prune_previous_version=False)
    basic_store.write("symbol", pd.DataFrame(), metadata=3, prune_previous_version=False)
    basic_store.snapshot("snapshot")
    basic_store.write("symbol", pd.DataFrame(), metadata=4, prune_previous_version=False)

    versions = basic_store.list_versions("symbol")
    assert len(versions) == 4
    versions = sorted(versions, key=lambda v: v["version"])
    assert not versions[2]["deleted"]
    assert versions[2]["snapshots"] == ["snapshot"]

    basic_store.delete_version("symbol", 2)
    versions = basic_store.list_versions("symbol")
    assert len(versions) == 4
    versions = sorted(versions, key=lambda v: v["version"])

    assert not versions[0]["deleted"]
    assert not versions[0]["snapshots"]
    assert not versions[1]["deleted"]
    assert not versions[1]["snapshots"]
    assert versions[2]["deleted"]
    assert versions[2]["snapshots"] == ["snapshot"]
    assert not versions[3]["deleted"]
    assert not versions[3]["snapshots"]

    # Test that deleting a set of versions doesn't affect the other versions
    basic_store.delete_versions("symbol", [0, 1])
    versions = basic_store.list_versions("symbol")
    assert len(versions) == 2
    versions = sorted(versions, key=lambda v: v["version"])
    assert versions[0]["version"] == 2
    assert versions[0]["deleted"]
    assert versions[0]["snapshots"] == ["snapshot"]
    assert versions[1]["version"] == 3
    assert not versions[1]["deleted"]
    assert not versions[1]["snapshots"]


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_dynamic_strings(basic_store):
    row = pd.Series(["A", "B", "C", "Aaba", "Baca", "CABA", "dog", "cat"])
    df = pd.DataFrame({"x": row})
    basic_store.write("strings", df, dynamic_strings=True)
    vit = basic_store.read("strings")
    assert_equal(vit.data, df)


@pytest.mark.storage
def test_dynamic_strings_non_contiguous(basic_store):
    df = sample_dataframe_only_strings(100, 0, 100)
    series = df.iloc[-1]
    series.name = None
    basic_store.write("strings", series, dynamic_strings=True)
    vit = basic_store.read("strings")
    assert_series_equal(vit.data, series)


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_dynamic_strings_with_all_nones(basic_store):
    df = pd.DataFrame({"x": [None, None]})
    basic_store.write("strings", df, dynamic_strings=True)
    data = basic_store.read("strings")
    assert data.data["x"][0] is None
    assert data.data["x"][1] is None


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


def test_named_tuple_flattening_rejected():
    fl = Flattener()
    SomeThing = namedtuple("SomeThing", "prop another_prop")
    nt = SomeThing(1, 2)
    assert fl.can_flatten(nt) is False


@pytest.mark.storage
def test_batch_operations(object_version_store_prune_previous):
    multi_data = {"sym1": np.arange(8), "sym2": np.arange(9), "sym3": np.arange(10)}

    for _ in range(10):
        object_version_store_prune_previous.batch_write(list(multi_data.keys()), list(multi_data.values()))
        result = object_version_store_prune_previous.batch_read(list(multi_data.keys()))
        assert len(result) == 3
        equals(result["sym1"].data, np.arange(8))
        equals(result["sym2"].data, np.arange(9))
        equals(result["sym3"].data, np.arange(10))


@pytest.mark.storage
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


@pytest.mark.storage
def test_batch_write_then_read(basic_store):
    symbol = "sym_d_1"
    data = pd.Series(index=[0], data=[1])

    # Write, then delete a symbol
    basic_store.write(symbol=symbol, data=data)
    basic_store.delete(symbol)

    # Batch write the same data to the same symbol
    basic_store.batch_write(symbols=[symbol], data_vector=[data])
    basic_store.read(symbol)


@pytest.mark.storage
def test_batch_write_then_list_symbol_without_cache(basic_store_factory):
    factory = basic_store_factory
    lib = factory(symbol_list=False, segment_row_size=10)
    df = pd.DataFrame([1, 2, 3])
    for idx in range(10):
        lib.version_store.clear()
        symbols = [f"s{idx}-{i}" for i in [1, 2, 3]]
        lib.batch_write(symbols=symbols, data_vector=[df, df, df])
        assert set(lib.list_symbols()) == set(symbols)


@marks([Marks.storage, Marks.dedup])
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_batch_read_metadata_multi_missing_keys(basic_store):
    lib = basic_store
    lib_tool = lib.library_tool()

    lib.write("s1", 0, metadata={"s1": "metadata"})
    key_to_delete = lib_tool.find_keys_for_id(KeyType.TABLE_INDEX, "s1")[0]
    lib_tool.remove(key_to_delete)

    with pytest.raises(StorageException):
        _ = lib.batch_read_metadata_multi(["s1"], [None])


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_write_composite_data_with_user_meta(basic_store):
    multi_data = {"sym1": np.arange(8), "sym2": np.arange(9), "sym3": np.arange(10)}
    basic_store.write("sym", multi_data, metadata={"a": 1})
    vitem = basic_store.read("sym")
    assert vitem.metadata == {"a": 1}
    equals(vitem.data["sym1"], np.arange(8))


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_dataframe_with_NaN_in_timestamp_column(basic_store):
    normal_df = pd.DataFrame({"col": [pd.Timestamp("now"), pd.NaT]})
    basic_store.write("normal", normal_df)
    assert_equal(normal_df, basic_store.read("normal").data)


@pytest.mark.storage
def test_dataframe_with_nan_and_nat_in_timestamp_column(basic_store):
    df_with_NaN_mixed_in_ts = pd.DataFrame({"col": [pd.Timestamp("now"), pd.NaT, np.nan]})
    basic_store.write("mixed_nan", df_with_NaN_mixed_in_ts)
    returned_df = basic_store.read("mixed_nan").data
    # NaN will now be converted to NaT
    isinstance(returned_df["col"][2], type(pd.NaT))


@pytest.mark.storage
def test_dataframe_with_nan_and_nat_only(basic_store):
    df_with_nan_and_nat_only = pd.DataFrame({"col": [pd.NaT, pd.NaT, np.nan]})  # Sample will be pd.NaT
    basic_store.write("nan_nat", df_with_nan_and_nat_only)
    assert_equal(basic_store.read("nan_nat").data, pd.DataFrame({"col": [pd.NaT, pd.NaT, pd.NaT]}))


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_resolve_defaults(basic_store_factory):
    lib = basic_store_factory()
    proto_cfg = lib._lib_cfg.lib_desc.version.write_options
    assert resolve_defaults("recursive_normalizers", proto_cfg, False) is False
    os.environ["recursive_normalizers"] = "True"
    assert resolve_defaults("recursive_normalizers", proto_cfg, False, uppercase=False) is True

    lib2 = basic_store_factory(dynamic_strings=True, reuse_name=True)
    proto_cfg = lib2._lib_cfg.lib_desc.version.write_options
    assert resolve_defaults("dynamic_strings", proto_cfg, False) is True
    del os.environ["recursive_normalizers"]


@pytest.mark.storage
def test_batch_read_meta(basic_store_tombstone_and_sync_passive):
    lib = basic_store_tombstone_and_sync_passive
    for idx in range(10):
        lib.write("sym" + str(idx), idx, metadata={"meta": idx})

    results_dict = lib.batch_read_metadata(["sym" + str(idx) for idx in range(10)])
    assert results_dict["sym1"].metadata == {"meta": 1}
    assert results_dict["sym5"].metadata == {"meta": 5}

    assert lib.read_metadata("sym6").metadata == results_dict["sym6"].metadata

    assert results_dict["sym2"].data is None


@pytest.mark.storage
def test_batch_read_metadata_symbol_doesnt_exist(basic_store_tombstone_and_sync_passive):
    lib = basic_store_tombstone_and_sync_passive
    for idx in range(10):
        lib.write("sym" + str(idx), idx, metadata={"meta": idx})

    results_dict = lib.batch_read_metadata(["sym1", "sym_doesnotexist", "sym2"])

    assert results_dict["sym1"].metadata == {"meta": 1}
    assert "sym_doesnotexist" not in results_dict


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_get_timerange_for_symbol(basic_store, sym):
    lib = basic_store
    initial_timestamp = pd.Timestamp("2019-01-01")
    df = pd.DataFrame(data=np.arange(100), index=pd.date_range(initial_timestamp, periods=100))
    lib.write(sym, df)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == datetime(2019, 1, 1)


@pytest.mark.storage
def test_get_timerange_for_symbol_tz(basic_store, sym):
    lib = basic_store
    dt1 = timezone("US/Eastern").localize(datetime(2021, 4, 1))
    dt2 = timezone("US/Eastern").localize(datetime(2021, 4, 1, 3))
    dst = pd.DataFrame({"a": [0, 1]}, index=[dt1, dt2])
    lib.write(sym, dst)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == dt1
    assert maxts == dt2


@pytest.mark.storage
def test_get_timerange_for_symbol_dst(basic_store, sym):
    lib = basic_store
    dst = pd.DataFrame({"a": [0, 1]}, index=[datetime(2021, 4, 1), datetime(2021, 4, 1, 3)])
    lib.write(sym, dst)
    mints, maxts = lib.get_timerange_for_symbol(sym)
    assert mints == datetime(2021, 4, 1)
    assert maxts == datetime(2021, 4, 1, 3)


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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

    # We can supply only an array of symbols, including repeating symbols
    results_dict = lib.batch_read_metadata_multi(["sym1", "sym2", "sym1", "sym3", "sym2", "sym1", "sym1"])
    assert results_dict["sym1"][2].metadata == {"meta1": 3}
    assert len(results_dict["sym1"]) == 1
    assert results_dict["sym2"][3].metadata == {"meta2": 4}
    assert results_dict["sym3"][0].metadata == {"meta3": 1}

    # The lists are of different sizr
    with pytest.raises(ArcticException):
        results_dict = lib.batch_read_metadata_multi(["sym1", "sym2"], [0, 0, -2])

    # With negative number we can go back from current versions
    assert lib.batch_read_metadata_multi(["sym1", "sym1"], [-1, -2]) == lib.batch_read_metadata_multi(
        ["sym1", "sym1"], [2, 1]
    )

    # Check DataError is thrown when requesting non-existing version
    with pytest.raises(TypeError):  # Not a good error though - issue 10070002655
        results_dict = lib.batch_read_metadata_multi(["sym1"], [10])


@pytest.mark.storage
def test_list_symbols(basic_store):
    lib = basic_store

    lib.write("a", 1)
    lib.write("b", 1)

    assert set(lib.list_symbols()) == set(lib.list_symbols(use_symbol_list=False))
    assert set(lib.list_symbols()) == set(lib.list_symbols(use_symbol_list=True))

    assert set(lib.list_symbols(regex="a")) == set(lib.list_symbols(regex="a", use_symbol_list=False))


@pytest.mark.storage
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


@pytest.mark.storage
def test_read_empty_index(lmdb_version_store_v1):
    lib = lmdb_version_store_v1
    sym = "test_read_empty_index"
    lib.write(sym, pd.DataFrame())
    assert len(lib.read_index(sym)) == 0


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_dynamic_schema_similar_index_column(basic_store_dynamic_schema):
    lib = basic_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.Series(dr, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_series_equal(returned, date_series)


@pytest.mark.storage
def test_dynamic_schema_similar_index_column_dataframe(basic_store_dynamic_schema):
    lib = basic_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.DataFrame({"date": np.arange(len(dr))}, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_equal(returned, date_series)


@pytest.mark.storage
def test_dynamic_schema_similar_index_column_dataframe_multiple_col(basic_store_dynamic_schema):
    lib = basic_store_dynamic_schema
    dr = pd.date_range("2020-01-01", "2020-01-31", name="date")
    date_series = pd.DataFrame({"col": np.arange(len(dr)), "date": np.arange(len(dr))}, index=dr)
    lib.write("date_series", date_series)
    returned = lib.read("date_series").data
    assert_equal(returned, date_series)


@pytest.mark.storage
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


@pytest.mark.parametrize("batch", (True, False))
def test_restore_version_as_of(lmdb_version_store, batch):
    lib = lmdb_version_store
    sym = "test_restore_version_as_of"

    def restore(as_of):
        if batch:
            res = lib.batch_restore_version([sym], [as_of])
            assert len(res) == 1
            return res[0]
        else:
            return lib.restore_version(sym, as_of)

    lib.version_store = ManualClockVersionStore(lib._library)

    first_ts = pd.Timestamp(1000)
    ManualClockVersionStore.time = first_ts.value
    first_df = get_sample_dataframe(20, 4)
    lib.write(sym, first_df, metadata=1)

    second_ts = pd.Timestamp(2000)
    ManualClockVersionStore.time = second_ts.value
    second_df = get_sample_dataframe(16, 6)
    lib.write(sym, second_df, metadata=2)

    # Can we restore the first version?
    # When
    third_ts = pd.Timestamp(3000)
    ManualClockVersionStore.time = third_ts.value
    restore_item = restore(first_ts)

    # Then
    assert restore_item.version == 2
    assert restore_item.metadata == 1
    latest = lib.read(sym)
    assert_equal(latest.data, first_df)
    assert latest.metadata == 1

    # Can we restore the second version?
    # When
    ManualClockVersionStore.time = 4000
    restore_item = restore(second_ts)

    # Then
    assert restore_item.version == 3
    assert restore_item.metadata == 2
    latest = lib.read(sym)
    assert_equal(latest.data, second_df)
    assert latest.metadata == 2

    # Can we restore a version that was itself generated by a restore_version call?
    # When
    ManualClockVersionStore.time = 5000
    restore_item = restore(third_ts)

    # Then
    assert restore_item.version == 4
    assert restore_item.metadata == 1
    latest = lib.read(sym)
    assert_equal(latest.data, first_df)
    assert latest.metadata == 1

    assert sym in lib.list_symbols(use_symbol_list=True)
    assert sym in lib.list_symbols(use_symbol_list=False)


@pytest.mark.parametrize("batch", (True, False))
def test_restore_version_as_of_deleted_but_in_snapshot(lmdb_version_store, batch):
    """Test restoring a version that has been deleted but which is kept alive by a snapshot."""
    lib = lmdb_version_store
    sym = "test_restore_version_as_of_deleted_in_snapshot"

    def restore(as_of):
        if batch:
            res = lib.batch_restore_version([sym], [as_of])
            assert len(res) == 1
            return res[0]
        else:
            return lib.restore_version(sym, as_of)

    lib.version_store = ManualClockVersionStore(lib._library)

    first_ts = pd.Timestamp(1000)
    ManualClockVersionStore.time = first_ts.value
    first_df = get_sample_dataframe(20, 4)
    lib.write(sym, first_df, metadata=1)

    second_ts = pd.Timestamp(2000)
    ManualClockVersionStore.time = second_ts.value
    lib.snapshot("snap")
    ManualClockVersionStore.time = pd.Timestamp(2500).value
    lib.delete(sym)
    assert sym not in lib.list_symbols()
    lib.compact_symbol_list()  # important to compact the symbol list and throw away the "add" entry else conflict resolution
    # in the symbol list, that refers to the version chain, can hide bugs with the symbol list state

    third_ts = pd.Timestamp(3000)
    ManualClockVersionStore.time = third_ts.value

    restore_item = restore("snap")

    # Then
    assert restore_item.version == 1
    assert restore_item.metadata == 1
    latest = lib.read(sym)
    assert_equal(latest.data, first_df)
    assert latest.metadata == 1
    assert latest.version == 1

    assert sym in lib.list_symbols(use_symbol_list=True)
    assert sym in lib.list_symbols(use_symbol_list=False)


@pytest.mark.parametrize("batch", (True, False))
def test_restore_version_deleted(lmdb_version_store_delayed_deletes_v1, batch):
    lib = lmdb_version_store_delayed_deletes_v1
    sym = "test_restore_version_deleted"

    def restore(as_of):
        if batch:
            res = lib.batch_restore_version([sym], [as_of])
            assert len(res) == 1
            return res[0]
        else:
            return lib.restore_version(sym, as_of)

    lib.version_store = ManualClockVersionStore(lib._library)

    first_ts = pd.Timestamp(1000)
    ManualClockVersionStore.time = first_ts.value
    first_df = get_sample_dataframe(20, 4)
    lib.write(sym, first_df, metadata=1)

    ManualClockVersionStore.time = 1500
    lib.delete(sym)
    lib.compact_symbol_list()

    ManualClockVersionStore.time = 2000
    with pytest.raises(NoSuchVersionException):
        restore(first_ts)

    # Then
    assert sym not in lib.list_symbols(use_symbol_list=True)
    assert sym not in lib.list_symbols(use_symbol_list=False)


def test_batch_restore_version_mixed_as_ofs(lmdb_version_store):
    lib = lmdb_version_store
    syms = ["s1", "s2", "s3"]
    first_data = [get_sample_dataframe(), get_sample_dataframe(), get_sample_dataframe()]
    first_metadata = ["s1-1", "s2-1", "s3-1"]

    second_data = [get_sample_dataframe(), get_sample_dataframe(), get_sample_dataframe()]
    second_metadata = ["s1-2", "s2-2", "s3-2"]

    third_data = [get_sample_dataframe(), get_sample_dataframe(), get_sample_dataframe()]
    third_metadata = ["s1-3", "s2-3", "s3-3"]

    lib.version_store = ManualClockVersionStore(lib._library)

    first_ts = pd.Timestamp(1000)
    ManualClockVersionStore.time = first_ts.value
    lib.batch_write(syms, first_data, first_metadata)

    second_ts = pd.Timestamp(2000)
    ManualClockVersionStore.time = second_ts.value
    lib.batch_write(syms, second_data, second_metadata)
    lib.snapshot("snap")

    third_ts = pd.Timestamp(3000)
    ManualClockVersionStore.time = third_ts.value
    lib.batch_write(syms, third_data, third_metadata)

    # When
    ManualClockVersionStore.time = pd.Timestamp(4000).value
    res = lib.batch_restore_version(syms, [third_ts, 1, first_ts])

    # Then
    assert res[0].symbol == "s1"
    assert res[1].symbol == "s2"
    assert res[2].symbol == "s3"
    assert res[0].version == 2  # it was already the version being restored, so is left alone
    assert res[1].version == 3
    assert res[2].version == 3
    assert res[0].metadata == "s1-3"
    assert res[1].metadata == "s2-2"
    assert res[2].metadata == "s3-1"

    latest = lib.batch_read(syms)
    assert latest["s1"].version == 2
    assert latest["s2"].version == 3
    assert latest["s3"].version == 3
    assert_equal(latest["s1"].data, third_data[0])
    assert_equal(latest["s2"].data, second_data[1])
    assert_equal(latest["s3"].data, first_data[2])
    assert latest["s1"].metadata == "s1-3"
    assert latest["s2"].metadata == "s2-2"
    assert latest["s3"].metadata == "s3-1"

    # check restore from snapshot and negative ver number
    res = lib.batch_restore_version(syms, [-3, "snap", -1])

    # check returned data
    assert res[0].symbol == "s1"
    assert res[1].symbol == "s2"
    assert res[2].symbol == "s3"
    assert res[0].version == 3
    assert res[1].version == 4
    assert res[2].version == 3  # We restored last version (-1 is last)
    assert res[0].metadata == "s1-1"
    assert res[1].metadata == "s2-2"
    assert res[2].metadata == "s3-1"

    # check latest version of symbols from the read
    latest = lib.batch_read(syms)
    assert latest["s1"].version == 3
    assert latest["s2"].version == 4
    assert latest["s3"].version == 3
    assert_equal(latest["s1"].data, first_data[0])
    assert_equal(latest["s2"].data, second_data[1])
    assert_equal(latest["s3"].data, first_data[2])
    assert latest["s1"].metadata == "s1-1"
    assert latest["s2"].metadata == "s2-2"
    assert latest["s3"].metadata == "s3-1"


@pytest.mark.parametrize("bad_thing", ("symbol", "as_of", "duplicate"))
def test_batch_restore_version_bad_input_noop(lmdb_version_store, bad_thing):
    """Leave everything alone if anything in the batch is bad."""
    lib = lmdb_version_store
    syms = ["s1", "s2", "s3"]
    first_data = [get_sample_dataframe(), get_sample_dataframe(), get_sample_dataframe()]
    first_metadata = ["s1-1", "s2-1", "s3-1"]

    second_data = [get_sample_dataframe(), get_sample_dataframe(), get_sample_dataframe()]
    second_metadata = ["s1-2", "s2-2", "s3-2"]

    lib.version_store = ManualClockVersionStore(lib._library)

    first_ts = pd.Timestamp(1000)
    ManualClockVersionStore.time = first_ts.value
    lib.batch_write(syms, first_data, first_metadata)

    second_ts = pd.Timestamp(2000)
    ManualClockVersionStore.time = second_ts.value
    lib.batch_write(syms, second_data, second_metadata)

    # When
    ManualClockVersionStore.time = pd.Timestamp(3000).value
    if bad_thing == "symbol":
        restore_syms = ["s1", "s2", "bad"]
        as_ofs = [1, second_ts, first_ts]
        with pytest.raises(
            NoSuchVersionException, match=r"E_NO_SUCH_VERSION Could not find.*restore_version.*missing versions \[bad\]"
        ):
            lib.batch_restore_version(restore_syms, as_ofs)
    elif bad_thing == "as_of":
        as_ofs = [first_ts, second_ts, 7]
        with pytest.raises(
            NoSuchVersionException, match=r"E_NO_SUCH_VERSION Could not find.*restore_version.*missing versions \[s3\]"
        ):
            lib.batch_restore_version(syms, as_ofs)
    elif bad_thing == "duplicate":
        restore_syms = ["s1", "s1", "s2"]
        as_ofs = [1, second_ts, first_ts]
        with pytest.raises(
            UserInputException,
            match=r"E_INVALID_USER_ARGUMENT Duplicate symbols in restore_version.*more than once \[s1\]",
        ):
            lib.batch_restore_version(restore_syms, as_ofs)
    else:
        raise RuntimeError(f"Unexpected bad_thing={bad_thing}")

    # Then
    latest = lib.batch_read(syms)
    assert latest["s1"].version == 1
    assert latest["s2"].version == 1
    assert latest["s3"].version == 1
    assert_equal(latest["s1"].data, second_data[0])
    assert_equal(latest["s2"].data, second_data[1])
    assert_equal(latest["s3"].data, second_data[2])
    assert latest["s1"].metadata == "s1-2"
    assert latest["s2"].metadata == "s2-2"
    assert latest["s3"].metadata == "s3-2"


@pytest.mark.parametrize("ver", (3, "snap"))
def test_restore_version_not_found(basic_store, ver):
    lib: NativeVersionStore = basic_store
    lib.write("abc", 1)
    lib.write("abc", 2)
    lib.write("bcd", 9)
    lib.snapshot("snap", versions={"bcd": 0})
    with pytest.raises(NoSuchVersionException, match=r"E_NO_SUCH_VERSION.*Symbols with missing versions \[abc\]"):
        lib.restore_version("abc", ver)


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_name_method(basic_store_factory):

    for lib_name in ["my name", "1243"]:
        lib: NativeVersionStore = basic_store_factory(name=lib_name)
        assert lib_name == lib.name()
        lib.version_store.clear()


@pytest.mark.storage
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


@pytest.mark.storage
def test_batch_append_with_throw_exception(basic_store, three_col_df):
    multi_data = {"sym1": three_col_df(), "sym2": three_col_df(1)}
    with pytest.raises(NoSuchVersionException):
        basic_store.batch_append(
            list(multi_data.keys()),
            list(multi_data.values()),
            write_if_missing=False,
        )


@pytest.mark.parametrize("use_date_range_clause", [True, False])
@marks([Marks.pipeline, Marks.storage])
def test_batch_read_date_range(basic_store_tombstone_and_sync_passive, use_date_range_clause, any_output_format):
    lmdb_version_store = basic_store_tombstone_and_sync_passive
    lmdb_version_store._set_output_format_for_pipeline_tests(any_output_format)
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
@marks([Marks.pipeline])
def test_batch_read_row_range(lmdb_version_store_v1, use_row_range_clause, any_output_format):
    lib = lmdb_version_store_v1
    lib._set_output_format_for_pipeline_tests(any_output_format)
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


@pytest.mark.storage
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


@pytest.mark.storage
def test_batch_read_symbol_doesnt_exist(basic_store):
    sym1 = "sym1"
    sym2 = "sym2"
    basic_store.write(sym1, 1)
    with pytest.raises(NoDataFoundException):
        _ = basic_store.batch_read([sym1, sym2])


@pytest.mark.storage
def test_batch_read_version_doesnt_exist(basic_store):
    sym1 = "sym1"
    sym2 = "sym2"
    basic_store.write(sym1, 1)
    basic_store.write(sym2, 2)
    with pytest.raises(NoDataFoundException):
        _ = basic_store.batch_read([sym1, sym2], as_ofs=[0, 1])


@pytest.mark.storage
def test_read_batch_deleted_version_doesnt_exist(basic_store):
    sym1 = "mysymbol"
    basic_store.write(sym1, 0)

    basic_store.delete(sym1)
    basic_store.write(sym1, 1)
    with pytest.raises(NoSuchVersionException):
        basic_store.read(sym1, as_of=0)

    with pytest.raises(NoSuchVersionException):
        basic_store.batch_read([sym1], as_ofs=[0])


@pytest.mark.storage
def test_index_keys_start_end_index(basic_store, sym):
    idx = pd.date_range("2022-01-01", periods=100, freq="D")
    df = pd.DataFrame({"a": range(len(idx))}, index=idx)
    basic_store.write(sym, df)

    lt = basic_store.library_tool()
    key = lt.find_keys_for_id(KeyType.TABLE_INDEX, sym)[0]
    assert key.start_index == 1640995200000000000
    assert key.end_index == 1649548800000000001


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


def subset_permutations(input_data):
    return (p for r in range(1, len(input_data) + 1) for p in itertools.permutations(input_data, r))


@pytest.mark.parametrize(
    "bucketize_dynamic",
    [
        pytest.param(
            True, marks=pytest.mark.xfail(reason="Bucketize dynamic is not used in production. There are bugs")
        ),
        False,
    ],
)
def test_dynamic_schema_read_columns(version_store_factory, lib_name, bucketize_dynamic):
    column_data = {"a": [1.0], "b": [2.0], "c": [3.0], "d": [4.0]}
    append_column_data = {"a": [5.0], "b": [6.0], "c": [7.0], "d": [8.0]}
    lmdb_lib = version_store_factory(
        lib_name, dynamic_schema=True, column_group_size=2, bucketize_dynamic=bucketize_dynamic
    )
    columns = ("a", "b", "c", "d")
    subset_perm = subset_permutations(columns)
    input_data = [
        (pd.DataFrame({c: column_data[c] for c in v1}), pd.DataFrame({c: append_column_data[c] for c in v2}))
        for v1 in subset_perm
        for v2 in subset_perm
    ]
    for to_write, to_append in input_data:
        lmdb_lib.write("test", to_write)
        lmdb_lib.append("test", to_append)
        columns = set(list(to_write.columns) + list(to_append.columns))
        for read_columns in subset_permutations(list(columns)):
            data = lmdb_lib.read("test", columns=read_columns).data
            expected = pd.DataFrame(
                {
                    c: [
                        column_data[c][0] if c in to_write else np.nan,
                        append_column_data[c][0] if c in to_append else np.nan,
                    ]
                    for c in read_columns
                }
            )
            data.sort_index(inplace=True, axis=1)
            expected.sort_index(inplace=True, axis=1)
            assert_frame_equal(data, expected)
        lmdb_lib.delete("test")


@pytest.mark.storage
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
@pytest.mark.storage
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


@pytest.mark.parametrize("method", ("append", "update"))
@pytest.mark.parametrize("num", (5, 50, 1001))
@pytest.mark.storage
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


@pytest.mark.storage
def test_wrong_df_col_order(basic_store):
    lib = basic_store

    df1 = pd.DataFrame({"col1": [11, 12, 13], "col2": [1, 2, 3]})
    sym = "symbol"
    lib.write(sym, df1)

    df2 = pd.DataFrame({"col2": [4, 5, 6], "col1": [14, 15, 16]})
    with pytest.raises(StreamDescriptorMismatch, match="type=TD<type=INT64, dim=0>, idx="):
        lib.append(sym, df2)


@pytest.mark.storage
def test_get_non_existing_columns_in_series(basic_store, sym):
    lib = basic_store
    dst = pd.Series(index=pd.date_range(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-02-01")), data=0.0)
    lib.write(sym, dst)
    assert basic_store.read(sym, columns=["col1"]).data.empty


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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


@pytest.mark.storage
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

        df2 = pd.DataFrame(
            {"d": range(x + 1, l + x + 1), "e": range(x + 2, l + x + 2), "f": range(x + 3, l + x + 3)}, index=idx
        )
        lib.write(symbol, df2)

        remove_most_recent_version_key(lib, symbol)

    vits = lib.batch_read(symbols, as_ofs=write_times)
    for x in range(num_items):
        assert_equal(vits[symbols[x]].data, expected[x])


@pytest.mark.parametrize("use_caching", [True, False])
@pytest.mark.storage
def test_version_chain_cache(basic_store, use_caching):
    timeout = sys.maxsize if use_caching else 0
    lib = basic_store
    symbol = "test"
    # Will write 10 versions
    num_of_versions = 10
    dataframes = [sample_dataframe() for _ in range(num_of_versions)]
    timestamps = []

    def assert_correct_dataframe(timestamp_and_version_index, deleted_versions):
        # Version
        version_index = timestamp_and_version_index
        if i in deleted_versions:
            with pytest.raises(NoSuchVersionException):
                lib.read(symbol, as_of=version_index)
        else:
            assert_equal(lib.read(symbol, as_of=version_index).data, dataframes[i])

        # Timestamp
        timestamp_index = timestamp_and_version_index

        def find_expected_version(first_to_check):
            for num in range(first_to_check, -1, -1):
                if num not in deleted_versions:
                    return num
            return None

        for timestamp, is_before in [
            (timestamps[timestamp_index].before, True),
            (timestamps[timestamp_index].after, False),
        ]:
            first_version_to_check = timestamp_index - 1 if is_before else timestamp_index
            expected_version_to_find = find_expected_version(first_version_to_check)
            if expected_version_to_find is None:
                with pytest.raises(NoSuchVersionException):
                    lib.read(symbol, as_of=timestamp)
            else:
                assert_frame_equal(lib.read(symbol, as_of=timestamp).data, dataframes[expected_version_to_find])

    with config_context("VersionMap.ReloadInterval", timeout):
        # Write versions and keep track of time before and after writing
        for i in range(num_of_versions):
            with distinct_timestamps(lib) as timestamp:
                lib.write(symbol, dataframes[i])
            timestamps.append(timestamp)

        # Validate the most recent version
        assert_equal(lib.read(symbol).data, dataframes[-1])

        # Check reading specific versions
        for i in range(num_of_versions):
            assert_correct_dataframe(i, {})

        # Ensure reading a non-existent version raises an exception
        with pytest.raises(NoSuchVersionException):
            lib.read(symbol, as_of=pd.Timestamp(0))

        # Delete specific versions
        delete_versions = [1, 3, 7, 9]
        lib.delete_versions(symbol, delete_versions)
        for i in range(num_of_versions):
            assert_correct_dataframe(i, delete_versions)

        with pytest.raises(NoSuchVersionException):
            lib.read(symbol, as_of=pd.Timestamp(0))

        # Delete all versions
        lib.delete(symbol)
        for i in range(num_of_versions):
            assert_correct_dataframe(i, set(range(num_of_versions)))
