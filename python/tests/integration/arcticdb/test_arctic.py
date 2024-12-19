"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import sys
import pytz
import math
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import List
from enum import Enum

from arcticdb_ext.exceptions import InternalException, SortingException, UserInputException
from arcticdb_ext.storage import NoDataFoundException
from arcticdb_ext.version_store import SortedValue
from arcticdb.exceptions import ArcticDbNotYetImplemented, LibraryNotFound, MismatchingLibraryOptions, StreamDescriptorMismatch, SchemaException
from arcticdb.adapters.mongo_library_adapter import MongoLibraryAdapter
from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions, EnterpriseLibraryOptions
from arcticdb import QueryBuilder
from arcticdb.storage_fixtures.api import StorageFixture, ArcticUriFields, StorageFixtureFactory
from arcticdb.storage_fixtures.mongo import MongoDatabase
from arcticdb.util.test import assert_frame_equal
from arcticdb.storage_fixtures.s3 import S3Bucket
from arcticdb.version_store.library import (
    WritePayload,
    ArcticUnsupportedDataTypeException,
    ReadRequest,
    StagedDataFinalizeMethod,
    ArcticInvalidApiUsageException,
)

from ...util.mark import AZURE_TESTS_MARK, MONGO_TESTS_MARK, REAL_S3_TESTS_MARK, SSL_TESTS_MARK, SSL_TEST_SUPPORTED

class ParameterDisplayStatus(Enum):
    NOT_SHOW = 1
    DISABLE = 2
    ENABLE = 3

parameter_display_status = [ParameterDisplayStatus.NOT_SHOW, ParameterDisplayStatus.DISABLE, ParameterDisplayStatus.ENABLE]
no_ssl_parameter_display_status = [ParameterDisplayStatus.NOT_SHOW, ParameterDisplayStatus.DISABLE]

class DefaultSetting:
    def __init__(self, factory):
        self.cafile = factory.client_cert_file
        self.capath = factory.client_cert_dir

def edit_connection_string(uri, delimiter, storage, ssl_setting, client_cert_file, client_cert_dir):
    # Clear default setting in the uri
    if SSL_TEST_SUPPORTED:
        uri = storage.replace_uri_field(uri, ArcticUriFields.CA_PATH, "", start=1, end=3).rstrip(delimiter)
        if isinstance(storage, S3Bucket) and "&ssl=" in uri:
            uri = storage.replace_uri_field(uri, ArcticUriFields.SSL, "", start=1, end=3).rstrip(delimiter)
    # http server with ssl verification doesn't make sense but it is permitted due to historical reason
    if ssl_setting == ParameterDisplayStatus.DISABLE:
        uri += f"{delimiter}ssl=False"
    elif ssl_setting == ParameterDisplayStatus.ENABLE:
        uri += f"{delimiter}ssl=True"
    if client_cert_file == ParameterDisplayStatus.DISABLE:
        uri += f"{delimiter}CA_cert_path="
    elif client_cert_file == ParameterDisplayStatus.ENABLE:
        assert storage.factory.client_cert_file
        uri += f"{delimiter}CA_cert_path={storage.factory.client_cert_file}"
    if client_cert_dir == ParameterDisplayStatus.DISABLE:
        uri += f"{delimiter}CA_cert_dir="
    elif client_cert_dir == ParameterDisplayStatus.ENABLE:
        assert storage.factory.client_cert_dir
        uri += f"{delimiter}CA_cert_dir={storage.factory.client_cert_dir}"
    return uri

# s3_storage will become non-ssl if SSL_TEST_SUPPORTED is False
@pytest.mark.parametrize('client_cert_file', parameter_display_status if SSL_TEST_SUPPORTED else no_ssl_parameter_display_status)
@pytest.mark.parametrize('client_cert_dir', parameter_display_status if SSL_TEST_SUPPORTED else no_ssl_parameter_display_status)
@pytest.mark.parametrize('ssl_setting', parameter_display_status if SSL_TEST_SUPPORTED else no_ssl_parameter_display_status)
def test_s3_verification(monkeypatch, s3_storage, client_cert_file, client_cert_dir, ssl_setting):
    storage = s3_storage
    # Leaving ca file and ca dir unset will fallback to using os default setting,
    # which is different from the test environment
    default_setting = DefaultSetting(storage.factory)
    monkeypatch.setattr("ssl.get_default_verify_paths", lambda: default_setting)
    uri = edit_connection_string(storage.arctic_uri, "&", storage, ssl_setting, client_cert_file, client_cert_dir)
    ac = Arctic(uri)
    lib = ac.create_library("test")
    lib.write("sym", pd.DataFrame())


@SSL_TESTS_MARK
@pytest.mark.parametrize('client_cert_file', no_ssl_parameter_display_status)
@pytest.mark.parametrize('client_cert_dir', no_ssl_parameter_display_status)
@pytest.mark.parametrize('ssl_setting', no_ssl_parameter_display_status)
def test_s3_no_ssl_verification(monkeypatch, s3_no_ssl_storage, client_cert_file, client_cert_dir, ssl_setting):        
    storage = s3_no_ssl_storage
    # Leaving ca file and ca dir unset will fallback to using os default setting,
    # which is different from the test environment
    default_setting = DefaultSetting(storage.factory)
    monkeypatch.setattr("ssl.get_default_verify_paths", lambda: default_setting)
    uri = edit_connection_string(storage.arctic_uri, "&", storage, ssl_setting, client_cert_file, client_cert_dir)
    ac = Arctic(uri)
    lib = ac.create_library("test")
    lib.write("sym", pd.DataFrame())


@REAL_S3_TESTS_MARK
@pytest.mark.skip(reason="This test is not stable")
def test_s3_sts_auth(real_s3_sts_storage):
    ac = Arctic(real_s3_sts_storage.arctic_uri)
    lib = ac.create_library("test")
    df = pd.DataFrame({'a': [1, 2, 3]})
    lib.write("sym", df)
    assert_frame_equal(lib.read("sym").data, df)
    lib = ac.get_library("test")
    assert_frame_equal(lib.read("sym").data, df)

    # Reload for testing a different codepath
    ac = Arctic(real_s3_sts_storage.arctic_uri)
    lib = ac.get_library("test")
    assert_frame_equal(lib.read("sym").data, df)


@REAL_S3_TESTS_MARK
def test_s3_sts_auth_store(real_s3_sts_version_store):
    lib = real_s3_sts_version_store
    df = pd.DataFrame({'a': [1, 2, 3]})
    lib.write("sym", df)
    assert_frame_equal(lib.read("sym").data, df)


@AZURE_TESTS_MARK
@pytest.mark.parametrize('client_cert_file', no_ssl_parameter_display_status)
@pytest.mark.parametrize('client_cert_dir', no_ssl_parameter_display_status)
def test_azurite_no_ssl_verification(monkeypatch, azurite_storage, client_cert_file, client_cert_dir):
    storage = azurite_storage
    # Leaving ca file and ca dir unset will fallback to using os default setting,
    # which is different from the test environment
    default_setting = DefaultSetting(storage.factory)
    monkeypatch.setattr("ssl.get_default_verify_paths", lambda: default_setting)
    uri = edit_connection_string(storage.arctic_uri, ";", storage, None, client_cert_file, client_cert_dir)
    ac = Arctic(uri)
    lib = ac.create_library("test")
    lib.write("sym", pd.DataFrame())


@AZURE_TESTS_MARK
@SSL_TESTS_MARK
@pytest.mark.parametrize('client_cert_file', parameter_display_status)
@pytest.mark.parametrize('client_cert_dir', parameter_display_status)
def test_azurite_ssl_verification(azurite_ssl_storage, monkeypatch, client_cert_file, client_cert_dir):
    storage = azurite_ssl_storage
    # Leaving ca file and ca dir unset will fallback to using os default setting,
    # which is different from the test environment
    default_setting = DefaultSetting(storage.factory)
    monkeypatch.setattr("ssl.get_default_verify_paths", lambda: default_setting)
    uri = edit_connection_string(storage.arctic_uri, ";", storage, None, client_cert_file, client_cert_dir)
    ac = Arctic(uri)
    lib = ac.create_library("test")
    lib.write("sym", pd.DataFrame())


def test_basic_metadata(lmdb_version_store):
    lib = lmdb_version_store
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    metadata = {"fluffy" : "muppets"}
    lib.write("my_symbol", df, metadata=metadata)
    vit = lib.read_metadata("my_symbol")
    assert vit.metadata == metadata


def test_sorted_roundtrip(arctic_library):
    lib = arctic_library

    symbol = "sorted_test"
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write(symbol, df)
    desc = lib.get_description(symbol)
    assert desc.sorted == 'ASCENDING'


def test_basic_write_read_update_and_append(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("my_symbol", df)

    assert lib.list_symbols() == ["my_symbol"]
    assert_frame_equal(lib.read("my_symbol").data, df)
    assert_frame_equal(lib.read("my_symbol", columns=["col1"]).data, df[["col1"]])

    assert_frame_equal(lib.head("my_symbol", n=1).data, df.head(n=1))
    assert_frame_equal(lib.tail("my_symbol", n=1).data, df.tail(n=1).reset_index(drop=True))

    lib.append("my_symbol", pd.DataFrame({"col1": [4, 5, 6], "col2": [7, 8, 9]}))
    assert lib["my_symbol"].version == 1
    assert_frame_equal(
        lib.read("my_symbol").data, pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6], "col2": [4, 5, 6, 7, 8, 9]})
    )

    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    df.index = pd.date_range("2018-01-01", periods=3, freq="H")
    lib.write("timeseries", df, metadata={"hello": "world"})
    assert lib["timeseries"].version == 0

    df = pd.DataFrame({"col1": [4, 5, 6], "col2": [7, 8, 9]})
    df.index = pd.date_range("2018-01-01", periods=3, freq="H")
    lib.update("timeseries", df)
    assert lib["timeseries"].version == 1
    df.index.freq = None
    assert_frame_equal(lib.read("timeseries").data, df)

    lib.write("meta", df, metadata={"hello": "world"})
    assert lib["meta"].version == 0

    read_metadata = lib.read_metadata("meta")
    assert read_metadata.metadata == {"hello": "world"}
    assert read_metadata.data is None
    assert read_metadata.version == 0

    lib.write("meta", df, metadata={"goodbye": "cruel world"})
    read_metadata = lib.read_metadata("meta")
    assert read_metadata.version == 1


def test_write_metadata_with_none(arctic_library):
    lib = arctic_library
    symbol = "symbol"
    meta = {"meta_" + str(symbol): 0}

    result_write = lib.write_metadata(symbol, meta)
    assert result_write.version == 0

    read_meta_symbol = lib.read_metadata(symbol)
    assert read_meta_symbol.data is None
    assert read_meta_symbol.metadata == meta
    assert read_meta_symbol.version == 0

    read_symbol = lib.read(symbol)
    assert read_symbol.data is None
    assert read_symbol.metadata == meta
    assert read_symbol.version == 0


@pytest.mark.parametrize("finalize_method", (StagedDataFinalizeMethod.WRITE, StagedDataFinalizeMethod.APPEND))
def test_staged_data(arctic_library, finalize_method):
    lib = arctic_library
    sym_with_metadata = "sym_with_metadata"
    sym_without_metadata = "sym_without_metadata"
    sym_unfinalized = "sym_unfinalized"
    df_0 = pd.DataFrame({"col": [1, 2]}, index=pd.date_range("2024-01-01", periods=2))
    df_1 = pd.DataFrame({"col": [3, 4]}, index=pd.date_range("2024-01-03", periods=2))
    df_2 = pd.DataFrame({"col": [5, 6]}, index=pd.date_range("2024-01-05", periods=2))
    expected = pd.concat([df_0, df_1, df_2])

    if finalize_method == StagedDataFinalizeMethod.WRITE:
        lib.write(sym_with_metadata, df_0, staged=True)
        lib.write(sym_without_metadata, df_0, staged=True)
        lib.write(sym_unfinalized, df_0, staged=True)
    else:
        # finalize_method == StagedDataFinalizeMethod.APPEND
        lib.write(sym_with_metadata, df_0, staged=False)
        lib.write(sym_without_metadata, df_0, staged=False)

    lib.write(sym_with_metadata, df_1, staged=True)
    lib.write(sym_with_metadata, df_2, staged=True)
    lib.write(sym_without_metadata, df_1, staged=True)
    lib.write(sym_without_metadata, df_2, staged=True)
    lib.write(sym_unfinalized, df_1, staged=True)
    lib.write(sym_unfinalized, df_2, staged=True)

    metadata = {"hello": "world"}
    finalize_result_meta = lib.finalize_staged_data(sym_with_metadata, finalize_method, metadata=metadata)
    assert finalize_result_meta.metadata == metadata
    assert finalize_result_meta.symbol == sym_with_metadata
    assert finalize_result_meta.library == lib.name
    assert finalize_result_meta.version == (1 if finalize_method == StagedDataFinalizeMethod.APPEND else 0)

    lib.finalize_staged_data(sym_without_metadata, finalize_method)

    assert set(lib.list_symbols()) == {sym_with_metadata, sym_without_metadata}

    sym_with_metadata_vit = lib.read(sym_with_metadata)
    assert_frame_equal(expected, sym_with_metadata_vit.data)
    assert sym_with_metadata_vit.metadata == metadata

    sym_without_metadata_vit = lib.read(sym_without_metadata)
    assert_frame_equal(expected, sym_without_metadata_vit.data)
    assert sym_without_metadata_vit.metadata is None


@pytest.mark.parametrize("finalize_method", (StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE))
@pytest.mark.parametrize("validate_index", (True, False, None))
def test_parallel_writes_and_appends_index_validation(arctic_library, finalize_method, validate_index):
    lib = arctic_library
    sym = "test_parallel_writes_and_appends_index_validation"
    if finalize_method == StagedDataFinalizeMethod.APPEND:
        df_0 = pd.DataFrame({"col": [1, 2]}, index=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")])
        lib.write(sym, df_0)
    df_1 = pd.DataFrame({"col": [3, 4]}, index=[pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")])
    df_2 = pd.DataFrame({"col": [5, 6]}, index=[pd.Timestamp("2024-01-03T12"), pd.Timestamp("2024-01-05")])
    lib.write(sym, df_2, staged=True)
    lib.write(sym, df_1, staged=True)
    if validate_index is None:
        # Test default behaviour when arg isn't provided
        with pytest.raises(SortingException):
            lib.finalize_staged_data(sym, finalize_method)
    elif validate_index:
        with pytest.raises(SortingException):
            lib.finalize_staged_data(sym, finalize_method, validate_index=True)
    else:
        lib.finalize_staged_data(sym, finalize_method, validate_index=False)
        received = lib.read(sym).data
        expected = pd.concat([df_0, df_1, df_2]) if finalize_method == StagedDataFinalizeMethod.APPEND else pd.concat([df_1, df_2])
        assert_frame_equal(received, expected)


@pytest.mark.parametrize("finalize_method", (StagedDataFinalizeMethod.APPEND, StagedDataFinalizeMethod.WRITE))
def test_finalize_without_adding_segments(arctic_library, finalize_method):
    lib = arctic_library
    with pytest.raises(UserInputException) as exception_info:
        lib.finalize_staged_data("sym", mode=finalize_method)


class TestAppendStagedData:
    def test_appended_df_interleaves_with_storage(self, arctic_library):
        lib = arctic_library
        initial_df = pd.DataFrame({"col": [1, 3]}, index=pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-03')], dtype="datetime64[ns]"))
        lib.write("sym", initial_df)
        df1 = pd.DataFrame({"col": [2]}, index=pd.DatetimeIndex([np.datetime64('2023-01-02')], dtype="datetime64[ns]"))
        lib.write("sym", df1, staged=True)
        with pytest.raises(SortingException) as exception_info:
            lib.finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        assert "append" in str(exception_info.value)

    def test_appended_df_start_same_as_df_end(self, arctic_library):
        lib = arctic_library
        df = pd.DataFrame(
            {"col": [1, 2, 3]},
            index=pd.DatetimeIndex([np.datetime64('2023-01-01'), np.datetime64('2023-01-02'), np.datetime64('2023-01-03')], dtype="datetime64[ns]")
        )
        lib.write("sym", df)
        df_to_append = pd.DataFrame(
            {"col": [4, 5, 6]},
            index=pd.DatetimeIndex([np.datetime64('2023-01-03'), np.datetime64('2023-01-04'), np.datetime64('2023-01-05')], dtype="datetime64[ns]")
        )
        lib.write("sym", df_to_append, staged=True)
        lib.finalize_staged_data("sym", mode=StagedDataFinalizeMethod.APPEND)
        res = lib.read("sym").data
        expected_df = pd.concat([df, df_to_append])
        assert_frame_equal(lib.read("sym").data, expected_df)

def test_snapshots_and_deletes(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("my_symbol", df)
    lib.write("my_symbol2", df)

    lib.snapshot("test1")

    assert lib.list_snapshots() == {"test1": None}

    assert_frame_equal(lib.read("my_symbol", as_of="test1").data, df)

    lib.delete("my_symbol")
    lib.snapshot("snap_after_delete")
    assert sorted(lib.list_symbols("test1")) == ["my_symbol", "my_symbol2"]
    assert lib.list_symbols("snap_after_delete") == ["my_symbol2"]

    lib.delete_snapshot("test1")
    assert lib.list_snapshots() == {"snap_after_delete": None}
    assert lib.list_symbols() == ["my_symbol2"]

def test_list_snapshots_no_metadata(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"a": [1, 2, 3]})

    snap1 = "snap1"
    metadata_snap1 = {"snap1": 1}
    symbol1 = "test_symbol"
    snap2 = "snap2"
    symbol2 = "test_symbol2"

    lib.write(symbol1, df)
    lib.snapshot(snap1, metadata=metadata_snap1)
    lib.write(symbol2, df)
    lib.snapshot(snap2)

    snaps_list = lib.list_snapshots(False)
    assert isinstance(snaps_list, List)
    assert set(snaps_list) == {snap1, snap2}

def test_delete_non_existent_snapshot(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("my_symbol", df)
    with pytest.raises(NoDataFoundException):
        lib.delete_snapshot("test")


def test_prune_previous_versions(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df, metadata={"very": "interesting"})
    lib.write("symbol", df, metadata={"muy": "interesante"}, prune_previous_versions=False)
    lib.write("symbol", df, metadata={"tres": "interessant"}, prune_previous_versions=False)
    lib.prune_previous_versions("symbol")
    assert len(lib.list_versions("symbol")) == 1
    assert ("symbol", 2) in lib.list_versions("symbol")
    assert lib["symbol"].metadata == {"tres": "interessant"}


def test_do_not_prune_previous_versions_by_default(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df)
    lib.write("symbol", df)
    lib.write("symbol", df)
    lib.write("symbol", df)
    lib.write("symbol", df)
    assert len(lib.list_versions("symbol")) == 5


def test_delete_version(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df, metadata={"very": "interesting"})
    lib.write("symbol", df, metadata={"muy": "interesante"}, prune_previous_versions=False)
    lib.write("symbol", df, metadata={"tres": "interessant"}, prune_previous_versions=False)
    lib.delete("symbol", versions=(1, 2))
    assert lib["symbol"].version == 0
    assert lib["symbol"].metadata == {"very": "interesting"}


def test_list_versions_write_append_update(arctic_library):
    lib = arctic_library
    # Note: can only update timeseries dataframes
    index = pd.date_range(start="2000-01-01", freq="D", periods=3)
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}, index=index)
    lib.write("symbol", df)
    index_append = pd.date_range(start="2000-01-04", freq="D", periods=3)
    df_append = pd.DataFrame({"col1": [7, 8, 9], "col2": [10, 11, 12]}, index=index_append)
    lib.append("symbol", df_append)
    index_update = pd.DatetimeIndex(["2000-01-03", "2000-01-05"])
    df_update = pd.DataFrame({"col1": [13, 14], "col2": [15, 16]}, index=index_update)
    lib.update("symbol", df_update)
    assert_frame_equal(lib.read("symbol").data, pd.concat([df.iloc[:-1], df_update, df_append.iloc[[2]]]))
    assert len(lib.list_versions("symbol")) == 3


def test_list_versions_latest_only(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df)
    lib.write("symbol", df)
    lib.write("symbol", df)
    assert len(lib.list_versions("symbol", latest_only=True)) == 1


def test_non_existent_list_versions_latest_only(arctic_library):
    lib = arctic_library
    assert len(lib.list_versions("symbol", latest_only=True)) == 0
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol2", df)
    lib.delete("symbol2")
    assert len(lib.list_versions("symbol2", latest_only=True)) == 0


def test_delete_version_with_snapshot(arctic_library):
    lib = arctic_library
    sym = "test_delete_version_with_snapshot"
    df = pd.DataFrame({"col": np.arange(10)}, index=pd.date_range("2024-01-01", periods=10))
    lib.write(sym, df)
    lib.snapshot("snap")
    lib.delete(sym)

    for method in ["read", "head", "tail", "read_metadata", "get_description"]:
        for as_of in [0, pd.Timestamp("2200-01-01")]:
            with pytest.raises(NoDataFoundException):
                getattr(lib, method)(sym, as_of=as_of)


def test_list_versions_with_snapshot(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df, metadata={"very": "interesting"})
    lib.write("symbol", df, metadata={"muy": "interesante"}, prune_previous_versions=False)
    lib.snapshot("my_snap")
    lib.write("symbol", df, metadata={"tres": "interessant"}, prune_previous_versions=False)
    lib.delete("symbol", versions=(1, 2))

    versions = lib.list_versions("symbol")

    assert not versions["symbol", 0].deleted
    assert not versions["symbol", 0].snapshots
    assert versions["symbol", 1].deleted
    assert versions["symbol", 1].snapshots == ["my_snap"]
    assert versions["symbol", 1].date > versions["symbol", 0].date


def test_list_versions_without_snapshot(arctic_library):
    lib = arctic_library
    lib.write("symbol", pd.DataFrame())

    versions = lib.list_versions("symbol")

    assert len(versions) == 1
    assert versions["symbol", 0].snapshots == []


def test_delete_version_that_does_not_exist(arctic_library):
    lib = arctic_library

    # symbol does not exist
    with pytest.raises(InternalException):
        lib.delete("symbol", versions=0)

    # version does not exist
    lib.write("symbol", pd.DataFrame())
    with pytest.raises(InternalException):
        lib.delete("symbol", versions=1)


def test_delete_date_range(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [5, 6, 7, 8]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    # when
    lib.delete_data_in_range("symbol", date_range=(datetime(2018, 1, 1), datetime(2018, 1, 2)))

    # then
    assert_frame_equal(
        lib["symbol"].data, pd.DataFrame({"column": [7, 8]}, index=pd.date_range(start="1/3/2018", end="1/4/2018"))
    )
    assert lib["symbol"].version == 1


def test_azure_repr_body_censored(arctic_library):
    ac_library_repr = repr(arctic_library)
    if "AccountKey=" in ac_library_repr:
        assert "AccountKey=..." in ac_library_repr.split(";")


def _test_mongo_repr_body(mongo_storage: MongoDatabase):
    # The arctic_uri has the PrefixingLibraryAdapterDecorator logic in it, so use mongo_uri
    ac = Arctic(f"{mongo_storage.mongo_uri}/?maxPoolSize=10")
    assert repr(ac) == f"Arctic(config=mongodb(endpoint={mongo_storage.mongo_uri[len('mongodb://'):]}))"

    # With pymongo, exception thrown in the uri_parser;
    with pytest.raises(UserInputException):
        uri = f"{mongo_storage.mongo_uri}//"
        Arctic(uri)


@MONGO_TESTS_MARK
def test_mongo_construction_with_pymongo(mongo_storage):
    _test_mongo_repr_body(mongo_storage)


@MONGO_TESTS_MARK
def test_mongo_construction_no_pymongo(monkeypatch, mongo_storage: MongoDatabase):
    import arcticdb.adapters.mongo_library_adapter

    monkeypatch.setattr(arcticdb.adapters.mongo_library_adapter, "_HAVE_PYMONGO", False)

    _test_mongo_repr_body(mongo_storage)


def test_s3_repr(s3_storage: S3Bucket, one_col_df):
    ac = s3_storage.create_arctic()
    assert ac.list_libraries() == []
    lib = ac.create_library("pytest_test_lib")

    http_endpoint = s3_storage.factory.endpoint
    s3_endpoint = http_endpoint[http_endpoint.index("//") + 2 :]
    config = f"S3(endpoint={s3_endpoint}, bucket={s3_storage.bucket})"
    assert repr(lib) == f"Library(Arctic(config={config}), path=pytest_test_lib, storage=s3_storage)"

    written_vi = lib.write("my_symbol", one_col_df())
    assert written_vi.host == config


class A:
    """A dummy user defined type that requires pickling to serialize."""

    def __init__(self, id: str):
        self.id = id

    def __eq__(self, other):
        return self.id == other.id


def test_write_object_with_pickle_mode(arctic_library):
    """Writing in pickle mode should succeed when the user uses the dedicated method."""
    lib = arctic_library
    lib.write_pickle("test_1", A("id_1"))
    assert lib["test_1"].data.id == "id_1"


def test_write_object_without_pickle_mode(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    with pytest.raises(ArcticUnsupportedDataTypeException):
        lib.write("test_1", A("id_1"))


def test_write_list_without_pickle_mode(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    with pytest.raises(ArcticUnsupportedDataTypeException):
        lib.write("test_1", [1, 2, 3])


def test_write_non_native_frame_with_pickle_mode(arctic_library):
    """Writing with pickle mode should work when the user calls the dedicated method."""
    lib = arctic_library
    df = pd.DataFrame({"col1": [A("id_1")]})
    lib.write_pickle("test_1", df)
    loaded: pd.DataFrame = lib["test_1"].data
    assert_frame_equal(loaded, df[["col1"]])


def test_write_non_native_frame_without_pickle_mode(arctic_library):
    """Writing outside of pickle mode should fail when the user does not use the dedicated method."""
    lib = arctic_library
    df = pd.DataFrame({"col1": [A("id_1")]})
    with pytest.raises(Exception):
        lib.write("test_1", df)


def test_write_with_unpacking(arctic_library):
    """Check the syntactic sugar that lets us unpack WritePayload in `write` calls using *."""
    lib = arctic_library
    df_1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    df_2 = pd.DataFrame({"col1": [-1, -2, -3], "col2": [-4, -5, -6], "anothercol": [0, 0, 0]})

    payload_1 = WritePayload("symbol_1", df_1)
    payload_2 = WritePayload("symbol_2", df_2, metadata="great_metadata")

    lib.write(*payload_1)
    lib.write(*payload_2)

    assert_frame_equal(lib.read("symbol_1", columns=["col1"]).data, df_1[["col1"]])
    assert_frame_equal(lib.read("symbol_2", columns=["col1"]).data, df_2[["col1"]])
    assert_frame_equal(lib.read("symbol_2", columns=["anothercol"]).data, df_2[["anothercol"]])

    symbol_2_loaded = lib.read("symbol_2")
    assert symbol_2_loaded.metadata == "great_metadata"


def test_prune_previous_versions_with_write(arctic_library):
    lib = arctic_library
    # When
    lib.write("sym", pd.DataFrame(), prune_previous_versions=True)
    lib.write("sym", pd.DataFrame({"col": [1, 2, 3]}))

    # Then
    v0 = lib.read("sym", as_of=0).data
    assert v0.empty

    v1 = lib.read("sym", as_of=1).data
    assert not v1.empty

    # We do not prune by default
    lib.write("sym", pd.DataFrame(), prune_previous_versions=True)
    with pytest.raises(NoDataFoundException):
        lib.read("sym", as_of=0)
    with pytest.raises(NoDataFoundException):
        lib.read("sym", as_of=1)

    v3 = lib.read("sym", as_of=2).data
    assert v3.empty


def test_append_documented_example(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    lib.write("symbol", df)
    to_append_df = pd.DataFrame({"column": [4, 5, 6]}, index=pd.date_range(start="1/4/2018", end="1/6/2018"))
    lib.append("symbol", to_append_df, prune_previous_versions=False)

    expected = pd.DataFrame({"column": [1, 2, 3, 4, 5, 6]}, index=pd.date_range(start="1/1/2018", end="1/6/2018"))
    assert_frame_equal(lib["symbol"].data, expected)
    # Check that old versions were not pruned
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 2
    assert ("symbol", 0) in symbols
    assert ("symbol", 1) in symbols
    assert_frame_equal(lib.read("symbol", as_of=0).data, df)


def test_append_prune_previous_versions(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3]}, index=pd.date_range(start="1/1/2018", end="1/3/2018"))
    lib.write("symbol", df)
    to_append_df = pd.DataFrame({"column": [4, 5, 6]}, index=pd.date_range(start="1/4/2018", end="1/6/2018"))
    lib.append("symbol", to_append_df, prune_previous_versions=True)

    expected = pd.DataFrame({"column": [1, 2, 3, 4, 5, 6]}, index=pd.date_range(start="1/1/2018", end="1/6/2018"))
    assert_frame_equal(lib["symbol"].data, expected)
    # Check that old versions were pruned
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 1
    assert ("symbol", 1) in symbols


def test_update_documented_example(arctic_library):
    """Test the example given on the `update` docstring."""
    lib = arctic_library
    # Given
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400, 40]}, index=pd.date_range(start="1/1/2018", end="1/3/2018", freq="2D"))

    # When
    lib.update("symbol", update_df, prune_previous_versions=False)

    # Then
    result = lib.read("symbol").data
    expected = pd.DataFrame({"column": [400, 40, 4]}, index=pd.to_datetime(["1/1/2018", "1/3/2018", "1/4/2018"]))
    assert_frame_equal(result, expected)
    # Check that old versions were not pruned
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 2
    assert ("symbol", 0) in symbols
    assert ("symbol", 1) in symbols
    assert_frame_equal(lib.read("symbol", as_of=0).data, df)


def test_update_prune_previous_versions(arctic_library):
    """Test that updating and pruning previous versions does indeed clear previous versions."""
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400, 40]}, index=pd.date_range(start="1/1/2018", end="1/3/2018", freq="2D"))

    lib.update("symbol", update_df, prune_previous_versions=True)

    result = lib.read("symbol").data
    expected = pd.DataFrame({"column": [400, 40, 4]}, index=pd.to_datetime(["1/1/2018", "1/3/2018", "1/4/2018"]))
    assert_frame_equal(result, expected)
    symbols = lib.list_versions("symbol")
    assert len(symbols) == 1
    assert ("symbol", 1) in symbols


def test_update_with_daterange(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400]}, index=pd.to_datetime(["1/2/2018"]))

    lib.update("symbol", update_df, date_range=(datetime(2010, 1, 1), datetime(2020, 1, 1)))

    result = lib["symbol"].data
    assert_frame_equal(result, update_df)


def test_update_with_daterange_no_width(arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": [400, 500]}, index=pd.date_range(start="1/2/2018", end="1/3/2018"))

    lib.update("symbol", update_df, date_range=(datetime(2018, 1, 2), datetime(2018, 1, 2)))

    result = lib["symbol"].data
    assert_frame_equal(
        result, pd.DataFrame({"column": [1, 400, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    )


def test_update_with_daterange_multi_index(arctic_library):
    lib = arctic_library
    # Given
    index = pd.MultiIndex.from_tuples(
        list(
            zip(
                [
                    datetime(2018, 1, 1),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 3),
                    datetime(2018, 1, 3),
                    datetime(2018, 1, 4),
                ],
                ["A", "B", "C", "D", "E", "F"],
            )
        )
    )
    df = pd.DataFrame({"column": [1, 2, 3, 4, 5, 6]}, index=index)
    lib.write("symbol", df)

    # When
    update_index = pd.MultiIndex.from_tuples(
        list(zip([datetime(2018, 1, 2), datetime(2018, 1, 2), datetime(2018, 1, 3)], ["B", "C", "D"]))
    )
    update_df = pd.DataFrame({"column": [100, 200, 300]}, index=update_index)
    lib.update("symbol", update_df, date_range=(datetime(2018, 1, 2), datetime(2018, 1, 3)))

    # Then
    result = lib["symbol"].data
    # note that 2018-1-3 "E" has been removed as it is within date_range even though it is outside update_index
    expected_index = pd.MultiIndex.from_tuples(
        list(
            zip(
                [
                    datetime(2018, 1, 1),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 3),
                    datetime(2018, 1, 4),
                ],
                ["A", "B", "C", "D", "F"],
            )
        )
    )
    assert_frame_equal(result, pd.DataFrame({"column": [1, 100, 200, 300, 6]}, index=expected_index))


def test_update_with_daterange_multi_index_no_width(arctic_library):
    lib = arctic_library

    # Given
    index = pd.MultiIndex.from_tuples(
        list(
            zip(
                [
                    datetime(2018, 1, 1),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 2),
                    datetime(2018, 1, 3),
                    datetime(2018, 1, 3),
                ],
                ["A", "B", "C", "D", "E"],
            )
        )
    )
    df = pd.DataFrame({"column": [1, 2, 3, 4, 5]}, index=index)
    lib.write("symbol", df)

    # When
    update_index = pd.MultiIndex.from_tuples(
        list(zip([datetime(2018, 1, 2), datetime(2018, 1, 2), datetime(2018, 1, 3)], ["B", "C", "D"]))
    )
    update_df = pd.DataFrame({"column": [100, 200, 300]}, index=update_index)
    lib.update("symbol", update_df, date_range=(datetime(2018, 1, 2), datetime(2018, 1, 2)))

    # Then
    result = lib["symbol"].data
    assert_frame_equal(result, pd.DataFrame({"column": [1, 100, 200, 4, 5]}, index=index))


def test_update_with_daterange_restrictive(arctic_library):
    """Here the update_df cover more dates than date_range. We should only touch data that lies within date_range."""
    lib = arctic_library
    df = pd.DataFrame({"column": np.arange(30)}, index=pd.date_range(start="1/1/2022", periods=30))
    lib.write("symbol", df)

    update_df = pd.DataFrame({"column": np.arange(30, 60)}, index=pd.date_range(start="1/1/2022", periods=30))
    lib.update("symbol", update_df, date_range=(datetime(2022, 1, 10), datetime(2022, 1, 20)))

    result = lib["symbol"].data
    assert result.size == 30
    expected = pd.DataFrame(
        {"column": np.concatenate((np.arange(0, 9), np.arange(39, 50), np.arange(20, 30)))},
        index=pd.date_range(start="1/1/2022", periods=30),
    )
    assert_frame_equal(expected, result)


def test_update_with_upsert(arctic_library):
    lib = arctic_library
    with pytest.raises(Exception):
        lib.update("symbol", pd.DataFrame())
    assert not lib.list_symbols()
    lib.update("symbol", pd.DataFrame(), upsert=True)
    assert "symbol" in lib.list_symbols()


def test_read_with_read_request_form(arctic_library):
    lib = arctic_library

    # Given
    q = QueryBuilder()
    q = q[q["A"] < 3]
    lib.write("s", pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))

    # When
    result = lib.read(*ReadRequest("s", as_of=0, columns=["A"], query_builder=q))

    # Then
    assert_frame_equal(result.data, pd.DataFrame({"A": [1, 2]}))


def test_has_symbol(arctic_library):
    lib = arctic_library
    lib.write("symbol", pd.DataFrame())
    lib.write("symbol", pd.DataFrame(), prune_previous_versions=False)
    assert lib.has_symbol("symbol")
    assert "symbol" in lib
    lib.snapshot("snapshot")
    lib.delete("symbol")
    assert "symbol" not in lib
    assert lib.has_symbol("symbol", as_of="snapshot")


@pytest.mark.skipif(sys.platform == "win32", reason="SKIP_WIN Numpy strings not supported yet")
def test_numpy_string(arctic_library):
    arctic_library.write("symbol", np.array(["ab", "cd", "efg"]))
    res = arctic_library.read("symbol").data
    np.testing.assert_array_equal(res, np.array(["ab", "cd", "efg"]))


@pytest.mark.skipif(sys.platform != "win32", reason="SKIP_WIN Numpy strings not supported yet")
def test_numpy_string_fails_on_windows(arctic_library):
    with pytest.raises(ArcticDbNotYetImplemented):
        arctic_library.write("symbol", np.array(["ab", "cd", "efg"]))


def test_get_description(arctic_library):
    lib = arctic_library

    # given
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    df.index.rename("named_index", inplace=True)
    lib.write("symbol", df)
    to_append_df = pd.DataFrame({"column": [5, 6]}, index=pd.date_range(start="1/5/2018", end="1/6/2018"))
    to_append_df.index.rename("named_index", inplace=True)
    lib.append("symbol", to_append_df)
    # when
    info = lib.get_description("symbol")
    original_info = lib.get_description("symbol", as_of=0)
    # then
    assert [c[0] for c in info.columns] == ["column"]
    assert info.date_range == (pd.Timestamp(year=2018, month=1, day=1), pd.Timestamp(year=2018, month=1, day=6))
    assert info.index[0].name == "named_index"
    assert info.index_type == "index"
    assert info.row_count == 6
    assert original_info.row_count == 4
    assert info.last_update_time > original_info.last_update_time
    assert info.last_update_time.tz == pytz.UTC
    assert original_info.sorted == "ASCENDING"
    assert info.sorted == "ASCENDING"


def test_get_description_unnamed_index(lmdb_library):
    lib = lmdb_library
    sym = "test_get_description_unnamed_index"
    df = pd.DataFrame({"col": [0]}, index=[pd.Timestamp(0)])
    lib.write(sym, df)
    index_info = lib.get_description(sym).index
    assert len(index_info) == 1
    assert index_info[0].name is None


@pytest.mark.parametrize("names", (None, ["top-level", "second-level"]))
def test_get_description_multiindex(lmdb_library, names):
    lib = lmdb_library
    sym = "test_get_description_multiindex"
    df = pd.DataFrame({"col": [0]}, index=pd.MultiIndex.from_arrays([[0], [1]], names=names))
    lib.write(sym, df)
    index_info = lib.get_description(sym).index
    assert len(index_info) == 2
    assert index_info[0].name == (names[0] if names is not None else None)
    assert index_info[1].name == (names[1] if names is not None else None)


# See test_write_tz in test_normalization.py for the V1 API equivalent
@pytest.mark.parametrize(
    "tz", ["UTC", "Europe/Amsterdam"]
)
def test_get_description_date_range_tz(arctic_library, tz):
    lib = arctic_library
    sym = "test_get_description_date_range_tz"
    index = index=pd.date_range(pd.Timestamp(0), periods=10, tz=tz)
    df = pd.DataFrame(data={"col1": np.arange(10)}, index=index)
    lib.write(sym, df)
    start_ts, end_ts = lib.get_description(sym).date_range
    assert isinstance(start_ts, pd.Timestamp)
    assert isinstance(end_ts, pd.Timestamp)
    assert start_ts == index[0]
    assert end_ts == index[-1]


def test_tail(arctic_library):
    lib = arctic_library

    # given
    df = pd.DataFrame({"column": [1, 2, 3, 4]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("sym", df)
    df = pd.DataFrame({"column": [5, 6, 7, 8]}, index=pd.date_range(start="1/1/2018", end="1/4/2018"))
    lib.write("sym", df, prune_previous_versions=False)
    df = pd.DataFrame({"column": [9]}, index=pd.date_range(start="1/1/2018", end="1/1/2018"))
    lib.write("sym", df, prune_previous_versions=False)

    # when
    first_version = lib.tail("sym", 2, as_of=0)
    second_version = lib.tail("sym", 2, as_of=1)
    third_version = lib.tail("sym", 2, as_of=2)

    # then
    assert first_version.version == 0
    assert_frame_equal(
        first_version.data, pd.DataFrame({"column": [3, 4]}, index=pd.date_range(start="1/3/2018", end="1/4/2018"))
    )
    assert second_version.version == 1
    assert_frame_equal(
        second_version.data, pd.DataFrame({"column": [7, 8]}, index=pd.date_range(start="1/3/2018", end="1/4/2018"))
    )
    assert third_version.version == 2
    assert_frame_equal(
        third_version.data, pd.DataFrame({"column": [9]}, index=pd.date_range(start="1/1/2018", end="1/1/2018"))
    )


def test_dedup(arctic_client):
    ac = arctic_client
    assert ac.list_libraries() == []
    errors = []
    # we are doing manual iteration due to a limitation that should be fixed by issue #1053
    for dedup in [True, False]:
        try:
            ac.create_library(f"pytest_test_library_{dedup}", LibraryOptions(dedup=dedup))
            lib = ac[f"pytest_test_library_{dedup}"]
            symbol = "test_dedup"
            lib.write_pickle(symbol, 1)
            lib.write_pickle(symbol, 1, prune_previous_versions=False)
            data_key_version = lib._nvs.read_index(symbol)["version_id"][0]
            assert data_key_version == 0 if dedup else 1
        except AssertionError as e:
            errors.append(f"Failed when using dedup value {dedup}: {str(e)}")
    assert not errors, "errors occurred:\n" + "\n".join(errors)


def test_segment_slicing(arctic_client):
    ac = arctic_client
    assert ac.list_libraries() == []
    rows_per_segment = 5
    columns_per_segment = 2
    ac.create_library(
        "pytest_test_library",
        LibraryOptions(rows_per_segment=rows_per_segment, columns_per_segment=columns_per_segment),
    )
    lib = ac["pytest_test_library"]
    symbol = "test_segment_slicing"
    rows = 12
    columns = 3
    data = {}
    for col in range(columns):
        data[f"col{col}"] = np.arange(100 * col, (100 * col) + rows)
    lib.write(symbol, pd.DataFrame(data))
    num_data_segments = len(lib._nvs.read_index(symbol))
    assert num_data_segments == math.ceil(rows / rows_per_segment) * math.ceil(columns / columns_per_segment)


@pytest.mark.parametrize("fixture", ["s3_storage", pytest.param("azurite_storage", marks=AZURE_TESTS_MARK)])
def test_reload_symbol_list(fixture, request):
    storage_fixture: StorageFixture = request.getfixturevalue(fixture)

    def get_symbol_list_keys():
        keys = storage_fixture.iter_underlying_object_names()
        symbol_list_keys = []
        for key in keys:
            if key.startswith(lib_name):
                path_components = key.split("/")
                if path_components[1] == "sl":
                    symbol_list_keys.append(path_components[2])
        return symbol_list_keys

    ac = Arctic(storage_fixture.arctic_uri)
    assert ac.list_libraries() == []

    lib_name = "pytest_test_lib"

    ac.create_library(lib_name)
    lib = ac[lib_name]

    lib.write_pickle("symbol_2", 2)

    for _ in range(15):
        lib.write_pickle("symbol_1", 1)
        lib.delete("symbol_1")

    # assert set(lib.list_symbols()) == {"symbol_2"}
    assert len(get_symbol_list_keys()) == 31

    lib.reload_symbol_list()
    assert len(get_symbol_list_keys()) == 1


@pytest.mark.parametrize(
    "fixture",
    [
        "s3_storage",
        pytest.param("azurite_storage", marks=AZURE_TESTS_MARK),
        pytest.param("mongo_storage", marks=MONGO_TESTS_MARK),
        pytest.param("real_s3_storage", marks=REAL_S3_TESTS_MARK),
    ],
)
def test_get_uri(fixture, request):
    storage_fixture: StorageFixture = request.getfixturevalue(fixture)
    ac = storage_fixture.create_arctic()
    assert ac.get_uri() == storage_fixture.arctic_uri


@AZURE_TESTS_MARK
def test_azure_sas_token(azurite_storage_factory: StorageFixtureFactory):
    with azurite_storage_factory.enforcing_permissions_context():
        # In the Azurite fixture implementation, enforcing permissions requires using SAS
        with azurite_storage_factory.create_fixture() as f:
            assert "SharedAccessSignature" in f.arctic_uri
            f.set_permission(read=True, write=True)
            ac = f.create_arctic()
            ac.create_library("x")

def test_lib_has_lib_tools_read_index(lmdb_library):
    lib = lmdb_library
    sym = "my_symbol"

    df = pd.DataFrame({"col": [1, 2, 3]})
    lib.write(sym, df)
    lib_tool = lib._dev_tools.library_tool()

    assert lib_tool.read_index(sym).equals(lib._nvs.read_index(sym))


def test_s3_force_uri_lib_config_handling(s3_storage):
    # force_uri_lib_config is a obsolete configuration. However, user still includes this option in their setup.
    # For backward compatibility, we need to make sure such setup will still work
    # Why it becomes obsolete: https://github.com/man-group/ArcticDB/pull/803
    Arctic(s3_storage.arctic_uri + "&force_uri_lib_config=true")

    with pytest.raises(ValueError):
        Arctic(s3_storage.arctic_uri + "&force_uri_lib_config=false")


@pytest.mark.parametrize("connection_string", ("mongodb://blah", "mongodb+srv://blah"))
def test_mongo_connection_string_format(connection_string):
    assert MongoLibraryAdapter.supports_uri(connection_string)


# See test of same name in test_normalization.py for V1 API equivalent
def test_norm_failure_error_message(arctic_library):
    lib = arctic_library
    sym = "test_norm_failure_error_message"
    col_name = "My unnormalizable column"
    df = pd.DataFrame({col_name: [1, [1, 2]]})
    with pytest.raises(ArcticDbNotYetImplemented) as write_exception:
        lib.write(sym, df)
    with pytest.raises(ArcticDbNotYetImplemented) as write_batch_exception:
        lib.write_batch([WritePayload(sym, df)])
    with pytest.raises(ArcticDbNotYetImplemented) as append_exception:
        lib.append(sym, df)
    with pytest.raises(ArcticDbNotYetImplemented) as append_batch_exception:
        lib.append_batch([WritePayload(sym, df)])
    with pytest.raises(ArcticDbNotYetImplemented) as update_exception:
        lib.update(sym, df)

    assert all(col_name in str(e.value) for e in
               [write_exception, write_batch_exception, append_exception, append_batch_exception, update_exception])
    assert "write_pickle" in str(write_exception.value) and "pickle_on_failure" not in str(write_exception.value)
    assert "write_pickle_batch" in str(write_batch_exception.value) and "pickle_on_failure" not in str(write_batch_exception.value)
    assert all("write_pickle" not in str(e.value) for e in
               [append_exception, append_batch_exception, update_exception])
