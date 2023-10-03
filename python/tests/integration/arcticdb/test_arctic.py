"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import sys

import pytz
from arcticdb_ext.exceptions import InternalException
from arcticdb.exceptions import ArcticDbNotYetImplemented, LibraryNotFound

from arcticdb_ext.storage import NoDataFoundException

from arcticdb.arctic import Arctic
from arcticdb.exceptions import MismatchingLibraryOptions
from arcticdb.encoding_version import EncodingVersion
from arcticdb.options import LibraryOptions
from arcticdb import QueryBuilder, DataError
from arcticc.pb2.s3_storage_pb2 import Config as S3Config

import math
import re
import pytest
import pandas as pd
from datetime import datetime, timezone
import numpy as np

from arcticdb.config import MACOS_CONDA_BUILD, MACOS_CONDA_BUILD_SKIP_REASON
from arcticdb.util.test import assert_frame_equal, RUN_MONGO_TEST

from botocore.client import BaseClient as BotoClient
import time


from arcticdb.version_store.library import (
    WritePayload,
    ArcticUnsupportedDataTypeException,
    ReadRequest,
    StagedDataFinalizeMethod,
    ArcticInvalidApiUsageException,
)


def test_library_creation_deletion(arctic_client):
    ac = arctic_client
    assert ac.list_libraries() == []
    ac.create_library("pytest_test_lib")
    with pytest.raises(ValueError):
        ac.create_library("pytest_test_lib")

    assert ac.list_libraries() == ["pytest_test_lib"]
    assert ac.has_library("pytest_test_lib")
    assert ac["pytest_test_lib"].name == "pytest_test_lib"

    ac.delete_library("pytest_test_lib")
    # Want this to be silent.
    ac.delete_library("library_that_does_not_exist")

    assert not ac.list_libraries()
    with pytest.raises(LibraryNotFound):
        _lib = ac["pytest_test_lib"]
    assert not ac.has_library("pytest_test_lib")


def test_get_library(arctic_client):
    ac = arctic_client
    assert ac.list_libraries() == []
    # Throws if library doesn't exist
    with pytest.raises(LibraryNotFound):
        _ = ac.get_library("pytest_test_lib")
    # Creates library with default options if just create_if_missing set to True
    lib = ac.get_library("pytest_test_lib_default_options", create_if_missing=True)

    assert lib.options() == LibraryOptions(encoding_version=ac._encoding_version)
    # Creates library with the specified options if create_if_missing set to True and options provided
    library_options = LibraryOptions(
        dynamic_schema=True,
        dedup=True,
        rows_per_segment=10,
        columns_per_segment=10,
        encoding_version=EncodingVersion.V1 if ac._encoding_version == EncodingVersion.V2 else EncodingVersion.V2,
    )
    lib = ac.get_library("pytest_test_lib_specified_options", create_if_missing=True, library_options=library_options)
    assert lib.options() == library_options
    # If the library already exists, create_if_missing is True, and options are provided, then the provided options must match the existing library
    library_options.dynamic_schema = False
    with pytest.raises(MismatchingLibraryOptions):
        _ = ac.get_library("pytest_test_lib_specified_options", create_if_missing=True, library_options=library_options)
    # Throws if library_options are provided but create_if_missing is False
    with pytest.raises(ArcticInvalidApiUsageException):
        _ = ac.get_library("pytest_test_lib", create_if_missing=False, library_options=library_options)


def test_do_not_persist_s3_details(moto_s3_endpoint_and_credentials):
    """We apply an in-memory overlay for these instead. In particular we should absolutely not persist credentials
    in the storage."""

    def _get_s3_storage_config(cfg):
        primary_storage_name = cfg.lib_desc.storage_ids[0]
        primary_any = cfg.storage_by_id[primary_storage_name]
        s3_config = S3Config()
        primary_any.config.Unpack(s3_config)
        return s3_config

    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials
    uri = (
        endpoint.replace("http://", "s3://").rsplit(":", 1)[0]
        + ":"
        + bucket
        + "?access="
        + aws_access_key
        + "&secret="
        + aws_secret_key
        + "&port="
        + port
    )

    ac = Arctic(uri)
    ac.create_library("test")

    lib = ac["test"]
    lib.write("sym", pd.DataFrame())

    config = ac._library_manager.get_library_config("test")
    s3_storage = _get_s3_storage_config(config)
    assert s3_storage.bucket_name == ""
    assert s3_storage.credential_name == ""
    assert s3_storage.credential_key == ""
    assert s3_storage.endpoint == ""
    assert s3_storage.max_connections == 0
    assert s3_storage.connect_timeout == 0
    assert s3_storage.request_timeout == 0
    assert not s3_storage.ssl
    assert s3_storage.prefix.startswith("test")
    assert not s3_storage.https
    assert s3_storage.region == ""
    assert not s3_storage.use_virtual_addressing

    assert "sym" in ac["test"].list_symbols()


def test_library_options(arctic_client):
    ac = arctic_client
    assert ac.list_libraries() == []
    ac.create_library("pytest_default_options")
    lib = ac["pytest_default_options"]
    assert lib.options() == LibraryOptions(encoding_version=ac._encoding_version)
    write_options = lib._nvs._lib_cfg.lib_desc.version.write_options
    assert not write_options.dynamic_schema
    assert not write_options.de_duplication
    assert write_options.segment_row_size == 100_000
    assert write_options.column_group_size == 127
    assert lib._nvs._lib_cfg.lib_desc.version.encoding_version == ac._encoding_version

    library_options = LibraryOptions(
        dynamic_schema=True, dedup=True, rows_per_segment=20, columns_per_segment=3, encoding_version=EncodingVersion.V2
    )
    ac.create_library(
        "pytest_explicit_options",
        library_options,
    )
    lib = ac["pytest_explicit_options"]
    assert lib.options() == library_options
    write_options = lib._nvs._lib_cfg.lib_desc.version.write_options
    assert write_options.dynamic_schema
    assert write_options.de_duplication
    assert write_options.segment_row_size == 20
    assert write_options.column_group_size == 3
    assert write_options.dynamic_strings
    assert lib._nvs._lib_cfg.lib_desc.version.encoding_version == EncodingVersion.V2


def test_separation_between_libraries(arctic_client):
    """Validate that symbols in one library are not exposed in another."""
    ac = arctic_client
    assert ac.list_libraries() == []

    ac.create_library("pytest_test_lib")
    ac.create_library("pytest_test_lib_2")

    assert set(ac.list_libraries()) == {"pytest_test_lib", "pytest_test_lib_2"}

    ac["pytest_test_lib"].write("test_1", pd.DataFrame())
    ac["pytest_test_lib_2"].write("test_2", pd.DataFrame())
    assert ac["pytest_test_lib"].list_symbols() == ["test_1"]
    assert ac["pytest_test_lib_2"].list_symbols() == ["test_2"]


def get_path_prefix_option(uri):
    if "azure" in uri:  # azure connection string has a different format
        return ";Path_prefix"
    else:
        return "&path_prefix"


def test_separation_between_libraries_with_prefixes(object_storage_uri_incl_bucket):
    """The motivation for the prefix feature is that separate users want to be able to create libraries
    with the same name in the same bucket without over-writing each other's work. This can be useful when
    creating a new bucket is time-consuming, for example due to organizational issues.
    """
    if "mongo" in object_storage_uri_incl_bucket:
        pytest.skip("Mongo doesn't support path_prefix")

    option = get_path_prefix_option(object_storage_uri_incl_bucket)
    if option not in object_storage_uri_incl_bucket:
        mercury_uri = f"{object_storage_uri_incl_bucket}{option}=/planet_mercury"
    else:
        # if we have a path_prefix, we assume that it is at the end and we simply append to it
        mercury_uri = f"{object_storage_uri_incl_bucket}/planet_mercury"

    ac_mercury = Arctic(mercury_uri)

    if option not in object_storage_uri_incl_bucket:
        mars_uri = f"{object_storage_uri_incl_bucket}{option}=/planet_mars"
    else:
        # if we have a path_prefix, we assume that it is at the end and we simply append to it
        mars_uri = f"{object_storage_uri_incl_bucket}/planet_mars"

    ac_mars = Arctic(mars_uri)

    assert ac_mars.list_libraries() == []
    ac_mercury.create_library("pytest_test_lib")
    ac_mercury.create_library("pytest_test_lib_2")
    ac_mars.create_library("pytest_test_lib")
    ac_mars.create_library("pytest_test_lib_2")
    assert ac_mercury.list_libraries() == ["pytest_test_lib", "pytest_test_lib_2"]
    assert ac_mars.list_libraries() == ["pytest_test_lib", "pytest_test_lib_2"]

    ac_mercury["pytest_test_lib"].write("test_1", pd.DataFrame())
    ac_mars["pytest_test_lib"].write("test_2", pd.DataFrame())

    assert ac_mercury["pytest_test_lib"].list_symbols() == ["test_1"]
    assert ac_mars["pytest_test_lib"].list_symbols() == ["test_2"]

    ac_mercury.delete_library("pytest_test_lib")
    ac_mercury.delete_library("pytest_test_lib_2")

    ac_mars.delete_library("pytest_test_lib")
    ac_mars.delete_library("pytest_test_lib_2")


def object_storage_uri_and_client():
    if MACOS_CONDA_BUILD:
        return [("moto_s3_uri_incl_bucket", "boto_client")]

    return [
        ("moto_s3_uri_incl_bucket", "boto_client"),
        ("azurite_azure_uri_incl_bucket", "azure_client_and_create_container"),
    ]


@pytest.mark.parametrize("connection_string, client", object_storage_uri_and_client())
def test_library_management_path_prefix(connection_string, client, request):
    connection_string = request.getfixturevalue(request.getfixturevalue("connection_string"))
    client = request.getfixturevalue(request.getfixturevalue("client"))

    if isinstance(client, BotoClient):
        test_bucket = sorted(client.list_buckets()["Buckets"], key=lambda bucket_meta: bucket_meta["CreationDate"])[-1][
            "Name"
        ]
    else:
        time.sleep(1)  # Azurite is slow....
        test_bucket = list(client.list_containers())

    URI = f"{connection_string}{get_path_prefix_option(connection_string)}=hello/world"
    ac = Arctic(URI)
    assert ac.list_libraries() == []

    ac.create_library("pytest_test_lib")

    ac["pytest_test_lib"].write("test_1", pd.DataFrame())
    ac["pytest_test_lib"].write("test_2", pd.DataFrame())

    assert sorted(ac["pytest_test_lib"].list_symbols()) == ["test_1", "test_2"]

    ac["pytest_test_lib"].snapshot("test_snapshot")
    assert ac["pytest_test_lib"].list_snapshots() == {"test_snapshot": None}

    if isinstance(client, BotoClient):
        keys = [d["Key"] for d in client.list_objects(Bucket=test_bucket)["Contents"]]
    else:
        REGEX = r".*Container=(?P<container>[^;]+).*"
        match = re.match(REGEX, connection_string)
        container = match.groupdict()["container"]
        keys = [blob["name"] for blob in client.get_container_client(container).list_blobs()]
    assert all(k.startswith("hello/world") for k in keys)
    assert any(k.startswith("hello/world/_arctic_cfg") for k in keys)
    assert any(k.startswith("hello/world/pytest_test_lib") for k in keys)

    ac.delete_library("pytest_test_lib")

    assert not ac.list_libraries()
    with pytest.raises(LibraryNotFound):
        _lib = ac["pytest_test_lib"]


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


def staged_write(sym, arctic_library):
    lib = arctic_library
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    df.index = pd.date_range("2018-01-01", periods=3, freq="H")
    lib.write(sym, df, staged=True)

    df = pd.DataFrame({"col1": [4, 5, 6], "col2": [7, 8, 9]})
    df.index = pd.date_range("2018-01-01 03:00:00", periods=3, freq="H")
    lib.write(sym, df, staged=True)


def test_parallel_writes_and_appends(arctic_library):
    lib = arctic_library
    staged_write("my_symbol", lib)
    staged_write("my_other_symbol", lib)
    staged_write("yet_another_symbol", lib)

    lib.finalize_staged_data("my_symbol", StagedDataFinalizeMethod.WRITE)
    lib.finalize_staged_data("my_other_symbol", StagedDataFinalizeMethod.WRITE)

    assert set(lib.list_symbols()) == {"my_symbol", "my_other_symbol"}

    comp_df = pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6], "col2": [4, 5, 6, 7, 8, 9]})
    comp_df.index = pd.date_range("2018-01-01", periods=6, freq="H")
    assert_frame_equal(lib.read("my_symbol").data, comp_df)
    assert_frame_equal(lib.read("my_other_symbol").data, comp_df)

    df = pd.DataFrame({"col1": [7, 8, 9], "col2": [10, 11, 12]})
    df.index = pd.date_range("2018-01-01 06:00:00", periods=3, freq="H")
    lib.write("my_symbol", df, staged=True)

    lib.finalize_staged_data("my_symbol", StagedDataFinalizeMethod.APPEND)

    comp_df = pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6, 7, 8, 9], "col2": [4, 5, 6, 7, 8, 9, 10, 11, 12]})
    comp_df.index = pd.date_range("2018-01-01", periods=9, freq="H")
    assert_frame_equal(lib.read("my_symbol").data, comp_df)


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
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    lib.write("symbol", df, metadata={"very": "interesting"})
    lib.write("symbol", df, metadata={"muy": "interesante"}, prune_previous_versions=False)
    lib.snapshot("my_snap")
    lib.delete("symbol", versions=1)
    assert lib["symbol"].version == 0
    assert lib["symbol"].metadata == {"very": "interesting"}
    assert lib.read("symbol", as_of=1).version == 1
    assert lib.read("symbol", as_of=1).metadata == {"muy": "interesante"}
    assert lib.read("symbol", as_of="my_snap").version == 1
    assert lib.read("symbol", as_of="my_snap").metadata == {"muy": "interesante"}


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


@pytest.mark.skipif(not RUN_MONGO_TEST, reason="Mongo test on windows is fiddly")
def test_mongo_repr(mongo_test_uri):
    max_pool_size = 10
    min_pool_size = 100
    selection_timeout_ms = 1000
    uri = f"{mongo_test_uri}/?maxPoolSize={max_pool_size}&minPoolSize={min_pool_size}&serverSelectionTimeoutMS={selection_timeout_ms}"
    ac = Arctic(uri)
    assert repr(ac) == f"Arctic(config=mongodb(endpoint={mongo_test_uri[len('mongodb://'):]}))"


def test_s3_repr(moto_s3_uri_incl_bucket):
    ac = Arctic(moto_s3_uri_incl_bucket)

    assert ac.list_libraries() == []
    ac.create_library("pytest_test_lib")

    lib = ac["pytest_test_lib"]
    s3_endpoint = moto_s3_uri_incl_bucket.split("//")[1].split(":")[0]
    port = moto_s3_uri_incl_bucket.split("port=")[1]
    s3_endpoint += f":{port}"
    bucket = moto_s3_uri_incl_bucket.split(":")[-1].split("?")[0]
    assert (
        repr(lib)
        == "Library("
        "Arctic("
        "config=S3("
        f"endpoint={s3_endpoint}, bucket={bucket})), path=pytest_test_lib, storage=s3_storage)"
    )

    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    written_vi = lib.write("my_symbol", df)
    assert re.match(r"S3\(endpoint=localhost:\d+, bucket=test_bucket_\d+\)", written_vi.host)


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
    assert info.date_range == (datetime(2018, 1, 1, tzinfo=timezone.utc), datetime(2018, 1, 6, tzinfo=timezone.utc))
    assert info.index[0] == ["named_index"]
    assert info.index_type == "index"
    assert info.row_count == 6
    assert original_info.row_count == 4
    assert info.last_update_time > original_info.last_update_time
    assert info.last_update_time.tz == pytz.UTC


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


@pytest.mark.parametrize("dedup", [True, False])
def test_dedup(arctic_client, dedup):
    ac = arctic_client
    assert ac.list_libraries() == []
    ac.create_library("pytest_test_library", LibraryOptions(dedup=dedup))
    lib = ac["pytest_test_library"]
    symbol = "test_dedup"
    lib.write_pickle(symbol, 1)
    lib.write_pickle(symbol, 1, prune_previous_versions=False)
    data_key_version = lib._nvs.read_index(symbol)["version_id"][0]
    assert data_key_version == 0 if dedup else 1


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


@pytest.mark.parametrize("connection_string, client", object_storage_uri_and_client())
def test_reload_symbol_list(connection_string, client, request):
    connection_string = request.getfixturevalue(request.getfixturevalue("connection_string"))
    client = request.getfixturevalue(request.getfixturevalue("client"))

    def get_symbol_list_keys():
        if isinstance(client, BotoClient):
            keys = [
                d["Key"] for d in client.list_objects(Bucket=test_bucket)["Contents"] if d["Key"].startswith(lib_name)
            ]
        else:
            time.sleep(1)  # Azurite is slow....
            REGEX = r".*Container=(?P<container>[^;]+).*"
            match = re.match(REGEX, connection_string)
            container = match.groupdict()["container"]
            keys = [
                blob["name"]
                for blob in client.get_container_client(container).list_blobs()
                if blob["name"].startswith(lib_name)
            ]
        symbol_list_keys = []
        for key in keys:
            path_components = key.split("/")
            if path_components[1] == "sl":
                symbol_list_keys.append(path_components[2])
        return symbol_list_keys

    if isinstance(client, BotoClient):
        test_bucket = sorted(client.list_buckets()["Buckets"], key=lambda bucket_meta: bucket_meta["CreationDate"])[-1][
            "Name"
        ]
    else:
        test_bucket = list(client.list_containers())

    ac = Arctic(connection_string)
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


def test_get_uri(object_storage_uri_incl_bucket):
    ac = Arctic(object_storage_uri_incl_bucket)
    assert ac.get_uri() == object_storage_uri_incl_bucket


def test_azure_no_ca_path(azurite_azure_test_connection_setting):
    endpoint, container, credential_name, credential_key, _ = azurite_azure_test_connection_setting
    Arctic(
        f"azure://DefaultEndpointsProtocol=http;AccountName={credential_name};AccountKey={credential_key};BlobEndpoint={endpoint}/{credential_name};Container={container}"
    )


@pytest.mark.skipif(MACOS_CONDA_BUILD, reason=MACOS_CONDA_BUILD_SKIP_REASON)
def test_azure_sas_token(azure_account_sas_token, azurite_azure_test_connection_setting):
    endpoint, container, credential_name, _, _ = azurite_azure_test_connection_setting
    ac = Arctic(
        f"azure://DefaultEndpointsProtocol=http;SharedAccessSignature={azure_account_sas_token};BlobEndpoint={endpoint}/{credential_name};Container={container}"
    )
    expected = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    sym = "test"
    lib = "lib"
    ac.create_library(lib)
    ac[lib].write(sym, expected)
    assert_frame_equal(expected, ac[lib].read(sym).data)

    assert ac.list_libraries() == [lib]


def test_s3_force_uri_lib_config_handling(moto_s3_uri_incl_bucket):
    # force_uri_lib_config is a obsolete configuration. However, user still includes this option in their setup. For backward compatitbility, we need to make sure such setup will still work
    # Why it becomes obsolete: https://github.com/man-group/ArcticDB/pull/803
    Arctic(f"{moto_s3_uri_incl_bucket}&force_uri_lib_config=true)")

    with pytest.raises(ValueError):
        Arctic(f"{moto_s3_uri_incl_bucket}&force_uri_lib_config=false)")
