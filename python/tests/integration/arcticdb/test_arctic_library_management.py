"""
Copyright 2024 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import sys

import pytest
import pandas as pd

from arcticdb.util.utils import delete_library
from arcticdb_ext.exceptions import InternalException, UserInputException
from arcticdb_ext.storage import KeyType
from arcticdb.exceptions import (
    ArcticDbNotYetImplemented,
    LibraryNotFound,
    MismatchingLibraryOptions,
)
from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions, EnterpriseLibraryOptions
from arcticdb.encoding_version import EncodingVersion
from arcticc.pb2.s3_storage_pb2 import Config as S3Config
from arcticdb.storage_fixtures.api import (
    StorageFixture,
    ArcticUriFields,
    StorageFixtureFactory,
)
from arcticdb.version_store.library import (
    WritePayload,
    ArcticUnsupportedDataTypeException,
    ReadRequest,
    StagedDataFinalizeMethod,
    ArcticInvalidApiUsageException,
)

from tests.util.mark import (
    AZURE_TESTS_MARK,
    MONGO_TESTS_MARK,
    REAL_GCP_TESTS_MARK,
    REAL_S3_TESTS_MARK,
    SSL_TEST_SUPPORTED,
    SSL_TEST_SUPPORTED,
)
from tests.util.storage_test import get_s3_storage_config

from arcticdb.options import ModifiableEnterpriseLibraryOption, ModifiableLibraryOption


@pytest.mark.storage
def test_library_creation_deletion(arctic_client, lib_name):
    ac = arctic_client
    ac.create_library(lib_name)
    try:
        with pytest.raises(ValueError):
            ac.create_library(lib_name)

        assert lib_name in ac.list_libraries()
        assert ac.has_library(lib_name)
        assert lib_name in ac
        if "mongo" in arctic_client.get_uri():
            # The mongo fixture uses PrefixingLibraryAdapterDecorator which leaks in this one case
            assert ac[lib_name].name.endswith(lib_name)
        else:
            assert ac[lib_name].name == lib_name

        ac.delete_library(lib_name)
        # Want this to be silent.
        ac.delete_library("library_that_does_not_exist")

        assert lib_name not in ac.list_libraries()
        with pytest.raises(LibraryNotFound):
            _lib = ac[lib_name]
        assert not ac.has_library(lib_name)
        assert lib_name not in ac
    finally:
        delete_library(ac, lib_name)


@pytest.mark.storage
def test_get_library(arctic_client, lib_name):
    ac = arctic_client
    # Throws if library doesn't exist
    with pytest.raises(LibraryNotFound):
        _ = ac.get_library(lib_name)
    # Creates library with default options if just create_if_missing set to True
    lname = f"{lib_name}_do"
    lib = ac.get_library(lname, create_if_missing=True)
    assert lib.options() == LibraryOptions(encoding_version=ac._encoding_version)
    ac.delete_library(lname)

    # Creates library with the specified options if create_if_missing set to True and options provided
    library_options = LibraryOptions(
        dynamic_schema=True,
        dedup=True,
        rows_per_segment=10,
        columns_per_segment=10,
        encoding_version=EncodingVersion.V1 if ac._encoding_version == EncodingVersion.V2 else EncodingVersion.V2,
    )
    lname = f"{lib_name}_so" # specific options
    lib = ac.get_library(
        lname, 
        create_if_missing=True,
        library_options=library_options,
    )
    assert lib.options() == library_options

    # If the library already exists, create_if_missing is True, and options are provided, then the provided options must match the existing library
    library_options.dynamic_schema = False
    with pytest.raises(MismatchingLibraryOptions):
        _ = ac.get_library(
            lname, # specific options
            create_if_missing=True,
            library_options=library_options,
        )
    # Throws if library_options are provided but create_if_missing is False
    with pytest.raises(ArcticInvalidApiUsageException):
        _ = ac.get_library(lib_name, create_if_missing=False, library_options=library_options)

    delete_library(ac, lib_name)


def test_create_library_enterprise_options_defaults(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name)

        enterprise_options = lib.enterprise_options()
        assert not enterprise_options.replication
        assert not enterprise_options.background_deletion
    finally:
        delete_library(ac, lib_name)

def test_create_library_enterprise_options_set(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(
            lib_name,
            enterprise_library_options=EnterpriseLibraryOptions(replication=True, background_deletion=True),
        )

        enterprise_options = lib.enterprise_options()
        assert enterprise_options.replication
        assert enterprise_options.background_deletion

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.sync_passive.enabled
        assert proto_options.delayed_deletes
    finally:
        delete_library(ac, lib_name)


def test_create_library_replication_option_set_writes_logs(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name, enterprise_library_options=EnterpriseLibraryOptions(replication=True))
        lt = lib._nvs.library_tool()
        assert not lt.find_keys(KeyType.LOG)

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        lib.write("abc", df)

        assert len(lt.find_keys(KeyType.LOG))
    finally:
        delete_library(ac, lib_name)


def test_create_library_background_deletion_option_set_does_not_delete(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(
            lib_name,
            enterprise_library_options=EnterpriseLibraryOptions(background_deletion=True),
        )
        lt = lib._nvs.library_tool()

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        lib.write("abc", df)
        lib.delete("abc")

        assert len(lt.find_keys(KeyType.TABLE_DATA))
    finally:
        delete_library(ac, lib_name)


def test_modify_options_affect_in_memory_lib(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name)

        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.REPLICATION, True)
        ac.modify_library_option(lib, ModifiableLibraryOption.DEDUP, False)

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.sync_passive.enabled
        assert not proto_options.de_duplication
    finally:
        delete_library(ac, lib_name)


def test_modify_options_affect_persistent_lib_config(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name)

        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.REPLICATION, True)
        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.BACKGROUND_DELETION, True)

        new_client = Arctic(ac.get_uri())
        new_lib = new_client[lib_name]
        proto_options = new_lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.sync_passive.enabled
        assert proto_options.delayed_deletes
    finally:
        delete_library(ac, lib_name)


def test_modify_options_dedup(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name)

        ac.modify_library_option(lib, ModifiableLibraryOption.DEDUP, False)

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert not proto_options.de_duplication

        ac.modify_library_option(lib, ModifiableLibraryOption.DEDUP, True)

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.de_duplication
    finally:
        delete_library(ac, lib_name)


def test_modify_options_rows_per_segment(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name)

        ac.modify_library_option(lib, ModifiableLibraryOption.ROWS_PER_SEGMENT, 100)

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.segment_row_size == 100

        ac.modify_library_option(lib, ModifiableLibraryOption.ROWS_PER_SEGMENT, 200)

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.segment_row_size == 200
    finally:
        delete_library(ac, lib_name)


def test_modify_options_cols_per_segment(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name)

        ac.modify_library_option(lib, ModifiableLibraryOption.COLUMNS_PER_SEGMENT, 100)

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.column_group_size == 100

        ac.modify_library_option(lib, ModifiableLibraryOption.COLUMNS_PER_SEGMENT, 200)

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.column_group_size == 200
    finally:
        delete_library(ac, lib_name)


def test_modify_options_replication(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name)

        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.REPLICATION, True)

        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert proto_options.sync_passive.enabled

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        lib.write("abc", df)

        lt = lib._nvs.library_tool()
        assert len(lt.find_keys(KeyType.LOG)) == 1

        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.REPLICATION, False)
        proto_options = lib._nvs.lib_cfg().lib_desc.version.write_options
        assert not proto_options.sync_passive.enabled

        lib.write("def", df)
        assert len(lt.find_keys(KeyType.LOG)) == 1
    finally:
        delete_library(ac, lib_name)


def test_modify_options_background_deletion(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    try:
        lib = ac.create_library(lib_name)
        lt = lib._nvs.library_tool()

        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.BACKGROUND_DELETION, True)
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        lib.write("abc", df)
        lib.delete("abc")

        assert len(lt.find_keys(KeyType.TABLE_DATA))
    finally:
        delete_library(ac, lib_name)


@pytest.mark.storage
def test_create_library_with_invalid_name(arctic_client_v1, lib_name):
    ac = arctic_client_v1
    try:
        # These should succeed because the names are valid
        valid_names = [lib_name, "lib-with-dash", "lib.with.dot", "lib123"]
        # These should fail because the names are invalid
        invalid_names = [chr(0), "lib>", "lib<", "lib*", "/lib", "lib...lib", lib_name * 1000]
        # This name should fail on mongo, and succeed on other storages
        if "mongo" in ac.get_uri():
            invalid_names += ["lib/with/slash"]
        else:
            valid_names += ["lib/with/slash"]

        for lib_name in valid_names:
            ac.create_library(lib_name)

        for lib_name in invalid_names:
            with pytest.raises(UserInputException):
                ac.create_library(lib_name)

        # Verify that library list is not corrupted
        all_libraries = set(ac.list_libraries())
        assert all(lib_name in all_libraries for lib_name in valid_names)
    finally:
        for lib_name in valid_names:
            delete_library(ac, lib_name)


def test_do_not_persist_s3_details(s3_storage):
    """We apply an in-memory overlay for these instead. In particular we should absolutely not persist credentials
    in the storage."""
    if SSL_TEST_SUPPORTED:
        assert s3_storage.arctic_uri.startswith("s3s://")
    ac = Arctic(s3_storage.arctic_uri)
    try:
        lib = ac.create_library("test")
        lib.write("sym", pd.DataFrame())

        config = ac._library_manager.get_library_config("test")
        s3_storage = get_s3_storage_config(config)
        assert s3_storage.bucket_name == ""
        assert s3_storage.credential_name == ""
        assert s3_storage.credential_key == ""
        assert s3_storage.endpoint == ""
        assert s3_storage.max_connections == 0
        assert s3_storage.connect_timeout == 0
        assert s3_storage.request_timeout == 0
        assert not s3_storage.ssl
        assert s3_storage.prefix.startswith("test")
        assert s3_storage.region == ""
        assert not s3_storage.use_virtual_addressing
        # HTTS is persisted on purpose to support backwards compatibility
        if SSL_TEST_SUPPORTED:
            assert s3_storage.https
        else:
            assert not s3_storage.https
    finally:
        delete_library(ac, "test")


@pytest.mark.storage
def test_library_options(arctic_client, lib_name):
    ac = arctic_client
    lib_name_do = f"{lib_name}_do" # default options
    ac.create_library(lib_name_do)
    lib = ac[lib_name_do]
    assert lib.options() == LibraryOptions(encoding_version=ac._encoding_version)
    write_options = lib._nvs._lib_cfg.lib_desc.version.write_options
    assert not write_options.dynamic_schema
    assert not write_options.de_duplication
    assert write_options.segment_row_size == 100_000
    assert write_options.column_group_size == 127
    assert lib._nvs._lib_cfg.lib_desc.version.encoding_version == ac._encoding_version

    library_options = LibraryOptions(
        dynamic_schema=True,
        dedup=True,
        rows_per_segment=20,
        columns_per_segment=3,
        encoding_version=EncodingVersion.V2,
    )
    ac.delete_library(lib_name_do)
    lib_name_eo = f"{lib_name}_eo" # explicit options
    ac.create_library(
        lib_name_eo,
        library_options,
    )
    lib = ac[lib_name_eo]
    assert lib.options() == library_options
    write_options = lib._nvs._lib_cfg.lib_desc.version.write_options
    assert write_options.dynamic_schema
    assert write_options.de_duplication
    assert write_options.segment_row_size == 20
    assert write_options.column_group_size == 3
    assert write_options.dynamic_strings
    assert lib._nvs._lib_cfg.lib_desc.version.encoding_version == EncodingVersion.V2
    delete_library(ac, lib_name_eo)


@pytest.mark.storage
def test_separation_between_libraries(arctic_client_v1, lib_name):
    # This fails for mem-backed without the library caching implemented in
    # issue #520 then re-implemented in issue #889
    """Validate that symbols in one library are not exposed in another."""
    ac = arctic_client_v1
    lib_name_1 = f"{lib_name}_1"
    lib_name_2 = f"{lib_name}_2"
    ac.create_library(lib_name_1)
    ac.create_library(lib_name_2)
    try:
        assert lib_name_1 in set(ac.list_libraries())
        assert lib_name_2 in set(ac.list_libraries())

        ac[lib_name_1].write("test_1", pd.DataFrame())
        ac[lib_name_2].write("test_2", pd.DataFrame())
        assert ac[lib_name_1].list_symbols() == ["test_1"]
        assert ac[lib_name_2].list_symbols() == ["test_2"]

    finally:
        delete_library(ac, lib_name_1)
        delete_library(ac, lib_name_2)


def add_path_prefix(storage_fixture, prefix):
    if "path_prefix".casefold() in storage_fixture.arctic_uri.casefold():
        return storage_fixture.replace_uri_field(
            storage_fixture.arctic_uri,
            ArcticUriFields.PATH_PREFIX,
            prefix,
            start=3,
            end=2,
        )

    if "azure" in storage_fixture.arctic_uri:  # azure connection string has a different format
        return f"{storage_fixture.arctic_uri};Path_prefix={prefix}"
    else:
        return f"{storage_fixture.arctic_uri}&path_prefix={prefix}"


@pytest.mark.parametrize(
    "fixture",
    [
        "s3_storage",
        pytest.param("azurite_storage", marks=AZURE_TESTS_MARK),
        pytest.param("real_s3_storage", marks=REAL_S3_TESTS_MARK),
        pytest.param("real_gcp_storage", marks=REAL_GCP_TESTS_MARK),
    ],
)
@pytest.mark.storage
def test_separation_between_libraries_with_prefixes(fixture, request):
    """The motivation for the prefix feature is that separate users want to be able to create libraries
    with the same name in the same bucket without over-writing each other's work. This can be useful when
    creating a new bucket is time-consuming, for example due to organizational issues.
    """
    storage_fixture: StorageFixture = request.getfixturevalue(fixture)
    mercury_uri = add_path_prefix(storage_fixture, "/planet/mercury")
    ac_mercury = Arctic(mercury_uri)

    mars_uri = add_path_prefix(storage_fixture, "/planet/mars")
    ac_mars = Arctic(mars_uri)

    try:
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

    finally:
        ac_mercury.delete_library("pytest_test_lib")
        ac_mercury.delete_library("pytest_test_lib_2")

        ac_mars.delete_library("pytest_test_lib")
        ac_mars.delete_library("pytest_test_lib_2")


@pytest.mark.parametrize("fixture", ["s3_storage", pytest.param("azurite_storage", marks=AZURE_TESTS_MARK)])
def test_library_management_path_prefix(fixture, request, lib_name):
    storage_fixture: StorageFixture = request.getfixturevalue(fixture)
    uri = add_path_prefix(storage_fixture, "hello/world")
    ac = Arctic(uri)

    try:
        ac.create_library(lib_name)

        ac[lib_name].write("test_1", pd.DataFrame())
        ac[lib_name].write("test_2", pd.DataFrame())

        assert sorted(ac[lib_name].list_symbols()) == ["test_1", "test_2"]

        ac[lib_name].snapshot("test_snapshot")
        assert ac[lib_name].list_snapshots() == {"test_snapshot": None}

        keys = list(storage_fixture.iter_underlying_object_names())
        print(keys)
        # filter out the keys that are not related to the library
        keys = [k for k in keys if "test_library_management_path_p" in k]
        print(keys)
        assert all(k.startswith("hello/world") for k in keys)
        assert any(k.startswith("hello/world/_arctic_cfg") for k in keys)
        assert any(k.startswith("hello/world/test_library_management_path_p") for k in keys)
    finally:    
        delete_library(ac, lib_name)

    assert lib_name not in ac.list_libraries()
    with pytest.raises(LibraryNotFound):
        _lib = ac[lib_name]
