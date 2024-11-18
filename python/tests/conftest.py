"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import enum
from typing import Callable, Generator, Iterator
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.version_store.library import Library
import hypothesis
import os
import pytest
import pandas as pd
import platform
import random
import re
import time
import requests
from datetime import datetime
from functools import partial

from arcticdb import LibraryOptions
from arcticdb.storage_fixtures.api import StorageFixture
from arcticdb.storage_fixtures.azure import AzuriteStorageFixtureFactory
from arcticdb.storage_fixtures.lmdb import LmdbStorageFixture
from arcticdb.storage_fixtures.s3 import (
    MotoS3StorageFixtureFactory,
    MotoNfsBackedS3StorageFixtureFactory,
    NfsS3Bucket,
    S3Bucket,
    real_s3_from_environment_variables,
    mock_s3_with_error_simulation,
)
from arcticdb.storage_fixtures.mongo import auto_detect_server
from arcticdb.storage_fixtures.in_memory import InMemoryStorageFixture
from arcticdb.version_store._normalization import MsgPackNormalizer
from arcticdb.util.test import create_df
from arcticdb.arctic import Arctic
from .util.mark import (
    AZURE_TESTS_MARK,
    MONGO_TESTS_MARK,
    REAL_S3_TESTS_MARK,
    SSL_TEST_SUPPORTED,
)

# region =================================== Misc. Constants & Setup ====================================
hypothesis.settings.register_profile("ci_linux", max_examples=100)
hypothesis.settings.register_profile("ci_windows", max_examples=100)
hypothesis.settings.register_profile("dev", max_examples=100)

hypothesis.settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))

# Use a smaller memory mapped limit for all tests
MsgPackNormalizer.MMAP_DEFAULT_SIZE = 20 * (1 << 20)

if platform.system() == "Linux":
    try:
        from ctypes import cdll

        cdll.LoadLibrary("libSegFault.so")
    except:
        pass


@pytest.fixture()
def sym(request : pytest.FixtureRequest):
    return request.node.name + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture()
def lib_name(request: pytest.FixtureRequest):
    name = re.sub(r"[^\w]", "_", request.node.name)[:30]
    return f"{name}.{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}"


@pytest.fixture
def get_stderr(capfd):
    from arcticdb_ext.log import flush_all

    def _get():
        flush_all()
        time.sleep(0.001)
        return capfd.readouterr().err

    return _get


@enum.unique
class EncodingVersion(enum.IntEnum):
    V1 = 0
    V2 = 1


@pytest.fixture(scope="session")
def only_test_encoding_version_v1():
    """Dummy fixture to reference at module/class level to reduce test cases"""


def pytest_generate_tests(metafunc):
    if "encoding_version" in metafunc.fixturenames:
        only_v1 = "only_test_encoding_version_v1" in metafunc.fixturenames
        metafunc.parametrize("encoding_version", [EncodingVersion.V1] if only_v1 else list(EncodingVersion))


# endregion
# region ======================================= Storage Fixtures =======================================
@pytest.fixture
def lmdb_storage(tmp_path) -> Iterator[LmdbStorageFixture]:
    with LmdbStorageFixture(tmp_path) as f:
        yield f

@pytest.fixture
def lmdb_library(lmdb_storage, lib_name):
    return lmdb_storage.create_arctic().create_library(lib_name)

@pytest.fixture
def lmdb_library_dynamic_schema(lmdb_storage, lib_name):
    return lmdb_storage.create_arctic().create_library(lib_name, library_options=LibraryOptions(dynamic_schema=True))

@pytest.fixture(
    scope="function",
    params=(
        "lmdb_library",
        "lmdb_library_dynamic_schema"
    ),
)
def lmdb_library_static_dynamic(request):
    yield request.getfixturevalue(request.param)

# ssl is enabled by default to maximize test coverage as ssl is enabled most of the times in real world
@pytest.fixture(scope="session")
def s3_storage_factory() -> Iterator[MotoS3StorageFixtureFactory]:
    with MotoS3StorageFixtureFactory(use_ssl=SSL_TEST_SUPPORTED, ssl_test_support=SSL_TEST_SUPPORTED, bucket_versioning=False) as f:
        yield f


@pytest.fixture(scope="session")
def s3_no_ssl_storage_factory() -> Iterator[MotoS3StorageFixtureFactory]:
    with MotoS3StorageFixtureFactory(use_ssl=False, ssl_test_support=SSL_TEST_SUPPORTED, bucket_versioning=False) as f:
        yield f


@pytest.fixture(scope="session")
def s3_ssl_disabled_storage_factory() -> Iterator[MotoS3StorageFixtureFactory]:
    with MotoS3StorageFixtureFactory(use_ssl=False, ssl_test_support=False, bucket_versioning=False) as f:
        yield f


@pytest.fixture(scope="session")
def s3_bucket_versioning_storage_factory() -> Iterator[MotoS3StorageFixtureFactory] :
    with MotoS3StorageFixtureFactory(use_ssl=False, ssl_test_support=False, bucket_versioning=True) as f:
        yield f


@pytest.fixture(scope="session")
def nfs_backed_s3_storage_factory() -> Iterator[MotoNfsBackedS3StorageFixtureFactory]:
    with MotoNfsBackedS3StorageFixtureFactory(use_ssl=False, ssl_test_support=False, bucket_versioning=False) as f:
        yield f


@pytest.fixture
def s3_storage(s3_storage_factory) -> Iterator[S3Bucket]:
    with s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture
def nfs_backed_s3_storage(nfs_backed_s3_storage_factory) -> Iterator[NfsS3Bucket]:
    with nfs_backed_s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture
def s3_no_ssl_storage(s3_no_ssl_storage_factory) -> Iterator[S3Bucket]:
    with s3_no_ssl_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture
def s3_ssl_disabled_storage(s3_ssl_disabled_storage_factory) -> Iterator[S3Bucket]:
    with s3_ssl_disabled_storage_factory.create_fixture() as f:
        yield f


# s3 storage is picked just for its versioning capabilities for verifying arcticdb atomicity
@pytest.fixture
def s3_bucket_versioning_storage(s3_bucket_versioning_storage_factory) -> Iterator[S3Bucket]:
    with s3_bucket_versioning_storage_factory.create_fixture() as f:
        s3_admin = f.factory._s3_admin
        bucket = f.bucket
        assert s3_admin.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"
        yield f


@pytest.fixture
def mock_s3_storage_with_error_simulation_factory():
    return mock_s3_with_error_simulation()


@pytest.fixture
def mock_s3_storage_with_error_simulation(mock_s3_storage_with_error_simulation_factory):
    with mock_s3_storage_with_error_simulation_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def real_s3_storage_factory():
    return real_s3_from_environment_variables(shared_path=False)


@pytest.fixture(scope="session")
def real_s3_shared_path_storage_factory():
    return real_s3_from_environment_variables(shared_path=True)


@pytest.fixture(scope="session")
def real_s3_storage_without_clean_up(real_s3_shared_path_storage_factory):
    return real_s3_shared_path_storage_factory.create_fixture()


@pytest.fixture
def real_s3_storage(real_s3_storage_factory):
    with real_s3_storage_factory.create_fixture() as f:
        yield f

# ssl cannot be ON by default due to azurite performance constraints https://github.com/man-group/ArcticDB/issues/1539
@pytest.fixture(scope="session")
def azurite_storage_factory():
    with AzuriteStorageFixtureFactory(use_ssl=False, ssl_test_support=SSL_TEST_SUPPORTED) as f:
        yield f


@pytest.fixture
def azurite_storage(azurite_storage_factory: AzuriteStorageFixtureFactory):
    with azurite_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def azurite_ssl_storage_factory():
    with AzuriteStorageFixtureFactory(use_ssl=True, ssl_test_support=SSL_TEST_SUPPORTED) as f:
        yield f


@pytest.fixture
def azurite_ssl_storage(azurite_ssl_storage_factory: AzuriteStorageFixtureFactory):
    with azurite_ssl_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def mongo_server():
    with auto_detect_server() as s:
        yield s


@pytest.fixture
def mongo_storage(mongo_server):
    with mongo_server.create_fixture() as f:
        yield f


@pytest.fixture
def mem_storage() -> Iterator[InMemoryStorageFixture]:
    with InMemoryStorageFixture() as f:
        yield f


# endregion
# region ==================================== `Arctic` API Fixtures ====================================
@pytest.fixture(
    scope="function",
    params=[
        "s3",
        "lmdb",
        "mem",
        pytest.param("azurite", marks=AZURE_TESTS_MARK),
        pytest.param("mongo", marks=MONGO_TESTS_MARK),
        pytest.param("real_s3", marks=REAL_S3_TESTS_MARK),
    ],
)
def arctic_client(request, encoding_version) -> Arctic:
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=encoding_version)
    assert not ac.list_libraries()
    return ac


@pytest.fixture(
    scope="function",
    params=[
        "s3",
        "mem",
        pytest.param("azurite", marks=AZURE_TESTS_MARK),
        pytest.param("mongo", marks=MONGO_TESTS_MARK),
        pytest.param("real_s3", marks=REAL_S3_TESTS_MARK),
    ],
)
def arctic_client_no_lmdb(request, encoding_version) -> Arctic:
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=encoding_version)
    assert not ac.list_libraries()
    return ac


@pytest.fixture
def arctic_library(arctic_client, lib_name) -> Arctic:
    return arctic_client.create_library(lib_name)


@pytest.fixture(
    scope="function",
    params=[
        "lmdb",
        "mem",
        pytest.param("real_s3", marks=REAL_S3_TESTS_MARK),
    ],
)
def basic_arctic_client(request, encoding_version) -> Arctic:
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=encoding_version)
    assert not ac.list_libraries()
    return ac


@pytest.fixture
def basic_arctic_library(basic_arctic_client, lib_name) -> Arctic:
    return basic_arctic_client.create_library(lib_name)


# endregion
# region ============================ `NativeVersionStore` Fixture Factories ============================
@pytest.fixture
def version_store_factory(lib_name, lmdb_storage) -> Callable[..., NativeVersionStore]:
    return lmdb_storage.create_version_store_factory(lib_name)


@pytest.fixture
def s3_store_factory_mock_storage_exception(lib_name, s3_storage):
    lib = s3_storage.create_version_store_factory(lib_name)
    endpoint = s3_storage.factory.endpoint
    # `rate_limit` in the uri will trigger the code injected to moto to give http 503 slow down response
    # The following interger is for how many requests until 503 repsonse is sent
    # -1 means the 503 response is disabled
    # Setting persisted throughout the lifetime of moto server, so it needs to be reset
    requests.post(endpoint + "/rate_limit", b"0", verify=False).raise_for_status()
    yield lib
    requests.post(endpoint + "/rate_limit", b"-1", verify=False).raise_for_status()


@pytest.fixture
def s3_store_factory(lib_name, s3_storage):
    return s3_storage.create_version_store_factory(lib_name)


@pytest.fixture
def s3_no_ssl_store_factory(lib_name, s3_no_ssl_storage):
    return s3_no_ssl_storage.create_version_store_factory(lib_name)


@pytest.fixture
def mock_s3_store_with_error_simulation_factory(lib_name, mock_s3_storage_with_error_simulation):
    return mock_s3_storage_with_error_simulation.create_version_store_factory(lib_name)


@pytest.fixture
def real_s3_store_factory(lib_name, real_s3_storage) -> Callable[..., NativeVersionStore]:
    return real_s3_storage.create_version_store_factory(lib_name)


@pytest.fixture
def azure_store_factory(lib_name, azurite_storage):
    return azurite_storage.create_version_store_factory(lib_name)


@pytest.fixture
def mongo_store_factory(mongo_storage, lib_name):
    return mongo_storage.create_version_store_factory(lib_name)


@pytest.fixture
def in_memory_store_factory(mem_storage, lib_name) -> Callable[..., NativeVersionStore]:
    return mem_storage.create_version_store_factory(lib_name)


# endregion
# region ================================ `NativeVersionStore` Fixtures =================================
@pytest.fixture(scope="function")
def real_s3_version_store(real_s3_store_factory):
    return real_s3_store_factory()


@pytest.fixture(scope="function")
def real_s3_version_store_dynamic_schema(real_s3_store_factory):
    return real_s3_store_factory(dynamic_strings=True, dynamic_schema=True)


@pytest.fixture
def mock_s3_store_with_error_simulation(mock_s3_store_with_error_simulation_factory):
    return mock_s3_store_with_error_simulation_factory()


@pytest.fixture
def mock_s3_store_with_mock_storage_exception(s3_store_factory_mock_storage_exception):
    return s3_store_factory_mock_storage_exception()


@pytest.fixture
def s3_version_store_v1(s3_store_factory):
    return s3_store_factory(dynamic_strings=True)


@pytest.fixture
def s3_version_store_v2(s3_store_factory, lib_name):
    library_name = lib_name + "_v2"
    return s3_store_factory(dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name)


@pytest.fixture
def s3_version_store_dynamic_schema_v1(s3_store_factory):
    return s3_store_factory(dynamic_strings=True, dynamic_schema=True)


@pytest.fixture
def s3_version_store_dynamic_schema_v2(s3_store_factory, lib_name):
    library_name = lib_name + "_v2"
    return s3_store_factory(
        dynamic_strings=True, dynamic_schema=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def s3_version_store(s3_version_store_v1, s3_version_store_v2, encoding_version):
    if encoding_version == EncodingVersion.V1:
        return s3_version_store_v1
    elif encoding_version == EncodingVersion.V2:
        return s3_version_store_v2
    else:
        raise ValueError(f"Unexoected encoding version: {encoding_version}")


@pytest.fixture(scope="function")
def mongo_version_store(mongo_store_factory):
    return mongo_store_factory()


@pytest.fixture(
    scope="function",
    params=[
        "s3_store_factory",
        pytest.param("azure_store_factory", marks=AZURE_TESTS_MARK),
        pytest.param("real_s3_store_factory", marks=REAL_S3_TESTS_MARK),
    ],
)
def object_store_factory(request) -> Callable[..., NativeVersionStore]:
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def object_version_store(object_store_factory) -> NativeVersionStore:
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    return object_store_factory()


@pytest.fixture
def object_version_store_prune_previous(object_store_factory) -> NativeVersionStore:
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    return object_store_factory(prune_previous_version=True)


@pytest.fixture(
    scope="function", params=["s3_store_factory", pytest.param("azure_store_factory", marks=AZURE_TESTS_MARK)]
)
def local_object_store_factory(request):
    """
    Designed to test all local object stores and their simulations
    Doesn't support LMDB or persistent storages
    """
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def local_object_version_store(local_object_store_factory):
    """
    Designed to test all local object stores and their simulations
    Doesn't support LMDB or persistent storages
    """
    return local_object_store_factory()


@pytest.fixture
def local_object_version_store_prune_previous(local_object_store_factory):
    """
    Designed to test all local object stores and their simulations
    Doesn't support LMDB or persistent storages
    """
    return local_object_store_factory(prune_previous_version=True)


@pytest.fixture(params=["version_store_factory", pytest.param("real_s3_store_factory", marks=REAL_S3_TESTS_MARK)])
def version_store_and_real_s3_basic_store_factory(request):
    """
    Just the version_store and real_s3 specifically for the test test_interleaved_store_read
    where the in_memory_store_factory is not designed to have this functionality.
    """
    return request.getfixturevalue(request.param)


@pytest.fixture(
    params=[
        "version_store_factory",
        "in_memory_store_factory",
        pytest.param("real_s3_store_factory", marks=REAL_S3_TESTS_MARK),
    ]
)
def basic_store_factory(request) -> Callable[..., NativeVersionStore]:
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def basic_store(basic_store_factory) -> NativeVersionStore:
    """
    Designed to test the bare minimum of stores
     - LMDB for local storage
     - mem for in-memory storage
     - AWS S3 for persistent storage, if enabled
    """
    return basic_store_factory()


@pytest.fixture
def azure_version_store(azure_store_factory):
    return azure_store_factory()


@pytest.fixture
def azure_version_store_dynamic_schema(azure_store_factory):
    return azure_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_string_coercion(version_store_factory) ->NativeVersionStore:
    return version_store_factory()


@pytest.fixture
def lmdb_version_store_v1(version_store_factory) -> NativeVersionStore:
    return version_store_factory(dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name)


@pytest.fixture(scope="function", params=("lmdb_version_store_v1", "lmdb_version_store_v2"))
def lmdb_version_store(request):
    yield request.getfixturevalue(request.param)


@pytest.fixture
def lmdb_version_store_prune_previous(version_store_factory) -> NativeVersionStore:
    return version_store_factory(dynamic_strings=True, prune_previous_version=True, use_tombstones=True)


@pytest.fixture
def lmdb_version_store_big_map(version_store_factory) -> NativeVersionStore:
    return version_store_factory(lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_very_big_map(version_store_factory) -> NativeVersionStore:
    return version_store_factory(lmdb_config={"map_size": 2**35})

@pytest.fixture
def lmdb_version_store_column_buckets(version_store_factory) -> NativeVersionStore:
    return version_store_factory(dynamic_schema=True, column_group_size=3, segment_row_size=2, bucketize_dynamic=True)


@pytest.fixture
def lmdb_version_store_dynamic_schema_v1(version_store_factory, lib_name) -> NativeVersionStore:
    return version_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_dynamic_schema_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_schema=True, dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def lmdb_version_store_dynamic_schema(
    lmdb_version_store_dynamic_schema_v1, lmdb_version_store_dynamic_schema_v2, encoding_version
):
    if encoding_version == EncodingVersion.V1:
        return lmdb_version_store_dynamic_schema_v1
    elif encoding_version == EncodingVersion.V2:
        return lmdb_version_store_dynamic_schema_v2
    else:
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


@pytest.fixture
def lmdb_version_store_empty_types_v1(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v1"
    return version_store_factory(dynamic_strings=True, empty_types=True, name=library_name)


@pytest.fixture
def lmdb_version_store_empty_types_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True, empty_types=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def lmdb_version_store_empty_types_dynamic_schema_v1(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v1"
    return version_store_factory(dynamic_strings=True, empty_types=True, dynamic_schema=True, name=library_name)


@pytest.fixture
def lmdb_version_store_empty_types_dynamic_schema_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True,
        empty_types=True,
        dynamic_schema=True,
        encoding_version=int(EncodingVersion.V2),
        name=library_name,
    )


@pytest.fixture
def lmdb_version_store_delayed_deletes_v1(version_store_factory) -> NativeVersionStore:
    return version_store_factory(
        delayed_deletes=True, dynamic_strings=True, empty_types=True, prune_previous_version=True
    )


@pytest.fixture
def lmdb_version_store_delayed_deletes_v2(version_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True,
        delayed_deletes=True,
        empty_types=True,
        encoding_version=int(EncodingVersion.V2),
        name=library_name,
    )


@pytest.fixture
def lmdb_version_store_tombstones_no_symbol_list(version_store_factory) -> NativeVersionStore:
    return version_store_factory(use_tombstones=True, dynamic_schema=True, symbol_list=False, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_allows_pickling(version_store_factory, lib_name) -> NativeVersionStore:
    return version_store_factory(use_norm_failure_handler_known_types=True, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_no_symbol_list(version_store_factory) -> NativeVersionStore:
    return version_store_factory(col_per_group=None, row_per_segment=None, symbol_list=False)


@pytest.fixture
def lmdb_version_store_tombstone_and_pruning(version_store_factory) -> NativeVersionStore:
    return version_store_factory(use_tombstones=True, prune_previous_version=True)


@pytest.fixture
def lmdb_version_store_tombstone(version_store_factory) -> NativeVersionStore:
    return version_store_factory(use_tombstones=True)


@pytest.fixture
def lmdb_version_store_tombstone_and_sync_passive(version_store_factory) -> NativeVersionStore:
    return version_store_factory(use_tombstones=True, sync_passive=True)


@pytest.fixture
def lmdb_version_store_ignore_order(version_store_factory) -> NativeVersionStore:
    return version_store_factory(ignore_sort_order=True)


@pytest.fixture
def lmdb_version_store_small_segment(version_store_factory) -> NativeVersionStore:
    return version_store_factory(column_group_size=1000, segment_row_size=1000, lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_tiny_segment(version_store_factory) -> NativeVersionStore:
    return version_store_factory(column_group_size=2, segment_row_size=2, lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_tiny_segment_dynamic(version_store_factory) -> NativeVersionStore:
    return version_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=True)


@pytest.fixture
def basic_store_prune_previous(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(dynamic_strings=True, prune_previous_version=True, use_tombstones=True)


@pytest.fixture
def basic_store_large_data(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(lmdb_config={"map_size": 2**30})


@pytest.fixture
def basic_store_column_buckets(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(dynamic_schema=True, column_group_size=3, segment_row_size=2, bucketize_dynamic=True)


@pytest.fixture
def basic_store_dynamic_schema_v1(basic_store_factory, lib_name) -> NativeVersionStore:
    return basic_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
def basic_store_dynamic_schema_v2(basic_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return basic_store_factory(
        dynamic_schema=True, dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def basic_store_dynamic_schema(basic_store_dynamic_schema_v1, basic_store_dynamic_schema_v2, encoding_version):
    if encoding_version == EncodingVersion.V1:
        return basic_store_dynamic_schema_v1
    elif encoding_version == EncodingVersion.V2:
        return basic_store_dynamic_schema_v2
    else:
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


@pytest.fixture
def basic_store_delayed_deletes(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(delayed_deletes=True)


@pytest.fixture
def basic_store_delayed_deletes_v1(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(delayed_deletes=True, dynamic_strings=True, prune_previous_version=True)


@pytest.fixture
def basic_store_delayed_deletes_v2(basic_store_factory, lib_name) -> NativeVersionStore:
    library_name = lib_name + "_v2"
    return basic_store_factory(
        dynamic_strings=True, delayed_deletes=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def basic_store_tombstones_no_symbol_list(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(use_tombstones=True, dynamic_schema=True, symbol_list=False, dynamic_strings=True)


@pytest.fixture
def basic_store_allows_pickling(basic_store_factory, lib_name) -> NativeVersionStore:
    return basic_store_factory(use_norm_failure_handler_known_types=True, dynamic_strings=True)


@pytest.fixture
def basic_store_no_symbol_list(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(symbol_list=False)


@pytest.fixture
def basic_store_tombstone_and_pruning(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(use_tombstones=True, prune_previous_version=True)


@pytest.fixture
def basic_store_tombstone(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(use_tombstones=True)


@pytest.fixture
def basic_store_tombstone_and_sync_passive(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(use_tombstones=True, sync_passive=True)


@pytest.fixture
def basic_store_ignore_order(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(ignore_sort_order=True)


@pytest.fixture
def basic_store_small_segment(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(column_group_size=1000, segment_row_size=1000, lmdb_config={"map_size": 2**30})


@pytest.fixture
def basic_store_tiny_segment(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(column_group_size=2, segment_row_size=2, lmdb_config={"map_size": 2**30})

@pytest.fixture
def basic_store_tiny_segment_dynamic(basic_store_factory) -> NativeVersionStore:
    return basic_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=True)


# endregion
@pytest.fixture
def one_col_df():
    return partial(create_df, columns=1)


@pytest.fixture
def two_col_df():
    return partial(create_df, columns=2)


@pytest.fixture
def three_col_df():
    return partial(create_df, columns=3)


def get_val(col):
    d_type = col % 3
    if d_type == 0:
        return random.random() * 10
    elif d_type == 1:
        return random.random()
    else:
        return str(random.random() * 10)


@pytest.fixture
def get_wide_df():
    def get_df(ts, width, max_col_width):
        cols = random.sample(range(max_col_width), width)
        return pd.DataFrame(index=[pd.Timestamp(ts)], data={str(col): get_val(col) for col in cols})

    return get_df


@pytest.fixture(
    scope="function",
    params=(
        "lmdb_version_store_empty_types_v1",
        "lmdb_version_store_empty_types_v2",
        "lmdb_version_store_empty_types_dynamic_schema_v1",
        "lmdb_version_store_empty_types_dynamic_schema_v2",
    ),
)
def lmdb_version_store_static_and_dynamic(request):
    """
    Designed to test all combinations between schema and encoding version for LMDB
    """
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    scope="function",
    params=(
        "lmdb_version_store_v1",
        "lmdb_version_store_v2",
        "s3_version_store_v1",
        "s3_version_store_v2",
        "in_memory_version_store",
        pytest.param("azure_version_store", marks=AZURE_TESTS_MARK),
        pytest.param("mongo_version_store", marks=MONGO_TESTS_MARK),
        pytest.param("real_s3_version_store", marks=REAL_S3_TESTS_MARK),
    ),
)
def object_and_mem_and_lmdb_version_store(request):
    """
    Designed to test all supported stores
    """
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    scope="function",
    params=(
        "lmdb_version_store_dynamic_schema_v1",
        "lmdb_version_store_dynamic_schema_v2",
        "s3_version_store_dynamic_schema_v1",
        "s3_version_store_dynamic_schema_v2",
        "in_memory_version_store_dynamic_schema",
        pytest.param("azure_version_store_dynamic_schema", marks=AZURE_TESTS_MARK),
        pytest.param("real_s3_version_store_dynamic_schema", marks=REAL_S3_TESTS_MARK),
    ),
)
def object_and_mem_and_lmdb_version_store_dynamic_schema(request):
    """
    Designed to test all supported stores
    """
    yield request.getfixturevalue(request.param)


@pytest.fixture
def in_memory_version_store(in_memory_store_factory):
    return in_memory_store_factory()


@pytest.fixture
def in_memory_version_store_dynamic_schema(in_memory_store_factory):
    return in_memory_store_factory(dynamic_schema=True)


@pytest.fixture
def in_memory_version_store_tiny_segment(in_memory_store_factory):
    return in_memory_store_factory(column_group_size=2, segment_row_size=2)


@pytest.fixture(params=["lmdb_version_store_tiny_segment", "in_memory_version_store_tiny_segment"])
def lmdb_or_in_memory_version_store_tiny_segment(request):
    return request.getfixturevalue(request.param)
