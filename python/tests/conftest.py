"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import enum
import hypothesis
import os
import pytest
import numpy as np
import pandas as pd
import random
from datetime import datetime

from arcticdb.storage_fixtures.api import StorageFixture
from arcticdb.storage_fixtures.azure import AzuriteStorageFixtureFactory
from arcticdb.storage_fixtures.lmdb import LmdbStorageFixture
from arcticdb.storage_fixtures.s3 import MotoS3StorageFixtureFactory, real_s3_from_environment_variables
from arcticdb.storage_fixtures.mongo import auto_detect_server
from arcticdb.storage_fixtures.in_memory import InMemoryStorageFixture
from arcticdb.version_store._normalization import MsgPackNormalizer
from arcticdb.util.test import configure_test_logger
from tests.util.mark import (
    AZURE_TESTS_MARK,
    MONGO_TESTS_MARK,
    REAL_S3_TESTS_MARK,
)

# region =================================== Misc. Constants & Setup ====================================
hypothesis.settings.register_profile("ci_linux", max_examples=100)
hypothesis.settings.register_profile("ci_windows", max_examples=100)
hypothesis.settings.register_profile("dev", max_examples=100)

hypothesis.settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))

configure_test_logger()

# Use a smaller memory mapped limit for all tests
MsgPackNormalizer.MMAP_DEFAULT_SIZE = 20 * (1 << 20)


@pytest.fixture()
def sym():
    return "test" + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture()
def lib_name():
    return f"local.test.{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}"


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
def lmdb_storage(tmp_path):
    with LmdbStorageFixture(tmp_path) as f:
        yield f


@pytest.fixture(scope="session")
def s3_storage_factory():
    with MotoS3StorageFixtureFactory() as f:
        yield f


@pytest.fixture
def s3_storage(s3_storage_factory):
    with s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def real_s3_storage_factory():
    return real_s3_from_environment_variables()


@pytest.fixture(scope="session")
def real_s3_storage_without_clean_up(real_s3_storage_factory):
    return real_s3_storage_factory.create_fixture()


@pytest.fixture
def real_s3_storage(real_s3_storage_factory):
    with real_s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def azurite_storage_factory():
    with AzuriteStorageFixtureFactory() as f:
        yield f


@pytest.fixture
def azurite_storage(azurite_storage_factory: AzuriteStorageFixtureFactory):
    with azurite_storage_factory.create_fixture() as f:
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
def mem_storage():
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
def arctic_client(request, encoding_version):
    storage_fixture: StorageFixture = request.getfixturevalue(request.param + "_storage")
    ac = storage_fixture.create_arctic(encoding_version=encoding_version)
    assert not ac.list_libraries()
    return ac


@pytest.fixture
def arctic_library(arctic_client, lib_name):
    return arctic_client.create_library(lib_name)


# endregion
# region ============================ `NativeVersionStore` Fixture Factories ============================
@pytest.fixture
def version_store_factory(lib_name, lmdb_storage):
    return lmdb_storage.create_version_store_factory(lib_name)


@pytest.fixture
def s3_store_factory(lib_name, s3_storage):
    return s3_storage.create_version_store_factory(lib_name)


@pytest.fixture
def real_s3_store_factory(lib_name, real_s3_storage):
    return real_s3_storage.create_version_store_factory(lib_name)


@pytest.fixture
def azure_store_factory(lib_name, azurite_storage):
    return azurite_storage.create_version_store_factory(lib_name)


@pytest.fixture
def mongo_store_factory(mongo_storage, lib_name):
    return mongo_storage.create_version_store_factory(lib_name)


@pytest.fixture
def in_memory_store_factory(mem_storage, lib_name):
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
def object_store_factory(request):
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def object_version_store(object_store_factory):
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    return object_store_factory()


@pytest.fixture
def object_version_store_prune_previous(object_store_factory):
    """
    Designed to test all object stores and their simulations
    Doesn't support LMDB
    """
    return object_store_factory(prune_previous_version=True)


@pytest.fixture(
    scope="function",
    params=[
        "s3_store_factory",
        pytest.param("azure_store_factory", marks=AZURE_TESTS_MARK),
    ],
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


@pytest.fixture(
    params=[
        "version_store_factory",
        pytest.param("real_s3_store_factory", marks=REAL_S3_TESTS_MARK),
    ],
)
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
    ],
)
def basic_store_factory(request):
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def basic_store(basic_store_factory):
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
def lmdb_version_store_string_coercion(version_store_factory):
    return version_store_factory()


@pytest.fixture
def lmdb_version_store_v1(version_store_factory):
    return version_store_factory(dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_v2(version_store_factory, lib_name):
    library_name = lib_name + "_v2"
    return version_store_factory(dynamic_strings=True, encoding_version=int(EncodingVersion.V2), name=library_name)


@pytest.fixture
def lmdb_version_store(lmdb_version_store_v1, lmdb_version_store_v2, encoding_version):
    if encoding_version == EncodingVersion.V1:
        return lmdb_version_store_v1
    elif encoding_version == EncodingVersion.V2:
        return lmdb_version_store_v2
    else:
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


@pytest.fixture
def lmdb_version_store_prune_previous(version_store_factory):
    return version_store_factory(dynamic_strings=True, prune_previous_version=True, use_tombstones=True)


@pytest.fixture
def lmdb_version_store_big_map(version_store_factory):
    return version_store_factory(lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_column_buckets(version_store_factory):
    return version_store_factory(dynamic_schema=True, column_group_size=3, segment_row_size=2, bucketize_dynamic=True)


@pytest.fixture
def lmdb_version_store_dynamic_schema_v1(version_store_factory, lib_name):
    return version_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_dynamic_schema_v2(version_store_factory, lib_name):
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
def lmdb_version_store_delayed_deletes_v1(version_store_factory):
    return version_store_factory(delayed_deletes=True, dynamic_strings=True, prune_previous_version=True)


@pytest.fixture
def lmdb_version_store_delayed_deletes_v2(version_store_factory, lib_name):
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True, delayed_deletes=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def lmdb_version_store_tombstones_no_symbol_list(version_store_factory):
    return version_store_factory(use_tombstones=True, dynamic_schema=True, symbol_list=False, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_allows_pickling(version_store_factory, lib_name):
    return version_store_factory(use_norm_failure_handler_known_types=True, dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_no_symbol_list(version_store_factory):
    return version_store_factory(col_per_group=None, row_per_segment=None, symbol_list=False)


@pytest.fixture
def lmdb_version_store_tombstone_and_pruning(version_store_factory):
    return version_store_factory(use_tombstones=True, prune_previous_version=True)


@pytest.fixture
def lmdb_version_store_tombstone(version_store_factory):
    return version_store_factory(use_tombstones=True)


@pytest.fixture
def lmdb_version_store_tombstone_and_sync_passive(version_store_factory):
    return version_store_factory(use_tombstones=True, sync_passive=True)


@pytest.fixture
def lmdb_version_store_ignore_order(version_store_factory):
    return version_store_factory(ignore_sort_order=True)


@pytest.fixture
def lmdb_version_store_small_segment(version_store_factory):
    return version_store_factory(column_group_size=1000, segment_row_size=1000, lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_tiny_segment(version_store_factory):
    return version_store_factory(column_group_size=2, segment_row_size=2, lmdb_config={"map_size": 2**30})


@pytest.fixture
def lmdb_version_store_tiny_segment_dynamic(version_store_factory):
    return version_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=True)


@pytest.fixture
def basic_store_prune_previous(basic_store_factory):
    return basic_store_factory(dynamic_strings=True, prune_previous_version=True, use_tombstones=True)


@pytest.fixture
def basic_store_large_data(basic_store_factory):
    return basic_store_factory(lmdb_config={"map_size": 2**30})


@pytest.fixture
def basic_store_column_buckets(basic_store_factory):
    return basic_store_factory(dynamic_schema=True, column_group_size=3, segment_row_size=2, bucketize_dynamic=True)


@pytest.fixture
def basic_store_dynamic_schema_v1(basic_store_factory, lib_name):
    return basic_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
def basic_store_dynamic_schema_v2(basic_store_factory, lib_name):
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
def basic_store_delayed_deletes(basic_store_factory):
    return basic_store_factory(delayed_deletes=True)


@pytest.fixture
def basic_store_delayed_deletes_v1(basic_store_factory):
    return basic_store_factory(delayed_deletes=True, dynamic_strings=True, prune_previous_version=True)


@pytest.fixture
def basic_store_delayed_deletes_v2(basic_store_factory, lib_name):
    library_name = lib_name + "_v2"
    return basic_store_factory(
        dynamic_strings=True, delayed_deletes=True, encoding_version=int(EncodingVersion.V2), name=library_name
    )


@pytest.fixture
def basic_store_tombstones_no_symbol_list(basic_store_factory):
    return basic_store_factory(use_tombstones=True, dynamic_schema=True, symbol_list=False, dynamic_strings=True)


@pytest.fixture
def basic_store_allows_pickling(basic_store_factory, lib_name):
    return basic_store_factory(use_norm_failure_handler_known_types=True, dynamic_strings=True)


@pytest.fixture
def basic_store_no_symbol_list(basic_store_factory):
    return basic_store_factory(symbol_list=False)


@pytest.fixture
def basic_store_tombstone_and_pruning(basic_store_factory):
    return basic_store_factory(use_tombstones=True, prune_previous_version=True)


@pytest.fixture
def basic_store_tombstone(basic_store_factory):
    return basic_store_factory(use_tombstones=True)


@pytest.fixture
def basic_store_tombstone_and_sync_passive(basic_store_factory):
    return basic_store_factory(use_tombstones=True, sync_passive=True)


@pytest.fixture
def basic_store_ignore_order(basic_store_factory):
    return basic_store_factory(ignore_sort_order=True)


@pytest.fixture
def basic_store_small_segment(basic_store_factory):
    return basic_store_factory(column_group_size=1000, segment_row_size=1000, lmdb_config={"map_size": 2**30})


@pytest.fixture
def basic_store_tiny_segment(basic_store_factory):
    return basic_store_factory(column_group_size=2, segment_row_size=2, lmdb_config={"map_size": 2**30})


@pytest.fixture
def basic_store_tiny_segment_dynamic(basic_store_factory):
    return basic_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=True)


# endregion


@pytest.fixture
def one_col_df():
    def create(start=0) -> pd.DataFrame:
        return pd.DataFrame({"x": np.arange(start, start + 10, dtype=np.int64)})

    return create


@pytest.fixture
def two_col_df():
    def create(start=0) -> pd.DataFrame:
        return pd.DataFrame(
            {"x": np.arange(start, start + 10, dtype=np.int64), "y": np.arange(start + 10, start + 20, dtype=np.int64)}
        )

    return create


@pytest.fixture
def three_col_df():
    def create(start=0) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "x": np.arange(start, start + 10, dtype=np.int64),
                "y": np.arange(start + 10, start + 20, dtype=np.int64),
                "z": np.arange(start + 20, start + 30, dtype=np.int64),
            },
            index=np.arange(start, start + 10, dtype=np.int64),
        )

    return create


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
