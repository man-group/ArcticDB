"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import functools
import enum
import os
import pytest
import requests
import numpy as np
import pandas as pd
import random
from datetime import datetime
from typing import Optional

from arcticdb.adapters.storage_fixture_api import _version_store_factory_impl
from arcticdb.adapters.azure_storage_fixture import AzuriteStorageFixtureFactory
from arcticdb.adapters.lmdb_storage_adapter import LmdbStorageFixture
from arcticdb.adapters.s3_storage_fixture import MotoS3StorageFixtureFactory
from arcticdb.arctic import Arctic
from arcticdb.version_store.helper import create_test_mongo_cfg
from arcticdb.version_store.library import Library
from arcticdb.version_store._normalization import MsgPackNormalizer
from arcticdb.util.test import configure_test_logger
from arcticdb.options import LibraryOptions
from arcticdb_ext.tools import AZURE_SUPPORT


configure_test_logger()

# Use a smaller memory mapped limit for all tests
MsgPackNormalizer.MMAP_DEFAULT_SIZE = 20 * (1 << 20)


@pytest.fixture
def _lmdb_storage(tmpdir):
    with LmdbStorageFixture(tmpdir) as f:
        yield f


@pytest.fixture(scope="session")
def s3_storage_factory():
    with MotoS3StorageFixtureFactory() as f:
        yield f


@pytest.fixture
def s3_bucket(s3_storage_factory):
    with s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def azurite_storage_factory():
    if AZURE_SUPPORT:
        with AzuriteStorageFixtureFactory() as f:
            yield f
    else:
        yield


@pytest.fixture
def azurite_container(azurite_storage_factory: AzuriteStorageFixtureFactory):
    with azurite_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="function", params=["S3", "LMDB", "Azure"] if AZURE_SUPPORT else ["S3", "LMDB"])
def arctic_client(request, encoding_version):
    fixture_name = {"S3": "s3_bucket", "LMDB": "_lmdb_storage", "Azure": "azurite_container"}[request.param]
    storage_fixture = request.getfixturevalue(fixture_name)
    ac = Arctic(storage_fixture.arctic_uri, encoding_version)
    assert not ac.list_libraries()
    return ac


@pytest.fixture(params=(["s3_bucket", "azurite_container"] if AZURE_SUPPORT else ["s3_bucket"]))
def object_storage_fixture(request):
    """``StorageFixture`` instances backed by an object store, i.e. not LMDB."""
    return request.getfixturevalue(request.param)


@pytest.fixture(scope="function")
def arctic_library(request, library_factory):
    return library_factory()


@pytest.fixture()
def sym():
    return "test" + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture()
def lib_name():
    return f"local.test_{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}"


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


@pytest.fixture
def version_store_factory(lib_name, _lmdb_storage):
    """Factory to create any number of distinct LMDB libs with the given WriteOptions or VersionStoreConfig.

    Accepts legacy options col_per_group and row_per_segment.
    `name` can be a magical value "_unique_" which will create libs with unique names.
    The values in `lmdb_config` populates the `LmdbConfig` Protobuf that creates the `Library` in C++. On Windows, it
    can be used to override the `map_size`.
    """
    return _lmdb_storage.create_version_store_factory(lib_name)


@pytest.fixture(scope="function")
def library_factory(arctic_client, lib_name):
    """Useful if a test wants to create libraries without manually supplying names for them."""

    def create_library(library_options: Optional[LibraryOptions] = None, name: str = lib_name) -> Library:
        if name == "_unique_":
            name = name + str(len(arctic_client.list_libraries()))
        arctic_client.create_library(name, library_options)
        out = arctic_client[name]
        return out

    return create_library


@pytest.fixture
def s3_store_factory(lib_name, s3_bucket):
    """Factory to create any number of S3 libs with the given WriteOptions or VersionStoreConfig.

    `name` can be a magical value "_unique_" which will create libs with unique names.
    This factory will clean up any libraries requested
    """
    return s3_bucket.create_version_store_factory(lib_name)


@pytest.fixture(scope="function")
def real_s3_credentials():
    endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
    region = os.getenv("ARCTICDB_REAL_S3_REGION")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    clear = True if str(os.getenv("ARCTICDB_REAL_S3_CLEAR")).lower() in ["true", "1"] else False

    yield endpoint, bucket, region, access_key, secret_key, clear


@pytest.fixture
def real_s3_store_factory(lib_name, real_s3_credentials):
    endpoint, bucket, region, aws_access_key, aws_secret_key, clear = real_s3_credentials

    # Not exposing the config factory to discourage people from creating libs that won't get cleaned up
    def make_cfg(name):
        # with_prefix=False to allow reuse_name to work correctly
        return create_test_s3_cfg(
            name, aws_access_key, aws_secret_key, bucket, endpoint, with_prefix=False, is_https=True, region=region
        )

    used = {}
    try:
        yield functools.partial(_version_store_factory_impl, used, make_cfg, lib_name)
    finally:
        if clear:
            for lib in used.values():
                lib.version_store.clear()


@pytest.fixture(scope="function")
def real_s3_version_store(real_s3_store_factory):
    return real_s3_store_factory()


@pytest.fixture
def azure_store_factory(lib_name, azurite_container):
    """Factory to create any number of Azure libs with the given WriteOptions or VersionStoreConfig.

    `name` can be a magical value "_unique_" which will create libs with unique names.
    This factory will clean up any libraries requested
    """
    return azurite_container.create_version_store_factory()


@pytest.fixture
def mongo_store_factory(request, lib_name):
    """Similar capability to `s3_store_factory`, but uses a mongo store."""
    # Use MongoDB if it's running (useful in CI), otherwise spin one up with pytest-server-fixtures.
    mongo_host = os.getenv("CI_MONGO_HOST", "localhost")
    mongo_port = os.getenv("CI_MONGO_PORT", 27017)
    mongo_path = f"{mongo_host}:{mongo_port}"
    try:
        res = requests.get(f"http://{mongo_path}")
        have_running_mongo = res.status_code == 200 and "mongodb" in res.text.lower()
    except requests.exceptions.ConnectionError:
        have_running_mongo = False

    if have_running_mongo:
        uri = f"mongodb://{mongo_path}"
    else:
        mongo_server_sess = request.getfixturevalue("mongo_server_sess")
        uri = f"mongodb://{mongo_server_sess.hostname}:{mongo_server_sess.port}"

    cfg_maker = functools.partial(create_test_mongo_cfg, uri=uri)
    used = {}
    try:
        yield functools.partial(_version_store_factory_impl, used, cfg_maker, lib_name)
    finally:
        for lib in used.values():
            lib.version_store.clear()


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
    scope="function", params=["s3_store_factory", "azure_store_factory"] if AZURE_SUPPORT else ["s3_store_factory"]
)
def object_store_factory(request):
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def object_version_store(object_store_factory):
    return object_store_factory()


@pytest.fixture
def object_version_store_prune_previous(object_store_factory):
    return object_store_factory(prune_previous_version=True)


@pytest.fixture
def azure_version_store(azure_store_factory):
    return azure_store_factory()


@pytest.fixture
def lmdb_version_store_string_coercion(version_store_factory):
    return version_store_factory()


@pytest.fixture
def lmdb_version_store_v1(version_store_factory):
    return version_store_factory(dynamic_strings=True)


@pytest.fixture
def lmdb_version_store_v2(version_store_factory, lib_name):
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True, encoding_version=int(EncodingVersion.V2), override_name=library_name
    )


@pytest.fixture
def lmdb_version_store(lmdb_version_store_v1, lmdb_version_store_v2, encoding_version):
    if encoding_version == EncodingVersion.V1:
        return lmdb_version_store_v1
    elif encoding_version == EncodingVersion.V2:
        return lmdb_version_store_v2
    else:
        raise ValueError(f"Unexoected encoding version: {encoding_version}")


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
        dynamic_schema=True, dynamic_strings=True, encoding_version=int(EncodingVersion.V2), override_name=library_name
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
        raise ValueError(f"Unexoected encoding version: {encoding_version}")


@pytest.fixture
def lmdb_version_store_delayed_deletes_v1(version_store_factory):
    return version_store_factory(delayed_deletes=True, dynamic_strings=True, prune_previous_version=True)


@pytest.fixture
def lmdb_version_store_delayed_deletes_v2(version_store_factory, lib_name):
    library_name = lib_name + "_v2"
    return version_store_factory(
        dynamic_strings=True, delayed_deletes=True, encoding_version=int(EncodingVersion.V2), override_name=library_name
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
        [
            "lmdb_version_store_v1",
            "lmdb_version_store_v2",
            "s3_version_store_v1",
            "s3_version_store_v2",
            "azure_version_store",
        ]
        if AZURE_SUPPORT
        else ["lmdb_version_store_v1", "lmdb_version_store_v2", "s3_version_store_v1", "s3_version_store_v2"]
    ),
)
def object_and_lmdb_version_store(request):
    yield request.getfixturevalue(request.param)
