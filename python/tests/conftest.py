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
import re
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
def sym(request):
    return request.node.name + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture()
def sym():
    return "test" + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture()
def lib_name():
    return f"local.test.{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}"


@pytest.fixture
def arcticdb_test_s3_config(moto_s3_endpoint_and_credentials):
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials

    def create(lib_name):
        return create_test_s3_cfg(lib_name, aws_access_key, aws_secret_key, bucket, endpoint)

    return create


@pytest.fixture
def arcticdb_test_azure_config(azurite_azure_test_connection_setting, azurite_azure_uri):
    def create(lib_name):
        (endpoint, container, credential_name, credential_key, ca_cert_path) = azurite_azure_test_connection_setting
        return create_test_azure_cfg(
            lib_name=lib_name,
            credential_name=credential_name,
            credential_key=credential_key,
            container_name=container,
            endpoint=azurite_azure_uri,
            ca_cert_path=ca_cert_path,
        )

    return create


def _version_store_factory_impl(
        used, make_cfg, default_name, *, name: str = None, reuse_name=False, **kwargs
) -> NativeVersionStore:
    """Common logic behind all the factory fixtures"""
    name = name or default_name
    if name == "_unique_":
        name = name + str(len(used))

    assert (name not in used) or reuse_name, f"{name} is already in use"
    cfg = make_cfg(name)
    lib = cfg.env_by_id[Defaults.ENV].lib_by_path[name]
    # Use symbol list by default (can still be overridden by kwargs)
    lib.version.symbol_list = True
    apply_lib_cfg(lib, kwargs)
    out = ArcticMemoryConfig(cfg, Defaults.ENV)[name]
    used[name] = out
    return out
>>>>>>> 53b03983 (Batch read versions with snapshots)


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
<<<<<<< HEAD
def s3_storage(s3_storage_factory):
    with s3_storage_factory.create_fixture() as f:
        yield f
=======
def version_store_factory(lib_name, tmpdir):
    """Factory to create any number of distinct LMDB libs with the given WriteOptions or VersionStoreConfig.

    Accepts legacy options col_per_group and row_per_segment.
    `name` can be a magical value "_unique_" which will create libs with unique names.
    The values in `lmdb_config` populates the `LmdbConfig` Protobuf that creates the `Library` in C++. On Windows, it
    can be used to override the `map_size`.
    """
    used: Dict[str, NativeVersionStore] = {}
    def create_version_store(
            col_per_group: Optional[int] = None,
            row_per_segment: Optional[int] = None,
            lmdb_config: Dict[str, Any] = None,
            override_name: str = None,
            **kwargs,
    ) -> NativeVersionStore:
        if col_per_group is not None and "column_group_size" not in kwargs:
            kwargs["column_group_size"] = col_per_group
        if row_per_segment is not None and "segment_row_size" not in kwargs:
            kwargs["segment_row_size"] = row_per_segment
        if lmdb_config is None:
            # 128 MiB - needs to be reasonably small else Windows build runs out of disk
            lmdb_config = {"map_size": 128 * (1 << 20)}

        if override_name is not None:
            library_name = override_name
        else:
            library_name = lib_name

        cfg_factory = functools.partial(create_test_lmdb_cfg, db_dir=str(tmpdir), lmdb_config=lmdb_config)
        return _version_store_factory_impl(used, cfg_factory, library_name, **kwargs)

    try:
        yield create_version_store
    except RuntimeError as e:
        if "mdb_" in str(e):  # Dump keys when we get uncaught exception from LMDB:
            for store in used.values():
                print(store)
                lt = store.library_tool()
                for kt in lt.key_types():
                    for key in lt.find_keys(kt):
                        print(key)
        raise
    finally:
        for result in used.values():
            #  pytest holds a member variable `cached_result` equal to `result` above which keeps the storage alive and
            #  locked. See https://github.com/pytest-dev/pytest/issues/5642 . So we need to decref the C++ objects keeping
            #  the LMDB env open before they will release the lock and allow Windows to delete the LMDB files.
            result.version_store = None
            result._library = None

        shutil.rmtree(tmpdir, ignore_errors=True)
>>>>>>> 53b03983 (Batch read versions with snapshots)


@pytest.fixture(scope="session")
def real_s3_storage_factory():
    return real_s3_from_environment_variables()


<<<<<<< HEAD
@pytest.fixture(scope="session")
def real_s3_storage_without_clean_up(real_s3_storage_factory):
    return real_s3_storage_factory.create_fixture()
=======
@pytest.fixture(scope="function")
def library_factory(arctic_client, lib_name):
    used: Dict[str, Library] = {}

    def create_library(
            library_options: Optional[LibraryOptions] = None,
    ) -> Library:
        return _arctic_library_factory_impl(used, lib_name, arctic_client, library_options)

    return create_library
>>>>>>> 53b03983 (Batch read versions with snapshots)


@pytest.fixture
def real_s3_storage(real_s3_storage_factory):
    with real_s3_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def azurite_storage_factory():
    with AzuriteStorageFixtureFactory() as f:
        yield f


@pytest.fixture
<<<<<<< HEAD
def azurite_storage(azurite_storage_factory: AzuriteStorageFixtureFactory):
    with azurite_storage_factory.create_fixture() as f:
        yield f
=======
def real_s3_store_factory(lib_name, **kwargs):
    endpoint, bucket, region, aws_access_key, aws_secret_key, path_prefix, clear = real_s3_credentials(
        shared_path=False
    )

    # Not exposing the config factory to discourage people from creating libs that won't get cleaned up
    def make_cfg(name):
        # with_prefix=False to allow reuse_name to work correctly
        return create_test_s3_cfg(
            name,
            aws_access_key,
            aws_secret_key,
            bucket,
            endpoint,
            with_prefix=path_prefix,
            is_https=True,
            region=region,
        )

    used = {}

    def create_version_store(
            col_per_group: Optional[int] = None,
            row_per_segment: Optional[int] = None,
            # lmdb_config: Dict[str, Any] = {},
            override_name: str = None,
            **kwargs,
    ) -> NativeVersionStore:
        if col_per_group is not None and "column_group_size" not in kwargs:
            kwargs["column_group_size"] = col_per_group
        if row_per_segment is not None and "segment_row_size" not in kwargs:
            kwargs["segment_row_size"] = row_per_segment

        if "lmdb_config" in kwargs:
            del kwargs["lmdb_config"]

        if override_name is not None:
            library_name = override_name
        else:
            library_name = lib_name

        return _version_store_factory_impl(used, make_cfg, library_name, **kwargs)

    try:
        yield create_version_store
    finally:
        if clear:
            for lib in used.values():
                lib.version_store.clear()
>>>>>>> 53b03983 (Batch read versions with snapshots)


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
<<<<<<< HEAD
=======
def azure_store_factory(lib_name, arcticdb_test_azure_config, azure_client_and_create_container):
    """Factory to create any number of Azure libs with the given WriteOptions or VersionStoreConfig.

    `name` can be a magical value "_unique_" which will create libs with unique names.
    This factory will clean up any libraries requested
    """
    used = {}
    try:
        yield functools.partial(_version_store_factory_impl, used, arcticdb_test_azure_config, lib_name)
    finally:
        for lib in used.values():
            lib.version_store.clear()


@pytest.fixture
def mongo_test_uri(request):
    """Similar capability to `s3_store_factory`, but uses a mongo store."""
    # Use MongoDB if it's running (useful in CI), otherwise spin one up with pytest-server-fixtures.
    # mongodb does not support prefix to differentiate different tests and the server is session-scoped
    # therefore lib_name is needed to be used to avoid reusing library name or list_versions check will fail

    if (
            "--group" not in sys.argv or sys.argv[sys.argv.index("--group") + 1] == "1"
    ):  # To detect pytest-split parallelization group in use
        mongo_host = os.getenv("CI_MONGO_HOST", "localhost")
        mongo_port = 27017
    else:
        mongo_host = os.getenv("CI_MONGO_HOST_2", "localhost")
        mongo_port = 27017

    mongo_path = f"{mongo_host}:{mongo_port}"
    try:
        res = requests.get(f"http://{mongo_path}")
        have_running_mongo = res.status_code == 200 and "mongodb" in res.text.lower()
    except requests.exceptions.ConnectionError:
        have_running_mongo = False

    if have_running_mongo:
        uri = f"mongodb://{mongo_path}"
    else:
        mongo_server = request.getfixturevalue("mongo_server")
        uri = f"mongodb://{mongo_server.hostname}:{mongo_server.port}"

    yield uri

    # mongodb does not support prefix to differentiate different tests and the server is session-scoped
    mongo_client = pymongo.MongoClient(uri)
    for db_name in mongo_client.list_database_names():
        if db_name not in ["local", "admin", "apiomat", "config"]:
            mongo_client.drop_database(db_name)


@pytest.fixture
def mongo_store_factory(request, lib_name, mongo_test_uri):
    cfg_maker = functools.partial(create_test_mongo_cfg, uri=mongo_test_uri)
    used = {}
    try:
        yield functools.partial(_version_store_factory_impl, used, cfg_maker, lib_name)
    finally:
        for lib in used.values():
            lib.version_store.clear()


@pytest.fixture
>>>>>>> 53b03983 (Batch read versions with snapshots)
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


<<<<<<< HEAD
=======
@pytest.fixture(scope="session")
def azurite_port():
    return _get_ephemeral_port()


@pytest.fixture
def azurite_container():
    global bucket_id
    container = f"testbucket{bucket_id}"
    bucket_id = bucket_id + 1
    return container


@pytest.fixture(scope="session")
def spawn_azurite(azurite_port):
    temp_folder = tempfile.TemporaryDirectory()
    try:  # Awaiting fix for cleanup in windows file-in-use problem
        p = subprocess.Popen(
            f"azurite --silent --blobPort {azurite_port} --blobHost 127.0.0.1 --queuePort 0 --tablePort 0",
            cwd=temp_folder.name,
            shell=True,
        )
        time.sleep(2)  # Wait for Azurite to start up
        yield
    finally:
        print("Killing Azurite")
        if sys.platform == "win32":  # different way of killing process on Windows
            os.system(f"taskkill /F /PID {p.pid}")
        else:
            p.kill()
        temp_folder = None  # For cleanup; For an unknown reason somehow using with syntax causes error


@pytest.fixture(
    scope="function",
    params=(
            [
                "moto_s3_uri_incl_bucket",
                pytest.param("azurite_azure_uri_incl_bucket", marks=AZURE_TESTS_MARK),
                pytest.param("mongo_test_uri", marks=MONGO_TESTS_MARK),
                pytest.param("unique_real_s3_uri", marks=REAL_S3_TESTS_MARK),
            ]
    ),
)
def object_storage_uri_incl_bucket(request):
    yield request.getfixturevalue(request.param)


>>>>>>> 53b03983 (Batch read versions with snapshots)
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
<<<<<<< HEAD
=======
def in_memory_store_factory(lib_name):
    cfg_maker = functools.partial(create_test_memory_cfg)
    used = {}

    def create_version_store(
            col_per_group: Optional[int] = None,
            row_per_segment: Optional[int] = None,
            override_name: str = None,
            **kwargs,
    ) -> NativeVersionStore:
        if col_per_group is not None and "column_group_size" not in kwargs:
            kwargs["column_group_size"] = col_per_group
        if row_per_segment is not None and "segment_row_size" not in kwargs:
            kwargs["segment_row_size"] = row_per_segment

        if "lmdb_config" in kwargs:
            del kwargs["lmdb_config"]

        if override_name is not None:
            library_name = override_name
        else:
            library_name = lib_name

        return _version_store_factory_impl(used, cfg_maker, library_name, **kwargs)

    try:
        yield create_version_store
    finally:
        for lib in used.values():
            lib.version_store.clear()


@pytest.fixture
>>>>>>> 53b03983 (Batch read versions with snapshots)
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
