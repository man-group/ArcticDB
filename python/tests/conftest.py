"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import functools
import multiprocessing
import shutil

import boto3
import werkzeug
from moto.server import DomainDispatcherApplication, create_backend_app

import sys
import signal
import enum

if sys.platform == "win32":
    # Hack to define signal.SIGKILL as some deps eg pytest-test-fixtures hardcode SIGKILL terminations.
    signal.SIGKILL = signal.SIGINT

import time
import os
import pytest
import numpy as np
import pandas as pd
import random
from datetime import datetime
from typing import Optional, Any, Dict
import subprocess
from pathlib import Path
import socket
import tempfile

import requests
from pytest_server_fixtures.base import get_ephemeral_port

from arcticdb.arctic import Arctic
from arcticdb.version_store.helper import (
    create_test_lmdb_cfg,
    create_test_s3_cfg,
    create_test_mongo_cfg,
    create_test_azure_cfg,
)
from arcticdb.config import Defaults
from arcticdb.util.test import configure_test_logger, apply_lib_cfg
from arcticdb.util.storage_test import get_real_s3_uri, real_s3_credentials
from arcticdb.version_store.helper import ArcticMemoryConfig
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store._normalization import MsgPackNormalizer
from arcticdb.options import LibraryOptions
from arcticdb_ext.storage import Library
from arcticdb_ext.tools import AZURE_SUPPORT

PERSISTENT_STORAGE_TESTS_ENABLED = os.getenv("ARCTICDB_PERSISTENT_STORAGE_TESTS") == "1"

if AZURE_SUPPORT:
    from azure.storage.blob import BlobServiceClient

configure_test_logger()

bucket_id = 0
# Use a smaller memory mapped limit for all tests
MsgPackNormalizer.MMAP_DEFAULT_SIZE = 20 * (1 << 20)


def run_server(port):
    werkzeug.run_simple(
        "0.0.0.0", port, DomainDispatcherApplication(create_backend_app, service="s3"), threaded=True, ssl_context=None
    )


@pytest.fixture(scope="module")
def _moto_s3_uri_module():
    port = get_ephemeral_port()
    p = multiprocessing.Process(target=run_server, args=(port,))
    p.start()

    time.sleep(0.5)

    yield f"http://localhost:{port}"

    try:
        # terminate sends SIGTERM - no need to be polite here so...
        os.kill(p.pid, signal.SIGKILL)

        p.terminate()
        p.join()
    except Exception:
        pass


@pytest.fixture(scope="function")
def azure_client_and_create_container(azurite_container, azurite_azure_uri):
    client = BlobServiceClient.from_connection_string(
        conn_str=azurite_azure_uri, container_name=azurite_container
    )  # add connection_verify=False to bypass ssl checking

    container_client = client.get_container_client(container=azurite_container)
    if not container_client.exists():
        client.create_container(name=azurite_container)

    return client


@pytest.fixture(scope="function")
def boto_client(_moto_s3_uri_module):
    endpoint = _moto_s3_uri_module
    client = boto3.client(
        service_name="s3", endpoint_url=endpoint, aws_access_key_id="awd", aws_secret_access_key="awd"
    )

    yield client


@pytest.fixture
def aws_access_key():
    return "awd"


@pytest.fixture
def aws_secret_key():
    return "awd"


@pytest.fixture(scope="function")
def moto_s3_endpoint_and_credentials(_moto_s3_uri_module, aws_access_key, aws_secret_key):
    global bucket_id

    endpoint = _moto_s3_uri_module
    port = endpoint.rsplit(":", 1)[1]
    client = boto3.client(
        service_name="s3", endpoint_url=endpoint, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )

    bucket = f"test_bucket_{bucket_id}"
    client.create_bucket(Bucket=bucket)
    bucket_id = bucket_id + 1
    yield endpoint, port, bucket, aws_access_key, aws_secret_key


@pytest.fixture(scope="function")
def moto_s3_uri_incl_bucket(moto_s3_endpoint_and_credentials):
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials
    yield endpoint.replace("http://", "s3://").rsplit(":", 1)[
        0
    ] + ":" + bucket + "?access=" + aws_access_key + "&secret=" + aws_secret_key + "&port=" + port


@pytest.fixture(
    scope="function",
    params=[
        "S3",
        "LMDB",
        pytest.param("Azure", marks=pytest.mark.skipif(not AZURE_SUPPORT, reason="Pending Azure Storge Conda support")),
    ],
)
def arctic_client(request, moto_s3_uri_incl_bucket, tmpdir, encoding_version):
    if request.param == "S3":
        ac = Arctic(moto_s3_uri_incl_bucket, encoding_version)
    elif request.param == "Azure":
        ac = Arctic(request.getfixturevalue("azurite_azure_uri_incl_bucket"), encoding_version)
    elif request.param == "LMDB":
        ac = Arctic(f"lmdb://{tmpdir}", encoding_version)
    else:
        raise NotImplementedError()

    assert not ac.list_libraries()
    yield ac


@pytest.fixture
def azurite_azure_test_connection_setting(azurite_port, azurite_container, spawn_azurite):
    credential_name = "devstoreaccount1"
    credential_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    endpoint = f"http://127.0.0.1:{azurite_port}"
    # Default cert path is used; May run into problem on Linux's non RHEL distribution; See more on https://github.com/man-group/ArcticDB/issues/514
    ca_cert_path = ""

    return endpoint, azurite_container, credential_name, credential_key, ca_cert_path


@pytest.fixture
def azurite_azure_uri(azurite_azure_test_connection_setting):
    (endpoint, container, credential_name, credential_key, ca_cert_path) = azurite_azure_test_connection_setting
    return f"azure://DefaultEndpointsProtocol=http;AccountName={credential_name};AccountKey={credential_key};BlobEndpoint={endpoint}/{credential_name};Container={container};CA_cert_path={ca_cert_path}"  # semi-colon at the end is not accepted by the sdk


@pytest.fixture
def azurite_azure_uri_incl_bucket(azurite_azure_uri, azure_client_and_create_container):
    return azurite_azure_uri


@pytest.fixture(scope="function")
def arctic_library(request, arctic_client):
    if AZURE_SUPPORT:
        request.getfixturevalue("azure_client_and_create_container")

    arctic_client.delete_library("pytest_test_lib")
    arctic_client.create_library("pytest_test_lib")
    return arctic_client["pytest_test_lib"]


@pytest.fixture()
def sym():
    return "test" + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture()
def lib_name():
    return f"local.test_{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}"


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
        lmdb_config: Dict[str, Any] = {},
        override_name: str = None,
        **kwargs,
    ) -> NativeVersionStore:
        if col_per_group is not None and "column_group_size" not in kwargs:
            kwargs["column_group_size"] = col_per_group
        if row_per_segment is not None and "segment_row_size" not in kwargs:
            kwargs["segment_row_size"] = row_per_segment

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


def _arctic_library_factory_impl(used, name, arctic_client, library_options) -> Library:
    """Common logic behind all the factory fixtures"""
    name = name or default_name
    if name == "_unique_":
        name = name + str(len(used))
    assert name not in used, f"{name} is already in use"
    arctic_client.create_library(name, library_options)
    out = arctic_client[name]
    used[name] = out
    return out


@pytest.fixture(scope="function")
def library_factory(arctic_client, lib_name):
    used: Dict[str, Library] = {}

    def create_library(
        library_options: Optional[LibraryOptions] = None,
    ) -> Library:
        return _arctic_library_factory_impl(used, lib_name, arctic_client, library_options)

    return create_library


@pytest.fixture
def s3_store_factory(lib_name, moto_s3_endpoint_and_credentials):
    """Factory to create any number of S3 libs with the given WriteOptions or VersionStoreConfig.

    `name` can be a magical value "_unique_" which will create libs with unique names.
    This factory will clean up any libraries requested
    """
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials

    # Not exposing the config factory to discourage people from creating libs that won't get cleaned up
    def make_cfg(name):
        # with_prefix=False to allow reuse_name to work correctly
        return create_test_s3_cfg(name, aws_access_key, aws_secret_key, bucket, endpoint, with_prefix=False)

    used = {}
    try:
        yield functools.partial(_version_store_factory_impl, used, make_cfg, lib_name)
    finally:
        for lib in used.values():
            lib.version_store.clear()


# @pytest.fixture(scope="function")
# def real_s3_credentials():
#     endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
#     bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
#     region = os.getenv("ARCTICDB_REAL_S3_REGION")
#     access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
#     secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
#     path_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_PATH_PREFIX")
#     clear = True if str(os.getenv("ARCTICDB_REAL_S3_CLEAR")).lower() in ["true", "1"] else False

#     yield endpoint, bucket, region, access_key, secret_key, path_prefix, clear


# @pytest.fixture(scope="function")
# def real_s3_uri(real_s3_credentials):
#     endpoint, bucket, region, access_key, secret_key, path_prefix, _ = real_s3_credentials
#     uri = f"s3s://{endpoint}:{bucket}?access={access_key}&secret={secret_key}&region={region}&path_prefix={path_prefix}"
#     yield uri


@pytest.fixture
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


@pytest.fixture(scope="function")
def real_s3_version_store(real_s3_store_factory):
    return real_s3_store_factory()


@pytest.fixture
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
    scope="function",
    params=[
        "s3_store_factory",
        pytest.param(
            "azure_store_factory",
            marks=pytest.mark.skipif(not AZURE_SUPPORT, reason="Pending Azure Storge Conda support"),
        ),
        pytest.param(
            "real_s3_store_factory",
            marks=pytest.mark.skipif(
                not PERSISTENT_STORAGE_TESTS_ENABLED,
                reason="This store can be used only if the persistent storage tests are enabled",
            ),
        ),
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
        pytest.param(
            "azure_store_factory",
            marks=pytest.mark.skipif(not AZURE_SUPPORT, reason="Pending Azure Storge Conda support"),
        ),
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
        # "version_store_factory",
        pytest.param(
            "real_s3_store_factory",
            marks=pytest.mark.skipif(
                not PERSISTENT_STORAGE_TESTS_ENABLED,
                reason="This store can be used only if the persistent storage tests are enabled",
            ),
        ),
    ],
)
def basic_store_factory(request):
    """
    Designed to test the bare minimum of stores
     - LMDB for local storage
     - AWS S3 for persistent storage, if enabled
    """
    store_factory = request.getfixturevalue(request.param)
    return store_factory


@pytest.fixture
def basic_store(basic_store_factory):
    """
    Designed to test the bare minimum of stores
     - LMDB for local storage
     - AWS S3 for persistent storage, if enabled
    """
    return basic_store_factory()


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
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


# TODO: Remove these
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
        raise ValueError(f"Unexpected encoding version: {encoding_version}")


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


## TODO: Remove these


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
        dynamic_schema=True, dynamic_strings=True, encoding_version=int(EncodingVersion.V2), override_name=library_name
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
def basic_store_delayed_deletes_v1(basic_store_factory):
    return basic_store_factory(delayed_deletes=True, dynamic_strings=True, prune_previous_version=True)


@pytest.fixture
def basic_store_delayed_deletes_v2(basic_store_factory, lib_name):
    library_name = lib_name + "_v2"
    return basic_store_factory(
        dynamic_strings=True, delayed_deletes=True, encoding_version=int(EncodingVersion.V2), override_name=library_name
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


@pytest.fixture(scope="module")
def azurite_port():
    return get_ephemeral_port()


@pytest.fixture
def azurite_container():
    global bucket_id
    container = f"testbucket{bucket_id}"
    bucket_id = bucket_id + 1
    return container


@pytest.fixture(scope="module")
def spawn_azurite(azurite_port):
    if AZURE_SUPPORT:
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

    else:
        yield


@pytest.fixture(
    scope="function",
    params=(
        [
            "moto_s3_uri_incl_bucket",
            pytest.param(
                "azurite_azure_uri_incl_bucket",
                marks=pytest.mark.skipif(not AZURE_SUPPORT, reason="Pending Azure Storge Conda support"),
            ),
            # TODO: See if we can support this
            # pytest.param(
            #     "real_s3_uri",
            #     marks=pytest.mark.skipif(
            #         not PERSISTENT_STORAGE_TESTS_ENABLED, reason="Can be used only when persistent storage is enabled"
            #     ),
            # ),
        ]
    ),
)
def object_storage_uri_incl_bucket(request):
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    scope="function",
    params=(
        [
            "lmdb_version_store_v1",
            "lmdb_version_store_v2",
            "s3_version_store_v1",
            "s3_version_store_v2",
            pytest.param(
                "mongo_version_store",
                marks=pytest.mark.skipif(sys.platform != "linux", reason="The mongo store is only supported on Linux"),
            ),
            pytest.param(
                "azure_version_store",
                marks=pytest.mark.skipif(not AZURE_SUPPORT, reason="Pending Azure Storage Conda support"),
            ),
            pytest.param(
                "real_s3_version_store",
                marks=pytest.mark.skipif(
                    not PERSISTENT_STORAGE_TESTS_ENABLED, reason="Can be used only when persistent storage is enabled"
                ),
            ),
        ]
    ),
)
def object_and_lmdb_version_store(request):
    """
    Designed to test all supported stores
    """
    yield request.getfixturevalue(request.param)
