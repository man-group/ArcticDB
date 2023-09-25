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
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict
import subprocess
from pathlib import Path
import socket
import tempfile
import pymongo
import trustme

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
from arcticdb.util.test import configure_test_logger, apply_lib_cfg, RUN_MONGO_TEST
from arcticdb.version_store.helper import ArcticMemoryConfig
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store._normalization import MsgPackNormalizer
from arcticdb.options import LibraryOptions
from arcticdb_ext.storage import Library
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions

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
        conn_str=azurite_azure_uri, container_name=azurite_container, connection_verify=False
    )  # ssl cert verification is off as it is for testing only and function requires CA bundle

    container_client = client.get_container_client(container=azurite_container)
    if not container_client.exists():
        client.create_container(name=azurite_container)

    return client


@pytest.fixture
def azure_account_sas_token(azure_client_and_create_container, azurite_azure_test_connection_setting):
    start_time = datetime.now(timezone.utc)
    expiry_time = start_time + timedelta(days=1)
    _, _, credential_name, credential_key, _ = azurite_azure_test_connection_setting

    sas_token = generate_account_sas(
        account_key=credential_key,
        account_name=credential_name,
        resource_types=ResourceTypes.from_string(
            "sco"
        ),  # Each letter stands for one kind of resources; https://github.com/Azure/azure-sdk-for-python/blob/70ace4351ff78c3fe5a6f579132fc30d305fd3c9/sdk/tables/azure-data-tables/azure/data/tables/_models.py#L585
        permission=AccountSasPermissions.from_string(
            "rwdlacup"
        ),  # Each letter stands for one kind of permission; https://github.com/Azure/azure-sdk-for-python/blob/70ace4351ff78c3fe5a6f579132fc30d305fd3c9/sdk/tables/azure-data-tables/azure/data/tables/_models.py#L656
        expiry=expiry_time,
        start=start_time,
    )
    return sas_token


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
        "Azure",
        pytest.param(
            "Mongo",
            marks=pytest.mark.skipif(not RUN_MONGO_TEST, reason="Mongo test on windows is fiddly"),
        ),
    ],
)
def arctic_client(request, moto_s3_uri_incl_bucket, tmpdir, encoding_version, lib_name):
    if request.param == "S3":
        ac = Arctic(moto_s3_uri_incl_bucket, encoding_version)
    elif request.param == "Azure":
        ac = Arctic(request.getfixturevalue("azurite_azure_uri_incl_bucket"), encoding_version)
    elif request.param == "LMDB":
        ac = Arctic(f"lmdb://{tmpdir}", encoding_version)
    elif request.param == "Mongo":
        ac = Arctic(request.getfixturevalue("mongo_test_uri"), encoding_version)
    else:
        raise NotImplementedError()

    assert not ac.list_libraries()
    yield ac


@pytest.fixture
def azurite_azure_test_connection_setting(azurite_port, azurite_container, spawn_azurite, temp_cert):
    credential_name = "devstoreaccount1"
    credential_key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    endpoint = f"https://127.0.0.1:{azurite_port}"
    # Default cert path is used; May run into problem on Linux's non RHEL distribution; See more on https://github.com/man-group/ArcticDB/issues/514
    (_, _, ca_cert_path) = temp_cert
    ca_cert_dir = os.path.dirname(ca_cert_path)

    return endpoint, azurite_container, credential_name, credential_key, ca_cert_path, ca_cert_dir


@pytest.fixture
def azurite_azure_uri(azurite_azure_test_connection_setting):
    (endpoint, container, credential_name, credential_key, ca_cert_path, _) = azurite_azure_test_connection_setting
    return f"azure://DefaultEndpointsProtocol=https;AccountName={credential_name};AccountKey={credential_key};BlobEndpoint={endpoint}/{credential_name};Container={container};CA_cert_path={ca_cert_path}"  # semi-colon at the end is not accepted by the sdk


@pytest.fixture
def azurite_azure_uri_incl_bucket(azurite_azure_uri, azure_client_and_create_container):
    return azurite_azure_uri


@pytest.fixture(scope="function")
def arctic_library(request, arctic_client, azure_client_and_create_container):
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
        (
            _,
            container,
            credential_name,
            credential_key,
            ca_cert_path,
            ca_cert_dir,
        ) = azurite_azure_test_connection_setting
        return create_test_azure_cfg(
            lib_name=lib_name,
            credential_name=credential_name,
            credential_key=credential_key,
            container_name=container,
            endpoint=azurite_azure_uri,
            ca_cert_path=ca_cert_path,
            ca_cert_dir=ca_cert_dir,
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


@pytest.fixture(scope="function", params=["s3_store_factory", "azure_store_factory"])
def object_store_factory(request):
    store_factory = request.getfixturevalue(request.param)
    return store_factory


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
def temp_cert():
    temp_folder = tempfile.TemporaryDirectory()
    key_file = f"{temp_folder.name}/key.pem"
    cert_file = f"{temp_folder.name}/cert.pem"
    client_cert_file = f"{temp_folder.name}/client.pem"
    ca = trustme.CA()
    server_cert = ca.issue_cert("127.0.0.1")
    server_cert.private_key_pem.write_to_path(key_file)
    server_cert.cert_chain_pems[0].write_to_path(cert_file)
    ca.cert_pem.write_to_path(client_cert_file)
    # Create the sym link for curl CURLOPT_CAPATH option; rehash only available on openssl >=1.1.1
    subprocess.run(
        f'ln -s "{client_cert_file}" "$(openssl x509 -hash -noout -in "{client_cert_file}")".0',
        cwd=temp_folder.name,
        shell=True,
    )
    return key_file, cert_file, client_cert_file


@pytest.fixture(scope="module")
def spawn_azurite(azurite_port, temp_cert):
    temp_folder = tempfile.TemporaryDirectory()
    key_file, cert_file, _ = temp_cert
    try:  # Awaiting fix for cleanup in windows file-in-use problem
        p = subprocess.Popen(
            {
                f"azurite --silent --blobPort {azurite_port} --blobHost 127.0.0.1 --queuePort 0 --tablePort 0 --key"
                f" {key_file} --cert {cert_file}"
            },
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
            "azurite_azure_uri_incl_bucket",
            pytest.param(
                "mongo_test_uri",
                marks=pytest.mark.skipif(not RUN_MONGO_TEST, reason="Mongo test on windows is fiddly"),
            ),
        ]
    ),
)
def object_storage_uri_incl_bucket(request):
    yield request.getfixturevalue(request.param)


@pytest.fixture
def object_version_store(object_store_factory):
    return object_store_factory()


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
    ),
)
def object_and_lmdb_version_store(request):
    yield request.getfixturevalue(request.param)
