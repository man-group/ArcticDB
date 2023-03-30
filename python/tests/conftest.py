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
from typing import Optional
from functools import partial

from pytest_server_fixtures.base import get_ephemeral_port
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
from arcticc.pb2.lmdb_storage_pb2 import Config as LmdbConfig

from arcticdb.arctic import Arctic
from arcticdb.version_store.helper import (
    get_storage_for_lib_name,
    get_lib_cfg,
    create_test_lmdb_cfg,
    create_test_s3_cfg,
)
from arcticdb.config import Defaults
from arcticdb.util.test import configure_test_logger, apply_lib_cfg
from arcticdb.version_store.helper import ArcticMemoryConfig
from arcticdb.version_store import NativeVersionStore
from pandas import DataFrame

configure_test_logger()

BUCKET_ID = 0


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
    global BUCKET_ID

    endpoint = _moto_s3_uri_module
    port = endpoint.rsplit(":", 1)[1]
    client = boto3.client(
        service_name="s3", endpoint_url=endpoint, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )

    bucket = f"test_bucket_{BUCKET_ID}"
    client.create_bucket(Bucket=bucket)
    BUCKET_ID = BUCKET_ID + 1
    yield endpoint, port, bucket, aws_access_key, aws_secret_key


@pytest.fixture(scope="function")
def moto_s3_uri_incl_bucket(moto_s3_endpoint_and_credentials):
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials
    yield endpoint.replace("http://", "s3://").rsplit(":", 1)[
        0
    ] + ":" + bucket + "?access=" + aws_access_key + "&secret=" + aws_secret_key + "&port=" + port


@pytest.fixture(scope="function")
def arctic_library(moto_s3_uri_incl_bucket):
    ac = Arctic(moto_s3_uri_incl_bucket)
    assert not ac.list_libraries()
    ac.create_library("pytest_test_lib")
    yield ac["pytest_test_lib"]


@pytest.fixture()
def sym():
    return "test" + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture()
def lib_name():
    return "local.test" + datetime.utcnow().strftime("%Y-%m-%dT%H_%M_%S_%f")


@pytest.fixture
def arcticdb_test_lmdb_config(tmpdir):
    def create(lib_name):
        return create_test_lmdb_cfg(lib_name=lib_name, db_dir=str(tmpdir))

    return create


def get_temp_dbdir(tmpdir, num):
    return str(tmpdir.mkdir("lmdb.{:x}".format(num)))


@pytest.fixture
def arcticdb_test_library_config(tmpdir):
    def create():
        lib_name = "test.example"
        env_name = "default"
        cfg = EnvironmentConfigsMap()
        env = cfg.env_by_id[env_name]

        lmdb = LmdbConfig()
        lmdb.path = get_temp_dbdir(tmpdir, 0)
        sid, storage = get_storage_for_lib_name(lib_name, env)
        storage.config.Pack(lmdb, type_url_prefix="cxx.arctic.org")

        lib_desc = env.lib_by_path[lib_name]
        lib_desc.storage_ids.append(sid)
        lib_desc.name = lib_name
        lib_desc.description = "A friendly config for testing"

        version = lib_desc.version
        version.write_options.column_group_size = 23
        version.write_options.segment_row_size = 42

        return cfg

    return create


@pytest.fixture
def arcticdb_test_s3_config(moto_s3_endpoint_and_credentials):
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials

    def create(lib_name):
        return create_test_s3_cfg(lib_name, aws_access_key, aws_secret_key, bucket, endpoint)

    return create


def _version_store_factory_impl(
    used, make_cfg, default_name, *, name: str = None, reuse_name=False, **kwargs
) -> NativeVersionStore:
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


def lmdb_version_store_cleanup(version_store_fixture):

    @functools.wraps(version_store_fixture)
    def wrapped(*args, **kwargs):
        result: NativeVersionStore = version_store_fixture(*args, **kwargs)

        yield result

        #  pytest holds a member variable `cached_result` equal to `result` above which keeps the storage alive and
        #  locked. See https://github.com/pytest-dev/pytest/issues/5642 . So we need to decref the C++ objects keeping
        #  the LMDB env open before they will release the lock and allow Windows to delete the LMDB files.
        result.version_store = None
        result._library = None

        cfg = result.lib_cfg()
        for storage in cfg.storage_by_id.values():
            lmdb = LmdbConfig()
            lmdb.ParseFromString(storage.config.value)
            shutil.rmtree(lmdb.path)

    return wrapped


@pytest.fixture
def version_store_factory(arcticdb_test_lmdb_config, lib_name):
    """Factory to create any number of distinct LMDB libs with the given WriteOptions or VersionStoreConfig.

    Accepts legacy options col_per_group and row_per_segment which defaults to 2.
    `name` can be a magical value "_unique_" which will create libs with unique names."""
    used = {}

    def version_store(col_per_group: Optional[int] = None, row_per_segment: Optional[int] = None, **kwargs) -> NativeVersionStore:
        if col_per_group is not None and "column_group_size" not in kwargs:
            kwargs["column_group_size"] = col_per_group
        if row_per_segment is not None and "segment_row_size" not in kwargs:
            kwargs["segment_row_size"] = row_per_segment
        return _version_store_factory_impl(used, arcticdb_test_lmdb_config, lib_name, **kwargs)

    return version_store


@pytest.fixture
def s3_store_factory(lib_name, arcticdb_test_s3_config):
    """Factory to create any number of S3 libs with the given WriteOptions or VersionStoreConfig.

    `name` can be a magical value "_unique_" which will create libs with unique names.
    This factory will clean up any libraries requested
    """
    used = {}
    try:
        yield partial(_version_store_factory_impl, used, arcticdb_test_s3_config, lib_name)
    finally:
        for lib in used.values():
            lib.version_store.clear()


@pytest.fixture
def lmdb_version_store_with_write_option(arcticdb_test_lmdb_config, request, lib_name):
    config = ArcticMemoryConfig(arcticdb_test_lmdb_config(lib_name), env=Defaults.ENV)
    lib_cfg = get_lib_cfg(config, Defaults.ENV, lib_name)
    for option in request.param:
        if option == "sync_passive.enabled":
            setattr(lib_cfg.version.write_options.sync_passive, "enabled", True)
        else:
            setattr(lib_cfg.version.write_options, option, True)

    return config[lib_name]


@pytest.fixture(scope="function")
def s3_version_store(s3_store_factory):
    return s3_store_factory()


@pytest.fixture(scope="function")
def s3_version_store_prune_previous(s3_store_factory):
    return s3_store_factory(prune_previous_version=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_string_coercion(version_store_factory):
    return version_store_factory()


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store(version_store_factory):
    return version_store_factory(dynamic_strings=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_column_buckets(version_store_factory):
    return version_store_factory(dynamic_schema=True, column_group_size=3, segment_row_size=2, bucketize_dynamic=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_dynamic_schema(version_store_factory):
    return version_store_factory(dynamic_schema=True, dynamic_strings=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_delayed_deletes(version_store_factory):
    return version_store_factory(delayed_deletes=True, dynamic_strings=True, prune_previous_version=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_tombstones_no_symbol_list(version_store_factory):
    return version_store_factory(use_tombstones=True, dynamic_schema=True, symbol_list=False, dynamic_strings=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_allows_pickling(version_store_factory, lib_name):
    return version_store_factory(use_norm_failure_handler_known_types=True, dynamic_strings=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_no_symbol_list(version_store_factory):
    return version_store_factory(col_per_group=None, row_per_segment=None, symbol_list=False)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_tombstone_and_pruning(version_store_factory):
    return version_store_factory(use_tombstones=True, prune_previous_version=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_tombstone(version_store_factory):
    return version_store_factory(use_tombstones=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_tombstone_and_sync_passive(version_store_factory):
    return version_store_factory(use_tombstones=True, sync_passive=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_ignore_order(version_store_factory):
    return version_store_factory(ignore_sort_order=True)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_small_segment(version_store_factory):
    return version_store_factory(column_group_size=1000, segment_row_size=1000)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_tiny_segment(version_store_factory):
    return version_store_factory(column_group_size=2, segment_row_size=2)


@pytest.fixture
@lmdb_version_store_cleanup
def lmdb_version_store_tiny_segment_dynamic(version_store_factory):
    return version_store_factory(column_group_size=2, segment_row_size=2, dynamic_schema=True)


@pytest.fixture
def one_col_df():
    def create(start=0):
        return DataFrame({"x": np.arange(start, start + 10, dtype=np.int64)})

    return create


@pytest.fixture
def two_col_df():
    def create(start=0):
        return DataFrame(
            {"x": np.arange(start, start + 10, dtype=np.int64), "y": np.arange(start + 10, start + 20, dtype=np.int64)}
        )

    return create


@pytest.fixture
def three_col_df():
    def create(start=0):
        return DataFrame(
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
