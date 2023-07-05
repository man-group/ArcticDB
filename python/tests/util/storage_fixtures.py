"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import multiprocessing
import shutil
import time
import functools
import pytest
import json
import socketserver
import requests
from typing import Optional, Any, Dict, Callable, NamedTuple, List
from contextlib import AbstractContextManager, contextmanager
from abc import abstractmethod
from tempfile import mkdtemp

import boto3
import werkzeug
from moto.server import DomainDispatcherApplication, create_backend_app

from arcticdb.config import Defaults
from arcticdb.version_store import NativeVersionStore
from arcticdb.version_store.helper import ArcticMemoryConfig, add_s3_library_to_env, add_lmdb_library_to_env
from arcticdb.util.test import apply_lib_cfg, gracefully_terminate_process
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap


def get_ephemeral_port():  # https://stackoverflow.com/a/61685162/
    with socketserver.TCPServer(("localhost", 0), None) as s:
        return s.server_address[1]


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


class StorageFixture(AbstractContextManager):
    """Manages the life-cycle of a piece of storage and provides test facing methods:"""

    generated_libs: Dict[str, NativeVersionStore] = {}

    def __enter__(self):
        """Fixtures are typically set up in __init__, so this just returns self"""
        return self

    @abstractmethod
    def get_arctic_uri(self) -> str:
        """Return the URI for use with the ``Arctic`` constructor"""

    @abstractmethod
    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        """Creates a new storage config instance.
        If ``lib_name`` is the same as a previous call on this instance, then that storage should be reused."""

    def create_version_store_factory(self, default_lib_name: str):
        """Returns a function that takes optional library options and produces ``NativeVersionStore``s"""
        return functools.partial(
            _version_store_factory_impl, self.generated_libs, self.create_test_cfg, default_lib_name
        )

    @abstractmethod
    def set_permission(self, *, read: bool, write: bool):
        """Makes the connection to the storage have the given permissions. If unsupported, call ``pytest.skip()``.
        See ``set_enforcing_permissions`` below."""


class StorageFixtureFactory(AbstractContextManager):
    """For ``StorageFixture``s backed by shared/expensive resources, implement this class to manage those."""

    enforcing_permissions = False

    def __exit__(self, exc_type, exc_value, traceback):
        """Properly clean up the fixture. This should be safe to be called multiple times."""

    def set_enforcing_permissions(self, enforcing: bool):
        """Controls permission enforcement.
        Should affect subsequent calls to ``create_fixture()`` and possibly existing fixtures."""
        pytest.skip(f"{self.__class__.__name__} does not support permissions")

    @contextmanager
    def enforcing_permissions_context(self, set_to=True):
        try:
            saved = self.enforcing_permissions
            self.set_enforcing_permissions(set_to)
            yield
        finally:
            self.set_enforcing_permissions(saved)

    @abstractmethod
    def create_fixture(self) -> StorageFixture:
        ...


Key = NamedTuple("Key", [("id", str), ("secret", str), ("user_name", str)])


# ========================== Concrete sub-types ==========================
class S3Bucket(StorageFixture):
    _bucket_id = 0
    bucket: str
    key: Key

    def __init__(self, factory: "MotoS3StorageFixtureFactory"):
        self.factory = factory
        factory._live_buckets.append(self)

        bucket = self.bucket = f"test_bucket_{self._bucket_id}"
        factory._s3_admin.create_bucket(Bucket=bucket)
        S3Bucket._bucket_id = S3Bucket._bucket_id + 1

        if factory.enforcing_permissions:
            self.key = factory._create_user_get_key(bucket + "_user")
        else:
            self.key = factory._DUMMY_KEY

    def __exit__(self, exc_type, exc_value, traceback):
        self.factory._cleanup_bucket(self)

    def get_arctic_uri(self):
        s3 = self.factory
        return f"s3://{s3.host}:{self.bucket}?access={self.key.id}&secret={self.key.secret}&port={s3.port}"

    def make_boto_client(self):
        return self.factory._boto("s3", self.key)

    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        add_s3_library_to_env(
            cfg,
            lib_name=lib_name,
            env_name=Defaults.ENV,
            credential_name=self.key.id,
            credential_key=self.key.secret,
            bucket_name=self.bucket,
            endpoint=self.factory.endpoint,
            with_prefix=False,  # to allow s3_store_factory reuse_name to work correctly
        )
        return cfg

    def set_permission(self, *, read: bool, write: bool):
        f = self.factory
        assert f.enforcing_permissions and self.key is not f._DUMMY_KEY
        if read:
            f._iam_admin.put_user_policy(
                UserName=self.key.user_name, PolicyName="bucket", PolicyDocument=f._RW_POLICY if write else f._RO_POLICY
            )
        else:
            f._iam_admin.delete_user_policy(UserName=self.key.user_name, PolicyName="bucket")


class _HostDispatcherApplication(DomainDispatcherApplication):
    """The stand-alone server needs a way to distinguish between S3 and IAM. We use the host for that"""

    _MAP = {"localhost": "s3", "127.0.0.1": "iam"}

    def get_backend_for_host(self, host):
        return self._MAP.get(host, host)


class MotoS3StorageFixtureFactory(StorageFixtureFactory):
    _DUMMY_KEY = Key("awd", "awd", "dummy")
    _RO_POLICY: str
    _RW_POLICY: str
    host = "localhost"
    port: int
    endpoint: str
    _iam_endpoint: str
    _p: multiprocessing.Process
    _s3_admin: Any
    _iam_admin: Any = None
    _live_buckets: List[S3Bucket] = []

    @staticmethod
    def run_server(port):
        werkzeug.run_simple(
            "0.0.0.0",
            port,
            _HostDispatcherApplication(create_backend_app),
            threaded=True,
            ssl_context=None,
        )

    def _boto(self, service: str, key: Key):
        return boto3.client(
            service_name=service,
            endpoint_url=self.endpoint if service == "s3" else self._iam_endpoint,
            region_name="us-east-1",
            aws_access_key_id=key.id,
            aws_secret_access_key=key.secret,
        )

    def __enter__(self):
        port = self.port = get_ephemeral_port()
        self.endpoint = f"http://{self.host}:{port}"
        self._iam_endpoint = f"http://127.0.0.1:{port}"

        p = self._p = multiprocessing.Process(target=self.run_server, args=(port,))
        p.start()
        time.sleep(0.5)

        self._s3_admin = self._boto("s3", self._DUMMY_KEY)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        gracefully_terminate_process(self._p)

    def _create_user_get_key(self, user: str, iam=None):
        iam = iam or self._iam_admin
        user_id = iam.create_user(UserName=user)["User"]["UserId"]
        response = iam.create_access_key(UserName=user)["AccessKey"]
        return Key(response["AccessKeyId"], response["SecretAccessKey"], user)

    def set_enforcing_permissions(self, enforcing: bool):
        # Inspired by https://github.com/getmoto/moto/blob/master/tests/test_s3/test_s3_auth.py
        if enforcing == self.enforcing_permissions:
            return
        if enforcing and not self._iam_admin:
            iam = self._boto("iam", self._DUMMY_KEY)

            def _policy(*statements):
                return json.dumps({"Version": "2012-10-17", "Statement": statements})

            policy = _policy(
                {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
                {"Effect": "Allow", "Action": "iam:*", "Resource": "*"},
            )
            policy_arn = iam.create_policy(PolicyName="admin", PolicyDocument=policy)["Policy"]["Arn"]

            self._RO_POLICY = _policy({"Effect": "Allow", "Action": ["s3:List*", "s3:Get*"], "Resource": "*"})
            self._RW_POLICY = _policy({"Effect": "Allow", "Action": "s3:*", "Resource": "*"})

            key = self._create_user_get_key("admin", iam)
            iam.attach_user_policy(UserName="admin", PolicyArn=policy_arn)
            self._iam_admin = self._boto("iam", key)
            self._s3_admin = self._boto("s3", key)

        # The number is the remaining requests before permission checks kick in
        requests.post(self._iam_endpoint + "/moto-api/reset-auth", "0" if enforcing else "inf")
        self.enforcing_permissions = enforcing

    def create_fixture(self) -> S3Bucket:
        return S3Bucket(self)

    def _cleanup_bucket(self, b: S3Bucket):
        self._live_buckets.remove(b)
        if not len(self._live_buckets):
            requests.post(self._iam_endpoint + "/moto-api/reset")
            self._iam_admin = None
        else:
            for lib in b.generated_libs.values():
                lib.version_store.clear()

            self._s3_admin.delete_bucket(Bucket=b.bucket)


class LmdbStorageFixture(StorageFixture):
    def __init__(self, db_dir: Optional[str]):
        self.db_dir = db_dir or mkdtemp(suffix="LmdbStorageFixtureFactory")

    def __exit__(self, exc_type, exc_value, traceback):
        for result in self.generated_libs.values():
            #  pytest holds a member variable `cached_result` equal to `result` above which keeps the storage alive and
            #  locked. See https://github.com/pytest-dev/pytest/issues/5642 . So we need to decref the C++ objects
            #  keeping the LMDB env open before they will release the lock and allow Windows to delete the LMDB files.
            result.version_store = None
            result._library = None

        shutil.rmtree(self.db_dir, ignore_errors=True)

    def get_arctic_uri(self) -> str:
        return "lmdb://" + self.db_dir  # TODO: extra config?

    def create_test_cfg(self, lib_name: str, *, lmdb_config: Dict[str, Any] = {}) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        add_lmdb_library_to_env(
            cfg, lib_name=lib_name, env_name=Defaults.ENV, db_dir=self.db_dir, lmdb_config=lmdb_config
        )
        return cfg

    def create_version_store_factory(self, default_lib_name: str):
        def create_version_store(
            col_per_group: Optional[int] = None,  # Legacy option names
            row_per_segment: Optional[int] = None,
            lmdb_config: Dict[str, Any] = {},  # Mainly to allow setting map_size
            **kwargs,
        ) -> NativeVersionStore:
            if col_per_group is not None and "column_group_size" not in kwargs:
                kwargs["column_group_size"] = col_per_group
            if row_per_segment is not None and "segment_row_size" not in kwargs:
                kwargs["segment_row_size"] = row_per_segment
            cfg_factory = self.create_test_cfg
            if lmdb_config:
                cfg_factory = functools.partial(cfg_factory, lmdb_config=lmdb_config)
            return _version_store_factory_impl(self.generated_libs, cfg_factory, default_lib_name, **kwargs)

        return create_version_store
