"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import logging
import multiprocessing
import json
import os
import re
import sys
import time

import requests
from typing import NamedTuple, Optional, Any, Type

from .api import *
from .utils import get_ephemeral_port, GracefulProcessUtils, wait_for_server_to_come_up
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap
from arcticdb.version_store.helper import add_s3_library_to_env

# All storage client libraries to be imported on-demand to speed up start-up of ad-hoc test runs

Key = NamedTuple("Key", [("id", str), ("secret", str), ("user_name", str)])
_PermissionCapableFactory: Type["MotoS3StorageFixtureFactory"] = None  # To be set later

logging.getLogger("botocore").setLevel(logging.INFO)


class S3Bucket(StorageFixture):
    _FIELD_REGEX = {
        ArcticUriFields.HOST: re.compile("^s3://()([^:/]+)"),
        ArcticUriFields.BUCKET: re.compile("^s3://[^:]+(:)([^?]+)"),
        ArcticUriFields.USER: re.compile("[?&](access=)([^&]+)(&?)"),
        ArcticUriFields.PASSWORD: re.compile("[?&](secret=)([^&]+)(&?)"),
    }

    key: Key
    _boto_bucket: Any = None

    def __init__(self, factory: "BaseS3StorageFixtureFactory", bucket: str):
        super().__init__()
        self.factory = factory
        self.bucket = bucket

        if isinstance(factory, _PermissionCapableFactory) and factory.enforcing_permissions:
            self.key = factory._create_user_get_key(bucket + "_user")
        else:
            self.key = factory.default_key

        secure, host, port = re.match(r"(?:http(s?)://)?([^:/]+)(?::(\d+))?", factory.endpoint).groups()
        self.arctic_uri = f"s3{secure or ''}://{host}:{self.bucket}?access={self.key.id}&secret={self.key.secret}"
        if port:
            self.arctic_uri += f"&port={port}"
        if factory.default_prefix:
            self.arctic_uri += f"&path_prefix={factory.default_prefix}"

    def __exit__(self, exc_type, exc_value, traceback):
        if self.factory.clean_bucket_on_fixture_exit:
            self.factory.cleanup_bucket(self)

    def create_test_cfg(self, lib_name: str) -> EnvironmentConfigsMap:
        cfg = EnvironmentConfigsMap()
        if self.factory.default_prefix:
            with_prefix = f"{self.factory.default_prefix}/{lib_name}"
        else:
            with_prefix = False

        add_s3_library_to_env(
            cfg,
            lib_name=lib_name,
            env_name=Defaults.ENV,
            credential_name=self.key.id,
            credential_key=self.key.secret,
            bucket_name=self.bucket,
            endpoint=self.factory.endpoint,
            with_prefix=with_prefix,
            is_https=self.factory.endpoint.startswith("s3s:"),
            region=self.factory.region,
        )
        return cfg

    def set_permission(self, *, read: bool, write: bool):
        factory = self.factory
        assert isinstance(factory, _PermissionCapableFactory)
        assert factory.enforcing_permissions and self.key is not factory.default_key
        if read:
            factory._iam_admin.put_user_policy(
                UserName=self.key.user_name,
                PolicyName="bucket",
                PolicyDocument=factory._RW_POLICY if write else factory._RO_POLICY,
            )
        else:
            factory._iam_admin.delete_user_policy(UserName=self.key.user_name, PolicyName="bucket")

    def get_boto_bucket(self):
        """Lazy singleton. Not thread-safe."""
        if not self._boto_bucket:
            self._boto_bucket = self.factory._boto("s3", self.key, api="resource").Bucket(self.bucket)
        return self._boto_bucket

    def iter_underlying_object_names(self):
        return (obj.key for obj in self.get_boto_bucket().objects.all())

    def copy_underlying_objects_to(self, destination: "S3Bucket"):
        source_client = self.factory._boto("s3", self.key)
        dest = destination.get_boto_bucket()
        for key in self.iter_underlying_object_names():
            dest.copy({"Bucket": self.bucket, "Key": key}, key, SourceClient=source_client)


class BaseS3StorageFixtureFactory(StorageFixtureFactory):
    """Logic and fields common to real and mock S3"""

    endpoint: str
    region: str
    default_key: Key
    default_bucket: Optional[str] = None
    default_prefix: Optional[str] = None
    clean_bucket_on_fixture_exit = True

    def __str__(self):
        return f"{type(self).__name__}[{self.default_bucket or self.endpoint}]"

    def _boto(self, service: str, key: Key, api="client"):
        import boto3

        ctor = getattr(boto3, api)
        return ctor(
            service_name=service,
            endpoint_url=self.endpoint if service == "s3" else self._iam_endpoint,
            region_name=self.region,
            aws_access_key_id=key.id,
            aws_secret_access_key=key.secret,
        )

    def create_fixture(self) -> S3Bucket:
        return S3Bucket(self, self.default_bucket)

    def cleanup_bucket(self, b: S3Bucket):
        # When dealing with a potentially shared bucket, we only clear our the libs we know about:
        b.slow_cleanup(failure_consequence="We will be charged unless we manually delete it. ")


def real_s3_from_environment_variables(*, shared_path: bool):
    out = BaseS3StorageFixtureFactory()
    out.endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    out.region = os.getenv("ARCTICDB_REAL_S3_REGION")
    out.default_bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    out.default_key = Key(access_key, secret_key, "unknown user")
    out.clean_bucket_on_fixture_exit = os.getenv("ARCTICDB_REAL_S3_CLEAR").lower() in ["true", "1"]
    if shared_path:
        out.default_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        out.default_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX")
    return out


class MotoS3StorageFixtureFactory(BaseS3StorageFixtureFactory):
    default_key = Key("awd", "awd", "dummy")
    _RO_POLICY: str
    _RW_POLICY: str
    host = "localhost"
    region = "us-east-1"
    port: int
    endpoint: str
    _enforcing_permissions = False
    _iam_endpoint: str
    _p: multiprocessing.Process
    _s3_admin: Any
    _iam_admin: Any = None
    _bucket_id = 0
    _live_buckets: List[S3Bucket] = []

    @staticmethod
    def run_server(port):
        import werkzeug
        from moto.server import DomainDispatcherApplication, create_backend_app

        class _HostDispatcherApplication(DomainDispatcherApplication):
            """The stand-alone server needs a way to distinguish between S3 and IAM. We use the host for that"""

            _MAP = {"s3.us-east-1.amazonaws.com": "s3", "localhost": "s3", "127.0.0.1": "iam"}

            _reqs_till_rate_limit = -1

            def get_backend_for_host(self, host):
                return self._MAP.get(host, host)

            def __call__(self, environ, start_response):
                path_info: bytes = environ.get("PATH_INFO", "")

                with self.lock:
                    if path_info in ("/rate_limit", b"/rate_limit"):
                        length = int(environ["CONTENT_LENGTH"])
                        body = environ["wsgi.input"].read(length).decode("ascii")
                        self._reqs_till_rate_limit = int(body)
                        start_response("200 OK", [("Content-Type", "text/plain")])
                        return [b"Limit accepted"]

                    if self._reqs_till_rate_limit == 0:
                        start_response("503 Slow down", [("Content-Type", "text/plain")])
                        return [b"Simulating rate limit"]
                    else:
                        self._reqs_till_rate_limit -= 1

                return super().__call__(environ, start_response)

        werkzeug.run_simple(
            "0.0.0.0",
            port,
            _HostDispatcherApplication(create_backend_app),
            threaded=True,
            ssl_context=None,
        )

    def _start_server(self):
        port = self.port = get_ephemeral_port(2)
        self.endpoint = f"http://{self.host}:{port}"
        self._iam_endpoint = f"http://127.0.0.1:{port}"

        p = self._p = multiprocessing.Process(target=self.run_server, args=(port,))
        p.start()
        wait_for_server_to_come_up(self.endpoint, "moto", p)

    def _safe_enter(self):
        for attempt in range(3):  # For unknown reason, Moto, when running in pytest-xdist, will randomly fail to start
            try:
                self._start_server()
                break
            except AssertionError as e:  # Thrown by wait_for_server_to_come_up
                sys.stderr.write(repr(e))
                GracefulProcessUtils.terminate(self._p)

        self._s3_admin = self._boto("s3", self.default_key)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        GracefulProcessUtils.terminate(self._p)

    def _create_user_get_key(self, user: str, iam=None):
        iam = iam or self._iam_admin
        user_id = iam.create_user(UserName=user)["User"]["UserId"]
        response = iam.create_access_key(UserName=user)["AccessKey"]
        return Key(response["AccessKeyId"], response["SecretAccessKey"], user)

    @property
    def enforcing_permissions(self):
        return self._enforcing_permissions

    @enforcing_permissions.setter
    def enforcing_permissions(self, enforcing: bool):
        # Inspired by https://github.com/getmoto/moto/blob/master/tests/test_s3/test_s3_auth.py
        if enforcing == self._enforcing_permissions:
            return
        if enforcing and not self._iam_admin:
            iam = self._boto("iam", self.default_key)

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
        self._enforcing_permissions = enforcing

    def create_fixture(self) -> S3Bucket:
        bucket = f"test_bucket_{self._bucket_id}"
        self._s3_admin.create_bucket(Bucket=bucket)
        self._bucket_id += 1

        out = S3Bucket(self, bucket)
        self._live_buckets.append(out)
        return out

    def cleanup_bucket(self, b: S3Bucket):
        self._live_buckets.remove(b)
        if len(self._live_buckets):
            b.slow_cleanup(failure_consequence="The following delete bucket call will also fail. ")
            self._s3_admin.delete_bucket(Bucket=b.bucket)
        else:
            requests.post(self._iam_endpoint + "/moto-api/reset")
            self._iam_admin = None


_PermissionCapableFactory = MotoS3StorageFixtureFactory
