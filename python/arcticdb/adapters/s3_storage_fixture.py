import re
import time
import json
import requests
from typing import Any, NamedTuple, List

import boto3
import werkzeug
from moto.server import DomainDispatcherApplication, create_backend_app

from arcticdb.version_store.helper import add_s3_library_to_env
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap

from .storage_fixture_api import *


Key = NamedTuple("Key", [("id", str), ("secret", str), ("user_name", str)])


class S3Bucket(StorageFixture):
    _FIELD_REGEX = {
        ArcticUriFields.HOST: re.compile("^s3://()([^:/]+)"),
        ArcticUriFields.BUCKET: re.compile("^s3://[^:]+(:)([^?]+)"),
        ArcticUriFields.USER: re.compile("[?&](access=)([^&]+)&?"),
        ArcticUriFields.PASSWORD: re.compile("[?&](secret=)([^&]+)&?"),
    }

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

        s3 = factory
        self.arctic_uri = f"s3://{s3.host}:{self.bucket}?access={self.key.id}&secret={self.key.secret}&port={s3.port}"

    def __exit__(self, exc_type, exc_value, traceback):
        self.factory.cleanup_bucket(self)

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
    _enforcing_permissions = False
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
        gracefully_kill_process(self._p)

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
        self._enforcing_permissions = enforcing

    def create_fixture(self) -> S3Bucket:
        return S3Bucket(self)

    def cleanup_bucket(self, b: S3Bucket):
        self._live_buckets.remove(b)
        if not len(self._live_buckets):
            requests.post(self._iam_endpoint + "/moto-api/reset")
            self._iam_admin = None
        else:
            for lib in b.generated_libs.values():
                lib.version_store.clear()

            self._s3_admin.delete_bucket(Bucket=b.bucket)
