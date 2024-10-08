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
import platform
from tempfile import mkdtemp
import boto3
import time


import requests
from typing import Optional, Any, Type

from .api import *
from .utils import get_ephemeral_port, GracefulProcessUtils, wait_for_server_to_come_up, safer_rmtree, get_ca_cert_for_testing
from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, AWSAuthMethod
from arcticdb.version_store.helper import add_s3_library_to_env

# All storage client libraries to be imported on-demand to speed up start-up of ad-hoc test runs

_PermissionCapableFactory: Type["MotoS3StorageFixtureFactory"] = None  # To be set later

logging.getLogger("botocore").setLevel(logging.INFO)

S3_CONFIG_PATH = os.path.expanduser(os.path.join("~", ".aws", "config"))
S3_BACKUP_CONFIG_PATH = os.path.expanduser(os.path.join("~", ".aws", "bk_config"))


class Key:
    def __init__(self, id: str, secret: str, user_name: str):
        self.id = id
        self.secret = secret
        self.user_name = user_name

class S3Bucket(StorageFixture):
    _FIELD_REGEX = {
        ArcticUriFields.HOST: re.compile("^s3://()([^:/]+)"),
        ArcticUriFields.BUCKET: re.compile("^s3://[^:]+(:)([^?]+)"),
        ArcticUriFields.USER: re.compile("[?&](access=)([^&]+)(&?)"),
        ArcticUriFields.PASSWORD: re.compile("[?&](secret=)([^&]+)(&?)"),
        ArcticUriFields.PATH_PREFIX: re.compile("[?&](path_prefix=)([^&]+)(&?)"),
        ArcticUriFields.CA_PATH: re.compile("[?&](CA_cert_path=)([^&]*)(&?)"),
        ArcticUriFields.SSL: re.compile("[?&](ssl=)([^&]+)(&?)"),
        ArcticUriFields.AWS_AUTH: re.compile("[?&](aws_auth=)([^&]+)(&?)"),
        ArcticUriFields.AWS_PROFILE: re.compile("[?&](aws_profile=)([^&]+)(&?)"),
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
        self.arctic_uri = f"s3{secure or ''}://{host}:{self.bucket}?"
        
        if factory.aws_auth == None or factory.aws_auth == AWSAuthMethod.DISABLED:
            self.arctic_uri += f"access={self.key.id}&secret={self.key.secret}"
        else:
            self.arctic_uri += f"aws_auth={factory.aws_auth}"
            if factory.aws_auth == AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER:
                assert factory.aws_profile is not None
                self.arctic_uri += f"&aws_profile={factory.aws_profile}"
        if port:
            self.arctic_uri += f"&port={port}"
        if factory.default_prefix:
            self.arctic_uri += f"&path_prefix={factory.default_prefix}"
        if factory.ssl:
            self.arctic_uri += "&ssl=True"
        if platform.system() == "Linux":
            if factory.client_cert_file:
                self.arctic_uri += f"&CA_cert_path={self.factory.client_cert_file}"
            # client_cert_dir is skipped on purpose; It will be tested manually in other tests

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
            is_https=self.factory.endpoint.startswith("https://"),
            region=self.factory.region,
            use_mock_storage_for_testing=self.factory.use_mock_storage_for_testing,
            ssl=self.factory.ssl,
            ca_cert_path=self.factory.client_cert_file,
            is_nfs_layout=False,
            use_raw_prefix=self.factory.use_raw_prefix,
            aws_auth=self.factory.aws_auth,
            aws_profile=self.factory.aws_profile,
        )# client_cert_dir is skipped on purpose; It will be tested manually in other tests
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


class NfsS3Bucket(S3Bucket):

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
            is_https=self.factory.endpoint.startswith("https://"),
            region=self.factory.region,
            use_mock_storage_for_testing=self.factory.use_mock_storage_for_testing,
            ssl=self.factory.ssl,
            ca_cert_path=self.factory.client_cert_file,
            is_nfs_layout=True,
            use_raw_prefix=self.factory.use_raw_prefix,
            aws_auth=self.factory.aws_auth,
            aws_profile=self.factory.aws_profile,
        )# client_cert_dir is skipped on purpose; It will be tested manually in other tests
        return cfg


class BaseS3StorageFixtureFactory(StorageFixtureFactory):
    """Logic and fields common to real and mock S3"""

    endpoint: str
    region: str
    default_key: Key
    default_bucket: Optional[str] = None
    default_prefix: Optional[str] = None
    use_raw_prefix: bool = False
    clean_bucket_on_fixture_exit = True
    use_mock_storage_for_testing = None  # If set to true allows error simulation

    def __init__(self):
        self.client_cert_file = None
        self.client_cert_dir = None
        self.ssl = False
        self.aws_auth = None
        self.aws_profile = None
        self.aws_policy_name = None
        self.aws_role = None
        self.aws_role_arn = None
        self.sts_test_key = None

    def __str__(self):
        return f"{type(self).__name__}[{self.default_bucket or self.endpoint}]"

    def _boto(self, service: str, key: Key, api="client"):
        ctor = getattr(boto3, api)
        return ctor(
            service_name=service,
            endpoint_url=self.endpoint if service == "s3" else self._iam_endpoint,
            region_name=self.region,
            aws_access_key_id=key.id,
            aws_secret_access_key=key.secret,
            verify=self.client_cert_file if self.client_cert_file else False,
        )  # verify=False cannot skip verification on buggy boto3 in py3.6

    def create_fixture(self) -> S3Bucket:
        return S3Bucket(self, self.default_bucket)

    def cleanup_bucket(self, b: S3Bucket):
        # When dealing with a potentially shared bucket, we only clear our the libs we know about:
        b.slow_cleanup(failure_consequence="We will be charged unless we manually delete it. ")


def real_s3_from_environment_variables(shared_path: bool):
    out = BaseS3StorageFixtureFactory()
    out.endpoint = os.getenv("ARCTICDB_REAL_S3_ENDPOINT")
    out.region = os.getenv("ARCTICDB_REAL_S3_REGION")
    out.default_bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
    access_key = os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY")
    secret_key = os.getenv("ARCTICDB_REAL_S3_SECRET_KEY")
    out.default_key = Key(access_key, secret_key, "unknown user")
    out.clean_bucket_on_fixture_exit = os.getenv("ARCTICDB_REAL_S3_CLEAR").lower() in ["true", "1"]
    out.ssl = out.endpoint.startswith("https://")
    if shared_path:
        out.default_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX")
    else:
        out.default_prefix = os.getenv("ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX")
    return out


def real_s3_sts_from_environment_variables(user_name: str, role_name: str, policy_name: str, profile_name: str):
    out = real_s3_from_environment_variables(False)
    iam_client = boto3.client("iam", aws_access_key_id=out.default_key.id, aws_secret_access_key=out.default_key.secret)
    # Create IAM user
    try:
        iam_client.create_user(UserName=user_name)
        out.sts_test_key = Key(None, None, user_name)
        print("User created successfully.")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print("User already exists.")
    except Exception as e:
        print(f"Error creating user: {e}")
        raise e

    account_id = boto3.client("sts", aws_access_key_id=out.default_key.id, aws_secret_access_key=out.default_key.secret).get_caller_identity().get("Account")
    # Create IAM role
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com",
                "AWS": account_id
            },
            "Action": "sts:AssumeRole"
        }
        ]
    }

    try:
        role_response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
        )
        out.aws_role_arn = role_response["Role"]["Arn"]
        out.aws_role = role_name
        print("Role created successfully.")
    except iam_client.exceptions.EntityAlreadyExistsException:
        out.aws_role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
        print("Role already exists.")
    except Exception as e:
        print(f"Error creating role: {e}")
        raise e

    # Create a policy for S3 bucket access
    s3_access_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{out.default_bucket}"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListObject"],
                "Resource": [f"arn:aws:s3:::{out.default_bucket}/*"]
            }
        ]
    }

    try:
        policy_response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(s3_access_policy_document)
        )
        out.aws_policy_name = policy_response["Policy"]["Arn"]
        print("Policy created successfully.")
    except iam_client.exceptions.EntityAlreadyExistsException:
        out.aws_policy_name = f"arn:aws:iam::{account_id}:policy/{policy_name}"
        print("Policy already exists.")
    except Exception as e:
        print(f"Error creating policy: {e}")
        raise e

    # Attach the policy to the role
    try:
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=out.aws_policy_name
        )
        print("Policyattached to role successfully.")
    except Exception as e:
        print(f"Error attaching policy to role: {e}")
        raise e

    # Create an inline policy for the user to assume the role
    assume_role_user_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": f"arn:aws:iam::{account_id}:role/{role_name}",
            }
        ]
    }

    try:
        iam_client.put_user_policy(
            UserName=user_name,
            PolicyName="AssumeRolePolicy",
            PolicyDocument=json.dumps(assume_role_user_policy_document)
        )
        print("Inline policy to assume role attached to user successfully.")
    except Exception as e:
        print(f"Error attaching inline policy to user: {e}")
        raise e

    print("User created with role to access bucket.")

    try:
        access_key_response = iam_client.create_access_key(UserName=user_name)
        out.sts_test_key.id = access_key_response["AccessKey"]["AccessKeyId"]
        out.sts_test_key.secret = access_key_response["AccessKey"]["SecretAccessKey"]
        print("Access key created successfully.")
    except Exception as e:
        print(f"Error creating access key: {e}")
        raise e
    
    out.aws_auth = AWSAuthMethod.STS_PROFILE_CREDENTIALS_PROVIDER
    out.aws_profile = profile_name
    real_s3_sts_write_local_credentials(out)
    return out


def real_s3_sts_write_local_credentials(factory: BaseS3StorageFixtureFactory):
    base_profile_name = factory.aws_profile + "_base"
    aws_credentials = f"""
[profile {factory.aws_profile}]
role_arn = {factory.aws_role_arn}
source_profile = {base_profile_name}

[profile {base_profile_name}]
aws_access_key_id = {factory.sts_test_key.id}
aws_secret_access_key = {factory.sts_test_key.secret}
"""
    aws_dir = os.path.dirname(S3_CONFIG_PATH)
    if not os.path.exists(aws_dir):
        os.makedirs(aws_dir)

    if os.path.exists(S3_CONFIG_PATH):
        os.rename(S3_CONFIG_PATH, S3_BACKUP_CONFIG_PATH)

    with open(S3_CONFIG_PATH, "w") as config_file:
        config_file.write(aws_credentials)


def real_s3_sts_resources_ready(factory: BaseS3StorageFixtureFactory): 
    sts_client = boto3.client(
        "sts",
        aws_access_key_id=factory.sts_test_key.id,
        aws_secret_access_key=factory.sts_test_key.secret
    )
    for _ in range(20):
        try:
            assumed_role = sts_client.assume_role(
                RoleArn=factory.aws_role_arn,
                RoleSessionName="TestSession"
            )
            print("Boto3 assume role successful.")
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                aws_session_token=assumed_role['Credentials']['SessionToken']
            )
            response = s3_client.list_objects_v2(Bucket=factory.default_bucket)
            print(f"S3 list objects test successful: {response['ResponseMetadata']['HTTPStatusCode']}")
            return
        except:
            print(f"Assume role failed. Retrying in 1 second...") # Don't print the exception as it could contain sensitive information, e.g. user id
            time.sleep(1)

    raise Exception("iam resources not ready")


def real_s3_sts_clean_up(factory: BaseS3StorageFixtureFactory, role_name: str, policy_name: str, user_name: str):
    iam_client = boto3.client("iam", aws_access_key_id=os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY"), aws_secret_access_key=os.getenv("ARCTICDB_REAL_S3_SECRET_KEY"))
    print("Starting cleanup process...")
    try:
        if factory.aws_policy_name:
            iam_client.detach_role_policy(
            RoleName=role_name,
            PolicyArn=factory.aws_policy_name
        )
        iam_client.delete_policy(
            PolicyArn=factory.aws_policy_name
        )
        print("Policy deleted successfully.")
    except Exception as e:
        print("Error deleting policy")

    try:
        if factory.aws_role: # == role_name; It's just a indicator whether role was created or not
            iam_client.delete_role(
                RoleName=role_name
            )
            print("Role deleted successfully.")
    except Exception as e:
        print("Error deleting role")

    
    try:
        if factory.sts_test_key and factory.sts_test_key.id:
            iam_client.delete_access_key(
                UserName=user_name,
                AccessKeyId=factory.sts_test_key.id
            )
            print("Access key id deleted successfully.")
    except Exception as e:
        print("Error deleting access key id")

    try:
        if factory.sts_test_key and factory.sts_test_key.user_name: # == user_name; It's just a indicator whether role was created or not
            # Detach user policies
            policies = iam_client.list_user_policies(UserName=user_name)["PolicyNames"]
            for policy_name in policies:
                iam_client.delete_user_policy(UserName=user_name, PolicyName=policy_name)
                print("Detached and deleted inline policy from user")

            # Delete the user
            iam_client.delete_user(
                UserName=user_name
            )
            print("User deleted successfully.")
    except Exception as e:
        print("Error deleting user")

        
    if os.path.exists(S3_CONFIG_PATH) and os.path.exists(S3_BACKUP_CONFIG_PATH):
        os.remove(S3_CONFIG_PATH)
        os.rename(S3_BACKUP_CONFIG_PATH, S3_CONFIG_PATH)


def mock_s3_with_error_simulation():
    """Creates a mock s3 storage fixture which can simulate errors depending on symbol names.

    The mock s3 is an internal ArctcDB construct and is intended to only test storage failures.
    For how to trigger failures you can refer to the documentation in mock_s3_client.hpp.
    """
    out = BaseS3StorageFixtureFactory()
    out.use_mock_storage_for_testing = True
    # We set some values which don't matter since we're using the mock storage
    out.default_key = Key("access key", "secret key", "unknown user")
    out.endpoint = "http://test"
    out.region = "us-east-1"
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

    def __init__(self,
                 use_ssl: bool,
                 ssl_test_support: bool,
                 bucket_versioning: bool,
                 default_prefix: str = None,
                 use_raw_prefix: bool = False):
        super().__init__()
        self.http_protocol = "https" if use_ssl else "http"
        self.ssl_test_support = ssl_test_support
        self.bucket_versioning = bucket_versioning
        self.default_prefix = default_prefix
        self.use_raw_prefix = use_raw_prefix

    @staticmethod
    def run_server(port, key_file, cert_file):
        import werkzeug
        from moto.server import DomainDispatcherApplication, create_backend_app

        class _HostDispatcherApplication(DomainDispatcherApplication):
            _reqs_till_rate_limit = -1

            def get_backend_for_host(self, host):
                """The stand-alone server needs a way to distinguish between S3 and IAM. We use the host for that"""
                if host is None:
                    return None
                if "s3" in host or host == "localhost":
                    return "s3"
                elif host == "127.0.0.1":
                    return "iam"
                elif host == "moto_api":
                    return "moto_api"
                else:
                    raise RuntimeError(f"Unknown host {host}")

            def __call__(self, environ, start_response):
                path_info: bytes = environ.get("PATH_INFO", "")

                with self.lock:
                    # Mock ec2 imds responses for testing
                    if path_info in (
                        "/latest/dynamic/instance-identity/document",
                        b"/latest/dynamic/instance-identity/document",
                    ):
                        start_response("200 OK", [("Content-Type", "text/plain")])
                        return [b"Something to prove imds is reachable"]

                    # Allow setting up a rate limit
                    if path_info in ("/rate_limit", b"/rate_limit"):
                        length = int(environ["CONTENT_LENGTH"])
                        body = environ["wsgi.input"].read(length).decode("ascii")
                        self._reqs_till_rate_limit = int(body)
                        start_response("200 OK", [("Content-Type", "text/plain")])
                        return [b"Limit accepted"]

                    if self._reqs_till_rate_limit == 0:
                        response_body = (
                            b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message>'
                            b"<RequestId>176C22715A856A29</RequestId><HostId>9Gjjt1m+cjU4OPvX9O9/8RuvnG41MRb/18Oux2o5H5MY7ISNTlXN+Dz9IG62/ILVxhAGI0qyPfg=</HostId></Error>"
                        )
                        start_response(
                            "503 Slow Down", [("Content-Type", "text/xml"), ("Content-Length", str(len(response_body)))]
                        )
                        return [response_body]
                    else:
                        self._reqs_till_rate_limit -= 1

                return super().__call__(environ, start_response)

        werkzeug.run_simple(
            "0.0.0.0",
            port,
            _HostDispatcherApplication(create_backend_app),
            threaded=True,
            ssl_context=(cert_file, key_file) if cert_file and key_file else None,
        )

    def _start_server(self):
        port = self.port = get_ephemeral_port(2)
        self.endpoint = f"{self.http_protocol}://{self.host}:{port}"
        self.working_dir = mkdtemp(suffix="MotoS3StorageFixtureFactory")
        self._iam_endpoint = f"{self.http_protocol}://localhost:{port}"

        self.ssl = self.http_protocol == "https" # In real world, using https protocol doesn't necessarily mean ssl will be verified
        if self.ssl_test_support:
            self.ca, self.key_file, self.cert_file, self.client_cert_file = get_ca_cert_for_testing(self.working_dir)
        else:
            self.ca = ""
            self.key_file = ""
            self.cert_file = ""
            self.client_cert_file = ""
        self.client_cert_dir = self.working_dir
        
        self._p = multiprocessing.Process(
            target=self.run_server,
            args=(
                port,
                self.key_file if self.http_protocol == "https" else None,
                self.cert_file if self.http_protocol == "https" else None,
            ),
        )
        self._p.start()
        wait_for_server_to_come_up(self.endpoint, "moto", self._p)

    def _safe_enter(self):
        for _ in range(3):  # For unknown reason, Moto, when running in pytest-xdist, will randomly fail to start
            try:
                self._start_server()
                break
            except AssertionError as e:  # Thrown by wait_for_server_to_come_up
                sys.stderr.write(repr(e))
                GracefulProcessUtils.terminate(self._p)

        self._s3_admin = self._boto(service="s3", key=self.default_key)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        GracefulProcessUtils.terminate(self._p)
        safer_rmtree(self, self.working_dir)

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
            iam = self._boto(service="iam", key=self.default_key)

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
            self._iam_admin = self._boto(service="iam", key=key)
            self._s3_admin = self._boto(service="s3", key=key)

        # The number is the remaining requests before permission checks kick in
        requests.post(self._iam_endpoint + "/moto-api/reset-auth", "0" if enforcing else "inf")
        self._enforcing_permissions = enforcing

    def create_fixture(self) -> S3Bucket:
        bucket = f"test_bucket_{self._bucket_id}"
        self._s3_admin.create_bucket(Bucket=bucket)
        self._bucket_id += 1
        if self.bucket_versioning:
            self._s3_admin.put_bucket_versioning(
                Bucket=bucket,
                VersioningConfiguration={
                    'Status': 'Enabled'
                }
            )

        out = S3Bucket(self, bucket)
        self._live_buckets.append(out)
        return out

    def cleanup_bucket(self, b: S3Bucket):
        self._live_buckets.remove(b)
        if len(self._live_buckets):
            b.slow_cleanup(failure_consequence="The following delete bucket call will also fail. ")
            self._s3_admin.delete_bucket(Bucket=b.bucket)
        else:
            requests.post(
                self._iam_endpoint + "/moto-api/reset", verify=False
            )  # If CA cert verify fails, it will take ages for this line to finish
            self._iam_admin = None


_PermissionCapableFactory = MotoS3StorageFixtureFactory


class MotoNfsBackedS3StorageFixtureFactory(MotoS3StorageFixtureFactory):

    def create_fixture(self, default_prefix=None, use_raw_prefix=False) -> NfsS3Bucket:
        bucket = f"test_bucket_{self._bucket_id}"
        self._s3_admin.create_bucket(Bucket=bucket)
        self._bucket_id += 1
        out = NfsS3Bucket(self, bucket)
        self._live_buckets.append(out)
        return out
