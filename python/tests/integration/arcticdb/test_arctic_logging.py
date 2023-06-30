"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import pytest
import os
import sys
from subprocess import run, PIPE

from arcticdb.adapters.storage_fixture_api import ArcticUriFields

# Tests in this file forks another process in order to capture the logging output


_DEFAULT_ENV = {
    **os.environ,
    "ARCTICDB_S3STORAGE_CHECKBUCKETMAXWAIT_INT": "3000",  # Moto is too slow and always time out on the default 1s
    "ARCTICDB_AZURESTORAGE_CHECKCONTAINERMAXWAITMS_INT": "1000",
}


def _check_storage_is_accessible_logging(uri, *expected_strs, env=_DEFAULT_ENV):
    code = f"""import logging; logging.basicConfig(); logging.getLogger("arcticdb.adapters").setLevel(logging.INFO)
from arcticdb import Arctic; Arctic("{uri}")"""
    p = run([sys.executable], universal_newlines=True, input=code, stderr=PIPE, timeout=15, env=env)
    print(p.stderr, flush=True)
    for expected in expected_strs:
        assert str(expected) in p.stderr


@pytest.mark.parametrize("field", [ArcticUriFields.USER, ArcticUriFields.PASSWORD])
def test_check_storage_is_accessible_invalid_access_key(field, object_storage_fixture):
    storage = object_storage_fixture
    uri = storage.replace_uri_field(storage.arctic_uri, field, "YmFk")  # "bad" in Base64 for Azure
    with storage.factory.enforcing_permissions_context():
        _check_storage_is_accessible_logging(uri, "WARN", "No permission", 403)


def test_check_storage_is_accessible_permission(object_storage_fixture):
    factory = object_storage_fixture.factory
    with factory.enforcing_permissions_context():
        with factory.create_fixture() as bucket:
            _check_storage_is_accessible_logging(bucket.arctic_uri, "WARNING", "No permission", 403)


def test_check_storage_is_accessible_no_bucket(object_storage_fixture):
    storage = object_storage_fixture
    uri = storage.replace_uri_field(storage.arctic_uri, ArcticUriFields.BUCKET, "bad")
    _check_storage_is_accessible_logging(uri, "Error", 404)


def test_check_storage_is_accessible_network_error_s3(s3_bucket):
    storage = s3_bucket
    uri = storage.replace_uri_field(storage.arctic_uri, ArcticUriFields.HOST, "256.0.0.0")
    env = {
        **_DEFAULT_ENV,
        "ARCTICDB_S3STORAGE_CONNECTTIMEOUTMS_INT": "500",
        "ARCTICDB_S3STORAGE_REQUESTTIMEOUTMS_INT": "500",
        "AWS_RETRY_MODE": "standard",
        "AWS_MAX_ATTEMPTS": "1",  # Otherwise it takes minutes before the AWS SDK will shut down
    }
    _check_storage_is_accessible_logging(uri, "WARN", "Cannot connect", env=env)

def test_check_storage_is_accessible_network_error_azure(azurite_container):
    # Azure's implementation is broken in that it will repeatedly try an invalid address until timeout
    storage = azurite_container
    uri = storage.replace_uri_field(storage.arctic_uri, ArcticUriFields.HOST, "256.0.0.0")
    _check_storage_is_accessible_logging(uri, "Unable to determine")

