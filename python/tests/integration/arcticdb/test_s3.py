import pytest
import os
import re
import sys
from subprocess import run, PIPE

from tests.util.storage_fixtures import MotoS3StorageFixtureFactory

_DEFAULT_ENV = {
    **os.environ,
    "ARCTICDB_S3STORAGE_CHECKBUCKETMAXWAIT_INT": "3000",  # Moto is too slow and always time out on the default 1s
}


def check_storage_is_accessible_logging(uri, expected, http_status, env=_DEFAULT_ENV):
    code = f"""from arcticdb import Arctic; Arctic("{uri}")"""
    p = run([sys.executable], universal_newlines=True, input=code, stderr=PIPE, timeout=15, env=env)
    print(p.stderr)
    assert expected in p.stderr
    assert str(http_status) in p.stderr


@pytest.mark.parametrize("change", ["access", "secret"])
def test_check_storage_is_accessible_invalid_access_key(change, s3_bucket):
    uri = re.compile(rf"(.*{change}=)[^&]+(.*)").match(s3_bucket.get_arctic_uri()).expand(r"\1bad_key\2")
    with s3_bucket.factory.enforcing_permissions_context():
        check_storage_is_accessible_logging(uri, "[warning]", 403)


def test_check_storage_is_accessible_permission(s3_storage_factory: MotoS3StorageFixtureFactory):
    with s3_storage_factory.enforcing_permissions_context():
        with s3_storage_factory.create_fixture() as bucket:
            check_storage_is_accessible_logging(bucket.get_arctic_uri(), "[warning]", 403)
            # Double-check the permission logic is not broken
            bucket.set_permission(read=True, write=True)  # Moto returns 403 on a read-only policy
            bucket.make_boto_client().head_bucket(Bucket=bucket.bucket)


def test_check_storage_is_accessible_no_bucket(s3_bucket):
    uri = re.compile(r"(s3:[^:]+)[^?]+(.*)").match(s3_bucket.get_arctic_uri()).expand(r"\1:bad_bucket\2")
    expected_msg = "UserInputException: E_INVALID_USER_ARGUMENT The specified bucket bad_bucket does not exist"
    check_storage_is_accessible_logging(uri, expected_msg, 404)


def test_check_storage_is_accessible_network_error(s3_bucket):
    uri = s3_bucket.get_arctic_uri().replace("localhost", "234.0.0.0")
    env = {
        **_DEFAULT_ENV,
        "ARCTICDB_S3STORAGE_CONNECTTIMEOUTMS_INT": "500",
        "ARCTICDB_S3STORAGE_REQUESTTIMEOUTMS_INT": "500",
        "AWS_RETRY_MODE": "standard",
        "AWS_MAX_ATTEMPTS": "1",  # Otherwise it takes minutes before the AWS SDK will shut down
    }
    check_storage_is_accessible_logging(uri, "[info]", -1, env)
