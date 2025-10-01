from pathlib import Path
from typing import List, Union
import arcticdb as adb
import os
import pytest
import logging
import re

from arcticdb.util.logger import get_logger, GitHubSanitizingException
from tests.util.mark import REAL_GCP_TESTS_MARK, REAL_S3_TESTS_MARK
from tests.util.storage_test import real_gcp_credentials, real_s3_credentials


logger = get_logger()


s3_endpoint, s3_bucket, s3_region, s3_access_key, s3_secret_key, s3_prefix, s3_clear = real_s3_credentials(
    shared_path=False
)
gcp_enpoint, gcp_bucket, gcp_region, gcp_access_key, gcp_secret_key, gcp_prefix, gcp_clear = real_gcp_credentials(
    shared_path=False
)

web_address = "<none>"
if s3_endpoint is not None:
    if "amazonaws.com" in s3_endpoint.lower():
        web_address = f"s3.{s3_region}.amazonaws.com"
    else:
        # VAST and Pure do not have region
        web_address = s3_endpoint
        if "://" in s3_endpoint:
            web_address = s3_endpoint.split("://")[1]


access_mark = "*access*"
secret_mark = "*secret*"


def check_creds_file_exists_on_machine():
    # Default locations based on OS
    home_dir = Path.home()
    if os.name == "nt":  # Windows
        default_path = home_dir / ".aws" / "credentials"
    else:  # Linux and macOS
        default_path = home_dir / ".aws" / "credentials"

    if default_path.exists():
        pytest.skip(
            "The test can be executed only on machine \
                    where no AWS credentials file exists"
        )


def execute_uri_test(uri: str, expected: List[str], access: str, secret: str):
    uri = uri.replace(access_mark, access)
    uri = uri.replace(secret_mark, secret)
    result = None
    try:

        ac = adb.Arctic(uri)
        ac.list_libraries()
    except Exception as e:
        result = str(e)

    if expected is None:
        if result is not None:
            raise GitHubSanitizingException(f"Uri {uri} expected to PASS, but it failed with {result}")
    else:
        if not any(word in result for word in expected):
            raise GitHubSanitizingException(
                f"Uri {uri} expected to FAIL with any of [{expected}] in message.\n Failed with different error: {result}"
            )


@pytest.mark.parametrize(
    "uri,expected",
    [
        (f"s3://{web_address}:{s3_bucket}?access={access_mark}&secret={secret_mark}", None),
        (f"s3s://{web_address}:{s3_bucket}?access={access_mark}&secret={secret_mark}", None),
        (f"s3s://{web_address}:{s3_bucket}?access={access_mark}&secret={secret_mark}&path_prefix=abc", None),
        (f"s3s://{web_address}:{s3_bucket}?access={access_mark}&secret={secret_mark}&aws_auth=False", None),
        (
            f"s3s://{web_address}:{s3_bucket}?access={access_mark}&secret={secret_mark}&aws_auth=True",
            ["E_PERMISSION Permission error"],
        ),
        (
            f"s3s://{web_address}:{s3_bucket}?access={access_mark}&secrets={secret_mark}",
            ["Invalid S3 URI. Invalid query parameter"],
        ),
        (f"s3://{web_address}:{s3_bucket}?access={access_mark}&secret={secret_mark}1", ["SignatureDoesNotMatch"]),
        (
            f"s3://{web_address}:{s3_bucket}?access={access_mark}1&secret={secret_mark}",
            ["InvalidAccessKeyId", "SignatureDoesNotMatch"],
        ),
        (f"s3s://{web_address}:{s3_bucket}?access={access_mark}", ["AccessDenied: Access Denied for object"]),
        (f"s3://{web_address}:{s3_bucket}?secret={secret_mark}", ["E_PERMISSION Permission error"]),
    ],
)
@REAL_S3_TESTS_MARK
@pytest.mark.storage
@pytest.mark.authentication
def test_arcticdb_s3_uri(uri: str, expected: List[str]):
    check_creds_file_exists_on_machine()
    execute_uri_test(uri, expected, s3_access_key, s3_secret_key)


@pytest.mark.parametrize(
    "uri,expected",
    [
        (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}", None),
        (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}", None),
        (
            f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}&path_prefix=abc",
            None,
        ),
        (
            f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}&aws_auth=True",
            ["Specified both access and awsauth=true in the GCPXML Arctic URI"],
        ),
        (
            f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secrets={secret_mark}",
            ["Invalid GCPXML URI. Invalid query parameter"],
        ),
        (
            f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}",
            ["Secret or awsauth=true must be specified in GCPXML"],
        ),
        (
            f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}1",
            ["SignatureDoesNotMatch"],
        ),
        (
            f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}1fds&secret={secret_mark}",
            ["S3Error#22 SignatureDoesNotMatch", "S3Error:22, HttpResponseCode:403, SignatureDoesNotMatch"],
        ),
        (
            f"gcpxml://storage.googleapis.com:{gcp_bucket}?secret={secret_mark}",
            ["Access token or awsauth=true must be specified in GCPXML"],
        ),
    ],
)
@pytest.mark.storage
@pytest.mark.authentication
@REAL_GCP_TESTS_MARK
def test_arcticdb_gcpxml_uri(uri: str, expected: List[str]):
    check_creds_file_exists_on_machine()
    execute_uri_test(uri, expected, gcp_access_key, gcp_secret_key)


@pytest.mark.parametrize(
    "uri,expected",
    [
        (
            f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}&aws_auth=False",
            None,
        ),
    ],
)
@pytest.mark.storage
@pytest.mark.authentication
@pytest.mark.bug_ids(["8796367702"])
@REAL_GCP_TESTS_MARK
@pytest.mark.xfail(condition=True, reason="gcpxml does not allow value true to aws_auth query param")
def test_arcticdb_gcpxml_uri_bad(uri: str, expected: str):
    check_creds_file_exists_on_machine()
    execute_uri_test(uri, expected, gcp_access_key, gcp_secret_key)


@REAL_S3_TESTS_MARK
@REAL_GCP_TESTS_MARK
@pytest.mark.storage
@pytest.mark.authentication
def test_arcticdb_s3_config_file(tmpdir):

    uri_gcp = f"gcpxml://storage.googleapis.com:{gcp_bucket}?aws_auth=true"
    uri_aws = f"s3://s3.{s3_region}.amazonaws.com:{s3_bucket}?aws_auth=true"

    def prepare_creds_file(access, secret):
        data = f"[default]\n"
        data += f"aws_access_key_id = {access}\n"
        data += f"aws_secret_access_key = {secret}\n"
        return data

    def execute_test(uri: str, data: str, will_pass: bool):
        file = tmpdir.join("frog")
        file.write(data)
        file_path = file.strpath
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = file_path
        crashed = False
        try:
            ac = adb.Arctic(uri)
            ac.list_libraries()
        except Exception as e:
            crashed = True
        assert will_pass != crashed

    check_creds_file_exists_on_machine()

    data = prepare_creds_file(gcp_access_key, "")
    execute_test(uri_gcp, data, False)

    data = prepare_creds_file(gcp_access_key, gcp_secret_key)
    execute_test(uri_gcp, data, True)

    data = prepare_creds_file("", gcp_secret_key)
    execute_test(uri_gcp, data, False)

    data = prepare_creds_file(gcp_access_key, gcp_secret_key)
    execute_test(uri_gcp, data, True)

    data = prepare_creds_file(gcp_secret_key, gcp_access_key)
    execute_test(uri_gcp, data, False)

    data = prepare_creds_file(s3_access_key, s3_secret_key)
    execute_test(uri_gcp, data, False)

    data = prepare_creds_file(s3_access_key, s3_secret_key)
    execute_test(uri_aws, data, True)

    data = prepare_creds_file(gcp_access_key, gcp_secret_key)
    execute_test(uri_aws, data, False)

    data = prepare_creds_file(gcp_access_key, gcp_secret_key)
    execute_test(uri_gcp, data, True)
