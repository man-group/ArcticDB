import arcticdb as adb
import os
import pytest
import logging
import re

from tests.util.mark import REAL_GCP_TESTS_MARK, REAL_S3_TESTS_MARK
from tests.util.storage_test import real_gcp_credentials, real_s3_credentials


class GitHubSanitizingHandler(logging.StreamHandler):
    """
    The handler sanitizes messages only when execution is in GitHub

    Enhancements should be mode as early as possible
    """

    def emit(self, record: logging.LogRecord):
        # Sanitize the message here
        record.msg = self.sanitize_message(record.msg)
        super().emit(record)

    @staticmethod
    def sanitize_message(message: str) -> str:
        if (os.getenv("GITHUB_ACTIONS") == "true") and isinstance(message, str):
            # Use regex to find and replace sensitive access keys
            sanitized_message = re.sub(r'(secret=)[^\s&]+', r'\1***', message)
            sanitized_message = re.sub(r'(access=)[^\s&]+', r'\1***', sanitized_message)
            return sanitized_message
        return message


class GitHubSanitizingException(Exception):
    def __init__(self, message: str):
        # Sanitize the message
        sanitized_message = GitHubSanitizingHandler.sanitize_message(message)
        super().__init__(sanitized_message)


def get_logger(logger_name):
    """
    This logger is sanitizing logger, which can handle masking secrets.
    Should be extended when needed and used wherever it is needed
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = GitHubSanitizingHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)    
    return logger

s3_enpoint, s3_bucket, s3_region, s3_access_key, s3_secret_key, s3_prefix, s3_clear = real_s3_credentials(shared_path=False)
gcp_enpoint, gcp_bucket, gcp_region, gcp_access_key, gcp_secret_key, gcp_prefix, gcp_clear = real_gcp_credentials(shared_path=False)

access_mark = "*access*"
secret_mark = "*secret*"


def execute_uri_test(uri:str, expected: str, access: str, secret: str):
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
        if expected not in result:
            raise GitHubSanitizingException(
                f"Uri {uri} expected to FAIL with [{expected}].\n Failed with error: {result}")
      

@pytest.mark.parametrize("uri,expected", [
    (f"s3://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}&secret={secret_mark}", None),
    (f"s3s://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}&secret={secret_mark}", None),
    (f"s3s://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}&secret={secret_mark}&path_prefix=abc", None),
    (f"s3s://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}&secret={secret_mark}&aws_auth=False", None),
    (f"s3s://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}&secret={secret_mark}&aws_auth=True", "InvalidAccessKeyId"),
    (f"s3s://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}&secrets={secret_mark}", "Invalid S3 URI. Invalid query parameter"),
    (f"s3://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}&secret={secret_mark}1", "SignatureDoesNotMatch"),
    (f"s3://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}1&secret={secret_mark}", "InvalidAccessKeyId"),
    (f"s3s://s3.{s3_region}.amazonaws.com:{s3_bucket}?access={access_mark}", "AccessDenied: Access Denied for object"),
    (f"s3://s3.{s3_region}.amazonaws.com:{s3_bucket}?secret={secret_mark}", "E_PERMISSION Permission error"),
])
@REAL_S3_TESTS_MARK
@pytest.mark.storage
@pytest.mark.authentication
def test_arcticdb_s3_uri(uri:str, expected: str):
    execute_uri_test(uri, expected, s3_access_key, s3_secret_key)


@pytest.mark.parametrize("uri,expected", [
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}", None),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}", None),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}&path_prefix=abc", None),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}&aws_auth=False", None),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}&aws_auth=True", "InvalidAccessKeyId"),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secrets={secret_mark}", "Invalid GCPXML URI. Invalid query parameter"),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}", "Secret or awsauth=true must be specified in GCPXML"),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}&secret={secret_mark}1", "SignatureDoesNotMatch"),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?access={access_mark}1fds&secret={secret_mark}", "S3Error#22 SignatureDoesNotMatch"),
    (f"gcpxml://storage.googleapis.com:{gcp_bucket}?secret={secret_mark}", None),
])
@pytest.mark.storage
@pytest.mark.authentication
@REAL_GCP_TESTS_MARK
def test_arcticdb_gcpxml_uri(uri:str, expected: str):
    execute_uri_test(uri, expected, gcp_access_key, gcp_secret_key)


@pytest.mark.storage
@pytest.mark.authentication
def test_arcticdb_s3_config_file():
    pass

