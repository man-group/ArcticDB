"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import logging
import pytest
import requests
import pandas as pd
from contextlib import contextmanager

from arcticdb_ext.exceptions import PermissionException
from arcticdb.arctic import Arctic
from arcticdb.util.test import assert_frame_equal
from ...util.mark import REAL_S3_TESTS_MARK, ARCTICDB_USING_CONDA

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _uri_with_profile(s3_storage, profile):
    # Reuse the fixture's URI (host, port, bucket, prefix, ...) and swap key auth for default-chain + profile auth
    # s3s://HOST:PORT?access=*&secret=*&port=12126&ssl=True&CA_cert_path=*
    key_auth = f"access={s3_storage.key.id}&secret={s3_storage.key.secret}"
    assert key_auth in s3_storage.arctic_uri, s3_storage.arctic_uri
    return s3_storage.arctic_uri.replace(key_auth, f"aws_auth=default&aws_profile={profile}")


@contextmanager
def _enforce_s3_access_key(endpoint, access_key):
    requests.post(endpoint + "/enforce_access_key", access_key.encode("ascii"), verify=False).raise_for_status()
    try:
        yield
    finally:
        requests.post(endpoint + "/enforce_access_key", b"", verify=False).raise_for_status()


def _assert_profile_selects_credentials(s3_storage, lib_name, profile_a, key_a, profile_b):
    # moto rejects any request not signed with key_a, so profile_a (which resolves to key_a) is accepted and
    # profile_b (a different key) is rejected with 403 -- proving the named profile's credentials are the ones used.
    with _enforce_s3_access_key(s3_storage.factory.endpoint, key_a):
        ac = Arctic(_uri_with_profile(s3_storage, profile_a))
        lib = ac.create_library(lib_name)
        df = pd.DataFrame({"a": [1, 2, 3]})
        lib.write("sym", df)
        assert_frame_equal(lib.read("sym").data, df)

        with pytest.raises(PermissionException):
            ac_wrong = Arctic(_uri_with_profile(s3_storage, profile_b))
            ac_wrong.create_library(lib_name + "_wrong")


@pytest.mark.skipif(
    ARCTICDB_USING_CONDA,
    reason="Reading profile credentials from the AWS config file needs aws-sdk-cpp >= 1.11.748 (CRT profile "
    "provider, like boto3); conda-forge currently ships <= 1.11.747, whose provider reads only the credentials file. "
    "test_s3_default_auth_with_profile_credentials_file covers the credentials-file path on every version.",
)
@pytest.mark.storage
@pytest.mark.authentication
def test_s3_default_auth_with_profile(s3_storage, lib_name, tmp_path, monkeypatch, route_env_to_extension):
    profile_a, key_a = "arctic_profile_a", "arctic_profile_a_key"
    profile_b, key_b = "arctic_profile_b", "arctic_profile_b_key"
    config_file = tmp_path / "aws_config"
    config_file.write_text(
        f"[profile {profile_a}]\n"
        f"aws_access_key_id = {key_a}\n"
        f"aws_secret_access_key = arctic_profile_a_secret\n\n"
        f"[profile {profile_b}]\n"
        f"aws_access_key_id = {key_b}\n"
        f"aws_secret_access_key = arctic_profile_b_secret\n"
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_file))
    _assert_profile_selects_credentials(s3_storage, lib_name, profile_a, key_a, profile_b)


@pytest.mark.storage
@pytest.mark.authentication
def test_s3_default_auth_with_profile_credentials_file(
    s3_storage, lib_name, tmp_path, monkeypatch, route_env_to_extension
):
    """aws_profile resolving from the AWS *credentials* file (`[X]`). Every supported aws-sdk-cpp version reads
    profile credentials from here (unlike the config file, which needs >= 1.11.748), so this also covers conda."""
    profile_a, key_a = "arctic_profile_a", "arctic_profile_a_key"
    profile_b, key_b = "arctic_profile_b", "arctic_profile_b_key"
    credentials_file = tmp_path / "aws_credentials"
    credentials_file.write_text(
        f"[{profile_a}]\n"
        f"aws_access_key_id = {key_a}\n"
        f"aws_secret_access_key = arctic_profile_a_secret\n\n"
        f"[{profile_b}]\n"
        f"aws_access_key_id = {key_b}\n"
        f"aws_secret_access_key = arctic_profile_b_secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_file))
    _assert_profile_selects_credentials(s3_storage, lib_name, profile_a, key_a, profile_b)


@REAL_S3_TESTS_MARK
@pytest.mark.storage
@pytest.mark.authentication
@pytest.mark.parametrize(
    "storage_fixture",
    ["real_s3_default_profile_storage", "real_s3_default_profile_credentials_storage"],
)
def test_s3_default_auth_profile_real(lib_name, storage_fixture, request):
    storage = request.getfixturevalue(storage_fixture)
    ac = Arctic(storage.arctic_uri)
    lib = ac.create_library(lib_name)
    df = pd.DataFrame({"a": [1, 2, 3]})
    lib.write("sym", df)
    assert_frame_equal(lib.read("sym").data, df)


@REAL_S3_TESTS_MARK
@pytest.mark.storage
@pytest.mark.authentication
def test_s3_default_auth_wrong_profile_real(lib_name, real_s3_default_profile_storage):
    # A non-existent profile resolves no credentials (env/IMDS fallback is disabled in the fixture), so the request
    # is unauthenticated and S3 rejects it
    storage = real_s3_default_profile_storage
    bad_uri = storage.arctic_uri.replace(
        f"aws_profile={storage.factory.aws_profile}", "aws_profile=nonexistent_profile_xyz"
    )
    with pytest.raises(PermissionException):
        ac = Arctic(bad_uri)
        ac.create_library(lib_name + "_wrong")
