"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import logging
import pytest
import requests
import os
import pandas as pd
from typing import Generator
from contextlib import contextmanager
from tempfile import mkdtemp
import random
from datetime import datetime

from arcticdb_ext.exceptions import PermissionException
from arcticdb.arctic import Arctic
from arcticdb.util.test import assert_frame_equal
from arcticdb.storage_fixtures.s3 import (
    MotoS3StorageFixtureFactory,
    BaseS3StorageFixtureFactory,
    S3Bucket,
    real_s3_from_environment_variables,
)
from arcticdb.storage_fixtures.utils import safer_rmtree
from arcticdb_ext.storage import NativeVariantStorage, AWSAuthMethod, S3Settings as NativeS3Settings
from ...util.mark import REAL_S3_TESTS_MARK, ARCTICDB_USING_CONDA

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture(scope="session")
def s3_storage_factory_default_auth() -> Generator[MotoS3StorageFixtureFactory, None, None]:
    with MotoS3StorageFixtureFactory(use_ssl=False, ssl_test_support=False, bucket_versioning=False) as f:
        f.aws_auth = AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN
        yield f


@pytest.fixture(scope="session")
def s3_storage_default_auth(s3_storage_factory_default_auth) -> Generator[S3Bucket, None, None]:
    with s3_storage_factory_default_auth.create_fixture() as f:
        yield f
        # Teardown empties this session bucket via slow_cleanup() -> Arctic(arctic_uri). The per-test
        # AWS_CONFIG_FILE/AWS_SHARED_CREDENTIALS_FILE that make aws_auth=default resolve are function-scoped and gone
        # by now, so the cleanup client would fail. Switch it to moto's admin access/secret so cleanup is deterministic.
        f.factory.aws_auth = AWSAuthMethod.DISABLED
        f.arctic_uri = f.get_arctic_uri()


@contextmanager
def _enforce_s3_access_key(endpoint, access_key):
    requests.post(endpoint + "/enforce_access_key", access_key.encode("ascii"), verify=False).raise_for_status()
    try:
        yield
    finally:
        requests.post(endpoint + "/enforce_access_key", b"", verify=False).raise_for_status()


def _assert_profile_selects_credentials(s3_storage_default_auth, lib_name, profile_a, key_a, profile_b):
    # moto rejects any request not signed with key_a, so profile_a (which resolves to key_a) is accepted and
    # profile_b (a different key) is rejected with 403 -- proving the named profile's credentials are the ones used.
    with _enforce_s3_access_key(s3_storage_default_auth.factory.endpoint, key_a):
        ac = Arctic(s3_storage_default_auth.get_arctic_uri(profile_a))
        lib = ac.create_library(lib_name)
        df = pd.DataFrame({"a": [1, 2, 3]})
        lib.write("sym", df)
        assert_frame_equal(lib.read("sym").data, df)

        with pytest.raises(PermissionException):
            ac_wrong = Arctic(s3_storage_default_auth.get_arctic_uri(profile_b))
            ac_wrong.create_library(lib_name + "_wrong")


@pytest.mark.skipif(
    ARCTICDB_USING_CONDA,
    reason="Reading profile credentials from the AWS config file needs aws-sdk-cpp >= 1.11.748 (CRT profile "
    "provider, like boto3); conda-forge currently ships <= 1.11.747, whose provider reads only the credentials file. "
    "test_s3_default_auth_with_profile_credentials_file covers the credentials-file path on every version.",
)
@pytest.mark.storage
@pytest.mark.authentication
def test_s3_default_auth_with_profile(s3_storage_default_auth, lib_name, tmp_path, monkeypatch, route_env_to_extension):
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
    _assert_profile_selects_credentials(s3_storage_default_auth, lib_name, profile_a, key_a, profile_b)


@pytest.mark.storage
@pytest.mark.authentication
def test_s3_default_auth_with_profile_credentials_file(
    s3_storage_default_auth, lib_name, tmp_path, monkeypatch, route_env_to_extension
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
    _assert_profile_selects_credentials(s3_storage_default_auth, lib_name, profile_a, key_a, profile_b)


def real_s3_default_profile_from_environment_variables(
    profile_name: str, profile_file_path: str, use_credentials_file: bool = False
) -> BaseS3StorageFixtureFactory:
    """Factory for testing aws_profile with the default credentials provider chain (non-STS): writes the real S3
    static credentials under the named profile and configures the fixture to authenticate via aws_auth=default + that
    profile.

    With use_credentials_file=False the keys are written to an AWS *config* file (``[profile X]``); with
    use_credentials_file=True they are written to an AWS *credentials* file (``[X]``). The distinction matters because
    aws-sdk-cpp only reads profile credentials from the config file since 1.11.748 (the CRT provider, like boto3),
    whereas every supported version reads them from the credentials file."""
    additional_suffix = f"{random.randint(0, 999)}_{datetime.utcnow().strftime('%Y-%m-%dT%H_%M_%S_%f')}"
    out = real_s3_from_environment_variables(False, NativeVariantStorage(), additional_suffix)
    if use_credentials_file:
        # Credentials file: profile-less section header, no region (region comes from the native config)
        profile_file_content = (
            f"[{profile_name}]\n"
            f"aws_access_key_id = {out.default_key.id}\n"
            f"aws_secret_access_key = {out.default_key.secret}\n"
        )
    else:
        profile_file_content = (
            f"[profile {profile_name}]\n"
            f"aws_access_key_id = {out.default_key.id}\n"
            f"aws_secret_access_key = {out.default_key.secret}\n"
        )
    profile_dir = os.path.dirname(profile_file_path)
    os.makedirs(profile_dir, exist_ok=True)
    fd = os.open(profile_file_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as profile_file:
        profile_file.write(profile_file_content)

    out.native_config = NativeVariantStorage(
        NativeS3Settings(
            aws_auth=AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN,
            aws_profile=profile_name,
            use_internal_client_wrapper_for_testing=False,
        )
    )
    out.aws_auth = AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN
    out.aws_profile = profile_name
    # Reset to ensure the client can't fall back to default key+secret auth and must resolve the profile
    out.default_key = Key(id="", secret="", user_name="unknown user")
    return out


@pytest.fixture(scope="session")
def real_s3_default_profile_storage_factory(monkeypatch_session) -> Generator[BaseS3StorageFixtureFactory, None, None]:
    profile_name = "default_cred_test_profile"
    working_dir = mkdtemp(suffix="S3DefaultProfileStorageFixtureFactory")
    config_file_path = os.path.join(working_dir, "config")
    try:
        f = real_s3_default_profile_from_environment_variables(
            profile_name=profile_name, profile_file_path=config_file_path
        )
        monkeypatch_session.setenv("AWS_CONFIG_FILE", config_file_path)
        # Stop the default credentials provider chain finding credentials anywhere except the named profile, so a
        # wrong profile deterministically fails to authenticate instead of falling back to env/IMDS credentials
        monkeypatch_session.setenv("AWS_EC2_METADATA_DISABLED", "true")
        yield f
    finally:
        safer_rmtree(None, working_dir)


@pytest.fixture
def real_s3_default_profile_storage(real_s3_default_profile_storage_factory) -> Generator[S3Bucket, None, None]:
    with real_s3_default_profile_storage_factory.create_fixture() as f:
        yield f


@pytest.fixture(scope="session")
def real_s3_default_profile_credentials_storage_factory(
    monkeypatch_session,
) -> Generator[BaseS3StorageFixtureFactory, None, None]:
    # Same as real_s3_default_profile_storage_factory but the profile credentials live in an AWS *credentials* file
    # (AWS_SHARED_CREDENTIALS_FILE), which every supported aws-sdk-cpp version reads (the config file needs >= 1.11.748)
    profile_name = "default_cred_test_profile"
    working_dir = mkdtemp(suffix="S3DefaultProfileCredentialsStorageFixtureFactory")
    credentials_file_path = os.path.join(working_dir, "credentials")
    try:
        f = real_s3_default_profile_from_environment_variables(
            profile_name=profile_name, profile_file_path=credentials_file_path, use_credentials_file=True
        )
        monkeypatch_session.setenv("AWS_SHARED_CREDENTIALS_FILE", credentials_file_path)
        monkeypatch_session.setenv("AWS_EC2_METADATA_DISABLED", "true")
        yield f
    finally:
        safer_rmtree(None, working_dir)


@pytest.fixture
def real_s3_default_profile_credentials_storage(
    real_s3_default_profile_credentials_storage_factory,
) -> Generator[S3Bucket, None, None]:
    with real_s3_default_profile_credentials_storage_factory.create_fixture() as f:
        yield f


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
    bad_uri = real_s3_default_profile_storage.get_arctic_uri("nonexistent_profile_xyz")
    with pytest.raises(PermissionException):
        ac = Arctic(bad_uri)
        ac.create_library(lib_name + "_wrong")
