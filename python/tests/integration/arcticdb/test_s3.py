"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from operator import add
import re
import time
from multiprocessing import Queue, Process

from urllib.parse import urlparse

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from arcticdb import Arctic

import pytest
import pandas as pd
import sys
import arcticdb as adb

from arcticdb_ext.exceptions import StorageException

from arcticdb_ext import set_config_string
from arcticdb_ext.storage import KeyType
from arcticdb.util.test import create_df, assert_frame_equal

from arcticdb.storage_fixtures.s3 import MotoNfsBackedS3StorageFixtureFactory
from arcticdb.storage_fixtures.s3 import MotoS3StorageFixtureFactory, Key

from arcticdb.util.test import config_context, config_context_string
from arcticdb_ext.storage import AWSAuthMethod, NativeVariantStorage, S3Settings as NativeS3Settings
from tests.util.mark import SKIP_CONDA_MARK, REAL_S3_TESTS_MARK
from tests.util.storage_test import real_s3_credentials

pytestmark = pytest.mark.skipif(
    sys.version_info.major == 3 and sys.version_info.minor == 6 and sys.platform == "linux",
    reason="Test setup segfaults",
)


@pytest.mark.parametrize("version_store", ["wrapped_s3_version_store", "mock_s3_store_with_error_simulation"])
def test_s3_storage_failures(request, version_store):
    lib = request.getfixturevalue(version_store)
    symbol_success = "symbol"
    symbol_fail_write = "symbol#Failure_Write_99_0"
    symbol_fail_read = "symbol#Failure_Read_17_0"
    df = pd.DataFrame({"a": list(range(100))}, index=list(range(100)))

    lib.write(symbol_success, df)
    result_df = lib.read(symbol_success).data
    assert_frame_equal(result_df, df)

    with pytest.raises(StorageException, match="Network error: S3Error:99"):
        lib.write(symbol_fail_write, df)

    with pytest.raises(StorageException, match="Unexpected error: S3Error:17"):
        lib.read(symbol_fail_read)


# TODO: To make this test run alongside other tests we'll need to:
# - Figure out how to do AWS::InitAPI multiple times in the same process. Currently we use std::call_once to ensure we
#   we do this exactly once.
# - Perform cleanup after tests (unset AWS_EC2_METADATA_DISABLED after done with each test, unset the runtime config
#   "EC2.TestIMDSEndpointOverride" to make follow up tests work as expected
@pytest.mark.skip(reason="Test only works if not run along other tests.")
@pytest.mark.parametrize("run_on_aws", [True, False])
def test_s3_running_on_aws_fast_check(lib_name, s3_storage_factory, run_on_aws):
    if run_on_aws:
        # To mock running on aws we override the IMDS endpoint with moto's endpoint which will be reachable.
        set_config_string("EC2.TestIMDSEndpointOverride", s3_storage_factory.endpoint)

    lib = s3_storage_factory.create_fixture().create_version_store_factory(lib_name)()
    lib_tool = lib.library_tool()
    # We use the AWS_EC2_METADATA_DISABLED variable to verify we're disabling the EC2 Metadata check when outside of AWS
    # For some reason os.getenv can't access environment variables from the cpp layer so we use lib_tool.inspect_env_variable
    if run_on_aws:
        assert lib_tool.inspect_env_variable("AWS_EC2_METADATA_DISABLED") == None
    else:
        assert lib_tool.inspect_env_variable("AWS_EC2_METADATA_DISABLED") == "true"


def test_nfs_backed_s3_storage(lib_name, nfs_clean_bucket):
    # Given
    lib = nfs_clean_bucket.create_version_store_factory(lib_name)()

    # When
    lib.write("s", data=create_df())

    # Then - should be written in "bucketized" structure
    bucket = nfs_clean_bucket.get_boto_bucket()
    objects = bucket.objects.all()

    # Expect one or two repetitions of 3 digit "buckets" in the object names
    bucketized_pattern = r".*/(sl|tdata|tindex|ver|vref)/([0-9]{1,3}/){1,2}.*"

    for o in objects:
        assert re.match(bucketized_pattern, o.key), f"Object {o.key} does not match pattern {bucketized_pattern}"


def read_repeatedly(version_store, queue: Queue):
    while True:
        try:
            version_store.list_versions("tst")
        except Exception as e:
            queue.put(e)
            raise
        time.sleep(0.1)


def write_repeatedly(version_store):
    while True:
        for i in range(10):
            version_store.snapshot(f"snap-{i}")
        for i in range(10):
            version_store.delete_snapshot(f"snap-{i}")
        time.sleep(0.1)


def test_racing_list_and_delete_nfs(nfs_backed_s3_storage, lib_name):
    """This test is for a regression with NFS where iterating snapshots raced with
    deleting them, due to a bug in our logic to suppress the KeyNotFoundException."""
    lib = nfs_backed_s3_storage.create_version_store_factory(lib_name)()
    lib.write("tst", [1, 2, 3])

    exceptions_in_reader = Queue()
    reader = Process(target=read_repeatedly, args=(lib, exceptions_in_reader))
    writer = Process(target=write_repeatedly, args=(lib,))

    try:
        reader.start()
        writer.start()

        # Run test for 2 seconds - this was enough for this regression test to reliably fail
        # 10 times in a row.
        reader.join(2)
        writer.join(0.001)
    finally:
        writer.terminate()
        reader.terminate()

    assert exceptions_in_reader.empty()


@pytest.fixture(scope="session", params=[MotoNfsBackedS3StorageFixtureFactory, MotoS3StorageFixtureFactory])
def s3_storage_dots_in_path(request):
    prefix = "some_path/.thing_with_a_dot/even.more.dots/end"

    with request.param(
        use_ssl=False, ssl_test_support=False, bucket_versioning=False, default_prefix=prefix, use_raw_prefix=True
    ) as f:
        with f.create_fixture() as g:
            yield g


def test_read_path_with_dot(lib_name, s3_storage_dots_in_path):
    # Given
    factory = s3_storage_dots_in_path.create_version_store_factory(lib_name)
    lib = factory()

    # When
    expected = create_df()
    lib.write("s", data=expected)

    # Then - should be readable
    res = lib.read("s").data
    pd.testing.assert_frame_equal(res, expected)


@pytest.fixture(scope="function")
def wrapped_s3_storage_bucket(wrapped_s3_storage_factory):
    with wrapped_s3_storage_factory.create_fixture() as bucket:
        yield bucket


def test_wrapped_s3_storage(lib_name, wrapped_s3_storage_bucket):
    lib = wrapped_s3_storage_bucket.create_version_store_factory(lib_name)()
    lib.write("s", data=create_df())
    test_bucket_name = wrapped_s3_storage_bucket.bucket

    with config_context("S3ClientTestWrapper.EnableFailures", 1):
        # Depending on the reload interval, the error might be different
        # Setting the reload interval to 0 will make the error be "StorageException"
        with config_context("VersionMap.ReloadInterval", 0):
            with pytest.raises(StorageException, match="Network error: S3Error:99"):
                lib.read("s")

        with config_context_string("S3ClientTestWrapper.FailureBucket", test_bucket_name):
            with pytest.raises(StorageException, match="Network error: S3Error:99"):
                lib.write("s", data=create_df())

        with config_context_string("S3ClientTestWrapper.FailureBucket", f"{test_bucket_name},non_existent_bucket"):
            with pytest.raises(StorageException, match="Network error: S3Error:99"):
                lib.write("s", data=create_df())

        # There should be no failures
        # given that we should not be simulating any failures for the test bucket
        with config_context_string("S3ClientTestWrapper.FailureBucket", "non_existent_bucket"):
            lib.read("s")
            lib.write("s", data=create_df())

    # There should be no problems after the failure simulation has been turned off
    lib.read("s")
    lib.write("s", data=create_df())


def test_library_get_key_path(lib_name, s3_and_nfs_storage_bucket, test_prefix):
    lib = s3_and_nfs_storage_bucket.create_version_store_factory(lib_name)()
    lib.write("s", data=create_df())
    lib_tool = lib.library_tool()

    keys_count = 0
    for key_type in KeyType.__members__.values():
        keys = lib_tool.find_keys(key_type)
        keys_count += len(keys)
        for key in keys:
            path = lib_tool.get_key_path(key)
            assert path != ""
            assert path.startswith(test_prefix)

    assert keys_count > 0


def test_custom_credentials_provider_chain(lib_name, monkeypatch):
    """Test that the _RBAC_ credentials path (DEFAULT_CREDENTIALS_PROVIDER_CHAIN) uses our
    custom MyAWSCredentialsProviderChain which excludes the problematic CRT-based
    STSAssumeRoleWebIdentityCredentialsProvider (see aws-sdk-cpp PR #3505, issues #3531, #3558).

    This test verifies that:
    1. S3Storage construction with _RBAC_ credentials doesn't hang or crash
    2. Basic read/write operations work through the custom chain against moto
    """
    # Set AWS env vars so that the EnvironmentAWSCredentialsProvider (the first provider
    # in our custom chain) picks up valid credentials for moto. This still exercises the
    # _RBAC_ → MyAWSCredentialsProviderChain code path in C++.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "awd")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "awd")

    native_config = NativeVariantStorage(NativeS3Settings(AWSAuthMethod.DEFAULT_CREDENTIALS_PROVIDER_CHAIN, "", False))
    with MotoS3StorageFixtureFactory(
        use_ssl=False,
        ssl_test_support=False,
        bucket_versioning=False,
        native_config=native_config,
    ) as factory:
        with factory.create_fixture(key=Key(id="_RBAC_", secret="_RBAC_", user_name="rbac_test")) as bucket:
            start = time.monotonic()
            lib = bucket.create_version_store_factory(lib_name)()
            df = pd.DataFrame({"a": [1, 2, 3]})
            lib.write("test_symbol", df)
            result = lib.read("test_symbol").data
            elapsed = time.monotonic() - start
            assert_frame_equal(result, df)
            # The CRT-based STS provider hang takes 10+ seconds. If library creation
            # and a round-trip write/read complete well under that, the custom chain
            # is working correctly and not hitting the problematic CRT path.
            assert elapsed < 10, f"Custom credentials provider chain took {elapsed:.1f}s (expected < 10s)"


@REAL_S3_TESTS_MARK
@pytest.mark.storage
@pytest.mark.authentication
def test_custom_credentials_provider_chain_real_s3(tmp_path, lib_name, monkeypatch):
    """Test the custom MyAWSCredentialsProviderChain against real S3.

    Uses aws_auth=true in the URI which triggers the _RBAC_ → custom chain code path.
    Credentials are supplied via AWS_SHARED_CREDENTIALS_FILE so that the
    ProfileConfigFileAWSCredentialsProvider in our custom chain picks them up.
    """
    s3_endpoint, s3_bucket, s3_region, s3_access_key, s3_secret_key, _, _ = real_s3_credentials(shared_path=False)

    # Write credentials to a temp file for the ProfileConfigFileAWSCredentialsProvider
    creds_file = tmp_path / "credentials"
    creds_file.write_text(f"[default]\naws_access_key_id = {s3_access_key}\naws_secret_access_key = {s3_secret_key}\n")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(creds_file))

    parsed_host = urlparse(s3_endpoint).hostname if s3_endpoint else None
    if parsed_host and parsed_host.endswith(".amazonaws.com"):
        host = f"s3.{s3_region}.amazonaws.com"
    else:
        host = parsed_host or ""

    uri = f"s3://{host}:{s3_bucket}?aws_auth=true&path_prefix=test_custom_chain_{lib_name}"

    ac = adb.Arctic(uri)
    try:
        lib = ac.create_library(lib_name)
        df = pd.DataFrame({"a": [1, 2, 3]})
        lib.write("test_symbol", df)
        result = lib.read("test_symbol").data
        assert_frame_equal(result, df)
    finally:
        ac.delete_library(lib_name)


def _delete_symbol_in_new_connection(args):
    """Worker function that creates a fresh Arctic connection and deletes a symbol."""
    uri, lib_name, symbol = args
    ac = Arctic(uri)
    lib = ac.get_library(lib_name)
    lib.delete(symbol)


@REAL_S3_TESTS_MARK
def test_process_pool_executor_delete_fails(real_s3_sts_storage, lib_name):
    ac = Arctic(real_s3_sts_storage.arctic_uri)
    lib = ac.create_library(lib_name)
    symbols = [f"test_sym_{i}" for i in range(4)]
    for sym in symbols:
        lib.write(sym, pd.DataFrame({"a": [1, 2, 3]}))

    with ProcessPoolExecutor(max_workers=len(symbols)) as executor:
        futures = [
            executor.submit(
                _delete_symbol_in_new_connection,
                (real_s3_sts_storage.arctic_uri, lib_name, sym),
            )
            for sym in symbols
        ]
        # Expect failure due to fork-unsafe AWS SDK
        for future in futures:
            with pytest.raises(Exception):
                future.result(timeout=30)


@REAL_S3_TESTS_MARK
def test_thread_pool_executor_delete_succeeds(real_s3_sts_storage, lib_name):
    ac = Arctic(real_s3_sts_storage.arctic_uri)
    lib = ac.create_library(lib_name)
    symbols = [f"test_sym_{i}" for i in range(4)]
    for sym in symbols:
        lib.write(sym, pd.DataFrame({"a": [1, 2, 3]}))

    def delete_from_lib(symbol):
        lib.delete(symbol)

    with ThreadPoolExecutor(max_workers=len(symbols)) as executor:
        futures = [executor.submit(delete_from_lib, sym) for sym in symbols]
        for future in futures:
            future.result(timeout=30)

    remaining = lib.list_symbols()
    for sym in symbols:
        assert sym not in remaining
