"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import re
import time
from multiprocessing import Queue, Process

import pytest
import pandas as pd
import sys

from arcticdb_ext.exceptions import StorageException

from arcticdb_ext import set_config_string
from arcticdb_ext.storage import KeyType
from arcticdb.util.test import create_df, assert_frame_equal

from arcticdb.storage_fixtures.s3 import MotoNfsBackedS3StorageFixtureFactory
from arcticdb.storage_fixtures.s3 import MotoS3StorageFixtureFactory

from arcticdb.util.test import config_context, config_context_string
from tests.util.mark import SKIP_CONDA_MARK

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
