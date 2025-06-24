"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import re
import time
from multiprocessing import Queue, Process
from difflib import unified_diff
from collections import defaultdict

import pytest
import pandas as pd
import sys

from arcticdb.util.utils import delete_nvs
from arcticdb_ext.exceptions import StorageException
from arcticdb_ext.storage import NoDataFoundException

from arcticdb_ext import set_config_string
from arcticdb_ext.storage import KeyType
from arcticdb.util.test import create_df, assert_frame_equal

from arcticdb.storage_fixtures.s3 import MotoNfsBackedS3StorageFixtureFactory, list_moto_storage
from arcticdb.storage_fixtures.s3 import MotoS3StorageFixtureFactory

import arcticdb.toolbox.query_stats as qs

from arcticdb.util.test import config_context, config_context_string

pytestmark = pytest.mark.skipif(
    sys.version_info.major == 3 and sys.version_info.minor == 6 and sys.platform == "linux",
    reason="Test setup segfaults",
)


def test_s3_storage_failures(mock_s3_store_with_error_simulation):
    lib = mock_s3_store_with_error_simulation
    try:
        symbol_success = "symbol"
        symbol_fail_write = "symbol#Failure_Write_99_0"
        symbol_fail_read = "symbol#Failure_Read_17_0"
        df = pd.DataFrame({"a": list(range(100))}, index=list(range(100)))

        lib.write(symbol_success, df)
        result_df = lib.read(symbol_success).data
        assert_frame_equal(result_df, df)

        with pytest.raises(StorageException, match="Network error: S3Error#99"):
            lib.write(symbol_fail_write, df)

        with pytest.raises(StorageException, match="Unexpected error: S3Error#17"):
            lib.read(symbol_fail_read)
    finally:
        delete_nvs(lib)


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
    try:
        lib_tool = lib.library_tool()
        # We use the AWS_EC2_METADATA_DISABLED variable to verify we're disabling the EC2 Metadata check when outside of AWS
        # For some reason os.getenv can't access environment variables from the cpp layer so we use lib_tool.inspect_env_variable
        if run_on_aws:
            assert lib_tool.inspect_env_variable("AWS_EC2_METADATA_DISABLED") == None
        else:
            assert lib_tool.inspect_env_variable("AWS_EC2_METADATA_DISABLED") == "true"
    finally:
        delete_nvs(lib)


def test_nfs_backed_s3_storage(lib_name, nfs_clean_bucket):
    # Given
    lib = nfs_clean_bucket.create_version_store_factory(lib_name)()
    
    try:
        # When
        lib.write("s", data=create_df())

        # Then - should be written in "bucketized" structure
        bucket = nfs_clean_bucket.get_boto_bucket()
        objects = bucket.objects.all()

        # Expect one or two repetitions of 3 digit "buckets" in the object names
        bucketized_pattern = r".*/(sl|tdata|tindex|ver|vref)/([0-9]{1,3}/){1,2}.*"

        for o in objects:
            assert re.match(bucketized_pattern, o.key), f"Object {o.key} does not match pattern {bucketized_pattern}"
    finally:
        delete_nvs(lib)


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
    try:
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
    finally:
        delete_nvs(lib)


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

    try:
        # When
        expected = create_df()
        lib.write("s", data=expected)

        # Then - should be readable
        res = lib.read("s").data
        pd.testing.assert_frame_equal(res, expected)
    finally:
        delete_nvs(lib)


@pytest.fixture(scope="function")
def wrapped_s3_storage_bucket(wrapped_s3_storage_factory):
    with wrapped_s3_storage_factory.create_fixture() as bucket:
        yield bucket


def test_wrapped_s3_storage(lib_name, wrapped_s3_storage_bucket):
    lib = wrapped_s3_storage_bucket.create_version_store_factory(lib_name)()

    try:
        lib.write("s", data=create_df())
        test_bucket_name = wrapped_s3_storage_bucket.bucket

        with config_context("S3ClientTestWrapper.EnableFailures", 1):
            # Depending on the reload interval, the error might be different
            # Setting the reload interval to 0 will make the error be "StorageException"
            with config_context("VersionMap.ReloadInterval", 0):
                with pytest.raises(StorageException, match="Network error: S3Error#99"):
                    lib.read("s")

            with config_context_string("S3ClientTestWrapper.FailureBucket", test_bucket_name):
                with pytest.raises(StorageException, match="Network error: S3Error#99"):
                    lib.write("s", data=create_df())

            with config_context_string("S3ClientTestWrapper.FailureBucket", f"{test_bucket_name},non_existent_bucket"):
                with pytest.raises(StorageException, match="Network error: S3Error#99"):
                    lib.write("s", data=create_df())

            # There should be no failures
            # given that we should not be simulating any failures for the test bucket
            with config_context_string("S3ClientTestWrapper.FailureBucket", "non_existent_bucket"):
                lib.read("s")
                lib.write("s", data=create_df())

        # There should be no problems after the failure simulation has been turned off
        lib.read("s")
        lib.write("s", data=create_df())
    finally:
        delete_nvs(lib)
        

@pytest.fixture(scope="session")
def test_prefix():
    return "test_bucket_prefix"


@pytest.fixture(scope="function", params=[MotoNfsBackedS3StorageFixtureFactory, MotoS3StorageFixtureFactory])
def storage_bucket(test_prefix, request):
    with request.param(
        use_ssl=False, ssl_test_support=False, bucket_versioning=False, default_prefix=test_prefix
    ) as factory:
        with factory.create_fixture() as bucket:
            yield bucket


def test_library_get_key_path(lib_name, storage_bucket, test_prefix):
    lib = storage_bucket.create_version_store_factory(lib_name)()

    try:
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
    finally:
        delete_nvs(lib)


def sum_operations(stats):
    """Sum up all operations from query stats.

    Args:
        stats: Dictionary containing query stats

    Returns:
        Dictionary with total counts, sizes, and times for each operation type
    """
    totals = {}

    for op_type, key_types in stats["storage_operations"].items():
        totals[op_type] = {"count": 0, "size_bytes": 0, "total_time_ms": 0}

        for key_type, metrics in key_types.items():
            totals[op_type]["count"] += metrics["count"]
            totals[op_type]["size_bytes"] += metrics["size_bytes"]
            totals[op_type]["total_time_ms"] += metrics["total_time_ms"]

    return totals


def sum_all_operations(stats):
    totals = sum_operations(stats)
    total_count = 0
    for op_type, metrics in totals.items():
        total_count += metrics["count"]
    return total_count


def visualize_stats_diff(stats1, stats2):
    """Visualize count differences between two stats dictionaries in a table format.

    Args:
        stats1: First stats dictionary
        stats2: Second stats dictionary

    Returns:
        String containing formatted count differences in a table
    """

    def get_counts(stats):
        counts = defaultdict(lambda: defaultdict(int))
        for op_type, key_types in stats.get("storage_operations", {}).items():
            for key_type, metrics in key_types.items():
                counts[op_type][key_type] = metrics.get("count", 0)
        return counts

    counts1 = get_counts(stats1)
    counts2 = get_counts(stats2)

    # Get all unique operations and key types
    all_ops = sorted(set(counts1.keys()) | set(counts2.keys()))
    all_key_types = set()
    for op in all_ops:
        all_key_types.update(counts1[op].keys())
        all_key_types.update(counts2[op].keys())
    all_key_types = sorted(all_key_types)

    # Build the table
    output = []
    output.append("Count Differences:")
    output.append("=" * 80)

    # Header
    header = "Operation".ljust(30) + "Key Type".ljust(20) + "Before".rjust(10) + "After".rjust(10) + "Diff".rjust(10)
    output.append(header)
    output.append("-" * 80)

    # Table rows
    for op in all_ops:
        for key_type in all_key_types:
            count1 = counts1[op][key_type]
            count2 = counts2[op][key_type]
            if count1 != count2:
                diff = count2 - count1
                diff_str = f"{diff:+d}" if diff != 0 else "0"
                row = f"{op[:28]:<30} {key_type[:18]:<20} {count1:>10} {count2:>10} {diff_str:>10}"
                output.append(row)

    if len(output) == 3:  # Only header, separator, and header row
        return "No count differences found"

    # Add summary
    output.append("-" * 80)
    total1 = sum(sum(counts.values()) for counts in counts1.values())
    total2 = sum(sum(counts.values()) for counts in counts2.values())
    total_diff = total2 - total1
    diff_str = f"{total_diff:+d}" if total_diff != 0 else "0"
    summary = f"Total:".ljust(50) + f"{total1:>10} {total2:>10} {diff_str:>10}"
    output.append(summary)

    return "\n".join(output)


def test_delete_over_time(lib_name, storage_bucket, clear_query_stats):
    qs.enable()
    expected_ops = 14
    lib = storage_bucket.create_version_store_factory(lib_name)()

    try:
        with config_context("VersionMap.ReloadInterval", 0):
            # Setup
            # First write and delete will add an extra couple of version keys
            lib.write("s", data=create_df())
            qs.reset_stats()
            lib.delete("s")

            assert sum_all_operations(qs.get_query_stats()) == expected_ops
            lib.write("s", data=create_df())
            qs.reset_stats()

            lib.delete("s")
            base_stats = qs.get_query_stats()
            base_ops_count = sum_all_operations(base_stats)
            # expected_ops + 2 (read the new version and the tombstone all key)
            assert base_ops_count == (expected_ops + 2)
            qs.reset_stats()

            iters = 10

            # make sure that the delete makes a constant number of operations
            for i in range(iters):
                lib.write("s", data=create_df())
                qs.reset_stats()

                lib.delete("s")
                stats = qs.get_query_stats()
                qs.reset_stats()
                assert sum_all_operations(stats) == base_ops_count == (expected_ops + 2), visualize_stats_diff(
                    base_stats, stats
                )
    finally:
        delete_nvs(lib)


def test_write_and_prune_previous_over_time(lib_name, storage_bucket, clear_query_stats):
    expected_ops = 9
    with config_context("VersionMap.ReloadInterval", 0):
        try:
            lib = storage_bucket.create_version_store_factory(lib_name)()
            qs.enable()
            lib.write("s", data=create_df())
            qs.reset_stats()

            lib.write("s", data=create_df(), prune_previous=True)

            base_stats = qs.get_query_stats()
            base_ops_count = sum_all_operations(base_stats)
            assert base_ops_count == expected_ops
            qs.reset_stats()

            iters = 10

            # make sure that the write and prune makes a constant number of operations
            for i in range(iters):
                lib.write("s", data=create_df(), prune_previous=True)
                stats = qs.get_query_stats()
                qs.reset_stats()
                assert sum_all_operations(stats) == base_ops_count == expected_ops, visualize_stats_diff(base_stats, stats)
        finally:
            delete_nvs(lib)