"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import shutil

from arcticdb import Arctic
from arcticdb.exceptions import NoSuchVersionException
from arcticdb.config import set_log_level
import arcticdb as adb
import datetime
import math
import sys
import time

from benchmarks.common import *


number = 13
timeout = 6000
rounds = 1
CONNECTION_STRING = "lmdb:///tmp/version_chain"
CONNECTION_STRING_SOME_DELETED = "lmdb:///tmp/version_chain_deleted"
LIB_NAME = "lib"
DELETION_POINT = 0.99

# TODO: Investigate why setup is taking ~50mins with 50k versions on ec2 runners.
# Locally it looks like it shouldn't take more than 15.
#params = ([25_000], ["forever", "default", "never"], [0.0, 0.99])
params = ([1001], ["forever", "default", "never"], [True, False])
param_names = ["num_versions", "caching", "deleted"]

def symbol_name(num_versions):
    return f"symbol_{num_versions}"

def setup_cache():
    start = time.time()
    _setup_cache()
    print(f"SETUP_CACHE TIME: {time.time() - start}", file=sys.stderr)

def _setup_cache():
    ac = Arctic(CONNECTION_STRING)

    num_versions_list, caching_list, deleted_list = params

    ac.delete_library(LIB_NAME)
    lib = ac.create_library(LIB_NAME)

    small_df = generate_random_floats_dataframe(2, 2)

    delete_points = {}
    for num_versions in num_versions_list:
        delete_points[num_versions] = math.floor(DELETION_POINT * num_versions)

    # To save setup time we populate the two libraries like:
    #
    # Step 1 - write 99% of the versions to one library
    # Step 2 - copy the library directory
    # Step 3 - delete on the copy. Write the other versions to both.
    start_time = time.time()
    adb._ext.set_config_int("VersionMap.ReloadInterval", sys.maxsize)
    # Batch operations by symbol to reduce overhead
    for num_versions in num_versions_list:
        symbol = symbol_name(num_versions)
        deletion_point = delete_points[num_versions]
        for i in range(deletion_point):
            lib.write(symbol, small_df)

    del lib
    del ac
    shutil.rmtree("/tmp/version_chain_deleted", ignore_errors=True)
    shutil.copytree("/tmp/version_chain", "/tmp/version_chain_deleted")

    ac = Arctic(CONNECTION_STRING)
    lib = ac[LIB_NAME]

    for num_versions in num_versions_list:
        symbol = symbol_name(num_versions)
        deletion_point = delete_points[num_versions]
        for i in range(deletion_point, num_versions):
            lib.write(symbol, small_df)
        # reasonableness check
        assert lib.read(symbol).version == num_versions - 1

    del lib
    del ac

    ac = Arctic(CONNECTION_STRING_SOME_DELETED)
    lib = ac[LIB_NAME]
    for num_versions in num_versions_list:
        symbol = symbol_name(num_versions)
        lib.delete(symbol)
        deletion_point = delete_points[num_versions]
        for i in range(deletion_point, num_versions):
            lib.write(symbol, small_df)
        # reasonableness checks
        assert lib.read(symbol).version == num_versions - 1
        # Only versions that have not been deleted are returned
        assert len(lib.list_versions(symbol)) == num_versions - deletion_point

    adb._ext.unset_config_int("VersionMap.ReloadInterval")

    print("IterateVersionChain: Setup cache took (s) :", time.time() - start_time, file=sys.stderr)


if __name__ == "__main__":
    setup_cache()