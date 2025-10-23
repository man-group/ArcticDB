"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import random
import time
import os
import pytest

from multiprocessing import Value
from threading import Thread
from arcticdb_ext import set_config_int
from arcticdb import log

from arcticdb.config import set_log_level
from arcticdb_ext.storage import KeyType, NoDataFoundException

from tests.util.mark import SLOW_TESTS_MARK

# set_log_level("DEBUG")


def write_data(lib, sym, done, error, interval, number_of_writes, number_of_deletes):
    set_config_int("VersionMap.ReloadInterval", interval)
    set_config_int("VersionMap.MaxReadRefTrials", 10)
    delete_version_id = 0
    try:
        for idx1 in range(10):
            print("Iteration {}/10".format(idx1))
            for idx2 in range(20):
                if idx2 % 4 == 3:
                    num_versions_to_delete = random.randint(1, 2)
                    number_of_deletes.value += num_versions_to_delete
                    if num_versions_to_delete == 1:
                        lib.delete_version(sym, delete_version_id)
                    else:
                        lib.delete_versions(sym, [delete_version_id, delete_version_id + 1])
                    print("Doing delete {}/{}".format(idx1, idx2))
                    delete_version_id += num_versions_to_delete
                else:
                    number_of_writes.value += 1
                    print("Doing write {}/{}".format(idx1, idx2))
                    lib.write(sym, idx2)
            vs = set([v["version"] for v in lib.list_versions(sym)])
            assert len(vs) == number_of_writes.value - delete_version_id
            for vid in vs:
                assert lib.has_symbol(sym, vid)
            for d_id in range(delete_version_id):
                assert d_id not in vs

    except Exception as e:
        log.version.error(f"Error in writer: {e}")
        error.value = 1

    print("Setting done")
    done.value = 1


def compact_data(lib, sym, done, error):
    set_config_int("VersionMap.MaxVersionBlocks", 1)
    while not done.value:
        lib.version_store._compact_version_map(sym)
        time.sleep(random.uniform(0, 0.05))


def read_data(lib, sym, done, error):
    while not done.value:
        vs = lib.list_versions(sym)
        for idx in range(len(vs) - 1):
            assert vs[idx]["version"] == vs[idx + 1]["version"] + 1


@SLOW_TESTS_MARK
@pytest.mark.skipif(
    os.environ.get("ARCTICDB_CODE_COVERAGE_BUILD", "0") == "1",
    reason=(
        "When we build for code coverage, we make a DEBUG binary, which is much slower and causes this test to take"
        " around ~4 hours which is breaking the build"
    ),
)
@pytest.mark.parametrize("interval", [1, 10_000_000_000_000])
@pytest.mark.skipif(True, reason="Test without this")
def test_stress_version_map_compact(object_version_store, sym, interval):
    done = Value("b", 0)
    error = Value("b", 0)
    number_of_writes = Value("i", 0)
    number_of_deletes = Value("i", 0)
    lib = object_version_store
    lib.version_store._set_validate_version_map()
    try:
        log.version.warn("Starting writer")
        writer = Thread(
            name="writer",
            target=write_data,
            args=(lib, sym, done, error, interval, number_of_writes, number_of_deletes),
        )
        writer.start()
        log.version.info("Starting compacter")
        compacter = Thread(name="compacter", target=compact_data, args=(lib, sym, done, error))
        compacter.start()
        log.version.info("Starting reader")
        reader = Thread(name="reader", target=read_data, args=(lib, sym, done, error))
        reader.start()

        log.version.info("Joining writer")
        writer.join()
        log.version.info("Joining compacter")
        compacter.join()
        log.version.info("Joining reader")
        reader.join()
        assert error.value == 0
        log.version.info("Done")
        writes = number_of_writes.value
        deletes = number_of_deletes.value

        # Check that the version map is compacted correctly
        # and all the keys are present
        lib_tool = lib.library_tool()
        version_keys = lib_tool.find_keys_for_id(KeyType.VERSION, sym)
        keys_in_chain = []
        for k in version_keys:
            keys_in_chain.extend(lib_tool.read_to_keys(k))

        index_keys = [k for k in keys_in_chain if k.type == KeyType.TABLE_INDEX]
        tombstone_keys = [k for k in keys_in_chain if k.type == KeyType.TOMBSTONE]
        assert len(index_keys) == writes
        assert len(tombstone_keys) == deletes
    finally:
        log.version.info("Clearing library")
        lib.version_store.clear()
        log.version.info("Finished")
