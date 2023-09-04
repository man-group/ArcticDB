"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import random
import time
import os
import pytest

from multiprocessing import Process, Value

from arcticdb_ext import set_config_int
from arcticdb import log


def write_data(lib, sym, done, error):
    set_config_int("VersionMap.ReloadInterval", 1)
    set_config_int("VersionMap.MaxReadRefTrials", 10)
    delete_version_id = 0
    number_of_writes = 0
    try:
        for idx1 in range(5):
            print("Iteration {}/10".format(idx1))
            for idx2 in range(20):
                if idx2 % 4 == 3:
                    lib.delete_version(sym, delete_version_id)
                    delete_version_id += 1
                else:
                    number_of_writes += 1
                    lib.write(sym, idx2)
            vs = set([v["version"] for v in lib.list_versions(sym)])
            assert len(vs) == number_of_writes - delete_version_id
            for vid in vs:
                assert lib.has_symbol(sym, vid)
            for d_id in range(delete_version_id):
                assert d_id not in vs

    except Exception as e:
        print(e)
        error.value = 1
    done.value = 1


def compact_data(lib, sym, done, error):
    set_config_int("VersionMap.MaxVersionBlocks", 1)
    while not done.value or error.value:
        lib.version_store._compact_version_map(sym)
        time.sleep(random.uniform(0, 0.05))


def read_data(lib, sym, done, error):
    while not done.value or error.value:
        vs = lib.list_versions(sym)
        for idx in range(len(vs) - 1):
            assert vs[idx]["version"] == vs[idx + 1]["version"] + 1


@pytest.mark.skipif(
    os.environ.get("ARCTICDB_CODE_COVERAGE_BUILD", "0") == "1",
    reason=(
        "When we build for code coverage, we make a DEBUG binary, which is much slower and causes this test to take"
        " around ~4 hours which is breaking the build"
    ),
)
def test_stress_version_map_compact(object_version_store, sym, capsys):
    done = Value("b", 0)
    error = Value("b", 0)
    lib = object_version_store
    lib.version_store._set_validate_version_map()
    with capsys.disabled():
        try:
            log.version.warn("Starting writer")
            writer = Process(name="writer", target=write_data, args=(lib, sym, done, error))
            writer.start()
            log.version.info("Starting compacter")
            compacter = Process(name="compacter", target=compact_data, args=(lib, sym, done, error))
            compacter.start()
            log.version.info("Starting reader")
            reader = Process(name="reader", target=read_data, args=(lib, sym, done, error))
            reader.start()

            log.version.info("Joining writer")
            writer.join()
            log.version.info("Joining compacter")
            compacter.join()
            log.version.info("Joining reader")
            reader.join()
            assert error.value == 0
            log.version.info("Done")
        finally:
            log.version.info("Clearing library")
            lib.version_store.clear()
            log.version.info("Finished")
