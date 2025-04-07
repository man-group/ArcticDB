import pandas as pd
import numpy as np
import pytest
import sys

from arcticdb_ext.tools import ReliableStorageLock, ReliableStorageLockManager
from tests.util.mark import REAL_S3_TESTS_MARK

import time

from arcticdb.util.test import assert_frame_equal
from multiprocessing import Process


one_sec = 1_000_000_000


def slow_increment_task(real_storage_factory, lib_name, symbol, sleep_time):
    # We need to explicitly build the library object in each process, otherwise the s3 library doesn't get copied
    # properly between processes, and we get spurious `XAmzContentSHA256Mismatch` errors.
    fixture = real_storage_factory.create_fixture()
    lib = fixture.create_arctic()[lib_name]
    lock = ReliableStorageLock("test_lock", lib._nvs._library, 10 * one_sec)
    lock_manager = ReliableStorageLockManager()
    lock_manager.take_lock_guard(lock)
    df = lib.read(symbol).data
    df["col"][0] = df["col"][0] + 1
    time.sleep(sleep_time)
    lib.write(symbol, df)
    lock_manager.free_lock_guard()


@pytest.mark.parametrize("num_processes,max_sleep", [(100, 1), (5, 20)])
@REAL_S3_TESTS_MARK
@pytest.mark.storage
def test_many_increments(real_storage_factory, lib_name, num_processes, max_sleep):
    fixture = real_storage_factory.create_fixture()
    lib = fixture.create_arctic().create_library(lib_name)
    init_df = pd.DataFrame({"col": [0]})
    symbol = "counter"
    lib._nvs.version_store.force_delete_symbol(symbol)
    lib.write(symbol, init_df)

    processes = [
        Process(target=slow_increment_task, args=(real_storage_factory, lib_name, symbol, 0 if i % 2 == 0 else max_sleep))
        for i in range(num_processes)
    ]
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    vit = lib.read(symbol)
    read_df = vit.data
    expected_df = pd.DataFrame({"col": [num_processes]})
    assert_frame_equal(read_df, expected_df)
    assert vit.version == num_processes
