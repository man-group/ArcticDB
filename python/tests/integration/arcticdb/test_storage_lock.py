import pandas as pd
import numpy as np
import pytest

from arcticdb_ext.tools import ReliableStorageLock, ReliableStorageLockManager
from tests.util.mark import PERSISTENT_STORAGE_TESTS_ENABLED, REAL_S3_TESTS_MARK

from multiprocessing import Process, set_start_method
set_start_method("fork") # Okay to fork an S3 lib
import time

from arcticdb.util.test import assert_frame_equal


one_sec = 1_000_000_000


def slow_increment_task(lib, symbol, sleep_time, lock_manager, lock):
    lock_manager.take_lock_guard(lock)
    df = lib.read(symbol).data
    df["col"][0] = df["col"][0] + 1
    time.sleep(sleep_time)
    lib.write(symbol, df)
    lock_manager.free_lock_guard()


@pytest.mark.parametrize("num_processes,max_sleep", [(100, 1), (5, 20)])
@REAL_S3_TESTS_MARK
def test_many_increments(real_s3_version_store, num_processes, max_sleep):
    lib = real_s3_version_store
    init_df = pd.DataFrame({"col": [0]})
    symbol = "counter"
    lib.write(symbol, init_df)
    lock = ReliableStorageLock("test_lock", lib._library, 10*one_sec)
    lock_manager = ReliableStorageLockManager()

    processes = [
        Process(target=slow_increment_task, args=(lib, symbol, 0 if i%2==0 else max_sleep, lock_manager, lock))
        for i in range(num_processes)
    ]
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    read_df = lib.read(symbol).data
    expected_df = pd.DataFrame({"col": [num_processes]})
    assert_frame_equal(read_df, expected_df)
