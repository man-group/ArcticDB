import os
import pandas as pd
import numpy as np
import pytest
import sys

from arcticdb.util.logger import get_logger
from arcticdb_ext.tools import ReliableStorageLock, ReliableStorageLockManager
from tests.util.mark import REAL_S3_TESTS_MARK, WINDOWS

import time

from arcticdb.util.test import assert_frame_equal
from multiprocessing import Process

logger = get_logger()

one_sec = 1_000_000_000

symbol_prefix = "process_id_"

max_processes = 30 if WINDOWS else 100 # Too many processes will trigger out of mem on windows
storage_lock_timeout_sec = 20 if WINDOWS else 10 # For Windows choosing longer wait for default storage lock timeout


def slow_increment_task(real_storage_factory, lib_name, symbol, sleep_time):
    # We need to explicitly build the library object in each process, otherwise the s3 library doesn't get copied
    # properly between processes, and we get spurious `XAmzContentSHA256Mismatch` errors.
    pid = os.getpid()
    logger.info(f"Process {pid}: initiated")
    fixture = real_storage_factory.create_fixture()
    lib = fixture.create_arctic()[lib_name]
    lock = ReliableStorageLock("test_lock", lib._nvs._library, storage_lock_timeout_sec * one_sec)
    lock_manager = ReliableStorageLockManager()
    lock_manager.take_lock_guard(lock)
    logger.info(f"Process {pid}: start read")
    df = lib.read(symbol).data
    logger.info(f"Process {pid}: previous value {df['col'][0]}")
    df["col", 0] = df["col"][0] + 1
    time.sleep(sleep_time)
    lib.write(symbol, df)
    logger.info(f"Process {pid}: incrementing and saving value {df['col'][0]}")
    symbol_name = f"{symbol_prefix}{pid}"
    lib.write(symbol_name, df)
    logger.info(f"Process {pid}: wrote unique symbol {symbol_name}")
    lock_manager.free_lock_guard()
    logger.info(f"Process {pid}: completed")

# NOTE: Is there is not enough memory the number of actually spawned processes
# will be lowe. The test counts the actual processes that did really got executed
@pytest.mark.parametrize("num_processes,max_sleep", [(max_processes, 1), (5, 2 * storage_lock_timeout_sec)])
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

    symbols = lib.list_symbols(regex=f"{symbol_prefix}.*")
    num_processes_succeeded = len(symbols)
    logger.info(f"Total number liver processes{num_processes_succeeded}")
    logger.info(f"{symbols}")

    vit = lib.read(symbol)
    read_df = vit.data
    expected_df = pd.DataFrame({"col": [num_processes_succeeded]})
    assert_frame_equal(read_df, expected_df)
    assert vit.version == num_processes_succeeded
