import os
import pandas as pd
import numpy as np
import pytest
import sys

from arcticdb.util.logger import get_logger
from arcticdb_ext.tools import ReliableStorageLock, ReliableStorageLockManager
from arcticdb.util.test import config_context
from tests.util.mark import REAL_S3_TESTS_MARK, WINDOWS

import time

from arcticdb.util.test import assert_frame_equal
from multiprocessing import Process

logger = get_logger()

one_sec = 1_000_000_000

symbol_prefix = "process_id_"

max_processes = 30 if WINDOWS else 100  # Too many processes will trigger out of mem on windows
storage_lock_timeout_sec = 20 if WINDOWS else 10  # For Windows choosing longer wait for default storage lock timeout


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
        Process(
            target=slow_increment_task, args=(real_storage_factory, lib_name, symbol, 0 if i % 2 == 0 else max_sleep)
        )
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


# --- Plain (unreliable) StorageLock coverage -------------------------------------------------------------------------
# These run on the in-memory backend; the unreliable lock works on any store. A short WaitMs keeps the acquire
# confirmation delay small.


def test_storage_lock_lifecycle(mem_library):
    with config_context("StorageLock.WaitMs", 50):
        l1 = mem_library._nvs.library_tool().get_storage_lock("lock")
        l2 = mem_library._nvs.library_tool().get_storage_lock("lock")

        assert l1.try_lock()
        assert not l2.try_lock()
        l1.unlock()

        assert l2.try_lock()
        l2.unlock()

        l1.lock()
        assert not l2.try_lock()
        l1.unlock()


def test_storage_lock_timeout_when_held(mem_library):
    with config_context("StorageLock.WaitMs", 50):
        l1 = mem_library._nvs.library_tool().get_storage_lock("lock")
        l2 = mem_library._nvs.library_tool().get_storage_lock("lock")

        assert l1.try_lock()
        with pytest.raises(Exception):
            l2.lock_timeout(100)
        l1.unlock()


def test_storage_lock_metadata_round_trip(mem_library):
    with config_context("StorageLock.WaitMs", 50):
        writer = mem_library._nvs.library_tool().get_storage_lock("lock")
        reader = mem_library._nvs.library_tool().get_storage_lock("lock")

        # No lock yet
        assert reader.read_metadata() is None

        # A separate reader observes the holder's metadata (the trace use-case)
        writer.lock(metadata={"job_name": "blah", "n": 42})
        assert reader.read_metadata() == {"job_name": "blah", "n": 42}

        # Metadata gone after unlock
        writer.unlock()
        assert reader.read_metadata() is None

        # Locking without metadata reports no metadata
        writer.lock()
        assert reader.read_metadata() is None
        writer.unlock()


def test_list_storage_locks_unreliable(mem_library):
    with config_context("StorageLock.WaitMs", 50):
        lock = mem_library._nvs.library_tool().get_storage_lock("mylock")
        lib_tool = mem_library._nvs.library_tool()

        assert lib_tool.list_storage_locks() == []

        lock.lock(metadata={"who": "me"})
        locks = lib_tool.list_storage_locks()
        assert len(locks) == 1
        info = locks[0]
        assert info["name"] == "mylock"
        assert info["active"] is True
        assert info["metadata"] == {"who": "me"}
        assert "timestamp" in info

        lock.unlock()
        assert lib_tool.list_storage_locks() == []
