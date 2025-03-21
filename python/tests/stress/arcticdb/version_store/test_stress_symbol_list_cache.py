import os

import pandas as pd
import numpy as np
import pytest
import sys

from arcticdb_ext import set_config_int
from tests.util.mark import REAL_S3_TESTS_MARK

import time

from multiprocessing import Process, Queue


one_sec = 1_000_000_000


def write_symbols_worker(real_s3_storage_factory, lib_name, result_queue, run_time, step_symbol_id, first_symbol_id):
    fixture = real_s3_storage_factory.create_fixture()
    lib = fixture.create_arctic()[lib_name]
    df = pd.DataFrame({"col": [1, 2, 3]})
    cnt = 0
    start_time = time.time()
    while time.time() - start_time < run_time:
        id = cnt * step_symbol_id + first_symbol_id
        lib.write(f"sym_{id}", df)
        cnt += 1

    result_queue.put((first_symbol_id, cnt))


def compact_symbol_list_worker(real_s3_storage_factory, lib_name, run_time):
    # Decrease the lock wait times to make lock failures more likely
    set_config_int("StorageLock.WaitMs", 1)
    # Trigger symbol list compaction on every list_symbols call
    set_config_int("SymbolList.MaxDelta", 1)
    fixture = real_s3_storage_factory.create_fixture()
    lib = fixture.create_arctic()[lib_name]

    start_time = time.time()
    while time.time() - start_time < run_time:
        lib.list_symbols()

# @REAL_S3_TESTS_MARK
@pytest.mark.parametrize("num_writers, num_compactors", [(2, 10), (5, 50)])
def test_stress_only_add_v0(real_s3_storage_factory, lib_name, num_writers, num_compactors):
    run_time = 20
    fixture = real_s3_storage_factory.create_fixture()
    ac = fixture.create_arctic()
    ac.delete_library(lib_name) # To make sure we have a clean slate
    lib = ac.create_library(lib_name)
    results_queue = Queue()

    writers = [
        Process(target=write_symbols_worker, args=(real_s3_storage_factory, lib_name, results_queue, run_time, num_writers, i))
        for i in range(num_writers)
    ]

    compactors = [
        Process(target=compact_symbol_list_worker, args=(real_s3_storage_factory, lib_name, run_time))
        for i in range(num_compactors)
    ]

    processes = writers + compactors

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    expected_symbol_list = set()
    while not results_queue.empty():
        first_id, cnt = results_queue.get()
        expected_symbol_list.update([f"sym_{first_id + i*num_writers}" for i in range(cnt)])

    result_symbol_list = set(lib.list_symbols())
    assert len(result_symbol_list) == len(expected_symbol_list)
    assert result_symbol_list == expected_symbol_list



