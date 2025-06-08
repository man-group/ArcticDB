import os
import time
from multiprocessing import Process, Queue

import pandas as pd
import pytest
from arcticdb_ext import set_config_int

from arcticdb.options import LibraryOptions, EnterpriseLibraryOptions
from arcticdb_ext.storage import KeyType

from python.arcticdb.config import set_log_level

one_sec = 1_000_000_000


def write_symbols_worker(lib, result_queue):
    df = pd.DataFrame({"col": [1, 2, 3]})
    id = os.getpid()
    sym = f"sym_{id}"
    lib.write(sym, df)
    result_queue.put(id)

@pytest.fixture(params=[
    #(0, 0, 0)
    #(0.1, 10, 50)  # (prob, min_ms, max_ms)
    #(0.5, 700, 1500)
    (0.5, 1100, 1700)
])
def slow_writing_library(request, real_s3_storage, lib_name):
    write_slowdown_prob, write_slowdown_min_ms, write_slowdown_max_ms = request.param
    arctic = real_s3_storage.create_arctic()
    cfg = arctic._library_adapter.get_library_config(lib_name, LibraryOptions(), EnterpriseLibraryOptions())
    cfg.lib_desc.version.failure_sim.write_slowdown_prob = write_slowdown_prob
    cfg.lib_desc.version.failure_sim.slow_down_min_ms = write_slowdown_min_ms
    cfg.lib_desc.version.failure_sim.slow_down_max_ms = write_slowdown_max_ms
    arctic._library_manager.write_library_config(cfg, lib_name)
    yield arctic.get_library(lib_name)
    arctic.delete_library(lib_name)

#@REAL_S3_TESTS_MARK
@pytest.mark.parametrize("num_writers, num_compactors", [(2, 10)])
def test_stress_only_add_v0(slow_writing_library, lib_name, num_writers, num_compactors):
    set_config_int("SymbolList.MaxDelta", 1) # Trigger symbol list compaction on every list_symbols call
    set_log_level(specific_log_levels={"lock": "DEBUG"})
    results_queue = Queue()

    writers = [
        Process(target=write_symbols_worker, args=(slow_writing_library, results_queue))
        for i in range(num_writers)
    ]

    compactors = [
        Process(target=slow_writing_library.list_symbols)
        for i in range(num_compactors)
    ]

    processes = writers + compactors

    for p in processes:
        p.start()

    for p in processes:
        p.join()
        if p.exitcode != 0:
            raise RuntimeError(f"Process {p.pid} failed with exit code {p.exitcode}")

    expected_symbol_list = set()

    while not results_queue.empty():
        expected_symbol_list.add(f"sym_{results_queue.get()}")

    result_symbol_list = set(slow_writing_library.list_symbols())
    assert len(result_symbol_list) == len(expected_symbol_list)
    assert result_symbol_list == expected_symbol_list

    lt = slow_writing_library._dev_tools.library_tool()
    compacted_keys = lt.find_keys_for_id(KeyType.SYMBOL_LIST, "__symbols__")
    assert len(compacted_keys) == 1
