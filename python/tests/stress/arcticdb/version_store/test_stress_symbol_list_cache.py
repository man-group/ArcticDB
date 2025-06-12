import os
import time
from multiprocessing import Process, Queue

import pandas as pd
import pytest
from arcticdb_ext import set_config_int

from arcticdb.options import LibraryOptions, EnterpriseLibraryOptions
from arcticdb.config import set_log_level
from arcticdb_ext.storage import KeyType


def write_symbols_worker(lib, result_queue):
    df = pd.DataFrame({"col": [1, 2, 3]})
    id = os.getpid()
    sym = f"sym_{id}"
    lib.write(sym, df)
    result_queue.put(id)

@pytest.fixture(params=[
    (0, 0, 0), # Probability of slowdown, min ms, max ms
    (0.1, 10, 50),
    (0.3, 300, 900),
    (0.5, 700, 1500),
    (0.5, 1100, 1700),
    (0.7, 1600, 2000)
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
@pytest.mark.parametrize("num_writers, num_compactors", [(1,1), (2, 10), (10, 50)])
def test_stress_compaction_many_writers(slow_writing_library, num_writers, num_compactors):
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

    expected_keys = 1
    cfg = slow_writing_library._nvs._lib_cfg
    if (
        num_compactors == 1
        and cfg.lib_desc.version.failure_sim.write_slowdown_prob > 0 
        and cfg.lib_desc.version.failure_sim.slow_down_max_ms >= 1500 
        ):
        expected_keys = 0 # If we have a single compactor and it the slowdown is too big, we expect 0 keys
    assert len(compacted_keys) == expected_keys
