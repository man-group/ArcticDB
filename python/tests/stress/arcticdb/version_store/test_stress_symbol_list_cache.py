import os
import time
from multiprocessing import Process, Queue

import pandas as pd
import pytest
from arcticdb_ext import set_config_int

from arcticdb.options import LibraryOptions, EnterpriseLibraryOptions
from arcticdb.config import set_log_level
from arcticdb import log
from arcticdb_ext.storage import KeyType

from tests.util.mark import REAL_S3_TESTS_MARK


def write_symbols_worker(lib, sym_id):
    df = pd.DataFrame({"col": [1, 2, 3]})
    sym = f"sym_{sym_id}"
    lib.write(sym, df)

def compact_symbols_worker(lib):
    set_config_int("SymbolList.MaxDelta", 1) # Trigger symbol list compaction on every list_symbols call
    set_log_level(specific_log_levels = {"lock":"DEBUG"})
    lib.list_symbols()

@pytest.fixture(params=[
    (0, 0, 0), # Probability of slowdown, min ms, max ms
    (0.3, 10, 50),
    (0.3, 100, 300),
    (0.3, 300, 500),
    (0.3, 700, 1200)
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

@REAL_S3_TESTS_MARK
@pytest.mark.parametrize("num_writers, num_compactors", [(1,1), (2, 10), (10, 50)])
def test_stress_compaction_many_writers(slow_writing_library, num_writers, num_compactors):
    writers = [
        Process(target=write_symbols_worker, args=(slow_writing_library, i))
        for i in range(num_writers)
    ]

    compactors = [
        Process(target=compact_symbols_worker, args=(slow_writing_library,))
        for i in range(num_compactors)
    ]

    processes = writers + compactors

    for p in processes:
        p.start()

    for p in processes:
        p.join()
        if p.exitcode != 0:
            raise RuntimeError(f"Process {p.pid} failed with exit code {p.exitcode}")

    expected_symbol_list = { f"sym_{i}" for i in range(num_writers) }

    result_symbol_list = set(slow_writing_library.list_symbols())
    assert len(result_symbol_list) == len(expected_symbol_list)
    assert result_symbol_list == expected_symbol_list

@pytest.mark.parametrize("compact_threshold", [1, 3, 8, 10])
def test_compaction_produces_single_key(real_s3_storage, lib_name, compact_threshold):
    df = pd.DataFrame({"col": [1]})
    real_s3_library = real_s3_storage.create_arctic().create_library(lib_name)
    num_symbols = 10
    set_config_int("SymbolList.MaxDelta", compact_threshold)
    for i in range(num_symbols):
        sym = f"sym_{i}"
        real_s3_library.write(sym, df)
        symbols = real_s3_library.list_symbols()

    expected_symbol_list = { f"sym_{i}" for i in range(num_symbols) }

    result_symbol_list = set(symbols)
    assert len(result_symbol_list) == len(expected_symbol_list)
    assert result_symbol_list == expected_symbol_list

    lt = real_s3_library._dev_tools.library_tool()
    all_keys = lt.find_keys(KeyType.SYMBOL_LIST)
    compacted_keys = [x for x in all_keys if x.id == "__symbols__"]
    add_keys = [x for x in all_keys if x.id == "__add__"]
    delete_keys = [x for x in all_keys if x.id == "__delete__"]
    other_keys = [x for x in all_keys if x.id != "__delete__" and x.id != "__add__" and x.id != "__symbols__"]

    expected_num_compacted_keys = 1 # First list_symbols call always compacts
    expected_add_keys = (num_symbols - 1) % compact_threshold
    expected_delete_keys = 0

    assert len(compacted_keys) == expected_num_compacted_keys
    assert len(add_keys) == expected_add_keys
    assert len(delete_keys) == expected_delete_keys
    assert len(other_keys) == 0
