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
    (0.1, 10, 50),
    (0.5, 300, 700),
    (0.7, 1100, 1700)
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

    lt = slow_writing_library._dev_tools.library_tool()
    compacted_keys = lt.find_keys_for_id(KeyType.SYMBOL_LIST, "__symbols__")

    cfg = slow_writing_library._nvs._lib_cfg
    if (
        num_compactors == 1
        and cfg.lib_desc.version.failure_sim.write_slowdown_prob > 0 
        and cfg.lib_desc.version.failure_sim.slow_down_max_ms > 500
        ):
        assert len(compacted_keys) <= 1 # If we have a single compactor and a big slowdown is possible it either fails or succeeds (depending on the slowdown)
        return
    assert len(compacted_keys) == 1
